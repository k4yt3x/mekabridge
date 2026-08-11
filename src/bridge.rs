//! Startup wiring, supervision, and graceful shutdown.
//!
//! The MCP listener is bound first, before any channel connects or the queue starts moving, so a
//! port conflict is a startup failure rather than a bridge that runs with no way for the agent to
//! answer anybody. meka retries a failed MCP connect in the background, so the bridge no longer has
//! to be up before meka; being up first still avoids the window where `[mcp].strict` refuses turns.

pub mod envelope;
pub mod inbound;
pub mod turn;

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use base64::Engine as _;
use chrono::Utc;
use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;

use crate::{
    bridge::{
        inbound::DrainContext,
        turn::{Presence, TurnRunner},
    },
    channel::{Channel, ChannelRegistry, ConversationId, InboundEvent, SendOptions},
    config::{Config, StorageConfig},
    error::Result,
    mcp::{
        ConversationSummary, DownloadedAttachment, OutboundSink, SinkError, ViewedAttachment, serve,
    },
    meka::MekaClient,
    store::Store,
};

/// Buffer between the channel pollers and the durable writer.
///
/// Backpressure here is deliberate. When the writer falls behind, blocking the poller is correct:
/// Telegram retains undelivered updates, whereas an unbounded in-memory buffer would lose them on a
/// crash, which is exactly what the durable queue exists to prevent.
const EVENT_BUFFER: usize = 64;

/// How long shutdown waits for an in-flight turn before giving up on it.
const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

/// How often expired attachments are swept.
const JANITOR_INTERVAL: Duration = Duration::from_secs(60 * 60);

/// How long delivered queue rows are kept.
///
/// They are not deleted on completion because they are what makes duplicate detection survive a
/// restart: Telegram replays updates whose offset was never committed.
const DELIVERED_RETENTION: chrono::Duration = chrono::Duration::days(7);

/// Media types meka will pass through to the provider as an image, from its
/// `ALLOWED_IMAGE_MIME_TYPES`. Anything else is replaced with a text placeholder on its side, so
/// the bridge screens for it here and returns a useful description instead.
const VIEWABLE_MEDIA_TYPES: &[&str] = &["image/png", "image/jpeg", "image/gif", "image/webp"];

/// meka's ceiling on a base64 image in an MCP tool result, from its `MAX_MCP_IMAGE_BYTES`.
const MAX_VIEW_BASE64_BYTES: usize = 10 * 1024 * 1024;

/// Ceiling on the raw bytes fetched for a view, sized so the base64 form stays under
/// [`MAX_VIEW_BASE64_BYTES`] with room to spare. Independent of `attachment_max_bytes`, which
/// bounds what may be written to disk rather than what may be shown.
const MAX_VIEW_BYTES: u64 = 7 * 1024 * 1024;

/// Run the bridge until a shutdown signal arrives.
pub async fn run(config: Config) -> Result<()> {
    let config = Arc::new(config);

    let store = Store::open(&config.storage.path).await?;
    let recovered = store.reset_in_flight().await?;
    if recovered > 0 {
        // Rows left in flight mean the previous run died mid-turn. The messages were never
        // delivered, so they go back in the queue.
        tracing::warn!(
            count = recovered,
            "recovered messages that were in flight when the bridge last stopped"
        );
    }

    let channels = Arc::new(ChannelRegistry::build(&config.channels)?);
    let meka = MekaClient::new(&config.meka)?;

    // Bind before anything else starts, so a port conflict is a startup failure rather than a meka
    // that can never take a turn.
    let mcp_server = serve::bind(&config.mcp).await?;
    if let Some(address) = mcp_server.local_addr() {
        tracing::info!("MCP endpoint bound to {}{}", address, config.mcp.path);
    }

    let shutdown = CancellationToken::new();
    let wake_drain = Arc::new(Notify::new());
    let (event_sender, event_receiver) = mpsc::channel::<InboundEvent>(EVENT_BUFFER);

    // Shared so the typing indicator can stop as soon as a reply actually lands.
    let presence = Arc::new(Presence::default());
    let sink = Arc::new(BridgeSink::new(
        store.clone(),
        Arc::clone(&channels),
        config.storage.clone(),
        meka.clone(),
        Arc::clone(&presence),
    ));
    let mut tasks = tokio::task::JoinSet::new();

    tasks.spawn({
        let config = Arc::clone(&config);
        let shutdown = shutdown.clone();
        let sink = Arc::clone(&sink) as Arc<dyn OutboundSink>;
        async move {
            if let Err(error) = serve::run(mcp_server, &config.mcp, sink, shutdown).await {
                tracing::error!("MCP server stopped: {}", error);
            }
        }
    });

    for channel in channels.iter() {
        let channel = Arc::clone(channel);
        let sender = event_sender.clone();
        let shutdown = shutdown.clone();
        tasks.spawn(async move {
            let id = channel.id().clone();
            match channel.run(sender, shutdown).await {
                Ok(()) => tracing::info!(channel = %id, "channel stopped"),
                // One channel failing must not take the others down: a Telegram outage should not
                // stop a Discord bridge, and the operator needs the process alive to see the log.
                Err(error) => {
                    tracing::error!(channel = %id, "channel stopped with an error: {}", error)
                }
            }
        });
    }
    // The writer ends when every channel has dropped its sender, so this handle must not linger.
    drop(event_sender);

    tasks.spawn({
        let store = store.clone();
        let config = Arc::clone(&config);
        let wake_drain = Arc::clone(&wake_drain);
        async move { inbound::writer(store, config, event_receiver, wake_drain).await }
    });

    tasks.spawn({
        let context = DrainContext {
            store: store.clone(),
            config: Arc::clone(&config),
            meka: meka.clone(),
            channels: Arc::clone(&channels),
            runner: TurnRunner::new(
                meka.clone(),
                Arc::clone(&channels),
                config.bridge.typing_indicator,
                Arc::clone(&presence),
            ),
            identities: Arc::new(tokio::sync::OnceCell::new()),
            permission_checked: Arc::new(tokio::sync::OnceCell::new()),
        };
        let wake_drain = Arc::clone(&wake_drain);
        let shutdown = shutdown.clone();
        async move { inbound::drain_loop(context, wake_drain, shutdown).await }
    });

    tasks.spawn({
        let store = store.clone();
        let config = Arc::clone(&config);
        let shutdown = shutdown.clone();
        async move { janitor(store, config, shutdown).await }
    });

    // Anything already queued from a previous run should go out without waiting for a poll tick.
    wake_drain.notify_one();
    tracing::info!(
        channels = channels.count(),
        "mekabridge is running; waiting for messages"
    );

    shutdown_signal().await;
    tracing::info!("shutting down");
    shutdown.cancel();

    match tokio::time::timeout(SHUTDOWN_DRAIN_TIMEOUT, async {
        while tasks.join_next().await.is_some() {}
    })
    .await
    {
        Ok(()) => tracing::info!("all tasks stopped cleanly"),
        Err(_elapsed) => {
            // An in-flight turn can legitimately outlive the drain window. Its batch stays
            // `in_flight` and the next start recovers it.
            tracing::warn!(
                "shutdown timed out after {}s; abandoning in-flight work",
                SHUTDOWN_DRAIN_TIMEOUT.as_secs()
            );
            tasks.abort_all();
        }
    }

    if let Err(error) = store.checkpoint().await {
        tracing::warn!("WAL checkpoint on shutdown failed: {}", error);
    }
    Ok(())
}

/// Periodically drop expired attachments and old delivered queue rows.
async fn janitor(store: Store, config: Arc<Config>, shutdown: CancellationToken) {
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return,
            () = tokio::time::sleep(JANITOR_INTERVAL) => {}
        }

        let retention = match chrono::Duration::from_std(config.storage.attachment_retention) {
            Ok(retention) => retention,
            Err(error) => {
                tracing::error!("[storage].attachment_retention is out of range: {}", error);
                continue;
            }
        };
        match store.take_expired_attachments(Utc::now() - retention).await {
            Ok(paths) => {
                for path in paths {
                    if let Err(error) = tokio::fs::remove_file(&path).await
                        && error.kind() != std::io::ErrorKind::NotFound
                    {
                        tracing::warn!("could not delete {}: {}", path.display(), error);
                    }
                }
            }
            Err(error) => tracing::error!("attachment sweep failed: {}", error),
        }

        match store
            .prune_delivered(Utc::now() - DELIVERED_RETENTION)
            .await
        {
            Ok(0) => {}
            Ok(count) => tracing::debug!(count, "pruned delivered queue rows"),
            Err(error) => tracing::error!("queue prune failed: {}", error),
        }
    }
}

/// Wait for SIGTERM or SIGINT.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(stream) => stream,
                Err(error) => {
                    tracing::warn!(
                        "could not install a SIGTERM handler ({}); relying on Ctrl+C alone",
                        error
                    );
                    if let Err(error) = tokio::signal::ctrl_c().await {
                        tracing::error!("failed to wait for Ctrl+C: {}", error);
                    }
                    return;
                }
            };
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                if let Err(error) = result {
                    tracing::error!("failed to wait for Ctrl+C: {}", error);
                }
                tracing::info!("SIGINT received");
            }
            _ = terminate.recv() => tracing::info!("SIGTERM received"),
        }
    }
    #[cfg(not(unix))]
    {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!("failed to wait for Ctrl+C: {}", error);
        }
        tracing::info!("Ctrl+C received");
    }
}

/// Implements the MCP server's outbound port over the channel registry.
///
/// Sends are validated against the conversation store, so the agent can only reach somebody the
/// bridge has actually seen. That is not much of a restriction in practice (Telegram bots cannot
/// open a conversation anyway) but it turns a hallucinated id into a clear tool error instead of an
/// opaque platform rejection.
pub struct BridgeSink {
    store: Store,
    channels: Arc<ChannelRegistry>,
    storage: StorageConfig,
    meka: MekaClient,
    presence: Arc<Presence>,
    /// Whether meka's active profile accepts images, resolved on first use.
    ///
    /// Cached because it cannot change without restarting meka, and queried lazily rather than at
    /// startup because the bridge deliberately comes up before meka does.
    vision: tokio::sync::OnceCell<bool>,
}

impl BridgeSink {
    /// Build the sink over a store and a channel registry.
    pub fn new(
        store: Store,
        channels: Arc<ChannelRegistry>,
        storage: StorageConfig,
        meka: MekaClient,
        presence: Arc<Presence>,
    ) -> Self {
        Self {
            store,
            channels,
            storage,
            meka,
            presence,
            vision: tokio::sync::OnceCell::new(),
        }
    }

    /// Whether meka will accept an image block, asked once and remembered.
    async fn vision_enabled(&self) -> bool {
        if let Some(known) = self.vision.get() {
            return *known;
        }
        match self.meka.info().await {
            Ok(info) => {
                // Only a successful answer is cached; a transient failure must not pin this to
                // false for the life of the process.
                let _ = self.vision.set(info.vision);
                info.vision
            }
            Err(error) => {
                tracing::warn!(
                    "could not read meka's vision capability ({}); describing images instead of \
                     showing them",
                    error
                );
                false
            }
        }
    }

    /// Resolve a handle to its record and the channel that can fetch it.
    async fn attachment(
        &self,
        handle: &str,
    ) -> std::result::Result<(crate::store::StoredAttachment, Arc<dyn Channel>), SinkError> {
        let record = self
            .store
            .attachment(handle)
            .await
            .map_err(|error| SinkError::Internal(error.to_string()))?
            .ok_or_else(|| SinkError::UnknownAttachment(handle.to_string()))?;
        let channel = self
            .channels
            .get(&record.channel_id)
            .ok_or_else(|| SinkError::UnknownChannel {
                conversation: record.conversation_id.clone(),
                channel: record.channel_id.clone(),
            })?
            .clone();
        Ok((record, channel))
    }

    async fn resolve(&self, id: &str) -> std::result::Result<ConversationId, SinkError> {
        let conversation = ConversationId::parse(id)
            .ok_or_else(|| SinkError::UnknownConversation(id.to_string()))?;
        let known = self
            .store
            .conversation(conversation.as_str())
            .await
            .map_err(|error| SinkError::Internal(error.to_string()))?;
        if known.is_none() {
            return Err(SinkError::UnknownConversation(id.to_string()));
        }
        if self.channels.get(conversation.channel()).is_none() {
            return Err(SinkError::UnknownChannel {
                conversation: id.to_string(),
                channel: conversation.channel().to_string(),
            });
        }
        Ok(conversation)
    }

    async fn note_sent(&self, conversation: &ConversationId) {
        // Stops the typing indicator re-arming here. Telegram already cleared it when this message
        // landed, so setting it again would announce a follow-up that is not coming.
        self.presence.note_sent(conversation);
        if let Err(error) = self
            .store
            .touch_outbound(conversation.as_str(), Utc::now())
            .await
        {
            tracing::warn!("failed to record an outbound message: {}", error);
        }
    }
}

#[async_trait]
impl OutboundSink for BridgeSink {
    async fn send_text(
        &self,
        conversation: &str,
        markdown: &str,
        options: SendOptions,
    ) -> std::result::Result<Vec<String>, SinkError> {
        let conversation = self.resolve(conversation).await?;
        let channel = self
            .channels
            .resolve(&conversation)
            .map_err(|error| SinkError::Internal(error.to_string()))?;
        let sent = channel
            .send_text(&conversation, markdown, &options)
            .await
            .map_err(|error| SinkError::Delivery(error.to_string()))?;
        self.note_sent(&conversation).await;
        tracing::info!(
            conversation = %conversation,
            parts = sent.len(),
            "the agent sent a message"
        );
        Ok(sent)
    }

    async fn send_file(
        &self,
        conversation: &str,
        path: &std::path::Path,
        caption: Option<&str>,
        as_photo: bool,
    ) -> std::result::Result<Vec<String>, SinkError> {
        let conversation = self.resolve(conversation).await?;
        let channel = self
            .channels
            .resolve(&conversation)
            .map_err(|error| SinkError::Internal(error.to_string()))?;
        let capabilities = channel.capabilities();
        if as_photo && !capabilities.photos {
            return Err(SinkError::Delivery(format!(
                "channel {} cannot send photos",
                conversation.channel()
            )));
        }
        if !as_photo && !capabilities.files {
            return Err(SinkError::Delivery(format!(
                "channel {} cannot send files",
                conversation.channel()
            )));
        }
        let sent = channel
            .send_file(&conversation, path, caption, as_photo)
            .await
            .map_err(|error| SinkError::Delivery(error.to_string()))?;
        self.note_sent(&conversation).await;
        tracing::info!(conversation = %conversation, "the agent sent a file");
        Ok(sent)
    }

    async fn react(
        &self,
        conversation: &str,
        message_id: &str,
        emoji: Option<&str>,
    ) -> std::result::Result<(), SinkError> {
        let conversation = self.resolve(conversation).await?;
        let channel = self
            .channels
            .resolve(&conversation)
            .map_err(|error| SinkError::Internal(error.to_string()))?;
        if !channel.capabilities().reactions {
            return Err(SinkError::Delivery(format!(
                "channel {} does not support reactions",
                conversation.channel()
            )));
        }
        channel
            .react(&conversation, message_id, emoji)
            .await
            .map_err(|error| SinkError::Delivery(error.to_string()))?;
        // Deliberately not `note_sent`: a reaction is not a message, and letting it advance
        // `last_outbound_at` would make a conversation the agent merely acknowledged look like one
        // it has actually replied in.
        tracing::info!(
            conversation = %conversation,
            message_id = %message_id,
            emoji = ?emoji,
            "the agent reacted to a message"
        );
        Ok(())
    }

    async fn view_attachment(
        &self,
        handle: &str,
    ) -> std::result::Result<ViewedAttachment, SinkError> {
        let (record, channel) = self.attachment(handle).await?;

        if !self.vision_enabled().await {
            return Ok(ViewedAttachment::Description(format!(
                "This is a {} ({}). The current model has no vision, so it cannot be shown. Use \
                 download_attachment to get the file itself.",
                record.kind,
                describe_file(&record)
            )));
        }

        // A video, animation, or animated sticker is not a viewable image, but the platform already
        // generated a still for it, so that is what "show me" resolves to. Falling back like this
        // has to be said out loud: a single frame of a video, or a preview of page one of a PDF, is
        // not the file, and an agent shown one without comment would reasonably think otherwise.
        let (file_ref, preview) = match (&record.thumb_ref, is_viewable_image(&record)) {
            (_, true) => (Some(record.file_ref.clone()), false),
            (Some(thumb_ref), false) => (Some(thumb_ref.clone()), true),
            (None, false) => (None, false),
        };
        let Some(file_ref) = file_ref else {
            return Ok(ViewedAttachment::Description(format!(
                "This is a {} ({}) and has no image preview. Use download_attachment to get the \
                 file itself.",
                record.kind,
                describe_file(&record)
            )));
        };

        let fetched = channel
            .fetch(&file_ref, MAX_VIEW_BYTES)
            .await
            .map_err(|error| SinkError::Delivery(error.to_string()))?;

        let media_type = fetched
            .media_type
            .or_else(|| record.media_type.clone())
            .unwrap_or_else(|| "image/jpeg".to_string());
        if !VIEWABLE_MEDIA_TYPES.contains(&media_type.as_str()) {
            return Ok(ViewedAttachment::Description(format!(
                "This is a {} of type {media_type}, which cannot be shown as an image. Use \
                 download_attachment to get the file itself.",
                record.kind
            )));
        }

        let data = base64::engine::general_purpose::STANDARD.encode(&fetched.bytes);
        // Unreachable while `MAX_VIEW_BYTES` stays under three quarters of this, which is the
        // point: it is the assertion that keeps the two constants honest with each other.
        // meka replaces an oversized image with a placeholder rather than erroring, so
        // crossing the line silently would look to the agent like a picture with nothing in
        // it.
        if data.len() > MAX_VIEW_BASE64_BYTES {
            return Ok(ViewedAttachment::Description(format!(
                "This {} is {} bytes, too large to show inline. Use download_attachment to get the \
                 file itself.",
                record.kind,
                fetched.bytes.len()
            )));
        }
        tracing::info!(handle = %record.handle, preview, "the agent viewed an attachment");
        Ok(ViewedAttachment::Image {
            media_type,
            data,
            note: preview.then(|| {
                format!(
                    "This is the preview frame for a {}, not the {} itself ({}).",
                    record.kind,
                    record.kind,
                    describe_file(&record)
                )
            }),
        })
    }

    async fn download_attachment(
        &self,
        handle: &str,
    ) -> std::result::Result<DownloadedAttachment, SinkError> {
        let (record, channel) = self.attachment(handle).await?;

        // Already on disk from an earlier call, so this is free and idempotent.
        if let Some(path) = &record.path
            && tokio::fs::try_exists(path).await.unwrap_or(false)
        {
            let bytes = tokio::fs::metadata(path)
                .await
                .map(|metadata| metadata.len())
                .unwrap_or_default();
            return Ok(DownloadedAttachment {
                path: path.clone(),
                bytes,
                media_type: record.media_type,
            });
        }

        let fetched = channel
            .fetch(&record.file_ref, self.storage.attachment_max_bytes)
            .await
            .map_err(|error| SinkError::Delivery(error.to_string()))?;

        tokio::fs::create_dir_all(&self.storage.attachment_dir)
            .await
            .map_err(|error| {
                SinkError::Internal(format!(
                    "could not create {}: {error}",
                    self.storage.attachment_dir.display()
                ))
            })?;

        // The platform's own extension is what lets a downstream tool recognise the type; the
        // sanitized reference keeps the name unique and inside the directory.
        let stem = sanitize_file_stem(&record.file_ref);
        let extension = fetched.extension.clone().or_else(|| {
            record
                .file_name
                .as_deref()
                .and_then(|name| std::path::Path::new(name).extension())
                .and_then(|extension| extension.to_str())
                .map(str::to_string)
        });
        let name = match extension {
            Some(extension) => format!("{stem}.{extension}"),
            None => stem,
        };
        let path = self.storage.attachment_dir.join(name);
        tokio::fs::write(&path, &fetched.bytes)
            .await
            .map_err(|error| {
                SinkError::Internal(format!("could not write {}: {error}", path.display()))
            })?;

        if let Err(error) = self
            .store
            .mark_attachment_downloaded(&record.handle, &path)
            .await
        {
            // The file is written and usable; only the retention sweep loses track of it.
            tracing::error!(
                handle = %record.handle,
                "failed to record the download, so this file will not be swept: {}",
                error
            );
        }
        tracing::info!(handle = %record.handle, path = %path.display(), "the agent downloaded an attachment");
        Ok(DownloadedAttachment {
            bytes: fetched.bytes.len() as u64,
            media_type: fetched.media_type.or(record.media_type),
            path,
        })
    }

    async fn conversations(
        &self,
        channel: Option<&str>,
        limit: usize,
    ) -> std::result::Result<Vec<ConversationSummary>, SinkError> {
        let records = self
            .store
            .list_conversations(channel, limit)
            .await
            .map_err(|error| SinkError::Internal(error.to_string()))?;
        Ok(records.into_iter().map(summarize).collect())
    }

    async fn conversation(
        &self,
        id: &str,
    ) -> std::result::Result<Option<ConversationSummary>, SinkError> {
        let record = self
            .store
            .conversation(id)
            .await
            .map_err(|error| SinkError::Internal(error.to_string()))?;
        Ok(record.map(summarize))
    }
}

/// Whether the attachment's own bytes are something a provider can look at directly.
fn is_viewable_image(record: &crate::store::StoredAttachment) -> bool {
    record
        .media_type
        .as_deref()
        .is_some_and(|media_type| VIEWABLE_MEDIA_TYPES.contains(&media_type))
}

/// Short human description of a file, for the cases where it cannot be shown.
fn describe_file(record: &crate::store::StoredAttachment) -> String {
    let mut parts = Vec::new();
    if let Some(name) = &record.file_name {
        parts.push(format!("{name:?}"));
    }
    if let Some(media_type) = &record.media_type {
        parts.push(media_type.clone());
    }
    if let Some(bytes) = record.bytes {
        parts.push(format!("{bytes} bytes"));
    }
    if parts.is_empty() {
        return "no further details".to_string();
    }
    parts.join(", ")
}

/// Reduce a platform file reference to something safe to use as a filename.
///
/// Telegram file ids are base64-ish and really do contain `/` and `-`, so they cannot be used
/// verbatim as a path component without risking traversal outside the attachment directory.
fn sanitize_file_stem(file_ref: &str) -> String {
    let cleaned: String = file_ref
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .take(48)
        .collect();
    if cleaned.is_empty() {
        format!("attachment-{}", Utc::now().timestamp_millis())
    } else {
        cleaned
    }
}

fn summarize(record: crate::store::ConversationRecord) -> ConversationSummary {
    ConversationSummary {
        id: record.id,
        channel: record.channel_id,
        platform: record.platform,
        title: record.title,
        kind: record.kind,
        last_inbound_at: record.last_inbound_at.map(|at| at.to_rfc3339()),
        last_outbound_at: record.last_outbound_at.map(|at| at.to_rfc3339()),
    }
}
