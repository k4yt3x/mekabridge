//! Startup wiring, supervision, and graceful shutdown.
//!
//! The MCP listener is bound first, before any channel connects or the queue starts moving, so a
//! port conflict is a startup failure rather than a bridge that runs with no way for the agent to
//! answer anybody. meka retries a failed MCP connect in the background, so the bridge no longer has
//! to be up before meka; being up first still avoids the window in which meka, unless its entry for
//! this bridge is marked `required`, runs turns with no tool to answer anybody.

pub mod envelope;
pub mod inbound;
pub mod turn;

use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

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
    channel::{
        Channel, ChannelRegistry, ChatKind, ConversationId, FileOptions, InboundEvent, SendOptions,
    },
    config::{Config, DefaultPolicy, StorageConfig},
    error::Result,
    mcp::{
        ConversationSummary, DownloadedAttachment, HistoryEntry, OutboundSink, SinkError,
        ToolSurface, ViewedAttachment, serve,
    },
    meka::MekaClient,
    store::{Policy, Store, UnseenSummary},
};

/// Buffer between the channel pollers and the durable writer.
///
/// A durability bound, not a throughput knob. teloxide advances its `getUpdates` offset the moment
/// a batch arrives and confirms it once its own buffer has drained into this channel, so blocking
/// the poller when this is full is what holds that confirmation back. Whatever sits here unwritten
/// when it goes out is acknowledged to Telegram and never sent again, which makes this depth the
/// number of messages a hard kill can lose.
///
/// Not one, though: Discord's typing notices ride the same channel on a `try_send` and are dropped
/// when it is full, so a depth an ordinary burst fills would stop the settle window working.
const EVENT_BUFFER: usize = 8;

/// How long shutdown waits for an in-flight turn before giving up on it.
const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

/// How often expired attachments are swept.
const JANITOR_INTERVAL: Duration = Duration::from_secs(60 * 60);

/// How long delivered queue rows are kept.
///
/// They are not deleted on completion because they are what makes duplicate detection survive a
/// restart: Telegram resends any update whose offset was never confirmed, and the bridge cannot
/// tell one of those from a message it has never seen without a record of having delivered it.
const DELIVERED_RETENTION: chrono::Duration = chrono::Duration::days(7);

/// Media types meka will pass through to the provider as an image, from its
/// `ALLOWED_IMAGE_MIME_TYPES`. Anything else is replaced with a text placeholder on its side, so
/// the bridge screens for it here and returns a useful description instead.
const VIEWABLE_MEDIA_TYPES: &[&str] = &["image/png", "image/jpeg", "image/gif", "image/webp"];

/// meka's ceiling on a base64 image in an MCP tool result, from its `MAX_MCP_IMAGE_BYTES`.
const MAX_VIEW_BASE64_BYTES: usize = 10 * 1024 * 1024;

/// Ceiling on the raw bytes fetched for a view. Independent of `attachment_max_bytes`, which bounds
/// what may be written to disk rather than what may be shown.
///
/// This is meka's *second* image ceiling, `image::MAX_IMAGE_RAW_BYTES`: the decoded size it will
/// hand a provider, checked separately from [`MAX_VIEW_BASE64_BYTES`] and far tighter. Sized off
/// the base64 one alone, this sat at nearly twice what meka accepts, so a five-megabyte screenshot
/// passed every check here and was then replaced on meka's side by a line of text saying it was
/// suppressed. That is the outcome the screening exists to avoid: the agent is better told the file
/// is too big to show than handed a picture with nothing in it.
const MAX_VIEW_BYTES: u64 = 3_750_000;

/// Run the bridge until a shutdown signal arrives.
pub async fn run(config: Config) -> Result<()> {
    // First, and here rather than in the caller, because this is a property of the daemon and not
    // of how it was started: every route to a running bridge goes through this function, so none
    // of them can forget. Startup before this point keeps the command-line disposition, which is
    // right for it. Nothing is connected yet, and a process still printing its own diagnostics to
    // a pipe somebody closed should stop like any other command would.
    #[cfg(unix)]
    fail_writes_on_broken_pipe();

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

    // Stated every startup rather than only when it is unusual. This is what decides whether a
    // group wakes the agent at all, and the symptom of getting it wrong is a bot that looks broken
    // rather than one that reports an error.
    tracing::info!(
        direct = config.bridge.default_policy.direct.as_str(),
        group = config.bridge.default_policy.group.as_str(),
        channel = config.bridge.default_policy.channel.as_str(),
        "attention defaults for conversations with no policy of their own"
    );

    let channels = Arc::new(ChannelRegistry::build(&config.channels)?);
    let meka = MekaClient::new(&config.meka)?;

    // Bind before anything else starts, so a port conflict is a startup failure rather than a meka
    // that can never take a turn.
    let mcp_server = serve::bind(&config.mcp).await?;
    if let Some(address) = mcp_server.local_addr() {
        tracing::info!("MCP endpoint bound to {}{}", address, config.mcp.path);
    }

    let shutdown = CancellationToken::new();
    // Fires when the last channel stops, as distinct from an operator asking the process to stop.
    // The two are kept apart so the exit status can say which happened.
    let deaf = CancellationToken::new();
    let wake_drain = Arc::new(Notify::new());
    let (event_sender, event_receiver) = mpsc::channel::<InboundEvent>(EVENT_BUFFER);

    // Shared so the typing indicator can stop as soon as a reply actually lands.
    let presence = Arc::new(Presence::default());
    let sink = Arc::new(BridgeSink::new(
        store.clone(),
        Arc::clone(&channels),
        config.storage.clone(),
        config.bridge.default_policy,
        meka.clone(),
        Arc::clone(&presence),
    ));
    let mut tasks = tokio::task::JoinSet::new();

    // Derived from the channels rather than read from `[mcp]`, so the tool list follows the same
    // per-channel setting that decides whether the calls would work at all.
    let surface = ToolSurface::for_channels(channels.iter().map(|channel| channel.capabilities()));
    tasks.spawn({
        let config = Arc::clone(&config);
        let shutdown = shutdown.clone();
        let sink = Arc::clone(&sink) as Arc<dyn OutboundSink>;
        async move {
            if let Err(error) = serve::run(mcp_server, &config.mcp, sink, surface, shutdown).await {
                tracing::error!("MCP server stopped: {}", error);
            }
        }
    });

    // Counts channels still running. A bridge with no live channel cannot do the one thing it is
    // for, and staying up in that state is the worst of both: a supervisor sees a healthy service
    // while every message goes unheard. One dead channel out of several is different, and is left
    // alone.
    let live_channels = Arc::new(AtomicUsize::new(channels.count()));
    for channel in channels.iter() {
        let channel = Arc::clone(channel);
        let sender = event_sender.clone();
        let shutdown = shutdown.clone();
        let live_channels = Arc::clone(&live_channels);
        let deaf = deaf.clone();
        tasks.spawn(async move {
            let id = channel.id().clone();
            match channel.run(sender, shutdown).await {
                Ok(()) => tracing::info!(channel = %id, "channel stopped"),
                // One channel failing must not take the others down: a Telegram outage should not
                // stop a Discord bridge, and the operator needs the process alive to see the log.
                Err(error) => {
                    tracing::error!(channel = %id, "channel stopped with an error: {}", error);
                }
            }
            if live_channels.fetch_sub(1, Ordering::SeqCst) == 1 {
                tracing::error!(
                    "every channel has stopped, so nothing can reach the agent; shutting down so \
                     this is not mistaken for a healthy service"
                );
                deaf.cancel();
            }
        });
    }
    // The writer ends when every channel has dropped its sender, so this handle must not linger.
    drop(event_sender);

    // Written by the writer and read by the drain loop, which is why it is shared rather than owned
    // by either.
    let typing = Arc::new(inbound::TypingState::default());
    tasks.spawn({
        let store = store.clone();
        let config = Arc::clone(&config);
        let wake_drain = Arc::clone(&wake_drain);
        let typing = Arc::clone(&typing);
        async move { inbound::writer(store, config, event_receiver, wake_drain, typing).await }
    });

    tasks.spawn({
        let context = DrainContext {
            store: store.clone(),
            config: Arc::clone(&config),
            meka: meka.clone(),
            channels: Arc::clone(&channels),
            typing,
            runner: TurnRunner::new(
                meka.clone(),
                Arc::clone(&channels),
                config.bridge.typing_indicator,
                config.bridge.typing_refresh,
                config.bridge.typing_max,
                Arc::clone(&presence),
            ),
            identities: Arc::new(tokio::sync::OnceCell::new()),
            permission_checked: Arc::new(tokio::sync::OnceCell::new()),
            notices: inbound::NoticeLog::default(),
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

    let went_deaf = tokio::select! {
        () = shutdown_signal() => false,
        () = deaf.cancelled() => true,
    };
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
    if went_deaf {
        // A non-zero exit, so a supervisor restarts rather than reporting a service that is running
        // and hearing nothing. Some causes clear on a restart and some do not: a refused intent
        // will fail again, which is what `StartLimitBurst` is for.
        return Err(crate::error::BridgeError::command(
            "every channel stopped; the bridge cannot reach anybody",
        ));
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

        // Zero means nothing is being recorded, so there is nothing to sweep. Skipped rather than
        // swept with a zero window, which would delete anything a previous configuration left
        // behind the moment somebody turned history off. Removing it is the operator's call, not a
        // side effect of changing a setting.
        if !config.storage.history_retention.is_zero() {
            match chrono::Duration::from_std(config.storage.history_retention) {
                Ok(retention) => match store.prune_messages(Utc::now() - retention).await {
                    Ok(0) => {}
                    Ok(count) => tracing::debug!(count, "pruned recorded messages"),
                    Err(error) => tracing::error!("history prune failed: {}", error),
                },
                Err(error) => {
                    tracing::error!("[storage].history_retention is out of range: {}", error);
                }
            }
        }
    }
}

/// Turn a closed pipe back into an ordinary `EPIPE`, which is what a network daemon needs.
///
/// A disposition belongs to the process, so the daemon inherits whatever
/// [`crate::cli::exit_quietly_on_broken_pipe`] left behind unless it says otherwise. Exiting
/// quietly is fatal here: the kernel kills the process before the write returns, so no reconnect
/// runs and nothing is logged, and systemd counts the corpse as a clean exit so
/// `Restart=on-failure` does not fire either.
///
/// Lives here, called by [`run`], rather than in whichever caller decided to start a daemon, so a
/// second way in cannot be written without it.
#[cfg(unix)]
fn fail_writes_on_broken_pipe() {
    // SAFETY: a valid signal number with one of the two dispositions libc defines for it. Other
    // threads exist by now, which is fine: `signal` is a thread-safe wrapper over `sigaction` and
    // the disposition it sets is a property of the process. The returned previous handler is not
    // needed.
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_IGN);
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

/// Register files against a message and stamp each with the handle the agent fetches by.
///
/// Shared by both directions. A file the agent sent is on the platform exactly as one it received
/// is, and the send response carries the reference needed to pull it back, so a session that did
/// not send it can still open it. Without this the bridge's own attachments would be the only ones
/// in the history that could not be viewed.
///
/// The handle id is `<conversation>:<external_id>:<index>`, stable across a redelivery so a replay
/// reuses the handle already issued rather than minting a second one for the same file. Outbound
/// messages use their platform message id as the external id, which cannot collide with an inbound
/// one; see the note in [`record_own_messages`] for why not, which differs per platform.
async fn register_files(
    store: &Store,
    conversation: &ConversationId,
    channel: &crate::channel::ChannelId,
    external_id: &str,
    timestamp: chrono::DateTime<Utc>,
    attachments: &mut [crate::channel::Attachment],
) -> std::result::Result<(), crate::store::StoreError> {
    for (index, attachment) in attachments.iter_mut().enumerate() {
        let handle = store
            .register_attachment(crate::store::AttachmentRecord {
                id: format!("{conversation}:{external_id}:{index}"),
                conversation_id: conversation.as_str().to_string(),
                channel_id: channel.as_str().to_string(),
                kind: attachment.kind.as_str().to_string(),
                file_ref: attachment.file_ref.clone(),
                thumb_ref: attachment.thumb_ref.clone(),
                file_name: attachment.file_name.clone(),
                media_type: attachment.media_type.clone(),
                bytes: attachment.bytes,
                path: None,
                created_at: timestamp,
            })
            .await?;
        attachment.handle = Some(handle);
    }
    Ok(())
}

/// Implements the MCP server's outbound port over the channel registry.
///
/// Sends are not restricted to conversations the bridge has seen. The agent may write to any id its
/// channel accepts, including one it was given in its system prompt rather than in an envelope,
/// which is what lets it message somebody first. A hallucinated id therefore fails at the platform
/// rather than here, and the platform's own wording is the more useful error anyway.
pub struct BridgeSink {
    store: Store,
    channels: Arc<ChannelRegistry>,
    storage: StorageConfig,
    /// What a conversation with no explicit decision follows, so a listing can report the policy
    /// actually in force rather than only the ones somebody wrote down.
    default_policy: DefaultPolicy,
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
        default_policy: DefaultPolicy,
        meka: MekaClient,
        presence: Arc<Presence>,
    ) -> Self {
        Self {
            store,
            channels,
            storage,
            default_policy,
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

    /// Check that an id is well formed and names a configured channel.
    ///
    /// Deliberately does not require the conversation to be one the bridge has seen. Whether a chat
    /// can be written to is the platform's judgement, not ours, and it is the only party that
    /// knows: Telegram refuses a user who never started the bot but accepts a group the bot
    /// sits in silently. Asking it and passing the answer back beats a guess made from the
    /// address book.
    fn resolve(&self, id: &str) -> std::result::Result<ConversationId, SinkError> {
        let conversation = ConversationId::parse(id)
            .ok_or_else(|| SinkError::MalformedConversation(id.to_string()))?;
        if self.channels.get(conversation.channel()).is_none() {
            return Err(SinkError::UnknownChannel {
                conversation: id.to_string(),
                channel: conversation.channel().to_string(),
            });
        }
        Ok(conversation)
    }

    /// Resolve a conversation for a moderation call, refusing a channel that has no such model.
    ///
    /// Whether the bot actually holds the right in that particular chat is left to the platform:
    /// only it knows, the answer changes without warning, and its refusal explains more than
    /// anything the bridge could infer.
    fn admin_target(
        &self,
        conversation: &str,
    ) -> std::result::Result<(ConversationId, Arc<dyn Channel>), SinkError> {
        let conversation = self.resolve(conversation)?;
        let channel = self
            .channels
            .resolve(&conversation)
            .map_err(|error| SinkError::Internal(error.to_string()))?
            .clone();
        if !channel.capabilities().admin {
            return Err(SinkError::Delivery(format!(
                "channel {} has no moderation tools",
                conversation.channel()
            )));
        }
        Ok((conversation, channel))
    }

    /// The id a conversation should be recorded under, which is not always the one the agent used.
    ///
    /// A dialling address names a person rather than a chat, so recording what was sent under it
    /// would leave the reply arriving in a different conversation and a policy set on one not
    /// applying to the other. A connector that cannot resolve it says so and the address is used as
    /// given, which is no worse than not asking.
    async fn canonical(&self, conversation: &ConversationId) -> ConversationId {
        let Ok(channel) = self.channels.resolve(conversation) else {
            return conversation.clone();
        };
        match channel.canonical_conversation(conversation).await {
            Ok(resolved) => resolved,
            Err(error) => {
                tracing::debug!(
                    conversation = %conversation,
                    "could not resolve the conversation to its final id: {}",
                    error
                );
                conversation.clone()
            }
        }
    }

    /// Everything that happens after a send lands, over the conversation it landed in.
    ///
    /// Resolves the canonical id once and hands it to all three, because a dialling address names a
    /// person rather than a chat and the reply will arrive under the resolved id. Recording the
    /// history under the address instead would put the bot's own message in a different
    /// conversation from the answer to it.
    async fn note_sent(
        &self,
        conversation: &ConversationId,
        channel: &Arc<dyn Channel>,
        session: Option<&str>,
        sent: Vec<crate::channel::SentMessage>,
    ) {
        let conversation = self.canonical(conversation).await;
        // Stops the typing indicator re-arming here. Telegram already cleared it when this message
        // landed, so setting it again would announce a follow-up that is not coming.
        self.presence.note_sent(&conversation);
        self.touch_outbound(&conversation, channel.platform()).await;
        self.record_own(&conversation, channel, session, sent, None)
            .await;
    }

    /// This sink's half of [`record_own_messages`], which carries the reasoning.
    async fn record_own(
        &self,
        conversation: &ConversationId,
        channel: &Arc<dyn Channel>,
        session: Option<&str>,
        sent: Vec<crate::channel::SentMessage>,
        revised_at: Option<chrono::DateTime<Utc>>,
    ) {
        record_own_messages(
            &self.store,
            self.storage.history_retention,
            conversation,
            channel.id(),
            session,
            sent,
            revised_at,
        )
        .await;
    }

    /// Stamp the conversation as one the agent has written in, minting it if it is new.
    async fn touch_outbound(
        &self,
        conversation: &ConversationId,
        platform: crate::channel::Platform,
    ) {
        let now = Utc::now();
        if let Err(error) = self
            .store
            .touch_outbound(crate::store::ConversationRecord {
                id: conversation.as_str().to_string(),
                channel_id: conversation.channel().to_string(),
                platform: platform.as_str().to_string(),
                chat: conversation.chat().to_string(),
                thread: conversation.thread().map(str::to_string),
                // Both unknown when the agent messages first, and both left alone on a conversation
                // that already exists.
                title: None,
                kind: crate::channel::ChatKind::Unknown.as_str().to_string(),
                created_at: now,
                last_inbound_at: None,
                last_outbound_at: Some(now),
            })
            .await
        {
            tracing::warn!("failed to record an outbound message: {}", error);
        }
    }
}

/// Write the bridge's own messages into the same history everybody else's is in.
///
/// One row per real platform message, not per tool call: text too long for one message becomes
/// several with several ids, and a single row could carry only one of them, so an id read back from
/// history would edit or react to the first part alone.
///
/// A free function rather than a method on [`BridgeSink`], because the drain loop's own failure
/// notice is outbound too and reaches its channel directly rather than through a sink.
///
/// Errors are logged rather than propagated: the send has already happened, and failing the tool
/// call would tell the agent its message did not go out when it did.
///
/// `seen` is true on every row, or the bridge's own output would count as a backlog and be offered
/// back to the agent as context it missed. `revised_at` gives a revision a deduplication key of its
/// own, since it keeps the message id it revises and the unique constraint would otherwise swallow
/// it.
async fn record_own_messages(
    store: &Store,
    history_retention: Duration,
    conversation: &ConversationId,
    channel: &crate::channel::ChannelId,
    session: Option<&str>,
    mut sent: Vec<crate::channel::SentMessage>,
    revised_at: Option<chrono::DateTime<Utc>>,
) {
    if history_retention.is_zero() {
        return;
    }
    for message in &mut sent {
        if let Err(error) = register_files(
            store,
            conversation,
            channel,
            &message.message_id,
            message.timestamp,
            &mut message.attachments,
        )
        .await
        {
            tracing::warn!(
                conversation = %conversation,
                "could not register the files of a message the agent sent: {}",
                error
            );
        }
        // The platform's message id serves as the deduplication key too, and cannot collide with an
        // inbound one, though for a different reason on each platform: Telegram numbers a chat's
        // messages in one sequence covering both directions, and Discord's snowflakes are unique
        // everywhere. Neither ever hands the bridge its own message as an inbound event in any
        // case.
        //
        // A revision carries the id it revises, so it takes the inbound path's `<id>:e<time>` shape
        // at millisecond rather than second resolution. Two edits sharing a key would leave the
        // second unrecorded, which two API round trips inside one millisecond cannot reach, and
        // superseding orders against the revision's own row so even a shared key marks correctly.
        let external_id = match revised_at {
            Some(revised_at) => {
                format!("{}:e{}", message.message_id, revised_at.timestamp_millis())
            }
            None => message.message_id.clone(),
        };
        let record = crate::store::MessageRecord {
            id: 0,
            conversation_id: conversation.as_str().to_string(),
            external_id: external_id.clone(),
            message_id: message.message_id.clone(),
            sender_id: (!message.sender.id.is_empty()).then(|| message.sender.id.clone()),
            sender_name: message.sender.display_name.clone(),
            text: message.text.clone(),
            notes: (!message.notes.is_empty()).then(|| message.notes.join("; ")),
            attachments: message
                .attachments
                .iter()
                .filter_map(|attachment| attachment.handle.clone())
                .collect(),
            addressed: false,
            seen: true,
            own: true,
            session_id: session.map(str::to_string),
            deleted_at: None,
            superseded_at: None,
            timestamp: message.timestamp,
        };
        if let Err(error) = store.record_message(record).await {
            tracing::warn!(
                conversation = %conversation,
                message_id = %message.message_id,
                "could not record a message the agent sent: {}",
                error
            );
            // Superseding is skipped when the replacement did not land, so a failure here leaves
            // the old wording readable rather than marking it stale with nothing to read in its
            // place.
            continue;
        }
        if let Some(revised_at) = revised_at
            && let Err(error) = store
                .supersede_message(
                    conversation.as_str(),
                    &message.message_id,
                    &external_id,
                    revised_at,
                )
                .await
        {
            tracing::warn!(
                conversation = %conversation,
                message_id = %message.message_id,
                "could not mark the wording an edit replaced: {}",
                error
            );
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
        session: Option<&str>,
    ) -> std::result::Result<Vec<String>, SinkError> {
        let conversation = self.resolve(conversation)?;
        let channel = self
            .channels
            .resolve(&conversation)
            .map_err(|error| SinkError::Internal(error.to_string()))?;
        // The result is kept rather than propagated, because `sent` is meaningful either way:
        // splitting means part two can be refused with part one already in the chat, and recording
        // only on success would leave the chat holding words the history has no record of. That is
        // the failure own-message recording exists to prevent, so the record comes first and the
        // error after it.
        let mut sent = Vec::new();
        let outcome = channel
            .send_text(&conversation, markdown, &options, &mut sent)
            .await;
        let ids: Vec<String> = sent
            .iter()
            .map(|message| message.message_id.clone())
            .collect();
        tracing::info!(
            conversation = %conversation,
            parts = sent.len(),
            failed = outcome.is_err(),
            "the agent sent a message"
        );
        // Skipped only when nothing landed and the send failed, so a chat that received nothing is
        // not minted into the address book with a time the agent last spoke there.
        if outcome.is_ok() || !sent.is_empty() {
            self.note_sent(&conversation, channel, session, sent).await;
        }
        outcome.map_err(|error| SinkError::Delivery(error.to_string()))?;
        Ok(ids)
    }

    async fn send_file(
        &self,
        conversation: &str,
        paths: &[std::path::PathBuf],
        caption: Option<&str>,
        options: FileOptions,
        session: Option<&str>,
    ) -> std::result::Result<Vec<String>, SinkError> {
        let conversation = self.resolve(conversation)?;
        let channel = self
            .channels
            .resolve(&conversation)
            .map_err(|error| SinkError::Internal(error.to_string()))?;
        let capabilities = channel.capabilities();
        // The trait promises channels a non-empty list, so it is enforced once here rather than in
        // each connector. Without it a channel that does not index its input, as Discord's does
        // not, quietly sends a caption-only message and reports a file delivered.
        if paths.is_empty() {
            return Err(SinkError::Delivery(
                "no files were given to send".to_string(),
            ));
        }
        if options.as_photo && !capabilities.photos {
            return Err(SinkError::Delivery(format!(
                "channel {} cannot send photos",
                conversation.channel()
            )));
        }
        if !options.as_photo && !capabilities.files {
            return Err(SinkError::Delivery(format!(
                "channel {} cannot send files",
                conversation.channel()
            )));
        }
        // Recorded before the error is raised, for the reason `send_text` above gives. Neither
        // platform here can half send a group, but the contract allows one that loops and this
        // side of it costs nothing to honour.
        let mut sent = Vec::new();
        let outcome = channel
            .send_files(&conversation, paths, caption, &options, &mut sent)
            .await;
        let ids: Vec<String> = sent
            .iter()
            .map(|message| message.message_id.clone())
            .collect();
        tracing::info!(
            conversation = %conversation,
            files = paths.len(),
            failed = outcome.is_err(),
            "the agent sent a file"
        );
        if outcome.is_ok() || !sent.is_empty() {
            self.note_sent(&conversation, channel, session, sent).await;
        }
        outcome.map_err(|error| SinkError::Delivery(error.to_string()))?;
        Ok(ids)
    }

    async fn react(
        &self,
        conversation: &str,
        message_id: &str,
        emoji: Option<&str>,
    ) -> std::result::Result<(), SinkError> {
        let conversation = self.resolve(conversation)?;
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

    async fn edit_message(
        &self,
        conversation: &str,
        message_id: &str,
        markdown: &str,
        link_preview: bool,
        session: Option<&str>,
    ) -> std::result::Result<(), SinkError> {
        let conversation = self.resolve(conversation)?;
        let channel = self
            .channels
            .resolve(&conversation)
            .map_err(|error| SinkError::Internal(error.to_string()))?;
        if !channel.capabilities().edit {
            return Err(SinkError::Delivery(format!(
                "channel {} cannot edit messages",
                conversation.channel()
            )));
        }
        let revised = channel
            .edit_text(&conversation, message_id, markdown, link_preview)
            .await
            .map_err(|error| SinkError::Delivery(error.to_string()))?;
        tracing::info!(
            conversation = %conversation,
            message_id = %message_id,
            "the agent edited a message"
        );
        // Deliberately not `note_sent`: revising a message is not new activity in the conversation,
        // and treating it as such would make a chat the agent only corrected itself in look freshly
        // answered. The history still has to learn about it, so the recording half runs on its own.
        //
        // The canonical id is resolved here rather than reused, because the row being revised was
        // recorded under it and an edit addressed to a dialling address would otherwise look for it
        // somewhere else.
        if let Some(revised) = revised {
            let conversation = self.canonical(&conversation).await;
            self.record_own(
                &conversation,
                channel,
                session,
                vec![revised],
                Some(Utc::now()),
            )
            .await;
        }
        Ok(())
    }

    async fn delete_message(
        &self,
        conversation: &str,
        message_id: &str,
    ) -> std::result::Result<(), SinkError> {
        let conversation = self.resolve(conversation)?;
        let channel = self
            .channels
            .resolve(&conversation)
            .map_err(|error| SinkError::Internal(error.to_string()))?;
        channel
            .delete_message(&conversation, message_id)
            .await
            .map_err(|error| SinkError::Delivery(error.to_string()))?;
        // Warn, not info: a deletion leaves no trace on the platform, so the chat itself no longer
        // shows it happened.
        tracing::warn!(
            conversation = %conversation,
            message_id = %message_id,
            "the agent deleted a message"
        );
        // Marked in the history the same way a deletion reported by the platform is, so a message
        // the agent removed reads back as removed rather than as one that was never there. Failing
        // here would report a deletion that did happen as an error, so it is logged instead.
        let conversation = self.canonical(&conversation).await;
        if let Err(error) = self
            .store
            .mark_deleted(conversation.as_str(), message_id, Utc::now())
            .await
        {
            tracing::warn!(
                conversation = %conversation,
                message_id = %message_id,
                "could not mark a deleted message in the history: {}",
                error
            );
        }
        Ok(())
    }

    async fn moderate_member(
        &self,
        conversation: &str,
        user_id: &str,
        action: crate::channel::MemberAction,
        until: Option<chrono::DateTime<Utc>>,
        revoke_messages: bool,
    ) -> std::result::Result<(), SinkError> {
        let (conversation, channel) = self.admin_target(conversation)?;
        channel
            .moderate_member(&conversation, user_id, action, until, revoke_messages)
            .await
            .map_err(|error| SinkError::Delivery(error.to_string()))?;
        // Warn for every one of these. They change somebody's standing in a chat, an operator has
        // no other record that it happened, and the agent can be talked into them by anyone whose
        // message it reads.
        tracing::warn!(
            conversation = %conversation,
            user_id = %user_id,
            action = action.as_str(),
            until = ?until,
            revoke_messages,
            "the agent moderated a member"
        );
        Ok(())
    }

    async fn set_member_rights(
        &self,
        conversation: &str,
        user_id: &str,
        rights: &[crate::channel::MemberRight],
    ) -> std::result::Result<(), SinkError> {
        let (conversation, channel) = self.admin_target(conversation)?;
        channel
            .set_member_rights(&conversation, user_id, rights)
            .await
            .map_err(|error| SinkError::Delivery(error.to_string()))?;
        tracing::warn!(
            conversation = %conversation,
            user_id = %user_id,
            rights = ?rights.iter().map(|right| right.as_str()).collect::<Vec<_>>(),
            "the agent changed a member's rights"
        );
        Ok(())
    }

    async fn set_member_roles(
        &self,
        conversation: &str,
        user_id: &str,
        roles: &[String],
    ) -> std::result::Result<(), SinkError> {
        let (conversation, channel) = self.admin_target(conversation)?;
        channel
            .set_member_roles(&conversation, user_id, roles)
            .await
            .map_err(|error| SinkError::Delivery(error.to_string()))?;
        tracing::warn!(
            conversation = %conversation,
            user_id = %user_id,
            roles = ?roles,
            "the agent changed a member's roles"
        );
        Ok(())
    }

    async fn pin_message(
        &self,
        conversation: &str,
        message_id: &str,
        pin: bool,
        silent: bool,
    ) -> std::result::Result<(), SinkError> {
        let (conversation, channel) = self.admin_target(conversation)?;
        channel
            .pin_message(&conversation, message_id, pin, silent)
            .await
            .map_err(|error| SinkError::Delivery(error.to_string()))?;
        tracing::info!(
            conversation = %conversation,
            message_id = %message_id,
            pin,
            "the agent changed a pinned message"
        );
        Ok(())
    }

    async fn set_chat(
        &self,
        conversation: &str,
        settings: crate::channel::ChatSettings,
    ) -> std::result::Result<(), SinkError> {
        let (conversation, channel) = self.admin_target(conversation)?;
        channel
            .set_chat(&conversation, &settings)
            .await
            .map_err(|error| SinkError::Delivery(error.to_string()))?;
        tracing::warn!(
            conversation = %conversation,
            title = ?settings.title,
            "the agent changed chat settings"
        );
        Ok(())
    }

    async fn member(
        &self,
        conversation: &str,
        user_id: Option<&str>,
    ) -> std::result::Result<crate::channel::MemberInfo, SinkError> {
        let (conversation, channel) = self.admin_target(conversation)?;
        channel
            .member(&conversation, user_id)
            .await
            .map_err(|error| SinkError::Delivery(error.to_string()))
    }

    async fn list_members(
        &self,
        conversation: &str,
        query: Option<&str>,
        limit: usize,
        after: Option<&str>,
    ) -> std::result::Result<crate::channel::MemberListing, SinkError> {
        let (conversation, channel) = self.admin_target(conversation)?;
        channel
            .list_members(&conversation, query, limit, after)
            .await
            .map_err(|error| SinkError::Delivery(error.to_string()))
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

        // Answered before fetching where the size is already known. A connector refusing an
        // oversize file returns its own wording, which names a byte ceiling no operator
        // configured and says nothing about `download_attachment`; the branch below that
        // *does* say it only fires on the base64 form, which is the looser of meka's two
        // limits and so never first.
        //
        // Only when the file itself is what will be fetched. `record.bytes` sizes the main file
        // while a preview fetches the thumbnail, so checking one against the other rejected every
        // oversized video on the strength of a number describing something else.
        if !preview && record.bytes.is_some_and(|bytes| bytes > MAX_VIEW_BYTES) {
            return Ok(ViewedAttachment::Description(format!(
                "This {} is {}, too large to show inline. Use download_attachment to get the file \
                 itself.",
                record.kind,
                describe_file(&record)
            )));
        }
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

    async fn set_policy(
        &self,
        conversation: &str,
        policy: Policy,
        until: Option<chrono::DateTime<Utc>>,
        reason: Option<&str>,
    ) -> std::result::Result<Option<Policy>, SinkError> {
        // Parsed but not required to be in the address book: ruling on a conversation before it has
        // said anything is a legitimate pre-emptive move, and the row is keyed by id either way.
        let conversation = self.resolve(conversation)?;
        let existing = self
            .store
            .policy(conversation.as_str())
            .await
            .map_err(|error| SinkError::Internal(error.to_string()))?;

        // Refused rather than accepted and quietly done nothing. In a one-to-one chat every message
        // is addressed to the agent, so mention-only would deliver exactly what it delivers now,
        // and the agent would believe it had turned something down.
        if policy == Policy::Mute {
            let kind = self
                .store
                .conversation(conversation.as_str())
                .await
                .map_err(|error| SinkError::Internal(error.to_string()))?
                .map(|record| record.kind);
            if kind.as_deref() == Some(ChatKind::Direct.as_str()) {
                return Err(SinkError::Delivery(format!(
                    "{conversation} is a one-to-one chat, where every message is addressed to you, \
                     so muting it would change nothing. Use block if you want it to stop reaching \
                     you."
                )));
            }
        }

        self.store
            .set_policy(conversation.as_str(), policy, until, reason, Utc::now())
            .await
            .map_err(|error| SinkError::Internal(error.to_string()))?;

        // Warn rather than info for the two that withhold something: this is the line an operator
        // greps for when the bot has gone quiet and nothing else explains it.
        match policy {
            Policy::Active => {
                tracing::info!(
                    conversation = %conversation,
                    "the agent set a conversation back to waking it for everything"
                );
            }
            Policy::Mute | Policy::Block => tracing::warn!(
                conversation = %conversation,
                policy = policy.as_str(),
                until = ?until,
                reason = ?reason,
                "the agent turned a conversation down; `mekabridge policy clear` undoes it"
            ),
        }
        Ok(existing.map(|record| record.policy))
    }

    async fn unseen(
        &self,
        conversation: Option<&str>,
    ) -> std::result::Result<UnseenSummary, SinkError> {
        // Parsed rather than trusted, so an id naming no configured channel is refused instead of
        // quietly matching nothing, which from the caller's side is indistinguishable from a room
        // that has gone quiet. `resolve` does not rewrite the id, so the string reaching the store
        // is the one the caller passed.
        let conversation = conversation.map(|id| self.resolve(id)).transpose()?;
        self.store
            .unseen_summary(conversation.as_ref().map(ConversationId::as_str))
            .await
            .map_err(|error| SinkError::Internal(error.to_string()))
    }

    async fn read_history(
        &self,
        conversation: &str,
        limit: usize,
        before: Option<i64>,
    ) -> std::result::Result<Vec<HistoryEntry>, SinkError> {
        let conversation = self.resolve(conversation)?;
        let records = self
            .store
            .history(conversation.as_str(), limit, before)
            .await
            .map_err(|error| SinkError::Internal(error.to_string()))?;
        Ok(records.into_iter().map(history_entry).collect())
    }

    async fn search_history(
        &self,
        query: &str,
        conversation: Option<&str>,
        limit: usize,
    ) -> std::result::Result<Vec<HistoryEntry>, SinkError> {
        let conversation = conversation.map(|id| self.resolve(id)).transpose()?;
        let records = self
            .store
            .search_messages(
                query,
                conversation.as_ref().map(ConversationId::as_str),
                limit,
            )
            .await
            // FTS5 rejects a malformed query rather than matching nothing, and its complaint names
            // the offending token, so it reaches the agent instead of becoming "no results". Only
            // that case gets the advice: attaching "try plain words" to a disk error would send the
            // agent rewriting a query that was never the problem.
            .map_err(|error| {
                let message = error.to_string();
                if message.contains("fts5") {
                    SinkError::Delivery(format!(
                        "{message}. Search for plain words, or use `a OR b`, `a NOT b`, or a \
                         \"quoted phrase\"."
                    ))
                } else {
                    SinkError::Internal(message)
                }
            })?;
        let mut entries: Vec<HistoryEntry> = records.into_iter().map(history_entry).collect();

        // Ask the platform too, when it keeps a record of its own and the search is aimed at one
        // chat. Discord's reaches back before the bot ever joined, which nothing the bridge holds
        // can, and its own index is better at matching than the bridge's. Failure here is not the
        // agent's problem: the local results still stand, so a platform that will not answer, is
        // still indexing, or has no search at all is logged and left.
        if let Some(conversation) = &conversation
            && entries.len() < limit
            && let Ok(channel) = self.channels.resolve(conversation)
        {
            match channel
                .search_messages(conversation, query, limit - entries.len())
                .await
            {
                Ok(found) => {
                    let seen: HashSet<String> = entries
                        .iter()
                        .map(|entry| entry.message_id.clone())
                        .collect();
                    for message in found {
                        if seen.contains(&message.message_id) {
                            continue;
                        }
                        entries.push(HistoryEntry {
                            conversation: conversation.as_str().to_string(),
                            message_id: message.message_id,
                            sender: message.sender_name,
                            sender_id: None,
                            text: message.text,
                            notes: None,
                            attachments: Vec::new(),
                            addressed: false,
                            // The connector settles this one from its own account id. The rest are
                            // unknowable from a platform search, which returns a message id, an
                            // author and a time and nothing the bridge recorded about it, so they
                            // are left at their defaults rather than guessed, the same way
                            // `attachments` and `addressed` above are.
                            own: message.own,
                            session: None,
                            deleted: false,
                            superseded: false,
                            timestamp: message.timestamp.to_rfc3339(),
                            // Not a row in the bridge's history, so there is nothing to page back
                            // from. Zero is the value `read_history` already treats as no cursor.
                            cursor: 0,
                        });
                    }
                }
                Err(error) => tracing::debug!(
                    conversation = %conversation,
                    "the platform's own search did not answer: {}",
                    error
                ),
            }
        }
        Ok(entries)
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
        let policies = self
            .store
            .list_policies()
            .await
            .map_err(|error| SinkError::Internal(error.to_string()))?;
        let unseen = self
            .store
            .unseen_counts()
            .await
            .map_err(|error| SinkError::Internal(error.to_string()))?;
        Ok(records
            .into_iter()
            .map(|record| {
                let policy = policies
                    .iter()
                    .find(|policy| policy.conversation_id == record.id)
                    .cloned();
                let unseen = unseen.get(&record.id).copied().unwrap_or_default();
                self.summarize(record, policy, unseen)
            })
            .collect())
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
        let Some(record) = record else {
            return Ok(None);
        };
        let policy = self
            .store
            .policy(&record.id)
            .await
            .map_err(|error| SinkError::Internal(error.to_string()))?;
        let unseen = self
            .store
            .unseen_counts()
            .await
            .map_err(|error| SinkError::Internal(error.to_string()))?
            .get(&record.id)
            .copied()
            .unwrap_or_default();
        Ok(Some(self.summarize(record, policy, unseen)))
    }
}

/// Translate a stored message into what the history tools hand back.
fn history_entry(record: crate::store::MessageRecord) -> HistoryEntry {
    HistoryEntry {
        conversation: record.conversation_id,
        message_id: record.message_id,
        sender: record.sender_name,
        sender_id: record.sender_id,
        text: record.text,
        notes: record.notes,
        attachments: record.attachments,
        addressed: record.addressed,
        own: record.own,
        session: record.session_id,
        deleted: record.deleted_at.is_some(),
        superseded: record.superseded_at.is_some(),
        timestamp: record.timestamp.to_rfc3339(),
        cursor: record.id,
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
    // Truncation alone is not enough to keep two refs apart. A Discord ref is three snowflakes and
    // runs past 48 characters, and the part that differs between two attachments of the *same*
    // message is the attachment id at the end, which is exactly what the cut removes. Two images on
    // one post therefore produced the same stem and, with the same extension, the same path: the
    // second overwrote the first, `download_attachment` handed back the wrong bytes with no error,
    // and sweeping either deleted both. The suffix is derived from the whole ref, so it survives
    // whatever the prefix loses.
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in file_ref.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x1000_0000_01b3);
    }
    if cleaned.is_empty() {
        format!("attachment-{hash:016x}")
    } else {
        format!("{cleaned}-{hash:016x}")
    }
}

impl BridgeSink {
    /// Render one conversation, resolving what its policy actually is.
    ///
    /// A lapsed record is treated as gone here rather than reported: it stays in the table until
    /// the next message from that chat clears it, so listing has to ignore one that is only
    /// still there because nothing has arrived since. What is left is the configured default
    /// for the chat's kind, which is also the answer for a conversation nobody has ruled on.
    fn summarize(
        &self,
        record: crate::store::ConversationRecord,
        policy: Option<crate::store::PolicyRecord>,
        unseen: u64,
    ) -> ConversationSummary {
        let live = policy.filter(|policy| !policy.expired(Utc::now()));
        let kind = match record.kind.as_str() {
            "direct" => ChatKind::Direct,
            "group" => ChatKind::Group,
            "channel" => ChatKind::Channel,
            _ => ChatKind::Unknown,
        };
        let effective = live.as_ref().map_or_else(
            || self.default_policy.for_kind(kind),
            |policy| policy.policy,
        );
        ConversationSummary {
            id: record.id,
            channel: record.channel_id,
            platform: record.platform,
            title: record.title,
            kind: record.kind,
            last_inbound_at: record.last_inbound_at.map(|at| at.to_rfc3339()),
            last_outbound_at: record.last_outbound_at.map(|at| at.to_rfc3339()),
            policy: effective.as_str().to_string(),
            policy_until: live.map(|policy| match policy.until {
                Some(until) => until.to_rfc3339(),
                None => "indefinite".to_string(),
            }),
            unseen,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{EVENT_BUFFER, MAX_VIEW_BASE64_BYTES, MAX_VIEW_BYTES, sanitize_file_stem};

    /// Read the disposition without disturbing it, so no window exists where a concurrent test
    /// writing to a closed pipe would take the whole process down.
    #[cfg(unix)]
    fn sigpipe_disposition() -> libc::sighandler_t {
        // SAFETY: a null `act` asks `sigaction` to report the current action and change nothing.
        // The zeroed `sigaction` is only an out-parameter.
        unsafe {
            let mut current: libc::sigaction = std::mem::zeroed();
            libc::sigaction(libc::SIGPIPE, std::ptr::null(), &mut current);
            current.sa_sigaction
        }
    }

    /// The only test that touches `SIGPIPE`, deliberately: the disposition is process-wide, so a
    /// second one would leave the suite briefly killable by a broken pipe in whichever test was
    /// running alongside it.
    ///
    /// Drives [`run`] rather than calling [`fail_writes_on_broken_pipe`] directly, because the call
    /// inside `run` is the part that was missing on 2026-08-30 and the part a refactor can drop.
    /// `run` sets the disposition before its first fallible step, so a store that cannot be opened
    /// is enough to reach it and return, with nothing bound and no network touched.
    #[tokio::test]
    #[cfg(unix)]
    async fn starting_the_daemon_survives_a_closed_peer() {
        let directory = tempfile::tempdir().expect("temp dir");
        let config_path = directory.path().join("config.toml");
        std::fs::write(
            &config_path,
            format!(
                "[meka]\ntoken = \"meka-token\"\n\n[storage]\npath = {:?}\n\n\
                 [[channels.telegram]]\nid = \"telegram\"\ntoken = \"bot-token\"\n\
                 allowed_users = [123]\n",
                // A directory, so SQLite refuses to open it as a database.
                directory.path()
            ),
        )
        .expect("write config");
        let config = crate::config::Config::load(Some(&config_path)).expect("config parses");

        // Stated rather than assumed. If SQLite ever opened a directory, `run` would carry on past
        // it to bind a port and start polling Telegram for real, and this would fail as a hang
        // rather than as an assertion.
        assert!(
            crate::store::Store::open(directory.path()).await.is_err(),
            "the test depends on `run` stopping at the store"
        );

        crate::cli::exit_quietly_on_broken_pipe();
        assert_eq!(
            sigpipe_disposition(),
            libc::SIG_DFL,
            "piping `mekabridge history` into `head` should end quietly, not panic"
        );

        assert!(
            super::run(config).await.is_err(),
            "the store was supposed to be unopenable"
        );
        assert_eq!(
            sigpipe_disposition(),
            libc::SIG_IGN,
            "a daemon holding a gateway socket open for days must survive a closed peer"
        );
        // Left ignored, which is where Rust's own startup puts it and what the rest of the suite
        // was running under before this test.
    }

    #[test]
    fn two_attachments_on_one_message_get_different_paths() {
        // A Discord file ref is three snowflakes and runs well past the 48 characters the stem
        // keeps, and the part that differs between two attachments of the *same* message is the id
        // at the end -- exactly what the truncation removes. Two screenshots on one post therefore
        // produced the same stem, and with the same extension the same path: the second overwrote
        // the first, `download_attachment` returned the wrong bytes with no error at all, and
        // sweeping either deleted both.
        let first =
            sanitize_file_stem("1183429847290374144/1183429847290374145/1183429847290374146");
        let second =
            sanitize_file_stem("1183429847290374144/1183429847290374145/1183429847290374147");
        assert_ne!(
            first, second,
            "two attachments on one message collided on the same file name"
        );
    }

    #[test]
    #[expect(
        clippy::assertions_on_constants,
        reason = "pinning a shipped constant against an external contract is the point"
    )]
    fn the_view_ceiling_matches_what_meka_will_actually_accept() {
        // meka applies two image ceilings, and the tighter is the decoded one it hands a provider,
        // `image::MAX_IMAGE_RAW_BYTES`. Sized off the base64 one alone this sat at nearly twice
        // what meka accepts, so an image passed every check here and was replaced on meka's side by
        // a line of text saying it was suppressed -- which is the outcome the screening exists to
        // avoid. meka's check is `>`, so equality passes.
        const MEKA_MAX_IMAGE_RAW_BYTES: u64 = 3_750_000;
        assert!(
            MAX_VIEW_BYTES <= MEKA_MAX_IMAGE_RAW_BYTES,
            "fetching up to {MAX_VIEW_BYTES} bytes hands meka more than it will show"
        );
        // And the base64 form of a file at the ceiling still fits the looser limit, which is what
        // makes the check on the encoded size a backstop rather than the deciding one.
        assert!(
            MAX_VIEW_BYTES.div_ceil(3) * 4 <= MAX_VIEW_BASE64_BYTES as u64,
            "the raw ceiling encodes past the base64 one"
        );
    }

    #[test]
    #[expect(
        clippy::assertions_on_constants,
        reason = "pinning a shipped constant against an external contract is the point"
    )]
    fn the_inbound_buffer_stays_small_enough_to_be_a_durability_bound() {
        // This number is how many messages a hard kill can lose, because the poller blocking on a
        // full channel is what holds back the offset confirmation. It was 64, picked as a
        // throughput buffer before anyone worked out what it bounded.
        assert!(
            EVENT_BUFFER <= 8,
            "a buffer of {EVENT_BUFFER} is that many messages lost on a `kill -9`"
        );
        // Not so small that Discord's typing `try_send` is starved by an ordinary burst.
        assert!(EVENT_BUFFER >= 4);
    }

    #[test]
    fn a_stem_stays_inside_the_attachment_directory() {
        // The reason the filter exists: Telegram file ids really do contain `/` and `-`.
        for hostile in ["../../etc/passwd", "..", "a/b\\c", "\u{0}"] {
            let stem = sanitize_file_stem(hostile);
            assert!(
                !stem.contains(['/', '\\', '.']),
                "{hostile:?} produced a stem with a path separator in it: {stem:?}"
            );
        }
    }
}
