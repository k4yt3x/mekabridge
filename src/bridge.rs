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
use chrono::Utc;
use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;

use crate::{
    bridge::{inbound::DrainContext, turn::TurnRunner},
    channel::{ChannelRegistry, ConversationId, InboundEvent, SendOptions},
    config::Config,
    error::Result,
    mcp::{ConversationSummary, OutboundSink, SinkError, serve},
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

    let channels = Arc::new(ChannelRegistry::build(&config.channels, &config.storage)?);
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

    let sink = Arc::new(BridgeSink::new(store.clone(), Arc::clone(&channels)));
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
            ),
            vision: Arc::new(tokio::sync::OnceCell::new()),
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
}

impl BridgeSink {
    /// Build the sink over a store and a channel registry.
    pub const fn new(store: Store, channels: Arc<ChannelRegistry>) -> Self {
        Self { store, channels }
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
