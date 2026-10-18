//! Startup wiring, supervision, and graceful shutdown.
//!
//! The MCP listener is bound first, before any channel connects or the queue starts moving, so a
//! port conflict is a startup failure rather than a bridge that runs with no way for the agent to
//! answer anybody. meka retries a failed MCP connect in the background, so the bridge no longer has
//! to be up before meka; being up first still avoids the window in which meka, unless its entry for
//! this bridge is marked `required`, runs turns with no tool to answer anybody.

pub mod envelope;
pub mod feed;
pub mod inbound;
pub mod turn;

use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use base64::Engine as _;
use chrono::Utc;
use tokio::sync::{Notify, mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::{
    bridge::{
        inbound::SessionContext,
        turn::{Presence, TurnTypist},
    },
    channel::{
        Channel, ChannelError, ChannelRegistry, ChatKind, ConversationId, FileOptions,
        FoundMessage, InboundEvent, Platform, SendOptions,
    },
    config::{Config, DefaultPolicy, StorageConfig},
    error::Result,
    mcp::{
        ConversationSummary, DownloadedAttachment, HistoryEntry, HistoryLookup, MissingMessage,
        OutboundSink, SinkError, ToolSurface, ViewedAttachment, serve,
    },
    meka::{MekaClient, find_profile},
    store::{
        AccountId, AccountIdentity, BacklogReport, BacklogScope, MessageKey, NewWatch, Policy,
        Store, WatchOutcome, WatchRecord,
    },
    watch::{WatchField, WatchMode},
};

/// Buffer between the channel pollers and the durable writer.
///
/// A durability bound, not a throughput knob. teloxide advances its `getUpdates` offset the moment
/// a burst arrives and confirms it once its own buffer has drained into this channel, so blocking
/// the poller when this is full is what holds that confirmation back. Whatever sits here unwritten
/// when it goes out is acknowledged to Telegram and never sent again, which makes this depth the
/// number of messages a hard kill can lose.
///
/// Not one, though: Discord's typing notices ride the same channel on a `try_send` and are dropped
/// when it is full, so a depth an ordinary burst fills would stop the settle window working.
const EVENT_BUFFER: usize = 8;

/// Buffer between the feed reader and the session task.
///
/// Sized so a model streaming a reply never has the reader waiting on a session task that is in
/// the middle of a request. meka drops a feed reader that falls behind its own 256-event ring, so
/// the reader has to be able to take a burst several times that size off the wire while the
/// session task is busy.
const FEED_BUFFER: usize = 4096;

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

/// Which account each channel is logged in as, resolved once at startup.
///
/// The account is part of every recorded message's identity, so the writer and the sink both need
/// it, and it cannot change while the process runs: a swapped token means a restart.
#[derive(Debug, Default)]
pub struct ChannelAccounts {
    by_channel: HashMap<String, AccountId>,
    /// `(channel, label)` for the item's orientation line, in the order the channels were
    /// added. The label is `None` for a channel whose account could not be resolved.
    identities: Vec<(String, Option<String>)>,
}

impl ChannelAccounts {
    /// Record that `channel` is logged in as `account`, known to people as `label`.
    pub fn add(&mut self, channel: &str, account: AccountId, label: String) {
        self.by_channel.insert(channel.to_string(), account);
        self.identities.push((channel.to_string(), Some(label)));
    }

    /// Record that `channel` could not say who it is logged in as.
    pub fn add_unknown(&mut self, channel: &str) {
        self.identities.push((channel.to_string(), None));
    }

    /// The account behind `channel`, if it could be resolved.
    pub fn get(&self, channel: &str) -> Option<AccountId> {
        self.by_channel.get(channel).copied()
    }

    /// Whether no channel at all resolved its account.
    pub fn is_empty(&self) -> bool {
        self.by_channel.is_empty()
    }

    /// Which account the agent appears as on each channel, for the item's orientation line.
    pub fn identities(&self) -> &[(String, Option<String>)] {
        &self.identities
    }
}

/// Ask each channel which account it is logged in as, and record the answer.
///
/// Done before anything is written because the account is part of every message's identity: a
/// message recorded without one could not be told apart from the same message id under a bot that
/// was since deleted and recreated. A channel that cannot answer is left out of the map, and
/// [`run`] then does not start it, which is the outcome its own login failing would have had.
///
/// A channel answering with a different account than last time is a bot swap, said at `warn`
/// because everything recorded under the old account carries message ids the new one cannot act
/// on.
async fn register_accounts(store: &Store, channels: &ChannelRegistry) -> ChannelAccounts {
    let mut accounts = ChannelAccounts::default();
    // By id rather than in the registry's own order, which is a hash map's, so an item names
    // the accounts in the same order on every start.
    let mut channels: Vec<&Arc<dyn Channel>> = channels.iter().collect();
    channels.sort_by(|left, right| left.id().as_str().cmp(right.id().as_str()));
    for channel in channels {
        let identity = match channel.probe().await {
            Ok(identity) => identity,
            Err(error) => {
                tracing::error!(
                    channel = %channel.id(),
                    "could not find out which account this channel is logged in as, so it will \
                     not be started: {}",
                    error
                );
                accounts.add_unknown(channel.id().as_str());
                continue;
            }
        };
        let label = identity.username.as_ref().map_or_else(
            || identity.display_name.clone(),
            |username| format!("@{username}"),
        );
        let registration = match store
            .register_account(
                channel.id().as_str(),
                channel.platform().as_str(),
                &AccountIdentity {
                    platform_id: identity.id,
                    username: identity.username,
                    display_name: Some(identity.display_name),
                },
                Utc::now(),
            )
            .await
        {
            Ok(registration) => registration,
            Err(error) => {
                tracing::error!(
                    channel = %channel.id(),
                    "could not record which account this channel is logged in as, so it will not \
                     be started: {}",
                    error
                );
                accounts.add_unknown(channel.id().as_str());
                continue;
            }
        };
        if let Some(previous) = registration.replaced {
            tracing::warn!(
                channel = %channel.id(),
                now = %label,
                before = %previous.label(),
                "this channel is logged in as a different account than last time; messages \
                 recorded under the old one cannot be replied to, reacted to, edited or deleted \
                 from this one, and history_read marks them as from a previous account"
            );
        }
        tracing::info!(channel = %channel.id(), account = %label, "logged in");
        accounts.add(channel.id().as_str(), registration.account, label);
    }
    accounts
}

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

    // After the bind, so a port conflict is still found before any platform is asked anything,
    // and before the sink and the writer exist, since both key what they write on the answer.
    let accounts = Arc::new(register_accounts(&store, &channels).await);
    if accounts.is_empty() {
        return Err(crate::error::BridgeError::command(
            "no channel could say which account it is logged in as, so nothing it heard could be \
             recorded; the errors above say why",
        ));
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
        Arc::clone(&accounts),
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
    //
    // Only channels whose account resolved are started. The rest are dead from the outset, which
    // is what they would have been had their own login failed, and they count as such.
    let running: Vec<Arc<dyn Channel>> = channels
        .iter()
        .filter(|channel| accounts.get(channel.id().as_str()).is_some())
        .map(Arc::clone)
        .collect();
    let live_channels = Arc::new(AtomicUsize::new(running.len()));
    for channel in running {
        let sender = event_sender.clone();
        let shutdown = shutdown.clone();
        let live_channels = Arc::clone(&live_channels);
        let deaf = deaf.clone();
        // Asked before the channel runs, so the first session it opens is not missed.
        let catching_up = channel.catch_up().map(|catch_up| {
            tokio::spawn(catch_up_on_reconnect(
                store.clone(),
                Arc::clone(&channel),
                catch_up,
                sender.clone(),
                CatchUpSettings {
                    default_policy: config.bridge.default_policy,
                    history_retention: config.storage.history_retention,
                    retry_after: CATCH_UP_RETRY_AFTER,
                },
                shutdown.clone(),
            ))
        });
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
            // It holds a sender, and the writer ends only once every sender is gone, so it has to
            // stop with the channel it was catching up.
            if let Some(catching_up) = catching_up {
                catching_up.abort();
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

    // Written by the writer and read by the session task, which is why it is shared rather than
    // owned by either.
    let typing = Arc::new(inbound::TypingState::default());
    tasks.spawn({
        let store = store.clone();
        let config = Arc::clone(&config);
        let wake_drain = Arc::clone(&wake_drain);
        let typing = Arc::clone(&typing);
        let accounts = Arc::clone(&accounts);
        async move {
            inbound::writer(store, config, event_receiver, wake_drain, typing, accounts).await
        }
    });

    // The session task binds the session and publishes it here; the reader opens the feed once
    // it has, and again on a rebind. The position is the last event the session task handled,
    // read by the reader on every connection so meka replays strictly after it.
    let (session_sender, session_receiver) = watch::channel::<Option<uuid::Uuid>>(None);
    let position = Arc::new(AtomicU64::new(0));
    let (feed_sender, feed_receiver) = mpsc::channel(FEED_BUFFER);
    tasks.spawn({
        let meka = meka.clone();
        let position = Arc::clone(&position);
        let shutdown = shutdown.clone();
        async move { feed::feed_reader(meka, session_receiver, position, feed_sender, shutdown).await }
    });

    tasks.spawn({
        let context = SessionContext {
            store: store.clone(),
            config: Arc::clone(&config),
            meka: meka.clone(),
            channels: Arc::clone(&channels),
            typing,
            accounts: Arc::clone(&accounts),
            permission_checked: Arc::new(tokio::sync::OnceCell::new()),
            pending_session: Arc::default(),
            notices: inbound::NoticeLog::default(),
            serving: inbound::ServingProfile::default(),
            session: session_sender,
            position,
        };
        let typist = TurnTypist::new(
            Arc::clone(&channels),
            config.bridge.typing_indicator,
            config.bridge.typing_refresh,
            config.bridge.typing_max,
            Arc::clone(&presence),
        );
        let wake_drain = Arc::clone(&wake_drain);
        let shutdown = shutdown.clone();
        async move { inbound::session_loop(context, typist, feed_receiver, wake_drain, shutdown).await }
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
            // A hand-over in flight is durable on whichever side it reached; the next start posts
            // it again under the same key or asks meka what became of it.
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
/// Keyed by the message and the file's position in it, stable across a redelivery so a replay
/// reuses the handle already issued rather than minting a second one for the same file. The key
/// carries the account, so a bot recreated under the same channel never inherits a handle to a file
/// only the old bot could fetch.
async fn register_files(
    store: &Store,
    key: MessageKey<'_>,
    timestamp: chrono::DateTime<Utc>,
    attachments: &mut [crate::channel::Attachment],
) -> std::result::Result<(), crate::store::StoreError> {
    for (position, attachment) in attachments.iter_mut().enumerate() {
        let handle = store
            .register_attachment(crate::store::AttachmentRecord {
                conversation: key.conversation.to_string(),
                account: key.account,
                message_id: key.message_id.to_string(),
                revision: key.revision,
                position: u32::try_from(position).unwrap_or(u32::MAX),
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
/// channel accepts, including one it was given in its system prompt rather than in an item,
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
    /// Which account each channel speaks as, which is part of the identity of everything the
    /// agent sends.
    accounts: Arc<ChannelAccounts>,
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
        accounts: Arc<ChannelAccounts>,
    ) -> Self {
        Self {
            store,
            channels,
            storage,
            default_policy,
            meka,
            presence,
            accounts,
        }
    }

    /// Whether the session asking can be shown an image, which is its profile's `vision`, asked
    /// afresh each time.
    ///
    /// `caller` is the session meka names on the call, and the one whose history keeps the image,
    /// which is not always the bridge's own: a sub-agent spawned from it runs on a profile of its
    /// own. A call naming none was made outside any meka turn, so no session keeps the image and
    /// the bridge's own answers for it, or before one is bound, the default profile a new one
    /// starts on. Not cached, because a request can move a session to another profile while both
    /// processes run.
    async fn session_has_vision(
        &self,
        caller: Option<&str>,
    ) -> std::result::Result<bool, SinkError> {
        let session_id = match caller {
            Some(caller) => Some(uuid::Uuid::parse_str(caller).map_err(|error| {
                SinkError::Internal(format!(
                    "the call names session {caller:?}, which is not a session id: {error}"
                ))
            })?),
            None => self
                .store
                .session_id()
                .await
                .map_err(|error| SinkError::Internal(error.to_string()))?,
        };
        let profile = async {
            match session_id {
                Some(session_id) => self
                    .meka
                    .session(session_id)
                    .await
                    .map(|session| Some(session.profile)),
                None => Ok(None),
            }
        };
        let (profile, profiles) = tokio::try_join!(profile, self.meka.profiles())
            .map_err(|error| SinkError::Internal(error.to_string()))?;
        // Not answered as "no vision" when the profile is missing: that is a session meka cannot
        // run, or a meka with no default, and either is a check that failed rather than a model
        // that cannot see.
        find_profile(&profiles, profile.as_deref())
            .map(|profile| profile.vision)
            .ok_or_else(|| {
                SinkError::Internal(match profile {
                    Some(name) => format!("meka has no profile {name:?}"),
                    None => "meka has no default profile".to_string(),
                })
            })
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
            .get(&record.channel)
            .ok_or_else(|| SinkError::UnknownChannel {
                conversation: record.conversation.clone(),
                channel: record.channel.clone(),
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
    /// Whether a read by the session `caller` counts as the agent having seen it.
    ///
    /// Seen is what the bridge owes its own session, so only that session reading counts. A
    /// sub-agent or another session reading a chat has not shown it to the one this bridge hands
    /// messages to, and marking it would leave that session's next mention there with nothing to
    /// say about what it missed; over-reporting a backlog is the safer of the two mistakes. A call
    /// that names no session is taken to be the bridge's own, as one made outside any turn is.
    async fn read_counts_as_seen(&self, caller: Option<&str>) -> bool {
        let Some(caller) = caller else {
            return true;
        };
        match self.store.session_id().await {
            Ok(Some(bound)) => uuid::Uuid::parse_str(caller).is_ok_and(|caller| caller == bound),
            Ok(None) => false,
            Err(error) => {
                tracing::error!("could not read which session is bound: {}", error);
                false
            }
        }
    }

    /// Mark what a read returned as seen, logging rather than failing the read: what was read was
    /// read, and the cost of a miss is a backlog reported once more.
    async fn mark_read(&self, conversation: &ConversationId, ids: &[i64]) {
        if let Err(error) = self.store.mark_read(ids).await {
            tracing::error!(
                conversation = %conversation,
                "failed to record what the agent read: {}",
                error
            );
        }
    }

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
        let Some(account) = self.accounts.get(channel.id().as_str()) else {
            tracing::warn!(
                conversation = %conversation,
                "not recording a message the agent sent: the channel's account is unknown"
            );
            return;
        };
        record_own_messages(
            &self.store,
            self.storage.history_retention,
            conversation,
            account,
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
        if let Err(error) = self
            .store
            .touch_outbound(conversation.as_str(), platform.as_str(), Utc::now())
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
/// A free function rather than a method on [`BridgeSink`], because the session task's own failure
/// notice is outbound too and reaches its channel directly rather than through a sink.
///
/// Errors are logged rather than propagated: the send has already happened, and failing the tool
/// call would tell the agent its message did not go out when it did.
///
/// `seen` is true on every row, or the bridge's own output would count as a backlog and be offered
/// back to the agent as context it missed. `revised_at` gives a revision a key of its own, since it
/// keeps the message id it revises and the unique constraint would otherwise swallow it.
async fn record_own_messages(
    store: &Store,
    history_retention: Duration,
    conversation: &ConversationId,
    account: AccountId,
    session: Option<&str>,
    mut sent: Vec<crate::channel::SentMessage>,
    revised_at: Option<chrono::DateTime<Utc>>,
) {
    if history_retention.is_zero() {
        return;
    }
    let revision = crate::channel::revision_of(revised_at);
    for message in &mut sent {
        let key = MessageKey {
            conversation: conversation.as_str(),
            account,
            message_id: &message.message_id,
            revision,
        };
        if let Err(error) =
            register_files(store, key, message.timestamp, &mut message.attachments).await
        {
            tracing::warn!(
                conversation = %conversation,
                "could not register the files of a message the agent sent: {}",
                error
            );
        }
        // The same key an inbound message gets, and it cannot collide with one, though for a
        // different reason on each platform: Telegram numbers a chat's messages in one sequence
        // covering both directions, and Discord's snowflakes are unique everywhere. Neither ever
        // hands the bridge its own message as an inbound event in any case.
        //
        // A revision keeps the id it revises and is told apart by its time, in milliseconds. Two
        // edits sharing a key would leave the second unrecorded, which two API round trips inside
        // one millisecond cannot reach, and superseding orders against the revision's own row so
        // even a shared key marks correctly.
        let record = crate::store::MessageRecord {
            id: 0,
            conversation: conversation.as_str().to_string(),
            account,
            message_id: message.message_id.clone(),
            revision,
            sender_id: (!message.sender.id.is_empty()).then(|| message.sender.id.clone()),
            sender_name: message.sender.display_name.clone(),
            text: message.text.clone(),
            notes: (!message.notes.is_empty()).then(|| message.notes.join("; ")),
            attachments: Vec::new(),
            addressed: false,
            seen: true,
            own: true,
            session_id: session.map(str::to_string),
            deleted_at: None,
            superseded_at: None,
            timestamp: message.timestamp,
            previous_account: false,
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
            && let Err(error) = store.supersede_message(key, revised_at).await
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

    async fn file_send(
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

    async fn message_react(
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

    async fn message_edit(
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

    async fn message_delete(
        &self,
        conversation: &str,
        message_ids: &[String],
    ) -> std::result::Result<(), SinkError> {
        let conversation = self.resolve(conversation)?;
        let channel = self
            .channels
            .resolve(&conversation)
            .map_err(|error| SinkError::Internal(error.to_string()))?;
        let outcome = channel.delete_messages(&conversation, message_ids).await;
        // Warn, not info, and written whether or not the call succeeded. A deletion leaves no trace
        // on the platform, so this line is the only record of what was removed, and a batch that
        // fails partway is exactly when that record matters: some of those messages are gone. The
        // ids are in it because a count could not answer "which ones".
        tracing::warn!(
            conversation = %conversation,
            message_ids = %message_ids.join(", "),
            count = message_ids.len(),
            outcome = match &outcome {
                Ok(()) => "deleted".to_string(),
                Err(error) => format!("attempted, and failed: {error}"),
            },
            "the agent deleted messages"
        );
        outcome.map_err(|error| SinkError::Delivery(error.to_string()))?;
        // Marked in the history the same way a deletion reported by the platform is, so a message
        // the agent removed reads back as removed rather than as one that was never there. Failing
        // here would report a deletion that did happen as an error, so it is logged instead.
        let conversation = self.canonical(&conversation).await;
        match self.accounts.get(channel.id().as_str()) {
            Some(account) => {
                for message_id in message_ids {
                    if let Err(error) = self
                        .store
                        .mark_deleted(conversation.as_str(), account, message_id, Utc::now())
                        .await
                    {
                        tracing::warn!(
                            conversation = %conversation,
                            message_id = %message_id,
                            "could not mark a deleted message in the history: {}",
                            error
                        );
                    }
                }
            }
            None => tracing::warn!(
                conversation = %conversation,
                count = message_ids.len(),
                "could not mark deleted messages in the history: the channel's account is unknown"
            ),
        }
        Ok(())
    }

    async fn member_moderate(
        &self,
        conversation: &str,
        user_id: &str,
        action: crate::channel::MemberAction,
        until: Option<chrono::DateTime<Utc>>,
    ) -> std::result::Result<(), SinkError> {
        let (conversation, channel) = self.admin_target(conversation)?;
        channel
            .moderate_member(&conversation, user_id, action, until)
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
            "the agent moderated a member"
        );
        Ok(())
    }

    async fn member_set_rights(
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

    async fn member_set_roles(
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

    async fn message_pin(
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

    async fn chat_set(
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

    async fn member_get(
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

    async fn member_list(
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

    async fn attachment_view(
        &self,
        handle: &str,
        session: Option<&str>,
    ) -> std::result::Result<ViewedAttachment, SinkError> {
        let (record, channel) = self.attachment(handle).await?;

        match self.session_has_vision(session).await {
            Ok(true) => {}
            Ok(false) => {
                return Ok(ViewedAttachment::Description(format!(
                    "This is a {} ({}). The current model has no vision, so it cannot be shown. \
                     Use attachment_download to get the file itself.",
                    record.kind,
                    describe_file(&record)
                )));
            }
            Err(error) => {
                tracing::warn!(
                    "could not ask meka whether the calling session's profile has vision ({}); \
                     describing the file instead of showing it",
                    error
                );
                // Not "no vision": that would stay in the agent's history as a fact about its
                // model, when all that failed was asking.
                return Ok(ViewedAttachment::Description(format!(
                    "This is a {} ({}). Whether the current model can see images could not be \
                     checked, so it is not shown. Use attachment_download to get the file itself.",
                    record.kind,
                    describe_file(&record)
                )));
            }
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
                "This is a {} ({}) and has no image preview. Use attachment_download to get the \
                 file itself.",
                record.kind,
                describe_file(&record)
            )));
        };

        // Answered before fetching where the size is already known. A connector refusing an
        // oversize file returns its own wording, which names a byte ceiling no operator
        // configured and says nothing about `attachment_download`; the branch below that
        // *does* say it only fires on the base64 form, which is the looser of meka's two
        // limits and so never first.
        //
        // Only when the file itself is what will be fetched. `record.bytes` sizes the main file
        // while a preview fetches the thumbnail, so checking one against the other rejected every
        // oversized video on the strength of a number describing something else.
        if !preview && record.bytes.is_some_and(|bytes| bytes > MAX_VIEW_BYTES) {
            return Ok(ViewedAttachment::Description(format!(
                "This {} is {}, too large to show inline. Use attachment_download to get the file \
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
                 attachment_download to get the file itself.",
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
                "This {} is {} bytes, too large to show inline. Use attachment_download to get the \
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

    async fn attachment_download(
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
        let platform = self
            .channels
            .resolve(&conversation)
            .map_err(|error| SinkError::Internal(error.to_string()))?
            .platform();
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
            .set_policy(
                conversation.as_str(),
                platform.as_str(),
                policy,
                until,
                reason,
                Utc::now(),
            )
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

    async fn backlog_check(
        &self,
        scope: &BacklogScope,
    ) -> std::result::Result<BacklogReport, SinkError> {
        // Parsed rather than trusted, so an id naming no configured channel is refused instead of
        // quietly matching nothing, which from the caller's side is indistinguishable from a room
        // that has gone quiet. `resolve` does not rewrite the id, so the string reaching the store
        // is the one the caller passed.
        for id in scope.conversations.iter().chain(&scope.exclude) {
            self.resolve(id)?;
        }
        backlog(&self.store, self.default_policy, scope)
            .await
            .map_err(|error| SinkError::Internal(error.to_string()))
    }

    async fn watch_write(
        &self,
        name: &str,
        patterns: &[String],
        field: WatchField,
        mode: WatchMode,
        conversation: Option<&str>,
        until: Option<chrono::DateTime<Utc>>,
        reason: Option<&str>,
    ) -> std::result::Result<WatchOutcome, SinkError> {
        // Validated here rather than left to the matcher, so a rule that could never fire is
        // refused while the agent is still looking at it instead of being stored and silently
        // matching nothing. The offending pattern is named: with a list this long, "one of these
        // is invalid" would leave the agent bisecting its own rule set.
        if let Err(why) = crate::watch::validate_name(name) {
            return Ok(WatchOutcome::Refused(format!(
                "{name:?} is not a usable watch name: {why}"
            )));
        }
        for pattern in patterns {
            if let Err(why) = crate::watch::compile_pattern(pattern) {
                return Ok(WatchOutcome::Refused(format!(
                    "the pattern {pattern:?} cannot be used: {why}"
                )));
            }
        }
        // Resolved for the same reason `set_policy` resolves: a watch scoped to an id that names no
        // configured channel would be stored and never consulted.
        let conversation = conversation.map(|id| self.resolve(id)).transpose()?;
        let platform = conversation
            .as_ref()
            .map(|conversation| {
                self.channels
                    .resolve(conversation)
                    .map(|channel| channel.platform())
                    .map_err(|error| SinkError::Internal(error.to_string()))
            })
            .transpose()?;
        self.store
            .write_watch(
                NewWatch {
                    name,
                    conversation: conversation.as_ref().map(ConversationId::as_str),
                    platform: platform.map(Platform::as_str),
                    field,
                    mode,
                    patterns,
                    until,
                    reason,
                },
                Utc::now(),
            )
            .await
            .map_err(|error| SinkError::Internal(error.to_string()))
    }

    async fn watch_delete(
        &self,
        name: &str,
    ) -> std::result::Result<Option<WatchRecord>, SinkError> {
        self.store
            .remove_watch(name)
            .await
            .map_err(|error| SinkError::Internal(error.to_string()))
    }

    async fn watch_list(&self) -> std::result::Result<Vec<WatchRecord>, SinkError> {
        self.store
            .list_watches(Utc::now())
            .await
            .map_err(|error| SinkError::Internal(error.to_string()))
    }

    async fn history_read(
        &self,
        conversation: &str,
        sender_ids: &[String],
        limit: usize,
        before: Option<i64>,
        after: Option<i64>,
        session: Option<&str>,
    ) -> std::result::Result<Vec<HistoryEntry>, SinkError> {
        let conversation = self.resolve(conversation)?;
        let records = self
            .store
            .history(conversation.as_str(), sender_ids, limit, before, after)
            .await
            .map_err(|error| SinkError::Internal(error.to_string()))?;
        // Reading is seeing. Left unmarked, a muted chat the agent kept up with through this tool
        // owed it the same messages for as long as they were kept, since only a hand-over marked
        // anything. Marked after the read, so each entry still says whether it was news.
        if self.read_counts_as_seen(session).await {
            let ids: Vec<i64> = records.iter().map(|record| record.id).collect();
            self.mark_read(&conversation, &ids).await;
        }
        Ok(records.into_iter().map(history_entry).collect())
    }

    async fn history_lookup(
        &self,
        conversation: &str,
        message_ids: &[String],
        session: Option<&str>,
    ) -> std::result::Result<HistoryLookup, SinkError> {
        let conversation = self.resolve(conversation)?;
        let records = self
            .store
            .messages_by_id(conversation.as_str(), message_ids)
            .await
            .map_err(|error| SinkError::Internal(error.to_string()))?;
        // Shown, so seen, the same as a read of the whole conversation. Marked once everything else
        // has answered rather than now: the platform is asked for what the record lacks one id at a
        // time, and a call that runs out of time in between would leave these marked and never
        // shown.
        let read: Vec<i64> = records.iter().map(|record| record.id).collect();
        let recorded: HashSet<String> = records
            .iter()
            .map(|record| record.message_id.clone())
            .collect();
        let mut lookup = HistoryLookup {
            entries: records.into_iter().map(history_entry).collect(),
            missing: Vec::new(),
        };

        // `resolve` above refused a channel that is not configured, so this only fails if the
        // registry changed underneath, which it does not.
        let channel = self
            .channels
            .resolve(&conversation)
            .map_err(|error| SinkError::Internal(error.to_string()))?;
        for message_id in message_ids
            .iter()
            .filter(|message_id| !recorded.contains(*message_id))
        {
            let reason = match channel.fetch_message(&conversation, message_id).await {
                Ok(Some(found)) => {
                    lookup.entries.push(platform_entry(&conversation, found));
                    continue;
                }
                Ok(None) => "not in this bridge's record, and the platform has no such message \
                             here"
                    .to_string(),
                Err(ChannelError::Unsupported { .. }) => "not in this bridge's record, and this \
                                                          platform gives a bot no way to fetch a \
                                                          message by id"
                    .to_string(),
                Err(error) => {
                    format!("not in this bridge's record, and fetching it failed: {error}")
                }
            };
            lookup.missing.push(MissingMessage {
                message_id: message_id.clone(),
                reason,
            });
        }
        // Reading order, with what the platform supplied placed among what was recorded. RFC 3339
        // at one offset sorts as text exactly as it does as time.
        lookup
            .entries
            .sort_by(|first, second| first.timestamp.cmp(&second.timestamp));
        if self.read_counts_as_seen(session).await {
            self.mark_read(&conversation, &read).await;
        }
        Ok(lookup)
    }

    async fn history_search(
        &self,
        query: &str,
        conversation: Option<&str>,
        sender_ids: &[String],
        limit: usize,
    ) -> std::result::Result<Vec<HistoryEntry>, SinkError> {
        let conversation = conversation.map(|id| self.resolve(id)).transpose()?;
        let records = self
            .store
            .search_messages(
                query,
                conversation.as_ref().map(ConversationId::as_str),
                sender_ids,
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
                .search_messages(conversation, query, sender_ids, limit - entries.len())
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
                        entries.push(platform_entry(conversation, message));
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
                    .find(|policy| policy.conversation == record.address)
                    .cloned();
                let unseen = unseen.get(&record.address).copied().unwrap_or_default();
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
            .policy(&record.address)
            .await
            .map_err(|error| SinkError::Internal(error.to_string()))?;
        let unseen = self
            .store
            .unseen_counts()
            .await
            .map_err(|error| SinkError::Internal(error.to_string()))?
            .get(&record.address)
            .copied()
            .unwrap_or_default();
        Ok(Some(self.summarize(record, policy, unseen)))
    }
}

/// What the chats a scope covers are holding for the agent, together and one by one.
///
/// Shared by the `backlog_check` tool and `mekabridge unseen`, so the tool and the gate command
/// cannot disagree about which chats a question covers. Each chat's policy is the one in force now,
/// an explicit ruling or else the configured default for its kind, since "the chats that do not
/// wake me" is a question about now.
pub async fn backlog(
    store: &Store,
    default_policy: DefaultPolicy,
    scope: &BacklogScope,
) -> std::result::Result<BacklogReport, crate::store::StoreError> {
    let named = !scope.conversations.is_empty();
    let mut backlogs = store
        .backlogs(named.then_some(scope.conversations.as_slice()))
        .await?;
    let policies = Policies::load(store, default_policy).await?;

    let candidates: Vec<String> = if named {
        scope.conversations.clone()
    } else {
        backlogs.keys().cloned().collect()
    };
    let mut counted = HashSet::new();
    let mut report = BacklogReport::default();
    for address in candidates {
        if scope.exclude.contains(&address) || !counted.insert(address.clone()) {
            continue;
        }
        let policy = policies.of(&address);
        match scope.policy {
            Some(wanted) if policy != wanted => continue,
            // A blocked chat owes nothing: the agent ruled it out, and what was recorded before the
            // block would otherwise count against it for as long as it was kept. Named, it is
            // answered for like any other.
            None if policy == Policy::Block && !named => continue,
            _ => {}
        }
        let summary = backlogs.remove(&address).unwrap_or_default();
        report.total.absorb(&summary);
        // A named chat is always present, so a gate pointing into it never sees the field vanish
        // when its backlog is read. Unnamed, only the ones with something waiting, which keeps the
        // answer to "every chat" readable on a bridge in hundreds of them.
        if named || summary.count > 0 {
            report.conversations.insert(address, summary);
        }
    }
    Ok(report)
}

/// The policy in force for each conversation now: its own ruling while that stands, and otherwise
/// the configured default for its kind.
struct Policies {
    rulings: HashMap<String, Policy>,
    kinds: HashMap<String, String>,
    default_policy: DefaultPolicy,
}

impl Policies {
    async fn load(
        store: &Store,
        default_policy: DefaultPolicy,
    ) -> std::result::Result<Self, crate::store::StoreError> {
        let kinds = store
            .list_conversations(None, usize::MAX)
            .await?
            .into_iter()
            .map(|record| (record.address, record.kind))
            .collect();
        let now = Utc::now();
        let rulings = store
            .list_policies()
            .await?
            .into_iter()
            .filter(|ruling| !ruling.expired(now))
            .map(|ruling| (ruling.conversation, ruling.policy))
            .collect();
        Ok(Self {
            rulings,
            kinds,
            default_policy,
        })
    }

    fn of(&self, address: &str) -> Policy {
        self.rulings.get(address).copied().unwrap_or_else(|| {
            self.default_policy.for_kind(chat_kind(
                self.kinds.get(address).map_or("", String::as_str),
            ))
        })
    }
}

/// How far before a session opened a recorded message has to be to count as where its gap starts.
///
/// The session's opening is this machine's clock and a message's time is the platform's, so a
/// message said just after the opening can carry a time just before it. Starting a little earlier
/// costs only messages fetched twice, which the writer recognises as redeliveries; starting from
/// one said after the opening would skip the whole gap.
const CATCH_UP_CLOCK_MARGIN: chrono::TimeDelta = chrono::TimeDelta::minutes(1);

/// Most messages fetched back for one conversation each time a channel catches up.
///
/// Up to six requests' worth on Discord. Past it the oldest of what was missed is left behind and
/// the log says so.
const MAX_CATCH_UP_PER_CONVERSATION: usize = 500;

/// How long a catch-up waits before trying again what it could not do the first time.
pub const CATCH_UP_RETRY_AFTER: Duration = Duration::from_secs(30);

/// What [`catch_up_on_reconnect`] needs from the bridge's configuration.
#[derive(Debug, Clone, Copy)]
pub struct CatchUpSettings {
    /// What a conversation with no ruling of its own follows, to tell which are blocked.
    pub default_policy: DefaultPolicy,
    /// `[storage].history_retention`, which bounds how far back is fetched.
    pub history_retention: Duration,
    /// [`CATCH_UP_RETRY_AFTER`], a setting only so a test need not wait it out.
    pub retry_after: Duration,
}

/// Fetch what each conversation on `channel` missed every time it opens a fresh session, and feed
/// it in as though it had just arrived.
///
/// Through the same writer as everything live, so it is gated, recorded and woken for exactly as
/// it would have been, and what overlaps with live delivery or the record is recognised there as a
/// redelivery. Starts from the newest message somebody else said in each conversation before the
/// session opened, so a chat nothing was said in costs one request and nothing else. Not from the
/// bot's own: those are sent over HTTP, which can keep working while the gateway is down, and one
/// sent in the middle of an outage would pass for the end of the gap before it. Nor from one that
/// arrived live while the session waited for its servers, for the same reason.
///
/// Only conversations with something recorded are asked about, so `history_retention` bounds the
/// list: a chat the bot left long ago, which would answer every session with a refusal Discord
/// counts against it, ages out. Blocked conversations are skipped, since the writer would only
/// throw their messages away.
pub async fn catch_up_on_reconnect(
    store: Store,
    channel: Arc<dyn Channel>,
    catch_up: crate::channel::CatchUp,
    sender: mpsc::Sender<InboundEvent>,
    settings: CatchUpSettings,
    shutdown: CancellationToken,
) {
    // Nothing older than the history keeps is fetched back. The operator chose not to keep it, and
    // fetching it again would only count it as owed until the janitor deleted it once more.
    let window = if settings.history_retention.is_zero() {
        catch_up.window
    } else {
        catch_up.window.min(settings.history_retention)
    };
    let window = chrono::TimeDelta::from_std(window).unwrap_or(chrono::TimeDelta::MAX);
    loop {
        let opened = tokio::select! {
            () = shutdown.cancelled() => return,
            opened = catch_up.openings.next() => opened,
        };
        let since = opened
            .checked_sub_signed(window)
            .unwrap_or(chrono::DateTime::<Utc>::MIN_UTC);
        let before = opened
            .checked_sub_signed(CATCH_UP_CLOCK_MARGIN)
            .unwrap_or(chrono::DateTime::<Utc>::MIN_UTC);
        let read = match store.catch_up_points(channel.id().as_str(), before).await {
            Ok(points) => Policies::load(&store, settings.default_policy)
                .await
                .map(|policies| (points, policies)),
            Err(error) => Err(error),
        };
        let (points, policies) = match read {
            Ok(read) => read,
            Err(error) => {
                tracing::error!(
                    channel = %channel.id(),
                    "could not read where to catch up from; trying again shortly: {}",
                    error
                );
                // Put back, so the next attempt catches up from this opening rather than from
                // whatever opens after it.
                catch_up.openings.record(opened);
                tokio::select! {
                    () = shutdown.cancelled() => return,
                    () = tokio::time::sleep(settings.retry_after) => continue,
                }
            }
        };

        let mut fetched = 0_usize;
        let mut conversations = 0_usize;
        let mut failed = Vec::new();
        for (address, newest) in points {
            if policies.of(&address) == Policy::Block {
                continue;
            }
            let Some(conversation) = ConversationId::parse(&address) else {
                continue;
            };
            match catch_up_conversation(
                &*channel,
                &conversation,
                newest.as_deref(),
                since,
                &sender,
                &shutdown,
            )
            .await
            {
                CaughtUp::Fetched(0) => {}
                CaughtUp::Fetched(count) => {
                    fetched += count;
                    conversations += 1;
                }
                CaughtUp::Failed(error) => {
                    tracing::warn!(
                        conversation = %conversation,
                        "could not fetch what was said while disconnected; trying again shortly: {}",
                        error
                    );
                    failed.push((conversation, newest));
                }
                CaughtUp::Stopped => return,
            }
        }
        // Once, after a pause: what fails here is usually Discord having a bad minute, which is
        // also when sessions drop. A chat that fails twice is given up on until the next session.
        if !failed.is_empty() {
            tokio::select! {
                () = shutdown.cancelled() => return,
                () = tokio::time::sleep(settings.retry_after) => {}
            }
            for (conversation, newest) in failed {
                match catch_up_conversation(
                    &*channel,
                    &conversation,
                    newest.as_deref(),
                    since,
                    &sender,
                    &shutdown,
                )
                .await
                {
                    CaughtUp::Fetched(0) => {}
                    CaughtUp::Fetched(count) => {
                        fetched += count;
                        conversations += 1;
                    }
                    CaughtUp::Failed(error) => tracing::warn!(
                        conversation = %conversation,
                        "gave up fetching what was said while disconnected: {}",
                        error
                    ),
                    CaughtUp::Stopped => return,
                }
            }
        }
        if fetched > 0 {
            tracing::info!(
                channel = %channel.id(),
                fetched,
                conversations,
                "fetched what was said while disconnected"
            );
        }
    }
}

/// What came of catching up one conversation.
enum CaughtUp {
    /// This many messages went to the writer, redeliveries included.
    Fetched(usize),
    Failed(crate::channel::ChannelError),
    /// The bridge is stopping, or the writer is gone.
    Stopped,
}

/// Fetch what one conversation missed and feed it to the writer.
async fn catch_up_conversation(
    channel: &dyn Channel,
    conversation: &ConversationId,
    newest: Option<&str>,
    since: chrono::DateTime<Utc>,
    sender: &mpsc::Sender<InboundEvent>,
    shutdown: &CancellationToken,
) -> CaughtUp {
    let found = tokio::select! {
        () = shutdown.cancelled() => return CaughtUp::Stopped,
        found = channel.backfill(conversation, newest, since, MAX_CATCH_UP_PER_CONVERSATION) => found,
    };
    let found = match found {
        Ok(found) => found,
        Err(error) => return CaughtUp::Failed(error),
    };
    if found.truncated {
        tracing::warn!(
            conversation = %conversation,
            "more was said while disconnected than {} messages; the oldest of it is not recorded",
            MAX_CATCH_UP_PER_CONVERSATION
        );
    }
    let count = found.events.len();
    for event in found.events {
        if sender.send(event).await.is_err() {
            return CaughtUp::Stopped;
        }
    }
    CaughtUp::Fetched(count)
}

/// The stored spelling of a conversation's shape, as the policy defaults are keyed.
fn chat_kind(kind: &str) -> ChatKind {
    match kind {
        "direct" => ChatKind::Direct,
        "group" => ChatKind::Group,
        "channel" => ChatKind::Channel,
        _ => ChatKind::Unknown,
    }
}

/// A message the platform supplied, as the history tools hand one back.
///
/// The connector settles whether it was the bot's own from its account id. The rest is unknowable
/// from a platform's record, which gives a message id, an author and a time and nothing the bridge
/// recorded about it, so it is left at its defaults rather than guessed.
fn platform_entry(conversation: &ConversationId, message: FoundMessage) -> HistoryEntry {
    HistoryEntry {
        conversation: conversation.as_str().to_string(),
        message_id: message.message_id,
        sender: message.sender_name,
        sender_id: None,
        text: message.text,
        notes: None,
        attachments: Vec::new(),
        addressed: false,
        unseen: false,
        own: message.own,
        session: None,
        deleted: false,
        superseded: false,
        previous_account: false,
        timestamp: message.timestamp.to_rfc3339(),
        // Not a row in the bridge's history, so there is nothing to page back from. Zero is the
        // value `history_read` already treats as no cursor.
        cursor: 0,
    }
}

/// Translate a stored message into what the history tools hand back.
fn history_entry(record: crate::store::MessageRecord) -> HistoryEntry {
    HistoryEntry {
        conversation: record.conversation,
        message_id: record.message_id,
        sender: record.sender_name,
        sender_id: record.sender_id,
        text: record.text,
        notes: record.notes,
        attachments: record.attachments,
        addressed: record.addressed,
        unseen: !record.seen,
        own: record.own,
        session: record.session_id,
        deleted: record.deleted_at.is_some(),
        superseded: record.superseded_at.is_some(),
        previous_account: record.previous_account,
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
    // second overwrote the first, `attachment_download` handed back the wrong bytes with no error,
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
        let effective = live.as_ref().map_or_else(
            || self.default_policy.for_kind(chat_kind(&record.kind)),
            |policy| policy.policy,
        );
        ConversationSummary {
            id: record.address,
            channel: record.channel,
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
            // A blocked chat owes nothing, as `backlog_check` has it: what was recorded before the
            // block would otherwise be reported as owed for as long as it was kept.
            unseen: if effective == Policy::Block {
                0
            } else {
                unseen
            },
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
        // the first, `attachment_download` returned the wrong bytes with no error at all, and
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
