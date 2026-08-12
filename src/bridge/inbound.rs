//! The inbound path: channel events into the durable queue, and the queue into turns.
//!
//! Two tasks, deliberately separate. The writer persists every event before acknowledging it, so a
//! crash cannot swallow a message a user already sent. The drain loop claims batches and runs
//! turns, and is the only thing here that talks to meka. One drain loop means the bridge never
//! races itself, but it no longer means the session is idle whenever the bridge wants it: meka runs
//! background tasks and scheduled wakes of its own, so a submission can be refused by a turn this
//! bridge knows nothing about. That refusal is a deferral, not a failure, and is treated as one.
//!
//! Batching is the reason messages that pile up during a turn become one turn rather than several.
//! That matches what happens to a person who puts their phone down: they come back to the whole
//! conversation, not to one message at a time, and it saves a provider round trip per message.

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    bridge::{
        envelope::{Envelope, MissedContext, MissedMessage},
        turn::TurnRunner,
    },
    channel::{ChannelRegistry, ConversationId, InboundEvent, InboundMessage},
    config::Config,
    meka::{MekaClient, MekaError, TurnOutcome},
    store::{ConversationRecord, EnqueueOutcome, Policy, PolicyRecord, QueuedMessage, Store},
};

/// Safety-net poll interval. The writer notifies the drain loop directly, so this only covers rows
/// that became eligible without an enqueue, such as a failed batch returning to `pending`.
const DRAIN_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// How long to leave a session alone after it refuses a submission because it is already running a
/// turn.
///
/// There is nothing to wait on but the next refusal, so this is the whole of the backoff. Short
/// enough that a chat is answered promptly once meka frees up, long enough that a turn lasting
/// minutes costs a handful of rejected requests rather than thousands.
const DEFER_RETRY_INTERVAL: Duration = Duration::from_secs(2);

/// Length of the per-turn fence marker. Six hex characters is 24 bits, which is far more than
/// enough against a user guessing it inside a single turn.
const NONCE_BYTES: usize = 3;

/// Persist events from every channel into the queue.
///
/// Runs until the sender side closes, which happens when all channels have stopped.
pub async fn writer(
    store: Store,
    config: Arc<Config>,
    mut events: mpsc::Receiver<InboundEvent>,
    wake_drain: Arc<Notify>,
) {
    while let Some(mut event) = events.recv().await {
        let conversation = event.conversation().clone();
        // A retraction never reaches the queue, the envelope, or the agent. It exists so the
        // bridge's own record of a chat does not outlive the chat: without it, `read_history` would
        // keep handing back a message its author deleted, and on a platform that reports deletions
        // there is no excuse for that.
        let InboundEvent::Message(message) = &mut event else {
            let InboundEvent::Retraction { message_id, .. } = &event else {
                continue;
            };
            match store
                .forget_message(conversation.as_str(), message_id)
                .await
            {
                Ok(true) => tracing::debug!(
                    conversation = %conversation,
                    message_id = %message_id,
                    "dropped a message its author deleted"
                ),
                Ok(false) => {}
                Err(error) => tracing::error!(
                    conversation = %conversation,
                    "failed to drop a deleted message: {}",
                    error
                ),
            }
            continue;
        };
        let disposition = match gate(&store, &config, message).await {
            Ok(disposition) => disposition,
            Err(error) => {
                // Failing open. A store that cannot answer should not also cost people their
                // messages, and the worst case is a chat being heard that would rather not have
                // been.
                tracing::error!(
                    conversation = %conversation,
                    "could not read the conversation's policy: {}",
                    error
                );
                Disposition::Deliver
            }
        };
        if disposition == Disposition::Discard {
            continue;
        }

        if let Err(error) = record_conversation(&store, message).await {
            tracing::error!(conversation = %conversation, "failed to record conversation: {}", error);
            continue;
        }
        if let Err(error) = register_attachments(&store, message).await {
            // Registration is what mints the handles the agent fetches by, so without it the files
            // are unreachable. The message still goes through: its text is usually the point, and
            // the envelope says the attachment cannot be fetched rather than pretending otherwise.
            tracing::error!(conversation = %conversation, "failed to register attachments: {}", error);
        }

        // Keyed on the gate's decision rather than on what the queue does with it below. `seen`
        // means the agent has been accounted for this message somehow, and every queue outcome
        // qualifies: it was queued, or it duplicates one already queued, or it was shed and counted
        // into the notice the next envelope carries. Only a withheld message is genuinely owed.
        let queued = disposition == Disposition::Deliver;
        if let Err(error) = record_message(&store, &config, message, queued).await {
            tracing::error!(conversation = %conversation, "failed to record a message: {}", error);
        }

        // A withheld message stops before the queue. It is recorded, so the agent can read it when
        // something finally does wake this conversation, but it consumes no queue depth and costs
        // no provider turn.
        if queued {
            let payload = match serde_json::to_string(&event) {
                Ok(payload) => payload,
                Err(error) => {
                    tracing::error!(conversation = %conversation, "failed to encode event: {}", error);
                    continue;
                }
            };

            let outcome = store
                .enqueue(
                    conversation.as_str(),
                    event.external_id(),
                    &payload,
                    event.timestamp(),
                    config.bridge.max_queue_depth,
                )
                .await;

            match outcome {
                Ok(EnqueueOutcome::Queued) => wake_drain.notify_one(),
                Ok(EnqueueOutcome::Duplicate) => {
                    tracing::debug!(
                        conversation = %conversation,
                        external_id = event.external_id(),
                        "ignoring a redelivered message"
                    );
                }
                Ok(EnqueueOutcome::Dropped) => {
                    // Counted rather than silently discarded: the next envelope tells the agent its
                    // view of the conversation is incomplete.
                    tracing::warn!(
                        conversation = %conversation,
                        "inbound queue is full at {} messages; dropping",
                        config.bridge.max_queue_depth
                    );
                    if let Err(error) = store.note_dropped(1).await {
                        tracing::error!("failed to record a dropped message: {}", error);
                    }
                }
                Err(error) => {
                    tracing::error!(conversation = %conversation, "failed to enqueue: {}", error);
                }
            }
        }
    }
    tracing::info!("inbound writer stopped: all channels have shut down");
}

/// What the gate decided should happen to one message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Disposition {
    /// Queue it, so the agent is woken.
    Deliver,
    /// Record it without queueing it. The agent reaches it through the history tools, or through
    /// the context printed alongside whatever eventually does wake this conversation.
    Withhold,
    /// Keep nothing.
    Discard,
}

/// Decide what happens to one message, and annotate it when a lapsed policy is being lifted.
///
/// Resolution order is the explicit decision first and the configured default second. A
/// conversation nobody has ruled on has no record at all, which is why changing
/// `[bridge].default_policy` moves every such conversation at once while leaving the ones somebody
/// ruled on where they were put.
///
/// A lapsed record is cleared here rather than swept on a timer, and what it did is handed to the
/// message that lifted it: a policy whose effect is invisible gives the agent nothing to judge
/// whether to renew it on.
async fn gate(
    store: &Store,
    config: &Config,
    message: &mut InboundMessage,
) -> Result<Disposition, crate::store::StoreError> {
    let conversation = message.conversation.clone();
    let now = chrono::Utc::now();
    let record = store.policy(conversation.as_str()).await?;

    let policy = match record {
        Some(record) if record.expired(now) => {
            // Read back uncounted through `expire_policy` rather than reused from above: the record
            // in hand came through a cache that deliberately does not see drop counts, and the
            // count is the whole of what the agent is being told.
            if let Some(lapsed) = store.expire_policy(conversation.as_str()).await? {
                announce_expiry(message, &lapsed);
                tracing::info!(
                    conversation = %conversation,
                    policy = lapsed.policy.as_str(),
                    dropped = lapsed.dropped,
                    "a conversation policy expired"
                );
            }
            config.bridge.default_policy.for_kind(message.chat_kind)
        }
        Some(record) => record.policy,
        None => config.bridge.default_policy.for_kind(message.chat_kind),
    };

    match policy {
        Policy::Active => Ok(Disposition::Deliver),
        Policy::Block => {
            store.note_blocked_drop(conversation.as_str()).await?;
            tracing::debug!(
                conversation = %conversation,
                "discarding a message from a blocked conversation"
            );
            Ok(Disposition::Discard)
        }
        Policy::Mute if message.addressed => Ok(Disposition::Deliver),
        Policy::Mute => {
            // An exchange the agent is already in carries on without a second mention, which is
            // what stops "@bot what do you think?" followed by "no, the other one"
            // dying halfway through. Measured from the agent's own last message and
            // deliberately not extended by inbound traffic, so a chat it has stopped
            // answering goes quiet again on its own.
            let following_up = store
                .last_outbound_at(conversation.as_str())
                .await?
                .and_then(|at| now.signed_duration_since(at).to_std().ok())
                .is_some_and(|since| since < config.bridge.mute_followup);
            if following_up {
                Ok(Disposition::Deliver)
            } else {
                tracing::trace!(
                    conversation = %conversation,
                    "withholding a message from a muted conversation"
                );
                Ok(Disposition::Withhold)
            }
        }
    }
}

/// Tell the agent what a policy it set did before it lapsed.
///
/// The wording differs by policy on purpose. Under a block the messages are gone and saying so is
/// the whole of it; under a mute they were recorded, so the note points at the tool that reaches
/// them, or the agent will assume the same thing happened to both.
fn announce_expiry(message: &mut crate::channel::InboundMessage, lapsed: &PolicyRecord) {
    let Some(until) = lapsed.until else {
        return;
    };
    let until = until.to_rfc3339();
    match lapsed.policy {
        Policy::Block if lapsed.dropped > 0 => {
            let noun = if lapsed.dropped == 1 {
                "message was"
            } else {
                "messages were"
            };
            message.notes.push(format!(
                "you had blocked this chat until {until}; {} {noun} discarded while it was blocked \
                 and cannot be recovered",
                lapsed.dropped
            ));
        }
        Policy::Mute => message.notes.push(format!(
            "you had muted this chat until {until}; anything said meanwhile was recorded, and \
             read_history will show it"
        )),
        Policy::Block | Policy::Active => {}
    }
}

/// Collect what each muted conversation in this batch said while the agent was not listening.
///
/// Marks it seen in the same call, so the next mention in that chat reports what has accumulated
/// since rather than the same backlog again. The consequence is that a turn which fails and is
/// retried from the queue gets a smaller lookback the second time: the messages themselves are
/// still in the history, and repeating the count on every attempt would be the worse trade.
///
/// Every conversation in the batch is asked, not only the muted ones. Under `active` the answer is
/// almost always nothing, because a delivered message is recorded as seen, but "almost" is doing
/// work: unmuting a conversation leaves behind whatever piled up while it was muted. Asking only
/// the muted ones would report that backlog to nobody and never clear it, so `unseen` would climb
/// for the life of the conversation and `list_conversations` would go on quoting it.
async fn missed_context(
    context: &DrainContext,
    events: &[InboundEvent],
) -> (
    Vec<MissedContext>,
    Vec<(ConversationId, chrono::DateTime<chrono::Utc>)>,
) {
    let now = chrono::Utc::now();
    let mut collected = Vec::new();
    // What to mark accounted for, once a turn carrying it has actually been accepted.
    let mut spent = Vec::new();
    let mut visited = BTreeSet::new();
    for event in events {
        let conversation = event.conversation();
        if !visited.insert(conversation.clone()) {
            continue;
        }
        let InboundEvent::Message(message) = event else {
            continue;
        };

        let policy = match context.store.policy(conversation.as_str()).await {
            Ok(Some(record)) if !record.expired(now) => record.policy,
            Ok(_) => context
                .config
                .bridge
                .default_policy
                .for_kind(message.chat_kind),
            Err(error) => {
                tracing::error!(
                    conversation = %conversation,
                    "could not read the conversation's policy: {}",
                    error
                );
                continue;
            }
        };
        let muted = policy == Policy::Mute;

        // Bounded by the newest message in this batch from this conversation, so anything that
        // lands while the turn is being assembled stays unseen and is reported next time
        // rather than silently marked.
        let through = events
            .iter()
            .filter(|event| event.conversation() == conversation)
            .map(InboundEvent::timestamp)
            .max()
            .unwrap_or(now);
        match context
            .store
            .take_unseen(
                conversation.as_str(),
                through,
                context.config.bridge.mute_context,
            )
            .await
        {
            // A conversation being heard in full with nothing owed has nothing to say, so it is
            // dropped rather than rendered as an empty block. A muted one is still worth a line: it
            // tells the agent why it is seeing one message out of a conversation.
            Ok((0, _)) if !muted => spent.push((conversation.clone(), through)),
            Ok((count, recent)) => {
                spent.push((conversation.clone(), through));
                collected.push(MissedContext {
                    conversation: conversation.clone(),
                    muted,
                    count,
                    recent: recent
                        .into_iter()
                        .map(|record| MissedMessage {
                            sender: record.sender_name,
                            // Descriptor lines stand in for a message whose content is not text, so
                            // a photo does not read back as somebody
                            // saying nothing.
                            text: match (record.text.trim().is_empty(), record.notes) {
                                (true, Some(notes)) => notes,
                                (true, None) => "[no text]".to_string(),
                                (false, _) => record.text,
                            },
                            timestamp: record.timestamp,
                        })
                        .collect(),
                });
            }
            Err(error) => tracing::error!(
                conversation = %conversation,
                "could not read what a conversation withheld: {}",
                error
            ),
        }
    }
    (collected, spent)
}

/// Write one message to the history, unless history is switched off.
async fn record_message(
    store: &Store,
    config: &Config,
    message: &InboundMessage,
    queued: bool,
) -> Result<(), crate::store::StoreError> {
    if config.storage.history_retention.is_zero() {
        return Ok(());
    }
    store
        .record_message(crate::store::MessageRecord {
            // Assigned by the store on insert.
            id: 0,
            conversation_id: message.conversation.as_str().to_string(),
            external_id: message.external_id.clone(),
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
            addressed: message.addressed,
            // A queued message is one the agent is about to be handed, so it is accounted for
            // already and must not also be offered back to it later as context it missed.
            seen: queued,
            timestamp: message.timestamp,
        })
        .await
}

/// Register the files an event brought with it and stamp each with the handle the agent fetches by.
///
/// Done here rather than in the channel: the channel has no store handle, and giving it one would
/// couple every platform to the database for no other reason. Runs before the payload is
/// serialized, so the handles travel with the queued event and survive a restart.
async fn register_attachments(
    store: &Store,
    message: &mut InboundMessage,
) -> Result<(), crate::store::StoreError> {
    for (index, attachment) in message.attachments.iter_mut().enumerate() {
        let handle = store
            .register_attachment(crate::store::AttachmentRecord {
                // Stable across a redelivery of the same message, so a replay reuses the handle
                // already issued rather than minting a second one for the same file.
                id: format!("{}:{}:{index}", message.conversation, message.external_id),
                conversation_id: message.conversation.as_str().to_string(),
                channel_id: message.channel.as_str().to_string(),
                kind: attachment.kind.as_str().to_string(),
                file_ref: attachment.file_ref.clone(),
                thumb_ref: attachment.thumb_ref.clone(),
                file_name: attachment.file_name.clone(),
                media_type: attachment.media_type.clone(),
                bytes: attachment.bytes,
                path: None,
                created_at: message.timestamp,
            })
            .await?;
        attachment.handle = Some(handle);
    }
    Ok(())
}

/// Store the conversation an event came from, so it stays in the address book the agent can list.
async fn record_conversation(
    store: &Store,
    message: &InboundMessage,
) -> Result<(), crate::store::StoreError> {
    store
        .upsert_conversation(ConversationRecord {
            id: message.conversation.as_str().to_string(),
            channel_id: message.channel.as_str().to_string(),
            platform: message.platform.as_str().to_string(),
            chat: message.conversation.chat().to_string(),
            thread: message.conversation.thread().map(str::to_string),
            title: message
                .chat_title
                .clone()
                .or_else(|| Some(message.sender.display_name.clone())),
            kind: message.chat_kind.as_str().to_string(),
            created_at: message.timestamp,
            last_inbound_at: Some(message.timestamp),
            last_outbound_at: None,
        })
        .await
}

/// Everything the drain loop needs.
pub struct DrainContext {
    pub store: Store,
    pub config: Arc<Config>,
    pub meka: MekaClient,
    pub channels: Arc<ChannelRegistry>,
    pub runner: TurnRunner,
    /// The account the agent appears as on each channel, resolved on first use.
    pub identities: Arc<tokio::sync::OnceCell<ChannelIdentities>>,
    /// Guards the one-per-process reconciliation of the session's permission level.
    pub permission_checked: Arc<tokio::sync::OnceCell<()>>,
}

/// Claim batches and run turns until `shutdown` fires.
///
/// A turn already in flight is allowed to finish; only the wait between turns is interruptible.
/// Cutting a turn off mid-flight would leave its batch `in_flight` for the next start to recover,
/// having already spent the provider tokens.
pub async fn drain_loop(context: DrainContext, wake: Arc<Notify>, shutdown: CancellationToken) {
    // When the previous turn ran, so the next batch can say which of its messages landed while the
    // agent was mid-turn and therefore could not have shaped the reply it sent.
    let mut last_turn: Option<TurnWindow> = None;
    loop {
        tokio::select! {
            () = shutdown.cancelled() => {
                tracing::info!("drain loop stopping");
                return;
            }
            () = wake.notified() => {}
            () = tokio::time::sleep(DRAIN_POLL_INTERVAL) => {}
        }

        loop {
            if shutdown.is_cancelled() {
                return;
            }
            // Let a burst finish before claiming any of it. Without this the first fragment of a
            // thought starts a turn on its own and the agent answers before reading the rest.
            if let Some(delay) = settle_delay(&context).await {
                tokio::select! {
                    () = shutdown.cancelled() => return,
                    () = tokio::time::sleep(delay) => {}
                }
                // Round again rather than claiming straight away: more may have arrived while
                // waiting, which extends the quiet period afresh.
                continue;
            }
            let batch = match context
                .store
                .claim_batch(context.config.bridge.batch_max_messages)
                .await
            {
                Ok(batch) => batch,
                Err(error) => {
                    tracing::error!("failed to claim a batch: {}", error);
                    break;
                }
            };
            if batch.is_empty() {
                break;
            }
            last_turn = deliver(&context, batch, last_turn, &shutdown).await;
        }
    }
}

/// Which account the agent appears as on each channel, as `(channel, identity)`. The identity is
/// `None` when the platform could not be reached to ask.
pub type ChannelIdentities = Vec<(String, Option<String>)>;

/// When a turn ran, so the next batch can tell which of its messages arrived while it was running.
#[derive(Debug, Clone, Copy)]
struct TurnWindow {
    started_at: chrono::DateTime<chrono::Utc>,
    ended_at: chrono::DateTime<chrono::Utc>,
}

/// How long to hold off before claiming, or `None` to claim now.
///
/// Returns `None` both when nothing is waiting and when the wait is over, because the caller treats
/// those the same: go look at the queue.
///
/// Timestamps here are the platform's own send times, not when the row was written, which is what
/// makes a backlog replayed after a restart release immediately instead of being debounced as
/// though it had just arrived. The cost is a dependence on the two clocks roughly agreeing; both
/// directions of skew degrade safely, into either no debounce or one full quiet period.
async fn settle_delay(context: &DrainContext) -> Option<Duration> {
    let settle = context.config.bridge.settle;
    let settle_max = context.config.bridge.settle_max;
    if settle.is_zero() {
        return None;
    }

    let window = match context.store.pending_window().await {
        Ok(window) => window,
        Err(error) => {
            // Delivering promptly beats stalling on a query the next round will retry anyway.
            tracing::error!("could not read the pending window: {}", error);
            return None;
        }
    };
    let (oldest, newest) = window?;

    let now = chrono::Utc::now();
    // Clamped because a platform clock ahead of this host's would otherwise read as negative.
    let elapsed =
        |since: chrono::DateTime<chrono::Utc>| (now - since).to_std().unwrap_or(Duration::ZERO);
    let quiet_for = elapsed(newest);
    let waited = elapsed(oldest);

    if quiet_for >= settle || waited >= settle_max {
        return None;
    }
    // Whichever expires first: the quiet period, or the ceiling on the oldest message.
    Some((settle - quiet_for).min(settle_max - waited))
}

/// Hand one batch to the agent and record what happened to it.
async fn deliver(
    context: &DrainContext,
    batch: Vec<QueuedMessage>,
    last_turn: Option<TurnWindow>,
    shutdown: &CancellationToken,
) -> Option<TurnWindow> {
    let started_at = chrono::Utc::now();
    let sequences: Vec<i64> = batch.iter().map(|message| message.seq).collect();

    let mut events: Vec<InboundEvent> = Vec::with_capacity(batch.len());
    let mut undecodable = Vec::new();
    for message in &batch {
        match serde_json::from_str::<InboundEvent>(&message.payload) {
            Ok(event) => events.push(event),
            Err(error) => {
                // A payload this build cannot read will never become readable, so retrying it would
                // wedge the queue behind it forever.
                tracing::error!(
                    seq = message.seq,
                    "dropping an undecodable queue payload: {}",
                    error
                );
                undecodable.push(message.seq);
            }
        }
    }
    if !undecodable.is_empty()
        && let Err(error) = context.store.complete_batch(&undecodable).await
    {
        tracing::error!("failed to discard undecodable payloads: {}", error);
    }
    if events.is_empty() {
        return last_turn;
    }

    // A message whose own timestamp falls inside the previous turn arrived while the agent was
    // working. Compared against platform time, so clock skew can only cause an under-report, which
    // reads as an ordinary message rather than a wrong claim.
    if let Some(window) = last_turn {
        for event in &mut events {
            let InboundEvent::Message(message) = event else {
                continue;
            };
            message.arrived_mid_turn =
                message.timestamp >= window.started_at && message.timestamp <= window.ended_at;
        }
    }

    let conversations: BTreeSet<ConversationId> = events
        .iter()
        .map(|event| event.conversation().clone())
        .collect();

    let dropped = context.store.take_dropped().await.unwrap_or_else(|error| {
        tracing::error!("failed to read the dropped-message counter: {}", error);
        0
    });

    let session_id = match ensure_session(context).await {
        Ok(session_id) => session_id,
        Err(error) => {
            record_failure(context, &sequences, &error.to_string()).await;
            return last_turn;
        }
    };

    let identities = channel_identities(context).await;

    let (missed, withheld) = missed_context(context, &events).await;

    let nonce = nonce();
    let message = Envelope {
        events: &events,
        dropped,
        identities: &identities,
        missed: &missed,
        nonce: &nonce,
    }
    .render();

    tracing::info!(
        messages = events.len(),
        conversations = conversations.len(),
        session_id = %session_id,
        "submitting a turn"
    );

    let result = submit(context, session_id, &message, &conversations, shutdown).await;

    // meka forgetting the session (its row was deleted, or the database was replaced) is
    // recoverable exactly once: bind a fresh session and replay the same batch into it.
    let result = match result {
        Err(error) if error.is_session_missing() && context.config.session.recreate_on_missing => {
            tracing::warn!(
                "meka no longer knows session {}; creating a replacement. The agent's memory of \
                 earlier conversations is gone. If this keeps happening, check meka's \
                 `[serve].delete_on_idle`: with it on, an idle session's row is deleted rather \
                 than merely evicted, which wipes the assistant every idle_timeout.",
                session_id
            );
            if let Err(error) = context.store.clear_session_id().await {
                tracing::error!("failed to clear the stale session binding: {}", error);
            }
            match ensure_session(context).await {
                Ok(replacement) => {
                    let message = Envelope {
                        events: &events,
                        dropped,
                        identities: &identities,
                        // Reused rather than recomputed: taking it again would come back empty,
                        // because the first call marked it seen.
                        missed: &missed,
                        nonce: &nonce,
                    }
                    .render();
                    submit(context, replacement, &message, &conversations, shutdown).await
                }
                Err(error) => Err(error),
            }
        }
        other => other,
    };

    // Spent for every outcome except the refusal above, which returns before this. A turn that
    // failed still reached the agent, and repeating the whole backlog on each retry would be the
    // worse trade; a turn meka never accepted did not reach it at all.
    if !matches!(&result, Err(error) if error.is_turn_in_flight()) {
        for (conversation, through) in &withheld {
            if let Err(error) = context
                .store
                .mark_seen(conversation.as_str(), *through)
                .await
            {
                tracing::error!(
                    conversation = %conversation,
                    "failed to record what the agent was shown: {}",
                    error
                );
            }
        }
    }

    match result {
        // A turn that produced meka's empty-response stand-in and called nothing did no work at
        // all: no message was sent, no tool ran. Handing the batch over again is therefore free of
        // side effects, and far better than leaving somebody who just messaged the bot in silence.
        Ok(report) if report.produced_nothing() => {
            tracing::warn!(
                text = %report.text_preview.trim(),
                "the model returned an empty response and called no tools; retrying the batch"
            );
            record_failure(context, &sequences, "the model returned an empty response").await;
        }
        Ok(report) => {
            match &report.outcome {
                TurnOutcome::Finished { stop_reason, .. } => tracing::info!(
                    stop_reason = %stop_reason,
                    sends = report.sends,
                    tool_calls = report.tool_calls,
                    text_chars = report.text_length,
                    "turn finished"
                ),
                TurnOutcome::Cancelled { reason } => {
                    tracing::warn!(reason = %reason, "turn cancelled");
                }
            }
            if report.is_silent() {
                // Not an error: the agent is allowed to read something and say nothing. Logged at
                // warn with what it produced instead, because from the other end this looks exactly
                // like a broken bridge, and the text is usually what explains which one it was.
                tracing::warn!(
                    conversations = conversations.len(),
                    tool_calls = report.tool_calls,
                    text = %report.text_preview.trim(),
                    "the agent sent no messages this turn"
                );
            }
            if let Err(error) = context.store.complete_batch(&sequences).await {
                tracing::error!("failed to mark a delivered batch: {}", error);
            }
            if let Err(error) = context.store.mark_turn_completed(chrono::Utc::now()).await {
                tracing::error!("failed to record the turn timestamp: {}", error);
            }
        }
        // The turn was accepted and then the stream died. meka keeps running it, so the batch did
        // reach the agent and resubmitting would duplicate a reply the user is about to receive.
        // The messages are marked delivered once the session goes idle: what the agent chose to do
        // with them is its business, and that is exactly the contract for a normal turn too.
        //
        // `wait_until_idle` is trustworthy here and nowhere else in this function. The turn it is
        // waiting on is one this bridge submitted over HTTP, which is the only kind meka counts in
        // the `turn_in_flight` it answers with; see `submit` for what that field misses.
        Err(error) if error.turn_may_still_be_running() => {
            tracing::warn!(
                "lost the turn stream ({}); the turn is still running, waiting for it to finish",
                error
            );
            match context
                .meka
                .wait_until_idle(session_id, context.config.meka.turn_timeout)
                .await
            {
                Ok(true) => {
                    tracing::info!("the interrupted turn finished; marking its batch delivered");
                    if let Err(error) = context.store.complete_batch(&sequences).await {
                        tracing::error!("failed to mark a delivered batch: {}", error);
                    }
                    if let Err(error) = context.store.mark_turn_completed(chrono::Utc::now()).await
                    {
                        tracing::error!("failed to record the turn timestamp: {}", error);
                    }
                }
                Ok(false) | Err(_) => {
                    // Still busy after a full turn budget, or unreachable. Requeueing risks a
                    // duplicate, but leaving the batch in flight forever loses it outright, and
                    // the attempt counter bounds how often that can repeat.
                    tracing::error!(
                        "could not confirm the interrupted turn finished; requeueing its batch, \
                         which may deliver the same messages twice"
                    );
                    record_failure(context, &sequences, &error.to_string()).await;
                }
            }
        }
        // meka spent a whole turn budget refusing, having retried throughout `submit`, so this is a
        // session that is wedged rather than merely occupied. Every submission was refused before
        // it ran, so nothing reached the agent and nothing was lost: the batch goes back to the
        // queue untouched and the next drain tick starts a fresh round of retries. Counting it as a
        // failed delivery would let a busy session burn the retry budget and eventually declare a
        // message undeliverable that meka never even saw.
        Err(error) if error.is_turn_in_flight() => {
            tracing::warn!(
                "meka has been busy with a turn this bridge did not start for a full turn budget; \
                 requeueing the batch"
            );
            if let Err(error) = context.store.release_batch(&sequences).await {
                tracing::error!("failed to requeue a deferred batch: {}", error);
            }
            // The envelope this counter was rendered into is being thrown away, so put it back or
            // the notice that the queue overflowed is lost to a turn that never ran.
            if dropped > 0
                && let Err(error) = context.store.note_dropped(dropped).await
            {
                tracing::error!("failed to restore the dropped-message counter: {}", error);
            }
            // The backlog this envelope reported is deliberately *not* marked accounted for. It
            // was never delivered, and spending it here would leave the retry telling a chat with
            // thirty messages waiting that nothing had been said in it.
            //
            // No turn ran, so there is no window to report either. Inventing one would flag the
            // next batch's messages as having arrived mid-turn against a turn that never happened,
            // and those messages are in that very batch.
            return last_turn;
        }
        Err(error) => {
            tracing::error!("turn failed: {}", error);
            record_failure(context, &sequences, &error.to_string()).await;
        }
    }

    Some(TurnWindow {
        started_at,
        ended_at: chrono::Utc::now(),
    })
}

/// Submit a turn, retrying on a timer while meka is busy with a turn of its own.
///
/// A `turn-in-flight` rejection means some turn is running: one of this bridge's own after a
/// dropped stream, or one meka started for itself. The batch is refused before it runs, so retrying
/// delivers it exactly once. It stays claimed throughout, so nothing else picks it up and the
/// envelope is not rebuilt per attempt.
///
/// The 409 is the only trustworthy sign that the session is busy. meka's `turn_in_flight` is not,
/// and asking it is worse than not asking: the field reports an atomic counter only `POST /turn`
/// increments, while the refusal comes from a session mutex that scheduled jobs and background-task
/// outcomes hold as well (meka's `schedule::run_prompt_in_session`). Through one of those the
/// session calls itself idle and refuses anyway, so waiting for it to go idle returns at once and
/// the retry becomes a spin as tight as the two processes can trade requests.
async fn submit(
    context: &DrainContext,
    session_id: Uuid,
    message: &str,
    conversations: &BTreeSet<ConversationId>,
    shutdown: &CancellationToken,
) -> Result<crate::bridge::turn::TurnReport, MekaError> {
    let deadline = tokio::time::Instant::now() + context.config.meka.turn_timeout;
    // Opened once for the whole episode rather than per attempt. Per attempt, each call started its
    // own `typing_max` countdown, so the ceiling could never be reached however long the wait ran;
    // it also cancelled every request mid-flight, which on a channel whose typing endpoint is
    // rate-limited past the retry interval meant the indicator never appeared at all.
    let mut typing: Option<CancellationToken> = None;
    let mut attempt = 0_u32;
    loop {
        let result = context.runner.run(session_id, message, conversations).await;
        let refused = result
            .as_ref()
            .err()
            .is_some_and(MekaError::is_turn_in_flight);
        // Belt and braces rather than load-bearing: the same budget bounds the turn itself, so a
        // 409 can only ever be seen strictly before the deadline -- a refusal that took longer
        // would have failed as a timeout and never reached this path. The guard costs one
        // comparison and keeps the loop correct if those two budgets are ever separated.
        if !refused || (attempt > 0 && tokio::time::Instant::now() >= deadline) {
            if let Some(typing) = typing {
                typing.cancel();
            }
            return result;
        }
        if attempt == 0 {
            // Once per episode, not once per attempt: at one line per attempt this buried whatever
            // an operator opened the log to find.
            tracing::info!(
                "meka is running a turn this bridge did not start; holding the batch and retrying \
                 every {}s until it finishes",
                DEFER_RETRY_INTERVAL.as_secs()
            );
            // meka refuses solely because it is running a turn, and since a backgrounded tool call
            // delivers its outcome as one, the agent is working just as much as it would be on
            // ours. Without this the chat is silent for as long as the other turn lasts.
            typing = Some(context.runner.start_typing(conversations));
        }
        attempt += 1;

        let interrupted = tokio::select! {
            () = shutdown.cancelled() => true,
            () = tokio::time::sleep(DEFER_RETRY_INTERVAL) => false,
        };
        if interrupted {
            if let Some(typing) = typing {
                typing.cancel();
            }
            return result;
        }
    }
}

/// Mark a batch failed, and tell the operator about anything that will never be delivered.
async fn record_failure(context: &DrainContext, sequences: &[i64], reason: &str) {
    let outcome = context
        .store
        .fail_batch(sequences, reason, context.config.bridge.turn_retries)
        .await;
    let outcome = match outcome {
        Ok(outcome) => outcome,
        Err(error) => {
            tracing::error!("failed to record a batch failure: {}", error);
            return;
        }
    };
    if !outcome.retrying.is_empty() {
        tracing::warn!(
            count = outcome.retrying.len(),
            "requeued messages after a failed turn"
        );
    }
    if outcome.exhausted.is_empty() {
        return;
    }
    tracing::error!(
        count = outcome.exhausted.len(),
        "giving up on messages after {} attempt(s); they will never reach the agent",
        context.config.bridge.turn_retries + 1
    );
    notify_owner(
        context,
        &format!(
            "mekabridge could not deliver {} message(s) to the agent after {} attempt(s). Last \
             error: {reason}",
            outcome.exhausted.len(),
            context.config.bridge.turn_retries + 1
        ),
    )
    .await;
}

/// Send an operator notice to the configured owner conversation.
///
/// This is the one case where the bridge writes chat content of its own. It is strictly about the
/// bridge being broken, never about the conversation, and it is skipped entirely when no owner is
/// configured.
async fn notify_owner(context: &DrainContext, text: &str) {
    let Some(owner) = &context.config.bridge.owner_conversation else {
        return;
    };
    let Some(conversation) = ConversationId::parse(owner) else {
        tracing::error!("[bridge].owner_conversation {:?} is not a valid id", owner);
        return;
    };
    let channel = match context.channels.resolve(&conversation) {
        Ok(channel) => channel,
        Err(error) => {
            tracing::error!("cannot reach the owner conversation: {}", error);
            return;
        }
    };
    if let Err(error) = channel
        .send_text(&conversation, text, &crate::channel::SendOptions::default())
        .await
    {
        tracing::error!("failed to notify the owner: {}", error);
    }
}

/// Which account the agent appears as on each channel, resolved once and remembered.
///
/// Cached because it is a network round trip per channel and the answer effectively never changes;
/// a rename is picked up on the next restart. A failed probe is not cached, so a channel that was
/// unreachable at first gets named once it comes back.
async fn channel_identities(context: &DrainContext) -> ChannelIdentities {
    if let Some(known) = context.identities.get() {
        return known.clone();
    }
    let mut identities = Vec::new();
    let mut complete = true;
    for channel in context.channels.iter() {
        let label = match channel.probe().await {
            Ok(identity) => identity
                .username
                .map(|username| format!("@{username}"))
                .or(Some(identity.display_name)),
            Err(error) => {
                tracing::debug!(channel = %channel.id(), "could not probe identity: {}", error);
                complete = false;
                None
            }
        };
        identities.push((channel.id().as_str().to_string(), label));
    }
    if complete {
        let _ = context.identities.set(identities.clone());
    }
    identities
}

/// Bring a session's permission level in line with `[session].permission`.
///
/// A session's level is fixed when it is created, so without this an operator who edits the config
/// sees no effect and no explanation. `ask` is the case that makes it matter: meka prompts for
/// every tool call at that level and this bridge answers no prompts, so a session created at `ask`
/// cannot reply to anyone until its level changes.
///
/// Runs once per process, on the first turn rather than at startup, because the bridge comes up
/// before meka does.
async fn reconcile_permission(context: &DrainContext, session_id: Uuid) {
    if context.permission_checked.get().is_some() {
        return;
    }
    let desired = context.config.session.permission;
    match context.meka.session(session_id).await {
        Ok(info) if info.permission == desired.as_str() => {
            let _ = context.permission_checked.set(());
        }
        Ok(info) => {
            tracing::warn!(
                "session {} is at permission {:?} but the config says {:?}; updating it",
                session_id,
                info.permission,
                desired.as_str()
            );
            match context
                .meka
                .set_session_permission(session_id, desired)
                .await
            {
                Ok(()) => {
                    tracing::info!("session permission set to {:?}", desired.as_str());
                    let _ = context.permission_checked.set(());
                }
                Err(error) => {
                    tracing::error!("could not update the session permission: {}", error);
                }
            }
        }
        Err(error) => {
            tracing::debug!(
                "could not read the session to check its permission: {}",
                error
            );
        }
    }
}

/// Return the bound meka session, creating one if this is the first turn.
async fn ensure_session(context: &DrainContext) -> Result<Uuid, MekaError> {
    if let Some(session_id) = context
        .store
        .session_id()
        .await
        .map_err(|error| MekaError::Decode(error.to_string()))?
    {
        reconcile_permission(context, session_id).await;
        return Ok(session_id);
    }
    let session_id = context
        .meka
        .create_session(
            context.config.session.cwd.as_deref(),
            context.config.session.permission,
        )
        .await?;
    tracing::info!(session_id = %session_id, "created the meka session for this bridge");
    context
        .store
        .set_session_id(session_id)
        .await
        .map_err(|error| MekaError::Decode(error.to_string()))?;
    Ok(session_id)
}

/// Random fence marker for one turn's envelope.
fn nonce() -> String {
    use rand::RngExt as _;
    let bytes: [u8; NONCE_BYTES] = rand::rng().random();
    bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;
    use crate::channel::{
        Admission, Attachment, AttachmentKind, ChannelId, ChatKind, InboundMessage, Platform,
        Sender,
    };

    fn attachment(file_ref: &str) -> Attachment {
        Attachment {
            kind: AttachmentKind::Photo,
            file_name: None,
            media_type: Some("image/jpeg".to_string()),
            bytes: Some(2048),
            file_ref: file_ref.to_string(),
            thumb_ref: None,
            handle: None,
        }
    }

    fn event_with(attachments: Vec<Attachment>) -> InboundMessage {
        InboundMessage {
            channel: ChannelId::new("telegram"),
            platform: Platform::Telegram,
            conversation: ConversationId::parse("telegram:1").expect("valid"),
            external_id: "1".to_string(),
            message_id: "1".to_string(),
            chat_kind: ChatKind::Direct,
            chat_title: None,
            sender: Sender {
                id: "1".to_string(),
                display_name: "Alice".to_string(),
                username: None,
                is_bot: false,
                on_behalf_of_chat: false,
            },
            admission: Admission::User,
            sender_allowlisted: true,
            addressed: false,
            sender_roles: Vec::new(),
            text: "look".to_string(),
            reply_to: None,
            edited_at: None,
            forwarded_from: None,
            group_id: None,
            notes: Vec::new(),
            arrived_mid_turn: false,
            attachments,
            timestamp: Utc::now(),
        }
    }

    async fn store() -> Store {
        let store = Store::open_in_memory().await.expect("opens");
        store
            .upsert_conversation(ConversationRecord {
                id: "telegram:1".to_string(),
                channel_id: "telegram".to_string(),
                platform: "telegram".to_string(),
                chat: "1".to_string(),
                thread: None,
                title: Some("Alice".to_string()),
                kind: "direct".to_string(),
                created_at: Utc::now(),
                last_inbound_at: Some(Utc::now()),
                last_outbound_at: None,
            })
            .await
            .expect("conversation");
        store
    }

    #[tokio::test]
    async fn registration_stamps_a_handle_onto_every_attachment() {
        // The handle is the agent's only way to reach the file, and it has to be on the event
        // before the payload is serialized or it does not survive a restart.
        let store = store().await;
        let mut event = event_with(vec![attachment("AgACx1"), attachment("AgACx2")]);
        register_attachments(&store, &mut event)
            .await
            .expect("registers");

        let message = &event;
        let handles: Vec<&str> = message
            .attachments
            .iter()
            .map(|attachment| {
                attachment
                    .handle
                    .as_deref()
                    .expect("every attachment gets a handle")
            })
            .collect();
        assert_eq!(handles.len(), 2);
        assert_ne!(handles[0], handles[1], "handles must be distinct");
    }

    #[tokio::test]
    async fn a_redelivered_message_reuses_its_original_handles() {
        // Telegram replays updates whose offset was never committed. Minting a second handle for
        // the same file would leave an orphan row the sweep later deletes out from under
        // the agent.
        let store = store().await;
        let mut first = event_with(vec![attachment("AgACx1")]);
        register_attachments(&store, &mut first)
            .await
            .expect("registers");
        let mut second = event_with(vec![attachment("AgACx1")]);
        register_attachments(&store, &mut second)
            .await
            .expect("registers again");

        let first = &first;
        let second = &second;
        assert_eq!(first.attachments[0].handle, second.attachments[0].handle);
    }

    #[tokio::test]
    async fn a_registered_attachment_can_be_looked_up_by_its_handle() {
        let store = store().await;
        let mut event = event_with(vec![attachment("AgACx1")]);
        register_attachments(&store, &mut event)
            .await
            .expect("registers");

        let message = &event;
        let handle = message.attachments[0]
            .handle
            .as_deref()
            .expect("handle assigned");
        let record = store
            .attachment(handle)
            .await
            .expect("query")
            .expect("the handle resolves");
        assert_eq!(record.file_ref, "AgACx1");
        assert_eq!(record.channel_id, "telegram");
        assert!(record.path.is_none(), "nothing is downloaded on arrival");
    }

    #[tokio::test]
    async fn a_message_without_attachments_registers_nothing() {
        let store = store().await;
        let mut event = event_with(Vec::new());
        register_attachments(&store, &mut event)
            .await
            .expect("registers");
        assert!(store.attachment("1").await.expect("query").is_none());
    }

    #[test]
    fn nonces_are_hex_and_hard_to_guess() {
        let first = nonce();
        assert_eq!(first.len(), NONCE_BYTES * 2);
        assert!(first.chars().all(|character| character.is_ascii_hexdigit()));

        // Not a statistical test, just a guard against a constant sneaking in: a fixed fence marker
        // would let a user close the fence and forge a header.
        let samples: BTreeSet<String> = (0..32).map(|_| nonce()).collect();
        assert!(samples.len() > 1, "nonce must vary between turns");
    }
}
