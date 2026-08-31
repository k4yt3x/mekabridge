//! The inbound path: channel events into the durable queue, and the queue into turns.
//!
//! Two tasks, deliberately separate: the writer persists every event before acknowledging it, and
//! the drain loop claims batches and runs turns. One drain loop means the bridge never races
//! itself, but not that the session is idle whenever it wants: meka runs background tasks and
//! scheduled wakes of its own, so a submission can be refused by a turn this bridge knows nothing
//! about. That refusal is a deferral, not a failure.
//!
//! Messages piling up during a turn become one turn rather than several, which is what happens to
//! somebody who puts their phone down and saves a provider round trip per message.

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    bridge::{
        envelope::{Envelope, MissedContext, MissedMessage},
        turn::{TurnReport, TurnRunner},
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

/// Ceiling on the wait between attempts, and on a `Retry-After` the upstream asks for.
///
/// A provider is entitled to ask for an hour. Honouring that would hold a conversation open for an
/// hour with nobody told, which is worse for the person waiting than being told promptly that it
/// did not work; the retry budget is meant to ride out a blip, not an outage.
const RETRY_DELAY_MAX: Duration = Duration::from_secs(120);

/// How long after the last notice somebody still counts as typing.
///
/// Discord repeats its notice about every ten seconds while a person keeps going, so this is that
/// heartbeat plus enough slack to survive one being late or dropped. Erring long costs a moment of
/// extra wait; erring short would release mid-sentence, which is the thing being fixed.
const TYPING_TTL: Duration = Duration::from_secs(12);

/// How long a conversation is left alone after being told that something failed.
///
/// Long enough that a sustained outage produces one message rather than one per batch, short enough
/// that somebody who comes back an hour later and tries again is told rather than ignored.
const NOTICE_INTERVAL: Duration = Duration::from_secs(15 * 60);

/// Ceiling on how many conversations are remembered as having been told.
const NOTICE_LOG_MAX: usize = 4096;

/// Ceiling on how many conversations are remembered as having somebody mid-thought.
///
/// Far above the number that can have messages waiting at once, since the queue itself is capped.
const LATEST_SENDER_MAX: usize = 4096;

/// Length of the per-turn fence marker.
///
/// Sixteen hex characters, 64 bits. Guessing is not really the threat, since any exact occurrence
/// of the marker in user text is redacted before it reaches the agent, so a guessing attempt has
/// its own winning candidate stripped out. It was 24 bits, which a batch of messages could
/// enumerate a meaningful fraction of; the extra bytes cost nothing and remove the argument.
const NONCE_BYTES: usize = 8;

/// Who is currently composing, by conversation.
///
/// In memory only, and deliberately: it decides how long a conversation waits before its messages
/// are claimed, and after a restart nothing is mid-sentence as far as this process knows, so the
/// floor alone is the right answer. Written by the inbound writer and read by the drain loop, which
/// already share state this way.
#[derive(Debug, Default)]
pub struct TypingState {
    inner: std::sync::Mutex<Typing>,
}

/// The two halves of "is the person mid-thought still going".
#[derive(Debug, Default)]
struct Typing {
    /// Last notice per conversation and author. Keyed as text rather than by [`ConversationId`],
    /// which does not borrow as one, so a lookup costs a hash rather than a walk of the map.
    seen: std::collections::HashMap<(String, String), chrono::DateTime<chrono::Utc>>,
    /// Who most recently had a message queued in each conversation.
    ///
    /// A conversation is held for that person alone. Holding it for anybody who happens to be
    /// typing would mean a mention in a busy room waiting out the ceiling because the room is
    /// busy, which is the opposite of what this is for.
    latest: std::collections::HashMap<String, String>,
}

impl TypingState {
    /// Record that somebody is composing, and forget anybody who has since stopped.
    ///
    /// Pruned on write rather than on a timer, so the map cannot outgrow the number of people
    /// typing right now, and no task exists solely to clean it.
    fn note(&self, conversation: &ConversationId, author: &str, at: chrono::DateTime<chrono::Utc>) {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        inner.seen.retain(|_, last| *last > Self::cutoff());
        // Clamped so a platform reporting a time ahead of this host's cannot leave an entry that
        // never ages out and holds a conversation for good.
        let at = at.min(chrono::Utc::now());
        inner
            .seen
            .insert((conversation.as_str().to_string(), author.to_string()), at);
    }

    /// Note that a message from `author` has been queued, which is the only "they stopped" signal
    /// either platform gives.
    ///
    /// Neither sends an event when somebody stops typing: the client simply hides the indicator
    /// when the message lands. Without treating the message itself as the end of composing, every
    /// message would be held until its author's last notice aged out, which is the whole of the
    /// wait rather than a bound on it.
    fn queued(&self, conversation: &ConversationId, author: &str) {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        inner
            .seen
            .remove(&(conversation.as_str().to_string(), author.to_string()));
        // Bounded by dropping the lot, the same rule the store's policy cache uses. An entry is
        // only needed between a message being queued and its conversation being claimed, and
        // losing one means a chat is not held for typing, which releases sooner rather than later.
        if inner.latest.len() >= LATEST_SENDER_MAX {
            inner.latest.clear();
        }
        inner
            .latest
            .insert(conversation.as_str().to_string(), author.to_string());
    }

    /// Whether the person whose message is waiting has started composing again.
    fn active(&self, conversation_id: &str) -> bool {
        let Ok(inner) = self.inner.lock() else {
            // A poisoned lock means a writer panicked holding it. Reporting "nobody is typing"
            // releases the conversation, which is the direction that keeps messages moving.
            return false;
        };
        let Some(author) = inner.latest.get(conversation_id) else {
            return false;
        };
        inner
            .seen
            .get(&(conversation_id.to_string(), author.clone()))
            .is_some_and(|last| *last > Self::cutoff())
    }

    /// The instant before which a notice is too old to mean anybody is still going.
    fn cutoff() -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now() - chrono::Duration::from_std(TYPING_TTL).unwrap_or_default()
    }
}

/// Persist events from every channel into the queue.
///
/// Runs until the sender side closes, which happens when all channels have stopped.
pub async fn writer(
    store: Store,
    config: Arc<Config>,
    mut events: mpsc::Receiver<InboundEvent>,
    wake_drain: Arc<Notify>,
    typing: Arc<TypingState>,
) {
    while let Some(mut event) = events.recv().await {
        let conversation = event.conversation().clone();
        // Only a message is ever queued. The other two are handled here and go no further, so this
        // is one exhaustive match rather than a let-else with a fallback: a variant added later has
        // to be given a decision rather than falling into a catch-all that silently drops it.
        let message = match &mut event {
            InboundEvent::Message(message) => message,
            // A retraction marks the message rather than removing it. The record is what the agent
            // has to check itself against: anything it was woken for is already in its session for
            // good, so erasing the row took away the only thing that could later tell it what it
            // acted on has been taken back. Marked rows stay out of everything it is owed, so a
            // retracted message is never offered as context it missed.
            InboundEvent::Retraction {
                message_id,
                timestamp,
                ..
            } => {
                match store
                    .mark_deleted(conversation.as_str(), message_id, *timestamp)
                    .await
                {
                    Ok(true) => tracing::debug!(
                        conversation = %conversation,
                        message_id = %message_id,
                        "marked a message its author deleted"
                    ),
                    Ok(false) => {}
                    Err(error) => tracing::error!(
                        conversation = %conversation,
                        "failed to mark a deleted message: {}",
                        error
                    ),
                }
                continue;
            }
            // Held in memory only. It decides how long a conversation waits before its messages are
            // claimed, so it never needs to survive a restart: after one, nothing is mid-sentence
            // as far as this process knows, and the floor alone is the right answer.
            InboundEvent::Typing {
                author, timestamp, ..
            } => {
                typing.note(&conversation, author, *timestamp);
                continue;
            }
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
                // Said to the agent as well as logged, because the envelope is about to tell it
                // this is a chat it hears in full. That is what is happening, but it is not what
                // anybody decided, and without this the agent would take a database fault for a
                // setting and answer a muted room as though it had been invited to.
                message.notes.push(
                    "the bridge could not read this chat's attention settings, so it is being \
                     heard in full until it can"
                        .to_string(),
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

        // Taken while the message is still borrowed, since the queue path below serializes the
        // whole event and cannot hold that borrow at the same time.
        let sender_id = message.sender.id.clone();

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
                Ok(EnqueueOutcome::Queued) => {
                    // Whoever sent this is no longer composing it. Neither platform says so: the
                    // client just hides the indicator when the message lands. Without treating the
                    // message as the end, it would be held until its author's last notice aged out,
                    // which is the whole of the wait rather than a bound on it.
                    typing.queued(&conversation, &sender_id);
                    wake_drain.notify_one();
                }
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
/// A conversation nobody has ruled on has no record at all, which is why changing
/// `[bridge].default_policy` moves every such conversation at once while leaving ruled-on ones
/// where they were put.
///
/// A lapsed record is cleared here rather than swept on a timer, and what it did is handed to the
/// message that lifted it, since a policy whose effect is invisible gives the agent nothing to
/// judge a renewal on.
async fn gate(
    store: &Store,
    config: &Config,
    message: &mut InboundMessage,
) -> Result<Disposition, crate::store::StoreError> {
    let conversation = message.conversation.clone();
    let now = chrono::Utc::now();
    let record = store.policy(conversation.as_str()).await?;

    // Whether a lapsed decision left a notice on this message. It has to survive the match below,
    // because a notice on a message nobody is woken for is a notice nobody reads.
    let mut announced = false;
    let policy = match record {
        Some(record) if record.expired(now) => {
            // Computed before the announcement, which borrows the message mutably, and needed by
            // it: a lapsing decision does not restore whatever preceded it, it falls
            // through to here.
            let fallback = config.bridge.default_policy.for_kind(message.chat_kind);
            // Read back uncounted through `expire_policy` rather than reused from above: the record
            // in hand came through a cache that deliberately does not see drop counts, and the
            // count is the whole of what the agent is being told.
            if let Some(lapsed) = store.expire_policy(conversation.as_str()).await? {
                announced = announce_expiry(message, &lapsed, fallback);
                tracing::info!(
                    conversation = %conversation,
                    policy = lapsed.policy.as_str(),
                    dropped = lapsed.dropped,
                    "a conversation policy expired"
                );
            }
            fallback
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
        // A decision the agent made has just lapsed, and this is the message that discovered it.
        // Withholding it would file the notice into the history, where nothing will read it, and
        // leave the agent believing it can still hear a room it cannot. That is the exact
        // confusion the notice exists to prevent, so it is worth the one turn. Expiries are rare
        // by construction: only an explicitly timed decision has one at all.
        Policy::Mute if announced => Ok(Disposition::Deliver),
        // Mention-only means mention-only. There was once a window here that went on delivering
        // everything for a few minutes after the agent spoke, on the theory that an exchange
        // already under way should carry on without a second mention. In a busy room it delivered
        // the room: the agent was woken by conversations it had no part in, each arriving in an
        // envelope indistinguishable from one addressed to it, and every reply it was nudged into
        // making pushed the window out again. Following a conversation on is the agent's call to
        // make now, either by hearing the chat in full for a while or by arranging its own
        // look-back, and both of those it can say out loud.
        Policy::Mute => {
            tracing::trace!(
                conversation = %conversation,
                "withholding a message from a muted conversation"
            );
            Ok(Disposition::Withhold)
        }
    }
}

/// Tell the agent what a policy it set did before it lapsed.
///
/// The wording differs by policy because the news does. A lapsed block or mute is about messages
/// missed, but a lapsed `active` is the other direction: the agent has stopped hearing a room it
/// asked to hear, which without saying so reads as the room having gone quiet.
///
/// `fallback` is what the conversation reverts to, which is not the policy that preceded the one
/// lapsing: "back on mentions only" would be a lie where groups default to being heard in full.
///
/// Returns whether anything was said, rather than letting the caller recompute it, so the two
/// cannot disagree about which cases produce a notice.
fn announce_expiry(
    message: &mut crate::channel::InboundMessage,
    lapsed: &PolicyRecord,
    fallback: Policy,
) -> bool {
    let Some(until) = lapsed.until else {
        return false;
    };
    let before = message.notes.len();
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
        // Nothing observable changed, so there is nothing to report.
        Policy::Active if fallback == Policy::Active => {}
        Policy::Active => message.notes.push(format!(
            "you had been hearing this chat in full until {until}; it now {}",
            match fallback {
                Policy::Mute => "wakes you for mentions only",
                Policy::Block => "will not reach you at all",
                Policy::Active => "is heard in full",
            }
        )),
        Policy::Block => {}
    }
    message.notes.len() > before
}

/// One conversation's backlog, and the two bounds that describe exactly what was counted.
///
/// Both are needed. The timestamp is the ceiling asked about and the id is the high-water mark of
/// the rows that answered; either alone marks a set the agent was never shown. See
/// [`Store::mark_seen`].
struct Spent {
    conversation: ConversationId,
    watermark: i64,
    through: chrono::DateTime<chrono::Utc>,
}

/// Collect what each muted conversation in this batch said while the agent was not listening.
///
/// Marks it seen in the same call, so a failed turn retried from the queue gets a smaller lookback
/// the second time. The messages are still in the history, and repeating the count on every attempt
/// is the worse trade.
///
/// Every conversation in the batch is asked, not only the muted ones: unmuting one leaves behind
/// whatever piled up while it was muted, and asking only the muted ones would report that backlog
/// to nobody and never clear it.
async fn missed_context(
    context: &DrainContext,
    events: &[InboundEvent],
) -> (Vec<MissedContext>, Vec<Spent>) {
    let now = chrono::Utc::now();
    let mut collected = Vec::new();
    // What to mark accounted for, once a turn carrying it has actually been accepted. Both bounds
    // are carried because either alone describes a different set than the one counted; see
    // `Store::mark_seen`.
    let mut spent: Vec<Spent> = Vec::new();
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
            Ok((0, _, watermark)) if !muted => spent.push(Spent {
                conversation: conversation.clone(),
                watermark,
                through,
            }),
            Ok((count, recent, watermark)) => {
                spent.push(Spent {
                    conversation: conversation.clone(),
                    watermark,
                    through,
                });
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
            own: false,
            session_id: None,
            deleted_at: None,
            superseded_at: None,
            timestamp: message.timestamp,
        })
        .await?;
    // An edit is recorded as a row of its own, since the queue needs a distinct key to deliver it
    // as an event rather than discard it as a redelivery. Marking what it replaced is what stops
    // `read_history` returning the old and the new wording as two messages that both look current.
    if let Some(edited_at) = message.edited_at {
        store
            .supersede_message(
                message.conversation.as_str(),
                &message.message_id,
                &message.external_id,
                edited_at,
            )
            .await?;
    }
    Ok(())
}

/// [`super::register_files`] over an inbound message's own fields.
///
/// Runs before the payload is serialized, so the handles travel with the queued event and survive a
/// restart.
async fn register_attachments(
    store: &Store,
    message: &mut InboundMessage,
) -> Result<(), crate::store::StoreError> {
    super::register_files(
        store,
        &message.conversation,
        &message.channel,
        &message.external_id,
        message.timestamp,
        &mut message.attachments,
    )
    .await
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
    /// Who is composing right now, so a conversation can be held until they stop.
    pub typing: Arc<TypingState>,
    /// Who has already been told that something failed, so an outage is reported once rather than
    /// once per batch.
    pub notices: NoticeLog,
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
            // Which conversations have settled, and when to look again for those that have not.
            // Decided per conversation because the rule differs per conversation: a chat on a
            // platform that reports typing waits for the person to stop, and one on a platform
            // that cannot report it waits only for the wire.
            let Readiness { ready, retry_in } = readiness(&context).await;
            if ready.is_empty() {
                let Some(delay) = retry_in else {
                    break;
                };
                // Woken as well as timed. The delay is how long the *soonest* conversation needs,
                // which under a typing hold can be the whole TTL, and without this branch a message
                // arriving in any other conversation would sit unlooked-at for that long. That is
                // the same one-chat-holds-another fault that splitting readiness per conversation
                // was meant to end.
                tokio::select! {
                    () = shutdown.cancelled() => return,
                    () = wake.notified() => {}
                    () = tokio::time::sleep(delay) => {}
                }
                // Round again rather than claiming straight away: more may have arrived while
                // waiting, which starts the quiet period afresh.
                continue;
            }
            let batch = match context
                .store
                .claim_batch(&ready, context.config.bridge.batch_max_messages)
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

/// What the drain loop should do this round.
struct Readiness {
    /// Conversations whose waiting messages may be claimed now.
    ready: Vec<String>,
    /// How long until the soonest unready conversation could become ready, or `None` when nothing
    /// is waiting at all. Only meaningful when `ready` is empty, since otherwise the loop claims
    /// and comes straight back.
    retry_in: Option<Duration>,
}

/// Decide which conversations have settled, and when to look again at those that have not.
///
/// The quiet period only applies where the platform reports typing, because without that signal it
/// is a guess, and a guess long enough to catch somebody composing is far too long to impose on
/// somebody who only meant to send one message.
///
/// Timestamps are the platform's own send times, not when the row was written, so a backlog
/// replayed after a restart releases immediately instead of being debounced as though it had just
/// arrived. The cost is a dependence on the two clocks roughly agreeing, and both directions of
/// skew degrade into either no debounce or one full quiet period.
async fn readiness(context: &DrainContext) -> Readiness {
    let windows = match context.store.pending_windows().await {
        Ok(windows) => windows,
        Err(error) => {
            // Delivering promptly beats stalling on a query the next round will retry anyway.
            tracing::error!("could not read the pending windows: {}", error);
            return Readiness {
                ready: Vec::new(),
                retry_in: Some(DRAIN_POLL_INTERVAL),
            };
        }
    };
    if windows.is_empty() {
        return Readiness {
            ready: Vec::new(),
            retry_in: None,
        };
    }

    let floor = context.config.bridge.coalesce_floor;
    let settle = context.config.bridge.settle;
    let settle_max = context.config.bridge.settle_max;
    let now = chrono::Utc::now();
    // Clamped because a platform clock ahead of this host's would otherwise read as negative.
    let elapsed =
        |since: chrono::DateTime<chrono::Utc>| (now - since).to_std().unwrap_or(Duration::ZERO);

    // Every conversation below either becomes ready or sets `soonest`, and an empty queue returned
    // above, so the caller is never handed nothing to do and no time to do it at.
    let mut ready = Vec::new();
    let mut soonest: Option<Duration> = None;
    for window in windows {
        // Checked ahead of everything below, the ceiling included. `settle_max` bounds how long a
        // person may be made to wait, which is not a reason to ignore what the upstream said about
        // when to come back: a conversation released early here simply fails again and spends
        // another attempt for nothing.
        //
        // Clamped on the way out as well as on the way in. `retry_delay` bounds what gets written,
        // but the value read back is a wall-clock instant, and a host clock stepping backwards --
        // an NTP correction, a restored snapshot -- would otherwise hold the conversation for the
        // whole size of the jump with nothing to overrule it, since the ceiling is skipped here.
        if let Some(not_before) = window.not_before {
            let remaining = (not_before - now)
                .to_std()
                .unwrap_or(Duration::ZERO)
                .min(RETRY_DELAY_MAX);
            if remaining > Duration::ZERO {
                soonest = Some(soonest.map_or(remaining, |soonest| soonest.min(remaining)));
                continue;
            }
        }
        let waited = elapsed(window.oldest);
        // Below a pending deferral, the ceiling wins over everything: a conversation nobody stops
        // typing in cannot hold its own messages forever. It also releases a backlog replayed after
        // downtime at once, since those carry their original send times.
        //
        // The cost is that a conversation whose oldest message is already past the ceiling skips
        // the floor, so a split post arriving with timestamps that old is not held together. That
        // needs a clock skewed by more than the ceiling or a delivery delayed by as much, and the
        // parts still all reach the agent, the later ones flagged `late:`.
        if waited >= settle_max {
            ready.push(window.conversation_id);
            continue;
        }
        let quiet_for = elapsed(window.newest);
        // Under the ceiling the floor always applies. Beyond it a conversation is held while
        // somebody is composing, and for the quiet period after they stop, both only where the
        // platform reports typing: without that signal there is nothing to wait on and any wait is
        // a guess.
        let hold = if !settle.is_zero() && reports_typing(context, &window.conversation_id) {
            if context.typing.active(&window.conversation_id) {
                // Nothing to count down to yet: look again on the next tick, and let the ceiling
                // be what eventually forces the issue.
                let remaining = TYPING_TTL.min(settle_max.saturating_sub(waited));
                soonest = Some(soonest.map_or(remaining, |soonest| soonest.min(remaining)));
                continue;
            }
            floor.max(settle)
        } else {
            floor
        };
        let remaining = hold.saturating_sub(quiet_for);
        if remaining > Duration::ZERO {
            // Never past the ceiling, or a conversation that keeps being typed in would be
            // reported as due later than the moment it is actually released.
            let remaining = remaining.min(settle_max.saturating_sub(waited));
            soonest = Some(soonest.map_or(remaining, |soonest| soonest.min(remaining)));
            continue;
        }
        ready.push(window.conversation_id);
    }
    Readiness {
        ready,
        retry_in: soonest,
    }
}

/// Whether the platform behind this conversation says when somebody is typing.
///
/// A conversation whose channel cannot be resolved is treated as not reporting it, which releases
/// sooner. That is the safe direction: the alternative holds messages for a channel nobody can ask
/// about, and the id came out of the queue rather than from a user, so it should always resolve.
fn reports_typing(context: &DrainContext, conversation_id: &str) -> bool {
    ConversationId::parse(conversation_id)
        .and_then(|conversation| context.channels.get(conversation.channel()).cloned())
        .is_some_and(|channel| channel.capabilities().typing_status)
}

/// Hand one batch to the agent and record what happened to it.
async fn deliver(
    context: &DrainContext,
    batch: Vec<QueuedMessage>,
    last_turn: Option<TurnWindow>,
    shutdown: &CancellationToken,
) -> Option<TurnWindow> {
    let started_at = chrono::Utc::now();
    // How many attempts the most-tried message here has already had, which is what the wait before
    // the next one is scaled from. Read now because the batch is consumed below.
    let attempts = batch
        .iter()
        .map(|message| message.attempts)
        .max()
        .unwrap_or(0);

    let mut events: Vec<InboundEvent> = Vec::with_capacity(batch.len());
    // Only the rows that survive decoding. Taking the whole batch here meant a row discarded just
    // below was still handed to `fail_batch` and `release_batch` later, and neither filters on
    // state, so the row it had just marked `done` was resurrected to `pending` to be discarded
    // again on the next round. It also inflated the count in the notice sent to the owner.
    let mut sequences: Vec<i64> = Vec::with_capacity(batch.len());
    let mut undecodable = Vec::new();
    for message in &batch {
        match serde_json::from_str::<InboundEvent>(&message.payload) {
            Ok(event) => {
                events.push(event);
                sequences.push(message.seq);
            }
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
    let delivered_count = events.len();

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
            // Nothing was submitted, so nothing ran and the batch is intact. Backed off all the
            // same: meka being unreachable now is the best evidence available about whether it will
            // be a moment from now.
            let retry = if error.is_retryable() {
                Retry::After(retry_delay(
                    context.config.bridge.retry_base,
                    attempts,
                    error.retry_after(),
                ))
            } else {
                Retry::Never
            };
            record_failure(context, &sequences, &Failure {
                reason: &error.to_string(),
                retry,
                answered: false,
                delivered: false,
            })
            .await;
            // The counter was taken above and the envelope carrying it is now discarded unbuilt, so
            // put it back or the agent is never told its view is incomplete. Only the paths that
            // give up *before* submitting need this: past that point the envelope reached meka and
            // the count was reported, and restoring it there would report the same overflow twice.
            if dropped > 0
                && let Err(error) = context.store.note_dropped(dropped).await
            {
                tracing::error!("failed to restore the dropped-message counter: {}", error);
            }
            return last_turn;
        }
    };

    let identities = channel_identities(context).await;

    let (missed, withheld) = missed_context(context, &events).await;

    let nonce = nonce();
    // Any row in the batch having been recovered makes the whole envelope uncertain, since the
    // agent cannot tell which of the messages in front of it was the interrupted one.
    let recovered = batch.iter().any(|message| message.recovered);
    let message = Envelope {
        events: &events,
        dropped,
        identities: &identities,
        missed: &missed,
        nonce: &nonce,
        recovered,
    }
    .render();

    tracing::info!(
        messages = events.len(),
        conversations = conversations.len(),
        session_id = %session_id,
        "submitting a turn"
    );

    let mut report = submit(context, session_id, &message, &conversations, shutdown).await;

    // meka forgetting the session (its row was deleted, or the database was replaced) is
    // recoverable exactly once: bind a fresh session and replay the same batch into it. Safe to
    // replay because the rejection happens before anything runs, so the first submission cannot
    // have left work behind.
    //
    // `had_side_effects` is what holds that up, rather than the reasoning alone. This runs ahead of
    // every other arm, so anything that arrives here as a missing session is replayed without the
    // usual question being asked. Today only a refused submission can, and refusals happen before
    // the turn starts. Should meka ever report it from further in -- a rejoin answering that the
    // session went away mid-turn is the obvious candidate -- replaying would hand a second copy to
    // an agent that has no memory of sending the first, and nothing here would have noticed.
    if matches!(&report.outcome, Err(error) if error.is_session_missing())
        && !report.had_side_effects()
        && context.config.session.recreate_on_missing
    {
        tracing::warn!(
            "meka no longer knows session {}; creating a replacement. The agent's memory of \
             earlier conversations is gone. If this keeps happening, check meka's \
             `[serve].delete_on_idle`: with it on, an idle session's row is deleted rather than \
             merely evicted, which wipes the assistant every idle_timeout.",
            session_id
        );
        if let Err(error) = context.store.clear_session_id().await {
            tracing::error!("failed to clear the stale session binding: {}", error);
        }
        report = match ensure_session(context).await {
            Ok(replacement) => {
                let message = Envelope {
                    events: &events,
                    dropped,
                    identities: &identities,
                    // Reused rather than recomputed: taking it again would come back empty, because
                    // the first call marked it seen.
                    missed: &missed,
                    nonce: &nonce,
                    recovered,
                }
                .render();
                submit(context, replacement, &message, &conversations, shutdown).await
            }
            Err(error) => TurnReport::failed(error),
        };
    }

    // meka spent a whole turn budget refusing, having retried throughout `submit`, so this is a
    // session that is wedged rather than merely occupied. Every submission was refused before it
    // ran, so nothing reached the agent and nothing was lost: the batch goes back to the queue
    // untouched and the next drain tick starts a fresh round of retries. Counting it as a failed
    // delivery would let a busy session burn the retry budget and eventually declare a message
    // undeliverable that meka never even saw.
    if matches!(&report.outcome, Err(error) if error.is_turn_in_flight()) {
        tracing::warn!(
            "meka has been busy with a turn this bridge did not start for a full turn budget; \
             requeueing the batch"
        );
        if let Err(error) = context.store.release_batch(&sequences).await {
            tracing::error!("failed to requeue a deferred batch: {}", error);
        }
        // The envelope this counter was rendered into is being thrown away, so put it back or the
        // notice that the queue overflowed is lost to a turn that never ran.
        if dropped > 0
            && let Err(error) = context.store.note_dropped(dropped).await
        {
            tracing::error!("failed to restore the dropped-message counter: {}", error);
        }
        // The backlog this envelope reported is deliberately *not* marked accounted for. It was
        // never delivered, and spending it here would leave the retry telling a chat with thirty
        // messages waiting that nothing had been said in it.
        //
        // No turn ran, so there is no window to report either. Inventing one would flag the next
        // batch's messages as having arrived mid-turn against a turn that never happened, and those
        // messages are in that very batch.
        return last_turn;
    }

    // Spent for every outcome except a refusal. A turn that failed still reached the agent, and
    // repeating the whole backlog on each retry would be the worse trade; a turn meka never
    // accepted did not reach it at all.
    //
    // Keyed on whether the turn was taken, not on which error came back. The `turn-in-flight` 409
    // used to be the only refusal excepted, and it is far from the only one: a meka restarting
    // refuses the socket, its concurrency limit answers 429, a rotated token answers 401. Each of
    // those spent a backlog nobody was shown, leaving the retry to tell the agent nothing had been
    // said in a chat with thirty messages waiting, and those messages gone from every path that
    // would have found them again.
    if !report.accepted {
        // The counter is restored for the same reason and on the same condition: the envelope it
        // was rendered into never reached anybody.
        if dropped > 0
            && let Err(error) = context.store.note_dropped(dropped).await
        {
            tracing::error!("failed to restore the dropped-message counter: {}", error);
        }
    }
    for spent in withheld.iter().filter(|_| report.accepted) {
        if let Err(error) = context
            .store
            .mark_seen(spent.conversation.as_str(), spent.watermark, spent.through)
            .await
        {
            tracing::error!(
                conversation = %spent.conversation,
                "failed to record what the agent was shown: {}",
                error
            );
        }
    }

    // Why the turn stopped, when it was cancelled rather than finished or failed. Kept out of
    // `failure` because it is not an error meka reported: nothing went wrong at the HTTP layer, the
    // work simply did not run to the end.
    let mut cancelled: Option<String> = None;
    let failure = match &report.outcome {
        Ok(TurnOutcome::Finished { stop_reason, .. }) => {
            tracing::info!(
                stop_reason = %stop_reason,
                sends = report.sends,
                tool_calls = report.tool_calls,
                text_chars = report.text_length,
                "turn finished"
            );
            None
        }
        // Not a success, and treating it as one is the same mistake as reading an idle session as a
        // finished turn. meka cancels a turn whose stream has had no subscriber for
        // `[serve].stream_reattach_grace`, reporting `client`, so this is what a rejoin that lands
        // after the kill gets back: a turn stopped partway through work nobody has seen. At
        // `stream_reattach_grace = "0s"`, which meka offers to restore its older behaviour, every
        // dropped stream ends here.
        //
        // The batch is therefore treated as undelivered. `had_side_effects` still applies below and
        // is what stops a cancellation that interrupted a half-finished send from being replayed.
        Ok(TurnOutcome::Cancelled { reason }) => {
            tracing::warn!(reason = %reason, "turn cancelled; its batch was not delivered");
            cancelled = Some(reason.clone());
            None
        }
        Err(error) => Some(error),
    };

    match failure {
        // A turn that produced meka's empty-response stand-in and called nothing did no work at
        // all: no message was sent, no tool ran. Handing the batch over again is therefore free of
        // side effects, and far better than leaving somebody who just messaged the bot in silence.
        // At once, too, rather than after a wait. Nothing upstream asked to be left alone here: an
        // empty response is the model having produced nothing, not a service saying it is busy.
        None if report.produced_nothing() => {
            tracing::warn!(
                text = %report.text_preview.trim(),
                "the model returned an empty response and called no tools; retrying the batch"
            );
            record_failure(context, &sequences, &Failure {
                reason: "the model returned an empty response",
                retry: Retry::Now,
                answered: false,
                delivered: false,
            })
            .await;
        }
        // Stopped partway. The batch is spent if the agent had already acted, for the same reason a
        // failure after side effects is, and owed back to the queue otherwise.
        None if cancelled.is_some() => {
            let reason = cancelled.unwrap_or_default();
            if report.had_side_effects() {
                tracing::error!(
                    sends = report.sends,
                    tool_calls = report.tool_calls,
                    "the turn was cancelled ({reason}) after the agent had already acted, so its \
                     batch will not be retried"
                );
                complete(context, &sequences).await;
                announce(
                    context,
                    &conversations,
                    &Failure {
                        reason: &format!("the turn was cancelled: {reason}"),
                        retry: Retry::Never,
                        answered: report.sends > 0,
                        delivered: true,
                    },
                    Lost {
                        count: delivered_count,
                        attempts: attempts + 1,
                        recoverable: false,
                    },
                )
                .await;
            } else {
                tracing::warn!("the turn was cancelled ({reason}) having done nothing; requeueing");
                record_failure(context, &sequences, &Failure {
                    reason: &format!("the turn was cancelled: {reason}"),
                    retry: Retry::After(retry_delay(
                        context.config.bridge.retry_base,
                        attempts,
                        None,
                    )),
                    answered: false,
                    delivered: false,
                })
                .await;
            }
        }
        None => {
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
            complete(context, &sequences).await;
        }
        // Must stay ahead of the lost-stream arm below: every dropped stream satisfies
        // `turn_outcome_unknown`, so behind it this guard is unreachable and a turn that had
        // already answered somebody gets handed back to be answered again. The counters are
        // truncated by a drop, so whatever they do show is a floor on what the agent did.
        //
        // Reachable rather than theoretical because meka scopes `content_started` to a single
        // provider call, so a rate limit partway through a tool loop arrives here with the earlier
        // iterations' calls already made.
        Some(error) if report.had_side_effects() => {
            tracing::error!(
                sends = report.sends,
                tool_calls = report.tool_calls,
                "the turn failed after the agent had already acted, so its batch will not be \
                 retried: {}",
                error
            );
            complete(context, &sequences).await;
            announce(
                context,
                &conversations,
                &Failure {
                    reason: &error.to_string(),
                    retry: Retry::Never,
                    answered: report.sends > 0,
                    delivered: true,
                },
                Lost {
                    count: delivered_count,
                    attempts: attempts + 1,
                    // Nothing was un-seen: the messages reached the agent, so they are accounted
                    // for rather than owed.
                    recoverable: false,
                },
            )
            .await;
        }
        // The turn was accepted, the stream died, and `run_turn` could not rejoin it. What the turn
        // did is genuinely unknown, and there is no longer any way to find out: a session that has
        // gone idle since proves nothing, because meka stops a turn whose stream has had no
        // subscriber for `[serve].stream_reattach_grace`. Reading idle as "it finished" is how a
        // message gets marked delivered that was never answered, so the batch is requeued instead.
        //
        // The session is not polled for idle first. That wait used to decide the outcome; now that
        // it cannot, it is a stall of up to a whole turn budget before the retry, buying nothing.
        // Should the old turn somehow still be going, the resubmission is refused with a 409 and
        // `submit` holds the batch on a two-second timer, which is the same waiting done properly.
        Some(error) if error.turn_outcome_unknown() => {
            tracing::error!(
                sends = report.sends,
                tool_calls = report.tool_calls,
                "lost the turn stream and could not rejoin it ({}); requeueing the batch, which \
                 may deliver the same messages twice",
                error
            );
            record_failure(context, &sequences, &Failure {
                reason: &error.to_string(),
                retry: Retry::After(retry_delay(
                    context.config.bridge.retry_base,
                    attempts,
                    error.retry_after(),
                )),
                // Nothing was sent, or the arm above would have taken this. Saying otherwise
                // would suppress the chat's notice while the owner's said the message was owed
                // back, which cannot both be true.
                answered: false,
                delivered: false,
            })
            .await;
        }
        Some(error) => {
            // Only the wording differs, deliberately: a context overflow is already non-retryable
            // below, so an arm of its own would duplicate everything under it to change one line.
            // Worth saying differently because it is the one failure here that is not about the
            // message that hit it. One permanent session carries every conversation, so a window
            // it has outgrown refuses the next message too.
            if error.is_context_overflow() {
                tracing::error!(
                    "the agent's session no longer fits the model's context window, so every \
                     message will fail until it is shortened: compact or rewind it on meka's \
                     side, or `mekabridge session reset --yes` to start a new one and lose what \
                     it remembers ({})",
                    error
                );
            } else {
                tracing::error!("turn failed: {}", error);
            }
            // A failure needing an operator will not stop happening on its own, so spending the
            // whole budget on it only delays the notice that says so.
            let retry = if error.is_retryable() {
                Retry::After(retry_delay(
                    context.config.bridge.retry_base,
                    attempts,
                    error.retry_after(),
                ))
            } else {
                Retry::Never
            };
            record_failure(context, &sequences, &Failure {
                reason: &error.to_string(),
                retry,
                answered: false,
                delivered: false,
            })
            .await;
        }
    }

    Some(TurnWindow {
        started_at,
        ended_at: chrono::Utc::now(),
    })
}

/// Mark a batch delivered and note when the turn that carried it ended.
async fn complete(context: &DrainContext, sequences: &[i64]) {
    if let Err(error) = context.store.complete_batch(sequences).await {
        tracing::error!("failed to mark a delivered batch: {}", error);
    }
    if let Err(error) = context.store.mark_turn_completed(chrono::Utc::now()).await {
        tracing::error!("failed to record the turn timestamp: {}", error);
    }
}

/// Submit a turn, retrying on a timer while meka is busy with a turn of its own.
///
/// A `turn-in-flight` rejection refuses the batch before it runs, so retrying delivers it exactly
/// once, and it stays claimed throughout so the envelope is not rebuilt per attempt.
///
/// The 409 is the only trustworthy sign that the session is busy; meka's `turn_in_flight` is not,
/// and asking it is worse than not asking. Its scheduled work locks the session's runtime before
/// marking itself busy, so in the window between the two the session calls itself idle and refuses
/// anyway, turning a wait-for-idle retry into a spin as tight as the two processes can trade
/// requests.
async fn submit(
    context: &DrainContext,
    session_id: Uuid,
    message: &str,
    conversations: &BTreeSet<ConversationId>,
    shutdown: &CancellationToken,
) -> TurnReport {
    let deadline = tokio::time::Instant::now() + context.config.meka.turn_timeout;
    let mut attempt = 0_u32;
    loop {
        let report = context.runner.run(session_id, message, conversations).await;
        let refused = report
            .outcome
            .as_ref()
            .err()
            .is_some_and(MekaError::is_turn_in_flight);
        // Belt and braces rather than load-bearing: the same budget bounds the turn itself, so a
        // 409 can only ever be seen strictly before the deadline -- a refusal that took longer
        // would have failed as a timeout and never reached this path. The guard costs one
        // comparison and keeps the loop correct if those two budgets are ever separated.
        if !refused || (attempt > 0 && tokio::time::Instant::now() >= deadline) {
            return report;
        }
        if attempt == 0 {
            // Once per episode, not once per attempt: at one line per attempt this buried whatever
            // an operator opened the log to find.
            //
            // Nothing is shown in the chat while this runs. The indicator used to go up here, on
            // the reasoning that meka refuses only because it is running some turn and the agent is
            // therefore working. True, and beside the point now that the indicator means the model
            // is writing a message to this chat specifically: somebody else's turn is the furthest
            // thing from that, and a chat waiting on one is genuinely being left alone.
            tracing::info!(
                "meka is running a turn this bridge did not start; holding the batch and retrying \
                 every {}s until it finishes",
                DEFER_RETRY_INTERVAL.as_secs()
            );
        }
        attempt += 1;

        let interrupted = tokio::select! {
            () = shutdown.cancelled() => true,
            () = tokio::time::sleep(DEFER_RETRY_INTERVAL) => false,
        };
        if interrupted {
            return report;
        }
    }
}

/// What should happen to a batch whose turn did not deliver it.
struct Failure<'a> {
    /// Recorded against the rows and quoted to the owner verbatim.
    reason: &'a str,
    /// Whether it is worth handing over again, and when.
    retry: Retry,
    /// Whether the agent got a word in anywhere before this went wrong.
    ///
    /// A chat that was answered is not owed an apology, and one sent anyway would contradict what
    /// the agent had just said in it. Whether *this* chat was answered would be the sharper
    /// question, and the answer is deliberately not asked for: the only record of it is the turn
    /// runner's presence state, which is meaningful solely after a turn actually ran, and half the
    /// callers here are failures where none did. Erring toward silence costs a chat in a
    /// multi-conversation batch its apology; erring the other way apologises to somebody who was
    /// just answered.
    answered: bool,
    /// Whether the messages reached the agent despite this.
    ///
    /// Changes the whole of what the owner is looking at. Messages that never arrived are lost and
    /// somebody has to decide what to do about them; messages the agent read and acted on before
    /// the turn died are not lost, but whatever it was in the middle of is half done.
    delivered: bool,
}

/// Whether a failed batch is worth another attempt, and how long to leave it first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Retry {
    /// Offer it again once this has passed.
    After(Duration),
    /// Offer it again on the next pass, for a failure that did nothing and says nothing about when
    /// it might stop happening.
    Now,
    /// Do not offer it again. For a failure that needs an operator, where further attempts only
    /// delay the notice saying so.
    Never,
}

/// How long a batch waits before it is offered again.
///
/// `attempts` is how many it has already had, so the first failure waits `base` and each one after
/// that twice as long. Exponential rather than fixed because the failure this exists for is an
/// upstream out of quota, where the right wait is unknowable and the cost of guessing short is an
/// attempt spent for nothing.
///
/// A `Retry-After` raises the wait but never lowers it. Letting it ask for *shorter* would undo the
/// point: meka's concurrency-limit 429 carries a flat one second, which taken literally puts all
/// four attempts inside four seconds.
fn retry_delay(base: Duration, attempts: u32, retry_after: Option<Duration>) -> Duration {
    // Capped exponent as well as capped result: `Duration * u32` panics on overflow, and this is
    // reached with whatever attempt count a database row happens to hold.
    let scheduled = base * 2_u32.saturating_pow(attempts.min(8));
    retry_after
        .map_or(scheduled, |asked| asked.max(scheduled))
        .min(RETRY_DELAY_MAX)
}

/// Mark a batch failed, and tell whoever is waiting about anything that will never be delivered.
async fn record_failure(context: &DrainContext, sequences: &[i64], failure: &Failure<'_>) {
    let (max_attempts, retry_at) = match failure.retry {
        Retry::After(delay) => (
            context.config.bridge.turn_retries,
            chrono::Duration::from_std(delay)
                .ok()
                .map(|delay| chrono::Utc::now() + delay),
        ),
        Retry::Now => (context.config.bridge.turn_retries, None),
        Retry::Never => (0, None),
    };
    let outcome = context
        .store
        .fail_batch(sequences, failure.reason, max_attempts, retry_at)
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
            retry_in = ?retry_at.map(|at| at - chrono::Utc::now()),
            "requeued messages after a failed turn"
        );
    }
    if outcome.exhausted.is_empty() {
        return;
    }
    // The row's own counter rather than `max_attempts`, which a permanent error forces to zero and
    // which would then report a message that had already been tried four times as having had one.
    let spent = outcome
        .exhausted
        .iter()
        .map(|message| message.attempts)
        .max()
        .unwrap_or(1);
    tracing::error!(
        count = outcome.exhausted.len(),
        "giving up on {} message(s) after {spent} attempt(s)",
        outcome.exhausted.len()
    );

    // Owed to the agent again. A message is marked seen the moment it is queued, on the assumption
    // that the agent is about to be handed it, and this is exactly where that assumption fails.
    // Without this it is neither delivered nor owed, so nothing but `read_history` over the right
    // window would ever turn it up again.
    let mut affected: BTreeSet<ConversationId> = BTreeSet::new();
    // Whether that actually worked, which it cannot when `[storage].history_retention` is zero:
    // there is no history row to un-see, so the message really is gone. The owner is told which of
    // the two happened rather than promised the recoverable one.
    let mut recoverable = false;
    for message in &outcome.exhausted {
        match context
            .store
            .mark_unseen(&message.conversation_id, &message.external_id)
            .await
        {
            Ok(marked) => recoverable |= marked,
            Err(error) => tracing::error!(
                conversation = %message.conversation_id,
                "failed to mark an undeliverable message unseen: {}",
                error
            ),
        }
        if let Some(conversation) = ConversationId::parse(&message.conversation_id) {
            affected.insert(conversation);
        }
    }
    announce(context, &affected, failure, Lost {
        count: outcome.exhausted.len(),
        attempts: spent,
        recoverable,
    })
    .await;
}

/// What became of the messages a notice is about.
struct Lost {
    count: usize,
    /// Attempts actually spent, which is not `turn_retries + 1` when a permanent error cut it
    /// short.
    attempts: u32,
    /// Whether they can still be reached, which needs a history to have been kept.
    recoverable: bool,
}

/// Tell the chats that were waiting, and the owner, that something did not get through.
///
/// Deliberately two different messages. The chat gets no detail at all: whoever is in it did
/// nothing wrong, cannot act on an upstream status code, and may not be somebody an operator would
/// hand a stack trace to. The owner gets everything, because the owner is the one who can fix it.
async fn announce(
    context: &DrainContext,
    affected: &BTreeSet<ConversationId>,
    failure: &Failure<'_>,
    lost: Lost,
) {
    let configured = context.config.bridge.owner_conversation.as_deref();
    let owner = configured.and_then(ConversationId::parse);
    if let Some(configured) = configured
        && owner.is_none()
    {
        // Startup validation only checks the channel prefix, so an id missing its chat segment gets
        // this far. Silence here would drop every operator notice this bridge ever tries to send
        // and leave nothing anywhere saying why.
        tracing::error!("[bridge].owner_conversation {configured:?} is not a valid id");
    }

    let mut told = Vec::new();
    if context.config.bridge.notify_failures && !failure.answered {
        for conversation in affected {
            // The owner hears the detailed version below, and hearing both would be one message
            // apologising for what the next one goes on to explain.
            if owner.as_ref() == Some(conversation) {
                continue;
            }
            if context.notices.due(conversation.as_str()).is_none() {
                continue;
            }
            // Which of the two happened matters to the person waiting. A message that never
            // reached the agent is worth sending again; one it read and acted on before the turn
            // died is not, and telling them it never arrived would have them repeat something that
            // may be half done.
            let text = if failure.delivered {
                "Something went wrong on my end while I was working on your message, so I may not \
                 have finished. Best to check before asking again."
            } else {
                "Something went wrong on my end and I could not get to your message. Please try \
                 again in a little while."
            };
            notify(context, conversation, text).await;
            told.push(conversation.as_str().to_string());
        }
    }

    let Some(owner) = owner else {
        return;
    };
    let Some(suppressed) = context.notices.due(owner.as_str()) else {
        return;
    };
    let count = lost.count;
    let mut text = if failure.delivered {
        format!(
            "mekabridge: a turn carrying {count} message(s) failed after the agent had already \
             acted, so whatever it was doing is half finished. The messages themselves are \
             accounted for and were not sent again, since a second run would repeat what the first \
             one did.\n\nError: {}",
            failure.reason
        )
    } else if lost.recoverable {
        format!(
            "mekabridge could not deliver {count} message(s) to the agent. They are back among \
             what it has not seen, so `unseen` counts them and they will be offered as context the \
             next time one of these chats wakes it.\n\nLast error: {}",
            failure.reason
        )
    } else {
        // `[storage].history_retention = "0s"` writes no history at all, so there is no row to put
        // back and nothing will ever surface these again. That is the one configuration where a
        // message is genuinely lost, and it is worth saying plainly rather than promising a
        // recovery that cannot happen.
        format!(
            "mekabridge could not deliver {count} message(s) to the agent, and no history is being \
             kept, so nothing will bring them back. Only the platform's own scrollback still has \
             them.\n\nLast error: {}",
            failure.reason
        )
    };
    if !affected.is_empty() {
        let ids: Vec<&str> = affected
            .iter()
            .map(crate::channel::ConversationId::as_str)
            .collect();
        text.push_str(&format!("\nConversations: {}", ids.join(", ")));
    }
    if !failure.delivered {
        text.push_str(&match failure.retry {
            Retry::Never => {
                "\nGiven up on at once: this one needs you rather than another attempt.".to_string()
            }
            Retry::Now | Retry::After(_) => {
                format!("\nTried {} time(s) before giving up.", lost.attempts)
            }
        });
    }
    if told.is_empty() {
        text.push_str("\nNobody was told in the chats themselves.");
    } else {
        text.push_str(&format!("\nApologised in: {}", told.join(", ")));
    }
    if suppressed > 0 {
        text.push_str(&format!(
            "\n{suppressed} further failure(s) since the last notice were not reported."
        ));
    }
    notify(context, &owner, &text).await;
}

/// Write one line of the bridge's own into a conversation.
///
/// The only place the bridge does that. It is strictly about the bridge being broken, never about
/// what anybody said, so it goes to the platform directly rather than through the sink: it is not
/// the agent speaking and must not stamp the conversation as answered. It is still recorded, under
/// no session, because the chat can see it and a record that omits it would leave the agent reading
/// replies to something it has no trace of.
async fn notify(context: &DrainContext, conversation: &ConversationId, text: &str) {
    let channel = match context.channels.resolve(conversation) {
        Ok(channel) => channel,
        Err(error) => {
            tracing::error!(conversation = %conversation, "cannot reach it to say so: {}", error);
            return;
        }
    };
    let mut sent = Vec::new();
    if let Err(error) = channel
        .send_text(
            conversation,
            text,
            &crate::channel::SendOptions::default(),
            &mut sent,
        )
        .await
    {
        tracing::error!(conversation = %conversation, "failed to say so: {}", error);
    }
    // Recorded like anything else the bridge puts in a chat. The session is `None`, which is what
    // distinguishes it: the row says the account spoke and names no session behind it, so the agent
    // reading the conversation back finds the notice where the people it was sent to see it,
    // instead of finding replies to something it has no record of.
    super::record_own_messages(
        &context.store,
        context.config.storage.history_retention,
        conversation,
        channel.id(),
        None,
        sent,
        None,
    )
    .await;
}

/// When each conversation was last told that something failed.
///
/// In memory only, because after a restart telling somebody again is the right answer rather than
/// the wrong one. Owned outright by the drain loop rather than shared behind an `Arc` the way
/// [`TypingState`] is, since nothing else here writes it; the mutex is for interior mutability
/// through the `&DrainContext` every function in this file takes.
///
/// Without it a provider out of quota for half an hour writes an apology into every affected chat
/// every time a batch runs out of attempts, which is a worse experience than the silence it
/// replaced.
#[derive(Debug, Default)]
pub struct NoticeLog {
    entries: std::sync::Mutex<std::collections::HashMap<String, Notice>>,
}

#[derive(Debug)]
struct Notice {
    /// When this conversation was last told, or `None` when it never has been.
    at: Option<chrono::DateTime<chrono::Utc>>,
    /// Failures inside the window that were not reported.
    suppressed: u64,
}

impl NoticeLog {
    /// Whether this conversation may be told now, and how many failures went unreported since it
    /// last was.
    ///
    /// `None` means it was told recently enough. The count matters for the owner and not for the
    /// chats: one message about one failure reads like a blip, and an upstream out of quota for an
    /// hour is not one.
    fn due(&self, conversation_id: &str) -> Option<u64> {
        let now = chrono::Utc::now();
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // Bounded by emptying it, the same rule the store's policy cache uses. An entry matters
        // only for the length of the window, and losing one means somebody is told twice rather
        // than not at all.
        if entries.len() >= NOTICE_LOG_MAX {
            entries.clear();
        }
        let notice = entries
            .entry(conversation_id.to_string())
            .or_insert(Notice {
                at: None,
                suppressed: 0,
            });
        let recent = notice.at.is_some_and(|at| {
            // A negative interval means the clock went backwards, which reads here as not recent
            // and so tells somebody again. The alternative is silence for however far it jumped.
            (now - at)
                .to_std()
                .is_ok_and(|since| since < NOTICE_INTERVAL)
        });
        if recent {
            notice.suppressed += 1;
            return None;
        }
        notice.at = Some(now);
        Some(std::mem::take(&mut notice.suppressed))
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
            width: None,
            height: None,
            duration_secs: None,
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

    fn lapsed(policy: Policy, dropped: u64) -> PolicyRecord {
        PolicyRecord {
            conversation_id: "telegram:1".to_string(),
            policy,
            until: Some(Utc::now()),
            reason: None,
            dropped,
            created_at: Utc::now(),
        }
    }

    #[test]
    fn a_listening_window_closing_is_announced() {
        // The agent asked to hear a room in full and has now stopped. Nothing was withheld, so
        // there is no backlog to hint at it, and from the inside a room it can no longer hear is
        // indistinguishable from a room that went quiet.
        let mut message = event_with(Vec::new());
        announce_expiry(&mut message, &lapsed(Policy::Active, 0), Policy::Mute);
        assert_eq!(message.notes.len(), 1, "got {:?}", message.notes);
        assert!(
            message.notes[0].contains("hearing this chat in full")
                && message.notes[0].contains("mentions only"),
            "got {:?}",
            message.notes
        );
    }

    #[test]
    fn a_listening_window_closing_onto_a_room_already_heard_in_full_says_nothing() {
        // Where the default for the kind is `active`, the expiry changed nothing anyone can
        // observe, and "you now hear this in full" would read as news.
        let mut message = event_with(Vec::new());
        announce_expiry(&mut message, &lapsed(Policy::Active, 0), Policy::Active);
        assert!(message.notes.is_empty(), "got {:?}", message.notes);
    }

    #[test]
    fn a_lapsed_mute_points_at_what_can_still_be_read() {
        // The distinction from a block is the whole of the note: under a mute the messages are
        // still there, and without being told the agent assumes they went the same way.
        let mut message = event_with(Vec::new());
        announce_expiry(&mut message, &lapsed(Policy::Mute, 0), Policy::Mute);
        assert!(
            message.notes[0].contains("read_history"),
            "got {:?}",
            message.notes
        );
    }

    #[test]
    fn a_lapsed_block_admits_what_cannot_be_recovered() {
        let mut message = event_with(Vec::new());
        announce_expiry(&mut message, &lapsed(Policy::Block, 4), Policy::Mute);
        assert!(
            message.notes[0].contains("cannot be recovered"),
            "got {:?}",
            message.notes
        );
    }

    #[test]
    fn an_indefinite_decision_being_lifted_announces_nothing() {
        // There is no expiry to report, because nothing expired: the agent lifted it by hand and
        // already knows.
        let mut message = event_with(Vec::new());
        let mut record = lapsed(Policy::Mute, 0);
        record.until = None;
        announce_expiry(&mut message, &record, Policy::Mute);
        assert!(message.notes.is_empty(), "got {:?}", message.notes);
    }

    #[test]
    fn the_wait_between_attempts_grows_and_is_bounded() {
        let base = Duration::from_secs(10);
        assert_eq!(retry_delay(base, 0, None), Duration::from_secs(10));
        assert_eq!(retry_delay(base, 1, None), Duration::from_secs(20));
        assert_eq!(retry_delay(base, 2, None), Duration::from_secs(40));
        assert_eq!(retry_delay(base, 3, None), Duration::from_secs(80));
        // Bounded rather than doubling forever, and bounded without panicking: this is reached with
        // whatever attempt count a row in the database happens to carry, and `Duration * u32`
        // panics on overflow rather than saturating.
        assert_eq!(retry_delay(base, 4, None), RETRY_DELAY_MAX);
        assert_eq!(retry_delay(base, u32::MAX, None), RETRY_DELAY_MAX);
    }

    #[test]
    fn a_retry_after_can_lengthen_the_wait_but_never_shorten_it() {
        let base = Duration::from_secs(10);
        // Longer than the schedule: honoured, because the upstream knows when it will be ready and
        // this does not.
        assert_eq!(
            retry_delay(base, 0, Some(Duration::from_secs(45))),
            Duration::from_secs(45)
        );
        // Shorter: ignored. meka attaches a flat one second to its concurrency-limit 429, and
        // taking that literally on the fourth attempt would put the whole budget inside a few
        // seconds, which is the failure this backoff exists to prevent arriving by another road.
        assert_eq!(
            retry_delay(base, 3, Some(Duration::from_secs(1))),
            Duration::from_secs(80)
        );
        // An upstream is entitled to ask for an hour. Waiting it out would hold a conversation open
        // for an hour with nobody told, which is worse than saying promptly that it did not work.
        assert_eq!(
            retry_delay(base, 0, Some(Duration::from_secs(3600))),
            RETRY_DELAY_MAX
        );
    }

    #[test]
    fn a_conversation_is_told_once_and_then_left_alone() {
        let log = NoticeLog::default();
        assert_eq!(
            log.due("telegram:1"),
            Some(0),
            "a chat nothing has been said to yet is due at once"
        );
        assert_eq!(log.due("telegram:1"), None, "and not again straight after");
        assert_eq!(log.due("telegram:1"), None);
        // A different conversation is a different decision: one chat being told does not silence
        // the rest, which is what a shared timer would do.
        assert_eq!(log.due("telegram:2"), Some(0));
    }

    #[test]
    fn what_went_unreported_is_counted_for_the_next_notice() {
        // Without this the owner reads one message about one failure, which is what a blip looks
        // like, when what happened was an upstream out of quota for half an hour.
        let log = NoticeLog::default();
        assert_eq!(log.due("telegram:1"), Some(0));
        for _ in 0..5 {
            assert_eq!(log.due("telegram:1"), None);
        }
        // Reaching back past the window rather than sleeping through it.
        {
            let mut entries = log
                .entries
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let notice = entries.get_mut("telegram:1").expect("recorded");
            notice.at = Some(
                Utc::now() - chrono::Duration::from_std(NOTICE_INTERVAL * 2).expect("in range"),
            );
        }
        assert_eq!(log.due("telegram:1"), Some(5));
        // And the tally starts again, so the next notice does not re-report what this one just did.
        {
            let mut entries = log
                .entries
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let notice = entries.get_mut("telegram:1").expect("recorded");
            notice.at = Some(
                Utc::now() - chrono::Duration::from_std(NOTICE_INTERVAL * 2).expect("in range"),
            );
        }
        assert_eq!(log.due("telegram:1"), Some(0));
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
