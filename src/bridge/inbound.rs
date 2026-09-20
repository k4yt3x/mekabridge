//! The inbound path: channel events into the durable queue, and the queue into meka's inbox.
//!
//! Two tasks, deliberately separate: the writer persists every event before acknowledging it, and
//! the session task claims messages, hands each to meka as an inbox item of its own, and acts on
//! what the session feed says became of them. One session task means the bridge never races itself
//! on a row's state.
//!
//! Messages that settle together are claimed together and handed over one after another, which is
//! what makes somebody who puts their phone down cost one provider round trip rather than several:
//! meka reads them all into one turn, under a header of its own per item. What arrives while the
//! agent is working reaches it inside that turn, at its next round boundary; nothing waits for a
//! turn to end.

use std::{
    collections::{BTreeSet, HashMap},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use chrono::{DateTime, Utc};
use tokio::sync::{Notify, mpsc, watch};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    bridge::{
        ChannelAccounts,
        envelope::{Item, MissedContext, MissedMessage},
        feed::FeedEvent,
        turn::{TurnTally, TurnTypist, notice_reports_lost_events},
    },
    channel::{ChannelRegistry, ConversationId, InboundEvent, InboundMessage},
    config::Config,
    meka::{
        InboxState, MekaClient, MekaError, ProblemDetail, StreamItem,
        sse::{TurnEvent, TurnSource},
    },
    store::{
        AccountId, Backlog, ConversationRecord, EnqueueOutcome, HandOver, MessageKey, Offer,
        Policy, PolicyRecord, QueueState, QueuedMessage, Recovered, Store, WatchRecord,
    },
    watch::Watchlist,
};

/// Safety-net poll interval. The writer notifies the session task directly, so this only covers
/// rows that became eligible without an enqueue, such as a released row returning to `pending`.
const DRAIN_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Ceiling on the wait between posts of a batch meka did not accept, and on a `Retry-After` the
/// upstream asks for.
///
/// The same five minutes meka's own inbox retries cap at. A provider is entitled to ask for an
/// hour; honouring that would hold a batch for the whole of the hour it has to be accepted in,
/// with nobody told.
const RETRY_DELAY_MAX: Duration = Duration::from_secs(5 * 60);

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
/// floor alone is the right answer. Written by the inbound writer and read by the session task,
/// which already share state this way.
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

/// The watches in force, compiled, and the rows they were compiled from.
///
/// Held across messages because compiling is the expensive half and the rows almost never change:
/// [`Store::watches`] hands back the same allocation while its cache holds and while a refetch
/// finds nothing different, so the pointer comparison here is what turns "re-read the watches on
/// every message" into "recompile them when the agent actually changes one".
#[derive(Default)]
struct Watches {
    held: Option<(Arc<Vec<WatchRecord>>, Watchlist)>,
}

impl Watches {
    /// The current watchlist, or `None` when nothing is being watched.
    ///
    /// A store that cannot answer yields `None`, which withholds the message rather than
    /// delivering it. That is the opposite of how [`gate`] fails on a policy it cannot read, and
    /// deliberately: failing open on a policy costs an unwanted turn, while failing open here is
    /// not available at all, since without the rules there is nothing to decide with.
    async fn current(&mut self, store: &Store, now: DateTime<Utc>) -> Option<&Watchlist> {
        let rows = match store.watches(now).await {
            Ok(rows) => rows,
            Err(error) => {
                tracing::error!(
                    "could not read the watches, so nothing will wake a muted chat but a mention: \
                     {}",
                    error
                );
                return None;
            }
        };
        let stale = !matches!(&self.held, Some((held, _)) if Arc::ptr_eq(held, &rows));
        if stale {
            let list = Watchlist::new(rows.as_ref().clone());
            self.held = Some((rows, list));
        }
        self.held
            .as_ref()
            .map(|(_, list)| list)
            .filter(|list| !list.is_empty())
    }
}

/// Persist events from every channel into the queue.
///
/// Runs until the sender side closes, which happens when all channels have stopped.
///
/// `accounts` says which account each channel is logged in as, and is part of the identity of
/// everything written here: a message from a channel whose account is unknown cannot be told apart
/// from the same message id under a bot that was since replaced, so it is refused rather than
/// filed.
pub async fn writer(
    store: Store,
    config: Arc<Config>,
    mut events: mpsc::Receiver<InboundEvent>,
    wake_drain: Arc<Notify>,
    typing: Arc<TypingState>,
    accounts: Arc<ChannelAccounts>,
) {
    let mut watches = Watches::default();
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
                let Some(account) = accounts.get(conversation.channel()) else {
                    tracing::error!(
                        conversation = %conversation,
                        "no account is registered for this channel, so a deletion cannot be \
                         recorded"
                    );
                    continue;
                };
                match store
                    .mark_deleted(conversation.as_str(), account, message_id, *timestamp)
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
        let Some(account) = accounts.get(message.channel.as_str()) else {
            // Unreachable while `run` only starts channels whose account resolved, and refused
            // rather than recorded under nothing if that ever changes: a row with no account has
            // no identity to be deduplicated by.
            tracing::error!(
                conversation = %conversation,
                "no account is registered for this channel, so the message cannot be recorded"
            );
            continue;
        };
        let disposition = match gate(&store, &config, &mut watches, message).await {
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
        if let Err(error) = register_attachments(&store, message, account).await {
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
        if let Err(error) = record_message(&store, &config, message, account, queued).await {
            tracing::error!(conversation = %conversation, "failed to record a message: {}", error);
        }

        // Taken while the message is still borrowed, since the queue path below serializes the
        // whole event and cannot hold that borrow at the same time.
        let sender_id = message.sender.id.clone();
        let message_id = message.message_id.clone();
        let revision = message.revision();

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
                    MessageKey {
                        conversation: conversation.as_str(),
                        account,
                        message_id: &message_id,
                        revision,
                    },
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
                        message_id = %message_id,
                        revision,
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
///
/// Watches are the third way through a mute, after a mention and a reply. They are consulted only
/// there, because that is the only policy where they decide anything: `active` delivers the message
/// whatever they say, and `block` keeps nothing for them to read.
async fn gate(
    store: &Store,
    config: &Config,
    watches: &mut Watches,
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

    // Recorded on the message before the arms below rather than inside the one that needs it, so
    // that a message which was also addressed still carries what it matched. The agent is told why
    // it was woken, and "you were named" alone would hide a rule that is firing on every mention.
    if policy == Policy::Mute {
        let matched = match watches.current(store, now).await {
            Some(list) => list.matches(message),
            None => Vec::new(),
        };
        message.matches = matched;
    }

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
        // A rule the agent set itself, which is the whole point of one: it asked to hear about
        // this, in a room it had otherwise turned down.
        Policy::Mute if !message.matches.is_empty() => Ok(Disposition::Deliver),
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
             history_read will show it"
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

/// What one conversation said while the agent was not listening, for the item about to state it.
///
/// Nothing is marked seen here: the bounds come back for [`Store::hold`] to stamp, in the same
/// transaction that records the rendered item, so the count and the item stating it are written
/// together or not at all. A hand-over that later dies gives back exactly the rows it named and no
/// others, which is what `messages.accounted_by` is for and what no watermark could express once
/// two hand-overs can be outstanding at once.
///
/// Every conversation in a claimed pass is asked, not only the muted ones: unmuting one leaves
/// behind whatever piled up while it was muted, and asking only the muted ones would report that
/// backlog to nobody and never clear it.
///
/// `through` bounds what may be counted, so anything that lands while the pass is being rendered
/// stays unseen and is stated next time rather than silently marked.
async fn missed_context(
    context: &SessionContext,
    message: &InboundMessage,
    through: DateTime<Utc>,
) -> (Option<MissedContext>, Option<Backlog>) {
    let conversation = &message.conversation;
    let now = Utc::now();
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
            return (None, None);
        }
    };
    let muted = policy == Policy::Mute;

    let (count, recent, watermark) = match context
        .store
        .take_unseen(
            conversation.as_str(),
            through,
            context.config.bridge.mute_context,
        )
        .await
    {
        Ok(taken) => taken,
        Err(error) => {
            tracing::error!(
                conversation = %conversation,
                "could not read what a conversation withheld: {}",
                error
            );
            return (None, None);
        }
    };
    // A conversation being heard in full with nothing owed has nothing to say, so it is dropped
    // rather than rendered as an empty block. A muted one is still worth a line: it tells the agent
    // why it is seeing one message out of a conversation.
    if count == 0 {
        let block = muted.then(|| MissedContext {
            conversation: conversation.clone(),
            muted,
            count,
            recent: Vec::new(),
        });
        return (block, None);
    }
    (
        Some(MissedContext {
            conversation: conversation.clone(),
            muted,
            count,
            recent: recent
                .into_iter()
                .map(|record| MissedMessage {
                    sender: record.sender_name,
                    // Descriptor lines stand in for a message whose content is not text, so a photo
                    // does not read back as somebody saying nothing.
                    text: match (record.text.trim().is_empty(), record.notes) {
                        (true, Some(notes)) => notes,
                        (true, None) => "[no text]".to_string(),
                        (false, _) => record.text,
                    },
                    timestamp: record.timestamp,
                })
                .collect(),
        }),
        Some(Backlog { watermark, through }),
    )
}

/// What identifies `message` in the store, once the account that received it is known.
fn key_of(message: &InboundMessage, account: AccountId) -> MessageKey<'_> {
    MessageKey {
        conversation: message.conversation.as_str(),
        account,
        message_id: &message.message_id,
        revision: message.revision(),
    }
}

/// Write one message to the history, unless history is switched off.
async fn record_message(
    store: &Store,
    config: &Config,
    message: &InboundMessage,
    account: AccountId,
    queued: bool,
) -> Result<(), crate::store::StoreError> {
    if config.storage.history_retention.is_zero() {
        return Ok(());
    }
    store
        .record_message(crate::store::MessageRecord {
            // Assigned by the store on insert.
            id: 0,
            conversation: message.conversation.as_str().to_string(),
            account,
            message_id: message.message_id.clone(),
            revision: message.revision(),
            sender_id: (!message.sender.id.is_empty()).then(|| message.sender.id.clone()),
            sender_name: message.sender.display_name.clone(),
            text: message.text.clone(),
            notes: (!message.notes.is_empty()).then(|| message.notes.join("; ")),
            // Read back from the registry rather than written here; the files were registered
            // against this same key just before.
            attachments: Vec::new(),
            addressed: message.addressed,
            // A queued message is one the agent is about to be handed, so it is accounted for
            // already and must not also be offered back to it later as context it missed.
            seen: queued,
            own: false,
            session_id: None,
            deleted_at: None,
            superseded_at: None,
            timestamp: message.timestamp,
            previous_account: false,
        })
        .await?;
    // An edit is recorded as a row of its own, since the queue needs a distinct key to deliver it
    // as an event rather than discard it as a redelivery. Marking what it replaced is what stops
    // `history_read` returning the old and the new wording as two messages that both look current.
    if let Some(edited_at) = message.edited_at {
        store
            .supersede_message(key_of(message, account), edited_at)
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
    account: AccountId,
) -> Result<(), crate::store::StoreError> {
    // Spelled out rather than taken from `key_of`, which would borrow the whole message while the
    // attachments are borrowed mutably.
    let key = MessageKey {
        conversation: message.conversation.as_str(),
        account,
        message_id: &message.message_id,
        revision: crate::channel::revision_of(message.edited_at),
    };
    super::register_files(store, key, message.timestamp, &mut message.attachments).await
}

/// Store the conversation an event came from, so it stays in the address book the agent can list.
async fn record_conversation(
    store: &Store,
    message: &InboundMessage,
) -> Result<(), crate::store::StoreError> {
    store
        .upsert_conversation(ConversationRecord {
            address: message.conversation.as_str().to_string(),
            channel: message.channel.as_str().to_string(),
            platform: message.platform.as_str().to_string(),
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

/// Everything the session task needs.
pub struct SessionContext {
    pub store: Store,
    pub config: Arc<Config>,
    pub meka: MekaClient,
    pub channels: Arc<ChannelRegistry>,
    /// Which account each channel is logged in as, for the envelope's orientation line and for
    /// keying what a failed batch owes.
    pub accounts: Arc<ChannelAccounts>,
    /// Guards the one-per-process reconciliation of the session's permission level.
    pub permission_checked: Arc<tokio::sync::OnceCell<()>>,
    /// A meka session created but not yet written down.
    ///
    /// Creating one and recording it are two systems, so a store that refuses the write leaves a
    /// real session behind on meka's side with nothing referring to it. Remembering it here means
    /// the retry persists that session instead of minting another, which turns an unbounded leak
    /// into at most one per process.
    pub pending_session: Arc<std::sync::Mutex<Option<Uuid>>>,
    /// Who is composing right now, so a conversation can be held until they stop.
    pub typing: Arc<TypingState>,
    /// Who has already been told that something failed, so an outage is reported once rather than
    /// once per batch.
    pub notices: NoticeLog,
    /// The session the feed follows. Published once bound, and again on a rebind, so the reader
    /// opens the right feed.
    pub session: watch::Sender<Option<Uuid>>,
    /// The last feed event handled, which the reader resumes from on a reconnect.
    pub position: Arc<AtomicU64>,
}

/// Hand messages to meka and act on what the feed says became of them, until `shutdown` fires.
///
/// The one task that talks to meka's inbox and the one writer of queue state, which is what makes
/// the ordering safe without locks: a post's 202 is recorded before the next feed event is looked
/// at, so an `inbox.delivered` meka emits before the 202 is even parsed still finds its row.
///
/// A post already in flight is allowed to finish; only the waits are interruptible. The row is
/// durable either way, and a restart posts it again under the same key.
pub async fn session_loop(
    context: SessionContext,
    mut typist: TurnTypist,
    mut feed: mpsc::Receiver<FeedEvent>,
    wake: Arc<Notify>,
    shutdown: CancellationToken,
) {
    // Nothing can be handed over or followed without a session, and the bridge comes up before
    // meka does, so this waits meka out rather than failing the start.
    let Some(mut session_id) = bind_session(&context, &shutdown).await else {
        return;
    };
    match context.store.recover().await {
        Ok(recovered) if recovered == Recovered::default() => {}
        Ok(recovered) => tracing::warn!(
            orphaned = recovered.orphaned,
            unposted = recovered.unposted,
            posted = recovered.posted,
            "recovered hand-overs the previous run left open"
        ),
        Err(error) => tracing::error!("failed to recover the queue: {}", error),
    }
    let mut turns: HashMap<String, TurnTally> = HashMap::new();
    // Nothing is due at startup: `recover` has just run, and the feed opening reconciles everything
    // handed over before it.
    let mut swept = tokio::time::Instant::now();
    loop {
        if swept.elapsed() >= context.config.bridge.straggler_sweep {
            settle_stragglers(&context, session_id).await;
            swept = tokio::time::Instant::now();
        }
        let next_due = work_queue(&context, &mut session_id, &shutdown).await;
        if shutdown.is_cancelled() {
            tracing::info!("session task stopping");
            return;
        }
        // Held across the inner loop rather than rebuilt per event, or a busy feed would push the
        // timer out for ever.
        let deadline = tokio::time::Instant::now() + next_due.unwrap_or(DRAIN_POLL_INTERVAL);
        loop {
            tokio::select! {
                () = shutdown.cancelled() => {
                    tracing::info!("session task stopping");
                    return;
                }
                () = wake.notified() => break,
                event = feed.recv() => match event {
                    Some(event) => {
                        let queue_changed = handle_feed(
                            &context,
                            &mut session_id,
                            &mut turns,
                            &mut typist,
                            event,
                            &shutdown,
                        )
                        .await;
                        if queue_changed {
                            break;
                        }
                    }
                    // The reader only stops on shutdown, which the arm above sees first.
                    None => return,
                },
                () = tokio::time::sleep_until(deadline) => break,
            }
        }
    }
}

/// Bind the meka session, waiting meka out.
///
/// `None` only on shutdown. Everything else is retried, because the bridge deliberately comes up
/// before meka does and a session that cannot be bound now is the ordinary case for the first
/// seconds of a boot.
async fn bind_session(context: &SessionContext, shutdown: &CancellationToken) -> Option<Uuid> {
    let mut attempt = 0_u32;
    loop {
        match ensure_session(context).await {
            Ok(session_id) => {
                // Read before the reader is told which session to follow, or it opens the feed
                // from the beginning and replays turns this bridge has already accounted for.
                match context.store.feed_position(session_id).await {
                    Ok(Some(position)) => context.position.store(position, Ordering::SeqCst),
                    Ok(None) => context.position.store(0, Ordering::SeqCst),
                    Err(error) => {
                        tracing::error!("could not read where the feed was read to: {}", error);
                    }
                }
                context.session.send_replace(Some(session_id));
                return Some(session_id);
            }
            Err(error) => {
                let wait = Duration::from_secs(1) * 2_u32.saturating_pow(attempt.min(5));
                tracing::warn!(
                    "could not bind the meka session ({}); trying again in {:?}",
                    error,
                    wait
                );
                attempt += 1;
                tokio::select! {
                    () = shutdown.cancelled() => return None,
                    () = tokio::time::sleep(wait) => {}
                }
            }
        }
    }
}

/// Post what meka has not accepted, then claim what has settled, and say how long until the next
/// thing that might change without a wake.
async fn work_queue(
    context: &SessionContext,
    session_id: &mut Uuid,
    shutdown: &CancellationToken,
) -> Option<Duration> {
    loop {
        if shutdown.is_cancelled() {
            return None;
        }
        let waiting = match context.store.rows_in(QueueState::InFlight).await {
            Ok(waiting) => waiting,
            Err(error) => {
                tracing::error!("could not read what is waiting for meka: {}", error);
                return Some(DRAIN_POLL_INTERVAL);
            }
        };
        // Oldest first, and nothing new is claimed while one waits, so what reaches the agent stays
        // in the order it was said.
        if let Some(row) = waiting.into_iter().next() {
            if let Some(not_before) = row.not_before {
                // Clamped on the way out as well as on the way in: a host clock stepping backwards
                // would otherwise hold the message for the whole size of the jump.
                let remaining = (not_before - Utc::now())
                    .to_std()
                    .unwrap_or(Duration::ZERO)
                    .min(RETRY_DELAY_MAX);
                if remaining > Duration::ZERO {
                    return Some(remaining);
                }
            }
            if !post(context, session_id, &row, shutdown).await {
                // Nothing was written against the post, so coming straight back would make the same
                // request again with no error able to stop it.
                return Some(DRAIN_POLL_INTERVAL);
            }
            continue;
        }
        let Readiness { ready, retry_in } = readiness(context).await;
        if ready.is_empty() {
            return retry_in;
        }
        let claimed = match context
            .store
            .claim(&ready, context.config.bridge.batch_max_messages)
            .await
        {
            Ok(claimed) => claimed,
            Err(error) => {
                tracing::error!("failed to claim messages for the agent: {}", error);
                return Some(DRAIN_POLL_INTERVAL);
            }
        };
        if claimed.is_empty() {
            return retry_in;
        }
        if !hold(context, *session_id, &claimed).await {
            return Some(DRAIN_POLL_INTERVAL);
        }
    }
}

/// Render every claimed row into an inbox item, each under a key of its own.
///
/// What is stated once rather than per message is stated by the first item entitled to it: the
/// dropped-message count by the first item of the pass, and a conversation's backlog by the first
/// item of that conversation. Both are recorded against that row in the same transaction that
/// stores the item, so a hand-over that dies gives back exactly what it took.
///
/// `false` when the store would not take a hand-over, so the rows still to be rendered are back
/// where they were and rendering them again at once would be a loop.
async fn hold(context: &SessionContext, session_id: Uuid, claimed: &[QueuedMessage]) -> bool {
    // Read rather than taken: the count comes off when the item stating it is stored, so a failure
    // between here and there loses the item and keeps the count.
    let mut dropped = context.store.peek_dropped().await.unwrap_or_else(|error| {
        tracing::error!("failed to read the dropped-message counter: {}", error);
        0
    });

    let mut events: Vec<(i64, InboundEvent)> = Vec::with_capacity(claimed.len());
    let mut undecodable = Vec::new();
    for row in claimed {
        match serde_json::from_str::<InboundEvent>(&row.payload) {
            Ok(event) => events.push((row.seq, event)),
            Err(error) => {
                // A payload this build cannot read will never become readable, so offering it again
                // would wedge the queue behind it forever.
                tracing::error!(
                    seq = row.seq,
                    "dropping an undecodable queue payload: {}",
                    error
                );
                undecodable.push(row.seq);
            }
        }
    }
    if !undecodable.is_empty()
        && let Err(error) = context.store.complete(&undecodable).await
    {
        tracing::error!("failed to discard undecodable payloads: {}", error);
    }

    // The newest arrival per conversation in this pass, which is the ceiling its backlog may be
    // counted to: a message claimed alongside is already being handed over, so counting it as
    // something the agent has not seen would report it twice.
    let mut newest: HashMap<&str, DateTime<Utc>> = HashMap::new();
    for (_, event) in &events {
        let at = event.timestamp();
        newest
            .entry(event.conversation().as_str())
            .and_modify(|newest| *newest = (*newest).max(at))
            .or_insert(at);
    }

    let mut stated: BTreeSet<&str> = BTreeSet::new();
    let mut conversations: BTreeSet<&str> = BTreeSet::new();
    let mut held = 0_usize;
    for (index, (seq, event)) in events.iter().enumerate() {
        let (missed, backlog) = match event {
            InboundEvent::Message(message) if stated.insert(message.conversation.as_str()) => {
                let through = newest
                    .get(message.conversation.as_str())
                    .copied()
                    .unwrap_or_else(Utc::now);
                missed_context(context, message, through).await
            }
            _ => (None, None),
        };
        let nonce = nonce();
        let body = Item {
            event,
            dropped,
            identities: context.accounts.identities(),
            missed: missed.as_ref(),
            nonce: &nonce,
        }
        .render();
        let key = format!("mekabridge-{}", Uuid::new_v4());
        let item = HandOver {
            key: &key,
            body: &body,
            session_id,
            dropped,
            accounted: backlog,
        };
        match context.store.hold(*seq, item, Utc::now()).await {
            Ok(true) => {
                // Said once per pass, by whichever item got there first.
                dropped = 0;
                held += 1;
                conversations.insert(event.conversation().as_str());
            }
            // Only reachable if something outside this task moved the row, which nothing does.
            Ok(false) => tracing::warn!(seq, "a claimed message was gone before it was rendered"),
            Err(error) => {
                tracing::error!(seq, "failed to record a hand-over: {}", error);
                // Back to the queue untouched: nothing was rendered for any of them and no offer
                // was spent.
                let unrendered: Vec<i64> = events[index..].iter().map(|(seq, _)| *seq).collect();
                if let Err(error) = context.store.unclaim(&unrendered).await {
                    tracing::error!("failed to release unrendered messages: {}", error);
                }
                return false;
            }
        }
    }
    if held > 0 {
        tracing::info!(
            messages = held,
            conversations = conversations.len(),
            "rendered messages for the agent"
        );
    }
    true
}

/// Hand one message to meka, and write down what came of the attempt.
///
/// `false` when the store would not take that outcome, so the row is still `in_flight` with nothing
/// recorded against it: no item id, no wait, no failure. Posting it again at once would be a
/// request loop against meka that no error could end, so the caller stops working the queue until
/// the next round instead.
async fn post(
    context: &SessionContext,
    session_id: &mut Uuid,
    row: &QueuedMessage,
    shutdown: &CancellationToken,
) -> bool {
    let (key, body) = match (row.key.as_deref(), row.body.as_deref()) {
        (Some(key), Some(body)) => (key, body),
        // A claim whose render never landed, which only a crash between the two leaves behind and
        // which startup recovery puts back. Nothing was minted and meka has nothing, so offering
        // the message again is free.
        (None, _) => {
            tracing::error!(
                seq = row.seq,
                "a message is in flight with nothing rendered for it"
            );
            if let Err(error) = context.store.unclaim(&[row.seq]).await {
                tracing::error!("failed to release an unrendered message: {}", error);
                return false;
            }
            return true;
        }
        // A key with no request kept behind it, which nothing here produces and only a database
        // edited by hand could hold. It cannot be posted, and it must not be offered again either:
        // meka may hold an item under that key already, so a fresh one would hand the same message
        // over twice. `unclaim` refuses a row that has a key, for that reason, so returning true
        // here would come straight back to this row and spin. Failing it is the one move that both
        // ends the round and says so.
        (Some(_), None) => {
            tracing::error!(
                seq = row.seq,
                "a message is in flight under a key with no request kept for it"
            );
            return fail(
                context,
                row,
                "the request it was handed over as was not kept",
                Retry::Never,
            )
            .await;
        }
    };
    let now = Utc::now();
    match context.meka.post_inbox(*session_id, body, key).await {
        Ok(receipt) => {
            if let Err(error) = context
                .store
                .posted(row.seq, *session_id, &receipt.item_id, now)
                .await
            {
                tracing::error!(seq = row.seq, "failed to record a hand-over: {}", error);
                return false;
            }
            tracing::info!(
                seq = row.seq,
                item = %receipt.item_id,
                replayed = receipt.replayed,
                "handed a message to the agent"
            );
            // A replayed key answers with the item's current state, which for a message posted
            // before a restart may already be its fate.
            if receipt.replayed {
                settle_replayed(context, row, receipt.state).await;
            }
            true
        }
        Err(error) if error.is_session_missing() => {
            if !context.config.session.recreate_on_missing {
                tracing::error!(
                    "meka no longer knows session {session_id}, and [session].recreate_on_missing \
                     is off; nothing can be handed over until one is bound"
                );
                return defer(context, row, &error, now).await;
            }
            // The row stays in flight, so the loop posts it into whatever is bound next -- but only
            // if a replacement was actually bound.
            rebind(context, session_id, shutdown).await
        }
        Err(error) if error.is_retryable() || error.is_session_locked() => {
            defer(context, row, &error, now).await
        }
        Err(error) => {
            // A failure needing an operator will not stop happening on its own, so spending the
            // hour on it only delays the notice that says so.
            tracing::error!(seq = row.seq, "meka refused a message: {}", error);
            fail(context, row, &error.to_string(), Retry::Never).await
        }
    }
}

/// Write down a post that did not go through, or give up once the message has waited long enough.
///
/// `false` when neither could be written, which leaves the row exactly as it was.
async fn defer(
    context: &SessionContext,
    row: &QueuedMessage,
    error: &MekaError,
    now: DateTime<Utc>,
) -> bool {
    let waited = row.held_at.map_or(Duration::ZERO, |held_at| {
        (now - held_at).to_std().unwrap_or(Duration::ZERO)
    });
    if waited >= context.config.bridge.hand_over_within {
        tracing::error!(
            seq = row.seq,
            posts = row.posts + 1,
            "giving up on a message meka never accepted: {}",
            error
        );
        return fail(context, row, &error.to_string(), Retry::Unaccepted).await;
    }
    let delay = retry_delay(
        context.config.bridge.retry_base,
        row.posts,
        error.retry_after(),
    );
    tracing::warn!(
        seq = row.seq,
        posts = row.posts + 1,
        retry_in = ?delay,
        "meka did not accept a message ({}); trying again",
        error
    );
    let not_before = now + chrono::Duration::from_std(delay).unwrap_or_default();
    if let Err(error) = context
        .store
        .defer(row.seq, &error.to_string(), not_before)
        .await
    {
        tracing::error!(
            seq = row.seq,
            "failed to record a deferred hand-over: {}",
            error
        );
        return false;
    }
    true
}

/// Bind a replacement for a session meka no longer knows.
///
/// Whatever meka had on the old session died with it, so those hand-overs are closed and their
/// messages put back among what the agent is owed: the next item from any of those chats reports
/// them as a backlog, which is how the replacement learns of them. Replaying them into it blind
/// would be the wrong call, since nothing here can say whether the old session read them before it
/// went.
///
/// `false` when the bridge still holds the same session afterwards, which happens when the stale
/// binding could not be cleared: [`ensure_session`] then hands the dead id straight back. The
/// caller has to stop rather than carry on, because nothing was written against the message that
/// hit the 404 either, so coming back to it would post to the same dead session at whatever rate
/// the two requests take.
async fn rebind(
    context: &SessionContext,
    session_id: &mut Uuid,
    shutdown: &CancellationToken,
) -> bool {
    let old = *session_id;
    tracing::warn!(
        "meka no longer knows session {old}; creating a replacement. The agent's memory of earlier \
         conversations is gone. If this keeps happening, check meka's `[serve].delete_on_idle`: \
         with it on, an idle session's row is deleted rather than merely evicted, which wipes the \
         assistant every idle_timeout."
    );
    if let Err(error) = context.store.clear_session_id().await {
        tracing::error!("failed to clear the stale session binding: {}", error);
    }
    context.position.store(0, Ordering::SeqCst);
    match context.store.rows_in(QueueState::Posted).await {
        Ok(posted) => {
            let stranded: Vec<QueuedMessage> = posted
                .into_iter()
                .filter(|row| row.session_id == Some(old))
                .collect();
            fail_all(
                context,
                &stranded,
                "the session it was handed to no longer exists",
                Retry::Never,
            )
            .await;
        }
        Err(error) => {
            tracing::error!("could not read what the old session held: {}", error);
        }
    }
    let Some(replacement) = bind_session(context, shutdown).await else {
        // Only reachable on shutdown, where every caller's own loop stops next.
        return false;
    };
    *session_id = replacement;
    if replacement == old {
        tracing::error!(
            "the binding to session {old} could not be cleared, so meka's replacement for it              cannot be bound; nothing can be handed over until the store takes a write"
        );
        return false;
    }
    true
}

/// Close a hand-over that will never reach the model, and tell whoever is waiting.
///
/// `false` when the store would not close it, so it is still open and still refusable.
async fn fail(context: &SessionContext, row: &QueuedMessage, reason: &str, retry: Retry) -> bool {
    match context.store.failed(row.seq, reason, Utc::now()).await {
        // Something else closed it first, so somebody has already been told whatever there was to
        // tell.
        Ok(None) => true,
        Ok(Some(closed)) => {
            let posts = closed.posts.max(1);
            give_up(context, std::slice::from_ref(&closed), reason, retry, posts).await;
            true
        }
        Err(error) => {
            tracing::error!(seq = row.seq, "failed to record a lost message: {}", error);
            false
        }
    }
}

/// Close several hand-overs that came apart for one reason, and say so once.
///
/// Together rather than one notice each, because that is what they are to the person reading them:
/// a session that went, a reset, one outage. Failures that happen independently still get a notice
/// each, bounded by [`NoticeLog`].
async fn fail_all(context: &SessionContext, rows: &[QueuedMessage], reason: &str, retry: Retry) {
    let mut closed = Vec::with_capacity(rows.len());
    for row in rows {
        match context.store.failed(row.seq, reason, Utc::now()).await {
            Ok(Some(row)) => closed.push(row),
            Ok(None) => {}
            Err(error) => {
                tracing::error!(seq = row.seq, "failed to record a lost message: {}", error);
            }
        }
    }
    if closed.is_empty() {
        return;
    }
    let posts = closed.iter().map(|row| row.posts).max().unwrap_or(1).max(1);
    give_up(context, &closed, reason, retry, posts).await;
}

/// Put messages nothing will deliver back among what the agent is owed, and say so.
async fn give_up(
    context: &SessionContext,
    rows: &[QueuedMessage],
    reason: &str,
    retry: Retry,
    attempts: u32,
) {
    tracing::error!(count = rows.len(), "giving up on {} message(s)", rows.len());
    // Owed to the agent again. A message is marked seen the moment it is queued, on the assumption
    // that the agent is about to be handed it, and this is exactly where that assumption fails.
    // Without this it is neither delivered nor owed, so nothing but `history_read` over the right
    // window would ever turn it up again. What each hand-over reported as a *backlog* is given back
    // by the store, against the row that reported it.
    let mut affected: BTreeSet<ConversationId> = BTreeSet::new();
    // Whether that actually worked, which it cannot when `[storage].history_retention` is zero:
    // there is no history row to un-see, so the message really is gone. The owner is told which of
    // the two happened rather than promised the recoverable one.
    let mut recoverable = false;
    for message in rows {
        match context.store.mark_unseen(message.key()).await {
            Ok(marked) => recoverable |= marked,
            Err(error) => tracing::error!(
                conversation = %message.conversation,
                "failed to mark an undeliverable message unseen: {}",
                error
            ),
        }
        if let Some(conversation) = ConversationId::parse(&message.conversation) {
            affected.insert(conversation);
        }
    }
    announce(
        context,
        &affected,
        &Failure {
            reason,
            retry,
            answered: false,
            delivered: false,
        },
        Lost {
            count: rows.len(),
            attempts,
            recoverable,
        },
    )
    .await;
}

/// Close a hand-over the model read. `false` for one already closed.
///
/// Nothing is marked seen here: the backlog this item stated was accounted for against its row when
/// the item was rendered, which is what lets a second item for the same chat state only what is
/// left while this one is still outstanding.
async fn deliver(context: &SessionContext, row: &QueuedMessage) -> bool {
    match context.store.delivered(row.seq, Utc::now()).await {
        Ok(true) => {
            tracing::info!(
                seq = row.seq,
                conversation = %row.conversation,
                "the agent read a message"
            );
            true
        }
        // The feed replays an outcome after a reconnect, and the second reading changes nothing.
        Ok(false) => false,
        Err(error) => {
            tracing::error!(
                seq = row.seq,
                "failed to record a delivered message: {}",
                error
            );
            false
        }
    }
}

/// Offer a message again, for a hand-over that came apart with nobody answered.
///
/// `delay` holds it back before the next claim. Right for an item meka took back, where whatever
/// took it back is probably still true a moment later; wrong for a turn that read it and produced
/// nothing, where the agent is idle and the only cost of waiting is the silence.
async fn offer_again(
    context: &SessionContext,
    row: &QueuedMessage,
    reason: &str,
    delay: Option<Duration>,
) {
    let now = Utc::now();
    let not_before = delay.map(|delay| now + chrono::Duration::from_std(delay).unwrap_or_default());
    let offer = match context
        .store
        .reoffer(
            row.seq,
            reason,
            context.config.bridge.max_offers,
            not_before,
            now,
        )
        .await
    {
        Ok(offer) => offer,
        Err(error) => {
            tracing::error!(seq = row.seq, "failed to offer a message again: {}", error);
            return;
        }
    };
    match offer {
        Offer::Retrying => tracing::warn!(
            seq = row.seq,
            conversation = %row.conversation,
            "a message did not reach the agent ({}); it is offered again",
            reason
        ),
        Offer::Exhausted(exhausted) => {
            let posts = exhausted.posts.max(1);
            give_up(
                context,
                std::slice::from_ref(&*exhausted),
                reason,
                Retry::Undelivered,
                posts,
            )
            .await;
        }
        Offer::Closed => {}
    }
}

/// Act on what a replayed key said the item had become.
async fn settle_replayed(context: &SessionContext, row: &QueuedMessage, state: InboxState) {
    match state {
        InboxState::Delivered => {
            deliver(context, row).await;
        }
        InboxState::Withdrawn => {
            // meka stamps the same state whether the item was cancelled unread or given up on after
            // its own hour of retries, and the receipt does not say which, so the reason claims
            // only what is known. Offering the message again is right for the first and
            // harmless for the second, where a fresh item is a fresh hour and the
            // offers are bounded either way.
            offer_again(
                context,
                row,
                "meka took it back before the agent read it",
                Some(context.config.bridge.retry_base),
            )
            .await;
        }
        InboxState::Pending | InboxState::Appended => {
            tracing::debug!(seq = row.seq, state = %state, "a message is still with meka");
        }
        InboxState::Other(name) => tracing::warn!(
            seq = row.seq,
            state = %name,
            "meka reports an item state this build does not know; leaving the message as it is"
        ),
    }
}

/// Ask meka about anything it has had for longer than an outcome can take.
///
/// The feed's replay and a reconnect's reconciliation settle these in the ordinary way, and neither
/// happens on a connection that stays up for days. What is left is the hand-over whose outcome
/// reached this bridge and was not written down: a store error inside `apply` is logged and nothing
/// re-drives it, the feed position moves past the event, and meka never sends it again. Its message
/// would sit with meka for as long as the connection lasts, counting against the queue depth and
/// reaching nobody.
///
/// meka gives up on an item within the hour, so one older than that has an outcome that has been
/// and gone -- or is one meka is holding for the next turn, which asking will not hurry.
/// Rate-limited by `[bridge].straggler_sweep` for that second case, and normally over an empty
/// list.
async fn settle_stragglers(context: &SessionContext, session_id: Uuid) {
    let posted = match context.store.rows_in(QueueState::Posted).await {
        Ok(posted) => posted,
        Err(error) => {
            tracing::error!("could not read what meka has: {}", error);
            return;
        }
    };
    let stale = Utc::now()
        - chrono::Duration::from_std(context.config.bridge.hand_over_within).unwrap_or_default();
    for row in posted {
        if row.posted_at.is_none_or(|posted| posted > stale) {
            continue;
        }
        tracing::warn!(
            seq = row.seq,
            "meka has had a message longer than an outcome can take; asking what became of it"
        );
        ask_about(context, session_id, &row).await;
    }
}

/// Ask meka what became of everything it has, for the events that may have been missed.
///
/// Run on every reconnect, not only on a notice saying the replay had a hole: the notice is matched
/// on prose, and a post under a key meka already holds costs one request and changes nothing on its
/// side.
async fn reconcile(context: &SessionContext, session_id: Uuid, before: DateTime<Utc>) {
    let posted = match context.store.rows_in(QueueState::Posted).await {
        Ok(posted) => posted,
        Err(error) => {
            tracing::error!("could not read what meka has: {}", error);
            return;
        }
    };
    for row in posted {
        // Handed over since the connection opened, so its outcome is on that connection and asking
        // would only spend a request to be told what is already on the way.
        if row.posted_at.is_some_and(|posted| posted > before) {
            continue;
        }
        ask_about(context, session_id, &row).await;
    }
}

/// Ask meka what became of one hand-over, under the key it was made with.
async fn ask_about(context: &SessionContext, session_id: Uuid, row: &QueuedMessage) {
    if row.session_id != Some(session_id) {
        // Nothing will ever report on it: its outcome would arrive on a feed for a session this
        // bridge no longer follows. A rebind closes these as it goes, so reaching one here means a
        // rebind could not read them, and leaving it would strand its message with meka for good.
        tracing::warn!(
            seq = row.seq,
            "a message was handed to a session this bridge no longer follows"
        );
        fail(
            context,
            row,
            "the session it was handed to is no longer this bridge's",
            Retry::Never,
        )
        .await;
        return;
    }
    let (Some(key), Some(body)) = (row.key.as_deref(), row.body.as_deref()) else {
        tracing::error!(
            seq = row.seq,
            "meka has an item this bridge kept no request for, so it cannot be asked about"
        );
        return;
    };
    match context.meka.post_inbox(session_id, body, key).await {
        Ok(receipt) if !receipt.replayed => {
            // meka has no record of the key, so the post just made is the hand-over now.
            //
            // One thing reaches here in practice: `[meka].token` was rotated, since meka scopes a
            // key to the token that used it. A meka whose store was replaced has no session either
            // and answers 404, which rebinds rather than arriving here.
            //
            // The item made under the old token is still meka's to deliver, so the agent reads this
            // message twice. Taken rather than prevented: withdrawing the copy just made would
            // leave the old one's fate unknowable and its message owed back, which trades one kind
            // of repetition for another, and this way it is delivered.
            tracing::warn!(
                seq = row.seq,
                item = %receipt.item_id,
                "meka had no record of a message it had accepted; it is handed over afresh"
            );
            if let Err(error) = context
                .store
                .posted(row.seq, session_id, &receipt.item_id, Utc::now())
                .await
            {
                tracing::error!(seq = row.seq, "failed to record a hand-over: {}", error);
            }
        }
        Ok(receipt) => settle_replayed(context, row, receipt.state).await,
        Err(error) => tracing::warn!(
            seq = row.seq,
            "could not ask meka about a message ({}); it is asked about again later",
            error
        ),
    }
}

/// The row meka knows as `item_id`, or `None` with the reason logged.
async fn row_for(context: &SessionContext, item_id: &str) -> Option<QueuedMessage> {
    match context.store.row_by_item(item_id).await {
        Ok(Some(row)) => Some(row),
        Ok(None) => {
            // Another client of the same session, or a message `queue clear` took out from under
            // the hand-over. Nothing here to settle either way.
            tracing::debug!(item = %item_id, "the feed named an item this bridge did not hand over");
            None
        }
        Err(error) => {
            tracing::error!(item = %item_id, "could not look up an item's message: {}", error);
            None
        }
    }
}

/// The conversations `rows` came from.
fn conversations_of(rows: &[QueuedMessage]) -> BTreeSet<ConversationId> {
    rows.iter()
        .filter_map(|row| ConversationId::parse(&row.conversation))
        .collect()
}

/// Drop a feed event belonging to a session this bridge has already replaced.
///
/// Its id is a number in that session's own sequence, so letting it reach the feed position would
/// have the reader reopen the replacement's feed from a `Last-Event-ID` that means nothing there,
/// and meka would replay nothing until the new session's own ids overtook it. Nothing else is lost
/// by dropping them: whatever the old session was told to do is settled by the rebind, which closes
/// every hand-over it held.
fn superseded(event: Uuid, current: Uuid) -> bool {
    tracing::debug!("dropping a feed event for session {event}, already replaced by {current}");
    false
}

/// Act on one thing the feed reader handed over.
///
/// Returns whether the queue may have changed, so the caller works it before waiting again.
async fn handle_feed(
    context: &SessionContext,
    session_id: &mut Uuid,
    turns: &mut HashMap<String, TurnTally>,
    typist: &mut TurnTypist,
    event: FeedEvent,
    shutdown: &CancellationToken,
) -> bool {
    match event {
        FeedEvent::Connected { session, at } => {
            if session != *session_id {
                return superseded(session, *session_id);
            }
            tracing::info!(session_id = %session_id, "following the session feed");
            reconcile(context, *session_id, at).await;
            true
        }
        FeedEvent::SessionGone { session } => {
            // Only for the session the reader was actually following. A post that hit the same 404
            // has already bound a replacement by now, and rebinding again would throw that one
            // away unread and tell the owner its messages were lost.
            if session != *session_id {
                tracing::debug!(
                    "the feed lost session {session}, which has already been replaced by \
                     {session_id}"
                );
                return false;
            }
            if context.config.session.recreate_on_missing {
                // The result is deliberately not consulted: whether or not a replacement was bound,
                // the hand-overs the old session held were closed on the way past, so the queue has
                // changed and is worth working either way.
                rebind(context, session_id, shutdown).await;
            } else {
                tracing::error!(
                    "meka no longer knows session {session_id}, and [session].recreate_on_missing \
                     is off; nothing can be handed over until one is bound"
                );
            }
            true
        }
        FeedEvent::Item {
            session,
            item: StreamItem { id, turn_id, event },
        } => {
            if session != *session_id {
                return superseded(session, *session_id);
            }
            // Bounded by dropping what is held, the same rule the typist uses: a tally matters
            // only between a turn's start and its terminal, and losing one costs that turn's log
            // line its counters.
            if turns.len() >= crate::bridge::turn::MAX_TRACKED_TURNS {
                tracing::warn!(
                    "tallying {} turns, so terminals are going missing; forgetting what is held",
                    turns.len()
                );
                turns.clear();
            }
            let queue_changed = apply(
                context,
                *session_id,
                turns,
                typist,
                turn_id.as_deref(),
                &event,
            )
            .await;
            if let Some(id) = id {
                context.position.fetch_max(id, Ordering::SeqCst);
                // Written down for the events that decide something, and not per text delta. A
                // position behind a delta is replayed into a tally at worst; one behind an inbox
                // outcome is replayed into a write that changes nothing the second time.
                let worth_keeping = matches!(
                    event,
                    TurnEvent::InboxDelivered { .. }
                        | TurnEvent::InboxFailed { .. }
                        | TurnEvent::InboxWithdrawn { .. }
                        | TurnEvent::Finished { .. }
                        | TurnEvent::Failed { .. }
                        | TurnEvent::Cancelled { .. }
                );
                if worth_keeping
                    && let Err(error) = context.store.set_feed_position(*session_id, id).await
                {
                    tracing::error!("failed to record the feed position: {}", error);
                }
            }
            queue_changed
        }
    }
}

/// How a turn ended, off its terminal event.
enum Ending {
    Finished(String),
    Failed(ProblemDetail),
    Cancelled(String),
}

/// Act on one feed event. Returns whether the queue may have changed.
async fn apply(
    context: &SessionContext,
    session_id: Uuid,
    turns: &mut HashMap<String, TurnTally>,
    typist: &mut TurnTypist,
    turn: Option<&str>,
    event: &TurnEvent,
) -> bool {
    match event {
        TurnEvent::Started {
            source, resumed, ..
        } => {
            let Some(turn) = turn else {
                return false;
            };
            typist.begin(turn);
            turns.entry(turn.to_string()).or_default();
            match source {
                TurnSource::Inbox { item_ids } => {
                    let mut conversations = BTreeSet::new();
                    let mut messages = 0;
                    for item_id in item_ids {
                        let Some(row) = row_for(context, item_id).await else {
                            continue;
                        };
                        messages += 1;
                        if let Some(conversation) = ConversationId::parse(&row.conversation) {
                            conversations.insert(conversation);
                        }
                    }
                    if *resumed {
                        tracing::debug!(
                            turn,
                            messages,
                            "rejoined a turn running on this bridge's messages"
                        );
                    } else {
                        tracing::info!(
                            turn,
                            messages,
                            conversations = conversations.len(),
                            "the agent was woken for this bridge's messages"
                        );
                    }
                    // Taken from a resumed announcement too, which is the whole worth of meka 0.57
                    // naming the source on one: a bridge that restarted mid-turn learns here whom
                    // the turn is answering, and without it a send composed after the reattach
                    // raises no indicator, since the typist has no target for a turn it was handed
                    // nothing for. Only the items the turn *opened* on, which is what meka records
                    // as its source; one appended at a later round boundary is learnt from its own
                    // `inbox.delivered` if that is still in the replay.
                    typist.expect(turn, conversations);
                }
                // A turn already running when the feed attached, and already counted. meka names
                // the source on that announcement since 0.57, so a resumed one no longer arrives
                // as `Unknown`: the inbox arm above takes its own, and everything else says only
                // that this is a rejoin.
                _ if *resumed => {
                    tracing::debug!(turn, "joined a turn already running");
                }
                TurnSource::Client => {
                    tracing::debug!(turn, "a turn somebody else submitted began");
                }
                TurnSource::Schedule { job_id } => {
                    tracing::debug!(turn, job = %job_id, "a scheduled job fired");
                }
                TurnSource::Background => {
                    tracing::debug!(turn, "a background outcome opened a turn");
                }
                TurnSource::Unknown(name) => {
                    tracing::debug!(turn, source = %name, "a turn began");
                }
            }
            false
        }
        TurnEvent::AssistantText { .. } => {
            if let Some(turn) = turn {
                turns.entry(turn.to_string()).or_default().note(event);
            }
            false
        }
        TurnEvent::ToolCallComposing { .. }
        | TurnEvent::ToolCallStarted { .. }
        | TurnEvent::ToolCallCompleted { .. } => {
            if let Some(turn) = turn {
                turns.entry(turn.to_string()).or_default().note(event);
                typist.observe(turn, event);
            }
            if let TurnEvent::ToolCallStarted { name, .. } = event {
                tracing::debug!(tool = %name, "agent tool call");
            }
            false
        }
        TurnEvent::InboxDelivered { item_ids } => {
            for item_id in item_ids {
                let Some(row) = row_for(context, item_id).await else {
                    continue;
                };
                deliver(context, &row).await;
                let Some(turn) = turn else {
                    continue;
                };
                // Recorded whether or not this was the call that closed the hand-over. A
                // reconciliation settles one a moment before its event is read on every reconnect
                // that spans a delivery, and the turn read it either way: the notice its failure
                // owes, and the second offer its silence owes, both hang on this association and
                // nothing else carries it.
                {
                    let tally = turns.entry(turn.to_string()).or_default();
                    if !tally.read.contains(&row.seq) {
                        tally.read.push(row.seq);
                    }
                }
                if let Some(conversation) = ConversationId::parse(&row.conversation) {
                    typist.expect(turn, BTreeSet::from([conversation]));
                }
            }
            false
        }
        TurnEvent::InboxFailed { item_id, reason } => {
            if let Some(row) = row_for(context, item_id).await {
                tracing::error!(seq = row.seq, "meka gave up on a message: {}", reason);
                fail(context, &row, reason, Retry::Undelivered).await;
            }
            false
        }
        TurnEvent::InboxWithdrawn { item_id } => {
            if let Some(row) = row_for(context, item_id).await {
                offer_again(
                    context,
                    &row,
                    "the turn that had opened on it was cancelled",
                    Some(context.config.bridge.retry_base),
                )
                .await;
            }
            true
        }
        TurnEvent::Finished { stop_reason, .. } => {
            let Some(turn) = turn else {
                return false;
            };
            let tally = turns.remove(turn);
            finish(
                context,
                turn,
                tally,
                typist,
                Ending::Finished(stop_reason.clone()),
            )
            .await
        }
        TurnEvent::Failed { error } => {
            let Some(turn) = turn else {
                return false;
            };
            let tally = turns.remove(turn);
            let problem: ProblemDetail = serde_json::from_value(error.clone()).unwrap_or_default();
            finish(context, turn, tally, typist, Ending::Failed(problem)).await
        }
        TurnEvent::Cancelled { reason } => {
            let Some(turn) = turn else {
                return false;
            };
            let tally = turns.remove(turn);
            finish(
                context,
                turn,
                tally,
                typist,
                Ending::Cancelled(reason.clone()),
            )
            .await
        }
        TurnEvent::Notice { level, text } => {
            tracing::warn!(level = %level, "meka notice: {}", text);
            if notice_reports_lost_events(text) {
                // Everything meka has, however recently: the notice says events were dropped, and
                // nothing here can say which.
                reconcile(context, session_id, Utc::now()).await;
                return true;
            }
            false
        }
        TurnEvent::ContextCompacted {
            source,
            replaced_count,
            generation,
        } => {
            // Worth a line at warn because of what this session is. One permanent context holds
            // everyone the agent has ever spoken to, on every platform, so a compaction is the
            // moment its memory of conversations nobody is currently having becomes a summary.
            // Nothing here can prevent it; an operator wondering why the agent forgot somebody
            // should be able to find when.
            tracing::warn!(
                source = %source,
                replaced = replaced_count,
                generation,
                "meka compacted the session; earlier conversations are now a summary"
            );
            false
        }
        TurnEvent::PermissionRequired { tool_name, .. } => {
            // Sessions declare `supports_permission_prompts: false`, so meka denies a gated tool
            // immediately rather than emitting this. Reaching here means the turn is about to
            // stall for the full timeout with nothing able to answer.
            tracing::error!(
                tool = %tool_name,
                "meka asked for permission, but this bridge has no approval channel; the turn will \
                 stall and deny. meka only asks when its [permissions].approvals is on for the \
                 session, so turn that off and raise [session].permission if the tool is one the \
                 agent should reach."
            );
            false
        }
        TurnEvent::Thinking { .. } | TurnEvent::Unknown { .. } => false,
    }
}

/// Close out a turn that ended. Returns whether the queue may have changed.
async fn finish(
    context: &SessionContext,
    turn: &str,
    tally: Option<TurnTally>,
    typist: &mut TurnTypist,
    ending: Ending,
) -> bool {
    typist.close(turn);
    let tally = tally.unwrap_or_default();
    match ending {
        Ending::Finished(stop_reason) => {
            tracing::info!(
                turn,
                stop_reason = %stop_reason,
                sends = tally.sends,
                tool_calls = tally.tool_calls,
                text_chars = tally.text_length,
                messages = tally.read.len(),
                "turn finished"
            );
            if tally.read.is_empty() {
                return false;
            }
            if let Err(error) = context.store.mark_turn_completed(Utc::now()).await {
                tracing::error!("failed to record the turn timestamp: {}", error);
            }
            // A turn that produced meka's empty-response stand-in and called nothing did no work
            // at all: no message was sent, no tool ran. Offering the messages again is therefore
            // free of side effects, and far better than leaving somebody who just messaged the bot
            // in silence. No wait before the next claim: the agent is idle, and the only cost of
            // one would be the silence going on longer.
            if tally.produced_nothing(&stop_reason) {
                tracing::warn!(
                    text = %tally.text_preview.trim(),
                    "the model returned an empty response and called no tools; offering its \
                     messages again"
                );
                for row in read_by(context, &tally).await {
                    offer_again(context, &row, "the model returned an empty response", None).await;
                }
                return true;
            }
            if tally.is_silent() {
                // Not an error: the agent is allowed to read something and say nothing. Logged at
                // warn with what it produced instead, because from the other end this looks
                // exactly like a broken bridge, and the text is usually what explains which one it
                // was.
                tracing::warn!(
                    messages = tally.read.len(),
                    tool_calls = tally.tool_calls,
                    text = %tally.text_preview.trim(),
                    "the agent sent no messages this turn"
                );
            }
            false
        }
        Ending::Failed(problem) => {
            let error = MekaError::Problem(problem);
            // Worth saying differently because it is the one failure here that is not about the
            // message that hit it. One permanent session carries every conversation, so a window
            // it has outgrown refuses the next message too. meka retries an item it has accepted
            // for an hour, so this is logged on each of those attempts, and the item is given up
            // on with `inbox.failed` at the end of them.
            if error.is_context_overflow() {
                tracing::error!(
                    "the agent's session no longer fits the model's context window, so every \
                     message will fail until it is shortened: compact or rewind it on meka's side, \
                     or `mekabridge session reset --yes` to start a new one and lose what it \
                     remembers ({})",
                    error
                );
            } else {
                tracing::error!(
                    turn,
                    sends = tally.sends,
                    tool_calls = tally.tool_calls,
                    "turn failed: {}",
                    error
                );
            }
            after_reading(context, &tally, &error.to_string()).await;
            false
        }
        Ending::Cancelled(reason) => {
            tracing::warn!(
                turn,
                sends = tally.sends,
                tool_calls = tally.tool_calls,
                reason = %reason,
                "turn cancelled"
            );
            after_reading(
                context,
                &tally,
                &format!("the turn was cancelled: {reason}"),
            )
            .await;
            false
        }
    }
}

/// A turn ended badly after the model had read this bridge's messages.
///
/// They are accounted for, since they were read, and are not offered again: a second run would
/// repeat whatever the first did, with the agent unable to remember doing it. What the agent was
/// in the middle of is half done, which is what the owner is told, and the chats only where the
/// agent had not got a word in.
async fn after_reading(context: &SessionContext, tally: &TurnTally, reason: &str) {
    if tally.read.is_empty() {
        return;
    }
    let rows = read_by(context, tally).await;
    let affected = conversations_of(&rows);
    let count = rows.len();
    tracing::error!(
        sends = tally.sends,
        tool_calls = tally.tool_calls,
        messages = count,
        "the turn ended after the agent had read this bridge's messages, so they are not offered \
         again: {}",
        reason
    );
    announce(
        context,
        &affected,
        &Failure {
            reason,
            retry: Retry::Never,
            answered: tally.sends > 0,
            delivered: true,
        },
        Lost {
            count,
            attempts: 1,
            recoverable: false,
        },
    )
    .await;
}

/// The rows a turn read, as the store has them now.
async fn read_by(context: &SessionContext, tally: &TurnTally) -> Vec<QueuedMessage> {
    match context.store.rows(&tally.read).await {
        Ok(rows) => rows,
        Err(error) => {
            tracing::error!("could not read what a turn was handed: {}", error);
            Vec::new()
        }
    }
}

/// How long a hand-over waits before it is posted again.
///
/// `attempts` is how many it has already had, so the first failure waits `base` and each one after
/// that twice as long. Exponential rather than fixed because the failure this exists for is meka
/// out of reach, where the right wait is unknowable and the cost of guessing short is a request
/// spent for nothing.
///
/// A `Retry-After` raises the wait but never lowers it. Letting it ask for *shorter* would undo the
/// point: meka's concurrency-limit 429 carries a flat one second, which taken literally puts every
/// attempt inside a few seconds.
fn retry_delay(base: Duration, attempts: u32, retry_after: Option<Duration>) -> Duration {
    // Capped exponent as well as capped result: `Duration * u32` panics on overflow, and this is
    // reached with whatever attempt count a database row happens to hold.
    let scheduled = base * 2_u32.saturating_pow(attempts.min(8));
    retry_after
        .map_or(scheduled, |asked| asked.max(scheduled))
        .min(RETRY_DELAY_MAX)
}

/// What should happen to a batch that did not reach the model.
struct Failure<'a> {
    /// Recorded against the rows and quoted to the owner verbatim.
    reason: &'a str,
    /// Why it is not being offered again.
    retry: Retry,
    /// Whether the agent got a word in anywhere before this went wrong.
    ///
    /// A chat that was answered is not owed an apology, and one sent anyway would contradict what
    /// the agent had just said in it. Whether *this* chat was answered would be the sharper
    /// question, and the answer is deliberately not asked for: the only record of it is the
    /// presence state, which is meaningful solely after a turn actually ran, and half the callers
    /// here are failures where none did. Erring toward silence costs a chat in a
    /// multi-conversation batch its apology; erring the other way apologises to somebody who was
    /// just answered.
    answered: bool,
    /// Whether the messages reached the agent despite this.
    ///
    /// Changes the whole of what the owner is looking at. Messages that never arrived are lost and
    /// somebody has to decide what to do about them; messages the agent read before the turn died
    /// are not lost, but whatever it was in the middle of is half done.
    delivered: bool,
}

/// Why a batch is not being offered again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Retry {
    /// A failure that needs an operator, where further attempts only delay the notice saying so.
    Never,
    /// meka never accepted it: every post over the hour failed.
    Unaccepted,
    /// meka accepted it and could not deliver it.
    Undelivered,
}

/// What the session task should do with the queue this round.
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
async fn readiness(context: &SessionContext) -> Readiness {
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
        // parts still all reach the agent, the later ones under a header saying when they arrived.
        if waited >= settle_max {
            ready.push(window.conversation);
            continue;
        }
        let quiet_for = elapsed(window.newest);
        // Under the ceiling the floor always applies. Beyond it a conversation is held while
        // somebody is composing, and for the quiet period after they stop, both only where the
        // platform reports typing: without that signal there is nothing to wait on and any wait is
        // a guess.
        let hold = if !settle.is_zero() && reports_typing(context, &window.conversation) {
            if context.typing.active(&window.conversation) {
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
        ready.push(window.conversation);
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
fn reports_typing(context: &SessionContext, conversation_id: &str) -> bool {
    ConversationId::parse(conversation_id)
        .and_then(|conversation| context.channels.get(conversation.channel()).cloned())
        .is_some_and(|channel| channel.capabilities().typing_status)
}

/// What became of the messages a notice is about.
struct Lost {
    count: usize,
    /// Attempts actually spent, which a permanent refusal cuts short at one.
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
    context: &SessionContext,
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
            "mekabridge: a turn ended badly after the agent had already read {count} message(s), \
             so whatever it was doing with them is half finished. The messages themselves are \
             accounted for and were not offered again, since a second run would repeat what the \
             first one did.\n\nError: {}",
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
            Retry::Unaccepted => format!(
                "\nmeka did not accept it in {} attempt(s) over the last hour.",
                lost.attempts
            ),
            Retry::Undelivered => "\nmeka accepted it and could not deliver it.".to_string(),
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
async fn notify(context: &SessionContext, conversation: &ConversationId, text: &str) {
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
    let Some(account) = context.accounts.get(channel.id().as_str()) else {
        tracing::warn!(
            conversation = %conversation,
            "not recording the notice: the channel's account is unknown"
        );
        return;
    };
    super::record_own_messages(
        &context.store,
        context.config.storage.history_retention,
        conversation,
        account,
        None,
        sent,
        None,
    )
    .await;
}

/// When each conversation was last told that something failed.
///
/// In memory only, because after a restart telling somebody again is the right answer rather than
/// the wrong one. Owned outright by the session task rather than shared behind an `Arc` the way
/// [`TypingState`] is, since nothing else here writes it; the mutex is for interior mutability
/// through the `&SessionContext` every function in this file takes.
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

/// Bring a session's permission level in line with `[session].permission`.
///
/// A session's level is fixed when it is created, so without this an operator who edits the config
/// sees no effect and no explanation. A session carried across meka 0.46 is the case that makes it
/// matter: one created at `ask` was migrated to `none` with approvals on, which denies every call
/// including `message_send`, so it cannot reply to anyone until its level changes.
///
/// Runs once per process, when the session is bound, which waits meka out because the bridge
/// comes up before meka does.
async fn reconcile_permission(context: &SessionContext, session_id: Uuid) {
    if context.permission_checked.get().is_some() {
        return;
    }
    let desired = context.config.session.permission;
    match context.meka.session(session_id).await {
        Ok(info) if info.permission.as_deref() == Some(desired.as_str()) => {
            let _ = context.permission_checked.set(());
        }
        // A row meka reports no level for lands here too, and is set rather than left: it is a
        // session this bridge did not create, so nothing has established it can reply.
        Ok(info) => {
            tracing::warn!(
                "session {} is at permission {:?} but the config says {:?}; updating it",
                session_id,
                info.permission.as_deref().unwrap_or("unrecorded"),
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

/// Return the bound meka session, creating one if this bridge has none yet.
async fn ensure_session(context: &SessionContext) -> Result<Uuid, MekaError> {
    if let Some(session_id) = context
        .store
        .session_id()
        .await
        .map_err(|error| MekaError::Decode(error.to_string()))?
    {
        reconcile_permission(context, session_id).await;
        return Ok(session_id);
    }
    let remembered = *context
        .pending_session
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let session_id = match remembered {
        Some(session_id) => session_id,
        None => {
            let session_id = context
                .meka
                .create_session(
                    context.config.session.cwd.as_deref(),
                    context.config.session.permission,
                )
                .await?;
            *context
                .pending_session
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(session_id);
            tracing::info!(session_id = %session_id, "created the meka session for this bridge");
            session_id
        }
    };
    context
        .store
        .set_session_id(session_id)
        .await
        .map_err(|error| MekaError::Decode(error.to_string()))?;
    *context
        .pending_session
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
    Ok(session_id)
}

/// Random fence marker for one envelope.
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
            matches: Vec::new(),
            sender_roles: Vec::new(),
            text: "look".to_string(),
            reply_to: None,
            edited_at: None,
            forwarded_from: None,
            group_id: None,
            notes: Vec::new(),
            attachments,
            timestamp: Utc::now(),
        }
    }

    async fn store() -> (Store, AccountId) {
        let store = Store::open_in_memory().await.expect("opens");
        let account = store
            .register_account(
                "telegram",
                "telegram",
                &crate::store::AccountIdentity {
                    platform_id: "4242".to_string(),
                    username: Some("mybot".to_string()),
                    display_name: None,
                },
                Utc::now(),
            )
            .await
            .expect("account")
            .account;
        store
            .upsert_conversation(ConversationRecord {
                address: "telegram:1".to_string(),
                channel: "telegram".to_string(),
                platform: "telegram".to_string(),
                title: Some("Alice".to_string()),
                kind: "direct".to_string(),
                created_at: Utc::now(),
                last_inbound_at: Some(Utc::now()),
                last_outbound_at: None,
            })
            .await
            .expect("conversation");
        (store, account)
    }

    #[tokio::test]
    async fn registration_stamps_a_handle_onto_every_attachment() {
        // The handle is the agent's only way to reach the file, and it has to be on the event
        // before the payload is serialized or it does not survive a restart.
        let (store, account) = store().await;
        let mut event = event_with(vec![attachment("AgACx1"), attachment("AgACx2")]);
        register_attachments(&store, &mut event, account)
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
        let (store, account) = store().await;
        let mut first = event_with(vec![attachment("AgACx1")]);
        register_attachments(&store, &mut first, account)
            .await
            .expect("registers");
        let mut second = event_with(vec![attachment("AgACx1")]);
        register_attachments(&store, &mut second, account)
            .await
            .expect("registers again");

        let first = &first;
        let second = &second;
        assert_eq!(first.attachments[0].handle, second.attachments[0].handle);
    }

    #[tokio::test]
    async fn a_registered_attachment_can_be_looked_up_by_its_handle() {
        let (store, account) = store().await;
        let mut event = event_with(vec![attachment("AgACx1")]);
        register_attachments(&store, &mut event, account)
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
        assert_eq!(record.channel, "telegram");
        assert!(record.path.is_none(), "nothing is downloaded on arrival");
    }

    #[tokio::test]
    async fn a_message_without_attachments_registers_nothing() {
        let (store, account) = store().await;
        let mut event = event_with(Vec::new());
        register_attachments(&store, &mut event, account)
            .await
            .expect("registers");
        assert!(store.attachment("1").await.expect("query").is_none());
    }

    fn lapsed(policy: Policy, dropped: u64) -> PolicyRecord {
        PolicyRecord {
            conversation: "telegram:1".to_string(),
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
            message.notes[0].contains("history_read"),
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
        assert_eq!(retry_delay(base, 4, None), Duration::from_secs(160));
        // Bounded rather than doubling forever, and bounded without panicking: this is reached with
        // whatever attempt count a row in the database happens to carry, and `Duration * u32`
        // panics on overflow rather than saturating.
        assert_eq!(retry_delay(base, 5, None), RETRY_DELAY_MAX);
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
        // taking that literally on the fourth attempt would put every post inside a few seconds,
        // which is the failure this backoff exists to prevent arriving by another road.
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

    /// A config carrying the shipped defaults, which is all the gate reads from one.
    fn gate_config() -> Config {
        Config::from_toml(
            r#"
[meka]
base_url = "http://127.0.0.1:8080"
token = "test-token"
[storage]
path = "/tmp/mekabridge-gate-tests.db"
[[channels.telegram]]
id = "telegram"
token = "123:fake"
allowed_users = [1]
"#,
            std::path::Path::new("/tmp/config.toml"),
        )
        .expect("valid config")
    }

    async fn gated(
        store: &Store,
        watches: &mut Watches,
        conversation: &str,
        text: &str,
        addressed: bool,
    ) -> (Disposition, Vec<crate::watch::WatchMatch>) {
        let mut message = event_with(Vec::new());
        message.conversation = ConversationId::parse(conversation).expect("valid");
        message.chat_kind = ChatKind::Group;
        message.text = text.to_string();
        message.addressed = addressed;
        let disposition = gate(store, &gate_config(), watches, &mut message)
            .await
            .expect("gate");
        (disposition, message.matches)
    }

    async fn muted_store(conversation: &str) -> Store {
        let store = Store::open_in_memory().await.expect("opens");
        store
            .set_policy(
                conversation,
                "telegram",
                Policy::Mute,
                None,
                None,
                Utc::now(),
            )
            .await
            .expect("mute");
        store
    }

    #[tokio::test]
    async fn a_watch_wakes_a_muted_conversation_and_says_which_one_did() {
        // The feature in one test: a room turned down to mentions only, a message nothing
        // addressed, and a rule the agent set that makes it worth a turn anyway.
        let store = muted_store("telegram:-100").await;
        store
            .add_watch(
                crate::store::NewWatch {
                    conversation: None,
                    platform: None,
                    field: crate::watch::WatchField::Text,
                    pattern: "deploy",
                    until: None,
                    reason: Some("releases"),
                },
                Utc::now(),
            )
            .await
            .expect("add");
        let mut watches = Watches::default();

        let (disposition, matched) = gated(
            &store,
            &mut watches,
            "telegram:-100",
            "the deploy is stuck",
            false,
        )
        .await;
        assert_eq!(disposition, Disposition::Deliver);
        assert_eq!(matched.len(), 1, "got {matched:?}");
        assert_eq!(matched[0].reason.as_deref(), Some("releases"));

        let (disposition, matched) = gated(
            &store,
            &mut watches,
            "telegram:-100",
            "unrelated chatter",
            false,
        )
        .await;
        assert_eq!(
            disposition,
            Disposition::Withhold,
            "a message matching nothing must still be withheld"
        );
        assert!(matched.is_empty());
    }

    #[tokio::test]
    async fn a_message_that_was_addressed_still_reports_what_it_matched() {
        // Computed before the arm that delivers on `addressed`, so a rule firing on every mention
        // is visible rather than hidden behind the mention that would have woken the agent anyway.
        let store = muted_store("telegram:-100").await;
        store
            .add_watch(
                crate::store::NewWatch {
                    conversation: None,
                    platform: None,
                    field: crate::watch::WatchField::Text,
                    pattern: "deploy",
                    until: None,
                    reason: None,
                },
                Utc::now(),
            )
            .await
            .expect("add");
        let mut watches = Watches::default();
        let (disposition, matched) = gated(
            &store,
            &mut watches,
            "telegram:-100",
            "@bot deploy now",
            true,
        )
        .await;
        assert_eq!(disposition, Disposition::Deliver);
        assert_eq!(matched.len(), 1, "got {matched:?}");
    }

    #[tokio::test]
    async fn watches_decide_nothing_where_the_policy_already_has() {
        // Consulted only under `mute`. A chat heard in full delivers the message whatever they
        // say, and a blocked one keeps nothing to match against, so running them there would be
        // work with no decision attached and a `matches` line that explained nothing.
        let store = Store::open_in_memory().await.expect("opens");
        store
            .add_watch(
                crate::store::NewWatch {
                    conversation: None,
                    platform: None,
                    field: crate::watch::WatchField::Text,
                    pattern: "deploy",
                    until: None,
                    reason: None,
                },
                Utc::now(),
            )
            .await
            .expect("add");
        let mut watches = Watches::default();

        store
            .set_policy(
                "telegram:-100",
                "telegram",
                Policy::Active,
                None,
                None,
                Utc::now(),
            )
            .await
            .expect("active");
        let (disposition, matched) = gated(
            &store,
            &mut watches,
            "telegram:-100",
            "the deploy is stuck",
            false,
        )
        .await;
        assert_eq!(disposition, Disposition::Deliver);
        assert!(matched.is_empty(), "nothing was decided by a watch here");

        store
            .set_policy(
                "telegram:-200",
                "telegram",
                Policy::Block,
                None,
                None,
                Utc::now(),
            )
            .await
            .expect("block");
        let (disposition, matched) = gated(
            &store,
            &mut watches,
            "telegram:-200",
            "the deploy is stuck",
            false,
        )
        .await;
        assert_eq!(
            disposition,
            Disposition::Discard,
            "a watch must not reach into a conversation the agent blocked"
        );
        assert!(matched.is_empty());
    }

    #[tokio::test]
    async fn a_watch_removed_stops_waking_the_agent_within_the_cache_window() {
        // The operator's way back. `mekabridge watch delete` runs in another process, so the gate
        // has to notice it without being restarted.
        let store = muted_store("telegram:-100").await;
        let outcome = store
            .add_watch(
                crate::store::NewWatch {
                    conversation: None,
                    platform: None,
                    field: crate::watch::WatchField::Text,
                    pattern: "deploy",
                    until: None,
                    reason: None,
                },
                Utc::now(),
            )
            .await
            .expect("add");
        let crate::store::WatchOutcome::Added(record) = outcome else {
            panic!("must add");
        };
        let mut watches = Watches::default();
        let (disposition, _) = gated(&store, &mut watches, "telegram:-100", "deploy", false).await;
        assert_eq!(disposition, Disposition::Deliver);

        store.remove_watch(record.id).await.expect("remove");
        let (disposition, matched) =
            gated(&store, &mut watches, "telegram:-100", "deploy", false).await;
        assert_eq!(disposition, Disposition::Withhold, "got {matched:?}");
    }
}
