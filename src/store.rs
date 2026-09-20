//! Durable state: the session binding, the accounts the bridge speaks as, the conversation address
//! book, the inbound queue, and the message history.
//!
//! The queue is the hand-off buffer between the platforms and meka's inbox. A message is written
//! before it is acknowledged to the platform, since a crash with a full buffer in memory would
//! silently swallow everything a user typed, and it stays here until meka has said the item
//! carrying it is durable on its side. Rows are claimed into a *batch*, one inbox item carrying one
//! rendered envelope; the batch row is what a retry, a reconciliation, or a restart posts again
//! under the same key, and it is where what the envelope stated once is kept until the item
//! reaches the model or provably never will.
//!
//! A row's state is a column rather than an in-memory flag so that a restart finds every hand-over
//! where it was: rows claimed but never bound into a batch go back to `pending`, and a batch is
//! posted again or asked about rather than guessed at.
//!
//! Payloads stay opaque JSON so the state machine can be tested on its own and does not change when
//! a platform adds fields.
//!
//! Three identities are kept apart. An *address* is `<channel>:<chat>[:<thread>]`, the routing
//! contract the agent and the operator use, and the only form of a conversation this API accepts.
//! An *account* is who a channel is logged in as, which changes when a bot is deleted and recreated
//! under the same channel slot. A *message* is identified by [`MessageKey`]: the account that
//! received it, the address it arrived at, the platform's message id, and which revision of it,
//! because a platform message id is only unique within the account that issued it.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    time::Instant,
};

use chrono::{DateTime, Utc};
use rusqlite::{
    OptionalExtension,
    types::{FromSql, FromSqlResult, ToSql, ToSqlOutput, ValueRef},
};
use uuid::Uuid;

use crate::watch::{MAX_WATCHES, WatchField};

/// Schema statements applied in order. The index of a statement is its schema version, tracked in
/// SQLite's `user_version`, so adding a migration means appending to this array and never editing
/// an existing entry.
const MIGRATIONS: &[&str] = &[
    include_str!("store/schema_001.sql"),
    include_str!("store/schema_002.sql"),
    include_str!("store/schema_003.sql"),
    include_str!("store/schema_004.sql"),
    include_str!("store/schema_005.sql"),
    include_str!("store/schema_006.sql"),
    include_str!("store/schema_007.sql"),
    include_str!("store/schema_008.sql"),
    include_str!("store/schema_009.sql"),
    include_str!("store/schema_010.sql"),
    include_str!("store/schema_011.sql"),
];

/// Attempts at the WAL pragma before giving up, and how long to wait between them.
///
/// Small: the window is one process finishing its own journal-mode change on a database neither has
/// opened before, which is microseconds. Bounded rather than unbounded because a genuinely stuck
/// lock should fail the start rather than hang it.
const WAL_PRAGMA_ATTEMPTS: u32 = 50;
const WAL_PRAGMA_BACKOFF: std::time::Duration = std::time::Duration::from_millis(20);

/// How long a resolved policy is reused before the store is consulted again.
///
/// The gate runs on every message, so without this a busy group pays a database round trip per
/// message to decide, almost always, to ignore it. Conversations with no policy are cached too,
/// being the common case.
///
/// Seconds rather than minutes because this is also how long an operator running `mekabridge policy
/// set` against a live daemon waits to see it take effect, and that CLI is the recovery path for a
/// policy the agent set on itself.
const POLICY_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(2);

/// Ceiling on cached policy lookups, past which the cache is emptied rather than grown.
const POLICY_CACHE_MAX_ENTRIES: usize = 4096;

/// `meta` key holding the meka session UUID this bridge instance owns.
const META_SESSION_ID: &str = "session_id";
/// `meta` key counting inbound messages shed because the queue was full, so the next envelope can
/// tell the agent that it is not seeing everything.
const META_DROPPED: &str = "dropped_messages";
/// `meta` key holding when the last turn completed, for `mekabridge status`.
const META_LAST_TURN: &str = "last_turn_at";
/// Prefix of the `meta` key holding the last feed event handled for a session, so a restart resumes
/// the feed from there. Keyed by session because ids start again on a new one.
const META_FEED_POSITION: &str = "feed_position:";

/// The `kind` recorded for a conversation nothing has arrived from yet.
const KIND_UNKNOWN: &str = "unknown";

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("database error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("database connection is closed")]
    ConnectionClosed,

    #[error("could not create directory {path}: {source}")]
    CreateDir {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("stored value for `{key}` is not usable: {message}")]
    Corrupt { key: String, message: String },

    #[error("database task failed: {0}")]
    Task(String),
}

impl From<tokio_rusqlite::Error> for StoreError {
    fn from(error: tokio_rusqlite::Error) -> Self {
        match error {
            tokio_rusqlite::Error::ConnectionClosed => Self::ConnectionClosed,
            tokio_rusqlite::Error::Close((_, source)) => Self::Sqlite(source),
            tokio_rusqlite::Error::Error(source) => Self::Sqlite(source),
            // `tokio_rusqlite::Error` is `#[non_exhaustive]`, so a future variant must not break
            // the build; it degrades to an opaque task failure instead.
            other => Self::Task(other.to_string()),
        }
    }
}

type Result<T> = std::result::Result<T, StoreError>;

/// A bot account's row id, minted by [`Store::register_account`].
///
/// A distinct type rather than a bare `i64` so it cannot be handed to something expecting a paging
/// cursor or a queue sequence number, both of which are also row ids.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AccountId(i64);

impl std::fmt::Display for AccountId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

impl ToSql for AccountId {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self.0))
    }
}

impl FromSql for AccountId {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        i64::column_result(value).map(Self)
    }
}

/// Who a channel is logged in as, as the platform reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountIdentity {
    /// The bot's own user id on the platform.
    pub platform_id: String,
    pub username: Option<String>,
    pub display_name: Option<String>,
}

/// An account a channel has been logged in as at some point.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountRecord {
    pub id: AccountId,
    pub channel: String,
    pub platform: String,
    /// Empty on the placeholder that holds rows written before accounts were recorded, whose
    /// account nothing can establish.
    pub platform_id: String,
    pub username: Option<String>,
    pub display_name: Option<String>,
    pub first_seen_at: DateTime<Utc>,
    pub last_seen_at: DateTime<Utc>,
}

impl AccountRecord {
    /// Whether this is the placeholder for rows written before accounts were recorded.
    pub fn is_placeholder(&self) -> bool {
        self.platform_id.is_empty()
    }

    /// How the account is referred to in prose: `@username` where the platform has one, else the
    /// display name, else the bare id.
    pub fn label(&self) -> String {
        self.username
            .as_ref()
            .map(|username| format!("@{username}"))
            .or_else(|| self.display_name.clone())
            .unwrap_or_else(|| self.platform_id.clone())
    }
}

/// What [`Store::register_account`] found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountRegistration {
    pub account: AccountId,
    /// The account this channel was logged in as before, when that was a different one. This is
    /// the bot-swap signal: everything recorded under it carries message ids the new account
    /// cannot act on.
    pub replaced: Option<AccountRecord>,
}

/// What identifies one platform message, and one revision of it.
///
/// The account is part of the key because a platform message id is only unique within the account
/// that issued it: a Telegram bot deleted and recreated under the same channel slot numbers its
/// private chats from 1 again, at the same addresses. `revision` is the edit time in epoch
/// milliseconds, or zero for the original, so an edit is a row of its own without being mistaken
/// for a redelivery of what it revises.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageKey<'a> {
    /// The `<channel>:<chat>[:<thread>]` address.
    pub conversation: &'a str,
    pub account: AccountId,
    /// Platform-native message id, which is what a reply or a reaction targets.
    pub message_id: &'a str,
    pub revision: i64,
}

/// A conversation the bridge has seen, which is also the set of addresses the agent may send to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConversationRecord {
    /// Canonical `<channel>:<chat>[:<thread>]` address.
    pub address: String,
    /// The channel slot, which is also the first segment of the address.
    pub channel: String,
    pub platform: String,
    /// Human-readable label: a group title, or a person's display name.
    pub title: Option<String>,
    /// `direct`, `group`, `channel`, or `unknown` for a conversation the agent messaged first,
    /// where nothing has arrived to say what shape it is.
    pub kind: String,
    pub created_at: DateTime<Utc>,
    pub last_inbound_at: Option<DateTime<Utc>>,
    pub last_outbound_at: Option<DateTime<Utc>>,
}

/// How much of a conversation reaches the agent.
///
/// The same three states both Telegram and Discord offer in their own notification settings, which
/// is deliberate: an agent reading a tool called `mute` should already know what it does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Policy {
    /// Every message wakes the agent.
    Active,
    /// Messages are received and recorded, but only one addressed to the agent wakes it. The rest
    /// stay readable through the history tools.
    Mute,
    /// Nothing gets through and nothing is kept.
    Block,
}

impl Policy {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Mute => "mute",
            Self::Block => "block",
        }
    }

    /// Parse the stored spelling. The CHECK constraint keeps anything else out of the column, so an
    /// unrecognised value means a hand-edited database and is reported rather than guessed at.
    fn parse(raw: &str) -> Option<Self> {
        match raw {
            "active" => Some(Self::Active),
            "mute" => Some(Self::Mute),
            "block" => Some(Self::Block),
            _ => None,
        }
    }
}

/// When one conversation's waiting messages arrived, as the span readiness is decided on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingWindow {
    /// The conversation's address.
    pub conversation: String,
    /// Bounds how long delivery may be deferred.
    pub oldest: DateTime<Utc>,
    /// Says whether a burst is still in progress.
    pub newest: DateTime<Utc>,
    /// When a failed batch may be offered again, latest first, or `None` when nothing here has
    /// failed.
    ///
    /// The latest rather than the earliest because the whole conversation waits: see
    /// `store/schema_005.sql` for why releasing part of one would deliver out of order.
    pub not_before: Option<DateTime<Utc>>,
}

/// What a conversation, or the whole bridge, is holding for the agent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnseenSummary {
    /// Recorded messages the agent has not been shown.
    pub count: u64,
    /// When the most recent of those was sent, or `None` when there are none.
    pub newest: Option<DateTime<Utc>>,
    /// When somebody else last said something that still stands, shown or not.
    ///
    /// Separate from [`Self::newest`] because the two answer different questions and only this one
    /// can be watched. See [`Self::marker`] for what each clause of that sentence is holding up:
    /// "somebody else" excludes the bridge's own messages, and "still stands" excludes retracted
    /// ones.
    pub latest: Option<DateTime<Utc>>,
}

impl UnseenSummary {
    /// The value a watcher compares, which moves when and only when something new was said.
    ///
    /// Deliberately not the backlog, which falls to zero every time an ordinary turn sweeps the
    /// conversation, so a watcher comparing successive readings would fire on the sweep. The newest
    /// recorded timestamp is indifferent to what has been shown, and changes on nothing but a new
    /// message.
    ///
    /// Two things move it backwards, and both are news of a kind: the author deleting the newest
    /// message, and retention pruning the last of them.
    pub fn marker(&self) -> String {
        self.latest.map_or_else(|| "never".to_string(), to_rfc3339)
    }

    /// The human and agent facing answer: how far behind, and since when.
    ///
    /// Not what a watcher should compare, for the reason given on [`Self::marker`].
    pub fn line(&self) -> String {
        match self.newest {
            Some(newest) => format!("{} unseen, newest {}", self.count, to_rfc3339(newest)),
            None => format!("{} unseen", self.count),
        }
    }
}

/// One watch: a pattern the agent set, which wakes it in a muted conversation.
///
/// The counterpart to [`PolicyRecord`] on the other side of the gate. A policy says how much of a
/// conversation reaches the agent by default; a watch is the exception it carved out of that by
/// hand.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchRecord {
    /// Row id, which is what the agent unwatches by and what the wake line names.
    pub id: i64,
    /// The conversation this is confined to, or `None` for every conversation.
    pub conversation: Option<String>,
    /// Which part of a message the pattern reads.
    pub field: WatchField,
    pub pattern: String,
    pub reason: Option<String>,
    /// When it lapses, or `None` for indefinite.
    pub until: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

/// Absence is meaningful: a conversation with no record follows the configured default for its chat
/// kind, so this type says "somebody ruled on this one" rather than "this one has a policy".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyRecord {
    /// The conversation's address.
    pub conversation: String,
    pub policy: Policy,
    /// When it lapses, or `None` for indefinite. On expiry the record is removed and the
    /// conversation returns to the configured default rather than to `active`, since the default
    /// is what it would have had if nobody had ruled on it.
    pub until: Option<DateTime<Utc>>,
    pub reason: Option<String>,
    /// Messages discarded since the record was written. Only ever non-zero under
    /// [`Policy::Block`], because that is the only policy that keeps nothing: under `mute` the
    /// messages are recorded and the count of what went unseen is derived from them.
    pub dropped: u64,
    pub created_at: DateTime<Utc>,
}

impl PolicyRecord {
    /// Whether this record has lapsed as of `now`.
    pub fn expired(&self, now: DateTime<Utc>) -> bool {
        self.until.is_some_and(|until| until <= now)
    }
}

/// Cached policy lookups, shared by every clone of a [`Store`].
///
/// Holds misses as well as hits: a conversation with no policy is the common case and the one the
/// gate asks about most.
#[derive(Default)]
struct PolicyCache {
    entries: HashMap<String, (Option<PolicyRecord>, Instant)>,
}

/// The watches as last read, held whole.
///
/// Not keyed by conversation, unlike [`PolicyCache`]: an unscoped watch applies everywhere, so
/// there is no per-conversation answer to cache, and the gate wants the whole set anyway.
struct WatchCache {
    rows: Arc<Vec<WatchRecord>>,
    fetched_at: Instant,
}

/// One message as the bridge recorded it, which is not the same as one it delivered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageRecord {
    /// Row id, which is also the paging cursor.
    ///
    /// Assigned by the store and ignored by [`Store::record_message`], so it is zero on a record
    /// being written and meaningful only on one read back. It exists because a timestamp cannot
    /// serve as a cursor here: Telegram stamps to the second, so a burst shares one, and no
    /// timestamp comparison can express "everything before this particular message" when three of
    /// them claim the same instant.
    pub id: i64,
    /// The conversation's address.
    pub conversation: String,
    /// The account this was received through, or sent from.
    pub account: AccountId,
    /// Platform-native id, which is what a reply or a reaction targets.
    pub message_id: String,
    /// Which revision of the message this row holds: the edit time in epoch milliseconds, or zero
    /// for the original wording.
    pub revision: i64,
    pub sender_id: Option<String>,
    pub sender_name: String,
    pub text: String,
    /// Descriptor lines for content with no text of its own, so a shared location does not read
    /// back as a blank line from somebody.
    pub notes: Option<String>,
    /// Handles for the files this message brought, so something found in history can be opened.
    ///
    /// Read back from the attachment registry by the message's own key, which is why
    /// [`Store::record_message`] ignores it: the files are registered before the message is, so
    /// their handles travel with the queued payload.
    pub attachments: Vec<String>,
    pub addressed: bool,
    pub seen: bool,
    /// Whether the bridge sent this rather than received it.
    ///
    /// Recorded as its own column rather than inferred from [`Self::sender_id`], so reading a row
    /// back never requires knowing which account the bot holds on that platform.
    pub own: bool,
    /// The meka session that sent an outbound message, and `None` on everything received.
    pub session_id: Option<String>,
    /// When the platform reported this message deleted.
    ///
    /// The row and its text are kept so the agent can find out that something it may already have
    /// acted on was retracted. Only platforms that report deletions ever set it.
    pub deleted_at: Option<DateTime<Utc>>,
    /// When a revision of this same message was recorded, making this row the older wording.
    pub superseded_at: Option<DateTime<Utc>>,
    pub timestamp: DateTime<Utc>,
    /// Whether this went through an account the channel no longer uses, so its `message_id` cannot
    /// be replied to, reacted to, edited, or deleted from the account now behind the address.
    ///
    /// Derived on read and ignored by [`Store::record_message`]. Never set on a row held by the
    /// placeholder account, whose real account is unknown rather than known to be gone.
    pub previous_account: bool,
}

impl MessageRecord {
    pub fn key(&self) -> MessageKey<'_> {
        MessageKey {
            conversation: &self.conversation,
            account: self.account,
            message_id: &self.message_id,
            revision: self.revision,
        }
    }
}

/// A message waiting to be handed to the agent, and, once claimed, the hand-over it became.
///
/// One row is one inbox item. It outlives the request that posts it, so a retry after an ambiguous
/// failure, or after a restart on either side, posts the same [`Self::key`] and the same
/// [`Self::body`], and meka answers with the item it already made and that item's current state,
/// which is also how a hand-over whose feed events were missed is reconciled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueuedMessage {
    pub seq: i64,
    /// The conversation's address.
    pub conversation: String,
    pub account: AccountId,
    pub message_id: String,
    pub revision: i64,
    pub payload: String,
    pub received_at: DateTime<Utc>,
    pub state: QueueState,
    /// The `Idempotency-Key` the item is posted under, minted once when the row is held.
    pub key: Option<String>,
    /// The rendered item, byte for byte. Dropped once the row reaches [`QueueState::Failed`],
    /// where it would only be a second copy of [`Self::payload`] that nothing will ever post.
    pub body: Option<String>,
    /// The meka session the item is, or will be, posted to.
    pub session_id: Option<Uuid>,
    /// meka's id for the item, from the 202.
    pub item_id: Option<String>,
    /// The dropped-message count this item reported, owed back if the item never delivers.
    pub dropped: u64,
    /// Hand-overs this row has been part of.
    pub attempts: u32,
    /// Posts made of the current item, successful or not.
    pub posts: u32,
    /// When the row may be claimed again, while it is pending, or posted again, while it is in
    /// flight.
    pub not_before: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    /// When the item was rendered.
    pub held_at: Option<DateTime<Utc>>,
    /// When meka accepted it.
    pub posted_at: Option<DateTime<Utc>>,
}

impl QueuedMessage {
    pub fn key(&self) -> MessageKey<'_> {
        MessageKey {
            conversation: &self.conversation,
            account: self.account,
            message_id: &self.message_id,
            revision: self.revision,
        }
    }
}

/// What happened to an [`Store::enqueue`] call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnqueueOutcome {
    Queued,
    /// The platform redelivered a message already in the queue. Not an error: after a crash,
    /// Telegram replays updates whose offset was never committed.
    Duplicate,
    /// The queue is at `max_depth`. The message is dropped and counted so the agent can be told.
    Dropped,
}

/// A watch to record, as [`Store::add_watch`] takes it.
///
/// Grouped rather than passed as six arguments, the way [`MessageKey`] groups the four fields that
/// identify a message: every one of these is part of one rule, and half of them are `Option`s that
/// would otherwise be told apart only by position.
#[derive(Debug, Clone, Copy)]
pub struct NewWatch<'a> {
    /// The conversation to confine it to, or `None` for every conversation.
    pub conversation: Option<&'a str>,
    /// Needed only alongside `conversation`, to mint the conversation row if nothing has arrived
    /// from it yet.
    pub platform: Option<&'a str>,
    pub field: WatchField,
    pub pattern: &'a str,
    pub until: Option<DateTime<Utc>>,
    pub reason: Option<&'a str>,
}

/// What happened to a [`Store::add_watch`] call.
///
/// Three outcomes rather than a record and an error, following [`EnqueueOutcome`]: the two that
/// are not a plain insert are both things the caller has to say out loud, and neither is a fault.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchOutcome {
    Added(WatchRecord),
    /// The same rule was already standing. Handed back rather than duplicated, so an agent
    /// syncing a list it keeps elsewhere can call this for every rule and be told what changed.
    Existing(WatchRecord),
    /// The deployment already holds [`crate::watch::MAX_WATCHES`], naming how many.
    AtCapacity(usize),
}

/// Where a queue row is in its hand-over.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueState {
    /// Waiting to be claimed.
    Pending,
    /// Claimed and rendered, and meka has not accepted it yet.
    InFlight,
    /// meka has the item; the feed says what becomes of it.
    Posted,
    /// The model read it.
    Done,
    /// It will never reach the model, and no offers are left.
    Failed,
}

impl QueueState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::InFlight => "in_flight",
            Self::Posted => "posted",
            Self::Done => "done",
            Self::Failed => "failed",
        }
    }

    fn parse(raw: &str) -> Option<Self> {
        match raw {
            "pending" => Some(Self::Pending),
            "in_flight" => Some(Self::InFlight),
            "posted" => Some(Self::Posted),
            "done" => Some(Self::Done),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }
}

/// What became of a row offered again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Offer {
    /// Back in the queue, for a hand-over of its own under a new key.
    Retrying,
    /// Out of offers, and now `failed`. This is what an operator needs to hear about: the agent was
    /// never handed it, and nothing will hand it over now. What is still possible is
    /// [`Store::mark_unseen`], which puts it back among what the agent is owed.
    Exhausted(Box<QueuedMessage>),
    /// Something else had already closed it, so nothing changed.
    Closed,
}

/// A rendered item, and what it takes with it when its row is held.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HandOver<'a> {
    /// The `Idempotency-Key` it is posted under, minted once and never again, so every post of
    /// this row is the same request to meka.
    pub key: &'a str,
    /// The rendered item, byte for byte. meka refuses the same key with other words.
    pub body: &'a str,
    /// The meka session it is, or will be, posted to. meka scopes keys per session.
    pub session_id: Uuid,
    /// The dropped-message count it states, which comes off the counter as it is stored.
    pub dropped: u64,
    /// The conversation backlog it states, on the item stating one.
    pub accounted: Option<Backlog>,
}

/// One conversation's backlog as an item reported it: the two bounds that describe exactly what
/// [`Store::take_unseen`] counted.
///
/// Both are needed. The timestamp is the ceiling asked about and the id is the high-water mark of
/// the rows that answered; either alone marks a set the agent was never shown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Backlog {
    pub watermark: i64,
    pub through: DateTime<Utc>,
}

/// What a restart found in the queue.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Recovered {
    /// Rows claimed but never rendered, returned to `pending`.
    pub orphaned: usize,
    /// Rows meka never accepted, which are posted again under their own key.
    pub unposted: usize,
    /// Rows meka has, whose fate the feed's replay or a reconciliation settles.
    pub posted: usize,
}

/// Queue row counts by state, for `mekabridge status`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueueStats {
    pub pending: u64,
    /// Rendered, and not yet accepted by meka.
    pub in_flight: u64,
    /// With meka, waiting for the feed to say what became of them.
    pub posted: u64,
    pub done: u64,
    pub failed: u64,
}

/// An attachment a platform holds, as the bridge records it on arrival.
///
/// Nothing is downloaded at this point. `path` is set later, and only if the agent asks for the
/// file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachmentRecord {
    /// The conversation's address.
    pub conversation: String,
    pub account: AccountId,
    pub message_id: String,
    pub revision: i64,
    /// Where in its message the file sits, so a redelivery finds the handle already issued.
    pub position: u32,
    pub kind: String,
    /// Platform-native reference used to fetch the file.
    pub file_ref: String,
    pub thumb_ref: Option<String>,
    pub file_name: Option<String>,
    pub media_type: Option<String>,
    pub bytes: Option<u64>,
    pub path: Option<PathBuf>,
    pub created_at: DateTime<Utc>,
}

/// A registered attachment, as read back by handle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredAttachment {
    pub handle: String,
    /// The conversation's address.
    pub conversation: String,
    /// The channel that can fetch it.
    pub channel: String,
    pub kind: String,
    pub file_ref: String,
    pub thumb_ref: Option<String>,
    pub file_name: Option<String>,
    pub media_type: Option<String>,
    pub bytes: Option<u64>,
    /// Where the file was written, once the agent has downloaded it.
    pub path: Option<PathBuf>,
}

/// Handle to the SQLite database. Cheap to clone; all clones share one connection thread.
#[derive(Clone)]
pub struct Store {
    connection: tokio_rusqlite::Connection,
    /// Shared across clones, so a policy written through any handle is seen by every other.
    policies: Arc<RwLock<PolicyCache>>,
    /// The watches in force, with when they were read. Shared for the same reason, and held whole
    /// rather than per conversation because the gate matches the entire set against every message.
    watches: Arc<RwLock<Option<WatchCache>>>,
}

impl Store {
    /// Open (creating if needed) the database at `path` and bring the schema up to date.
    pub async fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent).map_err(|source| StoreError::CreateDir {
                path: parent.to_path_buf(),
                source,
            })?;
        }
        let connection = tokio_rusqlite::Connection::open(path).await?;
        Self::prepare(connection).await
    }

    /// Open an in-memory database, for tests.
    pub async fn open_in_memory() -> Result<Self> {
        let connection = tokio_rusqlite::Connection::open_in_memory().await?;
        Self::prepare(connection).await
    }

    async fn prepare(connection: tokio_rusqlite::Connection) -> Result<Self> {
        connection
            .call(|connection| {
                // `busy_timeout` first, so everything after it waits rather than failing. WAL lets
                // `mekabridge status` read while the daemon writes.
                connection.pragma_update(None, "busy_timeout", 5_000)?;
                connection.pragma_update(None, "foreign_keys", "ON")?;
                // Retried by hand, because the busy handler `busy_timeout` installs is not
                // consulted for a journal-mode change: it takes an exclusive lock
                // and returns `SQLITE_BUSY` immediately if anyone else holds one.
                // Two processes opening a *new* database at the same moment
                // therefore raced here and one failed to start, which is exactly the
                // upgrade the migration loop below was hardened for. Only the first open of a given
                // file can collide; against an existing WAL database the pragma is a no-op.
                let mut attempt = 0;
                loop {
                    match connection.pragma_update(None, "journal_mode", "WAL") {
                        Ok(()) => break,
                        Err(error) if attempt < WAL_PRAGMA_ATTEMPTS => {
                            tracing::debug!("retrying the WAL pragma: {}", error);
                            attempt += 1;
                            std::thread::sleep(WAL_PRAGMA_BACKOFF);
                        }
                        Err(error) => return Err(error),
                    }
                }

                let version: u32 =
                    connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
                let mut applied = version as usize;
                if applied > MIGRATIONS.len() {
                    // Written by a newer build. Running against a schema this code does not know is
                    // worse than refusing: the loop below simply does not execute, so it would open
                    // silently and query tables whose shape has moved underneath it.
                    return Err(rusqlite::Error::SqliteFailure(
                        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                        Some(format!(
                            "the database is at schema version {applied}, newer than the {} this \
                             build knows. It was written by a later mekabridge; upgrade rather \
                             than rolling back.",
                            MIGRATIONS.len()
                        )),
                    ));
                }
                while applied < MIGRATIONS.len() {
                    let statement = MIGRATIONS.get(applied).copied().unwrap_or_default();
                    // The statements and the version bump go together or not at all.
                    // `execute_batch` is not transactional on its own, and
                    // neither was the `pragma_update` after it, so a crash in
                    // either place left a schema that was half applied and a version
                    // saying it had not been. Every later start then failed on the work already
                    // done -- `duplicate column name` -- and the bridge never ran again without
                    // somebody editing the database by hand. SQLite's DDL is transactional, so this
                    // costs nothing.
                    let transaction = connection
                        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                    // Re-read inside the lock. The version was last read before this loop, so two
                    // processes opening the database at the same moment after an upgrade both saw
                    // the old one; the transaction serialises them but the loser still ran a batch
                    // the winner had already committed, and failed on `duplicate column name` --
                    // the very error this transaction exists to prevent. systemd starting the
                    // daemon while an operator runs `doctor` is enough to reach it.
                    let current: u32 =
                        transaction.pragma_query_value(None, "user_version", |row| row.get(0))?;
                    if current as usize > applied {
                        applied = current as usize;
                        continue;
                    }
                    transaction.execute_batch(statement)?;
                    transaction.pragma_update(None, "user_version", (applied + 1) as i64)?;
                    transaction.commit()?;
                    applied += 1;
                }
                Ok(())
            })
            .await?;
        Ok(Self {
            connection,
            policies: Arc::default(),
            watches: Arc::default(),
        })
    }

    /// Flush the WAL so a restart does not pay replay cost.
    pub async fn checkpoint(&self) -> Result<()> {
        self.connection
            .call(|connection| {
                connection.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    async fn meta_get(&self, key: &'static str) -> Result<Option<String>> {
        let value = self
            .connection
            .call(move |connection| {
                connection
                    .query_row("SELECT value FROM meta WHERE key = ?1", [key], |row| {
                        row.get::<_, String>(0)
                    })
                    .optional()
            })
            .await?;
        Ok(value)
    }

    async fn meta_set(&self, key: &'static str, value: String) -> Result<()> {
        self.connection
            .call(move |connection| {
                connection.execute(
                    "INSERT INTO meta (key, value) VALUES (?1, ?2)
                     ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                    rusqlite::params![key, value],
                )?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    /// The meka session this bridge owns, if one has been created.
    pub async fn session_id(&self) -> Result<Option<Uuid>> {
        let Some(raw) = self.meta_get(META_SESSION_ID).await? else {
            return Ok(None);
        };
        Uuid::parse_str(&raw)
            .map(Some)
            .map_err(|error| StoreError::Corrupt {
                key: META_SESSION_ID.to_string(),
                message: format!("{raw:?} is not a UUID: {error}"),
            })
    }

    /// Bind this bridge to `session_id`.
    pub async fn set_session_id(&self, session_id: Uuid) -> Result<()> {
        self.meta_set(META_SESSION_ID, session_id.to_string()).await
    }

    /// Forget the session binding, and where its feed had been read to. The next start creates a
    /// fresh session.
    pub async fn clear_session_id(&self) -> Result<()> {
        self.connection
            .call(|connection| {
                let transaction = connection
                    .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                transaction.execute("DELETE FROM meta WHERE key = ?1", [META_SESSION_ID])?;
                transaction.execute(
                    "DELETE FROM meta WHERE substr(key, 1, ?2) = ?1",
                    rusqlite::params![META_FEED_POSITION, META_FEED_POSITION.len()],
                )?;
                transaction.commit()?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    /// The last feed event handled for `session_id`, so a restart resumes the feed from there.
    pub async fn feed_position(&self, session_id: Uuid) -> Result<Option<u64>> {
        let key = format!("{META_FEED_POSITION}{session_id}");
        let raw = self
            .connection
            .call({
                let key = key.clone();
                move |connection| {
                    connection
                        .query_row("SELECT value FROM meta WHERE key = ?1", [key], |row| {
                            row.get::<_, String>(0)
                        })
                        .optional()
                }
            })
            .await?;
        raw.map(|raw| {
            raw.parse::<u64>().map_err(|error| StoreError::Corrupt {
                key: key.clone(),
                message: format!("{raw:?} is not an event id: {error}"),
            })
        })
        .transpose()
    }

    /// Record the last feed event handled for `session_id`.
    pub async fn set_feed_position(&self, session_id: Uuid, id: u64) -> Result<()> {
        let key = format!("{META_FEED_POSITION}{session_id}");
        self.connection
            .call(move |connection| {
                connection.execute(
                    "INSERT INTO meta (key, value) VALUES (?1, ?2)
                     ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                    rusqlite::params![key, id.to_string()],
                )?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    /// When the last turn completed, if one ever has.
    pub async fn last_turn_at(&self) -> Result<Option<DateTime<Utc>>> {
        let Some(raw) = self.meta_get(META_LAST_TURN).await? else {
            return Ok(None);
        };
        DateTime::parse_from_rfc3339(&raw)
            .map(|value| Some(value.with_timezone(&Utc)))
            .map_err(|error| StoreError::Corrupt {
                key: META_LAST_TURN.to_string(),
                message: format!("{raw:?} is not an RFC 3339 timestamp: {error}"),
            })
    }

    /// Record that a turn just completed.
    pub async fn mark_turn_completed(&self, at: DateTime<Utc>) -> Result<()> {
        self.meta_set(META_LAST_TURN, to_rfc3339(at)).await
    }

    /// Count a message that was shed because the queue was full.
    ///
    /// One statement rather than a read followed by a write, so two callers counting at once cannot
    /// lose each other's tally.
    pub async fn note_dropped(&self, count: u64) -> Result<()> {
        let count = i64::try_from(count).unwrap_or(i64::MAX);
        self.connection
            .call(move |connection| {
                connection.execute(
                    "INSERT INTO meta (key, value) VALUES (?1, ?2)
                     ON CONFLICT(key) DO UPDATE
                         SET value = CAST(CAST(meta.value AS INTEGER) + excluded.value AS TEXT)",
                    rusqlite::params![META_DROPPED, count],
                )?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    /// How many messages have been shed since an item last reported it.
    ///
    /// Read only. What an item reports comes off the counter when that item is stored, in
    /// [`Self::hold`], so a crash between rendering and storing loses the item and keeps the count
    /// rather than the reverse.
    pub async fn peek_dropped(&self) -> Result<u64> {
        Ok(self
            .meta_get(META_DROPPED)
            .await?
            .and_then(|raw| raw.parse().ok())
            .unwrap_or(0))
    }

    /// Record which account `channel` is logged in as, and return its id.
    ///
    /// Called once per channel at every start, which is what keeps `last_seen_at` meaningful: the
    /// real account seen most recently is the one currently behind the channel, and a row from any
    /// other is one whose message ids the running bot cannot act on. The registration reports the
    /// account it displaced, if any, so the swap can be said out loud.
    pub async fn register_account(
        &self,
        channel: &str,
        platform: &str,
        identity: &AccountIdentity,
        at: DateTime<Utc>,
    ) -> Result<AccountRegistration> {
        let channel = channel.to_string();
        let platform = platform.to_string();
        let identity = identity.clone();
        let at = to_rfc3339(at);
        let registration = self
            .connection
            .call(move |connection| {
                let transaction = connection
                    .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                let previous = transaction
                    .query_row(
                        &format!("SELECT {ACCOUNT_COLUMNS} FROM accounts WHERE {ACCOUNT_CURRENT}"),
                        [&channel],
                        row_to_account,
                    )
                    .optional()?;
                transaction.execute(
                    "INSERT INTO accounts
                         (channel, platform, platform_id, username, display_name, first_seen_at,
                          last_seen_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6)
                     ON CONFLICT(channel, platform_id) DO UPDATE SET
                         platform = excluded.platform,
                         username = excluded.username,
                         display_name = excluded.display_name,
                         last_seen_at = excluded.last_seen_at",
                    rusqlite::params![
                        channel,
                        platform,
                        identity.platform_id,
                        identity.username,
                        identity.display_name,
                        at,
                    ],
                )?;
                let account = transaction.query_row(
                    "SELECT id FROM accounts WHERE channel = ?1 AND platform_id = ?2",
                    rusqlite::params![channel, identity.platform_id],
                    |row| row.get::<_, AccountId>(0),
                )?;
                transaction.commit()?;
                Ok(AccountRegistration {
                    account,
                    replaced: previous
                        .filter(|previous| previous.platform_id != identity.platform_id),
                })
            })
            .await?;
        Ok(registration)
    }

    /// The account `channel` is currently logged in as, if a real one has ever been recorded.
    pub async fn current_account(&self, channel: &str) -> Result<Option<AccountRecord>> {
        let channel = channel.to_string();
        let record = self
            .connection
            .call(move |connection| {
                connection
                    .query_row(
                        &format!("SELECT {ACCOUNT_COLUMNS} FROM accounts WHERE {ACCOUNT_CURRENT}"),
                        [channel],
                        row_to_account,
                    )
                    .optional()
            })
            .await?;
        Ok(record)
    }

    /// Every account ever recorded, by channel and then most recently seen first.
    pub async fn accounts(&self) -> Result<Vec<AccountRecord>> {
        let records = self
            .connection
            .call(|connection| {
                let mut statement = connection.prepare(&format!(
                    "SELECT {ACCOUNT_COLUMNS} FROM accounts
                     ORDER BY channel, last_seen_at DESC, id DESC"
                ))?;
                let rows = statement.query_map([], row_to_account)?;
                rows.collect::<std::result::Result<Vec<_>, _>>()
            })
            .await?;
        Ok(records)
    }

    /// Insert or refresh a conversation. `created_at` is preserved across updates so the address
    /// book keeps a stable notion of when a contact was first seen.
    pub async fn upsert_conversation(&self, record: ConversationRecord) -> Result<()> {
        self.connection
            .call(move |connection| {
                connection.execute(
                    "INSERT INTO conversations
                         (address, channel, platform, title, kind, created_at, last_inbound_at,
                          last_outbound_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                     ON CONFLICT(address) DO UPDATE SET
                         platform = excluded.platform,
                         title = COALESCE(excluded.title, conversations.title),
                         kind = excluded.kind,
                         last_inbound_at = COALESCE(
                             excluded.last_inbound_at, conversations.last_inbound_at),
                         last_outbound_at = COALESCE(
                             excluded.last_outbound_at, conversations.last_outbound_at)",
                    rusqlite::params![
                        record.address,
                        record.channel,
                        record.platform,
                        record.title,
                        record.kind,
                        to_rfc3339(record.created_at),
                        record.last_inbound_at.map(to_rfc3339),
                        record.last_outbound_at.map(to_rfc3339),
                    ],
                )?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    /// Look up one conversation by address.
    pub async fn conversation(&self, address: &str) -> Result<Option<ConversationRecord>> {
        let address = address.to_string();
        let record = self
            .connection
            .call(move |connection| {
                connection
                    .query_row(
                        &format!(
                            "SELECT {CONVERSATION_COLUMNS} FROM conversations WHERE address = ?1"
                        ),
                        [address],
                        row_to_conversation,
                    )
                    .optional()
            })
            .await?;
        Ok(record)
    }

    /// List conversations, most recently active first. `channel` filters to one channel instance.
    pub async fn list_conversations(
        &self,
        channel: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ConversationRecord>> {
        let channel = channel.map(str::to_string);
        let limit = limit.min(i64::MAX as usize) as i64;
        let records = self
            .connection
            .call(move |connection| {
                let mut statement = connection.prepare(&format!(
                    "SELECT {CONVERSATION_COLUMNS}
                     FROM conversations
                     WHERE (?1 IS NULL OR channel = ?1)
                     ORDER BY COALESCE(last_inbound_at, last_outbound_at, created_at) DESC
                     LIMIT ?2"
                ))?;
                let rows =
                    statement.query_map(rusqlite::params![channel, limit], row_to_conversation)?;
                rows.collect::<std::result::Result<Vec<_>, _>>()
            })
            .await?;
        Ok(records)
    }

    /// Stamp a conversation as having just received an outbound message, registering it first if
    /// the agent messaged somewhere the bridge has never heard from.
    ///
    /// Only the timestamp moves on an existing row. The kind is a placeholder standing in for a
    /// chat nothing is known about, so letting it overwrite a real one would discard what an
    /// inbound message already established; see [`Store::upsert_conversation`] for the inbound
    /// direction, which does know the title and kind and is allowed to update them.
    pub async fn touch_outbound(
        &self,
        address: &str,
        platform: &str,
        at: DateTime<Utc>,
    ) -> Result<()> {
        let address = address.to_string();
        let platform = platform.to_string();
        let at = to_rfc3339(at);
        self.connection
            .call(move |connection| {
                connection.execute(
                    "INSERT INTO conversations
                         (address, channel, platform, title, kind, created_at, last_inbound_at,
                          last_outbound_at)
                     VALUES (?1, ?2, ?3, NULL, ?4, ?5, NULL, ?5)
                     ON CONFLICT(address) DO UPDATE SET last_outbound_at = excluded.last_outbound_at",
                    rusqlite::params![
                        &address,
                        channel_of(&address),
                        platform,
                        KIND_UNKNOWN,
                        at
                    ],
                )?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    /// Rule on a conversation until `until`, or indefinitely when it is `None`.
    ///
    /// The conversation need not have written yet: ruling on one pre-emptively is legitimate, so a
    /// row is minted for it with what the address says and `platform` from the caller, and the
    /// first message from there fills in the rest.
    ///
    /// Re-ruling on a conversation resets the drop count, so the tally the agent is eventually
    /// shown belongs to the decision it is being told about rather than to every decision this chat
    /// has ever had.
    ///
    /// [`Policy::Active`] is written as a record rather than by removing one. The two are
    /// different: a record says "heard in full whatever the default is", and removing it says
    /// "follow the default", which for a group is `mute`.
    pub async fn set_policy(
        &self,
        address: &str,
        platform: &str,
        policy: Policy,
        until: Option<DateTime<Utc>>,
        reason: Option<&str>,
        at: DateTime<Utc>,
    ) -> Result<()> {
        let address = address.to_string();
        let platform = platform.to_string();
        let reason = reason.map(str::to_string);
        self.connection
            .call({
                let address = address.clone();
                move |connection| {
                    let transaction = connection
                        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                    transaction.execute(
                        "INSERT INTO conversations
                             (address, channel, platform, kind, created_at)
                         VALUES (?1, ?2, ?3, ?4, ?5)
                         ON CONFLICT(address) DO NOTHING",
                        rusqlite::params![
                            &address,
                            channel_of(&address),
                            platform,
                            KIND_UNKNOWN,
                            to_rfc3339(at)
                        ],
                    )?;
                    transaction.execute(
                        &format!(
                            "INSERT INTO conversation_policy
                                 (conversation_id, mode, until, reason, dropped, created_at)
                             VALUES ({CONVERSATION_ID_OF}, ?2, ?3, ?4, 0, ?5)
                             ON CONFLICT(conversation_id) DO UPDATE SET
                                 mode = excluded.mode,
                                 until = excluded.until,
                                 reason = excluded.reason,
                                 dropped = 0,
                                 created_at = excluded.created_at"
                        ),
                        rusqlite::params![
                            &address,
                            policy.as_str(),
                            until.map(to_rfc3339),
                            reason,
                            to_rfc3339(at),
                        ],
                    )?;
                    transaction.commit()?;
                    Ok(())
                }
            })
            .await?;
        self.forget_cached_policy(&address);
        Ok(())
    }

    /// Remove an explicit decision, returning the conversation to the configured default. Returns
    /// whether one was actually in place.
    pub async fn clear_policy(&self, address: &str) -> Result<bool> {
        let address = address.to_string();
        let removed = self
            .connection
            .call({
                let address = address.clone();
                move |connection| {
                    connection.execute(
                        &format!(
                            "DELETE FROM conversation_policy
                             WHERE conversation_id = {CONVERSATION_ID_OF}"
                        ),
                        [address],
                    )
                }
            })
            .await?;
        self.forget_cached_policy(&address);
        Ok(removed > 0)
    }

    /// The explicit decision on a conversation, expired or not.
    ///
    /// Expiry is the caller's judgement rather than a filter here, because an expired record is
    /// exactly what carries the drop count the agent needs to be told about.
    ///
    /// Served from a cache with a `POLICY_CACHE_TTL` window, since this runs on every inbound
    /// message. The policy itself is exact, because every write invalidates;
    /// [`PolicyRecord::dropped`] is not, because counting a drop deliberately does not. Use
    /// [`Store::expire_policy`] or [`Store::list_policies`] wherever the tally is what matters.
    pub async fn policy(&self, address: &str) -> Result<Option<PolicyRecord>> {
        if let Some(cached) = self.cached_policy(address) {
            return Ok(cached);
        }
        let record = self.read_policy(address).await?;
        self.cache_policy(address, record.clone());
        Ok(record)
    }

    async fn read_policy(&self, address: &str) -> Result<Option<PolicyRecord>> {
        let address = address.to_string();
        let record = self
            .connection
            .call(move |connection| {
                connection
                    .query_row(
                        &format!("SELECT {POLICY_COLUMNS} FROM {POLICY_FROM} WHERE c.address = ?1"),
                        [address],
                        row_to_policy,
                    )
                    .optional()
            })
            .await?;
        Ok(record)
    }

    /// Remove a lapsed decision and return what it was, in one transaction.
    ///
    /// Separate from [`Store::clear_policy`] because the drop count it returns is what the agent is
    /// told about, and reading it through the cache could under-report by up to the cache window.
    /// Doing both under one transaction also means a message arriving alongside the expiry cannot
    /// be counted into a record that is already gone.
    pub async fn expire_policy(&self, address: &str) -> Result<Option<PolicyRecord>> {
        let address = address.to_string();
        let record = self
            .connection
            .call({
                let address = address.clone();
                move |connection| {
                    let transaction = connection
                        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                    let record = transaction
                        .query_row(
                            &format!(
                                "SELECT {POLICY_COLUMNS} FROM {POLICY_FROM} WHERE c.address = ?1"
                            ),
                            [&address],
                            row_to_policy,
                        )
                        .optional()?;
                    transaction.execute(
                        &format!(
                            "DELETE FROM conversation_policy
                             WHERE conversation_id = {CONVERSATION_ID_OF}"
                        ),
                        [&address],
                    )?;
                    transaction.commit()?;
                    Ok(record)
                }
            })
            .await?;
        self.forget_cached_policy(&address);
        Ok(record)
    }

    /// Every explicit decision currently recorded, oldest first.
    pub async fn list_policies(&self) -> Result<Vec<PolicyRecord>> {
        let records = self
            .connection
            .call(move |connection| {
                let mut statement = connection.prepare(&format!(
                    "SELECT {POLICY_COLUMNS} FROM {POLICY_FROM} ORDER BY p.created_at"
                ))?;
                let rows = statement.query_map([], row_to_policy)?;
                rows.collect::<std::result::Result<Vec<_>, _>>()
            })
            .await?;
        Ok(records)
    }

    /// Record a watch, hand back the one already standing for the same rule, or refuse for want of
    /// room.
    ///
    /// Identity is the whole rule, `(conversation, field, pattern)`: the same words pointed at a
    /// different field are a different watch. Deliberately not a `UNIQUE` index, because SQLite
    /// treats NULLs as distinct and one would constrain the scoped rows while letting duplicate
    /// global ones through.
    ///
    /// `platform` is needed only when `conversation` is given, to mint the conversation row the
    /// same way [`Store::set_policy`] does when ruling on a chat nothing has arrived from yet.
    ///
    /// Lapsed rows are deleted here rather than swept on a timer, which keeps the ceiling honest:
    /// a watch that has expired is not one of the [`MAX_WATCHES`] a deployment is allowed.
    pub async fn add_watch(&self, watch: NewWatch<'_>, at: DateTime<Utc>) -> Result<WatchOutcome> {
        let NewWatch {
            conversation,
            platform,
            field,
            pattern,
            until,
            reason,
        } = watch;
        let conversation = conversation.map(str::to_string);
        let platform = platform.map(str::to_string);
        let pattern = pattern.to_string();
        let reason = reason.map(str::to_string);
        let outcome = self
            .connection
            .call(move |connection| {
                let transaction = connection
                    .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                transaction.execute(
                    "DELETE FROM watches WHERE until IS NOT NULL AND until <= ?1",
                    [to_rfc3339(at)],
                )?;
                if let Some(address) = &conversation {
                    transaction.execute(
                        "INSERT INTO conversations (address, channel, platform, kind, created_at)
                         VALUES (?1, ?2, ?3, ?4, ?5)
                         ON CONFLICT(address) DO NOTHING",
                        rusqlite::params![
                            address,
                            channel_of(address),
                            platform.as_deref().unwrap_or(""),
                            KIND_UNKNOWN,
                            to_rfc3339(at)
                        ],
                    )?;
                }
                // `IS` rather than `=`, and the same subquery whether or not a conversation was
                // given: looking up a NULL address matches no row, so the subquery is NULL and
                // this finds the unscoped watches, which is exactly the case a `= ?` would miss.
                let existing = transaction
                    .query_row(
                        &format!(
                            "SELECT {WATCH_COLUMNS} FROM {WATCH_FROM}
                             WHERE w.pattern = ?1 AND w.field = ?2
                               AND w.conversation_id IS {CONVERSATION_ID_OF_3}"
                        ),
                        rusqlite::params![&pattern, field.as_str(), &conversation],
                        row_to_watch,
                    )
                    .optional()?;
                if let Some(record) = existing {
                    transaction.commit()?;
                    return Ok(WatchOutcome::Existing(record));
                }
                let live: i64 =
                    transaction.query_row("SELECT COUNT(*) FROM watches", [], |row| row.get(0))?;
                if live >= MAX_WATCHES as i64 {
                    // Decided inside the transaction, so the count cannot move between the check
                    // and the insert. The lapsed rows were deleted above, so this counts only
                    // watches that are actually standing.
                    transaction.commit()?;
                    return Ok(WatchOutcome::AtCapacity(live.max(0) as usize));
                }
                transaction.execute(
                    &format!(
                        "INSERT INTO watches
                             (conversation_id, field, pattern, reason, until, created_at)
                         VALUES ({CONVERSATION_ID_OF}, ?2, ?3, ?4, ?5, ?6)"
                    ),
                    rusqlite::params![
                        &conversation,
                        field.as_str(),
                        &pattern,
                        reason,
                        until.map(to_rfc3339),
                        to_rfc3339(at),
                    ],
                )?;
                let id = transaction.last_insert_rowid();
                let record = transaction.query_row(
                    &format!("SELECT {WATCH_COLUMNS} FROM {WATCH_FROM} WHERE w.id = ?1"),
                    [id],
                    row_to_watch,
                )?;
                transaction.commit()?;
                Ok(WatchOutcome::Added(record))
            })
            .await?;
        // Unconditional, including the refusal: that path still commits the prune of lapsed rows
        // above, so "nothing was added" is not the same as "nothing changed".
        self.forget_cached_watches();
        Ok(outcome)
    }

    /// Remove one watch by id, reporting what it was so the caller can say what it stopped
    /// watching for. `None` means there was no such watch.
    pub async fn remove_watch(&self, id: i64) -> Result<Option<WatchRecord>> {
        let record = self
            .connection
            .call(move |connection| {
                let transaction = connection
                    .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                let record = transaction
                    .query_row(
                        &format!("SELECT {WATCH_COLUMNS} FROM {WATCH_FROM} WHERE w.id = ?1"),
                        [id],
                        row_to_watch,
                    )
                    .optional()?;
                if record.is_some() {
                    transaction.execute("DELETE FROM watches WHERE id = ?1", [id])?;
                }
                transaction.commit()?;
                Ok(record)
            })
            .await?;
        if record.is_some() {
            self.forget_cached_watches();
        }
        Ok(record)
    }

    /// Every watch still standing, oldest first, pruning any that have lapsed.
    ///
    /// The listing an agent or an operator reads. [`Store::watches`] is the gate's read of the same
    /// rows and is cached; this one is exact and writes.
    pub async fn list_watches(&self, at: DateTime<Utc>) -> Result<Vec<WatchRecord>> {
        let records = self
            .connection
            .call(move |connection| {
                let transaction = connection
                    .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                let pruned = transaction.execute(
                    "DELETE FROM watches WHERE until IS NOT NULL AND until <= ?1",
                    [to_rfc3339(at)],
                )?;
                let records = {
                    let mut statement = transaction.prepare(&format!(
                        "SELECT {WATCH_COLUMNS} FROM {WATCH_FROM} ORDER BY w.id"
                    ))?;
                    let rows = statement.query_map([], row_to_watch)?;
                    rows.collect::<std::result::Result<Vec<_>, _>>()?
                };
                transaction.commit()?;
                Ok((records, pruned))
            })
            .await?;
        let (records, pruned) = records;
        if pruned > 0 {
            self.forget_cached_watches();
        }
        Ok(records)
    }

    /// The watches the gate matches against, cached for the same window as a policy.
    ///
    /// Cached for the same reason the policy is: this runs on every message from a muted
    /// conversation, and the answer is almost always the same one. The TTL is also how long an
    /// operator running `mekabridge watch` against a live daemon waits to see it take effect.
    ///
    /// The `Arc` is deliberately reused when a refetch finds the rows unchanged, so a caller can
    /// tell "these are the same watches" by pointer and skip recompiling a regex set it already
    /// built. Lapsed rows are filtered rather than deleted, because a read on the hot path should
    /// not take a write lock; [`Store::list_watches`] and [`Store::add_watch`] do the pruning.
    pub async fn watches(&self, at: DateTime<Utc>) -> Result<Arc<Vec<WatchRecord>>> {
        if let Some(cached) = self.cached_watches() {
            return Ok(cached);
        }
        let at = to_rfc3339(at);
        let rows = self
            .connection
            .call(move |connection| {
                let mut statement = connection.prepare(&format!(
                    "SELECT {WATCH_COLUMNS} FROM {WATCH_FROM}
                     WHERE w.until IS NULL OR w.until > ?1
                     ORDER BY w.id"
                ))?;
                let rows = statement.query_map([at], row_to_watch)?;
                rows.collect::<std::result::Result<Vec<_>, _>>()
            })
            .await?;
        Ok(self.cache_watches(rows))
    }

    fn cached_watches(&self) -> Option<Arc<Vec<WatchRecord>>> {
        let cache = self.watches.read().ok()?;
        let held = cache.as_ref()?;
        (held.fetched_at.elapsed() < POLICY_CACHE_TTL).then(|| Arc::clone(&held.rows))
    }

    /// Hold `rows`, keeping the previous allocation when nothing has changed.
    ///
    /// The identity of the `Arc` is load-bearing: the gate compares it against the one its compiled
    /// watchlist was built from, so minting a new one on every refetch would recompile every
    /// pattern twice a second for no reason.
    fn cache_watches(&self, rows: Vec<WatchRecord>) -> Arc<Vec<WatchRecord>> {
        let Ok(mut cache) = self.watches.write() else {
            return Arc::new(rows);
        };
        let held = match cache.take() {
            Some(held) if *held.rows == rows => held.rows,
            _ => Arc::new(rows),
        };
        *cache = Some(WatchCache {
            rows: Arc::clone(&held),
            fetched_at: Instant::now(),
        });
        held
    }

    fn forget_cached_watches(&self) {
        if let Ok(mut cache) = self.watches.write() {
            *cache = None;
        }
    }

    /// Count one message discarded because its conversation is blocked.
    ///
    /// Only blocked conversations need this. Under `mute` the messages are recorded, so what went
    /// unseen is counted from the history rather than tallied here.
    pub async fn note_blocked_drop(&self, address: &str) -> Result<()> {
        let address = address.to_string();
        self.connection
            .call(move |connection| {
                connection.execute(
                    &format!(
                        "UPDATE conversation_policy SET dropped = dropped + 1
                         WHERE conversation_id = {CONVERSATION_ID_OF}"
                    ),
                    [address],
                )?;
                Ok(())
            })
            .await?;
        // Deliberately not invalidating the cache. The count is read back by `expire_policy`, which
        // bypasses the cache, and refreshing here would put a database read back on the hot path
        // for exactly the conversations this policy exists to make cheap.
        Ok(())
    }

    /// A cached lookup, or `None` when the entry is missing or stale. The inner `Option` is whether
    /// a record exists, which is itself worth caching.
    fn cached_policy(&self, address: &str) -> Option<Option<PolicyRecord>> {
        let cache = self.policies.read().ok()?;
        let (record, fetched_at) = cache.entries.get(address)?;
        (fetched_at.elapsed() < POLICY_CACHE_TTL).then(|| record.clone())
    }

    fn cache_policy(&self, address: &str, record: Option<PolicyRecord>) {
        if let Ok(mut cache) = self.policies.write() {
            // Nothing evicts by age, so a long-running bridge in many conversations would otherwise
            // hold an entry for every one it has ever seen. Dropping the lot on overflow is crude
            // but costs one refetch each and keeps the bound obvious.
            if cache.entries.len() >= POLICY_CACHE_MAX_ENTRIES {
                cache.entries.clear();
            }
            cache
                .entries
                .insert(address.to_string(), (record, Instant::now()));
        }
    }

    fn forget_cached_policy(&self, address: &str) {
        if let Ok(mut cache) = self.policies.write() {
            cache.entries.remove(address);
        }
    }

    /// Persist an inbound message.
    ///
    /// Enforces `max_depth` against rows still waiting (`pending` plus `in_flight`) so a stalled
    /// turn cannot let the queue grow without bound. Duplicates are recognised by the whole of
    /// `key`, so a message id reused by a recreated bot is a new message rather than a replay.
    pub async fn enqueue(
        &self,
        key: MessageKey<'_>,
        payload: &str,
        received_at: DateTime<Utc>,
        max_depth: usize,
    ) -> Result<EnqueueOutcome> {
        let key = OwnedKey::from(key);
        let payload = payload.to_string();
        let received_at = to_rfc3339(received_at);
        let max_depth = max_depth as i64;
        let outcome = self
            .connection
            .call(move |connection| {
                let transaction = connection
                    .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                let waiting: i64 = transaction.query_row(
                    "SELECT COUNT(*) FROM inbound_queue WHERE state IN ('pending', 'in_flight')",
                    [],
                    |row| row.get(0),
                )?;
                if waiting >= max_depth {
                    // Roll back rather than commit: nothing was written, and the caller records the
                    // drop separately so the count survives even if this transaction is retried.
                    transaction.rollback()?;
                    return Ok(EnqueueOutcome::Dropped);
                }
                let changed = transaction.execute(
                    &format!(
                        "INSERT INTO inbound_queue
                             (conversation_id, account_id, message_id, revision, payload,
                              received_at, state, attempts)
                         VALUES ({CONVERSATION_ID_OF}, ?2, ?3, ?4, ?5, ?6, 'pending', 0)
                         ON CONFLICT(conversation_id, message_id, account_id, revision) DO NOTHING"
                    ),
                    rusqlite::params![
                        key.conversation,
                        key.account,
                        key.message_id,
                        key.revision,
                        payload,
                        received_at
                    ],
                )?;
                transaction.commit()?;
                Ok(if changed == 0 {
                    EnqueueOutcome::Duplicate
                } else {
                    EnqueueOutcome::Queued
                })
            })
            .await?;
        Ok(outcome)
    }

    /// Oldest and newest arrival times per conversation with something waiting.
    ///
    /// Grouped rather than aggregated across the whole queue because readiness differs by
    /// conversation: a single window over every pending row meant one chat's burst deferring every
    /// other chat's delivery, which it did until this was split.
    ///
    /// `min`/`max` over the stored RFC 3339 text is sound because [`Store::enqueue`] writes a fixed
    /// `+00:00` offset and zero-padded fields, so byte order and chronological order agree.
    ///
    /// Derived from the queue rather than from in-memory state so the behaviour is the same on the
    /// first message after a restart.
    pub async fn pending_windows(&self) -> Result<Vec<PendingWindow>> {
        let rows: Vec<(String, String, String, Option<String>)> = self
            .connection
            .call(move |connection| {
                // `max()` ignores NULLs, so a conversation with one deferred row and ten fresh ones
                // reports that row's deferral rather than nothing.
                let mut statement = connection.prepare(&format!(
                    "SELECT c.address, min(q.received_at), max(q.received_at), max(q.not_before)
                     FROM {QUEUE_FROM}
                     WHERE q.state = 'pending'
                     GROUP BY q.conversation_id"
                ))?;
                let rows = statement
                    .query_map([], |row| {
                        Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                    })?
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                Ok(rows)
            })
            .await?;
        let parse = |column: &str, raw: &str| {
            DateTime::parse_from_rfc3339(raw)
                .map(|value| value.with_timezone(&Utc))
                .map_err(|error| StoreError::Corrupt {
                    key: format!("inbound_queue.{column}"),
                    message: format!("{raw:?} is not an RFC 3339 timestamp: {error}"),
                })
        };
        rows.into_iter()
            .map(|(conversation, oldest, newest, not_before)| {
                Ok(PendingWindow {
                    conversation,
                    oldest: parse("received_at", &oldest)?,
                    newest: parse("received_at", &newest)?,
                    not_before: not_before
                        .map(|raw| parse("not_before", &raw))
                        .transpose()?,
                })
            })
            .collect()
    }

    /// Atomically take up to `limit` pending messages from the conversations in `ready` and mark
    /// them in flight.
    ///
    /// Claiming and marking happen in one transaction so two session tasks (or one racing a
    /// restart) cannot hand the same message over twice.
    ///
    /// One pass may span conversations, which is what makes several chats that all became ready
    /// together reach the agent in one turn rather than one each: meka reads every item waiting for
    /// it into the turn it opens. `ready` narrows which are eligible, not how many may be claimed
    /// at once.
    pub async fn claim(&self, ready: &[String], limit: usize) -> Result<Vec<QueuedMessage>> {
        // SQLite does accept `IN ()` and evaluates it false, so this is for the round trip rather
        // than for correctness: nothing being ready is by far the most common tick, and there is no
        // reason to reach the database to be told so.
        if ready.is_empty() {
            return Ok(Vec::new());
        }
        let ready: Vec<String> = ready.to_vec();
        let limit = limit.min(i64::MAX as usize) as i64;
        let batch = self
            .connection
            .call(move |connection| {
                let placeholders = std::iter::repeat_n("?", ready.len())
                    .collect::<Vec<_>>()
                    .join(",");
                let transaction = connection
                    .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                let claimed = {
                    let mut statement = transaction.prepare(&format!(
                        "SELECT {QUEUE_COLUMNS}
                         FROM {QUEUE_FROM}
                         WHERE q.state = 'pending' AND c.address IN ({placeholders})
                         ORDER BY q.seq
                         LIMIT ?"
                    ))?;
                    // Bound in order, the limit last, matching the placeholder order above.
                    let mut values: Vec<&dyn ToSql> =
                        ready.iter().map(|id| id as &dyn ToSql).collect();
                    values.push(&limit);
                    let rows =
                        statement.query_map(rusqlite::params_from_iter(values), row_to_queued)?;
                    rows.collect::<std::result::Result<Vec<_>, _>>()?
                };
                for message in &claimed {
                    transaction.execute(
                        "UPDATE inbound_queue SET state = 'in_flight' WHERE seq = ?1",
                        [message.seq],
                    )?;
                }
                transaction.commit()?;
                Ok(claimed)
            })
            .await?;
        Ok(batch)
    }

    /// Read pending rows without claiming them.
    ///
    /// Distinct from [`Self::claim`] because an inspection command must not move rows into
    /// `in_flight`: doing so would hide them from the session task until the next restart.
    pub async fn peek_pending(&self, limit: usize) -> Result<Vec<QueuedMessage>> {
        let limit = limit.min(i64::MAX as usize) as i64;
        let rows = self
            .connection
            .call(move |connection| {
                let mut statement = connection.prepare(&format!(
                    "SELECT {QUEUE_COLUMNS}
                     FROM {QUEUE_FROM}
                     WHERE q.state = 'pending'
                     ORDER BY q.seq
                     LIMIT ?1"
                ))?;
                let rows = statement.query_map([limit], row_to_queued)?;
                rows.collect::<std::result::Result<Vec<_>, _>>()
            })
            .await?;
        Ok(rows)
    }

    /// Close claimed rows as delivered without any of them having been handed over.
    ///
    /// For a payload this build cannot decode, which will never become readable and would otherwise
    /// wedge the queue behind it for ever. Guarded on nothing having been rendered yet, like
    /// [`Self::unclaim`]: once a key is minted, meka may hold the item under it, and only the
    /// hand-over's own outcome may close the row.
    pub async fn complete(&self, sequences: &[i64]) -> Result<()> {
        let sequences = sequences.to_vec();
        let completed_at = to_rfc3339(Utc::now());
        self.connection
            .call(move |connection| {
                let transaction = connection
                    .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                for sequence in sequences {
                    transaction.execute(
                        "UPDATE inbound_queue
                         SET state = 'done', last_error = NULL, completed_at = ?2
                         WHERE seq = ?1 AND state = 'in_flight' AND key IS NULL",
                        rusqlite::params![sequence, &completed_at],
                    )?;
                }
                transaction.commit()?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    /// Return claimed rows to the queue without spending an offer.
    ///
    /// For rows that were never rendered, as distinct from ones that failed: nothing was made for
    /// anybody, nothing reached the agent, and counting it as a failed delivery would let a run of
    /// store errors exhaust the offer budget and declare a message undeliverable that was never
    /// even attempted.
    ///
    /// A row with a key is refused. Its item exists under that key, so meka may already hold it,
    /// and putting the row back would hand the same message over twice; [`Self::reoffer`] is what
    /// takes a rendered row back, and it sheds the key on the way.
    pub async fn unclaim(&self, sequences: &[i64]) -> Result<()> {
        let sequences = sequences.to_vec();
        self.connection
            .call(move |connection| {
                let transaction = connection
                    .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                for sequence in sequences {
                    transaction.execute(
                        "UPDATE inbound_queue SET state = 'pending'
                         WHERE seq = ?1 AND state = 'in_flight' AND key IS NULL",
                        [sequence],
                    )?;
                }
                transaction.commit()?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    /// Stamp a rendered item onto a claimed row, and take off what the item states only once.
    ///
    /// One transaction, because the writes describe one fact. The key is minted here and never
    /// again, so every post of this row is the same request to meka. The dropped-message count the
    /// item reported comes off the counter in the same write, so a crash between rendering and this
    /// call loses the item and keeps the count rather than the reverse. And the backlog the item
    /// states is marked seen here, stamped with this row's `seq`, which is what lets a second
    /// hand-over be outstanding for the same conversation without restating what the first one
    /// already said, and lets a hand-over that dies give back exactly the slice it took.
    ///
    /// `false` when the row was not a fresh claim any more, which nothing here acts on further.
    pub async fn hold(&self, seq: i64, item: HandOver<'_>, at: DateTime<Utc>) -> Result<bool> {
        let key = item.key.to_string();
        let body = item.body.to_string();
        let session = item.session_id.to_string();
        let dropped_count = i64::try_from(item.dropped).unwrap_or(i64::MAX);
        let held_at = to_rfc3339(at);
        let accounted = item
            .accounted
            .map(|backlog| (backlog.watermark, to_rfc3339(backlog.through)));
        let held = self
            .connection
            .call(move |connection| {
                let transaction = connection
                    .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                let changed = transaction.execute(
                    "UPDATE inbound_queue
                     SET key = ?2, body = ?3, session_id = ?4, dropped = ?5, held_at = ?6,
                         posts = 0, not_before = NULL, last_error = NULL
                     WHERE seq = ?1 AND state = 'in_flight' AND key IS NULL",
                    rusqlite::params![seq, key, body, session, dropped_count, held_at],
                )?;
                if changed == 0 {
                    transaction.commit()?;
                    return Ok(false);
                }
                if dropped_count > 0 {
                    // Clamped at zero. The counter can only have moved up since it was read, and a
                    // write racing this one is a count the next item reports.
                    transaction.execute(
                        "UPDATE meta
                         SET value = CAST(MAX(CAST(value AS INTEGER) - ?1, 0) AS TEXT)
                         WHERE key = ?2",
                        rusqlite::params![dropped_count, META_DROPPED],
                    )?;
                }
                if let Some((watermark, through)) = accounted {
                    // Both bounds together are the set `take_unseen` counted, and neither alone is.
                    // On the timestamp alone, a row written between the count and this call at or
                    // below the ceiling is marked without ever being shown, which is not a narrow
                    // window: Telegram stamps to whole seconds, and an edit carries the original
                    // message's time. On the id alone, that same edit sweeps the other way: a low
                    // id with a high timestamp, never counted and marked regardless.
                    transaction.execute(
                        "UPDATE messages SET seen = 1, accounted_by = ?1
                         WHERE conversation_id =
                                   (SELECT q.conversation_id FROM inbound_queue q WHERE q.seq = ?1)
                           AND seen = 0 AND id <= ?2 AND timestamp <= ?3",
                        rusqlite::params![seq, watermark, through],
                    )?;
                }
                transaction.commit()?;
                Ok(true)
            })
            .await?;
        Ok(held)
    }

    /// Record that meka has the row's item, on `session_id`.
    ///
    /// Accepted on a row already posted as well as on one waiting: a reconciliation that finds meka
    /// with no record of the item posts it afresh, and the new id is the one the feed will name.
    /// The session is written because a row held before a rebind is posted to the session the
    /// bridge has now.
    pub async fn posted(
        &self,
        seq: i64,
        session_id: Uuid,
        item_id: &str,
        at: DateTime<Utc>,
    ) -> Result<()> {
        let session_id = session_id.to_string();
        let item_id = item_id.to_string();
        let at = to_rfc3339(at);
        self.connection
            .call(move |connection| {
                connection.execute(
                    "UPDATE inbound_queue
                     SET state = 'posted', session_id = ?2, item_id = ?3, posted_at = ?4,
                         posts = posts + 1, not_before = NULL, last_error = NULL
                     WHERE seq = ?1 AND state IN ('in_flight', 'posted')",
                    rusqlite::params![seq, session_id, item_id, at],
                )?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    /// Record that a post did not go through, and when it may be made again.
    pub async fn defer(&self, seq: i64, error: &str, not_before: DateTime<Utc>) -> Result<()> {
        let error = error.to_string();
        let not_before = to_rfc3339(not_before);
        self.connection
            .call(move |connection| {
                connection.execute(
                    "UPDATE inbound_queue
                     SET posts = posts + 1, last_error = ?2, not_before = ?3
                     WHERE seq = ?1 AND state = 'in_flight'",
                    rusqlite::params![seq, error, not_before],
                )?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    /// Close a row the model has read.
    ///
    /// `false` for a row already closed: the feed replays an outcome after a reconnect, and the
    /// second reading must change nothing.
    pub async fn delivered(&self, seq: i64, at: DateTime<Utc>) -> Result<bool> {
        let at = to_rfc3339(at);
        let closed = self
            .connection
            .call(move |connection| {
                connection.execute(
                    "UPDATE inbound_queue
                     SET state = 'done', last_error = NULL, completed_at = ?2
                     WHERE seq = ?1 AND state IN ('in_flight', 'posted')",
                    rusqlite::params![seq, at],
                )
            })
            .await?;
        Ok(closed > 0)
    }

    /// Close a row that will never reach the model, and give back what its item had taken.
    ///
    /// Returns the row so the caller can put it among what the agent is owed and say who lost what,
    /// and nothing for a row something else had already closed. The rendered body goes, since
    /// nothing will ever post it again and keeping it would pin a second copy of somebody's message
    /// for the life of the database; the key and the item id stay, because they are what names this
    /// hand-over in meka's own logs.
    pub async fn failed(
        &self,
        seq: i64,
        error: &str,
        at: DateTime<Utc>,
    ) -> Result<Option<QueuedMessage>> {
        let error = error.to_string();
        let at = to_rfc3339(at);
        let row = self
            .connection
            .call(move |connection| {
                let transaction = connection
                    .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                let closed = transaction.execute(
                    "UPDATE inbound_queue
                     SET state = 'failed', body = NULL, last_error = ?2, completed_at = ?3
                     WHERE seq = ?1 AND state IN ('in_flight', 'posted')",
                    rusqlite::params![seq, error, at],
                )?;
                if closed == 0 {
                    transaction.commit()?;
                    return Ok(None);
                }
                restore(&transaction, seq)?;
                let row = read_row(&transaction, seq)?;
                transaction.commit()?;
                Ok(row)
            })
            .await?;
        Ok(row)
    }

    /// Offer a row again, spending one of its offers.
    ///
    /// For the two ways a hand-over comes apart without the message having been answered: meka took
    /// the item back before the model read it, and the model read it and came back with nothing at
    /// all, no text and no tool call. Either way the row goes back to `pending`, held until
    /// `not_before` when one is given, and the next claim renders it afresh under a new key. A row
    /// past `max_offers` fails instead and comes back so the caller can say so.
    pub async fn reoffer(
        &self,
        seq: i64,
        error: &str,
        max_offers: u32,
        not_before: Option<DateTime<Utc>>,
        at: DateTime<Utc>,
    ) -> Result<Offer> {
        let error = error.to_string();
        let not_before = not_before.map(to_rfc3339);
        let at = to_rfc3339(at);
        let offer = self
            .connection
            .call(move |connection| {
                let transaction = connection
                    .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                let changed = transaction.execute(
                    "UPDATE inbound_queue SET attempts = attempts + 1, last_error = ?2
                     WHERE seq = ?1 AND state IN ('in_flight', 'posted', 'done')",
                    rusqlite::params![seq, error],
                )?;
                if changed == 0 {
                    transaction.commit()?;
                    return Ok(Offer::Closed);
                }
                restore(&transaction, seq)?;
                let attempts: u32 = transaction.query_row(
                    "SELECT attempts FROM inbound_queue WHERE seq = ?1",
                    [seq],
                    |row| row.get(0),
                )?;
                if attempts > max_offers {
                    transaction.execute(
                        "UPDATE inbound_queue
                         SET state = 'failed', body = NULL, completed_at = ?2
                         WHERE seq = ?1",
                        rusqlite::params![seq, at],
                    )?;
                    let row = read_row(&transaction, seq)?;
                    transaction.commit()?;
                    return Ok(row.map_or(Offer::Closed, |row| Offer::Exhausted(Box::new(row))));
                }
                transaction.execute(
                    &format!(
                        "UPDATE inbound_queue SET state = 'pending', not_before = ?2, {QUEUE_CLEARED}
                         WHERE seq = ?1"
                    ),
                    rusqlite::params![seq, not_before],
                )?;
                transaction.commit()?;
                Ok(Offer::Retrying)
            })
            .await?;
        Ok(offer)
    }

    /// One row by sequence.
    pub async fn row(&self, seq: i64) -> Result<Option<QueuedMessage>> {
        let row = self
            .connection
            .call(move |connection| read_row(connection, seq))
            .await?;
        Ok(row)
    }

    /// Several rows by sequence, in delivery order.
    pub async fn rows(&self, sequences: &[i64]) -> Result<Vec<QueuedMessage>> {
        if sequences.is_empty() {
            return Ok(Vec::new());
        }
        let sequences = sequences.to_vec();
        let rows = self
            .connection
            .call(move |connection| {
                let placeholders = std::iter::repeat_n("?", sequences.len())
                    .collect::<Vec<_>>()
                    .join(",");
                let mut statement = connection.prepare(&format!(
                    "SELECT {QUEUE_COLUMNS} FROM {QUEUE_FROM}
                     WHERE q.seq IN ({placeholders})
                     ORDER BY q.seq"
                ))?;
                let rows =
                    statement.query_map(rusqlite::params_from_iter(sequences), row_to_queued)?;
                rows.collect::<std::result::Result<Vec<_>, _>>()
            })
            .await?;
        Ok(rows)
    }

    /// The row meka knows as `item_id`, if any.
    pub async fn row_by_item(&self, item_id: &str) -> Result<Option<QueuedMessage>> {
        let item_id = item_id.to_string();
        let row = self
            .connection
            .call(move |connection| {
                connection
                    .query_row(
                        &format!("SELECT {QUEUE_COLUMNS} FROM {QUEUE_FROM} WHERE q.item_id = ?1"),
                        [item_id],
                        row_to_queued,
                    )
                    .optional()
            })
            .await?;
        Ok(row)
    }

    /// Every row in `state`, oldest first.
    pub async fn rows_in(&self, state: QueueState) -> Result<Vec<QueuedMessage>> {
        let rows = self
            .connection
            .call(move |connection| {
                let mut statement = connection.prepare(&format!(
                    "SELECT {QUEUE_COLUMNS} FROM {QUEUE_FROM} WHERE q.state = ?1 ORDER BY q.seq"
                ))?;
                let rows = statement.query_map([state.as_str()], row_to_queued)?;
                rows.collect::<std::result::Result<Vec<_>, _>>()
            })
            .await?;
        Ok(rows)
    }

    /// Put the queue back the way a restart needs it, and say what was found. Called once at
    /// startup.
    ///
    /// Only rows claimed and never rendered move: the process died between the claim and
    /// [`Self::hold`], so nothing was rendered for them and nothing reached meka. A row that was
    /// rendered is left where it is, since it is either posted again under its own key or asked
    /// about, and either answer is better than a guess.
    pub async fn recover(&self) -> Result<Recovered> {
        let recovered = self
            .connection
            .call(|connection| {
                let transaction = connection
                    .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                let orphaned = transaction.execute(
                    "UPDATE inbound_queue SET state = 'pending'
                     WHERE state = 'in_flight' AND key IS NULL",
                    [],
                )?;
                let count = |state: QueueState| -> rusqlite::Result<usize> {
                    transaction
                        .query_row(
                            "SELECT COUNT(*) FROM inbound_queue WHERE state = ?1",
                            [state.as_str()],
                            |row| row.get::<_, i64>(0),
                        )
                        .map(|count| count.max(0) as usize)
                };
                let unposted = count(QueueState::InFlight)?;
                let posted = count(QueueState::Posted)?;
                transaction.commit()?;
                Ok(Recovered {
                    orphaned,
                    unposted,
                    posted,
                })
            })
            .await?;
        Ok(recovered)
    }

    pub async fn pending_count(&self) -> Result<u64> {
        let count = self
            .connection
            .call(|connection| {
                connection.query_row(
                    "SELECT COUNT(*) FROM inbound_queue WHERE state = 'pending'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
            })
            .await?;
        Ok(count.max(0) as u64)
    }

    /// Row counts by state.
    pub async fn queue_stats(&self) -> Result<QueueStats> {
        let stats = self
            .connection
            .call(|connection| {
                let mut stats = QueueStats::default();
                let mut statement = connection
                    .prepare("SELECT state, COUNT(*) FROM inbound_queue GROUP BY state")?;
                let rows = statement.query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
                })?;
                for row in rows {
                    let (state, count) = row?;
                    let count = count.max(0) as u64;
                    match QueueState::parse(&state) {
                        Some(QueueState::Pending) => stats.pending = count,
                        Some(QueueState::InFlight) => stats.in_flight = count,
                        Some(QueueState::Posted) => stats.posted = count,
                        Some(QueueState::Done) => stats.done = count,
                        Some(QueueState::Failed) => stats.failed = count,
                        None => {}
                    }
                }
                Ok(stats)
            })
            .await?;
        Ok(stats)
    }

    /// Delete every queue row regardless of state. Backs `mekabridge queue clear`.
    ///
    /// A hand-over that was still out gives back what its item took first, exactly as if it had
    /// failed. Deleting the row alone would not do it: `ON DELETE SET NULL` clears the stamp on the
    /// backlog that item reported but cannot un-see it, so a muted chat's messages would be marked
    /// as shown to an agent that never saw them, and nothing would ever offer them again. Rows that
    /// are already `done` are left alone, since their backlog really was read.
    pub async fn clear_queue(&self) -> Result<usize> {
        let deleted = self
            .connection
            .call(|connection| {
                let transaction = connection
                    .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                let outstanding: Vec<i64> = {
                    let mut statement = transaction.prepare(
                        "SELECT seq FROM inbound_queue WHERE state IN ('in_flight', 'posted')",
                    )?;
                    let rows = statement.query_map([], |row| row.get(0))?;
                    rows.collect::<rusqlite::Result<Vec<_>>>()?
                };
                for seq in outstanding {
                    restore(&transaction, seq)?;
                }
                let deleted = transaction.execute("DELETE FROM inbound_queue", [])?;
                transaction.commit()?;
                Ok(deleted)
            })
            .await?;
        Ok(deleted)
    }

    /// Drop delivered rows older than `before`.
    ///
    /// Completed rows are kept for a window rather than deleted immediately because they are what
    /// makes duplicate detection work across a restart. Going takes the rendered item with them,
    /// which is the copy of somebody's message worth not keeping; a row that ends `failed` had its
    /// body dropped when it failed, so it costs nothing to keep for the operator to find.
    pub async fn prune_delivered(&self, before: DateTime<Utc>) -> Result<usize> {
        let before = to_rfc3339(before);
        let deleted = self
            .connection
            .call(move |connection| {
                connection.execute(
                    // `coalesce` so rows written before `completed_at` existed keep their old
                    // behaviour rather than becoming immortal.
                    "DELETE FROM inbound_queue
                     WHERE state = 'done' AND coalesce(completed_at, received_at) < ?1",
                    [&before],
                )
            })
            .await?;
        Ok(deleted)
    }

    /// Record what somebody said, whether or not the agent was woken for it.
    ///
    /// Idempotent on the message's key, the same one the queue deduplicates on, so a platform
    /// replaying an update after a crash does not produce a second copy. Returns whether the row
    /// was written, which is `false` for exactly that replay.
    ///
    /// The conversation must already be on file; a message from an address the address book has
    /// never seen is refused by the schema rather than filed under nothing.
    pub async fn record_message(&self, record: MessageRecord) -> Result<bool> {
        let inserted = self
            .connection
            .call(move |connection| {
                connection.execute(
                    &format!(
                        "INSERT INTO messages
                             (conversation_id, account_id, message_id, revision, sender_id,
                              sender_name, text, notes, addressed, seen, own, session_id,
                              deleted_at, superseded_at, timestamp)
                         VALUES ({CONVERSATION_ID_OF}, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11,
                                 ?12, ?13, ?14, ?15)
                         ON CONFLICT(conversation_id, message_id, account_id, revision) DO NOTHING"
                    ),
                    rusqlite::params![
                        record.conversation,
                        record.account,
                        record.message_id,
                        record.revision,
                        record.sender_id,
                        record.sender_name,
                        record.text,
                        record.notes,
                        record.addressed,
                        record.seen,
                        record.own,
                        record.session_id,
                        record.deleted_at.map(to_rfc3339),
                        record.superseded_at.map(to_rfc3339),
                        to_rfc3339(record.timestamp),
                    ],
                )
            })
            .await?;
        Ok(inserted > 0)
    }

    /// Read a conversation back, oldest first, ending at `before` when one is given.
    ///
    /// Selected newest-first so `limit` takes the most recent, then reversed, which is what "the
    /// last twenty messages" means.
    ///
    /// `before` and `after` are [`MessageRecord::id`]s, not times. Paging on the timestamp cannot
    /// be made correct: a burst shares one second, so "older than this timestamp" either drops
    /// the siblings of the message paged from or returns them twice, and the first loses
    /// messages silently.
    ///
    /// Ordered by id so the sequence agrees with the cursor. That is also arrival order, which the
    /// timestamp is not for an edit: it carries the time of the message it revises.
    ///
    /// The two cursors read opposite ways and the difference is which end the limit cuts from.
    /// `before` walks backwards through what is already there, so it takes the newest rows under
    /// the cursor. `after` follows a conversation forwards, so it takes the *oldest* rows above
    /// the cursor: a caller sweeping a busy chat has to be handed the next page in order, not the
    /// latest page with a hole behind it. Setting both is refused at the tool, not here.
    pub async fn history(
        &self,
        address: &str,
        limit: usize,
        before: Option<i64>,
        after: Option<i64>,
    ) -> Result<Vec<MessageRecord>> {
        let address = address.to_string();
        let limit = limit.min(i64::MAX as usize) as i64;
        let mut records = self
            .connection
            .call(move |connection| {
                let order = if after.is_some() { "ASC" } else { "DESC" };
                let mut statement = connection.prepare(&format!(
                    "SELECT {MESSAGE_COLUMNS}
                     FROM {MESSAGE_FROM}
                     WHERE c.address = ?1
                       AND (?2 IS NULL OR m.id < ?2)
                       AND (?3 IS NULL OR m.id > ?3)
                     ORDER BY m.id {order}
                     LIMIT ?4"
                ))?;
                let rows = statement.query_map(
                    rusqlite::params![address, before, after, limit],
                    row_to_message,
                )?;
                rows.collect::<std::result::Result<Vec<_>, _>>()
            })
            .await?;
        // Oldest first either way, which is reading order. The forward query already is.
        if after.is_none() {
            records.reverse();
        }
        Ok(records)
    }

    /// Full-text search across recorded messages, best matches first.
    ///
    /// `conversation` narrows to one chat. The query goes to FTS5 verbatim, so its operators work,
    /// and a query it cannot parse comes back as an error rather than as silently zero results.
    pub async fn search_messages(
        &self,
        query: &str,
        conversation: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MessageRecord>> {
        let query = query.to_string();
        let conversation = conversation.map(str::to_string);
        let limit = limit.min(i64::MAX as usize) as i64;
        let records = self
            .connection
            .call(move |connection| {
                // Spelled out rather than built on `MESSAGE_FROM`, whose own joins would sit
                // between `messages` and the `ON` that ties it to the index.
                let mut statement = connection.prepare(&format!(
                    "SELECT {MESSAGE_COLUMNS}
                     FROM messages_fts f
                     JOIN messages m ON m.id = f.rowid
                     JOIN conversations c ON c.id = m.conversation_id
                     JOIN accounts a ON a.id = m.account_id
                     WHERE messages_fts MATCH ?1
                       AND (?2 IS NULL OR c.address = ?2)
                     ORDER BY f.rank
                     LIMIT ?3"
                ))?;
                let rows = statement.query_map(
                    rusqlite::params![query, conversation, limit],
                    row_to_message,
                )?;
                rows.collect::<std::result::Result<Vec<_>, _>>()
            })
            .await?;
        Ok(records)
    }

    /// Count what a conversation has withheld and mark it seen, returning the last `context` of it.
    ///
    /// One transaction because the three are one decision: the count the agent is told, the excerpt
    /// it is shown, and the flag that stops it being told the same thing again next time. Split
    /// apart, a message landing between the count and the update would either be reported twice or
    /// never at all.
    pub async fn take_unseen(
        &self,
        address: &str,
        through: DateTime<Utc>,
        context: usize,
    ) -> Result<(u64, Vec<MessageRecord>, i64)> {
        let address = address.to_string();
        let through = to_rfc3339(through);
        let context = context.min(i64::MAX as usize) as i64;
        let (count, mut records, watermark) = self
            .connection
            .call(move |connection| {
                // Deferred rather than immediate: nothing here writes since `mark_seen` was split
                // out, so taking the write lock would only invent a `SQLITE_BUSY` this cannot hit.
                let transaction = connection.transaction()?;
                // Counted and watermarked in one statement so the ceiling describes exactly the
                // rows counted. `MAX(id)` rather than the timestamp asked for: ids are assigned at
                // insert, so a row written while the turn runs is always above this, whereas its
                // timestamp can easily be at or below -- Telegram stamps to whole seconds, and an
                // edit carries the *original* message's time, which may be weeks old.
                let (count, watermark): (i64, Option<i64>) = transaction.query_row(
                    &format!(
                        "SELECT COUNT(*), MAX(m.id) FROM messages m
                         WHERE m.conversation_id = {CONVERSATION_ID_OF} AND m.seen = 0
                           AND m.timestamp <= ?2 AND {MESSAGE_CURRENT}"
                    ),
                    rusqlite::params![&address, &through],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )?;
                let records = {
                    let mut statement = transaction.prepare(&format!(
                        "SELECT {MESSAGE_COLUMNS}
                         FROM {MESSAGE_FROM}
                         WHERE c.address = ?1 AND m.seen = 0 AND m.timestamp <= ?2
                           AND {MESSAGE_CURRENT}
                         ORDER BY m.timestamp DESC, m.id DESC
                         LIMIT ?3"
                    ))?;
                    let rows = statement.query_map(
                        rusqlite::params![&address, &through, context],
                        row_to_message,
                    )?;
                    rows.collect::<std::result::Result<Vec<_>, _>>()?
                };
                transaction.commit()?;
                Ok((count.max(0) as u64, records, watermark.unwrap_or(0)))
            })
            .await?;
        records.reverse();
        Ok((count, records, watermark))
    }

    /// Put one message back among what the agent is owed.
    ///
    /// Keyed on one message rather than a watermark because a message is marked seen when it is
    /// queued, assuming the agent is about to be handed it, and that assumption fails exactly when
    /// its batch runs out of attempts. Without this the message is neither delivered nor owed, so
    /// nothing short of `history_read` over the right window finds it again.
    ///
    /// A `false` return is legitimate: `[storage].history_retention` of zero writes no history, so
    /// there is nothing to un-see.
    pub async fn mark_unseen(&self, key: MessageKey<'_>) -> Result<bool> {
        let key = OwnedKey::from(key);
        let changed = self
            .connection
            .call(move |connection| {
                connection.execute(
                    &format!(
                        "UPDATE messages SET seen = 0
                         WHERE conversation_id = {CONVERSATION_ID_OF} AND account_id = ?2
                           AND message_id = ?3 AND revision = ?4"
                    ),
                    rusqlite::params![key.conversation, key.account, key.message_id, key.revision],
                )
            })
            .await?;
        Ok(changed > 0)
    }

    /// How much is owed to the agent, without spending any of it.
    ///
    /// The read-only half of [`Store::take_unseen`], which exists because asking for the backlog
    /// consumes it, leaving the turn that asked with nothing to read.
    ///
    /// Three answers from one snapshot, because a caller comparing readings taken separately could
    /// see a message land between them. Only the third is one a watcher can compare; see
    /// [`UnseenSummary::marker`].
    ///
    /// `MAX` over the stored text works because RFC 3339 with a fixed `+00:00` offset sorts as text
    /// exactly as it does as time, and chrono's per-row fractional-second precision does not break
    /// that: `+` and `.` both sort below every digit.
    pub async fn unseen_summary(&self, conversation: Option<&str>) -> Result<UnseenSummary> {
        // Counting a `CASE` rather than summing it, so an empty table answers 0 instead of NULL.
        //
        // The unseen pair shares `MESSAGE_CURRENT` rather than spelling that rule out again;
        // writing the third by hand is how it came to disagree with the contract on
        // `UnseenSummary::marker` without anything saying so.
        //
        // The third column is what a watcher polls, so its filter decides when a scheduled job
        // wakes. Own messages are excluded or the agent replying moves the marker, firing the
        // watcher on a room whose only new message the agent wrote itself. Retracted rows are
        // excluded because a deletion moving the marker back is news, which used to happen by
        // itself when the row was removed and now has to be said explicitly.
        let columns = format!(
            "COUNT(CASE WHEN m.seen = 0 AND {MESSAGE_CURRENT} THEN 1 END),
             MAX(CASE WHEN m.seen = 0 AND {MESSAGE_CURRENT} THEN m.timestamp END),
             MAX(CASE WHEN m.own = 0 AND m.deleted_at IS NULL THEN m.timestamp END)"
        );
        let conversation = conversation.map(str::to_string);
        let (count, newest, latest): (i64, Option<String>, Option<String>) = self
            .connection
            .call(move |connection| match conversation {
                Some(address) => connection.query_row(
                    &format!(
                        "SELECT {columns} FROM messages m
                         WHERE m.conversation_id = {CONVERSATION_ID_OF}"
                    ),
                    [address],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                ),
                None => {
                    connection.query_row(&format!("SELECT {columns} FROM messages m"), [], |row| {
                        Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                    })
                }
            })
            .await?;
        Ok(UnseenSummary {
            count: count.max(0) as u64,
            newest: newest.as_deref().map(parse_rfc3339).transpose()?,
            latest: latest.as_deref().map(parse_rfc3339).transpose()?,
        })
    }

    /// Drop recorded messages older than `before`. The FTS index follows through its triggers.
    pub async fn prune_messages(&self, before: DateTime<Utc>) -> Result<usize> {
        let before = to_rfc3339(before);
        let deleted = self
            .connection
            .call(move |connection| {
                connection.execute("DELETE FROM messages WHERE timestamp < ?1", [before])
            })
            .await?;
        Ok(deleted)
    }

    /// Mark one recorded message deleted, because the platform says it is gone.
    ///
    /// The row and its text stay. A message the agent has been handed is in its session for good,
    /// and erasing the record removed the only thing that could later tell it what it acted on was
    /// retracted.
    ///
    /// Keyed on the message rather than on a revision of it, since a deletion names the message and
    /// knows nothing about the edit that may have given it a second row. That is also why every row
    /// for the message is marked rather than one. The account is part of the key because the same
    /// message id under a previous account is a different message.
    pub async fn mark_deleted(
        &self,
        address: &str,
        account: AccountId,
        message_id: &str,
        at: DateTime<Utc>,
    ) -> Result<bool> {
        let address = address.to_string();
        let message_id = message_id.to_string();
        let at = to_rfc3339(at);
        let marked = self
            .connection
            .call(move |connection| {
                connection.execute(
                    &format!(
                        "UPDATE messages SET deleted_at = ?4
                         WHERE conversation_id = {CONVERSATION_ID_OF} AND account_id = ?2
                           AND message_id = ?3 AND deleted_at IS NULL"
                    ),
                    rusqlite::params![address, account, message_id, at],
                )
            })
            .await?;
        Ok(marked > 0)
    }

    /// Mark the wordings a message had before the revision `key` names replaced them.
    ///
    /// Ordered on the revision's own row id rather than "everything that is not this one", which is
    /// the only form that survives being called twice or out of order: a redelivered older edit
    /// would mark the newer wording that had replaced it, and since `superseded_at IS NULL` stops
    /// the older one being cleared again, the message would end with every wording marked and none
    /// current.
    ///
    /// A revision that is not in the table, because history was off or retention has taken it,
    /// leaves the subquery `NULL` and marks nothing, leaving the wording it replaced readable
    /// rather than stale with no successor.
    pub async fn supersede_message(&self, key: MessageKey<'_>, at: DateTime<Utc>) -> Result<usize> {
        let key = OwnedKey::from(key);
        let at = to_rfc3339(at);
        let marked = self
            .connection
            .call(move |connection| {
                connection.execute(
                    &format!(
                        "UPDATE messages SET superseded_at = ?5
                         WHERE conversation_id = {CONVERSATION_ID_OF} AND account_id = ?2
                           AND message_id = ?3 AND superseded_at IS NULL
                           AND id < (SELECT id FROM messages
                                     WHERE conversation_id = {CONVERSATION_ID_OF}
                                       AND account_id = ?2 AND message_id = ?3
                                       AND revision = ?4)"
                    ),
                    rusqlite::params![
                        key.conversation,
                        key.account,
                        key.message_id,
                        key.revision,
                        at
                    ],
                )
            })
            .await?;
        Ok(marked)
    }

    /// Unseen counts for every conversation that has any, keyed by address.
    ///
    /// One grouped query rather than a count per conversation, because the caller is rendering a
    /// list and the alternative is fifty round trips to put a number on fifty rows.
    pub async fn unseen_counts(&self) -> Result<HashMap<String, u64>> {
        let counts = self
            .connection
            .call(|connection| {
                let mut statement = connection.prepare(&format!(
                    "SELECT c.address, COUNT(*)
                     FROM messages m JOIN conversations c ON c.id = m.conversation_id
                     WHERE m.seen = 0 AND {MESSAGE_CURRENT}
                     GROUP BY c.address"
                ))?;
                let rows = statement.query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?.max(0) as u64,
                    ))
                })?;
                rows.collect::<std::result::Result<HashMap<_, _>, _>>()
            })
            .await?;
        Ok(counts)
    }

    /// How many messages are recorded, for `mekabridge status`.
    pub async fn message_count(&self) -> Result<u64> {
        let count = self
            .connection
            .call(|connection| {
                connection.query_row("SELECT COUNT(*) FROM messages", [], |row| {
                    row.get::<_, i64>(0)
                })
            })
            .await?;
        Ok(count.max(0) as u64)
    }

    /// Register an attachment and return the handle the agent fetches it by.
    ///
    /// Idempotent on the message's key and the file's position in it: a redelivered message returns
    /// the handle already issued rather than minting a second one for the same file.
    pub async fn register_attachment(&self, record: AttachmentRecord) -> Result<String> {
        let handle = self
            .connection
            .call(move |connection| {
                connection.execute(
                    &format!(
                        "INSERT INTO attachments
                             (conversation_id, account_id, message_id, revision, position, kind,
                              file_ref, thumb_ref, file_name, media_type, bytes, path, created_at)
                         VALUES ({CONVERSATION_ID_OF}, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11,
                                 ?12, ?13)
                         ON CONFLICT(conversation_id, message_id, account_id, revision, position)
                             DO NOTHING"
                    ),
                    rusqlite::params![
                        record.conversation,
                        record.account,
                        record.message_id,
                        record.revision,
                        record.position,
                        record.kind,
                        record.file_ref,
                        record.thumb_ref,
                        record.file_name,
                        record.media_type,
                        record.bytes.map(|bytes| bytes as i64),
                        record.path.map(|path| path.to_string_lossy().into_owned()),
                        to_rfc3339(record.created_at),
                    ],
                )?;
                // Read back rather than using `last_insert_rowid`, which reports nothing useful
                // when the conflict clause suppressed the insert.
                connection.query_row(
                    &format!(
                        "SELECT handle FROM attachments
                         WHERE conversation_id = {CONVERSATION_ID_OF} AND account_id = ?2
                           AND message_id = ?3 AND revision = ?4 AND position = ?5"
                    ),
                    rusqlite::params![
                        record.conversation,
                        record.account,
                        record.message_id,
                        record.revision,
                        record.position
                    ],
                    |row| row.get::<_, i64>(0),
                )
            })
            .await?;
        Ok(handle.to_string())
    }

    /// Look up one attachment by the handle the agent quoted.
    pub async fn attachment(&self, handle: &str) -> Result<Option<StoredAttachment>> {
        // Parsed rather than compared as text so a handle the agent invented cannot match a row by
        // some other spelling of the same number.
        let Ok(handle) = handle.trim().parse::<i64>() else {
            return Ok(None);
        };
        let record = self
            .connection
            .call(move |connection| {
                connection
                    .query_row(
                        "SELECT a.handle, c.address, c.channel, a.kind, a.file_ref, a.thumb_ref,
                                a.file_name, a.media_type, a.bytes, a.path
                         FROM attachments a JOIN conversations c ON c.id = a.conversation_id
                         WHERE a.handle = ?1",
                        [handle],
                        |row| {
                            Ok(StoredAttachment {
                                handle: row.get::<_, i64>(0)?.to_string(),
                                conversation: row.get(1)?,
                                channel: row.get(2)?,
                                kind: row.get(3)?,
                                file_ref: row.get(4)?,
                                thumb_ref: row.get(5)?,
                                file_name: row.get(6)?,
                                media_type: row.get(7)?,
                                bytes: row.get::<_, Option<i64>>(8)?.map(|bytes| bytes as u64),
                                path: row.get::<_, Option<String>>(9)?.map(PathBuf::from),
                            })
                        },
                    )
                    .optional()
            })
            .await?;
        Ok(record)
    }

    /// Record that an attachment has been written to local disk, so the sweep can unlink it later.
    pub async fn mark_attachment_downloaded(&self, handle: &str, path: &Path) -> Result<()> {
        let Ok(handle) = handle.trim().parse::<i64>() else {
            return Ok(());
        };
        let path = path.to_string_lossy().into_owned();
        self.connection
            .call(move |connection| {
                connection.execute(
                    "UPDATE attachments SET path = ?2 WHERE handle = ?1",
                    rusqlite::params![handle, path],
                )?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    /// Delete attachment rows older than `before` and return the paths of any that were downloaded,
    /// so the caller can unlink them. Rows go first: a leftover file is recoverable, a leaked row
    /// is not observable.
    pub async fn take_expired_attachments(&self, before: DateTime<Utc>) -> Result<Vec<PathBuf>> {
        let before = to_rfc3339(before);
        let paths = self
            .connection
            .call(move |connection| {
                let transaction = connection
                    .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                let paths = {
                    let mut statement = transaction.prepare(
                        "SELECT path FROM attachments
                         WHERE created_at < ?1 AND path IS NOT NULL",
                    )?;
                    let rows = statement.query_map([&before], |row| row.get::<_, String>(0))?;
                    rows.collect::<std::result::Result<Vec<_>, _>>()?
                };
                transaction.execute("DELETE FROM attachments WHERE created_at < ?1", [&before])?;
                transaction.commit()?;
                Ok(paths)
            })
            .await?;
        Ok(paths.into_iter().map(PathBuf::from).collect())
    }
}

/// A [`MessageKey`] that owns its strings, so it can be moved onto the connection thread.
struct OwnedKey {
    conversation: String,
    account: AccountId,
    message_id: String,
    revision: i64,
}

impl From<MessageKey<'_>> for OwnedKey {
    fn from(key: MessageKey<'_>) -> Self {
        Self {
            conversation: key.conversation.to_string(),
            account: key.account,
            message_id: key.message_id.to_string(),
            revision: key.revision,
        }
    }
}

fn to_rfc3339(value: DateTime<Utc>) -> String {
    value.to_rfc3339()
}

/// The channel segment of an address, which is everything before the first colon.
fn channel_of(address: &str) -> &str {
    address.split(':').next().unwrap_or_default()
}

/// The row id of the conversation whose address is bound as `?1`.
///
/// Every table that hangs off a conversation is keyed on its integer id, while the API deals only
/// in addresses, so this is how a statement gets from one to the other without a round trip. It
/// yields NULL for an address the address book has never seen, and a `NOT NULL` column then refuses
/// the write, which is the schema saying what a lookup would have said.
const CONVERSATION_ID_OF: &str = "(SELECT id FROM conversations WHERE address = ?1)";

/// The columns [`row_to_conversation`] reads, in the order it reads them.
const CONVERSATION_COLUMNS: &str =
    "address, channel, platform, title, kind, created_at, last_inbound_at, last_outbound_at";

/// The columns [`row_to_account`] reads, in the order it reads them.
const ACCOUNT_COLUMNS: &str =
    "id, channel, platform, platform_id, username, display_name, first_seen_at, last_seen_at";

/// The account currently behind the channel bound as `?1`: the real one seen most recently.
///
/// The placeholder never qualifies. Its rows are ones whose account is unknown, and treating the
/// placeholder as "what the channel used to be" would mark every message recorded before accounts
/// existed as unanswerable on every deployment that never swapped a bot.
const ACCOUNT_CURRENT: &str =
    "channel = ?1 AND platform_id <> '' ORDER BY last_seen_at DESC, id DESC LIMIT 1";

/// The columns [`row_to_policy`] reads, over [`POLICY_FROM`].
const POLICY_COLUMNS: &str = "c.address, p.mode, p.until, p.reason, p.dropped, p.created_at";

const POLICY_FROM: &str = "conversation_policy p JOIN conversations c ON c.id = p.conversation_id";

/// The columns [`row_to_watch`] reads, in the order it reads them.
const WATCH_COLUMNS: &str = "w.id, c.address, w.field, w.pattern, w.reason, w.until, w.created_at";

/// A `LEFT` join, because a watch with no conversation is one that applies to every conversation,
/// and an inner join would drop exactly those.
const WATCH_FROM: &str = "watches w LEFT JOIN conversations c ON c.id = w.conversation_id";

/// [`CONVERSATION_ID_OF`] against the third bound parameter, for statements that need the address
/// somewhere other than first.
const CONVERSATION_ID_OF_3: &str = "(SELECT id FROM conversations WHERE address = ?3)";

/// The columns [`row_to_queued`] reads, over [`QUEUE_FROM`].
const QUEUE_COLUMNS: &str = "q.seq, c.address, q.account_id, q.message_id, q.revision, q.payload,
     q.received_at, q.state, q.key, q.body, q.session_id, q.item_id, q.dropped, q.attempts,
     q.posts, q.not_before, q.last_error, q.held_at, q.posted_at";

const QUEUE_FROM: &str = "inbound_queue q JOIN conversations c ON c.id = q.conversation_id";

/// Everything a row sheds when it goes back to the queue for a hand-over of its own.
///
/// `last_error` is deliberately not in it: why the last offer came apart is the one thing worth
/// still being able to read on a row that is waiting to be tried again.
const QUEUE_CLEARED: &str = "key = NULL, body = NULL, session_id = NULL, item_id = NULL,
     dropped = 0, posts = 0, held_at = NULL, posted_at = NULL, completed_at = NULL";

/// The columns [`row_to_message`] reads, in the order it reads them, over [`MESSAGE_FROM`].
///
/// One constant shared by every query that returns a [`MessageRecord`], and aliased because the
/// search join puts `messages` next to an FTS table that also has a `text` column. Written out
/// rather than `m.*` so the order is fixed here: with eighteen positional `row.get` calls, a column
/// added to one query and not another reads back as the wrong field instead of failing.
///
/// Two columns are derived. The handles come from the attachment registry by the message's own
/// key, in the order the files sat in the message; `group_concat` with its own `ORDER BY` needs
/// SQLite 3.44, which the bundled build is well past. Whether the account is a previous one
/// compares the row's account against the channel's current one and is false, not NULL, when the
/// channel has no current account yet; the placeholder is excluded for the reason on
/// [`ACCOUNT_CURRENT`].
const MESSAGE_COLUMNS: &str = "m.id, c.address, m.account_id, m.message_id, m.revision,
     m.sender_id, m.sender_name, m.text, m.notes,
     (SELECT group_concat(t.handle, ',' ORDER BY t.position) FROM attachments t
      WHERE t.conversation_id = m.conversation_id AND t.message_id = m.message_id
        AND t.account_id = m.account_id AND t.revision = m.revision),
     m.addressed, m.seen, m.own, m.session_id, m.deleted_at, m.superseded_at, m.timestamp,
     COALESCE(a.platform_id <> '' AND a.id <> (SELECT r.id FROM accounts r
                                               WHERE r.channel = c.channel
                                                 AND r.platform_id <> ''
                                               ORDER BY r.last_seen_at DESC, r.id DESC
                                               LIMIT 1), 0)";

const MESSAGE_FROM: &str = "messages m
     JOIN conversations c ON c.id = m.conversation_id
     JOIN accounts a ON a.id = m.account_id";

/// What excludes a recorded message from everything the agent is owed.
///
/// A retracted message must not be offered as context it missed, and neither must the older wording
/// of one that has since been edited, because the revision is in the table too and is the version
/// worth showing. Both still read back through `history_read` and `history_search`, which is where
/// finding them is the point.
const MESSAGE_CURRENT: &str = "m.deleted_at IS NULL AND m.superseded_at IS NULL";

fn row_to_message(row: &rusqlite::Row<'_>) -> rusqlite::Result<MessageRecord> {
    let attachments: Option<String> = row.get(9)?;
    let deleted_at: Option<String> = row.get(14)?;
    let superseded_at: Option<String> = row.get(15)?;
    let timestamp: String = row.get(16)?;
    Ok(MessageRecord {
        id: row.get(0)?,
        conversation: row.get(1)?,
        account: row.get(2)?,
        message_id: row.get(3)?,
        revision: row.get(4)?,
        sender_id: row.get(5)?,
        sender_name: row.get(6)?,
        text: row.get(7)?,
        notes: row.get(8)?,
        attachments: attachments
            .as_deref()
            .map(|joined| joined.split(',').map(str::to_string).collect())
            .unwrap_or_default(),
        addressed: row.get(10)?,
        seen: row.get(11)?,
        own: row.get(12)?,
        session_id: row.get(13)?,
        deleted_at: deleted_at.as_deref().map(parse_rfc3339).transpose()?,
        superseded_at: superseded_at.as_deref().map(parse_rfc3339).transpose()?,
        timestamp: parse_rfc3339(&timestamp)?,
        previous_account: row.get(17)?,
    })
}

fn row_to_policy(row: &rusqlite::Row<'_>) -> rusqlite::Result<PolicyRecord> {
    let mode: String = row.get(1)?;
    let until: Option<String> = row.get(2)?;
    let created_at: String = row.get(5)?;
    Ok(PolicyRecord {
        conversation: row.get(0)?,
        policy: Policy::parse(&mode).ok_or_else(|| {
            rusqlite::Error::FromSqlConversionFailure(
                1,
                rusqlite::types::Type::Text,
                format!("{mode:?} is not a known policy").into(),
            )
        })?,
        until: until.as_deref().map(parse_rfc3339).transpose()?,
        reason: row.get(3)?,
        dropped: row.get::<_, i64>(4)?.max(0) as u64,
        created_at: parse_rfc3339(&created_at)?,
    })
}

fn row_to_watch(row: &rusqlite::Row<'_>) -> rusqlite::Result<WatchRecord> {
    let field: String = row.get(2)?;
    let until: Option<String> = row.get(5)?;
    let created_at: String = row.get(6)?;
    Ok(WatchRecord {
        id: row.get(0)?,
        conversation: row.get(1)?,
        field: WatchField::parse(&field).ok_or_else(|| {
            rusqlite::Error::FromSqlConversionFailure(
                2,
                rusqlite::types::Type::Text,
                format!("{field:?} is not a known watch field").into(),
            )
        })?,
        pattern: row.get(3)?,
        reason: row.get(4)?,
        until: until.as_deref().map(parse_rfc3339).transpose()?,
        created_at: parse_rfc3339(&created_at)?,
    })
}

fn parse_rfc3339(raw: &str) -> std::result::Result<DateTime<Utc>, rusqlite::Error> {
    DateTime::parse_from_rfc3339(raw)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })
}

fn row_to_conversation(row: &rusqlite::Row<'_>) -> rusqlite::Result<ConversationRecord> {
    let created_at: String = row.get(5)?;
    let last_inbound_at: Option<String> = row.get(6)?;
    let last_outbound_at: Option<String> = row.get(7)?;
    Ok(ConversationRecord {
        address: row.get(0)?,
        channel: row.get(1)?,
        platform: row.get(2)?,
        title: row.get(3)?,
        kind: row.get(4)?,
        created_at: parse_rfc3339(&created_at)?,
        last_inbound_at: last_inbound_at.as_deref().map(parse_rfc3339).transpose()?,
        last_outbound_at: last_outbound_at.as_deref().map(parse_rfc3339).transpose()?,
    })
}

fn row_to_account(row: &rusqlite::Row<'_>) -> rusqlite::Result<AccountRecord> {
    let first_seen_at: String = row.get(6)?;
    let last_seen_at: String = row.get(7)?;
    Ok(AccountRecord {
        id: row.get(0)?,
        channel: row.get(1)?,
        platform: row.get(2)?,
        platform_id: row.get(3)?,
        username: row.get(4)?,
        display_name: row.get(5)?,
        first_seen_at: parse_rfc3339(&first_seen_at)?,
        last_seen_at: parse_rfc3339(&last_seen_at)?,
    })
}

fn row_to_queued(row: &rusqlite::Row<'_>) -> rusqlite::Result<QueuedMessage> {
    let received_at: String = row.get(6)?;
    let state: String = row.get(7)?;
    let session_id: Option<String> = row.get(10)?;
    let not_before: Option<String> = row.get(15)?;
    let held_at: Option<String> = row.get(17)?;
    let posted_at: Option<String> = row.get(18)?;
    Ok(QueuedMessage {
        seq: row.get(0)?,
        conversation: row.get(1)?,
        account: row.get(2)?,
        message_id: row.get(3)?,
        revision: row.get(4)?,
        payload: row.get(5)?,
        received_at: parse_rfc3339(&received_at)?,
        state: QueueState::parse(&state).ok_or_else(|| {
            corrupt_column(
                7,
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("{state:?} is not a queue state"),
                ),
            )
        })?,
        key: row.get(8)?,
        body: row.get(9)?,
        session_id: session_id
            .as_deref()
            .map(Uuid::parse_str)
            .transpose()
            .map_err(|error| corrupt_column(10, error))?,
        item_id: row.get(11)?,
        dropped: row.get::<_, i64>(12)?.max(0) as u64,
        attempts: row.get(13)?,
        posts: row.get(14)?,
        not_before: not_before.as_deref().map(parse_rfc3339).transpose()?,
        last_error: row.get(16)?,
        held_at: held_at.as_deref().map(parse_rfc3339).transpose()?,
        posted_at: posted_at.as_deref().map(parse_rfc3339).transpose()?,
    })
}

/// One row by sequence, on a connection or a transaction already in hand.
fn read_row(
    connection: &rusqlite::Connection,
    seq: i64,
) -> rusqlite::Result<Option<QueuedMessage>> {
    connection
        .query_row(
            &format!("SELECT {QUEUE_COLUMNS} FROM {QUEUE_FROM} WHERE q.seq = ?1"),
            [seq],
            row_to_queued,
        )
        .optional()
}

/// Give back everything the hand-over at `seq` took when it was rendered.
///
/// The dropped-message count goes back on the counter, so the next item reports it; the backlog it
/// stated goes back among what the agent is owed, exactly and only the rows it named, which is what
/// `accounted_by` is for. Both are idempotent against a second call only because every caller
/// guards on the state transition that precedes it.
fn restore(connection: &rusqlite::Connection, seq: i64) -> rusqlite::Result<()> {
    connection.execute(
        "UPDATE meta
         SET value = CAST(CAST(value AS INTEGER)
                          + (SELECT dropped FROM inbound_queue WHERE seq = ?1) AS TEXT)
         WHERE key = ?2 AND (SELECT dropped FROM inbound_queue WHERE seq = ?1) > 0",
        rusqlite::params![seq, META_DROPPED],
    )?;
    connection.execute(
        "UPDATE messages SET seen = 0, accounted_by = NULL WHERE accounted_by = ?1",
        [seq],
    )?;
    Ok(())
}

/// A value that is not what its column promised, reported the way `rusqlite` reports one so it
/// travels the same path as every other row-reading failure.
fn corrupt_column(
    index: usize,
    error: impl std::error::Error + Send + Sync + 'static,
) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(index, rusqlite::types::Type::Text, Box::new(error))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-08-05T12:00:00Z")
            .expect("literal parses")
            .with_timezone(&Utc)
    }

    fn identity(platform_id: &str) -> AccountIdentity {
        AccountIdentity {
            platform_id: platform_id.to_string(),
            username: Some("mybot".to_string()),
            display_name: Some("My Bot".to_string()),
        }
    }

    async fn register(store: &Store, channel: &str, platform_id: &str) -> AccountId {
        store
            .register_account(channel, "telegram", &identity(platform_id), now())
            .await
            .expect("register")
            .account
    }

    fn conversation(address: &str) -> ConversationRecord {
        ConversationRecord {
            address: address.to_string(),
            channel: channel_of(address).to_string(),
            platform: "telegram".to_string(),
            title: Some("Alice".to_string()),
            kind: "direct".to_string(),
            created_at: now(),
            last_inbound_at: Some(now()),
            last_outbound_at: None,
        }
    }

    /// A store logged in on `telegram`, with the given conversations on file.
    ///
    /// Every write that names a message needs both: the account is part of a message's identity,
    /// and the schema refuses a message from an address the address book has never seen.
    async fn seeded(addresses: &[&str]) -> (Store, AccountId) {
        let store = Store::open_in_memory().await.expect("opens");
        let account = register(&store, "telegram", "111").await;
        for address in addresses {
            store
                .upsert_conversation(conversation(address))
                .await
                .expect("upsert");
        }
        (store, account)
    }

    fn key<'a>(account: AccountId, conversation: &'a str, message_id: &'a str) -> MessageKey<'a> {
        MessageKey {
            conversation,
            account,
            message_id,
            revision: 0,
        }
    }

    fn message(
        account: AccountId,
        conversation: &str,
        message_id: &str,
        text: &str,
    ) -> MessageRecord {
        MessageRecord {
            id: 0,
            conversation: conversation.to_string(),
            account,
            message_id: message_id.to_string(),
            revision: 0,
            sender_id: Some("42".to_string()),
            sender_name: "Alice".to_string(),
            text: text.to_string(),
            notes: None,
            attachments: Vec::new(),
            addressed: false,
            seen: false,
            own: false,
            session_id: None,
            deleted_at: None,
            superseded_at: None,
            timestamp: now(),
            previous_account: false,
        }
    }

    /// An edit of `message_id`, as its own row.
    fn revision(
        account: AccountId,
        conversation: &str,
        message_id: &str,
        revision: i64,
        text: &str,
    ) -> MessageRecord {
        MessageRecord {
            revision,
            ..message(account, conversation, message_id, text)
        }
    }

    /// Render claimed rows into hand-overs with nothing to say beyond their bodies.
    async fn hold_all(store: &Store, rows: &[QueuedMessage]) -> Vec<QueuedMessage> {
        hold_reporting(store, rows, 0, None).await
    }

    /// The same, with the first row carrying a dropped count and a backlog.
    async fn hold_reporting(
        store: &Store,
        rows: &[QueuedMessage],
        dropped: u64,
        backlog: Option<Backlog>,
    ) -> Vec<QueuedMessage> {
        let mut held = Vec::with_capacity(rows.len());
        for (index, row) in rows.iter().enumerate() {
            let first = index == 0;
            let key = format!("key-{}-{}", row.seq, row.attempts);
            assert!(
                store
                    .hold(
                        row.seq,
                        HandOver {
                            key: &key,
                            body: "body",
                            session_id: Uuid::nil(),
                            dropped: if first { dropped } else { 0 },
                            accounted: first.then_some(backlog).flatten(),
                        },
                        now(),
                    )
                    .await
                    .expect("hold"),
                "a claimed row must take its hand-over"
            );
            held.push(store.row(row.seq).await.expect("read").expect("row"));
        }
        held
    }

    /// Enqueue one message and hand it over, which is what `accounted_by` hangs off.
    ///
    /// The queue row is synthetic: it carries no history of its own, so what it accounts for is
    /// exactly the backlog handed in.
    async fn hand_over(
        store: &Store,
        account: AccountId,
        conversation: &str,
        message_id: &str,
        backlog: Option<Backlog>,
    ) -> i64 {
        store
            .enqueue(key(account, conversation, message_id), "{}", now(), 1_000)
            .await
            .expect("enqueue");
        let claimed = store
            .claim(&[conversation.to_string()], 100)
            .await
            .expect("claim");
        let row = claimed
            .iter()
            .find(|row| row.message_id == message_id)
            .expect("the row just enqueued was claimed");
        hold_reporting(store, std::slice::from_ref(row), 0, backlog).await;
        row.seq
    }

    /// Claim from every conversation with something waiting.
    ///
    /// What `claim` meant before readiness became per conversation. These tests are about queue
    /// mechanics rather than about which chats have settled, so they say "all of them" once here
    /// instead of naming ids at twenty call sites.
    async fn claim_all(store: &Store, limit: usize) -> Vec<QueuedMessage> {
        let ready: Vec<String> = store
            .pending_windows()
            .await
            .expect("windows")
            .into_iter()
            .map(|window| window.conversation)
            .collect();
        store.claim(&ready, limit).await.expect("claim")
    }

    #[tokio::test]
    async fn registering_an_account_is_idempotent_and_reports_a_swap() {
        let store = Store::open_in_memory().await.expect("opens");
        let first = store
            .register_account("telegram", "telegram", &identity("111"), now())
            .await
            .expect("register");
        assert!(
            first.replaced.is_none(),
            "nothing preceded the first account"
        );

        let again = store
            .register_account(
                "telegram",
                "telegram",
                &identity("111"),
                now() + chrono::Duration::days(1),
            )
            .await
            .expect("register again");
        assert_eq!(again.account, first.account, "the same bot keeps its id");
        assert!(again.replaced.is_none(), "the same bot is not a swap");

        // The bot was deleted and recreated under the same channel slot.
        let swapped = store
            .register_account(
                "telegram",
                "telegram",
                &identity("222"),
                now() + chrono::Duration::days(2),
            )
            .await
            .expect("register the new bot");
        assert_ne!(swapped.account, first.account);
        let replaced = swapped.replaced.expect("the swap is reported");
        assert_eq!(replaced.id, first.account);
        assert_eq!(replaced.platform_id, "111");
        assert_eq!(replaced.label(), "@mybot");

        let current = store
            .current_account("telegram")
            .await
            .expect("read")
            .expect("present");
        assert_eq!(
            current.id, swapped.account,
            "the newest real account is current"
        );
        assert_eq!(store.accounts().await.expect("list").len(), 2);
    }

    #[tokio::test]
    async fn a_recreated_bot_reusing_message_ids_is_not_a_duplicate() {
        // The bug this whole shape exists for. A Telegram bot deleted and recreated under the same
        // channel slot numbers its private chats from 1 again, at the same addresses. Keyed on the
        // address and the message id alone, every message whose id the old bot had already used was
        // swallowed by the unique constraint: never queued, never recorded, and nothing said so.
        let (store, old_bot) = seeded(&["telegram:123"]).await;
        store
            .record_message(message(old_bot, "telegram:123", "1", "said to the old bot"))
            .await
            .expect("record");
        store
            .enqueue(key(old_bot, "telegram:123", "1"), "old", now(), 10)
            .await
            .expect("enqueue");

        let new_bot = register(&store, "telegram", "222").await;
        assert!(
            store
                .record_message(message(new_bot, "telegram:123", "1", "said to the new bot"))
                .await
                .expect("record"),
            "the same message id under a different account is a different message"
        );
        assert_eq!(
            store
                .enqueue(key(new_bot, "telegram:123", "1"), "new", now(), 10)
                .await
                .expect("enqueue"),
            EnqueueOutcome::Queued,
            "and it has to reach the agent"
        );

        let history = store
            .history("telegram:123", 10, None, None)
            .await
            .expect("read");
        let texts: Vec<(&str, bool)> = history
            .iter()
            .map(|row| (row.text.as_str(), row.previous_account))
            .collect();
        assert_eq!(
            texts,
            vec![
                ("said to the old bot", true),
                ("said to the new bot", false)
            ],
            "both are kept, and the old bot's row says its message id can no longer be acted on"
        );
    }

    #[tokio::test]
    async fn a_redelivery_under_the_same_account_is_still_recognised() {
        // The other half of the contract: scoping by account must not turn Telegram replaying an
        // unconfirmed batch into a second delivery.
        let (store, account) = seeded(&["telegram:123"]).await;
        store
            .enqueue(key(account, "telegram:123", "m1"), "a", now(), 10)
            .await
            .expect("first");
        assert_eq!(
            store
                .enqueue(key(account, "telegram:123", "m1"), "a", now(), 10)
                .await
                .expect("second"),
            EnqueueOutcome::Duplicate
        );
        assert_eq!(store.pending_count().await.expect("count"), 1);
    }

    #[tokio::test]
    async fn a_message_from_an_unknown_address_is_refused_rather_than_filed_under_nothing() {
        let (store, account) = seeded(&[]).await;
        let error = store
            .record_message(message(account, "telegram:404", "1", "lost"))
            .await
            .expect_err("no conversation row exists for the address");
        assert!(
            error.to_string().contains("NOT NULL"),
            "the schema is what refuses it: {error}"
        );
    }

    #[tokio::test]
    async fn when_the_agent_last_spoke_survives_a_restart() {
        // The address book orders on this and reports it, so a restart losing it would put a
        // conversation the agent has only ever written to at the bottom of a list it belongs at the
        // top of.
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("state.db");
        let sent_at = now();
        {
            let store = Store::open(&path).await.expect("opens");
            store
                .touch_outbound("telegram:-100", "telegram", sent_at)
                .await
                .expect("outbound");
        }

        let reopened = Store::open(&path).await.expect("reopens");
        let record = reopened
            .conversation("telegram:-100")
            .await
            .expect("read")
            .expect("the conversation is on file");
        assert_eq!(record.last_outbound_at, Some(sent_at));
        assert_eq!(record.channel, "telegram");
        assert_eq!(record.kind, "unknown");
    }

    #[tokio::test]
    async fn a_policy_round_trips_with_its_expiry() {
        let store = Store::open_in_memory().await.expect("opens");
        let until = now() + chrono::Duration::minutes(30);
        store
            .set_policy(
                "telegram:1",
                "telegram",
                Policy::Block,
                Some(until),
                Some("standup spam"),
                now(),
            )
            .await
            .expect("set");
        let record = store
            .policy("telegram:1")
            .await
            .expect("read")
            .expect("present");
        assert_eq!(record.conversation, "telegram:1");
        assert_eq!(record.policy, Policy::Block);
        assert_eq!(record.until, Some(until));
        assert_eq!(record.reason.as_deref(), Some("standup spam"));
        assert_eq!(record.dropped, 0);
        assert!(store.policy("telegram:2").await.expect("read").is_none());
    }

    #[tokio::test]
    async fn ruling_on_a_chat_that_has_not_written_puts_it_in_the_address_book() {
        // Pre-emptive, which is legitimate, and the row it mints is what the foreign key needs.
        // The first message from there then fills in what the address cannot say.
        let (store, account) = seeded(&[]).await;
        store
            .set_policy("telegram:-100", "telegram", Policy::Mute, None, None, now())
            .await
            .expect("set");
        let minted = store
            .conversation("telegram:-100")
            .await
            .expect("read")
            .expect("minted by the policy");
        assert_eq!(minted.kind, "unknown");
        assert_eq!(minted.channel, "telegram");
        assert_eq!(minted.title, None);

        let mut arrived = conversation("telegram:-100");
        arrived.kind = "group".to_string();
        arrived.title = Some("Deploy Crew".to_string());
        store.upsert_conversation(arrived).await.expect("upsert");
        let filled = store
            .conversation("telegram:-100")
            .await
            .expect("read")
            .expect("present");
        assert_eq!(filled.kind, "group");
        assert_eq!(filled.title.as_deref(), Some("Deploy Crew"));
        assert_eq!(
            store
                .policy("telegram:-100")
                .await
                .expect("read")
                .map(|record| record.policy),
            Some(Policy::Mute),
            "and the decision made before it wrote still stands"
        );
        store
            .record_message(message(account, "telegram:-100", "1", "hello"))
            .await
            .expect("the minted row is a real conversation");
    }

    #[tokio::test]
    async fn every_policy_survives_a_round_trip() {
        // `active` in particular. It is a real decision rather than the absence of one: a group
        // whose configured default is `mute` needs somewhere to record that this one is not.
        let store = Store::open_in_memory().await.expect("opens");
        for policy in [Policy::Active, Policy::Mute, Policy::Block] {
            store
                .set_policy("telegram:1", "telegram", policy, None, None, now())
                .await
                .expect("set");
            let record = store
                .policy("telegram:1")
                .await
                .expect("read")
                .expect("present");
            assert_eq!(record.policy, policy);
            assert_eq!(record.until, None);
        }
    }

    #[tokio::test]
    async fn clearing_a_policy_reports_whether_one_was_in_place() {
        let store = Store::open_in_memory().await.expect("opens");
        assert!(!store.clear_policy("telegram:1").await.expect("clear"));
        store
            .set_policy("telegram:1", "telegram", Policy::Mute, None, None, now())
            .await
            .expect("set");
        assert!(store.clear_policy("telegram:1").await.expect("clear"));
        assert!(store.policy("telegram:1").await.expect("read").is_none());
    }

    #[tokio::test]
    async fn a_write_is_visible_immediately_despite_the_cache() {
        // The cache would otherwise hold a read for its whole window, which would make the tool
        // that sets a policy appear not to have worked.
        let store = Store::open_in_memory().await.expect("opens");
        assert!(store.policy("telegram:1").await.expect("read").is_none());
        store
            .set_policy("telegram:1", "telegram", Policy::Block, None, None, now())
            .await
            .expect("set");
        let record = store
            .policy("telegram:1")
            .await
            .expect("read")
            .expect("the write must invalidate the cached miss");
        assert_eq!(record.policy, Policy::Block);

        store.clear_policy("telegram:1").await.expect("clear");
        assert!(
            store.policy("telegram:1").await.expect("read").is_none(),
            "clearing must invalidate the cached hit"
        );
    }

    #[tokio::test]
    async fn re_ruling_starts_the_drop_count_over() {
        // The count is reported when the policy lapses, so it has to belong to the decision being
        // reported rather than to every decision this conversation has ever had.
        let store = Store::open_in_memory().await.expect("opens");
        store
            .set_policy("telegram:1", "telegram", Policy::Block, None, None, now())
            .await
            .expect("set");
        store.note_blocked_drop("telegram:1").await.expect("count");
        store.note_blocked_drop("telegram:1").await.expect("count");
        // Read uncached: `policy` serves the drop count from a window that `note_blocked_drop`
        // deliberately does not invalidate.
        let listed = store.list_policies().await.expect("list");
        assert_eq!(listed[0].dropped, 2);

        store
            .set_policy("telegram:1", "telegram", Policy::Block, None, None, now())
            .await
            .expect("re-set");
        let listed = store.list_policies().await.expect("list");
        assert_eq!(listed[0].dropped, 0);
    }

    #[tokio::test]
    async fn expiring_a_policy_returns_the_exact_drop_count_and_removes_it() {
        let store = Store::open_in_memory().await.expect("opens");
        store
            .set_policy("telegram:1", "telegram", Policy::Block, None, None, now())
            .await
            .expect("set");
        store.note_blocked_drop("telegram:1").await.expect("count");
        store.note_blocked_drop("telegram:1").await.expect("count");
        store.note_blocked_drop("telegram:1").await.expect("count");

        let expired = store
            .expire_policy("telegram:1")
            .await
            .expect("expire")
            .expect("present");
        assert_eq!(
            expired.dropped, 3,
            "the count the agent is told must not come from the cache"
        );
        assert!(store.policy("telegram:1").await.expect("read").is_none());
    }

    #[tokio::test]
    async fn policies_are_listed_for_the_operator() {
        let store = Store::open_in_memory().await.expect("opens");
        store
            .set_policy("telegram:1", "telegram", Policy::Mute, None, None, now())
            .await
            .expect("set");
        store
            .set_policy(
                "telegram:2",
                "telegram",
                Policy::Block,
                Some(now() + chrono::Duration::hours(1)),
                None,
                now() + chrono::Duration::seconds(1),
            )
            .await
            .expect("set");
        let listed = store.list_policies().await.expect("list");
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0].conversation, "telegram:1");
        assert_eq!(listed[0].policy, Policy::Mute);
        assert_eq!(listed[1].conversation, "telegram:2");
        assert_eq!(listed[1].policy, Policy::Block);
    }

    #[tokio::test]
    async fn counting_a_drop_on_an_unruled_conversation_is_harmless() {
        // The writer only calls this behind a policy lookup, but a race between an operator lifting
        // a block and a message arriving must not become an error the message dies of.
        let store = Store::open_in_memory().await.expect("opens");
        store
            .note_blocked_drop("telegram:1")
            .await
            .expect("no row is not an error");
    }

    #[tokio::test]
    async fn a_recorded_message_reads_back() {
        let (store, account) = seeded(&["telegram:1"]).await;
        let mut record = message(account, "telegram:1", "10", "the deploy is stuck");
        record.notes = Some("photo".to_string());
        assert!(
            store.record_message(record.clone()).await.expect("record"),
            "a first recording is a write"
        );
        let history = store
            .history("telegram:1", 10, None, None)
            .await
            .expect("read");
        // The id is assigned on insert, so it is the one field the caller cannot predict.
        assert_eq!(history.len(), 1);
        assert!(history[0].id > 0, "a paging cursor must be handed back");
        assert_eq!(history[0], MessageRecord {
            id: history[0].id,
            ..record
        });
    }

    #[tokio::test]
    async fn a_message_reads_back_with_the_handles_of_its_files_in_order() {
        // The handles are no longer a column of the message: they come from the registry by the
        // message's own key, so a row can never quote a handle the sweep has already taken. The
        // order is the order the files sat in the message, which is what "the second picture"
        // means.
        let (store, account) = seeded(&["telegram:1"]).await;
        let mut handles = Vec::new();
        for position in [1, 0, 2] {
            handles.push((
                position,
                store
                    .register_attachment(attachment_record(account, "10", position, 0, None))
                    .await
                    .expect("register"),
            ));
        }
        handles.sort_by_key(|(position, _)| *position);
        store
            .record_message(message(account, "telegram:1", "10", "three photos"))
            .await
            .expect("record");
        store
            .record_message(message(account, "telegram:1", "11", "no photos"))
            .await
            .expect("record");

        let history = store
            .history("telegram:1", 10, None, None)
            .await
            .expect("read");
        assert_eq!(
            history[0].attachments,
            handles
                .into_iter()
                .map(|(_, handle)| handle)
                .collect::<Vec<_>>(),
            "in position order, whatever order they were registered in"
        );
        assert!(history[1].attachments.is_empty());
    }

    #[tokio::test]
    async fn recording_the_same_message_twice_keeps_one_copy() {
        // A platform replaying updates after a crash must not double the history.
        let (store, account) = seeded(&["telegram:1"]).await;
        let record = message(account, "telegram:1", "10", "hello");
        assert!(store.record_message(record.clone()).await.expect("record"));
        assert!(
            !store.record_message(record).await.expect("re-record"),
            "a replay is reported as one rather than written"
        );
        assert_eq!(
            store
                .history("telegram:1", 10, None, None)
                .await
                .expect("read")
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn history_returns_the_most_recent_in_the_order_they_were_said() {
        let (store, account) = seeded(&["telegram:1"]).await;
        for index in 0..5 {
            let mut record = message(
                account,
                "telegram:1",
                &index.to_string(),
                &format!("line {index}"),
            );
            record.timestamp = now() + chrono::Duration::seconds(index);
            store.record_message(record).await.expect("record");
        }
        let history = store
            .history("telegram:1", 3, None, None)
            .await
            .expect("read");
        let texts: Vec<&str> = history.iter().map(|row| row.text.as_str()).collect();
        assert_eq!(
            texts,
            vec!["line 2", "line 3", "line 4"],
            "the last three, oldest first"
        );
    }

    #[tokio::test]
    async fn paging_back_through_history_skips_nothing() {
        // Telegram stamps to the second, so a burst shares one timestamp. Paging on the timestamp
        // alone would drop every message in the second the previous page ended in, and the caller
        // would never know: the pages would simply not add up.
        let (store, account) = seeded(&["telegram:1"]).await;
        let stamp = now();
        for index in 0..6 {
            let mut record = message(
                account,
                "telegram:1",
                &index.to_string(),
                &format!("line {index}"),
            );
            // Two distinct seconds, three messages in each.
            record.timestamp = stamp + chrono::Duration::seconds(i64::from(index / 3));
            store.record_message(record).await.expect("record");
        }

        let mut seen = Vec::new();
        let mut cursor = None;
        loop {
            let page = store
                .history("telegram:1", 2, cursor, None)
                .await
                .expect("read");
            let Some(oldest) = page.first() else { break };
            cursor = Some(oldest.id);
            seen.extend(page.iter().map(|row| row.text.clone()));
        }
        seen.sort();
        assert_eq!(
            seen,
            vec!["line 0", "line 1", "line 2", "line 3", "line 4", "line 5"],
            "every message must appear exactly once across the pages"
        );
    }

    #[tokio::test]
    async fn history_is_scoped_to_one_conversation() {
        let (store, account) = seeded(&["telegram:1", "telegram:2"]).await;
        store
            .record_message(message(account, "telegram:1", "1", "ours"))
            .await
            .expect("record");
        store
            .record_message(message(account, "telegram:2", "1", "theirs"))
            .await
            .expect("record");
        let history = store
            .history("telegram:1", 10, None, None)
            .await
            .expect("read");
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].text, "ours");
    }

    #[tokio::test]
    async fn search_finds_a_message_and_can_be_narrowed_to_one_chat() {
        let (store, account) = seeded(&["telegram:1", "telegram:2"]).await;
        store
            .record_message(message(
                account,
                "telegram:1",
                "1",
                "the certificate expires on friday",
            ))
            .await
            .expect("record");
        store
            .record_message(message(
                account,
                "telegram:2",
                "1",
                "certificate renewal is automated",
            ))
            .await
            .expect("record");
        store
            .record_message(message(account, "telegram:1", "2", "lunch?"))
            .await
            .expect("record");

        let hits = store
            .search_messages("certificate", None, 10)
            .await
            .expect("search");
        assert_eq!(hits.len(), 2);

        let scoped = store
            .search_messages("certificate", Some("telegram:2"), 10)
            .await
            .expect("search");
        assert_eq!(scoped.len(), 1);
        assert_eq!(scoped[0].conversation, "telegram:2");
    }

    #[tokio::test]
    async fn pruning_history_takes_the_search_index_with_it() {
        // External-content FTS5 keeps no copy of the text, so a delete that skipped the trigger
        // would leave the index pointing at rows that no longer exist.
        let (store, account) = seeded(&["telegram:1"]).await;
        let mut old = message(account, "telegram:1", "1", "ancient business");
        old.timestamp = now() - chrono::Duration::days(40);
        store.record_message(old).await.expect("record");
        store
            .record_message(message(
                account,
                "telegram:1",
                "2",
                "ancient history repeats",
            ))
            .await
            .expect("record");

        let pruned = store
            .prune_messages(now() - chrono::Duration::days(30))
            .await
            .expect("prune");
        assert_eq!(pruned, 1);
        let hits = store
            .search_messages("ancient", None, 10)
            .await
            .expect("search");
        assert_eq!(hits.len(), 1, "the pruned row must leave the index too");
        assert_eq!(hits[0].message_id, "2");
    }

    #[tokio::test]
    async fn taking_unseen_counts_everything_and_returns_only_the_tail() {
        let (store, account) = seeded(&["telegram:1"]).await;
        for index in 0..10 {
            let mut record = message(
                account,
                "telegram:1",
                &index.to_string(),
                &format!("line {index}"),
            );
            record.timestamp = now() + chrono::Duration::seconds(index);
            store.record_message(record).await.expect("record");
        }
        let (count, context, _) = store
            .take_unseen("telegram:1", now() + chrono::Duration::hours(1), 3)
            .await
            .expect("take");
        assert_eq!(count, 10, "the agent is told everything it missed");
        let texts: Vec<&str> = context.iter().map(|row| row.text.as_str()).collect();
        assert_eq!(texts, vec!["line 7", "line 8", "line 9"]);
    }

    #[tokio::test]
    async fn a_deletion_marks_the_row_and_leaves_it_readable() {
        // The reversal of the earlier behaviour, which removed the row. A message the agent was
        // shown is in its session for good, so the record is the only thing that can later tell it
        // the message was withdrawn; erasing the row erased the notice along with the text.
        let (store, account) = seeded(&["telegram:1"]).await;
        store
            .record_message(message(account, "telegram:1", "1", "said too much"))
            .await
            .expect("record");

        let marked = store
            .mark_deleted("telegram:1", account, "1", now())
            .await
            .expect("mark");
        assert!(marked, "the row was there to mark");

        let history = store
            .history("telegram:1", 10, None, None)
            .await
            .expect("read");
        assert_eq!(history.len(), 1, "the row must survive the deletion");
        assert!(
            history.first().is_some_and(|row| row.deleted_at.is_some()),
            "and must say it was deleted"
        );
        let found = store
            .search_messages("said", None, 10)
            .await
            .expect("search");
        assert_eq!(found.len(), 1, "a deleted message is still searchable");
    }

    #[tokio::test]
    async fn a_deletion_names_the_message_of_one_account_only() {
        // The same message id under the old bot is a different message, and a deletion reported
        // to the new bot says nothing about it.
        let (store, old_bot) = seeded(&["telegram:1"]).await;
        store
            .record_message(message(
                old_bot,
                "telegram:1",
                "1",
                "the old bot's message 1",
            ))
            .await
            .expect("record");
        let new_bot = register(&store, "telegram", "222").await;
        store
            .record_message(message(
                new_bot,
                "telegram:1",
                "1",
                "the new bot's message 1",
            ))
            .await
            .expect("record");

        assert!(
            store
                .mark_deleted("telegram:1", new_bot, "1", now())
                .await
                .expect("mark")
        );
        let history = store
            .history("telegram:1", 10, None, None)
            .await
            .expect("read");
        let deleted: Vec<&str> = history
            .iter()
            .filter(|row| row.deleted_at.is_some())
            .map(|row| row.text.as_str())
            .collect();
        assert_eq!(deleted, vec!["the new bot's message 1"]);
    }

    #[tokio::test]
    async fn marking_a_deletion_twice_keeps_the_first_time() {
        // A platform can report the same deletion more than once, and a redelivery must not move
        // the time it happened forward.
        let (store, account) = seeded(&["telegram:1"]).await;
        store
            .record_message(message(account, "telegram:1", "1", "said too much"))
            .await
            .expect("record");
        let first = now();
        assert!(
            store
                .mark_deleted("telegram:1", account, "1", first)
                .await
                .expect("mark")
        );
        assert!(
            !store
                .mark_deleted(
                    "telegram:1",
                    account,
                    "1",
                    first + chrono::Duration::hours(1)
                )
                .await
                .expect("mark"),
            "the second report changes nothing"
        );
        let history = store
            .history("telegram:1", 10, None, None)
            .await
            .expect("read");
        assert_eq!(
            history.first().and_then(|row| row.deleted_at),
            Some(first),
            "the time kept is when it was first reported gone"
        );
    }

    #[tokio::test]
    async fn an_edit_supersedes_the_wording_it_replaced() {
        // Both rows stay, because "what did they say before they changed it" is worth answering,
        // but only one of them is current. Without the mark `history_read` returns the two wordings
        // as separate messages that both look like the live one.
        let (store, account) = seeded(&["telegram:1"]).await;
        store
            .record_message(message(account, "telegram:1", "7", "meet at four"))
            .await
            .expect("record");
        let edit = revision(
            account,
            "telegram:1",
            "7",
            1_754_400_600_000,
            "meet at five",
        );
        store.record_message(edit.clone()).await.expect("record");

        let marked = store
            .supersede_message(edit.key(), now())
            .await
            .expect("supersede");
        assert_eq!(marked, 1, "only the older wording is marked");

        let history = store
            .history("telegram:1", 10, None, None)
            .await
            .expect("read");
        assert_eq!(history.len(), 2, "both wordings are kept");
        let superseded: Vec<&str> = history
            .iter()
            .filter(|row| row.superseded_at.is_some())
            .map(|row| row.text.as_str())
            .collect();
        assert_eq!(
            superseded,
            vec!["meet at four"],
            "the revision itself must not be marked"
        );
    }

    #[tokio::test]
    async fn a_message_edited_twice_keeps_exactly_one_current_wording() {
        // Superseding by "every row that is not this one" is neither idempotent nor order
        // insensitive, and both failures end the same way: every wording marked, none current. The
        // agent is told the revision is elsewhere in the list under the same message id, and there
        // would be no such row; `MESSAGE_CURRENT` would then hide the message from `unseen` and the
        // missed-context lookback for good.
        //
        // Redelivery is the reachable path. The writer records before it enqueues, and the queue's
        // duplicate branch exists because a lost `getUpdates` confirmation re-offers a batch, so an
        // older edit arriving a second time is ordinary rather than exotic.
        let (store, account) = seeded(&["telegram:1"]).await;
        let current = async || {
            store
                .history("telegram:1", 10, None, None)
                .await
                .expect("read")
                .into_iter()
                .filter(|row| row.superseded_at.is_none())
                .map(|row| row.text)
                .collect::<Vec<_>>()
        };
        for (edit, text) in [(0, "one"), (1_000, "two"), (2_000, "three")] {
            let record = revision(account, "telegram:1", "9", edit, text);
            store.record_message(record.clone()).await.expect("record");
            store
                .supersede_message(record.key(), now())
                .await
                .expect("supersede");
        }
        assert_eq!(current().await, vec!["three"], "the last edit is current");

        // The same edit again, the way a replayed batch delivers it.
        let replayed = revision(account, "telegram:1", "9", 1_000, "two");
        store
            .record_message(replayed.clone())
            .await
            .expect("record");
        store
            .supersede_message(replayed.key(), now())
            .await
            .expect("supersede");
        assert_eq!(
            current().await,
            vec!["three"],
            "a redelivered older edit must not supersede the wording that replaced it"
        );
    }

    #[tokio::test]
    async fn what_the_agent_is_owed_skips_deleted_and_superseded_rows() {
        // Offering a retracted message, or wording that has since been rewritten, as "here is what
        // you missed" presents something untrue as news. The count and the excerpt have to agree on
        // that, or the agent is told about messages it is never shown.
        let (store, account) = seeded(&["telegram:1"]).await;
        for (message_id, text) in [("1", "kept"), ("2", "retracted"), ("3", "the old wording")] {
            store
                .record_message(message(account, "telegram:1", message_id, text))
                .await
                .expect("record");
        }
        store
            .mark_deleted("telegram:1", account, "2", now())
            .await
            .expect("mark");
        // Recorded rather than merely named: superseding is ordered against the revision's own row,
        // so a replacement that is not in the table marks nothing, which is the point.
        let edit = revision(account, "telegram:1", "3", 1_000, "the new wording");
        store.record_message(edit.clone()).await.expect("record");
        store
            .supersede_message(edit.key(), now())
            .await
            .expect("supersede");

        let summary = store
            .unseen_summary(Some("telegram:1"))
            .await
            .expect("summary");
        assert_eq!(
            summary.count, 2,
            "the live message and the current wording of the edited one"
        );
        let counts = store.unseen_counts().await.expect("counts");
        assert_eq!(counts.get("telegram:1"), Some(&2));

        let (count, context, _) = store
            .take_unseen("telegram:1", now() + chrono::Duration::hours(1), 10)
            .await
            .expect("take");
        assert_eq!(count, 2);
        let texts: Vec<&str> = context.iter().map(|row| row.text.as_str()).collect();
        assert_eq!(
            texts,
            vec!["kept", "the new wording"],
            "the count and the excerpt must describe the same set, and neither the retracted \
             message nor the wording an edit replaced is in it"
        );
    }

    #[tokio::test]
    async fn a_message_edited_before_it_was_shown_leaves_nothing_stranded() {
        // The two halves of this change meet here. Superseding hides the older wording from
        // everything the agent is owed, and the watermark is the highest id among the rows that
        // survive that filter, so the arithmetic only works if the row it hides sorts below the row
        // it keeps. If it did not, the older wording would sit unseen forever: skipped by the
        // filter on the way out and left behind by `mark_seen` on the way back.
        let (store, account) = seeded(&["telegram:1"]).await;
        store
            .record_message(message(account, "telegram:1", "5", "meet at four"))
            .await
            .expect("record");
        let edit = revision(account, "telegram:1", "5", 1_000, "meet at five");
        store.record_message(edit.clone()).await.expect("record");
        store
            .supersede_message(edit.key(), now())
            .await
            .expect("supersede");

        let through = now() + chrono::Duration::hours(1);
        let (count, context, watermark) = store
            .take_unseen("telegram:1", through, 10)
            .await
            .expect("take");
        assert_eq!(count, 1, "one message is owed, not two wordings of it");
        assert_eq!(
            context.first().map(|row| row.text.as_str()),
            Some("meet at five")
        );
        hand_over(
            &store,
            account,
            "telegram:1",
            "accounting-1",
            Some(Backlog { watermark, through }),
        )
        .await;

        assert_eq!(
            store
                .unseen_summary(Some("telegram:1"))
                .await
                .expect("summary")
                .count,
            0,
            "nothing is left owed"
        );
        assert!(
            store
                .history("telegram:1", 10, None, None)
                .await
                .expect("read")
                .iter()
                .all(|row| row.seen),
            "and no wording is left unseen behind the filter that hid it"
        );
    }

    #[tokio::test]
    async fn a_recorded_message_remembers_which_side_it_came_from() {
        // The columns that make the history cover both directions. Read back wrong, the agent's own
        // messages would be indistinguishable from another bot's in the same chat.
        let (store, account) = seeded(&["telegram:1"]).await;
        let mut record = message(account, "telegram:1", "1", "on it");
        record.own = true;
        record.session_id = Some("scheduled-news".to_string());
        record.seen = true;
        store.record_message(record).await.expect("record");

        let history = store
            .history("telegram:1", 10, None, None)
            .await
            .expect("read");
        let row = history.first().expect("one row");
        assert!(row.own);
        assert_eq!(row.session_id.as_deref(), Some("scheduled-news"));
        assert_eq!(
            store
                .unseen_summary(Some("telegram:1"))
                .await
                .expect("summary")
                .count,
            0,
            "the bridge's own output is not a backlog owed to the agent"
        );
    }

    #[tokio::test]
    async fn asking_what_is_unseen_does_not_spend_it() {
        // The whole reason this exists alongside `take_unseen`. A watcher asks on a timer, and if
        // asking consumed the backlog the turn it went on to trigger would find an empty room.
        let (store, account) = seeded(&["telegram:1"]).await;
        store
            .record_message(message(account, "telegram:1", "1", "the deploy is stuck"))
            .await
            .expect("record");

        let first = store
            .unseen_summary(Some("telegram:1"))
            .await
            .expect("summary");
        assert_eq!(first.count, 1);
        let again = store
            .unseen_summary(Some("telegram:1"))
            .await
            .expect("summary");
        assert_eq!(again, first, "asking twice must give the same answer");

        let (count, ..) = store
            .take_unseen("telegram:1", now() + chrono::Duration::hours(1), 5)
            .await
            .expect("take");
        assert_eq!(count, 1, "the backlog is still there for the turn to read");
    }

    #[tokio::test]
    async fn the_watcher_marker_moves_when_a_chat_does_and_not_otherwise() {
        // A watcher fires on this string changing, so a new message has to change it and nothing
        // else may. Both halves matter and the second is the one that costs turns: firing on
        // something other than a message sends the agent to read a room that has not moved.
        let (store, account) = seeded(&["telegram:1", "telegram:2"]).await;
        let marker = async || store.unseen_summary(None).await.expect("summary").marker();

        let quiet = marker().await;
        assert_eq!(quiet, "never", "a bridge that has heard nothing says so");

        let mut first = message(account, "telegram:1", "1", "one");
        first.timestamp = now();
        store.record_message(first).await.expect("record");
        let after_one = marker().await;
        assert_ne!(quiet, after_one, "a new message has to register");

        assert_eq!(marker().await, after_one, "a quiet room must not fire");

        // The one that used to fire wrongly. An ordinary turn sweeps the backlog to zero, which
        // moved a count-carrying marker and sent the watcher to announce news the agent had just
        // been handed.
        hand_over(
            &store,
            account,
            "telegram:1",
            "accounting-everything",
            Some(Backlog {
                watermark: i64::MAX,
                through: now() + chrono::Duration::hours(1),
            }),
        )
        .await;
        assert_eq!(
            marker().await,
            after_one,
            "being shown the backlog is not news of a new message"
        );

        let mut second = message(account, "telegram:2", "2", "two");
        second.timestamp = now() + chrono::Duration::seconds(30);
        store.record_message(second).await.expect("record");
        assert_ne!(
            marker().await,
            after_one,
            "a message in another chat still has to register on a bridge-wide watch"
        );
    }

    #[tokio::test]
    async fn the_agents_own_message_does_not_move_the_watcher_marker() {
        // The marker is what a scheduled job polls to decide whether a chat is worth a turn. Once
        // the bridge started recording its own output, the agent replying moved it, so a watcher
        // fired on the main session having spoken and spent a turn reading a room whose only new
        // message was the agent's own. Nothing was recorded here before, which is why the marker
        // was safe; keeping it safe now means saying so.
        let (store, account) = seeded(&["telegram:1"]).await;
        let marker = async || store.unseen_summary(None).await.expect("summary").marker();

        let mut theirs = message(account, "telegram:1", "1", "the deploy is stuck");
        theirs.timestamp = now();
        store.record_message(theirs).await.expect("record");
        let after_theirs = marker().await;

        let mut ours = message(account, "telegram:1", "2", "on it");
        ours.own = true;
        ours.seen = true;
        ours.timestamp = now() + chrono::Duration::seconds(30);
        store.record_message(ours).await.expect("record");
        assert_eq!(
            marker().await,
            after_theirs,
            "answering a chat is not the chat moving"
        );
    }

    #[tokio::test]
    async fn retracting_the_newest_message_still_moves_the_marker_back() {
        // Documented on `marker` as news of a kind, and it used to happen for free because a
        // deletion removed the row. Now the row is kept, so the marker has to skip it deliberately
        // or a watcher never learns that the message it last fired on has been withdrawn.
        let (store, account) = seeded(&["telegram:1"]).await;
        let marker = async || store.unseen_summary(None).await.expect("summary").marker();

        let mut first = message(account, "telegram:1", "1", "one");
        first.timestamp = now();
        store.record_message(first).await.expect("record");
        let after_first = marker().await;

        let mut second = message(account, "telegram:1", "2", "spoke too soon");
        second.timestamp = now() + chrono::Duration::seconds(30);
        store.record_message(second).await.expect("record");
        assert_ne!(marker().await, after_first);

        store
            .mark_deleted("telegram:1", account, "2", now())
            .await
            .expect("mark");
        assert_eq!(
            marker().await,
            after_first,
            "a retraction has to move the marker back, as removing the row used to"
        );
    }

    #[tokio::test]
    async fn the_marker_and_the_backlog_answer_different_questions() {
        // The count is what a person wants and the marker is what a watcher can use. Keeping both
        // is the point: collapsing them either fires spuriously or reports nothing useful.
        let (store, account) = seeded(&["telegram:1"]).await;
        store
            .record_message(message(account, "telegram:1", "1", "one"))
            .await
            .expect("record");
        hand_over(
            &store,
            account,
            "telegram:1",
            "accounting-everything",
            Some(Backlog {
                watermark: i64::MAX,
                through: now() + chrono::Duration::hours(1),
            }),
        )
        .await;

        let summary = store
            .unseen_summary(Some("telegram:1"))
            .await
            .expect("summary");
        assert_eq!(summary.count, 0, "it has been shown");
        assert_eq!(
            summary.newest, None,
            "so there is no unseen message to date"
        );
        assert!(
            summary.latest.is_some(),
            "but the chat has still said something, which is what a watcher tracks"
        );
        assert_eq!(summary.line(), "0 unseen");
        assert_ne!(summary.marker(), "never");
    }

    #[tokio::test]
    async fn unseen_can_be_asked_about_one_chat_or_all_of_them() {
        let (store, account) = seeded(&["telegram:1", "telegram:2"]).await;
        store
            .record_message(message(account, "telegram:1", "1", "one"))
            .await
            .expect("record");
        store
            .record_message(message(account, "telegram:2", "2", "two"))
            .await
            .expect("record");

        assert_eq!(
            store
                .unseen_summary(Some("telegram:1"))
                .await
                .expect("summary")
                .count,
            1
        );
        assert_eq!(store.unseen_summary(None).await.expect("summary").count, 2);
        assert_eq!(
            store
                .unseen_summary(Some("telegram:999"))
                .await
                .expect("summary"),
            UnseenSummary {
                count: 0,
                newest: None,
                latest: None
            },
            "a chat nothing has arrived from is empty, not an error"
        );
    }

    #[tokio::test]
    async fn unseen_is_not_reported_twice() {
        let (store, account) = seeded(&["telegram:1"]).await;
        store
            .record_message(message(account, "telegram:1", "1", "first"))
            .await
            .expect("record");
        let through = now() + chrono::Duration::hours(1);
        let (count, _, watermark) = store
            .take_unseen("telegram:1", through, 5)
            .await
            .expect("take");
        assert_eq!(count, 1);

        // Reading is not spending. The backlog is only marked once a turn carrying it has actually
        // been accepted, so until then the same count comes back.
        let (count, ..) = store
            .take_unseen("telegram:1", through, 5)
            .await
            .expect("take");
        assert_eq!(count, 1, "reading it again must not consume it");

        hand_over(
            &store,
            account,
            "telegram:1",
            "accounting-2",
            Some(Backlog { watermark, through }),
        )
        .await;
        let (count, context, _) = store
            .take_unseen("telegram:1", through, 5)
            .await
            .expect("take");
        assert_eq!(count, 0, "already reported once");
        assert!(context.is_empty());
    }

    #[tokio::test]
    async fn unseen_ignores_anything_after_the_message_that_woke_the_agent() {
        // The cut-off is the waking message's own timestamp, so a message that lands while the turn
        // is being assembled stays unseen and is reported next time rather than silently marked.
        let (store, account) = seeded(&["telegram:1"]).await;
        let mut earlier = message(account, "telegram:1", "1", "before");
        earlier.timestamp = now();
        store.record_message(earlier).await.expect("record");
        let mut later = message(account, "telegram:1", "2", "after");
        later.timestamp = now() + chrono::Duration::minutes(5);
        store.record_message(later).await.expect("record");

        let cutoff = now() + chrono::Duration::minutes(1);
        let (count, _, watermark) = store
            .take_unseen("telegram:1", cutoff, 5)
            .await
            .expect("take");
        assert_eq!(count, 1);
        hand_over(
            &store,
            account,
            "telegram:1",
            "accounting-3",
            Some(Backlog {
                watermark,
                through: cutoff,
            }),
        )
        .await;
        let (count, ..) = store
            .take_unseen("telegram:1", now() + chrono::Duration::hours(1), 5)
            .await
            .expect("take");
        assert_eq!(count, 1, "the later one is still owed");
    }

    #[tokio::test]
    async fn a_message_recorded_out_of_order_is_not_marked_seen_either() {
        // The other door into the same silent loss. Keying on the row id alone closes the mid-turn
        // window and opens this one: an edit carries its *original* message's timestamp, so it is
        // recorded with a low id and a high time. It falls outside the ceiling, so it is never
        // counted or shown, and inside the watermark, so it is marked anyway.
        let (store, account) = seeded(&["telegram:1"]).await;
        let ceiling = now();

        // Recorded first, so it has the lower id, but stamped after the ceiling: an ordinary
        // message that arrived while an older edit was still being processed.
        let mut later = message(account, "telegram:1", "1", "after the ceiling");
        later.timestamp = ceiling + chrono::Duration::minutes(5);
        store.record_message(later).await.expect("record");

        // An edit of something old: higher id, lower timestamp, so it is what sets the watermark.
        let mut edit = message(account, "telegram:1", "2", "edit of an old message");
        edit.timestamp = ceiling - chrono::Duration::hours(1);
        store.record_message(edit).await.expect("record");

        let (count, _, watermark) = store
            .take_unseen("telegram:1", ceiling, 5)
            .await
            .expect("take");
        assert_eq!(count, 1, "only the edit is inside the ceiling");

        hand_over(
            &store,
            account,
            "telegram:1",
            "accounting-4",
            Some(Backlog {
                watermark,
                through: ceiling,
            }),
        )
        .await;

        let summary = store.unseen_summary(None).await.expect("summary");
        assert_eq!(
            summary.count, 1,
            "a message above the ceiling was marked seen because its id was below the watermark"
        );
    }

    #[tokio::test]
    async fn a_message_recorded_while_the_turn_ran_is_not_marked_seen() {
        // The watermark used to be the timestamp the caller asked about, and everything at or below
        // it was marked seen when the turn ended. Anything written in between with a time inside
        // that range was marked without ever being shown: gone from `unseen`, from the lookback,
        // and from the CLI predicate. Not a narrow window either. Telegram stamps to whole seconds
        // so a burst shares one, and an edit carries the *original* message's time, so an edit of
        // anything older lands inside it however long the turn took.
        let (store, account) = seeded(&["telegram:1"]).await;
        let asked_about = now();
        let mut first = message(account, "telegram:1", "1", "before the turn");
        first.timestamp = asked_about;
        store.record_message(first).await.expect("record");

        let (count, _, watermark) = store
            .take_unseen("telegram:1", asked_about, 5)
            .await
            .expect("take");
        assert_eq!(count, 1);

        // Arrives while the turn is running, sharing the second the count was taken at.
        let mut during = message(account, "telegram:1", "2", "arrived mid-turn");
        during.timestamp = asked_about;
        store.record_message(during).await.expect("record");

        hand_over(
            &store,
            account,
            "telegram:1",
            "accounting-5",
            Some(Backlog {
                watermark,
                through: asked_about,
            }),
        )
        .await;

        let summary = store.unseen_summary(None).await.expect("summary");
        assert_eq!(
            summary.count, 1,
            "a message written while the turn ran was marked seen without ever being shown"
        );
    }

    #[tokio::test]
    async fn session_id_round_trips_and_clears() {
        let store = Store::open_in_memory().await.expect("opens");
        assert_eq!(store.session_id().await.expect("read"), None);
        let id = Uuid::new_v4();
        store.set_session_id(id).await.expect("write");
        assert_eq!(store.session_id().await.expect("read"), Some(id));
        store.clear_session_id().await.expect("clear");
        assert_eq!(store.session_id().await.expect("read"), None);
    }

    #[tokio::test]
    async fn enqueue_then_claim_then_complete() {
        let (store, account) = seeded(&["telegram:123"]).await;
        let outcome = store
            .enqueue(
                key(account, "telegram:123", "m1"),
                "{\"text\":\"hi\"}",
                now(),
                10,
            )
            .await
            .expect("enqueue");
        assert_eq!(outcome, EnqueueOutcome::Queued);
        assert_eq!(store.pending_count().await.expect("count"), 1);

        let batch = claim_all(&store, 10).await;
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].payload, "{\"text\":\"hi\"}");
        assert_eq!(batch[0].conversation, "telegram:123");
        assert_eq!(batch[0].key(), key(account, "telegram:123", "m1"));
        assert_eq!(store.pending_count().await.expect("count"), 0);

        let sequences: Vec<i64> = batch.iter().map(|message| message.seq).collect();
        store.complete(&sequences).await.expect("complete");
        let stats = store.queue_stats().await.expect("stats");
        assert_eq!(stats.done, 1);
        assert_eq!(stats.pending, 0);
    }

    #[tokio::test]
    async fn peek_does_not_claim_rows() {
        let (store, account) = seeded(&["telegram:123"]).await;
        store
            .enqueue(key(account, "telegram:123", "m1"), "a", now(), 10)
            .await
            .expect("enqueue");
        let peeked = store.peek_pending(10).await.expect("peek");
        assert_eq!(peeked.len(), 1);
        // The session task must still see it; an inspection command that consumed rows would strand
        // undelivered messages.
        assert_eq!(store.pending_count().await.expect("count"), 1);
        assert_eq!(claim_all(&store, 10).await.len(), 1);
    }

    #[tokio::test]
    async fn a_pending_window_spans_oldest_to_newest() {
        let (store, account) = seeded(&["telegram:123"]).await;
        assert!(
            store.pending_windows().await.expect("query").is_empty(),
            "an empty queue has no window to debounce against"
        );

        let first = now() - chrono::Duration::seconds(30);
        let last = now();
        store
            .enqueue(key(account, "telegram:123", "m1"), "a", first, 10)
            .await
            .expect("first");
        store
            .enqueue(key(account, "telegram:123", "m2"), "b", last, 10)
            .await
            .expect("second");

        let windows = store.pending_windows().await.expect("query");
        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0].conversation, "telegram:123");
        assert_eq!(windows[0].oldest.timestamp(), first.timestamp());
        assert_eq!(windows[0].newest.timestamp(), last.timestamp());
    }

    #[tokio::test]
    async fn each_conversation_gets_its_own_window() {
        // The reason this is grouped at all. One window over the whole queue meant a chat still
        // mid-burst deferred delivery for every other chat, which on a platform holding for
        // somebody to stop typing would be a busy room stalling a direct message.
        let (store, account) = seeded(&["telegram:123", "telegram:999"]).await;
        let old = now() - chrono::Duration::seconds(30);
        store
            .enqueue(key(account, "telegram:123", "m1"), "a", old, 10)
            .await
            .expect("first");
        store
            .enqueue(key(account, "telegram:999", "m2"), "b", now(), 10)
            .await
            .expect("second");

        let mut windows = store.pending_windows().await.expect("query");
        windows.sort_by(|left, right| left.conversation.cmp(&right.conversation));
        assert_eq!(windows.len(), 2, "got {windows:?}");
        assert_eq!(windows[0].newest.timestamp(), old.timestamp());
        assert_ne!(
            windows[0].newest.timestamp(),
            windows[1].newest.timestamp(),
            "one chat's arrival must not be reported as another's"
        );
    }

    #[tokio::test]
    async fn a_conversation_that_is_not_ready_is_left_alone() {
        // The claim narrows to the conversations the drain loop decided had settled. Without that
        // it would take the oldest rows in the queue whatever it had just concluded about them.
        let (store, account) = seeded(&["telegram:123", "telegram:999"]).await;
        store
            .enqueue(key(account, "telegram:123", "m1"), "a", now(), 10)
            .await
            .expect("first");
        store
            .enqueue(key(account, "telegram:999", "m2"), "b", now(), 10)
            .await
            .expect("second");

        let batch = store
            .claim(&["telegram:999".to_string()], 10)
            .await
            .expect("claim");
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].conversation, "telegram:999");
        assert_eq!(
            store.pending_count().await.expect("count"),
            1,
            "the other conversation stays pending"
        );
    }

    #[tokio::test]
    async fn claiming_nothing_is_not_an_error() {
        // By far the most common tick, since usually nothing has settled yet. Note this passes
        // either way: SQLite accepts `IN ()` and evaluates it false, so the early return is an
        // optimisation and this test only pins that an empty ask is answered rather than refused.
        let (store, account) = seeded(&["telegram:123"]).await;
        store
            .enqueue(key(account, "telegram:123", "m1"), "a", now(), 10)
            .await
            .expect("enqueue");
        assert!(store.claim(&[], 10).await.expect("claim").is_empty());
        assert_eq!(store.pending_count().await.expect("count"), 1);
    }

    #[tokio::test]
    async fn claimed_rows_leave_the_pending_window() {
        // The drain loop debounces on this, so an in-flight batch must not keep holding the window
        // open and defer the messages behind it.
        let (store, account) = seeded(&["telegram:123"]).await;
        store
            .enqueue(key(account, "telegram:123", "m1"), "a", now(), 10)
            .await
            .expect("enqueue");
        claim_all(&store, 10).await;
        assert!(store.pending_windows().await.expect("query").is_empty());
    }

    #[tokio::test]
    async fn an_edit_is_not_mistaken_for_a_redelivery() {
        // A platform edit reuses the id of the message it revises. The revision is what tells the
        // two apart; this pins the queue half of the contract, because keying an edit on the bare
        // message id makes it vanish into the duplicate check.
        let (store, account) = seeded(&["telegram:123"]).await;
        store
            .enqueue(key(account, "telegram:123", "m1"), "meet at 5", now(), 10)
            .await
            .expect("original");
        let edit = store
            .enqueue(
                MessageKey {
                    revision: 1_754_400_000_000,
                    ..key(account, "telegram:123", "m1")
                },
                "meet at 6",
                now(),
                10,
            )
            .await
            .expect("edit");
        assert_eq!(edit, EnqueueOutcome::Queued);
        assert_eq!(store.pending_count().await.expect("count"), 2);
    }

    #[tokio::test]
    async fn queue_depth_is_enforced_against_waiting_rows() {
        let (store, account) = seeded(&["telegram:123"]).await;
        for index in 0..2 {
            let outcome = store
                .enqueue(
                    key(account, "telegram:123", &format!("m{index}")),
                    "a",
                    now(),
                    2,
                )
                .await
                .expect("enqueue");
            assert_eq!(outcome, EnqueueOutcome::Queued);
        }
        let overflow = store
            .enqueue(key(account, "telegram:123", "m2"), "a", now(), 2)
            .await
            .expect("enqueue");
        assert_eq!(overflow, EnqueueOutcome::Dropped);

        // In-flight rows still count as waiting, otherwise a slow turn would let the queue grow
        // without bound.
        claim_all(&store, 2).await;
        let still_full = store
            .enqueue(key(account, "telegram:123", "m3"), "a", now(), 2)
            .await
            .expect("enqueue");
        assert_eq!(still_full, EnqueueOutcome::Dropped);
    }

    #[tokio::test]
    async fn claim_preserves_arrival_order() {
        let (store, account) = seeded(&["telegram:123"]).await;
        for index in 0..5 {
            store
                .enqueue(
                    key(account, "telegram:123", &format!("m{index}")),
                    &index.to_string(),
                    now(),
                    10,
                )
                .await
                .expect("enqueue");
        }
        let batch = claim_all(&store, 3).await;
        let payloads: Vec<&str> = batch
            .iter()
            .map(|message| message.payload.as_str())
            .collect();
        assert_eq!(payloads, vec!["0", "1", "2"]);
    }

    #[tokio::test]
    async fn an_unclaimed_row_keeps_its_offer_budget() {
        // Rows claimed and handed back before a batch was made of them were never offered to
        // meka: the process died, or the envelope could not be built. Spending an attempt on that
        // would let a fault of the bridge's own declare a message undeliverable that meka never
        // saw.
        let (store, account) = seeded(&["telegram:123"]).await;
        store
            .enqueue(key(account, "telegram:123", "m1"), "a", now(), 10)
            .await
            .expect("enqueue");

        for _ in 0..5 {
            let claimed = claim_all(&store, 10).await;
            assert_eq!(claimed.len(), 1, "the message must stay claimable");
            assert_eq!(claimed[0].attempts, 0, "an unclaim is not a spent offer");
            let sequences: Vec<i64> = claimed.iter().map(|message| message.seq).collect();
            store.unclaim(&sequences).await.expect("unclaim");
            assert_eq!(store.pending_count().await.expect("count"), 1);
        }

        // And the budget is still intact for a genuine offer afterwards.
        let claimed = claim_all(&store, 10).await;
        let held = hold_all(&store, &claimed).await;
        assert_eq!(
            store
                .reoffer(held[0].seq, "withdrawn", 1, None, now())
                .await
                .expect("reoffer"),
            Offer::Retrying,
            "the first real offer puts it back"
        );
        assert_eq!(store.queue_stats().await.expect("stats").failed, 0);
    }

    #[tokio::test]
    async fn a_row_is_offered_again_and_then_given_up_on() {
        let (store, account) = seeded(&["telegram:123"]).await;
        store
            .enqueue(key(account, "telegram:123", "m1"), "a", now(), 10)
            .await
            .expect("enqueue");
        let claimed = claim_all(&store, 10).await;
        let held = hold_all(&store, &claimed).await;
        let seq = held[0].seq;

        assert_eq!(
            store
                .reoffer(seq, "withdrawn", 1, None, now())
                .await
                .expect("reoffer"),
            Offer::Retrying
        );
        assert_eq!(store.pending_count().await.expect("count"), 1);

        let claimed = claim_all(&store, 10).await;
        assert_eq!(claimed[0].attempts, 1);
        let again = hold_all(&store, &claimed).await;
        assert_ne!(
            again[0].key, held[0].key,
            "a hand-over of its own is a key of its own"
        );
        let second = store
            .reoffer(seq, "withdrawn", 1, None, now())
            .await
            .expect("reoffer");
        let Offer::Exhausted(exhausted) = second else {
            panic!("the second offer runs it out: {second:?}");
        };
        assert_eq!(exhausted.conversation, "telegram:123");
        assert_eq!(store.pending_count().await.expect("count"), 0);
        assert_eq!(store.queue_stats().await.expect("stats").failed, 1);
    }

    #[tokio::test]
    async fn a_deferred_row_reports_when_it_may_be_offered_again() {
        // What stops a row whose item was taken back going straight into a new batch, which for a
        // turn an operator just cancelled would hand the agent the same thing at once.
        let (store, account) = seeded(&["telegram:123"]).await;
        store
            .enqueue(key(account, "telegram:123", "m1"), "a", now(), 10)
            .await
            .expect("enqueue");
        let claimed = claim_all(&store, 10).await;
        let held = hold_all(&store, &claimed).await;

        let retry_at = now() + chrono::Duration::seconds(30);
        store
            .reoffer(held[0].seq, "withdrawn", 3, Some(retry_at), now())
            .await
            .expect("reoffer");

        let windows = store.pending_windows().await.expect("windows");
        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0].not_before, Some(retry_at));
    }

    #[tokio::test]
    async fn one_deferred_row_defers_its_whole_conversation() {
        // `max()` rather than `min()`, and the reason is ordering: rows are claimed by `seq`, so
        // releasing the fresh message while its predecessor waits would hand the agent the second
        // half of an exchange before the first.
        let (store, account) = seeded(&["telegram:123"]).await;
        store
            .enqueue(key(account, "telegram:123", "m1"), "a", now(), 10)
            .await
            .expect("enqueue");
        let claimed = claim_all(&store, 10).await;
        let held = hold_all(&store, &claimed).await;
        let retry_at = now() + chrono::Duration::seconds(30);
        store
            .reoffer(held[0].seq, "withdrawn", 3, Some(retry_at), now())
            .await
            .expect("reoffer");

        store
            .enqueue(key(account, "telegram:123", "m2"), "b", now(), 10)
            .await
            .expect("enqueue");

        let windows = store.pending_windows().await.expect("windows");
        assert_eq!(
            windows[0].not_before,
            Some(retry_at),
            "a fresh message must not release its deferred predecessor"
        );
    }

    #[tokio::test]
    async fn an_exhausted_message_is_owed_to_the_agent_again() {
        // A message is marked seen the moment it is queued, on the assumption that the agent is
        // about to be handed it. Running out of attempts is exactly when that assumption fails, and
        // without this the message is neither delivered nor owed: absent from `unseen`, from the
        // missed-context lookback, and from the `mekabridge unseen` predicate.
        let (store, account) = seeded(&["telegram:123"]).await;
        let mut record = message(account, "telegram:123", "m1", "are you there?");
        record.seen = true;
        store.record_message(record).await.expect("record");
        assert_eq!(
            store
                .unseen_summary(Some("telegram:123"))
                .await
                .expect("summary")
                .count,
            0
        );

        assert!(
            store
                .mark_unseen(key(account, "telegram:123", "m1"))
                .await
                .expect("unsee")
        );
        assert_eq!(
            store
                .unseen_summary(Some("telegram:123"))
                .await
                .expect("summary")
                .count,
            1,
            "an undeliverable message must come back as something the agent has not seen"
        );

        assert!(
            !store
                .mark_unseen(key(account, "telegram:123", "nothing-here"))
                .await
                .expect("unsee"),
            "a message that was never recorded cannot be un-seen"
        );
    }

    #[tokio::test]
    async fn zero_offers_fails_on_the_first_release() {
        let (store, account) = seeded(&["telegram:123"]).await;
        store
            .enqueue(key(account, "telegram:123", "m1"), "a", now(), 10)
            .await
            .expect("enqueue");
        let claimed = claim_all(&store, 10).await;
        let held = hold_all(&store, &claimed).await;
        assert!(matches!(
            store
                .reoffer(held[0].seq, "boom", 0, None, now())
                .await
                .expect("reoffer"),
            Offer::Exhausted(_)
        ));
    }

    #[tokio::test]
    async fn a_restart_returns_only_the_rows_nothing_was_rendered_for() {
        // Two rows claimed at once; the process died after binding the first into a batch and
        // before binding the second. The bound one is a hand-over meka may already have and is
        // left for the post or the feed to settle; the other was never rendered for anybody.
        let (store, account) = seeded(&["telegram:123"]).await;
        for id in ["a", "b"] {
            store
                .enqueue(key(account, "telegram:123", id), id, now(), 10)
                .await
                .expect("enqueue");
        }
        let first = claim_all(&store, 1).await;
        let held = hold_all(&store, &first).await;
        claim_all(&store, 1).await;
        assert_eq!(store.pending_count().await.expect("count"), 0);

        let recovered = store.recover().await.expect("recover");
        assert_eq!(recovered, Recovered {
            orphaned: 1,
            unposted: 1,
            posted: 0,
        });
        assert_eq!(store.pending_count().await.expect("count"), 1);
        let rendered = store
            .row(held[0].seq)
            .await
            .expect("read")
            .expect("present");
        assert_eq!(rendered.state, QueueState::InFlight);
        assert_eq!(rendered.message_id, "a");

        store
            .posted(held[0].seq, Uuid::nil(), "item-1", now())
            .await
            .expect("post");
        let recovered = store.recover().await.expect("recover");
        assert_eq!(recovered, Recovered {
            orphaned: 0,
            unposted: 0,
            posted: 1,
        });
    }

    #[tokio::test]
    async fn last_turn_timestamp_round_trips() {
        let store = Store::open_in_memory().await.expect("opens");
        assert_eq!(store.last_turn_at().await.expect("read"), None);
        store.mark_turn_completed(now()).await.expect("write");
        assert_eq!(store.last_turn_at().await.expect("read"), Some(now()));
    }

    #[tokio::test]
    async fn the_dropped_count_comes_off_when_an_item_reports_it() {
        // Read and folded in two steps on purpose. The count is rendered into an envelope first,
        // and only the batch that carries that envelope takes it off the counter; a crash between
        // the two loses the envelope and keeps the count, where taking it at read time lost both.
        let (store, account) = seeded(&["telegram:123"]).await;
        assert_eq!(store.peek_dropped().await.expect("peek"), 0);
        store.note_dropped(2).await.expect("note");
        store.note_dropped(3).await.expect("note");
        assert_eq!(store.peek_dropped().await.expect("peek"), 5);
        assert_eq!(
            store.peek_dropped().await.expect("peek"),
            5,
            "reading does not spend it"
        );

        store
            .enqueue(key(account, "telegram:123", "m1"), "a", now(), 10)
            .await
            .expect("enqueue");
        let claimed = claim_all(&store, 10).await;
        // One more shed while the item was being rendered, which the next item reports.
        store.note_dropped(1).await.expect("note");
        let held = hold_reporting(&store, &claimed, 5, None).await;
        assert_eq!(held[0].dropped, 5);
        assert_eq!(store.peek_dropped().await.expect("peek"), 1);
        // And the item failing gives it back.
        store
            .failed(held[0].seq, "gone", now())
            .await
            .expect("fail");
        assert_eq!(store.peek_dropped().await.expect("peek"), 6);
    }

    #[tokio::test]
    async fn conversations_list_most_recent_first_and_filter_by_channel() {
        let store = Store::open_in_memory().await.expect("opens");
        let mut older = conversation("telegram:1");
        older.last_inbound_at = Some(now() - chrono::Duration::hours(2));
        let mut newer = conversation("telegram:2");
        newer.last_inbound_at = Some(now());
        let other_channel = conversation("second:9");

        store.upsert_conversation(older).await.expect("upsert");
        store.upsert_conversation(newer).await.expect("upsert");
        store
            .upsert_conversation(other_channel)
            .await
            .expect("upsert");

        let all = store.list_conversations(None, 10).await.expect("list");
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].address, "telegram:2");

        let filtered = store
            .list_conversations(Some("second"), 10)
            .await
            .expect("list");
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].address, "second:9");
        assert_eq!(filtered[0].channel, "second");
    }

    #[tokio::test]
    async fn upsert_keeps_created_at_and_fills_missing_title() {
        let store = Store::open_in_memory().await.expect("opens");
        store
            .upsert_conversation(conversation("telegram:123"))
            .await
            .expect("insert");

        let mut update = conversation("telegram:123");
        update.created_at = now() + chrono::Duration::days(1);
        update.title = None;
        store.upsert_conversation(update).await.expect("update");

        let stored = store
            .conversation("telegram:123")
            .await
            .expect("read")
            .expect("present");
        assert_eq!(stored.created_at, now(), "created_at must not move");
        assert_eq!(
            stored.title.as_deref(),
            Some("Alice"),
            "a missing title must not erase a known one"
        );
    }

    #[tokio::test]
    async fn a_row_carries_its_own_key_and_what_its_item_stated() {
        let (store, account) = seeded(&["telegram:123", "telegram:456"]).await;
        for (conversation, id) in [("telegram:123", "a"), ("telegram:456", "b")] {
            store
                .enqueue(key(account, conversation, id), id, now(), 10)
                .await
                .expect("enqueue");
        }
        let claimed = claim_all(&store, 10).await;
        let session = Uuid::new_v4();
        let first = claimed[0].seq;
        let item = HandOver {
            key: "mekabridge-1",
            body: "body",
            session_id: session,
            dropped: 0,
            accounted: None,
        };
        assert!(store.hold(first, item, now()).await.expect("hold"));
        let held = store.row(first).await.expect("read").expect("on file");
        assert_eq!(held.state, QueueState::InFlight);
        assert_eq!(held.key.as_deref(), Some("mekabridge-1"));
        assert_eq!(held.body.as_deref(), Some("body"));
        assert_eq!(held.session_id, Some(session));
        assert_eq!(held.held_at, Some(now()));
        // A second render of the same row is refused: the key it would post under is already
        // minted, and a row with two keys is two items to meka.
        assert!(
            !store
                .hold(
                    first,
                    HandOver {
                        key: "mekabridge-2",
                        body: "other",
                        ..item
                    },
                    now()
                )
                .await
                .expect("hold")
        );

        let stats = store.queue_stats().await.expect("stats");
        assert_eq!((stats.in_flight, stats.posted), (2, 0));
        assert_eq!(
            store
                .rows_in(QueueState::InFlight)
                .await
                .expect("list")
                .len(),
            2
        );

        // A post that did not go through is written down with its wait.
        let later = now() + chrono::Duration::seconds(10);
        store
            .defer(first, "connection refused", later)
            .await
            .expect("defer");
        let deferred = store.row(first).await.expect("read").expect("present");
        assert_eq!(deferred.posts, 1);
        assert_eq!(deferred.attempts, 0, "a failed post is not a spent offer");
        assert_eq!(deferred.not_before, Some(later));
        assert_eq!(deferred.last_error.as_deref(), Some("connection refused"));

        // And the 202 moves it on, clearing what the failed post left.
        store
            .posted(first, session, "item-9", now())
            .await
            .expect("post");
        let posted = store.row(first).await.expect("read").expect("present");
        assert_eq!(posted.state, QueueState::Posted);
        assert_eq!(posted.item_id.as_deref(), Some("item-9"));
        assert_eq!(posted.posts, 2);
        assert_eq!(posted.posted_at, Some(now()));
        assert_eq!(posted.not_before, None);
        assert_eq!(posted.last_error, None);
        assert_eq!(
            store.row_by_item("item-9").await.expect("read"),
            Some(posted),
            "the feed names items, so a row has to be found by one"
        );
        assert_eq!(store.row_by_item("nobody").await.expect("read"), None);
        let stats = store.queue_stats().await.expect("stats");
        assert_eq!((stats.in_flight, stats.posted), (1, 1));
    }

    #[tokio::test]
    async fn delivering_a_row_closes_it_once() {
        let (store, account) = seeded(&["telegram:123"]).await;
        store
            .enqueue(key(account, "telegram:123", "a"), "a", now(), 10)
            .await
            .expect("enqueue");
        let claimed = claim_all(&store, 10).await;
        let held = hold_all(&store, &claimed).await;
        let seq = held[0].seq;
        store
            .posted(seq, Uuid::nil(), "item-1", now())
            .await
            .expect("post");

        assert!(store.delivered(seq, now()).await.expect("deliver"));
        let stats = store.queue_stats().await.expect("stats");
        assert_eq!((stats.done, stats.in_flight, stats.posted), (1, 0, 0));
        // The feed replays an outcome after a reconnect, and the second reading changes nothing.
        assert!(!store.delivered(seq, now()).await.expect("deliver"));
        assert!(
            store
                .failed(seq, "late withdrawal", now())
                .await
                .expect("fail")
                .is_none(),
            "a delivered row is not reopened by a later event"
        );
        assert_eq!(store.queue_stats().await.expect("stats").done, 1);
    }

    #[tokio::test]
    async fn a_delivered_row_can_be_offered_again_when_the_model_said_nothing() {
        let (store, account) = seeded(&["telegram:123"]).await;
        store
            .enqueue(key(account, "telegram:123", "a"), "a", now(), 10)
            .await
            .expect("enqueue");
        let claimed = claim_all(&store, 10).await;
        let held = hold_all(&store, &claimed).await;
        let seq = held[0].seq;
        store
            .posted(seq, Uuid::nil(), "item-1", now())
            .await
            .expect("post");
        store.delivered(seq, now()).await.expect("deliver");

        assert_eq!(
            store
                .reoffer(seq, "empty response", 1, None, now())
                .await
                .expect("reoffer"),
            Offer::Retrying
        );
        let again = claim_all(&store, 10).await;
        assert_eq!(again.len(), 1);
        assert_eq!(again[0].attempts, 1);
        assert_eq!(
            (again[0].key.as_deref(), again[0].item_id.as_deref()),
            (None, None),
            "a row offered again sheds the hand-over that came apart"
        );
        assert_eq!(
            again[0].last_error.as_deref(),
            Some("empty response"),
            "and keeps why, which is the one thing worth reading on a row waiting to be tried again"
        );
        // A row still pending is not this function's to touch.
        store.unclaim(&[seq]).await.expect("unclaim");
        assert_eq!(
            store
                .reoffer(seq, "empty response", 1, None, now())
                .await
                .expect("reoffer"),
            Offer::Closed
        );
        assert_eq!(store.queue_stats().await.expect("stats").pending, 1);
    }

    #[tokio::test]
    async fn failing_a_row_closes_it_and_hands_it_back() {
        let (store, account) = seeded(&["telegram:123"]).await;
        store
            .enqueue(key(account, "telegram:123", "a"), "a", now(), 10)
            .await
            .expect("enqueue");
        let claimed = claim_all(&store, 10).await;
        let held = hold_all(&store, &claimed).await;
        let seq = held[0].seq;

        let closed = store
            .failed(seq, "no turn could deliver it", now())
            .await
            .expect("fail")
            .expect("the row it closed comes back");
        assert_eq!(closed.message_id, "a");
        assert_eq!(closed.state, QueueState::Failed);
        assert_eq!(
            closed.last_error.as_deref(),
            Some("no turn could deliver it")
        );
        assert_eq!(
            closed.body, None,
            "nothing will post it again, so a second copy of the message is not kept"
        );
        assert_eq!(
            closed.key.as_deref(),
            Some("key-1-0"),
            "the key stays: it is what names this hand-over in meka's own logs"
        );
        let stats = store.queue_stats().await.expect("stats");
        assert_eq!((stats.failed, stats.in_flight), (1, 0));
        assert!(
            !store
                .delivered(seq, now())
                .await
                .expect("a closed row must not fail the write"),
            "a closed row is not reopened by a later event"
        );
    }

    #[tokio::test]
    async fn what_an_item_states_once_is_owed_back_exactly_when_it_dies() {
        // The whole reason the backlog is accounted against the row that stated it. Two hand-overs
        // can be outstanding for one conversation at once, so "what has been reported" is a set
        // rather than a watermark, and only the rows the dead one named may be owed again.
        let (store, account) = seeded(&["telegram:1"]).await;
        for id in ["old-1", "old-2"] {
            store
                .record_message(message(account, "telegram:1", id, "said earlier"))
                .await
                .expect("record");
        }
        let through = now() + chrono::Duration::hours(1);
        let (count, _, watermark) = store
            .take_unseen("telegram:1", through, 10)
            .await
            .expect("take");
        assert_eq!(count, 2);

        store.note_dropped(4).await.expect("note");
        let stating = hand_over(
            &store,
            account,
            "telegram:1",
            "stating",
            Some(Backlog { watermark, through }),
        )
        .await;
        store
            .hold(
                stating,
                HandOver {
                    key: "unused",
                    body: "body",
                    session_id: Uuid::nil(),
                    dropped: 4,
                    accounted: None,
                },
                now(),
            )
            .await
            .expect("a second hold is refused");
        assert_eq!(
            store
                .unseen_summary(Some("telegram:1"))
                .await
                .expect("summary")
                .count,
            0,
            "the backlog is spent when the item stating it is rendered, not when it is read"
        );

        // A message that lands afterwards is a backlog of its own, and a second hand-over states
        // only that rather than restating what the first one already said.
        store
            .record_message(message(account, "telegram:1", "later", "said since"))
            .await
            .expect("record");
        let (count, _, second_watermark) = store
            .take_unseen("telegram:1", now() + chrono::Duration::hours(2), 10)
            .await
            .expect("take");
        assert_eq!(count, 1, "only what the first hand-over did not state");
        let second = hand_over(
            &store,
            account,
            "telegram:1",
            "second",
            Some(Backlog {
                watermark: second_watermark,
                through: now() + chrono::Duration::hours(2),
            }),
        )
        .await;

        // The first one dies. Exactly its two rows come back, and the count it reported with them.
        store.failed(stating, "gone", now()).await.expect("fail");
        let owed = store
            .history("telegram:1", 10, None, None)
            .await
            .expect("read")
            .into_iter()
            .filter(|row| !row.seen)
            .map(|row| row.message_id)
            .collect::<Vec<_>>();
        assert_eq!(
            owed,
            vec!["old-1".to_string(), "old-2".to_string()],
            "the dead hand-over owes back its own slice and nothing the live one stated"
        );
        assert_eq!(
            store.peek_dropped().await.expect("peek"),
            4,
            "and the dropped count it reported, which nobody read"
        );

        // And the survivor still owns its own slice, which is given back only if it dies too.
        store
            .reoffer(second, "withdrawn", 3, None, now())
            .await
            .expect("reoffer");
        assert_eq!(
            store
                .unseen_summary(Some("telegram:1"))
                .await
                .expect("summary")
                .count,
            3
        );
    }

    #[tokio::test]
    async fn the_feed_position_is_kept_per_session_and_goes_with_the_binding() {
        let store = Store::open_in_memory().await.expect("opens");
        let session = Uuid::new_v4();
        assert_eq!(store.feed_position(session).await.expect("read"), None);
        store.set_feed_position(session, 41).await.expect("write");
        store
            .set_feed_position(session, 42)
            .await
            .expect("overwrite");
        assert_eq!(store.feed_position(session).await.expect("read"), Some(42));
        // Ids start again on a new session, so a position is never read across one.
        assert_eq!(
            store.feed_position(Uuid::new_v4()).await.expect("read"),
            None
        );

        store.set_session_id(session).await.expect("bind");
        store.clear_session_id().await.expect("unbind");
        assert_eq!(store.session_id().await.expect("read"), None);
        assert_eq!(
            store.feed_position(session).await.expect("read"),
            None,
            "a position outliving its session would be sent to the next one"
        );
    }

    #[tokio::test]
    async fn a_delivered_row_cannot_be_released_back_into_the_queue() {
        // Every terminal write is guarded on the row still being in flight. Unguarded, a `done` row
        // could be released to `pending` and handed to the agent a second time. The session task
        // is serial so no live path does this today, but the store is the wrong place to rely on
        // that: a second process, or `mekabridge queue clear`, reaches these directly.
        let (store, account) = seeded(&["telegram:123"]).await;
        store
            .enqueue(key(account, "telegram:123", "a"), "a", now(), 10)
            .await
            .expect("enqueue");
        let claimed = claim_all(&store, 1).await;
        let seq = claimed.first().expect("claimed").seq;
        store.complete(&[seq]).await.expect("complete");

        store.unclaim(&[seq]).await.expect("unclaim");
        let stats = store.queue_stats().await.expect("stats");
        assert_eq!(
            stats.done, 1,
            "a delivered row was released back to pending"
        );
        assert_eq!(stats.pending, 0);

        // And the other direction: a row released back to the queue must not then be completable.
        // Without the guard on `complete` a late completion from an abandoned attempt marks a
        // message delivered while it is still waiting to be delivered, and it is never sent.
        store
            .enqueue(key(account, "telegram:123", "b"), "b", now(), 10)
            .await
            .expect("enqueue");
        let claimed = claim_all(&store, 1).await;
        let seq = claimed.first().expect("claimed").seq;
        store.unclaim(&[seq]).await.expect("unclaim");
        store.complete(&[seq]).await.expect("complete");
        let stats = store.queue_stats().await.expect("stats");
        assert_eq!(
            stats.pending, 1,
            "a row waiting in the queue was marked delivered: {stats:?}"
        );
        assert_eq!(stats.done, 1, "and nothing new was counted as delivered");

        // A rendered row is not unclaimable either: its key is minted and meka may already have the
        // item, so putting it back would hand the same message over twice.
        let claimed = claim_all(&store, 1).await;
        let held = hold_all(&store, &claimed).await;
        store.unclaim(&[held[0].seq]).await.expect("unclaim");
        assert_eq!(
            store.queue_stats().await.expect("stats").in_flight,
            1,
            "a rendered row was put back in the queue under a key meka may hold"
        );
    }

    #[tokio::test]
    async fn a_row_cleared_under_a_hand_over_settles_without_error() {
        // `mekabridge queue clear` against a running bridge takes the rows out from under the
        // session task, which then hears from the feed about an item it posted. Every write has to
        // shrug at that rather than fail the loop.
        let (store, account) = seeded(&["telegram:123"]).await;
        store
            .enqueue(key(account, "telegram:123", "a"), "a", now(), 10)
            .await
            .expect("enqueue");
        let claimed = claim_all(&store, 10).await;
        let held = hold_all(&store, &claimed).await;
        let seq = held[0].seq;
        store
            .posted(seq, Uuid::nil(), "item-1", now())
            .await
            .expect("post");
        assert_eq!(store.clear_queue().await.expect("clear"), 1);
        assert_eq!(store.row_by_item("item-1").await.expect("read"), None);

        assert!(
            !store
                .delivered(seq, now())
                .await
                .expect("a vanished row must not fail the write")
        );
        assert!(
            store
                .failed(seq, "gone", now())
                .await
                .expect("a vanished row must not fail the write")
                .is_none()
        );
        assert_eq!(
            store
                .reoffer(seq, "gone", 3, None, now())
                .await
                .expect("a vanished row must not fail the write"),
            Offer::Closed
        );
        let stats = store.queue_stats().await.expect("stats");
        assert_eq!(stats, QueueStats::default(), "{stats:?}");
    }

    #[tokio::test]
    async fn clearing_the_queue_owes_back_what_an_outstanding_hand_over_took() {
        // `queue clear` against a running bridge takes the rows out from under hand-overs that are
        // still out. Their items were never read, so what those items stated once has to go back:
        // the foreign key clears the stamp on the backlog but cannot un-see it, and a message
        // marked as shown to an agent that never saw it is offered by nothing ever again.
        let (store, account) = seeded(&["telegram:1"]).await;
        for id in ["old-1", "old-2"] {
            store
                .record_message(message(account, "telegram:1", id, "said earlier"))
                .await
                .expect("record");
        }
        let through = now() + chrono::Duration::hours(1);
        let (count, _, watermark) = store
            .take_unseen("telegram:1", through, 10)
            .await
            .expect("take");
        assert_eq!(count, 2);
        store.note_dropped(3).await.expect("note");

        let seq = hand_over(
            &store,
            account,
            "telegram:1",
            "outstanding",
            Some(Backlog { watermark, through }),
        )
        .await;
        store
            .posted(seq, Uuid::nil(), "item-1", now())
            .await
            .expect("post");
        assert_eq!(
            store
                .unseen_summary(Some("telegram:1"))
                .await
                .expect("summary")
                .count,
            0,
            "the backlog is spent while the item is with meka"
        );

        assert_eq!(store.clear_queue().await.expect("clear"), 1);
        assert_eq!(
            store
                .unseen_summary(Some("telegram:1"))
                .await
                .expect("summary")
                .count,
            2,
            "an item nobody read has to leave its backlog owed"
        );
        assert_eq!(
            store.peek_dropped().await.expect("peek"),
            3,
            "and the shed count it reported"
        );
    }

    #[tokio::test]
    async fn pruning_a_delivered_row_leaves_the_backlog_it_stated_seen() {
        // The other side of the same foreign key. A row swept for age was read, so its backlog was
        // genuinely shown; `ON DELETE SET NULL` must take the stamp and leave `seen` alone, or
        // seven days after every delivery the agent would be offered the whole thing again.
        let (store, account) = seeded(&["telegram:1"]).await;
        store
            .record_message(message(account, "telegram:1", "old-1", "said earlier"))
            .await
            .expect("record");
        let through = now() + chrono::Duration::hours(1);
        let (_, _, watermark) = store
            .take_unseen("telegram:1", through, 10)
            .await
            .expect("take");
        let seq = hand_over(
            &store,
            account,
            "telegram:1",
            "delivered",
            Some(Backlog { watermark, through }),
        )
        .await;
        store.delivered(seq, now()).await.expect("deliver");

        assert_eq!(
            store
                .prune_delivered(Utc::now() + chrono::Duration::seconds(1))
                .await
                .expect("prune"),
            1
        );
        assert_eq!(
            store
                .unseen_summary(Some("telegram:1"))
                .await
                .expect("summary")
                .count,
            0,
            "a backlog the agent was shown must not come back when its row is swept"
        );
        assert!(
            store
                .history("telegram:1", 10, None, None)
                .await
                .expect("read")
                .iter()
                .all(|row| row.seen)
        );
        let stamped: i64 = store
            .connection
            .call(|connection| {
                connection.query_row(
                    "SELECT COUNT(*) FROM messages WHERE accounted_by IS NOT NULL",
                    [],
                    |row| row.get(0),
                )
            })
            .await
            .expect("count");
        assert_eq!(
            stamped, 0,
            "the stamp names a row that no longer exists, so the foreign key has to clear it"
        );
    }

    #[tokio::test]
    async fn prune_delivered_only_removes_completed_rows() {
        let (store, account) = seeded(&["telegram:123"]).await;
        for id in ["done", "waiting"] {
            store
                .enqueue(
                    key(account, "telegram:123", id),
                    "a",
                    now() - chrono::Duration::days(30),
                    10,
                )
                .await
                .expect("enqueue");
        }
        let claimed = claim_all(&store, 1).await;
        let held = hold_all(&store, &claimed).await;
        store.delivered(held[0].seq, now()).await.expect("deliver");

        // Both rows carry a 30-day-old `received_at`, because that is the platform's send time and
        // an edit inherits the original message's. Retention is measured from delivery instead, so
        // a row completed a moment ago survives a prune at the retention boundary however old the
        // message it carries. Keyed on `received_at`, a delivered edit of an old message was swept
        // within the hour, and those rows are what makes duplicate detection survive a restart:
        // once one is gone the unique key has nothing to conflict with and a replayed update is
        // delivered a second time.
        let pruned = store
            .prune_delivered(now() - chrono::Duration::days(7))
            .await
            .expect("prune");
        assert_eq!(
            pruned, 0,
            "a row delivered moments ago was pruned on the age of the message it carried"
        );

        // Past the boundary measured from delivery, it does go. Anchored to the real clock rather
        // than this module's fixed `now()`, because `completed_at` is stamped when the row actually
        // completes and the fixture date sits in the past.
        let pruned = store
            .prune_delivered(Utc::now() + chrono::Duration::seconds(1))
            .await
            .expect("prune");
        assert_eq!(pruned, 1);
        assert_eq!(store.pending_count().await.expect("count"), 1);

        // A row that ends `failed` is never swept: it dropped its rendered item when it failed, so
        // keeping it costs a line rather than a copy of somebody's message, and it is the one thing
        // an operator has to find out what went wrong.
        let claimed = claim_all(&store, 10).await;
        let held = hold_all(&store, &claimed).await;
        store
            .failed(held[0].seq, "gave up", now())
            .await
            .expect("fail");
        let pruned = store
            .prune_delivered(Utc::now() + chrono::Duration::seconds(1))
            .await
            .expect("prune");
        assert_eq!(pruned, 0, "a failed row is not swept");
        assert_eq!(store.queue_stats().await.expect("stats").failed, 1);
    }

    fn attachment_record(
        account: AccountId,
        message_id: &str,
        position: u32,
        age_days: i64,
        path: Option<PathBuf>,
    ) -> AttachmentRecord {
        AttachmentRecord {
            conversation: "telegram:1".to_string(),
            account,
            message_id: message_id.to_string(),
            revision: 0,
            position,
            kind: "photo".to_string(),
            file_ref: format!("ref-{message_id}-{position}"),
            thumb_ref: None,
            file_name: None,
            media_type: Some("image/jpeg".to_string()),
            bytes: Some(1024),
            path,
            created_at: now() - chrono::Duration::days(age_days),
        }
    }

    #[tokio::test]
    async fn registering_an_attachment_returns_a_reusable_handle() {
        let (store, account) = seeded(&["telegram:1"]).await;
        let first = store
            .register_attachment(attachment_record(account, "a1", 0, 0, None))
            .await
            .expect("register");
        let again = store
            .register_attachment(attachment_record(account, "a1", 0, 0, None))
            .await
            .expect("register again");
        assert_eq!(
            first, again,
            "the same file must keep its handle on a replay"
        );

        let other = store
            .register_attachment(attachment_record(account, "a1", 1, 0, None))
            .await
            .expect("register");
        assert_ne!(first, other, "the next file in the same message is its own");
    }

    #[tokio::test]
    async fn a_recreated_bot_does_not_inherit_the_old_bots_file_handles() {
        // The worst of the collisions. A file id on Telegram is bound to the bot that received it,
        // so handing the new bot the old bot's handle for message 1 pointed `attachment_view` at a
        // file the new bot cannot fetch, or at the wrong picture.
        let (store, old_bot) = seeded(&["telegram:1"]).await;
        let old = store
            .register_attachment(attachment_record(old_bot, "1", 0, 0, None))
            .await
            .expect("register");
        let new_bot = register(&store, "telegram", "222").await;
        let new = store
            .register_attachment(attachment_record(new_bot, "1", 0, 0, None))
            .await
            .expect("register");
        assert_ne!(old, new);
    }

    #[tokio::test]
    async fn an_attachment_resolves_by_handle() {
        let (store, account) = seeded(&["telegram:1"]).await;
        let handle = store
            .register_attachment(attachment_record(account, "a1", 0, 0, None))
            .await
            .expect("register");
        let record = store
            .attachment(&handle)
            .await
            .expect("query")
            .expect("resolves");
        assert_eq!(record.file_ref, "ref-a1-0");
        assert_eq!(record.channel, "telegram");
        assert_eq!(record.conversation, "telegram:1");
        assert!(record.path.is_none());
    }

    #[tokio::test]
    async fn a_handle_that_is_not_a_number_resolves_to_nothing() {
        // The agent supplies this string, so a hallucinated value has to fail cleanly rather than
        // erroring out of the query layer.
        let (store, _) = seeded(&["telegram:1"]).await;
        assert!(
            store
                .attachment("not-a-handle")
                .await
                .expect("query")
                .is_none()
        );
        assert!(store.attachment("").await.expect("query").is_none());
    }

    #[tokio::test]
    async fn marking_a_download_makes_the_file_sweepable() {
        let (store, account) = seeded(&["telegram:1"]).await;
        store
            .register_attachment(attachment_record(account, "a1", 0, 40, None))
            .await
            .expect("register");
        // Before the download there is no file, so the sweep has nothing to unlink.
        assert!(
            store
                .take_expired_attachments(now() - chrono::Duration::days(30))
                .await
                .expect("take")
                .is_empty()
        );

        let handle = store
            .register_attachment(attachment_record(account, "a2", 0, 40, None))
            .await
            .expect("register");
        store
            .mark_attachment_downloaded(&handle, Path::new("/tmp/a2.jpg"))
            .await
            .expect("mark");
        let expired = store
            .take_expired_attachments(now() - chrono::Duration::days(30))
            .await
            .expect("take");
        assert_eq!(expired, vec![PathBuf::from("/tmp/a2.jpg")]);
    }

    #[tokio::test]
    async fn expired_attachments_are_returned_for_unlinking() {
        let (store, account) = seeded(&["telegram:1"]).await;
        store
            .register_attachment(attachment_record(
                account,
                "a1",
                0,
                40,
                Some(PathBuf::from("/tmp/a1.jpg")),
            ))
            .await
            .expect("register");
        store
            .register_attachment(attachment_record(
                account,
                "a2",
                0,
                0,
                Some(PathBuf::from("/tmp/a2.jpg")),
            ))
            .await
            .expect("register");

        let expired = store
            .take_expired_attachments(now() - chrono::Duration::days(30))
            .await
            .expect("take");
        assert_eq!(expired, vec![PathBuf::from("/tmp/a1.jpg")]);
        let still_expired = store
            .take_expired_attachments(now() - chrono::Duration::days(30))
            .await
            .expect("take");
        assert!(
            still_expired.is_empty(),
            "rows must be deleted, not just read"
        );
    }

    /// A database at schema version `version`, built the way every release up to it built one.
    async fn legacy_database(
        path: &Path,
        version: usize,
        populate: impl FnOnce(&rusqlite::Connection) -> rusqlite::Result<()> + Send + 'static,
    ) {
        let connection = tokio_rusqlite::Connection::open(path).await.expect("opens");
        connection
            .call(move |connection| {
                for (index, statements) in MIGRATIONS.iter().enumerate().take(version) {
                    connection.execute_batch(statements)?;
                    connection.pragma_update(None, "user_version", (index + 1) as i64)?;
                }
                populate(connection)?;
                Ok::<(), tokio_rusqlite::Error>(())
            })
            .await
            .expect("build a legacy database");
        connection.close().await.expect("closes");
    }

    #[tokio::test]
    async fn a_database_from_before_watches_gains_them_with_everything_still_in_place() {
        // The upgrade every existing deployment takes. Schema 11 only adds a table, so the risk is
        // not the new rows but the old ones: a migration that rebuilt something on the way past
        // would take somebody's history with it.
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("state.db");
        legacy_database(&path, 10, |connection| {
            connection.execute(
                "INSERT INTO conversations (address, channel, platform, kind, created_at)
                 VALUES ('telegram:1', 'telegram', 'telegram', 'direct', '2026-09-01T00:00:00Z')",
                [],
            )?;
            connection.execute(
                "INSERT INTO accounts
                     (channel, platform, platform_id, first_seen_at, last_seen_at)
                 VALUES ('telegram', 'telegram', '111', '2026-09-01T00:00:00Z',
                         '2026-09-01T00:00:00Z')",
                [],
            )?;
            connection.execute(
                "INSERT INTO messages
                     (conversation_id, account_id, message_id, sender_name, text, timestamp)
                 VALUES (1, 1, '7', 'Alice', 'still here', '2026-09-01T00:00:00Z')",
                [],
            )?;
            Ok(())
        })
        .await;

        let store = Store::open(&path).await.expect("upgrades");
        let history = store
            .history("telegram:1", 10, None, None)
            .await
            .expect("read");
        assert_eq!(
            history.len(),
            1,
            "the upgrade must not cost anybody their history"
        );
        assert_eq!(history[0].text, "still here");

        // And the new table works, including the scoped form that needs the conversation row.
        assert!(store.list_watches(now()).await.expect("list").is_empty());
        let outcome = store
            .add_watch(
                NewWatch {
                    conversation: Some("telegram:1"),
                    platform: Some("telegram"),
                    field: WatchField::Text,
                    pattern: "deploy",
                    until: None,
                    reason: None,
                },
                now(),
            )
            .await
            .expect("add");
        assert!(matches!(outcome, WatchOutcome::Added(_)));
    }

    #[tokio::test]
    async fn two_openers_racing_the_same_upgrade_both_survive() {
        // What a real upgrade looks like: systemd starts the daemon while an operator runs
        // `doctor`. The version was read once before the loop, so both saw the old one; the
        // transaction serialised them but the loser still ran a batch the winner had
        // committed and failed on `duplicate column name` -- the very error the transaction
        // was added to prevent.
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("state.db");
        legacy_database(&path, 1, |_| Ok(())).await;

        let opens = (0..4).map(|_| Store::open(&path));
        for outcome in futures::future::join_all(opens).await {
            outcome.expect("every opener must survive the race");
        }
    }

    #[tokio::test]
    async fn a_database_from_a_later_release_is_refused_rather_than_opened() {
        // Running against a schema this build does not know is worse than refusing: the migration
        // loop simply does not execute, so it would open silently and query tables whose shape has
        // moved underneath it.
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("state.db");
        drop(Store::open(&path).await.expect("builds"));
        let connection = tokio_rusqlite::Connection::open(&path)
            .await
            .expect("opens");
        connection
            .call(|connection| {
                connection.pragma_update(None, "user_version", 99_i64)?;
                Ok::<(), tokio_rusqlite::Error>(())
            })
            .await
            .expect("stamp a future version");
        connection.close().await.expect("closes");

        let refused = Store::open(&path).await;
        let message = refused
            .err()
            .map(|error| error.to_string())
            .expect("a newer schema must not open");
        assert!(
            message.contains("newer than"),
            "the refusal did not say why: {message}"
        );
    }

    #[tokio::test]
    async fn a_database_from_an_earlier_release_is_upgraded_in_place() {
        // The path a real deployment takes on upgrade, which the in-memory tests never exercise
        // because they build every schema from scratch. A migration that only works on a fresh
        // database would pass everything else and fail on the first real bridge to restart.
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("state.db");
        legacy_database(&path, 1, |connection| {
            connection.execute("INSERT INTO meta (key, value) VALUES ('session_id', ?1)", [
                Uuid::nil().to_string(),
            ])?;
            // A message that was already waiting when the daemon was stopped to upgrade it.
            // Written without the columns later migrations add, so it is the row most likely to
            // trip one of them up, and with no conversation row, since nothing required one.
            connection.execute(
                "INSERT INTO inbound_queue
                     (conversation_id, external_id, payload, received_at, state)
                 VALUES ('telegram:123', 'pending-across-the-upgrade', 'a', ?1, 'pending')",
                [to_rfc3339(now())],
            )?;
            Ok(())
        })
        .await;

        let store = Store::open(&path).await.expect("upgrades");
        assert_eq!(
            store.session_id().await.expect("read"),
            Some(Uuid::nil()),
            "an upgrade must carry existing state forward"
        );

        // The row predates `not_before` entirely, so its column is NULL. Reading and claiming it
        // has to treat that as "no deferral" rather than failing to parse or holding it forever.
        let windows = store.pending_windows().await.expect("windows");
        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0].not_before, None);
        let claimed = claim_all(&store, 10).await;
        assert_eq!(
            claimed.len(),
            1,
            "a message queued before the upgrade must still be deliverable after it"
        );
        assert_eq!(claimed[0].message_id, "pending-across-the-upgrade");
        let adopted = store
            .conversation("telegram:123")
            .await
            .expect("read")
            .expect("a conversation row was made for the orphaned queue row");
        assert_eq!(adopted.channel, "telegram");
        assert_eq!(adopted.kind, "unknown");

        let account = register(&store, "telegram", "111").await;
        store
            .set_policy("telegram:1", "telegram", Policy::Mute, None, None, now())
            .await
            .expect("the policy table is usable");
        store
            .record_message(message(account, "telegram:1", "1", "after the upgrade"))
            .await
            .expect("the history is usable");
    }

    #[tokio::test]
    async fn history_recorded_before_0_9_0_still_reads_back() {
        // The upgrade above starts from a schema with no `messages` table, so it never has a row in
        // it when the lifecycle columns are added. This is that case: a real bridge upgrading has a
        // populated history, and every one of those rows predates `own`, `deleted_at` and
        // `superseded_at`. They have to read back as somebody else's message that is neither
        // deleted nor superseded, which is what they are.
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("state.db");
        legacy_database(&path, 7, |connection| {
            connection.execute(
                "INSERT INTO messages
                     (conversation_id, external_id, message_id, sender_id, sender_name, text,
                      notes, attachments, addressed, seen, timestamp)
                 VALUES ('telegram:1', '4', '4', '111', 'Alice', 'said before the upgrade',
                         NULL, NULL, 0, 0, ?1)",
                [to_rfc3339(now())],
            )?;
            Ok(())
        })
        .await;

        let store = Store::open(&path).await.expect("upgrades");
        let history = store
            .history("telegram:1", 10, None, None)
            .await
            .expect("read");
        let row = history.first().expect("the old row survives");
        assert_eq!(row.text, "said before the upgrade");
        assert!(!row.own, "an unmarked row is not the bridge's own message");
        assert_eq!(row.deleted_at, None);
        assert_eq!(row.superseded_at, None);
        assert_eq!(row.session_id, None);
        assert_eq!(
            store
                .unseen_summary(Some("telegram:1"))
                .await
                .expect("summary")
                .count,
            1,
            "and it is still owed to the agent, since it was never shown"
        );
    }

    #[tokio::test]
    async fn a_mute_set_before_0_3_0_becomes_a_block() {
        // The rows this migration inherits were written by the old `mute` tool, which dropped
        // messages outright. That is now called `block`, and reading them as the new `mute` would
        // silently start delivering mentions from chats somebody had switched off entirely.
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("state.db");
        legacy_database(&path, 2, |connection| {
            connection.execute(
                "INSERT INTO mutes (conversation_id, until, reason, dropped, created_at)
                 VALUES ('telegram:-100', NULL, 'endless standup chatter', 41, ?1)",
                [to_rfc3339(now())],
            )?;
            Ok(())
        })
        .await;

        let store = Store::open(&path).await.expect("upgrades");
        let carried = store
            .policy("telegram:-100")
            .await
            .expect("read")
            .expect("the mute must survive the rename");
        assert_eq!(carried.policy, Policy::Block);
        assert_eq!(carried.reason.as_deref(), Some("endless standup chatter"));
        assert_eq!(carried.dropped, 41, "the tally has to come across too");
    }

    #[tokio::test]
    async fn history_recorded_before_0_13_0_is_carried_across_and_stays_clear_of_a_new_bot() {
        // The shape 0.13.0 exists for. Everything written before it belongs to whichever bot was
        // behind the channel at the time, which nothing recorded, so those rows go to a placeholder
        // account: distinct from every real one, so a recreated bot reusing the old ids collides
        // with nothing, and never reported as a previous account, since on a deployment that never
        // swapped bots it is the same one.
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("state.db");
        let old_cursor = 40_i64;
        legacy_database(&path, 8, move |connection| {
            connection.execute(
                "INSERT INTO conversations
                     (id, channel_id, platform, chat, thread, title, kind, created_at,
                      last_inbound_at)
                 VALUES ('telegram:123', 'telegram', 'telegram', '123', NULL, 'Alice', 'direct',
                         ?1, ?1)",
                [to_rfc3339(now())],
            )?;
            // The original, a Telegram edit of it (seconds), and an edit the bridge made of its
            // own message (milliseconds), which is the mixture a real database holds.
            for (id, external_id, message_id, text, own) in [
                (old_cursor, "1", "1", "hello", 0),
                (old_cursor + 1, "1:e1754400600", "1", "hello there", 0),
                (old_cursor + 2, "2", "2", "on it", 1),
                (old_cursor + 3, "2:e1754400600500", "2", "on it now", 1),
            ] {
                connection.execute(
                    "INSERT INTO messages
                         (id, conversation_id, external_id, message_id, sender_id, sender_name,
                          text, notes, attachments, addressed, seen, own, timestamp)
                     VALUES (?1, 'telegram:123', ?2, ?3, '111', 'Alice', ?4, NULL, NULL, 0, 1,
                             ?5, ?6)",
                    rusqlite::params![id, external_id, message_id, text, own, to_rfc3339(now())],
                )?;
            }
            connection.execute(
                "INSERT INTO inbound_queue
                     (conversation_id, external_id, payload, received_at, state, completed_at)
                 VALUES ('telegram:123', '1', 'a', ?1, 'done', ?1)",
                [to_rfc3339(now())],
            )?;
            // A file on the original, and one on the edit, in a threaded conversation nothing else
            // mentions so the address itself carries a colon.
            connection.execute(
                "INSERT INTO attachments
                     (handle, id, conversation_id, channel_id, kind, file_ref, created_at)
                 VALUES (7, 'telegram:123:1:0', 'telegram:123', 'telegram', 'photo', 'old-photo',
                         ?1),
                        (8, 'telegram:-100:77:12:e1754400600:1', 'telegram:-100:77', 'telegram',
                         'photo', 'threaded-photo', ?1)",
                [to_rfc3339(now())],
            )?;
            Ok(())
        })
        .await;

        let store = Store::open(&path).await.expect("upgrades");
        let placeholder = store
            .accounts()
            .await
            .expect("list")
            .into_iter()
            .find(AccountRecord::is_placeholder)
            .expect("the legacy rows have an account to belong to");
        assert_eq!(placeholder.channel, "telegram");
        assert_eq!(placeholder.platform, "telegram");
        assert!(
            store
                .current_account("telegram")
                .await
                .expect("read")
                .is_none(),
            "a placeholder is not a current account"
        );

        let history = store
            .history("telegram:123", 10, None, None)
            .await
            .expect("read");
        let carried: Vec<(i64, &str, i64, bool, bool)> = history
            .iter()
            .map(|row| {
                (
                    row.id,
                    row.message_id.as_str(),
                    row.revision,
                    row.own,
                    row.previous_account,
                )
            })
            .collect();
        assert_eq!(
            carried,
            vec![
                (old_cursor, "1", 0, false, false),
                (old_cursor + 1, "1", 1_754_400_600_000, false, false),
                (old_cursor + 2, "2", 0, true, false),
                (old_cursor + 3, "2", 1_754_400_600_500, true, false),
            ],
            "ids survive, the edit time is normalised to milliseconds whichever unit wrote it, \
             and nothing is reported as a previous account"
        );
        assert_eq!(
            history[0].attachments,
            vec!["7".to_string()],
            "a file registered under the old key is still found from its message"
        );
        let threaded = store
            .attachment("8")
            .await
            .expect("query")
            .expect("the handle survives");
        assert_eq!(threaded.conversation, "telegram:-100:77");
        assert_eq!(threaded.channel, "telegram");
        assert_eq!(threaded.file_ref, "threaded-photo");
        assert_eq!(
            store
                .search_messages("hello", None, 10)
                .await
                .expect("search")
                .len(),
            2,
            "the search index is rebuilt over the carried rows rather than left empty"
        );

        // The bot was recreated. Its first message reuses id 1, which the old bot had used.
        let new_bot = register(&store, "telegram", "999").await;
        assert!(
            store
                .record_message(message(new_bot, "telegram:123", "1", "hello again"))
                .await
                .expect("record"),
            "the recreated bot's first message must be recorded"
        );
        assert_eq!(
            store
                .enqueue(key(new_bot, "telegram:123", "1"), "b", now(), 10)
                .await
                .expect("enqueue"),
            EnqueueOutcome::Queued,
            "and delivered, despite the old bot's message 1 still being in the queue"
        );
        let new_handle = store
            .register_attachment(AttachmentRecord {
                conversation: "telegram:123".to_string(),
                account: new_bot,
                message_id: "1".to_string(),
                revision: 0,
                position: 0,
                kind: "photo".to_string(),
                file_ref: "new-photo".to_string(),
                thumb_ref: None,
                file_name: None,
                media_type: None,
                bytes: None,
                path: None,
                created_at: now(),
            })
            .await
            .expect("register");
        assert_ne!(new_handle, "7", "and its photo is not the old bot's photo");

        let history = store
            .history("telegram:123", 10, None, None)
            .await
            .expect("read");
        assert!(
            history.iter().all(|row| !row.previous_account),
            "the placeholder is unknown rather than previous, so nothing is flagged yet"
        );

        // A second swap, this time between two accounts the bridge knew, is reported.
        let newer_bot = register(&store, "telegram", "1000").await;
        store
            .record_message(message(
                newer_bot,
                "telegram:123",
                "1",
                "hello a third time",
            ))
            .await
            .expect("record");
        let history = store
            .history("telegram:123", 10, None, None)
            .await
            .expect("read");
        let flagged: Vec<(&str, bool)> = history
            .iter()
            .map(|row| (row.text.as_str(), row.previous_account))
            .collect();
        assert_eq!(flagged, vec![
            ("hello", false),
            ("hello there", false),
            ("on it", false),
            ("on it now", false),
            ("hello again", true),
            ("hello a third time", false),
        ]);
    }

    #[tokio::test]
    async fn a_message_a_0_13_turn_held_goes_back_to_the_queue_on_upgrade() {
        // What a deployment stopped mid-turn to upgrade looks like: a row the old drain loop had
        // claimed, and marked as recovered once already. The turn it was in ran to its end or was
        // cut off with the bridge none the wiser, and nothing can say which, so the row is offered
        // again as the previous release's startup would have offered it, and the column that
        // carried the uncertainty is gone with the uncertainty.
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("state.db");
        legacy_database(&path, 9, |connection| {
            connection.execute(
                "INSERT INTO accounts
                     (id, channel, platform, platform_id, first_seen_at, last_seen_at)
                 VALUES (1, 'telegram', 'telegram', '111', ?1, ?1)",
                [to_rfc3339(now())],
            )?;
            connection.execute(
                "INSERT INTO conversations (id, address, channel, platform, kind, created_at)
                 VALUES (1, 'telegram:123', 'telegram', 'telegram', 'direct', ?1)",
                [to_rfc3339(now())],
            )?;
            connection.execute(
                "INSERT INTO inbound_queue
                     (conversation_id, account_id, message_id, revision, payload, received_at,
                      state, attempts, recovered)
                 VALUES (1, 1, 'held-by-a-turn', 0, 'a', ?1, 'in_flight', 1, 1)",
                [to_rfc3339(now())],
            )?;
            Ok(())
        })
        .await;

        let store = Store::open(&path).await.expect("upgrades");
        let stats = store.queue_stats().await.expect("stats");
        assert_eq!((stats.pending, stats.in_flight), (1, 0), "{stats:?}");
        let claimed = claim_all(&store, 10).await;
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].message_id, "held-by-a-turn");
        assert_eq!(claimed[0].attempts, 1, "what was charged stays charged");
        assert_eq!(claimed[0].key, None, "and nothing is rendered for it yet");
        let columns: Vec<String> = store
            .connection
            .call(|connection| {
                let mut statement = connection.prepare("PRAGMA table_info(inbound_queue)")?;
                let names = statement.query_map([], |row| row.get::<_, String>(1))?;
                names.collect::<rusqlite::Result<Vec<_>>>()
            })
            .await
            .expect("columns");
        assert!(
            !columns.iter().any(|name| name == "recovered"),
            "{columns:?}"
        );
        for added in [
            "key",
            "body",
            "session_id",
            "item_id",
            "dropped",
            "posts",
            "held_at",
        ] {
            assert!(columns.iter().any(|name| name == added), "{columns:?}");
        }
        let accounted: Vec<String> = store
            .connection
            .call(|connection| {
                let mut statement = connection.prepare("PRAGMA table_info(messages)")?;
                let names = statement.query_map([], |row| row.get::<_, String>(1))?;
                names.collect::<rusqlite::Result<Vec<_>>>()
            })
            .await
            .expect("columns");
        assert!(
            accounted.iter().any(|name| name == "accounted_by"),
            "{accounted:?}"
        );
    }

    #[tokio::test]
    async fn migrations_are_idempotent_across_reopen() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("nested").join("state.db");
        let store = Store::open(&path).await.expect("first open");
        store.set_session_id(Uuid::nil()).await.expect("write");
        drop(store);

        let reopened = Store::open(&path).await.expect("second open");
        assert_eq!(
            reopened.session_id().await.expect("read"),
            Some(Uuid::nil()),
            "reopening must not wipe state"
        );
    }

    #[tokio::test]
    async fn the_forward_cursor_hands_back_the_next_page_rather_than_the_latest_one() {
        // What makes a sweep correct. Reading forwards has to take the *oldest* rows above the
        // cursor: taking the newest would hand a caller that had fallen behind the latest page
        // with a hole behind it, and the cursor it saved would step over whatever was in the hole.
        let (store, account) = seeded(&["telegram:1"]).await;
        for index in 0..5 {
            let mut record = message(
                account,
                "telegram:1",
                &index.to_string(),
                &format!("line {index}"),
            );
            record.timestamp = now() + chrono::Duration::seconds(index);
            store.record_message(record).await.expect("record");
        }
        let all = store
            .history("telegram:1", 10, None, None)
            .await
            .expect("read");
        let first = all[0].id;

        let page = store
            .history("telegram:1", 2, None, Some(first))
            .await
            .expect("read");
        let texts: Vec<&str> = page.iter().map(|row| row.text.as_str()).collect();
        assert_eq!(texts, vec!["line 1", "line 2"], "the two after the cursor");

        // A sweep keeps the newest cursor it was given, and the next call continues from it with
        // nothing read twice and nothing skipped.
        let next = store
            .history("telegram:1", 2, None, Some(page[1].id))
            .await
            .expect("read");
        let texts: Vec<&str> = next.iter().map(|row| row.text.as_str()).collect();
        assert_eq!(texts, vec!["line 3", "line 4"]);

        let caught_up = store
            .history("telegram:1", 2, None, Some(all[4].id))
            .await
            .expect("read");
        assert!(
            caught_up.is_empty(),
            "nothing has been said since the last one"
        );
    }

    #[tokio::test]
    async fn a_watch_is_recorded_once_however_often_it_is_asked_for() {
        // The agent keeps its rules somewhere of its own and syncs them, so it calls this for every
        // rule every time. A second row per call would fill the table and report every message
        // twice.
        let (store, _account) = seeded(&["telegram:1"]).await;
        let rule = NewWatch {
            conversation: None,
            platform: None,
            field: WatchField::Text,
            pattern: "deploy",
            until: None,
            reason: Some("releases"),
        };
        let WatchOutcome::Added(first) = store.add_watch(rule, now()).await.expect("add") else {
            panic!("the first call must add");
        };
        let WatchOutcome::Existing(again) = store.add_watch(rule, now()).await.expect("add") else {
            panic!("the second call must find the first");
        };
        assert_eq!(first.id, again.id);
        assert_eq!(store.list_watches(now()).await.expect("list").len(), 1);

        // The same words pointed at another field are a different rule, because they read a
        // different string and fire on different messages.
        let other = NewWatch {
            field: WatchField::Sender,
            ..rule
        };
        let WatchOutcome::Added(_) = store.add_watch(other, now()).await.expect("add") else {
            panic!("a different field must be a different watch");
        };
        assert_eq!(store.list_watches(now()).await.expect("list").len(), 2);
    }

    #[tokio::test]
    async fn a_scoped_watch_and_an_unscoped_one_are_different_rules() {
        // SQLite treats NULLs as distinct, so the duplicate check cannot be a unique index and is
        // a query instead. This is the case that would slip through one.
        let (store, _account) = seeded(&["telegram:1"]).await;
        let global = NewWatch {
            conversation: None,
            platform: None,
            field: WatchField::Text,
            pattern: "deploy",
            until: None,
            reason: None,
        };
        let scoped = NewWatch {
            conversation: Some("telegram:1"),
            platform: Some("telegram"),
            ..global
        };
        assert!(matches!(
            store.add_watch(global, now()).await.expect("add"),
            WatchOutcome::Added(_)
        ));
        assert!(matches!(
            store.add_watch(scoped, now()).await.expect("add"),
            WatchOutcome::Added(_)
        ));
        // And the unscoped one is still found rather than added twice, which is the half a unique
        // index over a nullable column would silently lose.
        assert!(matches!(
            store.add_watch(global, now()).await.expect("add"),
            WatchOutcome::Existing(_)
        ));
        let watches = store.list_watches(now()).await.expect("list");
        assert_eq!(watches.len(), 2);
        assert_eq!(watches[0].conversation, None);
        assert_eq!(watches[1].conversation.as_deref(), Some("telegram:1"));
    }

    #[tokio::test]
    async fn a_watch_scoped_to_a_chat_nothing_has_arrived_from_still_records() {
        // Ruling on a conversation before it has said anything is legitimate, and the row it needs
        // is minted here exactly as `set_policy` mints one.
        let (store, _account) = seeded(&[]).await;
        let outcome = store
            .add_watch(
                NewWatch {
                    conversation: Some("telegram:-100"),
                    platform: Some("telegram"),
                    field: WatchField::Text,
                    pattern: "deploy",
                    until: None,
                    reason: None,
                },
                now(),
            )
            .await
            .expect("add");
        let WatchOutcome::Added(record) = outcome else {
            panic!("must add");
        };
        assert_eq!(record.conversation.as_deref(), Some("telegram:-100"));
    }

    #[tokio::test]
    async fn a_lapsed_watch_stops_matching_and_is_swept() {
        let (store, _account) = seeded(&["telegram:1"]).await;
        let lapsed = NewWatch {
            conversation: None,
            platform: None,
            field: WatchField::Text,
            pattern: "deploy",
            until: Some(now() - chrono::Duration::minutes(1)),
            reason: None,
        };
        store.add_watch(lapsed, now()).await.expect("add");
        // Gone from what the gate matches before anything has swept it, since the read filters
        // rather than waiting for a write.
        assert!(store.watches(now()).await.expect("read").is_empty());
        assert!(store.list_watches(now()).await.expect("list").is_empty());
    }

    #[tokio::test]
    async fn removing_a_watch_reports_what_it_was() {
        let (store, _account) = seeded(&["telegram:1"]).await;
        let WatchOutcome::Added(record) = store
            .add_watch(
                NewWatch {
                    conversation: None,
                    platform: None,
                    field: WatchField::Text,
                    pattern: "deploy",
                    until: None,
                    reason: None,
                },
                now(),
            )
            .await
            .expect("add")
        else {
            panic!("must add");
        };
        let removed = store.remove_watch(record.id).await.expect("remove");
        assert_eq!(removed.expect("the row").pattern, "deploy");
        assert!(
            store
                .remove_watch(record.id)
                .await
                .expect("remove")
                .is_none(),
            "removing it twice is not an error, and says so"
        );
    }

    #[tokio::test]
    async fn the_watch_cache_answers_the_gate_without_rereading_and_notices_a_write() {
        // The identity of the handed-back `Arc` is what lets the gate skip recompiling a regex set
        // it already built, so a refetch that found nothing changed has to hand back the same
        // allocation rather than an equal one.
        let (store, _account) = seeded(&["telegram:1"]).await;
        store
            .add_watch(
                NewWatch {
                    conversation: None,
                    platform: None,
                    field: WatchField::Text,
                    pattern: "deploy",
                    until: None,
                    reason: None,
                },
                now(),
            )
            .await
            .expect("add");
        let first = store.watches(now()).await.expect("read");
        let again = store.watches(now()).await.expect("read");
        assert!(
            Arc::ptr_eq(&first, &again),
            "a cached read must not recompile"
        );

        store
            .add_watch(
                NewWatch {
                    conversation: None,
                    platform: None,
                    field: WatchField::Sender,
                    pattern: "spammer",
                    until: None,
                    reason: None,
                },
                now(),
            )
            .await
            .expect("add");
        let after = store.watches(now()).await.expect("read");
        assert_eq!(after.len(), 2, "a write has to be visible immediately");
        assert!(!Arc::ptr_eq(&first, &after));
    }

    #[tokio::test]
    async fn the_watch_table_has_a_ceiling() {
        // Every pattern is matched against every message in a muted room, and the agent can add
        // rules at runtime, so there has to be a limit that is not "until it gets slow".
        let (store, _account) = seeded(&["telegram:1"]).await;
        for index in 0..MAX_WATCHES {
            let pattern = format!("rule{index}");
            let outcome = store
                .add_watch(
                    NewWatch {
                        conversation: None,
                        platform: None,
                        field: WatchField::Text,
                        pattern: &pattern,
                        until: None,
                        reason: None,
                    },
                    now(),
                )
                .await
                .expect("add");
            assert!(matches!(outcome, WatchOutcome::Added(_)), "at {index}");
        }
        let outcome = store
            .add_watch(
                NewWatch {
                    conversation: None,
                    platform: None,
                    field: WatchField::Text,
                    pattern: "one too many",
                    until: None,
                    reason: None,
                },
                now(),
            )
            .await
            .expect("add");
        assert_eq!(outcome, WatchOutcome::AtCapacity(MAX_WATCHES));
        assert_eq!(
            store.list_watches(now()).await.expect("list").len(),
            MAX_WATCHES
        );
    }
}
