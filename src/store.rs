//! Durable state: the session binding, the conversation address book, and the inbound queue.
//!
//! One meka session runs one turn at a time, so messages arriving mid-turn have to wait somewhere,
//! and that cannot be process memory: a crash with a full queue would silently swallow everything a
//! user typed. Every message is written before it is acknowledged and marked done only once the
//! turn carrying it completed.
//!
//! A row's state is a column rather than an in-memory flag so that anything left `in_flight` at
//! startup is evidence of a crash mid-turn and can be returned to `pending`.
//!
//! Payloads stay opaque JSON so the state machine can be tested on its own and does not change when
//! a platform adds fields.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    time::Instant,
};

use chrono::{DateTime, Utc};
use rusqlite::OptionalExtension;
use uuid::Uuid;

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

/// A conversation the bridge has seen, which is also the set of addresses the agent may send to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConversationRecord {
    /// Canonical `<channel>:<chat>[:<thread>]` identifier.
    pub id: String,
    pub channel_id: String,
    pub platform: String,
    pub chat: String,
    pub thread: Option<String>,
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

/// When one conversation's waiting messages arrived, as the span the drain loop decides on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingWindow {
    pub conversation_id: String,
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

/// Absence is meaningful: a conversation with no record follows the configured default for its chat
/// kind, so this type says "somebody ruled on this one" rather than "this one has a policy".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyRecord {
    pub conversation_id: String,
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
    pub conversation_id: String,
    /// Deduplication key, shared with the queue. An edit carries a distinct one.
    pub external_id: String,
    /// Platform-native id, which is what a reply or a reaction targets.
    pub message_id: String,
    pub sender_id: Option<String>,
    pub sender_name: String,
    pub text: String,
    /// Descriptor lines for content with no text of its own, so a shared location does not read
    /// back as a blank line from somebody.
    pub notes: Option<String>,
    /// Handles for the files this message brought, so something found in history can be opened.
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
}

/// A message waiting to be handed to the agent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueuedMessage {
    pub seq: i64,
    pub conversation_id: String,
    /// Platform-native message id, used to reject duplicate deliveries.
    pub external_id: String,
    pub payload: String,
    pub received_at: DateTime<Utc>,
    pub attempts: u32,
    /// Whether crash recovery put this row back, as opposed to a failed turn.
    ///
    /// A failed turn was watched, so the bridge knows whether the agent acted and refuses to
    /// replay a batch that may already have been answered. A row stranded by a hard kill has
    /// no such answer, so it is handed over again with the uncertainty stated rather than
    /// hidden.
    pub recovered: bool,
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

/// Outcome of marking a batch failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailureOutcome {
    /// Rows returned to `pending` for another attempt.
    pub retrying: Vec<i64>,
    /// Rows that exhausted their attempts and are now `failed`. These are what an operator needs
    /// to hear about: the agent was never handed them, and nothing will hand them over now.
    /// What is still possible is [`Store::mark_unseen`], which puts them back among what it is
    /// owed.
    pub exhausted: Vec<QueuedMessage>,
}

/// Queue row counts by state, for `mekabridge status`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueueStats {
    pub pending: u64,
    pub in_flight: u64,
    pub done: u64,
    pub failed: u64,
}

/// An attachment a platform holds, as the bridge records it on arrival.
///
/// Nothing is downloaded at this point. `path` is set later, and only if the agent asks for the
/// file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachmentRecord {
    /// `<conversation>:<external_id>:<index>`, stable across a redelivery.
    pub id: String,
    pub conversation_id: String,
    pub channel_id: String,
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
    pub id: String,
    pub conversation_id: String,
    pub channel_id: String,
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

    async fn meta_delete(&self, key: &'static str) -> Result<()> {
        self.connection
            .call(move |connection| {
                connection.execute("DELETE FROM meta WHERE key = ?1", [key])?;
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

    /// Forget the session binding. The next turn creates a fresh session.
    pub async fn clear_session_id(&self) -> Result<()> {
        self.meta_delete(META_SESSION_ID).await
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
    pub async fn note_dropped(&self, count: u64) -> Result<()> {
        let current = self.dropped_count().await?;
        self.meta_set(META_DROPPED, (current.saturating_add(count)).to_string())
            .await
    }

    async fn dropped_count(&self) -> Result<u64> {
        Ok(self
            .meta_get(META_DROPPED)
            .await?
            .and_then(|raw| raw.parse().ok())
            .unwrap_or(0))
    }

    /// Read and reset the dropped-message counter.
    pub async fn take_dropped(&self) -> Result<u64> {
        let count = self.dropped_count().await?;
        if count > 0 {
            self.meta_delete(META_DROPPED).await?;
        }
        Ok(count)
    }

    /// Insert or refresh a conversation. `created_at` is preserved across updates so the address
    /// book keeps a stable notion of when a contact was first seen.
    pub async fn upsert_conversation(&self, record: ConversationRecord) -> Result<()> {
        self.connection
            .call(move |connection| {
                connection.execute(
                    "INSERT INTO conversations
                         (id, channel_id, platform, chat, thread, title, kind, created_at,
                          last_inbound_at, last_outbound_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                     ON CONFLICT(id) DO UPDATE SET
                         title = COALESCE(excluded.title, conversations.title),
                         kind = excluded.kind,
                         last_inbound_at = COALESCE(
                             excluded.last_inbound_at, conversations.last_inbound_at),
                         last_outbound_at = COALESCE(
                             excluded.last_outbound_at, conversations.last_outbound_at)",
                    rusqlite::params![
                        record.id,
                        record.channel_id,
                        record.platform,
                        record.chat,
                        record.thread,
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

    /// Look up one conversation by canonical id.
    pub async fn conversation(&self, id: &str) -> Result<Option<ConversationRecord>> {
        let id = id.to_string();
        let record = self
            .connection
            .call(move |connection| {
                connection
                    .query_row(
                        "SELECT id, channel_id, platform, chat, thread, title, kind, created_at,
                                last_inbound_at, last_outbound_at
                         FROM conversations WHERE id = ?1",
                        [id],
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
        let limit = limit as i64;
        let records = self
            .connection
            .call(move |connection| {
                let mut statement = connection.prepare(
                    "SELECT id, channel_id, platform, chat, thread, title, kind, created_at,
                            last_inbound_at, last_outbound_at
                     FROM conversations
                     WHERE (?1 IS NULL OR channel_id = ?1)
                     ORDER BY COALESCE(last_inbound_at, last_outbound_at, created_at) DESC
                     LIMIT ?2",
                )?;
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
    /// Only `last_outbound_at` is honoured from `record` on an existing row; everything else is
    /// insert-time detail. See [`Store::upsert_conversation`] for the inbound direction, which does
    /// know the title and kind and is allowed to update them.
    pub async fn touch_outbound(&self, record: ConversationRecord) -> Result<()> {
        self.connection
            .call(move |connection| {
                connection.execute(
                    // An insert rather than an update, because the agent may write to a chat that
                    // has never written to it and this is then the address book's first news of
                    // the conversation. On conflict only the timestamp moves:
                    // the `title` and `kind` passed here stand in for a chat
                    // nothing is known about, so letting them overwrite a real
                    // title would discard what an inbound message already
                    // established.
                    "INSERT INTO conversations
                         (id, channel_id, platform, chat, thread, title, kind, created_at,
                          last_inbound_at, last_outbound_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, NULL, ?9)
                     ON CONFLICT(id) DO UPDATE SET last_outbound_at = excluded.last_outbound_at",
                    rusqlite::params![
                        record.id,
                        record.channel_id,
                        record.platform,
                        record.chat,
                        record.thread,
                        record.title,
                        record.kind,
                        to_rfc3339(record.created_at),
                        record.last_outbound_at.map(to_rfc3339),
                    ],
                )?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    /// Rule on a conversation until `until`, or indefinitely when it is `None`.
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
        conversation_id: &str,
        policy: Policy,
        until: Option<DateTime<Utc>>,
        reason: Option<&str>,
        at: DateTime<Utc>,
    ) -> Result<()> {
        let conversation_id = conversation_id.to_string();
        let reason = reason.map(str::to_string);
        self.connection
            .call({
                let conversation_id = conversation_id.clone();
                move |connection| {
                    connection.execute(
                        "INSERT INTO conversation_policy
                             (conversation_id, mode, until, reason, dropped, created_at)
                         VALUES (?1, ?2, ?3, ?4, 0, ?5)
                         ON CONFLICT(conversation_id) DO UPDATE SET
                             mode = excluded.mode,
                             until = excluded.until,
                             reason = excluded.reason,
                             dropped = 0,
                             created_at = excluded.created_at",
                        rusqlite::params![
                            conversation_id,
                            policy.as_str(),
                            until.map(to_rfc3339),
                            reason,
                            to_rfc3339(at),
                        ],
                    )?;
                    Ok(())
                }
            })
            .await?;
        self.forget_cached_policy(&conversation_id);
        Ok(())
    }

    /// Remove an explicit decision, returning the conversation to the configured default. Returns
    /// whether one was actually in place.
    pub async fn clear_policy(&self, conversation_id: &str) -> Result<bool> {
        let conversation_id = conversation_id.to_string();
        let removed = self
            .connection
            .call({
                let conversation_id = conversation_id.clone();
                move |connection| {
                    connection.execute(
                        "DELETE FROM conversation_policy WHERE conversation_id = ?1",
                        [conversation_id],
                    )
                }
            })
            .await?;
        self.forget_cached_policy(&conversation_id);
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
    pub async fn policy(&self, conversation_id: &str) -> Result<Option<PolicyRecord>> {
        if let Some(cached) = self.cached_policy(conversation_id) {
            return Ok(cached);
        }
        let record = self.read_policy(conversation_id).await?;
        self.cache_policy(conversation_id, record.clone());
        Ok(record)
    }

    async fn read_policy(&self, conversation_id: &str) -> Result<Option<PolicyRecord>> {
        let conversation_id = conversation_id.to_string();
        let record = self
            .connection
            .call(move |connection| {
                connection
                    .query_row(
                        "SELECT conversation_id, mode, until, reason, dropped, created_at
                         FROM conversation_policy WHERE conversation_id = ?1",
                        [conversation_id],
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
    pub async fn expire_policy(&self, conversation_id: &str) -> Result<Option<PolicyRecord>> {
        let conversation_id = conversation_id.to_string();
        let record = self
            .connection
            .call({
                let conversation_id = conversation_id.clone();
                move |connection| {
                    let transaction = connection
                        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                    let record = transaction
                        .query_row(
                            "SELECT conversation_id, mode, until, reason, dropped, created_at
                             FROM conversation_policy WHERE conversation_id = ?1",
                            [&conversation_id],
                            row_to_policy,
                        )
                        .optional()?;
                    transaction.execute(
                        "DELETE FROM conversation_policy WHERE conversation_id = ?1",
                        [&conversation_id],
                    )?;
                    transaction.commit()?;
                    Ok(record)
                }
            })
            .await?;
        self.forget_cached_policy(&conversation_id);
        Ok(record)
    }

    /// Every explicit decision currently recorded, oldest first.
    pub async fn list_policies(&self) -> Result<Vec<PolicyRecord>> {
        let records = self
            .connection
            .call(move |connection| {
                let mut statement = connection.prepare(
                    "SELECT conversation_id, mode, until, reason, dropped, created_at
                     FROM conversation_policy ORDER BY created_at",
                )?;
                let rows = statement.query_map([], row_to_policy)?;
                rows.collect::<std::result::Result<Vec<_>, _>>()
            })
            .await?;
        Ok(records)
    }

    /// Count one message discarded because its conversation is blocked.
    ///
    /// Only blocked conversations need this. Under `mute` the messages are recorded, so what went
    /// unseen is counted from the history rather than tallied here.
    pub async fn note_blocked_drop(&self, conversation_id: &str) -> Result<()> {
        let conversation_id = conversation_id.to_string();
        self.connection
            .call(move |connection| {
                connection.execute(
                    "UPDATE conversation_policy SET dropped = dropped + 1
                     WHERE conversation_id = ?1",
                    [conversation_id],
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
    fn cached_policy(&self, conversation_id: &str) -> Option<Option<PolicyRecord>> {
        let cache = self.policies.read().ok()?;
        let (record, fetched_at) = cache.entries.get(conversation_id)?;
        (fetched_at.elapsed() < POLICY_CACHE_TTL).then(|| record.clone())
    }

    fn cache_policy(&self, conversation_id: &str, record: Option<PolicyRecord>) {
        if let Ok(mut cache) = self.policies.write() {
            // Nothing evicts by age, so a long-running bridge in many conversations would otherwise
            // hold an entry for every one it has ever seen. Dropping the lot on overflow is crude
            // but costs one refetch each and keeps the bound obvious.
            if cache.entries.len() >= POLICY_CACHE_MAX_ENTRIES {
                cache.entries.clear();
            }
            cache
                .entries
                .insert(conversation_id.to_string(), (record, Instant::now()));
        }
    }

    fn forget_cached_policy(&self, conversation_id: &str) {
        if let Ok(mut cache) = self.policies.write() {
            cache.entries.remove(conversation_id);
        }
    }

    /// Persist an inbound message.
    ///
    /// Enforces `max_depth` against rows still waiting (`pending` plus `in_flight`) so a stalled
    /// turn cannot let the queue grow without bound. Duplicates are recognised by
    /// `(conversation_id, external_id)`.
    pub async fn enqueue(
        &self,
        conversation_id: &str,
        external_id: &str,
        payload: &str,
        received_at: DateTime<Utc>,
        max_depth: usize,
    ) -> Result<EnqueueOutcome> {
        let conversation_id = conversation_id.to_string();
        let external_id = external_id.to_string();
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
                    "INSERT INTO inbound_queue
                         (conversation_id, external_id, payload, received_at, state, attempts)
                     VALUES (?1, ?2, ?3, ?4, 'pending', 0)
                     ON CONFLICT(conversation_id, external_id) DO NOTHING",
                    rusqlite::params![conversation_id, external_id, payload, received_at],
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
                let mut statement = connection.prepare(
                    "SELECT conversation_id, min(received_at), max(received_at), max(not_before)
                     FROM inbound_queue
                     WHERE state = 'pending'
                     GROUP BY conversation_id",
                )?;
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
            .map(|(conversation_id, oldest, newest, not_before)| {
                Ok(PendingWindow {
                    conversation_id,
                    oldest: parse("received_at", &oldest)?,
                    newest: parse("received_at", &newest)?,
                    not_before: not_before
                        .map(|raw| parse("not_before", &raw))
                        .transpose()?,
                })
            })
            .collect()
    }

    /// Atomically take up to `limit` pending messages from `ready` and mark them in flight.
    ///
    /// Claiming and marking happen in one transaction so two drain loops (or a drain loop racing a
    /// restart) cannot hand the same message to two turns.
    ///
    /// A batch may still span conversations, which is what makes several chats that all became
    /// ready together cost one turn rather than one each. `ready` narrows which are eligible, not
    /// how many may share a batch.
    pub async fn claim_batch(&self, ready: &[String], limit: usize) -> Result<Vec<QueuedMessage>> {
        // SQLite does accept `IN ()` and evaluates it false, so this is for the round trip rather
        // than for correctness: nothing being ready is by far the most common tick, and there is no
        // reason to reach the database to be told so.
        if ready.is_empty() {
            return Ok(Vec::new());
        }
        let ready: Vec<String> = ready.to_vec();
        let limit = limit as i64;
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
                        "SELECT seq, conversation_id, external_id, payload, received_at, attempts,
                                recovered
                         FROM inbound_queue
                         WHERE state = 'pending' AND conversation_id IN ({placeholders})
                         ORDER BY seq
                         LIMIT ?"
                    ))?;
                    // Bound in order, the limit last, matching the placeholder order above.
                    let mut values: Vec<&dyn rusqlite::ToSql> =
                        ready.iter().map(|id| id as &dyn rusqlite::ToSql).collect();
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
    /// Distinct from [`Self::claim_batch`] because an inspection command must not move rows into
    /// `in_flight`: doing so would hide them from the drain loop until the next crash recovery.
    pub async fn peek_pending(&self, limit: usize) -> Result<Vec<QueuedMessage>> {
        let limit = limit.min(i64::MAX as usize) as i64;
        let rows = self
            .connection
            .call(move |connection| {
                let mut statement = connection.prepare(
                    "SELECT seq, conversation_id, external_id, payload, received_at, attempts,
                                recovered
                     FROM inbound_queue
                     WHERE state = 'pending'
                     ORDER BY seq
                     LIMIT ?1",
                )?;
                let rows = statement.query_map([limit], row_to_queued)?;
                rows.collect::<std::result::Result<Vec<_>, _>>()
            })
            .await?;
        Ok(rows)
    }

    /// Mark a batch as delivered.
    pub async fn complete_batch(&self, sequences: &[i64]) -> Result<()> {
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
                         WHERE seq = ?1 AND state = 'in_flight'",
                        rusqlite::params![sequence, &completed_at],
                    )?;
                }
                transaction.commit()?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    /// Return a batch to the queue without spending an attempt.
    ///
    /// For a batch that was never handed over, as distinct from one that failed. meka refusing a
    /// submission because a turn is already running is the case this exists for: nothing reached
    /// the agent, nothing was lost, and counting it as a failed delivery would let a busy session
    /// exhaust the retry budget and declare a message undeliverable that was never even attempted.
    pub async fn release_batch(&self, sequences: &[i64]) -> Result<()> {
        let sequences = sequences.to_vec();
        self.connection
            .call(move |connection| {
                let transaction = connection
                    .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                for sequence in sequences {
                    transaction.execute(
                        "UPDATE inbound_queue SET state = 'pending'
                         WHERE seq = ?1 AND state = 'in_flight'",
                        [sequence],
                    )?;
                }
                transaction.commit()?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    /// Record that a batch's turn failed.
    ///
    /// Each row's attempt counter is incremented; rows still within `max_attempts` return to
    /// `pending` for another try, the rest become `failed` and are reported back so the operator
    /// can be told which messages the agent will never be handed.
    ///
    /// `retry_at` is when a retrying row may be offered again. `None` means at once, which is right
    /// for a failure that says nothing about when it might stop happening, and wrong for the one
    /// this argument exists for: coming straight back from a provider's rate limit spends the next
    /// attempt inside the same window it just bounced off.
    pub async fn fail_batch(
        &self,
        sequences: &[i64],
        error: &str,
        max_attempts: u32,
        retry_at: Option<DateTime<Utc>>,
    ) -> Result<FailureOutcome> {
        let sequences = sequences.to_vec();
        let error = error.to_string();
        let completed_at = to_rfc3339(Utc::now());
        let retry_at = retry_at.map(to_rfc3339);
        let outcome = self
            .connection
            .call(move |connection| {
                let transaction = connection
                    .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                let mut retrying = Vec::new();
                let mut exhausted = Vec::new();
                for sequence in sequences {
                    // Guarded like the state writes below. Unguarded, a row that is no longer in
                    // flight was still charged an attempt, and the exhausted arm then told the
                    // owner a message was permanently lost while the row sat in `pending` waiting
                    // to be delivered.
                    let charged = transaction.execute(
                        "UPDATE inbound_queue
                         SET attempts = attempts + 1, last_error = ?2, not_before = ?3
                         WHERE seq = ?1 AND state = 'in_flight'",
                        rusqlite::params![sequence, error, retry_at],
                    )?;
                    if charged == 0 {
                        continue;
                    }
                    // `optional` rather than `?`: a sequence that is gone yields
                    // `QueryReturnedNoRows`, which rolled back the whole transaction and left every
                    // *other* row in the batch stuck `in_flight` until the next restart. Those rows
                    // are invisible to `pending_windows`, so newer messages in the same
                    // conversation are then delivered ahead of them.
                    // `complete_batch` already skips silently; the
                    // asymmetry was the bug. Reachable from `mekabridge queue clear` against a
                    // running bridge.
                    let Some(attempts) = transaction
                        .query_row(
                            "SELECT attempts FROM inbound_queue WHERE seq = ?1",
                            [sequence],
                            |row| row.get::<_, u32>(0),
                        )
                        .optional()?
                    else {
                        continue;
                    };
                    if attempts > max_attempts {
                        transaction.execute(
                            "UPDATE inbound_queue SET state = 'failed', completed_at = ?2
                             WHERE seq = ?1 AND state = 'in_flight'",
                            rusqlite::params![sequence, &completed_at],
                        )?;
                        let message = transaction.query_row(
                            "SELECT seq, conversation_id, external_id, payload, received_at,
                                    attempts, recovered
                             FROM inbound_queue WHERE seq = ?1",
                            [sequence],
                            row_to_queued,
                        )?;
                        exhausted.push(message);
                    } else {
                        transaction.execute(
                            "UPDATE inbound_queue SET state = 'pending'
                             WHERE seq = ?1 AND state = 'in_flight'",
                            [sequence],
                        )?;
                        retrying.push(sequence);
                    }
                }
                transaction.commit()?;
                Ok(FailureOutcome {
                    retrying,
                    exhausted,
                })
            })
            .await?;
        Ok(outcome)
    }

    /// Return rows stranded `in_flight` by a crash to `pending`. Called once at startup.
    pub async fn reset_in_flight(&self) -> Result<usize> {
        let reset = self
            .connection
            .call(|connection| {
                connection.execute(
                    // The attempt is charged and the row flagged. Charged because a batch whose
                    // turn kills the process would otherwise be replayed for ever, its counter
                    // never moving. Flagged because nothing else can tell this apart from an
                    // ordinary requeue afterwards, and the agent is owed the difference.
                    "UPDATE inbound_queue
                     SET state = 'pending', attempts = attempts + 1, recovered = 1
                     WHERE state = 'in_flight'",
                    [],
                )
            })
            .await?;
        Ok(reset)
    }

    /// Number of messages waiting to be delivered.
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
                let mut statement = connection
                    .prepare("SELECT state, COUNT(*) FROM inbound_queue GROUP BY state")?;
                let rows = statement.query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
                })?;
                let mut stats = QueueStats::default();
                for row in rows {
                    let (state, count) = row?;
                    let count = count.max(0) as u64;
                    match state.as_str() {
                        "pending" => stats.pending = count,
                        "in_flight" => stats.in_flight = count,
                        "done" => stats.done = count,
                        "failed" => stats.failed = count,
                        _ => {}
                    }
                }
                Ok(stats)
            })
            .await?;
        Ok(stats)
    }

    /// Delete every queue row regardless of state. Backs `mekabridge queue clear`.
    pub async fn clear_queue(&self) -> Result<usize> {
        let deleted = self
            .connection
            .call(|connection| connection.execute("DELETE FROM inbound_queue", []))
            .await?;
        Ok(deleted)
    }

    /// Drop delivered rows older than `before`.
    ///
    /// Completed rows are kept for a window rather than deleted immediately because they are what
    /// makes duplicate detection work across a restart.
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
                    [before],
                )
            })
            .await?;
        Ok(deleted)
    }

    /// Record what somebody said, whether or not the agent was woken for it.
    ///
    /// Idempotent on `(conversation_id, external_id)`, the same key the queue deduplicates on, so a
    /// platform replaying an update after a crash does not produce a second copy.
    pub async fn record_message(&self, record: MessageRecord) -> Result<()> {
        self.connection
            .call(move |connection| {
                connection.execute(
                    "INSERT INTO messages
                         (conversation_id, external_id, message_id, sender_id, sender_name, text,
                          notes, attachments, addressed, seen, own, session_id, deleted_at,
                          superseded_at, timestamp)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)
                     ON CONFLICT(conversation_id, external_id) DO NOTHING",
                    rusqlite::params![
                        record.conversation_id,
                        record.external_id,
                        record.message_id,
                        record.sender_id,
                        record.sender_name,
                        record.text,
                        record.notes,
                        join_handles(&record.attachments),
                        record.addressed,
                        record.seen,
                        record.own,
                        record.session_id,
                        record.deleted_at.map(to_rfc3339),
                        record.superseded_at.map(to_rfc3339),
                        to_rfc3339(record.timestamp),
                    ],
                )?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    /// Read a conversation back, oldest first, ending at `before` when one is given.
    ///
    /// Selected newest-first so `limit` takes the most recent, then reversed, which is what "the
    /// last twenty messages" means.
    ///
    /// `before` is a [`MessageRecord::id`], not a time. Paging on the timestamp cannot be made
    /// correct: a burst shares one second, so "older than this timestamp" either drops the siblings
    /// of the message paged from or returns them twice, and the first loses messages silently.
    ///
    /// Ordered by id so the sequence agrees with the cursor. That is also arrival order, which the
    /// timestamp is not for an edit: it carries the time of the message it revises.
    pub async fn history(
        &self,
        conversation_id: &str,
        limit: usize,
        before: Option<i64>,
    ) -> Result<Vec<MessageRecord>> {
        let conversation_id = conversation_id.to_string();
        let limit = limit.min(i64::MAX as usize) as i64;
        let mut records = self
            .connection
            .call(move |connection| {
                let mut statement = connection.prepare(&format!(
                    "SELECT {MESSAGE_COLUMNS}
                     FROM messages m
                     WHERE m.conversation_id = ?1 AND (?2 IS NULL OR m.id < ?2)
                     ORDER BY m.id DESC
                     LIMIT ?3"
                ))?;
                let rows = statement.query_map(
                    rusqlite::params![conversation_id, before, limit],
                    row_to_message,
                )?;
                rows.collect::<std::result::Result<Vec<_>, _>>()
            })
            .await?;
        records.reverse();
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
                let mut statement = connection.prepare(&format!(
                    "SELECT {MESSAGE_COLUMNS}
                     FROM messages_fts f
                     JOIN messages m ON m.id = f.rowid
                     WHERE messages_fts MATCH ?1
                       AND (?2 IS NULL OR m.conversation_id = ?2)
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
        conversation_id: &str,
        through: DateTime<Utc>,
        context: usize,
    ) -> Result<(u64, Vec<MessageRecord>, i64)> {
        let conversation_id = conversation_id.to_string();
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
                         WHERE m.conversation_id = ?1 AND m.seen = 0 AND m.timestamp <= ?2
                           AND {MESSAGE_CURRENT}"
                    ),
                    rusqlite::params![&conversation_id, &through],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )?;
                let records = {
                    let mut statement = transaction.prepare(&format!(
                        "SELECT {MESSAGE_COLUMNS}
                         FROM messages m
                         WHERE m.conversation_id = ?1 AND m.seen = 0 AND m.timestamp <= ?2
                           AND {MESSAGE_CURRENT}
                         ORDER BY m.timestamp DESC, m.id DESC
                         LIMIT ?3"
                    ))?;
                    let rows = statement.query_map(
                        rusqlite::params![&conversation_id, &through, context],
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

    /// Mark everything a conversation withheld up to `through` as accounted for.
    ///
    /// Split from [`Self::take_unseen`] so the backlog is only spent once a turn carrying it has
    /// reached meka: marking at read time meant a submission meka refused threw away the envelope
    /// *and* the count.
    ///
    /// Both bounds together are the set [`Self::take_unseen`] counted, and neither alone is. On the
    /// timestamp alone, a row written between the count and this call at or below the ceiling is
    /// marked without ever being shown, which is not a narrow window: Telegram stamps to whole
    /// seconds, and an edit carries the *original* message's time. On the id alone, that same edit
    /// sweeps the other way, having a low id and a high timestamp, so it was never counted and is
    /// marked anyway.
    pub async fn mark_seen(
        &self,
        conversation_id: &str,
        through_id: i64,
        through: DateTime<Utc>,
    ) -> Result<usize> {
        let conversation_id = conversation_id.to_string();
        let through = to_rfc3339(through);
        let marked = self
            .connection
            .call(move |connection| {
                connection.execute(
                    "UPDATE messages SET seen = 1
                     WHERE conversation_id = ?1 AND seen = 0 AND id <= ?2 AND timestamp <= ?3",
                    rusqlite::params![&conversation_id, through_id, &through],
                )
            })
            .await?;
        Ok(marked)
    }

    /// Put one message back among what the agent is owed.
    ///
    /// Keyed on one message rather than a watermark because a message is marked seen when it is
    /// queued, assuming the agent is about to be handed it, and that assumption fails exactly when
    /// its batch runs out of attempts. Without this the message is neither delivered nor owed, so
    /// nothing short of `read_history` over the right window finds it again.
    ///
    /// A `false` return is legitimate: `[storage].history_retention` of zero writes no history, so
    /// there is nothing to un-see.
    pub async fn mark_unseen(&self, conversation_id: &str, external_id: &str) -> Result<bool> {
        let conversation_id = conversation_id.to_string();
        let external_id = external_id.to_string();
        let changed = self
            .connection
            .call(move |connection| {
                connection.execute(
                    "UPDATE messages SET seen = 0
                     WHERE conversation_id = ?1 AND external_id = ?2",
                    rusqlite::params![&conversation_id, &external_id],
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
    pub async fn unseen_summary(&self, conversation_id: Option<&str>) -> Result<UnseenSummary> {
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
        let conversation_id = conversation_id.map(str::to_string);
        let (count, newest, latest): (i64, Option<String>, Option<String>) = self
            .connection
            .call(move |connection| match conversation_id {
                Some(conversation_id) => connection.query_row(
                    &format!("SELECT {columns} FROM messages m WHERE m.conversation_id = ?1"),
                    [conversation_id],
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
    /// Keyed on the platform's message id rather than the queue's `external_id`, since a deletion
    /// names the message and knows nothing about the edit that may have given it a second row. That
    /// is also why every row for the message is marked rather than one.
    pub async fn mark_deleted(
        &self,
        conversation: &str,
        message_id: &str,
        at: DateTime<Utc>,
    ) -> Result<bool> {
        let conversation = conversation.to_string();
        let message_id = message_id.to_string();
        let at = to_rfc3339(at);
        let marked = self
            .connection
            .call(move |connection| {
                connection.execute(
                    "UPDATE messages SET deleted_at = ?3
                     WHERE conversation_id = ?1 AND message_id = ?2 AND deleted_at IS NULL",
                    rusqlite::params![conversation, message_id, at],
                )
            })
            .await?;
        Ok(marked > 0)
    }

    /// Mark the wordings a message had before `external_id` revised it.
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
    pub async fn supersede_message(
        &self,
        conversation: &str,
        message_id: &str,
        external_id: &str,
        at: DateTime<Utc>,
    ) -> Result<usize> {
        let conversation = conversation.to_string();
        let message_id = message_id.to_string();
        let external_id = external_id.to_string();
        let at = to_rfc3339(at);
        let marked = self
            .connection
            .call(move |connection| {
                connection.execute(
                    // The subquery names the message as well as the row, so a caller pairing an
                    // external id with the wrong message id marks nothing instead of superseding a
                    // different message's history. `(conversation_id, external_id)` is unique, so
                    // the extra clause only ever removes a mismatch.
                    "UPDATE messages SET superseded_at = ?4
                     WHERE conversation_id = ?1 AND message_id = ?2 AND superseded_at IS NULL
                       AND id < (SELECT id FROM messages
                                 WHERE conversation_id = ?1 AND message_id = ?2
                                   AND external_id = ?3)",
                    rusqlite::params![conversation, message_id, external_id, at],
                )
            })
            .await?;
        Ok(marked)
    }

    /// Unseen counts for every conversation that has any, keyed by conversation id.
    ///
    /// One grouped query rather than a count per conversation, because the caller is rendering a
    /// list and the alternative is fifty round trips to put a number on fifty rows.
    pub async fn unseen_counts(&self) -> Result<HashMap<String, u64>> {
        let counts = self
            .connection
            .call(|connection| {
                let mut statement = connection.prepare(&format!(
                    "SELECT m.conversation_id, COUNT(*) FROM messages m
                     WHERE m.seen = 0 AND {MESSAGE_CURRENT}
                     GROUP BY m.conversation_id"
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
    /// Idempotent on `record.id`: a redelivered message returns the handle already issued rather
    /// than minting a second one for the same file.
    pub async fn register_attachment(&self, record: AttachmentRecord) -> Result<String> {
        let handle = self
            .connection
            .call(move |connection| {
                connection.execute(
                    "INSERT INTO attachments
                         (id, conversation_id, channel_id, kind, file_ref, thumb_ref, file_name,
                          media_type, bytes, path, created_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
                     ON CONFLICT(id) DO NOTHING",
                    rusqlite::params![
                        record.id,
                        record.conversation_id,
                        record.channel_id,
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
                    "SELECT handle FROM attachments WHERE id = ?1",
                    [&record.id],
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
                        "SELECT handle, id, conversation_id, channel_id, kind, file_ref, thumb_ref,
                                file_name, media_type, bytes, path
                         FROM attachments WHERE handle = ?1",
                        [handle],
                        |row| {
                            Ok(StoredAttachment {
                                handle: row.get::<_, i64>(0)?.to_string(),
                                id: row.get(1)?,
                                conversation_id: row.get(2)?,
                                channel_id: row.get(3)?,
                                kind: row.get(4)?,
                                file_ref: row.get(5)?,
                                thumb_ref: row.get(6)?,
                                file_name: row.get(7)?,
                                media_type: row.get(8)?,
                                bytes: row.get::<_, Option<i64>>(9)?.map(|bytes| bytes as u64),
                                path: row.get::<_, Option<String>>(10)?.map(PathBuf::from),
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

fn to_rfc3339(value: DateTime<Utc>) -> String {
    value.to_rfc3339()
}

/// Attachment handles as one column. Handles are decimal row ids, so a comma cannot appear inside
/// one and the split back is unambiguous.
fn join_handles(handles: &[String]) -> Option<String> {
    (!handles.is_empty()).then(|| handles.join(","))
}

/// The columns [`row_to_message`] reads, in the order it reads them.
///
/// One constant shared by every query that returns a [`MessageRecord`], and aliased because the
/// search join puts `messages` next to an FTS table that also has a `text` column. Written out
/// rather than `m.*` so the order is fixed here: with sixteen positional `row.get` calls, a column
/// added to one query and not another reads back as the wrong field instead of failing.
const MESSAGE_COLUMNS: &str = "m.id, m.conversation_id, m.external_id, m.message_id, m.sender_id,
     m.sender_name, m.text, m.notes, m.attachments, m.addressed, m.seen, m.own, m.session_id,
     m.deleted_at, m.superseded_at, m.timestamp";

/// What excludes a recorded message from everything the agent is owed.
///
/// A retracted message must not be offered as context it missed, and neither must the older wording
/// of one that has since been edited, because the revision is in the table too and is the version
/// worth showing. Both still read back through `read_history` and `search_history`, which is where
/// finding them is the point.
const MESSAGE_CURRENT: &str = "m.deleted_at IS NULL AND m.superseded_at IS NULL";

fn row_to_message(row: &rusqlite::Row<'_>) -> rusqlite::Result<MessageRecord> {
    let attachments: Option<String> = row.get(8)?;
    let deleted_at: Option<String> = row.get(13)?;
    let superseded_at: Option<String> = row.get(14)?;
    let timestamp: String = row.get(15)?;
    Ok(MessageRecord {
        id: row.get(0)?,
        conversation_id: row.get(1)?,
        external_id: row.get(2)?,
        message_id: row.get(3)?,
        sender_id: row.get(4)?,
        sender_name: row.get(5)?,
        text: row.get(6)?,
        notes: row.get(7)?,
        attachments: attachments
            .as_deref()
            .map(|joined| joined.split(',').map(str::to_string).collect())
            .unwrap_or_default(),
        addressed: row.get(9)?,
        seen: row.get(10)?,
        own: row.get(11)?,
        session_id: row.get(12)?,
        deleted_at: deleted_at.as_deref().map(parse_rfc3339).transpose()?,
        superseded_at: superseded_at.as_deref().map(parse_rfc3339).transpose()?,
        timestamp: parse_rfc3339(&timestamp)?,
    })
}

fn row_to_policy(row: &rusqlite::Row<'_>) -> rusqlite::Result<PolicyRecord> {
    let mode: String = row.get(1)?;
    let until: Option<String> = row.get(2)?;
    let created_at: String = row.get(5)?;
    Ok(PolicyRecord {
        conversation_id: row.get(0)?,
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
    let created_at: String = row.get(7)?;
    let last_inbound_at: Option<String> = row.get(8)?;
    let last_outbound_at: Option<String> = row.get(9)?;
    Ok(ConversationRecord {
        id: row.get(0)?,
        channel_id: row.get(1)?,
        platform: row.get(2)?,
        chat: row.get(3)?,
        thread: row.get(4)?,
        title: row.get(5)?,
        kind: row.get(6)?,
        created_at: parse_rfc3339(&created_at)?,
        last_inbound_at: last_inbound_at.as_deref().map(parse_rfc3339).transpose()?,
        last_outbound_at: last_outbound_at.as_deref().map(parse_rfc3339).transpose()?,
    })
}

fn row_to_queued(row: &rusqlite::Row<'_>) -> rusqlite::Result<QueuedMessage> {
    let received_at: String = row.get(4)?;
    Ok(QueuedMessage {
        seq: row.get(0)?,
        conversation_id: row.get(1)?,
        external_id: row.get(2)?,
        payload: row.get(3)?,
        received_at: parse_rfc3339(&received_at)?,
        attempts: row.get(5)?,
        recovered: row.get::<_, i64>(6).unwrap_or(0) != 0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-08-05T12:00:00Z")
            .expect("literal parses")
            .with_timezone(&Utc)
    }

    fn conversation(id: &str) -> ConversationRecord {
        ConversationRecord {
            id: id.to_string(),
            channel_id: "telegram".to_string(),
            platform: "telegram".to_string(),
            chat: "123".to_string(),
            thread: None,
            title: Some("Alice".to_string()),
            kind: "direct".to_string(),
            created_at: now(),
            last_inbound_at: Some(now()),
            last_outbound_at: None,
        }
    }

    /// Claim from every conversation with something waiting.
    ///
    /// What `claim_batch` meant before readiness became per conversation. These tests are about
    /// queue mechanics rather than about which chats have settled, so they say "all of them" once
    /// here instead of naming ids at twenty call sites.
    async fn claim_all(store: &Store, limit: usize) -> Vec<QueuedMessage> {
        let ready: Vec<String> = store
            .pending_windows()
            .await
            .expect("windows")
            .into_iter()
            .map(|window| window.conversation_id)
            .collect();
        store.claim_batch(&ready, limit).await.expect("claim")
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
            let mut record = conversation("telegram:-100");
            record.last_outbound_at = Some(sent_at);
            store.touch_outbound(record).await.expect("outbound");
        }

        let reopened = Store::open(&path).await.expect("reopens");
        let record = reopened
            .conversation("telegram:-100")
            .await
            .expect("read")
            .expect("the conversation is on file");
        assert_eq!(record.last_outbound_at, Some(sent_at));
    }

    #[tokio::test]
    async fn a_policy_round_trips_with_its_expiry() {
        let store = Store::open_in_memory().await.expect("opens");
        let until = now() + chrono::Duration::minutes(30);
        store
            .set_policy(
                "telegram:1",
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
        assert_eq!(record.policy, Policy::Block);
        assert_eq!(record.until, Some(until));
        assert_eq!(record.reason.as_deref(), Some("standup spam"));
        assert_eq!(record.dropped, 0);
        assert!(store.policy("telegram:2").await.expect("read").is_none());
    }

    #[tokio::test]
    async fn every_policy_survives_a_round_trip() {
        // `active` in particular. It is a real decision rather than the absence of one: a group
        // whose configured default is `mute` needs somewhere to record that this one is not.
        let store = Store::open_in_memory().await.expect("opens");
        for policy in [Policy::Active, Policy::Mute, Policy::Block] {
            store
                .set_policy("telegram:1", policy, None, None, now())
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
            .set_policy("telegram:1", Policy::Mute, None, None, now())
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
            .set_policy("telegram:1", Policy::Block, None, None, now())
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
            .set_policy("telegram:1", Policy::Block, None, None, now())
            .await
            .expect("set");
        store.note_blocked_drop("telegram:1").await.expect("count");
        store.note_blocked_drop("telegram:1").await.expect("count");
        // Read uncached: `policy` serves the drop count from a window that `note_blocked_drop`
        // deliberately does not invalidate.
        let listed = store.list_policies().await.expect("list");
        assert_eq!(listed[0].dropped, 2);

        store
            .set_policy("telegram:1", Policy::Block, None, None, now())
            .await
            .expect("re-set");
        let listed = store.list_policies().await.expect("list");
        assert_eq!(listed[0].dropped, 0);
    }

    #[tokio::test]
    async fn expiring_a_policy_returns_the_exact_drop_count_and_removes_it() {
        let store = Store::open_in_memory().await.expect("opens");
        store
            .set_policy("telegram:1", Policy::Block, None, None, now())
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
            .set_policy("telegram:1", Policy::Mute, None, None, now())
            .await
            .expect("set");
        store
            .set_policy(
                "telegram:2",
                Policy::Block,
                Some(now() + chrono::Duration::hours(1)),
                None,
                now() + chrono::Duration::seconds(1),
            )
            .await
            .expect("set");
        let listed = store.list_policies().await.expect("list");
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0].conversation_id, "telegram:1");
        assert_eq!(listed[0].policy, Policy::Mute);
        assert_eq!(listed[1].conversation_id, "telegram:2");
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

    fn message(conversation: &str, external_id: &str, text: &str) -> MessageRecord {
        MessageRecord {
            id: 0,
            conversation_id: conversation.to_string(),
            external_id: external_id.to_string(),
            message_id: external_id.to_string(),
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
        }
    }

    #[tokio::test]
    async fn a_recorded_message_reads_back() {
        let store = Store::open_in_memory().await.expect("opens");
        let mut record = message("telegram:1", "10", "the deploy is stuck");
        record.attachments = vec!["7".to_string(), "8".to_string()];
        record.notes = Some("photo".to_string());
        store.record_message(record.clone()).await.expect("record");
        let history = store.history("telegram:1", 10, None).await.expect("read");
        // The id is assigned on insert, so it is the one field the caller cannot predict.
        assert_eq!(history.len(), 1);
        assert!(history[0].id > 0, "a paging cursor must be handed back");
        assert_eq!(history[0], MessageRecord {
            id: history[0].id,
            ..record
        });
    }

    #[tokio::test]
    async fn recording_the_same_message_twice_keeps_one_copy() {
        // A platform replaying updates after a crash must not double the history.
        let store = Store::open_in_memory().await.expect("opens");
        let record = message("telegram:1", "10", "hello");
        store.record_message(record.clone()).await.expect("record");
        store.record_message(record).await.expect("re-record");
        assert_eq!(
            store
                .history("telegram:1", 10, None)
                .await
                .expect("read")
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn history_returns_the_most_recent_in_the_order_they_were_said() {
        let store = Store::open_in_memory().await.expect("opens");
        for index in 0..5 {
            let mut record = message("telegram:1", &index.to_string(), &format!("line {index}"));
            record.timestamp = now() + chrono::Duration::seconds(index);
            store.record_message(record).await.expect("record");
        }
        let history = store.history("telegram:1", 3, None).await.expect("read");
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
        let store = Store::open_in_memory().await.expect("opens");
        let stamp = now();
        for index in 0..6 {
            let mut record = message("telegram:1", &index.to_string(), &format!("line {index}"));
            // Two distinct seconds, three messages in each.
            record.timestamp = stamp + chrono::Duration::seconds(i64::from(index / 3));
            store.record_message(record).await.expect("record");
        }

        let mut seen = Vec::new();
        let mut cursor = None;
        loop {
            let page = store.history("telegram:1", 2, cursor).await.expect("read");
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
        let store = Store::open_in_memory().await.expect("opens");
        store
            .record_message(message("telegram:1", "1", "ours"))
            .await
            .expect("record");
        store
            .record_message(message("telegram:2", "1", "theirs"))
            .await
            .expect("record");
        let history = store.history("telegram:1", 10, None).await.expect("read");
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].text, "ours");
    }

    #[tokio::test]
    async fn search_finds_a_message_and_can_be_narrowed_to_one_chat() {
        let store = Store::open_in_memory().await.expect("opens");
        store
            .record_message(message(
                "telegram:1",
                "1",
                "the certificate expires on friday",
            ))
            .await
            .expect("record");
        store
            .record_message(message(
                "telegram:2",
                "1",
                "certificate renewal is automated",
            ))
            .await
            .expect("record");
        store
            .record_message(message("telegram:1", "2", "lunch?"))
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
        assert_eq!(scoped[0].conversation_id, "telegram:2");
    }

    #[tokio::test]
    async fn pruning_history_takes_the_search_index_with_it() {
        // External-content FTS5 keeps no copy of the text, so a delete that skipped the trigger
        // would leave the index pointing at rows that no longer exist.
        let store = Store::open_in_memory().await.expect("opens");
        let mut old = message("telegram:1", "1", "ancient business");
        old.timestamp = now() - chrono::Duration::days(40);
        store.record_message(old).await.expect("record");
        store
            .record_message(message("telegram:1", "2", "ancient history repeats"))
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
        assert_eq!(hits[0].external_id, "2");
    }

    #[tokio::test]
    async fn taking_unseen_counts_everything_and_returns_only_the_tail() {
        let store = Store::open_in_memory().await.expect("opens");
        for index in 0..10 {
            let mut record = message("telegram:1", &index.to_string(), &format!("line {index}"));
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
        let store = Store::open_in_memory().await.expect("opens");
        store
            .record_message(message("telegram:1", "1", "said too much"))
            .await
            .expect("record");

        let marked = store
            .mark_deleted("telegram:1", "1", now())
            .await
            .expect("mark");
        assert!(marked, "the row was there to mark");

        let history = store.history("telegram:1", 10, None).await.expect("read");
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
    async fn marking_a_deletion_twice_keeps_the_first_time() {
        // A platform can report the same deletion more than once, and a redelivery must not move
        // the time it happened forward.
        let store = Store::open_in_memory().await.expect("opens");
        store
            .record_message(message("telegram:1", "1", "said too much"))
            .await
            .expect("record");
        let first = now();
        assert!(
            store
                .mark_deleted("telegram:1", "1", first)
                .await
                .expect("mark")
        );
        assert!(
            !store
                .mark_deleted("telegram:1", "1", first + chrono::Duration::hours(1))
                .await
                .expect("mark"),
            "the second report changes nothing"
        );
        let history = store.history("telegram:1", 10, None).await.expect("read");
        assert_eq!(
            history.first().and_then(|row| row.deleted_at),
            Some(first),
            "the time kept is when it was first reported gone"
        );
    }

    #[tokio::test]
    async fn an_edit_supersedes_the_wording_it_replaced() {
        // Both rows stay, because "what did they say before they changed it" is worth answering,
        // but only one of them is current. Without the mark `read_history` returns the two wordings
        // as separate messages that both look like the live one.
        let store = Store::open_in_memory().await.expect("opens");
        let mut original = message("telegram:1", "7", "meet at four");
        original.message_id = "7".to_string();
        store.record_message(original).await.expect("record");

        let mut revision = message("telegram:1", "7:e1", "meet at five");
        revision.message_id = "7".to_string();
        store.record_message(revision).await.expect("record");

        let marked = store
            .supersede_message("telegram:1", "7", "7:e1", now())
            .await
            .expect("supersede");
        assert_eq!(marked, 1, "only the older wording is marked");

        let history = store.history("telegram:1", 10, None).await.expect("read");
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
        let store = Store::open_in_memory().await.expect("opens");
        let current = async || {
            store
                .history("telegram:1", 10, None)
                .await
                .expect("read")
                .into_iter()
                .filter(|row| row.superseded_at.is_none())
                .map(|row| row.text)
                .collect::<Vec<_>>()
        };
        for (external_id, text) in [("9", "one"), ("9:e1", "two"), ("9:e2", "three")] {
            let mut record = message("telegram:1", external_id, text);
            record.message_id = "9".to_string();
            store.record_message(record).await.expect("record");
            store
                .supersede_message("telegram:1", "9", external_id, now())
                .await
                .expect("supersede");
        }
        assert_eq!(current().await, vec!["three"], "the last edit is current");

        // The same edit again, the way a replayed batch delivers it.
        let mut replayed = message("telegram:1", "9:e1", "two");
        replayed.message_id = "9".to_string();
        store.record_message(replayed).await.expect("record");
        store
            .supersede_message("telegram:1", "9", "9:e1", now())
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
        let store = Store::open_in_memory().await.expect("opens");
        for (external_id, text) in [("1", "kept"), ("2", "retracted"), ("3", "the old wording")] {
            store
                .record_message(message("telegram:1", external_id, text))
                .await
                .expect("record");
        }
        store
            .mark_deleted("telegram:1", "2", now())
            .await
            .expect("mark");
        // Recorded rather than merely named: superseding is ordered against the revision's own row,
        // so a replacement that is not in the table marks nothing, which is the point.
        let mut revision = message("telegram:1", "3:e1", "the new wording");
        revision.message_id = "3".to_string();
        store.record_message(revision).await.expect("record");
        store
            .supersede_message("telegram:1", "3", "3:e1", now())
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
        let store = Store::open_in_memory().await.expect("opens");
        let mut original = message("telegram:1", "5", "meet at four");
        original.message_id = "5".to_string();
        store.record_message(original).await.expect("record");
        let mut revision = message("telegram:1", "5:e1", "meet at five");
        revision.message_id = "5".to_string();
        store.record_message(revision).await.expect("record");
        store
            .supersede_message("telegram:1", "5", "5:e1", now())
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
        store
            .mark_seen("telegram:1", watermark, through)
            .await
            .expect("mark seen");

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
                .history("telegram:1", 10, None)
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
        let store = Store::open_in_memory().await.expect("opens");
        let mut record = message("telegram:1", "1", "on it");
        record.own = true;
        record.session_id = Some("scheduled-news".to_string());
        record.seen = true;
        store.record_message(record).await.expect("record");

        let history = store.history("telegram:1", 10, None).await.expect("read");
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
        let store = Store::open_in_memory().await.expect("opens");
        store
            .record_message(message("telegram:1", "1", "the deploy is stuck"))
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
        let store = Store::open_in_memory().await.expect("opens");
        let marker = async || store.unseen_summary(None).await.expect("summary").marker();

        let quiet = marker().await;
        assert_eq!(quiet, "never", "a bridge that has heard nothing says so");

        let mut first = message("telegram:1", "1", "one");
        first.timestamp = now();
        store.record_message(first).await.expect("record");
        let after_one = marker().await;
        assert_ne!(quiet, after_one, "a new message has to register");

        assert_eq!(marker().await, after_one, "a quiet room must not fire");

        // The one that used to fire wrongly. An ordinary turn sweeps the backlog to zero, which
        // moved a count-carrying marker and sent the watcher to announce news the agent had just
        // been handed.
        store
            .mark_seen("telegram:1", i64::MAX, now() + chrono::Duration::hours(1))
            .await
            .expect("mark seen");
        assert_eq!(
            marker().await,
            after_one,
            "being shown the backlog is not news of a new message"
        );

        let mut second = message("telegram:2", "2", "two");
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
        let store = Store::open_in_memory().await.expect("opens");
        let marker = async || store.unseen_summary(None).await.expect("summary").marker();

        let mut theirs = message("telegram:1", "1", "the deploy is stuck");
        theirs.timestamp = now();
        store.record_message(theirs).await.expect("record");
        let after_theirs = marker().await;

        let mut ours = message("telegram:1", "2", "on it");
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
        let store = Store::open_in_memory().await.expect("opens");
        let marker = async || store.unseen_summary(None).await.expect("summary").marker();

        let mut first = message("telegram:1", "1", "one");
        first.timestamp = now();
        store.record_message(first).await.expect("record");
        let after_first = marker().await;

        let mut second = message("telegram:1", "2", "spoke too soon");
        second.timestamp = now() + chrono::Duration::seconds(30);
        store.record_message(second).await.expect("record");
        assert_ne!(marker().await, after_first);

        store
            .mark_deleted("telegram:1", "2", now())
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
        let store = Store::open_in_memory().await.expect("opens");
        store
            .record_message(message("telegram:1", "1", "one"))
            .await
            .expect("record");
        store
            .mark_seen("telegram:1", i64::MAX, now() + chrono::Duration::hours(1))
            .await
            .expect("mark seen");

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
        let store = Store::open_in_memory().await.expect("opens");
        store
            .record_message(message("telegram:1", "1", "one"))
            .await
            .expect("record");
        store
            .record_message(message("telegram:2", "2", "two"))
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
        let store = Store::open_in_memory().await.expect("opens");
        store
            .record_message(message("telegram:1", "1", "first"))
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

        store
            .mark_seen("telegram:1", watermark, through)
            .await
            .expect("mark seen");
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
        let store = Store::open_in_memory().await.expect("opens");
        let mut earlier = message("telegram:1", "1", "before");
        earlier.timestamp = now();
        store.record_message(earlier).await.expect("record");
        let mut later = message("telegram:1", "2", "after");
        later.timestamp = now() + chrono::Duration::minutes(5);
        store.record_message(later).await.expect("record");

        let cutoff = now() + chrono::Duration::minutes(1);
        let (count, _, watermark) = store
            .take_unseen("telegram:1", cutoff, 5)
            .await
            .expect("take");
        assert_eq!(count, 1);
        store
            .mark_seen("telegram:1", watermark, cutoff)
            .await
            .expect("mark");
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
        let store = Store::open_in_memory().await.expect("opens");
        let ceiling = now();

        // Recorded first, so it has the lower id, but stamped after the ceiling: an ordinary
        // message that arrived while an older edit was still being processed.
        let mut later = message("telegram:1", "1", "after the ceiling");
        later.timestamp = ceiling + chrono::Duration::minutes(5);
        store.record_message(later).await.expect("record");

        // An edit of something old: higher id, lower timestamp, so it is what sets the watermark.
        let mut edit = message("telegram:1", "2", "edit of an old message");
        edit.timestamp = ceiling - chrono::Duration::hours(1);
        store.record_message(edit).await.expect("record");

        let (count, _, watermark) = store
            .take_unseen("telegram:1", ceiling, 5)
            .await
            .expect("take");
        assert_eq!(count, 1, "only the edit is inside the ceiling");

        store
            .mark_seen("telegram:1", watermark, ceiling)
            .await
            .expect("mark seen");

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
        let store = Store::open_in_memory().await.expect("opens");
        let asked_about = now();
        let mut first = message("telegram:1", "1", "before the turn");
        first.timestamp = asked_about;
        store.record_message(first).await.expect("record");

        let (count, _, watermark) = store
            .take_unseen("telegram:1", asked_about, 5)
            .await
            .expect("take");
        assert_eq!(count, 1);

        // Arrives while the turn is running, sharing the second the count was taken at.
        let mut during = message("telegram:1", "2", "arrived mid-turn");
        during.timestamp = asked_about;
        store.record_message(during).await.expect("record");

        store
            .mark_seen("telegram:1", watermark, asked_about)
            .await
            .expect("mark seen");

        let summary = store.unseen_summary(None).await.expect("summary");
        assert_eq!(
            summary.count, 1,
            "a message written while the turn ran was marked seen without ever being shown"
        );
    }

    async fn store_with_conversation() -> Store {
        let store = Store::open_in_memory().await.expect("opens");
        store
            .upsert_conversation(conversation("telegram:123"))
            .await
            .expect("upsert");
        store
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
        let store = store_with_conversation().await;
        let outcome = store
            .enqueue("telegram:123", "m1", "{\"text\":\"hi\"}", now(), 10)
            .await
            .expect("enqueue");
        assert_eq!(outcome, EnqueueOutcome::Queued);
        assert_eq!(store.pending_count().await.expect("count"), 1);

        let batch = claim_all(&store, 10).await;
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].payload, "{\"text\":\"hi\"}");
        assert_eq!(store.pending_count().await.expect("count"), 0);

        let sequences: Vec<i64> = batch.iter().map(|message| message.seq).collect();
        store.complete_batch(&sequences).await.expect("complete");
        let stats = store.queue_stats().await.expect("stats");
        assert_eq!(stats.done, 1);
        assert_eq!(stats.pending, 0);
    }

    #[tokio::test]
    async fn peek_does_not_claim_rows() {
        let store = store_with_conversation().await;
        store
            .enqueue("telegram:123", "m1", "a", now(), 10)
            .await
            .expect("enqueue");
        let peeked = store.peek_pending(10).await.expect("peek");
        assert_eq!(peeked.len(), 1);
        // The drain loop must still see it; an inspection command that consumed rows would strand
        // undelivered messages.
        assert_eq!(store.pending_count().await.expect("count"), 1);
        assert_eq!(claim_all(&store, 10).await.len(), 1);
    }

    #[tokio::test]
    async fn duplicate_external_ids_are_rejected() {
        let store = store_with_conversation().await;
        store
            .enqueue("telegram:123", "m1", "a", now(), 10)
            .await
            .expect("first");
        let second = store
            .enqueue("telegram:123", "m1", "a", now(), 10)
            .await
            .expect("second");
        assert_eq!(second, EnqueueOutcome::Duplicate);
        assert_eq!(store.pending_count().await.expect("count"), 1);
    }

    #[tokio::test]
    async fn a_pending_window_spans_oldest_to_newest() {
        let store = store_with_conversation().await;
        assert!(
            store.pending_windows().await.expect("query").is_empty(),
            "an empty queue has no window to debounce against"
        );

        let first = now() - chrono::Duration::seconds(30);
        let last = now();
        store
            .enqueue("telegram:123", "m1", "a", first, 10)
            .await
            .expect("first");
        store
            .enqueue("telegram:123", "m2", "b", last, 10)
            .await
            .expect("second");

        let windows = store.pending_windows().await.expect("query");
        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0].conversation_id, "telegram:123");
        assert_eq!(windows[0].oldest.timestamp(), first.timestamp());
        assert_eq!(windows[0].newest.timestamp(), last.timestamp());
    }

    #[tokio::test]
    async fn each_conversation_gets_its_own_window() {
        // The reason this is grouped at all. One window over the whole queue meant a chat still
        // mid-burst deferred delivery for every other chat, which on a platform holding for
        // somebody to stop typing would be a busy room stalling a direct message.
        let store = store_with_conversation().await;
        let old = now() - chrono::Duration::seconds(30);
        store
            .enqueue("telegram:123", "m1", "a", old, 10)
            .await
            .expect("first");
        store
            .enqueue("telegram:999", "m2", "b", now(), 10)
            .await
            .expect("second");

        let mut windows = store.pending_windows().await.expect("query");
        windows.sort_by(|left, right| left.conversation_id.cmp(&right.conversation_id));
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
        let store = store_with_conversation().await;
        store
            .enqueue("telegram:123", "m1", "a", now(), 10)
            .await
            .expect("first");
        store
            .enqueue("telegram:999", "m2", "b", now(), 10)
            .await
            .expect("second");

        let batch = store
            .claim_batch(&["telegram:999".to_string()], 10)
            .await
            .expect("claim");
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].conversation_id, "telegram:999");
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
        let store = store_with_conversation().await;
        store
            .enqueue("telegram:123", "m1", "a", now(), 10)
            .await
            .expect("enqueue");
        assert!(store.claim_batch(&[], 10).await.expect("claim").is_empty());
        assert_eq!(store.pending_count().await.expect("count"), 1);
    }

    #[tokio::test]
    async fn claimed_rows_leave_the_pending_window() {
        // The drain loop debounces on this, so an in-flight batch must not keep holding the window
        // open and defer the messages behind it.
        let store = store_with_conversation().await;
        store
            .enqueue("telegram:123", "m1", "a", now(), 10)
            .await
            .expect("enqueue");
        claim_all(&store, 10).await;
        assert!(store.pending_windows().await.expect("query").is_empty());
    }

    #[tokio::test]
    async fn an_edit_is_not_mistaken_for_a_redelivery() {
        // A platform edit reuses the id of the message it revises. The channel layer derives a
        // distinct key for that reason; this pins the queue half of the contract, because keying an
        // edit on the bare message id makes it vanish into the duplicate check.
        let store = store_with_conversation().await;
        store
            .enqueue("telegram:123", "m1", "meet at 5", now(), 10)
            .await
            .expect("original");
        let edit = store
            .enqueue("telegram:123", "m1:e1754400000", "meet at 6", now(), 10)
            .await
            .expect("edit");
        assert_eq!(edit, EnqueueOutcome::Queued);
        assert_eq!(store.pending_count().await.expect("count"), 2);
    }

    #[tokio::test]
    async fn queue_depth_is_enforced_against_waiting_rows() {
        let store = store_with_conversation().await;
        for index in 0..2 {
            let outcome = store
                .enqueue("telegram:123", &format!("m{index}"), "a", now(), 2)
                .await
                .expect("enqueue");
            assert_eq!(outcome, EnqueueOutcome::Queued);
        }
        let overflow = store
            .enqueue("telegram:123", "m2", "a", now(), 2)
            .await
            .expect("enqueue");
        assert_eq!(overflow, EnqueueOutcome::Dropped);

        // In-flight rows still count as waiting, otherwise a slow turn would let the queue grow
        // without bound.
        claim_all(&store, 2).await;
        let still_full = store
            .enqueue("telegram:123", "m3", "a", now(), 2)
            .await
            .expect("enqueue");
        assert_eq!(still_full, EnqueueOutcome::Dropped);
    }

    #[tokio::test]
    async fn claim_preserves_arrival_order() {
        let store = store_with_conversation().await;
        for index in 0..5 {
            store
                .enqueue(
                    "telegram:123",
                    &format!("m{index}"),
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
    async fn a_released_batch_keeps_its_attempt_budget() {
        // meka refusing a submission because a turn is already running is a deferral, not a failed
        // delivery: it now does that routinely for background tasks and scheduled wakes. Spending
        // an attempt on it would let a busy session declare a message undeliverable that meka never
        // saw, which is exactly what happened in the field.
        let store = store_with_conversation().await;
        store
            .enqueue("telegram:123", "m1", "a", now(), 10)
            .await
            .expect("enqueue");

        for _ in 0..5 {
            let batch = claim_all(&store, 10).await;
            assert_eq!(batch.len(), 1, "the message must stay claimable");
            assert_eq!(batch[0].attempts, 0, "a deferral is not an attempt");
            let sequences: Vec<i64> = batch.iter().map(|message| message.seq).collect();
            store.release_batch(&sequences).await.expect("release");
            assert_eq!(store.pending_count().await.expect("count"), 1);
        }

        // And the budget is still intact for a genuine failure afterwards.
        let batch = claim_all(&store, 10).await;
        let sequences: Vec<i64> = batch.iter().map(|message| message.seq).collect();
        let outcome = store
            .fail_batch(&sequences, "provider 502", 1, None)
            .await
            .expect("fail");
        assert_eq!(
            outcome.retrying, sequences,
            "the first real failure retries"
        );
        assert_eq!(store.queue_stats().await.expect("stats").failed, 0);
    }

    #[tokio::test]
    async fn failed_batch_retries_then_exhausts() {
        let store = store_with_conversation().await;
        store
            .enqueue("telegram:123", "m1", "a", now(), 10)
            .await
            .expect("enqueue");
        let batch = claim_all(&store, 10).await;
        let sequences: Vec<i64> = batch.iter().map(|message| message.seq).collect();

        let first = store
            .fail_batch(&sequences, "provider 502", 1, None)
            .await
            .expect("fail");
        assert_eq!(first.retrying, sequences);
        assert!(first.exhausted.is_empty());
        assert_eq!(store.pending_count().await.expect("count"), 1);

        let batch = claim_all(&store, 10).await;
        assert_eq!(batch[0].attempts, 1);
        let second = store
            .fail_batch(&sequences, "provider 502", 1, None)
            .await
            .expect("fail");
        assert!(second.retrying.is_empty());
        assert_eq!(second.exhausted.len(), 1);
        assert_eq!(store.pending_count().await.expect("count"), 0);
        assert_eq!(store.queue_stats().await.expect("stats").failed, 1);
    }

    #[tokio::test]
    async fn a_deferred_row_reports_when_it_may_be_offered_again() {
        // What stops the drain loop coming straight back to a provider that has just rate limited
        // it. Without the column the retry lands inside the same window and spends the budget for
        // nothing.
        let store = store_with_conversation().await;
        store
            .enqueue("telegram:123", "m1", "a", now(), 10)
            .await
            .expect("enqueue");
        let batch = claim_all(&store, 10).await;
        let sequences: Vec<i64> = batch.iter().map(|message| message.seq).collect();

        let retry_at = now() + chrono::Duration::seconds(30);
        store
            .fail_batch(&sequences, "rate limited", 3, Some(retry_at))
            .await
            .expect("fail");

        let windows = store.pending_windows().await.expect("windows");
        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0].not_before, Some(retry_at));
    }

    #[tokio::test]
    async fn one_deferred_row_defers_its_whole_conversation() {
        // `max()` rather than `min()`, and the reason is ordering: the drain loop claims by `seq`,
        // so releasing the fresh message while its predecessor waits out a rate limit would hand
        // the agent the second half of an exchange before the first.
        let store = store_with_conversation().await;
        store
            .enqueue("telegram:123", "m1", "a", now(), 10)
            .await
            .expect("enqueue");
        let batch = claim_all(&store, 10).await;
        let sequences: Vec<i64> = batch.iter().map(|message| message.seq).collect();
        let retry_at = now() + chrono::Duration::seconds(30);
        store
            .fail_batch(&sequences, "rate limited", 3, Some(retry_at))
            .await
            .expect("fail");

        store
            .enqueue("telegram:123", "m2", "b", now(), 10)
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
        let store = store_with_conversation().await;
        let mut record = message("telegram:123", "m1", "are you there?");
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
                .mark_unseen("telegram:123", "m1")
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
                .mark_unseen("telegram:123", "nothing-here")
                .await
                .expect("unsee"),
            "a message that was never recorded cannot be un-seen"
        );
    }

    #[tokio::test]
    async fn zero_retries_fails_on_first_attempt() {
        let store = store_with_conversation().await;
        store
            .enqueue("telegram:123", "m1", "a", now(), 10)
            .await
            .expect("enqueue");
        let batch = claim_all(&store, 10).await;
        let sequences: Vec<i64> = batch.iter().map(|message| message.seq).collect();
        let outcome = store
            .fail_batch(&sequences, "boom", 0, None)
            .await
            .expect("fail");
        assert_eq!(outcome.exhausted.len(), 1);
        assert!(outcome.retrying.is_empty());
    }

    #[tokio::test]
    async fn in_flight_rows_are_recovered_at_startup() {
        let store = store_with_conversation().await;
        store
            .enqueue("telegram:123", "m1", "a", now(), 10)
            .await
            .expect("enqueue");
        claim_all(&store, 10).await;
        assert_eq!(store.pending_count().await.expect("count"), 0);

        // Simulates a crash between claiming a batch and completing its turn.
        let recovered = store.reset_in_flight().await.expect("reset");
        assert_eq!(recovered, 1);
        assert_eq!(store.pending_count().await.expect("count"), 1);
    }

    #[tokio::test]
    async fn last_turn_timestamp_round_trips() {
        let store = Store::open_in_memory().await.expect("opens");
        assert_eq!(store.last_turn_at().await.expect("read"), None);
        store.mark_turn_completed(now()).await.expect("write");
        assert_eq!(store.last_turn_at().await.expect("read"), Some(now()));
    }

    #[tokio::test]
    async fn dropped_counter_accumulates_and_resets() {
        let store = Store::open_in_memory().await.expect("opens");
        assert_eq!(store.take_dropped().await.expect("take"), 0);
        store.note_dropped(2).await.expect("note");
        store.note_dropped(3).await.expect("note");
        assert_eq!(store.take_dropped().await.expect("take"), 5);
        assert_eq!(store.take_dropped().await.expect("take"), 0);
    }

    #[tokio::test]
    async fn conversations_list_most_recent_first_and_filter_by_channel() {
        let store = Store::open_in_memory().await.expect("opens");
        let mut older = conversation("telegram:1");
        older.last_inbound_at = Some(now() - chrono::Duration::hours(2));
        let mut newer = conversation("telegram:2");
        newer.chat = "2".to_string();
        newer.last_inbound_at = Some(now());
        let mut other_channel = conversation("second:9");
        other_channel.id = "second:9".to_string();
        other_channel.channel_id = "second".to_string();

        store.upsert_conversation(older).await.expect("upsert");
        store.upsert_conversation(newer).await.expect("upsert");
        store
            .upsert_conversation(other_channel)
            .await
            .expect("upsert");

        let all = store.list_conversations(None, 10).await.expect("list");
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].id, "telegram:2");

        let filtered = store
            .list_conversations(Some("second"), 10)
            .await
            .expect("list");
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].id, "second:9");
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
    async fn a_row_recovered_from_a_crash_is_marked_and_charged() {
        // The two ways a row returns to the queue are not the same, and only one of them is
        // understood. A failed turn was watched, so the bridge knows whether the agent acted. A row
        // stranded by a hard kill was in the hands of a turn nobody saw the end of: replaying it
        // silently presents work that may already be done as though it were new, and with the
        // attempt uncharged a batch that kills the process is replayed for ever.
        let store = store_with_conversation().await;
        store
            .enqueue("telegram:123", "a", "a", now(), 10)
            .await
            .expect("enqueue");
        let claimed = claim_all(&store, 1).await;
        assert!(!claimed.first().expect("claimed").recovered);

        let reset = store.reset_in_flight().await.expect("recover");
        assert_eq!(reset, 1);

        let back = claim_all(&store, 1).await;
        let row = back.first().expect("reclaimed");
        assert!(
            row.recovered,
            "a row stranded by a crash is indistinguishable from an ordinary retry"
        );
        assert_eq!(
            row.attempts, 1,
            "the interrupted attempt was not charged, so a poison batch replays without bound"
        );
    }

    #[tokio::test]
    async fn a_delivered_row_cannot_be_released_or_failed_back_into_the_queue() {
        // Every terminal write is guarded on the row still being in flight. Unguarded, a `done` row
        // could be released to `pending` and handed to the agent a second time, or re-failed --
        // which marks a delivered message unseen and tells the owner it was lost. The drain loop is
        // serial so no live path does this today, but the store is the wrong place to rely on that:
        // a second process, or `mekabridge queue clear`, reaches these directly.
        let store = store_with_conversation().await;
        store
            .enqueue("telegram:123", "a", "a", now(), 10)
            .await
            .expect("enqueue");
        let batch = claim_all(&store, 1).await;
        let seq = batch.first().expect("claimed").seq;
        store.complete_batch(&[seq]).await.expect("complete");

        store.release_batch(&[seq]).await.expect("release");
        let stats = store.queue_stats().await.expect("stats");
        assert_eq!(
            stats.done, 1,
            "a delivered row was released back to pending"
        );
        assert_eq!(stats.pending, 0);

        let outcome = store
            .fail_batch(&[seq], "boom", 0, None)
            .await
            .expect("fail");
        assert!(
            outcome.exhausted.is_empty(),
            "a delivered message was reported to the owner as permanently lost"
        );
        let stats = store.queue_stats().await.expect("stats");
        assert_eq!(stats.done, 1, "a delivered row was re-failed");
        assert_eq!(stats.failed, 0);

        // And the other direction: a row released back to the queue must not then be completable.
        // Without the guard on `complete_batch` a late completion from an abandoned attempt marks a
        // message delivered while it is still waiting to be delivered, and it is never sent.
        store
            .enqueue("telegram:123", "b", "b", now(), 10)
            .await
            .expect("enqueue");
        let batch = claim_all(&store, 1).await;
        let seq = batch.first().expect("claimed").seq;
        store.release_batch(&[seq]).await.expect("release");
        store.complete_batch(&[seq]).await.expect("complete");
        let stats = store.queue_stats().await.expect("stats");
        assert_eq!(
            stats.pending, 1,
            "a row waiting in the queue was marked delivered: {stats:?}"
        );
        assert_eq!(stats.done, 1, "and nothing new was counted as delivered");
    }

    #[tokio::test]
    async fn one_vanished_row_does_not_strand_the_rest_of_its_batch() {
        // `fail_batch` used to `?` on a row that had gone, rolling back the whole transaction and
        // leaving every other row in the batch `in_flight`. Those are invisible to
        // `pending_windows`, so newer messages in the same conversation are delivered ahead of them
        // until the next restart -- a real ordering violation on top of the stall. Reachable from
        // `mekabridge queue clear` against a running bridge.
        let store = store_with_conversation().await;
        for id in ["a", "b"] {
            store
                .enqueue("telegram:123", id, id, now(), 10)
                .await
                .expect("enqueue");
        }
        let batch = claim_all(&store, 10).await;
        assert_eq!(batch.len(), 2);
        let live = batch.first().expect("first").seq;
        let vanished = batch.get(1).expect("second").seq;
        store.clear_queue().await.expect("clear");
        store
            .enqueue("telegram:123", "a", "a", now(), 10)
            .await
            .expect("re-enqueue");
        let restored = claim_all(&store, 10).await;
        let live = restored.first().map_or(live, |row| row.seq);

        store
            .fail_batch(&[live, vanished], "boom", 3, None)
            .await
            .expect("a missing row must not abort the batch");
        let stats = store.queue_stats().await.expect("stats");
        assert_eq!(
            stats.in_flight, 0,
            "a live row was stranded in flight by a sibling that had gone: {stats:?}"
        );
    }

    #[tokio::test]
    async fn prune_delivered_only_removes_completed_rows() {
        let store = store_with_conversation().await;
        store
            .enqueue(
                "telegram:123",
                "done",
                "a",
                now() - chrono::Duration::days(30),
                10,
            )
            .await
            .expect("enqueue");
        store
            .enqueue(
                "telegram:123",
                "waiting",
                "b",
                now() - chrono::Duration::days(30),
                10,
            )
            .await
            .expect("enqueue");
        let batch = claim_all(&store, 1).await;
        store
            .complete_batch(&[batch[0].seq])
            .await
            .expect("complete");
        store.reset_in_flight().await.expect("reset");

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
    }

    fn attachment_record(id: &str, age_days: i64, path: Option<PathBuf>) -> AttachmentRecord {
        AttachmentRecord {
            id: id.to_string(),
            conversation_id: "telegram:123".to_string(),
            channel_id: "telegram".to_string(),
            kind: "photo".to_string(),
            file_ref: format!("ref-{id}"),
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
        let store = store_with_conversation().await;
        let first = store
            .register_attachment(attachment_record("a1", 0, None))
            .await
            .expect("register");
        let again = store
            .register_attachment(attachment_record("a1", 0, None))
            .await
            .expect("register again");
        assert_eq!(
            first, again,
            "the same file must keep its handle on a replay"
        );

        let other = store
            .register_attachment(attachment_record("a2", 0, None))
            .await
            .expect("register");
        assert_ne!(first, other);
    }

    #[tokio::test]
    async fn an_attachment_resolves_by_handle() {
        let store = store_with_conversation().await;
        let handle = store
            .register_attachment(attachment_record("a1", 0, None))
            .await
            .expect("register");
        let record = store
            .attachment(&handle)
            .await
            .expect("query")
            .expect("resolves");
        assert_eq!(record.file_ref, "ref-a1");
        assert_eq!(record.channel_id, "telegram");
        assert!(record.path.is_none());
    }

    #[tokio::test]
    async fn a_handle_that_is_not_a_number_resolves_to_nothing() {
        // The agent supplies this string, so a hallucinated value has to fail cleanly rather than
        // erroring out of the query layer.
        let store = store_with_conversation().await;
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
        let store = store_with_conversation().await;
        let handle = store
            .register_attachment(attachment_record("a1", 40, None))
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
            .register_attachment(attachment_record("a2", 40, None))
            .await
            .expect("register")
            .to_string()
            .max(handle);
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
        let store = store_with_conversation().await;
        store
            .register_attachment(attachment_record(
                "a1",
                40,
                Some(PathBuf::from("/tmp/a1.jpg")),
            ))
            .await
            .expect("register");
        store
            .register_attachment(attachment_record(
                "a2",
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

    #[tokio::test]
    async fn two_openers_racing_the_same_upgrade_both_survive() {
        // What a real upgrade looks like: systemd starts the daemon while an operator runs
        // `doctor`. The version was read once before the loop, so both saw the old one; the
        // transaction serialised them but the loser still ran a batch the winner had
        // committed and failed on `duplicate column name` -- the very error the transaction
        // was added to prevent.
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("state.db");
        let connection = tokio_rusqlite::Connection::open(&path)
            .await
            .expect("opens");
        connection
            .call(|connection| {
                connection.execute_batch(include_str!("store/schema_001.sql"))?;
                connection.pragma_update(None, "user_version", 1_i64)?;
                Ok::<(), tokio_rusqlite::Error>(())
            })
            .await
            .expect("build a version 1 database");
        connection.close().await.expect("closes");

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
        let connection = tokio_rusqlite::Connection::open(&path)
            .await
            .expect("opens");
        connection
            .call(|connection| {
                connection.execute_batch(include_str!("store/schema_001.sql"))?;
                connection.pragma_update(None, "user_version", 1_i64)?;
                connection.execute("INSERT INTO meta (key, value) VALUES ('session_id', ?1)", [
                    Uuid::nil().to_string(),
                ])?;
                // A message that was already waiting when the daemon was stopped to upgrade it.
                // Written without the columns later migrations add, so it is the row most likely to
                // trip one of them up.
                connection.execute(
                    "INSERT INTO inbound_queue
                         (conversation_id, external_id, payload, received_at, state)
                     VALUES ('telegram:123', 'pending-across-the-upgrade', 'a', ?1, 'pending')",
                    [to_rfc3339(now())],
                )?;
                Ok::<(), tokio_rusqlite::Error>(())
            })
            .await
            .expect("build a version 1 database");
        connection.close().await.expect("closes");

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
        store
            .set_policy("telegram:1", Policy::Mute, None, None, now())
            .await
            .expect("the table added by the upgrade is usable");
        store
            .record_message(message("telegram:1", "1", "after the upgrade"))
            .await
            .expect("the history added by the upgrade is usable");
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
        let connection = tokio_rusqlite::Connection::open(&path)
            .await
            .expect("opens");
        connection
            .call(|connection| {
                for (version, statements) in MIGRATIONS.iter().enumerate().take(7) {
                    connection.execute_batch(statements)?;
                    connection.pragma_update(None, "user_version", (version + 1) as i64)?;
                }
                connection.execute(
                    "INSERT INTO messages
                         (conversation_id, external_id, message_id, sender_id, sender_name, text,
                          notes, attachments, addressed, seen, timestamp)
                     VALUES ('telegram:1', '4', '4', '111', 'Alice', 'said before the upgrade',
                             NULL, NULL, 0, 0, ?1)",
                    [to_rfc3339(now())],
                )?;
                Ok::<(), tokio_rusqlite::Error>(())
            })
            .await
            .expect("build a version 7 database");
        connection.close().await.expect("closes");

        let store = Store::open(&path).await.expect("upgrades");
        let history = store.history("telegram:1", 10, None).await.expect("read");
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
        let connection = tokio_rusqlite::Connection::open(&path)
            .await
            .expect("opens");
        connection
            .call(|connection| {
                connection.execute_batch(include_str!("store/schema_001.sql"))?;
                connection.execute_batch(include_str!("store/schema_002.sql"))?;
                connection.pragma_update(None, "user_version", 2_i64)?;
                connection.execute(
                    "INSERT INTO mutes (conversation_id, until, reason, dropped, created_at)
                     VALUES ('telegram:-100', NULL, 'endless standup chatter', 41, ?1)",
                    [to_rfc3339(now())],
                )?;
                Ok::<(), tokio_rusqlite::Error>(())
            })
            .await
            .expect("build a version 2 database");
        connection.close().await.expect("closes");

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
}
