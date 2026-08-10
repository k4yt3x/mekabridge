//! Durable state: the session binding, the conversation address book, and the inbound queue.
//!
//! The queue is the part that matters most. One meka session runs one turn at a time, so messages
//! that arrive mid-turn have to wait somewhere, and "somewhere" cannot be process memory: a crash
//! with a full queue would silently swallow everything a user typed. Every inbound message is
//! therefore written to SQLite before it is acknowledged, claimed transactionally when a turn
//! starts, and only marked done once that turn has actually completed.
//!
//! Queue rows move through `pending` to `in_flight` to `done` or `failed`. Any row left `in_flight`
//! at startup is evidence of a crash mid-turn and is reset to `pending`, which is why the state is
//! a column rather than an in-memory flag.
//!
//! Payloads are opaque JSON strings here. Keeping the queue ignorant of message structure means the
//! state machine can be tested on its own and does not change when a new platform adds fields.

use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use rusqlite::OptionalExtension;
use uuid::Uuid;

/// Schema statements applied in order. The index of a statement is its schema version, tracked in
/// SQLite's `user_version`, so adding a migration means appending to this array and never editing
/// an existing entry.
const MIGRATIONS: &[&str] = &[include_str!("store/schema_001.sql")];

/// `meta` key holding the meka session UUID this bridge instance owns.
const META_SESSION_ID: &str = "session_id";
/// `meta` key recording that the one-time orientation preamble was delivered for the current
/// session.
const META_PREAMBLE_SENT: &str = "preamble_sent";
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
    /// `direct`, `group`, or `channel`.
    pub kind: String,
    pub created_at: DateTime<Utc>,
    pub last_inbound_at: Option<DateTime<Utc>>,
    pub last_outbound_at: Option<DateTime<Utc>>,
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
    /// to hear about, because their messages will never reach the agent.
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

/// An attachment downloaded from a platform and written to local disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachmentRecord {
    pub id: String,
    pub conversation_id: String,
    pub path: PathBuf,
    pub media_type: Option<String>,
    pub bytes: Option<u64>,
    pub created_at: DateTime<Utc>,
}

/// Handle to the SQLite database. Cheap to clone; all clones share one connection thread.
#[derive(Clone)]
pub struct Store {
    connection: tokio_rusqlite::Connection,
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
                // WAL lets `mekabridge status` read while the daemon writes. `busy_timeout` covers
                // the brief writer lock rather than surfacing SQLITE_BUSY to the caller.
                connection.pragma_update(None, "journal_mode", "WAL")?;
                connection.pragma_update(None, "foreign_keys", "ON")?;
                connection.pragma_update(None, "busy_timeout", 5_000)?;

                let version: u32 =
                    connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
                let mut applied = version as usize;
                while applied < MIGRATIONS.len() {
                    let statement = MIGRATIONS.get(applied).copied().unwrap_or_default();
                    connection.execute_batch(statement)?;
                    applied += 1;
                    connection.pragma_update(None, "user_version", applied as i64)?;
                }
                Ok(())
            })
            .await?;
        Ok(Self { connection })
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

    /// Bind this bridge to `session_id`, clearing the preamble flag so the new session gets its own
    /// orientation message.
    pub async fn set_session_id(&self, session_id: Uuid) -> Result<()> {
        self.meta_set(META_SESSION_ID, session_id.to_string())
            .await?;
        self.meta_delete(META_PREAMBLE_SENT).await
    }

    /// Forget the session binding. The next turn creates a fresh session.
    pub async fn clear_session_id(&self) -> Result<()> {
        self.meta_delete(META_SESSION_ID).await?;
        self.meta_delete(META_PREAMBLE_SENT).await
    }

    /// Whether the current session has already received the orientation preamble.
    pub async fn preamble_sent(&self) -> Result<bool> {
        Ok(self.meta_get(META_PREAMBLE_SENT).await?.is_some())
    }

    /// Record that the orientation preamble was delivered.
    pub async fn mark_preamble_sent(&self) -> Result<()> {
        self.meta_set(META_PREAMBLE_SENT, "1".to_string()).await
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

    /// Stamp a conversation as having just received an outbound message.
    pub async fn touch_outbound(&self, id: &str, at: DateTime<Utc>) -> Result<()> {
        let id = id.to_string();
        self.connection
            .call(move |connection| {
                connection.execute(
                    "UPDATE conversations SET last_outbound_at = ?2 WHERE id = ?1",
                    rusqlite::params![id, to_rfc3339(at)],
                )?;
                Ok(())
            })
            .await?;
        Ok(())
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
                let transaction = connection.transaction()?;
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

    /// Atomically take up to `limit` pending messages and mark them in flight.
    ///
    /// Claiming and marking happen in one transaction so two drain loops (or a drain loop racing a
    /// restart) cannot hand the same message to two turns.
    pub async fn claim_batch(&self, limit: usize) -> Result<Vec<QueuedMessage>> {
        let limit = limit as i64;
        let batch = self
            .connection
            .call(move |connection| {
                let transaction = connection.transaction()?;
                let claimed = {
                    let mut statement = transaction.prepare(
                        "SELECT seq, conversation_id, external_id, payload, received_at, attempts
                         FROM inbound_queue
                         WHERE state = 'pending'
                         ORDER BY seq
                         LIMIT ?1",
                    )?;
                    let rows = statement.query_map([limit], row_to_queued)?;
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
                    "SELECT seq, conversation_id, external_id, payload, received_at, attempts
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
        self.connection
            .call(move |connection| {
                let transaction = connection.transaction()?;
                for sequence in sequences {
                    transaction.execute(
                        "UPDATE inbound_queue SET state = 'done', last_error = NULL WHERE seq = ?1",
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
    /// can be told which messages will never be delivered.
    pub async fn fail_batch(
        &self,
        sequences: &[i64],
        error: &str,
        max_attempts: u32,
    ) -> Result<FailureOutcome> {
        let sequences = sequences.to_vec();
        let error = error.to_string();
        let outcome = self
            .connection
            .call(move |connection| {
                let transaction = connection.transaction()?;
                let mut retrying = Vec::new();
                let mut exhausted = Vec::new();
                for sequence in sequences {
                    transaction.execute(
                        "UPDATE inbound_queue
                         SET attempts = attempts + 1, last_error = ?2
                         WHERE seq = ?1",
                        rusqlite::params![sequence, error],
                    )?;
                    let attempts: u32 = transaction.query_row(
                        "SELECT attempts FROM inbound_queue WHERE seq = ?1",
                        [sequence],
                        |row| row.get(0),
                    )?;
                    if attempts > max_attempts {
                        transaction.execute(
                            "UPDATE inbound_queue SET state = 'failed' WHERE seq = ?1",
                            [sequence],
                        )?;
                        let message = transaction.query_row(
                            "SELECT seq, conversation_id, external_id, payload, received_at,
                                    attempts
                             FROM inbound_queue WHERE seq = ?1",
                            [sequence],
                            row_to_queued,
                        )?;
                        exhausted.push(message);
                    } else {
                        transaction.execute(
                            "UPDATE inbound_queue SET state = 'pending' WHERE seq = ?1",
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
                    "UPDATE inbound_queue SET state = 'pending' WHERE state = 'in_flight'",
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
                    "DELETE FROM inbound_queue WHERE state = 'done' AND received_at < ?1",
                    [before],
                )
            })
            .await?;
        Ok(deleted)
    }

    /// Record a downloaded attachment.
    pub async fn record_attachment(&self, record: AttachmentRecord) -> Result<()> {
        self.connection
            .call(move |connection| {
                connection.execute(
                    "INSERT INTO attachments
                         (id, conversation_id, path, media_type, bytes, created_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                     ON CONFLICT(id) DO NOTHING",
                    rusqlite::params![
                        record.id,
                        record.conversation_id,
                        record.path.to_string_lossy().into_owned(),
                        record.media_type,
                        record.bytes.map(|bytes| bytes as i64),
                        to_rfc3339(record.created_at),
                    ],
                )?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    /// Delete attachment rows older than `before` and return their paths so the caller can unlink
    /// the files. Rows go first: a leftover file is recoverable, a leaked row is not observable.
    pub async fn take_expired_attachments(&self, before: DateTime<Utc>) -> Result<Vec<PathBuf>> {
        let before = to_rfc3339(before);
        let paths = self
            .connection
            .call(move |connection| {
                let transaction = connection.transaction()?;
                let paths = {
                    let mut statement = transaction
                        .prepare("SELECT path FROM attachments WHERE created_at < ?1")?;
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
    async fn binding_a_new_session_resets_the_preamble_flag() {
        let store = Store::open_in_memory().await.expect("opens");
        store.set_session_id(Uuid::new_v4()).await.expect("write");
        store.mark_preamble_sent().await.expect("mark");
        assert!(store.preamble_sent().await.expect("read"));
        // A replacement session has an empty context, so it must be oriented again.
        store.set_session_id(Uuid::new_v4()).await.expect("rebind");
        assert!(!store.preamble_sent().await.expect("read"));
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

        let batch = store.claim_batch(10).await.expect("claim");
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
        assert_eq!(store.claim_batch(10).await.expect("claim").len(), 1);
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
        store.claim_batch(2).await.expect("claim");
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
        let batch = store.claim_batch(3).await.expect("claim");
        let payloads: Vec<&str> = batch
            .iter()
            .map(|message| message.payload.as_str())
            .collect();
        assert_eq!(payloads, vec!["0", "1", "2"]);
    }

    #[tokio::test]
    async fn failed_batch_retries_then_exhausts() {
        let store = store_with_conversation().await;
        store
            .enqueue("telegram:123", "m1", "a", now(), 10)
            .await
            .expect("enqueue");
        let batch = store.claim_batch(10).await.expect("claim");
        let sequences: Vec<i64> = batch.iter().map(|message| message.seq).collect();

        let first = store
            .fail_batch(&sequences, "provider 502", 1)
            .await
            .expect("fail");
        assert_eq!(first.retrying, sequences);
        assert!(first.exhausted.is_empty());
        assert_eq!(store.pending_count().await.expect("count"), 1);

        let batch = store.claim_batch(10).await.expect("reclaim");
        assert_eq!(batch[0].attempts, 1);
        let second = store
            .fail_batch(&sequences, "provider 502", 1)
            .await
            .expect("fail");
        assert!(second.retrying.is_empty());
        assert_eq!(second.exhausted.len(), 1);
        assert_eq!(store.pending_count().await.expect("count"), 0);
        assert_eq!(store.queue_stats().await.expect("stats").failed, 1);
    }

    #[tokio::test]
    async fn zero_retries_fails_on_first_attempt() {
        let store = store_with_conversation().await;
        store
            .enqueue("telegram:123", "m1", "a", now(), 10)
            .await
            .expect("enqueue");
        let batch = store.claim_batch(10).await.expect("claim");
        let sequences: Vec<i64> = batch.iter().map(|message| message.seq).collect();
        let outcome = store.fail_batch(&sequences, "boom", 0).await.expect("fail");
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
        store.claim_batch(10).await.expect("claim");
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
        let batch = store.claim_batch(1).await.expect("claim");
        store
            .complete_batch(&[batch[0].seq])
            .await
            .expect("complete");
        store.reset_in_flight().await.expect("reset");

        let pruned = store.prune_delivered(now()).await.expect("prune");
        assert_eq!(pruned, 1);
        assert_eq!(store.pending_count().await.expect("count"), 1);
    }

    #[tokio::test]
    async fn expired_attachments_are_returned_for_unlinking() {
        let store = store_with_conversation().await;
        store
            .record_attachment(AttachmentRecord {
                id: "a1".to_string(),
                conversation_id: "telegram:123".to_string(),
                path: PathBuf::from("/tmp/a1.jpg"),
                media_type: Some("image/jpeg".to_string()),
                bytes: Some(1024),
                created_at: now() - chrono::Duration::days(40),
            })
            .await
            .expect("record");
        store
            .record_attachment(AttachmentRecord {
                id: "a2".to_string(),
                conversation_id: "telegram:123".to_string(),
                path: PathBuf::from("/tmp/a2.jpg"),
                media_type: None,
                bytes: None,
                created_at: now(),
            })
            .await
            .expect("record");

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
