-- Message identity scoped by the account that received it, and foreign keys throughout.
--
-- Until now a message was `(conversation, external_id)`, where the conversation is the address
-- `<channel>:<chat>` and the external id is the platform's message id. That asserts a message id is
-- unique within a chat for ever, which is false on Telegram: a private chat numbers its messages
-- per bot account, so a bot deleted and recreated under the same channel slot starts again at 1
-- while the address stays the same. Every colliding message was then swallowed by the unique
-- constraint: not queued, so never delivered, and not recorded, so invisible to the history tools,
-- with nothing above debug level saying so. A recreated bot's photos fared worse still, since the
-- attachment registry handed back the old bot's handle for a file the new bot cannot fetch.
--
-- The account is what scopes the id space, and it was not in the model at all: the channel slot
-- name is a config entry, not an identity. `accounts` records who each slot has been logged in as,
-- and the three tables that hold platform message ids are keyed on the account as well. The address
-- deliberately stays what it was, since it is the routing contract the agent, the operator's
-- config, and every policy refer to; a bot swap changes which account is behind an address, not the
-- address.
--
-- The edit encoding goes with it. An edit used to be `<id>:e<time>` in a string, in seconds on one
-- platform and milliseconds on another; it is now a `revision` column, the edit time in epoch
-- milliseconds and zero for the original. `NOT NULL DEFAULT 0` rather than a nullable edit time
-- because SQLite treats NULLs as distinct in a unique constraint, which would stop a redelivered
-- original being recognised.
--
-- Rows written before this migration belong to a placeholder account per channel, `platform_id`
-- empty, rather than to whichever account is running when the bridge next starts. Adopting them
-- into the live account would be right for every deployment that never swapped bots and would
-- re-create the collision for the one that did, for the rest of its retention window; the
-- placeholder costs at most one redelivery check at the first restart, and a graceful restart does
-- not exercise that.
--
-- Nothing enforced the parent row until now, so a policy set on a chat that had not yet written,
-- which `set_policy` permits on purpose, has no conversation row. Those addresses get one before
-- the constraints go on, with what can be read off the address and `unknown` for the rest; the
-- next message from there fills it in.
--
-- SQLite cannot alter a unique constraint, so every table that carries one is rebuilt. Row ids are
-- carried across: `messages.id` is the agent's paging cursor and the `mark_seen` watermark,
-- `attachments.handle` travels inside queued payloads, and `inbound_queue.seq` is delivery order.

CREATE TABLE accounts (
    id            INTEGER PRIMARY KEY,
    -- The channel slot from the config, which is also the first segment of every address.
    channel       TEXT NOT NULL,
    platform      TEXT NOT NULL,
    -- The bot's own user id on the platform. Empty only on the placeholder that holds rows written
    -- before accounts were recorded, for which it is unknown.
    platform_id   TEXT NOT NULL,
    username      TEXT,
    display_name  TEXT,
    first_seen_at TEXT NOT NULL,
    -- Bumped every start. The account a channel is currently logged in as is the real one seen
    -- most recently, which is what decides whether a recorded message can still be replied to.
    last_seen_at  TEXT NOT NULL,
    UNIQUE (channel, platform_id)
);

-- One placeholder per channel that has any row at all. `min(platform)` picks a real platform over
-- the 'unknown' the tables without one contribute, since both platform names sort below it.
INSERT INTO accounts (channel, platform, platform_id, first_seen_at, last_seen_at)
SELECT channel, min(platform), '', min(at), max(at)
FROM (
    SELECT channel_id AS channel, platform, created_at AS at FROM conversations
    UNION ALL
    SELECT substr(conversation_id, 1, instr(conversation_id, ':') - 1), 'unknown', created_at
    FROM conversation_policy
    UNION ALL
    SELECT substr(conversation_id, 1, instr(conversation_id, ':') - 1), 'unknown', received_at
    FROM inbound_queue
    UNION ALL
    SELECT substr(conversation_id, 1, instr(conversation_id, ':') - 1), 'unknown', timestamp
    FROM messages
    UNION ALL
    SELECT channel_id, 'unknown', created_at FROM attachments
)
GROUP BY channel;

CREATE TABLE conversations_new (
    id               INTEGER PRIMARY KEY,
    -- `<channel>:<chat>[:<thread>]`, the only form the agent and the operator ever see. Unique
    -- here rather than as `(channel, chat, thread)` so a NULL thread never has to be reasoned
    -- about.
    address          TEXT NOT NULL UNIQUE,
    channel          TEXT NOT NULL,
    platform         TEXT NOT NULL,
    title            TEXT,
    kind             TEXT NOT NULL,
    created_at       TEXT NOT NULL,
    last_inbound_at  TEXT,
    last_outbound_at TEXT
);

INSERT INTO conversations_new
    (address, channel, platform, title, kind, created_at, last_inbound_at, last_outbound_at)
SELECT id, channel_id, platform, title, kind, created_at, last_inbound_at, last_outbound_at
FROM conversations;

-- Addresses that appear in a child table without a row of their own.
INSERT OR IGNORE INTO conversations_new (address, channel, platform, kind, created_at)
SELECT address,
       substr(address, 1, instr(address, ':') - 1),
       COALESCE(
           (SELECT a.platform FROM accounts a
            WHERE a.channel = substr(address, 1, instr(address, ':') - 1)),
           'unknown'),
       'unknown',
       min(at)
FROM (
    SELECT conversation_id AS address, created_at AS at FROM conversation_policy
    UNION ALL
    SELECT conversation_id, received_at FROM inbound_queue
    UNION ALL
    SELECT conversation_id, timestamp FROM messages
    UNION ALL
    SELECT conversation_id, created_at FROM attachments
)
GROUP BY address;

DROP TABLE conversations;
ALTER TABLE conversations_new RENAME TO conversations;
CREATE INDEX idx_conversations_channel ON conversations (channel);

CREATE TABLE conversation_policy_new (
    conversation_id INTEGER PRIMARY KEY REFERENCES conversations (id) ON DELETE CASCADE,
    mode            TEXT NOT NULL CHECK (mode IN ('active', 'mute', 'block')),
    until           TEXT,
    reason          TEXT,
    dropped         INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT NOT NULL
);

INSERT INTO conversation_policy_new (conversation_id, mode, until, reason, dropped, created_at)
SELECT c.id, p.mode, p.until, p.reason, p.dropped, p.created_at
FROM conversation_policy p
JOIN conversations c ON c.address = p.conversation_id;

-- Every copy below is an inner join, and an inner join that misses drops the row without a word,
-- which is the failure this whole migration exists to end. The joins cannot miss by construction,
-- since every address and channel they look up was adopted above, but a construction argument is
-- not a check: the count has to come out equal, and a CHECK constraint is the one thing plain SQL
-- can fail on, so a mismatch aborts the statement and with it the transaction.
CREATE TEMP TABLE migration_check (lost INTEGER NOT NULL CHECK (lost = 0));
INSERT INTO migration_check
SELECT (SELECT COUNT(*) FROM conversation_policy)
     - (SELECT COUNT(*) FROM conversation_policy_new);

DROP TABLE conversation_policy;
ALTER TABLE conversation_policy_new RENAME TO conversation_policy;

-- The unique key leads with the conversation and the message id so that a lookup naming a message
-- without its revision, which is what a deletion or an edit does, is served from the same index.
CREATE TABLE inbound_queue_new (
    seq             INTEGER PRIMARY KEY AUTOINCREMENT,
    conversation_id INTEGER NOT NULL REFERENCES conversations (id) ON DELETE CASCADE,
    account_id      INTEGER NOT NULL REFERENCES accounts (id),
    message_id      TEXT NOT NULL,
    revision        INTEGER NOT NULL DEFAULT 0,
    payload         TEXT NOT NULL,
    received_at     TEXT NOT NULL,
    state           TEXT NOT NULL DEFAULT 'pending'
                    CHECK (state IN ('pending', 'in_flight', 'done', 'failed')),
    attempts        INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT,
    not_before      TEXT,
    completed_at    TEXT,
    recovered       INTEGER NOT NULL DEFAULT 0,
    UNIQUE (conversation_id, message_id, account_id, revision)
);

-- The legacy edit time is seconds where a Telegram edit wrote it and milliseconds everywhere else.
-- The two are eleven orders of magnitude apart, so the magnitude says which it is: anything under
-- 1e11 is seconds, since 1e11 milliseconds was 1973 and 1e11 seconds is the year 5138.
INSERT INTO inbound_queue_new
    (seq, conversation_id, account_id, message_id, revision, payload, received_at, state, attempts,
     last_error, not_before, completed_at, recovered)
SELECT q.seq, c.id, a.id,
       CASE WHEN instr(q.external_id, ':e') = 0 THEN q.external_id
            ELSE substr(q.external_id, 1, instr(q.external_id, ':e') - 1) END,
       CASE WHEN instr(q.external_id, ':e') = 0 THEN 0
            WHEN CAST(substr(q.external_id, instr(q.external_id, ':e') + 2) AS INTEGER)
                 < 100000000000
            THEN CAST(substr(q.external_id, instr(q.external_id, ':e') + 2) AS INTEGER) * 1000
            ELSE CAST(substr(q.external_id, instr(q.external_id, ':e') + 2) AS INTEGER) END,
       q.payload, q.received_at, q.state, q.attempts, q.last_error, q.not_before, q.completed_at,
       q.recovered
FROM inbound_queue q
JOIN conversations c ON c.address = q.conversation_id
JOIN accounts a ON a.channel = c.channel AND a.platform_id = '';

INSERT INTO migration_check
SELECT (SELECT COUNT(*) FROM inbound_queue) - (SELECT COUNT(*) FROM inbound_queue_new);

DROP TABLE inbound_queue;
ALTER TABLE inbound_queue_new RENAME TO inbound_queue;
CREATE INDEX idx_inbound_queue_state ON inbound_queue (state, seq);

-- The index is rebuilt from the new table below rather than carried across, so the triggers and
-- the external-content table go first; dropping `messages` would drop the triggers anyway.
DROP TRIGGER messages_fts_insert;
DROP TRIGGER messages_fts_delete;
DROP TABLE messages_fts;

-- No `attachments` column any more. The files a message brought are found from the registry by the
-- same key that identifies the message, so a handle can no longer outlive the row it names.
CREATE TABLE messages_new (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    conversation_id INTEGER NOT NULL REFERENCES conversations (id) ON DELETE CASCADE,
    account_id      INTEGER NOT NULL REFERENCES accounts (id),
    message_id      TEXT NOT NULL,
    revision        INTEGER NOT NULL DEFAULT 0,
    sender_id       TEXT,
    sender_name     TEXT NOT NULL,
    text            TEXT NOT NULL,
    notes           TEXT,
    addressed       INTEGER NOT NULL DEFAULT 0,
    seen            INTEGER NOT NULL DEFAULT 0,
    own             INTEGER NOT NULL DEFAULT 0,
    session_id      TEXT,
    deleted_at      TEXT,
    superseded_at   TEXT,
    timestamp       TEXT NOT NULL,
    UNIQUE (conversation_id, message_id, account_id, revision)
);

INSERT INTO messages_new
    (id, conversation_id, account_id, message_id, revision, sender_id, sender_name, text, notes,
     addressed, seen, own, session_id, deleted_at, superseded_at, timestamp)
SELECT m.id, c.id, a.id, m.message_id,
       CASE WHEN instr(m.external_id, ':e') = 0 THEN 0
            WHEN CAST(substr(m.external_id, instr(m.external_id, ':e') + 2) AS INTEGER)
                 < 100000000000
            THEN CAST(substr(m.external_id, instr(m.external_id, ':e') + 2) AS INTEGER) * 1000
            ELSE CAST(substr(m.external_id, instr(m.external_id, ':e') + 2) AS INTEGER) END,
       m.sender_id, m.sender_name, m.text, m.notes, m.addressed, m.seen, m.own, m.session_id,
       m.deleted_at, m.superseded_at, m.timestamp
FROM messages m
JOIN conversations c ON c.address = m.conversation_id
JOIN accounts a ON a.channel = c.channel AND a.platform_id = '';

INSERT INTO migration_check
SELECT (SELECT COUNT(*) FROM messages) - (SELECT COUNT(*) FROM messages_new);

DROP TABLE messages;
ALTER TABLE messages_new RENAME TO messages;
CREATE INDEX idx_messages_conversation ON messages (conversation_id, timestamp);
CREATE INDEX idx_messages_unseen ON messages (conversation_id, seen);
CREATE INDEX idx_messages_timestamp ON messages (timestamp);

CREATE VIRTUAL TABLE messages_fts USING fts5(
    text,
    content = 'messages',
    content_rowid = 'id',
    tokenize = 'unicode61 remove_diacritics 2'
);

INSERT INTO messages_fts (messages_fts) VALUES ('rebuild');

CREATE TRIGGER messages_fts_insert AFTER INSERT ON messages BEGIN
    INSERT INTO messages_fts (rowid, text) VALUES (new.id, new.text);
END;

CREATE TRIGGER messages_fts_delete AFTER DELETE ON messages BEGIN
    INSERT INTO messages_fts (messages_fts, rowid, text) VALUES ('delete', old.id, old.text);
END;

-- Keyed by the message it came with plus its position in that message, in place of the
-- `<conversation>:<external_id>:<index>` string. Not a foreign key to `messages`: files are
-- registered before the message is recorded, so the handles travel with the queued payload, and
-- with history switched off the message is never recorded at all while its files stay fetchable.
CREATE TABLE attachments_new (
    handle          INTEGER PRIMARY KEY AUTOINCREMENT,
    conversation_id INTEGER NOT NULL REFERENCES conversations (id) ON DELETE CASCADE,
    account_id      INTEGER NOT NULL REFERENCES accounts (id),
    message_id      TEXT NOT NULL,
    revision        INTEGER NOT NULL DEFAULT 0,
    position        INTEGER NOT NULL,
    kind            TEXT NOT NULL,
    file_ref        TEXT NOT NULL,
    thumb_ref       TEXT,
    file_name       TEXT,
    media_type      TEXT,
    bytes           INTEGER,
    path            TEXT,
    created_at      TEXT NOT NULL,
    UNIQUE (conversation_id, message_id, account_id, revision, position)
);

-- The old id is the address, the external id and the position joined by colons, and both the
-- address and the external id can contain colons themselves. The address is a column of its own,
-- so what follows it is `<external_id>:<position>`, and the position is the run of digits at the
-- end.
INSERT INTO attachments_new
    (handle, conversation_id, account_id, message_id, revision, position, kind, file_ref,
     thumb_ref, file_name, media_type, bytes, path, created_at)
SELECT o.handle, c.id, a.id,
       CASE WHEN instr(o.external_id, ':e') = 0 THEN o.external_id
            ELSE substr(o.external_id, 1, instr(o.external_id, ':e') - 1) END,
       CASE WHEN instr(o.external_id, ':e') = 0 THEN 0
            WHEN CAST(substr(o.external_id, instr(o.external_id, ':e') + 2) AS INTEGER)
                 < 100000000000
            THEN CAST(substr(o.external_id, instr(o.external_id, ':e') + 2) AS INTEGER) * 1000
            ELSE CAST(substr(o.external_id, instr(o.external_id, ':e') + 2) AS INTEGER) END,
       o.position, o.kind, o.file_ref, o.thumb_ref, o.file_name, o.media_type, o.bytes, o.path,
       o.created_at
FROM (
    SELECT *,
           substr(stripped, 1, length(stripped) - 1) AS external_id,
           CAST(substr(rest, length(stripped) + 1) AS INTEGER) AS position
    FROM (
        SELECT *, rtrim(rest, '0123456789') AS stripped
        FROM (
            SELECT *, substr(id, length(conversation_id) + 2) AS rest FROM attachments
        )
    )
) o
JOIN conversations c ON c.address = o.conversation_id
JOIN accounts a ON a.channel = c.channel AND a.platform_id = '';

INSERT INTO migration_check
SELECT (SELECT COUNT(*) FROM attachments) - (SELECT COUNT(*) FROM attachments_new);

DROP TABLE attachments;
ALTER TABLE attachments_new RENAME TO attachments;
CREATE INDEX idx_attachments_created ON attachments (created_at);

DROP TABLE migration_check;
