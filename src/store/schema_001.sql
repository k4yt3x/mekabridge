-- Initial mekabridge schema.
--
-- Applied by src/store.rs when `user_version` is 0. Never edit this file after release; add a new
-- migration instead, since existing databases only run the statements they have not seen.

CREATE TABLE meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE conversations (
    id               TEXT PRIMARY KEY,
    channel_id       TEXT NOT NULL,
    platform         TEXT NOT NULL,
    chat             TEXT NOT NULL,
    thread           TEXT,
    title            TEXT,
    kind             TEXT NOT NULL,
    created_at       TEXT NOT NULL,
    last_inbound_at  TEXT,
    last_outbound_at TEXT
);

CREATE INDEX idx_conversations_channel ON conversations (channel_id);

-- `seq` is the delivery order the agent sees, so it must be monotonic across restarts. AUTOINCREMENT
-- (rather than a plain INTEGER PRIMARY KEY) prevents SQLite from reusing the rowid of a deleted row,
-- which would otherwise let a pruned message's number reappear ahead of newer traffic.
CREATE TABLE inbound_queue (
    seq             INTEGER PRIMARY KEY AUTOINCREMENT,
    conversation_id TEXT NOT NULL,
    external_id     TEXT NOT NULL,
    payload         TEXT NOT NULL,
    received_at     TEXT NOT NULL,
    state           TEXT NOT NULL DEFAULT 'pending'
                    CHECK (state IN ('pending', 'in_flight', 'done', 'failed')),
    attempts        INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT,
    UNIQUE (conversation_id, external_id)
);

CREATE INDEX idx_inbound_queue_state ON inbound_queue (state, seq);

CREATE TABLE attachments (
    id              TEXT PRIMARY KEY,
    conversation_id TEXT NOT NULL,
    path            TEXT NOT NULL,
    media_type      TEXT,
    bytes           INTEGER,
    created_at      TEXT NOT NULL
);

CREATE INDEX idx_attachments_created ON attachments (created_at);
