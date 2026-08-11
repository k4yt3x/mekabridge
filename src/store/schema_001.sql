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

-- Registry of the files a platform holds for the messages this bridge has seen.
--
-- Nothing is downloaded on arrival. The bridge records what the platform has and hands the agent a
-- handle; bytes move only if the agent asks for them, so `path` is the record of a download that has
-- already happened rather than a requirement.
CREATE TABLE attachments (
    -- Short id the agent quotes to fetch this file. AUTOINCREMENT for the same reason
    -- `inbound_queue.seq` uses it: the janitor prunes rows, and a reused handle would silently point
    -- a later fetch at a different file.
    handle          INTEGER PRIMARY KEY AUTOINCREMENT,
    -- `<conversation>:<external_id>:<index>`, stable across a redelivery so a replayed message
    -- reuses the handle already issued instead of minting a second one.
    id              TEXT NOT NULL UNIQUE,
    conversation_id TEXT NOT NULL,
    channel_id      TEXT NOT NULL,
    kind            TEXT NOT NULL,
    -- Platform-native reference used to fetch the file.
    file_ref        TEXT NOT NULL,
    -- Still frame, for media whose primary file is not a viewable image.
    thumb_ref       TEXT,
    file_name       TEXT,
    media_type      TEXT,
    bytes           INTEGER,
    -- Set once the agent has downloaded the file, which is also what the retention sweep unlinks.
    path            TEXT,
    created_at      TEXT NOT NULL
);

CREATE INDEX idx_attachments_created ON attachments (created_at);
CREATE INDEX idx_attachments_conversation ON attachments (conversation_id);
