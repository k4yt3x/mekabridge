-- What was said, as opposed to what is waiting to be delivered.
--
-- The queue is a work list: a row enters it because the agent is going to be woken for it, and
-- leaves once that has happened. This is the record of the conversation itself, and the two diverge
-- the moment a conversation is muted, because then most of what arrives is recorded and never
-- queued at all.
--
-- Everything not blocked is recorded, not only the muted conversations. A history that exists in
-- some chats and not others is a worse tool than one that behaves the same everywhere, and an agent
-- whose session has been compacted has as much use for scroll-back in a chat it was listening to as
-- in one it was not.
--
-- Retention is `[storage].history_retention`, and zero means nothing is written here at all.

CREATE TABLE messages (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    conversation_id TEXT NOT NULL,
    -- Same key the queue deduplicates on, so a redelivered message does not appear twice and an
    -- edit, which carries a distinct one, appears as the separate event it is.
    external_id     TEXT NOT NULL,
    -- Platform-native id, which is what a reply or a reaction targets.
    message_id      TEXT NOT NULL,
    sender_id       TEXT,
    sender_name     TEXT NOT NULL,
    text            TEXT NOT NULL,
    -- The descriptor lines a message with no text of its own carries: a location, a contact card, a
    -- poll. Without them a photo reads back as a blank line from somebody.
    notes           TEXT,
    -- Handles for the files this message brought, comma separated. Stored rather than joined from
    -- `attachments` so a search result can hand the agent something it can open directly.
    attachments     TEXT,
    -- Whether this message was addressed to the agent: a mention, a reply to it, or a direct chat.
    addressed       INTEGER NOT NULL DEFAULT 0,
    -- Whether the agent has been shown this, either as a message it was woken for or as the context
    -- rendered alongside one. Per message rather than a watermark on the conversation: a mention
    -- delivered out of a muted chat arrives long after the messages around it, so a single
    -- high-water mark would mark them all seen in the wrong order.
    seen            INTEGER NOT NULL DEFAULT 0,
    timestamp       TEXT NOT NULL,
    UNIQUE (conversation_id, external_id)
);

CREATE INDEX idx_messages_conversation ON messages (conversation_id, timestamp);
CREATE INDEX idx_messages_unseen ON messages (conversation_id, seen);
CREATE INDEX idx_messages_timestamp ON messages (timestamp);

-- External content, so the text is stored once in `messages` rather than duplicated here. That makes
-- the triggers below load bearing: without them the index and the table drift apart, and a search
-- starts returning rows that no longer exist.
CREATE VIRTUAL TABLE messages_fts USING fts5(
    text,
    content = 'messages',
    content_rowid = 'id',
    tokenize = 'unicode61 remove_diacritics 2'
);

CREATE TRIGGER messages_fts_insert AFTER INSERT ON messages BEGIN
    INSERT INTO messages_fts (rowid, text) VALUES (new.id, new.text);
END;

CREATE TRIGGER messages_fts_delete AFTER DELETE ON messages BEGIN
    INSERT INTO messages_fts (messages_fts, rowid, text) VALUES ('delete', old.id, old.text);
END;

-- There is deliberately no update trigger. `seen` is updated, but it is not indexed here, so the
-- pair above is enough; `text` is the column that would need one and it is written once and never
-- revised, because an edit arrives under its own `external_id` as a new row rather than rewriting
-- the old one. Anything that starts updating `text` has to add the third trigger, or the index will
-- go on matching the old wording and returning rows that no longer contain it.
