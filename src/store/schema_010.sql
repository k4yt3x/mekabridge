-- One inbox item per message: the hand-over to meka's inbox, which since meka 0.55 is where a
-- message waits for the agent.
--
-- Until now a claimed batch was handed to meka as a turn the bridge held open and watched to the
-- end, and the queue row's state was the whole record of what became of it. The inbox changes who
-- holds the message: meka answers the moment the item is durable on its side, reads it into a
-- running turn or opens one for it, retries a turn that fails before the model produced anything,
-- and reports the item's fate on the session feed. What the bridge keeps is the hand-over itself.
--
-- The queue row is that hand-over. meka batches for itself -- one turn reads every item waiting for
-- it and writes one block per item into a single user message -- so a second layer of batching here
-- would only duplicate what the server already does, at the cost of a table, a second state machine
-- and the invariants tying the two together.
--
-- The row outlives the request that posts it, so a retry after an ambiguous failure, or after a
-- restart on either side, posts the same key and the same bytes and is answered with the item it
-- already made rather than a second copy. What the item states once is kept here so it can be owed
-- back if the item never reaches the model: the dropped-message count, and, through
-- `messages.accounted_by`, the exact backlog rows it reported.

CREATE TABLE inbound_queue_new (
    seq             INTEGER PRIMARY KEY AUTOINCREMENT,
    conversation_id INTEGER NOT NULL REFERENCES conversations (id) ON DELETE CASCADE,
    account_id      INTEGER NOT NULL REFERENCES accounts (id),
    message_id      TEXT NOT NULL,
    revision        INTEGER NOT NULL DEFAULT 0,
    payload         TEXT NOT NULL,
    received_at     TEXT NOT NULL,
    -- `in_flight` is rendered and not yet accepted; `posted` is meka's, until the feed says what
    -- became of it.
    state           TEXT NOT NULL DEFAULT 'pending'
                    CHECK (state IN ('pending', 'in_flight', 'posted', 'done', 'failed')),
    -- The `Idempotency-Key` the item is posted under, minted once when the row is held.
    key             TEXT,
    -- The rendered item. Byte-identical on every post, since meka refuses the same key with other
    -- words.
    body            TEXT,
    -- The meka session the item is, or will be, posted to. meka scopes keys per session.
    session_id      TEXT,
    -- meka's id for the item, from the 202.
    item_id         TEXT,
    -- The dropped-message count this item reported, taken off the counter when the row is held and
    -- put back if the item never delivers.
    dropped         INTEGER NOT NULL DEFAULT 0,
    -- Hand-overs this row has been part of, bounded by `[bridge].max_offers`.
    attempts        INTEGER NOT NULL DEFAULT 0,
    -- Posts of the current item, which counts something else entirely: one offer can be posted many
    -- times over while meka is out of reach.
    posts           INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT,
    -- When the row may be claimed again, while it is pending, or posted again, while it is in
    -- flight. The two never overlap, so one column says both.
    not_before      TEXT,
    -- When the item was rendered, which is what `[bridge].hand_over_within` counts from.
    held_at         TEXT,
    -- When meka accepted it, which is what a reconciliation compares against: an item handed over
    -- before a connection opened may have had its outcome reported while nothing was listening.
    posted_at       TEXT,
    completed_at    TEXT,
    UNIQUE (conversation_id, message_id, account_id, revision)
);

-- Rows a 0.13 turn held when the daemon stopped go back to the queue. That turn ran to its end or
-- was cut off with the bridge none the wiser, and nothing here can say which, so they are offered
-- again as the previous release's startup offered them, rather than being lost.
--
-- The `recovered` column goes with them. Whether crash recovery put a row back used to be worth
-- saying to the agent, since the turn that held it may have answered. An item is with meka or it is
-- not, and the feed says which, so there is no longer an interrupted turn to be uncertain about.
INSERT INTO inbound_queue_new
    (seq, conversation_id, account_id, message_id, revision, payload, received_at, state, attempts,
     last_error, not_before, completed_at)
SELECT seq, conversation_id, account_id, message_id, revision, payload, received_at,
       CASE WHEN state = 'in_flight' THEN 'pending' ELSE state END,
       attempts, last_error, not_before, completed_at
FROM inbound_queue;

-- A rebuild that silently dropped rows would lose somebody's messages with nothing said, and a
-- CHECK constraint is the one thing plain SQL can fail on, so a mismatch aborts the statement and
-- with it the whole migration.
CREATE TEMP TABLE migration_check (lost INTEGER NOT NULL CHECK (lost = 0));
INSERT INTO migration_check
SELECT (SELECT COUNT(*) FROM inbound_queue) - (SELECT COUNT(*) FROM inbound_queue_new);
DROP TABLE migration_check;

DROP TABLE inbound_queue;
ALTER TABLE inbound_queue_new RENAME TO inbound_queue;
CREATE INDEX idx_inbound_queue_state ON inbound_queue (state, seq);
CREATE UNIQUE INDEX idx_inbound_queue_key ON inbound_queue (key) WHERE key IS NOT NULL;
CREATE INDEX idx_inbound_queue_item ON inbound_queue (item_id) WHERE item_id IS NOT NULL;

-- The hand-over that reported this message as a backlog it had not seen, which is how the backlog
-- is owed back exactly if that hand-over dies. A watermark cannot express it: once two hand-overs
-- can be outstanding at the same time, what one of them stated and the other did not is a set
-- rather than a range.
--
-- Added after the rebuild above rather than before it. With foreign keys on, `DROP TABLE` performs
-- an implicit delete first, which would fire this column's `ON DELETE` action on the way past.
ALTER TABLE messages
    ADD COLUMN accounted_by INTEGER REFERENCES inbound_queue (seq) ON DELETE SET NULL;
CREATE INDEX idx_messages_accounted ON messages (accounted_by) WHERE accounted_by IS NOT NULL;
