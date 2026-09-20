-- Watches: a third reason a muted conversation wakes the agent, beside a mention and a reply.
--
-- Until now the gate had exactly two ways to decide a muted chat was worth a turn, both of them
-- facts the platform reports about a message. A watch is the agent's own standing reason, which is
-- what a person setting a keyword notification in their client is doing. It lives here rather than
-- in a config file because the agent writes it at runtime and an operator has to be able to undo
-- one, exactly as for `conversation_policy`.
--
-- `conversation_id` is nullable, and NULL means every conversation. That is the difference between
-- "tell me when this room mentions a deploy" and "tell me whenever anyone anywhere does", and a
-- watch worth setting is as often the second.
--
-- `field` names what the pattern reads. One watch inspects one field: the wake line the agent is
-- shown says which matched, and a rule that could have fired on any of three would leave it
-- guessing. The CHECK is what keeps a hand-edited value out of the column, since the matcher has to
-- map it to a string on the message and cannot invent a meaning for a fourth.
--
-- No UNIQUE over (conversation_id, field, pattern). SQLite treats NULLs as distinct, so a unique
-- index would constrain the scoped rows and quietly let duplicates of the global ones through,
-- which is the wrong half. `Store::add_watch` does the check in its transaction instead.
CREATE TABLE watches (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    conversation_id INTEGER REFERENCES conversations (id) ON DELETE CASCADE,
    field           TEXT NOT NULL DEFAULT 'text'
                    CHECK (field IN ('text', 'sender', 'sender_id')),
    pattern         TEXT NOT NULL,
    reason          TEXT,
    -- When it lapses, or NULL for indefinite. A lapsed row is deleted on the next read rather than
    -- swept on a timer, which is how `conversation_policy` handles the same question.
    until           TEXT,
    created_at      TEXT NOT NULL
);

CREATE INDEX idx_watches_conversation ON watches (conversation_id);
