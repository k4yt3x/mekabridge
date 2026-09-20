-- A watch becomes a named rule holding many patterns.
--
-- 0.16.0 gave a watch one pattern, which made a rule set of a hundred spam signatures a hundred
-- watches: a hundred calls to install, a hundred rows to read back, and the same `reason` written
-- a hundred times. That last repetition is the tell. A rule set is one standing reason to wake
-- with many spellings, and the schema was making the agent say it was a hundred reasons.
--
-- Matching never had the problem the shape suggested: every pattern of a field compiles into one
-- `RegexSet` and costs one pass whatever the count. So this is about what the agent and the
-- operator have to say and read, not about speed.
--
-- `name` is the identity from here on, chosen by whoever writes the rule. Syncing a rules file
-- kept elsewhere used to mean a second watch and an orphaned first, because identity was the
-- pattern itself and an edited pattern is a different watch. Naming the rule makes the write an
-- upsert: the call says what the watch should be, and the row ends up equal to it.
--
-- `mode` is `any` or `all`. `all` exists because "these terms, in any order" is a rule people
-- genuinely write, and the obvious spelling for it -- lookahead, `(?=.*A)(?=.*B)` -- is exactly
-- what Rust's regex refuses, that refusal being what buys the linear-time guarantee the gate
-- relies on. Saying it in the rule instead of in the pattern keeps both.

CREATE TABLE watches_new (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    -- Unique across the bridge rather than per conversation: the agent refers to a watch by name
    -- in a tool call that need not name a chat, so two watches sharing one would be ambiguous
    -- exactly where it matters.
    name            TEXT NOT NULL UNIQUE,
    conversation_id INTEGER REFERENCES conversations (id) ON DELETE CASCADE,
    field           TEXT NOT NULL DEFAULT 'text'
                    CHECK (field IN ('text', 'sender', 'sender_id')),
    mode            TEXT NOT NULL DEFAULT 'any' CHECK (mode IN ('any', 'all')),
    reason          TEXT,
    -- When it lapses, or NULL for indefinite. A lapsed row is deleted on the next read rather than
    -- swept on a timer, which is how `conversation_policy` handles the same question.
    until           TEXT,
    created_at      TEXT NOT NULL
);

-- Every 0.16.0 watch is kept and named after the id it already had. A generated name is a poor
-- name, but it is unique by construction and it is recoverable: the agent or the operator can
-- write the rule again under a better one. Dropping the rows instead would have cost somebody a
-- rule set over a release that was a week old.
INSERT INTO watches_new (id, name, conversation_id, field, mode, reason, until, created_at)
SELECT id, 'watch-' || id, conversation_id, field, 'any', reason, until, created_at
FROM watches;

-- The patterns have to outlive the table they came from, and `DROP TABLE` below takes them with
-- it. A temp table is the only place to put them that the drop cannot reach.
CREATE TEMP TABLE watch_pattern_carry AS SELECT id, pattern FROM watches;

-- A rebuild that silently dropped rows would lose somebody's rules with nothing said, and a CHECK
-- constraint is the one thing plain SQL can fail on, so a mismatch aborts the statement and with
-- it the whole migration.
CREATE TEMP TABLE migration_check (lost INTEGER NOT NULL CHECK (lost = 0));
INSERT INTO migration_check
SELECT (SELECT COUNT(*) FROM watches) - (SELECT COUNT(*) FROM watches_new);

DROP TABLE watches;
ALTER TABLE watches_new RENAME TO watches;
CREATE INDEX idx_watches_conversation ON watches (conversation_id);

-- Created after the rename so its foreign key is written against the name the table will keep,
-- rather than relying on `ALTER TABLE ... RENAME` to rewrite a reference to `watches_new`.
--
-- `position` preserves the order the patterns were written in, which is the order the wake line
-- reports a hit from and the order they read back in. The same idiom `attachments` uses to keep an
-- album's photos in the order they were posted.
CREATE TABLE watch_patterns (
    watch_id INTEGER NOT NULL REFERENCES watches (id) ON DELETE CASCADE,
    position INTEGER NOT NULL,
    pattern  TEXT NOT NULL,
    PRIMARY KEY (watch_id, position)
);

INSERT INTO watch_patterns (watch_id, position, pattern)
SELECT id, 0, pattern FROM watch_pattern_carry;

INSERT INTO migration_check
SELECT (SELECT COUNT(*) FROM watch_pattern_carry) - (SELECT COUNT(*) FROM watch_patterns);

DROP TABLE watch_pattern_carry;
DROP TABLE migration_check;
