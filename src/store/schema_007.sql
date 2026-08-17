-- Whether this row was returned to the queue by crash recovery rather than by a failed turn.
--
-- The two are not the same and the difference decides what the agent should be told. A failed turn
-- is a turn the bridge watched: it knows whether the agent acted, and refuses to replay a batch that
-- may already have been answered. A row stranded by `kill -9` was being processed by a turn nobody
-- watched to the end, so the same question has no answer, and replaying it silently presents work
-- that may already be done as though it were new.
ALTER TABLE inbound_queue ADD COLUMN recovered INTEGER NOT NULL DEFAULT 0;
