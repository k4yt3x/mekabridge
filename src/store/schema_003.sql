-- Attention policy per conversation, replacing the mute table of 0.2.0.
--
-- A policy is the agent's own decision about where its attention goes, not an access control: an
-- allowlist says who may speak to it at all, this says how much of what they say is worth being
-- woken for. Three states, named the way both Telegram and Discord name them in their own
-- notification settings:
--
--   active  every message wakes the agent
--   mute    messages are recorded, but only one addressed to the agent wakes it
--   block   nothing gets through and nothing is kept
--
-- The table only holds explicit decisions. A conversation with no row follows `[bridge].default_policy`
-- for its chat kind, which is why nothing needs backfilling here and why changing that setting moves
-- every conversation nobody has ruled on.
--
-- `mode` defaults to 'block' precisely so this migration needs no backfill: every row written before
-- 0.3.0 came from the old `mute` tool, which dropped messages outright, and that is now called block.

ALTER TABLE mutes RENAME TO conversation_policy;

ALTER TABLE conversation_policy ADD COLUMN mode TEXT NOT NULL DEFAULT 'block'
    CHECK (mode IN ('active', 'mute', 'block'));
