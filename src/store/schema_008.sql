-- What the bridge itself said, and what has happened to a message since it was recorded.
--
-- Until now `messages` held only what other people said, so the agent could not answer "did I
-- already tell them that?" about its own account. It matters more the more sessions share one bot:
-- a scheduled session sends a message, and the session that gets asked about it later has no record
-- of it anywhere, because it was never its own transcript and the bridge kept nothing.
--
-- With `own` the table covers both directions, one row per real platform message, and the two
-- lifecycle columns carry what became of each. The current state of a conversation is every row
-- where `deleted_at` and `superseded_at` are both NULL; the marked rows are what that state used to
-- be.
--
-- `deleted_at` replaces deleting the row outright. Erasing was the earlier stance and it was
-- deliberate, but it left the agent no way to learn that something it had already acted on was
-- retracted, which is the failure worth preventing. Only platforms that report deletions can set it:
-- Discord does, and the Telegram Bot API never tells a bot that somebody deleted a message, so
-- Telegram rows are simply never marked and age out under retention as before.
--
-- `superseded_at` goes on the older row when an edit of the same `message_id` is recorded. An edit
-- already arrives as its own row, under its own `external_id`, and nothing used to connect the two,
-- so `read_history` returned the pre-edit and post-edit wordings as two messages that both looked
-- current.
--
-- No FTS trigger comes with this. `text` is still written once and never revised: an edit appends and
-- a deletion marks, so the reasoning at the end of schema_004.sql is unchanged, and none of these
-- four columns is in the index.
--
-- Every column is defaulted or nullable, so nothing needs backfilling. Rows written before this
-- migration read back as somebody else's message that is neither deleted nor superseded, which is
-- what they are.

ALTER TABLE messages ADD COLUMN own INTEGER NOT NULL DEFAULT 0;

-- The meka session that sent it, for an outbound row. The bridge owns one permanent session, but
-- scheduled and isolated ones speak on the same account, and without this the history says the bot
-- said something while nothing says which of them did.
ALTER TABLE messages ADD COLUMN session_id TEXT;

ALTER TABLE messages ADD COLUMN deleted_at TEXT;

ALTER TABLE messages ADD COLUMN superseded_at TEXT;
