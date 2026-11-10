-- Reactions to messages, one row per reactor, emoji and kind of reaction, as the platforms report
-- them.
--
-- Keyed by the message the way `attachments` is, by conversation, account and platform id, rather
-- than a foreign key to `messages`: a reaction to a message the bridge never recorded, said before
-- it was there or since taken by retention, is still a fact about that message. No revision,
-- because a reaction is made to the message and every wording of it shares one set.
--
-- Whether a reaction is the agent's own is not stored. It is the account's own platform id in
-- `reactor_id`, which the join to `accounts` settles on read.
CREATE TABLE reactions (
    conversation_id  INTEGER NOT NULL REFERENCES conversations (id) ON DELETE CASCADE,
    account_id       INTEGER NOT NULL REFERENCES accounts (id),
    message_id       TEXT NOT NULL,
    -- The platform's id for whoever reacted, or for the chat they reacted as.
    reactor_id       TEXT NOT NULL,
    -- NULL where the platform did not name them, as Discord does not in a direct message.
    reactor_name     TEXT,
    reactor_username TEXT,
    reactor_bot      INTEGER NOT NULL DEFAULT 0,
    reactor_as_chat  INTEGER NOT NULL DEFAULT 0,
    -- What identifies the emoji: the character itself, or a custom emoji's platform id, which
    -- outlives the name a server gives it.
    emoji            TEXT NOT NULL,
    -- How a person sees the emoji, as it was named when the reaction was made.
    emoji_name       TEXT NOT NULL,
    -- A Discord super reaction, which somebody can add beside an ordinary one of the same emoji
    -- and take back separately.
    super_reaction   INTEGER NOT NULL DEFAULT 0,
    reacted_at       TEXT NOT NULL,
    PRIMARY KEY (conversation_id, account_id, message_id, reactor_id, emoji, super_reaction)
);

CREATE INDEX idx_reactions_reacted ON reactions (reacted_at);

-- How many reactions of each emoji a message has, from a platform that counts them without saying
-- who made them: a Telegram channel, whose reactions are anonymous. Replaced whole on every count.
CREATE TABLE reaction_counts (
    conversation_id INTEGER NOT NULL REFERENCES conversations (id) ON DELETE CASCADE,
    account_id      INTEGER NOT NULL REFERENCES accounts (id),
    message_id      TEXT NOT NULL,
    emoji           TEXT NOT NULL,
    emoji_name      TEXT NOT NULL,
    count           INTEGER NOT NULL,
    counted_at      TEXT NOT NULL,
    PRIMARY KEY (conversation_id, account_id, message_id, emoji)
);

CREATE INDEX idx_reaction_counts_counted ON reaction_counts (counted_at);
