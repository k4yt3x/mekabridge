-- Conversations the agent has chosen to stop hearing from.
--
-- A mute is the agent's own decision about where its attention goes, not an access control: an
-- allowlist says who may speak to it at all, this says what it is currently willing to be woken for.
-- Enforced in the inbound writer, before anything is queued, so a muted chat costs no queue depth
-- and no provider turn.

CREATE TABLE mutes (
    conversation_id TEXT PRIMARY KEY,
    -- When the mute lapses. NULL is indefinite, which only `unmute` or the operator clears.
    -- Evaluated lazily against the current time on each inbound message, so there is no timer to
    -- restore after a restart and an expiry that passed while the process was down is simply late.
    until           TEXT,
    -- What the agent said it was for, echoed back when the mute is listed.
    reason          TEXT,
    -- Messages discarded since the mute was set. Reported to the agent when the mute lapses,
    -- because a mute with no visible effect gives it nothing to decide whether to renew on.
    dropped         INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT NOT NULL
);
