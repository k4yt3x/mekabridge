# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/2.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Relay between one permanent meka session and messaging platforms, Telegram first.
- MCP server exposing six tools for sending, reacting, fetching files, and listing conversations.
- Streamable HTTP and stdio MCP transports, with optional bearer auth and health endpoints.
- Send tools annotated read-only, so an agent at meka's `read` level can still reply.
- Durable SQLite inbound queue that survives a crash and recovers in-flight batches on start.
- Batching, so messages arriving during a turn are delivered together in the next one.
- Per-turn envelope carrying channel, conversation id, message id, sender, and reply context.
- Message ids in the envelope, so the agent can reply to and react to one specific message.
- `react`, letting the agent acknowledge a message with an emoji instead of writing back.
- An admission line saying whether a sender is allowlisted individually or only via their chat.
- Forwarded-message origin, so text somebody relayed does not read as their own words.
- Album ids, so the parts of a multi-photo post read as one post.
- Bot senders and anonymous group admins marked as such rather than looking like ordinary people.
- Edited messages delivered again and marked as edits, instead of being dropped as duplicates.
- Inbound animations, video notes, locations, venues, contacts, polls, dice, and stories.
- A placeholder for any unrecognised message type, so nothing arrives and silently disappears.
- `view_attachment` and `download_attachment`, so the agent fetches only the files it needs.
- Still frames for video, animations, and animated stickers, so those are viewable without ffmpeg.
- A caveat sent with a still frame, so a preview is never mistaken for the whole file.
- `link_preview`, defaulting off, since agent replies cite links more often than they feature them.
- Logging when the bot is added to or removed from a chat, warning if the chat is not allowlisted.
- Nonce-fenced user text in the envelope, so a message cannot forge a routing header.
- One-time session preamble naming the connected channels and bot identities.
- Markdown to Telegram HTML rendering with structure-aware splitting at the 4096 character limit.
- Telegram allowlists by user and by chat, required at startup so a bot is never open to anyone.
- Typing indicators in originating chats, stopping once the agent replies there.
- A 30 second ceiling on the typing indicator, since a long turn is working rather than composing.
- Upload indicators while a file transfers, instead of sending it in silence.
- Sessions declaring `supports_permission_prompts: false`, so gated tools deny at once.
- A running session's permission reconciled with the config rather than fixed at creation.
- Retry of a turn that returns nothing and calls no tools, instead of leaving the sender in silence.
- A warning, with the text the agent produced, when a turn delivers no message.
- Recovery from a dropped turn stream by waiting on `turn_in_flight` rather than resubmitting.
- Recovery from a `turn-in-flight` rejection by waiting for the running turn to finish.
- Replacement of a session meka no longer knows, replaying the batch into it once.
- Operator commands: `doctor`, `status`, `queue`, `conversations`, `session`, and `cancel`.
- `config init` writing a commented starter config, and `config path` / `config validate`.
- Owner notifications when a batch cannot be delivered after its retries.
- Interop test running meka's rmcp 2.x client against this crate's 3.x server.
