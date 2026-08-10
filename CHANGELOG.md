# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/2.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Relay between one permanent meka session and messaging platforms, Telegram first.
- MCP server exposing `send_message`, `send_file`, `list_conversations`, and `get_conversation`.
- Streamable HTTP and stdio MCP transports, with optional bearer auth and health endpoints.
- Send tools annotated read-only, so an agent at meka's `read` level can still reply.
- Durable SQLite inbound queue that survives a crash and recovers in-flight batches on start.
- Batching, so messages arriving during a turn are delivered together in the next one.
- Per-turn envelope carrying channel, conversation id, sender, and reply context to the agent.
- Nonce-fenced user text in the envelope, so a message cannot forge a routing header.
- One-time session preamble naming the connected channels and bot identities.
- Markdown to Telegram HTML rendering with structure-aware splitting at the 4096 character limit.
- Telegram allowlists by user and by chat, required at startup so a bot is never open to anyone.
- Inbound attachments downloaded to disk and named by path in the envelope.
- Inbound images attached to the turn itself when meka's provider profile has vision enabled.
- A per-turn image budget, so a batch of photos cannot exceed meka's request size limit.
- Typing indicators in originating chats while a turn runs.
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
