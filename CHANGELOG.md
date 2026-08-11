# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/2.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-08-11

### Added

- Relay between one permanent meka session and chat platforms, Telegram first.
- Replying, staying quiet, messaging someone else, and messaging first are all the agent's call.
- Six agent tools: send a message or a file, react, view or download an attachment, list chats.
- Streamable HTTP and stdio MCP transports, with optional bearer auth and health endpoints.
- Runs at meka's `read` permission level, so nothing the agent needs requires `write`.
- Durable queue, so nothing anyone sent is lost to a crash or a restart.
- Messages typed in a burst settle into one turn instead of being answered mid-thought.
- Messages that land mid-turn are flagged, so the agent can correct a premature reply.
- Every message carries who sent it, why they are allowed through, and what it replies to.
- Forwarded messages name their original author, so relayed text is not read as the sender's.
- Edits arrive as edits, and the parts of a multi-photo post arrive as one post.
- Photos, documents, voice, audio, video, GIFs, stickers, locations, contacts, and polls.
- Anything else arrives as a placeholder, so no message ever silently disappears.
- Attachments are fetched on demand rather than downloaded up front.
- Video, GIFs, and animated stickers are viewable as the still frame the platform already made.
- Markdown rendered into Telegram's formatting, with long messages split cleanly.
- Emoji reactions, for acknowledging a message without sending one.
- Typing indicators while a turn runs, and upload indicators while a file sends.
- Link previews off by default, since agent replies cite links more than they feature them.
- Allowlists by user and by chat, required at startup so a bot is never open to anyone.
- User text fenced in the envelope, so a message cannot forge a routing header.
- A warning when the bot is added to a chat that is not allowlisted and will be ignored there.
- Session permission kept in step with the config, rather than fixed when the session was made.
- Automatic recovery from dropped streams, forgotten sessions, and empty model responses.
- The owner is notified when a message cannot be delivered to the agent at all.
- Operator commands: `doctor`, `status`, `queue`, `conversations`, `session`, and `cancel`.
- `config init` writing a commented starter config, plus `config path` and `config validate`.

[Unreleased]: https://github.com/k4yt3x/mekabridge/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/k4yt3x/mekabridge/releases/tag/v0.1.0
