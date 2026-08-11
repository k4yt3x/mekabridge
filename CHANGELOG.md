# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/2.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `allow_all`, for a bot anyone may message, such as a public or customer-service bot.
- The agent can mute a noisy chat, for a while or indefinitely, and unmute it later.
- Muted chats are shown in the conversation list, and the agent is told what it missed on expiry.
- `mekabridge mute list|add|rm`, so an operator can lift a mute the agent set on itself.
- The agent can edit a message it sent, correcting itself in place instead of sending a follow-up.
- The agent can delete a message, its own anywhere or anyone's in a group it moderates.
- Group moderation: restrict, ban, unban, or kick a member, with an optional duration.
- The agent can promote and demote administrators, pin messages, and set a group's title.
- The agent can check its own rights in a group before acting, rather than finding out by failing.
- `admin_tools = false` hides the moderation tools from a bot that should never use them.

- A Security page in the docs, covering the trust model and what each setting does and does not do.

### Changed

- The agent can message any chat, including ones that have never messaged it first.
- A chat the agent messages first now joins the conversation list instead of going unrecorded.
- `doctor` warns when privacy mode is hiding group messages from the bot.
- The agent is told how a sender was admitted without being told what to conclude from it.

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
