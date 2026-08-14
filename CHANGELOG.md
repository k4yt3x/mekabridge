# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/2.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `unseen`, reporting what the agent has not been shown without spending it. Tool and CLI alike.
- `mekabridge unseen` exits 0, 1, or 2, so a gate can tell a failure from a quiet room.
- `mekabridge unseen` prints a watcher marker on stdout that moves only when a chat does.
- `unmute` and `unblock` take a `duration`, for joining a discussion without having to leave it.
- A Group Attention page, covering what wakes the agent and what it can arrange for itself.

### Changed

- `woke you:` is stated on every message from a group, including the ones nothing addressed.
- A muted conversation's envelope block names the two things that do wake the agent there.
- A lapsed policy's notice now wakes the agent, instead of being filed where nothing reads it.

### Removed

- **Breaking:** `[bridge].mute_followup`; a config still setting it is refused by name at startup.
- The agent is no longer told to send a holding reply for a slow turn; the indicator now lasts.

### Fixed

- A list ran into the paragraphs around it, so prose mixed with bullets arrived as one block.
- A list written with blank lines between its items was packed as tight as one without.
- A second paragraph inside a list item was joined to its bullet, reading as a broken item.
- The agent was told the typing indicator lapses after 30 seconds, untrue since 0.5.0.
- The agent was told a reply to it would not wake a muted chat, when on both platforms it does.

## [0.6.0] - 2026-08-12

### Changed

- **Breaking:** `allowed_users` admits a person in a direct message only, not in groups or servers.
- Startup warns when a channel allowlists people but no rooms, so it is reachable by DM alone.
- The `admitted:` line reports the grant and whether the sender is on `allowed_users` separately.

### Fixed

- A deferred batch lost what a muted chat had missed, and the retry then reported it as silent.
- `typing_max` never applied while a batch waited on a turn meka started itself.
- `online_only` emptied the list on a platform reporting no presence, reading as nobody around.
- The `presence` warning told operators to enable Server Members, which the bridge never asks for.
- `presence` was missing from the config reference and the template `config init` writes.

## [0.5.0] - 2026-08-12

### Added

- `list_members`, reporting who is in a chat, with `coverage` saying how much of it is covered.
- Discord member search, which needs no privileged intent, and full rosters where the intent is on.
- Discord presence behind `presence` on the channel, reporting who is online, idle, or busy.
- `online_only` on `list_members`, for finding who is actually around to be given work.
- `[bridge].typing_max`, a ceiling on the typing indicator, following the turn budget by default.

### Changed

- The typing indicator now lasts as long as the turn, rather than lapsing after 30 seconds.
- The indicator opens once meka accepts the turn, not when the bridge tries to submit one.
- A chat waiting on a turn meka started itself now sees the indicator instead of silence.
- A busy session is retried on a timer, and logged once per wait rather than once per attempt.

### Fixed

- A batch meka refused was resubmitted hundreds of times a second until the other turn finished.
- A cancelled typing indicator still went out, leaving chats typing minutes after the agent stopped.
- A deferred batch flagged the next batch's own messages as having arrived mid-turn.
- The queue-overflow notice was lost when a deferred batch's envelope was discarded.
- A zero `[meka].turn_timeout` was accepted, so every turn timed out before it could start.
- The documented moderation tool group omitted `set_member_roles`.

## [0.4.1] - 2026-08-12

### Fixed

- A message could be declared undeliverable when meka was busy with a turn of its own.

## [0.4.0] - 2026-08-12

### Added

- Discord, as a second platform: servers, channels, threads, forum posts, and direct messages.
- Server channels default to mentions only; `@everyone` and role pings never wake the agent.
- `[[channels.discord]]`, with allowlists by user, role, channel, and whole server.
- `set_member_roles`, since Discord grants privileges through roles rather than to a person.
- `slowmode` on `set_chat`, for quieting a room rather than a person. Discord only.
- `discord:@<user id>` reaches somebody who has never written to the bot first.
- `search_history` also asks Discord, reaching messages from before the bot joined the server.
- A message deleted on Discord is dropped from the bridge's history, so it is never replayed.
- The envelope gains the sender's roles, why the agent was woken, and two weaker `admitted:` values.
- Ids in Discord message text are resolved to names before the agent reads them.
- A refused Discord intent is reported in plain English instead of a silent reconnect loop.

### Changed

- `member` reports roles and when a restriction lifts, which improves the Telegram answer too.
- `[bridge].default_policy` now covers Discord: server channels are groups, announcements channels.
- Only the moderation tools a configured platform can honour are offered to the agent.
- The minimum supported Rust version is 1.89, which is twilight's floor.

### Fixed

- The daemon stayed up after its last channel died, looking healthy while hearing nothing.
- A heading containing bold lost its formatting for the rest of the line on Telegram.
- The docs said pinning needs `MANAGE_MESSAGES`; Discord now requires the separate `PIN_MESSAGES`.

### Security

- A newline in a display name or nickname could forge an envelope header line; now flattened.

## [0.3.0] - 2026-08-12

### Added

- `mute`, which turns a chat down to mentions only instead of silencing it: the rest is recorded.
- The agent is woken in a muted chat by a mention, a reply to it, or its own recent message there.
- A record of every message from every chat that is not blocked, kept for `history_retention`.
- `read_history` and `search_history`, so the agent can catch up on what it was not woken for.
- A mention in a muted chat arrives with the count of what was missed and the last few messages.
- `mekabridge policy list|set|clear` and `mekabridge history`, the operator's side of both.
- `[bridge].mute_followup`, so answering a mention does not need a second one to carry on.
- `[bridge].mute_context`, the lookback printed alongside a mention. `0` makes the agent ask.
- `doctor` reports the attention defaults, how many chats override them, and what history holds.

### Changed

- **Breaking:** groups and channels default to mentions only; direct messages are unchanged.
- **Breaking:** set `[bridge.default_policy]` to `active` per chat kind to restore 0.2.x.
- **Breaking:** the old `mute` tool is now `block`, which is what it always did.
- **Breaking:** existing mutes migrate to blocks, so a chat already silenced stays silenced.
- **Breaking:** `mekabridge mute list|add|rm` is now `mekabridge policy list|set|clear`.
- `list_conversations` reports `policy`, `policy_until`, and `unseen` instead of `muted_until`.
- A shed message is no longer called lost, since `read_history` still reaches it.
- Telegram privacy mode should now be off; `doctor` explains why it defeats the history.
- The policy behind each message is cached briefly, so a busy chat no longer costs a query each.

## [0.2.1] - 2026-08-11

### Fixed

- Telegram polls no longer abort after 17s, which logged a network error on every quiet poll.

## [0.2.0] - 2026-08-11

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
- A Security page in the docs, covering the trust model and what each setting does and does not do.

### Changed

- **Breaking:** moderation tools are offered by default; set `admin_tools = false` to withhold them.
- The agent can message any chat, including ones that have never messaged it first.
- A chat the agent messages first now joins the conversation list instead of going unrecorded.
- `doctor` warns when privacy mode is hiding group messages from the bot.
- The agent is told how a sender was admitted without being told what to conclude from it.

### Fixed

- `doctor` and the docs described images as riding the turn, which they have not since 0.1.0.
- Editing a message to empty text blamed length instead of saying it renders to nothing.
- `mekabridge mute add` accepted a malformed id, silencing nothing while reporting success.

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

[Unreleased]: https://github.com/k4yt3x/mekabridge/compare/0.6.0...HEAD
[0.6.0]: https://github.com/k4yt3x/mekabridge/compare/0.5.0...0.6.0
[0.5.0]: https://github.com/k4yt3x/mekabridge/compare/0.4.1...0.5.0
[0.4.1]: https://github.com/k4yt3x/mekabridge/compare/0.4.0...0.4.1
[0.4.0]: https://github.com/k4yt3x/mekabridge/compare/0.3.0...0.4.0
[0.3.0]: https://github.com/k4yt3x/mekabridge/compare/0.2.1...0.3.0
[0.2.1]: https://github.com/k4yt3x/mekabridge/compare/0.2.0...0.2.1
[0.2.0]: https://github.com/k4yt3x/mekabridge/compare/0.1.0...0.2.0
[0.1.0]: https://github.com/k4yt3x/mekabridge/releases/tag/0.1.0
