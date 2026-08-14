# Telegram

## Setup

Create a bot with [@BotFather](https://t.me/BotFather) and keep the token. Find your numeric user id with [@userinfobot](https://t.me/userinfobot).

```toml
[[channels.telegram]]
id = "telegram"
token = "${TELEGRAM_BOT_TOKEN}"
allowed_users = [123456789]
```

Message the bot once before expecting the agent to reach you. Telegram bots cannot open a conversation with a user who has never contacted them, so a conversation only becomes addressable after the first inbound message.

## The allowlist

A bot token is a public entry point: anyone who guesses or discovers the bot's name can message it. mekabridge refuses to start with an empty allowlist for that reason.

- `allowed_users` admits specific people **in their own chat with the bot**, and nowhere else. It says who may talk to the agent, not which rooms it listens in.
- `allowed_chats` admits a whole group or channel, so every member of it can talk to the agent. Group ids are negative, for example `-1001234567890`. **This is the only way to be heard in a group**, including for people already on `allowed_users`.
- `allow_all` admits everyone, for a public or customer-service bot. On Telegram a private chat id is the user's own id, so this opens direct messages too, not just groups.

Messages from outside the allowlist are dropped at debug level with no reply. Not replying is deliberate: an "unauthorized" response would confirm to a stranger that the bot is live.

Remember that everyone who is allowed shares one agent context and one memory. See [Security](./security.md) for what that means and what the `admitted` line does and does not promise.

## What the agent sees

Each inbound message arrives with a header. Everything in it is written by the bridge and cannot be forged from message text, which is fenced separately behind a per-turn random marker.

```
channel: telegram
conversation: telegram:-1001234567890
message: 4471
from: Bob (@bob, id 987654321)
admitted: chat allowlist (this room is allowed); sender not individually allowlisted
chat: group "Deploy Crew"
at: 2026-08-11T14:22:31+00:00
forwarded from: Alice (@alice, id 111222333)
album: 13294839284
in reply to a message from Mica (id 4468): "deploy finished"
attachment: photo, image/jpeg, 2.1 MiB [417]
```

Only `channel`, `conversation`, `message`, `from`, `admitted`, `chat`, and `at` always appear; the rest show up when they apply.

- **`woke you`** appears on every message from a chat that is not one-to-one, including ones nothing addressed, saying what pulled it in. It is absent in a direct chat, where every message is addressed to it anyway.

- **`message`** is that message's own id. It is what `reply_to` and `react` take. An edit reads `message: 4471 (edited, revised at ...)`.
- **`admitted`** carries two facts. First the grant that let the message through: `user allowlist` (a direct message from somebody you named), `chat allowlist` (the room is allowed), or `open channel` (nothing was checked). Then, separately, whether the sender's own account is on `allowed_users` at all. Since that list reaches direct messages only, somebody you named writing in an allowlisted group is admitted *by the group*, and the second clause is what still identifies them. The bridge reports both; what to make of them belongs in the agent's instructions.
- **`forwarded from`** means the text is somebody else's words, not the sender's. Worth weighing before acting on instructions inside it.
- **`album`** ties the parts of a multi-photo post together, so a batch of pictures does not read as several unrelated ones.
- **`attachment`** ends with a handle for the fetch tools. See [Attachments](#attachments).

A sender who is another bot is marked `[bot]`. An anonymous group admin, who posts as the chat rather than as an account, reads as `Deploy Crew (posted as the chat itself, no individual account)` rather than being dressed up as a person.

## Groups and forum topics

Group messages work the same as direct ones. Forum topics get their own conversation id with a third segment (`telegram:-1001234567890:77`), so the agent replies into the right topic rather than the group's General.

**Turn privacy mode off** (`/setprivacy` in @BotFather, then remove and re-add the bot to each group). With it on, Telegram delivers only commands, mentions, and replies to the bot, which sounds like the same thing the `mute` policy does but is not: privacy mode never delivers the rest at all, so nothing is recorded and `read_history` has nothing to show when a mention arrives halfway through a discussion. The policy withholds the turn and keeps the message. `mekabridge doctor` warns while privacy mode is on.

Being added to or removed from a group is logged. If the group is not allowlisted the log line is a warning, because from the outside that state looks identical to a broken bot.

## Editing and reactions

An edited message is delivered again, marked as an edit, rather than being mistaken for a repeat of the original. The agent sees the revised text and knows which message it revises.

In the other direction the agent can revise its own messages with `edit_message` and retract them with `delete_message`, which is how a person corrects a typo rather than sending a second message about it. Telegram declines to edit a message older than 48 hours.

The agent can react to any message with `react`. Reactions are its decision alone: the bridge never acknowledges anything on its own, because a reaction is content and deciding whether to respond at all belongs to the agent.

## Moderation

With `admin_tools` on (the default) and the bot made an administrator, the agent can moderate a group: restrict, ban, unban, or kick members, promote and demote administrators, pin messages, and set the title.

Telegram enforces all of it. Every call needs the matching admin right in that specific chat, and no bot can act on another administrator, so a group's owner cannot be evicted by their own bot. Rights are per chat, so the bot may moderate one group and not another; `member` with no `user_id` reports what it holds where it is.

Two Telegram behaviours worth knowing:

- A restriction or ban shorter than 30 seconds or longer than 366 days is treated as **permanent**. The bridge refuses those rather than passing them through, since the failure is otherwise silent.
- `unrestrict` restores the group's own default permissions, read back from the group, rather than granting everything. Somebody reinstated ends up with what everyone else has, not more.

Anonymous admins post as the chat and carry no user id, so they cannot be moderated this way.

## Listing who is in a chat

Telegram will not enumerate ordinary members. The Bot API has no method for it and none for searching them, and no permission or setting changes that, so `list_members` answers a narrower question than it is asked and says so:

- It returns the chat's **administrators**, with `coverage: administrators`.
- It carries `total`, the full headcount, which is the one thing Telegram will say about everybody.
- A `query` is refused, with an error explaining that Telegram has no member search rather than quietly returning nothing.

Telegram also reports **no presence of any kind**. There is no online status, no last-seen, and no last-online date anywhere in the Bot API, at any permission level, so `presence` is absent rather than `unknown` on every Telegram member. Recency is the only proxy: `get_conversation` and `read_history` carry timestamps, so "posted four minutes ago" is available where "is online" is not.

To reach an ordinary member, the agent needs their user id, which it gets from the `from:` line of something they said. `member` then works on them normally.

Discord differs here: it will list a whole server, given one intent. See [Discord](./discord.md#listing-who-is-in-a-server).

## What wakes the agent in a group

Groups default to `mute`, meaning the agent is woken only by a message addressed to it. Everything else said there is still received and recorded, so it can read the surrounding discussion when it needs to. See [`[bridge.default_policy]`](../configuration/config-file.md).

"Addressed to it" is Telegram's own notion, not a guess from the text. Four things count:

| Signal | Matched by |
|---|---|
| A `text_mention` entity, which carries a whole `User` | user id |
| A reply to a message the bot sent | user id |
| `via_bot`, from the bot's inline mode | user id |
| A `mention` entity (`@yourbot`) or `/command@yourbot` | username, case-insensitively |

Only spans Telegram itself marked as entities are read, so the bot's name appearing as ordinary words does not count. A bare `/command` does not either: in a group with several bots it is ambiguous, and Telegram's own privacy mode only forwards it on the strength of which bot spoke last.

Every message in a private chat is addressed to the agent, since there is nobody else it could be for. That is also why muting a private chat is refused, and why direct messages default to `active`.

The bot's username is read once at startup, so renaming it in @BotFather needs a restart before mentions are recognised again.

## Messages are not held back

Telegram has no way to tell a bot that somebody is typing: the Bot API lets a bot *send* a chat
action and never receive one. So a message here starts a turn as soon as it arrives, rather than
waiting to see whether more is coming. `[bridge].settle` does not apply.

The trade is that two messages a few seconds apart produce two turns, and the agent may answer the
first before reading the second. If the second lands while the agent is still working on the first, it
arrives flagged `late:`, so the agent knows its reply was written without it and can revise with
`edit_message`. The alternative was a fixed wait on every
message, which is a long time to sit still for somebody who only ever meant to send one. See
[Group attention](./group-attention.md).

Everything is still held for one second, which is not configurable and is not about typing: an
album arrives as one update per photo, and without that floor a post would reach the agent as a
photo followed by a separate turn carrying the rest.

## Muting and blocking a chat

`mute` turns a chat down to mentions only, and nothing else in that chat wakes the agent; `block` stops it reaching the agent at all and keeps nothing. Both can carry a duration, as can `unmute`, which is how the agent hears a room in full for a while without having to remember to quieten it again. See [Group attention](./group-attention.md). `mekabridge policy list` and `mekabridge policy clear` are the operator's way back if the agent rules on something it should not have.

## Formatting

The agent writes Markdown. mekabridge renders it into Telegram's HTML subset, which has no headings, lists, tables, or images. The mapping:

| Markdown | Telegram |
|----------|----------|
| `**bold**`, `*italic*`, `~~strike~~`, `` `code` `` | `<b>`, `<i>`, `<s>`, `<code>` |
| Fenced code | `<pre><code class="language-x">` |
| Headings | Bold on their own line |
| Lists | `• ` and `1. ` prefixes, indented when nested |
| Tables | A monospace block with aligned columns |
| Images | A link, labelled with the alt text |
| Block quotes | `<blockquote>` |

Raw HTML in agent output is escaped and shown as text rather than passed through, so a stray `<script>` cannot be injected and a stray `<` cannot make Telegram reject the whole message.

Messages longer than 4096 characters are split. Splitting happens on the parsed structure rather than on the rendered HTML string, so a chunk boundary never lands inside a tag and styling that spans the boundary is closed and reopened. A long bold passage arrives as several messages that are each individually valid.

If rendering ever misbehaves on a particular message, `parse_mode = "none"` sends the Markdown verbatim as plain text.

## Attachments

**Nothing is downloaded on arrival.** The envelope announces what came in and hands the agent a handle; the agent fetches only what it decides it needs, with `view_attachment` to look at a picture or `download_attachment` to get the file on disk.

```
attachment: photo, image/jpeg, 2.1 MiB [417]
attachment: document, "q3-report.pdf", application/pdf, 8.4 MiB [418]
```

Three reasons it works this way. A download inside the polling loop stalls every later message behind it, so one large file used to delay an entire conversation. Disk filled with files nobody asked for. And because the bridge owns one permanent session, an image attached to a turn stays in the agent's context for the life of that session, whether or not it ever mattered; now only the pictures it chose to look at do.

Photos, documents, voice notes, audio, video, video notes, animations, and stickers all arrive this way. Content with no file at all becomes a descriptor line instead, so a shared location, a contact card, a poll, or a dice roll is still something the agent can see and respond to:

```
note: location: 51.5074, -0.1278
note: contact card: Bob Smith, phone +15551234567
```

The invariant is that an allowlisted message always produces something. An unrecognised message type becomes a placeholder rather than disappearing, so a future Bot API addition degrades instead of going silently missing.

### Viewing video and GIFs

Telegram generates a still frame for every video, animation, and animated sticker. `view_attachment` resolves to that frame, so "show me" works for them with no transcoding and no ffmpeg dependency.

This matters more than it sounds. Telegram's cloud Bot API caps `getFile` at 20 MiB regardless of what `attachment_max_bytes` says, so a phone video often cannot be downloaded at all, and the thumbnail is the only part of it the bridge can retrieve. Only a local Bot API server lifts that ceiling.

### Sending

Outbound, the agent uses `send_file` with an absolute path, optionally `as_photo` for images it wants shown inline.

## Link previews

Telegram renders a preview card for the first link in a message. mekabridge disables this by default: the agent cites links as references far more often than it makes one the subject of a message, and a card on each part of a split reply is noise. Turn them back on per channel:

```toml
[[channels.telegram]]
link_preview = true
```

## Rate limits

Outbound calls go through teloxide's `Throttle` adaptor. Telegram allows roughly one message per second per chat and answers bursts with 429s carrying a `retry_after`; the adaptor paces requests so a multi-part reply does not lose its tail.

## Typing indicators

When a turn starts, the chats it came from show a typing indicator, refreshed every four seconds because Telegram clears it after about five.

It stops on whichever comes first:

- **The agent sends a message there.** Telegram already clears the status when a message from the bot arrives, so re-arming it afterwards would tell somebody who was just answered that a second message is coming.
- **Thirty seconds pass.** A turn still running after that is working through tool calls, not composing a sentence. Telegram's own guidance is to use the action when a reply will take a *noticeable* time to arrive, not as a general busy light, and no person types for ten minutes.

The restraint is deliberate. The indicator is a claim that a message is about to arrive, and the bridge cannot actually know that: the agent is free to read something and say nothing, which is a supported outcome. Showing "typing" for a whole turn that ends in silence is worse than showing nothing, because it is a promise the bridge was never in a position to make.

Sending a file declares the upload instead, so a large attachment shows "sending a photo" or "sending a file" rather than transferring in silence.

This is the one thing mekabridge emits without the agent asking. It is presence rather than content, so it does not compete with the agent's decision about whether to reply. Turn it off entirely with `[bridge].typing_indicator = false`.

### On a "thinking" indicator

There isn't one. `sendChatAction` accepts ten values, all naming a kind of message about to arrive, and none of them mean "reasoning".

Telegram did build something for this: Bot API 10.1 added **rich messages**, which stream AI-generated replies and include a dedicated thinking block. mekabridge cannot use it. teloxide supports Bot API 9.1, and reaching rich messages would mean hand-rolling those calls against the raw HTTP API.
