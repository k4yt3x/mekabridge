# Discord

## Setup

Create an application at the [Developer Portal](https://discord.com/developers/applications), add a bot to it, and keep the token. Turn Developer Mode on in the Discord client (User Settings, Advanced) so you can right-click anything and copy its id.

```toml
[[channels.discord]]
id = "discord"
token = "${DISCORD_BOT_TOKEN}"
allowed_users = ["245119312739729408"]
```

Then enable the **Message Content** intent on the Bot page, under Privileged Gateway Intents. mekabridge asks for it by default, and a bot that asks for an intent it has not been granted does not degrade: Discord closes the gateway with a `4014` at startup. Set `message_content = false` if you would rather run without it, and read [Without the message content intent](#without-the-message-content-intent) first.

Invite the bot with the `bot` scope and, at minimum, View Channels, Send Messages, Read Message History, and Add Reactions. Add the moderation permissions only if you want the agent moderating: Timeout Members for `restrict`, Kick Members, Ban Members, Manage Roles, Pin Messages, and Manage Channels for slowmode.

Permissions are only half of it. Discord also refuses any moderation against somebody whose highest role sits at or above the bot's, and will not let the bot grant a role at or above its own, so the bot's role has to be dragged above the people it is expected to moderate. The server owner and anybody holding Administrator cannot be moderated at all, whatever the bot holds.

## Ids

Everything that holds messages in Discord is a channel with its own snowflake: a server text channel, a thread, a forum post, the text chat inside a voice channel, and a direct message are all channels. So one form covers all of them.

```
discord:1183429847290374144
```

A thread is a conversation in its own right, which is also the right answer for attention: it is a separate room, so muting the channel it hangs off does not silence it and vice versa.

The one gap is messaging somebody who has never written. A Discord user id is not a channel id, so `discord:@<user id>` is accepted as a dialling address:

```
discord:@245119312739729408
```

The bridge opens the direct-message channel, sends, and records the conversation under the real channel id, which is also the id the reply arrives under. The two converge after the first message. Discord will refuse if you share no server with the person.

## The allowlist

A bot token is a public entry point, and Discord's reach is wider than Telegram's: **anybody who shares a server with the bot can open a direct message with it.** In one busy server that is thousands of people. mekabridge refuses to start with an empty allowlist for that reason, and `allowed_users` gates direct messages as well as anything else.

Four grants, checked from the narrowest to the widest:

- `allowed_users` admits specific people **in a direct message**, and nowhere else. Since anyone sharing a server with the bot can open a DM, this is the list that decides who may do so; it grants nothing inside a server.
- `allowed_roles` admits anyone holding one of these roles. Cheap and idiomatic: the roles ride along on every server message, so no lookup is needed. It reads as `role allowlist` rather than `user allowlist`, because anybody who can hand out the role can hand out access with it.
- `allowed_channels` admits everyone in these channels. A thread inherits the standing of the channel it was started in.
- `allowed_guilds` admits **every member of the server**. This is much the largest grant, and `mekabridge doctor` and startup both say so.
- `allow_all` admits everyone, for a public bot.

Messages from outside the allowlist are dropped at debug level with no reply, so a stranger learns nothing about whether the bot is live.

Everyone who is allowed shares one agent context and one memory. See [Security](./security.md).

## Attention

Server channels default to **mentions only**, and direct messages are heard in full. That is the shipped default for every platform, and it fits Discord particularly well: a bot in a busy server would otherwise spend a provider turn on every line of chat.

What wakes the agent in a muted channel:

- Being **@mentioned by name**.
- A **reply** to one of its messages. Discord adds the bot to the message's mention list when the reply keeps its ping, and the bridge also treats a reply with the ping turned off as addressing it.

What deliberately does not: `@everyone`, `@here`, and role pings. Those are broadcasts rather than address, and counting them would make one `@everyone` in a large server the cheapest possible way for anybody to force a turn.

Nor does an ordinary message, however soon after the agent last spoke there. Until 0.7.0 a five-minute window followed the agent's own message and delivered everything said in the channel meanwhile, which in a busy channel meant delivering the channel.

Everything withheld is still recorded, and the mention that wakes the agent arrives with a count of what it missed and the last few messages. Use `mekabridge policy set discord:<id> active` to hear a channel in full, or `block` to record nothing at all. The agent can follow a channel on past a mention itself; see [Group attention](./group-attention.md).

## What the agent sees

```
channel: discord
conversation: discord:1183429847290374144
message: 1287733441209827329
from: Ali (@alice_dev, id 245119312739729408)
roles: Moderators, Release Team
admitted: server allowlist (the server this was said in is allowed); sender not individually allowlisted
chat: group "#deploys in Acme Corp"
at: 2026-08-12T14:03:11.427+00:00
woke you: you were named, or this replies to something you said
in reply to a message from Ali (id 1287733441209827300): "rollback is done"
attachment: photo, "crash.png", image/png, 412.3 KiB [418]
text (verbatim, fenced by a19f4c):
<<<a19f4c
@mekabot can you check #incidents, Release Team should see this
a19f4c>>>
```

- **`from`** uses the sender's nickname in that server when they have one, falling back to their display name and then their handle.
- **`roles`** is what they hold in this server, which is the difference between a stranger and a moderator. Discord supplies it on every message, so it costs nothing.
- **`chat`** names the channel and the server. A thread reads `#deploys › rollback tonight in Acme Corp`, so the agent can tell a tangent from the room it came out of.
- **`woke you`** appears on every message from a channel rather than a direct message, including ones nothing addressed, so it says what pulled it into a chat it is otherwise half listening to. It is hedged when the message both names the agent and replies to somebody, because the bridge is told one bit and will not invent the rest.
- **`admitted`** gains two grants on Discord. `role allowlist` means the sender holds a role you allowed, so an operator granted access deliberately but nobody looked at the account. `server allowlist` means neither the person nor the channel was allowlisted, only the server they are both in. The clause after the semicolon is a separate fact: whether the sender is on `allowed_users`, which on Discord admits direct messages only and so never appears as the grant here.

### Mentions are resolved to names

Discord sends raw content full of ids: `<@123>`, `<@&456>`, `<#789>`, `<:shrug:111>`, `<t:1712345678:R>`. Unresolved, the agent reads opaque numbers, so the bridge rewrites them into `@Alice`, `@Moderators`, `#general`, `:shrug:`, and an absolute UTC time.

This does edit what the sender literally typed, inside text the envelope otherwise fences as verbatim. It is the one place the bridge changes a message body, and the alternative is an agent that cannot tell who was named.

The reverse is deliberately not done. The agent writing `@Alice` does not become a mention, because guessing wrong pings a stranger. Writing `<@245119312739729408>` literally does work: `<` is the one Markdown-significant character left unescaped, precisely so a deliberate mention survives. Whether it actually notifies is then decided by `mention_roles` and `mention_everyone`.

## Sending

Replies are Markdown, rendered into Discord's dialect: bold, italic, strikethrough, inline and fenced code, quotes, and headings down to three levels. Spoilers have no Markdown source form, so the agent cannot write one. Tables become a code block, since Discord has none and monospace at least keeps the columns lined up. Text that was not meant as markup is escaped, so `send_message_now` does not come out italicised.

The limit is 2000 characters against Telegram's 4096, so long replies split about twice as often. Discord counts what is sent rather than what it renders as, so escapes and code fences are charged to the same allowance; the splitter measures the emitted message, not the text under it, because a message over the limit is refused outright rather than trimmed.

Every outgoing message carries an explicit mention policy. User mentions the agent writes do notify; `@everyone`, `@here`, and role pings never do unless you set `mention_everyone` or `mention_roles`. A reply does not ping the person being replied to, since answering somebody is not a reason to notify them twice.

## Moderation

Available when `admin_tools` is on, which is the default. Discord refuses each call unless the bot holds the matching permission **in that channel**, so nothing here works by accident.

| Tool | Discord | Note |
|---|---|---|
| `moderate_member` `restrict` | Timeout | Needs a duration, capped at 28 days. Discord has no indefinite timeout, so the bridge refuses rather than silently choosing a length |
| `moderate_member` `unrestrict` | Clears the timeout | Returns them to exactly their roles |
| `moderate_member` `ban` | Ban | **Permanent.** Discord has no ban expiry, so a duration is refused with a pointer at `restrict`. `revoke_messages` deletes the last 7 days, which is Discord's ceiling |
| `moderate_member` `unban` | Unban | |
| `moderate_member` `kick` | Kick | A real primitive, unlike Telegram's ban-then-unban |
| `set_member_roles` | Replaces the roles somebody holds, by name | Discord has no per-member privileges, so this replaces `set_member_rights`, which is not offered on a Discord channel |
| `pin_message` | Pin or unpin | Needs `PIN_MESSAGES`, which Discord split out from `MANAGE_MESSAGES`. Always announces a pin, so `silent` is refused rather than ignored. 50 pins per channel |
| `set_chat` | Name, topic, and slowmode | Slowmode is Discord-only, 0 to 6 hours |
| `member` | Standing, roles, and timeout state | Omit `user_id` to ask about the bot itself. Permissions are computed for the specific channel, including its overwrites |

An operator can undo any of it from the Discord client, and `mekabridge policy` lifts a mute or a block the agent set on itself.

## History

The bridge records what it sees, the same as on Telegram, and `read_history` and `search_history` reach it. Discord adds two things on top.

**Deletions are honoured.** Discord tells the bridge when a message is deleted, so the recorded copy goes too. The agent cannot be handed back something its author removed. Telegram reports nothing, so its archive cannot do this.

**`search_history` also asks Discord.** When the search names one conversation, the bridge queries Discord's own guild search alongside its local index and merges the results. That reaches messages from before the bot ever joined, which nothing the bridge recorded can. It needs the message content intent and Read Message History, it does not cover direct messages, and a freshly joined server answers nothing until Discord finishes indexing it. All three are handled by falling back to the local results rather than failing the search.

## Listing who is in a server

`list_members` answers two different questions with two different requirements, and only one of them needs anything switched on.

**Searching by name is not gated.** Passing `query` uses Discord's member search, which works with no privileged intent, so a bot can always answer "is there someone here called Dana".

**Listing everyone needs the Server Members intent.** Omitting `query` walks the full roster, which Discord restricts to applications with **Server Members** enabled on the Bot page, under Privileged Gateway Intents. Once a bot is in 100 or more servers, Discord must approve it. Without it the call fails with an error naming that switch and pointing at the search, rather than returning a short list that reads like the whole server.

Unlike Message Content, this one carries no startup risk. Discord gates the HTTP API on the application setting alone, "independently of Gateway restrictions, and unaffected by which intents your app passes in the `intents` parameter when Identifying". So mekabridge never asks for it at the gateway, enabling it changes nothing about how the bridge connects, and it cannot produce a `4014`.

`total` comes back either way, from Discord's own approximate count, so the size of a server is knowable even when its roster is not.

## Seeing who is online

Off by default. Set `presence = true` on the channel to turn it on.

This is the one capability that cannot be reached over HTTP: Discord delivers presence only over the gateway, so the intent goes into the connection handshake rather than being checked per call. Two consequences follow.

**It can stop the bot starting.** Enable **Presence Intent** under Privileged Gateway Intents in the Developer Portal *before* setting the flag. Asking for an intent you have not been granted closes the gateway with a `4014` at startup, exactly as with Message Content. This is the opposite of member listing, where a missing toggle only fails the one call.

**It is a running tally, not a lookup.** Availability is accumulated from the gateway and held in memory. Nothing is written to disk, and it starts empty on every restart.

The bridge keeps **only the status** — online, idle, do not disturb, offline. Discord also sends what each person is playing, listening to, and their custom status; none of that is stored, logged, or exposed. Enabling this still means ingesting the availability of everyone in every server the bot is in, none of whom opted into it, so it is worth turning on deliberately rather than by default.

Somebody who has set themselves invisible is reported as offline. That is what they chose to appear as, and undoing it for a bot's benefit is not the bridge's call.

`member` and `list_members` both carry the result, and `online_only` on a listing narrows it to people at their machine. See [`list_members`](./mcp-tools.md#list_members) for what the statuses mean and why `unknown` is not offline.

## Attachments

Announced in the envelope with a handle, fetched only when the agent asks, as everywhere else. Two Discord specifics are worth knowing:

- **Handles die with their message.** Discord's CDN links are signed and expire, so the bridge stores a reference to the message and re-requests it to get a fresh link. That is always correct, and it means deleting the message makes the file unreachable. On Telegram the file id outlives the message.
- **Videos have no still frame.** Discord exposes no thumbnail to a bot, so `view_attachment` on a video has nothing to show. On Telegram it falls back to the still frame the platform already made.

Stickers arrive as a note rather than a file. An animated one says so, since it is not viewable.

## Without the message content intent

Setting `message_content = false` is supported and coherent, but narrower than it sounds. Discord blanks `content`, `embeds`, and `attachments` on every server message **except** those that mention the bot, replies to it, and direct messages.

So the agent is still woken by name and still reads the message that woke it. What it loses is everything else: the muted-channel backlog is empty, `read_history` has nothing to show about what led up to a mention, and Discord's own search refuses to answer. `mekabridge doctor` warns while this is the case.

## Limits worth knowing

- **1000 gateway connections per 24 hours.** A crash loop at `RestartSec=5` burns that in about 83 minutes, so give the service a longer restart delay than you would on Telegram.
- **10 MiB uploads** on an unboosted server, 50 or 100 MiB at boost tiers 2 and 3.
- Rate limits are handled by the client library, including the global 50 requests per second.
- One shard, which covers up to 2500 servers. Well past the point where one agent context stops making sense.
