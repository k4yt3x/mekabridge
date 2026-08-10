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

- `allowed_users` admits specific people wherever they message from.
- `allowed_chats` admits a whole group or channel, so every member of it can talk to the agent. Group ids are negative, for example `-1001234567890`.

Messages from outside the allowlist are dropped at debug level with no reply. Not replying is deliberate: an "unauthorized" response would confirm to a stranger that the bot is live.

Remember that everyone who is allowed shares one agent context. This is a personal assistant bridge, not a multi-tenant service.

## Groups and forum topics

Group messages work the same as direct ones. The envelope tells the agent which chat a message came from and who sent it:

```
conversation: telegram:-1001234567890
from: Alice (@alice, id 123456789)
chat: group "Deploy Crew"
```

Forum topics get their own conversation id with a third segment (`telegram:-1001234567890:77`), so the agent replies into the right topic rather than the group's General.

Note that a bot in a group only receives every message if privacy mode is off (`/setprivacy` in @BotFather). With privacy mode on, it sees only commands and replies to itself.

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

Inbound photos, documents, voice notes, audio, video, and stickers are downloaded into `[storage].attachment_dir`, and the envelope names the local path. Files above `attachment_max_bytes` are not downloaded, and the envelope says so rather than pretending nothing arrived.

Images additionally ride on the turn itself when meka's provider profile has vision enabled, so the agent sees the picture directly instead of having to open the file. Telegram photos are JPEG and stickers are WebP, both of which meka accepts unconverted. See [Vision and images](./meka-integration.md#vision-and-images) for the size limits, which matter because a batch can carry a photo from each of several messages.

Outbound, the agent uses `send_file` with an absolute path, optionally `as_photo` for images it wants shown inline.

## Rate limits

Outbound calls go through teloxide's `Throttle` adaptor. Telegram allows roughly one message per second per chat and answers bursts with 429s carrying a `retry_after`; the adaptor paces requests so a multi-part reply does not lose its tail.

## Typing indicators

While a turn triggered by a chat is running, that chat shows a typing indicator, refreshed every four seconds because Telegram clears it after about five.

This is the one thing mekabridge emits without the agent asking. It is presence rather than content, the same signal a person's phone shows, so it does not compete with the agent's decision about whether to reply. Turn it off with `[bridge].typing_indicator = false`.
