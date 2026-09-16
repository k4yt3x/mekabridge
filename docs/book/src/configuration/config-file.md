# Config File

TOML, loaded from the platform config directory unless `--config` says otherwise. `mekabridge config path` prints the resolved location.

Unknown keys are rejected, so a typo is a startup error rather than a setting that silently does nothing.

## Secrets

Every credential accepts either an inline value or a file:

```toml
token = "${MEKA_BRIDGE_TOKEN}"        # environment substitution, recommended
token_file = "/etc/mekabridge/meka.token"   # file, recommended for production
token = "sk_literal"                   # inline, logs a warning at startup
```

`${VAR}` substitution works anywhere in a string value, not just for tokens. Secrets never appear in logs; only their origin does (`<redacted from ${MEKA_BRIDGE_TOKEN}>`).

Relative paths resolve against the config file's directory, not the process working directory, so a config keeps working under systemd.

## `[meka]`

How to reach `meka serve`.

| Key | Default | Meaning |
|-----|---------|---------|
| `base_url` | `http://127.0.0.1:8080` | Where meka is listening |
| `token` / `token_file` | *(required)* | Bearer token; needs the `sessions:r` and `sessions:w` scopes |
| `connect_timeout` | `10s` | TCP connect budget |
| `max_retries` | `3` | Attempts against retryable failures on read-only calls |

Handing a message over is never retried at this layer. The schedule belongs on the row instead, where it survives a restart and `mekabridge queue list` can show it.

> **`turn_timeout` was removed in 0.14.0 and a config still setting it is refused at startup by name.** It was a ceiling on a turn this bridge held open end to end, and it holds none: a message goes into meka's inbox and meka runs the turn. Delete the line. Since meka 0.51 nothing times out a reply on either side, so if you want a ceiling it belongs in meka rather than in a client that is no longer watching.

## `[session]`

The one meka session this instance owns.

| Key | Default | Meaning |
|-----|---------|---------|
| `cwd` | meka's working directory | Absolute path the agent works in |
| `permission` | `read` | meka permission level. `read`, `workspace` or `unrestricted`; see below |
| `recreate_on_missing` | `true` | Create a replacement when meka reports the stored session is gone |

All three work for replying. The bridge's send tools are annotated read-only, because they change
nothing on your machine, so replying sits at meka's `read` level:

- `read`: the agent can answer messages, run commands, and browse, but cannot modify files.
- `workspace`: it can also write, confined to the session's roots.
- `unrestricted`: no boundary, and the only level that reaches the five moderation tools.

> **Two levels have been retired and are refused at startup by name:** `write`, which meka 0.42
> split into `workspace` and `unrestricted`, and `ask`, which meka 0.46 replaced with an
> `approvals` switch beside the level. Refusing them here rather than passing them on is the point:
> meka would otherwise reject the session on the first message rather than at launch.

> **`permission = "none"` cannot reply**, and `doctor` reports it as a failure: no tool is
> executable at that level, `send_message` included. It is also what meka's 0.46 store migration
> turns an existing `ask` session into, so that is where a bridge upgraded across it lands. Naming
> a workable level and restarting is enough; the bridge reconciles a running session's level with
> the config before its next turn.

Why moderation needs the top rung rather than `workspace` is
[explained under meka Integration](../usage/meka-integration.md#why-unrestricted-and-not-workspace);
the short version is that meka will not let an unsandboxed MCP tool run at a level whose promise is
confinement. `doctor` reports when the moderation tools are registered and the session sits below it.

If you would rather sends be gated too, invert it from meka's side:

```toml
[mcp.servers.tool_permissions]
send_message = "unrestricted"
```

When `recreate_on_missing` fires, the agent's memory of every past conversation is gone. It is logged at warn level.

## `[bridge]`

| Key | Default | Meaning |
|-----|---------|---------|
| `owner_conversation` | none | Conversation that receives the detailed version of an operator notice, e.g. `telegram:123456789`. To be reached by direct message on Discord, `discord:@<user id>` |
| `notify_failures` | `true` | Tell the affected chat, in one line and no detail, when its message could not be delivered |
| `max_queue_depth` | `256` | Messages that may be waiting before new ones are shed |
| `settle` | `3s` | Quiet period a chat goes through before its messages reach the agent, **on platforms that report typing**. Ignored elsewhere. `0s` turns it off |
| `settle_max` | `30s` | Ceiling on that wait, so a compose box left open cannot strand a message. Only reached where typing is reported, and does not override a message waiting out a retry |
| `batch_max_messages` | `32` | Most messages claimed, rendered and posted in one pass. Not a limit on what one turn reads: meka takes every item posted around it |
| `typing_indicator` | `true` | Show a typing state in the originating chats while the model is writing a message, and at no other time |
| `typing_max` | `2m` | Ceiling on how long one such window may last. Not a safety net: it is the only thing that closes a window whose closing event never arrives |
| `mute_context` | `5` | Messages of missed context printed alongside a mention in a muted conversation. `0` withholds them and leaves the agent to ask |

`settle` and `settle_max` only apply where the platform tells the bridge that somebody is typing. Discord does. Telegram has no such update at all: the Bot API lets a bot *send* a chat action and never receive one, so there a message starts a turn as soon as it arrives.

That split is deliberate. Without the signal any wait is a guess, and there is no number that works: two seconds is nowhere near long enough for somebody to type a second sentence, and a number long enough for that is a long time to make somebody wait who only ever meant to send one message. So the wait exists only where it can end when the person actually stops.

Every conversation is also held for one second regardless, which is not configurable. It exists for the wire rather than for people: platforms split one thing into several messages, Telegram's multi-photo albums above all, and without it a post arrives as one photo followed by a separate turn carrying the rest. Those parts land milliseconds apart, so a second is generous. See [Group attention](../usage/group-attention.md).

The indicator tracks one thing: the interval in which the model is writing the arguments to a send tool, which meka opens with `tool_call.composing`. It is not raised while the agent reads, searches, thinks, or waits on a turn somebody else started, and a turn that never writes a message never draws one at all. The window is closed by the next tool call meka announces, whichever one that is, rather than by the one that opened it: meka can retry a provider round mid-arguments, and the retry comes back with different call ids, so the closing event for the original never arrives.

That is narrower than it used to be. The indicator previously went up when a turn started and stayed up for its whole length, which for a turn spent on a dozen tool calls was a claim that a reply was seconds away for minutes at a time. The cost of the change is that a long turn now looks like silence; the benefit is that when the indicator is up it is true.

Two things follow from where the signal comes from. On a provider backend that does not stream a tool call as it is written, `openai-chat-completions` today, meka resolves the name and arguments together, so the window has no duration and the indicator is at most a flicker. And because `tool_call.composing` carries only the tool name, the indicator is shown in the conversations the turn read items from rather than the message's eventual target; those are the same thing unless the turn read items from several chats.

`typing_max` is the only thing that closes a window whose closing event never arrives. A window is normally closed by the next tool call meka announces, but a feed that goes quiet after `tool_call.composing` produces no such event. Sized for a whole turn, as it once was, the ceiling could never fire and a chat showed "typing" for half an hour before falling silent with nothing delivered. Two minutes is generous against a model writing a long reply and short enough that nobody is misled for long.

How often the indicator is renewed is not configurable: Telegram clears the status after about five seconds and Discord after ten, so the interval is fixed at four to look continuous on both.

> **`turn_retries` was removed in 0.14.0 and is refused at startup by name too.** It counted attempts at a turn the bridge submitted and watched. meka now retries an item it has accepted itself, for an hour, and the bridge posts one it has not accepted on the same schedule, so the two halves add up to the same hour whichever side the outage is on. There is no longer a number here for an operator to have a preference about. Delete the line.

A message that could not be delivered produces two notices, deliberately different. The chat it came from is told one line that says nothing but that something went wrong: whoever is in it did nothing wrong, cannot act on an upstream status code, and is not necessarily somebody you would hand one to. `owner_conversation` gets the rest: which chats lost what, how many attempts were made, and the error verbatim.

Both are rate limited to one message per conversation per fifteen minutes, and the owner's notice says how many failures went unreported in between, so a provider out of quota for an hour reads as an hour rather than as a blip.

A chat is told only where the agent had not already answered it. A turn that sent something and then died has left work half done, which is worth telling the owner about, but an apology in a chat the agent had just replied in would contradict what it said there.

Those two are the only places the bridge writes chat content of its own, and `notify_failures = false` removes the first. With it off and no `owner_conversation` set, a message the agent never receives is reported only in the logs, and the bridge says so at startup.

`owner_conversation` names a chat, not a person, and startup can only check the shape of the id, never whether anything answers to it. Discord makes that gap sharp, because a user id and a channel id are both snowflakes: the wrong one is accepted here and then answered with `Unknown Channel` on every send, and nothing says so. Write `discord:@<user id>` to be reached by direct message and the bridge opens the channel itself. `mekabridge doctor` asks the platform whether the configured id resolves, which is the only way to settle it short of sending.

The message is also put back among what the agent has not seen, so `unseen` counts it and it comes back as missed context the next time that conversation wakes.

When the queue is full, further messages are dropped and counted, and the next item handed over tells the agent how many it did not see. Nothing is discarded silently.

`mute_followup` was removed in 0.7.0, and a config still setting it is refused at startup by name. That is deliberate: a knob that silently stopped doing anything would leave an operator reading their own config as the explanation for behaviour it no longer controls. Delete the line. A muted conversation now wakes the agent only when somebody names it or replies to something it said, and following a conversation on is the agent's own call. See [Group attention](../usage/group-attention.md).

`mute_context` trades a few lines of the item that wakes a chat against a tool call. A bare `@bot what do you think about that?` is meaningless without the antecedent, and `read_history` to recover it costs a whole model round trip. Capped at 50, because a generous lookback quietly turns mention-only back into every message.

## `[bridge.default_policy]`

What reaches the agent from a conversation nobody has ruled on, by kind of chat.

| Key | Default | Meaning |
|-----|---------|---------|
| `direct` | `active` | One-to-one chats |
| `group` | `mute` | Groups and supergroups |
| `channel` | `mute` | Broadcast channels |

The three policies:

- **`active`**: every message wakes the agent and costs a provider turn.
- **`mute`**: everything is received and recorded, but only a message addressed to the agent wakes it. The rest is readable with `read_history` and `search_history`, and the agent is told how much it missed.
- **`block`**: nothing is delivered and nothing is kept.

These are the same three states Telegram and Discord offer in their own notification settings, which is deliberate: an agent reading a tool called `mute` should already know what it does.

Groups default to `mute` because a bot with privacy mode off receives every message said in every group it is in, and in a busy one almost none of it concerns the agent. A one-to-one chat has nobody else in it, so `mute` there would be a no-op at best; the bridge refuses it and points at `block`.

A conversation with an explicit decision keeps it, so changing these defaults moves only the conversations nobody has ruled on. `mekabridge policy list` shows both.

Setting a default to `block` is allowed but warns on every startup: a bridge that answers nobody is hard to tell from a broken one.

## `[mcp]`

The endpoint meka connects to.

| Key | Default | Meaning |
|-----|---------|---------|
| `transport` | `http` | `http` (streamable HTTP) or `stdio` |
| `bind` | `127.0.0.1:9100` | Listen address for `http` |
| `path` | `/mcp` | Endpoint path |
| `token` / `token_file` | none | Bearer token meka must present |
| `allowed_hosts` | loopback names | `Host` values accepted, a DNS-rebinding guard |
| `health` | `true` | Serve `/health/live` and `/health/ready` |

A non-loopback `bind` without a `token` is a `doctor` failure: anyone who can reach the port can send messages as the agent. Moving off loopback also means setting `allowed_hosts`, because rmcp only accepts loopback host names by default.

## `[storage]`

| Key | Default | Meaning |
|-----|---------|---------|
| `path` | `<data dir>/mekabridge/mekabridge.db` | SQLite database |
| `attachment_dir` | `<data dir>/mekabridge/attachments` | Where `download_attachment` writes files |
| `attachment_max_bytes` | `20971520` (20 MiB) | Ceiling on what `download_attachment` will fetch. Telegram's cloud API caps `getFile` at 20 MiB regardless, so raising this only helps against a local Bot API server |
| `attachment_retention` | `30d` | How long an attachment stays reachable. Governs both the handle and any file downloaded through it, so past this the agent can no longer fetch a file from an older message |
| `history_retention` | `30d` | How long a recorded message stays readable through `read_history` and `search_history`. `0s` records nothing at all |

Every message the bridge is not blocking is recorded, not only the ones from muted conversations. A history that works in some chats and not others is a worse tool than one that behaves the same everywhere, and an agent whose session has been compacted has as much use for scroll-back in a chat it was listening to as in one it was not.

That means the database holds the content of conversations the agent never read. Smaller a change than it sounds, since delivered queue payloads were already kept for seven days, but it goes from being a queue to being a chat log. `history_retention = "0s"` turns it off entirely; delivery is unaffected either way, and a muted conversation simply has nothing to show when it wakes the agent. See [Security](../usage/security.md).

The default matches `attachment_retention` so a message and the picture attached to it fall out of reach together, rather than leaving the agent a description of a file it can no longer open.

## `[log]`

| Key | Default | Meaning |
|-----|---------|---------|
| `level` | `info` | `EnvFilter` directive, e.g. `mekabridge=debug,teloxide=warn` |
| `format` | `text` | `text` or `json` |

`RUST_LOG` overrides `level`, and `-v` / `-vv` override both.

## `[[channels.telegram]]`

One table per bot. Each platform gets its own array, so adding a platform never disturbs an existing entry.

| Key | Default | Meaning |
|-----|---------|---------|
| `id` | *(required)* | Instance name, unique across all platforms. Becomes the first segment of conversation ids, so it may only contain letters, digits, `-` and `_` |
| `token` / `token_file` | *(required)* | Bot token from @BotFather |
| `allowed_users` | `[]` | Telegram user ids allowed to message the bot **directly**. Grants nothing in a group |
| `allowed_chats` | `[]` | Group and channel ids where every member is allowed. Group ids are negative. The only way to be heard in a group |
| `allow_all` | `false` | Accept everyone, for a public or customer-service bot |
| `admin_tools` | `true` | Offer the agent the group moderation tools |
| `parse_mode` | `html` | `html` renders Markdown into Telegram's HTML subset; `none` sends the Markdown verbatim |
| `poll_timeout` | `30s` | `getUpdates` long-poll timeout. The HTTP client is sized from this, so raising it is safe |

At least one of `allowed_users`, `allowed_chats`, or `allow_all` must be set. Startup fails otherwise, because a bot with an empty allowlist would accept messages from anyone who finds it, and that should be a decision rather than an oversight.

`allow_all` warns on every startup. On Telegram a private chat id *is* the user's own id, so it admits individuals as well as groups, and every message it lets through costs a provider turn. Setting `allowed_users` alongside it still does something useful: those people are marked on the `admitted:` line as individually named, wherever they write.

`admin_tools` is on because Telegram refuses every moderation call unless the bot is an administrator with the matching right, so on a bot that administers nothing they are inert. Turn it off when the bot is an administrator for some other reason, such as reading all group messages. See [Security](../usage/security.md).

## `[[channels.discord]]`

One table per bot, in its own array. Ids here are **strings**, because Discord snowflakes are strings in its own API and that is what you get when you copy one out of the client with Developer Mode on. Each is checked at startup, so a typo is a startup error rather than an allowlist entry that silently never matches.

| Key | Default | Meaning |
|-----|---------|---------|
| `id` | *(required)* | Instance name, unique across all platforms |
| `token` / `token_file` | *(required)* | Bot token from the Developer Portal's Bot page |
| `allowed_users` | `[]` | User ids allowed to message the bot **directly**. Grants nothing inside a server |
| `allowed_guilds` | `[]` | Servers where **every member** is allowed |
| `allowed_channels` | `[]` | Channels where every participant is allowed. A thread inherits its parent's standing |
| `allowed_roles` | `[]` | Anyone holding one of these roles |
| `allow_all` | `false` | Accept everyone |
| `admin_tools` | `true` | Offer the agent the moderation tools |
| `message_content` | `true` | Ask for the privileged `MESSAGE_CONTENT` intent |
| `presence` | `false` | Track who is online, which needs the privileged Presence Intent. Off by default: it cannot be reached over HTTP, so an ungranted intent closes the gateway at startup |
| `mention_everyone` | `false` | Let a message the agent writes ping `@everyone` or `@here` |
| `mention_roles` | `false` | Let a message the agent writes ping a role |

At least one allowlist, or `allow_all`, must be set. `allowed_users` gates direct messages, which is not optional: anybody sharing a server with the bot can open one. It grants nothing inside a server, so a config naming only people is reachable by DM alone; startup warns when that is the case.

`allowed_guilds` warns on every startup. It is much the largest of the four grants, since everyone in the server can wake the agent by name.

`message_content` must be enabled on the Bot page of the Developer Portal before it can be requested. Asking for a privileged intent that is not enabled does not degrade: Discord closes the gateway with a `4014` at startup. Running with it off is supported and warns, because the agent is still woken by mentions but has no record of what led up to one. See [Discord](../usage/discord.md).

## A complete example

```toml
[meka]
base_url = "http://127.0.0.1:8080"
token_file = "/etc/mekabridge/meka.token"

[session]
cwd = "/var/lib/mekabridge/workspace"
permission = "workspace"

[bridge]
owner_conversation = "telegram:123456789"
batch_max_messages = 32

[bridge.default_policy]
direct = "active"
group = "mute"
channel = "mute"

[mcp]
transport = "http"
bind = "127.0.0.1:9100"
path = "/mcp"
token_file = "/etc/mekabridge/mcp.token"

[storage]
path = "/var/lib/mekabridge/state.db"
attachment_dir = "/var/lib/mekabridge/attachments"
attachment_retention = "14d"
history_retention = "14d"

[log]
level = "info"
format = "json"

[[channels.telegram]]
id = "telegram"
token_file = "/etc/mekabridge/telegram.token"
allowed_users = [123456789]
allowed_chats = [-1001234567890]

[[channels.discord]]
id = "discord"
token_file = "/etc/mekabridge/discord.token"
allowed_users = ["245119312739729408"]
allowed_channels = ["111222333444555666"]
```
