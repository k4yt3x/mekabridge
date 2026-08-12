# Security

## The trust model in one paragraph

The allowlist decides who may reach the agent. Everything after that is the agent's judgement, shaped by the instructions you give it in meka's config. The bridge enforces no per-sender policy of its own: it reports who is talking and how they got in, and supplies tools. If you need "this person may ask for anything, that one may only ask about order status", write that in the agent's instructions, because nothing in the bridge will do it for you.

## The allowlist

A bot token is a public entry point. Anyone who learns the bot's name can message it, so mekabridge refuses to start without an allowlist:

| Setting | Admits | Platform |
|---|---|---|
| `allowed_users` | Specific people, **in a direct message only** | both |
| `allowed_chats` | Every member of a group or channel | Telegram |
| `allowed_channels` | Everyone in these channels, and in threads started in them | Discord |
| `allowed_roles` | Anyone holding one of these roles | Discord |
| `allowed_guilds` | **Every member of the server** | Discord |
| `allow_all` | Everyone | both |

Messages from outside are dropped at debug level with no reply. Not replying is deliberate: an "unauthorized" response confirms to a stranger that the bot is live.

Each inbound message carries an `admitted:` line saying which rule let it through, and they are not equivalent claims. `user allowlist` means that account was vetted individually, and it appears only on a direct message, because that is the only place the list reaches. `role allowlist` means it was not: somebody granted the role's holders access, and whoever administers the server can hand that role to anyone without the bridge hearing about it. `chat allowlist` means the room was allowed, not the person. `server allowlist` means not even the room: all that was checked is that they are somewhere in a server you named. `open channel` means nothing was checked.

The line ends with a second, separate fact: whether the sender's own account is on `allowed_users`, wherever they wrote. The two answer different questions. Since the user list reaches direct messages only, somebody you named by hand writing in an allowlisted group is admitted *by the group*, and reporting only that would describe the person who configured the bot as an unvetted member of the room.

**`allowed_guilds` is a much larger grant than it looks.** A Telegram chat allowlist admits a room. This admits everybody in a server, each of whom can wake the agent by name, so a five-thousand-member server is five thousand people with a turn each. Startup warns whenever it is set. Prefer `allowed_channels` or `allowed_roles` where you can.

**On Discord, anybody who shares a server with the bot can send it a direct message.** That is Discord's rule, not the bridge's, and it is why `allowed_users` gates direct messages there rather than being an extra convenience.

It grants nothing inside a server. Naming somebody so they can message the bot privately should not also make the bot answer them in every channel of every server it can see, which is a much wider grant than one line of config looks like. To be heard in a server, name the channel, the role, or the server itself.

**`allow_all` admits individuals, not just groups.** On Telegram a private chat id *is* the user's id, so opening a channel opens direct messages too. Every turn costs provider tokens, so an open bot is also an open invitation to spend your budget. `[bridge.default_policy]` is the preventive lever, since it applies before anybody has said anything; the agent's own `mute` and `block` are reactive.

### What the allowlist does not do

It is checked on the way **in** only. Removing an id stops that person reaching the agent; it does not stop the agent messaging them, because outbound is deliberately unrestricted (see below). If you need somebody to stop hearing from the bot, block the bot from their end or say so in the agent's instructions.

## Outbound is unrestricted

The agent may send to any conversation id its channel accepts, including one it has never received a message from. This is what lets it message you first from an id in its system prompt.

The platform remains the real gate, and it is stricter than it looks: Telegram will not let a bot open a conversation with somebody who has never started it, and will not let it post in a group it is not a member of. A hallucinated id fails at the API with the platform's own wording.

## Prompt injection

Anyone who can message the bot can put words in front of the model, and forwarded messages can put a third party's words there. Two mechanisms make the structure trustworthy; neither makes the *content* trustworthy.

**The envelope cannot be forged.** Routing headers sit outside a per-turn random nonce that fences user text. A message reading `conversation: telegram:999` arrives visibly quoted inside the fence rather than as a header, and any occurrence of the nonce itself is stripped before fencing.

**Provenance is always stated.** `from:`, `admitted:`, and `forwarded from:` mean the agent never has to guess whose words it is reading.

What neither prevents is an admitted sender persuading the agent to do something. The tools that matter here are the ones with effects you cannot undo from the chat:

- **`mute`** and **`block`** can be aimed at any chat, including yours. A conversation blocked indefinitely is unreachable from inside the bridge, and a blocked one keeps nothing, so what was said while it was blocked cannot be recovered afterwards.
- **`moderate_member`** and **`set_member_rights`** change somebody's standing in a group.
- **`delete_message`** removes a message for everyone.

Each of these logs at warn, which is often the only surviving record. Recovery is out of band:

```console
$ mekabridge policy list        # the defaults, and every conversation ruled on individually
$ mekabridge policy clear telegram:-1001234567890
```

Telegram supplies one guardrail free: a bot administrator cannot ban, restrict, or demote another administrator, so the owner of a group cannot be evicted by their own bot.

## Moderation tools

`admin_tools` is on by default, because Telegram refuses every one of these calls unless the bot is an administrator with the matching right, so on a bot that administers nothing they are inert.

Turn it off when the bot is an administrator for a different reason. Promoting a bot is also the usual way to let it read every message in a group, and somebody who did that for the reading alone should not quietly acquire an agent that can ban people:

```toml
[[channels.telegram]]
admin_tools = false
```

That removes them from `tools/list` entirely rather than failing at call time, so the agent never offers a capability the deployment does not want.

For a finer grain, meka can raise the required permission on individual tools:

```toml
[mcp.servers.tool_permissions]
moderate_member = "write"
```

Note that this only helps if the session is *not* at `write`, and running at `write` unlocks file modification too. Config is usually the better lever.

## Privacy mode

A Telegram bot in a group sees only messages that mention it or reply to it, unless privacy mode is off (`/setprivacy` in @BotFather) or the bot is an administrator. It is on by default and looks exactly like a broken allowlist from the outside. `mekabridge doctor` reports it.

Since 0.3.0 the recommended setting is **off**. Privacy mode and the `mute` policy limit what wakes the agent in the same way, but privacy mode does it by never delivering the message at all, so nothing is recorded and `read_history` comes back empty exactly when the agent is trying to work out what a mention referred to. `mute` withholds the turn and keeps the message.

## What the bridge stores

Since 0.3.0 the database holds a record of every message from every conversation the agent is not blocking, including the ones it was never woken for. That is what `read_history` and `search_history` read, and what makes a mention in a muted group answerable.

It is a real change in what sits on disk. Before, the database held the queue, the address book, and attachment metadata; delivered queue payloads were already kept for seven days, so a partial log existed, but it is now a chat log by design.

```toml
[storage]
history_retention = "0s"   # record nothing
```

Delivery is unaffected either way. What changes is that a muted conversation has nothing to show the agent when it wakes, so `mute` becomes closer to a quieter `block`. The database also carries no separate protection: it is as sensitive as the conversations in it, and `send_file` can read it (see below).

## What the agent can reach on your machine

The bridge runs at meka's `read` permission by default, which is enough for every tool here because they change nothing locally. Two things to know:

- **`send_file` reads any path the bridge process can read**, and sends it to a chat. Under the systemd units in [Operations](./operations.md) that is a different user from meka's, so it includes the bridge's own config and its database. Anyone who can talk the agent into a `send_file` call can exfiltrate those.
- **`download_attachment` writes** into `[storage].attachment_dir`, bounded by `attachment_max_bytes` and swept on `attachment_retention`.

Confine the bridge with the systemd hardening in [Operations](./operations.md), and keep `[session].cwd` pointed at a directory that holds nothing you would mind the agent reading aloud.

## Credentials

Every secret is wrapped in a type whose `Debug` and `Display` redact, so a config struct can be logged wholesale without auditing each field. Prefer `${ENV_VAR}` or `token_file` over an inline literal; an inline token is allowed but warns at every startup.

The bot token never reaches the agent. teloxide strips it from the URL in network errors before they are surfaced, so a failed request cannot leak it into a tool result.

## The MCP endpoint

Anyone who can reach it can send messages as the agent. It binds to loopback by default and needs no token there. Moving it off loopback needs both:

```toml
[mcp]
bind = "0.0.0.0:9100"
token = "${MEKABRIDGE_MCP_TOKEN}"
allowed_hosts = ["bridge.internal"]
```

`token` is compared in constant time. `allowed_hosts` is rmcp's DNS-rebinding guard, which defaults to loopback names only and so must be set for any other bind. Running non-loopback without a token logs a warning at startup rather than refusing, but there is no good reason to do it.

Health endpoints (`/health/live`, `/health/ready`) stay open so an orchestrator does not need the credential.
