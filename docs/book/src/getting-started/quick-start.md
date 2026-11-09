# Quick Start

End to end, from nothing to the agent answering a Telegram message.

## 1. Create a Telegram bot

Message [@BotFather](https://t.me/BotFather), send `/newbot`, and keep the token it gives you.

Find your own numeric user id by messaging [@userinfobot](https://t.me/userinfobot). You need it for the allowlist: a bot token is a public entry point, so Meka Bridge refuses to start without one.

Then send your bot any message. Telegram bots cannot open a conversation with somebody who has never contacted them, so this first message is what makes you reachable.

## 2. Configure Meka Bridge

```bash
meka-bridge config init
```

Edit the file so it has at least:

```toml
[meka]
base_url = "http://127.0.0.1:8080"
token = "${MEKA_BRIDGE_TOKEN}"

[session]
permission = "read"

[mcp]
bind = "127.0.0.1:9100"

[[channels.telegram]]
id = "telegram"
token = "${TELEGRAM_BOT_TOKEN}"
allowed_users = [123456789]
```

Export the two secrets:

```bash
export MEKA_BRIDGE_TOKEN=...
export TELEGRAM_BOT_TOKEN=...
```

## 3. Configure Meka

Give Meka a token for the bridge, in Meka's `config.toml`:

```toml
[serve]
bind = "127.0.0.1:8080"

[[serve.tokens]]
token = "${MEKA_BRIDGE_TOKEN}"
description = "meka-bridge"
scopes = ["sessions:r", "sessions:w"]
```

And point Meka at the bridge's MCP endpoint:

```toml
[[mcp.servers]]
name = "meka-bridge"
transport = "http"
url = "http://127.0.0.1:9100/mcp"
required = true
eager_load_tools = ["message_send", "conversation_list"]
```

`eager_load_tools` matters. Without it Meka ships MCP tools deferred, and the agent spends a round trip loading `message_send` before it can reply; see [Eager loading](../usage/meka-integration.md#eager-loading).

## 4. Start, bridge first

```bash
meka-bridge         # first
meka serve          # second
```

Meka retries a failed MCP connect in the background with backoff, so starting them the other way round recovers on its own. It just takes a few minutes, during which `required = true` makes Meka refuse turns rather than run them without the bridge's tools. Leave it out and that window is silent: the agent reads each message and has nothing to answer with.

## 5. Say something

Message your bot. The log should show it being handed over and read:

```
INFO meka_bridge::bridge::inbound: rendered messages for the agent messages=1 conversations=1
INFO meka_bridge::bridge::inbound: handed a message to the agent seq=1 item=... replayed=false
INFO meka_bridge::bridge::inbound: the agent read a message seq=1 conversation=telegram:123456789
INFO meka_bridge::bridge: the agent sent a message conversation=telegram:123456789 parts=1
```

If the agent read the message and chose not to reply, you get this instead, which is a normal outcome rather than a fault:

```
WARN meka_bridge::bridge::inbound: the agent sent no messages this turn messages=1
```

## Troubleshooting the first run

| Symptom | Cause |
|---------|-------|
| `doctor` reports Meka unreachable | `meka serve` is not running, or `base_url` is wrong |
| Meka refuses every turn | Meka started before Meka Bridge; wait a few minutes for it to reconnect, or restart it |
| Bot ignores you entirely | Your user id is not in `allowed_users` |
| The agent reads but never replies | The session is at `permission = "none"`; use `read` |
| The agent chats but will not ban or rename | Moderation needs `unrestricted`; see [Meka Integration](../usage/meka-integration.md#why-unrestricted-and-not-workspace) |

More in [Operations](../usage/operations.md).
