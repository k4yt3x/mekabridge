# Quick Start

End to end, from nothing to the agent answering a Telegram message.

## 1. Create a Telegram bot

Message [@BotFather](https://t.me/BotFather), send `/newbot`, and keep the token it gives you.

Find your own numeric user id by messaging [@userinfobot](https://t.me/userinfobot). You need it for the allowlist: a bot token is a public entry point, so mekabridge refuses to start without one.

Then send your bot any message. Telegram bots cannot open a conversation with somebody who has never contacted them, so this first message is what makes you reachable.

## 2. Configure mekabridge

```bash
mekabridge config init
```

Edit the file so it has at least:

```toml
[meka]
base_url = "http://127.0.0.1:8080"
token = "${MEKA_BRIDGE_TOKEN}"

[session]
permission = "write"

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

## 3. Configure meka

Give meka a token for the bridge, in meka's `config.toml`:

```toml
[serve]
bind = "127.0.0.1:8080"

[[serve.tokens]]
token = "${MEKA_BRIDGE_TOKEN}"
description = "mekabridge"
scopes = ["sessions:r", "sessions:w"]
```

And point meka at the bridge's MCP endpoint:

```toml
[[mcp.servers]]
name = "mekabridge"
transport = "http"
url = "http://127.0.0.1:9100/mcp"
required = true
eager_load_tools = ["send_message", "list_conversations"]
```

`eager_load_tools` matters. Without it meka ships MCP tools deferred, and the agent pays a `load_tool` round trip before every single reply.

## 4. Start, bridge first

```bash
mekabridge          # first
meka serve          # second
```

meka retries a failed MCP connect in the background with backoff, so starting them the other way round recovers on its own. It just takes a few minutes, during which `required = true` makes meka refuse turns rather than run them without the bridge's tools. Leave it out and that window is silent: the agent reads each message and has nothing to answer with.

## 5. Say something

Message your bot. The log should show a turn being submitted:

```
INFO mekabridge::bridge::inbound: submitting a turn messages=1 conversations=1 session_id=...
INFO mekabridge::bridge: the agent sent a message conversation=telegram:123456789 parts=1
```

If the agent read the message and chose not to reply, you get this instead, which is a normal outcome rather than a fault:

```
INFO mekabridge::bridge::inbound: the agent sent no messages this turn conversations=1
```

## Troubleshooting the first run

| Symptom | Cause |
|---------|-------|
| `doctor` reports meka unreachable | `meka serve` is not running, or `base_url` is wrong |
| meka refuses every turn | meka started before mekabridge; wait a few minutes for it to reconnect, or restart it |
| Bot ignores you entirely | Your user id is not in `allowed_users` |
| Turns stall for a minute then do nothing | The session is at `permission = "ask"`; use `write` |

More in [Operations](../usage/operations.md).
