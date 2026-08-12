# mekabridge

A bridge between the [meka](https://github.com/k4yt3x/meka) agent and messaging platforms.

People message a bot, the agent reads what they wrote, and the agent decides what to do about it.

## Overview

mekabridge treats the agent as a person with a phone.

- **Inbound** messages from every configured channel are queued and handed to the agent in batches. One meka session runs one turn at a time, so anything arriving mid-turn waits, the way messages wait while somebody is in a meeting.
- **Outbound** messages happen only because the agent called an MCP tool. The bridge never writes chat content of its own. Replying, staying quiet, replying to somebody else, replying on a different platform, or messaging first tomorrow are all the agent's decisions.

One instance owns exactly one meka session, permanently. That session is the agent's memory: everyone it has talked to, on every platform, in one continuous context.

> This is a personal assistant bridge, not a multi-tenant chatbot host. Everyone on the allowlist shares one agent context. The allowlist is mandatory and starts empty.

## Installation

```bash
cargo install --locked --git https://github.com/k4yt3x/mekabridge.git
```

## Quick start

```bash
mekabridge config init      # write a starter config
$EDITOR "$(mekabridge config path)"
mekabridge doctor           # check meka, channels, database, and the MCP port
```

Minimal config:

```toml
[meka]
token = "${MEKA_BRIDGE_TOKEN}"

[[channels.telegram]]
id = "telegram"
token = "${TELEGRAM_BOT_TOKEN}"
allowed_users = [123456789]
```

On meka's side, add a token for the bridge and point meka at its MCP endpoint:

```toml
[[serve.tokens]]
token = "${MEKA_BRIDGE_TOKEN}"
scopes = ["sessions:r", "sessions:w"]

[[mcp.servers]]
name = "mekabridge"
transport = "http"
url = "http://127.0.0.1:9100/mcp"
eager_load_tools = ["send_message", "list_conversations"]
```

Then start them, bridge first where you can:

```bash
mekabridge
meka serve
```

meka retries a failed MCP connect in the background, so the wrong order recovers on its own within a few minutes. Until it does, `[mcp].strict` makes meka refuse every turn, so starting the bridge first still saves you a confusing window.

## Tools the agent gets

| Tool | Purpose |
|------|---------|
| `send_message` | Send Markdown to a conversation. Long text is split automatically |
| `send_file` | Send a local file, optionally shown inline as a photo |
| `list_conversations` | The address book, so the agent can find an id it no longer has in context |
| `get_conversation` | Details for one conversation |

Routing is explicit because it has to be: meka's MCP client sends no session identity with a tool call, so an MCP server cannot infer which conversation a call belongs to. Every send names its target, which is also what makes messaging somebody else, or messaging first, the same operation as replying.

## Operator commands

```bash
mekabridge doctor                   # check everything, non-zero on real problems
mekabridge status                   # session, queue depth, conversations
mekabridge queue list               # what is waiting
mekabridge conversations list       # who the agent can message
mekabridge session show
mekabridge cancel                   # stop the running turn
```

None of these talk to the daemon. They read the SQLite database and call meka directly, so they work whether the bridge is up or down.

## Documentation

See the [documentation](./docs/book/src/introduction.md) for configuration, Telegram and Discord setup, meka integration, operations, and architecture.

## License

MIT
