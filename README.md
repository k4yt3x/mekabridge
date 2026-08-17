# mekabridge

A bridge between the [meka](https://github.com/k4yt3x/meka) agent and messaging platforms.

People message a bot, the agent reads what they wrote, and the agent decides what to do about it.

## Overview

mekabridge treats the agent as a person with a phone.

- **Inbound** messages from every configured channel are queued and handed to the agent in batches. One meka session runs one turn at a time, so anything arriving mid-turn waits, the way messages wait while somebody is in a meeting.
- **Outbound** messages happen only because the agent called an MCP tool. The bridge never writes chat content of its own. Replying, staying quiet, replying to somebody else, replying on a different platform, or messaging first tomorrow are all the agent's decisions.

One instance owns exactly one meka session, permanently. That session is the agent's memory: everyone it has talked to, on every platform, in one continuous context.

> The session is not per person. Everyone who can reach the bot shares one agent context and one memory, so anything said in one conversation can inform an answer in another. The allowlist starts empty for that reason: what the agent knows is worth as much as what it can do.

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
required = true
eager_load_tools = ["send_message", "list_conversations"]
```

Then start them, bridge first where you can:

```bash
mekabridge
meka serve
```

meka retries a failed MCP connect in the background, so the wrong order recovers on its own within a few minutes. Set `required = true` on the server entry, as above: it is what makes meka refuse turns while the bridge is unreachable instead of running them with no way to reply. It is not the default.

## Tools the agent gets

| Group | Tools |
|-------|-------|
| Sending | `send_message`, `send_file`, `react`, `edit_message`, `delete_message` |
| Attachments | `view_attachment`, `download_attachment` |
| Address book | `list_conversations`, `get_conversation` |
| Attention | `mute`, `unmute`, `block`, `unblock`, `unseen` |
| History | `read_history`, `search_history` |
| Moderation | `moderate_member`, `set_member_rights`, `set_member_roles`, `pin_message`, `set_chat`, `member`, `list_members` |

Every one is annotated read-only, so the whole surface works at meka's `read` permission level. The moderation group is offered only where a configured platform can honour it and `admin_tools` is on, which is the default.

Routing is explicit because it has to be: meka's MCP client sends no session identity with a tool call, so an MCP server cannot infer which conversation a call belongs to. Every send names its target, which is also what makes messaging somebody else, or messaging first, the same operation as replying.

The agent is not woken for everything. Groups and server channels default to mentions only; the rest is recorded and reachable through the history tools. See [Group attention](./docs/book/src/usage/group-attention.md).

## Operator commands

```bash
mekabridge doctor                   # check everything, non-zero on real problems
mekabridge status                   # session, queue depth, conversations
mekabridge queue list               # what is waiting
mekabridge conversations list       # who the agent can message
mekabridge policy list              # what reaches the agent, and from where
mekabridge policy clear <id>        # undo a decision the agent made about a chat
mekabridge history <id>             # what was said, including what the agent never saw
mekabridge unseen [<id>]            # what is waiting unread, as an exit code a job can gate on
mekabridge session show
mekabridge cancel                   # stop the running turn
```

None of these talk to the daemon. They read the SQLite database and call meka directly, so they work whether the bridge is up or down.

## Documentation

See the [documentation](./docs/book/src/introduction.md) for configuration, Telegram and Discord setup, meka integration, operations, and architecture.

## License

MIT
