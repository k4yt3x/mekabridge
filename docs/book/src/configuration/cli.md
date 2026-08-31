# CLI

```
mekabridge [OPTIONS] [COMMAND]
```

With no subcommand, mekabridge runs the daemon.

## Global options

| Flag | Meaning |
|------|---------|
| `-c`, `--config <PATH>` | Config file to load |
| `-v`, `-vv` | Raise log verbosity, overriding `[log].level` |
| `--log-format <text\|json>` | Override `[log].format` |

## Commands

| Command | Purpose |
|---------|---------|
| `run` | Run the daemon (the default) |
| `doctor` | Check config, database, meka, session permission, channels, where operator notices go, and the MCP port |
| `status` | Session, queue depth, conversation count, configured channels |
| `queue list [--limit N]` | Messages waiting to be delivered |
| `queue clear --yes` | Delete every queue row, including undelivered ones |
| `conversations list [--channel X] [--limit N]` | Known conversations, most recently active first |
| `policy list` | The configured defaults, plus every conversation with a policy of its own |
| `policy set <id> <active\|mute\|block> [--duration 30m] [--reason X]` | Override the default for one conversation |
| `policy clear <id>` | Drop a conversation's own policy so it follows the default again |
| `history <id> [--limit N] [--search WORDS]` | What a conversation said, including what the agent was never woken for |
| `unseen [<id>]` | What the agent has not been shown, as a line and an exit code. Built to gate a scheduled job |
| `session show` | The bound session and what meka says about it |
| `session reset --yes` | Forget the binding so the next message starts a fresh session |
| `cancel` | Cancel the turn currently running |
| `config path` | Where the config would be loaded from |
| `config init [--force]` | Write a starter config |
| `config validate` | Load and validate, reporting the first problem |

## No control socket

The operator commands do not talk to a running daemon. They read the SQLite database directly, which WAL mode permits concurrently, and call meka's HTTP API themselves. So they work whether the bridge is up or down, and a wedged daemon can still be inspected. Output is pipe-friendly.

```bash
mekabridge conversations list | grep group
mekabridge queue list --limit 5
```

The `policy` commands exist because the agent rules on conversations itself. One it muted or blocked indefinitely is unreachable from inside the bridge, including the one you would use to ask it to undo them, so this is the way back.

`policy clear` and `policy set <id> active` are different. Clearing returns the conversation to `[bridge.default_policy]`, which for a group is normally `mute`; setting it active overrides that default so the agent is woken for everything there.

`history` is also how you see what recording is actually costing you, and what a muted conversation is holding. `*` marks a message the agent has not been shown and `@` one that was addressed to it.

## Destructive commands

`queue clear` and `session reset` refuse to act without `--yes`.

`session reset` discards the agent's memory of every conversation it has had. The session still exists inside meka; only the binding is dropped. Delete it in meka if you want the history gone.

## Exit codes

`0` on success, `1` on failure. `doctor` fails when it finds something that would stop the bridge working, so it can gate a deployment:

```bash
mekabridge doctor && systemctl restart mekabridge
```

`unseen` is the exception, because its exit code is its answer: `0` when something is waiting, `1` when nothing is, and `2` when the question could not be answered at all. The third exists so a watcher gating on it cannot mistake a broken command for a quiet room. See [Group attention](../usage/group-attention.md).
