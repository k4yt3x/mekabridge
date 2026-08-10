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
| `doctor` | Check config, database, meka, session permission, channels, and the MCP port |
| `status` | Session, queue depth, conversation count, configured channels |
| `queue list [--limit N]` | Messages waiting to be delivered |
| `queue clear --yes` | Delete every queue row, including undelivered ones |
| `conversations list [--channel X] [--limit N]` | Known conversations, most recently active first |
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

## Destructive commands

`queue clear` and `session reset` refuse to act without `--yes`.

`session reset` discards the agent's memory of every conversation it has had. The session still exists inside meka; only the binding is dropped. Delete it in meka if you want the history gone.

## Exit codes

`0` on success, `1` on failure. `doctor` fails when it finds something that would stop the bridge working, so it can gate a deployment:

```bash
mekabridge doctor && systemctl restart mekabridge
```
