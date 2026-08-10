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
| `turn_timeout` | `30m` | Ceiling on one turn. On expiry the turn is cancelled server-side so meka stops burning provider tokens |
| `max_retries` | `3` | Attempts against retryable failures on read-only calls |

Turn submission is never retried at this layer. A replayed `POST /turn` can bill twice and send a second round of messages, so turn-level retry belongs to the queue's attempt counter instead.

## `[session]`

The one meka session this instance owns.

| Key | Default | Meaning |
|-----|---------|---------|
| `cwd` | meka's working directory | Absolute path the agent works in |
| `permission` | `read` | meka permission level. `read` or `write`; see below |
| `recreate_on_missing` | `true` | Create a replacement when meka reports the stored session is gone |

Both `read` and `write` work. The bridge's send tools are annotated read-only, because they change
nothing on your machine, so replying sits at meka's `read` level:

- `read`: the agent can answer messages, run commands, and browse, but cannot modify files.
- `write`: it can also edit files.

> **Do not use `permission = "ask"`.** meka compares the *session* level against `ask` before
> dispatching, so at that level every tool call is prompted, read-only ones included. mekabridge
> declares `supports_permission_prompts: false`, so meka denies each prompt immediately and the agent
> cannot even reply. `doctor` reports `ask` and `none` as failures.

If you would rather sends be gated behind `write` after all, invert it from meka's side:

```toml
[mcp.servers.tool_permissions]
send_message = "write"
```

When `recreate_on_missing` fires, the agent's memory of every past conversation is gone. It is logged at warn level, and the replacement session gets the orientation preamble again.

## `[bridge]`

| Key | Default | Meaning |
|-----|---------|---------|
| `owner_conversation` | none | Conversation that receives operator notices, e.g. `telegram:123456789` |
| `max_queue_depth` | `256` | Messages that may be waiting before new ones are shed |
| `batch_max_messages` | `32` | Most messages handed to the agent in one turn |
| `turn_retries` | `1` | Extra attempts for a batch whose turn failed |
| `typing_indicator` | `true` | Show a typing state in originating chats while a turn runs |

`owner_conversation` is the only place the bridge writes chat content itself, and only to say that it could not deliver something. Without it, a repeated delivery failure is visible only in the logs.

When the queue is full, further messages are dropped and counted, and the next envelope tells the agent how many it did not see. Nothing is discarded silently.

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
| `attachment_dir` | `<data dir>/mekabridge/attachments` | Downloaded inbound files |
| `attachment_max_bytes` | `20971520` (20 MiB) | Files larger than this are not downloaded; the envelope says why. Separate from meka's 3.75 MB per-image cap for attaching an image to a turn |
| `attachment_retention` | `30d` | How long downloads are kept |

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
| `allowed_users` | `[]` | Telegram user ids allowed to reach the agent |
| `allowed_chats` | `[]` | Group and channel ids where every member is allowed. Group ids are negative |
| `parse_mode` | `html` | `html` renders Markdown into Telegram's HTML subset; `none` sends the Markdown verbatim |
| `poll_timeout` | `30s` | `getUpdates` long-poll timeout |

At least one of `allowed_users` or `allowed_chats` must be non-empty. Startup fails otherwise, because a bot with an empty allowlist accepts messages from anyone who finds it.

## A complete example

```toml
[meka]
base_url = "http://127.0.0.1:8080"
token_file = "/etc/mekabridge/meka.token"
turn_timeout = "30m"

[session]
cwd = "/var/lib/mekabridge/workspace"
permission = "write"

[bridge]
owner_conversation = "telegram:123456789"
batch_max_messages = 32
turn_retries = 1

[mcp]
transport = "http"
bind = "127.0.0.1:9100"
path = "/mcp"
token_file = "/etc/mekabridge/mcp.token"

[storage]
path = "/var/lib/mekabridge/state.db"
attachment_dir = "/var/lib/mekabridge/attachments"
attachment_retention = "14d"

[log]
level = "info"
format = "json"

[[channels.telegram]]
id = "telegram"
token_file = "/etc/mekabridge/telegram.token"
allowed_users = [123456789]
allowed_chats = [-1001234567890]
```
