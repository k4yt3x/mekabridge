# Operations

## systemd

Two units, ordered so the bridge is listening before meka looks for it.

`/etc/systemd/system/mekabridge.service`:

```ini
[Unit]
Description=mekabridge
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=mekabridge
ExecStart=/usr/local/bin/mekabridge
Restart=on-failure
RestartSec=5s
Environment=MEKABRIDGE_CONFIG=/etc/mekabridge/config.toml

# The bridge only needs its own state and whatever the agent's cwd is.
StateDirectory=mekabridge
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true
NoNewPrivileges=true
ReadWritePaths=/var/lib/mekabridge

[Install]
WantedBy=multi-user.target
```

`/etc/systemd/system/meka.service`:

```ini
[Unit]
Description=meka serve
After=network-online.target mekabridge.service
Requires=mekabridge.service

[Service]
Type=simple
User=meka
ExecStart=/usr/local/bin/meka serve
Restart=on-failure
RestartSec=5s

[Install]
WantedBy=multi-user.target
```

`Requires=` plus `After=` means meka starts after the bridge and is stopped if the bridge is. meka does recover on its own from a bridge that was missing at boot, retrying in the background with backoff, so this is about avoiding the window rather than avoiding a permanent break: while the server is disconnected, `[mcp].strict` makes meka refuse every turn.

Restarting the bridge alone is safe at any point. The transport closes from a connected state, so meka reconnects immediately rather than waiting on the cold-start backoff.

## Shutdown

SIGTERM or SIGINT stops the channels and the drain loop, then waits up to 30 seconds for an in-flight turn to finish, then checkpoints the database.

A turn already running is allowed to complete. Cutting it off would leave its batch marked in flight with the provider tokens already spent. If the drain window expires anyway, the batch stays in flight and the next start returns it to the queue, so nothing is lost either way.

## Crash recovery

Every inbound message is written to SQLite before it is acknowledged, so a `kill -9` with a full queue loses nothing. On start, rows left in flight are returned to pending:

```
WARN mekabridge::bridge: recovered messages that were in flight when the bridge last stopped count=3
```

Delivered rows are kept for seven days rather than deleted immediately, because they are what makes duplicate detection work across a restart: Telegram replays updates whose offset was never committed.

## Logs worth knowing

| Line | Meaning |
|------|---------|
| `submitting a turn messages=N` | A batch went to the agent |
| `the agent sent no messages this turn` | The agent read the batch and sent nothing. Legal, but logged at warn with the text it produced instead, because from the other end it is indistinguishable from a broken bridge |
| `the model returned an empty response` | The provider came back with no content and no tool calls. Nothing ran, so the batch is retried |
| `inbound queue is full` | Messages are being shed; the agent is told how many in the next envelope |
| `recovered messages that were in flight` | The previous run died mid-turn |
| `meka no longer knows session ...` | The session was deleted in meka; a replacement is created and the agent's memory is gone |
| `meka asked for permission` | Unexpected: sessions declare they cannot answer prompts, so meka should deny without asking |
| `lost the turn stream` | The connection to meka dropped; the bridge waits for the turn to finish rather than resubmitting |
| `turn image budget reached` | A batch carried more image data than fits in one turn; the rest are named by path |

JSON logs with `--log-format json` or `[log].format = "json"`.

## Health checks

With `[mcp].health = true` (the default), the MCP listener also serves:

- `GET /health/live`: the process is up
- `GET /health/ready`: the same today, reserved for a deeper check

Both are exempt from the bearer token so an orchestrator does not need the credential.

For a deeper check, `mekabridge doctor` exits non-zero when something would actually stop the bridge working.

## Backups

Everything durable is in the SQLite database at `[storage].path`. Back it up with `sqlite3 state.db ".backup out.db"` rather than copying the file, since WAL mode means a plain copy can catch a torn state.

The database holds the session binding, the conversation address book, and the queue. It does not hold conversation history; that lives inside meka's own session database.

## Troubleshooting

**meka refuses every turn.** meka started while the bridge's port was closed. It retries in the background and should clear within a few minutes; restart meka if you would rather not wait. See [meka Integration](./meka-integration.md).

**One message went unanswered while others worked.** Look for `the model returned an empty response`
or `the agent sent no messages this turn` around that timestamp. The first means the provider
returned nothing at all; the bridge retries once and reports a failure if it happens again. The
second carries the text the agent produced instead, which is usually enough to tell "it decided not
to reply" from "it wrote an answer and never sent it". Neither is a delivery fault: the queue will
show the batch as `done`.

**The agent reads messages but never replies.** Check `mekabridge doctor`. The usual cause is
`[session].permission` being `ask` or `none`, at which meka denies the send. (`read` is fine: the
send tools are annotated read-only.) The
give-away in the log is `turn finished ... sends=0 tool_calls=0` with a non-zero `text_chars`, which
means the agent wrote a reply that had nowhere to go. Since meka 0.37 the bridge reconciles a
running session's level with the config on the next turn, so fixing the config and restarting is
enough; no session reset needed.

**Telegram polls fail with `A network error: error sending request`.** Connectivity to
`api.telegram.org`, not a bridge fault. A common cause is a host that resolves it to IPv6 while
having no working IPv6 route: `curl` hides this by falling back to IPv4, but the bot's HTTP client
may not. Compare `curl -4` and `curl -6` against `https://api.telegram.org/` to confirm, then
deprioritise IPv6 in `/etc/gai.conf` or fix the route.

**Gated tools are denied and the agent cannot reply.** The session is at `permission = "ask"`. Because the bridge declares it cannot answer prompts, gated calls are denied at once rather than stalling, but `send_message` needs `write`, so nothing gets sent. Set `write`, then `mekabridge session reset --yes` if the existing session was created at the wrong level.

**The agent says it cannot see an image.** Either the provider profile has `vision = false`, in which case the image is only named by path, or the turn's image budget was spent on earlier photos in the same batch. `mekabridge doctor` reports the vision setting.

**The bot ignores a user.** Their id is not in `allowed_users`. Run with `-v` to see the drop at debug level.

**The agent replies to the wrong person.** Check the conversation ids in `mekabridge conversations list`. The agent routes by the id in the envelope header, so this usually means it reused a stale id rather than the one in front of it.

**Messages arrive in one lump after a delay.** That is batching working as intended: everything that arrived during a turn is delivered together in the next one.

**A file was not downloaded.** It exceeded `attachment_max_bytes`. The envelope says so, and the agent can tell the user.
