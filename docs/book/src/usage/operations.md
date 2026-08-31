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
Restart=always
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
Restart=always
RestartSec=5s

[Install]
WantedBy=multi-user.target
```

`Restart=always` rather than `on-failure`, because `on-failure` does not cover every way a process can stop. systemd counts an exit on SIGHUP, SIGINT, SIGTERM or SIGPIPE as a success, so a daemon killed by one of those is left dead with `Result=success` and nothing in the journal to say the chat went quiet. `always` still honours `systemctl stop`, which is the only stop an operator asked for.

`Requires=` plus `After=` means meka starts after the bridge and is stopped if the bridge is. meka does recover on its own from a bridge that was missing at boot, retrying in the background with backoff, so this is about avoiding the window rather than avoiding a permanent break: while the server is disconnected, `required = true` makes meka refuse turns rather than run them with no way to reply.

Restarting the bridge alone is safe at any point, but meka's reconnect is lazy: it is driven by the next tool call that finds the transport closed, not by the restart.

## Shutdown

SIGTERM or SIGINT stops the channels and the drain loop, then waits up to 30 seconds for an in-flight turn to finish, then checkpoints the database.

A turn already running is allowed to complete. Cutting it off would leave its batch marked in flight with the provider tokens already spent. If the drain window expires anyway, the batch stays in flight and the next start returns it to the queue, so nothing is lost either way.

## Crash recovery

Every inbound message is written to SQLite before the agent is woken for it, so a crash mid-turn loses no work: on start, rows left in flight are returned to pending.

```
WARN mekabridge::bridge: recovered messages that were in flight when the bridge last stopped count=3
```

Delivered rows are kept for seven days rather than deleted immediately, because they are what makes duplicate detection work across a restart: Telegram resends any update whose offset was never confirmed, and without a record of having delivered one the bridge cannot tell it from a message it has never seen.

**There is one window a hard kill can still lose.** Telegram's client library confirms a batch's offset as soon as the batch arrives, before the bridge has stored any of it, so a small number of messages can be acknowledged to Telegram and not yet written. The buffer between the pollers and the writer bounds that number at eight, and blocking the poller when it is full is what stops the confirmation going out any earlier. In practice this only bites on `SIGKILL`, an OOM kill, or power loss: `SIGTERM` drains, and an idle bridge has nothing in flight. Those messages are lost outright rather than recorded as unseen, which makes it the one loss path here that leaves no trace. Closing it fully means the bridge driving `getUpdates` itself, from the highest id it has actually stored.

Discord has no equivalent window, because its gateway replays from a sequence number on resume. It has the opposite gap: nothing backfills messages sent while the connection was down.

That asymmetry is what the restart policy is for. Telegram holds undelivered updates for a day, so a bridge that was down catches up on the next start, but everything sent on Discord meanwhile is gone and was never recorded, which leaves the unseen count looking normal throughout an outage.

## Logs worth knowing

| Line | Meaning |
|------|---------|
| `submitting a turn messages=N` | A batch went to the agent |
| `the agent sent no messages this turn` | The agent read the batch and sent nothing. Legal, but logged at warn with the text it produced instead, because from the other end it is indistinguishable from a broken bridge |
| `the model returned an empty response` | The provider came back with no content and no tool calls. Nothing ran, so the batch is retried at once |
| `requeued messages after a failed turn` | The batch goes back for another attempt. `retry_in` says how long it waits first, and is absent only for the empty-response case, which is offered again at once |
| `the turn failed after the agent had already acted` | Not retried, and the batch is marked delivered. meka only retries an upstream failure while nothing has reached its frontend, so one that gets this far may have a sent message and a shell command behind it |
| `giving up on messages after N attempt(s)` | The batch is `failed`. The chat is told something went wrong, the owner is told what, and the message goes back to being unseen |
| `inbound queue is full` | Messages are being shed; the agent is told how many in the next envelope |
| `recovered messages that were in flight` | The previous run died mid-turn |
| `meka no longer knows session ...` | The session was deleted in meka; a replacement is created and the agent's memory is gone |
| `meka asked for permission` | Unexpected: sessions declare they cannot answer prompts, so meka should deny without asking |
| `the turn was cancelled ... having done nothing` | meka stopped the turn before the agent had acted, most often because the stream went away for longer than `[serve].stream_reattach_grace`. The batch goes back to the queue |
| `the turn was cancelled ... after the agent had already acted` | Stopped partway through work that had real effects. Not retried, for the same reason a failure after a send is not |
| `lost the turn stream ...; rejoining it` | The connection to meka dropped and is being resumed from the last event seen. The turn keeps running and nothing is delivered twice |
| `could not rejoin the turn stream ...; trying again` | The rejoin request itself failed. Retried while meka still holds the turn open; only repeated lines are a concern |
| `lost the turn stream and could not rejoin it` | Resuming failed too, so what the turn did is unknown. The batch goes back to the queue, which may deliver the same messages twice |
| `meka notice: Replay buffer ...` or `Fell behind ...` | The rejoin could not replay everything, so some events are gone. The batch is closed rather than retried; the owner is told only if the turn then failed as well |
| `the agent viewed an attachment` | An image was fetched and passed to the model. `preview=true` means it was a still frame, not the file |
| `the agent downloaded an attachment` | A file was written to `[storage].attachment_dir` |
| `the agent turned a conversation down` | Muted or blocked. `mekabridge policy clear` undoes it |
| `a conversation policy expired` | A chat is back on its default; the count is what a block discarded meanwhile |
| `the agent moderated a member` | Somebody was restricted, banned, or reinstated in a group |
| `the agent changed a member's rights` | An administrator was promoted or demoted |
| `the agent deleted a message` | The message is gone from the platform, so this line is the only record |

The last four are logged at warn deliberately. They change state an operator cannot reconstruct from
the chat afterwards, and the agent can be talked into them by anyone whose message it reads.

JSON logs with `--log-format json` or `[log].format = "json"`.

## Health checks

With `[mcp].health = true` (the default), the MCP listener also serves:

- `GET /health/live`: the process is up
- `GET /health/ready`: the same today, reserved for a deeper check

Both are exempt from the bearer token so an orchestrator does not need the credential.

For a deeper check, `mekabridge doctor` exits non-zero when something would actually stop the bridge working.

## Backups

Everything durable is in the SQLite database at `[storage].path`. Back it up with `sqlite3 state.db ".backup out.db"` rather than copying the file, since WAL mode means a plain copy can catch a torn state.

The database holds the session binding, the conversation address book, the queue, and, since 0.3.0, a record of every message from every conversation the agent is not blocking. It does not hold the agent's side of the conversation or its reasoning; that lives inside meka's own session database.

That last part is what `read_history` and `search_history` read, and it makes the file as sensitive as the chats in it. `[storage].history_retention` bounds how far back it goes, and `"0s"` turns it off. See [Security](./security.md).

## Troubleshooting

**meka refuses every turn.** meka started while the bridge's port was closed. It retries in the background and should clear within a few minutes; restart meka if you would rather not wait. See [meka Integration](./meka-integration.md).

**One message went unanswered while others worked.** Look for `the model returned an empty response`
or `the agent sent no messages this turn` around that timestamp. The first means the provider
returned nothing at all; the bridge hands the batch straight back and reports a failure if it keeps
happening. The second carries the text the agent produced instead, which is usually enough to tell
"it decided not to reply" from "it wrote an answer and never sent it". Neither is a delivery fault:
the queue will show the batch as `done`.

**A chat was told the bridge had a problem.** Three things produce that notice, and the owner's
copy says which:

- The retry budget ran out: `[bridge].turn_retries` attempts spaced 10s, 20s and 40s apart, so about
  a minute. A rate limit or an overloaded provider is the usual cause, and meka's own three retries
  happen inside the first attempt, so the upstream has been unavailable for a while by then.
- The error was one no retry could fix, such as a rejected token. Given up on immediately, so this
  one arrives seconds after the message rather than a minute.
- The turn failed *after* the agent had already sent or run something. Not retried, and the chat is
  told it may not have finished rather than that its message never arrived.

`owner_conversation` has meka's error verbatim, which since meka 0.42 is not the same as the
provider's: a provider refusal arrives as "the provider rejected or failed this turn; its response is
in the server log", because the upstream's own body has held account identifiers and request
fragments and meka will not republish it. The reason it failed is in meka's log, not in the notice.
`mekabridge queue list` shows the rows as `failed`, and
`mekabridge unseen` counts what the agent still has not been shown, unless
`[storage].history_retention` is zero, in which case there is no history to put the message back
into and it is gone.

**A chat was told the bridge had a problem, but the owner's copy never came.** Most likely `[bridge].owner_conversation` names a chat the bridge cannot post to, which `mekabridge doctor` reports under `channels`. The cause to check first is a Discord user id where a channel id belongs: the two are both snowflakes, so startup validation accepts it and Discord answers `Unknown Channel` on every send. `discord:@<user id>` is the form that reaches a person. Failing that, the owner's notice is rate limited like the chat's, to one every fifteen minutes.

**The agent reads messages but never replies.** Check `mekabridge doctor`. The usual cause is
`[session].permission` being `ask` or `none`, at which meka denies the send. (`read` is fine, and so
is everything above it: the send tools are annotated read-only.) The
give-away in the log is `turn finished ... sends=0 tool_calls=0` with a non-zero `text_chars`, which
means the agent wrote a reply that had nowhere to go. Since meka 0.37 the bridge reconciles a
running session's level with the config on the next turn, so fixing the config and restarting is
enough; no session reset needed.

**Telegram polls fail with `A network error: error sending request ... /GetUpdates`.** If the bridge
carries on working either side of it, this is almost certainly not connectivity. `getUpdates` holds
the connection open until an update arrives or `[[channels.telegram]].poll_timeout` elapses, so the
HTTP client has to outlast it. mekabridge sizes the client at `poll_timeout` plus a margin for
exactly this reason; before 0.2.1 it used teloxide's default of 17 seconds against a 30-second poll,
and every poll that went 17 seconds without a message was aborted client-side. On an idle bot that is
most of them.

The tell is the shape rather than the message: a warning every few seconds on a quiet bot, each one
followed immediately by a successful poll, and messages still arriving normally. A real connectivity
fault does not recover between one log line and the next.

If it persists on 0.2.1 or later, then look at the network. One cause worth ruling out is a host that
resolves `api.telegram.org` to IPv6 with no working IPv6 route; compare `curl -4` and `curl -6`
against `https://api.telegram.org/`, since plain `curl` hides it by falling back.

**Gated tools are denied and the agent cannot reply.** The session is at `permission = "ask"`. meka compares the *session* level against `ask` before dispatch, so every call is prompted including read-only ones, and because the bridge declares it cannot answer prompts each is denied at once rather than stalling. That includes `send_message`, so nothing gets sent. Set `read`, then `mekabridge session reset --yes` if the existing session was created at the wrong level.

**The agent chats but will not ban, purge or rename.** The session is below `unrestricted`, where meka puts every tool annotated destructive. `workspace` is not enough and looks like it should be; [meka Integration](./meka-integration.md#why-unrestricted-and-not-workspace) explains why and gives the narrower alternative. `mekabridge doctor` reports exactly this pairing. The refusal is otherwise visible only inside the tool result, so nothing in the log names it.

**The agent says it cannot see an image.** Check that it actually called `view_attachment`: nothing is downloaded on arrival, so a picture only enters the context when the agent asks for it. If it did call the tool and got a description instead of the image, the provider profile has `vision = false`; `mekabridge doctor` reports the setting.

**The bot ignores a user.** Their id is not in `allowed_users`, or their conversation is blocked. Run with `-v` to see the drop at debug level, and check `mekabridge policy list`.

**The bot has gone quiet in a group and nothing looks broken.** Most likely it is working as configured: groups default to `mute`, so the agent is woken only when somebody mentions it or replies to it. `mekabridge policy list` shows the defaults and any conversation ruled on individually, and `mekabridge history <id>` shows what is being recorded but withheld. If you want it woken by everything there, `mekabridge policy set <id> active`.

The other two causes are the agent having muted or blocked the chat itself (same `policy list`, then `policy clear`), and Telegram privacy mode, which withholds the messages before the bridge ever sees them. `mekabridge doctor` reports privacy mode.

**The agent answers a mention and then ignores the reply to it.** Working as configured since 0.7.0. A muted conversation wakes the agent for a mention or a reply to something it said, and for nothing else; the five-minute window that used to follow the agent's own message is gone. `mekabridge unseen <id>` shows what is piling up unheard. The agent can follow a conversation on by unmuting the chat for a while or scheduling its own look-back, both covered in [Group attention](./group-attention.md).

**A watcher the agent set up never fires.** Run its gate command by hand and check the exit code: `mekabridge unseen <id>` exits `2` when it could not answer, which is distinct from `1` for a quiet room precisely so this is diagnosable. A malformed conversation id is a `2`. A well-formed id for a chat nothing has ever arrived from exits `1` and says so on stderr.

**The agent answers a mention without the context around it.** Check `[bridge].mute_context`, which is how many preceding messages are printed alongside a mention in a muted chat. At `0` the agent has to call `read_history` itself. Check also that `[storage].history_retention` is not `0s`, which records nothing at all, and that Telegram privacy mode is off, which would mean there was nothing to record.

**A moderation call fails.** The bot needs to be an administrator of that specific chat with the matching right. Have the agent call `member` with no `user_id` to see what it actually holds there. Telegram also refuses any action against another administrator.

**The agent replies to the wrong person.** Check the conversation ids in `mekabridge conversations list`. The agent routes by the id in the envelope header, so this usually means it reused a stale id rather than the one in front of it.

**Messages arrive in one lump after a delay.** That is batching working as intended: everything that arrived during a turn is delivered together in the next one.

**Every reply feels a couple of seconds slow.** That is `[bridge].settle`, which waits for a chat to go quiet so a burst becomes one turn instead of being answered after the first fragment. Lower it if you would rather have the latency back, but expect the agent to reply mid-thought more often.

**Replies in a busy group are consistently delayed by the same amount.** The chat never goes quiet for a full `settle`, so `settle_max` is releasing every batch rather than acting as an occasional fallback. Lower `settle_max`, or accept it as the price of one turn per burst instead of one per message.

**A file was not downloaded.** It exceeded `attachment_max_bytes`. The envelope says so, and the agent can tell the user.
