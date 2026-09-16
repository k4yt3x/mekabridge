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

The bridge recovers from the opposite order on its own too, and waits rather than failing: it cannot bind a session or open a feed until meka answers, so it retries both with backoff and starts handing messages over the moment it can. Messages that arrive meanwhile wait in the queue.

Restarting the bridge alone is safe at any point, but meka's reconnect is lazy: it is driven by the next tool call that finds the transport closed, not by the restart.

## Shutdown

SIGTERM or SIGINT stops the channels, the feed reader and the session task, then waits up to 30 seconds for anything in flight, then checkpoints the database.

Nothing here has to finish. A hand-over is durable on whichever side it reached: one meka has already accepted is meka's to run, and one it has not is posted again on the next start under the same key. A turn already running on meka's side is unaffected by the bridge stopping at all, since the bridge no longer holds it open.

## Crash recovery

Every inbound message is written to SQLite before the agent is woken for it, and the hand-over that carries it is written down too, so a crash at any point loses nothing and repeats nothing:

```
WARN mekabridge::bridge::inbound: recovered hand-overs the previous run left open orphaned=1 unposted=1 posted=2
```

- **`orphaned`**: rows claimed with nothing ever rendered for them. Nothing reached meka, so they go straight back to the queue.
- **`unposted`**: an item was rendered and never posted. The same bytes go out under the same key, rather than a fresh item that would state a backlog already marked seen and so state nothing at all.
- **`posted`**: meka has the item and its fate is unknown. The feed's replay usually says, and every one of them is asked about under its own key as soon as the feed is open, so nothing waits on an outcome that has been and gone.

`mekabridge queue list` shows the same states, per row, while the bridge runs.

Delivered rows are kept for seven days rather than deleted immediately, because they are what makes duplicate detection work across a restart: Telegram resends any update whose offset was never confirmed, and without a record of having delivered one the bridge cannot tell it from a message it has never seen. Sweeping one takes the rendered item with it. A row that ends `failed` drops its rendered item there and then and is kept, since it is what an operator has to look at.

**There is one window a hard kill can still lose.** Telegram's client library confirms an update batch's offset as soon as the batch arrives, before the bridge has stored any of it, so a small number of messages can be acknowledged to Telegram and not yet written. The buffer between the pollers and the writer bounds that number at eight, and blocking the poller when it is full is what stops the confirmation going out any earlier. In practice this only bites on `SIGKILL`, an OOM kill, or power loss: `SIGTERM` drains, and an idle bridge has nothing in flight. Those messages are lost outright rather than recorded as unseen, which makes it the one loss path here that leaves no trace. Closing it fully means the bridge driving `getUpdates` itself, from the highest id it has actually stored.

Discord has no equivalent window, because its gateway replays from a sequence number on resume. It has the opposite gap: nothing backfills messages sent while the connection was down.

That asymmetry is what the restart policy is for. Telegram holds undelivered updates for a day, so a bridge that was down catches up on the next start, but everything sent on Discord meanwhile is gone and was never recorded, which leaves the unseen count looking normal throughout an outage.

## Logs worth knowing

| Line | Meaning |
|------|---------|
| `following the session feed` | The feed is open. Everything below that reports an outcome arrives on it |
| `rendered messages for the agent` | A settled pass became one inbox item per message. None has left yet |
| `handed a message to the agent` | meka accepted the item and now owns getting it read. `replayed=true` means meka already had it, which is how a restart settles rather than sends twice |
| `the agent was woken for this bridge's messages` | meka opened a turn on one of this bridge's items |
| `the agent read a message` | The provider accepted a request carrying it, so the model has seen it. This, and only this, is what marks a message delivered |
| `turn finished sends=N tool_calls=N` | A turn that carried this bridge's messages ended. `messages` says how many it read |
| `the agent sent no messages this turn` | The agent read them and sent nothing. Legal, but logged at warn with the text it produced instead, because from the other end it is indistinguishable from a broken bridge |
| `the model returned an empty response` | The provider came back with no content and no tool calls. Nothing ran, so the messages are offered again |
| `meka did not accept a message (...); trying again` | The post did not get through. `retry_in` says how long it waits, doubling from 10s to 5m |
| `giving up on a message meka never accepted` | An hour of posts, none of which got through. The message goes back among what the agent has not seen and both the chat and the owner are told |
| `meka refused a message` | A refusal no wait can clear: a rejected token, a malformed request, a context overflow. Given up on at once, so this arrives seconds after the message rather than an hour later |
| `meka gave up on a message` | meka spent its own hour on the item. Same outcome as the line above, decided on the other side |
| `a message did not reach the agent (...); it is offered again` | Its item was taken back unread, or the turn that read it produced nothing at all. It is handed over afresh under a new key |
| `the turn ended after the agent had read this bridge's messages` | Not offered again, and the messages are marked delivered. meka only retries an upstream failure while nothing has reached its frontend, so one that gets this far may have a sent message and a shell command behind it |
| `the agent's session no longer fits the model's context window` | Not something a retry fixes, and the next message will fail the same way. The one failure that is about the session rather than the message |
| `giving up on N message(s)` | The chat is told something went wrong, the owner is told what, and the messages go back to being unseen |
| `recovered hand-overs the previous run left open` | The previous run stopped mid-hand-over. `orphaned`, `unposted` and `posted` say what state each was in; see [Crash recovery](#crash-recovery) |
| `inbound queue is full` | Messages are being shed; the agent is told how many on the next item handed over |
| `lost the session feed (...); reconnecting` | The connection to meka dropped. It is reopened from the last event acted on and meka replays what was missed; nothing is lost and nothing is delivered twice |
| `could not open the session feed (...); trying again` | The reconnect itself failed. Retried with backoff up to 30s; only repeated lines are a concern |
| `meka notice: the replay does not reach your Last-Event-ID ...` or `Fell behind ...` | Some events are gone. Every hand-over still out is asked about under its own key, so nothing is waiting on an outcome that has already passed |
| `meka has had a message longer than an outcome can take; asking what became of it` | An outcome that arrived and could not be written down. meka gives up on an item within the hour, so anything older has an answer waiting; the bridge asks rather than leaving the message with meka |
| `meka had no record of a message it had accepted` | meka's store was replaced, or `[meka].token` was rotated, which changes the scope of the key. It is handed over afresh, and on a rotation the agent may read it twice |
| `skipping a feed frame this build cannot read` | One event this version does not understand. Skipped rather than reconnected over, since a reconnect would replay it and fail identically |
| `logged in` | Which account a channel resolved to at startup, which is what every message it records is keyed on |
| `this channel is logged in as a different account than last time` | The bot behind a channel was replaced. Nothing is lost, but messages recorded under the old account cannot be replied to, reacted to, edited, or deleted from the new one, and the history tools mark them |
| `could not find out which account this channel is logged in as` | The platform did not answer the startup probe, so the channel is not started this run; the others are unaffected |
| `meka no longer knows session ...` | The session was deleted in meka; a replacement is bound and the agent's memory is gone |
| `meka asked for permission` | Unexpected: sessions declare they cannot answer prompts, so meka should deny without asking |
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

The database holds the session binding, the account each channel has been logged in as, the conversation address book, the queue, and, since 0.3.0, a record of every message from every conversation the agent is not blocking. It does not hold the agent's side of the conversation or its reasoning; that lives inside meka's own session database.

Upgrading to 0.13.0 rebuilds every table on the first start, and 0.14.0 rebuilds the queue again. Both are one-way: an older build refuses to open the result rather than misread it. Take the backup first.

That last part is what `read_history` and `search_history` read, and it makes the file as sensitive as the chats in it. `[storage].history_retention` bounds how far back it goes, and `"0s"` turns it off. See [Security](./security.md).

## Troubleshooting

**meka refuses every turn.** meka started while the bridge's port was closed. It retries in the background and should clear within a few minutes; restart meka if you would rather not wait. See [meka Integration](./meka-integration.md).

**One message went unanswered while others worked.** Look for `the model returned an empty response`
or `the agent sent no messages this turn` around that timestamp. The first means the provider
returned nothing at all; the messages are offered again, and reported as undeliverable if it keeps
happening. The second carries the text the agent produced instead, which is usually enough to tell
"it decided not to reply" from "it wrote an answer and never sent it". Neither is a delivery fault:
the queue will show the rows as `done`.

**A chat was told the bridge had a problem.** Four things produce that notice, and the owner's copy
says which:

- meka could not be reached for an hour from when the item was rendered. The posts are spaced 10s, 20s
  and so on up to 5m apart, so this arrives long after the message rather than straight away.
- meka had the item and spent its own hour on it, then gave up. Same wait, decided on the other side.
- The error was one no wait could fix, such as a rejected token or a context overflow. Given up on
  immediately, so this one arrives seconds after the message.
- The turn failed *after* the model had read the messages. Not handed over again, and the chat is
  told it may not have finished rather than that its message never arrived. A chat the agent had
  already answered is told nothing at all, since an apology would contradict what it just said.

`owner_conversation` has meka's error verbatim, and after a provider failure the upstream's own
response alongside it, as `The provider said: ...`. That is what names a revoked credential or a
spent quota rather than leaving the notice pointing at meka's log. Two things withhold it: a meka
older than the `[serve] relay_provider_errors` key, and an operator who set that key to `false`,
which is worth doing where read-only meka tokens go to people not entitled to the provider account,
since the same text names it. `mekabridge queue list` shows the rows as `failed`, and
`mekabridge unseen` counts what the agent still has not been shown, unless
`[storage].history_retention` is zero, in which case there is no history to put the message back
into and it is gone.

**Every message suddenly fails, in every chat at once.** Look for `the agent's session no longer fits the model's context window`. One permanent session carries every conversation, so a window it has outgrown refuses the next message too, and no number of attempts changes that. Shorten the conversation on meka's side, with `POST /v1/sessions/{id}/compact` or `/rewind` in its REPL. `mekabridge session reset --yes` also clears it, at the price of the agent's memory of every conversation; anything handed over is closed and put back among what the agent has not seen, so the new session meets it as a backlog.

**Messages are piling up and nothing is being handed over.** `mekabridge queue list` shows the hand-overs as well as the rows. A hand-over `waiting on meka` with a rising post count and a `last error` is meka being unreachable or refusing; one `with meka` that never settles means the feed is not open, which the log says with `could not open the session feed`. One waiting hand-over holds the queue behind it deliberately: nothing overtakes it, since whatever is refusing the first will refuse the second, and releasing the later one would hand the agent the second half of a morning before the first.

**A chat was told the bridge had a problem, but the owner's copy never came.** Most likely `[bridge].owner_conversation` names a chat the bridge cannot post to, which `mekabridge doctor` reports under `channels`. The cause to check first is a Discord user id where a channel id belongs: the two are both snowflakes, so startup validation accepts it and Discord answers `Unknown Channel` on every send. `discord:@<user id>` is the form that reaches a person. Failing that, the owner's notice is rate limited like the chat's, to one every fifteen minutes.

**The agent reads messages but never replies.** Check `mekabridge doctor`. The usual cause is
`[session].permission` being `none`, at which meka denies the send. (`read` is fine, and so is
everything above it: the send tools are annotated read-only.) The
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

**Every tool is denied and the agent cannot reply.** The session is at `permission = "none"`, where nothing is executable, `send_message` included, so nothing gets sent. The common way to arrive here is meka's 0.46 upgrade: it retired the `ask` level, and its store migration turns an existing `ask` session into `none` with approvals on, which denies every call because this bridge has nobody to put an approval to. Set `[session].permission` to `read` and restart; as above, the bridge reconciles the level before its next turn, so no session reset is needed. The `approvals` switch the migration left on can stay: with no one to ask, meka denies an above-level call either way.

**The agent chats but will not ban, purge or rename.** The session is below `unrestricted`, where meka puts every tool annotated destructive. `workspace` is not enough and looks like it should be; [meka Integration](./meka-integration.md#why-unrestricted-and-not-workspace) explains why and gives the narrower alternative. `mekabridge doctor` reports exactly this pairing. The refusal is otherwise visible only inside the tool result, so nothing in the log names it.

**The agent says it cannot see an image.** Check that it actually called `view_attachment`: nothing is downloaded on arrival, so a picture only enters the context when the agent asks for it. If it did call the tool and got a description instead of the image, the profile has `vision = false`; `mekabridge doctor` reports the setting.

**The bot ignores a user.** Their id is not in `allowed_users`, or their conversation is blocked. Run with `-v` to see the drop at debug level, and check `mekabridge policy list`.

**The bot was deleted and recreated.** Put the new token in the same channel entry and restart; nothing else is needed. The bridge asks each channel which account it is logged in as at startup and records it, so the next start logs `this channel is logged in as a different account than last time` at warn, and `mekabridge status` names the account. Conversation ids do not change, since a private chat's id is the person's own, and policies and `owner_conversation` carry over. Message ids do: Telegram numbers a private chat per bot, so the new bot starts again at 1, and everything recorded under the old bot is marked `previous_account` by `read_history` and `[previous bot account]` by `mekabridge history`, because the new bot cannot reply to, react to, edit, or delete those. Before 0.13.0 the new bot's messages collided with the old bot's in the database and were silently dropped, not delivered and not recorded, for as long as the old rows were within retention.

**The bot has gone quiet in a group and nothing looks broken.** Most likely it is working as configured: groups default to `mute`, so the agent is woken only when somebody mentions it or replies to it. `mekabridge policy list` shows the defaults and any conversation ruled on individually, and `mekabridge history <id>` shows what is being recorded but withheld. If you want it woken by everything there, `mekabridge policy set <id> active`.

The other two causes are the agent having muted or blocked the chat itself (same `policy list`, then `policy clear`), and Telegram privacy mode, which withholds the messages before the bridge ever sees them. `mekabridge doctor` reports privacy mode.

**The agent answers a mention and then ignores the reply to it.** Working as configured since 0.7.0. A muted conversation wakes the agent for a mention or a reply to something it said, and for nothing else; the five-minute window that used to follow the agent's own message is gone. `mekabridge unseen <id>` shows what is piling up unheard. The agent can follow a conversation on by unmuting the chat for a while or scheduling its own look-back, both covered in [Group attention](./group-attention.md).

**A watcher the agent set up never fires.** Run its gate command by hand and check the exit code: `mekabridge unseen <id>` exits `2` when it could not answer, which is distinct from `1` for a quiet room precisely so this is diagnosable. A malformed conversation id is a `2`. A well-formed id for a chat nothing has ever arrived from exits `1` and says so on stderr.

**The agent answers a mention without the context around it.** Check `[bridge].mute_context`, which is how many preceding messages are printed alongside a mention in a muted chat. At `0` the agent has to call `read_history` itself. Check also that `[storage].history_retention` is not `0s`, which records nothing at all, and that Telegram privacy mode is off, which would mean there was nothing to record.

**A moderation call fails.** The bot needs to be an administrator of that specific chat with the matching right. Have the agent call `member` with no `user_id` to see what it actually holds there. Telegram also refuses any action against another administrator.

**The agent replies to the wrong person.** Check the conversation ids in `mekabridge conversations list`. The agent routes by the `conversation:` header, so this usually means it reused a stale id rather than the one in front of it.

**Messages arrive in one lump after a delay.** That is batching working as intended: everything that settles together is handed over together, and meka reads the lot into one turn.

**Every reply feels a couple of seconds slow.** That is `[bridge].settle`, which waits for a chat to go quiet so a burst becomes one turn instead of being answered after the first fragment. Lower it if you would rather have the latency back, but expect the agent to reply mid-thought more often.

**Replies in a busy group are consistently delayed by the same amount.** The chat never goes quiet for a full `settle`, so `settle_max` is releasing every pass rather than acting as an occasional fallback. Lower `settle_max`, or accept it as the price of one turn per burst instead of one per message.

**A file was not downloaded.** It exceeded `attachment_max_bytes`. The `attachment:` line says so, and the agent can tell the user.
