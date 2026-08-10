# meka Integration

meka and mekabridge are each other's client. Getting the two configurations to agree is most of the setup.

## What meka needs from you

### A token for the bridge

```toml
[[serve.tokens]]
token = "${MEKA_BRIDGE_TOKEN}"
description = "mekabridge"
scopes = ["sessions:r", "sessions:w"]
```

`sessions:w` covers creating sessions, submitting turns, and cancelling. `sessions:r` covers reading session metadata, which `mekabridge doctor` and `session show` use.

### An MCP server entry

```toml
[[mcp.servers]]
name = "mekabridge"
transport = "http"
url = "http://127.0.0.1:9100/mcp"
eager_load_tools = ["send_message", "list_conversations"]
```

If `[mcp].token` is set on the bridge, add the matching `auth_token` here:

```toml
auth_token = "${MEKABRIDGE_MCP_TOKEN}"
```

The `name` becomes the namespace prefix, so the agent sees `mcp__mekabridge__send_message`.

## Eager loading

meka ships MCP tools **deferred** by default: the agent has to call `load_tool` before it can use one. For a bridge that is exactly backwards, because `send_message` is used on almost every turn.

```toml
eager_load_tools = ["send_message", "list_conversations"]
```

Leave `send_file` and `get_conversation` deferred; they are used rarely enough that keeping the tools array lean is worth the occasional round trip.

## Permissions

meka resolves each MCP tool's required permission through a five-step chain. With no override, the
tool's own `readOnlyHint` decides, and every tool this bridge exposes is annotated read-only:

| Tool | `readOnlyHint` | Required level |
|------|----------------|----------------|
| `send_message` | `true` | read |
| `send_file` | `true` | read |
| `list_conversations` | `true` | read |
| `get_conversation` | `true` | read |

The send tools are read-only on purpose. They change nothing on the machine meka runs on, which is
what the hint is about; `openWorldHint: true` carries the caveat that they act outside it.

The alternative would gate replying behind `write`, and that reads badly once you look at what the
levels mean in practice. In every other meka front-end, answering the user is not permission-gated at
all: the REPL just prints the reply. Here it travels as a tool call, which is a property of the
transport rather than a difference in kind. Gating it would make `read` mean "the agent may run
commands, fetch URLs and read every file, but may not say hello", which is not a posture anyone would
pick deliberately, and its failure is silent from both ends.

It also would not contain anything. meka grants `fetch_url` at `read`, so an agent at that level can
already push arbitrary bytes to any host on the internet. These tools reach only conversations
already in the bridge's store, which is to say people on your allowlist, so they are strictly more
constrained than a tool meka already treats as read-only.

If you want sends gated anyway, invert it on meka's side:

```toml
[mcp.servers.tool_permissions]
send_message = "write"
```

Do not run the session at `ask`. meka checks the session level before dispatch, so at `ask` every
call is prompted including read-only ones, and this bridge answers no prompts.

## Start order

Start mekabridge first, then `meka serve`, where you have the choice.

meka connects to its MCP servers at startup and retries a failed connect in the background with
backoff, from five seconds up to five minutes, so the wrong order recovers on its own. What it costs
you is the interval: with `[mcp].strict` at its default of `true`, every turn is rejected while a
configured MCP server is not connected, so a meka that came up first will refuse work for up to a few
minutes with no obvious cause.

Restarting the bridge alone is fine at any point. The transport closes from a `Connected` state, so
meka's reconnect fires immediately rather than waiting on the cold-start backoff.

`mekabridge doctor` reads meka's readiness probe and reports when meka sees an unhealthy MCP server,
which is usually this and usually resolves itself.

If you would rather meka tolerate a missing bridge outright:

```toml
[mcp]
strict = false
```

Turns then run without the bridge's tools, which means the agent reads messages and has no way to
answer them. Useful for keeping meka usable from the REPL while the bridge is down, not much use to
the bridge itself.

### With systemd

`After=` alone only orders start-up, not readiness. Ordering the units is still worth doing to avoid
the refusal window:

```ini
# meka.service
[Unit]
After=mekabridge.service
Requires=mekabridge.service
```

See [Operations](./operations.md) for complete units.

## Session retention

mekabridge's whole model rests on one session living forever, so meka's session GC settings matter
more here than they do for a typical client.

meka evicts an idle session's in-memory state after `[serve].idle_timeout` (24 hours by default) but
keeps the database row, and a later request re-attaches transparently. That is exactly what a bridge
wants: nobody messages the bot overnight, the session is evicted, the next morning's message brings
it back with its history intact.

> **Do not set `[serve].delete_on_idle = true`.** It makes eviction delete the row as well. The
> bridge would then find its session gone, create a replacement, and the assistant would lose every
> conversation it has ever had, once per idle period, with only a warning in the log to show for it.
> The default is `false`; leave it there.

Re-attaching is also safe mid-tool-call as of meka 0.37.0, which drops orphaned tool calls when it
adopts a persisted session. That matters because this bridge can create exactly that state: a
shutdown abandons an in-flight turn after its drain window, and crash recovery requeues the batch, so
a session's log can stop between a tool call and its result.

## Permission prompts

mekabridge creates its session with `supports_permission_prompts: false`, telling meka that this
client has no interface to show an approval prompt on.

With that set, a gated tool call is denied immediately with an explanatory notice instead of parking
the turn on the SSE channel for a minute waiting for an answer that can never arrive.

It is a safety net, not a way to run at `ask`. meka checks the session level before dispatch, so at
`ask` every call is prompted, `send_message` included, and each is denied at once. The agent cannot
reply at all. `doctor` reports `ask` as a failure for that reason.

## Vision and images

When the active provider profile has `vision = true`, mekabridge attaches inbound images to the turn
itself, so the agent simply sees the picture. It checks `vision` on `GET /v1/info` first, because
attaching to a profile without it is a 422.

Two limits apply, and the bridge respects both rather than discovering them from an error:

- meka caps each image at 3.75 MB decoded.
- The whole request is bounded by meka's `[serve].max_body_bytes`, 10 MiB by default.

The second one matters more than it looks, because mekabridge batches: a turn can carry a photo from
each of several messages. The bridge therefore budgets across the turn and falls back to naming a
file by path once the budget is spent. Nothing is lost when that happens; the agent can still open
the file, it just costs a tool call. The envelope says which images travelled with the turn and which
are only on disk.

With `vision = false`, every attachment is named by path, which is how the bridge worked before meka
accepted images at all.

## What the agent is told

meka captures an MCP server's `instructions` from the handshake and surfaces them to the agent. mekabridge uses that to explain the model once, rather than repeating it in every tool description:

> mekabridge connects you to people on messaging platforms such as Telegram. Incoming messages are delivered to you in the user turn, each with a header naming the channel, the conversation id, and who sent it. Nothing is sent back automatically: if you want to reply, call send_message with that conversation id.

On the first turn of a session, the bridge also prepends a short orientation naming the connected channels and the bot identity, which the MCP handshake cannot know.

## Attachments

Inbound files are always downloaded to `[storage].attachment_dir` and named in the envelope, whether
or not they also ride on the turn:

```
attachment: photo, image/jpeg, 2.1 MiB, attached to this message and saved to /var/lib/mekabridge/attachments/abc.jpg
attachment: document "dump.sql", 900.0 MiB, not downloaded: exceeds the configured limit
```

Keeping the path even for an attached image is deliberate: a tool that operates on a file still needs
somewhere to point at.

## Recovering a dropped turn stream

If the connection to meka drops mid-turn, the turn keeps running: meka holds the runtime lock and the
spawned task completes. mekabridge does not resubmit, because that would duplicate a reply the user is
about to receive. It polls `turn_in_flight` on `GET /v1/sessions/{id}` instead, and marks the batch
delivered once the session goes idle, which is the same contract as a turn it watched to completion.

The same field covers the reverse case. A `turn-in-flight` 409 on submit means one of the bridge's own
earlier turns is still going, so it waits and resubmits rather than counting a failed attempt.
