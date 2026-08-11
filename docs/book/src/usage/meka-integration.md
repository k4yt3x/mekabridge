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

Turns carry no images. Inbound files are announced with a handle and fetched only when the agent asks,
so an image enters the context because the agent decided to look at it, not because somebody sent it.

That decision is the one that matters for a permanent session. An image attached to a turn stays in
the history for the life of that session, so auto-attaching meant a photo dropped in a group weeks ago
was still costing tokens on every turn today.

`view_attachment` returns the image as an MCP image block, and meka forwards those to the provider as
multimodal content, so the agent sees the picture in the same call. Two of meka's limits apply and the
bridge screens for both rather than discovering them from a placeholder:

- 10 MiB on a base64 image in a tool result.
- `image/png`, `image/jpeg`, `image/gif`, and `image/webp` only. Anything else meka replaces with a
  text placeholder, so the bridge returns a description naming the file instead.

Telegram photos are JPEG, stickers are WebP, and video stills are JPEG, so all of them pass.

When the active profile reports `vision = false` on `GET /v1/info`, `view_attachment` returns a
description rather than an image. The probe result is cached, since it cannot change without
restarting meka, and a failed probe is not cached so a transient error does not pin the bridge to
"no vision" for the life of the process.

## What the agent is told

meka captures an MCP server's `instructions` from the handshake and surfaces them to the agent. mekabridge uses that to explain the model once, rather than repeating it in every tool description:

> mekabridge connects you to people on messaging platforms such as Telegram. Nothing you write here reaches them. Your turn text, your reasoning, and your tool output are all invisible: the only way to be heard is send_message on a channel.

It goes on to explain each header line, when to send a holding message on a long turn, and how attachments are fetched. Two constraints shape it:

- **meka truncates a server's `instructions` to 2048 characters** at handshake, silently, appending an ellipsis. The value is captured in a `OnceLock` on first connect, so a version that went over would stay cut until meka restarts. `the_instructions_fit_inside_mekas_cap` guards the length; trim a paragraph rather than raising it.
- **It is re-emitted in full after every compaction**, so its length is paid again each time rather than once per session.

These survive compaction. They are not in the system prompt, which meka asserts deliberately, but
meka drops `last_rendered_world` at a compaction boundary and re-states the whole world in full on
the next turn, so the instructions come back on their own.

The one thing the handshake cannot carry is which account the agent appears as, because that comes
from a network probe and `get_info` is synchronous. It rides the envelope instead:

```
[mekabridge] 1 new message.
[mekabridge] You are @MicaAgentBot on telegram.
```

Stated every turn rather than once at session start. A one-time orientation would be an ordinary user
message, so the first compaction would fold it into a summary and nothing would ever restate it,
leaving the agent unable to recognise its own handle when somebody addresses it in a group. A line
per turn costs a few tokens, is always current, and survives a rename without a restart.

## Attachments

Inbound files are announced with a handle and fetched on demand:

```
attachment: photo, image/jpeg, 2.1 MiB [417]
attachment: document, "dump.sql", 900.0 MiB [418]
```

The agent passes the handle to `view_attachment` or `download_attachment`. Handles are minted when the
message is queued, so they survive a restart, and a redelivered message reuses the handle it was
already given rather than minting a second one for the same file.

Anything downloaded is recorded, so `[storage].attachment_retention` reclaims it later. Files the agent
never asked for cost nothing, because they were never fetched.

## Recovering a dropped turn stream

If the connection to meka drops mid-turn, the turn keeps running: meka holds the runtime lock and the
spawned task completes. mekabridge does not resubmit, because that would duplicate a reply the user is
about to receive. It polls `turn_in_flight` on `GET /v1/sessions/{id}` instead, and marks the batch
delivered once the session goes idle, which is the same contract as a turn it watched to completion.

The same field covers the reverse case. A `turn-in-flight` 409 on submit means one of the bridge's own
earlier turns is still going, so it waits and resubmits rather than counting a failed attempt.
