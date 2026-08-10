# MCP Tools

These are the only way a message reaches a person. The bridge never authors chat content of its own.

meka namespaces them by the server name from its config, so with `name = "mekabridge"` the agent sees `mcp__mekabridge__send_message`.

## Why routing is explicit

meka's MCP client sends a progress token and a tool-use id in `_meta` on a `tools/call`, and nothing else. There is no session identity on the wire, so an MCP server cannot work out which conversation a call belongs to.

Every send therefore takes a `conversation` id. The agent reads it off the header attached to each inbound message, or looks it up with `list_conversations`. That constraint is also what makes the interesting behaviour possible: because the target is always explicit, replying to somebody else, replying on a different platform, or messaging first are all the same operation.

## `send_message`

Send a message to a person or group.

| Argument | Type | Meaning |
|----------|------|---------|
| `conversation` | string | Target, e.g. `telegram:123456789` |
| `text` | string | Body, written as Markdown |
| `reply_to` | string, optional | Platform message id to reply to |
| `silent` | bool, optional | Deliver without a notification sound |

Returns the platform message ids produced. Long text is split, so there may be several.

## `send_file`

Send a local file.

| Argument | Type | Meaning |
|----------|------|---------|
| `conversation` | string | Target |
| `path` | string | Absolute path readable by the bridge process |
| `caption` | string, optional | Text shown alongside |
| `as_photo` | bool, optional | Show inline rather than as a download |

Relative paths and missing files are rejected before the platform is contacted, so the agent gets "not a readable file" instead of an opaque upload error.

## `list_conversations`

Known conversations, most recently active first.

| Argument | Type | Meaning |
|----------|------|---------|
| `channel` | string, optional | Restrict to one configured channel |
| `limit` | integer, optional | Default 50, capped at 200 |

This is not garnish. In a session that runs for months, compaction eventually summarises away the older parts of the context. `list_conversations` is how the agent re-derives its address book instead of scrolling back through history that may no longer be there.

## `get_conversation`

One conversation by id, with its title, kind, and last activity.

## Error handling

Failures come back as tool-level errors, not protocol errors. The distinction matters: an MCP client renders a protocol error opaquely, whereas a tool error's text reaches the agent, which can then act on it.

```
no conversation with id "telegram:999"; call list_conversations to see the ids this bridge knows
```

Sends are validated against the conversation store, so the agent can only reach somebody the bridge has actually seen. In practice that is barely a restriction, since Telegram bots cannot initiate anyway, but it turns a hallucinated id into a clear message rather than a platform rejection.

## Transports

Streamable HTTP by default, which is what a long-running daemon wants. stdio is also supported for debugging with MCP Inspector:

```toml
[mcp]
transport = "stdio"
```

## Version skew

mekabridge builds against rmcp 3.x while meka pins 2.x. The two negotiate a mutually supported protocol version at `initialize`. That path is covered by an integration test that runs a real 2.x client against the real server on every `cargo test`, asserting that the handshake, the tool list, the annotations, and the input schemas all survive the gap.
