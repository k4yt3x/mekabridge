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
| `reply_to` | string, optional | Message id to reply to, from a header's `message:` line |
| `silent` | bool, optional | Deliver without a notification sound |

Returns the platform message ids produced. Long text is split, so there may be several.

`reply_to` quotes the message being answered, which is worth doing in a busy group or when picking up something said a while ago. Only the first part of a split reply carries the quote; repeating it on every part is noise.

## `send_file`

Send a local file.

| Argument | Type | Meaning |
|----------|------|---------|
| `conversation` | string | Target |
| `path` | string | Absolute path readable by the bridge process |
| `caption` | string, optional | Text shown alongside |
| `as_photo` | bool, optional | Show inline rather than as a download |

Relative paths and missing files are rejected before the platform is contacted, so the agent gets "not a readable file" instead of an opaque upload error.

## `react`

Attach an emoji reaction to a message.

| Argument | Type | Meaning |
|----------|------|---------|
| `conversation` | string | Conversation the message is in |
| `message_id` | string | From the header's `message:` line |
| `emoji` | string, optional | Omit to remove a reaction added earlier |

Reacting costs no message and sends no notification, which makes it the right answer to something that needs acknowledging but not answering, or a way to signal "seen, will reply properly later" while a long turn runs.

The bridge never reacts on its own. Like sending, this happens only because the agent decided it should.

Telegram accepts a fixed set of emoji and allows bots one reaction per message. The set changes, so the bridge does not keep its own copy: it sends what the agent chose and passes Telegram's rejection back verbatim.

## `view_attachment`

Look at a picture, without writing anything to disk.

| Argument | Type | Meaning |
|----------|------|---------|
| `attachment` | string | The handle in square brackets on an `attachment:` line |

Returns the image itself, which meka forwards to the provider as a multimodal block, so the agent sees the picture in that same call.

Videos, animations, and animated stickers resolve to the still frame the platform already generated, so this works for them without any transcoding. Anything with no viewable form, such as a PDF or a voice note, comes back as a description naming the file and pointing at `download_attachment` instead. So does everything when the active profile has no vision.

## `download_attachment`

Write a file to disk and get its path back.

| Argument | Type | Meaning |
|----------|------|---------|
| `attachment` | string | The handle in square brackets on an `attachment:` line |

For the cases where the agent needs the file rather than a look at it: reading a document, running a tool over an archive. Files land in `[storage].attachment_dir` and are bounded by `attachment_max_bytes`. Calling it twice returns the same path without fetching again.

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

## Permission level

Every tool is annotated `readOnlyHint: true`. meka derives a tool's required permission from that annotation, so the alternative would place them at `write`, where a bridge running at `read` would understand every message and be unable to answer any of them.

That is honest for four of the six. `download_attachment` does write a file, but only into `[storage].attachment_dir`, which exists for exactly that, is bounded by `attachment_max_bytes`, and is swept on `attachment_retention`.

The five that reach the platform also carry `openWorldHint: true`, which is the accurate caveat: they change nothing locally, but they do act on the outside world.

## Transports

Streamable HTTP by default, which is what a long-running daemon wants. stdio is also supported for debugging with MCP Inspector:

```toml
[mcp]
transport = "stdio"
```

## Version skew

mekabridge builds against rmcp 3.x while meka pins 2.x. The two negotiate a mutually supported protocol version at `initialize`. That path is covered by an integration test that runs a real 2.x client against the real server on every `cargo test`, asserting that the handshake, the tool list, the annotations, and the input schemas all survive the gap.
