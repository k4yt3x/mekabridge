# MCP Tools

These are the only way a message reaches a person. The bridge never authors chat content of its own.

meka namespaces them by the server name from its config, so with `name = "mekabridge"` the agent sees `mcp__mekabridge__send_message`.

## Why routing is explicit

meka's MCP client sends a progress token and a tool-use id in `_meta` on a `tools/call`, and nothing else. There is no session identity on the wire, so an MCP server cannot work out which conversation a call belongs to.

Every send therefore takes a `conversation` id. The agent reads it off the header attached to each inbound message, looks it up with `list_conversations`, or is simply told one in its own instructions. That constraint is also what makes the interesting behaviour possible: because the target is always explicit, replying to somebody else, replying on a different platform, or messaging first are all the same operation.

Any well-formed id naming a configured channel is accepted, whether or not the bridge has seen that conversation before. Whether the chat can actually be written to is the platform's judgement, not the bridge's.

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

## `edit_message`

Replace the text of a message the agent sent.

| Argument | Type | Meaning |
|----------|------|---------|
| `conversation` | string | Conversation the message is in |
| `message_id` | string | Id returned by `send_message` |
| `text` | string | Replacement body, as Markdown |

The new text replaces the old entirely. Correcting a reply in place is what a person does; the alternative is a second message saying "sorry, I meant".

An edit is one message, so replacement text long enough to need splitting is refused rather than truncated. Telegram also declines to edit messages older than 48 hours, and passes that back verbatim.

## `delete_message`

Remove a message.

| Argument | Type | Meaning |
|----------|------|---------|
| `conversation` | string | Conversation the message is in |
| `message_id` | string | Message to delete |

The agent's own messages anywhere, and anyone's in a group where it is an administrator with the delete right. This cannot be undone and the message disappears for everyone, so it is logged at warn: that log line is the only remaining record.

## `mute`, `unmute`, `block`, and `unblock`

How much of a conversation reaches the agent. All four take the same arguments; `unmute` and `unblock` take only `conversation`.

| Argument | Type | Meaning |
|----------|------|---------|
| `conversation` | string | Chat to rule on |
| `duration` | string, optional | `30m`, `2h`, `7d`. Omit to leave it until changed |
| `reason` | string, optional | Recorded alongside, shown when listing |

**`mute`** turns a conversation down to mentions only. The agent is still woken when somebody mentions it or replies to it, and for `mute_followup` after it has spoken there. Everything else is received and **recorded**, so `read_history` and `search_history` reach it, and the next thing that does wake the conversation says how much accumulated.

**`block`** stops a conversation reaching the agent at all. Nothing is delivered and nothing is kept, so unlike a mute there is no way to read afterwards what was said; the agent is only told how many messages went. It is the heavier of the two and belongs on a chat there is no reason to read later.

Both are attention management, not access control. An allowlist decides who may speak to the agent; these decide what it is worth being woken for. Neither consumes queue depth or a provider turn.

Muting a one-to-one chat is refused: every message there is addressed to the agent, so it would change nothing, and reporting success for a no-op is worse than saying so.

`unmute` and `unblock` both set the conversation to `active`, which is an explicit override rather than a return to the default. That distinction matters when the default for the chat's kind is `mute`: to go back to following the default, an operator uses `mekabridge policy clear`.

A decision the agent made can only be undone by the agent or by an operator. That matters if it silences the conversation you would use to ask it to stop:

```console
$ mekabridge policy list
$ mekabridge policy clear telegram:-1001234567890
```

## `read_history` and `search_history`

Read back what was said, including messages the agent was never woken for.

| Argument | Type | Meaning |
|----------|------|---------|
| `conversation` | string | Chat to read. Optional for `search_history`, which spans all of them without it |
| `query` | string | `search_history` only. `a OR b`, `a NOT b`, and `"quoted phrases"` work |
| `limit` | number, optional | Default 20, capped at 100 |
| `before` | number, optional | `read_history` only. The `cursor` of the oldest message you were given, to page further back |

This is what makes `mute` usable: somebody mentions the agent halfway through a discussion, and the discussion is here. `mute_context` already prints the last few alongside the mention, so these are for going deeper.

Both read only what the bridge recorded. That means nothing from before the bridge was installed, nothing past `history_retention`, nothing at all if it is `0s`, and nothing from a blocked conversation. An empty result says which of those it might be, rather than implying the chat was silent.

Attachment handles come back with each message, so a picture found in history can go straight to `view_attachment` while it is still within `attachment_retention`.

Each message also carries a `cursor`, which is what `before` takes. It is deliberately not a timestamp: Telegram stamps to the second, so a burst shares one, and paging on a timestamp would drop the siblings of the message paged from without anything saying so.

## `moderate_member`

Restrict, ban, or reinstate somebody in a group. Present only when `admin_tools` is on.

| Argument | Type | Meaning |
|----------|------|---------|
| `conversation` | string | Group to act in |
| `user_id` | string | Numeric id from a header's `from:` line |
| `action` | enum | `restrict`, `unrestrict`, `ban`, `unban`, `kick` |
| `duration` | string, optional | For `restrict` and `ban` only |
| `revoke_messages` | bool, optional | Also delete their history. Cannot be undone |

`unrestrict` restores whatever the group allows ordinary members, read back from the group rather than assumed, so it cannot leave somebody with more than everyone else has. `kick` removes without banning, which Telegram expresses as a ban lifted immediately.

**Durations must be between 30 seconds and 366 days.** Telegram treats anything outside that window as permanent, silently, so the bridge refuses it rather than letting a ten-second mute become a life sentence. A duration passed to an action that ignores it is refused for the same reason.

Anonymous admins and channel posts have no user id and cannot be moderated this way.

## `set_member_rights`

Promote, adjust, or demote an administrator.

| Argument | Type | Meaning |
|----------|------|---------|
| `conversation` | string | Group to act in |
| `user_id` | string | Numeric id |
| `rights` | array | The complete set they should end up with |

The list replaces what they hold rather than adding to it, so an empty list demotes. Telegram lets a bot grant only privileges it holds itself.

## `pin_message` and `set_chat`

`pin_message` takes `conversation`, `message_id`, `pin` (false to unpin), and optional `silent`. `set_chat` takes `conversation` and an optional `title` and `description`; omitted fields are left alone.

## `member`

Somebody's standing in a chat and the privileges they hold.

| Argument | Type | Meaning |
|----------|------|---------|
| `conversation` | string | Chat to look in |
| `user_id` | string, optional | Omit to ask about the bot itself |

Omitting `user_id` is the useful case: it lets the agent find out what it is allowed to do in a group before trying, rather than discovering it from a failure. An owner is reported as holding everything, which is what Telegram means by sending no rights flags at all for one.

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

Each entry carries `policy` (the one actually in force, whether from an explicit decision or the configured default), `policy_until` when somebody ruled on it explicitly, and `unseen` for how much a muted conversation is holding.

## `get_conversation`

One conversation by id, with its title, kind, and last activity.

## Error handling

Failures come back as tool-level errors, not protocol errors. The distinction matters: an MCP client renders a protocol error opaquely, whereas a tool error's text reaches the agent, which can then act on it.

```
"telegram-999" is not a conversation id; the form is <channel>:<chat>, for example `telegram:123456789`
```

Only two things are refused here: an id that is not well formed, and one naming a channel that is not configured. Everything else is the platform's call, and its own wording is passed through verbatim, so "chat not found" or "bot can't initiate conversation with a user" reaches the agent as written.

## Permission level

Every tool is annotated `readOnlyHint: true`. meka derives a tool's required permission from that annotation, so the alternative would place them at `write`, where a bridge running at `read` would understand every message and be unable to answer any of them.

`download_attachment` is the only one that genuinely writes, and only into `[storage].attachment_dir`, which exists for exactly that, is bounded by `attachment_max_bytes`, and is swept on `attachment_retention`.

Everything that reaches the platform also carries `openWorldHint: true`, which is the accurate caveat: those change nothing locally, but they do act on the outside world. The policy and history tools do not, since they only change or read what this bridge itself holds.

See [Security](./security.md) for gating individual tools and for what the moderation group can reach.

## Transports

Streamable HTTP by default, which is what a long-running daemon wants. stdio is also supported for debugging with MCP Inspector:

```toml
[mcp]
transport = "stdio"
```

## Version skew

mekabridge builds against rmcp 3.x while meka pins 2.x. The two negotiate a mutually supported protocol version at `initialize`. That path is covered by an integration test that runs a real 2.x client against the real server on every `cargo test`, asserting that the handshake, the tool list, the annotations, and the input schemas all survive the gap.
