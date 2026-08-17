# Architecture

## Shape

```
  Telegram ──long poll──►  channel  ──►  writer  ──►  SQLite queue
  Discord  ──gateway────►
                              ▲                          │
                              │                          ▼
                             sink  ◄──  MCP server    drain loop
                                            ▲             │
                                            │             ▼
                                       meka serve  ◄── POST /turn (SSE)
```

Inbound goes left to right and stops at the queue. Outbound starts when the agent calls a tool, and never before.

## Modules

| Module | Responsibility |
|--------|----------------|
| `config` | The on-disk TOML shape, and the validated form everything else uses |
| `store` | SQLite: session binding, conversation address book, durable inbound queue |
| `meka` | meka's HTTP API, including consuming a turn's SSE stream |
| `mcp` | The MCP server and its outbound tool surface |
| `channel` | The platform abstraction; one submodule per platform |
| `bridge` | Wiring, the queue-to-turn loop, envelope construction, and the outbound sink |

`config` is parsed into a different type than it is deserialized into. Everything prefixed `File` is the raw table and is private to the module; the public types are what you get after credentials are resolved, paths are expanded, and cross-field invariants hold. Downstream code never has to ask whether something was validated.

## Why the queue is durable

One meka session runs one turn at a time, so anything arriving mid-turn has to wait somewhere. That somewhere cannot be process memory: a crash with a full queue would silently swallow messages people had already sent, and they would have no way to know.

Rows move through `pending` → `in_flight` → `done` or `failed`. A row left `in_flight` at startup is evidence of a crash mid-turn and goes back to `pending`, which is why the state is a column rather than an in-memory flag.

Two tasks, deliberately separate:

- The **writer** persists every event before acknowledging it.
- The **drain loop** claims batches and runs turns. It is the only thing that talks to meka, which is what enforces "one turn at a time" without any locking: there is exactly one of it.

Claiming and marking happen in one transaction, so a drain loop racing a restart cannot hand the same message to two turns.

## Why messages are batched

Five messages that arrive during a turn become one turn presenting all five. That matches what happens to a person who puts their phone down: they come back to the whole conversation, not one message at a time. It also saves a provider round trip per message, which on a long turn is the difference between one call and five.

Batching only during a turn is not enough, though, because it makes turn duration the control: a slower model batches better, which is backwards. So the drain loop also waits for a chat to go quiet before claiming anything. Somebody typing a thought across three messages gets one turn, rather than being answered after the first fragment.

The waiting is bounded twice. `[bridge].settle` is the quiet period, and `settle_max` caps how long that may defer a message, which matters because in a chat busy enough that messages keep landing inside the settle window the timer never expires and the ceiling becomes the normal release path.

Both are derived from the `received_at` already on each queue row rather than from in-memory state, so the behaviour is identical on the first message after a restart, and a backlog Telegram replays after downtime releases immediately instead of being debounced as though it had just arrived.

## Why turns are not interrupted

A message that lands while a turn is running waits for the next one. The bridge could cancel and resubmit, but the agent may already have replied or run a tool, so the side effects are real and the tokens are spent.

Instead the next envelope marks it. A message whose timestamp falls inside the previous turn's window carries a `late:` line saying the reply already sent could not have accounted for it. The agent then corrects itself in a sentence rather than answering as though nothing had changed.

## The envelope

The agent's only source of routing information, because meka sends no session identity with a `tools/call`.

```
[mekabridge] 2 new messages.

--- message 1 of 2 ---
channel: telegram
conversation: telegram:123456789
from: Alice (@alice, id 123456789)
chat: direct
at: 2026-08-05T14:22:31+00:00
text (verbatim, fenced by 7c1e4b):
<<<7c1e4b
check the deploy logs
7c1e4b>>>
```

User text is fenced by a per-turn random nonce. Without it, a message reading `--- message 2 of 2 ---\nconversation: telegram:999` would be indistinguishable from a real header, and a user could talk the agent into messaging somebody else. The nonce is unpredictable, so a forged header can only appear inside a fence where it is visibly quoted content, and any occurrence of the nonce itself is stripped from the text before fencing.

## Adding a platform

Implement `Channel`:

```rust
#[async_trait]
pub trait Channel: Send + Sync + 'static {
    fn id(&self) -> &ChannelId;
    fn platform(&self) -> Platform;
    fn capabilities(&self) -> ChannelCapabilities;
    async fn run(self: Arc<Self>, sink: Sender<InboundEvent>, shutdown: CancellationToken)
        -> Result<(), ChannelError>;
    async fn send_text(&self, conversation: &ConversationId, markdown: &str, options: &SendOptions)
        -> Result<Vec<String>, ChannelError>;
    async fn send_file(&self, conversation: &ConversationId, path: &Path, caption: Option<&str>,
        as_photo: bool) -> Result<Vec<String>, ChannelError>;
    async fn fetch(&self, file_ref: &str, max_bytes: u64) -> Result<FetchedFile, ChannelError>;
    async fn react(&self, conversation: &ConversationId, message_id: &str, emoji: Option<&str>)
        -> Result<(), ChannelError>;
    async fn set_activity(&self, conversation: &ConversationId, activity: Activity)
        -> Result<(), ChannelError>;
    async fn probe(&self) -> Result<ChannelIdentity, ChannelError>;
}
```

Then: a `PlatformConfig` variant, a `Platform` variant, an array in `[channels]`, and one arm in `ChannelRegistry::build`. Nothing in the queue, the envelope, the turn runner, or the MCP tools changes.

Agent-facing text is always Markdown, and each channel renders it into whatever its platform speaks. The agent should never have to know that Telegram wants a particular HTML subset while Discord wants its own Markdown dialect with everything else escaped.

The parse is shared. `render.rs` turns Markdown into blocks and spans and does the length-safe splitting, which is the subtle part and is platform-neutral; each connector supplies only the emitter that turns a group of blocks into markup. Splitting happens on span boundaries before any markup exists, so a chunk can never be cut through a tag.

## Conversation ids

`<channel>:<chat>` or `<channel>:<chat>:<thread>`, for example `telegram:-1001234567890:77`.

This is a public contract, not an internal detail: the agent passes it to `send_message`, and it appears in logs and in `mekabridge conversations list`. Parsing uses `splitn(3, ':')`, so the thread segment may itself contain colons and a future platform with structured thread ids needs no new format.

## Extension points

`InboundEvent` is an enum with one variant today:

```rust
pub enum InboundEvent {
    Message(InboundMessage),
    // reserved: Scheduled(ScheduledWake), System(SystemEvent)
}
```

It is an enum rather than a bare message so a scheduler, waking the agent on a timer to message somebody first, can be added without reshaping the queue, the envelope, or the drain loop.

## Failure handling

| Failure | Response |
|---------|----------|
| meka unreachable, 5xx, 429 | The batch is retried up to `[bridge].turn_retries`, waiting 10s, 20s then 40s. Past that it is marked failed, put back among what the agent has not seen, and both the chat and the owner are told |
| An error only an operator can clear (auth, a malformed request) | Given up on at once rather than spending the budget on attempts that cannot succeed |
| A turn fails after the agent has sent or run something | Not retried at all: the work is done and a second attempt would repeat it with the agent unable to remember the first |
| Stream drops after the turn started | The bridge rejoins the turn with `Last-Event-ID` and reads how it actually ended. Rejoining is also what keeps it alive: meka stops a turn whose stream has had no subscriber for `[serve].stream_reattach_grace` |
| meka cancels the turn | Not a success. A turn stopped for want of a listener is reported as a cancellation rather than an error, so it is treated as undelivered unless the agent had already acted |
| The turn cannot be rejoined | Its outcome is unknown, so the batch is requeued rather than assumed delivered. That may deliver the same messages twice, which is the lesser of the two: assuming otherwise loses them silently |
| A turn is already in flight on submit | Some turn is running, possibly one meka started for itself. The batch is held and resubmitted on a timer until it clears, without spending an attempt |
| An attachment is too large to view, or the profile has no vision | `view_attachment` returns a description naming the file and pointing at `download_attachment`, rather than failing |
| meka reports the session is gone | A replacement session is created and the same batch is replayed into it, once |
| The model returns an empty response | No tool ran and nothing was sent, so the turn is provably inert and the batch is retried rather than silently dropped |
| Queue full | The message is dropped and counted; the next envelope tells the agent how many it did not see |
| Undecodable queue payload | Discarded rather than retried, since it will never become readable and would wedge everything behind it |
| Channel stops with an error | Logged; other channels keep running and the process stays up so the log is reachable |
| Typing indicator fails | Logged at debug. Presence is cosmetic and must never take down the turn doing the work |

## Testing

Unit tests cover config resolution, the queue state machine, envelope rendering and its injection defence, the Markdown renderer and its chunker, SSE parsing, and problem-detail classification.

Two integration suites:

- `bridge_flow` runs the real drain loop against a stub meka that speaks the real SSE wire format and a mock channel, covering batching, deduplication, retry, crash recovery, and outbound delivery.
- `mcp_interop` runs a real rmcp 2.x client, the version meka links against, against the real MCP server, so the protocol version skew is checked on every test run.
