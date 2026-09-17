# Architecture

## Shape

```
  Telegram ──long poll──►  channel  ──►  writer  ──►  SQLite queue
  Discord  ──gateway────►                                │
                              ▲                          ▼
                             sink  ◄──  MCP server    session task ──POST /inbox──┐
                                            ▲             ▲                       │
                                            │             │                       ▼
                                       meka serve  ◄──  feed reader ◄──GET /stream─┘
```

Inbound goes left to right and stops at the queue. Outbound starts when the agent calls a tool, and never before.

Two connections to meka, and they do opposite things. Messages go **out** through the session inbox, which answers as soon as the item is durable on meka's side and never waits for a turn. What became of them comes **back** on the session feed, one long-lived connection carrying every turn on the session, whoever started it.

## Modules

| Module | Responsibility |
|--------|----------------|
| `config` | The on-disk TOML shape, and the validated form everything else uses |
| `store` | SQLite: session binding, the accounts each channel speaks as, conversation address book, durable inbound queue, message history |
| `meka` | meka's HTTP API: the session inbox, the session feed, and the error taxonomy |
| `mcp` | The MCP server and its outbound tool surface |
| `channel` | The platform abstraction; one submodule per platform |
| `bridge` | Wiring, the feed reader, the session task, inbox-item construction, and the outbound sink |

`config` is parsed into a different type than it is deserialized into. Everything prefixed `File` is the raw table and is private to the module; the public types are what you get after credentials are resolved, paths are expanded, and cross-field invariants hold. Downstream code never has to ask whether something was validated.

## Why the queue is durable

A message is written down before the platform is told it arrived. That cannot be process memory: a crash with a full buffer would silently swallow messages people had already sent, and they would have no way to know.

It stays there until meka says the item carrying it is durable on *its* side, and is only marked delivered once the feed says the model actually read it. A row moves through `pending` → `in_flight` → `posted` → `done` or `failed`, and its state is a column rather than an in-memory flag so a restart finds every hand-over where it was.

Three tasks, deliberately separate:

- The **writer** persists every event before acknowledging it.
- The **feed reader** holds `GET /v1/sessions/{id}/stream` open and hands every frame straight on. It touches nothing and decides nothing, which is what keeps meka's per-consumer buffer drained while the session task is busy with a request of its own.
- The **session task** claims messages, renders and posts them, and acts on what the feed says. It is the only writer of queue state, so the bridge never races itself.

Claiming happens in one transaction, so a session task racing a restart cannot hand the same message over twice.

## Why a hand-over is written down

One claimed message becomes one **inbox item**, and its queue row *is* the hand-over: the same row holds the `Idempotency-Key`, the rendered item byte for byte, and whatever that item stated once and nothing else will state again.

Two things fall into that last category. The count of messages the queue had to shed is carried by the first item of a claimed pass. A conversation's backlog is carried by the first item from that conversation, and every history row it named is marked seen against that row (`messages.accounted_by`) in the same transaction that stores the item.

All of it exists for the same moment: a bridge that stops between posting an item and hearing what became of it. On the next start it posts the same bytes under the same key, and meka answers with the item it already made and that item's current state rather than making a second. If the item is never read, the shed count goes back on the counter and exactly the rows it named are owed again.

Per row rather than per batch, because two hand-overs can be outstanding for one conversation at once. What has already been reported is then a *set*, not a high-water mark: the first item states the backlog and marks it seen, the second counts only what has landed since, and if the first dies it gives back its own half while the second keeps its own. A watermark cannot express that.

The same question is asked whenever the feed reconnects, and whenever meka says a replay had a hole in it. A hand-over is never left waiting on an outcome that has already been and gone.

## Why messages still arrive together

Five messages that settle together are claimed together and posted as five items, one after another. meka reads them all into one turn -- whatever is waiting when it opens, and the rest at its next round boundaries -- so the agent sees all five in that turn under a header meka writes above each. That matches what happens to a person who puts their phone down: they come back to the whole conversation, not one message at a time. It also saves a provider round trip per message, which on a long turn is the difference between one call and five.

The batching is meka's, not the bridge's. Doing it here as well would mean a second unit of hand-over with its own state, its own retries and its own idea of what had been reported, all describing something the server already does.

What decides when they go is the chat going quiet, not a turn running. The session task waits for that before claiming anything, so somebody typing a thought across three messages is read once rather than answered after the first fragment. Tying it to a turn instead would make turn duration the control, and a slower model would batch better, which is backwards.

The waiting is bounded twice. `[bridge].settle` is the quiet period, and `settle_max` caps how long that may defer a message, which matters because in a chat busy enough that messages keep landing inside the settle window the timer never expires and the ceiling becomes the normal release path.

Both are derived from the `received_at` already on each queue row rather than from in-memory state, so the behaviour is identical on the first message after a restart, and a backlog Telegram replays after downtime releases immediately instead of being debounced as though it had just arrived.

## How a message reaches a turn already running

It is handed over the moment it settles, whatever meka is doing. Every item is posted as a `steer`, which meka reads into the running turn at its next round boundary, after that round's tool results and before the next request. So somebody correcting themselves ten seconds into a ten-minute task is read inside that task, the way a person glances at a message mid-job.

Nothing is cancelled to make room, and nothing waits for a turn to end. A message that lands during a scheduled job or a background-outcome turn is read by that turn.

meka writes its own header above each item, naming this bridge as the sender, when it arrived, and whether it landed mid-turn. That replaces the `late:` line the bridge used to add: it was a claim about a reply that had already gone out, which the bridge could only guess at, and there is no longer a gap for it to describe.

## What one item looks like

The agent's only source of routing information, because meka sends no session identity with a `tools/call`.

```
[mekabridge] You are @mybot on telegram.

channel: telegram
conversation: telegram:123456789
message: 4821
from: Alice (@alice, id 123456789)
admitted: user allowlist
chat: direct
at: 2026-08-05T14:22:31+00:00
text (verbatim, fenced by 7c1e4b):
<<<7c1e4b
check the deploy logs
7c1e4b>>>
```

One item per message, so there is nothing to number and no count to state: meka writes its own header above each item, naming the sender and when it arrived, and a line here saying the same thing could only disagree with it.

User text is fenced by a per-item random nonce. Without it, a message reading `conversation: telegram:999` would be indistinguishable from a real header, and a user could talk the agent into messaging somebody else. The nonce is unpredictable, so a forged header can only appear inside a fence where it is visibly quoted content, and any occurrence of the nonce itself is stripped from the text before fencing.

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

Then: a `PlatformConfig` variant, a `Platform` variant, an array in `[channels]`, and one arm in `ChannelRegistry::build`. Nothing in the queue, the item renderer, the session task, or the MCP tools changes.

Agent-facing text is always Markdown, and each channel renders it into whatever its platform speaks. The agent should never have to know that Telegram wants a particular HTML subset while Discord wants its own Markdown dialect with everything else escaped.

The parse is shared. `render.rs` turns Markdown into blocks and spans and does the length-safe splitting, which is the subtle part and is platform-neutral; each connector supplies only the emitter that turns a group of blocks into markup. Splitting happens on span boundaries before any markup exists, so a chunk can never be cut through a tag.

## Conversation ids

`<channel>:<chat>` or `<channel>:<chat>:<thread>`, for example `telegram:-1001234567890:77`.

This is a public contract, not an internal detail: the agent passes it to `send_message`, and it appears in logs and in `mekabridge conversations list`. Parsing uses `splitn(3, ':')`, so the thread segment may itself contain colons and a future platform with structured thread ids needs no new format.

The address is deliberately not the whole identity of what is stored under it. A platform message id is only unique within the bot account that issued it: a Telegram bot deleted and recreated under the same channel numbers its private chats from 1 again, at the same addresses. So the store records which account each channel is logged in as, asked of the platform at startup, and keys every message, queue row, and attachment on the account, the address, the platform message id, and the revision (the edit time, or zero for the original). The address itself stays stable across a bot swap, which is what keeps policies, `owner_conversation`, and the ids in the agent's memory working; what changes is that messages recorded under the previous account are marked `previous_account` when read back, since their ids cannot be acted on from the new one.

## Extension points

`InboundEvent` is an enum with one variant today:

```rust
pub enum InboundEvent {
    Message(InboundMessage),
    // reserved: Scheduled(ScheduledWake), System(SystemEvent)
}
```

It is an enum rather than a bare message so a scheduler, waking the agent on a timer to message somebody first, can be added without reshaping the queue, the item renderer, or the hand-over.

## Failure handling

The line running through all of it: a hand-over is spent when the feed says the **model read it**, and at no other point. Nothing is inferred from a request having been accepted, or from a turn having ended.

| Failure | Response |
|---------|----------|
| meka unreachable, 5xx, 429, or another process holding the session | The same bytes are posted again under the same key, waiting 10s and doubling to 5m. Past an hour from when the item was rendered it is given up on, put back among what the agent has not seen, and both the chat and the owner are told |
| An error only an operator can clear (auth, a malformed request, a context overflow) | Given up on at once rather than spending the hour on attempts that cannot succeed |
| meka gives up on the item (`inbox.failed`) | Its own hour of retries is spent. The messages are owed to the agent again and both the chat and the owner are told |
| The item is taken back unread (`inbox.withdrawn`), as cancelling the turn it opened does | Nothing was read, so the messages go back into the queue and are handed over afresh, a bounded number of times |
| A turn fails or is cancelled after the model read the messages | Not handed over again: the work is done and a second run would repeat it with the agent unable to remember the first. The owner is told; the chats are told only where the agent had not got a word in |
| The feed connection drops | Reopened from the last event acted on, and meka replays what was missed across turns. Every hand-over still out is then asked about, so an outcome reported while nothing was listening is not waited on for ever |
| meka says a replay had a hole in it | The same question, for every hand-over out. The notice is the cue rather than the mechanism, so a rewording costs nothing |
| An attachment is too large to view, or the profile has no vision | `view_attachment` returns a description naming the file and pointing at `download_attachment`, rather than failing |
| meka reports the session is gone | A replacement is bound and anything not yet accepted posted into it. Anything the old session had is closed, and its messages come back as a backlog rather than being replayed blind |
| The model returns an empty response | No tool ran and nothing was sent, so the turn is provably inert and the messages are offered again rather than silently dropped |
| Queue full | The message is dropped and counted; the next item handed over tells the agent how many it did not see |
| Undecodable queue payload | Discarded rather than offered again, since it will never become readable and would wedge everything behind it |
| Channel stops with an error | Logged; other channels keep running and the process stays up so the log is reachable |
| Typing indicator fails | Logged at debug. Presence is cosmetic and must never take down the work it decorates |

## Testing

Unit tests cover config resolution, the queue state machine, item rendering and its injection defence, the Markdown renderer and its chunker, SSE parsing, and problem-detail classification.

Two integration suites:

- `bridge_flow` runs the real session task and feed reader against a stub meka that speaks the real inbox and feed wire formats, drains its inbox into one turn the way meka does, and a mock channel, covering batching, deduplication, the hand-over's whole lifecycle, restart recovery, and outbound delivery.
- `mcp_interop` runs a real rmcp 2.x client, one major version behind the server's own, against the real MCP server, so the protocol version skew is checked on every test run.
