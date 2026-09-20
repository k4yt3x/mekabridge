# Group Attention

A busy group is the case where a bridge either works or becomes unusable. Every message that reaches
the agent costs a provider turn, and every message the agent is woken for reads, from the inside,
like something it is expected to answer. Get this wrong in either direction and the bot is
insufferable or deaf.

The bridge's side of it is deliberately small: decide what wakes the agent, record everything else,
and say plainly which is which. Everything about *following a conversation on* belongs to the agent,
because the agent is the only party that knows whether it is done.

## What wakes the agent

In a conversation on `mute`, which is the default for groups and server channels:

- Somebody **names** it.
- Somebody uses their client's **reply** button on one of its messages. This counts even with the
  ping turned off on Discord, because answering the agent is addressing it however the client
  renders it.
- The message matches a **watch** the agent set. See [Watches](#watches).

That is the whole list. Somebody answering the agent in ordinary prose, without naming it and
without using reply, does not reach it, and neither does a follow-up typed straight after it has
spoken. `@everyone`, `@here`, and role pings deliberately do not count either.

The first two are somebody else's decision about the agent. The third is the agent's own, which is
why it is the only one that can be set and unset at runtime.

Everything withheld is still **recorded**. `history_read` and `history_search` reach it, and the
next thing that does wake the conversation arrives with a count of what accumulated and the last
`mute_context` messages inline.

## Every message says why it is there

Every message from a chat that is not one-to-one carries a `woke you:` line:

```
woke you: you were named
woke you: you were named, or this replies to something you said
woke you: nothing here named you; this chat was being heard in full when it arrived
woke you: this matched your watch "spam-signatures" (`看我简介`, spam signature) in the text
woke you: this matched your watch "pump-and-dump" (all 3 patterns) in the text
woke you: you were named, and this matched your watch "deploys" (`deploy`)
```

The third is written in the past tense on purpose. It reports why the message was delivered, which
is not always the same as how the conversation is set now: a message can sit in the queue through a
turn lasting minutes, and the agent may have muted the room in the meantime.

The line is stated for every such message, including the ones nothing addressed. Printing it only
for a mention would make its absence the signal, and an absent line is not one: overheard chatter
and a chat being heard in full would render identically.

## What the agent does instead of a window

Before 0.7.0 the bridge kept a muted conversation open for five minutes after the agent spoke, on
the theory that an exchange already under way should carry on without a second mention. In a busy
room it delivered the room, in items indistinguishable from a message addressed to the agent,
and each reply the agent was nudged into making pushed the window out again. It is gone.

What replaces it is four things the agent asks for, none of which involves the bridge guessing.

### Look back once

The agent answers, is done for now, and wants one glance in a few minutes to see whether its answer
landed. meka's own scheduler does this, and needs nothing from the bridge:

```
schedule_create(
  at: "5m",
  prompt: "You argued for rolling back in the deploy group (telegram:-1001234567890).
           history_read it and see whether anyone pushed back or asked a follow-up
           without naming you. Reply only if something is actually owed."
)
```

One turn, at a time the agent picked, framed by the agent as a look-back rather than a summons. The
prompt is delivered with no human present, so it has to carry its own context.

### Listen for a word

A look-back is for one conversation the agent is already in the middle of. A watch is standing: it
wakes the agent in a room it has otherwise turned down, whenever a message matches a pattern it set.

```
watch_write(name: "my-deploy", patterns: ["rolling back", "rollback"], reason: "I own this deploy")
```

See [Watches](#watches) below.

### Gate a scheduled job

For a room worth sweeping on a timer, a recurring job with a gate costs nothing while the room is
quiet. `backlog_check` is the gate, and a tool gate runs at `read`:

```
schedule_create(
  every: "2m",
  gate: {
    check: { tool: "mcp__mekabridge__backlog_check",
             arguments: { conversation: "telegram:-1001234567890" } },
    when: { at: "/latest", is: "changed" }
  },
  prompt: "Something new was said in the deploy group since you last looked. ..."
)
```

The pointer matters. `latest` moves when, and only when, somebody else says something that still
stands; `unseen` is the backlog and falls to zero every time an ordinary turn sweeps the
conversation, so a gate on that would fire on the sweep and announce news the agent had just been
handed. Gating on the whole result has the same fault, since it contains both.

`mekabridge unseen` is the shell form of the same question, for a gate outside meka or a job that
predates the tool:

```
schedule_create(
  every: "2m",
  gate: { check: { command: "mekabridge unseen telegram:-1001234567890" }, when: "changed" },
  prompt: "..."
)
```

Prefer the tool gate. A shell gate runs unattended with no sandbox, so meka requires
`permission = "unrestricted"` to create one; a tool gate needs only `read`, and so does an ungated
`at:` job.

Pair it with `history_read(after: <cursor>)` in the prompt and the sweep reads exactly what it has
not seen: keep the newest `cursor` you were handed, pass it back next time, and nothing is read
twice or missed in between.

### Join the discussion

When the agent really has been pulled in as a participant, mentions-only is the wrong shape and
asking three people to `@` it on every message is obnoxious. `conversation_unmute` takes a duration:

```
conversation_unmute(conversation: "telegram:-1001234567890", duration: "20m")
```

The room is heard in full for twenty minutes and then falls back to the configured default for its
kind. The agent does not have to remember to mute it again, and a turn that fails cannot leave a
busy group wide open.

When the window closes the agent is told, so it does not read the silence as the room having gone
quiet. The message that discovers the expiry is delivered even if nothing in it addresses the agent,
which costs one turn: a notice attached to a message nobody is woken for is a notice nobody reads,
and it is the exact confusion the notice exists to prevent.

## Watches

A watch is a **named rule**: one or more patterns that wake the agent in a conversation it has
otherwise turned down. It is the keyword notification every chat client has, and it exists for the
same reason: mentions-only is the right default for a busy room and the wrong one for the two or
three things in that room you would always want to know about.

```
watch_write(name: "spam-signatures", patterns: ["看我简介", "跑分", "日入过万"], reason: "spam")
watch_write(name: "the-owner", patterns: ["^432688118$"], field: "sender_id",
            conversation: "telegram:-1001234567890")
```

| Argument | Meaning |
|----------|---------|
| `name` | What identifies the rule. Writing again under the same name replaces what it holds |
| `patterns` | One or more regular expressions. Case-insensitive unless a pattern says `(?-i)` |
| `match` | `any` (default): one pattern firing is enough. `all`: every pattern must match |
| `field` | What they read: `text` (default), `sender`, or `sender_id` |
| `conversation` | Confine it to one chat. Omit to watch every muted chat |
| `duration` | `2h`, `7d`. Omit to keep it until it is removed |
| `reason` | Shown back when it fires, and when listing |

`watch_list` shows what is standing, and `watch_delete` takes the name off it, or off the line that
said a watch fired.

### One rule, many spellings

A rule set is one reason to wake written several ways, so it is one watch. A hundred patterns cost
what one costs: every pattern of a field compiles into a single automaton that the message is
passed through once, so the count affects what you have to read, not what the bridge has to do.

That is also why the name matters. It is the identity, so a rules file you keep elsewhere syncs in
one call and the reply says what actually moved:

```
Updated watch "spam-signatures": 67 patterns, in the text of every muted conversation. 1 added, 0 removed.
```

Writing the same rule twice reports `0 added, 0 removed`, which is how a sync that had nothing to do
says so. Nothing is orphaned, because the name is what the write lands on.

### `any` and `all`

`any` is a list of spellings for one thing, and the wake line names the spelling that hit.

`all` fires only when every pattern matches, anywhere in the field and in any order. It is how you
say "these terms together": the obvious spelling for that is lookahead, `(?=.*A)(?=.*B)`, and the
engine here refuses lookaround outright, that refusal being what guarantees a rule cannot take
super-linear time on a message somebody crafts. Saying it in the rule keeps both.

```
watch_write(name: "pump-and-dump", patterns: ["购买", "止盈", "止损"], match: "all")
```

On `sender`, which is two strings, `all` asks that every pattern match **the field** rather than the
same string, so a rule may take one term from the display name and another from the username.

### One watch reads one field

`text` is the message body, captions included. `sender` is the display name and the username, either
of which counts, because which of the two a person is known by differs by platform. `sender_id` is
the platform id, which is how you follow one person rather than a turn of phrase.

A rule inspects one of them, and the `woke you:` line says which matched:

```
woke you: this matched your watch "spam-signatures" (`跑分`, spam) in the text
woke you: this matched your watch "pump-and-dump" (all 3 patterns) in the text
woke you: this matched your watches "spam-signatures" (`跑分`), "newcomers" (`^hi$`) and 1 more
woke you: you were named, and this matched your watch "deploys" (`deploy`)
```

### What a watch is not

It decides that a message is **worth a turn**. It decides nothing about what the message means. The
bridge has no opinion about whether `看我简介` is spam, a quotation, or somebody discussing spam; it
wakes the agent, prints the surrounding conversation, and leaves the judgement where the judgement
can be made.

That division is what makes the feature safe to tune for **recall**. A pattern that is too broad
costs turns and is visible in the wake line, so the agent can see which rule is firing and narrow
it. A pattern that is too narrow costs nothing and misses silently, which is the failure you cannot
see. Prefer the first, and keep whatever taxonomy you use to judge a match in the agent's own
workspace rather than here.

### Limits

At most 500 watches and 2000 patterns across all of them, each pattern at most 256 characters, and a
name at most 64. Patterns are matched with the `regex` crate, which is linear in the length of the
message whatever the pattern does, so a rule the agent wrote badly cannot hang the bridge. One that
will not compile is refused when it is written, and the refusal names the pattern rather than the
rule, so a list of a hundred does not have to be bisected by hand.

A stored pattern that stops compiling (which means a hand-edited database) is skipped under `any`,
where it only costs one of several ways to fire. Under `all` the whole watch is disabled instead,
because dropping a term there would leave a rule that fires on strictly more than it was written
for.

Watches are consulted **only** where a conversation is on `mute`. In a chat heard in full every
message already arrives, and a blocked chat keeps nothing to match against, so in both a watch would
be work with no decision attached.

### Undoing a watch from outside

The same way back as for policies, and for the same reason: the agent sets these itself, and a rule
that fires on everything is expensive to leave standing.

```console
$ mekabridge watch list [--patterns]
$ mekabridge watch write spam-signatures --patterns-file ./rules/spam.txt --reason 'spam'
$ mekabridge watch write pump --match all --pattern '购买' --pattern '止盈' --pattern '止损'
$ mekabridge watch delete spam-signatures
```

`--patterns-file` reads one pattern per line, skipping blank lines and `#` comments, so a rule set
can live in a file under version control and be diffed. It combines with `--pattern`. A change from
either side is picked up by a running bridge within a couple of seconds, without a restart.

## `mekabridge unseen`

```console
$ mekabridge unseen telegram:-1001234567890
2026-08-14T10:22:31+00:00
3 unseen, newest 2026-08-14T10:22:31+00:00
$ echo $?
0
```

**stdout** carries one value and nothing else: when anything was last said there, or `never`. That
is what a `when: "changed"` gate compares. It is the same instant `backlog_check` reports as
`latest`, which omits the field entirely rather than writing `never`; either way a gate sees no
change until somebody speaks. **stderr** carries the backlog, which is what a person wants
and a gate ignores. They are split because they answer different questions and only one of them can
be watched: a backlog falls to zero every time an ordinary turn sweeps the conversation, so a
watcher gating on it would fire on the sweep and spend a turn announcing news the agent had just
been handed.

Two things move the marker backwards rather than forwards, and both fire the gate once: the author
deleting the newest message, and retention pruning the last of them.

| Exit | Meaning |
|------|---------|
| `0` | Something is waiting |
| `1` | Nothing is waiting |
| `2` | The question could not be answered |

The three are distinct because a gate can only tell "fired" from "did not". A watcher that treated a
failure as "nothing new" would go quiet exactly like a room that had, and stay quiet until somebody
noticed. A malformed conversation id is a `2` for the same reason.

Omit the conversation to ask about every chat at once.

Asking does **not** count as having seen anything, so the turn a watcher goes on to trigger still
finds the backlog waiting for `history_read`.

Nothing volatile appears on either stream. A relative time or a chat title would fire a watcher on
its own.

Two settings make the predicate permanently `1`, and neither is an error: `[storage].history_retention
= "0s"` records nothing at all, and a conversation set to `block` keeps nothing. Check those before
concluding a room is quiet.

## Waiting for somebody to finish

A person typing a thought across three messages should get one turn, not three, and the agent should
not answer "hey" before the question arrives. The only honest way to know they are still going is to
be told, and exactly one of the two platforms will tell you.

**Discord does.** The bridge asks for the two typing intents, which are unprivileged, and holds a
conversation while somebody is composing in it, plus `settle` after they stop, capped by
`settle_max`.

**Telegram cannot.** The Bot API lets a bot *send* a chat action and never receive one; there is no
update for it. So nothing on Telegram is held waiting for anybody, and a message is handed over as
soon as it arrives.

That asymmetry is deliberate rather than an omission. Without the signal any wait is a guess, and
there is no number that works for both cases: a few seconds is nowhere near long enough to type a
second sentence, and long enough for that is a long time to make somebody wait who only ever meant
to send one message. So the wait exists only where it can end when the person actually stops.

The consequence on Telegram is worth stating plainly: two messages a few seconds apart are handed
over separately, and the agent may answer the first before reading the second. If the second lands
while the agent is still working on the first, meka reads it into that same turn at its next round
boundary, under a header saying it arrived while the agent was working, so the agent can account for
it before finishing or correct itself with `message_edit`.

Every conversation is held for one second regardless, on every platform, unless its oldest waiting
message is already older than `settle_max`, which only happens after downtime or under a badly
skewed clock. That second is not about typing and is not configurable: platforms split one thing into several messages, Telegram sends a
multi-photo album as one update per photo, and without a floor a post would arrive as a photo
followed by a separate turn carrying the rest. Those parts land milliseconds apart.

## Operator controls

None of the above takes the operator out of the loop. A decision the agent made can be undone from
outside the chat:

```console
$ mekabridge policy list
$ mekabridge policy set telegram:-1001234567890 mute
$ mekabridge policy clear telegram:-1001234567890
```

`policy clear` returns a conversation to the default for its kind, which is different from setting
it to `active` explicitly. See [MCP Tools](./mcp-tools.md) for the agent's side of the same
controls, and [Config File](../configuration/config-file.md) for `[bridge.default_policy]`.
