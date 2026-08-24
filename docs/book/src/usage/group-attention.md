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

That is the whole list. Somebody answering the agent in ordinary prose, without naming it and
without using reply, does not reach it, and neither does a follow-up typed straight after it has
spoken. `@everyone`, `@here`, and role pings deliberately do not count either.

Everything withheld is still **recorded**. `read_history` and `search_history` reach it, and the
next thing that does wake the conversation arrives with a count of what accumulated and the last
`mute_context` messages inline.

## Every message says why it is there

Every message from a chat that is not one-to-one carries a `woke you:` line:

```
woke you: you were named
woke you: you were named, or this replies to something you said
woke you: nothing here named you; this chat was being heard in full when it arrived
```

The third is written in the past tense on purpose. It reports why the message was delivered, which
is not always the same as how the conversation is set now: a batch can sit in the queue through a
turn lasting minutes, and the agent may have muted the room in the meantime.

The line is stated for every such message, including the ones nothing addressed. Printing it only
for a mention would make its absence the signal, and an absent line is not one: overheard chatter
and a chat being heard in full would render identically.

## What the agent does instead of a window

Before 0.7.0 the bridge kept a muted conversation open for five minutes after the agent spoke, on
the theory that an exchange already under way should carry on without a second mention. In a busy
room it delivered the room, in envelopes indistinguishable from a message addressed to the agent,
and each reply the agent was nudged into making pushed the window out again. It is gone.

What replaces it is three things the agent asks for, none of which involves the bridge guessing.

### Look back once

The agent answers, is done for now, and wants one glance in a few minutes to see whether its answer
landed. meka's own scheduler does this, and needs nothing from the bridge:

```
schedule_create(
  at: "5m",
  prompt: "You argued for rolling back in the deploy group (telegram:-1001234567890).
           read_history it and see whether anyone pushed back or asked a follow-up
           without naming you. Reply only if something is actually owed."
)
```

One turn, at a time the agent picked, framed by the agent as a look-back rather than a summons. The
prompt is delivered with no human present, so it has to carry its own context.

### Stand watch

For a room worth following for a while, a recurring job with a gate costs nothing while the room is
quiet. `mekabridge unseen` is the gate:

```
schedule_create(
  every: "2m",
  gate: { command: "mekabridge unseen telegram:-1001234567890", fire: "on-change" },
  prompt: "Something new was said in the deploy group since you last looked. ..."
)
```

`on-change` fires when the command's output differs from the previous run, so a turn is spent only
once the chat has actually moved. Gates run a shell command unattended and meka requires
`permission = "unrestricted"` to create one, as of 0.42: `workspace` used to authorise a gate and no
longer does. An ungated `at:` job works at `read`.

The `unseen` MCP tool answers the same question, for the agent to read directly. It returns the
backlog rather than the marker, because that is what is useful to read and the wrong thing to gate
on.

### Join the discussion

When the agent really has been pulled in as a participant, mentions-only is the wrong shape and
asking three people to `@` it on every message is obnoxious. `unmute` takes a duration:

```
unmute(conversation: "telegram:-1001234567890", duration: "20m")
```

The room is heard in full for twenty minutes and then falls back to the configured default for its
kind. The agent does not have to remember to mute it again, and a turn that fails cannot leave a
busy group wide open.

When the window closes the agent is told, so it does not read the silence as the room having gone
quiet. The message that discovers the expiry is delivered even if nothing in it addresses the agent,
which costs one turn: a notice attached to a message nobody is woken for is a notice nobody reads,
and it is the exact confusion the notice exists to prevent.

## `mekabridge unseen`

```console
$ mekabridge unseen telegram:-1001234567890
2026-08-14T10:22:31+00:00
3 unseen, newest 2026-08-14T10:22:31+00:00
$ echo $?
0
```

**stdout** carries one value and nothing else: when anything was last said there, or `never`. That
is what an `on-change` gate compares. **stderr** carries the backlog, which is what a person wants
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
finds the backlog waiting for `read_history`.

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
update for it. So nothing on Telegram is held waiting for anybody, and a message starts a turn as
soon as it arrives.

That asymmetry is deliberate rather than an omission. Without the signal any wait is a guess, and
there is no number that works for both cases: a few seconds is nowhere near long enough to type a
second sentence, and long enough for that is a long time to make somebody wait who only ever meant
to send one message. So the wait exists only where it can end when the person actually stops.

The consequence on Telegram is worth stating plainly: two messages a few seconds apart produce two
turns, and the agent may answer the first before reading the second. If the second lands while the agent is still working on the first, it arrives
flagged `late:`, so the agent knows its reply was written without it and can correct itself with
`edit_message`.

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
