# Introduction

mekabridge connects a [meka](https://github.com/k4yt3x/meka) agent to messaging platforms. People message a bot, the agent reads what they wrote, and the agent decides what to do about it.

The design treats the agent as a person with a phone.

- **Inbound** messages from every configured channel are queued and handed to the agent in batches. One meka session runs one turn at a time, so anything that arrives mid-turn waits, the same way messages wait while somebody is in a meeting.
- **Outbound** messages happen only because the agent called a tool. The bridge never writes chat content of its own. Replying, staying quiet, replying to somebody else, replying on a different platform, or messaging first tomorrow morning are all the agent's decisions.

One mekabridge instance owns exactly one meka session, permanently. That session is the agent's memory: everyone it has talked to, on every platform, in one continuous context.

## One session, shared by everyone

The session is not per person. Everyone who can reach the bot shares one agent context and one memory, so anything said in one conversation can inform an answer in another, and anyone admitted can ask about anything already in it.

That is a property to design around rather than a limit to work around. A personal assistant wants it. A customer-service bot answering strangers in a public group probably does not, and should be pointed at a meka instance kept for that purpose. The allowlist starts empty for the same reason: what the agent knows is worth as much as what it can do.

The bridge reports facts and supplies capabilities; who counts as trusted, and what they may ask for, belongs in the agent's own instructions. See [Security](./usage/security.md).

## How the pieces fit

```
                  MCP (streamable HTTP)
    meka serve  ──────── tools/call ────────►  mekabridge  ◄──── long poll ────  Telegram
      :8080     ◄─ POST /v1/sessions/{id}/turn ─┘   │
                                                    └── SQLite: session, queue, conversations
```

meka and mekabridge each act as the other's client. meka calls the bridge's MCP tools to send messages; the bridge calls meka's HTTP API to run turns.

> **Start the bridge first where you can.** meka retries a failed MCP connect in the background, so the wrong order heals itself within a few minutes, but `[mcp].strict` makes meka refuse turns until it does. See [meka Integration](./usage/meka-integration.md).

## Status

Telegram is the supported platform today. The channel layer is an abstraction with one implementation, so adding another is a new module plus a factory arm; nothing in the queue, envelope, or turn machinery changes.

Continue to [Installation](./getting-started/installation.md).
