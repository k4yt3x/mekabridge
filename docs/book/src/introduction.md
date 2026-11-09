# Introduction

Meka Bridge connects a [Meka](https://github.com/k4yt3x/meka) agent to messaging platforms. People message a bot, the agent reads what they wrote, and the agent decides what to do about it.

The design treats the agent as a person with a phone.

- **Inbound** messages from every configured channel are queued and handed to the agent through Meka's session inbox, one item per message. Meka reads them all into one turn, so a burst costs one turn rather than one each, and something that arrives while the agent is working reaches it inside that same turn, the way a person glances at a message mid-task.
- **Outbound** messages happen only because the agent called a tool. The bridge never writes chat content of its own. Replying, staying quiet, replying to somebody else, replying on a different platform, or messaging first tomorrow morning are all the agent's decisions.

One Meka Bridge instance owns exactly one Meka session, permanently. That session is the agent's memory: everyone it has talked to, on every platform, in one continuous context.

## One session, shared by everyone

The session is not per person. Everyone who can reach the bot shares one agent context and one memory, so anything said in one conversation can inform an answer in another, and anyone admitted can ask about anything already in it.

That is a property to design around rather than a limit to work around. A personal assistant wants it. A customer-service bot answering strangers in a public group probably does not, and should be pointed at a Meka instance kept for that purpose. The allowlist starts empty for the same reason: what the agent knows is worth as much as what it can do.

The bridge reports facts and supplies capabilities; who counts as trusted, and what they may ask for, belongs in the agent's own instructions. See [Security](./usage/security.md).

## How the pieces fit

```
                  MCP (streamable HTTP)
    meka serve  ──────── tools/call ────────►  meka-bridge  ◄──── long poll ────  Telegram
                                                            ◄──── gateway ──────  Discord
      :8080     ◄─ POST /v1/sessions/{id}/inbox ┘   │
                ──── GET /v1/sessions/{id}/feed ►   └── SQLite: session, queue, conversations
```

Meka and Meka Bridge each act as the other's client. Meka calls the bridge's MCP tools to send messages; the bridge puts messages in Meka's inbox and follows the session's event feed to learn what became of them.

> **Start the bridge first where you can**, and set `required = true` on Meka's server entry for it. Meka retries a failed MCP connect in the background, so the wrong order heals itself within a few minutes; without `required` it runs turns meanwhile, with no tool to answer anybody. See [Meka Integration](./usage/meka-integration.md).

## Status

Telegram and Discord are the supported platforms. The channel layer is where a platform's differences live, so adding another is a new module plus a factory arm; nothing in the queue, the item renderer, or the hand-over changes.

Continue to [Installation](./getting-started/installation.md).
