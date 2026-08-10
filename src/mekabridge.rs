//! mekabridge relays messages between third-party chat platforms and a single, permanent
//! [meka](https://github.com/k4yt3x/meka) agent session.
//!
//! The design treats the agent as a person with a phone. Inbound messages from every configured
//! channel are queued and handed to the agent in batches, because one meka session runs one turn at
//! a time and interrupting a turn mid-flight is not something the protocol supports gracefully.
//! Outbound messages are *only* ever sent because the agent called an MCP tool: the bridge never
//! authors chat content of its own, so replying, not replying, replying to somebody else, and
//! replying on a different platform are all decisions the agent makes rather than policy baked into
//! the relay.
//!
//! Module map:
//!
//! - [`config`] parses and validates the TOML config, resolving credentials once at startup.
//! - [`store`] owns the SQLite database: the session binding, known conversations, and the durable
//!   inbound queue.
//! - [`meka`] speaks meka's HTTP API, including consuming a turn's SSE stream.
//! - [`mcp`] is the MCP server meka connects to, exposing the outbound tool surface.
//! - [`channel`] defines the platform abstraction; each submodule is one platform.
//! - [`bridge`] wires it together and owns the queue-to-turn loop.

pub mod bridge;
pub mod channel;
pub mod cli;
pub mod config;
pub mod error;
pub mod mcp;
pub mod meka;
pub mod store;
