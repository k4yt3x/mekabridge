//! mekabridge relays messages between third-party chat platforms and a single, permanent
//! [meka](https://github.com/k4yt3x/meka) agent session.
//!
//! Inbound messages are queued and handed over in batches because one meka session runs one turn at
//! a time, and interrupting a turn mid-flight is not something the protocol supports gracefully.
//!
//! Outbound messages are *only* ever sent because the agent called an MCP tool. The bridge authors
//! no chat content of its own, so replying, staying quiet, and replying somewhere else are all the
//! agent's decisions rather than policy baked into the relay.

pub mod bridge;
pub mod channel;
pub mod cli;
pub mod config;
pub mod error;
pub mod mcp;
pub mod meka;
pub mod render;
pub mod store;
