//! mekabridge relays messages between third-party chat platforms and a single, permanent
//! [meka](https://github.com/k4yt3x/meka) agent session.
//!
//! Inbound messages are queued and handed over one at a time, through meka's session inbox, which
//! is where a message waits for the agent. meka reads them all into one turn, so messages that
//! settle together still cost one provider round trip, and one that lands while the agent is
//! working is read by that same turn rather than waiting for it.
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
pub mod watch;
