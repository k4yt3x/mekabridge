//! The error union the binary and orchestration layers propagate.
//!
//! Domain modules keep their own narrow error enums ([`crate::meka::MekaError`],
//! [`crate::channel::ChannelError`], [`crate::store::StoreError`]) so retry decisions can match on
//! a small set of variants without pattern-matching the whole program's failure surface.
//! [`BridgeError`] only aggregates them.

use std::path::PathBuf;

/// Convenience alias for fallible mekabridge operations.
pub type Result<T> = std::result::Result<T, BridgeError>;

/// Every way mekabridge can fail, as seen by `main` and the orchestration layer.
#[derive(Debug, thiserror::Error)]
pub enum BridgeError {
    #[error("configuration is invalid: {message}")]
    Config { message: String },

    #[error("could not read config file {path}: {source}")]
    ConfigRead {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("config file {path} is not valid TOML: {source}")]
    ConfigParse {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },

    #[error("channel: {0}")]
    Channel(#[from] crate::channel::ChannelError),

    #[error("meka: {0}")]
    Meka(#[from] crate::meka::MekaError),

    /// A command refused to act, or found something wrong. Distinct from [`Self::Config`] so the
    /// message is not prefixed with "configuration is invalid", which would be misleading for, say,
    /// a `--yes` confirmation that was not given.
    #[error("{message}")]
    Command { message: String },

    #[error("store: {0}")]
    Store(#[from] crate::store::StoreError),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl BridgeError {
    /// Shorthand for the common "config says something impossible" case.
    pub fn config(message: impl Into<String>) -> Self {
        Self::Config {
            message: message.into(),
        }
    }

    /// Shorthand for a command that declined to act or found a problem.
    pub fn command(message: impl Into<String>) -> Self {
        Self::Command {
            message: message.into(),
        }
    }
}
