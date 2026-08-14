//! Command-line surface and logging setup.
//!
//! The daemon is the default subcommand; everything else is an operator tool. Those tools
//! deliberately need no IPC with a running daemon: they read the SQLite database (WAL allows
//! concurrent readers) and talk to meka's HTTP API directly, so `mekabridge status` works whether
//! or not the daemon is up.

pub mod commands;

use std::{path::PathBuf, process::ExitCode};

use clap::{ArgAction, Parser, Subcommand, ValueEnum};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

use crate::{
    config::{Config, LogFormat, default_config_path},
    error::Result,
    store::Policy,
};

/// Default row cap for the listing commands.
const DEFAULT_LIST_LIMIT: usize = 50;

/// Relay between the meka agent and third-party chat platforms.
#[derive(Debug, Parser)]
#[command(name = "mekabridge", version, about, long_about = None)]
pub struct Cli {
    /// Path to config.toml. Defaults to the platform config directory.
    #[arg(
        short,
        long,
        global = true,
        value_name = "PATH",
        env = "MEKABRIDGE_CONFIG"
    )]
    pub config: Option<PathBuf>,

    /// Increase log verbosity. Repeat for more detail. Overrides `[log].level`.
    #[arg(short, long, global = true, action = ArgAction::Count)]
    pub verbose: u8,

    /// Log output format. Overrides `[log].format`.
    #[arg(long, global = true, value_name = "FORMAT")]
    pub log_format: Option<LogFormatArg>,

    #[command(subcommand)]
    pub command: Option<Command>,
}

/// Log format selectable from the command line.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum LogFormatArg {
    Text,
    Json,
}

impl From<LogFormatArg> for LogFormat {
    fn from(value: LogFormatArg) -> Self {
        match value {
            LogFormatArg::Text => Self::Text,
            LogFormatArg::Json => Self::Json,
        }
    }
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run the bridge daemon. This is the default when no subcommand is given.
    Run,

    /// Check configuration, meka, channels, and the MCP endpoint.
    Doctor,

    /// Show the session, queue depth, and known conversations.
    Status,

    /// Inspect the inbound queue.
    Queue {
        #[command(subcommand)]
        command: QueueCommand,
    },

    /// Inspect the conversations the agent can message.
    Conversations {
        #[command(subcommand)]
        command: ConversationsCommand,
    },

    /// Inspect or override how much of a conversation reaches the agent.
    Policy {
        #[command(subcommand)]
        command: PolicyCommand,
    },

    /// Read back what a conversation has said, including what the agent was never woken for.
    History {
        /// Conversation id, for example `telegram:-1001234567890`.
        conversation: String,

        /// Maximum messages to print.
        #[arg(long, default_value_t = DEFAULT_LIST_LIMIT)]
        limit: usize,

        /// Only show messages matching these words.
        #[arg(long)]
        search: Option<String>,
    },

    /// Report what the agent has not been shown, for use as a scheduled job's gate.
    ///
    /// Exits 0 when something is waiting, 1 when nothing is, and 2 when the question could not be
    /// answered. The three are distinct on purpose: a watcher that treats a failure as "nothing
    /// new" goes quiet exactly like a room that has, and stays quiet until somebody notices.
    Unseen {
        /// Conversation id, for example `telegram:-1001234567890`. Omit to ask about every chat.
        conversation: Option<String>,
    },

    /// Inspect or reset the agent's session.
    Session {
        #[command(subcommand)]
        command: SessionCommand,
    },

    /// Cancel the turn that is currently running, if any.
    Cancel,

    /// Inspect or create the configuration file.
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
}

#[derive(Debug, Subcommand)]
pub enum QueueCommand {
    /// List messages waiting to be delivered.
    List {
        /// Maximum rows to print.
        #[arg(long, default_value_t = DEFAULT_LIST_LIMIT)]
        limit: usize,
    },

    /// Delete every queued message, including undelivered ones.
    Clear {
        /// Confirm the deletion.
        #[arg(long)]
        yes: bool,
    },
}

#[derive(Debug, Subcommand)]
pub enum ConversationsCommand {
    /// List known conversations, most recently active first.
    List {
        /// Restrict to one configured channel.
        #[arg(long)]
        channel: Option<String>,

        /// Maximum rows to print.
        #[arg(long, default_value_t = DEFAULT_LIST_LIMIT)]
        limit: usize,
    },
}

/// How much of a conversation reaches the agent, selectable from the command line.
///
/// Mirrors `store::Policy` rather than deriving `ValueEnum` on it, so the store stays free of a
/// dependency on the argument parser. `LogFormatArg` does the same for `[log].format`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum PolicyArg {
    /// Wake the agent for every message.
    Active,
    /// Record everything, but only wake the agent when it is mentioned or replied to.
    Mute,
    /// Deliver nothing and keep nothing.
    Block,
}

impl From<PolicyArg> for Policy {
    fn from(value: PolicyArg) -> Self {
        match value {
            PolicyArg::Active => Self::Active,
            PolicyArg::Mute => Self::Mute,
            PolicyArg::Block => Self::Block,
        }
    }
}

/// Operator control over attention policies.
///
/// The agent sets these itself, so this exists as the way back: a conversation it muted or blocked
/// indefinitely is otherwise unreachable, including the one an operator would use to ask it to undo
/// them.
#[derive(Debug, Subcommand)]
pub enum PolicyCommand {
    /// List the configured defaults and every conversation with a policy of its own.
    List,

    /// Give one conversation a policy, overriding the default for its kind.
    Set {
        /// Conversation id, for example `telegram:-1001234567890`.
        conversation: String,

        /// What should reach the agent from it.
        policy: PolicyArg,

        /// How long, as a duration like `30m` or `7d`. Omit to leave it until it is changed.
        #[arg(long)]
        duration: Option<String>,

        /// Note recorded alongside the policy.
        #[arg(long)]
        reason: Option<String>,
    },

    /// Drop a conversation's own policy so it follows the configured default again.
    ///
    /// Not the same as setting it to `active`: for a group the default is normally `mute`, so this
    /// returns it to mention-only rather than making it wake the agent for everything.
    Clear {
        /// Conversation id.
        conversation: String,
    },
}

#[derive(Debug, Subcommand)]
pub enum SessionCommand {
    /// Show the bound session.
    Show,

    /// Forget the session binding so the next message starts a fresh one.
    Reset {
        /// Confirm that the agent's memory of past conversations may be discarded.
        #[arg(long)]
        yes: bool,
    },
}

#[derive(Debug, Subcommand)]
pub enum ConfigCommand {
    /// Print the path the config would be loaded from.
    Path,

    /// Load and validate the config, reporting the first problem found.
    Validate,

    /// Write a starter config to the default path.
    Init {
        /// Overwrite an existing file.
        #[arg(long)]
        force: bool,
    },
}

impl Cli {
    /// Execute the selected subcommand.
    ///
    /// The exit code is part of the interface for [`Command::Unseen`] and nothing else, which is
    /// why it is returned rather than left to the caller to derive from `Ok`/`Err`.
    pub fn run(self) -> Result<ExitCode> {
        // Answered before anything else, because its failures have to be told apart from its
        // answers. Everything below reports failure by returning `Err`, which the binary turns
        // into exit 1, and exit 1 is this command's way of saying "nothing new".
        if let Some(Command::Unseen { conversation }) = &self.command {
            return Ok(commands::unseen(
                self.config.as_deref(),
                conversation.as_deref(),
            ));
        }
        self.run_command().map(|()| ExitCode::SUCCESS)
    }

    fn run_command(self) -> Result<()> {
        let config_path = self.config.clone();
        match self.command {
            Some(Command::Config { command }) => match command {
                ConfigCommand::Path => {
                    let path = resolve_config_path(config_path.as_deref())?;
                    println!("{}", path.display());
                    Ok(())
                }
                ConfigCommand::Init { force } => {
                    let path = resolve_config_path(config_path.as_deref())?;
                    commands::config_init(&path, force)
                }
                ConfigCommand::Validate => {
                    let config = Config::load(config_path.as_deref())?;
                    for warning in &config.warnings {
                        println!("warning: {warning}");
                    }
                    println!(
                        "configuration is valid: {} channel(s), meka at {}",
                        config.channels.len(),
                        config.meka.base_url
                    );
                    Ok(())
                }
            },
            Some(Command::Run) | None => {
                let config = Config::load(config_path.as_deref())?;
                init_logging(&config, self.verbose, self.log_format);
                for warning in &config.warnings {
                    tracing::warn!("{}", warning);
                }
                block_on(crate::bridge::run(config))
            }
            Some(command) => {
                let config = Config::load(config_path.as_deref())?;
                block_on(dispatch(command, config))
            }
        }
    }
}

/// Run the operator subcommands, which are async because they touch SQLite and meka.
async fn dispatch(command: Command, config: Config) -> Result<()> {
    match command {
        Command::Doctor => commands::doctor(&config).await,
        Command::Status => commands::status(&config).await,
        Command::Queue { command } => match command {
            QueueCommand::List { limit } => commands::queue_list(&config, limit).await,
            QueueCommand::Clear { yes } => commands::queue_clear(&config, yes).await,
        },
        Command::Conversations { command } => match command {
            ConversationsCommand::List { channel, limit } => {
                commands::conversations_list(&config, channel.as_deref(), limit).await
            }
        },
        Command::Policy { command } => match command {
            PolicyCommand::List => commands::policy_list(&config).await,
            PolicyCommand::Set {
                conversation,
                policy,
                duration,
                reason,
            } => {
                commands::policy_set(
                    &config,
                    &conversation,
                    policy.into(),
                    duration.as_deref(),
                    reason.as_deref(),
                )
                .await
            }
            PolicyCommand::Clear { conversation } => {
                commands::policy_clear(&config, &conversation).await
            }
        },
        Command::History {
            conversation,
            limit,
            search,
        } => commands::history_show(&config, &conversation, limit, search.as_deref()).await,
        Command::Session { command } => match command {
            SessionCommand::Show => commands::session_show(&config).await,
            SessionCommand::Reset { yes } => commands::session_reset(&config, yes).await,
        },
        Command::Cancel => commands::cancel(&config).await,
        // Handled before dispatch: the first two need no config or runtime, and `Unseen` owns its
        // own exit code and so cannot report through this function's `Result`.
        Command::Run | Command::Config { .. } | Command::Unseen { .. } => Ok(()),
    }
}

fn resolve_config_path(override_path: Option<&std::path::Path>) -> Result<PathBuf> {
    match override_path {
        Some(path) => Ok(path.to_path_buf()),
        None => default_config_path(),
    }
}

/// Build a runtime for one command.
///
/// Created here rather than with `#[tokio::main]` so `mekabridge config path`, which touches
/// nothing async, does not pay for a thread pool.
fn block_on<T, F: Future<Output = Result<T>>>(future: F) -> Result<T> {
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(future)
}

/// Build the tracing subscriber.
///
/// Precedence is `RUST_LOG`, then `-v` repetitions, then `[log].level`. Logging is initialised
/// after the config is loaded, so config errors are reported by `main` on stderr instead.
fn init_logging(config: &Config, verbose: u8, format_override: Option<LogFormatArg>) {
    let filter = match EnvFilter::try_from_default_env() {
        Ok(filter) => filter,
        Err(_) => {
            let directive = match verbose {
                0 => config.log.level.clone(),
                1 => "mekabridge=debug,info".to_string(),
                _ => "mekabridge=trace,debug".to_string(),
            };
            EnvFilter::new(directive)
        }
    };
    let format = format_override.map_or(config.log.format, LogFormat::from);
    let registry = tracing_subscriber::registry().with(filter);
    match format {
        LogFormat::Text => registry.with(tracing_subscriber::fmt::layer()).init(),
        LogFormat::Json => registry
            .with(tracing_subscriber::fmt::layer().json())
            .init(),
    }
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory as _;

    use super::*;

    #[test]
    fn the_command_tree_is_well_formed() {
        Cli::command().debug_assert();
    }

    #[test]
    fn no_subcommand_runs_the_daemon() {
        let cli = Cli::try_parse_from(["mekabridge"]).expect("parses");
        assert!(cli.command.is_none());
    }

    #[test]
    fn the_config_path_can_come_from_the_environment() {
        // systemd units set this rather than threading a flag through ExecStart.
        // SAFETY: single-threaded test section.
        unsafe { std::env::set_var("MEKABRIDGE_CONFIG", "/etc/mekabridge/from-env.toml") };
        let cli = Cli::try_parse_from(["mekabridge", "status"]).expect("parses");
        assert_eq!(
            cli.config.as_deref(),
            Some(std::path::Path::new("/etc/mekabridge/from-env.toml"))
        );
        unsafe { std::env::remove_var("MEKABRIDGE_CONFIG") };
    }

    #[test]
    fn global_flags_work_after_a_subcommand() {
        let cli = Cli::try_parse_from(["mekabridge", "status", "--config", "/tmp/x.toml", "-vv"])
            .expect("parses");
        assert_eq!(
            cli.config.as_deref(),
            Some(std::path::Path::new("/tmp/x.toml"))
        );
        assert_eq!(cli.verbose, 2);
        assert!(matches!(cli.command, Some(Command::Status)));
    }

    #[test]
    fn destructive_commands_require_confirmation_flags() {
        let cli = Cli::try_parse_from(["mekabridge", "queue", "clear"]).expect("parses");
        match cli.command {
            Some(Command::Queue {
                command: QueueCommand::Clear { yes },
            }) => assert!(!yes, "clear must default to unconfirmed"),
            other => panic!("unexpected parse: {other:?}"),
        }

        let cli = Cli::try_parse_from(["mekabridge", "session", "reset"]).expect("parses");
        match cli.command {
            Some(Command::Session {
                command: SessionCommand::Reset { yes },
            }) => assert!(!yes, "reset must default to unconfirmed"),
            other => panic!("unexpected parse: {other:?}"),
        }
    }

    #[test]
    fn list_limits_have_a_default() {
        let cli = Cli::try_parse_from(["mekabridge", "queue", "list"]).expect("parses");
        match cli.command {
            Some(Command::Queue {
                command: QueueCommand::List { limit },
            }) => assert_eq!(limit, DEFAULT_LIST_LIMIT),
            other => panic!("unexpected parse: {other:?}"),
        }
    }
}
