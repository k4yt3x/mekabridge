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
    store::{BacklogScope, Policy},
    watch::{WatchField, WatchMode},
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

    /// Inspect or override the patterns that wake the agent in a muted conversation.
    Watch {
        #[command(subcommand)]
        command: WatchCommand,
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
        /// Conversation ids, for example `telegram:-1001234567890`. Omit to ask about every chat.
        #[arg(conflicts_with_all = ["exclude", "policy"])]
        conversations: Vec<String>,
        /// Leave this chat out of every chat. Repeat for several.
        #[arg(long, value_name = "ID")]
        exclude: Vec<String>,
        /// Only the chats under this policy now. `mute` is exactly the chats that do not wake the
        /// agent.
        #[arg(long, value_enum)]
        policy: Option<PolicyArg>,
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

/// Operator control over the agent's watches.
///
/// The way back, as the `policy` commands are: the agent sets these itself, and a rule it wrote
/// badly can wake it on every message in a busy room, which is an expensive thing to have to ask
/// it nicely to stop doing.
#[derive(Debug, Subcommand)]
pub enum WatchCommand {
    /// List every watch currently standing.
    List {
        /// Print each watch's patterns as well as its name and count.
        #[arg(long)]
        patterns: bool,
    },

    /// Create a watch, or replace what the one by that name holds.
    Write {
        /// Name for the rule. Writing again under the same name replaces it.
        name: String,

        /// Regular expression to look for. Case-insensitive unless it says `(?-i)`. Repeat for
        /// several, and combine with `--patterns-file`.
        #[arg(long = "pattern")]
        patterns: Vec<String>,

        /// Read patterns from a file, one per line. Blank lines and lines starting with `#` are
        /// skipped, so a rules file can carry its own notes.
        #[arg(long)]
        patterns_file: Option<PathBuf>,

        /// Which part of a message to match against.
        #[arg(long, default_value = "text")]
        field: WatchFieldArg,

        /// Whether one pattern firing is enough, or every one has to match.
        #[arg(long = "match", default_value = "any")]
        mode: WatchModeArg,

        /// Confine it to one conversation. Omit to watch every muted chat.
        #[arg(long)]
        conversation: Option<String>,

        /// How long, as a duration like `2h` or `7d`. Omit to leave it until it is removed.
        #[arg(long)]
        duration: Option<String>,

        /// Note recorded alongside, shown when the watch fires and when listing.
        #[arg(long)]
        reason: Option<String>,
    },

    /// Remove a watch by name.
    Delete {
        /// Watch name, as printed by `mekabridge watch list`.
        name: String,
    },
}

/// Mirrors `watch::WatchMode`, for the same reason [`WatchFieldArg`] mirrors `watch::WatchField`.
#[derive(Debug, Clone, Copy, ValueEnum)]
#[value(rename_all = "snake_case")]
pub enum WatchModeArg {
    /// Wake on any one pattern matching.
    Any,
    /// Wake only when every pattern matches.
    All,
}

impl From<WatchModeArg> for WatchMode {
    fn from(value: WatchModeArg) -> Self {
        match value {
            WatchModeArg::Any => Self::Any,
            WatchModeArg::All => Self::All,
        }
    }
}

/// Mirrors `watch::WatchField` rather than deriving `ValueEnum` on it, so the matcher stays free of
/// a clap dependency. Same arrangement as [`PolicyArg`].
#[derive(Debug, Clone, Copy, ValueEnum)]
#[value(rename_all = "snake_case")]
pub enum WatchFieldArg {
    /// The message body.
    Text,
    /// The sender's display name and username.
    Sender,
    /// The sender's platform id.
    SenderId,
}

impl From<WatchFieldArg> for WatchField {
    fn from(value: WatchFieldArg) -> Self {
        match value {
            WatchFieldArg::Text => Self::Text,
            WatchFieldArg::Sender => Self::Sender,
            WatchFieldArg::SenderId => Self::SenderId,
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
        if let Some(Command::Unseen {
            conversations,
            exclude,
            policy,
        }) = &self.command
        {
            return Ok(commands::unseen(self.config.as_deref(), &BacklogScope {
                conversations: conversations.clone(),
                exclude: exclude.clone(),
                policy: policy.map(Policy::from),
            }));
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
        Command::Watch { command } => match command {
            WatchCommand::List { patterns } => commands::watch_list(&config, patterns).await,
            WatchCommand::Write {
                name,
                patterns,
                patterns_file,
                field,
                mode,
                conversation,
                duration,
                reason,
            } => {
                commands::watch_write(
                    &config,
                    &name,
                    &patterns,
                    patterns_file.as_deref(),
                    field.into(),
                    mode.into(),
                    conversation.as_deref(),
                    duration.as_deref(),
                    reason.as_deref(),
                )
                .await
            }
            WatchCommand::Delete { name } => commands::watch_delete(&config, &name).await,
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

/// Let a closed pipe end the process, as every other command-line tool does.
///
/// Rust sets `SIGPIPE` to `SIG_IGN` at startup, which turns `mekabridge conversations list | head`
/// into a panic on the first write past the closed pipe.
///
/// Called from `main` rather than [`Cli::run`], because `--help` prints before `run` is reached.
/// The daemon takes the opposite disposition back for itself in [`crate::bridge::run`], so nothing
/// here has to work out which subcommand is the daemon.
#[cfg(unix)]
pub fn exit_quietly_on_broken_pipe() {
    // SAFETY: a valid signal number with one of the two dispositions libc defines for it. Called
    // before any other thread is spawned. The returned previous handler is not needed.
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_DFL);
    }
}

/// Hold rmcp at `warn` where the directive would let its `info` through.
///
/// meka 0.79 speaks MCP 2026-07-28 to this bridge, which has no session, so rmcp serves every
/// request on its own and logs each at `info` as it starts and ends: three lines a tool call, which
/// buried everything else at the default level. Only a directive whose catch-all level is `info`
/// or more verbose is touched, so a quieter one stays exactly as quiet, and one that names rmcp,
/// `rmcp=info` or `rmcp::service=debug` say, is the operator's to keep. `RUST_LOG` and `-vv` are
/// left as they are.
fn quiet_rmcp(directive: String) -> String {
    let names_rmcp = directive.split(',').map(str::trim).any(|part| {
        part.strip_prefix("rmcp")
            .is_some_and(|rest| rest.is_empty() || rest.starts_with(['=', ':', '[']))
    });
    let lets_info_through = directive.split(',').map(str::trim).any(|part| {
        ["info", "debug", "trace"]
            .iter()
            .any(|level| part.eq_ignore_ascii_case(level))
    });
    if names_rmcp || !lets_info_through {
        directive
    } else {
        format!("{directive},rmcp=warn")
    }
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
                0 => quiet_rmcp(config.log.level.clone()),
                1 => quiet_rmcp("mekabridge=debug,info".to_string()),
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
    fn rmcp_is_held_at_warn_only_where_its_info_would_get_through() {
        assert_eq!(quiet_rmcp("info".to_string()), "info,rmcp=warn");
        assert_eq!(
            quiet_rmcp("mekabridge=debug,info".to_string()),
            "mekabridge=debug,info,rmcp=warn"
        );
        assert_eq!(quiet_rmcp("DEBUG".to_string()), "DEBUG,rmcp=warn");
        // An operator who asked for it gets it, at whatever level they named.
        assert_eq!(quiet_rmcp("info,rmcp=debug".to_string()), "info,rmcp=debug");
        assert_eq!(
            quiet_rmcp("info, rmcp::service=info".to_string()),
            "info, rmcp::service=info"
        );
        // Another crate whose name merely starts the same way is not rmcp.
        assert_eq!(
            quiet_rmcp("info,rmcp_macros=debug".to_string()),
            "info,rmcp_macros=debug,rmcp=warn"
        );
        // A quieter directive is never made louder: appending `rmcp=warn` to `error` would show
        // rmcp's warnings, to `off` would break the silence asked for, and to an empty directive
        // would leave rmcp the only thing logged at all.
        for quiet in ["error", "warn", "off", "", "mekabridge=debug,teloxide=warn"] {
            assert_eq!(quiet_rmcp(quiet.to_string()), quiet, "{quiet:?}");
        }
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
    fn unseen_takes_the_same_scope_as_the_tool() {
        let cli = Cli::try_parse_from(["mekabridge", "unseen", "telegram:1", "telegram:2"])
            .expect("parses");
        match cli.command {
            Some(Command::Unseen { conversations, .. }) => {
                assert_eq!(conversations, ["telegram:1", "telegram:2"]);
            }
            other => panic!("unexpected parse: {other:?}"),
        }

        let cli = Cli::try_parse_from([
            "mekabridge",
            "unseen",
            "--exclude",
            "telegram:3",
            "--exclude",
            "telegram:4",
            "--policy",
            "mute",
        ])
        .expect("parses");
        match cli.command {
            Some(Command::Unseen {
                exclude, policy, ..
            }) => {
                assert_eq!(exclude, ["telegram:3", "telegram:4"]);
                assert_eq!(policy, Some(PolicyArg::Mute));
            }
            other => panic!("unexpected parse: {other:?}"),
        }

        // Naming chats and narrowing every chat are refused together, as the tool refuses them.
        assert!(
            Cli::try_parse_from(["mekabridge", "unseen", "telegram:1", "--policy", "mute"])
                .is_err()
        );
        assert!(
            Cli::try_parse_from([
                "mekabridge",
                "unseen",
                "telegram:1",
                "--exclude",
                "telegram:2"
            ])
            .is_err()
        );
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
