//! Implementations of the operator subcommands.
//!
//! None of these talk to a running daemon. They read the SQLite database directly (WAL permits
//! concurrent readers) and call meka's HTTP API themselves, so every command works whether the
//! bridge is up or down. That avoids inventing a control socket, and it means a wedged daemon can
//! still be inspected.

use std::path::Path;

use chrono::Utc;

use crate::{
    channel::{ChannelRegistry, Platform},
    config::{Config, McpTransport},
    error::{BridgeError, Result},
    meka::MekaClient,
    store::{Policy, Store},
};

/// Starter config written by `mekabridge config init`.
const CONFIG_TEMPLATE: &str = include_str!("config_template.toml");

/// Report on every moving part, in the order a failure would bite.
///
/// Returns an error when something is broken badly enough that the bridge would not work, so this
/// doubles as a health check in a deployment script.
pub async fn doctor(config: &Config) -> Result<()> {
    let mut failures = 0_usize;
    let mut warnings = 0_usize;

    println!("config");
    println!("  ok     parsed and validated");
    for warning in &config.warnings {
        println!("  warn   {warning}");
        warnings += 1;
    }

    println!("attention");
    println!(
        "  ok     default policy: direct {}, group {}, channel {}",
        config.bridge.default_policy.direct.as_str(),
        config.bridge.default_policy.group.as_str(),
        config.bridge.default_policy.channel.as_str()
    );

    println!("storage");
    match Store::open(&config.storage.path).await {
        Ok(store) => {
            println!("  ok     database at {}", config.storage.path.display());
            match store.queue_stats().await {
                Ok(stats) => println!(
                    "  ok     queue: {} pending, {} in flight, {} failed",
                    stats.pending, stats.in_flight, stats.failed
                ),
                Err(error) => {
                    println!("  fail   could not read queue stats: {error}");
                    failures += 1;
                }
            }
            match store.list_policies().await {
                Ok(policies) if policies.is_empty() => {
                    println!("  ok     no conversation overrides the default");
                }
                Ok(policies) => println!(
                    "  ok     {} conversation(s) have a policy of their own; `mekabridge policy \
                     list` shows them",
                    policies.len()
                ),
                Err(error) => {
                    println!("  fail   could not read conversation policies: {error}");
                    failures += 1;
                }
            }
            if config.storage.history_retention.is_zero() {
                // Not a failure: some deployments deliberately keep no chat log. It does change
                // what a muted conversation can offer the agent, so it is stated
                // rather than left to be discovered when read_history comes back
                // empty.
                println!(
                    "  warn   history is off, so nothing a muted conversation withholds can be \
                     read back later. Set [storage].history_retention to record it."
                );
                warnings += 1;
            } else {
                match store.message_count().await {
                    Ok(count) => println!(
                        "  ok     history: {count} message(s) recorded, kept for {}",
                        humantime::format_duration(config.storage.history_retention)
                    ),
                    Err(error) => {
                        println!("  fail   could not read the message history: {error}");
                        failures += 1;
                    }
                }
            }
        }
        Err(error) => {
            println!("  fail   {error}");
            failures += 1;
        }
    }

    println!("meka at {}", config.meka.base_url);
    let meka = MekaClient::new(&config.meka)?;
    match meka.info().await {
        Ok(info) => {
            println!(
                "  ok     reachable, version {}, model {}",
                info.version,
                info.model.as_deref().unwrap_or("(none configured)")
            );
            if info.vision {
                println!(
                    "  ok     vision is on, so view_attachment hands the agent the picture itself"
                );
            } else {
                println!(
                    "  warn   the active profile has vision off, so view_attachment returns a \
                     description rather than the image and the agent can only reach a file through \
                     download_attachment. Set `vision = true` under [providers.<name>]."
                );
                warnings += 1;
            }
        }
        Err(error) => {
            println!("  fail   {error}");
            failures += 1;
        }
    }
    match meka.ready().await {
        Ok(ready) => {
            println!("  ok     readiness: {}", ready.status);
            if !ready.provider_configured {
                println!("  warn   meka reports no provider is configured; turns will fail");
                warnings += 1;
            }
            if !ready.mcp_servers_healthy {
                // The usual cause is meka having started before this bridge was listening. meka
                // retries a failed cold start in the background with backoff, so this clears on its
                // own; it is reported because `[mcp].strict` refuses turns until it does.
                println!(
                    "  warn   meka reports an unhealthy MCP server. If that is this bridge, it \
                     should reconnect on its own within a few minutes; turns are refused until it \
                     does when [mcp].strict is on."
                );
                warnings += 1;
            }
        }
        Err(error) => {
            println!("  warn   readiness probe failed: {error}");
            warnings += 1;
        }
    }

    println!("session");
    let store = Store::open(&config.storage.path).await.ok();
    let bound = match &store {
        Some(store) => store.session_id().await.unwrap_or(None),
        None => None,
    };
    match bound {
        None => println!("  ok     no session yet; one is created on the first message"),
        Some(session_id) => match meka.session(session_id).await {
            Ok(info) => {
                println!(
                    "  ok     bound to {session_id} (permission {})",
                    info.permission
                );
                if info.turn_in_flight {
                    println!("  ok     a turn is running on it right now");
                }
                if let Some(problem) = permission_problem(&info.permission) {
                    println!(
                        "  fail   the session is at permission {:?}. {problem}",
                        info.permission
                    );
                    failures += 1;
                }
            }
            Err(error) if error.is_session_missing() => {
                println!(
                    "  warn   meka no longer knows session {session_id}; a replacement will be created"
                );
                warnings += 1;
            }
            Err(error) => {
                println!("  warn   could not read session {session_id}: {error}");
                warnings += 1;
            }
        },
    }
    if let Some(problem) = permission_problem(config.session.permission.as_str()) {
        println!(
            "  fail   [session].permission is {:?}. {problem}",
            config.session.permission.as_str()
        );
        failures += 1;
    }

    println!("channels");
    match ChannelRegistry::build(&config.channels) {
        Ok(registry) => {
            for channel in registry.iter() {
                match channel.probe().await {
                    Ok(identity) => {
                        let label = identity
                            .username
                            .map_or(identity.display_name.clone(), |username| {
                                format!("@{username}")
                            });
                        println!("  ok     {} authenticated as {label}", channel.id());
                        if !identity.reads_all_group_messages {
                            // Both platforms have a switch that withholds everything not aimed at
                            // the bot, which is what the `mute` policy does except that nothing is
                            // recorded. Leaving it on therefore does not save a turn, it only
                            // empties the history the agent would otherwise read when a mention
                            // arrives halfway through a discussion. The fix differs, so the advice
                            // does.
                            match channel.platform() {
                                Platform::Telegram => println!(
                                    "  warn   {} has privacy mode on, so Telegram withholds group \
                                     messages that do not mention it. The `mute` policy already \
                                     limits what wakes the agent, and privacy mode on top of it \
                                     means read_history has nothing to show. Turn it off with \
                                     /setprivacy in @BotFather, then remove and re-add the bot to \
                                     each group.",
                                    channel.id()
                                ),
                                Platform::Discord => println!(
                                    "  warn   {} runs with `message_content = false`, so Discord \
                                     blanks the text of every server message except those \
                                     mentioning the bot. Mentions still wake the agent, but \
                                     read_history and search_history will have nothing to show it \
                                     about what led up to one.",
                                    channel.id()
                                ),
                            }
                            warnings += 1;
                        }
                        if channel.platform() == Platform::Discord {
                            // The gateway refuses a privileged intent at connect with a 4014 rather
                            // than degrading, and `probe` only exercises the REST API, so a token
                            // that authenticates here can still fail to connect a minute later.
                            println!(
                                "  note   {} asks Discord for the GUILDS, GUILD_MESSAGES and \
                                 DIRECT_MESSAGES intents{}. A privileged intent that is not \
                                 enabled on the Bot page of the Developer Portal closes the \
                                 gateway with a 4014 at startup, which this check cannot see.",
                                channel.id(),
                                if identity.reads_all_group_messages {
                                    ", plus the privileged MESSAGE_CONTENT intent"
                                } else {
                                    ""
                                }
                            );
                        }
                        let capabilities = channel.capabilities();
                        if capabilities.presence {
                            println!(
                                "  ok     {} tracks who is online; availability is built up from \
                                 the gateway, so it is empty for a moment after every restart and \
                                 reads as unknown until it fills",
                                channel.id()
                            );
                        }
                        if capabilities.admin {
                            println!(
                                "  ok     {} offers the moderation tools ({}); each call still \
                                 needs the matching right in the chat it targets",
                                channel.id(),
                                if capabilities.member_roles {
                                    "privileges granted through roles"
                                } else {
                                    "privileges granted to a person directly"
                                }
                            );
                        }
                    }
                    Err(error) => {
                        println!("  fail   {}: {error}", channel.id());
                        failures += 1;
                    }
                }
            }
        }
        Err(error) => {
            println!("  fail   {error}");
            failures += 1;
        }
    }

    println!("mcp");
    match config.mcp.transport {
        McpTransport::Stdio => println!("  ok     stdio transport; nothing to bind"),
        McpTransport::Http => match tokio::net::TcpListener::bind(config.mcp.bind).await {
            Ok(listener) => {
                drop(listener);
                println!(
                    "  ok     {}{} is free (meka should use transport = \"http\", url = \
                     \"http://{}{}\")",
                    config.mcp.bind, config.mcp.path, config.mcp.bind, config.mcp.path
                );
            }
            Err(error) => {
                // A daemon already holding the port is the common case and is fine.
                println!(
                    "  warn   could not bind {}: {error}. This is expected if mekabridge is \
                     already running.",
                    config.mcp.bind
                );
                warnings += 1;
            }
        },
    }
    if config.mcp.token.is_none() && !config.mcp.bind.ip().is_loopback() {
        println!(
            "  fail   the MCP endpoint is on a non-loopback address with no [mcp].token; anyone \
             who can reach it can send messages as the agent"
        );
        failures += 1;
    }

    println!();
    if failures > 0 {
        println!("{failures} failure(s), {warnings} warning(s)");
        return Err(BridgeError::command(format!(
            "doctor found {failures} failure(s)"
        )));
    }
    println!("no failures, {warnings} warning(s)");
    Ok(())
}

/// Why a permission level will not work for this bridge, or `None` if it will.
///
/// `read` and `write` both work: the send tools are annotated read-only, so replying sits at meka's
/// `read` level and only file-modifying tools need `write`.
///
/// `ask` does not, and the reason is not obvious. meka compares the *session* level against `Ask`
/// before dispatching any tool, so at `ask` every call is prompted, read-only ones included. This
/// bridge declares `supports_permission_prompts: false`, so meka denies each one immediately and
/// the agent cannot even reply.
fn permission_problem(level: &str) -> Option<&'static str> {
    match level {
        "read" | "write" => None,
        "ask" => Some(
            "meka prompts for every tool call at `ask`, including read-only ones, and this bridge \
             cannot answer a prompt, so each is denied at once. That includes send_message, so the \
             agent can never reply. Use \"read\" or \"write\".",
        ),
        _ => Some(
            "no tools are executable at this level, so the agent cannot reply. Use \"read\" to let \
             it answer messages, or \"write\" to also let it modify files.",
        ),
    }
}

/// Print what the bridge is currently holding.
pub async fn status(config: &Config) -> Result<()> {
    let store = Store::open(&config.storage.path).await?;
    let stats = store.queue_stats().await?;
    let conversations = store.list_conversations(None, usize::MAX).await?;

    println!("database:      {}", config.storage.path.display());
    match store.session_id().await? {
        Some(session_id) => println!("session:       {session_id}"),
        None => println!("session:       none yet"),
    }
    match store.last_turn_at().await? {
        Some(at) => println!("last turn:     {} ({})", at.to_rfc3339(), describe_age(at)),
        None => println!("last turn:     never"),
    }
    println!(
        "queue:         {} pending, {} in flight, {} delivered, {} failed",
        stats.pending, stats.in_flight, stats.done, stats.failed
    );
    println!("conversations: {}", conversations.len());
    println!("channels:      {}", config.channels.len());
    for channel in &config.channels {
        println!("  {} ({})", channel.id, platform_name(channel));
    }
    Ok(())
}

/// List queued messages.
pub async fn queue_list(config: &Config, limit: usize) -> Result<()> {
    let store = Store::open(&config.storage.path).await?;
    let stats = store.queue_stats().await?;
    println!(
        "{} pending, {} in flight, {} delivered, {} failed",
        stats.pending, stats.in_flight, stats.done, stats.failed
    );
    // Claiming would mark rows in flight, which is exactly what an inspection command must not do,
    // so this peeks at pending rows without touching their state.
    let pending = store.peek_pending(limit).await?;
    if pending.is_empty() {
        println!("nothing waiting");
        return Ok(());
    }
    println!();
    for message in pending {
        println!(
            "  #{:<6} {:<28} attempts {}  received {}",
            message.seq,
            message.conversation_id,
            message.attempts,
            message.received_at.to_rfc3339()
        );
    }
    Ok(())
}

/// Delete every queued message.
pub async fn queue_clear(config: &Config, confirmed: bool) -> Result<()> {
    if !confirmed {
        return Err(BridgeError::command(
            "this deletes every queued message, including undelivered ones; pass --yes to confirm",
        ));
    }
    let store = Store::open(&config.storage.path).await?;
    let deleted = store.clear_queue().await?;
    println!("deleted {deleted} queue row(s)");
    Ok(())
}

/// List known conversations.
pub async fn conversations_list(
    config: &Config,
    channel: Option<&str>,
    limit: usize,
) -> Result<()> {
    let store = Store::open(&config.storage.path).await?;
    let conversations = store.list_conversations(channel, limit).await?;
    if conversations.is_empty() {
        println!("no conversations yet");
        return Ok(());
    }
    for conversation in conversations {
        println!(
            "  {:<28} {:<8} {:<20} last inbound {}",
            conversation.id,
            conversation.kind,
            conversation.title.as_deref().unwrap_or("-"),
            conversation
                .last_inbound_at
                .map_or_else(|| "never".to_string(), |at| at.to_rfc3339())
        );
    }
    Ok(())
}

/// Trim a timestamp to whole seconds for display.
///
/// `to_rfc3339` keeps the nanoseconds SQLite round-trips, which is right in a payload and wrong in
/// a column: nine extra digits push every field after it out of line.
fn short_time(at: chrono::DateTime<Utc>) -> String {
    at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

/// List the conversations somebody has ruled on explicitly.
pub async fn policy_list(config: &Config) -> Result<()> {
    let store = Store::open(&config.storage.path).await?;
    let policies = store.list_policies().await?;
    println!(
        "default: direct {}, group {}, channel {}",
        config.bridge.default_policy.direct.as_str(),
        config.bridge.default_policy.group.as_str(),
        config.bridge.default_policy.channel.as_str()
    );
    if policies.is_empty() {
        println!("no conversation has a policy of its own");
        return Ok(());
    }
    let unseen = store.unseen_counts().await?;
    let now = Utc::now();
    for policy in policies {
        // A lapsed policy is cleared by the next message from that chat, so one that has already
        // run out can still be sitting here. Saying which is which stops it reading as being in
        // force.
        let until = match policy.until {
            Some(until) if until <= now => format!("expired {}", short_time(until)),
            Some(until) => format!("until {}", short_time(until)),
            None => "indefinite".to_string(),
        };
        // Two different tallies, so they are labelled rather than run together: what a block threw
        // away is gone, what a mute withheld is still readable.
        let withheld = match policy.policy {
            Policy::Block => format!("{} discarded", policy.dropped),
            Policy::Mute => format!(
                "{} unseen",
                unseen.get(&policy.conversation_id).copied().unwrap_or(0)
            ),
            Policy::Active => "-".to_string(),
        };
        println!(
            "  {:<28} {:<8} {:<34} {:<14} {}",
            policy.conversation_id,
            policy.policy.as_str(),
            until,
            withheld,
            policy.reason.as_deref().unwrap_or("-")
        );
    }
    Ok(())
}

/// Rule on a conversation from the command line.
pub async fn policy_set(
    config: &Config,
    conversation: &str,
    policy: Policy,
    duration: Option<&str>,
    reason: Option<&str>,
) -> Result<()> {
    let until = duration
        .map(|duration| {
            let parsed = humantime::parse_duration(duration).map_err(|error| {
                BridgeError::config(format!("{duration:?} is not a duration: {error}"))
            })?;
            chrono::Duration::from_std(parsed)
                .map_err(|_| BridgeError::config(format!("{duration:?} is too long")))
        })
        .transpose()?
        .map(|duration| Utc::now() + duration);

    // Validated rather than stored as typed. A policy is keyed by exact id, so a mistyped one would
    // insert a row that governs nothing and report success, which is the worst way to find out.
    let conversation = crate::channel::ConversationId::parse(conversation).ok_or_else(|| {
        BridgeError::config(format!(
            "{conversation:?} is not a conversation id; the form is <channel>:<chat>, as printed by \
             `mekabridge conversations list`"
        ))
    })?;
    let conversation = conversation.as_str();

    let store = Store::open(&config.storage.path).await?;
    store
        .set_policy(conversation, policy, until, reason, Utc::now())
        .await?;
    match until {
        Some(until) => println!(
            "{conversation} set to {} until {}",
            policy.as_str(),
            short_time(until)
        ),
        None => println!("{conversation} set to {}", policy.as_str()),
    }
    Ok(())
}

/// Remove a conversation's own policy, including one the agent set on itself and cannot be asked to
/// undo.
///
/// Distinct from `policy set active`: this returns the conversation to the configured default,
/// which for a group is normally `mute`, whereas setting it active overrides that default.
pub async fn policy_clear(config: &Config, conversation: &str) -> Result<()> {
    let store = Store::open(&config.storage.path).await?;
    if store.clear_policy(conversation).await? {
        println!("{conversation} now follows the configured default");
    } else {
        println!("{conversation} had no policy of its own");
    }
    Ok(())
}

/// Report what the agent has not been shown, as an exit code and one line.
///
/// Built to be a scheduled job's gate, which is why it owns its exit code rather than reporting
/// through `Result` like every other subcommand. A gate distinguishes only "fired" from "did not",
/// so a failure that exits the same way as an honest "nothing new" produces a watcher that goes
/// silent and stays silent. 2 keeps those apart.
///
/// The line on stdout is the other half: a gate watching for it to change needs it to change when
/// and only when something was said, which is what [`crate::store::UnseenSummary::line`] is for.
pub fn unseen(config_path: Option<&Path>, conversation: Option<&str>) -> std::process::ExitCode {
    /// Could not answer, which is not the same as nothing to report.
    const UNAVAILABLE: u8 = 2;
    /// Nothing waiting, the conventional "no match" of a predicate.
    const NOTHING: u8 = 1;

    let answer = super::block_on(async {
        // Checked rather than passed through, because an id the store simply does not match reads
        // as an empty room. A gate built around a typo would then decline to fire for as long as
        // anybody left it running.
        let parsed = match conversation.map(crate::channel::ConversationId::parse) {
            Some(None) => {
                return Err(BridgeError::command(format!(
                    "{:?} is not a conversation id; expected something like \
                     `telegram:-1001234567890`",
                    conversation.unwrap_or_default()
                )));
            }
            other => other.flatten(),
        };
        let config = Config::load(config_path)?;
        // The other half of the same mistake, and the one that survives a well-formed id: a channel
        // segment naming nothing configured can never match a row. The `unseen` tool refuses this
        // through the channel registry, and a gate reading the exit code deserves the same answer
        // from out here.
        if let Some(parsed) = &parsed
            && !config
                .channels
                .iter()
                .any(|channel| channel.id == parsed.channel())
        {
            return Err(BridgeError::command(format!(
                "no channel named {:?} is configured, so {} can never match anything",
                parsed.channel(),
                parsed
            )));
        }
        let store = Store::open(&config.storage.path).await?;
        let summary = store.unseen_summary(conversation).await?;
        // A well-formed id for a chat nothing has arrived from yet is not an error: watching a
        // room before it has said anything is the point. Still worth a word, because it is also
        // what a mistyped id looks like, and the two are otherwise identical from out here. On
        // stderr, which a gate ignores and a person running this by hand will want.
        if let Some(conversation) = conversation
            && summary.count == 0
            && store.conversation(conversation).await?.is_none()
        {
            eprintln!(
                "mekabridge: nothing has ever been recorded from {conversation}, so this will \
                 report nothing until something is"
            );
        }
        Ok(summary)
    });
    match answer {
        Ok(summary) => {
            // Split across the two streams because a gate and a person want different answers and
            // only one of them can have stdout. A watcher comparing output needs a value that
            // moves when the chat does and at no other time, which the backlog is not: it falls to
            // zero on every ordinary turn, and a watcher would fire on that and announce news the
            // agent had just been handed. The count is the useful answer for anybody reading, and
            // it is on stderr, which a gate ignores and a terminal shows.
            println!("{}", summary.marker());
            eprintln!("{}", summary.line());
            if summary.count > 0 {
                std::process::ExitCode::SUCCESS
            } else {
                std::process::ExitCode::from(NOTHING)
            }
        }
        Err(error) => {
            eprintln!("mekabridge: {error}");
            std::process::ExitCode::from(UNAVAILABLE)
        }
    }
}

/// Print what a conversation has said, for checking what history actually holds.
pub async fn history_show(
    config: &Config,
    conversation: &str,
    limit: usize,
    search: Option<&str>,
) -> Result<()> {
    let store = Store::open(&config.storage.path).await?;
    let messages = match search {
        Some(query) => {
            store
                .search_messages(query, Some(conversation), limit)
                .await?
        }
        None => store.history(conversation, limit, None).await?,
    };
    if messages.is_empty() {
        println!("nothing recorded");
        return Ok(());
    }
    for message in messages {
        let marker = if message.addressed { "@" } else { " " };
        let seen = if message.seen { " " } else { "*" };
        println!(
            "{seen}{marker} {}  {:<20} {}",
            short_time(message.timestamp),
            message.sender_name,
            message.text.replace('\n', " ")
        );
    }
    println!("\n* not yet shown to the agent, @ addressed to it");
    Ok(())
}

/// Show the bound session.
pub async fn session_show(config: &Config) -> Result<()> {
    let store = Store::open(&config.storage.path).await?;
    let Some(session_id) = store.session_id().await? else {
        println!("no session is bound yet; one is created on the first message");
        return Ok(());
    };
    println!("session: {session_id}");
    let meka = MekaClient::new(&config.meka)?;
    match meka.session(session_id).await {
        Ok(info) => {
            println!("title:      {}", info.title);
            println!("permission: {}", info.permission);
            println!("cwd:        {}", info.cwd.as_deref().unwrap_or("-"));
            println!(
                "turn:       {}",
                if info.turn_in_flight {
                    "running now"
                } else {
                    "idle"
                }
            );
        }
        Err(error) => println!("meka:       could not read it ({error})"),
    }
    Ok(())
}

/// Forget the session binding so the next message starts a fresh one.
pub async fn session_reset(config: &Config, confirmed: bool) -> Result<()> {
    if !confirmed {
        return Err(BridgeError::command(
            "this discards the agent's memory of every conversation it has had; pass --yes to \
             confirm",
        ));
    }
    let store = Store::open(&config.storage.path).await?;
    match store.session_id().await? {
        Some(session_id) => {
            store.clear_session_id().await?;
            println!("unbound session {session_id}; the next message starts a new one");
            println!(
                "note: the session still exists in meka. Delete it there if you want the history \
                 gone."
            );
        }
        None => println!("no session was bound"),
    }
    Ok(())
}

/// Cancel whatever turn is running.
pub async fn cancel(config: &Config) -> Result<()> {
    let store = Store::open(&config.storage.path).await?;
    let Some(session_id) = store.session_id().await? else {
        println!("no session is bound, so no turn can be running");
        return Ok(());
    };
    let meka = MekaClient::new(&config.meka)?;
    meka.cancel_turn(session_id).await?;
    // meka's cancel is idempotent and returns 204 whether or not a turn was in flight, so there is
    // nothing more definite to report.
    println!("cancellation sent for session {session_id}");
    Ok(())
}

/// Write a starter config.
pub fn config_init(path: &Path, force: bool) -> Result<()> {
    if path.exists() && !force {
        return Err(BridgeError::command(format!(
            "{} already exists; pass --force to overwrite it",
            path.display()
        )));
    }
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, CONFIG_TEMPLATE)?;
    println!("wrote {}", path.display());
    println!("edit it, then run `mekabridge doctor` to check the setup");
    Ok(())
}

fn platform_name(channel: &crate::config::ChannelConfig) -> &'static str {
    match channel.platform {
        crate::config::PlatformConfig::Telegram(_) => "telegram",
        crate::config::PlatformConfig::Discord(_) => "discord",
    }
}

/// Human-readable age, so `status` reads without arithmetic.
fn describe_age(at: chrono::DateTime<Utc>) -> String {
    let elapsed = Utc::now().signed_duration_since(at);
    let seconds = elapsed.num_seconds();
    if seconds < 0 {
        return "in the future".to_string();
    }
    if seconds < 60 {
        return format!("{seconds}s ago");
    }
    if seconds < 3600 {
        return format!("{}m ago", seconds / 60);
    }
    if seconds < 86_400 {
        return format!("{}h ago", seconds / 3600);
    }
    format!("{}d ago", seconds / 86_400)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_and_write_both_let_the_agent_reply() {
        // The send tools are annotated read-only, so `read` is a supported posture: the agent can
        // answer messages without being able to modify files.
        assert!(permission_problem("read").is_none());
        assert!(permission_problem("write").is_none());
    }

    #[test]
    fn ask_and_none_are_reported_as_unworkable() {
        // `ask` is the subtle one: meka prompts on the session level, so even a read-only call is
        // gated, and this bridge denies every prompt.
        let ask = permission_problem("ask").expect("ask must be rejected");
        assert!(ask.contains("send_message"), "{ask}");
        assert!(permission_problem("none").is_some());
    }

    #[test]
    fn age_is_described_in_the_largest_useful_unit() {
        let now = Utc::now();
        assert!(describe_age(now).ends_with("s ago"));
        assert_eq!(describe_age(now - chrono::Duration::minutes(5)), "5m ago");
        assert_eq!(describe_age(now - chrono::Duration::hours(3)), "3h ago");
        assert_eq!(describe_age(now - chrono::Duration::days(2)), "2d ago");
    }

    #[test]
    fn a_future_timestamp_does_not_produce_nonsense() {
        // Clock skew between the daemon host and the operator's shell is real.
        let future = Utc::now() + chrono::Duration::hours(1);
        assert_eq!(describe_age(future), "in the future");
    }

    #[test]
    fn the_bundled_template_is_a_valid_config() {
        // The template ships with placeholder env vars, so resolution needs them present.
        // SAFETY: these run in a single-threaded test section.
        unsafe {
            std::env::set_var("MEKA_BRIDGE_TOKEN", "test-token");
            std::env::set_var("TELEGRAM_BOT_TOKEN", "123:test");
        }
        let config = Config::from_toml(CONFIG_TEMPLATE, Path::new("/etc/mekabridge/config.toml"))
            .expect("the shipped template must parse and validate");
        assert_eq!(config.channels.len(), 1);
    }

    #[test]
    fn config_init_refuses_to_clobber_without_force() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("config.toml");
        std::fs::write(&path, "existing").expect("write");
        let error = config_init(&path, false).expect_err("must refuse");
        assert!(error.to_string().contains("--force"));
        assert_eq!(std::fs::read_to_string(&path).expect("read"), "existing");
    }

    #[test]
    fn config_init_creates_missing_directories() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("nested").join("config.toml");
        config_init(&path, false).expect("writes");
        assert!(path.exists());
    }
}
