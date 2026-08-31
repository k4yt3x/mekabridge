//! Implementations of the operator subcommands.
//!
//! None of these talk to a running daemon. They read the SQLite database directly (WAL permits
//! concurrent readers) and call meka's HTTP API themselves, so every command works whether the
//! bridge is up or down. That avoids inventing a control socket, and it means a wedged daemon can
//! still be inspected.

use std::{collections::HashSet, path::Path};

use chrono::Utc;

use crate::{
    channel::{ChannelError, ChannelRegistry, ConversationId, Platform},
    config::{Config, McpTransport, PlatformConfig},
    error::{BridgeError, Result},
    meka::MekaClient,
    store::{Policy, Store},
};

/// Starter config written by `mekabridge config init`.
const CONFIG_TEMPLATE: &str = include_str!("config_template.toml");

/// One line `doctor` prints, and whether it counts against the run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Check {
    Ok(String),
    Warn(String),
    Fail(String),
}

/// How many failures and warnings a set of checks contributes.
///
/// Shared with [`doctor`] rather than reimplemented there, so a test can assert on the arithmetic
/// that decides the exit code.
pub fn verdict(checks: &[Check]) -> (usize, usize) {
    let failures = checks
        .iter()
        .filter(|check| matches!(check, Check::Fail(_)))
        .count();
    let warnings = checks
        .iter()
        .filter(|check| matches!(check, Check::Warn(_)))
        .count();
    (failures, warnings)
}

/// Decide what meka's readiness answer means.
///
/// Pure, and separate from [`doctor`], so it can be tested at all. `doctor` talks to the store, to
/// meka, and to every configured channel, so its exit status says only that *something* is wrong: a
/// test asserting `is_err()` against a readiness body passes just as well when the readiness checks
/// are deleted, because a channel with a placeholder token fails alongside them.
pub fn assess_readiness(ready: &crate::meka::ReadyStatus) -> Vec<Check> {
    let mut checks = Vec::new();
    // meka answers 503 with this same body, naming which subsystem is the blocker, so a readiness
    // that is not `ok` has to count against the run.
    if ready.status == "ok" {
        checks.push(Check::Ok(format!("readiness: {}", ready.status)));
    } else {
        checks.push(Check::Warn(format!("readiness: {}", ready.status)));
    }
    if !ready.session_db {
        // meka could not read its own session store, so no turn can run and nothing here recovers
        // on its own.
        checks.push(Check::Fail(
            "meka cannot reach its session database; no turn can run".to_string(),
        ));
    }
    if !ready.provider_configured {
        // Likewise terminal: without a provider meka cannot serve a single turn. Reported as a
        // warning, `doctor` exited 0 against it, which is the opposite of what a gate wants.
        checks.push(Check::Fail(
            "meka reports no provider is configured; no turn can run".to_string(),
        ));
    }
    if !ready.mcp_servers_healthy {
        // Usually meka having started before this bridge was listening, which it retries out of on
        // its own, so a warning. Worth saying that this flag only counts servers meka was told are
        // required: with the entry for this bridge left at the default, meka reports itself healthy
        // while running turns that have no way to answer anybody, and this line never appears.
        checks.push(Check::Warn(
            "meka reports an unhealthy required MCP server. If that is this bridge, it should \
             reconnect on its own within a few minutes; turns are refused until it does."
                .to_string(),
        ));
    }
    checks
}

/// Ask the platform whether the conversation operator notices go to is one it can reach.
///
/// Startup validation can only judge the shape of the id, and on Discord the shape says nothing:
/// a user id and a channel id are both snowflakes, so the wrong one sits in the config looking
/// correct until a notice fails to send, which by definition is during an incident. This is the
/// only check here that costs a round trip to settle a question the config cannot answer alone.
///
/// Warns rather than fails. Nothing about the bridge stops working, but the one channel it has for
/// telling an operator that a message never reached the agent is dead, and quietly.
async fn assess_owner_conversation(
    registry: &ChannelRegistry,
    owner: &str,
    authenticated: &HashSet<&str>,
) -> Vec<Check> {
    // Both of these are reported already, by config validation, with the form the id should take.
    let Some(conversation) = ConversationId::parse(owner) else {
        return Vec::new();
    };
    let Some(channel) = registry.get(conversation.channel()) else {
        return Vec::new();
    };
    // A channel that is not logged in refuses every question the same way, so asking would return
    // its own failure a second time, dressed up as a problem with this id. That failure is printed
    // just above and is what has to be fixed first.
    if !authenticated.contains(conversation.channel()) {
        return Vec::new();
    }

    match channel.describe_conversation(&conversation).await {
        Ok(info) => {
            let title = info
                .title
                .map(|title| format!(" ({title})"))
                .unwrap_or_default();
            // Worth printing even when it did not change: seeing the resolved id is how an operator
            // matches this against what `mekabridge conversations list` shows.
            let resolved = if info.id.as_str() == owner {
                String::new()
            } else {
                format!(", which is {}", info.id.as_str())
            };
            vec![Check::Ok(format!(
                "[bridge].owner_conversation reaches a {} chat{title}{resolved}",
                info.kind.as_str()
            ))]
        }
        Err(ChannelError::Unsupported { .. }) => vec![Check::Ok(format!(
            "{} cannot look a conversation id up, so [bridge].owner_conversation is taken on trust",
            channel.id()
        ))],
        Err(error) => {
            let mut line = format!(
                "[bridge].owner_conversation {owner:?} cannot be reached, so an operator notice \
                 goes nowhere: {error}"
            );
            if channel.platform() == Platform::Discord && !conversation.chat().starts_with('@') {
                line.push_str(
                    ". A Discord user id and a channel id look alike, and only a channel id can be \
                     posted to. To reach a person, dial them as `discord:@<user id>` and the \
                     bridge opens the direct message itself",
                );
            }
            vec![Check::Warn(line)]
        }
    }
}

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
    // Carried out of the match so the verdict can be printed under `session`, next to the other
    // two permission checks. Empty means either meka said nothing (a build predating the field) or
    // it could not be reached, and both read the same way: no opinion, so no verdict.
    let mut enabled_permissions: Vec<String> = Vec::new();
    match meka.info().await {
        Ok(info) => {
            println!("  ok     reachable, version {}", info.version);
            // A separate call since meka 0.44, which took `model` off `/v1/info` because the word
            // there named a backend while the same word on `POST /v1/sessions` names a profile.
            // Every meka this bridge supports has this endpoint, back to 0.42, so a failure here
            // is a real one rather than a version it is too old for.
            match meka.providers().await {
                Ok(profiles) => match profiles.iter().find(|profile| profile.active) {
                    Some(profile) => println!(
                        "  ok     sessions run on profile {} ({}), model {}",
                        profile.name,
                        profile.backend,
                        profile.model.as_deref().unwrap_or("(none configured)")
                    ),
                    None => {
                        // The bridge creates its session without naming a profile, so meka has to
                        // have one to fall back on. Nothing marked active means either no profile
                        // is configured or several are with no default chosen between them, and
                        // meka defers that failure to the first session rather than refusing to
                        // start, so this surfaces at the first message otherwise. The remedy is
                        // meka's own wording: `default_provider` is what a command writes, and
                        // sending an operator to hand-edit the file instead is worse advice.
                        println!(
                            "  fail   meka has no default provider profile, so creating the \
                             bridge's session will fail; run `meka provider use <name>`"
                        );
                        failures += 1;
                    }
                },
                Err(error) => {
                    println!("  warn   could not read the provider profiles: {error}");
                    warnings += 1;
                }
            }
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
            enabled_permissions = info.enabled_permissions;
        }
        Err(error) => {
            println!("  fail   {error}");
            failures += 1;
        }
    }
    match meka.ready().await {
        Ok(ready) => {
            let checks = assess_readiness(&ready);
            for check in &checks {
                match check {
                    Check::Ok(line) => println!("  ok     {line}"),
                    Check::Warn(line) => println!("  warn   {line}"),
                    Check::Fail(line) => println!("  fail   {line}"),
                }
            }
            let (failed, warned) = verdict(&checks);
            failures += failed;
            warnings += warned;
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
    if let Some(problem) =
        level_meka_will_not_create(config.session.permission.as_str(), &enabled_permissions)
    {
        println!("  fail   {problem}");
        failures += 1;
    }
    let admin_tools = config
        .channels
        .iter()
        .any(|channel| match &channel.platform {
            PlatformConfig::Telegram(telegram) => telegram.admin_tools,
            PlatformConfig::Discord(discord) => discord.admin_tools,
        });
    if let Some(reach) = moderation_reach(config.session.permission.as_str(), admin_tools) {
        println!("  ok     {reach}");
    }

    println!("channels");
    match ChannelRegistry::build(&config.channels) {
        Ok(registry) => {
            let mut authenticated: HashSet<&str> = HashSet::new();
            for channel in registry.iter() {
                match channel.probe().await {
                    Ok(identity) => {
                        let label = identity
                            .username
                            .map_or(identity.display_name.clone(), |username| {
                                format!("@{username}")
                            });
                        println!("  ok     {} authenticated as {label}", channel.id());
                        authenticated.insert(channel.id().as_str());
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
            if let Some(owner) = &config.bridge.owner_conversation {
                let checks = assess_owner_conversation(&registry, owner, &authenticated).await;
                for check in &checks {
                    match check {
                        Check::Ok(line) => println!("  ok     {line}"),
                        Check::Warn(line) => println!("  warn   {line}"),
                        Check::Fail(line) => println!("  fail   {line}"),
                    }
                }
                let (failed, warned) = verdict(&checks);
                failures += failed;
                warnings += warned;
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
/// `read`, `workspace` and `unrestricted` all work: the send tools are annotated read-only, so
/// replying sits at meka's `read` level and every rung above it allows what `read` allows.
///
/// `ask` does not, and the reason is not obvious. meka compares the *session* level against `Ask`
/// before dispatching any tool, so at `ask` every call is prompted, read-only ones included. This
/// bridge declares `supports_permission_prompts: false`, so meka denies each one immediately and
/// the agent cannot even reply. `ask` is also outside meka's default enabled set, so a session
/// asking for it is usually refused at creation before any of that is reached.
///
/// What no level short of `unrestricted` reaches is the five moderation tools, and that is not
/// stated here because it is not a *failure*: a bridge that never moderates is correct at `read`.
/// [`moderation_reach`] reports it separately, as a statement of fact.
///
/// `write` is unreachable from `[session].permission`, which refuses it while parsing, but not from
/// the level meka reports for an existing session: a session created against meka 0.41 carries it,
/// and that is the reading this arm is for.
fn permission_problem(level: &str) -> Option<&'static str> {
    match level {
        "read" | "workspace" | "unrestricted" => None,
        "ask" => Some(
            "meka prompts for every tool call at `ask`, including read-only ones, and this bridge \
             cannot answer a prompt, so each is denied at once. That includes send_message, so the \
             agent can never reply. Use \"read\", \"workspace\" or \"unrestricted\".",
        ),
        "write" => Some(
            "meka 0.42 retired `write` and split it into `workspace` and `unrestricted`, so a \
             session cannot be created at this level at all. Use \"read\" to answer messages, or \
             \"unrestricted\" to also moderate.",
        ),
        _ => Some(
            "no tools are executable at this level, so the agent cannot reply. Use \"read\" to let \
             it answer messages.",
        ),
    }
}

/// Why meka will refuse to create a session at `level`, or `None` if it will accept one.
///
/// Separate from [`permission_problem`], which asks whether a level suits *this bridge*. This one
/// asks whether the level exists on the other side at all, and it is the earlier failure: meka
/// checks its `[permissions].enabled` set before anything else, so a level outside it is a 422 on
/// the first message rather than an agent that behaves oddly. `ask` is the one to catch, being
/// outside meka's default set.
///
/// An empty `enabled` means no opinion, from a meka that did not report the field or could not be
/// reached, and yields no verdict rather than a guess.
fn level_meka_will_not_create(level: &str, enabled: &[String]) -> Option<String> {
    if enabled.is_empty() || enabled.iter().any(|allowed| allowed == level) {
        return None;
    }
    Some(format!(
        "meka will not create a session at {level:?}; it enables {}. Add it to meka's \
         [permissions].enabled, or set [session].permission to one of those.",
        enabled.join(", ")
    ))
}

/// That the five moderation tools are registered and cannot be dispatched at `level`, if so.
///
/// Stated rather than warned about, because `admin_tools` defaults on and `permission` defaults to
/// `read`, so this is the shipped posture and always has been: before 0.42 the same pairing put
/// them out of reach of `write`. A line that fires on every default install teaches operators to
/// skim warnings, which costs more than this is worth.
///
/// Worth stating at all because the level that *does* reach them moved, and the failure is
/// otherwise silent from both ends: the agent is told inside a tool result nothing surfaces, and
/// the operator sees a bot that chats normally and declines to ban.
///
/// The chain is three steps and only the last is surprising. `readOnlyHint: false` resolves to
/// `unrestricted` rather than to the old `write`. `Permission::allows` then treats `workspace`,
/// `ask` and `unrestricted` as equal, which reads like `workspace` is enough. But an MCP tool runs
/// in its server's own process, which meka does not sandbox, so a second gate refuses anything
/// requiring `unrestricted` while the session sits at `workspace`, rather than promise a
/// confinement it cannot apply.
fn moderation_reach(level: &str, admin_tools: bool) -> Option<String> {
    if !admin_tools || level == "unrestricted" {
        return None;
    }
    Some(format!(
        "the moderation tools are registered but do not run at {level:?}: meka resolves them to \
         `unrestricted` and refuses them below it. Raise [session].permission, or grant them \
         individually with meka's [mcp.servers.*].tool_permissions"
    ))
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
        // The bridge's own messages read as an ordinary participant otherwise, distinguishable
        // only by whoever happens to know the bot's display name.
        let side = if message.own { ">" } else { " " };
        // A retracted or rewritten message reading as though it still stands is the same defect the
        // agent's own history tools were fixed for; an operator scrolling this deserves the same
        // answer.
        let state = match (message.deleted_at, message.superseded_at) {
            (Some(_), _) => "  [deleted]",
            (None, Some(_)) => "  [superseded by a later edit]",
            (None, None) => "",
        };
        println!(
            "{seen}{marker}{side} {}  {:<20} {}{state}",
            short_time(message.timestamp),
            message.sender_name,
            message.text.replace('\n', " ")
        );
    }
    println!("\n* not yet shown to the agent, @ addressed to it, > sent by the agent");
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
    // Aliased rather than imported as `Result`, which would shadow the module's own alias for
    // every other test here and turn an ordinary `Result<()>` into a confusing arity error.
    use std::{result::Result as StdResult, sync::Arc};

    use async_trait::async_trait;
    use tokio_util::sync::CancellationToken;

    use crate::{
        channel::{
            Channel, ChannelCapabilities, ChannelError, ChannelId, ChannelIdentity, ChatKind,
            ConversationId, ConversationInfo, FetchedFile, FileOptions, InboundEvent, Platform,
            SendOptions, SentMessage,
        },
        cli::commands::{Check, assess_owner_conversation, assess_readiness},
        meka::ReadyStatus,
    };

    /// What the platform under test says when asked about a conversation.
    ///
    /// [`ChannelError`] is not `Clone`, so the refusal is described here and built on the spot
    /// rather than held.
    enum Answer {
        Reaches(ConversationInfo),
        /// The id names nothing the bot can post to, which is what a Discord user id does.
        Refuses,
        /// The platform offers no way to look a conversation up.
        CannotSay,
    }

    struct OwnerProbe {
        id: ChannelId,
        platform: Platform,
        answer: Answer,
    }

    #[async_trait]
    impl Channel for OwnerProbe {
        fn id(&self) -> &ChannelId {
            &self.id
        }

        fn platform(&self) -> Platform {
            self.platform
        }

        fn capabilities(&self) -> ChannelCapabilities {
            ChannelCapabilities {
                member_rights: false,
                member_roles: false,
                typing_indicator: false,
                typing_status: false,
                files: true,
                photos: true,
                reactions: false,
                edit: false,
                admin: false,
                presence: false,
            }
        }

        async fn run(
            self: Arc<Self>,
            _sink: tokio::sync::mpsc::Sender<InboundEvent>,
            shutdown: CancellationToken,
        ) -> StdResult<(), ChannelError> {
            shutdown.cancelled().await;
            Ok(())
        }

        async fn send_text(
            &self,
            _conversation: &ConversationId,
            _markdown: &str,
            _options: &SendOptions,
            _sent: &mut Vec<SentMessage>,
        ) -> StdResult<(), ChannelError> {
            Ok(())
        }

        async fn send_files(
            &self,
            _conversation: &ConversationId,
            _paths: &[std::path::PathBuf],
            _caption: Option<&str>,
            _options: &FileOptions,
            _sent: &mut Vec<SentMessage>,
        ) -> StdResult<(), ChannelError> {
            Ok(())
        }

        async fn fetch(
            &self,
            _file_ref: &str,
            _max_bytes: u64,
        ) -> StdResult<FetchedFile, ChannelError> {
            Ok(FetchedFile {
                bytes: Vec::new(),
                media_type: None,
                extension: None,
            })
        }

        async fn react(
            &self,
            _conversation: &ConversationId,
            _message_id: &str,
            _emoji: Option<&str>,
        ) -> StdResult<(), ChannelError> {
            Ok(())
        }

        async fn describe_conversation(
            &self,
            _conversation: &ConversationId,
        ) -> StdResult<ConversationInfo, ChannelError> {
            match &self.answer {
                Answer::Reaches(info) => Ok(info.clone()),
                Answer::Refuses => Err(ChannelError::Delivery {
                    channel: self.id.as_str().to_string(),
                    message: "reading the channel: Unknown Channel".to_string(),
                }),
                Answer::CannotSay => Err(ChannelError::Unsupported {
                    channel: self.id.as_str().to_string(),
                    feature: "looking a conversation up",
                }),
            }
        }

        async fn set_activity(
            &self,
            _conversation: &ConversationId,
            _activity: crate::channel::Activity,
        ) -> StdResult<(), ChannelError> {
            Ok(())
        }

        async fn probe(&self) -> StdResult<ChannelIdentity, ChannelError> {
            Ok(ChannelIdentity {
                id: "1".to_string(),
                display_name: "Probe".to_string(),
                username: None,
                reads_all_group_messages: true,
            })
        }
    }

    async fn owner_checks(platform: Platform, answer: Answer, owner: &str) -> Vec<Check> {
        owner_checks_with(platform, answer, owner, true).await
    }

    async fn owner_checks_with(
        platform: Platform,
        answer: Answer,
        owner: &str,
        authenticated: bool,
    ) -> Vec<Check> {
        let name = match platform {
            Platform::Telegram => "telegram",
            Platform::Discord => "discord",
        };
        let channel = Arc::new(OwnerProbe {
            id: ChannelId::new(name),
            platform,
            answer,
        });
        let registry =
            crate::channel::ChannelRegistry::from_channels([channel as Arc<dyn Channel>]);
        let logged_in = if authenticated {
            HashSet::from([name])
        } else {
            HashSet::new()
        };
        assess_owner_conversation(&registry, owner, &logged_in).await
    }

    #[tokio::test]
    async fn an_owner_conversation_that_goes_nowhere_is_found_before_it_is_needed() {
        // The 2026-08-30 config: a Discord user id where a channel id belongs. It parses, it names
        // a configured channel, and every operator notice sent to it was silently lost.
        let checks = owner_checks(
            Platform::Discord,
            Answer::Refuses,
            "discord:1354919639859335428",
        )
        .await;
        assert_eq!(verdict(&checks), (0, 1), "unreachable, but the bridge runs");
        let Some(Check::Warn(line)) = checks.first() else {
            panic!("expected one warning, got {checks:?}");
        };
        assert!(
            line.contains("discord:@<user id>"),
            "the warning has to name the form that works: {line}"
        );
    }

    #[tokio::test]
    async fn a_dialling_address_is_not_told_to_become_a_dialling_address() {
        // Already in the `@` form, so the failure is something else and the hint would misdirect.
        // Keyed on the same phrase the test above requires, so rewording the hint breaks that one
        // loudly rather than leaving this one asserting nothing.
        let checks = owner_checks(
            Platform::Discord,
            Answer::Refuses,
            "discord:@1354919639859335428",
        )
        .await;
        let Some(Check::Warn(line)) = checks.first() else {
            panic!("expected one warning, got {checks:?}");
        };
        assert!(
            !line.contains("discord:@<user id>"),
            "misdirecting hint: {line}"
        );
    }

    #[tokio::test]
    async fn a_telegram_failure_is_not_given_discord_advice() {
        // Telegram chat ids and user ids are not interchangeable, so the whole confusion the hint
        // explains does not exist there.
        let checks = owner_checks(Platform::Telegram, Answer::Refuses, "telegram:123").await;
        let Some(Check::Warn(line)) = checks.first() else {
            panic!("expected one warning, got {checks:?}");
        };
        assert!(
            !line.contains("discord:@<user id>"),
            "advice for the wrong platform: {line}"
        );
    }

    #[tokio::test]
    async fn an_id_that_resolved_to_itself_is_not_reported_as_having_moved() {
        let checks = owner_checks(
            Platform::Telegram,
            Answer::Reaches(ConversationInfo {
                id: ConversationId::parse("telegram:-1001234567890").expect("valid"),
                kind: ChatKind::Group,
                title: Some("Acme".to_string()),
            }),
            "telegram:-1001234567890",
        )
        .await;
        let Some(Check::Ok(line)) = checks.first() else {
            panic!("expected one pass, got {checks:?}");
        };
        assert!(!line.contains("which is"), "nothing moved: {line}");
    }

    #[tokio::test]
    async fn a_resolved_owner_conversation_reports_the_id_it_resolved_to() {
        // The dialling address is not the id anything else will show, so printing only the
        // configured one would leave an operator unable to match this against
        // `mekabridge conversations list`.
        let checks = owner_checks(
            Platform::Discord,
            Answer::Reaches(ConversationInfo {
                id: ConversationId::parse("discord:1537199580129525881").expect("valid"),
                kind: ChatKind::Direct,
                title: Some("Kay".to_string()),
            }),
            "discord:@1354919639859335428",
        )
        .await;
        assert_eq!(verdict(&checks), (0, 0));
        let Some(Check::Ok(line)) = checks.first() else {
            panic!("expected one pass, got {checks:?}");
        };
        assert!(line.contains("direct"), "{line}");
        assert!(line.contains("Kay"), "{line}");
        assert!(line.contains("discord:1537199580129525881"), "{line}");
    }

    #[tokio::test]
    async fn a_platform_that_cannot_look_up_says_so_rather_than_claiming_a_pass() {
        let checks = owner_checks(Platform::Telegram, Answer::CannotSay, "telegram:123").await;
        assert_eq!(verdict(&checks), (0, 0), "no lookup is not a fault");
        let Some(Check::Ok(line)) = checks.first() else {
            panic!("expected one pass, got {checks:?}");
        };
        assert!(line.contains("taken on trust"), "{line}");
    }

    #[tokio::test]
    async fn a_channel_that_is_not_logged_in_is_not_blamed_on_the_owner_conversation() {
        // A rejected token refuses this lookup exactly as a wrong id does, so asking would report
        // the token problem a second time and attach advice about ids to it.
        assert!(
            owner_checks_with(
                Platform::Discord,
                Answer::Refuses,
                "discord:1354919639859335428",
                false,
            )
            .await
            .is_empty()
        );
    }

    #[tokio::test]
    async fn an_owner_conversation_config_validation_already_rejected_is_not_reported_twice() {
        // Startup fails on a channel that is not configured and warns on an id that will not
        // parse. Repeating either here would only make `doctor` noisier about the same mistake.
        assert!(
            owner_checks(Platform::Telegram, Answer::Refuses, "slack:123")
                .await
                .is_empty()
        );
        assert!(
            owner_checks(Platform::Telegram, Answer::Refuses, "telegram")
                .await
                .is_empty()
        );
    }

    fn readiness(status: &str, session_db: bool, provider: bool, mcp: bool) -> ReadyStatus {
        ReadyStatus {
            status: status.to_string(),
            session_db,
            provider_configured: provider,
            mcp_servers_healthy: mcp,
        }
    }

    #[test]
    fn a_meka_that_cannot_serve_a_turn_fails_rather_than_warns() {
        // `doctor` doubles as a deployment gate, so what it does with a degraded readiness is its
        // exit code. Both of these mean no turn can run and neither clears on its own, and both
        // were reported as warnings, which exits 0.
        assert!(
            assess_readiness(&readiness("degraded", false, true, true))
                .iter()
                .any(|check| matches!(check, Check::Fail(_))),
            "an unreachable session database was not a failure"
        );
        assert!(
            assess_readiness(&readiness("degraded", true, false, true))
                .iter()
                .any(|check| matches!(check, Check::Fail(_))),
            "no configured provider was not a failure"
        );
        // An unhealthy MCP server is the one that does clear on its own, so it stays a warning.
        let mcp_down = assess_readiness(&readiness("degraded", true, true, false));
        assert!(
            mcp_down
                .iter()
                .all(|check| !matches!(check, Check::Fail(_)))
        );
        assert!(mcp_down.iter().any(|check| matches!(check, Check::Warn(_))),);
    }

    #[test]
    fn a_failing_check_is_what_makes_doctor_exit_non_zero() {
        // `assess_readiness` is well covered, but what `doctor` *does* with a `Fail` is the whole
        // point: it is the deployment gate's exit code. Counting one as a warning leaves the gate
        // green against a meka that cannot serve a turn, which is the bug the extraction was made
        // to fix and the one line the extraction does not itself cover.
        let checks = assess_readiness(&readiness("degraded", false, false, false));
        let failures = checks
            .iter()
            .filter(|check| matches!(check, Check::Fail(_)))
            .count();
        assert!(failures >= 2, "got {checks:?}");
        assert_eq!(
            verdict(&checks),
            (failures, 2),
            "the counts `doctor` adds up must match the checks it was handed"
        );
        // A healthy answer contributes to neither, so nothing else in `doctor` can be nudged into
        // failing by a readiness that is fine.
        assert_eq!(
            verdict(&assess_readiness(&readiness("ok", true, true, true))),
            (0, 0)
        );
    }

    #[test]
    fn a_healthy_readiness_counts_against_nothing() {
        assert_eq!(assess_readiness(&readiness("ok", true, true, true)), vec![
            Check::Ok("readiness: ok".to_string())
        ]);
        // A status meka itself never sends still has to register: reading the body rather than the
        // status is what made the answer legible, and printing it under `ok` regardless would let a
        // gate go green against a meka that said it was degraded.
        assert!(
            assess_readiness(&readiness("degraded", true, true, true))
                .iter()
                .any(|check| matches!(check, Check::Warn(_))),
            "a degraded readiness was reported as ok"
        );
    }

    use super::*;

    #[test]
    fn every_level_that_can_reply_is_accepted() {
        // The send tools are annotated read-only, so `read` is a supported posture: the agent can
        // answer messages without being able to modify anything. The two rungs above it allow
        // everything `read` allows, so they work for replying too.
        assert!(permission_problem("read").is_none());
        assert!(permission_problem("workspace").is_none());
        assert!(permission_problem("unrestricted").is_none());
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
    fn the_retired_write_level_is_named_rather_than_lumped_in_with_a_typo() {
        // The one an operator upgrading meka actually hits. A session cannot be created at it at
        // all, so the message has to say that and name what replaced it, or the reader concludes
        // the bridge stopped supporting a level meka still has.
        let problem = permission_problem("write").expect("write must be rejected");
        assert!(problem.contains("0.42"), "{problem}");
        assert!(problem.contains("unrestricted"), "{problem}");
    }

    #[test]
    fn moderation_is_only_within_reach_at_unrestricted() {
        // `workspace` is the trap: meka's `allows` treats it as equal to `unrestricted`, so the
        // level looks sufficient, and a second gate refuses the call because an MCP tool runs
        // outside anything meka can confine.
        assert!(moderation_reach("unrestricted", true).is_none());
        for level in ["read", "workspace"] {
            let reach = moderation_reach(level, true)
                .unwrap_or_else(|| panic!("{level} cannot reach the moderation tools"));
            assert!(reach.contains("tool_permissions"), "{reach}");
        }
    }

    #[test]
    fn a_level_meka_does_not_enable_is_caught_before_the_first_message() {
        // meka's own default set. `ask` is opt-in and absent from it, which is the case worth
        // catching: without this the session is refused at creation, on the first message, long
        // after `doctor` said everything was fine.
        let enabled: Vec<String> = ["none", "read", "workspace", "unrestricted"]
            .iter()
            .map(|level| (*level).to_string())
            .collect();
        assert!(level_meka_will_not_create("read", &enabled).is_none());
        assert!(level_meka_will_not_create("unrestricted", &enabled).is_none());
        let refused = level_meka_will_not_create("ask", &enabled).expect("ask is not enabled");
        assert!(refused.contains("ask"), "{refused}");
        // The way out has to name what meka would accept; "not enabled" alone leaves the operator
        // guessing which of five words to try.
        assert!(refused.contains("workspace"), "{refused}");
    }

    #[test]
    fn a_meka_with_no_opinion_draws_no_verdict() {
        // Empty covers two cases that must not become a guess: a meka too old to report the field,
        // and one `doctor` could not reach at all. Failing either would make `doctor` red over a
        // configuration that is fine.
        for level in ["read", "ask", "unrestricted", "nonsense"] {
            assert!(level_meka_will_not_create(level, &[]).is_none());
        }
    }

    #[test]
    fn moderation_reach_is_silent_when_the_tools_are_not_registered() {
        // Nothing is out of reach if it was never offered, and a warning about a capability the
        // operator deliberately turned off is noise that trains them to ignore the rest.
        for level in ["read", "workspace", "unrestricted"] {
            assert!(moderation_reach(level, false).is_none());
        }
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
