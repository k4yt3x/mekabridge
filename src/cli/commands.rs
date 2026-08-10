//! Implementations of the operator subcommands.
//!
//! None of these talk to a running daemon. They read the SQLite database directly (WAL permits
//! concurrent readers) and call meka's HTTP API themselves, so every command works whether the
//! bridge is up or down. That avoids inventing a control socket, and it means a wedged daemon can
//! still be inspected.

use std::path::Path;

use chrono::Utc;

use crate::{
    channel::ChannelRegistry,
    config::{Config, McpTransport},
    error::{BridgeError, Result},
    meka::MekaClient,
    store::Store,
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
                println!("  ok     vision is on; inbound images ride on the turn itself");
            } else {
                println!(
                    "  warn   the active provider profile has vision off, so inbound images are \
                     only named by path. Set `vision = true` under [providers.<name>] to have the \
                     agent see them."
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
    match ChannelRegistry::build(&config.channels, &config.storage) {
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
