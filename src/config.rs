//! Configuration: the on-disk TOML shape, and the validated form the rest of the program uses.
//!
//! The two are deliberately different types. Everything prefixed `File` is the raw deserialized
//! table, private to this module; the public types are what you get after credentials are resolved,
//! paths are expanded, and cross-field invariants are checked. Downstream code therefore never has
//! to ask "was this validated yet", and validation errors all surface at startup instead of at the
//! first inbound message.

pub mod secret;

use std::{
    collections::HashSet,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use serde::Deserialize;
use url::Url;

use crate::{
    config::secret::Secret,
    error::{BridgeError, Result},
};

/// Directory name used under the platform config and data directories.
const APP_DIR: &str = "mekabridge";

/// Validated configuration.
#[derive(Debug)]
pub struct Config {
    pub meka: MekaConfig,
    pub session: SessionConfig,
    pub bridge: BridgeConfig,
    pub mcp: McpConfig,
    pub storage: StorageConfig,
    pub log: LogConfig,
    pub channels: Vec<ChannelConfig>,
    /// Advisories collected while resolving. Configuration is parsed before the tracing subscriber
    /// exists, so these are replayed by the caller once logging is up.
    pub warnings: Vec<String>,
}

/// How to reach `meka serve`.
#[derive(Debug)]
pub struct MekaConfig {
    pub base_url: Url,
    pub token: Secret,
    pub connect_timeout: Duration,
    /// Wall-clock ceiling on a single turn, including the whole SSE stream.
    pub turn_timeout: Duration,
    /// Attempts made against retryable failures (connect errors, 5xx, 429) before giving up.
    pub max_retries: u32,
}

/// The one meka session this bridge instance owns.
#[derive(Debug)]
pub struct SessionConfig {
    pub cwd: Option<PathBuf>,
    pub permission: Permission,
    /// Create a replacement session when meka reports the stored id no longer exists.
    pub recreate_on_missing: bool,
}

/// Cross-cutting relay behaviour.
#[derive(Debug)]
pub struct BridgeConfig {
    /// Conversation that receives operator notifications, such as a turn that failed every retry.
    pub owner_conversation: Option<String>,
    pub max_queue_depth: usize,
    pub batch_max_messages: usize,
    /// Quiet period a conversation must go through before its messages are handed to the agent.
    ///
    /// Without one, the first fragment of a burst starts a turn on its own and the agent answers
    /// before it has read the rest of the thought.
    pub settle: Duration,
    /// Ceiling on how long [`BridgeConfig::settle`] may defer a message.
    ///
    /// Matters more than it looks: in a chat busy enough that messages keep arriving inside the
    /// settle window, the timer never expires and this becomes the normal release path rather than
    /// a rare fallback.
    pub settle_max: Duration,
    /// Extra attempts for a batch whose turn failed. `0` means a failed batch is never retried.
    pub turn_retries: u32,
    pub typing_indicator: bool,
}

/// How meka reaches this bridge's MCP server.
#[derive(Debug)]
pub struct McpConfig {
    pub transport: McpTransport,
    pub bind: SocketAddr,
    pub path: String,
    /// Bearer token meka must present. `None` leaves the endpoint unauthenticated, which is only
    /// reasonable on a loopback bind.
    pub token: Option<Secret>,
    /// `Host` header values the MCP transport accepts. rmcp defaults to loopback names to blunt
    /// DNS rebinding, so a non-loopback bind needs this set.
    pub allowed_hosts: Vec<String>,
    pub health: bool,
}

/// MCP transport the bridge serves on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpTransport {
    /// Streamable HTTP on [`McpConfig::bind`]. meka connects with `transport = "http"`.
    Http,
    /// stdio, for `meka mcp` style child-process launches and MCP Inspector debugging.
    Stdio,
}

/// Where durable state lives.
#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub path: PathBuf,
    pub attachment_dir: PathBuf,
    pub attachment_max_bytes: u64,
    pub attachment_retention: Duration,
}

/// Logging setup.
#[derive(Debug)]
pub struct LogConfig {
    /// `EnvFilter` directive string, for example `info` or `mekabridge=debug,teloxide=warn`.
    pub level: String,
    pub format: LogFormat,
}

/// Log output shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogFormat {
    Text,
    Json,
}

/// One configured channel instance.
#[derive(Debug)]
pub struct ChannelConfig {
    /// Unique instance name. Becomes the first segment of every conversation id this channel
    /// produces, so it may not contain `:`.
    pub id: String,
    pub platform: PlatformConfig,
}

/// Per-platform settings. One variant per supported platform.
#[derive(Debug)]
pub enum PlatformConfig {
    Telegram(TelegramConfig),
}

/// Telegram bot settings.
#[derive(Debug)]
pub struct TelegramConfig {
    pub token: Secret,
    /// Telegram user ids permitted to reach the agent. Empty means nobody, which is a deliberate
    /// fail-closed default: a bot token is a public entry point.
    pub allowed_users: Vec<i64>,
    /// Additional chat ids (groups, channels) permitted regardless of sender.
    pub allowed_chats: Vec<i64>,
    /// Accept every sender, making both allowlists advisory rather than gates.
    ///
    /// Off unless the operator asks for it. What it enables is a bot anyone may message, which is
    /// the shape a customer-service or public bot needs and the wrong shape for everything else.
    pub allow_all: bool,
    /// Offer the agent the moderation tools.
    ///
    /// On by default, because Telegram already gates each one on the admin rights the bot actually
    /// holds in a chat, and a bot that is nobody's administrator can do nothing with them. Turn it
    /// off to keep them out of the tool list entirely: promoting a bot is also how an operator
    /// lets it read every message in a group, and somebody who did that for the reading alone
    /// should not silently acquire an agent that can ban people.
    pub admin_tools: bool,
    pub parse_mode: TelegramParseMode,
    /// Whether a link in an outgoing message gets a preview card.
    pub link_preview: bool,
    /// `getUpdates` long-poll timeout.
    pub poll_timeout: Duration,
}

/// How Telegram messages are formatted on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TelegramParseMode {
    /// Render agent markdown into the Telegram HTML subset.
    Html,
    /// Send the markdown verbatim as plain text. The escape hatch when rendering misbehaves.
    None,
}

/// meka permission level a session runs at.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Permission {
    None,
    Read,
    Ask,
    Write,
}

impl Permission {
    /// Wire form accepted by meka's session API.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Read => "read",
            Self::Ask => "ask",
            Self::Write => "write",
        }
    }
}

impl Config {
    /// Load and validate configuration.
    ///
    /// `path` overrides the default location; when it is `None` the platform config directory is
    /// used and a missing file is an error, because there is no useful zero-config behaviour (a bot
    /// token and a meka token are both mandatory).
    pub fn load(path: Option<&Path>) -> Result<Self> {
        let path = match path {
            Some(path) => path.to_path_buf(),
            None => default_config_path()?,
        };
        let raw = std::fs::read_to_string(&path).map_err(|source| BridgeError::ConfigRead {
            path: path.clone(),
            source,
        })?;
        Self::from_toml(&raw, &path)
    }

    /// Parse and validate configuration already read into memory. `path` is only used for error
    /// messages and for resolving `token_file` entries relative to the config directory.
    pub fn from_toml(raw: &str, path: &Path) -> Result<Self> {
        let file: FileConfig = toml::from_str(raw).map_err(|source| BridgeError::ConfigParse {
            path: path.to_path_buf(),
            source,
        })?;
        file.resolve(path)
    }
}

/// Default config file location, `<config dir>/mekabridge/config.toml`.
pub fn default_config_path() -> Result<PathBuf> {
    let base = dirs::config_dir().ok_or_else(|| {
        BridgeError::config("no platform config directory is available; pass --config")
    })?;
    Ok(base.join(APP_DIR).join("config.toml"))
}

/// Default data directory, `<data dir>/mekabridge`.
fn default_data_dir() -> Result<PathBuf> {
    let base = dirs::data_dir().ok_or_else(|| {
        BridgeError::config("no platform data directory is available; set [storage].path")
    })?;
    Ok(base.join(APP_DIR))
}

/// Expand a leading `~` and make relative paths absolute against the config file's directory.
///
/// Resolving relative to the config file rather than the process working directory means a config
/// keeps working when the daemon is started from somewhere else, which is the normal case under
/// systemd.
fn expand_path(raw: &Path, config_dir: &Path) -> Result<PathBuf> {
    let expanded = if let Ok(rest) = raw.strip_prefix("~") {
        let home = dirs::home_dir().ok_or_else(|| {
            BridgeError::config("`~` used in a path but no home directory is set")
        })?;
        home.join(rest)
    } else {
        raw.to_path_buf()
    };
    if expanded.is_absolute() {
        return Ok(expanded);
    }
    // `Path::join` on a bare "." yields "././x"; stripping it keeps printed paths readable.
    let joined = config_dir.join(expanded);
    Ok(joined
        .strip_prefix("./")
        .map_or(joined.clone(), Path::to_path_buf))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    meka: FileMeka,
    #[serde(default)]
    session: FileSession,
    #[serde(default)]
    bridge: FileBridge,
    #[serde(default)]
    mcp: FileMcp,
    #[serde(default)]
    storage: FileStorage,
    #[serde(default)]
    log: FileLog,
    #[serde(default)]
    channels: FileChannels,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileMeka {
    #[serde(default = "default_meka_base_url")]
    base_url: String,
    #[serde(default)]
    token: Option<String>,
    #[serde(default)]
    token_file: Option<PathBuf>,
    #[serde(default = "default_connect_timeout", with = "humantime_serde")]
    connect_timeout: Duration,
    #[serde(default = "default_turn_timeout", with = "humantime_serde")]
    turn_timeout: Duration,
    #[serde(default = "default_max_retries")]
    max_retries: u32,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileSession {
    #[serde(default)]
    cwd: Option<PathBuf>,
    #[serde(default = "default_permission")]
    permission: Permission,
    #[serde(default = "default_true")]
    recreate_on_missing: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileBridge {
    #[serde(default)]
    owner_conversation: Option<String>,
    #[serde(default = "default_max_queue_depth")]
    max_queue_depth: usize,
    #[serde(default = "default_batch_max_messages")]
    batch_max_messages: usize,
    #[serde(default = "default_settle", with = "humantime_serde")]
    settle: Duration,
    #[serde(default = "default_settle_max", with = "humantime_serde")]
    settle_max: Duration,
    #[serde(default = "default_turn_retries")]
    turn_retries: u32,
    #[serde(default = "default_true")]
    typing_indicator: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileMcp {
    #[serde(default = "default_mcp_transport")]
    transport: McpTransport,
    #[serde(default = "default_mcp_bind")]
    bind: SocketAddr,
    #[serde(default = "default_mcp_path")]
    path: String,
    #[serde(default)]
    token: Option<String>,
    #[serde(default)]
    token_file: Option<PathBuf>,
    #[serde(default)]
    allowed_hosts: Vec<String>,
    #[serde(default = "default_true")]
    health: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileStorage {
    #[serde(default)]
    path: Option<PathBuf>,
    #[serde(default)]
    attachment_dir: Option<PathBuf>,
    #[serde(default = "default_attachment_max_bytes")]
    attachment_max_bytes: u64,
    #[serde(default = "default_attachment_retention", with = "humantime_serde")]
    attachment_retention: Duration,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileLog {
    #[serde(default = "default_log_level")]
    level: String,
    #[serde(default = "default_log_format")]
    format: LogFormat,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileChannels {
    #[serde(default)]
    telegram: Vec<FileTelegram>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileTelegram {
    id: String,
    #[serde(default)]
    token: Option<String>,
    #[serde(default)]
    token_file: Option<PathBuf>,
    #[serde(default)]
    allowed_users: Vec<i64>,
    #[serde(default)]
    allowed_chats: Vec<i64>,
    #[serde(default)]
    allow_all: bool,
    #[serde(default = "default_true")]
    admin_tools: bool,
    #[serde(default = "default_telegram_parse_mode")]
    parse_mode: TelegramParseMode,
    #[serde(default = "default_link_preview")]
    link_preview: bool,
    #[serde(default = "default_poll_timeout", with = "humantime_serde")]
    poll_timeout: Duration,
}

impl FileConfig {
    fn resolve(self, config_path: &Path) -> Result<Config> {
        let config_dir = config_path.parent().unwrap_or(Path::new(".")).to_path_buf();
        let mut warnings = Vec::new();

        let base_url = Url::parse(&self.meka.base_url).map_err(|source| {
            BridgeError::config(format!(
                "[meka].base_url {:?} is not a valid URL: {source}",
                self.meka.base_url
            ))
        })?;
        let meka = MekaConfig {
            base_url,
            token: secret::resolve(
                "[meka]",
                self.meka.token.as_deref(),
                self.meka
                    .token_file
                    .as_deref()
                    .map(|path| expand_path(path, &config_dir))
                    .transpose()?
                    .as_deref(),
                &mut warnings,
            )?,
            connect_timeout: self.meka.connect_timeout,
            turn_timeout: self.meka.turn_timeout,
            max_retries: self.meka.max_retries,
        };

        if let Some(cwd) = &self.session.cwd
            && !cwd.is_absolute()
        {
            return Err(BridgeError::config(format!(
                "[session].cwd must be an absolute path, got {}",
                cwd.display()
            )));
        }
        let session = SessionConfig {
            cwd: self.session.cwd,
            permission: self.session.permission,
            recreate_on_missing: self.session.recreate_on_missing,
        };

        if self.bridge.batch_max_messages == 0 {
            return Err(BridgeError::config(
                "[bridge].batch_max_messages must be at least 1",
            ));
        }
        if self.bridge.settle_max < self.bridge.settle {
            return Err(BridgeError::config(format!(
                "[bridge].settle_max ({:?}) must be at least settle ({:?}), or the ceiling would \
                 fire before the quiet period ever could",
                self.bridge.settle_max, self.bridge.settle
            )));
        }
        if self.bridge.max_queue_depth < self.bridge.batch_max_messages {
            return Err(BridgeError::config(format!(
                "[bridge].max_queue_depth ({}) must be at least batch_max_messages ({})",
                self.bridge.max_queue_depth, self.bridge.batch_max_messages
            )));
        }

        let mcp_token = match (&self.mcp.token, &self.mcp.token_file) {
            (None, None) => None,
            _ => Some(secret::resolve(
                "[mcp]",
                self.mcp.token.as_deref(),
                self.mcp
                    .token_file
                    .as_deref()
                    .map(|path| expand_path(path, &config_dir))
                    .transpose()?
                    .as_deref(),
                &mut warnings,
            )?),
        };
        if !self.mcp.path.starts_with('/') {
            return Err(BridgeError::config(format!(
                "[mcp].path must start with '/', got {:?}",
                self.mcp.path
            )));
        }
        let mcp = McpConfig {
            transport: self.mcp.transport,
            bind: self.mcp.bind,
            path: self.mcp.path,
            token: mcp_token,
            allowed_hosts: self.mcp.allowed_hosts,
            health: self.mcp.health,
        };

        let data_dir = match (&self.storage.path, &self.storage.attachment_dir) {
            (Some(_), Some(_)) => None,
            _ => Some(default_data_dir()?),
        };
        let storage_path = match self.storage.path {
            Some(path) => expand_path(&path, &config_dir)?,
            None => data_dir
                .as_ref()
                .ok_or_else(|| BridgeError::config("no data directory"))?
                .join("mekabridge.db"),
        };
        let attachment_dir = match self.storage.attachment_dir {
            Some(path) => expand_path(&path, &config_dir)?,
            None => data_dir
                .as_ref()
                .ok_or_else(|| BridgeError::config("no data directory"))?
                .join("attachments"),
        };
        let storage = StorageConfig {
            path: storage_path,
            attachment_dir,
            attachment_max_bytes: self.storage.attachment_max_bytes,
            attachment_retention: self.storage.attachment_retention,
        };

        let mut channels = Vec::new();
        let mut seen_ids = HashSet::new();
        for telegram in self.channels.telegram {
            validate_channel_id(&telegram.id)?;
            if !seen_ids.insert(telegram.id.clone()) {
                return Err(BridgeError::config(format!(
                    "duplicate channel id {:?}; ids must be unique across all platforms",
                    telegram.id
                )));
            }
            let label = format!("[[channels.telegram]] id = {:?}", telegram.id);
            if telegram.allow_all {
                // Repeated at every startup rather than logged once at the point of change, because
                // the risk is ongoing and the config that set it may have been written long ago by
                // somebody else.
                warnings.push(format!(
                    "{label} sets `allow_all`, so anyone who finds the bot can reach the agent. On \
                     Telegram a private chat id is the user's own id, so this admits individuals \
                     as well as groups."
                ));
            } else if telegram.allowed_users.is_empty() && telegram.allowed_chats.is_empty() {
                return Err(BridgeError::config(format!(
                    "{label} has an empty allowlist; set `allowed_users` or `allowed_chats`, or \
                     `allow_all` if the bot really should accept messages from anyone who finds it"
                )));
            }
            let token = secret::resolve(
                &label,
                telegram.token.as_deref(),
                telegram
                    .token_file
                    .as_deref()
                    .map(|path| expand_path(path, &config_dir))
                    .transpose()?
                    .as_deref(),
                &mut warnings,
            )?;
            channels.push(ChannelConfig {
                id: telegram.id,
                platform: PlatformConfig::Telegram(TelegramConfig {
                    token,
                    allowed_users: telegram.allowed_users,
                    allowed_chats: telegram.allowed_chats,
                    allow_all: telegram.allow_all,
                    admin_tools: telegram.admin_tools,
                    parse_mode: telegram.parse_mode,
                    link_preview: telegram.link_preview,
                    poll_timeout: telegram.poll_timeout,
                }),
            });
        }
        if channels.is_empty() {
            return Err(BridgeError::config(
                "no channels are configured; add at least one [[channels.telegram]] entry",
            ));
        }

        if let Some(owner) = &self.bridge.owner_conversation {
            let channel_id = owner.split(':').next().unwrap_or_default();
            if !seen_ids.contains(channel_id) {
                return Err(BridgeError::config(format!(
                    "[bridge].owner_conversation {owner:?} names channel {channel_id:?}, which is \
                     not configured"
                )));
            }
        }

        Ok(Config {
            meka,
            session,
            bridge: BridgeConfig {
                owner_conversation: self.bridge.owner_conversation,
                max_queue_depth: self.bridge.max_queue_depth,
                batch_max_messages: self.bridge.batch_max_messages,
                settle: self.bridge.settle,
                settle_max: self.bridge.settle_max,
                turn_retries: self.bridge.turn_retries,
                typing_indicator: self.bridge.typing_indicator,
            },
            mcp,
            storage,
            log: LogConfig {
                level: self.log.level,
                format: self.log.format,
            },
            channels,
            warnings,
        })
    }
}

/// Channel ids end up as the first segment of `<channel>:<chat>` conversation ids, so they must not
/// contain the separator or whitespace.
fn validate_channel_id(id: &str) -> Result<()> {
    if id.is_empty() {
        return Err(BridgeError::config("channel id must not be empty"));
    }
    if !id
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || character == '-' || character == '_')
    {
        return Err(BridgeError::config(format!(
            "channel id {id:?} may only contain ASCII letters, digits, '-' and '_'"
        )));
    }
    Ok(())
}

// The `Default` impls below exist because `#[serde(default)]` on an omitted *table* calls
// `Default::default()`, while `#[serde(default = "...")]` on a field only applies when the table is
// present. A derived `Default` would silently disagree with the per-field defaults (giving
// `recreate_on_missing = false`, a zero attachment cap, and so on) whenever a section was left out
// entirely, so each is written by hand to match.
impl Default for FileSession {
    fn default() -> Self {
        Self {
            cwd: None,
            permission: default_permission(),
            recreate_on_missing: true,
        }
    }
}

impl Default for FileStorage {
    fn default() -> Self {
        Self {
            path: None,
            attachment_dir: None,
            attachment_max_bytes: default_attachment_max_bytes(),
            attachment_retention: default_attachment_retention(),
        }
    }
}

impl Default for FileBridge {
    fn default() -> Self {
        Self {
            owner_conversation: None,
            max_queue_depth: default_max_queue_depth(),
            batch_max_messages: default_batch_max_messages(),
            settle: default_settle(),
            settle_max: default_settle_max(),
            turn_retries: default_turn_retries(),
            typing_indicator: true,
        }
    }
}

impl Default for FileMcp {
    fn default() -> Self {
        Self {
            transport: default_mcp_transport(),
            bind: default_mcp_bind(),
            path: default_mcp_path(),
            token: None,
            token_file: None,
            allowed_hosts: Vec::new(),
            health: true,
        }
    }
}

impl Default for FileLog {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            format: default_log_format(),
        }
    }
}

impl Default for Permission {
    fn default() -> Self {
        default_permission()
    }
}

fn default_meka_base_url() -> String {
    "http://127.0.0.1:8080".to_string()
}

const fn default_connect_timeout() -> Duration {
    Duration::from_secs(10)
}

const fn default_turn_timeout() -> Duration {
    Duration::from_secs(30 * 60)
}

const fn default_max_retries() -> u32 {
    3
}

/// Default to the lowest level that still works.
///
/// `read` lets the agent reply, because the bridge's send tools are annotated read-only; `write`
/// additionally lets it modify files. Anyone on the allowlist can drive this agent, so the default
/// is the one that answers messages without also handing out write access to the session's `cwd`.
const fn default_permission() -> Permission {
    Permission::Read
}

const fn default_true() -> bool {
    true
}

const fn default_max_queue_depth() -> usize {
    256
}

/// Long enough to catch the fragments of one thought typed in quick succession, short enough that a
/// reply still feels prompt.
const fn default_settle() -> Duration {
    Duration::from_secs(2)
}

/// Deliberately modest. A busy chat releases on this ceiling every time rather than on the quiet
/// period, so it is felt as constant added latency rather than an occasional delay.
const fn default_settle_max() -> Duration {
    Duration::from_secs(6)
}

const fn default_batch_max_messages() -> usize {
    32
}

const fn default_turn_retries() -> u32 {
    1
}

const fn default_mcp_transport() -> McpTransport {
    McpTransport::Http
}

fn default_mcp_bind() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 9100))
}

fn default_mcp_path() -> String {
    "/mcp".to_string()
}

const fn default_attachment_max_bytes() -> u64 {
    20 * 1024 * 1024
}

const fn default_attachment_retention() -> Duration {
    Duration::from_secs(30 * 24 * 60 * 60)
}

fn default_log_level() -> String {
    "info".to_string()
}

const fn default_log_format() -> LogFormat {
    LogFormat::Text
}

/// Off by default. The agent cites links as references far more often than it makes one the subject
/// of a message, and a card on each part of a split answer is noise.
const fn default_link_preview() -> bool {
    false
}

const fn default_telegram_parse_mode() -> TelegramParseMode {
    TelegramParseMode::Html
}

const fn default_poll_timeout() -> Duration {
    Duration::from_secs(30)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(raw: &str) -> Result<Config> {
        Config::from_toml(raw, Path::new("/etc/mekabridge/config.toml"))
    }

    const MINIMAL: &str = r#"
[meka]
token = "meka-token"

[[channels.telegram]]
id = "telegram"
token = "bot-token"
allowed_users = [123]
"#;

    #[test]
    fn minimal_config_fills_in_defaults() {
        let config = parse(MINIMAL).expect("minimal config is valid");
        assert_eq!(config.meka.base_url.as_str(), "http://127.0.0.1:8080/");
        assert_eq!(config.meka.token.expose(), "meka-token");
        assert_eq!(
            config.session.permission,
            Permission::Read,
            "the default must match the shipped template, and be the lowest level that can reply"
        );
        assert_eq!(config.bridge.batch_max_messages, 32);
        assert_eq!(config.mcp.transport, McpTransport::Http);
        assert_eq!(config.mcp.path, "/mcp");
        assert!(config.mcp.token.is_none());
        assert_eq!(config.channels.len(), 1);
        assert_eq!(config.channels[0].id, "telegram");

        let PlatformConfig::Telegram(telegram) = &config.channels[0].platform;
        assert_eq!(telegram.parse_mode, TelegramParseMode::Html);
        assert!(
            !telegram.link_preview,
            "link previews default off; the template and the docs both say so"
        );
    }

    #[test]
    fn settle_defaults_are_modest() {
        let config = parse(MINIMAL).expect("valid");
        assert_eq!(config.bridge.settle, Duration::from_secs(2));
        assert_eq!(
            config.bridge.settle_max,
            Duration::from_secs(6),
            "a busy chat releases on this every time, so it is felt as constant latency"
        );
    }

    #[test]
    fn debouncing_can_be_turned_off_entirely() {
        // The escape hatch for anyone who would rather have the latency back: zero means hand every
        // message over the moment it lands, as the bridge did before settling existed.
        let raw = format!("{MINIMAL}\n[bridge]\nsettle = \"0s\"\n");
        let config = parse(&raw).expect("zero is a valid setting, not a rejected one");
        assert!(config.bridge.settle.is_zero());
    }

    #[test]
    fn a_ceiling_below_the_quiet_period_is_rejected() {
        // It would fire before the quiet period could ever elapse, making `settle` dead config.
        let raw = format!("{MINIMAL}\n[bridge]\nsettle = \"5s\"\nsettle_max = \"1s\"\n");
        let error = parse(&raw).expect_err("must be rejected");
        assert!(error.to_string().contains("settle_max"), "got: {error}");
    }

    #[test]
    fn link_previews_can_be_turned_back_on() {
        let raw = format!("{MINIMAL}link_preview = true\n");
        let config = parse(&raw).expect("valid");
        let PlatformConfig::Telegram(telegram) = &config.channels[0].platform;
        assert!(telegram.link_preview);
    }

    #[test]
    fn unknown_fields_are_rejected() {
        let raw = format!("{MINIMAL}\n[session]\npermision = \"read\"\n");
        let error = parse(&raw).expect_err("typo must be rejected");
        assert!(matches!(error, BridgeError::ConfigParse { .. }));
    }

    #[test]
    fn empty_allowlist_is_rejected() {
        let raw = r#"
[meka]
token = "meka-token"

[[channels.telegram]]
id = "telegram"
token = "bot-token"
"#;
        let error = parse(raw).expect_err("an open bot must be rejected");
        assert!(error.to_string().contains("empty allowlist"));
    }

    #[test]
    fn allow_all_replaces_the_allowlist_and_warns() {
        let raw = r#"
[meka]
token = "meka-token"

[[channels.telegram]]
id = "telegram"
token = "bot-token"
allow_all = true
"#;
        let config = parse(raw).expect("an explicitly open bot is allowed");
        let PlatformConfig::Telegram(telegram) = &config.channels[0].platform;
        assert!(telegram.allow_all);
        assert!(
            config
                .warnings
                .iter()
                .any(|warning| warning.contains("allow_all")),
            "opening a bot to everyone must be said out loud on every startup: {:?}",
            config.warnings
        );
    }

    #[test]
    fn the_empty_allowlist_error_points_at_the_way_out() {
        let raw = r#"
[meka]
token = "meka-token"

[[channels.telegram]]
id = "telegram"
token = "bot-token"
"#;
        let error = parse(raw).expect_err("an accidentally open bot must be rejected");
        assert!(error.to_string().contains("allow_all"), "got: {error}");
    }

    #[test]
    fn missing_channels_are_rejected() {
        let raw = "[meka]\ntoken = \"meka-token\"\n";
        let error = parse(raw).expect_err("a bridge with no channels is useless");
        assert!(error.to_string().contains("no channels"));
    }

    #[test]
    fn duplicate_channel_ids_are_rejected() {
        let raw = r#"
[meka]
token = "meka-token"

[[channels.telegram]]
id = "same"
token = "a"
allowed_users = [1]

[[channels.telegram]]
id = "same"
token = "b"
allowed_users = [2]
"#;
        let error = parse(raw).expect_err("duplicate ids must be rejected");
        assert!(error.to_string().contains("duplicate channel id"));
    }

    #[test]
    fn channel_id_with_separator_is_rejected() {
        let raw = r#"
[meka]
token = "meka-token"

[[channels.telegram]]
id = "tele:gram"
token = "a"
allowed_users = [1]
"#;
        let error = parse(raw).expect_err("':' would corrupt conversation ids");
        assert!(error.to_string().contains("may only contain"));
    }

    #[test]
    fn owner_conversation_must_name_a_configured_channel() {
        let raw = format!("{MINIMAL}\n[bridge]\nowner_conversation = \"discord:1\"\n");
        let error = parse(&raw).expect_err("unknown channel must be rejected");
        assert!(error.to_string().contains("not configured"));
    }

    #[test]
    fn owner_conversation_accepts_a_configured_channel() {
        let raw = format!("{MINIMAL}\n[bridge]\nowner_conversation = \"telegram:123\"\n");
        let config = parse(&raw).expect("configured channel is accepted");
        assert_eq!(
            config.bridge.owner_conversation.as_deref(),
            Some("telegram:123")
        );
    }

    #[test]
    fn queue_depth_below_batch_size_is_rejected() {
        let raw = format!("{MINIMAL}\n[bridge]\nmax_queue_depth = 4\nbatch_max_messages = 8\n");
        let error = parse(&raw).expect_err("a queue smaller than a batch is a misconfiguration");
        assert!(error.to_string().contains("at least batch_max_messages"));
    }

    #[test]
    fn relative_cwd_is_rejected() {
        let raw = format!("{MINIMAL}\n[session]\ncwd = \"relative/path\"\n");
        let error = parse(&raw).expect_err("meka requires an absolute cwd");
        assert!(error.to_string().contains("absolute path"));
    }

    #[test]
    fn mcp_path_must_be_rooted() {
        let raw = format!("{MINIMAL}\n[mcp]\npath = \"mcp\"\n");
        let error = parse(&raw).expect_err("a path without a leading slash must be rejected");
        assert!(error.to_string().contains("must start with '/'"));
    }

    #[test]
    fn durations_accept_humantime_strings() {
        let raw = r#"
[meka]
token = "meka-token"
turn_timeout = "90s"
connect_timeout = "2s"

[[channels.telegram]]
id = "telegram"
token = "bot-token"
allowed_users = [123]
"#;
        let config = parse(raw).expect("humantime durations parse");
        assert_eq!(config.meka.turn_timeout, Duration::from_secs(90));
        assert_eq!(config.meka.connect_timeout, Duration::from_secs(2));
    }

    #[test]
    fn omitted_tables_use_the_same_defaults_as_omitted_fields() {
        // Regression guard: a derived `Default` on the file sections would give
        // `recreate_on_missing = false` and a zero attachment cap here.
        let without_tables = parse(MINIMAL).expect("valid");
        let with_empty_tables = parse(&format!(
            "{MINIMAL}\n[session]\n[storage]\n[bridge]\n[mcp]\n"
        ))
        .expect("valid");
        assert_eq!(
            without_tables.session.recreate_on_missing,
            with_empty_tables.session.recreate_on_missing
        );
        assert!(without_tables.session.recreate_on_missing);
        assert_eq!(
            without_tables.storage.attachment_max_bytes,
            with_empty_tables.storage.attachment_max_bytes
        );
        assert_eq!(
            without_tables.storage.attachment_max_bytes,
            20 * 1024 * 1024
        );
        assert_eq!(
            without_tables.storage.attachment_retention,
            with_empty_tables.storage.attachment_retention
        );
    }

    #[test]
    fn storage_paths_default_under_the_data_directory() {
        let config = parse(MINIMAL).expect("valid");
        assert!(config.storage.path.ends_with("mekabridge/mekabridge.db"));
        assert!(
            config
                .storage
                .attachment_dir
                .ends_with("mekabridge/attachments")
        );
    }

    #[test]
    fn relative_storage_paths_resolve_against_the_config_directory() {
        let raw = format!("{MINIMAL}\n[storage]\npath = \"state.db\"\n");
        let config = parse(&raw).expect("valid");
        assert_eq!(
            config.storage.path,
            Path::new("/etc/mekabridge/state.db"),
            "relative paths must anchor to the config file, not the process cwd"
        );
    }
}
