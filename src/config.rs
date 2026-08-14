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
    channel::ChatKind,
    config::secret::Secret,
    error::{BridgeError, Result},
    store::Policy,
};

/// Directory name used under the platform config and data directories.
const APP_DIR: &str = "mekabridge";

/// Shortest a conversation is ever held before its messages are claimed.
///
/// A second is generous for the thing it exists for: the parts of a split post land milliseconds
/// apart, so this is sized against network delay and reconnects rather than against how fast
/// anybody types.
const DEFAULT_COALESCE_FLOOR: Duration = Duration::from_secs(1);

/// First wait between attempts at a batch whose turn failed, doubled on each further attempt.
///
/// A failed turn used to be reoffered on the very next pass of the drain loop, which for the
/// failure this exists for is the wrong move twice over: the upstream said it was out of quota or
/// overloaded, and coming straight back spends the next attempt inside the same window. Ten seconds
/// is long enough to be past a burst and short enough that somebody waiting on a reply is not left
/// wondering.
const DEFAULT_RETRY_BASE: Duration = Duration::from_secs(10);

/// Ceiling on `[bridge].mute_context`. The lookback is charged to every turn a muted conversation
/// wakes, so a generous setting quietly turns "only wake me for mentions" back into "send me the
/// whole chat".
const MAX_MUTE_CONTEXT: usize = 50;

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
    /// Quiet period a conversation goes through before its messages are handed to the agent, on
    /// platforms that report when somebody is typing.
    ///
    /// Ignored everywhere else, and that is the point. Without the signal any wait is a guess, and
    /// a guess long enough to catch somebody typing a second sentence is far too long to impose on
    /// somebody who only ever meant to send one. Where the signal exists the conversation is held
    /// while they are still going and for this long after they stop.
    pub settle: Duration,
    /// Shortest a conversation is held before its messages are claimed.
    ///
    /// Deliberately absent from the file format. It exists for the wire rather than for people:
    /// platforms split one thing into several messages, Telegram's multi-photo albums above all,
    /// and without a floor a post arrives as one photo followed by a separate turn carrying the
    /// rest. An operator tuning it would be tuning Telegram's wire format rather than a
    /// preference, so it lives here, where the test harness can shrink it and a config file
    /// cannot reach it.
    pub coalesce_floor: Duration,
    /// Ceiling on how long [`BridgeConfig::settle`] may defer a message.
    ///
    /// What stops a conversation being held indefinitely by somebody who never finishes: the
    /// typing signal is a heartbeat, so a client that keeps sending it, or a compose box left
    /// open, would otherwise hold a chat open for as long as it liked.
    pub settle_max: Duration,
    /// Extra attempts for a batch whose turn failed. `0` means a failed batch is never retried.
    ///
    /// Spaced out rather than made back to back: the failure this budget exists for is an upstream
    /// out of quota, and a second attempt in the same second lands in the same window as the
    /// first.
    pub turn_retries: u32,
    /// How long the first of those attempts waits, doubling thereafter.
    ///
    /// Absent from the file format for the same reason as [`BridgeConfig::coalesce_floor`]: it
    /// describes how an upstream behaves under load rather than anything an operator has a
    /// preference about. Together with [`BridgeConfig::turn_retries`] it decides how long somebody
    /// waits before being told there will be no answer, and that is the knob worth having.
    pub retry_base: Duration,
    /// Whether a chat is told when a message from it could not be delivered to the agent.
    ///
    /// The one exception to the bridge writing no chat content of its own, so it is defeatable.
    /// The notice says nothing but that something went wrong: whoever is in the chat did
    /// nothing wrong, cannot act on an upstream status code, and is not necessarily somebody
    /// an operator would hand one to. Detail goes to [`BridgeConfig::owner_conversation`]
    /// instead.
    pub notify_failures: bool,
    pub typing_indicator: bool,
    /// Ceiling on how long the typing indicator is held for one turn.
    ///
    /// A safety net rather than a schedule: the indicator already stops when the agent replies and
    /// when the turn ends, so this only fires if a turn outlives both. Defaults to
    /// `[meka].turn_timeout`, which is the longest a turn can run, so in practice it never fires.
    ///
    /// Worth raising rather than lowering. A cap shorter than a turn is the worst of both: the
    /// indicator stops while the agent is still working, and a chat that has gone quiet for
    /// minutes reads as a bot that has died rather than one that is busy.
    pub typing_max: Duration,
    /// What happens to a conversation nobody has ruled on, decided by its chat kind.
    pub default_policy: DefaultPolicy,
    /// Messages of missed context rendered into the envelope when a muted conversation wakes.
    ///
    /// A mention in a busy chat is usually meaningless on its own, and a tool round trip to
    /// recover what it referred to costs a whole model call. `0` withholds them and leaves the
    /// agent to ask.
    pub mute_context: usize,
}

/// Policy for a conversation with no explicit decision recorded, by chat kind.
///
/// Split by kind because the honest answer differs: in a one-to-one chat every message is addressed
/// to the agent, so mention-only would silence it entirely, while in a group of five thousand
/// almost nothing is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DefaultPolicy {
    pub direct: Policy,
    pub group: Policy,
    pub channel: Policy,
}

impl DefaultPolicy {
    /// The policy a conversation of this shape gets when nobody has ruled on it.
    pub const fn for_kind(self, kind: ChatKind) -> Policy {
        match kind {
            ChatKind::Direct => self.direct,
            ChatKind::Group => self.group,
            ChatKind::Channel => self.channel,
            // A conversation the agent messaged first, which nothing has ever arrived in. Its shape
            // is unknown, so it is heard in full until something says otherwise.
            ChatKind::Unknown => Policy::Active,
        }
    }
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
    /// How long a recorded message stays readable through the history tools.
    ///
    /// Zero records nothing at all, which is the switch for a deployment that does not want a chat
    /// log on disk. Delivery is unaffected either way: this governs what the agent can go back
    /// for, not what reaches it.
    pub history_retention: Duration,
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
    Discord(DiscordConfig),
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

/// Discord bot settings.
///
/// Ids are strings rather than the `i64` Telegram uses. Snowflakes are strings everywhere in
/// Discord's own API, a string is what you get when you copy one out of the client, and they are
/// validated as `u64` at load time so a typo is a startup error rather than a chat that silently
/// never matches.
#[derive(Debug)]
pub struct DiscordConfig {
    pub token: Secret,
    /// Discord user ids permitted to reach the agent, anywhere, including in a direct message.
    ///
    /// This gates DMs, which is not optional: anyone sharing a server with the bot may open one.
    /// In a single busy server that is thousands of people, each of whom would otherwise get an
    /// unconditional agent turn for the price of a "hi".
    pub allowed_users: Vec<u64>,
    /// Servers whose members are all permitted. The largest of the four grants by far.
    pub allowed_guilds: Vec<u64>,
    /// Individual channels, including threads, whose participants are permitted.
    pub allowed_channels: Vec<u64>,
    /// Roles whose holders are permitted. The idiomatic Discord way to scope access, and cheap:
    /// the roles ride along on every guild message.
    pub allowed_roles: Vec<u64>,
    /// Accept every sender, making all four allowlists advisory rather than gates.
    pub allow_all: bool,
    /// Offer the agent the moderation tools. See [`TelegramConfig::admin_tools`].
    pub admin_tools: bool,
    /// Request the privileged `MESSAGE_CONTENT` intent.
    ///
    /// Without it Discord blanks the text of every guild message except those mentioning the bot,
    /// so the agent can still be woken by a mention but has no record of what led up to it. It
    /// must be enabled in the Developer Portal first: asking for it without that closes the
    /// gateway with a 4014 rather than degrading.
    pub message_content: bool,
    /// Track who is online, which needs the server presence intent.
    ///
    /// Unlike the member roster, this cannot be reached over HTTP at all, so it goes into the
    /// gateway handshake and an ungranted intent closes the connection with a 4014 at startup
    /// rather than failing one call. Off by default for that reason, and because it means
    /// ingesting the availability of everyone in every server the bot is in.
    pub presence: bool,
    /// Allow an outgoing message to ping `@everyone` and `@here`.
    pub mention_everyone: bool,
    /// Allow an outgoing message to ping a role.
    pub mention_roles: bool,
    /// Whether a link in an outgoing message gets a preview card.
    pub link_preview: bool,
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
    notify_failures: bool,
    #[serde(default = "default_true")]
    typing_indicator: bool,
    /// Unset follows `[meka].turn_timeout`, which is resolved once both are known.
    #[serde(default, with = "humantime_serde")]
    typing_max: Option<Duration>,
    #[serde(default)]
    default_policy: FileDefaultPolicy,
    #[serde(default = "default_mute_context")]
    mute_context: usize,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileDefaultPolicy {
    #[serde(default = "default_direct_policy")]
    direct: Policy,
    #[serde(default = "default_group_policy")]
    group: Policy,
    #[serde(default = "default_group_policy")]
    channel: Policy,
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
    #[serde(default = "default_history_retention", with = "humantime_serde")]
    history_retention: Duration,
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
    #[serde(default)]
    discord: Vec<FileDiscord>,
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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileDiscord {
    id: String,
    #[serde(default)]
    token: Option<String>,
    #[serde(default)]
    token_file: Option<PathBuf>,
    #[serde(default)]
    allowed_users: Vec<String>,
    #[serde(default)]
    allowed_guilds: Vec<String>,
    #[serde(default)]
    allowed_channels: Vec<String>,
    #[serde(default)]
    allowed_roles: Vec<String>,
    #[serde(default)]
    allow_all: bool,
    #[serde(default = "default_true")]
    admin_tools: bool,
    #[serde(default = "default_true")]
    message_content: bool,
    #[serde(default)]
    presence: bool,
    #[serde(default)]
    mention_everyone: bool,
    #[serde(default)]
    mention_roles: bool,
    #[serde(default = "default_link_preview")]
    link_preview: bool,
}

/// Parse a list of Discord snowflakes, naming the field and the offending value on failure.
///
/// Zero is rejected along with anything unparseable. No snowflake is ever zero, and the id type the
/// connector builds these into panics on one, so letting it through here would turn a config typo
/// into a crash at startup rather than the error it is.
fn parse_snowflakes(label: &str, field: &str, raw: &[String]) -> Result<Vec<u64>> {
    raw.iter()
        .map(|value| {
            value
                .parse::<u64>()
                .ok()
                .filter(|parsed| *parsed != 0)
                .ok_or_else(|| {
                    BridgeError::config(format!(
                        "{label}: {field} contains {value:?}, which is not a Discord id; ids are \
                         positive decimal numbers, copied with Developer Mode on"
                    ))
                })
        })
        .collect()
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
        if self.meka.turn_timeout.is_zero() {
            // Every turn would time out on the spot, and worse, the budget doubles as the ceiling
            // on how long a batch may wait out a turn meka is running for itself. At
            // zero that wait ends before it starts, and the batch is requeued and
            // resubmitted as fast as the two processes can trade requests.
            return Err(BridgeError::config(
                "[meka].turn_timeout must be greater than zero",
            ));
        }
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
        if self.bridge.mute_context > MAX_MUTE_CONTEXT {
            return Err(BridgeError::config(format!(
                "[bridge].mute_context is {} but may be at most {MAX_MUTE_CONTEXT}; a larger \
                 lookback would put more of a chat into every turn than the mention that woke it",
                self.bridge.mute_context
            )));
        }
        // Not rejected, because "only the conversations I name" is a coherent posture, but said out
        // loud: a bridge that blocks by default answers nobody, and that is indistinguishable from
        // one that is broken.
        for (kind, policy) in [
            ("direct", self.bridge.default_policy.direct),
            ("group", self.bridge.default_policy.group),
            ("channel", self.bridge.default_policy.channel),
        ] {
            if policy == Policy::Block {
                warnings.push(format!(
                    "[bridge].default_policy.{kind} is `block`, so nothing from a {kind} \
                     conversation reaches the agent unless that conversation has been given a \
                     policy of its own"
                ));
            }
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
            history_retention: self.storage.history_retention,
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
            // Says out loud what changed under them. `allowed_users` used to admit a person
            // wherever they wrote, so a config naming only people worked in groups too; now it
            // reaches direct messages alone, and a bot that had been answering in a group would go
            // quiet there with nothing to explain it.
            if !telegram.allowed_users.is_empty()
                && telegram.allowed_chats.is_empty()
                && !telegram.allow_all
            {
                warnings.push(format!(
                    "{label} allowlists people but no chats, so the agent is reachable by direct \
                     message only. Add the group's id to `allowed_chats` to be heard in it; \
                     `allowed_users` no longer admits anybody outside their own chat."
                ));
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
        for discord in self.channels.discord {
            validate_channel_id(&discord.id)?;
            if !seen_ids.insert(discord.id.clone()) {
                return Err(BridgeError::config(format!(
                    "duplicate channel id {:?}; ids must be unique across all platforms",
                    discord.id
                )));
            }
            let label = format!("[[channels.discord]] id = {:?}", discord.id);
            let allowed_users = parse_snowflakes(&label, "allowed_users", &discord.allowed_users)?;
            let allowed_guilds =
                parse_snowflakes(&label, "allowed_guilds", &discord.allowed_guilds)?;
            let allowed_channels =
                parse_snowflakes(&label, "allowed_channels", &discord.allowed_channels)?;
            let allowed_roles = parse_snowflakes(&label, "allowed_roles", &discord.allowed_roles)?;
            if discord.allow_all {
                warnings.push(format!(
                    "{label} sets `allow_all`, so anyone who finds the bot can reach the agent, \
                     including by direct message."
                ));
            } else if allowed_users.is_empty()
                && allowed_guilds.is_empty()
                && allowed_channels.is_empty()
                && allowed_roles.is_empty()
            {
                return Err(BridgeError::config(format!(
                    "{label} has an empty allowlist; set `allowed_users`, `allowed_guilds`, \
                     `allowed_channels`, or `allowed_roles`, or `allow_all` if the bot really \
                     should accept messages from anyone who finds it"
                )));
            }
            if !allowed_users.is_empty()
                && allowed_guilds.is_empty()
                && allowed_channels.is_empty()
                && allowed_roles.is_empty()
                && !discord.allow_all
            {
                warnings.push(format!(
                    "{label} allowlists people but no channels, servers, or roles, so the agent is \
                     reachable by direct message only. Add ids to `allowed_channels`, \
                     `allowed_roles`, or `allowed_guilds` to be heard in a server; \
                     `allowed_users` no longer admits anybody outside a direct message."
                ));
            }
            if !allowed_guilds.is_empty() {
                // A Telegram chat allowlist admits a room. This admits everybody in a server, which
                // in a large one is thousands of people who can each wake the agent by name.
                warnings.push(format!(
                    "{label} sets `allowed_guilds`, which admits every member of {} server(s), not \
                     just the people already in a shared channel.",
                    allowed_guilds.len()
                ));
            }
            if discord.presence {
                warnings.push(format!(
                    "{label} sets `presence = true`, so this bot asks Discord for the Presence \
                     Intent. Enable it under Privileged Gateway Intents in the Developer Portal \
                     first, or the gateway closes with a 4014 at startup. The bridge will track the \
                     availability of everyone in every server it is in; it keeps only online or \
                     idle or busy, never what they are doing, and writes none of it to disk."
                ));
            }
            if !discord.message_content {
                warnings.push(format!(
                    "{label} sets `message_content = false`, so Discord blanks the text of every \
                     server message except those mentioning the bot. The agent can still be woken \
                     by name but will have no record of what led up to it."
                ));
            }
            let token = secret::resolve(
                &label,
                discord.token.as_deref(),
                discord
                    .token_file
                    .as_deref()
                    .map(|path| expand_path(path, &config_dir))
                    .transpose()?
                    .as_deref(),
                &mut warnings,
            )?;
            channels.push(ChannelConfig {
                id: discord.id,
                platform: PlatformConfig::Discord(DiscordConfig {
                    token,
                    allowed_users,
                    allowed_guilds,
                    allowed_channels,
                    allowed_roles,
                    allow_all: discord.allow_all,
                    admin_tools: discord.admin_tools,
                    message_content: discord.message_content,
                    presence: discord.presence,
                    mention_everyone: discord.mention_everyone,
                    mention_roles: discord.mention_roles,
                    link_preview: discord.link_preview,
                }),
            });
        }
        if channels.is_empty() {
            return Err(BridgeError::config(
                "no channels are configured; add at least one [[channels.telegram]] or \
                 [[channels.discord]] entry",
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
            // Not rejected outright, only because the id could name a conversation this build's
            // parser is stricter about than the platform is. It is still worth saying: every
            // operator notice the bridge tries to send goes nowhere.
            if crate::channel::ConversationId::parse(owner).is_none() {
                warnings.push(format!(
                    "[bridge].owner_conversation {owner:?} is not a conversation id, so operator \
                     notices cannot be delivered; it wants the <channel>:<chat> form"
                ));
            }
        } else if !self.bridge.notify_failures {
            warnings.push(
                "[bridge].notify_failures is off and no [bridge].owner_conversation is set, so a \
                 message the agent never receives is reported only in the logs"
                    .to_string(),
            );
        }

        // The recovery path for an undeliverable message is to put it back among what the agent has
        // not seen, and that needs a history row to exist. With retention off there is none, so a
        // message that runs out of attempts is gone for good rather than merely late.
        if storage.history_retention.is_zero() {
            warnings.push(
                "[storage].history_retention is zero, so a message the agent never receives cannot \
                 be recovered afterwards; only the platform's own scrollback will still have it"
                    .to_string(),
            );
        }

        Ok(Config {
            meka,
            session,
            bridge: BridgeConfig {
                owner_conversation: self.bridge.owner_conversation,
                max_queue_depth: self.bridge.max_queue_depth,
                batch_max_messages: self.bridge.batch_max_messages,
                coalesce_floor: DEFAULT_COALESCE_FLOOR,
                retry_base: DEFAULT_RETRY_BASE,
                settle: self.bridge.settle,
                settle_max: self.bridge.settle_max,
                turn_retries: self.bridge.turn_retries,
                notify_failures: self.bridge.notify_failures,
                typing_indicator: self.bridge.typing_indicator,
                // Follows the turn budget unless pinned, so the indicator lasts exactly as long as
                // the work does rather than lapsing partway through a long turn.
                typing_max: self.bridge.typing_max.unwrap_or(self.meka.turn_timeout),
                default_policy: DefaultPolicy {
                    direct: self.bridge.default_policy.direct,
                    group: self.bridge.default_policy.group,
                    channel: self.bridge.default_policy.channel,
                },
                mute_context: self.bridge.mute_context,
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
            history_retention: default_history_retention(),
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
            notify_failures: true,
            typing_indicator: true,
            typing_max: None,
            default_policy: FileDefaultPolicy::default(),
            mute_context: default_mute_context(),
        }
    }
}

impl Default for FileDefaultPolicy {
    fn default() -> Self {
        Self {
            direct: default_direct_policy(),
            group: default_group_policy(),
            channel: default_group_policy(),
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

/// Long enough for the gap between sentences, which is what it is now for. It only applies where
/// the platform reports typing, so the wait ends when the person stops rather than when this
/// expires, and erring long costs nothing on the common path.
const fn default_settle() -> Duration {
    Duration::from_secs(3)
}

/// Long enough for a considered paragraph, short enough that a compose box somebody opened and
/// walked away from does not strand a message for a minute. Only reached where the platform reports
/// typing, since nowhere else is a conversation held long enough to hit it.
const fn default_settle_max() -> Duration {
    Duration::from_secs(30)
}

const fn default_batch_max_messages() -> usize {
    32
}

/// Four attempts in all, spaced 10s, 20s and 40s apart, so a batch survives a little over a minute
/// of an upstream being unavailable.
///
/// One retry was defensible only while the retries were free and instant. Now that each costs a
/// real wait, this is the number that decides how long somebody is left with no answer before they
/// are told there will not be one, and a minute is about as long as silence reads as thinking.
const fn default_turn_retries() -> u32 {
    3
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

/// Matches [`default_attachment_retention`], so a message and the picture attached to it fall out
/// of reach together rather than leaving the agent a description of a file it can no longer open.
const fn default_history_retention() -> Duration {
    Duration::from_secs(30 * 24 * 60 * 60)
}

/// In a one-to-one chat every message is addressed to the agent, so anything but `active` would
/// silence it against the only person talking to it.
const fn default_direct_policy() -> Policy {
    Policy::Active
}

/// Groups and channels default to mention-only, which is how a person configures a busy room on
/// their own phone. The agent still receives and records everything said there; it is woken for
/// what is addressed to it, and can read the rest when it needs to.
const fn default_group_policy() -> Policy {
    Policy::Mute
}

/// Enough to make sense of "what do you think about that?" without paying a tool round trip for the
/// antecedent, which costs a whole model call.
const fn default_mute_context() -> usize {
    5
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

    const MINIMAL_DISCORD: &str = r#"
[meka]
token = "meka-token"

[[channels.discord]]
id = "discord"
token = "bot-token"
allowed_users = ["245119312739729408"]
"#;

    #[test]
    fn a_discord_channel_parses_its_snowflakes() {
        let config = parse(MINIMAL_DISCORD).expect("valid");
        let PlatformConfig::Discord(discord) = &config.channels[0].platform else {
            panic!("the config under test declares one Discord channel");
        };
        assert_eq!(discord.allowed_users, vec![245_119_312_739_729_408]);
        assert!(
            discord.message_content,
            "the intent is requested by default"
        );
        assert!(!discord.mention_everyone);
    }

    #[test]
    fn a_snowflake_that_is_not_a_number_is_rejected_at_load() {
        let raw = MINIMAL_DISCORD.replace("245119312739729408", "not-an-id");
        let error = parse(&raw).expect_err("a typo is a config error");
        assert!(error.to_string().contains("allowed_users"), "got: {error}");
    }

    #[test]
    fn a_zero_snowflake_is_rejected_rather_than_panicking_later() {
        // No Discord id is ever zero, and the id type the connector builds these into panics on
        // one, so letting it through here would turn a typo into a crash at startup.
        let raw = MINIMAL_DISCORD.replace("245119312739729408", "0");
        let error = parse(&raw).expect_err("zero is not an id");
        assert!(error.to_string().contains("allowed_users"), "got: {error}");
    }

    #[test]
    fn a_discord_channel_with_no_allowlist_at_all_is_refused() {
        let raw = MINIMAL_DISCORD.replace(r#"allowed_users = ["245119312739729408"]"#, "");
        let error = parse(&raw).expect_err("an open bot must be a decision");
        assert!(error.to_string().contains("allowlist"), "got: {error}");
    }

    #[test]
    fn allowlisting_a_whole_server_warns_every_startup() {
        let raw = MINIMAL_DISCORD.replace(
            r#"allowed_users = ["245119312739729408"]"#,
            r#"allowed_guilds = ["987654321098765432"]"#,
        );
        let config = parse(&raw).expect("valid");
        assert!(
            config
                .warnings
                .iter()
                .any(|warning| warning.contains("allowed_guilds")),
            "admitting a whole server is the largest grant there is: {:?}",
            config.warnings
        );
    }

    #[test]
    fn two_platforms_may_not_share_a_channel_id() {
        let raw = format!(
            "{MINIMAL}
{}",
            MINIMAL_DISCORD
                .replace(
                    "
[meka]
token = \"meka-token\"
",
                    ""
                )
                .replace("id = \"discord\"", "id = \"telegram\"")
        );
        let error = parse(&raw).expect_err("ids are unique across platforms");
        assert!(error.to_string().contains("duplicate"), "got: {error}");
    }

    #[test]
    fn the_typing_ceiling_follows_the_turn_budget_by_default() {
        // A ceiling shorter than a turn is the failure this defaults away from: the indicator stops
        // while the agent is still working, and the chat reads as a dead bot rather than a busy
        // one.
        let config = parse(MINIMAL).expect("valid");
        assert_eq!(
            config.bridge.typing_max, config.meka.turn_timeout,
            "unpinned, the indicator lasts exactly as long as a turn can"
        );

        let raw = MINIMAL.replace(
            "token = \"meka-token\"",
            "token = \"meka-token\"\nturn_timeout = \"5m\"",
        );
        let config = parse(&raw).expect("valid");
        assert_eq!(config.bridge.typing_max, Duration::from_secs(300));
    }

    #[test]
    fn allowlisting_only_people_says_the_bot_is_reachable_by_dm_alone() {
        // The upgrade hazard. `allowed_users` used to admit a person wherever they wrote, so a
        // config naming only people worked in groups too. It now reaches direct messages alone, and
        // a bot that had been answering in a group goes quiet there with nothing on the wire to
        // explain it. This warning is the only thing that does.
        let telegram = parse(MINIMAL).expect("valid");
        assert!(
            telegram
                .warnings
                .iter()
                .any(|warning| warning.contains("direct message only")),
            "got: {:?}",
            telegram.warnings
        );

        let discord = parse(MINIMAL_DISCORD).expect("valid");
        assert!(
            discord
                .warnings
                .iter()
                .any(|warning| warning.contains("direct message only")),
            "got: {:?}",
            discord.warnings
        );
    }

    #[test]
    fn naming_a_room_as_well_draws_no_warning() {
        let raw = format!("{MINIMAL}allowed_chats = [-1001234567890]\n");
        let config = parse(&raw).expect("valid");
        assert!(
            !config
                .warnings
                .iter()
                .any(|warning| warning.contains("direct message only")),
            "a config that names a room is not DM-only: {:?}",
            config.warnings
        );
    }

    #[test]
    fn a_zero_turn_budget_is_refused() {
        // It is also the ceiling on waiting out a turn meka is running for itself. At zero that
        // wait ends before it begins, and the batch is requeued and resubmitted as fast as
        // the two processes can trade requests, which is the spin this rejects outright.
        let raw = MINIMAL.replace(
            "token = \"meka-token\"",
            "token = \"meka-token\"\nturn_timeout = \"0s\"",
        );
        let error = parse(&raw).expect_err("a zero turn budget cannot be honoured");
        assert!(error.to_string().contains("turn_timeout"), "got: {error}");
    }

    #[test]
    fn the_typing_ceiling_can_be_pinned() {
        let raw = format!("{MINIMAL}\n[bridge]\ntyping_max = \"45s\"\n");
        let config = parse(&raw).expect("valid");
        assert_eq!(config.bridge.typing_max, Duration::from_secs(45));
    }

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

        let PlatformConfig::Telegram(telegram) = &config.channels[0].platform else {
            panic!("the config under test declares one Telegram channel");
        };
        assert_eq!(telegram.parse_mode, TelegramParseMode::Html);
        assert!(
            !telegram.link_preview,
            "link previews default off; the template and the docs both say so"
        );
    }

    #[test]
    fn groups_default_to_mentions_only_and_direct_chats_to_everything() {
        // The shipped answer to "the bot receives every message in every group". A one-to-one chat
        // has nobody else in it, so mention-only there would silence the agent entirely.
        let config = parse(MINIMAL).expect("valid");
        assert_eq!(config.bridge.default_policy.direct, Policy::Active);
        assert_eq!(config.bridge.default_policy.group, Policy::Mute);
        assert_eq!(config.bridge.default_policy.channel, Policy::Mute);
        assert_eq!(
            config.bridge.default_policy.for_kind(ChatKind::Unknown),
            Policy::Active,
            "a chat the agent messaged first has an unknown shape, so it is heard in full"
        );
    }

    #[test]
    fn an_existing_bridge_section_still_gets_the_new_defaults() {
        // The shape every config written before 0.3.0 has: `[bridge]` is present and populated, and
        // `default_policy` is not mentioned at all. A derived `Default` on `FileBridge` would give
        // `active` for every kind here, which is the failure the hand-written impls exist to
        // prevent, and it would silently leave an upgraded deployment paying a turn per group
        // message.
        let raw = format!(
            "{MINIMAL}\n[bridge]\nbatch_max_messages = 32\nsettle = \"2s\"\ntyping_indicator = \
             true\n"
        );
        let config = parse(&raw).expect("valid");
        assert_eq!(config.bridge.default_policy.group, Policy::Mute);
        assert_eq!(config.bridge.default_policy.channel, Policy::Mute);
        assert_eq!(config.bridge.default_policy.direct, Policy::Active);
        assert_eq!(config.bridge.mute_context, 5);
    }

    #[test]
    fn the_default_policy_can_be_set_per_chat_kind() {
        let raw = format!("{MINIMAL}\n[bridge.default_policy]\ngroup = \"active\"\n");
        let config = parse(&raw).expect("valid");
        assert_eq!(config.bridge.default_policy.group, Policy::Active);
        assert_eq!(
            config.bridge.default_policy.channel,
            Policy::Mute,
            "the kinds are independent; setting one must not move another"
        );
    }

    #[test]
    fn blocking_by_default_is_allowed_but_said_out_loud() {
        // A coherent posture, but a bridge that answers nobody is indistinguishable from a broken
        // one, so it does not get to be silent about it.
        let raw = format!("{MINIMAL}\n[bridge.default_policy]\ngroup = \"block\"\n");
        let config = parse(&raw).expect("valid");
        assert!(
            config
                .warnings
                .iter()
                .any(|warning| warning.contains("default_policy.group")),
            "got: {:?}",
            config.warnings
        );
    }

    #[test]
    fn an_oversized_mute_context_is_rejected() {
        // The lookback is charged to every turn a muted chat wakes, so a generous setting quietly
        // turns mention-only back into every message.
        let raw = format!("{MINIMAL}\n[bridge]\nmute_context = 500\n");
        let error = parse(&raw).expect_err("must be rejected");
        assert!(error.to_string().contains("mute_context"), "got: {error}");
    }

    #[test]
    fn a_config_still_setting_mute_followup_is_refused() {
        // Held to the same standard as any other key that does not exist. A knob that silently
        // stopped doing anything would leave an operator reading their own config as the
        // explanation for behaviour it no longer controls, and looking for the fault somewhere
        // else entirely.
        let raw = format!("{MINIMAL}\n[bridge]\nmute_followup = \"5m\"\n");
        let error = parse(&raw).expect_err("a key that no longer exists must be refused");
        assert!(error.to_string().contains("mute_followup"), "got: {error}");
    }

    #[test]
    fn history_can_be_turned_off_entirely() {
        // The switch for a deployment that does not want a chat log on disk. Zero has to be a valid
        // setting rather than a rejected one.
        let raw = format!("{MINIMAL}\n[storage]\nhistory_retention = \"0s\"\n");
        let config = parse(&raw).expect("zero is a valid setting");
        assert!(config.storage.history_retention.is_zero());
    }

    #[test]
    fn the_settle_defaults_are_sized_for_somebody_composing() {
        // Both only apply where the platform reports typing, so neither is latency anybody pays on
        // a message they finished writing before sending. That is what lets them be this generous.
        let config = parse(MINIMAL).expect("valid");
        assert_eq!(config.bridge.settle, Duration::from_secs(3));
        assert_eq!(config.bridge.settle_max, Duration::from_secs(30));
        assert_eq!(
            config.bridge.coalesce_floor,
            Duration::from_secs(1),
            "the floor is the whole of the wait where typing cannot be seen, so it stays small"
        );
    }

    #[test]
    fn waiting_for_somebody_to_finish_can_be_turned_off() {
        // For an operator who would rather be answered mid-sentence than wait. Zero removes the
        // whole typing-gated wait, including the hold while somebody is still composing, and
        // nothing else: the floor is not reachable from the file format, because it exists for the
        // wire rather than as a preference.
        let raw = format!("{MINIMAL}\n[bridge]\nsettle = \"0s\"\n");
        let config = parse(&raw).expect("zero is a valid setting, not a rejected one");
        assert!(config.bridge.settle.is_zero());
        assert!(!config.bridge.coalesce_floor.is_zero());
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
        let PlatformConfig::Telegram(telegram) = &config.channels[0].platform else {
            panic!("the config under test declares one Telegram channel");
        };
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
        let PlatformConfig::Telegram(telegram) = &config.channels[0].platform else {
            panic!("the config under test declares one Telegram channel");
        };
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
