//! The platform abstraction.
//!
//! A channel is one configured bot instance on one platform. It does two things: pushes inbound
//! events into the bridge, and delivers outbound messages on request. Everything platform-specific
//! (long polling, rate limits, formatting, file upload) lives behind the [`Channel`] trait, so
//! adding a platform means writing one submodule and one factory arm.
//!
//! Agent-facing text is always Markdown. Each channel renders it into whatever its platform speaks,
//! rather than the agent having to know that Telegram wants a particular HTML subset and Discord
//! wants something else.

pub mod telegram;

use std::{collections::HashMap, path::Path, sync::Arc};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::config::{ChannelConfig, PlatformConfig, StorageConfig};

/// Supported platforms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Platform {
    Telegram,
}

impl Platform {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Telegram => "telegram",
        }
    }
}

/// A configured channel instance's name, unique across the process.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ChannelId(String);

impl ChannelId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ChannelId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// A conversation address, in the form `<channel>:<chat>` or `<channel>:<chat>:<thread>`.
///
/// This is the string the agent passes to `send_message`, so it is a stable public contract rather
/// than an internal detail. It is deliberately readable: an operator reading a log, or a person
/// telling the agent "message the group again", can both work with `telegram:-1001234567890`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ConversationId(String);

impl ConversationId {
    /// Build an id from its parts. `chat` and `thread` are platform-native identifiers.
    pub fn new(channel: &ChannelId, chat: &str, thread: Option<&str>) -> Self {
        Self(match thread {
            Some(thread) => format!("{}:{}:{}", channel.as_str(), chat, thread),
            None => format!("{}:{}", channel.as_str(), chat),
        })
    }

    /// Parse an id the agent supplied. Returns `None` when it is not well formed, which is how a
    /// typo becomes a helpful tool error rather than a lookup that silently finds nothing.
    pub fn parse(raw: &str) -> Option<Self> {
        let mut parts = raw.splitn(3, ':');
        let channel = parts.next().filter(|part| !part.is_empty())?;
        let chat = parts.next().filter(|part| !part.is_empty())?;
        if channel.contains(':') || chat.is_empty() {
            return None;
        }
        // A third segment is optional but must not be empty when the separator is present.
        if let Some(thread) = parts.next()
            && thread.is_empty()
        {
            return None;
        }
        Some(Self(raw.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The channel instance segment.
    pub fn channel(&self) -> &str {
        self.0.split(':').next().unwrap_or_default()
    }

    /// The platform-native chat identifier.
    pub fn chat(&self) -> &str {
        let mut parts = self.0.splitn(3, ':');
        parts.next();
        parts.next().unwrap_or_default()
    }

    /// The platform-native thread identifier, for forum topics and similar.
    pub fn thread(&self) -> Option<&str> {
        let mut parts = self.0.splitn(3, ':');
        parts.next();
        parts.next();
        parts.next()
    }
}

impl std::fmt::Display for ConversationId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Shape of a conversation, which is what tells the agent whether it is talking to one person.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChatKind {
    Direct,
    Group,
    Channel,
}

impl ChatKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Group => "group",
            Self::Channel => "channel",
        }
    }
}

/// Who sent an inbound message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Sender {
    /// Platform-native user id.
    pub id: String,
    pub display_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
}

/// Context for a message that was sent as a reply to another.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplyContext {
    pub message_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sender_name: Option<String>,
    /// Short quote of the message being replied to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub excerpt: Option<String>,
}

/// Broad category of a downloaded attachment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttachmentKind {
    Photo,
    Document,
    Audio,
    Voice,
    Video,
    Sticker,
}

impl AttachmentKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Photo => "photo",
            Self::Document => "document",
            Self::Audio => "audio",
            Self::Voice => "voice",
            Self::Video => "video",
            Self::Sticker => "sticker",
        }
    }
}

/// A file that came in with a message.
///
/// Everything is downloaded to local disk and named by path in the envelope, because that is what a
/// tool operating on a file needs. Images are additionally attached to the turn itself when meka's
/// profile has vision enabled and the turn's size budget allows, so the agent can simply look.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attachment {
    pub kind: AttachmentKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes: Option<u64>,
    /// Where the bridge saved it, absent when the download was skipped or failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<std::path::PathBuf>,
    /// Why the file is not on disk, when `path` is absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unavailable: Option<String>,
    /// Set by the bridge when the file was attached to the turn itself rather than only named by
    /// path. Not persisted with the queued payload: whether a turn can carry an image depends on
    /// meka's vision setting and the turn's size budget, both of which are decided at delivery
    /// time, not at receipt.
    #[serde(skip)]
    pub inlined: bool,
}

/// One message received from a platform.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InboundMessage {
    pub channel: ChannelId,
    pub platform: Platform,
    pub conversation: ConversationId,
    /// Platform-native message id, used both for reply threading and for queue deduplication.
    pub external_id: String,
    pub chat_kind: ChatKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chat_title: Option<String>,
    pub sender: Sender,
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<ReplyContext>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<Attachment>,
    pub timestamp: DateTime<Utc>,
}

/// Something that happened and should eventually reach the agent.
///
/// An enum rather than a bare message so a scheduler (waking the agent on a timer) or a system
/// notice can be added later without reshaping the queue, the envelope, or the drain loop.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InboundEvent {
    Message(InboundMessage),
}

impl InboundEvent {
    /// Conversation this event belongs to.
    pub const fn conversation(&self) -> &ConversationId {
        match self {
            Self::Message(message) => &message.conversation,
        }
    }

    /// Identifier unique within the conversation, used for deduplication.
    pub fn external_id(&self) -> &str {
        match self {
            Self::Message(message) => &message.external_id,
        }
    }

    pub const fn timestamp(&self) -> DateTime<Utc> {
        match self {
            Self::Message(message) => message.timestamp,
        }
    }
}

/// Per-send knobs every platform can approximate.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SendOptions {
    /// Platform message id to reply to, threading the reply where supported.
    pub reply_to: Option<String>,
    /// Deliver without a notification sound.
    pub silent: bool,
}

/// What a channel can do, so callers can degrade instead of failing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChannelCapabilities {
    pub typing_indicator: bool,
    pub files: bool,
    pub photos: bool,
}

/// Who a channel is logged in as. Used by `mekabridge doctor`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelIdentity {
    pub id: String,
    pub display_name: String,
    pub username: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum ChannelError {
    #[error("channel {channel}: authentication failed: {message}")]
    Auth { channel: String, message: String },

    #[error("{id:?} is not a valid conversation id: {reason}")]
    InvalidConversation { id: String, reason: String },

    #[error("channel {channel} does not support {feature}")]
    Unsupported {
        channel: String,
        feature: &'static str,
    },

    #[error("channel {channel}: the platform rejected the request: {message}")]
    Delivery { channel: String, message: String },

    #[error("channel {channel}: {message}")]
    Transport { channel: String, message: String },

    #[error("channel {channel}: {message}")]
    Setup { channel: String, message: String },
}

/// One platform connector.
#[async_trait]
pub trait Channel: Send + Sync + 'static {
    fn id(&self) -> &ChannelId;

    fn platform(&self) -> Platform;

    fn capabilities(&self) -> ChannelCapabilities;

    /// Consume platform updates until `shutdown` fires, pushing events into `sink`.
    async fn run(
        self: Arc<Self>,
        sink: mpsc::Sender<InboundEvent>,
        shutdown: CancellationToken,
    ) -> Result<(), ChannelError>;

    /// Deliver Markdown text, splitting it if the platform has a length limit. Returns one id per
    /// message actually sent.
    async fn send_text(
        &self,
        conversation: &ConversationId,
        markdown: &str,
        options: &SendOptions,
    ) -> Result<Vec<String>, ChannelError>;

    /// Deliver a local file.
    async fn send_file(
        &self,
        conversation: &ConversationId,
        path: &Path,
        caption: Option<&str>,
        as_photo: bool,
    ) -> Result<Vec<String>, ChannelError>;

    /// Show a transient "typing" state. Presence, not content, so this does not count as the bridge
    /// speaking on the agent's behalf. Best effort: failures are logged, never propagated to a
    /// turn.
    async fn set_typing(&self, conversation: &ConversationId) -> Result<(), ChannelError>;

    /// Confirm the credential works and report who the bot is.
    async fn probe(&self) -> Result<ChannelIdentity, ChannelError>;
}

/// Every configured channel, keyed by instance name.
pub struct ChannelRegistry {
    channels: HashMap<String, Arc<dyn Channel>>,
}

impl ChannelRegistry {
    /// Construct every configured channel. Adding a platform means adding one arm here.
    pub fn build(configs: &[ChannelConfig], storage: &StorageConfig) -> Result<Self, ChannelError> {
        let mut channels: HashMap<String, Arc<dyn Channel>> = HashMap::new();
        for config in configs {
            let id = ChannelId::new(config.id.clone());
            let channel: Arc<dyn Channel> = match &config.platform {
                PlatformConfig::Telegram(telegram) => Arc::new(telegram::TelegramChannel::new(
                    id.clone(),
                    telegram,
                    storage,
                )?),
            };
            channels.insert(id.as_str().to_string(), channel);
        }
        Ok(Self { channels })
    }

    /// Build a registry from already-constructed channels.
    ///
    /// The factory path covers normal startup; this exists so tests (and any future embedding) can
    /// supply their own [`Channel`] implementations without going through platform config.
    pub fn from_channels(channels: impl IntoIterator<Item = Arc<dyn Channel>>) -> Self {
        Self {
            channels: channels
                .into_iter()
                .map(|channel| (channel.id().as_str().to_string(), channel))
                .collect(),
        }
    }

    /// Look up a channel by instance name.
    pub fn get(&self, id: &str) -> Option<&Arc<dyn Channel>> {
        self.channels.get(id)
    }

    /// Find the channel that owns a conversation.
    pub fn resolve(
        &self,
        conversation: &ConversationId,
    ) -> Result<&Arc<dyn Channel>, ChannelError> {
        self.get(conversation.channel())
            .ok_or_else(|| ChannelError::InvalidConversation {
                id: conversation.as_str().to_string(),
                reason: format!("channel {:?} is not configured", conversation.channel()),
            })
    }

    pub fn iter(&self) -> impl Iterator<Item = &Arc<dyn Channel>> {
        self.channels.values()
    }

    /// How many channels are configured. Named `count` rather than `len` because a registry is not
    /// a collection and the `len`/`is_empty` pair would only add a method nothing calls.
    pub fn count(&self) -> usize {
        self.channels.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conversation_id_round_trips_without_a_thread() {
        let id = ConversationId::new(&ChannelId::new("telegram"), "123", None);
        assert_eq!(id.as_str(), "telegram:123");
        assert_eq!(id.channel(), "telegram");
        assert_eq!(id.chat(), "123");
        assert_eq!(id.thread(), None);
    }

    #[test]
    fn conversation_id_round_trips_with_a_thread() {
        let id = ConversationId::new(&ChannelId::new("telegram"), "-1001234", Some("77"));
        assert_eq!(id.as_str(), "telegram:-1001234:77");
        assert_eq!(id.channel(), "telegram");
        assert_eq!(id.chat(), "-1001234");
        assert_eq!(id.thread(), Some("77"));
    }

    #[test]
    fn negative_chat_ids_survive_parsing() {
        // Telegram groups have negative ids, and the '-' must not be mistaken for a separator.
        let id = ConversationId::parse("telegram:-1001234567890").expect("valid");
        assert_eq!(id.chat(), "-1001234567890");
    }

    #[test]
    fn malformed_conversation_ids_are_rejected() {
        assert!(ConversationId::parse("telegram").is_none());
        assert!(ConversationId::parse("telegram:").is_none());
        assert!(ConversationId::parse(":123").is_none());
        assert!(ConversationId::parse("").is_none());
        assert!(ConversationId::parse("telegram:123:").is_none());
    }

    #[test]
    fn thread_segments_may_contain_colons() {
        // `splitn(3, ..)` keeps everything after the second separator in the thread segment, so a
        // future platform with structured thread ids does not need a new id format.
        let id = ConversationId::parse("slack:C123:thread:1699:5").expect("valid");
        assert_eq!(id.channel(), "slack");
        assert_eq!(id.chat(), "C123");
        assert_eq!(id.thread(), Some("thread:1699:5"));
    }

    #[test]
    fn inbound_event_payloads_round_trip_through_json() {
        // The queue stores events as JSON, so a payload written before a restart has to deserialize
        // afterwards.
        let event = InboundEvent::Message(InboundMessage {
            channel: ChannelId::new("telegram"),
            platform: Platform::Telegram,
            conversation: ConversationId::parse("telegram:123").expect("valid"),
            external_id: "42".to_string(),
            chat_kind: ChatKind::Direct,
            chat_title: None,
            sender: Sender {
                id: "123".to_string(),
                display_name: "Alice".to_string(),
                username: Some("alice".to_string()),
            },
            text: "hello".to_string(),
            reply_to: None,
            attachments: vec![Attachment {
                kind: AttachmentKind::Photo,
                file_name: Some("photo.jpg".to_string()),
                media_type: Some("image/jpeg".to_string()),
                bytes: Some(2048),
                path: Some(std::path::PathBuf::from("/var/lib/mekabridge/a.jpg")),
                unavailable: None,
                inlined: false,
            }],
            timestamp: DateTime::parse_from_rfc3339("2026-08-05T12:00:00Z")
                .expect("literal parses")
                .with_timezone(&Utc),
        });
        let encoded = serde_json::to_string(&event).expect("serializes");
        let decoded: InboundEvent = serde_json::from_str(&encoded).expect("deserializes");
        assert_eq!(decoded, event);
        assert_eq!(decoded.external_id(), "42");
        assert_eq!(decoded.conversation().as_str(), "telegram:123");
    }
}
