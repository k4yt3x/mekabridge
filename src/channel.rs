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

use crate::config::{ChannelConfig, PlatformConfig};

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
    /// Platform-native user id. Empty when the message was posted on behalf of a chat rather than
    /// by an individual, which is what [`Sender::on_behalf_of_chat`] marks.
    pub id: String,
    pub display_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    /// The sender is another bot rather than a person.
    #[serde(default)]
    pub is_bot: bool,
    /// The message was posted as the chat itself, not by an identifiable account. Telegram does
    /// this for anonymous group admins and for channel posts forwarded into a linked
    /// discussion group. Without this the display name falls back to the chat title and an
    /// anonymous post is indistinguishable from a named person.
    #[serde(default)]
    pub on_behalf_of_chat: bool,
}

/// Why an inbound message was allowed to reach the agent.
///
/// The two are very different trust positions and the platform layer is the only thing that knows
/// which applies. Somebody speaking in an allowlisted group has not been vetted individually; the
/// agent needs that distinction to weigh what they ask for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Admission {
    /// The sender's own account is on the channel's user allowlist.
    User,
    /// The sender is not individually allowlisted; the chat they spoke in is.
    Chat,
}

impl Admission {
    /// How the envelope describes this admission to the agent.
    pub const fn describe(self) -> &'static str {
        match self {
            Self::User => "user allowlist",
            Self::Chat => "chat allowlist (sender not individually allowlisted)",
        }
    }
}

/// Where a forwarded message originally came from.
///
/// Mirrors the shape every platform converges on without letting a platform type cross the trait
/// boundary. This matters more than it looks: text someone forwarded from a stranger is not their
/// own words, and without this the agent reads it as if it were.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ForwardOrigin {
    /// An identifiable account.
    User {
        name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        username: Option<String>,
    },
    /// An account whose privacy settings hide the link back to it.
    HiddenUser { name: String },
    /// A group or supergroup.
    Chat { title: String },
    /// A channel post, which is addressable by id.
    Channel {
        title: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message_id: Option<String>,
    },
}

impl ForwardOrigin {
    /// How the envelope names this origin.
    pub fn describe(&self) -> String {
        match self {
            Self::User { name, id, username } => {
                let mut rendered = name.clone();
                match (username, id) {
                    (Some(username), Some(id)) => {
                        rendered.push_str(&format!(" (@{username}, id {id})"));
                    }
                    (Some(username), None) => rendered.push_str(&format!(" (@{username})")),
                    (None, Some(id)) => rendered.push_str(&format!(" (id {id})")),
                    (None, None) => {}
                }
                rendered
            }
            Self::HiddenUser { name } => {
                format!("{name} (account hidden by their privacy settings)")
            }
            Self::Chat { title } => format!("the group {title:?}"),
            Self::Channel { title, message_id } => match message_id {
                Some(message_id) => format!("the channel {title:?} (message {message_id})"),
                None => format!("the channel {title:?}"),
            },
        }
    }
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

/// Broad category of an attachment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttachmentKind {
    Photo,
    Document,
    Audio,
    Voice,
    Video,
    /// A short round video, which Telegram calls a video note.
    VideoNote,
    /// A soundless looping clip, delivered as MP4 rather than as a real GIF.
    Animation,
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
            Self::VideoNote => "video note",
            Self::Animation => "animation",
            Self::Sticker => "sticker",
        }
    }
}

/// A file that came in with a message.
///
/// Nothing is downloaded on arrival. The envelope announces what arrived and hands the agent a
/// handle, and the agent fetches only what it decides it needs. Three reasons: a download inside
/// the polling loop stalls every later message behind it, disk fills with files nobody asked for,
/// and, because the bridge owns one permanent session, an image attached to a turn stays in the
/// agent's context for the life of that session whether or not it ever mattered.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attachment {
    pub kind: AttachmentKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes: Option<u64>,
    /// Platform-native reference used to fetch the file later. Telegram file ids stay valid
    /// indefinitely for the same bot, which is what makes deferring the download safe.
    pub file_ref: String,
    /// Reference to a still frame, set when the file itself is not a viewable image. A video, an
    /// animation, or an animated sticker resolves to this, so "show me" works without transcoding
    /// anything.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thumb_ref: Option<String>,
    /// Short id the agent passes to the fetch tools. Assigned when the bridge registers the
    /// attachment, so it is unset on an attachment fresh from a channel.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handle: Option<String>,
}

/// A transient "something is coming" signal shown in a chat.
///
/// Platforms model this as a declaration of what the user is about to receive rather than as a
/// general busy light, which is why the variants name message kinds instead of bridge states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Activity {
    /// A text message is being composed.
    Typing,
    /// A photo is uploading.
    SendingPhoto,
    /// A file is uploading.
    SendingFile,
}

/// Bytes pulled from a platform on demand.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedFile {
    pub bytes: Vec<u8>,
    /// What the platform says this is, which can be better informed than the stored media type.
    pub media_type: Option<String>,
    /// Extension the platform's own path carried, used to name the file on disk.
    pub extension: Option<String>,
}

/// One message received from a platform.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InboundMessage {
    pub channel: ChannelId,
    pub platform: Platform,
    pub conversation: ConversationId,
    /// Queue deduplication key, unique within the conversation.
    ///
    /// Usually the platform message id, but not always: an edit carries the *same* message id as
    /// the message it revises, so an edit derives a distinct key. Use
    /// [`InboundMessage::message_id`] for anything that addresses the message on the platform.
    pub external_id: String,
    /// Platform-native message id, which is what a reply or a reaction targets.
    pub message_id: String,
    pub chat_kind: ChatKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chat_title: Option<String>,
    pub sender: Sender,
    /// Why this message was allowed through.
    pub admission: Admission,
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<ReplyContext>,
    /// Set when this is a revision of a message the agent may already have seen.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub edited_at: Option<DateTime<Utc>>,
    /// Set when the message was forwarded from somewhere else.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub forwarded_from: Option<ForwardOrigin>,
    /// Groups the messages of one album together. Platforms deliver an album as several messages
    /// sharing this id, so without it a batch of photos reads as unrelated pictures.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<Attachment>,
    /// Non-file content that has no text of its own: a shared location, a contact card, a poll.
    /// Rendered as a descriptor line so the message is never silently empty.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub notes: Vec<String>,
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
    pub reactions: bool,
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

    /// Retrieve one file the platform holds, identified by an [`Attachment::file_ref`].
    ///
    /// Bounded by `max_bytes` so a caller cannot be made to buffer an arbitrarily large file, and
    /// returning bytes rather than writing to disk because where they end up is the bridge's
    /// decision, not the platform's.
    async fn fetch(&self, file_ref: &str, max_bytes: u64) -> Result<FetchedFile, ChannelError>;

    /// Attach a reaction to one message, or clear it with `None`.
    ///
    /// Only ever called because the agent asked. The bridge does not acknowledge messages on its
    /// own: a reaction is content, and deciding whether to respond at all is the agent's.
    async fn react(
        &self,
        conversation: &ConversationId,
        message_id: &str,
        emoji: Option<&str>,
    ) -> Result<(), ChannelError>;

    /// Show a transient activity state. Presence, not content, so this does not count as the bridge
    /// speaking on the agent's behalf. Best effort: failures are logged, never propagated to a
    /// turn.
    async fn set_activity(
        &self,
        conversation: &ConversationId,
        activity: Activity,
    ) -> Result<(), ChannelError>;

    /// Confirm the credential works and report who the bot is.
    async fn probe(&self) -> Result<ChannelIdentity, ChannelError>;
}

/// Every configured channel, keyed by instance name.
pub struct ChannelRegistry {
    channels: HashMap<String, Arc<dyn Channel>>,
}

impl ChannelRegistry {
    /// Construct every configured channel. Adding a platform means adding one arm here.
    pub fn build(configs: &[ChannelConfig]) -> Result<Self, ChannelError> {
        let mut channels: HashMap<String, Arc<dyn Channel>> = HashMap::new();
        for config in configs {
            let id = ChannelId::new(config.id.clone());
            let channel: Arc<dyn Channel> = match &config.platform {
                PlatformConfig::Telegram(telegram) => {
                    Arc::new(telegram::TelegramChannel::new(id.clone(), telegram)?)
                }
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
            message_id: "42".to_string(),
            chat_kind: ChatKind::Direct,
            chat_title: None,
            sender: Sender {
                id: "123".to_string(),
                display_name: "Alice".to_string(),
                username: Some("alice".to_string()),
                is_bot: false,
                on_behalf_of_chat: false,
            },
            admission: Admission::User,
            text: "hello".to_string(),
            reply_to: None,
            edited_at: None,
            forwarded_from: Some(ForwardOrigin::User {
                name: "Bob".to_string(),
                id: Some("999".to_string()),
                username: Some("bob".to_string()),
            }),
            group_id: Some("13294839284".to_string()),
            notes: Vec::new(),
            attachments: vec![Attachment {
                kind: AttachmentKind::Photo,
                file_name: Some("photo.jpg".to_string()),
                media_type: Some("image/jpeg".to_string()),
                bytes: Some(2048),
                file_ref: "AgACAgEAAx".to_string(),
                thumb_ref: None,
                handle: Some("417".to_string()),
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
