//! Discord connector.
//!
//! Events come off twilight's `Shard`, which is a `Stream` of gateway messages, consumed directly
//! rather than through a framework. That mirrors the Telegram connector's reason for skipping
//! teloxide's `Dispatcher`: there is exactly one destination for every event, so a routing layer
//! would only sit between the socket and the queue.
//!
//! Two things about Discord shape almost everything here.
//!
//! Everything that holds messages is a channel with its own snowflake. A server text channel, a
//! thread, a forum post, and a direct message are all channels, so one conversation id form,
//! `discord:<channel_id>`, addresses all of them and the thread segment of [`ConversationId`] goes
//! unused. The exception is dialling somebody who has never written, since a Discord user id is not
//! a channel id: `discord:@<user_id>` is accepted for that and resolves to the real channel on
//! first send.
//!
//! Permissions are per channel, not per server. The bot can be free to post in one channel of a
//! server and silent in the next, so "can I do this" is never a question about the server, and
//! [`Channel::member`] computes the answer for the specific channel asked about.

pub mod cache;
pub mod presence;
pub mod render;

use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tokio::sync::{RwLock, mpsc};
use tokio_util::sync::CancellationToken;
use twilight_gateway::{
    ConfigBuilder, Event, EventTypeFlags, Intents, Shard, ShardId, StreamExt as _,
};
use twilight_http::{
    Client,
    request::{Method, RequestBuilder, channel::reaction::RequestReactionType},
    response::DeserializeBodyError,
};
use twilight_model::{
    channel::{
        ChannelType, Message,
        message::{
            AllowedMentions, EmojiReactionType, MentionType, MessageFlags, MessageType,
            sticker::StickerFormatType,
        },
    },
    gateway::{
        CloseCode,
        payload::outgoing::UpdatePresence,
        presence::{ActivityType, MinimalActivity, Status},
    },
    guild::Permissions,
    http::attachment::Attachment as OutboundAttachment,
    id::{
        Id,
        marker::{ChannelMarker, GuildMarker, MessageMarker, RoleMarker, UserMarker},
    },
    util::Timestamp,
};
use twilight_util::permission_calculator::PermissionCalculator;

use crate::{
    channel::{
        Activity, Admission, Attachment, AttachmentKind, Channel, ChannelCapabilities,
        ChannelError, ChannelId, ChannelIdentity, ChatKind, ChatSettings, ConversationId,
        FetchedFile, FileOptions, ForwardOrigin, FoundMessage, InboundEvent, InboundMessage,
        MemberAction, MemberCoverage, MemberInfo, MemberListing, MemberStatus, Platform, Presence,
        ReplyContext, SendOptions, Sender,
        discord::{cache::NameCache, presence::PresenceCache},
    },
    config::DiscordConfig,
};

/// Longest excerpt kept from a replied-to message.
const REPLY_EXCERPT_CHARS: usize = 160;

/// Most attachments Discord will take on one message.
///
/// Enforced here because twilight does not validate the count, so without it Discord answers a
/// generic rejection only after every file has been read into memory and uploaded.
const MAX_ATTACHMENTS: usize = 10;

/// Most members Discord will return from one listing or search call.
const MAX_MEMBER_PAGE: usize = 1000;

/// Discord's ceiling on a timeout, which it enforces and will not round for you.
const MAX_TIMEOUT: chrono::TimeDelta = chrono::TimeDelta::days(28);

/// Longest history a ban may delete, in seconds.
const MAX_BAN_DELETE_SECONDS: u32 = 604_800;

/// Ceiling on slowmode, in seconds.
const MAX_SLOWMODE_SECONDS: u16 = 21_600;

/// Most matches Discord's search returns per page.
const MAX_SEARCH_LIMIT: usize = 25;

/// What to say when Discord will not search a server it has not finished indexing.
const SEARCH_NOT_INDEXED: &str = "Discord is still indexing this server, so its own search cannot answer yet. Try again shortly.";

/// How many times to wait for Discord to finish indexing a server before giving up on a search.
///
/// A guild the bot has just joined answers `202 Index not yet available` with a `retry_after`, so
/// one retry is the difference between a search that works a moment later and one that reports no
/// results when there are plenty.
const SEARCH_INDEX_ATTEMPTS: usize = 3;

/// Gateway events this connector acts on.
///
/// Passed to `next_event` so everything else is discarded before it is deserialized. The intents
/// already decide what Discord sends; this decides what is worth turning into a struct.
const WANTED_EVENTS: EventTypeFlags = EventTypeFlags::READY
    .union(EventTypeFlags::MESSAGE_CREATE)
    .union(EventTypeFlags::MESSAGE_UPDATE)
    .union(EventTypeFlags::MESSAGE_DELETE)
    .union(EventTypeFlags::MESSAGE_DELETE_BULK)
    .union(EventTypeFlags::TYPING_START)
    .union(EventTypeFlags::GUILD_CREATE)
    .union(EventTypeFlags::GUILD_UPDATE)
    .union(EventTypeFlags::GUILD_DELETE)
    .union(EventTypeFlags::CHANNEL_CREATE)
    .union(EventTypeFlags::CHANNEL_UPDATE)
    .union(EventTypeFlags::CHANNEL_DELETE)
    .union(EventTypeFlags::THREAD_CREATE)
    .union(EventTypeFlags::THREAD_UPDATE)
    .union(EventTypeFlags::THREAD_DELETE)
    .union(EventTypeFlags::ROLE_CREATE)
    .union(EventTypeFlags::ROLE_UPDATE)
    .union(EventTypeFlags::ROLE_DELETE)
    .union(EventTypeFlags::PRESENCE_UPDATE);

pub struct DiscordChannel {
    id: ChannelId,
    http: Arc<Client>,
    token: String,
    /// Plain HTTP, for pulling attachments off the CDN. Discord's own client is for its API.
    downloader: reqwest::Client,
    allowed_users: HashSet<Id<UserMarker>>,
    allowed_guilds: HashSet<Id<GuildMarker>>,
    allowed_channels: HashSet<Id<ChannelMarker>>,
    allowed_roles: HashSet<Id<RoleMarker>>,
    allow_all: bool,
    admin_tools: bool,
    message_content: bool,
    presence: bool,
    mention_everyone: bool,
    mention_roles: bool,
    names: Arc<NameCache>,
    /// Who is online, accumulated from the gateway. Empty and unused unless `presence` is set.
    presences: Arc<PresenceCache>,
    /// The bot's own account id, filled by [`Channel::run`] before the first event is read. It is
    /// what `addressed` compares against, and what keeps the bot from answering itself.
    identity: tokio::sync::OnceCell<Id<UserMarker>>,
    /// Direct-message channels already opened, so dialling the same person twice costs one call.
    dm_channels: RwLock<HashMap<Id<UserMarker>, Id<ChannelMarker>>>,
}

/// Pick rustls' crypto provider before anything opens a connection.
///
/// twilight leaves the choice to the application, and both `ring` and `aws-lc-rs` are in this
/// dependency tree by way of other crates, so rustls cannot select one on its own and panics at the
/// first handshake instead. Installing one here makes that a decision rather than an accident. The
/// error case means somebody already installed one, which is equally fine.
fn install_crypto_provider() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        if rustls::crypto::ring::default_provider()
            .install_default()
            .is_err()
        {
            tracing::debug!("a rustls crypto provider was already installed");
        }
    });
}

impl DiscordChannel {
    pub fn new(id: ChannelId, config: &DiscordConfig) -> Result<Self, ChannelError> {
        install_crypto_provider();
        let token = config.token.expose().to_string();
        let downloader =
            reqwest::Client::builder()
                .build()
                .map_err(|error| ChannelError::Setup {
                    channel: id.as_str().to_string(),
                    message: format!("could not build the Discord download client: {error}"),
                })?;
        Ok(Self {
            id,
            http: Arc::new(Client::new(token.clone())),
            token,
            downloader,
            allowed_users: config.allowed_users.iter().copied().map(Id::new).collect(),
            allowed_guilds: config.allowed_guilds.iter().copied().map(Id::new).collect(),
            allowed_channels: config
                .allowed_channels
                .iter()
                .copied()
                .map(Id::new)
                .collect(),
            allowed_roles: config.allowed_roles.iter().copied().map(Id::new).collect(),
            allow_all: config.allow_all,
            admin_tools: config.admin_tools,
            message_content: config.message_content,
            presence: config.presence,
            mention_everyone: config.mention_everyone,
            mention_roles: config.mention_roles,
            names: NameCache::new(),
            presences: Arc::new(PresenceCache::default()),
            identity: tokio::sync::OnceCell::new(),
            dm_channels: RwLock::new(HashMap::new()),
        })
    }

    /// The name cache, for `doctor` and for tests.
    pub fn names(&self) -> &Arc<NameCache> {
        &self.names
    }

    /// Which gateway intents to identify with.
    ///
    /// `GUILD_MEMBERS` is deliberately absent. The partial member object, roles included, rides
    /// along on every guild message, and the REST member lookup works without it, so requesting a
    /// second privileged intent would buy nothing this connector uses.
    const fn intents(&self) -> Intents {
        // The two typing intents are unprivileged, so unlike `MESSAGE_CONTENT` and
        // `GUILD_PRESENCES` they need no portal toggle and cannot close the gateway with a 4014.
        // They are what lets a conversation be held until somebody stops composing rather than
        // until a guessed timer expires, which is a distinction Telegram cannot offer at all.
        let base = Intents::GUILDS
            .union(Intents::GUILD_MESSAGES)
            .union(Intents::DIRECT_MESSAGES)
            .union(Intents::GUILD_MESSAGE_TYPING)
            .union(Intents::DIRECT_MESSAGE_TYPING);
        let base = if self.message_content {
            base.union(Intents::MESSAGE_CONTENT)
        } else {
            base
        };
        if self.presence {
            base.union(Intents::GUILD_PRESENCES)
        } else {
            base
        }
    }

    /// Why this message is allowed through, or `None` to drop it.
    ///
    /// Ordered from the most specific grant to the least so the agent is told the narrowest true
    /// reason: somebody individually allowlisted reads as `user allowlist` even when the server
    /// they spoke in would also have admitted them.
    fn admission(
        &self,
        user_id: Id<UserMarker>,
        channel_id: Id<ChannelMarker>,
        guild_id: Option<Id<GuildMarker>>,
        roles: &[Id<RoleMarker>],
        direct: bool,
    ) -> Option<Admission> {
        // Direct messages only. `allowed_users` names people the bot should be reachable by, which
        // is not the same as a pass into every room it can see: somebody allowlisted so they can
        // message the bot privately should not thereby be heard in a server channel nobody named,
        // on a server nobody named. Reaching them in a channel is what the channel, role, and
        // server grants are for.
        // `direct` rather than merely "no guild": a group DM also carries no guild id, and up to
        // ten people can be in one, so admitting it here would report `user allowlist` for a room
        // the operator never named and the docs promise is a one-to-one chat.
        if direct && self.allowed_users.contains(&user_id) {
            return Some(Admission::User);
        }
        if roles.iter().any(|role| self.allowed_roles.contains(role)) {
            // Not `User`: nobody looked at this account. Somebody with the run of the server can
            // hand the role to anyone, and the agent should be told which of the two it is.
            return Some(Admission::Role);
        }
        if self.allowed_channels.contains(&channel_id) {
            return Some(Admission::Chat);
        }
        // A thread inherits the standing of the channel it hangs off. Allowlisting `#support` and
        // then being deaf inside every thread started in it would be a surprise, and the thread is
        // visible to exactly the people the parent already admits.
        if let Some(parent) = self.names.parent_of(channel_id)
            && self.allowed_channels.contains(&parent)
        {
            return Some(Admission::Chat);
        }
        if let Some(guild_id) = guild_id
            && self.allowed_guilds.contains(&guild_id)
        {
            return Some(Admission::Server);
        }
        if self.allow_all {
            return Some(Admission::Open);
        }
        None
    }

    /// Whether this message was aimed at the bot rather than merely said in front of it.
    ///
    /// Discord answers this natively. `mentions` is a resolved array of user objects on every
    /// message, and replying with the ping left on puts the person being replied to in it, so one
    /// id comparison covers both being named and being answered. No text is searched and no
    /// username is compared.
    ///
    /// Role pings and `@everyone` deliberately do not count. Both are broadcasts rather than
    /// address, and counting them would make one `@everyone` in a large server the cheapest
    /// possible way for anyone to force a turn.
    fn addressed(&self, message: &Message, channel_kind: Option<ChannelType>) -> bool {
        // In a direct message there is nobody else it could be for.
        if matches!(channel_kind, Some(ChannelType::Private)) || message.guild_id.is_none() {
            return true;
        }
        let Some(me) = self.identity.get() else {
            tracing::warn!(
                channel = %self.id,
                "the bot's own identity is unknown, so mentions cannot be recognised; treating \
                 this message as not addressed to it"
            );
            return false;
        };
        if message.mentions.iter().any(|mention| mention.id == *me) {
            return true;
        }
        // A reply with the ping turned off is not in `mentions`, but answering the agent is still
        // addressing it.
        message
            .referenced_message
            .as_ref()
            .is_some_and(|replied| replied.author.id == *me)
    }

    /// Resolve a conversation id to the channel a message would go to.
    ///
    /// `discord:@<user_id>` opens (or reuses) the direct-message channel with that person, which is
    /// the one case where the id the agent holds is not already a channel.
    async fn target(
        &self,
        conversation: &ConversationId,
    ) -> Result<Id<ChannelMarker>, ChannelError> {
        let chat = conversation.chat();
        if let Some(raw) = chat.strip_prefix('@') {
            let user_id = self.parse_user(raw)?;
            if let Some(channel_id) = self.dm_channels.read().await.get(&user_id).copied() {
                return Ok(channel_id);
            }
            let channel = self
                .http
                .create_private_channel(user_id)
                .await
                .map_err(|error| self.delivery_error("opening a direct message", &error))?
                .model()
                .await
                .map_err(|error| self.decode_error("the direct message channel", &error))?;
            self.dm_channels.write().await.insert(user_id, channel.id);
            self.names.insert_channel(&channel);
            return Ok(channel.id);
        }
        chat.parse::<u64>()
            .ok()
            .and_then(Id::new_checked)
            .ok_or_else(|| ChannelError::InvalidConversation {
                id: conversation.as_str().to_string(),
                reason:
                    "a Discord conversation is `discord:<channel id>`, or `discord:@<user id>` \
                         to open a direct message with somebody who has not written first"
                        .to_string(),
            })
    }

    fn parse_user(&self, raw: &str) -> Result<Id<UserMarker>, ChannelError> {
        raw.trim()
            .parse::<u64>()
            .ok()
            .and_then(Id::new_checked)
            .ok_or_else(|| ChannelError::InvalidConversation {
                id: raw.to_string(),
                reason: "a Discord user id is a number, from the `from:` line of a message header"
                    .to_string(),
            })
    }

    fn parse_message(&self, raw: &str) -> Result<Id<MessageMarker>, ChannelError> {
        raw.trim()
            .parse::<u64>()
            .ok()
            .and_then(Id::new_checked)
            .ok_or_else(|| ChannelError::InvalidConversation {
                id: raw.to_string(),
                reason: "a Discord message id is a number, from the `message:` line of a message \
                         header"
                    .to_string(),
            })
    }

    /// The server a conversation is in, which every moderation call needs and a direct message has
    /// none of.
    async fn guild_of(
        &self,
        conversation: &ConversationId,
        channel_id: Id<ChannelMarker>,
    ) -> Result<Id<GuildMarker>, ChannelError> {
        if let Some(guild_id) = self.names.guild_of(channel_id) {
            return Ok(guild_id);
        }
        // The cache is the gateway's own state, so a miss means a channel it was never told about.
        // Asking is cheap and beats refusing.
        let channel = self
            .http
            .channel(channel_id)
            .await
            .map_err(|error| self.delivery_error("looking up the channel", &error))?
            .model()
            .await
            .map_err(|error| self.decode_error("the channel", &error))?;
        self.names.insert_channel(&channel);
        channel
            .guild_id
            .ok_or_else(|| ChannelError::InvalidConversation {
                id: conversation.as_str().to_string(),
                reason: "this is a direct message, which has no server, so there is nothing to \
                         moderate here"
                    .to_string(),
            })
    }

    fn allowed_mentions(&self) -> AllowedMentions {
        let mut parse = vec![MentionType::Users];
        if self.mention_roles {
            parse.push(MentionType::Roles);
        }
        if self.mention_everyone {
            parse.push(MentionType::Everyone);
        }
        AllowedMentions {
            parse,
            // A reply pings the person being replied to unless this is off. Answering somebody is
            // not a reason to notify them twice.
            replied_user: false,
            ..Default::default()
        }
    }

    /// Flags for an edit, which unlike a send must state the choice in both directions.
    ///
    /// Leaving flags off an edit keeps whatever the original send chose, so an edit asking for a
    /// preview on a message sent without one would silently do nothing. `SUPPRESS_EMBEDS` is the
    /// only flag `update_message` accepts, so writing the whole set cannot clobber anything else.
    const fn edit_flags(link_preview: bool) -> MessageFlags {
        if link_preview {
            MessageFlags::empty()
        } else {
            MessageFlags::SUPPRESS_EMBEDS
        }
    }

    const fn outbound_flags(link_preview: bool) -> Option<MessageFlags> {
        if link_preview {
            None
        } else {
            Some(MessageFlags::SUPPRESS_EMBEDS)
        }
    }

    fn delivery_error(&self, doing: &str, error: &twilight_http::Error) -> ChannelError {
        ChannelError::Delivery {
            channel: self.id.as_str().to_string(),
            message: format!("{doing}: {error}"),
        }
    }

    fn decode_error(&self, what: &str, error: &DeserializeBodyError) -> ChannelError {
        ChannelError::Transport {
            channel: self.id.as_str().to_string(),
            message: format!("Discord's response for {what} could not be read: {error}"),
        }
    }

    /// Turn one gateway message into a bridge event, or `None` to ignore it.
    fn to_event(
        &self,
        message: &Message,
        channel_kind: Option<ChannelType>,
    ) -> Option<InboundEvent> {
        // The bot's own messages come back over the gateway. Feeding them to the agent would have
        // it answering itself.
        if self
            .identity
            .get()
            .is_some_and(|me| *me == message.author.id)
        {
            return None;
        }
        // Join notices, pin notices, boost announcements and the rest are Discord talking, not a
        // person. A forwarded message arrives as a DEFAULT with snapshots, so this does not lose
        // it.
        if !matches!(
            message.kind,
            MessageType::Regular | MessageType::Reply | MessageType::ThreadStarterMessage
        ) {
            return None;
        }

        let roles = message
            .member
            .as_ref()
            .map(|member| member.roles.clone())
            .unwrap_or_default();
        // Discord announces server channels over the gateway but not direct-message channels, so
        // the cache has nothing to say about a DM. The absent `guild_id` is the reliable signal,
        // and without this a DM would be filed as an unknown room and shown that way in the
        // chat list.
        let chat_kind = match channel_kind {
            Some(kind) => chat_kind(Some(kind)),
            None if message.guild_id.is_none() => ChatKind::Direct,
            None => ChatKind::Unknown,
        };
        let sender_allowlisted = self.allowed_users.contains(&message.author.id);
        let admission = self.admission(
            message.author.id,
            message.channel_id,
            message.guild_id,
            &roles,
            chat_kind == ChatKind::Direct,
        )?;

        let conversation = ConversationId::new(&self.id, &message.channel_id.to_string(), None);
        let display_name = message
            .member
            .as_ref()
            .and_then(|member| member.nick.clone())
            .or_else(|| message.author.global_name.clone())
            .unwrap_or_else(|| message.author.name.clone());

        let mut notes = Vec::new();
        if !self.message_content && message.content.is_empty() && message.attachments.is_empty() {
            notes.push(
                "this message's text is hidden because the bot does not hold Discord's message \
                 content intent"
                    .to_string(),
            );
        }
        for sticker in &message.sticker_items {
            notes.push(match sticker.format_type {
                StickerFormatType::Lottie => {
                    format!("sticker {:?} (animated, not viewable)", sticker.name)
                }
                _ => format!("sticker {:?}", sticker.name),
            });
        }
        if message.poll.is_some() {
            notes.push("a poll, which the bridge cannot read the options of".to_string());
        }

        let forwarded_from = message.message_snapshots.first().map(|_| {
            // A snapshot deliberately excludes its author, so the honest answer is that this text
            // was written somewhere else by somebody the bridge was not told the name of.
            ForwardOrigin::HiddenUser {
                name: "someone else (Discord does not name the author of a forward)".to_string(),
            }
        });
        let mut text = self.demention(&message.content, &message.mentions, message.guild_id);
        for snapshot in &message.message_snapshots {
            // Against the snapshot's own resolved mentions, not the outer message's: a name that
            // appears only in the forwarded text is not in the outer array and would come out as
            // "@someone".
            let quoted = self.demention(
                &snapshot.message.content,
                &snapshot.message.mentions,
                message.guild_id,
            );
            if !quoted.trim().is_empty() {
                if !text.is_empty() {
                    text.push_str("\n\n");
                }
                text.push_str(&quoted);
            }
        }

        let reply_to = message.referenced_message.as_ref().map(|replied| {
            let sender_name = replied
                .author
                .global_name
                .clone()
                .unwrap_or_else(|| replied.author.name.clone());
            ReplyContext {
                message_id: replied.id.to_string(),
                sender_name: Some(sender_name),
                // Likewise resolved against the replied-to message, which carries the mentions
                // that appear in it.
                excerpt: excerpt(&self.demention(
                    &replied.content,
                    &replied.mentions,
                    replied.guild_id.or(message.guild_id),
                )),
            }
        });
        // twilight cannot tell "Discord did not fetch the target" from "the target was deleted",
        // because both arrive as an absent `referenced_message`. Saying the reply exists and its
        // target is unavailable beats printing no reply at all.
        let reply_to = reply_to.or_else(|| {
            message
                .reference
                .as_ref()
                .and_then(|reference| reference.message_id)
                .map(|message_id| ReplyContext {
                    message_id: message_id.to_string(),
                    sender_name: None,
                    excerpt: None,
                })
        });

        let edited_at = message.edited_timestamp.and_then(timestamp_to_chrono);
        // An edit is a new event rather than a replacement, so it needs an id of its own or the
        // queue would take it for a redelivery of the original.
        let external_id = match edited_at {
            Some(edited_at) => format!("{}:e{}", message.id, edited_at.timestamp_millis()),
            None => message.id.to_string(),
        };

        Some(InboundEvent::Message(Box::new(InboundMessage {
            channel: self.id.clone(),
            platform: Platform::Discord,
            conversation,
            external_id,
            message_id: message.id.to_string(),
            chat_kind,
            chat_title: self.names.describe(message.channel_id),
            sender: Sender {
                id: message.author.id.to_string(),
                display_name,
                username: Some(message.author.name.clone()),
                is_bot: message.author.bot,
                on_behalf_of_chat: message.webhook_id.is_some(),
            },
            admission,
            sender_allowlisted,
            addressed: self.addressed(message, channel_kind),
            sender_roles: message
                .guild_id
                .map(|guild_id| self.names.role_names(guild_id, &roles))
                .unwrap_or_default(),
            text,
            reply_to,
            edited_at,
            forwarded_from,
            group_id: None,
            attachments: self.attachments(message),
            notes,
            arrived_mid_turn: false,
            timestamp: timestamp_to_chrono(message.timestamp).unwrap_or_else(Utc::now),
        })))
    }

    /// Files attached to a message, as handles rather than bytes.
    ///
    /// The reference is `<channel>/<message>/<attachment>` rather than the URL Discord supplied,
    /// because a Discord CDN URL is signed and expires. Re-requesting the message when the agent
    /// finally asks costs one call and is always correct, where storing the URL would leave a
    /// handle that works for a while and then silently does not.
    fn attachments(&self, message: &Message) -> Vec<Attachment> {
        message
            .attachments
            .iter()
            .map(|attachment| {
                let media_type = attachment.content_type.clone();
                // Discord models a voice note as an ordinary audio attachment carrying the
                // waveform its recorder produced, which is the only thing distinguishing it.
                let voice = attachment.duration_secs.is_some() && attachment.waveform.is_some();
                let kind = attachment_kind(media_type.as_deref(), voice);
                Attachment {
                    kind,
                    file_name: Some(attachment.filename.clone()),
                    media_type,
                    bytes: Some(attachment.size),
                    file_ref: format!("{}/{}/{}", message.channel_id, message.id, attachment.id),
                    // Discord exposes no still frame for a video to a bot, so there is nothing to
                    // fall back to. Saying nothing is better than a handle that fetches the video.
                    thumb_ref: None,
                    handle: None,
                }
            })
            .collect()
    }

    /// Rewrite Discord's id markup into names.
    ///
    /// Raw content is full of `<@123>`, `<@&456>`, `<#789>`, `<:shrug:111>` and `<t:1712:R>`. Left
    /// alone the agent reads opaque numbers, so they are resolved from the message's own resolved
    /// `mentions` array and the name cache.
    ///
    /// This does change what the sender literally typed, inside text the envelope fences as
    /// verbatim. The alternative is worse: an agent that cannot tell who was named.
    fn demention(
        &self,
        content: &str,
        mentions: &[twilight_model::channel::message::Mention],
        guild_id: Option<Id<GuildMarker>>,
    ) -> String {
        if !content.contains('<') && !content.contains('@') {
            return content.to_string();
        }
        let mut out = String::with_capacity(content.len());
        let mut rest = content;
        while let Some(start) = rest.find('<') {
            out.push_str(&rest[..start]);
            let after = &rest[start..];
            let Some(end) = after.find('>') else {
                out.push_str(after);
                return out;
            };
            let token = &after[1..end];
            out.push_str(&self.resolve_token(token, mentions, guild_id));
            rest = &after[end + 1..];
        }
        out.push_str(rest);
        out
    }

    /// One `<...>` token, resolved to a name or left as it was.
    fn resolve_token(
        &self,
        token: &str,
        mentions: &[twilight_model::channel::message::Mention],
        guild_id: Option<Id<GuildMarker>>,
    ) -> String {
        if let Some(raw) = token.strip_prefix("@&") {
            let name = raw
                .parse::<u64>()
                .ok()
                .and_then(Id::new_checked)
                .zip(guild_id)
                .and_then(|(role, guild)| self.names.role_name(guild, role));
            return match name {
                Some(name) => format!("@{name}"),
                None => "@a role".to_string(),
            };
        }
        if let Some(raw) = token.strip_prefix('@') {
            // `<@!123>` is the old nickname form and still turns up in older messages.
            let raw = raw.strip_prefix('!').unwrap_or(raw);
            let name = raw.parse::<u64>().ok().and_then(|id| {
                mentions
                    .iter()
                    .find(|mention| mention.id.get() == id)
                    .map(|mention| {
                        mention
                            .member
                            .as_ref()
                            .and_then(|member| member.nick.clone())
                            .unwrap_or_else(|| mention.name.clone())
                    })
            });
            return match name {
                Some(name) => format!("@{name}"),
                None => "@someone".to_string(),
            };
        }
        if let Some(raw) = token.strip_prefix('#') {
            let name = raw
                .parse::<u64>()
                .ok()
                .and_then(Id::new_checked)
                .and_then(|id| self.names.channel_name(id));
            return name.unwrap_or_else(|| "#a channel".to_string());
        }
        if let Some(rest) = token.strip_prefix("t:") {
            // `<t:1712345678:R>` renders as a relative time in the client. The agent cannot
            // evaluate that, so it becomes the absolute instant it stands for.
            let seconds = rest.split(':').next().unwrap_or_default();
            if let Ok(seconds) = seconds.parse::<i64>()
                && let Some(at) = DateTime::from_timestamp(seconds, 0)
            {
                return at.to_rfc3339();
            }
            return format!("<{token}>");
        }
        // `<:name:id>` and `<a:name:id>` are custom emoji.
        let emoji = token.strip_prefix('a').unwrap_or(token);
        if let Some(rest) = emoji.strip_prefix(':')
            && let Some((name, _)) = rest.split_once(':')
        {
            return format!(":{name}:");
        }
        format!("<{token}>")
    }

    /// Handle one gateway event.
    async fn handle(&self, event: Event, sink: &mpsc::Sender<InboundEvent>) {
        match event {
            Event::GuildCreate(guild) => {
                if let twilight_model::gateway::payload::incoming::GuildCreate::Available(guild) =
                    *guild
                {
                    self.names.insert_guild(&guild);
                    // Skipped entirely when the intent is off, because nothing reads the cache
                    // in that case: `presence_of` answers `None` before it is consulted. Seeding
                    // anyway would hold a map of every member of every server for no reader.
                    if self.presence {
                        self.presences.seed(
                            guild.id,
                            guild
                                .presences
                                .iter()
                                .map(|presence| (presence.user.id(), presence.status)),
                            Utc::now(),
                        );
                    }
                }
            }
            Event::TypingStart(typing) => {
                // The bot's own indicator comes back over the gateway like anybody else's. Left
                // unfiltered it would hold a conversation open for as long as the bridge kept
                // showing that the agent was working, which is exactly the wrong direction: the
                // chat would go quiet while the bridge waited on itself.
                if self.identity.get() == Some(&typing.user_id) {
                    return;
                }
                let conversation =
                    ConversationId::new(&self.id, &typing.channel_id.to_string(), None);
                // `try_send` rather than `send`: a busy server produces a lot of these and they are
                // advisory, so dropping one when the writer is behind costs a slightly early
                // release. Blocking the gateway task behind the durable writer would cost real
                // messages.
                if sink
                    .try_send(InboundEvent::Typing {
                        conversation,
                        author: typing.user_id.to_string(),
                        timestamp: Utc::now(),
                    })
                    .is_err()
                {
                    tracing::trace!(channel = %self.id, "dropped a typing notice; the writer is behind");
                }
            }
            Event::GuildUpdate(guild) => self.names.rename_guild(guild.id, &guild.name),
            Event::GuildDelete(guild) => {
                self.names.remove_guild(guild.id);
                self.presences.forget(guild.id);
            }
            Event::PresenceUpdate(update) => {
                self.presences
                    .update(update.guild_id, update.user.id(), update.status, Utc::now());
            }
            Event::ChannelCreate(channel) => self.names.insert_channel(&channel),
            Event::ChannelUpdate(channel) => self.names.insert_channel(&channel),
            Event::ChannelDelete(channel) => self.names.remove_channel(channel.id),
            Event::ThreadCreate(thread) => self.names.insert_channel(&thread),
            Event::ThreadUpdate(thread) => self.names.insert_channel(&thread),
            Event::ThreadDelete(thread) => self.names.remove_channel(thread.id),
            Event::RoleCreate(role) => self.names.insert_role(role.guild_id, &role.role),
            Event::RoleUpdate(role) => self.names.insert_role(role.guild_id, &role.role),
            Event::RoleDelete(role) => self.names.remove_role(role.guild_id, role.role_id),
            Event::MessageCreate(message) => {
                let kind = self.names.kind_of(message.channel_id);
                if let Some(event) = self.to_event(&message.0, kind) {
                    self.deliver(event, sink).await;
                }
            }
            Event::MessageUpdate(message) => {
                // Discord fires this when it resolves a link into an embed, with nothing the sender
                // did. Without this guard every posted link would arrive as an edit.
                if message.edited_timestamp.is_none() {
                    return;
                }
                let kind = self.names.kind_of(message.channel_id);
                if let Some(event) = self.to_event(&message.0, kind) {
                    self.deliver(event, sink).await;
                }
            }
            Event::MessageDelete(message) => {
                self.retract(message.channel_id, [message.id], sink).await;
            }
            Event::MessageDeleteBulk(bulk) => {
                self.retract(bulk.channel_id, bulk.ids, sink).await;
            }
            _ => {}
        }
    }

    /// Tell the bridge that messages are gone, so its record does not outlive Discord's.
    ///
    /// Telegram sends nothing when a message is deleted, so its archive cannot do this. Discord
    /// does, and an archive that keeps replaying something its author removed is a promise the
    /// bridge should not make when it does not have to.
    async fn retract(
        &self,
        channel_id: Id<ChannelMarker>,
        ids: impl IntoIterator<Item = Id<MessageMarker>>,
        sink: &mpsc::Sender<InboundEvent>,
    ) {
        let conversation = ConversationId::new(&self.id, &channel_id.to_string(), None);
        for id in ids {
            let event = InboundEvent::Retraction {
                conversation: conversation.clone(),
                message_id: id.to_string(),
                timestamp: Utc::now(),
            };
            self.deliver(event, sink).await;
        }
    }

    async fn deliver(&self, event: InboundEvent, sink: &mpsc::Sender<InboundEvent>) {
        if sink.send(event).await.is_err() {
            tracing::warn!(channel = %self.id, "the bridge stopped accepting events");
        }
    }

    /// Somebody's availability, or nothing at all when the operator has not switched presence on.
    ///
    /// `None` and [`crate::channel::PresenceStatus::Unknown`] both mean "cannot say", and the
    /// difference is worth keeping: `None` is a bridge that was never asked to track this, which no
    /// amount of waiting will change, while `Unknown` is one that has not caught up yet.
    fn presence_of(&self, guild_id: Id<GuildMarker>, user: Id<UserMarker>) -> Option<Presence> {
        self.presence.then(|| self.presences.get(guild_id, user))
    }

    /// Everything needed to place a member in a server, fetched once and reused across a page.
    ///
    /// Deriving status per member from a full permission calculation would be a round trip each,
    /// which for a thousand-member page is not worth doing to fill in one field. The owner and the
    /// set of roles carrying `ADMINISTRATOR` answer it from two calls for the whole page.
    async fn server_standing(
        &self,
        guild_id: Id<GuildMarker>,
    ) -> Result<ServerStanding, ChannelError> {
        let guild = self
            .http
            .guild(guild_id)
            .with_counts(true)
            .await
            .map_err(|error| self.delivery_error("reading the server", &error))?
            .model()
            .await
            .map_err(|error| self.decode_error("the server", &error))?;
        let roles = self
            .http
            .roles(guild_id)
            .await
            .map_err(|error| self.delivery_error("reading the server's roles", &error))?
            .models()
            .await
            .map_err(|error| self.decode_error("the server's roles", &error))?;
        Ok(ServerStanding {
            owner_id: guild.owner_id,
            total: guild.approximate_member_count,
            admin_roles: roles
                .iter()
                .filter(|role| role.permissions.contains(Permissions::ADMINISTRATOR))
                .map(|role| role.id)
                .collect(),
        })
    }

    /// Turn one member of a listing into the platform-neutral shape.
    fn summarise_member(
        &self,
        guild_id: Id<GuildMarker>,
        member: &twilight_model::guild::Member,
        standing: &ServerStanding,
    ) -> MemberInfo {
        let restricted_until = member
            .communication_disabled_until
            .and_then(timestamp_to_chrono)
            .filter(|until| *until > Utc::now());
        let status = if member.user.id == standing.owner_id {
            MemberStatus::Owner
        } else if member
            .roles
            .iter()
            .any(|role| standing.admin_roles.contains(role))
        {
            MemberStatus::Administrator
        } else if restricted_until.is_some() {
            MemberStatus::Restricted
        } else {
            MemberStatus::Member
        };
        MemberInfo {
            user_id: member.user.id.to_string(),
            display_name: member
                .nick
                .clone()
                .or_else(|| member.user.global_name.clone())
                .or_else(|| Some(member.user.name.clone())),
            status,
            rights: Vec::new(),
            roles: self.names.role_names(guild_id, &member.roles),
            restricted_until,
            presence: self.presence_of(guild_id, member.user.id),
        }
    }

    /// The path and query for one search request.
    ///
    /// Built from parts rather than one long literal, and separated out so a test can look at it:
    /// a `\\`-continued string here was once joined by rustfmt with its indentation intact, which
    /// put a run of spaces in the URL and made every search fail to build a request at all.
    fn search_path(
        &self,
        guild_id: Id<GuildMarker>,
        channel_id: Id<ChannelMarker>,
        query: &str,
        limit: usize,
    ) -> String {
        let limit = limit.clamp(1, MAX_SEARCH_LIMIT);
        let encoded: String = url::form_urlencoded::byte_serialize(query.as_bytes()).collect();
        format!("guilds/{guild_id}/messages/search?content={encoded}")
            + &format!("&channel_id={channel_id}")
            + &format!("&limit={limit}")
            + "&sort_by=timestamp"
    }

    /// Ask Discord to search its own record of one channel.
    ///
    /// twilight has no builder for this endpoint, so the request is assembled by hand and handed to
    /// the same client, which means the same auth, the same ratelimiter, and the same error
    /// handling as everything else here.
    async fn search_guild(
        &self,
        guild_id: Id<GuildMarker>,
        channel_id: Id<ChannelMarker>,
        query: &str,
        limit: usize,
    ) -> Result<Vec<FoundMessage>, ChannelError> {
        let path = self.search_path(guild_id, channel_id, query, limit);

        for attempt in 0..SEARCH_INDEX_ATTEMPTS {
            let request = RequestBuilder::raw(Method::Get, path.clone())
                .build()
                .map_err(|error| ChannelError::Transport {
                    channel: self.id.as_str().to_string(),
                    message: format!("could not build the search request: {error}"),
                })?;
            let body = self
                .http
                .request::<serde_json::Value>(request)
                .await
                .map_err(|error| self.delivery_error("searching this server", &error))?
                .model()
                .await
                .map_err(|error| self.decode_error("the search results", &error))?;

            // A guild that has not been indexed yet answers 200-shaped but with an error code and a
            // `retry_after` rather than results. Reporting "nothing found" here would be a lie.
            if let Some(retry_after) = body.get("retry_after").and_then(serde_json::Value::as_f64) {
                if attempt + 1 == SEARCH_INDEX_ATTEMPTS {
                    return Err(ChannelError::Delivery {
                        channel: self.id.as_str().to_string(),
                        message: SEARCH_NOT_INDEXED.to_string(),
                    });
                }
                let wait = Duration::from_secs_f64(retry_after.clamp(0.5, 5.0));
                tokio::time::sleep(wait).await;
                continue;
            }

            // The `messages` field is an array of arrays: the inner one used to carry surrounding
            // context and now holds the single match.
            let found = body
                .get("messages")
                .and_then(serde_json::Value::as_array)
                .map(|groups| {
                    groups
                        .iter()
                        .filter_map(|group| match group {
                            serde_json::Value::Array(inner) => inner.first(),
                            other => Some(other),
                        })
                        .filter_map(found_message)
                        .collect()
                })
                .unwrap_or_default();
            return Ok(found);
        }
        Ok(Vec::new())
    }

    /// The bot's own permissions in one channel.
    ///
    /// Computed rather than guessed, because Discord applies role permissions and then per-channel
    /// overwrites, so holding a permission in a server says nothing about holding it in a given
    /// room. Two REST calls, only ever on the agent's own initiative.
    async fn permissions_in(
        &self,
        guild_id: Id<GuildMarker>,
        channel_id: Id<ChannelMarker>,
        user_id: Id<UserMarker>,
        member_roles: &[Id<RoleMarker>],
    ) -> Result<Permissions, ChannelError> {
        let roles = self
            .http
            .roles(guild_id)
            .await
            .map_err(|error| self.delivery_error("reading the server's roles", &error))?
            .models()
            .await
            .map_err(|error| self.decode_error("the server's roles", &error))?;
        let everyone = roles
            .iter()
            .find(|role| role.id.cast() == guild_id)
            .map(|role| role.permissions)
            .unwrap_or_else(Permissions::empty);
        let held: Vec<(Id<RoleMarker>, Permissions)> = roles
            .iter()
            .filter(|role| member_roles.contains(&role.id))
            .map(|role| (role.id, role.permissions))
            .collect();

        let channel = self
            .http
            .channel(channel_id)
            .await
            .map_err(|error| self.delivery_error("reading the channel", &error))?
            .model()
            .await
            .map_err(|error| self.decode_error("the channel", &error))?;
        // A thread carries no overwrites of its own and inherits the channel it hangs off, so
        // asking about a thread means asking about its parent. Without this the calculator would
        // run against an empty overwrite set and report permissions the bot does not have there.
        let overwrites = match channel.permission_overwrites {
            Some(overwrites) => overwrites,
            None => match channel.parent_id {
                Some(parent) => self
                    .http
                    .channel(parent)
                    .await
                    .map_err(|error| self.delivery_error("reading the parent channel", &error))?
                    .model()
                    .await
                    .map_err(|error| self.decode_error("the parent channel", &error))?
                    .permission_overwrites
                    .unwrap_or_default(),
                None => Vec::new(),
            },
        };

        let calculator = PermissionCalculator::new(guild_id, user_id, everyone, &held);
        Ok(calculator.in_channel(channel.kind, &overwrites))
    }
}

/// Discord's channel type, as the bridge's notion of a chat's shape.
fn chat_kind(kind: Option<ChannelType>) -> ChatKind {
    match kind {
        Some(ChannelType::Private) => ChatKind::Direct,
        // Announcement channels are broadcast-shaped, which is the distinction `Channel` draws.
        Some(ChannelType::GuildAnnouncement | ChannelType::AnnouncementThread) => ChatKind::Channel,
        Some(
            ChannelType::GuildText
            | ChannelType::GuildVoice
            | ChannelType::GuildStageVoice
            | ChannelType::PublicThread
            | ChannelType::PrivateThread
            | ChannelType::Group,
        ) => ChatKind::Group,
        // A message arrived, so it is some kind of room; which one the gateway did not say.
        _ => ChatKind::Unknown,
    }
}

/// Classify an attachment from what Discord says it is.
fn attachment_kind(media_type: Option<&str>, voice: bool) -> AttachmentKind {
    let media_type = media_type.unwrap_or_default();
    if media_type == "image/gif" {
        return AttachmentKind::Animation;
    }
    if media_type.starts_with("image/") {
        return AttachmentKind::Photo;
    }
    if media_type.starts_with("video/") {
        return AttachmentKind::Video;
    }
    if media_type.starts_with("audio/") {
        return if voice {
            AttachmentKind::Voice
        } else {
            AttachmentKind::Audio
        };
    }
    AttachmentKind::Document
}

/// Server-wide facts needed to place every member of a page, fetched once per listing.
struct ServerStanding {
    owner_id: Id<UserMarker>,
    /// Discord's own estimate of the server's size, which it will give even where it will not give
    /// the roster.
    total: Option<u64>,
    admin_roles: HashSet<Id<RoleMarker>>,
}

/// Whether Discord refused outright, as opposed to failing for some other reason.
///
/// A privileged-intent refusal arrives as a plain 403 rather than anything more specific, so this
/// is what stands between "you have not switched the intent on" and a transport error the operator
/// would go looking in the wrong place for.
fn is_forbidden(error: &twilight_http::Error) -> bool {
    matches!(
        error.kind(),
        twilight_http::error::ErrorType::Response { status, .. } if status.get() == 403
    )
}

fn timestamp_to_chrono(timestamp: Timestamp) -> Option<DateTime<Utc>> {
    DateTime::from_timestamp_micros(timestamp.as_micros())
}

/// One search hit, from the JSON Discord returned.
///
/// Anything without an author, text, and a time is skipped rather than filled in with placeholders:
/// a result the agent cannot attribute is worse than one fewer result.
fn found_message(value: &serde_json::Value) -> Option<FoundMessage> {
    let author = value.get("author")?;
    let sender_name = author
        .get("global_name")
        .and_then(serde_json::Value::as_str)
        .or_else(|| author.get("username").and_then(serde_json::Value::as_str))?
        .to_string();
    let text = value.get("content")?.as_str()?.to_string();
    if text.trim().is_empty() {
        return None;
    }
    let timestamp = value
        .get("timestamp")?
        .as_str()
        .and_then(|raw| DateTime::parse_from_rfc3339(raw).ok())?
        .with_timezone(&Utc);
    Some(FoundMessage {
        message_id: value.get("id")?.as_str()?.to_string(),
        sender_name,
        text,
        timestamp,
    })
}

fn excerpt(text: &str) -> Option<String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    let mut excerpt: String = trimmed.chars().take(REPLY_EXCERPT_CHARS).collect();
    if trimmed.chars().count() > REPLY_EXCERPT_CHARS {
        excerpt.push('\u{2026}');
    }
    Some(excerpt.replace('\n', " "))
}

#[async_trait]
impl Channel for DiscordChannel {
    fn id(&self) -> &ChannelId {
        &self.id
    }

    fn platform(&self) -> Platform {
        Platform::Discord
    }

    fn capabilities(&self) -> ChannelCapabilities {
        ChannelCapabilities {
            typing_indicator: true,
            files: true,
            photos: true,
            reactions: true,
            edit: true,
            admin: self.admin_tools,
            presence: self.presence,
            typing_status: true,
            // Discord grants privileges through roles only. There is no per-member permission set
            // to hand a named list of rights to.
            member_rights: false,
            member_roles: self.admin_tools,
        }
    }

    async fn run(
        self: Arc<Self>,
        sink: mpsc::Sender<InboundEvent>,
        shutdown: CancellationToken,
    ) -> Result<(), ChannelError> {
        // Primed before the first event, because `addressed` compares against it and a message
        // arriving first would be judged as not addressed to a bot whose id is unknown.
        let identity = self.probe().await?;
        let me = identity
            .id
            .parse::<u64>()
            .ok()
            .and_then(Id::new_checked)
            .ok_or_else(|| ChannelError::Auth {
                channel: self.id.as_str().to_string(),
                message: "Discord returned a user id that is not a snowflake".to_string(),
            })?;
        if self.identity.set(me).is_err() {
            // Only reachable if `run` were entered twice for the same channel, which the registry
            // does not do. Worth a line rather than a discarded result: it would mean the connector
            // is running twice and racing itself.
            tracing::warn!(channel = %self.id, "the Discord identity was already set");
        }

        let presence = UpdatePresence::new(
            vec![
                MinimalActivity {
                    kind: ActivityType::Custom,
                    name: "relaying to meka".to_string(),
                    url: None,
                }
                .into(),
            ],
            false,
            None,
            Status::Online,
        )
        .map_err(|error| ChannelError::Setup {
            channel: self.id.as_str().to_string(),
            message: format!("could not build the presence update: {error}"),
        })?;
        let config = ConfigBuilder::new(self.token.clone(), self.intents())
            .presence(presence.d)
            .build();
        let mut shard = Shard::with_config(ShardId::ONE, config);
        tracing::info!(channel = %self.id, "connecting to the Discord gateway");

        let mut fatal: Option<CloseCode> = None;
        loop {
            let item = tokio::select! {
                () = shutdown.cancelled() => {
                    tracing::info!(channel = %self.id, "shutting down the Discord gateway");
                    return Ok(());
                }
                item = shard.next_event(WANTED_EVENTS) => item,
            };
            let Some(item) = item else { break };
            match item {
                Ok(Event::GatewayClose(frame)) => {
                    if let Some(frame) = frame
                        && let Ok(code) = CloseCode::try_from(frame.code)
                        && !code.can_reconnect()
                    {
                        fatal = Some(code);
                    }
                }
                Ok(event) => self.handle(event, &sink).await,
                Err(error) => {
                    // Recoverable by construction: the shard reconnects itself, and the next call
                    // continues the stream.
                    tracing::warn!(channel = %self.id, "gateway error: {}", error);
                }
            }
        }

        match fatal {
            // `MESSAGE_CONTENT` is the only privileged intent this connector asks for, so when it
            // was asked for, that is what was refused. With it off, a 4014 means something else
            // entirely, and telling the operator to turn off a setting already off sends them in a
            // circle.
            Some(CloseCode::DisallowedIntents) if !self.message_content => {
                Err(ChannelError::Auth {
                    channel: self.id.as_str().to_string(),
                    message: "Discord refused an intent this bot asked for, none of which are \
                          privileged. Check the Bot page of the Developer Portal"
                        .to_string(),
                })
            }
            Some(CloseCode::DisallowedIntents) => Err(ChannelError::Auth {
                channel: self.id.as_str().to_string(),
                message: "Discord refused the message content intent. Enable it under Privileged \
                          Gateway Intents on the Bot page of the Developer Portal, or set \
                          `message_content = false` to run without it"
                    .to_string(),
            }),
            Some(CloseCode::AuthenticationFailed) => Err(ChannelError::Auth {
                channel: self.id.as_str().to_string(),
                message: "Discord rejected the bot token".to_string(),
            }),
            Some(code) => Err(ChannelError::Transport {
                channel: self.id.as_str().to_string(),
                message: format!(
                    "the Discord gateway closed with code {}, which cannot be reconnected",
                    code as u16
                ),
            }),
            None => {
                tracing::info!(channel = %self.id, "the Discord gateway stream ended");
                Ok(())
            }
        }
    }

    async fn send_text(
        &self,
        conversation: &ConversationId,
        markdown: &str,
        options: &SendOptions,
    ) -> Result<Vec<String>, ChannelError> {
        let channel_id = self.target(conversation).await?;
        let bodies = render::to_markdown(markdown, render::MESSAGE_LIMIT);
        if bodies.is_empty() {
            return Ok(Vec::new());
        }
        let reply_to = options
            .reply_to
            .as_deref()
            .map(|raw| self.parse_message(raw))
            .transpose()?;
        let mentions = self.allowed_mentions();

        let mut flags =
            Self::outbound_flags(options.link_preview).unwrap_or_else(MessageFlags::empty);
        if options.silent {
            flags |= MessageFlags::SUPPRESS_NOTIFICATIONS;
        }

        let mut sent = Vec::with_capacity(bodies.len());
        for (index, body) in bodies.iter().enumerate() {
            let mut request = self
                .http
                .create_message(channel_id)
                .content(body)
                .allowed_mentions(Some(&mentions));
            if !flags.is_empty() {
                request = request.flags(flags);
            }
            // Only the first part quotes what is being replied to; repeating the quote on every
            // part of a long answer is noise.
            if index == 0
                && let Some(reply_to) = reply_to
            {
                request = request.reply(reply_to).fail_if_not_exists(false);
            }
            let message = request
                .await
                .map_err(|error| {
                    // A partial send is worth surfacing precisely: the agent needs to know some of
                    // its message did land, so it does not resend the whole thing.
                    ChannelError::Delivery {
                        channel: self.id.as_str().to_string(),
                        message: format!("part {} of {} failed: {error}", index + 1, bodies.len()),
                    }
                })?
                .model()
                .await
                .map_err(|error| self.decode_error("the sent message", &error))?;
            sent.push(message.id.to_string());
        }
        Ok(sent)
    }

    async fn send_files(
        &self,
        conversation: &ConversationId,
        paths: &[PathBuf],
        caption: Option<&str>,
        options: &FileOptions,
    ) -> Result<Vec<String>, ChannelError> {
        let channel_id = self.target(conversation).await?;
        // Answered rather than sent as a caption-only message, which is what an empty list would
        // otherwise produce here: nothing indexes `paths`, so there is no panic to stop it. The
        // bridge enforces the same contract, and this is the local half of it.
        if paths.is_empty() {
            return Err(ChannelError::Delivery {
                channel: self.id.as_str().to_string(),
                message: "no files were given to send".to_string(),
            });
        }
        // Refused here because twilight does not validate the count: without this Discord answers a
        // generic rejection, after every file has been read into memory and uploaded.
        if paths.len() > MAX_ATTACHMENTS {
            return Err(ChannelError::Delivery {
                channel: self.id.as_str().to_string(),
                message: format!(
                    "Discord takes at most {MAX_ATTACHMENTS} attachments on one message, and {} \
                     were given. Send them in several batches.",
                    paths.len()
                ),
            });
        }
        // Discord renders an image inline from the attachment itself, so there is no separate photo
        // send to choose. The flag only decides which indicator is shown while it uploads.
        let activity = if options.as_photo {
            Activity::SendingPhoto
        } else {
            Activity::SendingFile
        };
        if let Err(error) = self.set_activity(conversation, activity).await {
            tracing::debug!(conversation = %conversation, "upload indicator failed: {}", error);
        }

        // Every file is read whole and held until the request is built, unlike Telegram's, which
        // streams from the path. Ten files is ten buffers resident at once, which is what
        // `MAX_ATTACHMENTS` bounds in practice as much as Discord's own rule does.
        let mut attachments = Vec::with_capacity(paths.len());
        for (index, path) in paths.iter().enumerate() {
            let bytes = tokio::fs::read(path)
                .await
                .map_err(|error| ChannelError::Delivery {
                    channel: self.id.as_str().to_string(),
                    message: format!("could not read {}: {error}", path.display()),
                })?;
            let name = path
                .file_name()
                .map(|name| name.to_string_lossy().to_string())
                .unwrap_or_else(|| "file".to_string());
            // The third argument is the attachment's id, which Discord uses to match a part of the
            // multipart body to its descriptor. Enumerated rather than fixed: reusing one id for
            // every file leaves the request describing a single attachment several times over.
            attachments.push(OutboundAttachment::from_bytes(name, bytes, index as u64));
        }
        let mentions = self.allowed_mentions();
        let reply_to = options
            .send
            .reply_to
            .as_deref()
            .map(|raw| self.parse_message(raw))
            .transpose()?;

        let mut request = self
            .http
            .create_message(channel_id)
            .attachments(&attachments)
            .allowed_mentions(Some(&mentions));
        if let Some(reply_to) = reply_to {
            request = request.reply(reply_to);
        }
        // A caption is a message body, so a link in one answers to the same switch a link in a
        // message does. Skipped rather than set to empty when a preview *is* wanted, matching
        // `send_text`: on a create, leaving flags unset is already "no suppression".
        let mut flags =
            Self::outbound_flags(options.send.link_preview).unwrap_or_else(MessageFlags::empty);
        if options.send.silent {
            flags |= MessageFlags::SUPPRESS_NOTIFICATIONS;
        }
        if !flags.is_empty() {
            request = request.flags(flags);
        }
        // Refused rather than truncated, for the same reason as Telegram's: the caption belongs to
        // the file and cannot be continued, so dropping everything past the first part loses it
        // silently while still reporting success.
        let body = match caption {
            None => None,
            Some(caption) => {
                let mut bodies = render::to_markdown(caption, render::MESSAGE_LIMIT);
                if bodies.len() > 1 {
                    return Err(ChannelError::Delivery {
                        channel: self.id.as_str().to_string(),
                        message: format!(
                            "the caption is longer than the {} characters Discord allows on a \
                             message. Shorten it, or send the file with a short caption and the \
                             rest as a message.",
                            render::MESSAGE_LIMIT
                        ),
                    });
                }
                bodies.pop()
            }
        };
        if let Some(body) = &body {
            request = request.content(body);
        }
        let message = request
            .await
            .map_err(|error| self.delivery_error("sending the file", &error))?
            .model()
            .await
            .map_err(|error| self.decode_error("the sent message", &error))?;
        Ok(vec![message.id.to_string()])
    }

    async fn fetch(&self, file_ref: &str, max_bytes: u64) -> Result<FetchedFile, ChannelError> {
        let mut parts = file_ref.split('/');
        let (Some(channel), Some(message), Some(attachment)) =
            (parts.next(), parts.next(), parts.next())
        else {
            return Err(ChannelError::InvalidConversation {
                id: file_ref.to_string(),
                reason: "a Discord file reference is `<channel>/<message>/<attachment>`"
                    .to_string(),
            });
        };
        let channel_id: Id<ChannelMarker> = channel
            .parse::<u64>()
            .ok()
            .and_then(Id::new_checked)
            .ok_or_else(|| ChannelError::InvalidConversation {
            id: file_ref.to_string(),
            reason: "the channel part is not a Discord id".to_string(),
        })?;
        let message_id = self.parse_message(message)?;

        // Re-requested rather than remembered: the URL Discord handed over with the original
        // message is signed and has since expired.
        let message = self
            .http
            .message(channel_id, message_id)
            .await
            .map_err(|error| ChannelError::Delivery {
                channel: self.id.as_str().to_string(),
                message: format!(
                    "could not re-read the message holding this file, which is what Discord \
                     requires to refresh its expiring link: {error}"
                ),
            })?
            .model()
            .await
            .map_err(|error| self.decode_error("the message holding this file", &error))?;
        let found = message
            .attachments
            .iter()
            .find(|candidate| candidate.id.to_string() == attachment)
            .ok_or_else(|| ChannelError::Delivery {
                channel: self.id.as_str().to_string(),
                message: "that file is no longer attached to its message".to_string(),
            })?;
        if found.size > max_bytes {
            return Err(ChannelError::Delivery {
                channel: self.id.as_str().to_string(),
                message: format!(
                    "this file is {} bytes, over the {max_bytes} byte limit",
                    found.size
                ),
            });
        }

        let response = self
            .downloader
            .get(&found.url)
            .send()
            .await
            .map_err(|error| ChannelError::Transport {
                channel: self.id.as_str().to_string(),
                message: format!("downloading the file failed: {error}"),
            })?;
        let extension = Path::new(&found.filename)
            .extension()
            .map(|extension| extension.to_string_lossy().to_string());
        let bytes = response
            .bytes()
            .await
            .map_err(|error| ChannelError::Transport {
                channel: self.id.as_str().to_string(),
                message: format!("reading the downloaded file failed: {error}"),
            })?;
        Ok(FetchedFile {
            bytes: bytes.to_vec(),
            media_type: found.content_type.clone(),
            extension,
        })
    }

    async fn react(
        &self,
        conversation: &ConversationId,
        message_id: &str,
        emoji: Option<&str>,
    ) -> Result<(), ChannelError> {
        let channel_id = self.target(conversation).await?;
        let message_id = self.parse_message(message_id)?;
        match emoji {
            Some(emoji) => {
                let reaction = RequestReactionType::Unicode { name: emoji };
                self.http
                    .create_reaction(channel_id, message_id, &reaction)
                    .await
                    .map_err(|error| self.delivery_error("adding the reaction", &error))?;
            }
            None => {
                // Discord has no "clear whatever I put here", so the bot's own reactions are read
                // back and removed one at a time.
                let message = self
                    .http
                    .message(channel_id, message_id)
                    .await
                    .map_err(|error| self.delivery_error("reading the message", &error))?
                    .model()
                    .await
                    .map_err(|error| self.decode_error("the message", &error))?;
                for reaction in message.reactions.iter().filter(|reaction| reaction.me) {
                    let request = match &reaction.emoji {
                        EmojiReactionType::Unicode { name } => {
                            RequestReactionType::Unicode { name }
                        }
                        EmojiReactionType::Custom { id, name, .. } => RequestReactionType::Custom {
                            id: *id,
                            name: name.as_deref(),
                        },
                    };
                    self.http
                        .delete_current_user_reaction(channel_id, message_id, &request)
                        .await
                        .map_err(|error| self.delivery_error("clearing the reaction", &error))?;
                }
            }
        }
        Ok(())
    }

    async fn edit_text(
        &self,
        conversation: &ConversationId,
        message_id: &str,
        markdown: &str,
        link_preview: bool,
    ) -> Result<(), ChannelError> {
        let channel_id = self.target(conversation).await?;
        let message_id = self.parse_message(message_id)?;
        let bodies = render::to_markdown(markdown, render::MESSAGE_LIMIT);
        let [body] = bodies.as_slice() else {
            return Err(ChannelError::Delivery {
                channel: self.id.as_str().to_string(),
                message: if bodies.is_empty() {
                    "that text renders to nothing, so there would be no message left".to_string()
                } else {
                    format!(
                        "that text is too long to be one Discord message ({} parts at {} \
                         characters each), and an edit cannot be split",
                        bodies.len(),
                        render::MESSAGE_LIMIT
                    )
                },
            });
        };
        let mentions = self.allowed_mentions();
        // Set unconditionally, unlike the send path, because an edit has to be able to *lift*
        // suppression as well as apply it: leaving flags alone keeps whatever the original send
        // chose, so an edit asking for a preview on a message sent without one would silently do
        // nothing. `SUPPRESS_EMBEDS` is the only flag `update_message` accepts, so writing the
        // whole set cannot clobber anything else.
        self.http
            .update_message(channel_id, message_id)
            .content(Some(body))
            .allowed_mentions(Some(&mentions))
            .flags(Self::edit_flags(link_preview))
            .await
            .map_err(|error| self.delivery_error("editing the message", &error))?;
        Ok(())
    }

    async fn delete_message(
        &self,
        conversation: &ConversationId,
        message_id: &str,
    ) -> Result<(), ChannelError> {
        let channel_id = self.target(conversation).await?;
        let message_id = self.parse_message(message_id)?;
        self.http
            .delete_message(channel_id, message_id)
            .await
            .map_err(|error| self.delivery_error("deleting the message", &error))?;
        Ok(())
    }

    async fn moderate_member(
        &self,
        conversation: &ConversationId,
        user_id: &str,
        action: MemberAction,
        until: Option<DateTime<Utc>>,
        revoke_messages: bool,
    ) -> Result<(), ChannelError> {
        let channel_id = self.target(conversation).await?;
        let guild_id = self.guild_of(conversation, channel_id).await?;
        let user = self.parse_user(user_id)?;

        match action {
            MemberAction::Restrict => {
                // Discord timeouts are always bounded, so there is no "until further notice" to
                // fall back on. Refusing beats silently choosing a length nobody asked for.
                let until = until.ok_or_else(|| ChannelError::Delivery {
                    channel: self.id.as_str().to_string(),
                    message:
                        "Discord restrictions always expire, so this one needs a duration, up \
                              to 28 days"
                            .to_string(),
                })?;
                if until.signed_duration_since(Utc::now()) > MAX_TIMEOUT {
                    return Err(ChannelError::Delivery {
                        channel: self.id.as_str().to_string(),
                        message: "Discord restrictions last at most 28 days; for longer, ban them"
                            .to_string(),
                    });
                }
                let timestamp = Timestamp::from_secs(until.timestamp()).map_err(|error| {
                    ChannelError::Delivery {
                        channel: self.id.as_str().to_string(),
                        message: format!("that is not a time Discord accepts: {error}"),
                    }
                })?;
                self.http
                    .update_guild_member(guild_id, user)
                    .communication_disabled_until(Some(timestamp))
                    .await
                    .map_err(|error| self.delivery_error("restricting the member", &error))?;
            }
            MemberAction::Unrestrict => {
                // Simpler than Telegram, where lifting a restriction means reading the chat's own
                // defaults back so as not to hand out more than everyone else has. Discord's
                // timeout is a single field, and clearing it returns them to exactly their roles.
                self.http
                    .update_guild_member(guild_id, user)
                    .communication_disabled_until(None)
                    .await
                    .map_err(|error| self.delivery_error("lifting the restriction", &error))?;
            }
            MemberAction::Ban => {
                if until.is_some() {
                    return Err(ChannelError::Delivery {
                        channel: self.id.as_str().to_string(),
                        message:
                            "Discord bans never expire, so a duration cannot be honoured; ban \
                                  without one, or restrict them for a while instead"
                                .to_string(),
                    });
                }
                let mut request = self.http.create_ban(guild_id, user);
                if revoke_messages {
                    // Telegram deletes all of their history; Discord's ceiling is seven days, so
                    // this is as close as the same request gets.
                    request = request.delete_message_seconds(MAX_BAN_DELETE_SECONDS);
                }
                request
                    .await
                    .map_err(|error| self.delivery_error("banning the member", &error))?;
            }
            MemberAction::Unban => {
                self.http
                    .delete_ban(guild_id, user)
                    .await
                    .map_err(|error| self.delivery_error("lifting the ban", &error))?;
            }
            MemberAction::Kick => {
                self.http
                    .remove_guild_member(guild_id, user)
                    .await
                    .map_err(|error| self.delivery_error("removing the member", &error))?;
            }
        }
        Ok(())
    }

    async fn set_member_roles(
        &self,
        conversation: &ConversationId,
        user_id: &str,
        roles: &[String],
    ) -> Result<(), ChannelError> {
        let channel_id = self.target(conversation).await?;
        let guild_id = self.guild_of(conversation, channel_id).await?;
        let user = self.parse_user(user_id)?;

        let mut resolved = Vec::with_capacity(roles.len());
        for name in roles {
            let role = self.names.role_by_name(guild_id, name).ok_or_else(|| {
                let catalogue = self.names.role_catalogue(guild_id);
                ChannelError::Delivery {
                    channel: self.id.as_str().to_string(),
                    message: if catalogue.is_empty() {
                        format!("this server has no role called {name:?}")
                    } else {
                        format!(
                            "this server has no role called {name:?}; it has {}",
                            catalogue.join(", ")
                        )
                    },
                }
            })?;
            resolved.push(role);
        }
        self.http
            .update_guild_member(guild_id, user)
            .roles(&resolved)
            .await
            .map_err(|error| self.delivery_error("changing the member's roles", &error))?;
        Ok(())
    }

    async fn pin_message(
        &self,
        conversation: &ConversationId,
        message_id: &str,
        pin: bool,
        silent: bool,
    ) -> Result<(), ChannelError> {
        if silent {
            // Discord always announces a pin in the channel and offers no way not to. Saying so
            // beats accepting the argument and quietly ignoring it.
            return Err(ChannelError::Unsupported {
                channel: self.id.as_str().to_string(),
                feature: "pinning without announcing it",
            });
        }
        let channel_id = self.target(conversation).await?;
        let message_id = self.parse_message(message_id)?;
        if pin {
            self.http
                .create_pin(channel_id, message_id)
                .await
                .map_err(|error| self.delivery_error("pinning the message", &error))?;
        } else {
            self.http
                .delete_pin(channel_id, message_id)
                .await
                .map_err(|error| self.delivery_error("unpinning the message", &error))?;
        }
        Ok(())
    }

    async fn set_chat(
        &self,
        conversation: &ConversationId,
        settings: &ChatSettings,
    ) -> Result<(), ChannelError> {
        let channel_id = self.target(conversation).await?;
        let mut request = self.http.update_channel(channel_id);
        if let Some(title) = &settings.title {
            request = request.name(title);
        }
        if let Some(description) = &settings.description {
            request = request.topic(description);
        }
        if let Some(slowmode) = settings.slowmode {
            let seconds = u16::try_from(slowmode.as_secs()).unwrap_or(u16::MAX);
            if seconds > MAX_SLOWMODE_SECONDS {
                return Err(ChannelError::Delivery {
                    channel: self.id.as_str().to_string(),
                    message: "Discord's slowmode tops out at 6 hours".to_string(),
                });
            }
            request = request.rate_limit_per_user(seconds);
        }
        request
            .await
            .map_err(|error| self.delivery_error("changing the channel settings", &error))?;
        Ok(())
    }

    async fn member(
        &self,
        conversation: &ConversationId,
        user_id: Option<&str>,
    ) -> Result<MemberInfo, ChannelError> {
        let channel_id = self.target(conversation).await?;
        let guild_id = self.guild_of(conversation, channel_id).await?;
        let user = match user_id {
            Some(raw) => self.parse_user(raw)?,
            // Asked before the gateway loop has primed it, which the tool can be in the moments
            // after startup. Worth one REST call rather than an error the agent can do nothing
            // about, since this is the case that exists to answer "what am I allowed to do here".
            None => match self.identity.get().copied() {
                Some(me) => me,
                None => {
                    let identity = self.probe().await?;
                    identity
                        .id
                        .parse::<u64>()
                        .ok()
                        .and_then(Id::new_checked)
                        .ok_or_else(|| ChannelError::Auth {
                            channel: self.id.as_str().to_string(),
                            message: "Discord returned a user id that is not a snowflake"
                                .to_string(),
                        })?
                }
            },
        };

        let member = match self.http.guild_member(guild_id, user).await {
            Ok(response) => response
                .model()
                .await
                .map_err(|error| self.decode_error("the member", &error))?,
            Err(error) => {
                tracing::debug!(channel = %self.id, "member lookup failed: {}", error);
                // Not in the server. Whether they were thrown out or simply left is a different
                // question, and one call answers it, which beats reporting "not found" for both.
                let banned = self.http.ban(guild_id, user).await.is_ok();
                return Ok(MemberInfo {
                    user_id: user.to_string(),
                    display_name: None,
                    status: if banned {
                        MemberStatus::Banned
                    } else {
                        MemberStatus::Left
                    },
                    rights: Vec::new(),
                    roles: Vec::new(),
                    restricted_until: None,
                    // Not in the server, so there is nothing to be present for.
                    presence: None,
                });
            }
        };

        let permissions = self
            .permissions_in(guild_id, channel_id, user, &member.roles)
            .await?;
        let guild = self
            .http
            .guild(guild_id)
            .await
            .map_err(|error| self.delivery_error("reading the server", &error))?
            .model()
            .await
            .map_err(|error| self.decode_error("the server", &error))?;
        let restricted_until = member
            .communication_disabled_until
            .and_then(timestamp_to_chrono)
            .filter(|until| *until > Utc::now());

        let status = if guild.owner_id == user {
            MemberStatus::Owner
        } else if permissions.contains(Permissions::ADMINISTRATOR) {
            MemberStatus::Administrator
        } else if restricted_until.is_some() {
            MemberStatus::Restricted
        } else {
            MemberStatus::Member
        };

        Ok(MemberInfo {
            user_id: user.to_string(),
            display_name: member
                .nick
                .clone()
                .or_else(|| member.user.global_name.clone())
                .or(Some(member.user.name.clone())),
            status,
            // Discord has no per-member rights, only roles, so this stays empty and `roles` carries
            // the answer.
            rights: Vec::new(),
            roles: self.names.role_names(guild_id, &member.roles),
            restricted_until,
            presence: self.presence_of(guild_id, user),
        })
    }

    /// List a server's members, or search them by name.
    ///
    /// The two halves have very different requirements, which is the whole reason both are offered.
    /// Enumerating everyone needs the `GUILD_MEMBERS` privileged intent enabled in the Developer
    /// Portal; searching by name does not. So a bot whose operator has not enabled it can still
    /// answer "is there someone called Dana here", just not "who is here".
    ///
    /// The intent is checked at the application level, independently of what the gateway identified
    /// with, so this needs no change to the intents this bot connects with and cannot cause a
    /// `4014`. Discord answers with a plain 403 instead, which is turned into a message saying
    /// which switch to flip.
    async fn list_members(
        &self,
        conversation: &ConversationId,
        query: Option<&str>,
        limit: usize,
        after: Option<&str>,
    ) -> Result<MemberListing, ChannelError> {
        let channel_id = self.target(conversation).await?;
        // A direct message belongs to no server, so there is no roster to ask for. Caught here
        // because `guild_of` would otherwise report it as a malformed conversation, which sends
        // whoever reads the error looking for a typo that is not there.
        if chat_kind(self.names.kind_of(channel_id)) == ChatKind::Direct {
            return Err(ChannelError::Unsupported {
                channel: self.id.as_str().to_string(),
                feature: "listing the members of a direct message, which is just the two of you",
            });
        }
        let guild_id = self.guild_of(conversation, channel_id).await?;
        let limit = limit.clamp(1, MAX_MEMBER_PAGE) as u16;

        if let Some(query) = query {
            let members = self
                .http
                .search_guild_members(guild_id, query)
                .limit(limit)
                .await
                .map_err(|error| self.delivery_error("searching this server's members", &error))?
                .models()
                .await
                .map_err(|error| self.decode_error("the member search results", &error))?;
            let standing = self.server_standing(guild_id).await?;
            return Ok(MemberListing {
                coverage: MemberCoverage::Matching,
                members: members
                    .iter()
                    .map(|member| self.summarise_member(guild_id, member, &standing))
                    .collect(),
                // Deliberately absent: the server's size reported against a search reads as the
                // number of matches, which is a different and much smaller number.
                total: None,
                next_after: None,
            });
        }

        let mut request = self.http.guild_members(guild_id).limit(limit);
        if let Some(after) = after {
            request = request.after(self.parse_user(after)?);
        }
        let members = match request.await {
            Ok(response) => response
                .models()
                .await
                .map_err(|error| self.decode_error("the member list", &error))?,
            Err(error) if is_forbidden(&error) => {
                return Err(ChannelError::Unsupported {
                    channel: self.id.as_str().to_string(),
                    feature: "listing everyone in a Discord server without the server members \
                              intent, which is switched on under Privileged Gateway Intents on the \
                              bot's page in the Discord Developer Portal. Searching members by \
                              name works without it",
                });
            }
            Err(error) => {
                return Err(self.delivery_error("listing this server's members", &error));
            }
        };

        let standing = self.server_standing(guild_id).await?;
        // Discord pages by "the highest user id seen so far", and a short page means the end.
        let next_after = (members.len() == limit as usize)
            .then(|| members.iter().map(|member| member.user.id.get()).max())
            .flatten()
            .map(|highest| highest.to_string());

        Ok(MemberListing {
            coverage: MemberCoverage::Everyone,
            members: members
                .iter()
                .map(|member| self.summarise_member(guild_id, member, &standing))
                .collect(),
            total: standing.total,
            next_after,
        })
    }

    async fn canonical_conversation(
        &self,
        conversation: &ConversationId,
    ) -> Result<ConversationId, ChannelError> {
        if !conversation.chat().starts_with('@') {
            return Ok(conversation.clone());
        }
        // Resolving opens the direct-message channel, which `target` has already done or cached by
        // the time this is called.
        let channel_id = self.target(conversation).await?;
        Ok(ConversationId::new(&self.id, &channel_id.to_string(), None))
    }

    async fn search_messages(
        &self,
        conversation: &ConversationId,
        query: &str,
        limit: usize,
    ) -> Result<Vec<FoundMessage>, ChannelError> {
        if !self.message_content {
            return Err(ChannelError::Unsupported {
                channel: self.id.as_str().to_string(),
                feature: "searching Discord's own history without the message content intent",
            });
        }
        let channel_id = self.target(conversation).await?;
        // Discord's search is scoped to a server, and a direct message belongs to none, so there is
        // nothing to ask. The bridge's own record still covers it. Resolved through the same helper
        // the moderation calls use, so a channel the cache has not heard of is looked up rather
        // than mistaken for a direct message.
        let guild_id = self.guild_of(conversation, channel_id).await?;
        self.search_guild(guild_id, channel_id, query, limit).await
    }

    async fn set_activity(
        &self,
        conversation: &ConversationId,
        activity: Activity,
    ) -> Result<(), ChannelError> {
        // Discord has one indicator and no separate upload state, so every activity is the same
        // call. It expires after ten seconds on its own.
        let _ = activity;
        let channel_id = self.target(conversation).await?;
        self.http
            .create_typing_trigger(channel_id)
            .await
            .map_err(|error| self.delivery_error("showing the typing indicator", &error))?;
        Ok(())
    }

    async fn probe(&self) -> Result<ChannelIdentity, ChannelError> {
        let user = self
            .http
            .current_user()
            .await
            .map_err(|error| ChannelError::Auth {
                channel: self.id.as_str().to_string(),
                message: format!("Discord rejected the bot token: {error}"),
            })?
            .model()
            .await
            .map_err(|error| self.decode_error("the bot's own account", &error))?;
        Ok(ChannelIdentity {
            id: user.id.to_string(),
            display_name: user
                .global_name
                .clone()
                .unwrap_or_else(|| user.name.clone()),
            username: Some(user.name.clone()),
            // Whether Discord actually granted the intent is only knowable from the gateway, which
            // closes with a 4014 rather than degrading. So this reports what was asked for, and a
            // refusal surfaces as a startup error instead.
            reads_all_group_messages: self.message_content,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::secret::Secret;

    #[tokio::test]
    async fn more_attachments_than_discord_takes_are_refused_before_any_read() {
        // twilight does not validate the count, so without this check Discord answers a generic
        // rejection only after every file has been read into memory and uploaded.
        let channel = channel_with(vec![1], vec![], vec![], vec![], false);
        let conversation = ConversationId::parse("discord:1").expect("valid");
        let paths: Vec<PathBuf> = (0..=MAX_ATTACHMENTS)
            .map(|index| PathBuf::from(format!("/tmp/{index}.png")))
            .collect();
        let error = channel
            .send_files(&conversation, &paths, None, &FileOptions::default())
            .await
            .expect_err("an over-long attachment list must be refused");
        let message = error.to_string();
        assert!(message.contains("10"), "{message}");
    }

    #[test]
    fn an_edit_states_the_preview_choice_in_both_directions() {
        // Discord keeps a message's flags across an edit, so an edit that leaves them alone cannot
        // lift a suppression the original send applied. Reverting `edit_flags` to "set only when
        // suppressing" would compile and pass everything else while silently ignoring an agent
        // that asked for a card on a message it had sent without one.
        assert_eq!(
            DiscordChannel::edit_flags(false),
            MessageFlags::SUPPRESS_EMBEDS,
            "an edit without a preview must say so rather than leave the flag alone"
        );
        assert_eq!(
            DiscordChannel::edit_flags(true),
            MessageFlags::empty(),
            "an edit asking for a preview must clear the suppression, not skip the field"
        );
    }

    #[test]
    fn a_send_only_states_suppression_when_it_wants_it() {
        // The opposite convention, and deliberate: a create has no prior flags to lift, so the
        // absent case is already "no suppression" and writing an empty set would be noise.
        assert_eq!(
            DiscordChannel::outbound_flags(false),
            Some(MessageFlags::SUPPRESS_EMBEDS)
        );
        assert_eq!(DiscordChannel::outbound_flags(true), None);
    }

    fn channel_with(
        allowed_users: Vec<u64>,
        allowed_guilds: Vec<u64>,
        allowed_channels: Vec<u64>,
        allowed_roles: Vec<u64>,
        allow_all: bool,
    ) -> DiscordChannel {
        let config = DiscordConfig {
            token: Secret::new("fake.token", "test"),
            allowed_users,
            allowed_guilds,
            allowed_channels,
            allowed_roles,
            allow_all,
            admin_tools: true,
            message_content: true,
            presence: false,
            mention_everyone: false,
            mention_roles: false,
        };
        DiscordChannel::new(ChannelId::new("discord"), &config).expect("constructs")
    }

    /// A channel that knows who it is, which is what `addressed` needs.
    fn identified() -> DiscordChannel {
        // Granted the server as well as the person: the user list no longer admits anybody in a
        // server channel, and these fixtures are server messages.
        let channel = channel_with(vec![1], vec![900], vec![], vec![], false);
        channel
            .identity
            .set(Id::new(99))
            .expect("the identity is unset");
        channel
    }

    fn message(overrides: serde_json::Value) -> Message {
        let mut value = serde_json::json!({
            "id": "1000",
            "channel_id": "2000",
            "guild_id": "900",
            "author": {
                "id": "1",
                "username": "alice",
                "discriminator": "0",
                "avatar": null,
                "bot": false,
            },
            "content": "hello",
            "timestamp": "2026-08-12T14:03:11.427000+00:00",
            "edited_timestamp": null,
            "tts": false,
            "mention_everyone": false,
            "mentions": [],
            "mention_roles": [],
            "attachments": [],
            "embeds": [],
            "pinned": false,
            "type": 0,
        });
        let serde_json::Value::Object(overrides) = overrides else {
            panic!("overrides must be an object");
        };
        let serde_json::Value::Object(base) = &mut value else {
            unreachable!("the literal above is an object");
        };
        base.extend(overrides);
        serde_json::from_value(value).expect("the fixture deserializes")
    }

    #[tokio::test]
    async fn an_allowlisted_person_is_admitted_as_themselves_in_a_direct_message() {
        let channel = channel_with(vec![1], vec![900], vec![], vec![], false);
        assert_eq!(
            channel.admission(Id::new(1), Id::new(2000), None, &[], true),
            Some(Admission::User)
        );
    }

    #[tokio::test]
    async fn an_allowlisted_person_is_not_thereby_admitted_in_a_server() {
        // `allowed_users` says who may message the bot, not which rooms it listens in. Anyone who
        // shares a server with a bot can open a DM with it, so the individual grant is how somebody
        // reaches it privately; letting that same grant carry into every channel of every server it
        // can see would make one entry far wider than it reads.
        let channel = channel_with(vec![1], vec![], vec![], vec![], false);
        assert_eq!(
            channel.admission(Id::new(1), Id::new(2000), Some(Id::new(900)), &[], false),
            None,
            "an individual grant must not reach into an unlisted server"
        );
    }

    #[tokio::test]
    async fn a_listed_person_in_a_listed_server_is_admitted_by_the_server() {
        // Both name them, but only the server grant reaches a server channel, so that is what the
        // agent is told. `user allowlist` here would claim more was checked about this room than
        // was.
        let channel = channel_with(vec![1], vec![900], vec![], vec![], false);
        assert_eq!(
            channel.admission(Id::new(1), Id::new(2000), Some(Id::new(900)), &[], false),
            Some(Admission::Server)
        );
    }

    #[tokio::test]
    async fn a_server_grant_is_reported_as_a_server_grant() {
        let channel = channel_with(vec![], vec![900], vec![], vec![], false);
        assert_eq!(
            channel.admission(Id::new(7), Id::new(2000), Some(Id::new(900)), &[], false),
            Some(Admission::Server)
        );
    }

    #[tokio::test]
    async fn a_role_holder_is_admitted_but_not_as_a_vetted_account() {
        // Reporting this as `User` would tell the agent the account was looked at, when all that
        // was checked is a role anybody who administers the server can hand out.
        let channel = channel_with(vec![], vec![], vec![], vec![555], false);
        let admission = channel.admission(
            Id::new(7),
            Id::new(2000),
            Some(Id::new(900)),
            &[Id::new(555)],
            false,
        );
        assert_eq!(admission, Some(Admission::Role));
        assert!(
            Admission::Role.describe().contains("role you allow"),
            "the grant has to name what was actually checked"
        );
    }

    #[tokio::test]
    async fn a_group_dm_is_not_a_direct_message() {
        // A group DM carries no server id, so a rule written as "no guild" admits it. Up to ten
        // people can be in one, and the envelope would report `user allowlist` for a room the
        // operator never named and the docs promise is a one-to-one chat.
        let channel = channel_with(vec![1], vec![], vec![], vec![], false);
        assert_eq!(
            channel.admission(Id::new(1), Id::new(2000), None, &[], false),
            None,
            "a group DM must not ride in on the individual grant"
        );
        assert_eq!(
            channel.admission(Id::new(1), Id::new(2000), None, &[], true),
            Some(Admission::User),
            "a real direct message still works"
        );
    }

    #[tokio::test]
    async fn a_stranger_in_an_unlisted_server_is_dropped() {
        let channel = channel_with(vec![1], vec![], vec![], vec![], false);
        assert_eq!(
            channel.admission(Id::new(7), Id::new(2000), Some(Id::new(900)), &[], false),
            None
        );
    }

    #[tokio::test]
    async fn a_direct_message_from_a_stranger_is_dropped_too() {
        // Anyone sharing a server with the bot can open a DM, so this is not a small set.
        let channel = channel_with(vec![1], vec![900], vec![], vec![], false);
        assert_eq!(
            channel.admission(Id::new(7), Id::new(3000), None, &[], true),
            None
        );
    }

    #[tokio::test]
    async fn allow_all_admits_a_stranger_as_unvetted() {
        let channel = channel_with(vec![], vec![], vec![], vec![], true);
        assert_eq!(
            channel.admission(Id::new(7), Id::new(2000), Some(Id::new(900)), &[], false),
            Some(Admission::Open)
        );
    }

    #[tokio::test]
    async fn being_named_wakes_the_agent() {
        let channel = identified();
        let message = message(serde_json::json!({
            "mentions": [{
                "id": "99",
                "username": "mekabot",
                "discriminator": "0",
                "avatar": null,
                "bot": true,
                "public_flags": 0,
            }],
        }));
        assert!(channel.addressed(&message, Some(ChannelType::GuildText)));
    }

    #[tokio::test]
    async fn a_reply_to_the_agent_wakes_it_without_a_mention() {
        let channel = identified();
        let message = message(serde_json::json!({
            "type": 19,
            "referenced_message": {
                "id": "999",
                "channel_id": "2000",
                "author": {
                    "id": "99",
                    "username": "mekabot",
                    "discriminator": "0",
                    "avatar": null,
                    "bot": true,
                },
                "content": "earlier",
                "timestamp": "2026-08-12T14:00:00.000000+00:00",
                "edited_timestamp": null,
                "tts": false,
                "mention_everyone": false,
                "mentions": [],
                "mention_roles": [],
                "attachments": [],
                "embeds": [],
                "pinned": false,
                "type": 0,
            },
        }));
        assert!(channel.addressed(&message, Some(ChannelType::GuildText)));
    }

    #[tokio::test]
    async fn a_broadcast_does_not_wake_the_agent() {
        // Otherwise one @everyone in a large server is the cheapest way anybody can force a turn.
        let channel = identified();
        let message = message(serde_json::json!({
            "mention_everyone": true,
            "mention_roles": ["555"],
        }));
        assert!(!channel.addressed(&message, Some(ChannelType::GuildText)));
    }

    #[tokio::test]
    async fn ordinary_chatter_does_not_wake_the_agent() {
        let channel = identified();
        assert!(!channel.addressed(
            &message(serde_json::json!({})),
            Some(ChannelType::GuildText)
        ));
    }

    #[tokio::test]
    async fn a_direct_message_is_always_addressed() {
        let channel = identified();
        let message = message(serde_json::json!({"guild_id": null}));
        assert!(channel.addressed(&message, Some(ChannelType::Private)));
    }

    /// A `TYPING_START` as the gateway delivers it.
    fn typing_start(user_id: u64) -> Event {
        Event::TypingStart(Box::new(
            twilight_model::gateway::payload::incoming::TypingStart {
                channel_id: Id::new(555),
                guild_id: Some(Id::new(900)),
                member: None,
                timestamp: 1_760_000_000,
                user_id: Id::new(user_id),
            },
        ))
    }

    #[tokio::test]
    async fn somebody_typing_is_reported_so_the_chat_can_wait_for_them() {
        let channel = identified();
        let (sender, mut receiver) = mpsc::channel(4);
        channel.handle(typing_start(1), &sender).await;

        let event = receiver.try_recv().expect("a typing notice is reported");
        let InboundEvent::Typing { conversation, .. } = event else {
            panic!("expected a typing notice, got {event:?}");
        };
        assert_eq!(conversation.as_str(), "discord:555");
    }

    #[tokio::test]
    async fn the_bots_own_typing_is_not_reported_back_to_itself() {
        // The gateway echoes our own indicator like anybody else's, and the bridge raises one for
        // the whole of every turn. Unfiltered, a chat would be held open for as long as the agent
        // was working on it and then held again by the next turn, which from the outside looks
        // exactly like the bridge having stalled.
        let channel = identified();
        let (sender, mut receiver) = mpsc::channel(4);
        channel.handle(typing_start(99), &sender).await;
        assert!(
            receiver.try_recv().is_err(),
            "the bot's own typing must not hold a conversation open"
        );
    }

    #[test]
    fn typing_events_are_asked_for_from_the_gateway() {
        // The intent decides whether Discord sends these at all; this flag decides whether the
        // shard hands them to `handle`. Both are needed, and the handler tests call `handle`
        // directly, so without this assertion they would keep passing while the event never
        // arrived in production and every chat released on the floor alone.
        assert!(WANTED_EVENTS.contains(EventTypeFlags::TYPING_START));
    }

    #[tokio::test]
    async fn the_typing_intents_are_requested_and_are_not_privileged() {
        // Unprivileged, so unlike MESSAGE_CONTENT they need no portal toggle and cannot close the
        // gateway with a 4014. Asserted because getting this wrong fails at startup for every
        // deployment rather than degrading.
        let channel = channel_with(vec![1], vec![], vec![], vec![], false);
        let intents = channel.intents();
        assert!(intents.contains(Intents::GUILD_MESSAGE_TYPING));
        assert!(intents.contains(Intents::DIRECT_MESSAGE_TYPING));

        // The fixture asks for `MESSAGE_CONTENT` and nothing else privileged, so that is the whole
        // of what may appear here. Asserted as an equality rather than as an absence, or a future
        // privileged intent added alongside these would slip through.
        let privileged = Intents::GUILD_MEMBERS
            .union(Intents::GUILD_PRESENCES)
            .union(Intents::MESSAGE_CONTENT);
        assert_eq!(
            intents.intersection(privileged),
            Intents::MESSAGE_CONTENT,
            "the typing intents must not drag a privileged one along with them"
        );
    }

    #[tokio::test]
    async fn mentions_are_rewritten_into_names() {
        let channel = identified();
        let message = message(serde_json::json!({
            "content": "<@1> and <@!2> should look",
            "mentions": [
                {"id": "1", "username": "alice", "discriminator": "0", "avatar": null,
                 "bot": false, "public_flags": 0},
                {"id": "2", "username": "bob", "discriminator": "0", "avatar": null,
                 "bot": false, "public_flags": 0},
            ],
        }));
        assert_eq!(
            channel.demention(&message.content, &message.mentions, message.guild_id),
            "@alice and @bob should look"
        );
    }

    #[tokio::test]
    async fn an_unresolvable_mention_says_someone_rather_than_a_number() {
        let channel = identified();
        let message = message(serde_json::json!({"content": "ask <@404>"}));
        assert_eq!(
            channel.demention(&message.content, &message.mentions, message.guild_id),
            "ask @someone"
        );
    }

    #[tokio::test]
    async fn roles_and_channels_are_resolved_from_the_cache() {
        let channel = identified();
        let mut room: twilight_model::channel::Channel =
            serde_json::from_value(serde_json::json!({
                "id": "2000",
                "type": 0,
            }))
            .expect("a channel fixture deserializes");
        room.name = Some("deploys".to_string());
        room.guild_id = Some(Id::new(900));
        channel.names.insert_channel(&room);
        let role: twilight_model::guild::Role = serde_json::from_value(serde_json::json!({
            "id": "555",
            "name": "Release Team",
            "color": 0,
            "colors": {"primary_color": 0},
            "flags": 0,
            "hoist": false,
            "managed": false,
            "mentionable": true,
            "permissions": "0",
            "position": 1,
        }))
        .expect("a role fixture deserializes");
        channel.names.insert_role(Id::new(900), &role);

        let message = message(serde_json::json!({"content": "<@&555> see <#2000>"}));
        assert_eq!(
            channel.demention(&message.content, &message.mentions, message.guild_id),
            "@Release Team see #deploys"
        );
    }

    #[tokio::test]
    async fn a_custom_emoji_keeps_its_name_and_loses_its_id() {
        let channel = identified();
        let message = message(serde_json::json!({"content": "nice <:shrug:12345> work"}));
        assert_eq!(
            channel.demention(&message.content, &message.mentions, message.guild_id),
            "nice :shrug: work"
        );
    }

    #[tokio::test]
    async fn a_relative_timestamp_becomes_an_absolute_one() {
        // The client renders `<t:...:R>` as "in 3 hours", which the agent cannot evaluate.
        let channel = identified();
        let message = message(serde_json::json!({"content": "at <t:1760000000:R>"}));
        let rendered = channel.demention(&message.content, &message.mentions, message.guild_id);
        assert!(rendered.starts_with("at 2025-10-09T"), "got {rendered:?}");
    }

    #[tokio::test]
    async fn text_with_no_markup_is_left_exactly_alone() {
        let channel = identified();
        let message = message(serde_json::json!({"content": "plain text"}));
        assert_eq!(
            channel.demention(&message.content, &message.mentions, message.guild_id),
            "plain text"
        );
    }

    #[tokio::test]
    async fn a_real_search_response_is_parsed() {
        // Shaped from an actual response: `messages` is an array of arrays, a bot author has a null
        // `global_name`, and the timestamp carries six fractional digits.
        let body = serde_json::json!({
            "analytics_id": "d275be96",
            "doing_deep_historical_index": false,
            "total_results": 1,
            "messages": [[{
                "id": "3000",
                "channel_id": "2000",
                "type": 0,
                "content": "operations check: an attached file",
                "timestamp": "2026-08-12T04:34:57.115000+00:00",
                "edited_timestamp": null,
                "author": {
                    "id": "99",
                    "username": "mekabot",
                    "discriminator": "2982",
                    "global_name": null,
                    "bot": true,
                },
            }]],
        });
        let messages = body
            .get("messages")
            .and_then(serde_json::Value::as_array)
            .expect("the response has a messages array");
        let found: Vec<_> = messages
            .iter()
            .filter_map(|group| match group {
                serde_json::Value::Array(inner) => inner.first(),
                other => Some(other),
            })
            .filter_map(found_message)
            .collect();
        assert_eq!(found.len(), 1, "the real response shape must parse");
        assert_eq!(found[0].sender_name, "mekabot");
        assert_eq!(found[0].text, "operations check: an attached file");
    }

    #[tokio::test]
    async fn the_search_path_is_a_legal_uri() {
        // This was built from a `\\`-continued literal once, and rustfmt joined the lines while
        // keeping the indentation, putting a run of spaces inside the URL. A space is not a legal
        // URI character, so every search failed to build a request and the whole Discord search leg
        // was silently dead. Assert on the shape rather than trusting the formatter.
        let channel = identified();
        let path = channel.search_path(Id::new(900), Id::new(2000), "deploy find me", 25);
        assert!(
            !path.contains(' '),
            "a space in the path makes the request unbuildable: {path:?}"
        );
        assert!(
            path.starts_with("guilds/900/messages/search?"),
            "got {path:?}"
        );
        assert!(path.contains("channel_id=2000"), "got {path:?}");
        assert!(path.contains("limit=25"), "got {path:?}");
        // The query is percent-encoded rather than pasted in.
        assert!(path.contains("content=deploy+find+me"), "got {path:?}");
    }

    #[tokio::test]
    async fn demention_survives_arbitrary_unicode() {
        // Message content is whatever anybody typed, and this routine indexes into it by byte.
        // Every cut lands next to an ASCII `<` or `>`, but that is worth proving rather
        // than reasoning about, because the failure mode is a panic in the gateway loop on
        // somebody else's text.
        let channel = identified();
        for content in [
            "日本語 <@1> テキスト",
            "\u{1f600}\u{1f601}<#2000>\u{1f602}",
            "café <@&555> naïve",
            "\u{1f1ef}\u{1f1f5}<:shrug:1>\u{200d}",
            "a\u{0301}<@!404>e\u{0301}",
            "<\u{1f600}>",
            "\u{1f600}<",
            "<",
            ">",
            "<>",
            "\u{1f600}>\u{1f600}<\u{1f600}",
        ] {
            let message = message(serde_json::json!({"content": content}));
            // The assertion is that it returns at all: a panic here would take the connector down.
            let rendered = channel.demention(&message.content, &message.mentions, message.guild_id);
            assert!(rendered.is_char_boundary(0) || rendered.is_empty());
        }
    }

    #[tokio::test]
    async fn an_unterminated_bracket_does_not_swallow_the_rest() {
        let channel = identified();
        let message = message(serde_json::json!({"content": "a < b and c"}));
        assert_eq!(
            channel.demention(&message.content, &message.mentions, message.guild_id),
            "a < b and c"
        );
    }

    #[tokio::test]
    async fn the_agent_does_not_hear_its_own_messages() {
        let channel = identified();
        let message = message(serde_json::json!({
            "author": {
                "id": "99",
                "username": "mekabot",
                "discriminator": "0",
                "avatar": null,
                "bot": true,
            },
        }));
        assert!(
            channel
                .to_event(&message, Some(ChannelType::GuildText))
                .is_none()
        );
    }

    #[tokio::test]
    async fn a_join_notice_is_not_a_message_from_a_person() {
        let channel = identified();
        // Type 7 is USER_JOIN, which Discord authors on the member's behalf.
        let message = message(serde_json::json!({"type": 7}));
        assert!(
            channel
                .to_event(&message, Some(ChannelType::GuildText))
                .is_none()
        );
    }

    #[tokio::test]
    async fn an_edit_gets_an_id_of_its_own_so_it_is_not_taken_for_a_redelivery() {
        let channel = identified();
        let original = message(serde_json::json!({}));
        let edited = message(serde_json::json!({
            "edited_timestamp": "2026-08-12T14:05:00.000000+00:00",
        }));
        let Some(InboundEvent::Message(original)) =
            channel.to_event(&original, Some(ChannelType::GuildText))
        else {
            panic!("the original is a message");
        };
        let Some(InboundEvent::Message(edited)) =
            channel.to_event(&edited, Some(ChannelType::GuildText))
        else {
            panic!("the edit is a message");
        };
        assert_eq!(original.message_id, edited.message_id);
        assert_ne!(original.external_id, edited.external_id);
        assert!(edited.edited_at.is_some());
    }

    #[tokio::test]
    async fn a_direct_message_is_recognised_without_the_cache_knowing_the_channel() {
        // Discord sends no channel event for a DM, so the cache never learns about one and the
        // absent server id is the only signal there is.
        let channel = identified();
        let message = message(serde_json::json!({"guild_id": null}));
        let Some(InboundEvent::Message(event)) = channel.to_event(&message, None) else {
            panic!("a direct message is a message");
        };
        assert_eq!(event.chat_kind, ChatKind::Direct);
    }

    #[tokio::test]
    async fn a_server_channel_defaults_to_mentions_only_and_a_direct_message_does_not() {
        // The 0.3.0 attention policy keys off this, so the mapping is what decides whether a busy
        // server wakes the agent for everything.
        assert_eq!(chat_kind(Some(ChannelType::GuildText)), ChatKind::Group);
        assert_eq!(chat_kind(Some(ChannelType::PublicThread)), ChatKind::Group);
        assert_eq!(chat_kind(Some(ChannelType::GuildVoice)), ChatKind::Group);
        assert_eq!(
            chat_kind(Some(ChannelType::GuildAnnouncement)),
            ChatKind::Channel
        );
        assert_eq!(chat_kind(Some(ChannelType::Private)), ChatKind::Direct);
        assert_eq!(chat_kind(None), ChatKind::Unknown);
    }

    #[tokio::test]
    async fn attachments_are_classified_from_what_discord_says_they_are() {
        assert_eq!(
            attachment_kind(Some("image/gif"), false),
            AttachmentKind::Animation
        );
        assert_eq!(
            attachment_kind(Some("image/png"), false),
            AttachmentKind::Photo
        );
        assert_eq!(
            attachment_kind(Some("video/mp4"), false),
            AttachmentKind::Video
        );
        assert_eq!(
            attachment_kind(Some("audio/ogg"), true),
            AttachmentKind::Voice
        );
        assert_eq!(
            attachment_kind(Some("audio/ogg"), false),
            AttachmentKind::Audio
        );
        assert_eq!(attachment_kind(None, false), AttachmentKind::Document);
    }

    #[tokio::test]
    async fn an_attachment_handle_points_at_the_message_rather_than_the_signed_url() {
        // Discord's CDN links expire, so a stored URL would be a handle that works for a while.
        let channel = identified();
        let message = message(serde_json::json!({
            "attachments": [{
                "id": "3000",
                "filename": "crash.png",
                "content_type": "image/png",
                "size": 412,
                "url": "https://cdn.discordapp.com/attachments/2000/1000/crash.png?ex=deadbeef",
                "proxy_url": "https://media.discordapp.net/attachments/2000/1000/crash.png",
            }],
        }));
        let attachments = channel.attachments(&message);
        let [attachment] = attachments.as_slice() else {
            panic!("one attachment");
        };
        assert_eq!(attachment.file_ref, "2000/1000/3000");
        assert!(!attachment.file_ref.contains("ex="));
        assert_eq!(attachment.kind, AttachmentKind::Photo);
    }

    #[tokio::test]
    async fn a_dialling_address_is_a_valid_conversation_id() {
        // `discord:@<user>` is how the agent reaches somebody who has never written.
        let id = ConversationId::parse("discord:@245119312739729408").expect("parses");
        assert_eq!(id.chat(), "@245119312739729408");
    }

    #[tokio::test]
    async fn an_ordinary_conversation_id_is_already_its_own_final_form() {
        // Only a dialling address needs resolving, and resolving one costs a REST call, so an
        // ordinary id must not trigger it. This runs with no network, which proves it does not.
        let channel = identified();
        let id = ConversationId::parse("discord:1183429847290374144").expect("parses");
        assert_eq!(
            channel
                .canonical_conversation(&id)
                .await
                .expect("no lookup"),
            id
        );
    }
}
