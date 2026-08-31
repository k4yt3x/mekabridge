//! Telegram connector.
//!
//! Updates are consumed straight off teloxide's long-polling stream rather than through its
//! `Dispatcher`. The dispatcher exists to route updates to handlers, and this bridge has exactly
//! one destination for every update, so the dptree layer would only add indirection between the
//! socket and the queue.
//!
//! Outbound calls go through teloxide's `Throttle` adaptor. Telegram enforces roughly one message
//! per second per chat and bursts get 429s with a `retry_after`; the adaptor paces requests so a
//! multi-part reply does not lose its tail.

pub mod render;

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use futures::StreamExt;
use teloxide::{
    Bot,
    adaptors::{Throttle, throttle::Limits},
    net::Download,
    payloads::{
        BanChatMemberSetters as _, EditMessageTextSetters as _, PinChatMessageSetters as _,
        PromoteChatMemberSetters as _, RestrictChatMemberSetters as _, SendChatActionSetters as _,
        SendDocumentSetters as _, SendMediaGroupSetters as _, SendMessageSetters as _,
        SendPhotoSetters as _, SetChatDescriptionSetters as _, SetMessageReactionSetters as _,
        UnbanChatMemberSetters as _, UnpinChatMessageSetters as _,
    },
    prelude::Requester,
    types::{
        AllowedUpdate, ChatAction, ChatId, ChatPermissions, FileId, InputFile, InputMedia,
        InputMediaDocument, InputMediaPhoto, LinkPreviewOptions, MediaKind, Message,
        MessageEntityKind, MessageId, MessageKind, MessageOrigin, ParseMode, ReactionType,
        Recipient, ReplyParameters, ThreadId, UpdateKind, UserId,
    },
    update_listeners::{AsUpdateStream, Polling},
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    channel::{
        Activity, Admission, Attachment, AttachmentKind, Channel, ChannelCapabilities,
        ChannelError, ChannelId, ChannelIdentity, ChatKind, ChatSettings, ConversationId,
        ConversationInfo, FetchedFile, FileOptions, ForwardOrigin, InboundEvent, InboundMessage,
        MemberAction, MemberCoverage, MemberInfo, MemberListing, MemberRight, MemberStatus,
        Platform, ReplyContext, SendOptions, Sender, SentMessage,
    },
    config::{TelegramConfig, TelegramParseMode},
};

/// Most files Telegram will take in one album.
///
/// The Bot API's own ceiling on `sendMediaGroup`. Enforced here rather than left to the API so an
/// over-long list costs no upload, and so the agent is told the number rather than reading a
/// generic rejection.
const MAX_ALBUM_ITEMS: usize = 10;

/// Longest excerpt kept from a replied-to message, enough for the agent to know what is being
/// referenced without pasting an entire prior message into the turn.
const REPLY_EXCERPT_CHARS: usize = 160;

/// How much longer the HTTP client waits than the long poll it is carrying.
///
/// `getUpdates` holds the connection open until an update arrives or `poll_timeout` elapses, so the
/// client's own timeout has to outlast it. teloxide's default client stops at 17 seconds and does
/// not adjust for the poll timeout, the code that would being commented out behind a FIXME in
/// `teloxide-core`, so any `poll_timeout` above that aborts client-side on every quiet poll and
/// surfaces as a reconnect several times a minute on an idle bot.
///
/// The margin covers writing the empty response back over a slow link, not a retry.
const POLL_RESPONSE_MARGIN: std::time::Duration = std::time::Duration::from_secs(15);

/// Who the bot is, as far as deciding whether a message was aimed at it.
///
/// Resolved once at startup rather than per message. Telegram identifies a bot in message text by
/// username and offers no id form there, so renaming the bot means restarting the bridge; the
/// alternative is a `getMe` on every inbound message.
#[derive(Debug, Clone)]
struct BotIdentity {
    id: UserId,
    username: Option<String>,
}

pub struct TelegramChannel {
    id: ChannelId,
    bot: Throttle<Bot>,
    /// The unthrottled client, kept because the `Download` trait is implemented on `Bot` rather
    /// than on the throttling adaptor. Downloads do not contend with the send rate limits
    /// anyway.
    downloader: Bot,
    allowed_users: Vec<i64>,
    allowed_chats: Vec<i64>,
    allow_all: bool,
    admin_tools: bool,
    parse_mode: TelegramParseMode,
    poll_timeout: std::time::Duration,
    /// Filled by [`Channel::run`] before the first update is read.
    identity: tokio::sync::OnceCell<BotIdentity>,
}

impl TelegramChannel {
    pub fn new(id: ChannelId, config: &TelegramConfig) -> Result<Self, ChannelError> {
        // Built rather than taken from `Bot::new`, whose client would abort a long poll before
        // Telegram answers it. See [`POLL_RESPONSE_MARGIN`].
        let client = teloxide::net::default_reqwest_settings()
            .timeout(config.poll_timeout + POLL_RESPONSE_MARGIN)
            .build()
            .map_err(|error| ChannelError::Setup {
                channel: id.as_str().to_string(),
                message: format!("could not build the Telegram HTTP client: {error}"),
            })?;
        let bot = Bot::with_client(config.token.expose(), client);
        Ok(Self {
            id,
            bot: Throttle::new_spawn(bot.clone(), Limits::default()),
            downloader: bot,
            allowed_users: config.allowed_users.clone(),
            allowed_chats: config.allowed_chats.clone(),
            allow_all: config.allow_all,
            admin_tools: config.admin_tools,
            parse_mode: config.parse_mode,
            poll_timeout: config.poll_timeout,
            identity: tokio::sync::OnceCell::new(),
        })
    }

    /// Whether this message was aimed at the bot rather than merely said in front of it.
    ///
    /// Every signal here is one Telegram produced. Most compare user ids; only a plain `mention`
    /// entity and a targeted command come down to a username, because that is the sole identifier
    /// Telegram uses for a bot inside message text.
    ///
    /// Deliberately narrow: this is what wakes a conversation the agent is only half listening to,
    /// so counting the bot's name appearing as ordinary words would turn a mention-only chat back
    /// into every message.
    fn addressed(&self, message: &Message) -> bool {
        // In a one-to-one chat there is nobody else it could be for. Checked before the identity so
        // a direct message still reads as addressed even if `getMe` never succeeded.
        if message.chat.is_private() {
            return true;
        }
        let Some(me) = self.identity.get() else {
            tracing::warn!(
                channel = %self.id,
                "the bot's own identity is unknown, so mentions cannot be recognised; \
                 treating this message as not addressed to it"
            );
            return false;
        };

        // Replying to something the agent said is addressing it, and needs no mention.
        if message
            .reply_to_message()
            .and_then(|replied| replied.from.as_ref())
            .is_some_and(|user| user.id == me.id)
        {
            return true;
        }
        // Somebody used this bot's inline mode to produce the message.
        if message.via_bot.as_ref().is_some_and(|via| via.id == me.id) {
            return true;
        }

        // A message carries text entities or caption entities, never both, so both are consulted
        // and the empty one costs nothing. `parse_entities` resolves Telegram's UTF-16
        // offsets, which is why the entity's own text is read from it rather than sliced
        // out of the body here.
        let entities = message
            .parse_entities()
            .into_iter()
            .chain(message.parse_caption_entities())
            .flatten();
        let matches_username = |candidate: &str| {
            me.username
                .as_deref()
                // Telegram usernames are case insensitive, and clients preserve whatever the sender
                // typed.
                .is_some_and(|username| candidate.eq_ignore_ascii_case(username))
        };
        for entity in entities {
            match entity.kind() {
                // Carries a whole `User`, so this one is an id comparison. Telegram uses it for
                // accounts with no username, and for the `tg://user?id=` markdown form.
                MessageEntityKind::TextMention { user } if user.id == me.id => return true,
                MessageEntityKind::Mention
                    if matches_username(entity.text().trim_start_matches('@')) =>
                {
                    return true;
                }
                // `/restart@this_bot`. A bare `/restart` is not counted: in a group with several
                // bots it is ambiguous, and Telegram's own privacy mode only forwards it on the
                // strength of who spoke last.
                MessageEntityKind::BotCommand
                    if entity
                        .text()
                        .split_once('@')
                        .is_some_and(|(_, target)| matches_username(target)) =>
                {
                    return true;
                }
                _ => {}
            }
        }
        false
    }

    /// Whether a message may reach the agent, and on what basis.
    ///
    /// A bot token is a public entry point: anyone who learns the bot's name can message it. An
    /// update from outside the allowlist is dropped without a reply, because replying would confirm
    /// to a stranger that the bot is live.
    ///
    /// The lists are checked in descending order of how much they say about the sender, so somebody
    /// who qualifies under more than one is reported at the strongest. That ordering is why
    /// `allow_all` comes last rather than short-circuiting: an open channel can still name the
    /// people it knows, and "this one was vetted" stays worth saying.
    fn admission(&self, user_id: Option<i64>, chat_id: i64, direct: bool) -> Option<Admission> {
        // Direct messages only. `allowed_users` names people the bot should be reachable by, which
        // is not the same as a pass into every group it happens to have been added to: somebody
        // allowlisted so they can message the bot privately should not thereby be heard in a group
        // nobody named. Reaching them in a group is what `allowed_chats` is for.
        //
        // Told whether the chat is private rather than comparing the chat id against the user id.
        // The two are equal in a Telegram private chat, but relying on that would leave the rule
        // resting on a coincidence of the id space rather than on the thing being asked.
        if direct && user_id.is_some_and(|user_id| self.allowed_users.contains(&user_id)) {
            return Some(Admission::User);
        }
        if self.allowed_chats.contains(&chat_id) {
            return Some(Admission::Chat);
        }
        if self.allow_all {
            return Some(Admission::Open);
        }
        None
    }

    /// Log the bot being added to or removed from a chat.
    ///
    /// Not routed to the agent: it is an operational fact about the deployment, not a message from
    /// anybody. Being added to a chat that is not allowlisted is logged at warn, because from the
    /// outside it looks like the bot is simply ignoring everyone there, and an operator otherwise
    /// has no way to find out that it happened.
    fn note_membership_change(&self, update: &teloxide::types::ChatMemberUpdated) {
        let chat_id = update.chat.id;
        let title = update.chat.title().unwrap_or("a private chat");
        if update.new_chat_member.is_present() {
            if self.allowed_chats.contains(&chat_id.0) {
                tracing::info!(
                    channel = %self.id,
                    chat_id = chat_id.0,
                    "added to {:?}, which is allowlisted",
                    title
                );
            } else {
                tracing::warn!(
                    channel = %self.id,
                    chat_id = chat_id.0,
                    "added to {:?}, which is NOT allowlisted, so everything said there is ignored. \
                     Add {} to [[channels.telegram]].allowed_chats to let the agent see it.",
                    title,
                    chat_id.0
                );
            }
        } else {
            tracing::info!(
                channel = %self.id,
                chat_id = chat_id.0,
                "removed from {:?}",
                title
            );
        }
    }

    fn delivery_error(&self, error: &impl std::fmt::Display) -> ChannelError {
        ChannelError::Delivery {
            channel: self.id.as_str().to_string(),
            message: error.to_string(),
        }
    }

    /// Resolve a conversation id into the Telegram chat and optional forum topic it names.
    fn target(
        &self,
        conversation: &ConversationId,
    ) -> Result<(ChatId, Option<ThreadId>), ChannelError> {
        let chat =
            conversation
                .chat()
                .parse::<i64>()
                .map_err(|_| ChannelError::InvalidConversation {
                    id: conversation.as_str().to_string(),
                    reason: "the chat segment must be a Telegram chat id".to_string(),
                })?;
        let thread = conversation
            .thread()
            .map(|thread| {
                thread
                    .parse::<i32>()
                    .map(|raw| ThreadId(MessageId(raw)))
                    .map_err(|_| ChannelError::InvalidConversation {
                        id: conversation.as_str().to_string(),
                        reason: "the thread segment must be a Telegram topic id".to_string(),
                    })
            })
            .transpose()?;
        Ok((ChatId(chat), thread))
    }

    /// Parse a message id the agent supplied.
    fn message_id(&self, raw: &str) -> Result<MessageId, ChannelError> {
        raw.trim()
            .parse::<i32>()
            .map(MessageId)
            .map_err(|_| ChannelError::InvalidConversation {
                id: raw.to_string(),
                reason: "a Telegram message id is a number, from the `message:` line of a header"
                    .to_string(),
            })
    }

    /// Parse a user id the agent supplied.
    fn user_id(&self, raw: &str) -> Result<UserId, ChannelError> {
        raw.trim()
            .parse::<u64>()
            .map(UserId)
            .map_err(|_| ChannelError::InvalidConversation {
                id: raw.to_string(),
                reason: "a Telegram user id is a positive number, from the `from:` line of a \
                         message header. Anonymous admins and channel posts have no user id, so \
                         they cannot be moderated this way"
                    .to_string(),
            })
    }

    /// Whether Telegram should attach a preview card to a link in this message.
    ///
    /// Sent in both directions rather than omitted when a preview is wanted, which matters on
    /// `editMessageText`: an absent field leaves Telegram to decide, and "the default" and "what
    /// the message already had" are indistinguishable from here. An edit asking for a card on a
    /// message sent without one would then silently do nothing, which is the failure Discord's
    /// edit path sets flags unconditionally to avoid.
    ///
    /// Costs nothing on the send path: `is_disabled` is `skip_serializing_if`, so the enabled form
    /// goes out as the empty object Telegram already reads as its defaults.
    const fn link_preview(enabled: bool) -> LinkPreviewOptions {
        LinkPreviewOptions {
            is_disabled: !enabled,
            url: None,
            prefer_small_media: false,
            prefer_large_media: false,
            show_above_text: false,
        }
    }

    /// Shape the edit request, separated from awaiting it so the shaping can be asserted on.
    ///
    /// `JsonRequest` derefs to its payload, so a test can read back what would go on the wire
    /// without a bot token or a network. Worth extracting for one field because that field is the
    /// one whose absence is invisible: a preview the agent asked for and did not get looks
    /// identical to one it never asked for.
    fn edit_request(
        &self,
        chat: ChatId,
        message_id: MessageId,
        body: String,
        parse_mode: Option<ParseMode>,
        link_preview: bool,
    ) -> teloxide::requests::JsonRequest<teloxide::payloads::EditMessageText> {
        let mut request = self
            .bot
            .edit_message_text(Recipient::Id(chat), message_id, body);
        if let Some(parse_mode) = parse_mode {
            request = request.parse_mode(parse_mode);
        }
        request.link_preview_options(Self::link_preview(link_preview))
    }

    /// The parse mode a caption is rendered with, or `None` when the channel sends Markdown as-is.
    const fn caption_parse_mode(&self) -> Option<ParseMode> {
        match self.parse_mode {
            TelegramParseMode::Html => Some(ParseMode::Html),
            TelegramParseMode::None => None,
        }
    }

    /// Whether this many files go through `sendMediaGroup` rather than a single-file endpoint.
    ///
    /// The boundary is Telegram's, not a preference: `sendMediaGroup` requires **at least two**
    /// items, so one file through it is an API error and has to keep the endpoint built for one
    /// file. Named rather than written inline because the rule is easy to invert while reading and
    /// the cost of inverting it is a rejection the agent cannot act on.
    const fn groups_into_an_album(paths: &[PathBuf]) -> bool {
        paths.len() > 1
    }

    /// The items of an album, with the caption on exactly one of them.
    ///
    /// Exactly one, and this is the whole reason it is a function. The Bot API has **no** group
    /// caption: what renders under an album is emergent client behaviour when a single item carries
    /// one. Caption every item, which is the obvious reading of "the album's caption", and the
    /// official clients render *no* group caption at all, so the caption looks silently dropped
    /// while each file quietly keeps a copy. Index 0 is arbitrary but has to be some single index.
    ///
    /// One `as_photo` for the whole group is also load bearing: Telegram refuses an album that
    /// mixes documents with photos, and choosing per file could describe a group it will not take.
    fn album_items(
        paths: &[PathBuf],
        caption: Option<String>,
        as_photo: bool,
        parse_mode: Option<ParseMode>,
    ) -> Vec<InputMedia> {
        paths
            .iter()
            .enumerate()
            .map(|(index, path)| {
                let file = InputFile::file(path);
                // Only the first item is captioned; see above.
                let caption = (index == 0).then(|| caption.clone()).flatten();
                if as_photo {
                    let mut item = InputMediaPhoto::new(file);
                    if let Some(caption) = caption {
                        item = item.caption(caption);
                        if let Some(parse_mode) = parse_mode {
                            item = item.parse_mode(parse_mode);
                        }
                    }
                    InputMedia::Photo(item)
                } else {
                    let mut item = InputMediaDocument::new(file);
                    if let Some(caption) = caption {
                        item = item.caption(caption);
                        if let Some(parse_mode) = parse_mode {
                            item = item.parse_mode(parse_mode);
                        }
                    }
                    InputMedia::Document(item)
                }
            })
            .collect()
    }

    /// Shape the album request, separated from awaiting it so the shaping can be asserted on.
    ///
    /// `JsonRequest` derefs to its payload, so a test can read back what would go on the wire
    /// without a bot token or a network.
    fn album_request(
        &self,
        chat: ChatId,
        thread: Option<ThreadId>,
        paths: &[PathBuf],
        caption: Option<String>,
        reply_to: Option<MessageId>,
        options: &FileOptions,
    ) -> <Throttle<Bot> as Requester>::SendMediaGroup {
        let items = Self::album_items(paths, caption, options.as_photo, self.caption_parse_mode());
        let mut request = self.bot.send_media_group(Recipient::Id(chat), items);
        if let Some(thread) = thread {
            request = request.message_thread_id(thread);
        }
        if options.send.silent {
            request = request.disable_notification(true);
        }
        if let Some(reply_to) = reply_to {
            request = request.reply_parameters(ReplyParameters::new(reply_to));
        }
        request
    }

    /// Split agent Markdown into wire-ready message bodies.
    fn render(&self, markdown: &str, limit: usize) -> (Vec<String>, Option<ParseMode>) {
        match self.parse_mode {
            TelegramParseMode::Html => (render::to_html(markdown, limit), Some(ParseMode::Html)),
            TelegramParseMode::None => (crate::render::plain(markdown, limit), None),
        }
    }

    /// Convert one Telegram message into a bridge event, downloading any attachment it carries.
    async fn to_event(&self, message: &Message) -> Option<InboundEvent> {
        let chat_id = message.chat.id;
        let user = message.from.as_ref();
        let user_id = user.map(|user| user.id.0 as i64);
        // Recognition is independent of where they wrote, unlike admission.
        let sender_allowlisted =
            user_id.is_some_and(|user_id| self.allowed_users.contains(&user_id));
        let Some(admission) = self.admission(user_id, chat_id.0, message.chat.is_private()) else {
            tracing::debug!(
                channel = %self.id,
                chat_id = chat_id.0,
                user_id = ?user_id,
                "dropping a message from outside the allowlist"
            );
            return None;
        };

        let thread = message
            .thread_id
            .filter(|_| message.is_topic_message)
            .map(|thread| thread.0.0.to_string());
        let conversation = ConversationId::new(&self.id, &chat_id.0.to_string(), thread.as_deref());

        let chat_kind = if message.chat.is_private() {
            ChatKind::Direct
        } else if message.chat.is_channel() {
            ChatKind::Channel
        } else {
            ChatKind::Group
        };

        // A message with no `from` was posted as the chat itself: an anonymous group admin, or a
        // channel post auto-forwarded into its discussion group.
        let on_behalf_of_chat = user.is_none();
        let sender = Sender {
            id: user_id.map(|id| id.to_string()).unwrap_or_default(),
            display_name: user.map_or_else(
                || {
                    message
                        .sender_chat
                        .as_ref()
                        .and_then(|chat| chat.title())
                        .or_else(|| message.chat.title())
                        .unwrap_or("unknown sender")
                        .to_string()
                },
                display_name,
            ),
            username: user.and_then(|user| user.username.clone()),
            is_bot: user.is_some_and(|user| user.is_bot),
            on_behalf_of_chat,
        };

        let reply_to = message.reply_to_message().map(|replied| ReplyContext {
            message_id: replied.id.0.to_string(),
            sender_name: replied.from.as_ref().map(display_name),
            excerpt: replied
                .text()
                .or_else(|| replied.caption())
                .map(|text| truncate(text, REPLY_EXCERPT_CHARS)),
        });

        let text = message
            .text()
            .or_else(|| message.caption())
            .unwrap_or_default()
            .to_string();
        let (attachments, notes) = describe_content(message);
        if text.trim().is_empty() && attachments.is_empty() && notes.is_empty() {
            // Joins, pins, and other service messages carry nothing for the agent to act on.
            return None;
        }

        let edited_at = message.edit_date().copied();
        Some(InboundEvent::Message(Box::new(InboundMessage {
            channel: self.id.clone(),
            platform: Platform::Telegram,
            conversation,
            // An edit reuses the id of the message it revises, so keying the queue on the bare id
            // would let the dedupe constraint swallow it. The revision timestamp makes each edit a
            // distinct row while `message_id` still addresses the original for replies.
            external_id: match edited_at {
                Some(edited_at) => format!("{}:e{}", message.id.0, edited_at.timestamp()),
                None => message.id.0.to_string(),
            },
            message_id: message.id.0.to_string(),
            chat_kind,
            chat_title: message.chat.title().map(str::to_string),
            sender,
            admission,
            sender_allowlisted,
            addressed: self.addressed(message),
            sender_roles: Vec::new(),
            text,
            reply_to,
            edited_at,
            forwarded_from: message.forward_origin().map(forward_origin),
            group_id: message
                .media_group_id()
                .map(|group| group.0.clone().to_string()),
            notes,
            arrived_mid_turn: false,
            attachments,
            timestamp: message.date,
        })))
    }
}

#[async_trait]
impl Channel for TelegramChannel {
    fn id(&self) -> &ChannelId {
        &self.id
    }

    fn platform(&self) -> Platform {
        Platform::Telegram
    }

    fn capabilities(&self) -> ChannelCapabilities {
        ChannelCapabilities {
            typing_indicator: true,
            files: true,
            photos: true,
            reactions: true,
            edit: true,
            admin: self.admin_tools,
            // No presence of any kind in the Bot API, so this is not a switch an operator can flip.
            presence: false,
            // Nor any typing status. The Bot API lets a bot send a chat action and never receive
            // one: there is no update kind for it, so this is a platform limit rather than a
            // setting. A Telegram chat is therefore never held waiting for somebody to finish.
            typing_status: false,
            // Telegram grants privileges to a person directly, and has no roles to hand out.
            member_rights: self.admin_tools,
            member_roles: false,
        }
    }

    async fn run(
        self: Arc<Self>,
        sink: mpsc::Sender<InboundEvent>,
        shutdown: CancellationToken,
    ) -> Result<(), ChannelError> {
        // Resolved before the first update, because deciding whether a message mentions the bot
        // needs to know what the bot is called and that answer cannot be fetched per message. A
        // failure here is the same failure `probe` reports, so it is raised the same way rather
        // than left to surface later as a chat that never wakes.
        let me = self
            .bot
            .get_me()
            .await
            .map_err(|error| ChannelError::Auth {
                channel: self.id.as_str().to_string(),
                message: error.to_string(),
            })?;
        let _ = self.identity.set(BotIdentity {
            id: me.id,
            username: me.username.clone(),
        });

        // Named explicitly rather than left to teloxide's inference. Telegram withholds several
        // update kinds unless they are requested, and asking for only what is consumed keeps the
        // long poll from carrying traffic that would be discarded anyway.
        let mut listener = Polling::builder(self.bot.clone())
            .timeout(self.poll_timeout)
            .allowed_updates(vec![
                AllowedUpdate::Message,
                AllowedUpdate::EditedMessage,
                AllowedUpdate::MyChatMember,
            ])
            .build();
        let mut updates = Box::pin(listener.as_stream());
        tracing::info!(channel = %self.id, "telegram long polling started");

        loop {
            let update = tokio::select! {
                () = shutdown.cancelled() => {
                    tracing::info!(channel = %self.id, "telegram polling stopping");
                    return Ok(());
                }
                update = updates.next() => update,
            };
            let Some(update) = update else {
                return Err(ChannelError::Transport {
                    channel: self.id.as_str().to_string(),
                    message: "the update stream ended unexpectedly".to_string(),
                });
            };
            let update = match update {
                Ok(update) => update,
                Err(error) => {
                    // teloxide retries network failures internally, so anything surfacing here is
                    // worth logging but not worth tearing the channel down for.
                    tracing::warn!(channel = %self.id, "telegram update error: {}", error);
                    continue;
                }
            };
            let message = match &update.kind {
                UpdateKind::Message(message) | UpdateKind::EditedMessage(message) => message,
                UpdateKind::MyChatMember(update) => {
                    self.note_membership_change(update);
                    continue;
                }
                _ => continue,
            };
            if let Some(event) = self.to_event(message).await
                && sink.send(event).await.is_err()
            {
                tracing::info!(channel = %self.id, "bridge stopped accepting updates");
                return Ok(());
            }
        }
    }

    async fn send_text(
        &self,
        conversation: &ConversationId,
        markdown: &str,
        options: &SendOptions,
        sent: &mut Vec<SentMessage>,
    ) -> Result<(), ChannelError> {
        let (chat, thread) = self.target(conversation)?;
        let (bodies, parse_mode) = self.render(markdown, render::MESSAGE_LIMIT);
        if bodies.is_empty() {
            return Ok(());
        }

        let reply_to = options
            .reply_to
            .as_deref()
            .and_then(|raw| raw.parse::<i32>().ok())
            .map(MessageId);

        for (index, body) in bodies.iter().enumerate() {
            let mut request = self.bot.send_message(Recipient::Id(chat), body.clone());
            if let Some(parse_mode) = parse_mode {
                request = request.parse_mode(parse_mode);
            }
            if let Some(thread) = thread {
                request = request.message_thread_id(thread);
            }
            if options.silent {
                request = request.disable_notification(true);
            }
            // Per part, deliberately, unlike the reply quote below. A part is a whole message to
            // Telegram, so suppressing all but one would need to know which one holds the link the
            // agent meant; guessing wrong drops a card that was explicitly asked for, which is the
            // worse failure. The tool description says each part previews its own first link.
            request = request.link_preview_options(Self::link_preview(options.link_preview));
            // Only the first part quotes the message being replied to; repeating the quote on every
            // part of a long answer is noise.
            if index == 0
                && let Some(reply_to) = reply_to
            {
                request = request.reply_parameters(ReplyParameters::new(reply_to));
            }
            let message = request.await.map_err(|error| {
                // A partial send is worth surfacing precisely: the agent needs to know some of its
                // message did land, so it does not simply resend the whole thing.
                ChannelError::Delivery {
                    channel: self.id.as_str().to_string(),
                    message: format!("part {} of {} failed: {error}", index + 1, bodies.len()),
                }
            })?;
            // Pushed as each one lands rather than collected and returned, so a part refused
            // after an earlier one went out still leaves the caller holding what the chat now has.
            sent.push(sent_message(&message));
        }
        Ok(())
    }

    /// `options.send.link_preview` is accepted and has no effect here, deliberately. Neither
    /// `sendPhoto`, `sendDocument` nor `sendMediaGroup` carries `link_preview_options`, so a link
    /// in a caption never expands into a card whatever is asked for. Refusing the call instead
    /// would make the agent handle a platform difference it cannot see from the tool schema,
    /// for a request that is harmless; the tool description states it so the absent card is not
    /// read as a bug.
    async fn send_files(
        &self,
        conversation: &ConversationId,
        paths: &[PathBuf],
        caption: Option<&str>,
        options: &FileOptions,
        sent: &mut Vec<SentMessage>,
    ) -> Result<(), ChannelError> {
        let (chat, thread) = self.target(conversation)?;
        // Refused before anything is opened, so an over-long list costs no upload. `sendMediaGroup`
        // would answer with its own error, but only after the files had been read and sent.
        if paths.len() > MAX_ALBUM_ITEMS {
            return Err(ChannelError::Delivery {
                channel: self.id.as_str().to_string(),
                message: format!(
                    "Telegram takes at most {MAX_ALBUM_ITEMS} files in one album, and {} were \
                     given. Send them in several batches.",
                    paths.len()
                ),
            });
        }

        // Declared before the upload starts, because that is what the action is for: the docs say
        // to choose it by what the user is about to receive. A large file otherwise
        // transfers in complete silence.
        let activity = if options.as_photo {
            Activity::SendingPhoto
        } else {
            Activity::SendingFile
        };
        if let Err(error) = self.set_activity(conversation, activity).await {
            tracing::debug!(conversation = %conversation, "upload indicator failed: {}", error);
        }

        // Captions have their own, much smaller limit than messages, and unlike a message a caption
        // cannot be continued: it belongs to the file. Taking the first part and dropping the rest
        // lost the difference silently and still reported success, so a caption that does not fit
        // is refused and the agent is told the limit it has to write to. The same limit governs an
        // album's caption, which is one item's caption wearing a different hat.
        let caption_body = match caption {
            None => None,
            Some(caption) => {
                let (mut bodies, _) = self.render(caption, render::CAPTION_LIMIT);
                if bodies.len() > 1 {
                    return Err(ChannelError::Delivery {
                        channel: self.id.as_str().to_string(),
                        message: format!(
                            "the caption is longer than the {} characters Telegram allows on a \
                             file. Shorten it, or send the file with a short caption and the rest \
                             as a message.",
                            render::CAPTION_LIMIT
                        ),
                    });
                }
                bodies.pop()
            }
        };
        let parse_mode = self.caption_parse_mode();
        let reply_to = options
            .send
            .reply_to
            .as_deref()
            .map(|raw| self.message_id(raw))
            .transpose()?;

        // Two endpoints; see `groups_into_an_album`.
        if Self::groups_into_an_album(paths) {
            let messages = self
                .album_request(chat, thread, paths, caption_body, reply_to, options)
                .await
                .map_err(|error| self.delivery_error(&error))?;
            sent.extend(messages.iter().map(sent_message));
            return Ok(());
        }

        // The trait says `paths` is non-empty, and the tool refuses an empty list before reaching
        // here. Answered rather than unwrapped so a future caller that breaks the contract gets a
        // sentence instead of a panic in the drain loop.
        let Some(path) = paths.first() else {
            return Err(ChannelError::Delivery {
                channel: self.id.as_str().to_string(),
                message: "no files were given to send".to_string(),
            });
        };
        let file = InputFile::file(path);
        let message = if options.as_photo {
            let mut request = self.bot.send_photo(Recipient::Id(chat), file);
            if let Some(caption) = caption_body {
                request = request.caption(caption);
                if let Some(parse_mode) = parse_mode {
                    request = request.parse_mode(parse_mode);
                }
            }
            if let Some(thread) = thread {
                request = request.message_thread_id(thread);
            }
            if options.send.silent {
                request = request.disable_notification(true);
            }
            if let Some(reply_to) = reply_to {
                request = request.reply_parameters(ReplyParameters::new(reply_to));
            }
            request.await
        } else {
            let mut request = self.bot.send_document(Recipient::Id(chat), file);
            if let Some(caption) = caption_body {
                request = request.caption(caption);
                if let Some(parse_mode) = parse_mode {
                    request = request.parse_mode(parse_mode);
                }
            }
            if let Some(thread) = thread {
                request = request.message_thread_id(thread);
            }
            if options.send.silent {
                request = request.disable_notification(true);
            }
            if let Some(reply_to) = reply_to {
                request = request.reply_parameters(ReplyParameters::new(reply_to));
            }
            request.await
        }
        .map_err(|error| self.delivery_error(&error))?;
        sent.push(sent_message(&message));
        Ok(())
    }

    async fn fetch(&self, file_ref: &str, max_bytes: u64) -> Result<FetchedFile, ChannelError> {
        let file = self
            .downloader
            .get_file(FileId(file_ref.to_string()))
            .await
            .map_err(|error| self.delivery_error(&error))?;

        // Checked before transferring anything, so an oversized file costs one metadata call rather
        // than a partial download.
        let size = u64::from(file.size);
        if size > max_bytes {
            return Err(ChannelError::Delivery {
                channel: self.id.as_str().to_string(),
                message: format!(
                    "the file is {size} bytes, over the configured limit of {max_bytes} bytes"
                ),
            });
        }

        let mut bytes = Vec::with_capacity(size as usize);
        self.downloader
            .download_file(&file.path, &mut bytes)
            .await
            .map_err(|error| self.delivery_error(&error))?;

        // Telegram's own path carries the real extension, which is a better signal than anything
        // the sender named the file.
        let extension = Path::new(&file.path)
            .extension()
            .and_then(|extension| extension.to_str())
            .map(str::to_string);
        Ok(FetchedFile {
            media_type: extension.as_deref().and_then(media_type_for_extension),
            extension,
            bytes,
        })
    }

    async fn react(
        &self,
        conversation: &ConversationId,
        message_id: &str,
        emoji: Option<&str>,
    ) -> Result<(), ChannelError> {
        let (chat, _thread) = self.target(conversation)?;
        let message_id = self.message_id(message_id)?;

        // Telegram publishes a fixed set of reaction emoji and revises it. Sending whatever the
        // agent chose and passing the rejection back is what keeps this from drifting out of date
        // against a locally hardcoded list.
        let reactions: Vec<ReactionType> = emoji
            .map(|emoji| {
                vec![ReactionType::Emoji {
                    emoji: emoji.to_string(),
                }]
            })
            .unwrap_or_default();

        self.bot
            .set_message_reaction(Recipient::Id(chat), message_id)
            .reaction(reactions)
            .await
            .map(|_| ())
            .map_err(|error| self.delivery_error(&error))
    }

    async fn edit_text(
        &self,
        conversation: &ConversationId,
        message_id: &str,
        markdown: &str,
        link_preview: bool,
    ) -> Result<Option<SentMessage>, ChannelError> {
        let (chat, _thread) = self.target(conversation)?;
        let message_id = self.message_id(message_id)?;
        let (bodies, parse_mode) = self.render(markdown, render::MESSAGE_LIMIT);

        // An edit replaces one message, so text that would have been split on the way out has
        // nowhere to go. Refusing beats silently publishing the first part as the whole revision.
        // Zero bodies is a separate case: the renderer drops input that produces nothing visible,
        // and reporting that as "too long" would send the agent looking for length it does not
        // have.
        let [body] = bodies.as_slice() else {
            return Err(ChannelError::Delivery {
                channel: self.id.as_str().to_string(),
                message: if bodies.is_empty() {
                    "the replacement text renders to nothing, so there is no message to leave \
                     behind. Use delete_message to remove it instead."
                        .to_string()
                } else {
                    format!(
                        "the replacement text is too long for one message ({} parts); an edit \
                         cannot span several",
                        bodies.len()
                    )
                },
            });
        };

        self.edit_request(chat, message_id, body.clone(), parse_mode, link_preview)
            .await
            .map(|message| Some(sent_message(&message)))
            .map_err(|error| self.delivery_error(&error))
    }

    async fn delete_message(
        &self,
        conversation: &ConversationId,
        message_id: &str,
    ) -> Result<(), ChannelError> {
        let (chat, _thread) = self.target(conversation)?;
        let message_id = self.message_id(message_id)?;
        self.bot
            .delete_message(Recipient::Id(chat), message_id)
            .await
            .map(|_| ())
            .map_err(|error| self.delivery_error(&error))
    }

    async fn set_activity(
        &self,
        conversation: &ConversationId,
        activity: Activity,
    ) -> Result<(), ChannelError> {
        let (chat, thread) = self.target(conversation)?;
        let mut request = self
            .bot
            .send_chat_action(Recipient::Id(chat), chat_action(activity));
        if let Some(thread) = thread {
            request = request.message_thread_id(thread);
        }
        request
            .await
            .map(|_| ())
            .map_err(|error| self.delivery_error(&error))
    }

    async fn moderate_member(
        &self,
        conversation: &ConversationId,
        user_id: &str,
        action: MemberAction,
        until: Option<chrono::DateTime<chrono::Utc>>,
        revoke_messages: bool,
    ) -> Result<(), ChannelError> {
        let (chat, _thread) = self.target(conversation)?;
        let user = self.user_id(user_id)?;
        let until = until
            .map(|until| clamp_until(self.id.as_str(), until))
            .transpose()?;

        match action {
            MemberAction::Restrict => {
                let mut request = self
                    .bot
                    .restrict_chat_member(Recipient::Id(chat), user, ChatPermissions::empty())
                    // Without this Telegram infers some permissions from others, which makes an
                    // empty set mean less than it says. Sending exactly what we mean is the point.
                    .use_independent_chat_permissions(true);
                if let Some(until) = until {
                    request = request.until_date(until);
                }
                request
                    .await
                    .map(|_| ())
                    .map_err(|error| self.delivery_error(&error))
            }
            MemberAction::Unrestrict => {
                // Lifting a restriction is not "grant everything": that would leave the person with
                // more than the chat allows anybody else. The chat's own defaults are the only
                // correct target, so they are read back rather than assumed.
                let permissions = self
                    .bot
                    .get_chat(Recipient::Id(chat))
                    .await
                    .map_err(|error| self.delivery_error(&error))?
                    .permissions()
                    .unwrap_or_else(ChatPermissions::all);
                self.bot
                    .restrict_chat_member(Recipient::Id(chat), user, permissions)
                    .use_independent_chat_permissions(true)
                    .await
                    .map(|_| ())
                    .map_err(|error| self.delivery_error(&error))
            }
            MemberAction::Ban => {
                let mut request = self
                    .bot
                    .ban_chat_member(Recipient::Id(chat), user)
                    .revoke_messages(revoke_messages);
                if let Some(until) = until {
                    request = request.until_date(until);
                }
                request
                    .await
                    .map(|_| ())
                    .map_err(|error| self.delivery_error(&error))
            }
            MemberAction::Unban => self
                .bot
                .unban_chat_member(Recipient::Id(chat), user)
                // Without this, "unbanning" somebody who is currently a member removes them from
                // the chat, which is the opposite of what was asked for.
                .only_if_banned(true)
                .await
                .map(|_| ())
                .map_err(|error| self.delivery_error(&error)),
            MemberAction::Kick => {
                // Telegram has no kick: it is a ban lifted straight away, which removes the person
                // while leaving them free to rejoin.
                self.bot
                    .ban_chat_member(Recipient::Id(chat), user)
                    .revoke_messages(revoke_messages)
                    .await
                    .map_err(|error| self.delivery_error(&error))?;
                self.bot
                    .unban_chat_member(Recipient::Id(chat), user)
                    .only_if_banned(true)
                    .await
                    .map(|_| ())
                    // The half-finished state has to be named. A bare failure here reads as "the
                    // kick did not happen", when in fact the person is removed and still banned,
                    // and the fix is an explicit unban rather than a retry.
                    .map_err(|error| ChannelError::Delivery {
                        channel: self.id.as_str().to_string(),
                        message: format!(
                            "the user was removed, but lifting the ban afterwards failed \
                             ({error}), so they are banned rather than kicked. Use the `unban` \
                             action to let them rejoin."
                        ),
                    })
            }
        }
    }

    async fn set_member_rights(
        &self,
        conversation: &ConversationId,
        user_id: &str,
        rights: &[MemberRight],
    ) -> Result<(), ChannelError> {
        let (chat, _thread) = self.target(conversation)?;
        let user = self.user_id(user_id)?;
        let held = |right: MemberRight| rights.contains(&right);
        // Every flag is sent on every call, including the false ones. Telegram treats an omitted
        // flag as "leave alone", so sending only the granted ones would make this add rights and
        // never remove them, and an empty list would silently do nothing instead of demoting.
        self.bot
            .promote_chat_member(Recipient::Id(chat), user)
            .can_manage_chat(held(MemberRight::ManageChat))
            .can_delete_messages(held(MemberRight::DeleteMessages))
            .can_restrict_members(held(MemberRight::RestrictMembers))
            .can_promote_members(held(MemberRight::PromoteMembers))
            .can_pin_messages(held(MemberRight::PinMessages))
            .can_change_info(held(MemberRight::ChangeInfo))
            .can_invite_users(held(MemberRight::InviteUsers))
            .can_manage_topics(held(MemberRight::ManageTopics))
            .can_manage_video_chats(held(MemberRight::ManageVideoChats))
            .await
            .map(|_| ())
            .map_err(|error| self.delivery_error(&error))
    }

    async fn pin_message(
        &self,
        conversation: &ConversationId,
        message_id: &str,
        pin: bool,
        silent: bool,
    ) -> Result<(), ChannelError> {
        let (chat, _thread) = self.target(conversation)?;
        let message_id = self.message_id(message_id)?;
        if pin {
            self.bot
                .pin_chat_message(Recipient::Id(chat), message_id)
                .disable_notification(silent)
                .await
                .map(|_| ())
                .map_err(|error| self.delivery_error(&error))
        } else {
            self.bot
                .unpin_chat_message(Recipient::Id(chat))
                .message_id(message_id)
                .await
                .map(|_| ())
                .map_err(|error| self.delivery_error(&error))
        }
    }

    async fn set_chat(
        &self,
        conversation: &ConversationId,
        settings: &ChatSettings,
    ) -> Result<(), ChannelError> {
        let (chat, _thread) = self.target(conversation)?;
        if settings.slowmode.is_some() {
            // Telegram has no slowmode a bot can set. Accepting the argument and doing nothing
            // would have the agent report a change that never happened.
            return Err(ChannelError::Unsupported {
                channel: self.id.as_str().to_string(),
                feature: "slowmode, which Telegram does not expose to bots",
            });
        }
        let mut applied = Vec::new();
        if let Some(title) = &settings.title {
            self.bot
                .set_chat_title(Recipient::Id(chat), title.clone())
                .await
                .map_err(|error| self.delivery_error(&error))?;
            applied.push("title");
        }
        if let Some(description) = &settings.description {
            self.bot
                .set_chat_description(Recipient::Id(chat))
                .description(description.clone())
                .await
                // Telegram has no way to change both at once, so a failure on the second leaves the
                // first standing. Saying which landed stops the agent retrying the whole thing or
                // telling somebody nothing changed.
                .map_err(|error| ChannelError::Delivery {
                    channel: self.id.as_str().to_string(),
                    message: match applied.as_slice() {
                        [] => error.to_string(),
                        applied => format!(
                            "the {} was changed, but the description was not ({error})",
                            applied.join(" and ")
                        ),
                    },
                })?;
        }
        Ok(())
    }

    async fn member(
        &self,
        conversation: &ConversationId,
        user_id: Option<&str>,
    ) -> Result<MemberInfo, ChannelError> {
        let (chat, _thread) = self.target(conversation)?;
        let user = match user_id {
            Some(user_id) => self.user_id(user_id)?,
            None => {
                self.bot
                    .get_me()
                    .await
                    .map_err(|error| self.delivery_error(&error))?
                    .id
            }
        };
        let member = self
            .bot
            .get_chat_member(Recipient::Id(chat), user)
            .await
            .map_err(|error| self.delivery_error(&error))?;
        Ok(MemberInfo {
            user_id: user.0.to_string(),
            display_name: Some(display_name(&member.user)),
            status: member_status(&member.kind),
            rights: member_rights(&member.kind),
            // Telegram has no roles, and grants privileges to the person directly.
            roles: Vec::new(),
            // The Bot API reports no presence of any kind, at any permission level.
            presence: None,
            // A Telegram restriction can be unbounded, which has no end date to report.
            restricted_until: match &member.kind {
                teloxide::types::ChatMemberKind::Restricted(restricted) => {
                    match restricted.until_date {
                        teloxide::types::UntilDate::Date(until) if until > chrono::Utc::now() => {
                            Some(until)
                        }
                        _ => None,
                    }
                }
                _ => None,
            },
        })
    }

    async fn describe_conversation(
        &self,
        conversation: &ConversationId,
    ) -> Result<ConversationInfo, ChannelError> {
        let (chat, _thread) = self.target(conversation)?;
        let info = self
            .bot
            .get_chat(Recipient::Id(chat))
            .await
            .map_err(|error| self.delivery_error(&error))?;
        let kind = if info.is_private() {
            ChatKind::Direct
        } else if info.is_channel() {
            ChatKind::Channel
        } else {
            ChatKind::Group
        };
        Ok(ConversationInfo {
            // Telegram's ids are final, so the id asked about is the answer. A thread segment
            // rides along unchecked: the Bot API answers for a chat and has nothing that says
            // whether one of its topics still exists.
            id: conversation.clone(),
            kind,
            // A private chat has no title, and is named by whoever is on the other end.
            title: info
                .title()
                .map(str::to_string)
                .or_else(|| info.first_name().map(str::to_string))
                .or_else(|| info.username().map(|username| format!("@{username}"))),
        })
    }

    /// List a chat's administrators, which is the whole of what the Bot API will enumerate.
    ///
    /// Telegram has no method for listing ordinary members and no way to search them, by design. So
    /// this answers a narrower question than it is asked, says so through
    /// [`MemberCoverage::Administrators`], and carries the total headcount alongside, which is the
    /// one thing the platform will say about everyone.
    async fn list_members(
        &self,
        conversation: &ConversationId,
        query: Option<&str>,
        limit: usize,
        after: Option<&str>,
    ) -> Result<MemberListing, ChannelError> {
        if query.is_some() {
            return Err(ChannelError::Unsupported {
                channel: self.id.as_str().to_string(),
                feature: "searching members by name on Telegram, which the Bot API has no method \
                          for. Omit the query to get the administrators, or use member with a user \
                          id you already have",
            });
        }
        // Paging an administrator list would be inventing a limit Telegram does not have: it
        // returns every administrator in one call, and a chat cannot hold enough of them for that
        // to matter.
        if after.is_some() {
            return Err(ChannelError::Unsupported {
                channel: self.id.as_str().to_string(),
                feature: "paging through members on Telegram, which returns its administrators \
                          whole",
            });
        }
        let (chat, _thread) = self.target(conversation)?;

        let administrators = self
            .bot
            .get_chat_administrators(Recipient::Id(chat))
            .await
            .map_err(|error| self.delivery_error(&error))?;
        // Best effort: the administrators are the answer, and the headcount is a bonus that should
        // not cost them. Logged rather than dropped, so a chat where this always fails is
        // discoverable instead of just quietly missing a field.
        let total = match self.bot.get_chat_member_count(Recipient::Id(chat)).await {
            Ok(count) => Some(u64::from(count)),
            Err(error) => {
                tracing::debug!(
                    channel = %self.id,
                    "could not read the chat's member count: {}",
                    error
                );
                None
            }
        };

        if administrators.len() > limit {
            // Truncating would hand back a short list with no cursor to continue from, which is
            // indistinguishable from a chat that simply has few administrators. There are never
            // many, so raising the limit is always possible.
            return Err(ChannelError::Unsupported {
                channel: self.id.as_str().to_string(),
                feature: "listing part of a Telegram chat's administrators, which arrive whole and \
                          cannot be paged. Raise the limit past their number",
            });
        }
        Ok(MemberListing {
            coverage: MemberCoverage::Administrators,
            members: administrators
                .iter()
                .map(|member| MemberInfo {
                    user_id: member.user.id.0.to_string(),
                    display_name: Some(display_name(&member.user)),
                    status: member_status(&member.kind),
                    rights: member_rights(&member.kind),
                    roles: Vec::new(),
                    restricted_until: None,
                    presence: None,
                })
                .collect(),
            total,
            next_after: None,
        })
    }

    async fn probe(&self) -> Result<ChannelIdentity, ChannelError> {
        let me = self
            .bot
            .get_me()
            .await
            .map_err(|error| ChannelError::Auth {
                channel: self.id.as_str().to_string(),
                message: error.to_string(),
            })?;
        Ok(ChannelIdentity {
            id: me.id.0.to_string(),
            display_name: me.first_name.clone(),
            username: me.username.clone(),
            reads_all_group_messages: me.can_read_all_group_messages,
        })
    }
}

/// Telegram's floor and ceiling on how long a restriction or ban may last.
///
/// Anything outside this window is treated by the API as *forever*, silently. A thirty-second mute
/// becoming permanent is the worst failure this surface has, so the bounds are enforced here and
/// reported rather than left to be discovered.
const MIN_RESTRICTION: chrono::Duration = chrono::Duration::seconds(30);
const MAX_RESTRICTION: chrono::Duration = chrono::Duration::days(366);

/// Reject a duration Telegram would quietly turn into a permanent one.
fn clamp_until(
    channel: &str,
    until: chrono::DateTime<chrono::Utc>,
) -> Result<chrono::DateTime<chrono::Utc>, ChannelError> {
    let remaining = until - chrono::Utc::now();
    if remaining < MIN_RESTRICTION {
        return Err(ChannelError::Delivery {
            channel: channel.to_string(),
            message: format!(
                "Telegram treats anything under {} seconds as permanent, so this would not expire. \
                 Use a longer duration, or omit it if you meant it to be permanent.",
                MIN_RESTRICTION.num_seconds()
            ),
        });
    }
    if remaining > MAX_RESTRICTION {
        return Err(ChannelError::Delivery {
            channel: "telegram".to_string(),
            message: format!(
                "Telegram treats anything over {} days as permanent, so this would not expire. Use \
                 a shorter duration, or omit it if you meant it to be permanent.",
                MAX_RESTRICTION.num_days()
            ),
        });
    }
    Ok(until)
}

/// Translate Telegram's membership model into the platform-neutral one.
fn member_status(kind: &teloxide::types::ChatMemberKind) -> MemberStatus {
    use teloxide::types::ChatMemberKind;
    match kind {
        ChatMemberKind::Owner(_) => MemberStatus::Owner,
        ChatMemberKind::Administrator(_) => MemberStatus::Administrator,
        ChatMemberKind::Member { .. } => MemberStatus::Member,
        ChatMemberKind::Restricted(_) => MemberStatus::Restricted,
        ChatMemberKind::Left => MemberStatus::Left,
        ChatMemberKind::Banned(_) => MemberStatus::Banned,
    }
}

/// The privileges a member holds, which is what tells the agent what it may do in a chat.
fn member_rights(kind: &teloxide::types::ChatMemberKind) -> Vec<MemberRight> {
    use teloxide::types::ChatMemberKind;
    // teloxide gives most of these an accessor on the enum that already folds in an owner's
    // implicit authority. These four exist only as struct fields, so the owner rule has to be
    // written out for them.
    let (pin, change_info, invite, topics) = match kind {
        ChatMemberKind::Owner(_) => (true, true, true, true),
        ChatMemberKind::Administrator(administrator) => (
            administrator.can_pin_messages,
            administrator.can_change_info,
            administrator.can_invite_users,
            administrator.can_manage_topics,
        ),
        _ => (false, false, false, false),
    };
    [
        (MemberRight::ManageChat, kind.can_manage_chat()),
        (MemberRight::DeleteMessages, kind.can_delete_messages()),
        (MemberRight::RestrictMembers, kind.can_restrict_members()),
        (MemberRight::PromoteMembers, kind.can_promote_members()),
        (MemberRight::PinMessages, pin),
        (MemberRight::ChangeInfo, change_info),
        (MemberRight::InviteUsers, invite),
        (MemberRight::ManageTopics, topics),
        (MemberRight::ManageVideoChats, kind.can_manage_video_chats()),
    ]
    .into_iter()
    .filter_map(|(right, held)| held.then_some(right))
    .collect()
}

/// Best-effort human name for a Telegram user.
/// Describe a message Telegram just made, as the record of something the bridge sent.
///
/// Read off the response rather than off what was submitted. HTML is only how a body is handed to
/// Telegram, which parses the markup away and stores the text with entities beside it, and that
/// stored form is what every received message is recorded in. Taking it back off the response keeps
/// the bridge's own rows in the same shape as everybody else's without a second rendering pass.
fn sent_message(message: &Message) -> SentMessage {
    let (attachments, notes) = describe_content(message);
    let user = message.from.as_ref();
    SentMessage {
        message_id: message.id.0.to_string(),
        text: message
            .text()
            .or_else(|| message.caption())
            .unwrap_or_default()
            .to_string(),
        sender: Sender {
            id: user.map(|user| user.id.0.to_string()).unwrap_or_default(),
            display_name: user.map_or_else(
                || message.chat.title().unwrap_or("unknown sender").to_string(),
                display_name,
            ),
            username: user.and_then(|user| user.username.clone()),
            is_bot: user.is_some_and(|user| user.is_bot),
            // A send by the bot itself always carries `from`, so this only becomes true for a
            // message posted as the chat, which is what a channel post is.
            on_behalf_of_chat: user.is_none(),
        },
        attachments,
        notes,
        timestamp: message.date,
    }
}

/// Pixel size and running time, as far as one media kind has them.
///
/// Grouped rather than passed as three more arguments to `describe_content`'s builder, which is
/// already at five: they arrive together, they are absent together, and every arm that has neither
/// says so once with [`Shape::none`] instead of three times with `None`.
#[derive(Debug, Default, Clone, Copy)]
struct Shape {
    width: Option<u32>,
    height: Option<u32>,
    duration_secs: Option<u32>,
}

impl Shape {
    /// A document or a voice-less file: the platform reports neither size nor length.
    const fn none() -> Self {
        Self {
            width: None,
            height: None,
            duration_secs: None,
        }
    }

    const fn sized(width: u32, height: u32) -> Self {
        Self {
            width: Some(width),
            height: Some(height),
            duration_secs: None,
        }
    }

    const fn clip(width: u32, height: u32, duration_secs: u32) -> Self {
        Self {
            width: Some(width),
            height: Some(height),
            duration_secs: Some(duration_secs),
        }
    }

    const fn sound(duration_secs: u32) -> Self {
        Self {
            width: None,
            height: None,
            duration_secs: Some(duration_secs),
        }
    }
}

fn display_name(user: &teloxide::types::User) -> String {
    match &user.last_name {
        Some(last) if !last.is_empty() => format!("{} {}", user.first_name, last),
        _ => user.first_name.clone(),
    }
}

/// Translate Telegram's forward metadata into the platform-neutral shape.
fn forward_origin(origin: &MessageOrigin) -> ForwardOrigin {
    match origin {
        MessageOrigin::User { sender_user, .. } => ForwardOrigin::User {
            name: display_name(sender_user),
            id: Some(sender_user.id.0.to_string()),
            username: sender_user.username.clone(),
        },
        MessageOrigin::HiddenUser {
            sender_user_name, ..
        } => ForwardOrigin::HiddenUser {
            name: sender_user_name.clone(),
        },
        MessageOrigin::Chat { sender_chat, .. } => ForwardOrigin::Chat {
            title: sender_chat.title().unwrap_or("a group").to_string(),
        },
        MessageOrigin::Channel {
            chat, message_id, ..
        } => ForwardOrigin::Channel {
            title: chat.title().unwrap_or("a channel").to_string(),
            message_id: Some(message_id.0.to_string()),
        },
    }
}

/// Everything a message carries beyond its text: files to fetch on demand, and descriptions of
/// content that has no file at all.
///
/// Exhaustive over `MediaKind` on purpose, so a type Telegram adds later becomes a compile error
/// here rather than a message that silently disappears. That is what went wrong before: the
/// previous version tested six accessors and gave up, so a caption-less GIF produced no text and no
/// attachment, `to_event` returned `None`, and the message vanished with nothing in the log.
fn describe_content(message: &Message) -> (Vec<Attachment>, Vec<String>) {
    let attachment = |kind,
                      file: &teloxide::types::FileMeta,
                      file_name,
                      media_type,
                      shape: Shape,
                      thumb: Option<&teloxide::types::PhotoSize>| {
        (
            vec![Attachment {
                kind,
                file_name,
                media_type,
                bytes: Some(file.size as u64),
                width: shape.width,
                height: shape.height,
                duration_secs: shape.duration_secs,
                file_ref: file.id.to_string(),
                thumb_ref: thumb.map(|thumb| thumb.file.id.to_string()),
                handle: None,
            }],
            Vec::new(),
        )
    };
    let note = |text: String| (Vec::new(), vec![text]);
    let nothing = || (Vec::new(), Vec::new());

    // Dice hangs off `MessageKind` rather than `MediaKind`, so it is checked before the match.
    if let Some(dice) = message.dice() {
        return note(format!(
            "dice roll: {:?} showing {}",
            dice.emoji, dice.value
        ));
    }

    let MessageKind::Common(common) = &message.kind else {
        // Joins, pins, forum topic changes, video chat events. Real service messages carry nothing
        // for the agent to act on, and waking it for them would spend a provider turn on noise.
        return nothing();
    };

    match &common.media_kind {
        // The text itself is the content, and the caller already has it.
        MediaKind::Text(_) => nothing(),

        MediaKind::Photo(media) => {
            // Telegram sends several resolutions of the same photo; the last is the largest.
            let Some(largest) = media.photo.last() else {
                return note("a photo arrived with no usable resolution".to_string());
            };
            attachment(
                AttachmentKind::Photo,
                &largest.file,
                None,
                Some("image/jpeg".to_string()),
                Shape::sized(largest.width, largest.height),
                None,
            )
        }
        MediaKind::Document(media) => attachment(
            AttachmentKind::Document,
            &media.document.file,
            media.document.file_name.clone(),
            media.document.mime_type.as_ref().map(ToString::to_string),
            Shape::none(),
            media.document.thumbnail.as_ref(),
        ),
        MediaKind::Animation(media) => attachment(
            AttachmentKind::Animation,
            &media.animation.file,
            media.animation.file_name.clone(),
            media.animation.mime_type.as_ref().map(ToString::to_string),
            Shape::clip(
                media.animation.width,
                media.animation.height,
                media.animation.duration.seconds(),
            ),
            media.animation.thumbnail.as_ref(),
        ),
        MediaKind::Voice(media) => attachment(
            AttachmentKind::Voice,
            &media.voice.file,
            None,
            media.voice.mime_type.as_ref().map(ToString::to_string),
            Shape::sound(media.voice.duration.seconds()),
            None,
        ),
        MediaKind::Audio(media) => attachment(
            AttachmentKind::Audio,
            &media.audio.file,
            media.audio.file_name.clone(),
            media.audio.mime_type.as_ref().map(ToString::to_string),
            Shape::sound(media.audio.duration.seconds()),
            media.audio.thumbnail.as_ref(),
        ),
        MediaKind::Video(media) => attachment(
            AttachmentKind::Video,
            &media.video.file,
            media.video.file_name.clone(),
            media.video.mime_type.as_ref().map(ToString::to_string),
            Shape::clip(
                media.video.width,
                media.video.height,
                media.video.duration.seconds(),
            ),
            media.video.thumbnail.as_ref(),
        ),
        MediaKind::VideoNote(media) => attachment(
            AttachmentKind::VideoNote,
            &media.video_note.file,
            None,
            None,
            // A video note is square, so Telegram reports one side rather than two.
            Shape::clip(
                media.video_note.length,
                media.video_note.length,
                media.video_note.duration.seconds(),
            ),
            media.video_note.thumbnail.as_ref(),
        ),
        MediaKind::Sticker(media) => {
            let sticker = &media.sticker;
            // An animated (.tgs) or video (.webm) sticker is not a viewable image, but its
            // thumbnail is, so the preview reference is what makes "show me" work for those.
            let animated = sticker.is_animated() || sticker.is_video();
            let thumb = sticker.thumbnail.as_ref().filter(|_| animated);
            let (attachments, mut notes) = attachment(
                AttachmentKind::Sticker,
                &sticker.file,
                None,
                (!animated).then(|| "image/webp".to_string()),
                Shape::sized(sticker.width.into(), sticker.height.into()),
                thumb,
            );
            notes.push(match &sticker.emoji {
                Some(emoji) => format!("sticker {emoji}"),
                None => "sticker".to_string(),
            });
            (attachments, notes)
        }

        MediaKind::Location(media) => {
            let mut described = format!(
                "location: {}, {}",
                media.location.latitude, media.location.longitude
            );
            if media.location.live_period.is_some() {
                described.push_str(" (live, updating)");
            }
            note(described)
        }
        MediaKind::Venue(media) => note(format!(
            "venue: {:?} at {:?} ({}, {})",
            media.venue.title,
            media.venue.address,
            media.venue.location.latitude,
            media.venue.location.longitude
        )),
        MediaKind::Contact(media) => {
            let name = match &media.contact.last_name {
                Some(last) => format!("{} {}", media.contact.first_name, last),
                None => media.contact.first_name.clone(),
            };
            note(format!(
                "contact card: {name}, phone {}",
                media.contact.phone_number
            ))
        }
        MediaKind::Poll(media) => {
            let options: Vec<&str> = media
                .poll
                .options
                .iter()
                .map(|option| option.text.as_str())
                .collect();
            note(format!(
                "poll: {:?} with options {}",
                media.poll.question,
                options.join(", ")
            ))
        }
        MediaKind::Story(_) => note("a story, which bots cannot read".to_string()),

        // Content this bridge has no way to render. Announced rather than dropped, so the agent can
        // at least say "somebody sent me something I cannot open" instead of appearing to ignore
        // it.
        MediaKind::PaidMedia(_) => note("paid media, which this bridge cannot open".to_string()),
        MediaKind::Game(_) => note("a game".to_string()),
        MediaKind::Checklist(_) => note("a checklist".to_string()),

        // A chat turning into a supergroup. Structural, not something anybody said.
        MediaKind::Migration(_) => nothing(),
    }
}

/// Telegram's chat action for a bridge activity.
///
/// Telegram frames these as a declaration of what the user is about to receive rather than as a
/// general "the bot is busy" light, which is why the mapping is by message kind.
const fn chat_action(activity: Activity) -> ChatAction {
    match activity {
        Activity::Typing => ChatAction::Typing,
        Activity::SendingPhoto => ChatAction::UploadPhoto,
        Activity::SendingFile => ChatAction::UploadDocument,
    }
}

/// Media type for the extension Telegram's own file path carries.
///
/// Only the types that matter for viewing an image are mapped. Anything else keeps whatever the
/// message advertised, which is the sender's claim but the best available.
fn media_type_for_extension(extension: &str) -> Option<String> {
    let media_type = match extension.to_ascii_lowercase().as_str() {
        "jpg" | "jpeg" => "image/jpeg",
        "png" => "image/png",
        "gif" => "image/gif",
        "webp" => "image/webp",
        _ => return None,
    };
    Some(media_type.to_string())
}

/// Shorten `text` to at most `limit` characters, appending an ellipsis when it was cut.
fn truncate(text: &str, limit: usize) -> String {
    let mut out: String = text.chars().take(limit).collect();
    if text.chars().count() > limit {
        out.push('…');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn channel(allowed_users: Vec<i64>, allowed_chats: Vec<i64>) -> TelegramChannel {
        configured(allowed_users, allowed_chats, false)
    }

    /// The caption of an album item, or `None`, without caring which variant it is.
    fn item_caption(item: &InputMedia) -> Option<&str> {
        match item {
            InputMedia::Photo(photo) => photo.caption.as_deref(),
            InputMedia::Document(document) => document.caption.as_deref(),
            other => panic!("an album should hold only photos or documents, got {other:?}"),
        }
    }

    #[test]
    fn one_file_does_not_go_through_the_album_endpoint() {
        // `sendMediaGroup` requires at least two items, so routing one file through it is an API
        // error the agent can do nothing about. The boundary is the platform's, so it is pinned.
        assert!(!TelegramChannel::groups_into_an_album(&[PathBuf::from(
            "/tmp/a.png"
        )]));
        assert!(TelegramChannel::groups_into_an_album(&[
            PathBuf::from("/tmp/a.png"),
            PathBuf::from("/tmp/b.png")
        ]));
    }

    #[test]
    fn exactly_one_album_item_carries_the_caption() {
        // The invariant Telegram will not enforce and whose breach is invisible from here. The Bot
        // API has no group caption: what renders under an album is emergent client behaviour when a
        // single item carries one. Caption every item and the official clients render *no* group
        // caption at all, so it reads as though the caption were dropped while each file quietly
        // keeps a copy.
        let paths = [
            PathBuf::from("/tmp/a.png"),
            PathBuf::from("/tmp/b.png"),
            PathBuf::from("/tmp/c.png"),
        ];
        let items = TelegramChannel::album_items(
            &paths,
            Some("the caption".to_string()),
            true,
            Some(ParseMode::Html),
        );
        assert_eq!(items.len(), 3);
        let captioned: Vec<&str> = items.iter().filter_map(item_caption).collect();
        assert_eq!(
            captioned,
            ["the caption"],
            "an album must carry its caption on exactly one item"
        );
    }

    #[test]
    fn an_album_without_a_caption_carries_none_at_all() {
        // The other half, so the fix above cannot become "always caption item zero" with a stand-in
        // when the agent gave none.
        let paths = [PathBuf::from("/tmp/a.pdf"), PathBuf::from("/tmp/b.pdf")];
        let items = TelegramChannel::album_items(&paths, None, false, None);
        assert!(items.iter().all(|item| item_caption(item).is_none()));
    }

    #[test]
    fn as_photo_decides_the_whole_album() {
        // Telegram refuses an album mixing documents with photos, so one flag governs every item.
        let paths = [PathBuf::from("/tmp/a"), PathBuf::from("/tmp/b")];
        assert!(
            TelegramChannel::album_items(&paths, None, true, None)
                .iter()
                .all(|item| matches!(item, InputMedia::Photo(_)))
        );
        assert!(
            TelegramChannel::album_items(&paths, None, false, None)
                .iter()
                .all(|item| matches!(item, InputMedia::Document(_)))
        );
    }

    #[tokio::test]
    async fn more_files_than_telegram_takes_are_refused_before_any_upload() {
        let channel = channel(vec![1], vec![]);
        let conversation = ConversationId::parse("telegram:1").expect("valid");
        let paths: Vec<PathBuf> = (0..=MAX_ALBUM_ITEMS)
            .map(|index| PathBuf::from(format!("/tmp/{index}.png")))
            .collect();
        let error = channel
            .send_files(
                &conversation,
                &paths,
                None,
                &FileOptions::default(),
                &mut Vec::new(),
            )
            .await
            .expect_err("an over-long album must be refused");
        let message = error.to_string();
        // The number has to be in the message: "too many" leaves the agent guessing how many to
        // drop. None of these paths exists, so reaching the platform at all would fail differently.
        assert!(message.contains("10"), "{message}");
    }

    #[tokio::test]
    async fn an_album_states_its_reply_and_silence_on_the_wire() {
        // `sendMediaGroup` carries both, and neither reached a file send at all before this.
        use teloxide::requests::HasPayload as _;
        let channel = channel(vec![1], vec![]);
        let paths = [PathBuf::from("/tmp/a.png"), PathBuf::from("/tmp/b.png")];
        let options = FileOptions {
            as_photo: true,
            send: SendOptions {
                reply_to: Some("4471".to_string()),
                silent: true,
                link_preview: false,
            },
        };
        let request = channel.album_request(
            ChatId(1),
            None,
            &paths,
            None,
            Some(MessageId(4471)),
            &options,
        );
        let payload = request.payload_ref();
        assert_eq!(payload.disable_notification, Some(true));
        assert!(
            payload.reply_parameters.is_some(),
            "the reply target never reached the request"
        );
    }

    #[tokio::test]
    async fn an_edit_states_the_preview_choice_on_the_wire() {
        // The call site, not just the helper. Reverting this line to "send options only when
        // suppressing" compiles, passes every other test, and reintroduces exactly the failure the
        // unconditional form exists to prevent: an edit that asks for a card and silently gets
        // whatever the message already had.
        let channel = channel(vec![1], vec![]);
        for wanted in [true, false] {
            let request = channel.edit_request(
                ChatId(1),
                MessageId(42),
                "see https://example.com".to_string(),
                Some(ParseMode::Html),
                wanted,
            );
            let options = request
                .link_preview_options
                .clone()
                .unwrap_or_else(|| panic!("link_preview={wanted} left the field off the wire"));
            assert_eq!(
                options.is_disabled, !wanted,
                "an edit asking for link_preview={wanted} did not say so"
            );
        }
    }

    #[test]
    fn the_preview_choice_is_stated_in_both_directions() {
        // The enabled form has to be an explicit "default options" object rather than an absent
        // field. `is_disabled` is `skip_serializing_if`, so it serialises to `{}`, which is what
        // Telegram means by default and is distinguishable from sending nothing at all. Sending
        // nothing on an edit leaves Telegram to choose between its default and whatever the
        // message already had, and a preview the agent asked for could go missing either way.
        let wanted = TelegramChannel::link_preview(true);
        assert!(!wanted.is_disabled);
        assert_eq!(
            serde_json::to_string(&wanted).expect("serialises"),
            "{}",
            "the enabled form must be the empty object Telegram reads as default options"
        );

        let refused = TelegramChannel::link_preview(false);
        assert!(refused.is_disabled);
        assert!(
            serde_json::to_string(&refused)
                .expect("serialises")
                .contains("is_disabled"),
            "the disabled form has to actually say so on the wire"
        );
    }

    #[tokio::test]
    async fn searching_members_by_name_is_refused_with_the_reason() {
        // The Bot API has no member search and no member listing, so the only honest answer is to
        // say which question Telegram will actually take. Refused before any network call, so this
        // needs no live bot.
        let channel = channel(vec![1], Vec::new());
        let error = channel
            .list_members(
                &ConversationId::parse("telegram:-100").expect("valid"),
                Some("dana"),
                50,
                None,
            )
            .await
            .expect_err("Telegram cannot do this");
        let message = error.to_string();
        assert!(message.contains("Bot API has no method"), "got: {message}");
        assert!(
            message.contains("administrators"),
            "the refusal has to name what does work: {message}"
        );
    }

    #[tokio::test]
    async fn paging_members_is_refused_rather_than_faked() {
        // Telegram returns its administrators whole, so there is no cursor to hand back. Accepting
        // `after` and ignoring it would replay the same first page forever while looking like
        // progress. Refused before any network call, so this needs no live bot.
        let channel = channel(vec![1], Vec::new());
        let error = channel
            .list_members(
                &ConversationId::parse("telegram:-100").expect("valid"),
                None,
                50,
                Some("12345"),
            )
            .await
            .expect_err("Telegram cannot do this");
        assert!(error.to_string().contains("whole"), "got: {error}");
    }

    #[tokio::test]
    async fn slowmode_is_refused_rather_than_ignored() {
        // Telegram has no slowmode a bot can set. Accepting the argument and reporting success
        // would have the agent tell somebody it quieted a room it did not touch. Refused before any
        // network call, so this needs no live bot.
        let channel = channel(vec![1], Vec::new());
        let error = channel
            .set_chat(
                &ConversationId::parse("telegram:1").expect("valid"),
                &ChatSettings {
                    slowmode: Some(std::time::Duration::from_secs(30)),
                    ..Default::default()
                },
            )
            .await
            .expect_err("Telegram cannot do this");
        assert!(
            matches!(error, ChannelError::Unsupported { .. }),
            "got {error:?}"
        );
    }

    fn configured(
        allowed_users: Vec<i64>,
        allowed_chats: Vec<i64>,
        allow_all: bool,
    ) -> TelegramChannel {
        let config = TelegramConfig {
            token: crate::config::secret::Secret::new("123:fake", "test"),
            allowed_users,
            allowed_chats,
            allow_all,
            admin_tools: true,
            parse_mode: TelegramParseMode::Html,
            poll_timeout: std::time::Duration::from_secs(1),
        };
        TelegramChannel::new(ChannelId::new("telegram"), &config).expect("constructs")
    }

    #[tokio::test]
    async fn allowlist_admits_listed_users_in_their_own_chat() {
        let channel = channel(vec![111], vec![]);
        assert_eq!(
            channel.admission(Some(111), 111, true),
            Some(Admission::User)
        );
        assert_eq!(channel.admission(Some(222), 222, true), None);
    }

    #[tokio::test]
    async fn an_allowlisted_user_is_not_thereby_admitted_in_a_group() {
        // `allowed_users` says who may message the bot, not which rooms it listens in. Somebody
        // allowlisted so they can talk to it privately should not drag it into every group they
        // happen to add it to, because that is a room the operator never named.
        let channel = channel(vec![111], vec![]);
        assert_eq!(
            channel.admission(Some(111), -1001234, false),
            None,
            "an individual grant must not reach into an unlisted group"
        );
    }

    #[tokio::test]
    async fn allowlist_admits_listed_chats_regardless_of_sender() {
        // A group is allowlisted as a whole so every member can talk to the agent in it.
        let channel = channel(vec![], vec![-1001234]);
        assert_eq!(
            channel.admission(Some(999), -1001234, false),
            Some(Admission::Chat)
        );
        assert_eq!(channel.admission(Some(999), -1009999, false), None);
    }

    #[tokio::test]
    async fn a_listed_user_in_a_listed_group_is_admitted_by_the_group() {
        // Both lists name them, but only one of the two grants reaches a group, so that is the one
        // the agent is told about. Reporting `user allowlist` here would claim the person was
        // vetted for this room when what was vetted is the room itself.
        let channel = channel(vec![111], vec![-1001234]);
        assert_eq!(
            channel.admission(Some(111), -1001234, false),
            Some(Admission::Chat)
        );
        assert_eq!(
            channel.admission(Some(111), 111, true),
            Some(Admission::User),
            "their own chat is still theirs"
        );
    }

    #[tokio::test]
    async fn allowlist_rejects_anonymous_senders_outside_allowed_chats() {
        let channel = channel(vec![111], vec![]);
        assert_eq!(channel.admission(None, 111, true), None);
    }

    #[tokio::test]
    async fn empty_allowlist_admits_nobody() {
        // Config rejects this combination, but the channel must fail closed regardless.
        let channel = channel(vec![], vec![]);
        assert_eq!(channel.admission(Some(1), 1, true), None);
        assert_eq!(channel.admission(None, 1, true), None);
    }

    #[tokio::test]
    async fn an_open_channel_admits_anyone() {
        let channel = configured(vec![], vec![], true);
        assert_eq!(
            channel.admission(Some(999), 999, true),
            Some(Admission::Open)
        );
        assert_eq!(
            channel.admission(None, -100123, false),
            Some(Admission::Open)
        );
    }

    #[tokio::test]
    async fn an_open_channel_still_names_the_people_it_knows() {
        // Being open does not make everyone equally unknown. Someone on the user list is still
        // reported as vetted, which is the whole reason the agent is told the admission at all.
        let channel = configured(vec![111], vec![-1001234], true);
        assert_eq!(
            channel.admission(Some(111), 111, true),
            Some(Admission::User)
        );
        assert_eq!(
            channel.admission(Some(222), -1001234, false),
            Some(Admission::Chat)
        );
        assert_eq!(
            channel.admission(Some(222), 222, true),
            Some(Admission::Open)
        );
        // Open is what carries them in a group now, not the user list, and the agent is told so.
        assert_eq!(
            channel.admission(Some(111), -1009999, false),
            Some(Admission::Open)
        );
    }

    #[test]
    fn the_client_outlasts_the_long_poll_it_carries() {
        // teloxide's default client stops at 17 seconds and does not extend itself for the poll
        // timeout, so anything at or above that would abort every quiet poll client-side. The
        // symptom is a network error and a reconnect every few seconds on an idle bot, which reads
        // like a connectivity fault rather than a configuration one.
        const TELOXIDE_DEFAULT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(17);
        assert!(
            POLL_RESPONSE_MARGIN > std::time::Duration::ZERO,
            "the client timeout must exceed the poll timeout, not merely equal it"
        );

        // Read back through the real config rather than a copy of the default, so raising
        // `poll_timeout` without raising the client's ceiling fails here.
        let config = crate::config::Config::from_toml(
            "[meka]\ntoken = \"t\"\n\n[[channels.telegram]]\nid = \"telegram\"\ntoken = \
             \"t\"\nallowed_users = [1]\n",
            std::path::Path::new("/etc/mekabridge/config.toml"),
        )
        .expect("minimal config is valid");
        let crate::config::PlatformConfig::Telegram(telegram) = &config.channels[0].platform else {
            panic!("the config under test declares one Telegram channel");
        };
        assert!(
            telegram.poll_timeout + POLL_RESPONSE_MARGIN > TELOXIDE_DEFAULT_TIMEOUT,
            "the shipped poll timeout of {:?} plus the margin must clear teloxide's own {:?}",
            telegram.poll_timeout,
            TELOXIDE_DEFAULT_TIMEOUT
        );
    }

    #[tokio::test]
    async fn conversation_targets_parse_chat_and_thread() {
        let channel = channel(vec![1], vec![]);
        let conversation = ConversationId::parse("telegram:-1001234:77").expect("valid");
        let (chat, thread) = channel.target(&conversation).expect("resolves");
        assert_eq!(chat, ChatId(-1001234));
        assert_eq!(thread, Some(ThreadId(MessageId(77))));
    }

    #[tokio::test]
    async fn conversation_targets_without_a_thread_resolve_to_the_chat() {
        let channel = channel(vec![1], vec![]);
        let conversation = ConversationId::parse("telegram:42").expect("valid");
        let (chat, thread) = channel.target(&conversation).expect("resolves");
        assert_eq!(chat, ChatId(42));
        assert_eq!(thread, None);
    }

    #[tokio::test]
    async fn non_numeric_chat_segments_are_rejected() {
        let channel = channel(vec![1], vec![]);
        let conversation = ConversationId::parse("telegram:not-a-number").expect("parses");
        let error = channel.target(&conversation).expect_err("must be rejected");
        assert!(error.to_string().contains("Telegram chat id"));
    }

    /// Build a real `Message` from wire JSON, so the entity offsets and kinds are the ones Telegram
    /// actually sends rather than a hand-built approximation.
    fn wire_message(value: serde_json::Value) -> Message {
        serde_json::from_value(value).expect("valid Telegram message")
    }

    /// A channel that knows who it is, which is what `addressed` needs.
    fn identified() -> TelegramChannel {
        let channel = channel(vec![], vec![-1001234]);
        channel
            .identity
            .set(BotIdentity {
                id: UserId(4242),
                username: Some("mekabot".to_string()),
            })
            .expect("fresh channel");
        channel
    }

    fn group_message(extra: serde_json::Value) -> serde_json::Value {
        let mut base = serde_json::json!({
            "message_id": 42,
            "date": 1_754_899_200,
            "chat": {"id": -1001234, "type": "supergroup", "title": "Ops"},
            "from": {"id": 111, "is_bot": false, "first_name": "Alice"},
        });
        let (Some(base_map), Some(extra_map)) = (base.as_object_mut(), extra.as_object()) else {
            panic!("both must be objects");
        };
        for (key, value) in extra_map {
            base_map.insert(key.clone(), value.clone());
        }
        base
    }

    #[tokio::test]
    async fn a_username_mention_addresses_the_bot() {
        let channel = identified();
        let message = wire_message(group_message(serde_json::json!({
            "text": "@mekabot what do you think?",
            "entities": [{"type": "mention", "offset": 0, "length": 8}],
        })));
        assert!(channel.addressed(&message));
    }

    #[tokio::test]
    async fn a_username_mention_is_matched_case_insensitively() {
        // Telegram usernames are case insensitive and clients keep whatever the sender typed, so a
        // case-sensitive comparison would drop real mentions.
        let channel = identified();
        let message = wire_message(group_message(serde_json::json!({
            "text": "@MekaBot ping",
            "entities": [{"type": "mention", "offset": 0, "length": 8}],
        })));
        assert!(channel.addressed(&message));
    }

    #[tokio::test]
    async fn somebody_elses_mention_does_not_address_the_bot() {
        let channel = identified();
        let message = wire_message(group_message(serde_json::json!({
            "text": "@otherbot are you there?",
            "entities": [{"type": "mention", "offset": 0, "length": 9}],
        })));
        assert!(!channel.addressed(&message));
    }

    #[tokio::test]
    async fn the_bots_name_as_ordinary_words_does_not_address_it() {
        // The whole value of mention-only is lost if the name appearing in conversation counts.
        // Only spans Telegram itself marked as a mention are read, so this has no entities
        // at all.
        let channel = identified();
        let message = wire_message(group_message(serde_json::json!({
            "text": "mekabot keeps answering things nobody asked it",
        })));
        assert!(!channel.addressed(&message));
    }

    #[tokio::test]
    async fn a_text_mention_addresses_the_bot_by_id() {
        // The first-class form: Telegram supplies a whole `User`, so this is an id comparison
        // rather than a name match.
        let channel = identified();
        let message = wire_message(group_message(serde_json::json!({
            "text": "look at this",
            "entities": [{
                "type": "text_mention",
                "offset": 0,
                "length": 4,
                "user": {"id": 4242, "is_bot": true, "first_name": "Mica"},
            }],
        })));
        assert!(channel.addressed(&message));

        let someone_else = wire_message(group_message(serde_json::json!({
            "text": "look at this",
            "entities": [{
                "type": "text_mention",
                "offset": 0,
                "length": 4,
                "user": {"id": 999, "is_bot": false, "first_name": "Bob"},
            }],
        })));
        assert!(!channel.addressed(&someone_else));
    }

    #[tokio::test]
    async fn replying_to_the_bot_addresses_it() {
        let channel = identified();
        let message = wire_message(group_message(serde_json::json!({
            "text": "no, the other one",
            "reply_to_message": {
                "message_id": 41,
                "date": 1_754_899_100,
                "chat": {"id": -1001234, "type": "supergroup", "title": "Ops"},
                "from": {"id": 4242, "is_bot": true, "first_name": "Mica"},
                "text": "here you go",
            },
        })));
        assert!(channel.addressed(&message));
    }

    #[tokio::test]
    async fn replying_to_somebody_else_does_not_address_the_bot() {
        let channel = identified();
        let message = wire_message(group_message(serde_json::json!({
            "text": "agreed",
            "reply_to_message": {
                "message_id": 41,
                "date": 1_754_899_100,
                "chat": {"id": -1001234, "type": "supergroup", "title": "Ops"},
                "from": {"id": 999, "is_bot": false, "first_name": "Bob"},
                "text": "we should ship it",
            },
        })));
        assert!(!channel.addressed(&message));
    }

    #[tokio::test]
    async fn a_command_aimed_at_the_bot_addresses_it_but_a_bare_one_does_not() {
        let channel = identified();
        let targeted = wire_message(group_message(serde_json::json!({
            "text": "/status@mekabot",
            "entities": [{"type": "bot_command", "offset": 0, "length": 15}],
        })));
        assert!(channel.addressed(&targeted));

        // Ambiguous in a group with several bots, and Telegram's own privacy mode only forwards it
        // on the strength of who spoke last, which is not something this can reconstruct.
        let bare = wire_message(group_message(serde_json::json!({
            "text": "/status",
            "entities": [{"type": "bot_command", "offset": 0, "length": 7}],
        })));
        assert!(!channel.addressed(&bare));
    }

    #[tokio::test]
    async fn a_mention_in_a_caption_addresses_the_bot() {
        // A media message carries caption entities rather than text entities, and consulting only
        // one of the two would make a mention on a photo invisible.
        let channel = identified();
        let message = wire_message(group_message(serde_json::json!({
            "caption": "@mekabot what is this?",
            "caption_entities": [{"type": "mention", "offset": 0, "length": 8}],
            "photo": [{
                "file_id": "AgACAgEAAx",
                "file_unique_id": "AQAD",
                "file_size": 2048,
                "width": 90,
                "height": 60,
            }],
        })));
        assert!(channel.addressed(&message));
    }

    #[tokio::test]
    async fn every_message_in_a_private_chat_addresses_the_bot() {
        // Nobody else is there, so requiring a mention would silence the agent against the only
        // person talking to it.
        let channel = identified();
        let message = wire_message(serde_json::json!({
            "message_id": 42,
            "date": 1_754_899_200,
            "chat": {"id": 111, "type": "private", "first_name": "Alice"},
            "from": {"id": 111, "is_bot": false, "first_name": "Alice"},
            "text": "are you around?",
        }));
        assert!(channel.addressed(&message));
    }

    #[tokio::test]
    async fn a_group_message_is_not_addressed_when_the_identity_is_unknown() {
        // `run` resolves the identity before reading any update, so this only happens if that
        // failed. Failing closed keeps a muted group quiet rather than waking the agent for
        // everything.
        let channel = channel(vec![], vec![-1001234]);
        let message = wire_message(group_message(serde_json::json!({
            "text": "@mekabot hello",
            "entities": [{"type": "mention", "offset": 0, "length": 8}],
        })));
        assert!(!channel.addressed(&message));
    }

    #[test]
    fn a_restriction_telegram_would_make_permanent_is_rejected() {
        // The trap this closes: Telegram accepts an out-of-range `until_date` and silently treats
        // it as forever, so a ten-second mute becomes a life sentence with no error anywhere.
        let now = chrono::Utc::now();
        let too_short = clamp_until("telegram", now + chrono::Duration::seconds(10))
            .expect_err("under the floor must be rejected");
        assert!(too_short.to_string().contains("permanent"), "{too_short}");

        let too_long = clamp_until("telegram", now + chrono::Duration::days(400))
            .expect_err("over the ceiling must be rejected");
        assert!(too_long.to_string().contains("permanent"), "{too_long}");
    }

    #[test]
    fn a_restriction_inside_telegrams_window_is_accepted() {
        let until = chrono::Utc::now() + chrono::Duration::hours(1);
        assert_eq!(
            clamp_until("telegram", until).expect("an hour is in range"),
            until
        );
    }

    #[tokio::test]
    async fn an_edit_that_renders_to_nothing_says_so_rather_than_blaming_length() {
        // The renderer drops input with nothing visible in it, so zero bodies and several bodies
        // both fail the single-message check. Reporting the empty case as "too long" would send the
        // agent hunting for length it does not have.
        let channel = channel(vec![1], vec![]);
        let conversation = ConversationId::parse("telegram:1").expect("valid");
        let error = channel
            .edit_text(&conversation, "42", "   ", false)
            .await
            .expect_err("an empty revision must be refused");
        assert!(error.to_string().contains("renders to nothing"), "{error}");
        assert!(error.to_string().contains("delete_message"), "{error}");
    }

    #[test]
    fn a_duration_only_applies_where_it_means_something() {
        assert!(MemberAction::Restrict.accepts_duration());
        assert!(MemberAction::Ban.accepts_duration());
        // Telegram takes `until_date` on neither of these, so accepting one would be a promise the
        // platform never made.
        assert!(!MemberAction::Unban.accepts_duration());
        assert!(!MemberAction::Unrestrict.accepts_duration());
        assert!(!MemberAction::Kick.accepts_duration());
    }

    #[test]
    fn an_owner_holds_every_right_without_them_being_listed() {
        // Telegram sends an owner with no rights flags at all, because holding everything is
        // implied. Reading that literally would have the agent believe it cannot moderate a group
        // it owns.
        let member: teloxide::types::ChatMember = serde_json::from_value(serde_json::json!({
            "user": {"id": 42, "is_bot": true, "first_name": "Mica"},
            "status": "creator",
            "is_anonymous": false,
        }))
        .expect("valid chat member");
        let rights = member_rights(&member.kind);
        assert_eq!(member_status(&member.kind), MemberStatus::Owner);
        for right in [
            MemberRight::DeleteMessages,
            MemberRight::RestrictMembers,
            MemberRight::PinMessages,
            MemberRight::ChangeInfo,
            MemberRight::InviteUsers,
        ] {
            assert!(rights.contains(&right), "an owner must hold {right:?}");
        }
    }

    #[test]
    fn an_administrator_reports_only_the_rights_it_was_given() {
        let member: teloxide::types::ChatMember = serde_json::from_value(serde_json::json!({
            "user": {"id": 42, "is_bot": true, "first_name": "Mica"},
            "status": "administrator",
            "can_be_edited": false,
            "is_anonymous": false,
            "can_manage_chat": true,
            "can_delete_messages": true,
            "can_manage_video_chats": false,
            "can_restrict_members": true,
            "can_promote_members": false,
            "can_change_info": false,
            "can_invite_users": true,
            "can_pin_messages": false,
            "can_manage_topics": false,
        }))
        .expect("valid chat member");
        let rights = member_rights(&member.kind);
        assert_eq!(member_status(&member.kind), MemberStatus::Administrator);
        assert!(rights.contains(&MemberRight::RestrictMembers));
        assert!(rights.contains(&MemberRight::InviteUsers));
        assert!(!rights.contains(&MemberRight::PinMessages));
        assert!(!rights.contains(&MemberRight::PromoteMembers));
    }

    #[test]
    fn an_ordinary_member_holds_nothing() {
        let member: teloxide::types::ChatMember = serde_json::from_value(serde_json::json!({
            "user": {"id": 7, "is_bot": false, "first_name": "Alice"},
            "status": "member",
        }))
        .expect("valid chat member");
        assert_eq!(member_status(&member.kind), MemberStatus::Member);
        assert!(member_rights(&member.kind).is_empty());
    }

    // Constructing a channel spawns the throttle worker, so this needs a runtime even though the
    // assertion itself is synchronous.
    #[tokio::test]
    async fn an_anonymous_sender_cannot_be_moderated() {
        // Anonymous admins and channel posts arrive with an empty sender id, so the agent has
        // nothing to pass. The error has to explain that rather than looking like a typo.
        let channel = channel(vec![1], vec![]);
        let error = channel.user_id("").expect_err("must be rejected");
        assert!(error.to_string().contains("Anonymous"), "{error}");
    }

    #[test]
    fn truncate_appends_an_ellipsis_only_when_cutting() {
        assert_eq!(truncate("short", 10), "short");
        assert_eq!(truncate("abcdefghij", 5), "abcde…");
        // Character-based, so multibyte text is not cut mid-codepoint.
        assert_eq!(truncate("日本語テキスト", 3), "日本語…");
    }

    /// Build a `Message` from Telegram's own wire shape.
    ///
    /// Deserializing is the only way to construct one outside teloxide, and it is also the more
    /// faithful test: these payloads are what the API actually sends.
    fn telegram_message(value: serde_json::Value) -> Message {
        serde_json::from_value(value).expect("valid Telegram message payload")
    }

    fn private_message(extra: serde_json::Value) -> Message {
        let mut base = serde_json::json!({
            "message_id": 4471,
            "date": 1_754_400_000,
            "chat": {"id": 111, "type": "private", "first_name": "Alice"},
            "from": {"id": 111, "is_bot": false, "first_name": "Alice", "username": "alice"},
        });
        let (Some(base_map), Some(extra_map)) = (base.as_object_mut(), extra.as_object()) else {
            panic!("both payloads must be objects");
        };
        for (key, value) in extra_map {
            base_map.insert(key.clone(), value.clone());
        }
        telegram_message(base)
    }

    fn inbound(event: InboundEvent) -> InboundMessage {
        let InboundEvent::Message(message) = event else {
            panic!("a Telegram message never produces a retraction");
        };
        *message
    }

    #[tokio::test]
    async fn a_plain_message_carries_its_own_id() {
        let channel = channel(vec![111], vec![]);
        let event = channel
            .to_event(&private_message(serde_json::json!({"text": "hello"})))
            .await
            .expect("allowlisted message becomes an event");
        let message = inbound(event);
        assert_eq!(message.message_id, "4471");
        assert_eq!(message.external_id, "4471");
        assert_eq!(message.admission, Admission::User);
        assert!(!message.sender.is_bot);
        assert!(!message.sender.on_behalf_of_chat);
    }

    #[tokio::test]
    async fn an_edit_gets_a_distinct_queue_key_but_keeps_its_message_id() {
        // The bug this pins: an edit reuses the original's message id, so keying the queue on it
        // sent every edit into the duplicate check and the agent never heard about it.
        let channel = channel(vec![111], vec![]);
        let event = channel
            .to_event(&private_message(serde_json::json!({
                "text": "meet at 6",
                "edit_date": 1_754_400_600,
            })))
            .await
            .expect("edits reach the agent");
        let message = inbound(event);
        assert_eq!(message.message_id, "4471");
        assert_eq!(message.external_id, "4471:e1754400600");
        assert!(message.edited_at.is_some());
    }

    #[tokio::test]
    async fn a_second_edit_of_one_message_is_not_deduplicated_against_the_first() {
        let channel = channel(vec![111], vec![]);
        let first = inbound(
            channel
                .to_event(&private_message(serde_json::json!({
                    "text": "meet at 6",
                    "edit_date": 1_754_400_600,
                })))
                .await
                .expect("first edit"),
        );
        let second = inbound(
            channel
                .to_event(&private_message(serde_json::json!({
                    "text": "meet at 7",
                    "edit_date": 1_754_400_900,
                })))
                .await
                .expect("second edit"),
        );
        assert_ne!(first.external_id, second.external_id);
    }

    #[tokio::test]
    async fn a_forwarded_message_names_its_original_author() {
        let channel = channel(vec![111], vec![]);
        let event = channel
            .to_event(&private_message(serde_json::json!({
                "text": "run this script",
                "forward_origin": {
                    "type": "user",
                    "date": 1_754_300_000,
                    "sender_user": {
                        "id": 999,
                        "is_bot": false,
                        "first_name": "Bob",
                        "username": "bob",
                    },
                },
            })))
            .await
            .expect("event");
        assert_eq!(
            inbound(event).forwarded_from,
            Some(ForwardOrigin::User {
                name: "Bob".to_string(),
                id: Some("999".to_string()),
                username: Some("bob".to_string()),
            })
        );
    }

    #[tokio::test]
    async fn a_hidden_forward_origin_is_preserved_rather_than_dropped() {
        let channel = channel(vec![111], vec![]);
        let event = channel
            .to_event(&private_message(serde_json::json!({
                "text": "see this",
                "forward_origin": {
                    "type": "hidden_user",
                    "date": 1_754_300_000,
                    "sender_user_name": "Carol",
                },
            })))
            .await
            .expect("event");
        assert_eq!(
            inbound(event).forwarded_from,
            Some(ForwardOrigin::HiddenUser {
                name: "Carol".to_string()
            })
        );
    }

    #[tokio::test]
    async fn an_anonymous_admin_is_marked_as_posting_for_the_chat() {
        // No `from`, so the display name falls back to the chat title. Without the flag that reads
        // as an ordinary person called "Deploy Crew".
        let channel = channel(vec![], vec![-1001234]);
        let event = channel
            .to_event(&telegram_message(serde_json::json!({
                "message_id": 12,
                "date": 1_754_400_000,
                "chat": {"id": -1001234, "type": "supergroup", "title": "Deploy Crew"},
                "sender_chat": {"id": -1001234, "type": "supergroup", "title": "Deploy Crew"},
                "text": "ship it",
            })))
            .await
            .expect("event");
        let message = inbound(event);
        assert!(message.sender.on_behalf_of_chat);
        assert_eq!(message.sender.display_name, "Deploy Crew");
        assert!(message.sender.id.is_empty());
        assert_eq!(message.admission, Admission::Chat);
    }

    #[tokio::test]
    async fn a_bot_sender_is_flagged() {
        let channel = channel(vec![], vec![-1001234]);
        let event = channel
            .to_event(&telegram_message(serde_json::json!({
                "message_id": 13,
                "date": 1_754_400_000,
                "chat": {"id": -1001234, "type": "supergroup", "title": "Deploy Crew"},
                "from": {"id": 555, "is_bot": true, "first_name": "WeatherBot", "username": "weatherbot"},
                "text": "it is raining",
            })))
            .await
            .expect("event");
        assert!(inbound(event).sender.is_bot);
    }

    #[tokio::test]
    async fn a_caption_less_gif_still_reaches_the_agent() {
        // The regression this pins: teloxide models an animation separately from a document, so the
        // old document-only check found nothing, the message had no text either, and it vanished
        // with no log line at all.
        let channel = channel(vec![111], vec![]);
        let event = channel
            .to_event(&private_message(serde_json::json!({
                "animation": {
                    "file_id": "CgACgif",
                    "file_unique_id": "u1",
                    "file_size": 51200,
                    "width": 320,
                    "height": 240,
                    "duration": 3,
                    "mime_type": "video/mp4",
                    "thumbnail": {
                        "file_id": "AAMCthumb",
                        "file_unique_id": "u2",
                        "file_size": 900,
                        "width": 90,
                        "height": 68,
                    },
                },
                "document": {
                    "file_id": "CgACgif",
                    "file_unique_id": "u1",
                    "file_size": 51200,
                },
            })))
            .await
            .expect("a GIF must produce an event");
        let message = inbound(event);
        assert_eq!(message.attachments.len(), 1);
        assert_eq!(message.attachments[0].kind, AttachmentKind::Animation);
        assert_eq!(message.attachments[0].file_ref, "CgACgif");
        assert_eq!(
            message.attachments[0].thumb_ref.as_deref(),
            Some("AAMCthumb"),
            "the still frame is what makes an animation viewable without transcoding"
        );
    }

    #[tokio::test]
    async fn a_video_note_is_recognised_rather_than_dropped() {
        let channel = channel(vec![111], vec![]);
        let event = channel
            .to_event(&private_message(serde_json::json!({
                "video_note": {
                    "file_id": "DQACnote",
                    "file_unique_id": "u1",
                    "file_size": 40960,
                    "length": 240,
                    "duration": 5,
                },
            })))
            .await
            .expect("a video note must produce an event");
        let message = inbound(event);
        assert_eq!(message.attachments[0].kind, AttachmentKind::VideoNote);
    }

    #[tokio::test]
    async fn a_video_carries_the_still_telegram_already_generated() {
        let channel = channel(vec![111], vec![]);
        let event = channel
            .to_event(&private_message(serde_json::json!({
                "video": {
                    "file_id": "BAACvideo",
                    "file_unique_id": "u1",
                    "file_size": 88_000_000,
                    "width": 1920,
                    "height": 1080,
                    "duration": 30,
                    "mime_type": "video/mp4",
                    "thumbnail": {
                        "file_id": "AAMCvthumb",
                        "file_unique_id": "u2",
                        "file_size": 1200,
                        "width": 320,
                        "height": 180,
                    },
                },
            })))
            .await
            .expect("event");
        let message = inbound(event);
        assert_eq!(message.attachments[0].kind, AttachmentKind::Video);
        // Telegram caps getFile at 20 MiB, so for a phone video the thumbnail is often the only
        // thing that can be retrieved at all.
        assert_eq!(
            message.attachments[0].thumb_ref.as_deref(),
            Some("AAMCvthumb")
        );
        // Telegram sends these in the same payload the rest of this is read from, so not carrying
        // them was leaving the agent to spend a fetch on a decision the envelope could have made.
        assert_eq!(message.attachments[0].width, Some(1920));
        assert_eq!(message.attachments[0].height, Some(1080));
        assert_eq!(message.attachments[0].duration_secs, Some(30));
    }

    #[tokio::test]
    async fn a_voice_note_says_how_long_it_is() {
        // The case with no dimensions at all, and the one where the length is the whole decision:
        // a nine-second note and a nine-minute one are answered differently.
        let channel = channel(vec![111], vec![]);
        let event = channel
            .to_event(&private_message(serde_json::json!({
                "voice": {
                    "file_id": "AWADvoice",
                    "file_unique_id": "u9",
                    "file_size": 4200,
                    "duration": 9,
                    "mime_type": "audio/ogg",
                },
            })))
            .await
            .expect("event");
        let message = inbound(event);
        assert_eq!(message.attachments[0].kind, AttachmentKind::Voice);
        assert_eq!(message.attachments[0].duration_secs, Some(9));
        assert_eq!(message.attachments[0].width, None);
        assert_eq!(message.attachments[0].height, None);
    }

    #[tokio::test]
    async fn a_shared_location_becomes_a_note_instead_of_disappearing() {
        let channel = channel(vec![111], vec![]);
        let event = channel
            .to_event(&private_message(serde_json::json!({
                "location": { "latitude": 51.5074, "longitude": -0.1278 },
            })))
            .await
            .expect("a location must produce an event");
        let message = inbound(event);
        assert!(message.attachments.is_empty());
        assert_eq!(message.notes, vec!["location: 51.5074, -0.1278"]);
    }

    #[tokio::test]
    async fn a_contact_card_becomes_a_note() {
        let channel = channel(vec![111], vec![]);
        let event = channel
            .to_event(&private_message(serde_json::json!({
                "contact": {
                    "phone_number": "+15551234567",
                    "first_name": "Bob",
                    "last_name": "Smith",
                },
            })))
            .await
            .expect("event");
        let message = inbound(event);
        assert_eq!(message.notes, vec![
            "contact card: Bob Smith, phone +15551234567"
        ]);
    }

    #[tokio::test]
    async fn a_photo_takes_the_largest_resolution_offered() {
        let channel = channel(vec![111], vec![]);
        let event = channel
            .to_event(&private_message(serde_json::json!({
                "photo": [
                    {"file_id": "small", "file_unique_id": "u1", "file_size": 1000, "width": 90, "height": 60},
                    {"file_id": "large", "file_unique_id": "u2", "file_size": 90000, "width": 1280, "height": 853},
                ],
            })))
            .await
            .expect("event");
        let message = inbound(event);
        assert_eq!(message.attachments[0].file_ref, "large");
        assert_eq!(
            message.attachments[0].media_type.as_deref(),
            Some("image/jpeg")
        );
    }

    #[tokio::test]
    async fn album_members_carry_the_group_they_belong_to() {
        let channel = channel(vec![111], vec![]);
        let event = channel
            .to_event(&private_message(serde_json::json!({
                "media_group_id": "13294839284",
                "photo": [
                    {"file_id": "one", "file_unique_id": "u1", "file_size": 9000, "width": 800, "height": 600},
                ],
            })))
            .await
            .expect("event");
        assert_eq!(inbound(event).group_id.as_deref(), Some("13294839284"));
    }

    #[tokio::test]
    async fn a_media_type_this_build_cannot_render_is_announced_rather_than_dropped() {
        // A game carries no file and no caption. Before the media match was made exhaustive this
        // produced nothing at all and the message disappeared, which is the same failure mode GIFs
        // had. The exhaustive match means a type Telegram adds later is a compile error here.
        let channel = channel(vec![111], vec![]);
        let event = channel
            .to_event(&private_message(serde_json::json!({
                "game": {
                    "title": "Corsairs",
                    "description": "A game",
                    "photo": [],
                },
            })))
            .await
            .expect("unrenderable content must still reach the agent");
        let message = inbound(event);
        assert!(message.attachments.is_empty());
        assert_eq!(message.notes, vec!["a game"]);
    }

    #[tokio::test]
    async fn a_service_message_with_nothing_to_act_on_is_still_ignored() {
        // The never-drop invariant is about content, not about joins and pins. Those genuinely
        // carry nothing, and waking the agent for them would burn a provider turn on noise.
        let channel = channel(vec![], vec![-1001234]);
        assert!(
            channel
                .to_event(&telegram_message(serde_json::json!({
                    "message_id": 20,
                    "date": 1_754_400_000,
                    "chat": {"id": -1001234, "type": "supergroup", "title": "Deploy Crew"},
                    "from": {"id": 111, "is_bot": false, "first_name": "Alice"},
                    "new_chat_members": [
                        {"id": 222, "is_bot": false, "first_name": "Bob"}
                    ],
                })))
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn messages_from_outside_the_allowlist_produce_no_event() {
        let channel = channel(vec![222], vec![]);
        assert!(
            channel
                .to_event(&private_message(serde_json::json!({"text": "let me in"})))
                .await
                .is_none()
        );
    }

    #[test]
    fn activities_map_to_the_action_for_what_is_arriving() {
        // A file upload showing "typing" would describe the wrong thing entirely, and Telegram's
        // own guidance is to pick the action by the message kind the user is about to
        // receive.
        assert_eq!(chat_action(Activity::Typing), ChatAction::Typing);
        assert_eq!(chat_action(Activity::SendingPhoto), ChatAction::UploadPhoto);
        assert_eq!(
            chat_action(Activity::SendingFile),
            ChatAction::UploadDocument
        );
    }

    #[tokio::test]
    async fn plain_parse_mode_sends_markdown_verbatim() {
        let mut channel = channel(vec![1], vec![]);
        channel.parse_mode = TelegramParseMode::None;
        let (bodies, parse_mode) = channel.render("**bold**", render::MESSAGE_LIMIT);
        assert_eq!(bodies, vec!["**bold**"]);
        assert!(parse_mode.is_none());
    }

    #[tokio::test]
    async fn html_parse_mode_renders_and_declares_html() {
        let channel = channel(vec![1], vec![]);
        let (bodies, parse_mode) = channel.render("**bold**", render::MESSAGE_LIMIT);
        assert_eq!(bodies, vec!["<b>bold</b>"]);
        assert_eq!(parse_mode, Some(ParseMode::Html));
    }
}
