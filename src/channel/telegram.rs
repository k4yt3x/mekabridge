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

use std::{path::Path, sync::Arc};

use async_trait::async_trait;
use futures::StreamExt;
use teloxide::{
    Bot,
    adaptors::{Throttle, throttle::Limits},
    net::Download,
    payloads::{
        SendChatActionSetters as _, SendDocumentSetters as _, SendMessageSetters as _,
        SendPhotoSetters as _, SetMessageReactionSetters as _,
    },
    prelude::Requester,
    types::{
        AllowedUpdate, ChatAction, ChatId, FileId, InputFile, LinkPreviewOptions, MediaKind,
        Message, MessageId, MessageKind, MessageOrigin, ParseMode, ReactionType, Recipient,
        ReplyParameters, ThreadId, UpdateKind,
    },
    update_listeners::{AsUpdateStream, Polling},
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    channel::{
        Activity, Admission, Attachment, AttachmentKind, Channel, ChannelCapabilities,
        ChannelError, ChannelId, ChannelIdentity, ChatKind, ConversationId, FetchedFile,
        ForwardOrigin, InboundEvent, InboundMessage, Platform, ReplyContext, SendOptions, Sender,
    },
    config::{TelegramConfig, TelegramParseMode},
};

/// Longest excerpt kept from a replied-to message, enough for the agent to know what is being
/// referenced without pasting an entire prior message into the turn.
const REPLY_EXCERPT_CHARS: usize = 160;

pub struct TelegramChannel {
    id: ChannelId,
    bot: Throttle<Bot>,
    /// The unthrottled client, kept because the `Download` trait is implemented on `Bot` rather
    /// than on the throttling adaptor. Downloads do not contend with the send rate limits
    /// anyway.
    downloader: Bot,
    allowed_users: Vec<i64>,
    allowed_chats: Vec<i64>,
    parse_mode: TelegramParseMode,
    link_preview: bool,
    poll_timeout: std::time::Duration,
}

impl TelegramChannel {
    pub fn new(id: ChannelId, config: &TelegramConfig) -> Result<Self, ChannelError> {
        let bot = Bot::new(config.token.expose());
        Ok(Self {
            id,
            bot: Throttle::new_spawn(bot.clone(), Limits::default()),
            downloader: bot,
            allowed_users: config.allowed_users.clone(),
            allowed_chats: config.allowed_chats.clone(),
            parse_mode: config.parse_mode,
            link_preview: config.link_preview,
            poll_timeout: config.poll_timeout,
        })
    }

    /// Whether a message may reach the agent, and on what basis.
    ///
    /// A bot token is a public entry point: anyone who learns the bot's name can message it. An
    /// update from outside the allowlist is dropped without a reply, because replying would confirm
    /// to a stranger that the bot is live.
    ///
    /// The user allowlist is checked first so that somebody who is both individually allowed and
    /// speaking in an allowed chat is reported at the stronger of the two, which is the one the
    /// agent should weigh.
    fn admission(&self, user_id: Option<i64>, chat_id: i64) -> Option<Admission> {
        if user_id.is_some_and(|user_id| self.allowed_users.contains(&user_id)) {
            return Some(Admission::User);
        }
        if self.allowed_chats.contains(&chat_id) {
            return Some(Admission::Chat);
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

    /// Split agent Markdown into wire-ready message bodies.
    fn render(&self, markdown: &str, limit: usize) -> (Vec<String>, Option<ParseMode>) {
        match self.parse_mode {
            TelegramParseMode::Html => (render::to_html(markdown, limit), Some(ParseMode::Html)),
            TelegramParseMode::None => (render::to_plain(markdown, limit), None),
        }
    }

    /// Convert one Telegram message into a bridge event, downloading any attachment it carries.
    async fn to_event(&self, message: &Message) -> Option<InboundEvent> {
        let chat_id = message.chat.id;
        let user = message.from.as_ref();
        let user_id = user.map(|user| user.id.0 as i64);
        let Some(admission) = self.admission(user_id, chat_id.0) else {
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
        Some(InboundEvent::Message(InboundMessage {
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
            text,
            reply_to,
            edited_at,
            forwarded_from: message.forward_origin().map(forward_origin),
            group_id: message
                .media_group_id()
                .map(|group| group.0.clone().to_string()),
            notes,
            attachments,
            timestamp: message.date,
        }))
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
        }
    }

    async fn run(
        self: Arc<Self>,
        sink: mpsc::Sender<InboundEvent>,
        shutdown: CancellationToken,
    ) -> Result<(), ChannelError> {
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
    ) -> Result<Vec<String>, ChannelError> {
        let (chat, thread) = self.target(conversation)?;
        let (bodies, parse_mode) = self.render(markdown, render::MESSAGE_LIMIT);
        if bodies.is_empty() {
            return Ok(Vec::new());
        }

        let reply_to = options
            .reply_to
            .as_deref()
            .and_then(|raw| raw.parse::<i32>().ok())
            .map(MessageId);

        let mut sent = Vec::with_capacity(bodies.len());
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
            if !self.link_preview {
                request = request.link_preview_options(LinkPreviewOptions {
                    is_disabled: true,
                    url: None,
                    prefer_small_media: false,
                    prefer_large_media: false,
                    show_above_text: false,
                });
            }
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
            sent.push(message.id.0.to_string());
        }
        Ok(sent)
    }

    async fn send_file(
        &self,
        conversation: &ConversationId,
        path: &Path,
        caption: Option<&str>,
        as_photo: bool,
    ) -> Result<Vec<String>, ChannelError> {
        let (chat, thread) = self.target(conversation)?;

        // Declared before the upload starts, because that is what the action is for: the docs say
        // to choose it by what the user is about to receive. A large file otherwise
        // transfers in complete silence.
        let activity = if as_photo {
            Activity::SendingPhoto
        } else {
            Activity::SendingFile
        };
        if let Err(error) = self.set_activity(conversation, activity).await {
            tracing::debug!(conversation = %conversation, "upload indicator failed: {}", error);
        }

        let file = InputFile::file(path);
        // Captions have their own, much smaller limit than messages.
        let caption_body = caption.and_then(|caption| {
            let (bodies, _) = self.render(caption, render::CAPTION_LIMIT);
            bodies.into_iter().next()
        });
        let parse_mode =
            matches!(self.parse_mode, TelegramParseMode::Html).then_some(ParseMode::Html);

        let message = if as_photo {
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
            request.await
        }
        .map_err(|error| self.delivery_error(&error))?;
        Ok(vec![message.id.0.to_string()])
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
        let message_id = message_id.parse::<i32>().map(MessageId).map_err(|_| {
            ChannelError::InvalidConversation {
                id: message_id.to_string(),
                reason: "a Telegram message id is a number".to_string(),
            }
        })?;

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
        })
    }
}

/// Best-effort human name for a Telegram user.
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
                      thumb: Option<&teloxide::types::PhotoSize>| {
        (
            vec![Attachment {
                kind,
                file_name,
                media_type,
                bytes: Some(file.size as u64),
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
                None,
            )
        }
        MediaKind::Document(media) => attachment(
            AttachmentKind::Document,
            &media.document.file,
            media.document.file_name.clone(),
            media.document.mime_type.as_ref().map(ToString::to_string),
            media.document.thumbnail.as_ref(),
        ),
        MediaKind::Animation(media) => attachment(
            AttachmentKind::Animation,
            &media.animation.file,
            media.animation.file_name.clone(),
            media.animation.mime_type.as_ref().map(ToString::to_string),
            media.animation.thumbnail.as_ref(),
        ),
        MediaKind::Voice(media) => attachment(
            AttachmentKind::Voice,
            &media.voice.file,
            None,
            media.voice.mime_type.as_ref().map(ToString::to_string),
            None,
        ),
        MediaKind::Audio(media) => attachment(
            AttachmentKind::Audio,
            &media.audio.file,
            media.audio.file_name.clone(),
            media.audio.mime_type.as_ref().map(ToString::to_string),
            media.audio.thumbnail.as_ref(),
        ),
        MediaKind::Video(media) => attachment(
            AttachmentKind::Video,
            &media.video.file,
            media.video.file_name.clone(),
            media.video.mime_type.as_ref().map(ToString::to_string),
            media.video.thumbnail.as_ref(),
        ),
        MediaKind::VideoNote(media) => attachment(
            AttachmentKind::VideoNote,
            &media.video_note.file,
            None,
            None,
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
        let config = TelegramConfig {
            token: crate::config::secret::Secret::new("123:fake", "test"),
            allowed_users,
            allowed_chats,
            parse_mode: TelegramParseMode::Html,
            link_preview: false,
            poll_timeout: std::time::Duration::from_secs(1),
        };
        TelegramChannel::new(ChannelId::new("telegram"), &config).expect("constructs")
    }

    #[tokio::test]
    async fn allowlist_admits_listed_users() {
        let channel = channel(vec![111], vec![]);
        assert_eq!(channel.admission(Some(111), 111), Some(Admission::User));
        assert_eq!(channel.admission(Some(222), 222), None);
    }

    #[tokio::test]
    async fn allowlist_admits_listed_chats_regardless_of_sender() {
        // A group is allowlisted as a whole so every member can talk to the agent in it.
        let channel = channel(vec![], vec![-1001234]);
        assert_eq!(
            channel.admission(Some(999), -1001234),
            Some(Admission::Chat)
        );
        assert_eq!(channel.admission(Some(999), -1009999), None);
    }

    #[tokio::test]
    async fn being_individually_allowlisted_outranks_the_chat() {
        // Both apply here. The agent is told the stronger one, because "this person was vetted" and
        // "this person happens to be in a vetted room" are not the same claim.
        let channel = channel(vec![111], vec![-1001234]);
        assert_eq!(
            channel.admission(Some(111), -1001234),
            Some(Admission::User)
        );
        assert_eq!(
            channel.admission(Some(222), -1001234),
            Some(Admission::Chat)
        );
    }

    #[tokio::test]
    async fn allowlist_rejects_anonymous_senders_outside_allowed_chats() {
        let channel = channel(vec![111], vec![]);
        assert_eq!(channel.admission(None, 111), None);
    }

    #[tokio::test]
    async fn empty_allowlist_admits_nobody() {
        // Config rejects this combination, but the channel must fail closed regardless.
        let channel = channel(vec![], vec![]);
        assert_eq!(channel.admission(Some(1), 1), None);
        assert_eq!(channel.admission(None, 1), None);
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
        let InboundEvent::Message(message) = event;
        message
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
