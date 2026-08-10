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
use chrono::Utc;
use futures::StreamExt;
use teloxide::{
    Bot,
    adaptors::{Throttle, throttle::Limits},
    net::Download,
    payloads::{
        SendChatActionSetters as _, SendDocumentSetters as _, SendMessageSetters as _,
        SendPhotoSetters as _,
    },
    prelude::Requester,
    types::{
        ChatAction, ChatId, FileId, InputFile, Message, MessageId, ParseMode, Recipient,
        ReplyParameters, ThreadId, UpdateKind,
    },
    update_listeners::{AsUpdateStream, Polling},
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    channel::{
        Attachment, AttachmentKind, Channel, ChannelCapabilities, ChannelError, ChannelId,
        ChannelIdentity, ChatKind, ConversationId, InboundEvent, InboundMessage, Platform,
        ReplyContext, SendOptions, Sender,
    },
    config::{StorageConfig, TelegramConfig, TelegramParseMode},
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
    poll_timeout: std::time::Duration,
    attachment_dir: std::path::PathBuf,
    attachment_max_bytes: u64,
}

impl TelegramChannel {
    pub fn new(
        id: ChannelId,
        config: &TelegramConfig,
        storage: &StorageConfig,
    ) -> Result<Self, ChannelError> {
        let bot = Bot::new(config.token.expose());
        Ok(Self {
            id,
            bot: Throttle::new_spawn(bot.clone(), Limits::default()),
            downloader: bot,
            allowed_users: config.allowed_users.clone(),
            allowed_chats: config.allowed_chats.clone(),
            parse_mode: config.parse_mode,
            poll_timeout: config.poll_timeout,
            attachment_dir: storage.attachment_dir.clone(),
            attachment_max_bytes: storage.attachment_max_bytes,
        })
    }

    /// Whether a message may reach the agent.
    ///
    /// A bot token is a public entry point: anyone who learns the bot's name can message it. An
    /// update from outside the allowlist is dropped without a reply, because replying would confirm
    /// to a stranger that the bot is live.
    fn is_allowed(&self, user_id: Option<i64>, chat_id: i64) -> bool {
        if self.allowed_chats.contains(&chat_id) {
            return true;
        }
        user_id.is_some_and(|user_id| self.allowed_users.contains(&user_id))
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
        if !self.is_allowed(user_id, chat_id.0) {
            tracing::debug!(
                channel = %self.id,
                chat_id = chat_id.0,
                user_id = ?user_id,
                "dropping a message from outside the allowlist"
            );
            return None;
        }

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

        let sender = Sender {
            id: user_id.map(|id| id.to_string()).unwrap_or_default(),
            display_name: user.map_or_else(
                || message.chat.title().unwrap_or("unknown sender").to_string(),
                display_name,
            ),
            username: user.and_then(|user| user.username.clone()),
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
        let attachments = self.download_attachments(message).await;
        if text.trim().is_empty() && attachments.is_empty() {
            // Joins, pins, and other service messages carry nothing for the agent to act on.
            return None;
        }

        Some(InboundEvent::Message(InboundMessage {
            channel: self.id.clone(),
            platform: Platform::Telegram,
            conversation,
            external_id: message.id.0.to_string(),
            chat_kind,
            chat_title: message.chat.title().map(str::to_string),
            sender,
            text,
            reply_to,
            attachments,
            timestamp: message.date,
        }))
    }

    /// Download whatever files a message carries into the attachment directory.
    ///
    /// A failure here is recorded on the attachment rather than dropping the message: the agent
    /// should still see that a file arrived and why it cannot read it.
    async fn download_attachments(&self, message: &Message) -> Vec<Attachment> {
        let Some(descriptor) = describe_attachment(message) else {
            return Vec::new();
        };

        let mut attachment = Attachment {
            kind: descriptor.kind,
            file_name: descriptor.file_name.clone(),
            media_type: descriptor.media_type,
            bytes: Some(descriptor.bytes),
            path: None,
            unavailable: None,
            inlined: false,
        };

        if descriptor.bytes > self.attachment_max_bytes {
            attachment.unavailable = Some(format!(
                "not downloaded: {} bytes exceeds the configured limit of {} bytes",
                descriptor.bytes, self.attachment_max_bytes
            ));
            return vec![attachment];
        }

        match self
            .fetch_file(&descriptor.file_id, descriptor.file_name.as_deref())
            .await
        {
            Ok(path) => attachment.path = Some(path),
            Err(error) => {
                tracing::warn!(channel = %self.id, "attachment download failed: {}", error);
                attachment.unavailable = Some(format!("download failed: {error}"));
            }
        }
        vec![attachment]
    }

    async fn fetch_file(
        &self,
        file_id: &str,
        file_name: Option<&str>,
    ) -> Result<std::path::PathBuf, ChannelError> {
        let file = self
            .downloader
            .get_file(FileId(file_id.to_string()))
            .await
            .map_err(|error| self.delivery_error(&error))?;

        tokio::fs::create_dir_all(&self.attachment_dir)
            .await
            .map_err(|error| ChannelError::Setup {
                channel: self.id.as_str().to_string(),
                message: format!(
                    "could not create {}: {error}",
                    self.attachment_dir.display()
                ),
            })?;

        // Telegram's own path suffix keeps the extension, which is what lets the agent (and
        // meka's RenderImage) recognise the type. The file id prefix keeps names unique.
        let extension = Path::new(&file.path)
            .extension()
            .and_then(|extension| extension.to_str())
            .or_else(|| {
                file_name
                    .and_then(|name| Path::new(name).extension())
                    .and_then(|extension| extension.to_str())
            });
        let stem = sanitize_file_stem(file_id);
        let local_name = match extension {
            Some(extension) => format!("{stem}.{extension}"),
            None => stem,
        };
        let destination = self.attachment_dir.join(local_name);

        let mut handle = tokio::fs::File::create(&destination)
            .await
            .map_err(|error| ChannelError::Setup {
                channel: self.id.as_str().to_string(),
                message: format!("could not write {}: {error}", destination.display()),
            })?;
        self.downloader
            .download_file(&file.path, &mut handle)
            .await
            .map_err(|error| self.delivery_error(&error))?;
        Ok(destination)
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
        }
    }

    async fn run(
        self: Arc<Self>,
        sink: mpsc::Sender<InboundEvent>,
        shutdown: CancellationToken,
    ) -> Result<(), ChannelError> {
        let mut listener = Polling::builder(self.bot.clone())
            .timeout(self.poll_timeout)
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

    async fn set_typing(&self, conversation: &ConversationId) -> Result<(), ChannelError> {
        let (chat, thread) = self.target(conversation)?;
        let mut request = self
            .bot
            .send_chat_action(Recipient::Id(chat), ChatAction::Typing);
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

/// One attachment as advertised by Telegram, before it has been downloaded.
struct AttachmentDescriptor {
    kind: AttachmentKind,
    file_id: String,
    file_name: Option<String>,
    media_type: Option<String>,
    bytes: u64,
}

/// Identify the single attachment a message carries, if any.
fn describe_attachment(message: &Message) -> Option<AttachmentDescriptor> {
    let descriptor = |kind, file: &teloxide::types::FileMeta, file_name, media_type| {
        Some(AttachmentDescriptor {
            kind,
            file_id: file.id.to_string(),
            file_name,
            media_type,
            bytes: file.size as u64,
        })
    };

    if let Some(photos) = message.photo() {
        // Telegram sends several resolutions of the same photo; the last is the largest.
        let largest = photos.last()?;
        return descriptor(
            AttachmentKind::Photo,
            &largest.file,
            None,
            Some("image/jpeg".to_string()),
        );
    }
    if let Some(document) = message.document() {
        return descriptor(
            AttachmentKind::Document,
            &document.file,
            document.file_name.clone(),
            document.mime_type.as_ref().map(ToString::to_string),
        );
    }
    if let Some(voice) = message.voice() {
        return descriptor(
            AttachmentKind::Voice,
            &voice.file,
            None,
            voice.mime_type.as_ref().map(ToString::to_string),
        );
    }
    if let Some(audio) = message.audio() {
        return descriptor(
            AttachmentKind::Audio,
            &audio.file,
            audio.file_name.clone(),
            audio.mime_type.as_ref().map(ToString::to_string),
        );
    }
    if let Some(video) = message.video() {
        return descriptor(
            AttachmentKind::Video,
            &video.file,
            video.file_name.clone(),
            video.mime_type.as_ref().map(ToString::to_string),
        );
    }
    if let Some(sticker) = message.sticker() {
        return descriptor(AttachmentKind::Sticker, &sticker.file, None, None);
    }
    None
}

/// Reduce a Telegram file id to something safe to use as a filename.
///
/// File ids are base64-ish and can contain `/` and `-`, so they cannot be used verbatim as a path
/// component without risking traversal outside the attachment directory.
fn sanitize_file_stem(file_id: &str) -> String {
    let cleaned: String = file_id
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .take(48)
        .collect();
    if cleaned.is_empty() {
        format!("attachment-{}", Utc::now().timestamp_millis())
    } else {
        cleaned
    }
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
        let storage = StorageConfig {
            path: std::path::PathBuf::from("/tmp/mekabridge-test.db"),
            attachment_dir: std::path::PathBuf::from("/tmp/mekabridge-test-attachments"),
            attachment_max_bytes: 1024,
            attachment_retention: std::time::Duration::from_secs(60),
        };
        let config = TelegramConfig {
            token: crate::config::secret::Secret::new("123:fake", "test"),
            allowed_users,
            allowed_chats,
            parse_mode: TelegramParseMode::Html,
            poll_timeout: std::time::Duration::from_secs(1),
        };
        TelegramChannel::new(ChannelId::new("telegram"), &config, &storage).expect("constructs")
    }

    #[tokio::test]
    async fn allowlist_admits_listed_users() {
        let channel = channel(vec![111], vec![]);
        assert!(channel.is_allowed(Some(111), 111));
        assert!(!channel.is_allowed(Some(222), 222));
    }

    #[tokio::test]
    async fn allowlist_admits_listed_chats_regardless_of_sender() {
        // A group is allowlisted as a whole so every member can talk to the agent in it.
        let channel = channel(vec![], vec![-1001234]);
        assert!(channel.is_allowed(Some(999), -1001234));
        assert!(!channel.is_allowed(Some(999), -1009999));
    }

    #[tokio::test]
    async fn allowlist_rejects_anonymous_senders_outside_allowed_chats() {
        let channel = channel(vec![111], vec![]);
        assert!(!channel.is_allowed(None, 111));
    }

    #[tokio::test]
    async fn empty_allowlist_admits_nobody() {
        // Config rejects this combination, but the channel must fail closed regardless.
        let channel = channel(vec![], vec![]);
        assert!(!channel.is_allowed(Some(1), 1));
        assert!(!channel.is_allowed(None, 1));
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
    fn file_stems_cannot_escape_the_attachment_directory() {
        // Telegram file ids are base64-ish and really do contain '/' and '-'.
        let stem = sanitize_file_stem("../../etc/passwd");
        assert!(!stem.contains('/'));
        assert!(!stem.contains('.'));
        let joined = Path::new("/var/lib/mekabridge").join(&stem);
        assert!(joined.starts_with("/var/lib/mekabridge"));
    }

    #[test]
    fn file_stems_are_bounded_and_never_empty() {
        assert_eq!(sanitize_file_stem(&"a".repeat(200)).len(), 48);
        assert!(!sanitize_file_stem("///").is_empty());
    }

    #[test]
    fn truncate_appends_an_ellipsis_only_when_cutting() {
        assert_eq!(truncate("short", 10), "short");
        assert_eq!(truncate("abcdefghij", 5), "abcde…");
        // Character-based, so multibyte text is not cut mid-codepoint.
        assert_eq!(truncate("日本語テキスト", 3), "日本語…");
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
