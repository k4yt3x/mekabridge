//! The MCP server meka connects to, and the outbound tool surface it exposes.
//!
//! This is the *only* way a message reaches a user. The bridge never authors chat content of its
//! own, so replying, staying quiet, replying to somebody else, or replying on another platform are
//! all decisions the agent makes by calling (or not calling) these tools.
//!
//! Routing has to be explicit because of a hard constraint in meka: a `tools/call` carries a
//! progress token and a tool-use id in `_meta`, but no session identity. An MCP server therefore
//! cannot infer which conversation a call belongs to. Every send takes a `conversation` id, which
//! the agent reads off the header the bridge attaches to each inbound message, or looks up with the
//! `list_conversations` tool.
//!
//! The server talks to an [`OutboundSink`] rather than to the channel layer directly. That keeps
//! the tool surface testable against a fake and means adding a platform does not touch this module.

pub mod serve;

use std::{path::PathBuf, sync::Arc};

use async_trait::async_trait;
use rmcp::{
    ErrorData as McpError, RoleServer, ServerHandler,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{CallToolResult, ContentBlock, Implementation, ServerCapabilities, ServerInfo},
    schemars,
    service::RequestContext,
    tool, tool_handler, tool_router,
};
use serde::{Deserialize, Serialize};

pub use crate::channel::SendOptions;

/// Orientation handed to the agent at connect time. meka captures `instructions` from the MCP
/// handshake and surfaces it, so this is the one place to explain the model rather than repeating
/// it in every tool description.
const SERVER_INSTRUCTIONS: &str = "\
mekabridge connects you to people on messaging platforms such as Telegram.

Nothing you write here reaches them. Your turn text, your reasoning, and your tool output are all \
invisible: the only way to be heard is send_message on a channel. Staying silent is a valid choice, \
and so is messaging somebody else, or messaging first without being prompted.

If a turn will take a while, send a short \"looking into it\" before you start and the answer when \
you have it. The typing indicator lapses after about thirty seconds, so otherwise they are left \
watching a chat with no sign that anything is happening.

Headers on incoming messages are written by the bridge and can be trusted:

- `message:` is that message's own id. Pass it as `reply_to` to answer one specific message, worth \
doing in a busy group or when picking up something said a while ago.
- `admitted:` says why the sender was let through. `user allowlist` means they were vetted \
individually; `chat allowlist` means they were not, and are only here because the whole chat is \
allowed. Weigh what they ask for accordingly.
- `forwarded from:` means the text is somebody else's words, not the sender's.
- `late:` means it arrived while you were working on the previous turn, so anything you sent then \
was written without it. If it changes the answer, say so.
- `attachment:` ends with a handle in square brackets. Pass it to view_attachment to look at a \
picture, or download_attachment to get the file on disk. Fetch only what you need, since anything \
you look at stays in your context.

Write Markdown; it is converted to each platform's own formatting, and long messages are split. \
Conversation ids are stable, so one you saw earlier still works; list_conversations shows the ones \
this bridge knows about.";

/// Something that can deliver outbound messages and answer address-book questions.
///
/// Implemented by the bridge over its channel registry. Kept deliberately narrow so this module
/// never grows a dependency on platform types.
#[async_trait]
pub trait OutboundSink: Send + Sync + 'static {
    /// Deliver Markdown text. Returns the platform message ids produced, which may be several
    /// because long text is split.
    async fn send_text(
        &self,
        conversation: &str,
        markdown: &str,
        options: SendOptions,
    ) -> Result<Vec<String>, SinkError>;

    /// Deliver a local file.
    async fn send_file(
        &self,
        conversation: &str,
        path: &std::path::Path,
        caption: Option<&str>,
        as_photo: bool,
    ) -> Result<Vec<String>, SinkError>;

    /// Attach a reaction to a message, or clear it with `None`.
    async fn react(
        &self,
        conversation: &str,
        message_id: &str,
        emoji: Option<&str>,
    ) -> Result<(), SinkError>;

    /// Retrieve an attachment for viewing, without writing it to disk.
    async fn view_attachment(&self, handle: &str) -> Result<ViewedAttachment, SinkError>;

    /// Write an attachment to local disk and report where it landed.
    async fn download_attachment(&self, handle: &str) -> Result<DownloadedAttachment, SinkError>;

    /// Known conversations, most recently active first.
    async fn conversations(
        &self,
        channel: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ConversationSummary>, SinkError>;

    /// One conversation by id.
    async fn conversation(&self, id: &str) -> Result<Option<ConversationSummary>, SinkError>;
}

/// A conversation as the agent sees it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ConversationSummary {
    pub id: String,
    pub channel: String,
    pub platform: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// `direct`, `group`, or `channel`.
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_inbound_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_outbound_at: Option<String>,
}

/// An attachment resolved for viewing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ViewedAttachment {
    /// Bytes the agent can actually look at, handed back as an MCP image block.
    Image {
        media_type: String,
        /// Base64-encoded, standard alphabet with padding.
        data: String,
        /// Sent alongside the image when it is not the file itself. A video resolves to a single
        /// still frame, and without saying so the agent would reasonably believe it had seen the
        /// whole thing.
        note: Option<String>,
    },
    /// Everything that cannot be shown: a PDF, a voice note, a video with no usable still, or any
    /// file at all when the model has no vision. Describing it is more useful than failing, because
    /// it tells the agent what to do instead.
    Description(String),
}

/// An attachment written to local disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DownloadedAttachment {
    pub path: std::path::PathBuf,
    pub bytes: u64,
    pub media_type: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum SinkError {
    #[error(
        "no conversation with id {0:?}; call list_conversations to see the ids this bridge knows"
    )]
    UnknownConversation(String),

    #[error(
        "no attachment with handle {0:?}; use the handle in square brackets on the message's \
         `attachment:` line. Old attachments are forgotten after the configured retention period."
    )]
    UnknownAttachment(String),

    #[error("conversation {conversation:?} names channel {channel:?}, which is not configured")]
    UnknownChannel {
        conversation: String,
        channel: String,
    },

    #[error("the platform rejected the message: {0}")]
    Delivery(String),

    #[error("{0}")]
    Internal(String),
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SendMessageArgs {
    /// Conversation to send to, for example `telegram:123456789`. Shown in the header of every
    /// incoming message.
    pub conversation: String,
    /// Message body, written as Markdown.
    pub text: String,
    /// Id of a message to reply to, quoting it so it is clear what is being answered. This is the
    /// `message:` line from an incoming message's header.
    #[serde(default)]
    pub reply_to: Option<String>,
    /// Deliver without a notification sound.
    #[serde(default)]
    pub silent: bool,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SendFileArgs {
    /// Conversation to send to.
    pub conversation: String,
    /// Absolute path to a file readable by the bridge process.
    pub path: String,
    /// Text shown alongside the file.
    #[serde(default)]
    pub caption: Option<String>,
    /// Send as a viewable photo rather than a downloadable document. Only valid for images, and
    /// the platform may recompress them.
    #[serde(default)]
    pub as_photo: bool,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ReactArgs {
    /// Conversation the message is in.
    pub conversation: String,
    /// Id of the message to react to, from the `message:` line of its header.
    pub message_id: String,
    /// The emoji to react with. Omit to remove a reaction you added earlier.
    #[serde(default)]
    pub emoji: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct AttachmentArgs {
    /// The handle shown in square brackets on an `attachment:` line, for example `417`.
    pub attachment: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ListConversationsArgs {
    /// Restrict to one configured channel, for example `telegram`.
    #[serde(default)]
    pub channel: Option<String>,
    /// Maximum number to return. Defaults to 50.
    #[serde(default)]
    pub limit: Option<u32>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct GetConversationArgs {
    /// Conversation id to look up.
    pub conversation: String,
}

/// `_meta` key meka uses to name the session a tool call came from. Only used for log correlation
/// here: this bridge owns exactly one session, so the value identifies nothing it does not already
/// know, but having it in both logs makes tracing a message across the two processes possible.
const SESSION_META_KEY: &str = "meka/sessionId";

/// Largest `limit` [`BridgeMcpServer::list_conversations`] will honour, so a runaway argument
/// cannot push a huge blob into the agent's context.
const MAX_CONVERSATION_LIMIT: usize = 200;
const DEFAULT_CONVERSATION_LIMIT: usize = 50;

/// The MCP server exposing mekabridge's outbound tools.
#[derive(Clone)]
pub struct BridgeMcpServer {
    sink: Arc<dyn OutboundSink>,
    tool_router: ToolRouter<Self>,
}

#[tool_router]
impl BridgeMcpServer {
    pub fn new(sink: Arc<dyn OutboundSink>) -> Self {
        Self {
            sink,
            tool_router: Self::tool_router(),
        }
    }

    /// Send a chat message.
    #[tool(
        description = "Send a message to a person or group on a connected messaging platform. This \
                       is how you reply to someone who messaged you, and how you message someone \
                       without being prompted. `conversation` is the id from the header of an \
                       incoming message, or from list_conversations. `text` is Markdown and is \
                       converted to the platform's own formatting; long text is split across \
                       several messages automatically.",
        annotations(title = "Send message", read_only_hint = true, open_world_hint = true)
    )]
    async fn send_message(
        &self,
        Parameters(args): Parameters<SendMessageArgs>,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        self.send_message_inner(args, calling_session(&context))
            .await
    }

    /// The body of [`Self::send_message`], separated from the protocol plumbing.
    ///
    /// `RequestContext` cannot be constructed outside rmcp, so keeping the logic here is what makes
    /// it unit-testable; the wire path is covered by the interop tests instead.
    async fn send_message_inner(
        &self,
        args: SendMessageArgs,
        session: Option<String>,
    ) -> Result<CallToolResult, McpError> {
        if args.text.trim().is_empty() {
            return Ok(CallToolResult::error(vec![ContentBlock::text(
                "`text` is empty; nothing was sent.",
            )]));
        }
        let options = SendOptions {
            reply_to: args.reply_to,
            silent: args.silent,
        };
        match self
            .sink
            .send_text(&args.conversation, &args.text, options)
            .await
        {
            Ok(message_ids) => {
                if let Some(session) = session {
                    tracing::debug!(session = %session, "send_message from meka session");
                }
                Ok(sent_result(&args.conversation, &message_ids))
            }
            Err(error) => Ok(sink_failure(&error)),
        }
    }

    /// Send a file or image.
    #[tool(
        description = "Send a file from the local filesystem to a conversation. Use this to deliver \
                       something you produced, such as a report, an archive, or a rendered chart. \
                       Set `as_photo` for images you want shown inline rather than offered as a \
                       download.",
        annotations(title = "Send file", read_only_hint = true, open_world_hint = true)
    )]
    async fn send_file(
        &self,
        Parameters(args): Parameters<SendFileArgs>,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        self.send_file_inner(args, calling_session(&context)).await
    }

    /// The body of [`Self::send_file`]. See [`Self::send_message_inner`] for why it is split out.
    async fn send_file_inner(
        &self,
        args: SendFileArgs,
        session: Option<String>,
    ) -> Result<CallToolResult, McpError> {
        let path = PathBuf::from(&args.path);
        if !path.is_absolute() {
            return Ok(CallToolResult::error(vec![ContentBlock::text(format!(
                "`path` must be absolute, got {:?}.",
                args.path
            ))]));
        }
        // Checked here rather than left to the platform so the agent gets "no such file" instead of
        // an opaque upload failure from the Telegram API.
        if !path.is_file() {
            return Ok(CallToolResult::error(vec![ContentBlock::text(format!(
                "{} is not a readable file.",
                path.display()
            ))]));
        }
        match self
            .sink
            .send_file(
                &args.conversation,
                &path,
                args.caption.as_deref(),
                args.as_photo,
            )
            .await
        {
            Ok(message_ids) => {
                if let Some(session) = session {
                    tracing::debug!(session = %session, "send_file from meka session");
                }
                Ok(sent_result(&args.conversation, &message_ids))
            }
            Err(error) => Ok(sink_failure(&error)),
        }
    }

    /// React to a message.
    #[tool(
        description = "React to a message with an emoji, the way a person taps a reaction rather \
                       than writing back. Good for acknowledging something that needs no reply, or \
                       for signalling you have seen a message you will answer properly later. \
                       `message_id` is the `message:` line from that message's header. Omit `emoji` \
                       to remove a reaction you added before. Platforms accept only a fixed set of \
                       emoji and usually one per message; if yours is rejected the error says so.",
        annotations(
            title = "React to message",
            read_only_hint = true,
            open_world_hint = true
        )
    )]
    async fn react(
        &self,
        Parameters(args): Parameters<ReactArgs>,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        self.react_inner(args, calling_session(&context)).await
    }

    /// The body of [`Self::react`]. See [`Self::send_message_inner`] for why it is split out.
    async fn react_inner(
        &self,
        args: ReactArgs,
        session: Option<String>,
    ) -> Result<CallToolResult, McpError> {
        let emoji = args.emoji.as_deref().map(str::trim).filter(|emoji| {
            // An empty string would clear the reaction, which `emoji: null` already expresses.
            // Treating it as "clear" silently would hide a caller bug, so it is refused below.
            !emoji.is_empty()
        });
        if args.emoji.is_some() && emoji.is_none() {
            return Ok(CallToolResult::error(vec![ContentBlock::text(
                "`emoji` is empty; omit it entirely to remove a reaction.",
            )]));
        }
        match self
            .sink
            .react(&args.conversation, &args.message_id, emoji)
            .await
        {
            Ok(()) => {
                if let Some(session) = session {
                    tracing::debug!(session = %session, "react from meka session");
                }
                let summary = match emoji {
                    Some(emoji) => format!(
                        "Reacted {emoji} to message {} in {}.",
                        args.message_id, args.conversation
                    ),
                    None => format!(
                        "Removed your reaction from message {} in {}.",
                        args.message_id, args.conversation
                    ),
                };
                Ok(CallToolResult::success(vec![ContentBlock::text(summary)]))
            }
            Err(error) => Ok(sink_failure(&error)),
        }
    }

    /// Look at an attachment.
    #[tool(
        description = "Look at a picture somebody sent you. Nothing arrives downloaded, so call \
                       this when you want to actually see an image before deciding what to say \
                       about it. `attachment` is the handle in square brackets on the message's \
                       `attachment:` line. Videos, animations, and animated stickers come back as a \
                       still frame. Anything that is not viewable, such as a PDF or a voice note, \
                       comes back as a description instead; use download_attachment for those.",
        annotations(
            title = "View attachment",
            read_only_hint = true,
            open_world_hint = true
        )
    )]
    async fn view_attachment(
        &self,
        Parameters(args): Parameters<AttachmentArgs>,
    ) -> Result<CallToolResult, McpError> {
        match self.sink.view_attachment(&args.attachment).await {
            Ok(ViewedAttachment::Image {
                media_type,
                data,
                note,
            }) => {
                let mut blocks = Vec::new();
                // The caveat leads, so it is read before the picture rather than after it has
                // already been taken for the whole file.
                if let Some(note) = note {
                    blocks.push(ContentBlock::text(note));
                }
                blocks.push(ContentBlock::image(data, media_type));
                Ok(CallToolResult::success(blocks))
            }
            Ok(ViewedAttachment::Description(description)) => {
                Ok(CallToolResult::success(vec![ContentBlock::text(
                    description,
                )]))
            }
            Err(error) => Ok(sink_failure(&error)),
        }
    }

    /// Download an attachment to disk.
    #[tool(
        description = "Download a file somebody sent you and get back the path it was saved to. Use \
                       this when you need the file itself, to read a document or run a tool over \
                       it, rather than just to look at a picture. `attachment` is the handle in \
                       square brackets on the message's `attachment:` line. Large files may be \
                       refused; the error says the limit.",
        // `read_only_hint` deserves the caveat: this does write a file. It writes only into
        // `[storage].attachment_dir`, which exists for exactly this, is bounded by
        // `attachment_max_bytes`, and is swept on `attachment_retention`. Marking it otherwise would
        // put it at meka's `write` level, where a bridge run at `read` could receive a document and
        // never open it.
        annotations(title = "Download attachment", read_only_hint = true, open_world_hint = true)
    )]
    async fn download_attachment(
        &self,
        Parameters(args): Parameters<AttachmentArgs>,
    ) -> Result<CallToolResult, McpError> {
        match self.sink.download_attachment(&args.attachment).await {
            Ok(downloaded) => {
                let media_type = downloaded
                    .media_type
                    .map(|media_type| format!(", {media_type}"))
                    .unwrap_or_default();
                Ok(CallToolResult::success(vec![ContentBlock::text(format!(
                    "Saved to {} ({} bytes{media_type}).",
                    downloaded.path.display(),
                    downloaded.bytes
                ))]))
            }
            Err(error) => Ok(sink_failure(&error)),
        }
    }

    /// List known conversations.
    #[tool(
        description = "List the conversations this bridge knows about, most recently active first. \
                       Use it to find a conversation id when you want to message someone whose id \
                       is not in front of you.",
        annotations(
            title = "List conversations",
            read_only_hint = true,
            open_world_hint = false
        )
    )]
    async fn list_conversations(
        &self,
        Parameters(args): Parameters<ListConversationsArgs>,
    ) -> Result<CallToolResult, McpError> {
        let limit = args
            .limit
            .map_or(DEFAULT_CONVERSATION_LIMIT, |limit| limit as usize)
            .clamp(1, MAX_CONVERSATION_LIMIT);
        match self
            .sink
            .conversations(args.channel.as_deref(), limit)
            .await
        {
            Ok(conversations) if conversations.is_empty() => {
                Ok(CallToolResult::success(vec![ContentBlock::text(
                    "No conversations yet. A conversation appears once someone has messaged the \
                     bot at least once.",
                )]))
            }
            Ok(conversations) => Ok(json_result(&conversations)),
            Err(error) => Ok(sink_failure(&error)),
        }
    }

    /// Look up one conversation.
    #[tool(
        description = "Look up a single conversation by id, to check who it belongs to or when it \
                       was last active.",
        annotations(
            title = "Get conversation",
            read_only_hint = true,
            open_world_hint = false
        )
    )]
    async fn get_conversation(
        &self,
        Parameters(args): Parameters<GetConversationArgs>,
    ) -> Result<CallToolResult, McpError> {
        match self.sink.conversation(&args.conversation).await {
            Ok(Some(conversation)) => Ok(json_result(&conversation)),
            Ok(None) => Ok(sink_failure(&SinkError::UnknownConversation(
                args.conversation,
            ))),
            Err(error) => Ok(sink_failure(&error)),
        }
    }
}

// Pointed at the stored field rather than left to default: the macro's default expression is
// `Self::tool_router()`, which would rebuild the whole router on every `tools/call` and every
// `tools/list`.
#[tool_handler(router = self.tool_router)]
impl ServerHandler for BridgeMcpServer {
    fn get_info(&self) -> ServerInfo {
        // The protocol version is deliberately left at the default so it is negotiated with the
        // client. meka pins an older rmcp than this crate builds against, and negotiation is what
        // keeps the two interoperable across upgrades.
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::from_build_env())
            .with_instructions(SERVER_INSTRUCTIONS)
    }
}

/// The meka session a tool call came from.
///
/// meka only sets it for a call made inside an agent turn, so this is `None` for anything else.
fn calling_session(context: &RequestContext<RoleServer>) -> Option<String> {
    context
        .meta
        .get(SESSION_META_KEY)
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
}

/// Render a successful send.
fn sent_result(conversation: &str, message_ids: &[String]) -> CallToolResult {
    let summary = match message_ids.len() {
        0 => format!("Sent to {conversation}."),
        1 => format!(
            "Sent to {conversation} (message id {}).",
            message_ids.first().map_or("", String::as_str)
        ),
        count => format!(
            "Sent to {conversation} as {count} messages (ids {}).",
            message_ids.join(", ")
        ),
    };
    CallToolResult::success(vec![ContentBlock::text(summary)])
}

/// Serialize a value as the tool's text payload.
fn json_result<T: Serialize>(value: &T) -> CallToolResult {
    match serde_json::to_string_pretty(value) {
        Ok(rendered) => CallToolResult::success(vec![ContentBlock::text(rendered)]),
        Err(error) => CallToolResult::error(vec![ContentBlock::text(format!(
            "failed to serialize the result: {error}"
        ))]),
    }
}

/// Report a sink failure to the agent.
///
/// These are tool-level errors rather than protocol errors on purpose: the agent should see the
/// message and be able to recover (by listing conversations, fixing an id, or giving up) instead of
/// getting an opaque "tool failed" from its client.
fn sink_failure(error: &SinkError) -> CallToolResult {
    tracing::warn!("outbound tool call failed: {}", error);
    CallToolResult::error(vec![ContentBlock::text(error.to_string())])
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    #[derive(Default)]
    struct FakeSink {
        sent: Mutex<Vec<(String, String, SendOptions)>>,
        reactions: Mutex<Vec<(String, String, Option<String>)>>,
        conversations: Vec<ConversationSummary>,
        fail_with: Option<&'static str>,
    }

    fn summary(id: &str) -> ConversationSummary {
        ConversationSummary {
            id: id.to_string(),
            channel: "telegram".to_string(),
            platform: "telegram".to_string(),
            title: Some("Alice".to_string()),
            kind: "direct".to_string(),
            last_inbound_at: Some("2026-08-05T12:00:00Z".to_string()),
            last_outbound_at: None,
        }
    }

    #[async_trait]
    impl OutboundSink for FakeSink {
        async fn send_text(
            &self,
            conversation: &str,
            markdown: &str,
            options: SendOptions,
        ) -> Result<Vec<String>, SinkError> {
            if let Some(reason) = self.fail_with {
                return Err(SinkError::Delivery(reason.to_string()));
            }
            if !self
                .conversations
                .iter()
                .any(|item| item.id == conversation)
            {
                return Err(SinkError::UnknownConversation(conversation.to_string()));
            }
            let mut sent = self
                .sent
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            sent.push((conversation.to_string(), markdown.to_string(), options));
            Ok(vec!["1001".to_string()])
        }

        async fn send_file(
            &self,
            _conversation: &str,
            _path: &std::path::Path,
            _caption: Option<&str>,
            _as_photo: bool,
        ) -> Result<Vec<String>, SinkError> {
            Ok(vec!["2001".to_string()])
        }

        async fn react(
            &self,
            conversation: &str,
            message_id: &str,
            emoji: Option<&str>,
        ) -> Result<(), SinkError> {
            if let Some(reason) = self.fail_with {
                return Err(SinkError::Delivery(reason.to_string()));
            }
            if !self
                .conversations
                .iter()
                .any(|item| item.id == conversation)
            {
                return Err(SinkError::UnknownConversation(conversation.to_string()));
            }
            let mut reactions = self
                .reactions
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            reactions.push((
                conversation.to_string(),
                message_id.to_string(),
                emoji.map(str::to_string),
            ));
            Ok(())
        }

        async fn view_attachment(&self, handle: &str) -> Result<ViewedAttachment, SinkError> {
            match handle {
                "417" => Ok(ViewedAttachment::Image {
                    media_type: "image/png".to_string(),
                    data: "aW1hZ2U=".to_string(),
                    note: None,
                }),
                "418" => Ok(ViewedAttachment::Description(
                    "This is a document (\"q3.pdf\", application/pdf) and has no image preview."
                        .to_string(),
                )),
                other => Err(SinkError::UnknownAttachment(other.to_string())),
            }
        }

        async fn download_attachment(
            &self,
            handle: &str,
        ) -> Result<DownloadedAttachment, SinkError> {
            if handle != "418" {
                return Err(SinkError::UnknownAttachment(handle.to_string()));
            }
            Ok(DownloadedAttachment {
                path: PathBuf::from("/var/lib/mekabridge/attachments/q3.pdf"),
                bytes: 8_400_000,
                media_type: Some("application/pdf".to_string()),
            })
        }

        async fn conversations(
            &self,
            channel: Option<&str>,
            limit: usize,
        ) -> Result<Vec<ConversationSummary>, SinkError> {
            Ok(self
                .conversations
                .iter()
                .filter(|item| channel.is_none_or(|channel| item.channel == channel))
                .take(limit)
                .cloned()
                .collect())
        }

        async fn conversation(&self, id: &str) -> Result<Option<ConversationSummary>, SinkError> {
            Ok(self
                .conversations
                .iter()
                .find(|item| item.id == id)
                .cloned())
        }
    }

    fn server_with(sink: FakeSink) -> (BridgeMcpServer, Arc<FakeSink>) {
        let sink = Arc::new(sink);
        (
            BridgeMcpServer::new(Arc::clone(&sink) as Arc<dyn OutboundSink>),
            sink,
        )
    }

    fn text_of(result: &CallToolResult) -> String {
        result
            .content
            .iter()
            .filter_map(|block| block.as_text().map(|text| text.text.clone()))
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[tokio::test]
    async fn send_message_delivers_and_reports_the_message_id() {
        let (server, sink) = server_with(FakeSink {
            conversations: vec![summary("telegram:1")],
            ..FakeSink::default()
        });
        let result = server
            .send_message_inner(
                SendMessageArgs {
                    conversation: "telegram:1".to_string(),
                    text: "hello".to_string(),
                    reply_to: None,
                    silent: false,
                },
                None,
            )
            .await
            .expect("tool runs");
        assert_eq!(result.is_error, Some(false));
        assert!(text_of(&result).contains("1001"));
        let sent = sink.sent.lock().expect("lock");
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].1, "hello");
    }

    #[tokio::test]
    async fn send_message_passes_reply_and_silent_through() {
        let (server, sink) = server_with(FakeSink {
            conversations: vec![summary("telegram:1")],
            ..FakeSink::default()
        });
        server
            .send_message_inner(
                SendMessageArgs {
                    conversation: "telegram:1".to_string(),
                    text: "hi".to_string(),
                    reply_to: Some("42".to_string()),
                    silent: true,
                },
                None,
            )
            .await
            .expect("tool runs");
        let sent = sink.sent.lock().expect("lock");
        assert_eq!(sent[0].2, SendOptions {
            reply_to: Some("42".to_string()),
            silent: true,
        });
    }

    #[tokio::test]
    async fn unknown_conversation_is_a_tool_error_with_a_recovery_hint() {
        // A tool-level error rather than a protocol error, so the agent actually sees the text and
        // can go look the id up instead of getting an opaque failure.
        let (server, _sink) = server_with(FakeSink {
            conversations: vec![summary("telegram:1")],
            ..FakeSink::default()
        });
        let result = server
            .send_message_inner(
                SendMessageArgs {
                    conversation: "telegram:999".to_string(),
                    text: "hello".to_string(),
                    reply_to: None,
                    silent: false,
                },
                None,
            )
            .await
            .expect("tool runs");
        assert_eq!(result.is_error, Some(true));
        let text = text_of(&result);
        assert!(text.contains("telegram:999"));
        assert!(text.contains("list_conversations"));
    }

    #[tokio::test]
    async fn empty_text_is_rejected_without_calling_the_sink() {
        let (server, sink) = server_with(FakeSink {
            conversations: vec![summary("telegram:1")],
            ..FakeSink::default()
        });
        let result = server
            .send_message_inner(
                SendMessageArgs {
                    conversation: "telegram:1".to_string(),
                    text: "   ".to_string(),
                    reply_to: None,
                    silent: false,
                },
                None,
            )
            .await
            .expect("tool runs");
        assert_eq!(result.is_error, Some(true));
        assert!(sink.sent.lock().expect("lock").is_empty());
    }

    #[tokio::test]
    async fn delivery_failures_surface_to_the_agent() {
        let (server, _sink) = server_with(FakeSink {
            conversations: vec![summary("telegram:1")],
            fail_with: Some("chat not found"),
            ..FakeSink::default()
        });
        let result = server
            .send_message_inner(
                SendMessageArgs {
                    conversation: "telegram:1".to_string(),
                    text: "hello".to_string(),
                    reply_to: None,
                    silent: false,
                },
                None,
            )
            .await
            .expect("tool runs");
        assert_eq!(result.is_error, Some(true));
        assert!(text_of(&result).contains("chat not found"));
    }

    #[tokio::test]
    async fn send_file_rejects_relative_paths() {
        let (server, _sink) = server_with(FakeSink::default());
        let result = server
            .send_file_inner(
                SendFileArgs {
                    conversation: "telegram:1".to_string(),
                    path: "report.pdf".to_string(),
                    caption: None,
                    as_photo: false,
                },
                None,
            )
            .await
            .expect("tool runs");
        assert_eq!(result.is_error, Some(true));
        assert!(text_of(&result).contains("must be absolute"));
    }

    #[tokio::test]
    async fn send_file_rejects_a_missing_file() {
        let (server, _sink) = server_with(FakeSink::default());
        let result = server
            .send_file_inner(
                SendFileArgs {
                    conversation: "telegram:1".to_string(),
                    path: "/nonexistent/mekabridge/report.pdf".to_string(),
                    caption: None,
                    as_photo: false,
                },
                None,
            )
            .await
            .expect("tool runs");
        assert_eq!(result.is_error, Some(true));
        assert!(text_of(&result).contains("not a readable file"));
    }

    #[tokio::test]
    async fn react_attaches_an_emoji_to_a_message() {
        let (server, sink) = server_with(FakeSink {
            conversations: vec![summary("telegram:1")],
            ..FakeSink::default()
        });
        let result = server
            .react_inner(
                ReactArgs {
                    conversation: "telegram:1".to_string(),
                    message_id: "4471".to_string(),
                    emoji: Some("👍".to_string()),
                },
                None,
            )
            .await
            .expect("tool runs");
        assert_eq!(result.is_error, Some(false));
        let reactions = sink.reactions.lock().expect("lock");
        assert_eq!(reactions.as_slice(), [(
            "telegram:1".to_string(),
            "4471".to_string(),
            Some("👍".to_string())
        )]);
    }

    #[tokio::test]
    async fn omitting_the_emoji_clears_the_reaction() {
        let (server, sink) = server_with(FakeSink {
            conversations: vec![summary("telegram:1")],
            ..FakeSink::default()
        });
        let result = server
            .react_inner(
                ReactArgs {
                    conversation: "telegram:1".to_string(),
                    message_id: "4471".to_string(),
                    emoji: None,
                },
                None,
            )
            .await
            .expect("tool runs");
        assert_eq!(result.is_error, Some(false));
        assert!(text_of(&result).contains("Removed"));
        let reactions = sink.reactions.lock().expect("lock");
        assert_eq!(reactions[0].2, None);
    }

    #[tokio::test]
    async fn an_empty_emoji_is_refused_rather_than_read_as_a_clear() {
        // `emoji: ""` almost certainly means a caller built the argument wrong. Silently treating
        // it as "remove the reaction" would hide that.
        let (server, sink) = server_with(FakeSink {
            conversations: vec![summary("telegram:1")],
            ..FakeSink::default()
        });
        let result = server
            .react_inner(
                ReactArgs {
                    conversation: "telegram:1".to_string(),
                    message_id: "4471".to_string(),
                    emoji: Some("  ".to_string()),
                },
                None,
            )
            .await
            .expect("tool runs");
        assert_eq!(result.is_error, Some(true));
        assert!(sink.reactions.lock().expect("lock").is_empty());
    }

    #[tokio::test]
    async fn a_rejected_reaction_reaches_the_agent_verbatim() {
        // Telegram publishes a fixed emoji set and revises it, so the platform's own complaint is
        // the only accurate explanation available.
        let (server, _sink) = server_with(FakeSink {
            conversations: vec![summary("telegram:1")],
            fail_with: Some("REACTION_INVALID"),
            ..FakeSink::default()
        });
        let result = server
            .react_inner(
                ReactArgs {
                    conversation: "telegram:1".to_string(),
                    message_id: "4471".to_string(),
                    emoji: Some("🦀".to_string()),
                },
                None,
            )
            .await
            .expect("tool runs");
        assert_eq!(result.is_error, Some(true));
        assert!(text_of(&result).contains("REACTION_INVALID"));
    }

    #[tokio::test]
    async fn list_conversations_clamps_the_limit() {
        let conversations: Vec<ConversationSummary> = (0..300)
            .map(|index| summary(&format!("telegram:{index}")))
            .collect();
        let (server, _sink) = server_with(FakeSink {
            conversations,
            ..FakeSink::default()
        });
        let result = server
            .list_conversations(Parameters(ListConversationsArgs {
                channel: None,
                limit: Some(100_000),
            }))
            .await
            .expect("tool runs");
        let parsed: Vec<serde_json::Value> =
            serde_json::from_str(&text_of(&result)).expect("json array");
        assert_eq!(parsed.len(), MAX_CONVERSATION_LIMIT);
    }

    #[tokio::test]
    async fn list_conversations_explains_an_empty_address_book() {
        let (server, _sink) = server_with(FakeSink::default());
        let result = server
            .list_conversations(Parameters(ListConversationsArgs {
                channel: None,
                limit: None,
            }))
            .await
            .expect("tool runs");
        assert_eq!(result.is_error, Some(false));
        assert!(text_of(&result).contains("No conversations yet"));
    }

    #[tokio::test]
    async fn get_conversation_reports_unknown_ids_as_tool_errors() {
        let (server, _sink) = server_with(FakeSink::default());
        let result = server
            .get_conversation(Parameters(GetConversationArgs {
                conversation: "telegram:7".to_string(),
            }))
            .await
            .expect("tool runs");
        assert_eq!(result.is_error, Some(true));
        assert!(text_of(&result).contains("telegram:7"));
    }

    #[test]
    fn every_tool_is_registered_with_a_description() {
        let router = BridgeMcpServer::tool_router();
        let tools = router.list_all();
        let mut names: Vec<&str> = tools.iter().map(|tool| tool.name.as_ref()).collect();
        names.sort_unstable();
        // The exact set, not a spot check: a tool silently dropped from the router is a capability
        // the agent loses with nothing to indicate it, and the docs list these by name.
        assert_eq!(names, vec![
            "download_attachment",
            "get_conversation",
            "list_conversations",
            "react",
            "send_file",
            "send_message",
            "view_attachment",
        ]);
        for tool in &tools {
            assert!(
                tool.description
                    .as_ref()
                    .is_some_and(|text| !text.is_empty()),
                "tool {} has no description; the description is what the agent reads",
                tool.name
            );
        }
    }

    #[test]
    fn the_instructions_fit_inside_mekas_cap() {
        // meka truncates a server's `instructions` to `MAX_MCP_DESCRIPTION_LENGTH` at handshake and
        // appends an ellipsis. There is no error and no log line, so going over would silently cost
        // the agent whichever paragraphs happen to sit at the end, and the value is captured in a
        // `OnceLock` on first connect so it would stay lost until meka restarts.
        const MEKA_MAX_DESCRIPTION_CHARS: usize = 2048;
        let length = SERVER_INSTRUCTIONS.chars().count();
        assert!(
            length <= MEKA_MAX_DESCRIPTION_CHARS,
            "the instructions are {length} characters, over meka's {MEKA_MAX_DESCRIPTION_CHARS}; \
             the tail would be silently cut. Trim a paragraph rather than raising this."
        );
    }

    #[test]
    fn every_tool_is_read_only_so_the_agent_can_reply_at_read() {
        // meka derives a tool's required permission from `readOnlyHint` when no config overrides
        // it. If a send tool ever flips to `false` it lands at meka's `write` level, and a bridge
        // run at `read` silently becomes a bot that understands every message and answers none.
        let router = BridgeMcpServer::tool_router();
        for tool in router.list_all() {
            let annotations = tool
                .annotations
                .as_ref()
                .unwrap_or_else(|| panic!("{} has no annotations", tool.name));
            assert_eq!(
                annotations.read_only_hint,
                Some(true),
                "{} must stay read-only or the agent loses the ability to use it at `read`",
                tool.name
            );
        }
    }

    #[test]
    fn the_send_tools_declare_that_they_reach_outside_the_machine() {
        // The honest caveat that rides with `readOnlyHint: true`: these change nothing locally, but
        // they do act on the outside world.
        let router = BridgeMcpServer::tool_router();
        for tool in router.list_all() {
            let open_world = tool
                .annotations
                .as_ref()
                .and_then(|annotations| annotations.open_world_hint);
            let expected = match tool.name.as_ref() {
                // Everything that talks to the platform, in either direction.
                "send_message"
                | "send_file"
                | "react"
                | "view_attachment"
                | "download_attachment" => Some(true),
                _ => Some(false),
            };
            assert_eq!(open_world, expected, "openWorldHint for {}", tool.name);
        }
    }
}
