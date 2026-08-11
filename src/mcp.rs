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

pub use crate::channel::{ChatSettings, MemberAction, MemberInfo, MemberRight, SendOptions};

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
- `admitted:` says how the sender reached you: vetted individually, allowed only because the whole \
chat is, or not checked at all because the channel is open to everyone.
- `forwarded from:` means the text is somebody else's words, not the sender's.
- `late:` means it arrived while you were working on the previous turn, so anything you sent then \
was written without it. If it changes the answer, say so.
- `attachment:` ends with a handle in square brackets. Pass it to view_attachment to look at a \
picture, or download_attachment to get the file on disk. Fetch only what you need, since anything \
you look at stays in your context.

You can also edit or delete what you sent, react, mute a chat that keeps waking you for nothing, \
and moderate members of a group you administer.

Write Markdown; it is converted to each platform's own formatting, and long messages are split. \
Conversation ids are stable, and any id you were given works whether or not that chat has written \
to you. list_conversations shows the ones this bridge knows about and which you have muted.";

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

    /// Replace the text of a message the agent sent.
    async fn edit_message(
        &self,
        conversation: &str,
        message_id: &str,
        markdown: &str,
    ) -> Result<(), SinkError>;

    /// Remove a message.
    async fn delete_message(&self, conversation: &str, message_id: &str) -> Result<(), SinkError>;

    /// Restrict, ban, or reinstate somebody in a chat.
    async fn moderate_member(
        &self,
        conversation: &str,
        user_id: &str,
        action: MemberAction,
        until: Option<chrono::DateTime<chrono::Utc>>,
        revoke_messages: bool,
    ) -> Result<(), SinkError>;

    /// Grant exactly `rights`. An empty slice demotes.
    async fn set_member_rights(
        &self,
        conversation: &str,
        user_id: &str,
        rights: &[MemberRight],
    ) -> Result<(), SinkError>;

    /// Pin or unpin a message.
    async fn pin_message(
        &self,
        conversation: &str,
        message_id: &str,
        pin: bool,
        silent: bool,
    ) -> Result<(), SinkError>;

    /// Change chat-level settings.
    async fn set_chat(&self, conversation: &str, settings: ChatSettings) -> Result<(), SinkError>;

    /// Somebody's standing in a chat, or the bot's own when `user_id` is `None`.
    async fn member(
        &self,
        conversation: &str,
        user_id: Option<&str>,
    ) -> Result<MemberInfo, SinkError>;

    /// Retrieve an attachment for viewing, without writing it to disk.
    async fn view_attachment(&self, handle: &str) -> Result<ViewedAttachment, SinkError>;

    /// Write an attachment to local disk and report where it landed.
    async fn download_attachment(&self, handle: &str) -> Result<DownloadedAttachment, SinkError>;

    /// Stop delivering messages from a conversation. `until` of `None` is indefinite.
    async fn mute(
        &self,
        conversation: &str,
        until: Option<chrono::DateTime<chrono::Utc>>,
        reason: Option<&str>,
    ) -> Result<(), SinkError>;

    /// Resume delivery. Reports whether a mute was actually in place.
    async fn unmute(&self, conversation: &str) -> Result<bool, SinkError>;

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
    /// `direct`, `group`, `channel`, or `unknown` when the agent messaged first and nothing has
    /// arrived from there to say which.
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_inbound_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_outbound_at: Option<String>,
    /// Set while the agent is not being woken for this conversation. `"indefinite"` when no expiry
    /// was given, otherwise the time it lapses.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub muted_until: Option<String>,
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
        "{0:?} is not a conversation id; the form is <channel>:<chat>, for example \
         `telegram:123456789`"
    )]
    MalformedConversation(String),

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
    /// incoming message, but any valid id works, including one for a chat that has never messaged
    /// you.
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
pub struct EditMessageArgs {
    /// Conversation the message is in.
    pub conversation: String,
    /// Id of the message to revise. For a message you sent, this is the id reported by
    /// send_message.
    pub message_id: String,
    /// Replacement body, written as Markdown. It replaces the message entirely.
    pub text: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct DeleteMessageArgs {
    /// Conversation the message is in.
    pub conversation: String,
    /// Id of the message to delete.
    pub message_id: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ModerateMemberArgs {
    /// Chat to act in. Moderation only applies to groups and channels.
    pub conversation: String,
    /// Numeric id of the person, from the `from:` line of one of their messages.
    pub user_id: String,
    /// What to do: `restrict` stops them posting, `unrestrict` gives back what the chat allows
    /// everyone, `ban` removes and keeps them out, `unban` lifts a ban, `kick` removes them but
    /// lets them rejoin.
    pub action: MemberAction,
    /// How long, as a duration like `1h` or `7d`. Only meaningful for `restrict` and `ban`; omit
    /// for permanent. Must be between 30 seconds and 366 days, because Telegram silently treats
    /// anything outside that as permanent.
    #[serde(default)]
    pub duration: Option<String>,
    /// Also delete everything they have posted. Only for `ban` and `kick`, and it cannot be
    /// undone.
    #[serde(default)]
    pub revoke_messages: bool,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SetMemberRightsArgs {
    /// Chat to act in.
    pub conversation: String,
    /// Numeric id of the person.
    pub user_id: String,
    /// The complete set of privileges they should end up with. This replaces what they hold rather
    /// than adding to it, so an empty list demotes them to an ordinary member.
    pub rights: Vec<MemberRight>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct PinMessageArgs {
    /// Chat the message is in.
    pub conversation: String,
    /// Id of the message.
    pub message_id: String,
    /// True to pin, false to unpin.
    pub pin: bool,
    /// Pin without notifying everyone in the chat.
    #[serde(default)]
    pub silent: bool,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SetChatArgs {
    /// Chat to change.
    pub conversation: String,
    /// New title. Omit to leave it alone.
    #[serde(default)]
    pub title: Option<String>,
    /// New description. Omit to leave it alone.
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct MemberArgs {
    /// Chat to look in.
    pub conversation: String,
    /// Numeric id of the person. Omit to ask about yourself, which is how you find out what you
    /// are allowed to do in this chat.
    #[serde(default)]
    pub user_id: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct AttachmentArgs {
    /// The handle shown in square brackets on an `attachment:` line, for example `417`.
    pub attachment: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct MuteArgs {
    /// Conversation to stop hearing from.
    pub conversation: String,
    /// How long to stay muted, as a duration like `30m`, `2h`, or `7d`. Omit to mute indefinitely,
    /// which lasts until you unmute it.
    #[serde(default)]
    pub duration: Option<String>,
    /// Why, for your own reference when you list conversations later.
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct UnmuteArgs {
    /// Conversation to start hearing from again.
    pub conversation: String,
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

/// Which optional groups of tools to offer.
///
/// Removing a tool the deployment cannot use is not only tidiness: an agent that can see
/// `moderate_member` will eventually be asked to use it, and answering "I have no such tool" is a
/// worse conversation than the tool never existing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ToolSurface {
    /// Offer the group moderation tools.
    pub admin: bool,
}

impl Default for ToolSurface {
    fn default() -> Self {
        Self { admin: true }
    }
}

/// Tools removed when [`ToolSurface::admin`] is off.
const ADMIN_TOOLS: &[&str] = &[
    "moderate_member",
    "set_member_rights",
    "pin_message",
    "set_chat",
    "member",
];

/// The MCP server exposing mekabridge's outbound tools.
#[derive(Clone)]
pub struct BridgeMcpServer {
    sink: Arc<dyn OutboundSink>,
    tool_router: ToolRouter<Self>,
}

#[tool_router]
impl BridgeMcpServer {
    pub fn new(sink: Arc<dyn OutboundSink>, surface: ToolSurface) -> Self {
        // Built in full and then trimmed, because the `#[tool]` macros register at compile time and
        // there is no way to make one conditional at its definition.
        let mut tool_router = Self::tool_router();
        if !surface.admin {
            for name in ADMIN_TOOLS {
                tool_router.remove_route(name);
            }
        }
        Self { sink, tool_router }
    }

    /// Send a chat message.
    #[tool(
        description = "Send a message to a person or group on a connected messaging platform. This \
                       is how you reply to someone who messaged you, and how you message someone \
                       without being prompted. `conversation` is any valid id: from the header of \
                       an incoming message, from list_conversations, or one you were told about \
                       some other way, including a chat that has never messaged you. `text` is \
                       Markdown and is converted to the platform's own formatting; long text is \
                       split across several messages automatically.",
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

    /// Revise a message already sent.
    #[tool(
        description = "Replace the text of a message you already sent, the way a person corrects a \
                       typo rather than sending a follow-up. The new text replaces the old \
                       entirely, so include everything you want to keep. `message_id` is the id \
                       send_message reported. You can only edit your own messages, and platforms \
                       may refuse an edit to an old one.",
        annotations(title = "Edit message", read_only_hint = true, open_world_hint = true)
    )]
    async fn edit_message(
        &self,
        Parameters(args): Parameters<EditMessageArgs>,
    ) -> Result<CallToolResult, McpError> {
        if args.text.trim().is_empty() {
            // An empty edit is not a delete on any platform here; it is a rejected request. Saying
            // which tool does mean it beats letting the agent discover that from an API error.
            return Ok(CallToolResult::error(vec![ContentBlock::text(
                "`text` is empty; nothing was changed. Use delete_message to remove a message.",
            )]));
        }
        match self
            .sink
            .edit_message(&args.conversation, &args.message_id, &args.text)
            .await
        {
            Ok(()) => Ok(CallToolResult::success(vec![ContentBlock::text(format!(
                "Edited message {} in {}.",
                args.message_id, args.conversation
            ))])),
            Err(error) => Ok(sink_failure(&error)),
        }
    }

    /// Remove a message.
    #[tool(
        description = "Delete a message. Use it to retract something you sent, or, where you are a \
                       moderator, to remove somebody else's. This cannot be undone and the message \
                       disappears for everyone, so prefer edit_message when you only want to \
                       correct yourself.",
        annotations(
            title = "Delete message",
            read_only_hint = true,
            open_world_hint = true
        )
    )]
    async fn delete_message(
        &self,
        Parameters(args): Parameters<DeleteMessageArgs>,
    ) -> Result<CallToolResult, McpError> {
        match self
            .sink
            .delete_message(&args.conversation, &args.message_id)
            .await
        {
            Ok(()) => Ok(CallToolResult::success(vec![ContentBlock::text(format!(
                "Deleted message {} in {}.",
                args.message_id, args.conversation
            ))])),
            Err(error) => Ok(sink_failure(&error)),
        }
    }

    /// Restrict, ban, or reinstate somebody.
    #[tool(
        description = "Moderate a member of a group you administer: `restrict` stops them posting \
                       but leaves them in, `unrestrict` gives back whatever the group allows \
                       everyone, `ban` removes and keeps them out, `unban` lifts a ban, `kick` \
                       removes them but lets them rejoin. `user_id` is the numeric id from the \
                       `from:` line of their message. Needs the matching admin right in that group, \
                       and no bot can act on another administrator.",
        annotations(
            title = "Moderate member",
            read_only_hint = true,
            open_world_hint = true
        )
    )]
    async fn moderate_member(
        &self,
        Parameters(args): Parameters<ModerateMemberArgs>,
    ) -> Result<CallToolResult, McpError> {
        let until = match parse_duration(args.duration.as_deref()) {
            Ok(until) => until,
            Err(message) => return Ok(CallToolResult::error(vec![ContentBlock::text(message)])),
        };
        // Saying so beats silently ignoring it: an agent that asked for a one-hour unban and got a
        // permanent one would have no way to tell.
        if until.is_some() && !args.action.accepts_duration() {
            return Ok(CallToolResult::error(vec![ContentBlock::text(format!(
                "`duration` means nothing for `{}`; it applies to `restrict` and `ban` only.",
                args.action.as_str()
            ))]));
        }
        match self
            .sink
            .moderate_member(
                &args.conversation,
                &args.user_id,
                args.action,
                until,
                args.revoke_messages,
            )
            .await
        {
            Ok(()) => {
                let window = match until {
                    Some(until) => format!(" until {}", until.to_rfc3339()),
                    None => String::new(),
                };
                Ok(CallToolResult::success(vec![ContentBlock::text(format!(
                    "Applied `{}` to user {} in {}{window}.",
                    args.action.as_str(),
                    args.user_id,
                    args.conversation
                ))]))
            }
            Err(error) => Ok(sink_failure(&error)),
        }
    }

    /// Promote, adjust, or demote an administrator.
    #[tool(
        description = "Set exactly which admin privileges somebody holds in a group. This replaces \
                       what they have rather than adding to it, so pass the complete set you want \
                       them to end up with, and pass an empty list to demote them to an ordinary \
                       member. You can only grant privileges you hold yourself.",
        annotations(
            title = "Set member rights",
            read_only_hint = true,
            open_world_hint = true
        )
    )]
    async fn set_member_rights(
        &self,
        Parameters(args): Parameters<SetMemberRightsArgs>,
    ) -> Result<CallToolResult, McpError> {
        match self
            .sink
            .set_member_rights(&args.conversation, &args.user_id, &args.rights)
            .await
        {
            Ok(()) => Ok(CallToolResult::success(vec![ContentBlock::text(
                if args.rights.is_empty() {
                    format!(
                        "Demoted user {} in {} to an ordinary member.",
                        args.user_id, args.conversation
                    )
                } else {
                    let granted: Vec<&str> =
                        args.rights.iter().map(|right| right.as_str()).collect();
                    format!(
                        "User {} in {} now holds: {}.",
                        args.user_id,
                        args.conversation,
                        granted.join(", ")
                    )
                },
            )])),
            Err(error) => Ok(sink_failure(&error)),
        }
    }

    /// Pin or unpin a message.
    #[tool(
        description = "Pin a message to the top of a chat, or unpin one. Pinning notifies everyone \
                       unless `silent` is set. Needs the pin-messages admin right.",
        annotations(title = "Pin message", read_only_hint = true, open_world_hint = true)
    )]
    async fn pin_message(
        &self,
        Parameters(args): Parameters<PinMessageArgs>,
    ) -> Result<CallToolResult, McpError> {
        match self
            .sink
            .pin_message(&args.conversation, &args.message_id, args.pin, args.silent)
            .await
        {
            Ok(()) => Ok(CallToolResult::success(vec![ContentBlock::text(format!(
                "{} message {} in {}.",
                if args.pin { "Pinned" } else { "Unpinned" },
                args.message_id,
                args.conversation
            ))])),
            Err(error) => Ok(sink_failure(&error)),
        }
    }

    /// Change a chat's title or description.
    #[tool(
        description = "Change a group's title or description. Omit a field to leave it as it is. \
                       Needs the change-info admin right.",
        annotations(
            title = "Set chat details",
            read_only_hint = true,
            open_world_hint = true
        )
    )]
    async fn set_chat(
        &self,
        Parameters(args): Parameters<SetChatArgs>,
    ) -> Result<CallToolResult, McpError> {
        let settings = ChatSettings {
            title: args.title,
            description: args.description,
        };
        if settings.is_empty() {
            return Ok(CallToolResult::error(vec![ContentBlock::text(
                "Nothing to change; set `title`, `description`, or both.",
            )]));
        }
        match self.sink.set_chat(&args.conversation, settings).await {
            Ok(()) => Ok(CallToolResult::success(vec![ContentBlock::text(format!(
                "Updated {}.",
                args.conversation
            ))])),
            Err(error) => Ok(sink_failure(&error)),
        }
    }

    /// Check somebody's standing in a chat, including your own.
    #[tool(
        description = "Look up somebody's standing in a chat and which admin privileges they hold. \
                       Omit `user_id` to ask about yourself, which is how you find out what you \
                       are allowed to do in a group before trying it.",
        annotations(
            title = "Check membership",
            read_only_hint = true,
            open_world_hint = true
        )
    )]
    async fn member(
        &self,
        Parameters(args): Parameters<MemberArgs>,
    ) -> Result<CallToolResult, McpError> {
        match self
            .sink
            .member(&args.conversation, args.user_id.as_deref())
            .await
        {
            Ok(member) => Ok(json_result(&member)),
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

    /// Stop being woken by a conversation.
    #[tool(
        description = "Stop receiving messages from a conversation, so a noisy chat does not \
                       interrupt you. Messages sent while it is muted are discarded rather than \
                       held, and you are told how many when the mute lapses. `duration` is \
                       something like `30m`, `2h`, or `7d`; omit it to mute until you unmute. Use \
                       this on a group that keeps waking you for nothing, not on someone who is \
                       simply asking for something you would rather not do.",
        annotations(
            title = "Mute conversation",
            read_only_hint = true,
            open_world_hint = false
        )
    )]
    async fn mute(
        &self,
        Parameters(args): Parameters<MuteArgs>,
    ) -> Result<CallToolResult, McpError> {
        let until = match parse_duration(args.duration.as_deref()) {
            Ok(until) => until,
            Err(message) => return Ok(CallToolResult::error(vec![ContentBlock::text(message)])),
        };
        match self
            .sink
            .mute(&args.conversation, until, args.reason.as_deref())
            .await
        {
            Ok(()) => Ok(CallToolResult::success(vec![ContentBlock::text(
                match until {
                    Some(until) => format!(
                        "Muted {} until {}. Messages sent before then are discarded, not held.",
                        args.conversation,
                        until.to_rfc3339()
                    ),
                    None => format!(
                        "Muted {} indefinitely. Nothing from it will reach you until you unmute \
                         it.",
                        args.conversation
                    ),
                },
            )])),
            Err(error) => Ok(sink_failure(&error)),
        }
    }

    /// Start hearing from a conversation again.
    #[tool(
        description = "Lift a mute so a conversation can reach you again. Messages sent while it \
                       was muted are gone; this only affects what arrives from now on.",
        annotations(
            title = "Unmute conversation",
            read_only_hint = true,
            open_world_hint = false
        )
    )]
    async fn unmute(
        &self,
        Parameters(args): Parameters<UnmuteArgs>,
    ) -> Result<CallToolResult, McpError> {
        match self.sink.unmute(&args.conversation).await {
            Ok(true) => Ok(CallToolResult::success(vec![ContentBlock::text(format!(
                "Unmuted {}.",
                args.conversation
            ))])),
            Ok(false) => Ok(CallToolResult::success(vec![ContentBlock::text(format!(
                "{} was not muted; nothing changed.",
                args.conversation
            ))])),
            Err(error) => Ok(sink_failure(&error)),
        }
    }

    /// List known conversations.
    #[tool(
        description = "List the conversations this bridge knows about, most recently active first. \
                       Use it to find a conversation id when you want to message someone whose id \
                       is not in front of you, and to see which conversations you currently have \
                       muted.",
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
                    "No conversations yet. One appears here once somebody has messaged the bot, or \
                     once you have messaged them. You can still send to an id you know without \
                     it being listed.",
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

/// Resolve a humantime duration argument into an absolute expiry.
///
/// `Ok(None)` means the caller omitted it, which every tool taking one reads as "no expiry". That
/// is why an unparseable or empty value is an error rather than a fall back to `None`: a mistyped
/// half-hour silently becoming permanent is the worst outcome any of these tools has, and it is
/// invisible from both ends.
fn parse_duration(
    raw: Option<&str>,
) -> std::result::Result<Option<chrono::DateTime<chrono::Utc>>, String> {
    let Some(raw) = raw.map(str::trim) else {
        return Ok(None);
    };
    if raw.is_empty() {
        return Err("`duration` is empty; omit it entirely for no expiry.".to_string());
    }
    let parsed = humantime::parse_duration(raw).map_err(|error| {
        format!(
            "`duration` {raw:?} is not a duration ({error}); write it like `30m`, `2h`, or `7d`."
        )
    })?;
    let parsed = chrono::Duration::from_std(parsed).map_err(|_| {
        format!("`duration` {raw:?} is too long to represent; omit it for no expiry.")
    })?;
    Ok(Some(chrono::Utc::now() + parsed))
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
    use crate::channel::ConversationId;

    /// A recorded `mute` call: conversation, expiry, reason.
    type RecordedMute = (
        String,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<String>,
    );

    /// A recorded `moderate_member` call: conversation, user, action, expiry.
    type RecordedModeration = (
        String,
        String,
        MemberAction,
        Option<chrono::DateTime<chrono::Utc>>,
    );

    #[derive(Default)]
    struct FakeSink {
        sent: Mutex<Vec<(String, String, SendOptions)>>,
        reactions: Mutex<Vec<(String, String, Option<String>)>>,
        edits: Mutex<Vec<(String, String, String)>>,
        deletes: Mutex<Vec<(String, String)>>,
        mutes: Mutex<Vec<RecordedMute>>,
        moderations: Mutex<Vec<RecordedModeration>>,
        promotions: Mutex<Vec<(String, Vec<MemberRight>)>>,
        pins: Mutex<Vec<(String, bool)>>,
        chat_settings: Mutex<Vec<ChatSettings>>,
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
            muted_until: None,
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
            // Deliberately not checked against `conversations`: the real sink sends to any id its
            // channel accepts, seen before or not, and leaves the verdict to the platform.
            if ConversationId::parse(conversation).is_none() {
                return Err(SinkError::MalformedConversation(conversation.to_string()));
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

        async fn edit_message(
            &self,
            conversation: &str,
            message_id: &str,
            markdown: &str,
        ) -> Result<(), SinkError> {
            if let Some(reason) = self.fail_with {
                return Err(SinkError::Delivery(reason.to_string()));
            }
            let mut edits = self
                .edits
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            edits.push((
                conversation.to_string(),
                message_id.to_string(),
                markdown.to_string(),
            ));
            Ok(())
        }

        async fn delete_message(
            &self,
            conversation: &str,
            message_id: &str,
        ) -> Result<(), SinkError> {
            if let Some(reason) = self.fail_with {
                return Err(SinkError::Delivery(reason.to_string()));
            }
            let mut deletes = self
                .deletes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            deletes.push((conversation.to_string(), message_id.to_string()));
            Ok(())
        }

        async fn moderate_member(
            &self,
            conversation: &str,
            user_id: &str,
            action: MemberAction,
            until: Option<chrono::DateTime<chrono::Utc>>,
            _revoke_messages: bool,
        ) -> Result<(), SinkError> {
            if let Some(reason) = self.fail_with {
                return Err(SinkError::Delivery(reason.to_string()));
            }
            let mut moderations = self
                .moderations
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            moderations.push((conversation.to_string(), user_id.to_string(), action, until));
            Ok(())
        }

        async fn set_member_rights(
            &self,
            _conversation: &str,
            user_id: &str,
            rights: &[MemberRight],
        ) -> Result<(), SinkError> {
            let mut promotions = self
                .promotions
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            promotions.push((user_id.to_string(), rights.to_vec()));
            Ok(())
        }

        async fn pin_message(
            &self,
            _conversation: &str,
            message_id: &str,
            pin: bool,
            _silent: bool,
        ) -> Result<(), SinkError> {
            let mut pins = self
                .pins
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            pins.push((message_id.to_string(), pin));
            Ok(())
        }

        async fn set_chat(
            &self,
            _conversation: &str,
            settings: ChatSettings,
        ) -> Result<(), SinkError> {
            let mut chats = self
                .chat_settings
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            chats.push(settings);
            Ok(())
        }

        async fn member(
            &self,
            _conversation: &str,
            user_id: Option<&str>,
        ) -> Result<MemberInfo, SinkError> {
            Ok(MemberInfo {
                user_id: user_id.unwrap_or("42").to_string(),
                display_name: Some("Bot".to_string()),
                status: crate::channel::MemberStatus::Administrator,
                rights: vec![MemberRight::RestrictMembers, MemberRight::DeleteMessages],
            })
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

        async fn mute(
            &self,
            conversation: &str,
            until: Option<chrono::DateTime<chrono::Utc>>,
            reason: Option<&str>,
        ) -> Result<(), SinkError> {
            if let Some(reason) = self.fail_with {
                return Err(SinkError::Delivery(reason.to_string()));
            }
            let mut mutes = self
                .mutes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            mutes.push((conversation.to_string(), until, reason.map(str::to_string)));
            Ok(())
        }

        async fn unmute(&self, conversation: &str) -> Result<bool, SinkError> {
            let mut mutes = self
                .mutes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let before = mutes.len();
            mutes.retain(|(muted, ..)| muted != conversation);
            Ok(mutes.len() != before)
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
            BridgeMcpServer::new(
                Arc::clone(&sink) as Arc<dyn OutboundSink>,
                ToolSurface::default(),
            ),
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
    async fn a_conversation_the_bridge_has_never_seen_is_still_deliverable() {
        // Sending first is a supported move, so an id that is merely unfamiliar must not be
        // second-guessed here. Only the platform knows whether the chat is writable.
        let (server, sink) = server_with(FakeSink {
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
        assert_eq!(result.is_error, Some(false));
        assert_eq!(sink.sent.lock().expect("lock").len(), 1);
    }

    #[tokio::test]
    async fn a_malformed_conversation_id_is_a_tool_error_with_a_recovery_hint() {
        // A tool-level error rather than a protocol error, so the agent actually sees the text and
        // can fix the id instead of getting an opaque failure.
        let (server, sink) = server_with(FakeSink::default());
        let result = server
            .send_message_inner(
                SendMessageArgs {
                    conversation: "not-an-id".to_string(),
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
        assert!(text.contains("not-an-id"));
        assert!(text.contains("<channel>:<chat>"), "got: {text}");
        assert!(sink.sent.lock().expect("lock").is_empty());
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
    async fn editing_replaces_the_text_of_a_message() {
        let (server, sink) = server_with(FakeSink::default());
        let result = server
            .edit_message(Parameters(EditMessageArgs {
                conversation: "telegram:1".to_string(),
                message_id: "4471".to_string(),
                text: "actually, **tomorrow**".to_string(),
            }))
            .await
            .expect("tool runs");
        assert_eq!(result.is_error, Some(false));
        assert_eq!(sink.edits.lock().expect("lock").as_slice(), [(
            "telegram:1".to_string(),
            "4471".to_string(),
            "actually, **tomorrow**".to_string()
        )]);
    }

    #[tokio::test]
    async fn an_empty_edit_is_refused_rather_than_read_as_a_deletion() {
        // Clearing a message is not how any platform here spells "delete", so an empty body would
        // either fail at the API or leave a blank message standing.
        let (server, sink) = server_with(FakeSink::default());
        let result = server
            .edit_message(Parameters(EditMessageArgs {
                conversation: "telegram:1".to_string(),
                message_id: "4471".to_string(),
                text: "   ".to_string(),
            }))
            .await
            .expect("tool runs");
        assert_eq!(result.is_error, Some(true));
        assert!(text_of(&result).contains("delete_message"));
        assert!(sink.edits.lock().expect("lock").is_empty());
    }

    #[tokio::test]
    async fn deleting_removes_a_message() {
        let (server, sink) = server_with(FakeSink::default());
        let result = server
            .delete_message(Parameters(DeleteMessageArgs {
                conversation: "telegram:1".to_string(),
                message_id: "4471".to_string(),
            }))
            .await
            .expect("tool runs");
        assert_eq!(result.is_error, Some(false));
        assert_eq!(sink.deletes.lock().expect("lock").as_slice(), [(
            "telegram:1".to_string(),
            "4471".to_string()
        )]);
    }

    #[tokio::test]
    async fn a_refused_edit_reaches_the_agent_verbatim() {
        // Telegram refuses an edit to a message older than 48 hours, among other cases. Its own
        // wording is the only accurate explanation available.
        let (server, _sink) = server_with(FakeSink {
            fail_with: Some("message can't be edited"),
            ..FakeSink::default()
        });
        let result = server
            .edit_message(Parameters(EditMessageArgs {
                conversation: "telegram:1".to_string(),
                message_id: "4471".to_string(),
                text: "too late".to_string(),
            }))
            .await
            .expect("tool runs");
        assert_eq!(result.is_error, Some(true));
        assert!(text_of(&result).contains("can't be edited"));
    }

    #[tokio::test]
    async fn muting_with_a_duration_sets_an_expiry() {
        let (server, sink) = server_with(FakeSink::default());
        let before = chrono::Utc::now();
        let result = server
            .mute(Parameters(MuteArgs {
                conversation: "telegram:1".to_string(),
                duration: Some("30m".to_string()),
                reason: Some("standup spam".to_string()),
            }))
            .await
            .expect("tool runs");
        assert_eq!(result.is_error, Some(false));
        let mutes = sink.mutes.lock().expect("lock");
        let until = mutes[0].1.expect("an expiry was set");
        let elapsed = until - before;
        assert!(
            elapsed >= chrono::Duration::minutes(29) && elapsed <= chrono::Duration::minutes(31),
            "expected roughly 30 minutes, got {elapsed}"
        );
    }

    #[tokio::test]
    async fn muting_without_a_duration_is_indefinite() {
        let (server, sink) = server_with(FakeSink::default());
        let result = server
            .mute(Parameters(MuteArgs {
                conversation: "telegram:1".to_string(),
                duration: None,
                reason: None,
            }))
            .await
            .expect("tool runs");
        assert!(text_of(&result).contains("indefinitely"));
        assert_eq!(sink.mutes.lock().expect("lock")[0].1, None);
    }

    #[tokio::test]
    async fn an_unparseable_duration_is_refused_rather_than_treated_as_forever() {
        // Reading a bad duration as "no expiry" would turn a mistyped half-hour into a permanent
        // silence that only an operator can lift.
        let (server, sink) = server_with(FakeSink::default());
        for duration in ["half an hour", "  "] {
            let result = server
                .mute(Parameters(MuteArgs {
                    conversation: "telegram:1".to_string(),
                    duration: Some(duration.to_string()),
                    reason: None,
                }))
                .await
                .expect("tool runs");
            assert_eq!(result.is_error, Some(true), "for {duration:?}");
        }
        assert!(sink.mutes.lock().expect("lock").is_empty());
    }

    #[tokio::test]
    async fn unmuting_says_so_when_nothing_was_muted() {
        // Reporting success for a no-op would let the agent believe it had fixed something.
        let (server, _sink) = server_with(FakeSink::default());
        let result = server
            .unmute(Parameters(UnmuteArgs {
                conversation: "telegram:1".to_string(),
            }))
            .await
            .expect("tool runs");
        assert_eq!(result.is_error, Some(false));
        assert!(text_of(&result).contains("was not muted"));
    }

    #[tokio::test]
    async fn moderating_passes_the_action_and_expiry_through() {
        let (server, sink) = server_with(FakeSink::default());
        let result = server
            .moderate_member(Parameters(ModerateMemberArgs {
                conversation: "telegram:-100".to_string(),
                user_id: "999".to_string(),
                action: MemberAction::Restrict,
                duration: Some("1h".to_string()),
                revoke_messages: false,
            }))
            .await
            .expect("tool runs");
        assert_eq!(result.is_error, Some(false));
        let moderations = sink.moderations.lock().expect("lock");
        assert_eq!(moderations[0].1, "999");
        assert_eq!(moderations[0].2, MemberAction::Restrict);
        assert!(moderations[0].3.is_some());
    }

    #[tokio::test]
    async fn a_duration_on_an_action_that_ignores_it_is_refused() {
        // Telegram would accept and discard it, leaving an agent that asked for a one-hour unban
        // believing it had got one.
        let (server, sink) = server_with(FakeSink::default());
        for action in [
            MemberAction::Unban,
            MemberAction::Kick,
            MemberAction::Unrestrict,
        ] {
            let result = server
                .moderate_member(Parameters(ModerateMemberArgs {
                    conversation: "telegram:-100".to_string(),
                    user_id: "999".to_string(),
                    action,
                    duration: Some("1h".to_string()),
                    revoke_messages: false,
                }))
                .await
                .expect("tool runs");
            assert_eq!(result.is_error, Some(true), "for {}", action.as_str());
            assert!(text_of(&result).contains(action.as_str()));
        }
        assert!(sink.moderations.lock().expect("lock").is_empty());
    }

    #[tokio::test]
    async fn an_empty_rights_list_reads_as_a_demotion() {
        // The list is a replacement rather than an addition, so "no rights" is meaningful input and
        // must not be mistaken for a missing argument.
        let (server, sink) = server_with(FakeSink::default());
        let result = server
            .set_member_rights(Parameters(SetMemberRightsArgs {
                conversation: "telegram:-100".to_string(),
                user_id: "999".to_string(),
                rights: Vec::new(),
            }))
            .await
            .expect("tool runs");
        assert_eq!(result.is_error, Some(false));
        assert!(text_of(&result).contains("Demoted"));
        assert_eq!(sink.promotions.lock().expect("lock")[0].1, Vec::new());
    }

    #[tokio::test]
    async fn granting_rights_reports_what_was_granted() {
        let (server, sink) = server_with(FakeSink::default());
        let result = server
            .set_member_rights(Parameters(SetMemberRightsArgs {
                conversation: "telegram:-100".to_string(),
                user_id: "999".to_string(),
                rights: vec![MemberRight::DeleteMessages, MemberRight::PinMessages],
            }))
            .await
            .expect("tool runs");
        assert!(text_of(&result).contains("delete_messages, pin_messages"));
        assert_eq!(sink.promotions.lock().expect("lock")[0].1.len(), 2);
    }

    #[tokio::test]
    async fn setting_nothing_on_a_chat_is_refused() {
        let (server, sink) = server_with(FakeSink::default());
        let result = server
            .set_chat(Parameters(SetChatArgs {
                conversation: "telegram:-100".to_string(),
                title: None,
                description: None,
            }))
            .await
            .expect("tool runs");
        assert_eq!(result.is_error, Some(true));
        assert!(sink.chat_settings.lock().expect("lock").is_empty());
    }

    #[tokio::test]
    async fn checking_your_own_membership_needs_no_user_id() {
        let (server, _sink) = server_with(FakeSink::default());
        let result = server
            .member(Parameters(MemberArgs {
                conversation: "telegram:-100".to_string(),
                user_id: None,
            }))
            .await
            .expect("tool runs");
        assert_eq!(result.is_error, Some(false));
        let parsed: serde_json::Value =
            serde_json::from_str(&text_of(&result)).expect("json object");
        assert_eq!(parsed["status"], "administrator");
        assert!(
            parsed["rights"]
                .as_array()
                .is_some_and(|rights| rights.contains(&serde_json::json!("restrict_members")))
        );
    }

    #[test]
    fn turning_off_admin_tools_removes_exactly_the_admin_tools() {
        let full = BridgeMcpServer::new(
            Arc::new(FakeSink::default()) as Arc<dyn OutboundSink>,
            ToolSurface::default(),
        );
        let trimmed = BridgeMcpServer::new(
            Arc::new(FakeSink::default()) as Arc<dyn OutboundSink>,
            ToolSurface { admin: false },
        );
        let names = |server: &BridgeMcpServer| -> Vec<String> {
            server
                .tool_router
                .list_all()
                .iter()
                .map(|tool| tool.name.to_string())
                .collect()
        };
        let full_names = names(&full);
        let trimmed_names = names(&trimmed);
        // Every name in `ADMIN_TOOLS` has to match a real tool, or the trimming silently does
        // nothing and the surface stays open when the operator asked for it closed.
        for name in ADMIN_TOOLS {
            assert!(
                full_names.iter().any(|tool| tool == name),
                "{name} is listed for removal but is not a registered tool"
            );
            assert!(
                !trimmed_names.iter().any(|tool| tool == name),
                "{name} survived the trim"
            );
        }
        assert_eq!(full_names.len() - trimmed_names.len(), ADMIN_TOOLS.len());
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
            "delete_message",
            "download_attachment",
            "edit_message",
            "get_conversation",
            "list_conversations",
            "member",
            "moderate_member",
            "mute",
            "pin_message",
            "react",
            "send_file",
            "send_message",
            "set_chat",
            "set_member_rights",
            "unmute",
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
                // Everything that talks to the platform, in either direction. `mute` and `unmute`
                // are not here on purpose: they change what this bridge delivers to the agent and
                // touch nothing outside the machine. `member` is, because it reads live state from
                // the platform even though it changes nothing.
                "send_message"
                | "send_file"
                | "react"
                | "edit_message"
                | "delete_message"
                | "moderate_member"
                | "set_member_rights"
                | "pin_message"
                | "set_chat"
                | "member"
                | "view_attachment"
                | "download_attachment" => Some(true),
                _ => Some(false),
            };
            assert_eq!(open_world, expected, "openWorldHint for {}", tool.name);
        }
    }
}
