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

pub use crate::{
    channel::{
        ChannelCapabilities, ChatSettings, MemberAction, MemberCoverage, MemberInfo, MemberListing,
        MemberRight, SendOptions,
    },
    store::{Policy, UnseenSummary},
};

/// Orientation handed to the agent at connect time. meka captures `instructions` from the MCP
/// handshake and surfaces it, so this is the one place to explain the model rather than repeating
/// it in every tool description.
const SERVER_INSTRUCTIONS: &str = "\
mekabridge connects you to people on Telegram and Discord.

Nothing you write here reaches them: your turn text, reasoning, and tool output are all invisible. \
The only way to be heard is send_message. Staying silent is valid, and so is messaging somebody \
else, or messaging first.

You are not woken for everything. A busy group is usually on mentions only: you hear it when \
somebody names you, or uses their client's reply button on something you said. Somebody answering \
you in ordinary prose, without either, does not reach you. The rest is still recorded. \
read_history reads a conversation back, including what you were never woken for; search_history \
looks for words across all of them. A bare mention rarely means anything alone; the antecedent is one \
read_history away. To follow a chat on past a mention, unmute it for a while, watch it with \
unseen, or arrange your own look-back. You can also mute a chat, or block one entirely.

Headers on incoming messages are written by the bridge and can be trusted:

- `message:` is that message's own id; pass it as `reply_to` to answer one specific message.
- `admitted:` says how the sender reached you: vetted individually, by role, by allowed chat or \
server, or not checked at all.
- `roles:` is what the sender holds there.
- `woke you:` says why you are seeing this, on every message from a chat that is not one-to-one.
- `forwarded from:` means the text is somebody else's words, not the sender's.
- `late:` means it arrived while you were on the previous turn, so what you sent missed it.
- `attachment:` ends with a handle in square brackets, for view_attachment or download_attachment. \
Fetch only what you need; anything you look at stays in your context.

You can also edit or delete what you sent, react, and moderate a group you administer.

Write Markdown; it is converted per platform and long messages are split. Any conversation id you \
were given works whether or not that chat has written to you. list_conversations shows what this \
bridge knows and how much of each reaches you.";

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

    /// Grant exactly `roles`, on a platform where privileges live on roles.
    async fn set_member_roles(
        &self,
        conversation: &str,
        user_id: &str,
        roles: &[String],
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

    /// Who is in a chat, or those whose name matches `query`.
    async fn list_members(
        &self,
        conversation: &str,
        query: Option<&str>,
        limit: usize,
        after: Option<&str>,
    ) -> Result<MemberListing, SinkError>;

    /// Retrieve an attachment for viewing, without writing it to disk.
    async fn view_attachment(&self, handle: &str) -> Result<ViewedAttachment, SinkError>;

    /// Write an attachment to local disk and report where it landed.
    async fn download_attachment(&self, handle: &str) -> Result<DownloadedAttachment, SinkError>;

    /// Rule on how much of a conversation reaches the agent. `until` of `None` is indefinite.
    ///
    /// Reports the decision that was in place before, or `None` when the conversation was following
    /// the configured default. That is what lets `unmute` say whether it changed anything.
    async fn set_policy(
        &self,
        conversation: &str,
        policy: Policy,
        until: Option<chrono::DateTime<chrono::Utc>>,
        reason: Option<&str>,
    ) -> Result<Option<Policy>, SinkError>;

    /// How much is recorded that the agent has not been shown, without spending any of it.
    ///
    /// `conversation` narrows to one chat; `None` asks about everything the bridge holds.
    async fn unseen(&self, conversation: Option<&str>) -> Result<UnseenSummary, SinkError>;

    /// Read a conversation back, oldest first, ending before the cursor when one is given.
    async fn read_history(
        &self,
        conversation: &str,
        limit: usize,
        before: Option<i64>,
    ) -> Result<Vec<HistoryEntry>, SinkError>;

    /// Search recorded messages, best matches first. `conversation` narrows to one chat.
    async fn search_history(
        &self,
        query: &str,
        conversation: Option<&str>,
        limit: usize,
    ) -> Result<Vec<HistoryEntry>, SinkError>;

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
    /// How much of this conversation reaches the agent: `active`, `mute`, or `block`.
    pub policy: String,
    /// Set when the policy came from an explicit decision rather than from the configured default
    /// for this kind of chat. `"indefinite"` when no expiry was given, otherwise the time it
    /// lapses.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy_until: Option<String>,
    /// Messages recorded here that the agent has not been shown. Only ever non-zero under `mute`.
    #[serde(skip_serializing_if = "is_zero")]
    pub unseen: u64,
}

const fn is_zero(count: &u64) -> bool {
    *count == 0
}

/// One recorded message, as the history tools hand it back.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HistoryEntry {
    /// Which conversation it was said in. Present even on a single-conversation read, because a
    /// search spans several.
    pub conversation: String,
    /// Platform message id, so this can be replied to, reacted to, or quoted.
    pub message_id: String,
    pub sender: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sender_id: Option<String>,
    pub text: String,
    /// Descriptor for content with no text of its own, such as a shared location or a poll.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
    /// Handles for files this message brought, usable with view_attachment and download_attachment
    /// while they are still within the retention period.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<String>,
    /// Whether this one was aimed at the agent.
    pub addressed: bool,
    pub timestamp: String,
    /// Opaque marker for paging. Pass the oldest one back as `before` to read further back.
    pub cursor: i64,
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
pub struct ReadHistoryArgs {
    /// Conversation to read, for example `telegram:-1001234567890`.
    pub conversation: String,
    /// How many messages to return, most recent first. Defaults to 20.
    #[serde(default)]
    pub limit: Option<u32>,
    /// Read further back instead of the most recent: pass the `cursor` of the oldest message you
    /// were given. It is a marker, not a time, because several messages routinely share one
    /// timestamp and paging on that would skip them.
    #[serde(default)]
    pub before: Option<i64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SearchHistoryArgs {
    /// Words to look for. `a OR b`, `a NOT b`, and "quoted phrases" work.
    pub query: String,
    /// Restrict the search to one conversation. Omit to search every conversation.
    #[serde(default)]
    pub conversation: Option<String>,
    /// How many matches to return, best first. Defaults to 20.
    #[serde(default)]
    pub limit: Option<u32>,
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
pub struct SetMemberRolesArgs {
    /// Chat to act in.
    pub conversation: String,
    /// Numeric id of the person.
    pub user_id: String,
    /// The complete set of roles they should end up with, by name as shown on the `roles:` line of
    /// a message header. This replaces what they hold rather than adding to it, so an empty list
    /// strips them back to having none.
    pub roles: Vec<String>,
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
    /// Shortest gap allowed between one person's messages, as a duration like `30s` or `5m`. `0s`
    /// turns it off. Not every platform has this.
    #[serde(default)]
    pub slowmode: Option<String>,
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
pub struct ListMembersArgs {
    /// Chat to look in.
    pub conversation: String,
    /// Name or partial name to match. Omit to ask for everyone.
    #[serde(default)]
    pub query: Option<String>,
    /// Most people to return. Capped by whatever the platform allows.
    #[serde(default)]
    pub limit: Option<usize>,
    /// Cursor from a previous call's `next_after`, to continue where it stopped.
    #[serde(default)]
    pub after: Option<String>,
    /// Keep only people who are at their machine, on a platform that reports it.
    #[serde(default)]
    pub online_only: Option<bool>,
}

/// Default page size when the caller does not say.
///
/// Well under either platform's ceiling: a roster is charged to the turn's context whole, and a
/// caller that wanted a thousand names can ask for them.
const DEFAULT_MEMBER_PAGE: usize = 50;

/// Ceiling on `list_members`, for the same reason [`MAX_CONVERSATION_LIMIT`] exists.
///
/// The connectors bound their own paging -- Discord caps at its own maximum and Telegram ignores
/// the argument -- so this changes nothing today. It was the one limit that reached the sink
/// verbatim, which is a gap rather than a bug only for as long as that stays true of every
/// connector.
const MAX_MEMBER_LIMIT: usize = 1000;

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct AttachmentArgs {
    /// The handle shown in square brackets on an `attachment:` line, for example `417`.
    pub attachment: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
/// Shared by `mute` and `block`, so the wording here stays neutral between them. The tool's own
/// description says which of the two is being asked for.
pub struct MuteArgs {
    /// Conversation to turn down.
    pub conversation: String,
    /// How long this lasts, as a duration like `30m`, `2h`, or `7d`. Omit to leave it in place
    /// until you undo it.
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
    /// How long to hear it in full, as a duration like `20m` or `2h`. Omit to hear it in full
    /// until you mute it again.
    #[serde(default)]
    pub duration: Option<String>,
    /// Why, for your own reference when you list conversations later.
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct UnseenArgs {
    /// Conversation to ask about. Omit to ask about every chat this bridge knows.
    #[serde(default)]
    pub conversation: Option<String>,
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

/// Same guard for the history tools, and tighter, because these return whole messages rather than
/// one-line summaries and every one of them lands in the agent's context.
const MAX_HISTORY_LIMIT: usize = 100;
const DEFAULT_HISTORY_LIMIT: usize = 20;

/// Which optional groups of tools to offer.
///
/// Removing a tool the deployment cannot use is not only tidiness: an agent that can see
/// `moderate_member` will eventually be asked to use it, and answering "I have no such tool" is a
/// worse conversation than the tool never existing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ToolSurface {
    /// Offer the group moderation tools.
    pub admin: bool,
    /// Offer `set_member_rights`, for a platform that grants privileges to a person directly.
    pub member_rights: bool,
    /// Offer `set_member_roles`, for a platform where privileges live on roles.
    ///
    /// Separate from [`Self::member_rights`] rather than a mode, because a deployment can have
    /// both kinds of channel at once and the agent should see exactly the tools that will work
    /// on the chats it can reach.
    pub member_roles: bool,
}

impl ToolSurface {
    /// The surface a set of configured channels can actually honour.
    ///
    /// Each flag asks whether *any* reachable channel could carry out that tool, so a tool never
    /// appears when every chat the agent can act in would reject it, and never disappears merely
    /// because one channel among several cannot do it. Taking capabilities rather than the registry
    /// keeps this decidable without building any channels, which is what makes it testable.
    pub fn for_channels(capabilities: impl IntoIterator<Item = ChannelCapabilities>) -> Self {
        let mut surface = Self {
            admin: false,
            member_rights: false,
            member_roles: false,
        };
        for capability in capabilities {
            surface.admin |= capability.admin;
            surface.member_rights |= capability.member_rights;
            surface.member_roles |= capability.member_roles;
        }
        surface
    }
}

impl Default for ToolSurface {
    fn default() -> Self {
        Self {
            admin: true,
            member_rights: true,
            member_roles: true,
        }
    }
}

/// Tools removed when [`ToolSurface::admin`] is off.
const ADMIN_TOOLS: &[&str] = &[
    "moderate_member",
    "set_member_rights",
    "set_member_roles",
    "pin_message",
    "set_chat",
    "member",
    "list_members",
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
        if surface.admin {
            // Both are moderation tools, but no platform has both models, so offering the wrong one
            // would put a tool in the list that fails on every chat the agent can reach.
            if !surface.member_rights {
                tool_router.remove_route("set_member_rights");
            }
            if !surface.member_roles {
                tool_router.remove_route("set_member_roles");
            }
        } else {
            for name in ADMIN_TOOLS {
                tool_router.remove_route(name);
            }
        }
        Self { sink, tool_router }
    }

    /// Shared body of mute, unmute, block, and unblock.
    ///
    /// Four tools rather than one with a level argument, because a tool list is the only place the
    /// agent learns what it can do and named verbs read better there than an enum. They differ only
    /// in the policy they set, so the wording of the result is decided here in one place.
    async fn rule(
        &self,
        policy: Policy,
        conversation: &str,
        duration: Option<&str>,
        reason: Option<&str>,
    ) -> Result<CallToolResult, McpError> {
        let until = match parse_duration(duration) {
            Ok(until) => until,
            Err(message) => return Ok(CallToolResult::error(vec![ContentBlock::text(message)])),
        };
        let previous = match self
            .sink
            .set_policy(conversation, policy, until, reason)
            .await
        {
            Ok(previous) => previous,
            Err(error) => return Ok(sink_failure(&error)),
        };

        let lapses = match until {
            Some(until) => format!(" until {}", until.to_rfc3339()),
            None => String::new(),
        };
        let message = match policy {
            Policy::Mute => format!(
                "Muted {conversation}{lapses}. You will still be woken when somebody mentions you \
                 or replies to you there, and everything else is recorded for read_history."
            ),
            Policy::Block => format!(
                "Blocked {conversation}{lapses}. Nothing from it will reach you, and nothing said \
                 meanwhile is kept."
            ),
            // Both undo tools land here, so the answer says what changed rather than which verb was
            // used. Saying nothing changed when the conversation was already active is worth the
            // extra branch: it is the difference between "I lifted it" and "there was nothing to
            // lift".
            Policy::Active => {
                let changed = match previous {
                    Some(Policy::Mute) => format!("{conversation} is no longer muted"),
                    Some(Policy::Block) => format!("{conversation} is no longer blocked"),
                    Some(Policy::Active) => {
                        format!("{conversation} was already set to wake you for everything")
                    }
                    None => format!(
                        "{conversation} had no setting of its own, and an explicit one may differ \
                         from the default for this kind of chat"
                    ),
                };
                match until {
                    // What it goes back to matters more than when, and is not obvious: an
                    // expiring `active` does not restore whatever was there before, it falls
                    // through to the default for the chat's kind.
                    // What it reverts to is deliberately not named. This has no idea what kind
                    // of chat it is, and the default for each kind is the operator's to set, so
                    // "back to mentions only" would be a guess dressed as a fact.
                    Some(until) => format!(
                        "{changed}. You will be woken for everything there until {}, after which \
                         it falls back to whatever this deployment's default is for a chat of its \
                         kind. list_conversations reports where it lands.",
                        until.to_rfc3339()
                    ),
                    None => format!("{changed}. You will be woken for everything there."),
                }
            }
        };
        Ok(CallToolResult::success(vec![ContentBlock::text(message)]))
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
            read_only_hint = false,
            destructive_hint = true,
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
            read_only_hint = false,
            destructive_hint = true,
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
            read_only_hint = false,
            destructive_hint = true,
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

    /// Replace the roles somebody holds.
    #[tool(
        description = "Replace the set of roles somebody holds in a server, by name. This is the \
                       whole set, so an empty list strips them back to none. On Discord a role is \
                       what carries privileges, so this is how you promote and demote. Needs the \
                       manage-roles permission, and you cannot grant a role above your own.",
        annotations(
            title = "Set member roles",
            read_only_hint = false,
            destructive_hint = true,
            open_world_hint = true
        )
    )]
    async fn set_member_roles(
        &self,
        Parameters(args): Parameters<SetMemberRolesArgs>,
    ) -> Result<CallToolResult, McpError> {
        match self
            .sink
            .set_member_roles(&args.conversation, &args.user_id, &args.roles)
            .await
        {
            Ok(()) => Ok(CallToolResult::success(vec![ContentBlock::text(
                if args.roles.is_empty() {
                    format!(
                        "User {} in {} now holds no roles.",
                        args.user_id, args.conversation
                    )
                } else {
                    format!(
                        "User {} in {} now holds: {}.",
                        args.user_id,
                        args.conversation,
                        args.roles.join(", ")
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
        description = "Change a group's title, description, or slowmode. Omit a field to leave it \
                       as it is. Needs the change-info admin right, and slowmode only exists on \
                       some platforms.",
        annotations(
            title = "Set chat details",
            read_only_hint = false,
            destructive_hint = true,
            open_world_hint = true
        )
    )]
    async fn set_chat(
        &self,
        Parameters(args): Parameters<SetChatArgs>,
    ) -> Result<CallToolResult, McpError> {
        let slowmode = match args.slowmode.as_deref() {
            Some(raw) => match humantime::parse_duration(raw) {
                Ok(duration) => Some(duration),
                Err(error) => {
                    return Ok(CallToolResult::error(vec![ContentBlock::text(format!(
                        "{raw:?} is not a duration I understand ({error}). Try something like \
                         `30s` or `5m`, or `0s` to turn slowmode off."
                    ))]));
                }
            },
            None => None,
        };
        let settings = ChatSettings {
            slowmode,
            title: args.title,
            description: args.description,
        };
        if settings.is_empty() {
            return Ok(CallToolResult::error(vec![ContentBlock::text(
                "Nothing to change; set `title`, `description`, or `slowmode`.",
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

    /// Who is in a chat.
    #[tool(
        description = "Find out who is in a chat. Pass `query` to look for someone by name, or \
                       omit it to ask for everyone. What comes back depends on the platform and is \
                       reported in `coverage`: `everyone` is the full roster, `matching` is a name \
                       search, and `administrators` means the platform will only enumerate those, \
                       which is the case on Telegram. `total` is the headcount where the platform \
                       gives one, and is often known even when the roster is not. Page with \
                       `after` when `next_after` comes back set. If a listing is refused, the \
                       error says which switch turns it on rather than leaving you to guess. \
                       Where the platform reports availability, each person carries a `presence` \
                       with their status and how recently it was known; `online_only` keeps just \
                       those at their machine. Treat `do_not_disturb` as present but asking not to \
                       be interrupted, and `unknown` as no answer rather than as absent.",
        annotations(title = "List members", read_only_hint = true, open_world_hint = true)
    )]
    async fn list_members(
        &self,
        Parameters(args): Parameters<ListMembersArgs>,
    ) -> Result<CallToolResult, McpError> {
        match self
            .sink
            .list_members(
                &args.conversation,
                args.query.as_deref(),
                args.limit
                    .unwrap_or(DEFAULT_MEMBER_PAGE)
                    .clamp(1, MAX_MEMBER_LIMIT),
                args.after.as_deref(),
            )
            .await
        {
            Ok(mut listing) => {
                if args.online_only.unwrap_or(false) {
                    // A channel that reports no presence at all answers every member `None`, so
                    // filtering would empty the list and read as "nobody is around" -- the exact
                    // confusion the `unknown` state exists to prevent, and worse here because an
                    // empty page carries no cursor suggesting there is more. Refused instead.
                    if listing
                        .members
                        .iter()
                        .all(|member| member.presence.is_none())
                        && !listing.members.is_empty()
                    {
                        return Ok(sink_failure(&SinkError::Delivery(
                            "this platform does not report who is online, so `online_only` would \
                             hide everybody rather than narrow the list. Ask without it, and use \
                             the message timestamps in read_history to judge who is around."
                                .to_string(),
                        )));
                    }
                    // Filtered here rather than in the connector so the platform still pages the
                    // way it wants to. `unknown` is dropped along with offline: this argument is
                    // asked by somebody about to hand out work, and "might be there" is not a
                    // basis for that. The unfiltered call still shows them.
                    listing.members.retain(|member| {
                        member
                            .presence
                            .is_some_and(|presence| presence.status.is_present())
                    });
                    // Says what the members now are, so three survivors of a fifty-person page are
                    // not read as three people online in a chat of fourteen hundred. `total` still
                    // describes the chat, which is why the label has to change instead.
                    listing.coverage = MemberCoverage::Present;
                }
                Ok(json_result(&listing))
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

    /// Stop being woken by everything a conversation says.
    #[tool(
        description = "Turn a conversation down to mentions only, the way you would mute a busy \
                       group on your own phone. You are woken when somebody names you or uses \
                       their client's reply button on something you said, and for nothing else \
                       there: somebody answering you in ordinary prose, without either, does not \
                       reach you. Everything else is recorded rather than \
                       discarded: read_history and search_history reach it, and you are told how \
                       much you missed when something does wake you. `duration` is something like \
                       `2h` or `7d`; omit it to leave it muted until you unmute. To follow a \
                       conversation on, unmute it for a while, or arrange your own look-back. Use \
                       block instead if you want a chat to stop reaching you at all.",
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
        self.rule(
            Policy::Mute,
            &args.conversation,
            args.duration.as_deref(),
            args.reason.as_deref(),
        )
        .await
    }

    /// Start being woken by a conversation again.
    #[tool(
        description = "Hear everything from a conversation again, undoing a mute. `duration` is \
                       something like `20m` or `2h`: use it when you have been pulled into a \
                       discussion and want the whole of it for a while without having to remember \
                       to mute the chat afterwards. When it lapses the conversation goes back to \
                       this deployment's default for a chat of its kind. Omit it to \
                       hear the chat in full until you mute it again. Anything said while it was \
                       muted was recorded and is still readable with read_history.",
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
        self.rule(
            Policy::Active,
            &args.conversation,
            args.duration.as_deref(),
            args.reason.as_deref(),
        )
        .await
    }

    /// Stop hearing a conversation at all.
    #[tool(
        description = "Stop a conversation reaching you at all. Nothing from it is delivered and \
                       nothing is kept, so unlike mute there is no way to read afterwards what was \
                       said while it was blocked; you are only told how many messages went. \
                       `duration` is something like `2h` or `7d`; omit it to block until you \
                       unblock. This is the heavier of the two: prefer mute for a chat that is \
                       merely noisy, and keep this for one there is no reason to read later.",
        annotations(
            title = "Block conversation",
            read_only_hint = true,
            open_world_hint = false
        )
    )]
    async fn block(
        &self,
        Parameters(args): Parameters<MuteArgs>,
    ) -> Result<CallToolResult, McpError> {
        self.rule(
            Policy::Block,
            &args.conversation,
            args.duration.as_deref(),
            args.reason.as_deref(),
        )
        .await
    }

    /// Start hearing a blocked conversation again.
    #[tool(
        description = "Lift a block so a conversation can reach you again. What was said while it \
                       was blocked is gone; this only affects what arrives from now on. \
                       `duration` bounds how long you hear it in full, after which it goes back to \
                       the default for its kind rather than back to being blocked.",
        annotations(
            title = "Unblock conversation",
            read_only_hint = true,
            open_world_hint = false
        )
    )]
    async fn unblock(
        &self,
        Parameters(args): Parameters<UnmuteArgs>,
    ) -> Result<CallToolResult, McpError> {
        self.rule(
            Policy::Active,
            &args.conversation,
            args.duration.as_deref(),
            args.reason.as_deref(),
        )
        .await
    }

    /// Report what is waiting without spending it.
    #[tool(
        description = "How many recorded messages you have not been shown, and when the most \
                       recent of them arrived. Asking does not count as having seen them, so \
                       read_history still returns them afterwards. Built to be polled: the answer \
                       is one short line that changes only when something new has been said, so a \
                       scheduled job can watch a chat with it and spend a turn only once the chat \
                       has actually moved. Omit `conversation` to ask about every chat at once.",
        annotations(
            title = "Unseen messages",
            read_only_hint = true,
            open_world_hint = false
        )
    )]
    async fn unseen(
        &self,
        Parameters(args): Parameters<UnseenArgs>,
    ) -> Result<CallToolResult, McpError> {
        match self.sink.unseen(args.conversation.as_deref()).await {
            Ok(summary) => Ok(CallToolResult::success(vec![ContentBlock::text(
                summary.line(),
            )])),
            Err(error) => Ok(sink_failure(&error)),
        }
    }

    /// Read a conversation back.
    #[tool(
        description = "Read recent messages from a conversation, oldest first, including ones you \
                       were never woken for. This is how you catch up on a muted chat: somebody \
                       mentions you halfway through a discussion, and this is the discussion. It \
                       reads what this bridge recorded, so it does not go back before the bridge \
                       was installed or past the configured retention. A block stops a chat being \
                       recorded from that point on; whatever was recorded before it is still \
                       here. It holds what people said, not what you replied: your own messages \
                       are not recorded, so this is one side of the conversation. Pass the oldest `cursor` you were given back as \
                       `before` to page further back.",
        annotations(title = "Read history", read_only_hint = true, open_world_hint = false)
    )]
    async fn read_history(
        &self,
        Parameters(args): Parameters<ReadHistoryArgs>,
    ) -> Result<CallToolResult, McpError> {
        let limit = args
            .limit
            .map_or(DEFAULT_HISTORY_LIMIT, |limit| limit as usize)
            .clamp(1, MAX_HISTORY_LIMIT);
        match self
            .sink
            .read_history(&args.conversation, limit, args.before)
            .await
        {
            Ok(entries) if entries.is_empty() => {
                Ok(CallToolResult::success(vec![ContentBlock::text(format!(
                    "Nothing recorded for {}. Either nothing has been said there since this bridge \
                     started, it is older than the retention period, or the conversation is \
                     blocked and nothing from it is kept.",
                    args.conversation
                ))]))
            }
            Ok(entries) => Ok(json_result(&entries)),
            Err(error) => Ok(sink_failure(&error)),
        }
    }

    /// Search what was said.
    #[tool(
        description = "Search recorded messages for words, across every conversation or within \
                       one. Use it to find something you were told a while ago, or to check what a \
                       chat was discussing before it mentioned you. Matching is on whole words; \
                       `a OR b`, `a NOT b`, and \"quoted phrases\" work. Same limits as \
                       read_history: only what this bridge recorded, since it was installed and \
                       within the configured retention. A block stops a chat being recorded from \
                       that point on; what was recorded before it is still searchable.",
        annotations(
            title = "Search history",
            read_only_hint = true,
            open_world_hint = false
        )
    )]
    async fn search_history(
        &self,
        Parameters(args): Parameters<SearchHistoryArgs>,
    ) -> Result<CallToolResult, McpError> {
        let limit = args
            .limit
            .map_or(DEFAULT_HISTORY_LIMIT, |limit| limit as usize)
            .clamp(1, MAX_HISTORY_LIMIT);
        match self
            .sink
            .search_history(&args.query, args.conversation.as_deref(), limit)
            .await
        {
            Ok(entries) if entries.is_empty() => {
                Ok(CallToolResult::success(vec![ContentBlock::text(format!(
                    "Nothing recorded matches {:?}.",
                    args.query
                ))]))
            }
            Ok(entries) => Ok(json_result(&entries)),
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
    // `checked_add_signed` rather than `+`: the plain operator panics once the result leaves
    // chrono's representable range, and `from_std` above admits durations far past it. The value
    // reaching here is whatever the model wrote, which is whatever the last person to message the
    // bot talked it into, so an unhandled panic here is a stalled turn anybody can ask for.
    chrono::Utc::now()
        .checked_add_signed(parsed)
        .map(Some)
        .ok_or_else(|| {
            format!("`duration` {raw:?} lands too far in the future; omit it for no expiry.")
        })
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

    /// A recorded policy decision: conversation, policy, expiry, reason.
    type RecordedPolicy = (
        String,
        Policy,
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
        /// Stands in for a platform that reports no availability at all, such as Telegram.
        no_presence: bool,
        sent: Mutex<Vec<(String, String, SendOptions)>>,
        reactions: Mutex<Vec<(String, String, Option<String>)>>,
        edits: Mutex<Vec<(String, String, String)>>,
        deletes: Mutex<Vec<(String, String)>>,
        policies: Mutex<Vec<RecordedPolicy>>,
        history: Vec<HistoryEntry>,
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
            policy: "active".to_string(),
            policy_until: None,
            unseen: 0,
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

        async fn set_member_roles(
            &self,
            _conversation: &str,
            _user_id: &str,
            _roles: &[String],
        ) -> Result<(), SinkError> {
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
                roles: Vec::new(),
                restricted_until: None,
                user_id: user_id.unwrap_or("42").to_string(),
                display_name: Some("Bot".to_string()),
                status: crate::channel::MemberStatus::Administrator,
                rights: vec![MemberRight::RestrictMembers, MemberRight::DeleteMessages],
                presence: None,
            })
        }

        async fn list_members(
            &self,
            _conversation: &str,
            query: Option<&str>,
            limit: usize,
            _after: Option<&str>,
        ) -> Result<MemberListing, SinkError> {
            Ok(MemberListing {
                coverage: if query.is_some() {
                    crate::channel::MemberCoverage::Matching
                } else {
                    crate::channel::MemberCoverage::Everyone
                },
                members: vec![
                    MemberInfo {
                        user_id: "42".to_string(),
                        display_name: Some("Alice".to_string()),
                        status: crate::channel::MemberStatus::Member,
                        rights: Vec::new(),
                        roles: Vec::new(),
                        restricted_until: None,
                        // Present unless the fixture is standing in for a platform that reports
                        // nothing, which is a different case from one person being unaccounted for.
                        presence: (!self.no_presence).then_some(crate::channel::Presence {
                            status: crate::channel::PresenceStatus::Unknown,
                            as_of: None,
                        }),
                    },
                    MemberInfo {
                        user_id: "43".to_string(),
                        display_name: Some("Dana".to_string()),
                        status: crate::channel::MemberStatus::Member,
                        rights: Vec::new(),
                        roles: Vec::new(),
                        restricted_until: None,
                        presence: (!self.no_presence).then_some(crate::channel::Presence {
                            status: crate::channel::PresenceStatus::Online,
                            as_of: None,
                        }),
                    },
                ],
                total: Some(7),
                next_after: (limit == 1).then(|| "42".to_string()),
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

        async fn set_policy(
            &self,
            conversation: &str,
            policy: Policy,
            until: Option<chrono::DateTime<chrono::Utc>>,
            reason: Option<&str>,
        ) -> Result<Option<Policy>, SinkError> {
            if let Some(reason) = self.fail_with {
                return Err(SinkError::Delivery(reason.to_string()));
            }
            let mut policies = self
                .policies
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let previous = policies
                .iter()
                .find(|(ruled, ..)| ruled == conversation)
                .map(|(_, policy, ..)| *policy);
            policies.retain(|(ruled, ..)| ruled != conversation);
            policies.push((
                conversation.to_string(),
                policy,
                until,
                reason.map(str::to_string),
            ));
            Ok(previous)
        }

        async fn unseen(&self, conversation: Option<&str>) -> Result<UnseenSummary, SinkError> {
            if let Some(reason) = self.fail_with {
                return Err(SinkError::Delivery(reason.to_string()));
            }
            let matching: Vec<&HistoryEntry> = self
                .history
                .iter()
                .filter(|entry| conversation.is_none_or(|id| entry.conversation == id))
                .collect();
            Ok(UnseenSummary {
                count: matching.len() as u64,
                latest: matching
                    .iter()
                    .filter_map(|entry| chrono::DateTime::parse_from_rfc3339(&entry.timestamp).ok())
                    .map(|at| at.with_timezone(&chrono::Utc))
                    .max(),
                newest: matching
                    .iter()
                    .filter_map(|entry| chrono::DateTime::parse_from_rfc3339(&entry.timestamp).ok())
                    .map(|at| at.with_timezone(&chrono::Utc))
                    .max(),
            })
        }

        async fn read_history(
            &self,
            conversation: &str,
            limit: usize,
            _before: Option<i64>,
        ) -> Result<Vec<HistoryEntry>, SinkError> {
            if let Some(reason) = self.fail_with {
                return Err(SinkError::Delivery(reason.to_string()));
            }
            Ok(self
                .history
                .iter()
                .filter(|entry| entry.conversation == conversation)
                .take(limit)
                .cloned()
                .collect())
        }

        async fn search_history(
            &self,
            query: &str,
            conversation: Option<&str>,
            limit: usize,
        ) -> Result<Vec<HistoryEntry>, SinkError> {
            if let Some(reason) = self.fail_with {
                return Err(SinkError::Delivery(reason.to_string()));
            }
            Ok(self
                .history
                .iter()
                .filter(|entry| {
                    conversation.is_none_or(|wanted| entry.conversation == wanted)
                        && entry.text.contains(query)
                })
                .take(limit)
                .cloned()
                .collect())
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
        let policies = sink.policies.lock().expect("lock");
        assert_eq!(policies[0].1, Policy::Mute);
        let until = policies[0].2.expect("an expiry was set");
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
        assert!(
            !text_of(&result).contains("until"),
            "no expiry should be printed"
        );
        assert_eq!(sink.policies.lock().expect("lock")[0].2, None);
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
        assert!(sink.policies.lock().expect("lock").is_empty());
    }

    #[tokio::test]
    async fn unmuting_says_so_when_there_was_nothing_to_lift() {
        // Reporting a lift for a no-op would let the agent believe it had fixed something. The
        // wording also has to admit that the conversation now overrides its default rather than
        // merely returning to it, because those are different states.
        let (server, _sink) = server_with(FakeSink::default());
        let result = server
            .unmute(Parameters(UnmuteArgs {
                conversation: "telegram:1".to_string(),
                duration: None,
                reason: None,
            }))
            .await
            .expect("tool runs");
        assert_eq!(result.is_error, Some(false));
        let text = text_of(&result);
        assert!(text.contains("had no setting of its own"), "got: {text}");
    }

    #[tokio::test]
    async fn hearing_a_chat_in_full_can_be_asked_for_by_the_hour() {
        // Being pulled into a live discussion is the case this exists for. Without an expiry the
        // agent has to remember to mute the room afterwards, and a turn that fails or forgets
        // leaves a busy group waking it for every message indefinitely.
        let (server, sink) = server_with(FakeSink::default());
        let result = server
            .unmute(Parameters(UnmuteArgs {
                conversation: "telegram:-100".to_string(),
                duration: Some("20m".to_string()),
                reason: Some("design discussion".to_string()),
            }))
            .await
            .expect("tool runs");
        assert_eq!(result.is_error, Some(false));

        let recorded = sink.policies.lock().expect("lock");
        let (conversation, policy, until, reason) = &recorded[0];
        assert_eq!(conversation, "telegram:-100");
        assert_eq!(*policy, Policy::Active);
        assert!(until.is_some(), "a duration has to reach the store");
        assert_eq!(reason.as_deref(), Some("design discussion"));

        // What it reverts to is the part the agent cannot infer: an expiring `active` does not
        // restore the mute that preceded it, it falls through to the default for the kind. Which
        // default that is stays unnamed, because this call site has no idea what kind of chat it
        // is and the per-kind defaults are the operator's to set.
        let text = text_of(&result);
        assert!(text.contains("falls back to"), "got: {text}");
        assert!(
            !text.contains("mentions only"),
            "the tool cannot know the chat's kind, so it must not name the default: {text}"
        );
    }

    #[tokio::test]
    async fn unmuting_a_muted_conversation_reports_what_it_lifted() {
        let (server, _sink) = server_with(FakeSink::default());
        server
            .mute(Parameters(MuteArgs {
                conversation: "telegram:1".to_string(),
                duration: None,
                reason: None,
            }))
            .await
            .expect("tool runs");
        let result = server
            .unmute(Parameters(UnmuteArgs {
                conversation: "telegram:1".to_string(),
                duration: None,
                reason: None,
            }))
            .await
            .expect("tool runs");
        assert!(text_of(&result).contains("no longer muted"));
    }

    #[tokio::test]
    async fn blocking_and_muting_are_different_decisions() {
        // The whole point of the split: one keeps what it withholds and the other does not, and the
        // agent picks between them on that basis.
        let (server, sink) = server_with(FakeSink::default());
        server
            .block(Parameters(MuteArgs {
                conversation: "telegram:1".to_string(),
                duration: None,
                reason: None,
            }))
            .await
            .expect("tool runs");
        assert_eq!(sink.policies.lock().expect("lock")[0].1, Policy::Block);

        let result = server
            .unblock(Parameters(UnmuteArgs {
                conversation: "telegram:1".to_string(),
                duration: None,
                reason: None,
            }))
            .await
            .expect("tool runs");
        assert!(text_of(&result).contains("no longer blocked"));
        assert_eq!(sink.policies.lock().expect("lock")[0].1, Policy::Active);
    }

    fn history_entry(conversation: &str, message_id: &str, text: &str) -> HistoryEntry {
        HistoryEntry {
            conversation: conversation.to_string(),
            message_id: message_id.to_string(),
            sender: "Alice".to_string(),
            sender_id: Some("111".to_string()),
            text: text.to_string(),
            notes: None,
            attachments: Vec::new(),
            addressed: false,
            // Derived from the id so entries in one test are distinguishable by time.
            timestamp: format!("2026-08-11T09:3{message_id}:00+00:00"),
            cursor: 1,
        }
    }

    #[tokio::test]
    async fn asking_what_is_unseen_answers_in_one_comparable_line() {
        // The tool exists to be gated on, so the shape of the answer is the contract: one line, no
        // prose that varies, and a timestamp rather than anything relative.
        let (server, _sink) = server_with(FakeSink {
            history: vec![history_entry("telegram:1", "1", "the deploy is stuck")],
            ..FakeSink::default()
        });
        let result = server
            .unseen(Parameters(UnseenArgs {
                conversation: Some("telegram:1".to_string()),
            }))
            .await
            .expect("tool runs");
        assert_eq!(result.is_error, Some(false));
        let text = text_of(&result);
        assert_eq!(text.lines().count(), 1, "got: {text}");
        assert!(text.starts_with("1 unseen, newest "), "got: {text}");
    }

    #[tokio::test]
    async fn asking_what_is_unseen_with_no_conversation_covers_everything() {
        let (server, _sink) = server_with(FakeSink {
            history: vec![
                history_entry("telegram:1", "1", "one"),
                history_entry("telegram:2", "2", "two"),
            ],
            ..FakeSink::default()
        });
        let result = server
            .unseen(Parameters(UnseenArgs { conversation: None }))
            .await
            .expect("tool runs");
        assert!(text_of(&result).starts_with("2 unseen"), "{result:?}");
    }

    #[tokio::test]
    async fn a_quiet_chat_says_nothing_is_waiting_rather_than_failing() {
        // A watcher polls this constantly, and the quiet answer is by far the common one. An error
        // here would look to the caller exactly like the bridge being down.
        let (server, _sink) = server_with(FakeSink::default());
        let result = server
            .unseen(Parameters(UnseenArgs {
                conversation: Some("telegram:1".to_string()),
            }))
            .await
            .expect("tool runs");
        assert_eq!(result.is_error, Some(false));
        assert_eq!(text_of(&result), "0 unseen");
    }

    #[tokio::test]
    async fn reading_history_hands_back_what_was_recorded() {
        let (server, _sink) = server_with(FakeSink {
            history: vec![HistoryEntry {
                conversation: "telegram:-100".to_string(),
                message_id: "41".to_string(),
                sender: "Alice".to_string(),
                sender_id: Some("111".to_string()),
                text: "the deploy is stuck".to_string(),
                notes: None,
                attachments: vec!["7".to_string()],
                addressed: false,
                timestamp: "2026-08-11T09:30:00+00:00".to_string(),
                cursor: 7,
            }],
            ..FakeSink::default()
        });
        let result = server
            .read_history(Parameters(ReadHistoryArgs {
                conversation: "telegram:-100".to_string(),
                limit: None,
                before: None,
            }))
            .await
            .expect("tool runs");
        let text = text_of(&result);
        assert!(text.contains("the deploy is stuck"), "got: {text}");
        assert!(
            text.contains("\"7\""),
            "an attachment handle has to survive, or a picture found in history cannot be \
             opened: {text}"
        );
    }

    #[tokio::test]
    async fn an_empty_history_says_why_it_might_be_empty() {
        let (server, _sink) = server_with(FakeSink::default());
        let result = server
            .read_history(Parameters(ReadHistoryArgs {
                conversation: "telegram:-100".to_string(),
                limit: None,
                before: None,
            }))
            .await
            .expect("tool runs");
        assert_eq!(result.is_error, Some(false));
        let text = text_of(&result);
        assert!(text.contains("retention"), "got: {text}");
        assert!(text.contains("blocked"), "got: {text}");
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
    async fn a_listing_says_how_much_of_the_chat_it_covers() {
        // The whole point of the shape. Telegram will only ever enumerate administrators, so a
        // caller handed a bare array would reason about a room of three when there are hundreds.
        let (server, _sink) = server_with(FakeSink::default());
        let result = server
            .list_members(Parameters(ListMembersArgs {
                conversation: "telegram:-100".to_string(),
                query: None,
                limit: None,
                after: None,
                online_only: None,
            }))
            .await
            .expect("tool runs");
        let body = text_of(&result);
        assert!(body.contains("\"coverage\""), "got: {body}");
        assert!(body.contains("everyone"), "got: {body}");
        assert!(
            body.contains("\"total\""),
            "the headcount is worth having: {body}"
        );
    }

    #[tokio::test]
    async fn searching_by_name_is_reported_as_a_search_not_a_roster() {
        let (server, _sink) = server_with(FakeSink::default());
        let result = server
            .list_members(Parameters(ListMembersArgs {
                conversation: "discord:1".to_string(),
                query: Some("dana".to_string()),
                limit: None,
                after: None,
                online_only: None,
            }))
            .await
            .expect("tool runs");
        assert!(text_of(&result).contains("matching"));
    }

    #[tokio::test]
    async fn online_only_drops_anyone_whose_presence_is_merely_unknown() {
        // The caller asking this is about to hand somebody work. "Might be there" is not a basis
        // for that, and a channel that does not track presence at all reports every member as
        // unknown, so passing them through would make the filter silently do nothing.
        let (server, _sink) = server_with(FakeSink::default());
        let result = server
            .list_members(Parameters(ListMembersArgs {
                conversation: "discord:1".to_string(),
                query: None,
                limit: None,
                after: None,
                online_only: Some(true),
            }))
            .await
            .expect("tool runs");
        let body = text_of(&result);
        assert!(
            !body.contains("Alice"),
            "somebody whose presence is unknown was offered as available: {body}"
        );
        assert!(
            body.contains("Dana"),
            "the person actually online was filtered out too: {body}"
        );
        assert!(
            body.contains("present"),
            "a filtered page must not still claim to cover everyone: {body}"
        );
    }

    #[tokio::test]
    async fn online_only_is_refused_where_the_platform_reports_no_presence() {
        // Telegram reports availability for nobody, so filtering would empty the list and read as
        // "nobody is around". An empty page carries no cursor either, so there is nothing to
        // suggest the answer is an artefact of the filter rather than the truth.
        let (server, _sink) = server_with(FakeSink {
            no_presence: true,
            ..FakeSink::default()
        });
        let result = server
            .list_members(Parameters(ListMembersArgs {
                conversation: "telegram:-100".to_string(),
                query: None,
                limit: None,
                after: None,
                online_only: Some(true),
            }))
            .await
            .expect("tool runs");
        let body = text_of(&result);
        assert!(
            body.contains("does not report who is online"),
            "got: {body}"
        );
    }

    #[tokio::test]
    async fn an_unfiltered_listing_says_it_covers_everyone() {
        // The counterpart to the test above: `coverage` only narrows when the filter actually ran.
        let (server, _sink) = server_with(FakeSink::default());
        let unfiltered = server
            .list_members(Parameters(ListMembersArgs {
                conversation: "discord:1".to_string(),
                query: None,
                limit: None,
                after: None,
                online_only: None,
            }))
            .await
            .expect("tool runs");
        assert!(text_of(&unfiltered).contains("everyone"));
    }

    #[tokio::test]
    async fn setting_nothing_on_a_chat_is_refused() {
        let (server, sink) = server_with(FakeSink::default());
        let result = server
            .set_chat(Parameters(SetChatArgs {
                slowmode: None,
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
            ToolSurface {
                admin: false,
                member_rights: true,
                member_roles: true,
            },
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
    fn the_paging_cursor_is_declared_as_a_number() {
        // The agent only ever sees the schema, so a regression to the string form this used to take
        // would not fail anywhere: it would hand back a timestamp, the tool would refuse it, and
        // the agent would conclude the history simply ends.
        let router = BridgeMcpServer::tool_router();
        let tools = router.list_all();
        let read_history = tools
            .iter()
            .find(|tool| tool.name == "read_history")
            .expect("read_history is registered");
        let schema = serde_json::to_value(&read_history.input_schema).expect("schema serializes");
        let before = &schema["properties"]["before"];
        assert_eq!(
            before["type"],
            serde_json::json!(["integer", "null"]),
            "got: {before}"
        );
        assert!(
            schema["required"]
                .as_array()
                .is_some_and(|required| !required.contains(&serde_json::json!("before"))),
            "paging is optional: {schema}"
        );
    }

    #[tokio::test]
    async fn a_history_result_carries_the_cursor_the_agent_pages_by() {
        // The schema covers the way in; this covers the way out. Without the cursor in the payload
        // there is nothing to pass back, so paging would be undocumented rather than merely
        // awkward.
        let (server, _sink) = server_with(FakeSink {
            history: vec![HistoryEntry {
                conversation: "telegram:-100".to_string(),
                message_id: "41".to_string(),
                sender: "Alice".to_string(),
                sender_id: None,
                text: "the deploy is stuck".to_string(),
                notes: None,
                attachments: Vec::new(),
                addressed: false,
                timestamp: "2026-08-11T09:30:00+00:00".to_string(),
                cursor: 8_212,
            }],
            ..FakeSink::default()
        });
        let result = server
            .read_history(Parameters(ReadHistoryArgs {
                conversation: "telegram:-100".to_string(),
                limit: None,
                before: None,
            }))
            .await
            .expect("tool runs");
        let text = text_of(&result);
        let parsed: serde_json::Value = serde_json::from_str(&text).expect("the result is JSON");
        assert_eq!(parsed[0]["cursor"], 8_212, "got: {text}");
    }

    /// Capabilities as each connector reports them, so these tests move when the connectors do.
    fn telegram_capabilities(admin_tools: bool) -> ChannelCapabilities {
        ChannelCapabilities {
            presence: false,
            typing_status: false,
            typing_indicator: true,
            files: true,
            photos: true,
            reactions: true,
            edit: true,
            admin: admin_tools,
            member_rights: admin_tools,
            member_roles: false,
        }
    }

    fn discord_capabilities(admin_tools: bool) -> ChannelCapabilities {
        ChannelCapabilities {
            member_rights: false,
            member_roles: admin_tools,
            typing_status: true,
            ..telegram_capabilities(admin_tools)
        }
    }

    #[test]
    fn a_platform_only_tool_is_absent_when_that_platform_is_not_configured() {
        // The decision a real deployment actually gets. Every other test here builds a
        // `ToolSurface` by hand, so without this an inverted `any` would ship without a
        // failure anywhere.
        let telegram_only = ToolSurface::for_channels([telegram_capabilities(true)]);
        assert!(
            telegram_only.member_rights,
            "Telegram grants rights directly"
        );
        assert!(
            !telegram_only.member_roles,
            "there is no Discord here, so a roles tool would fail on every chat"
        );

        let discord_only = ToolSurface::for_channels([discord_capabilities(true)]);
        assert!(discord_only.member_roles);
        assert!(
            !discord_only.member_rights,
            "there is no Telegram here, so a rights tool would fail on every chat"
        );
    }

    #[test]
    fn a_deployment_with_both_platforms_gets_both_moderation_models() {
        let both =
            ToolSurface::for_channels([telegram_capabilities(true), discord_capabilities(true)]);
        assert!(both.admin);
        assert!(both.member_rights);
        assert!(both.member_roles);
    }

    #[test]
    fn one_channel_that_can_do_it_is_enough() {
        // Turning admin off for one channel must not withdraw the tools from the other, since the
        // agent can still act in that one.
        let mixed =
            ToolSurface::for_channels([telegram_capabilities(false), discord_capabilities(true)]);
        assert!(mixed.admin);
        assert!(mixed.member_roles);
        assert!(!mixed.member_rights, "the Telegram channel opted out");
    }

    #[test]
    fn no_channel_offering_moderation_withdraws_all_of_it() {
        let surface =
            ToolSurface::for_channels([telegram_capabilities(false), discord_capabilities(false)]);
        assert!(!surface.admin);
        assert!(!surface.member_rights);
        assert!(!surface.member_roles);
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
            "block",
            "delete_message",
            "download_attachment",
            "edit_message",
            "get_conversation",
            "list_conversations",
            "list_members",
            "member",
            "moderate_member",
            "mute",
            "pin_message",
            "react",
            "read_history",
            "search_history",
            "send_file",
            "send_message",
            "set_chat",
            "set_member_rights",
            "set_member_roles",
            "unblock",
            "unmute",
            "unseen",
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
    fn a_duration_past_the_end_of_time_is_refused_rather_than_fatal() {
        // `humantime` accepts durations chrono can hold but `DateTime + TimeDelta` cannot add, and
        // the plain operator panics rather than erroring. The value is whatever the model wrote,
        // which is whatever the last person to message the bot talked it into, so an unhandled
        // panic here is a stalled turn anybody can ask for. Reachable from `mute`, `unmute`,
        // `block`, `unblock` and `moderate_member`.
        let refused = parse_duration(Some("9999999999days"));
        assert!(
            refused.is_err(),
            "a duration past the representable range was accepted: {refused:?}"
        );
        // An ordinary one still resolves.
        assert!(parse_duration(Some("30m")).expect("valid").is_some());
        assert!(parse_duration(None).expect("valid").is_none());
    }

    #[test]
    fn only_the_irreversible_tools_ask_for_write() {
        // meka derives a tool's required permission from `readOnlyHint` when no config overrides
        // it, so this list is the permission model. The line is what a tool can do to *other
        // people*, not whether it changes anything at all: almost every tool here modifies
        // something, and gating replying behind `write` would make `read` mean "understands every
        // message and answers none", which fails silently from both ends.
        //
        // What sits on the far side is irreversible and aimed outward: banning somebody and purging
        // their history, deleting other people's messages, changing privileges, renaming the room.
        // A `read` session can talk; it cannot ban. Tools that only change this bridge's own
        // bookkeeping stay read-only however much they modify -- `block` discards inbound messages
        // while it is set, but it is this bridge's own record and the agent can lift it itself.
        const NEEDS_WRITE: &[&str] = &[
            "delete_message",
            "moderate_member",
            "set_member_rights",
            "set_member_roles",
            "set_chat",
        ];
        let router = BridgeMcpServer::tool_router();
        let mut seen = Vec::new();
        for tool in router.list_all() {
            let annotations = tool
                .annotations
                .as_ref()
                .unwrap_or_else(|| panic!("{} has no annotations", tool.name));
            let needs_write = NEEDS_WRITE.contains(&tool.name.as_ref());
            if needs_write {
                seen.push(tool.name.to_string());
            }
            assert_eq!(
                annotations.read_only_hint,
                Some(!needs_write),
                "{} is on the wrong side of the permission line",
                tool.name
            );
            // The second half of the claim: anything that is not read-only says why.
            if needs_write {
                assert_eq!(
                    annotations.destructive_hint,
                    Some(true),
                    "{} asks for write without saying it is destructive",
                    tool.name
                );
            }
        }
        seen.sort_unstable();
        let mut expected: Vec<String> =
            NEEDS_WRITE.iter().map(|name| (*name).to_string()).collect();
        expected.sort_unstable();
        assert_eq!(
            seen, expected,
            "a tool that needs write is missing from the surface entirely"
        );
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
                | "set_member_roles"
                | "pin_message"
                | "set_chat"
                | "member"
                | "list_members"
                | "view_attachment"
                | "download_attachment" => Some(true),
                _ => Some(false),
            };
            assert_eq!(open_world, expected, "openWorldHint for {}", tool.name);
        }
    }
}
