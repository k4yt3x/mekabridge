//! Interop between mekabridge's MCP server and an MCP client that is not the one it was built
//! against.
//!
//! The two are separate processes, each free to link whatever rmcp it likes, and the protocol
//! negotiates a mutually supported version at `initialize`. What this bridge owes is a surface that
//! survives that negotiation rather than one that happens to match a crate version: meka has moved
//! its own pin through 1.3, 1.5, 1.7, 2.1 and 3.1, and each move is the kind of thing that breaks
//! quietly. So these tests drive the real server over a real socket with a real rmcp 2.x client,
//! one major behind the server's own, on every `cargo test`.
//!
//! The client is deliberately not the version meka pins today, which since meka 0.42 is the same
//! major as the server's. Pinning the test to whatever meka currently links would make it agree
//! with the server by construction, which is the one thing it must not do.

// Integration tests live in their own crate, so the `allow-*-in-tests` clippy settings that cover
// `#[cfg(test)]` modules do not apply here. Assertions read better with `expect` than with matches.
#![allow(clippy::expect_used, clippy::panic)]

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use mekabridge::{
    channel::MemberStatus,
    config::{McpConfig, McpTransport},
    mcp::{
        ChatSettings, ConversationSummary, DownloadedAttachment, HistoryEntry, MemberAction,
        MemberCoverage, MemberInfo, MemberListing, MemberRight, OutboundSink, Policy, SendOptions,
        SinkError, ToolSurface, UnseenSummary, ViewedAttachment, WatchOutcome, WatchRecord, serve,
    },
    watch::{WatchField, WatchMode},
};
use rmcp2::{
    ServiceExt,
    model::{CallToolRequestParams, ClientInfo},
    transport::StreamableHttpClientTransport,
};
use tokio_util::sync::CancellationToken;

/// Base64 of a 1x1 PNG.
const ONE_PIXEL_PNG: &str = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==";

#[derive(Default)]
struct RecordingSink {
    sent: Mutex<Vec<(String, String)>>,
    reactions: Mutex<Vec<(String, Option<String>)>>,
    edits: Mutex<Vec<(String, String, String)>>,
    deletes: Mutex<Vec<String>>,
    #[allow(clippy::type_complexity)]
    moderations: Mutex<Vec<(String, MemberAction, Option<chrono::DateTime<chrono::Utc>>)>>,
    #[allow(clippy::type_complexity)]
    policies: Mutex<Vec<(String, Policy, Option<chrono::DateTime<chrono::Utc>>)>>,
    roles: Mutex<Vec<Vec<String>>>,
    watches: Mutex<Vec<WatchRecord>>,
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
impl OutboundSink for RecordingSink {
    async fn send_text(
        &self,
        conversation: &str,
        markdown: &str,
        _options: SendOptions,
        _session: Option<&str>,
    ) -> Result<Vec<String>, SinkError> {
        if conversation != "telegram:1" {
            return Err(SinkError::UnknownConversation(conversation.to_string()));
        }
        let mut sent = self
            .sent
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        sent.push((conversation.to_string(), markdown.to_string()));
        Ok(vec!["1001".to_string()])
    }

    async fn file_send(
        &self,
        _conversation: &str,
        _paths: &[std::path::PathBuf],
        _caption: Option<&str>,
        _options: mekabridge::mcp::FileOptions,
        _session: Option<&str>,
    ) -> Result<Vec<String>, SinkError> {
        Ok(vec!["2001".to_string()])
    }

    async fn message_react(
        &self,
        conversation: &str,
        message_id: &str,
        emoji: Option<&str>,
    ) -> Result<(), SinkError> {
        if conversation != "telegram:1" {
            return Err(SinkError::UnknownConversation(conversation.to_string()));
        }
        let mut reactions = self
            .reactions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        reactions.push((message_id.to_string(), emoji.map(str::to_string)));
        Ok(())
    }

    async fn attachment_view(&self, handle: &str) -> Result<ViewedAttachment, SinkError> {
        match handle {
            // A one-pixel PNG, so the assertion is against real image bytes rather than a stub.
            "417" => Ok(ViewedAttachment::Image {
                media_type: "image/png".to_string(),
                data: ONE_PIXEL_PNG.to_string(),
                note: None,
            }),
            // A still frame standing in for a video, which must arrive with its caveat attached.
            "419" => Ok(ViewedAttachment::Image {
                media_type: "image/jpeg".to_string(),
                data: ONE_PIXEL_PNG.to_string(),
                note: Some(
                    "This is the preview frame for a video, not the video itself.".to_string(),
                ),
            }),
            "418" => Ok(ViewedAttachment::Description(
                "This is a document (\"q3.pdf\", application/pdf) and has no image preview. Use \
                 attachment_download to get the file itself."
                    .to_string(),
            )),
            other => Err(SinkError::UnknownAttachment(other.to_string())),
        }
    }

    async fn attachment_download(&self, handle: &str) -> Result<DownloadedAttachment, SinkError> {
        if handle != "418" {
            return Err(SinkError::UnknownAttachment(handle.to_string()));
        }
        Ok(DownloadedAttachment {
            path: std::path::PathBuf::from("/var/lib/mekabridge/attachments/q3.pdf"),
            bytes: 8_400_000,
            media_type: Some("application/pdf".to_string()),
        })
    }

    async fn conversations(
        &self,
        _channel: Option<&str>,
        _limit: usize,
    ) -> Result<Vec<ConversationSummary>, SinkError> {
        Ok(vec![summary("telegram:1")])
    }

    async fn conversation(&self, id: &str) -> Result<Option<ConversationSummary>, SinkError> {
        Ok((id == "telegram:1").then(|| summary(id)))
    }

    async fn message_edit(
        &self,
        conversation: &str,
        message_id: &str,
        markdown: &str,
        _link_preview: bool,
        _session: Option<&str>,
    ) -> Result<(), SinkError> {
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

    async fn message_delete(
        &self,
        _conversation: &str,
        message_ids: &[String],
    ) -> Result<(), SinkError> {
        let mut deletes = self
            .deletes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        deletes.extend(message_ids.iter().cloned());
        Ok(())
    }

    async fn member_moderate(
        &self,
        _conversation: &str,
        user_id: &str,
        action: MemberAction,
        until: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<(), SinkError> {
        let mut moderations = self
            .moderations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        moderations.push((user_id.to_string(), action, until));
        Ok(())
    }

    async fn member_set_rights(
        &self,
        _conversation: &str,
        _user_id: &str,
        _rights: &[MemberRight],
    ) -> Result<(), SinkError> {
        Ok(())
    }

    async fn member_set_roles(
        &self,
        _conversation: &str,
        _user_id: &str,
        roles: &[String],
    ) -> Result<(), SinkError> {
        self.roles
            .lock()
            .expect("not poisoned")
            .push(roles.to_vec());
        Ok(())
    }

    async fn message_pin(
        &self,
        _conversation: &str,
        _message_id: &str,
        _pin: bool,
        _silent: bool,
    ) -> Result<(), SinkError> {
        Ok(())
    }

    async fn chat_set(
        &self,
        _conversation: &str,
        _settings: ChatSettings,
    ) -> Result<(), SinkError> {
        Ok(())
    }

    async fn member_get(
        &self,
        _conversation: &str,
        user_id: Option<&str>,
    ) -> Result<MemberInfo, SinkError> {
        Ok(MemberInfo {
            roles: Vec::new(),
            restricted_until: None,
            user_id: user_id.unwrap_or("7").to_string(),
            display_name: Some("Bot".to_string()),
            status: MemberStatus::Administrator,
            rights: vec![MemberRight::RestrictMembers],
            presence: None,
        })
    }

    async fn member_list(
        &self,
        _conversation: &str,
        _query: Option<&str>,
        _limit: usize,
        _after: Option<&str>,
    ) -> Result<MemberListing, SinkError> {
        Ok(MemberListing {
            coverage: MemberCoverage::Administrators,
            members: Vec::new(),
            total: Some(3),
            next_after: None,
        })
    }

    async fn set_policy(
        &self,
        conversation: &str,
        policy: Policy,
        until: Option<chrono::DateTime<chrono::Utc>>,
        _reason: Option<&str>,
    ) -> Result<Option<Policy>, SinkError> {
        let mut policies = self
            .policies
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let previous = policies
            .iter()
            .find(|(ruled, ..)| ruled == conversation)
            .map(|(_, policy, _)| *policy);
        policies.retain(|(ruled, ..)| ruled != conversation);
        policies.push((conversation.to_string(), policy, until));
        Ok(previous)
    }

    async fn backlog_check(&self, _conversation: Option<&str>) -> Result<UnseenSummary, SinkError> {
        Ok(UnseenSummary {
            count: 0,
            newest: None,
            latest: None,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn watch_write(
        &self,
        name: &str,
        patterns: &[String],
        field: WatchField,
        mode: WatchMode,
        conversation: Option<&str>,
        until: Option<chrono::DateTime<chrono::Utc>>,
        reason: Option<&str>,
    ) -> Result<WatchOutcome, SinkError> {
        let mut watches = self
            .watches
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let record = WatchRecord {
            id: watches.len() as i64 + 1,
            name: name.to_string(),
            conversation: conversation.map(str::to_string),
            field,
            mode,
            patterns: patterns.to_vec(),
            reason: reason.map(str::to_string),
            until,
            created_at: chrono::Utc::now(),
        };
        match watches.iter().position(|watch| watch.name == name) {
            Some(position) => {
                watches[position] = record.clone();
                Ok(WatchOutcome::Updated {
                    record,
                    added: 0,
                    removed: 0,
                })
            }
            None => {
                watches.push(record.clone());
                Ok(WatchOutcome::Created(record))
            }
        }
    }

    async fn watch_delete(&self, name: &str) -> Result<Option<WatchRecord>, SinkError> {
        let mut watches = self
            .watches
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let position = watches.iter().position(|watch| watch.name == name);
        Ok(position.map(|position| watches.remove(position)))
    }

    async fn watch_list(&self) -> Result<Vec<WatchRecord>, SinkError> {
        Ok(self
            .watches
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone())
    }

    async fn history_read(
        &self,
        conversation: &str,
        _sender_ids: &[String],
        _limit: usize,
        _before: Option<i64>,
        _after: Option<i64>,
    ) -> Result<Vec<HistoryEntry>, SinkError> {
        Ok(vec![HistoryEntry {
            conversation: conversation.to_string(),
            message_id: "41".to_string(),
            sender: "Alice".to_string(),
            sender_id: Some("111".to_string()),
            text: "the deploy is stuck".to_string(),
            notes: None,
            attachments: Vec::new(),
            addressed: false,
            own: false,
            session: None,
            deleted: false,
            superseded: false,
            previous_account: false,
            timestamp: "2026-08-11T09:30:00+00:00".to_string(),
            cursor: 41,
        }])
    }

    async fn history_search(
        &self,
        _query: &str,
        _conversation: Option<&str>,
        _sender_ids: &[String],
        _limit: usize,
    ) -> Result<Vec<HistoryEntry>, SinkError> {
        Ok(Vec::new())
    }
}

/// `CallToolRequestParams` is `#[non_exhaustive]`, so it has to be built through its constructor
/// rather than with a struct literal.
fn tool_params(name: &'static str, arguments: serde_json::Value) -> CallToolRequestParams {
    let mut params = CallToolRequestParams::new(name);
    params.arguments = arguments.as_object().cloned();
    params
}

fn send_message_params(arguments: serde_json::Value) -> CallToolRequestParams {
    tool_params("message_send", arguments)
}

struct Harness {
    url: String,
    shutdown: CancellationToken,
    sink: Arc<RecordingSink>,
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

/// Start the MCP server on an ephemeral loopback port with the full tool surface.
async fn start() -> Harness {
    start_with(ToolSurface::default()).await
}

/// Start with a chosen tool surface, so the conditional registration can be checked both ways.
async fn start_with(surface: ToolSurface) -> Harness {
    let config = McpConfig {
        transport: McpTransport::Http,
        // Port 0 lets the OS pick, so concurrent test binaries cannot collide.
        bind: ([127, 0, 0, 1], 0).into(),
        path: "/mcp".to_string(),
        token: None,
        allowed_hosts: Vec::new(),
        health: true,
    };
    let sink = Arc::new(RecordingSink::default());
    let server = serve::bind(&config).await.expect("binds");
    let local_addr = server.local_addr().expect("http server has an address");
    let shutdown = CancellationToken::new();

    tokio::spawn({
        let sink = Arc::clone(&sink);
        let shutdown = shutdown.clone();
        async move {
            let result = serve::run(server, &config, sink, surface, shutdown).await;
            if let Err(error) = result {
                eprintln!("mcp server stopped: {error}");
            }
        }
    });

    Harness {
        url: format!("http://{local_addr}/mcp"),
        shutdown,
        sink,
    }
}

async fn connect(
    harness: &Harness,
) -> rmcp2::service::RunningService<rmcp2::RoleClient, ClientInfo> {
    let transport = StreamableHttpClientTransport::from_uri(harness.url.clone());
    ClientInfo::default()
        .serve(transport)
        .await
        .expect("a client a major version behind must complete the initialize handshake")
}

#[tokio::test]
async fn handshake_succeeds_across_the_version_gap() {
    let harness = start().await;
    let client = connect(&harness).await;

    let info = client.peer_info().expect("server info is returned");
    let instructions = info
        .instructions
        .as_deref()
        .expect("server instructions orient the agent and must survive negotiation");
    // Asserted on a header line rather than on a tool name. The instructions deliberately do not
    // enumerate the tools, whose own descriptions are in front of the agent whenever it reaches for
    // one; what only they can supply is the envelope's headers, which appear in a user message that
    // no schema describes.
    assert!(
        instructions.contains("`admitted:`"),
        "instructions should explain the envelope headers, got: {instructions}"
    );

    client.cancel().await.expect("clean shutdown");
}

#[tokio::test]
async fn each_moderation_model_is_offered_only_where_it_would_work() {
    // A platform grants privileges to a person or through roles, never both, so offering the wrong
    // tool puts one in the list that fails on every chat the agent can reach.
    let harness = start_with(ToolSurface {
        admin: true,
        member_rights: false,
        member_roles: true,
    })
    .await;
    let client = connect(&harness).await;

    let tools = client.list_all_tools().await.expect("tools/list works");
    let names: Vec<&str> = tools.iter().map(|tool| tool.name.as_ref()).collect();
    assert!(names.contains(&"member_set_roles"), "got {names:?}");
    assert!(!names.contains(&"member_set_rights"), "got {names:?}");
    assert!(names.contains(&"member_moderate"), "got {names:?}");

    client.cancel().await.expect("clean shutdown");
}

#[tokio::test]
async fn setting_roles_round_trips_across_the_version_gap() {
    let harness = start().await;
    let client = connect(&harness).await;

    let result = client
        .call_tool(tool_params(
            "member_set_roles",
            serde_json::json!({
                "conversation": "discord:123",
                "user_id": "456",
                "roles": ["Moderators", "Release Team"],
            }),
        ))
        .await
        .expect("the call succeeds");
    assert_ne!(result.is_error, Some(true), "got {result:?}");

    let recorded = harness.sink.roles.lock().expect("not poisoned").clone();
    assert_eq!(recorded, vec![vec![
        "Moderators".to_string(),
        "Release Team".to_string()
    ]]);

    client.cancel().await.expect("clean shutdown");
}

#[tokio::test]
async fn turning_off_admin_tools_removes_exactly_those() {
    // Conditional registration is the one thing here that can silently drop a tool, so both halves
    // are pinned: the moderation tools go, and nothing else does.
    let harness = start_with(ToolSurface {
        admin: false,
        member_rights: true,
        member_roles: true,
    })
    .await;
    let client = connect(&harness).await;

    let tools = client.list_all_tools().await.expect("tools/list works");
    let mut names: Vec<&str> = tools.iter().map(|tool| tool.name.as_ref()).collect();
    names.sort_unstable();
    assert_eq!(names, vec![
        "attachment_download",
        "attachment_view",
        "backlog_check",
        "conversation_block",
        "conversation_get",
        "conversation_list",
        "conversation_mute",
        "conversation_unblock",
        "conversation_unmute",
        "file_send",
        "history_read",
        "history_search",
        "message_delete",
        "message_edit",
        "message_react",
        "message_send",
        "watch_delete",
        "watch_list",
        "watch_write",
    ]);

    client.cancel().await.expect("clean shutdown");
}

#[tokio::test]
async fn all_tools_are_visible_to_an_older_client() {
    let harness = start().await;
    let client = connect(&harness).await;

    let tools = client.list_all_tools().await.expect("tools/list works");
    let mut names: Vec<&str> = tools.iter().map(|tool| tool.name.as_ref()).collect();
    names.sort_unstable();
    assert_eq!(names, vec![
        "attachment_download",
        "attachment_view",
        "backlog_check",
        "chat_set",
        "conversation_block",
        "conversation_get",
        "conversation_list",
        "conversation_mute",
        "conversation_unblock",
        "conversation_unmute",
        "file_send",
        "history_read",
        "history_search",
        "member_get",
        "member_list",
        "member_moderate",
        "member_set_rights",
        "member_set_roles",
        "message_delete",
        "message_edit",
        "message_pin",
        "message_react",
        "message_send",
        "watch_delete",
        "watch_list",
        "watch_write",
    ]);

    for tool in &tools {
        assert!(
            tool.description
                .as_ref()
                .is_some_and(|text| !text.is_empty()),
            "{} lost its description across the version gap",
            tool.name
        );
        // meka resolves each tool's required permission from `readOnlyHint` when config does not
        // override it, so the annotation has to survive the version gap intact or the permission
        // model changes silently. The conversational surface is read-only on purpose -- classifying
        // the send tools as writes would leave a bridge at `read` unable to answer anybody -- while
        // the five that take irreversible action on somebody else's account ask for `write`.
        const NEEDS_WRITE: &[&str] = &[
            "message_delete",
            "member_moderate",
            "member_set_rights",
            "member_set_roles",
            "chat_set",
        ];
        let read_only = tool
            .annotations
            .as_ref()
            .and_then(|annotations| annotations.read_only_hint);
        assert_eq!(
            read_only,
            Some(!NEEDS_WRITE.contains(&tool.name.as_ref())),
            "readOnlyHint for {}",
            tool.name
        );
    }

    client.cancel().await.expect("clean shutdown");
}

#[tokio::test]
async fn input_schemas_survive_negotiation() {
    let harness = start().await;
    let client = connect(&harness).await;

    let tools = client.list_all_tools().await.expect("tools/list works");
    let send = tools
        .iter()
        .find(|tool| tool.name.as_ref() == "message_send")
        .expect("message_send is advertised");
    let schema = serde_json::to_value(&*send.input_schema).expect("schema serializes");
    let properties = schema
        .get("properties")
        .and_then(serde_json::Value::as_object)
        .expect("message_send has an object schema");
    assert!(properties.contains_key("conversation"));
    assert!(properties.contains_key("text"));

    let required: Vec<&str> = schema
        .get("required")
        .and_then(serde_json::Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(serde_json::Value::as_str)
                .collect()
        })
        .unwrap_or_default();
    assert!(required.contains(&"conversation"), "schema: {schema}");
    assert!(required.contains(&"text"), "schema: {schema}");

    // The switch has to reach the wire, and has to reach it *optional*. A field schemars never
    // emitted would leave the agent unable to ask for a preview at all, and one emitted as required
    // would force every existing caller to start passing it. Neither shows up in a unit test of the
    // handler, because both are decided by the derive on the way out.
    for (tool_name, field) in [
        ("message_send", "link_preview"),
        ("message_edit", "link_preview"),
        ("file_send", "link_preview"),
    ] {
        let tool = tools
            .iter()
            .find(|tool| tool.name.as_ref() == tool_name)
            .unwrap_or_else(|| panic!("{tool_name} is advertised"));
        let schema = serde_json::to_value(&*tool.input_schema).expect("schema serializes");
        let properties = schema
            .get("properties")
            .and_then(serde_json::Value::as_object)
            .unwrap_or_else(|| panic!("{tool_name} has an object schema"));
        assert!(
            properties.contains_key(field),
            "{tool_name} does not advertise {field}: {schema}"
        );
        let required: Vec<&str> = schema
            .get("required")
            .and_then(serde_json::Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(serde_json::Value::as_str)
                    .collect()
            })
            .unwrap_or_default();
        assert!(
            !required.contains(&field),
            "{tool_name} makes {field} mandatory: {schema}"
        );
    }

    // `paths` has to arrive as a required *array*. Nothing in a handler test can see this: the
    // shape is decided by the derive on the way out, and a schema advertising a bare string
    // would have the agent send one path as a scalar and be refused by serde on every call.
    let file_send = tools
        .iter()
        .find(|tool| tool.name.as_ref() == "file_send")
        .expect("file_send is advertised");
    let schema = serde_json::to_value(&*file_send.input_schema).expect("schema serializes");
    assert_eq!(
        schema["properties"]["paths"]["type"], "array",
        "file_send must take a list of paths: {schema}"
    );
    let required: Vec<&str> = schema
        .get("required")
        .and_then(serde_json::Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(serde_json::Value::as_str)
                .collect()
        })
        .unwrap_or_default();
    assert!(required.contains(&"paths"), "schema: {schema}");

    client.cancel().await.expect("clean shutdown");
}

#[tokio::test]
async fn a_batch_of_message_ids_survives_the_version_gap() {
    // The array is the part worth proving on the wire. schemars and the server agree locally, but
    // the client that validates arguments in production is somebody else's build, and a list read
    // as a string would arrive as one id spelled oddly rather than as an error.
    let harness = start().await;
    let client = connect(&harness).await;

    let tools = client.list_all_tools().await.expect("tools/list works");
    let delete = tools
        .iter()
        .find(|tool| tool.name.as_ref() == "message_delete")
        .expect("message_delete is advertised");
    let schema = serde_json::to_value(&*delete.input_schema).expect("schema serializes");
    assert_eq!(
        schema["properties"]["message_ids"]["type"],
        serde_json::json!("array"),
        "an older client has to see the ids as a list: {schema}"
    );

    let result = client
        .call_tool(tool_params(
            "message_delete",
            serde_json::json!({"conversation": "telegram:1", "message_ids": ["11", "12", "13"]}),
        ))
        .await
        .expect("the call must succeed");
    assert_eq!(result.is_error, Some(false));

    let recorded = harness.sink.deletes.lock().expect("lock").clone();
    assert_eq!(recorded, ["11", "12", "13"], "every id crossed the wire");

    client.cancel().await.expect("clean shutdown");
}

#[tokio::test]
async fn the_sender_filter_survives_the_version_gap() {
    let harness = start().await;
    let client = connect(&harness).await;

    let tools = client.list_all_tools().await.expect("tools/list works");
    let read = tools
        .iter()
        .find(|tool| tool.name.as_ref() == "history_read")
        .expect("history_read is advertised");
    let schema = serde_json::to_value(&*read.input_schema).expect("schema serializes");
    assert_eq!(
        schema["properties"]["sender_ids"]["type"],
        serde_json::json!("array"),
        "an older client has to see the filter as a list: {schema}"
    );

    let result = client
        .call_tool(tool_params(
            "history_read",
            serde_json::json!({"conversation": "telegram:1", "sender_ids": ["111"]}),
        ))
        .await
        .expect("the call must succeed");
    assert_eq!(result.is_error, Some(false));

    client.cancel().await.expect("clean shutdown");
}

#[tokio::test]
async fn the_history_cursor_survives_the_version_gap() {
    // `before` is a number, and every other tool argument this bridge takes is a string. Serde and
    // schemars agree locally, but the client that validates arguments in production is somebody
    // else's build, so the numeric form is exercised over a real socket against one.
    let harness = start().await;
    let client = connect(&harness).await;

    let tools = client.list_all_tools().await.expect("tools/list works");
    let read = tools
        .iter()
        .find(|tool| tool.name.as_ref() == "history_read")
        .expect("history_read is advertised");
    let schema = serde_json::to_value(&*read.input_schema).expect("schema serializes");
    assert_eq!(
        schema["properties"]["before"]["type"],
        serde_json::json!(["integer", "null"]),
        "an older client has to see the cursor as a number: {schema}"
    );

    let result = client
        .call_tool(tool_params(
            "history_read",
            serde_json::json!({"conversation": "telegram:1", "before": 8212, "limit": 5}),
        ))
        .await
        .expect("the call must succeed");
    assert_eq!(result.is_error, Some(false));

    // And the cursor has to come back out, or there is nothing to page with.
    let text = result
        .content
        .iter()
        .find_map(|block| block.as_text().map(|text| text.text.clone()))
        .expect("a text block");
    let parsed: serde_json::Value = serde_json::from_str(&text).expect("the result is JSON");
    assert_eq!(parsed[0]["cursor"], 41, "got: {text}");

    client.cancel().await.expect("clean shutdown");
}

#[tokio::test]
async fn the_watch_tools_round_trip_across_the_version_gap() {
    // The enum argument is the part worth proving on the wire: `field` is a schema-typed string,
    // and a client that serialized it differently from the way the server reads it would fail only
    // here, not in the unit tests either side of the boundary.
    let harness = start().await;
    let client = connect(&harness).await;

    let result = client
        .call_tool(tool_params(
            "watch_write",
            serde_json::json!({
                "name": "the-owner",
                "patterns": ["^432688118$", "^432688119$"],
                "field": "sender_id",
                "match": "any",
                "conversation": "telegram:1",
                "reason": "the owner"
            }),
        ))
        .await
        .expect("the call must succeed");
    assert_eq!(result.is_error, Some(false), "{result:?}");

    let listed = client
        .call_tool(tool_params("watch_list", serde_json::json!({})))
        .await
        .expect("the call must succeed");
    let text = listed
        .content
        .iter()
        .filter_map(|block| block.as_text().map(|text| text.text.clone()))
        .collect::<String>();
    let parsed: serde_json::Value = serde_json::from_str(&text).expect("JSON");
    assert_eq!(parsed[0]["name"], "the-owner", "got: {text}");
    assert_eq!(parsed[0]["field"], "sender_id", "got: {text}");
    assert_eq!(parsed[0]["match"], "any", "got: {text}");
    assert_eq!(parsed[0]["conversation"], "telegram:1", "got: {text}");
    // The list has to survive as a list: a client that flattened it would turn a rule the agent
    // can edit into one long alternation it cannot.
    assert_eq!(
        parsed[0]["patterns"].as_array().map(Vec::len),
        Some(2),
        "got: {text}"
    );

    let removed = client
        .call_tool(tool_params(
            "watch_delete",
            serde_json::json!({"name": "the-owner"}),
        ))
        .await
        .expect("the call must succeed");
    assert_eq!(removed.is_error, Some(false), "{removed:?}");
}

#[tokio::test]
async fn the_backlog_result_is_a_document_a_gate_can_point_into() {
    // meka's scheduler parses a tool result's text as JSON when there is no structured content and
    // then resolves a JSON pointer into it. If this came back as prose, every gate built on it
    // would be declined as "the probe did not return JSON" rather than simply not firing.
    let harness = start().await;
    let client = connect(&harness).await;

    let result = client
        .call_tool(tool_params(
            "backlog_check",
            serde_json::json!({"conversation": "telegram:1"}),
        ))
        .await
        .expect("the call must succeed");
    assert_eq!(result.is_error, Some(false));
    let text = result
        .content
        .iter()
        .filter_map(|block| block.as_text().map(|text| text.text.clone()))
        .collect::<String>();
    let parsed: serde_json::Value =
        serde_json::from_str(text.trim()).expect("a gate has to be able to parse this");
    assert!(parsed.get("unseen").is_some(), "got: {text}");
}

#[tokio::test]
async fn the_policy_tools_round_trip_across_the_version_gap() {
    let harness = start().await;
    let client = connect(&harness).await;

    for (tool, expected) in [
        ("conversation_mute", Policy::Mute),
        ("conversation_block", Policy::Block),
    ] {
        let result = client
            .call_tool(tool_params(
                tool,
                serde_json::json!({"conversation": "telegram:1", "duration": "2h"}),
            ))
            .await
            .expect("the call must succeed");
        assert_eq!(result.is_error, Some(false), "for {tool}");

        let recorded = {
            let policies = harness
                .sink
                .policies
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            policies.clone()
        };
        assert_eq!(recorded.len(), 1, "each decision replaces the last");
        assert_eq!(recorded[0].1, expected, "for {tool}");
        assert!(recorded[0].2.is_some(), "the duration must survive: {tool}");
    }

    let result = client
        .call_tool(tool_params(
            "conversation_unblock",
            serde_json::json!({"conversation": "telegram:1"}),
        ))
        .await
        .expect("the call must succeed");
    assert_eq!(result.is_error, Some(false));
    let recorded = {
        let policies = harness
            .sink
            .policies
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        policies.clone()
    };
    assert_eq!(recorded[0].1, Policy::Active);

    client.cancel().await.expect("clean shutdown");
}

#[tokio::test]
async fn calling_send_message_reaches_the_sink() {
    let harness = start().await;
    let client = connect(&harness).await;

    let result = client
        .call_tool(send_message_params(serde_json::json!({
            "conversation": "telegram:1",
            "text": "hello from the agent",
        })))
        .await
        .expect("tools/call works");

    assert_eq!(result.is_error, Some(false));
    // Copy out of the guard's scope before the next await, so no lock is held across it.
    let sent = {
        let guard = harness.sink.sent.lock().expect("lock");
        guard.clone()
    };
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].0, "telegram:1");
    assert_eq!(sent[0].1, "hello from the agent");

    client.cancel().await.expect("clean shutdown");
}

#[tokio::test]
async fn tool_level_errors_arrive_as_results_not_protocol_failures() {
    let harness = start().await;
    let client = connect(&harness).await;

    // The agent has to be able to read the failure and recover; a protocol error would be rendered
    // opaquely by its client instead.
    let result = client
        .call_tool(send_message_params(serde_json::json!({
            "conversation": "telegram:does-not-exist",
            "text": "hello",
        })))
        .await
        .expect("the call itself must succeed");

    assert_eq!(result.is_error, Some(true));
    let text: String = result
        .content
        .iter()
        .filter_map(|block| block.as_text().map(|text| text.text.clone()))
        .collect();
    assert!(text.contains("conversation_list"), "got: {text}");

    client.cancel().await.expect("clean shutdown");
}

#[tokio::test]
async fn react_round_trips_across_the_version_gap() {
    let harness = start().await;
    let client = connect(&harness).await;

    let result = client
        .call_tool(tool_params(
            "message_react",
            serde_json::json!({
                "conversation": "telegram:1",
                "message_id": "4471",
                "emoji": "👍",
            }),
        ))
        .await
        .expect("the call must succeed");
    assert_eq!(result.is_error, Some(false));

    // Cloned out inside its own scope so the guard cannot straddle the await below.
    let recorded = {
        let reactions = harness
            .sink
            .reactions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        reactions.clone()
    };
    assert_eq!(recorded.as_slice(), [(
        "4471".to_string(),
        Some("👍".to_string())
    )]);

    client.cancel().await.expect("clean shutdown");
}

#[tokio::test]
async fn view_attachment_returns_a_real_image_block_to_an_older_client() {
    // The assumption the deferred-attachment design rests on: meka forwards an MCP image block
    // straight through as a multimodal block, so the agent sees a picture in one tool call. If the
    // block arrives as text across the version gap, "show me" quietly stops working.
    let harness = start().await;
    let client = connect(&harness).await;

    let result = client
        .call_tool(tool_params(
            "attachment_view",
            serde_json::json!({ "attachment": "417" }),
        ))
        .await
        .expect("the call must succeed");
    assert_eq!(result.is_error, Some(false));

    let image = result
        .content
        .iter()
        .find_map(|block| block.as_image())
        .expect("the result must carry an image block, not a text description");
    assert_eq!(image.mime_type, "image/png");
    assert_eq!(image.data, ONE_PIXEL_PNG);

    client.cancel().await.expect("clean shutdown");
}

#[tokio::test]
async fn a_preview_frame_arrives_with_its_caveat_attached() {
    // A still frame is not the video. The agent has to be told, in the same result, or it will
    // reasonably conclude it has seen the whole thing.
    let harness = start().await;
    let client = connect(&harness).await;

    let result = client
        .call_tool(tool_params(
            "attachment_view",
            serde_json::json!({ "attachment": "419" }),
        ))
        .await
        .expect("the call must succeed");
    assert_eq!(result.is_error, Some(false));

    let text: String = result
        .content
        .iter()
        .filter_map(|block| block.as_text().map(|text| text.text.clone()))
        .collect();
    assert!(
        text.contains("not the video itself"),
        "the caveat must ride along with the frame, got: {text}"
    );
    assert!(
        result
            .content
            .iter()
            .any(|block| block.as_image().is_some()),
        "the frame itself must still be shown"
    );

    client.cancel().await.expect("clean shutdown");
}

#[tokio::test]
async fn an_unviewable_attachment_comes_back_as_a_description() {
    let harness = start().await;
    let client = connect(&harness).await;

    let result = client
        .call_tool(tool_params(
            "attachment_view",
            serde_json::json!({ "attachment": "418" }),
        ))
        .await
        .expect("the call must succeed");
    // Not an error: the agent asked a reasonable question and gets a usable answer plus a next
    // step.
    assert_eq!(result.is_error, Some(false));
    let text: String = result
        .content
        .iter()
        .filter_map(|block| block.as_text().map(|text| text.text.clone()))
        .collect();
    assert!(text.contains("no image preview"), "got: {text}");

    client.cancel().await.expect("clean shutdown");
}

#[tokio::test]
async fn an_unknown_attachment_handle_tells_the_agent_where_to_look() {
    let harness = start().await;
    let client = connect(&harness).await;

    let result = client
        .call_tool(tool_params(
            "attachment_download",
            serde_json::json!({ "attachment": "9999" }),
        ))
        .await
        .expect("the call itself must succeed");
    assert_eq!(result.is_error, Some(true));
    let text: String = result
        .content
        .iter()
        .filter_map(|block| block.as_text().map(|text| text.text.clone()))
        .collect();
    assert!(text.contains("attachment:"), "got: {text}");

    client.cancel().await.expect("clean shutdown");
}

#[tokio::test]
async fn bearer_token_gates_the_endpoint() {
    let config = McpConfig {
        transport: McpTransport::Http,
        bind: ([127, 0, 0, 1], 0).into(),
        path: "/mcp".to_string(),
        token: Some(mekabridge::config::secret::Secret::new("s3cret", "test")),
        allowed_hosts: Vec::new(),
        health: true,
    };
    let server = serve::bind(&config).await.expect("binds");
    let local_addr = server.local_addr().expect("address");
    let shutdown = CancellationToken::new();
    tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            let sink: Arc<dyn OutboundSink> = Arc::new(RecordingSink::default());
            let _ = serve::run(server, &config, sink, ToolSurface::default(), shutdown).await;
        }
    });

    let http = reqwest::Client::new();
    let unauthenticated = http
        .post(format!("http://{local_addr}/mcp"))
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream")
        .body("{}")
        .send()
        .await
        .expect("request completes");
    assert_eq!(unauthenticated.status(), reqwest::StatusCode::UNAUTHORIZED);

    // Health probes stay open so an orchestrator does not need the credential.
    let health = http
        .get(format!("http://{local_addr}/health/live"))
        .send()
        .await
        .expect("request completes");
    assert_eq!(health.status(), reqwest::StatusCode::OK);

    shutdown.cancel();
}
