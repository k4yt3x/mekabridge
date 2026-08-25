//! Interop between mekabridge's MCP server and the MCP client version meka actually links against.
//!
//! mekabridge builds against rmcp 3.x while meka pins 2.x. The two are separate processes and the
//! protocol negotiates a mutually supported version at `initialize`, but that negotiation is
//! exactly the kind of thing that quietly breaks on an upgrade. These tests drive the real server
//! over a real socket with a real 2.x client, so the skew is checked on every `cargo test` rather
//! than discovered in production.

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
        SinkError, ToolSurface, UnseenSummary, ViewedAttachment, serve,
    },
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

    async fn send_file(
        &self,
        _conversation: &str,
        _paths: &[std::path::PathBuf],
        _caption: Option<&str>,
        _options: mekabridge::mcp::FileOptions,
    ) -> Result<Vec<String>, SinkError> {
        Ok(vec!["2001".to_string()])
    }

    async fn react(
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

    async fn view_attachment(&self, handle: &str) -> Result<ViewedAttachment, SinkError> {
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
                 download_attachment to get the file itself."
                    .to_string(),
            )),
            other => Err(SinkError::UnknownAttachment(other.to_string())),
        }
    }

    async fn download_attachment(&self, handle: &str) -> Result<DownloadedAttachment, SinkError> {
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

    async fn edit_message(
        &self,
        conversation: &str,
        message_id: &str,
        markdown: &str,
        _link_preview: bool,
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

    async fn delete_message(&self, _conversation: &str, message_id: &str) -> Result<(), SinkError> {
        let mut deletes = self
            .deletes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        deletes.push(message_id.to_string());
        Ok(())
    }

    async fn moderate_member(
        &self,
        _conversation: &str,
        user_id: &str,
        action: MemberAction,
        until: Option<chrono::DateTime<chrono::Utc>>,
        _revoke_messages: bool,
    ) -> Result<(), SinkError> {
        let mut moderations = self
            .moderations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        moderations.push((user_id.to_string(), action, until));
        Ok(())
    }

    async fn set_member_rights(
        &self,
        _conversation: &str,
        _user_id: &str,
        _rights: &[MemberRight],
    ) -> Result<(), SinkError> {
        Ok(())
    }

    async fn set_member_roles(
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

    async fn pin_message(
        &self,
        _conversation: &str,
        _message_id: &str,
        _pin: bool,
        _silent: bool,
    ) -> Result<(), SinkError> {
        Ok(())
    }

    async fn set_chat(
        &self,
        _conversation: &str,
        _settings: ChatSettings,
    ) -> Result<(), SinkError> {
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
            user_id: user_id.unwrap_or("7").to_string(),
            display_name: Some("Bot".to_string()),
            status: MemberStatus::Administrator,
            rights: vec![MemberRight::RestrictMembers],
            presence: None,
        })
    }

    async fn list_members(
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

    async fn unseen(&self, _conversation: Option<&str>) -> Result<UnseenSummary, SinkError> {
        Ok(UnseenSummary {
            count: 0,
            newest: None,
            latest: None,
        })
    }

    async fn read_history(
        &self,
        conversation: &str,
        _limit: usize,
        _before: Option<i64>,
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
            timestamp: "2026-08-11T09:30:00+00:00".to_string(),
            cursor: 41,
        }])
    }

    async fn search_history(
        &self,
        _query: &str,
        _conversation: Option<&str>,
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
    tool_params("send_message", arguments)
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
        .expect("meka's rmcp version must complete the initialize handshake")
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
    assert!(
        instructions.contains("send_message"),
        "instructions should name the reply tool, got: {instructions}"
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
    assert!(names.contains(&"set_member_roles"), "got {names:?}");
    assert!(!names.contains(&"set_member_rights"), "got {names:?}");
    assert!(names.contains(&"moderate_member"), "got {names:?}");

    client.cancel().await.expect("clean shutdown");
}

#[tokio::test]
async fn setting_roles_round_trips_across_the_version_gap() {
    let harness = start().await;
    let client = connect(&harness).await;

    let result = client
        .call_tool(tool_params(
            "set_member_roles",
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
        "block",
        "delete_message",
        "download_attachment",
        "edit_message",
        "get_conversation",
        "list_conversations",
        "mute",
        "react",
        "read_history",
        "search_history",
        "send_file",
        "send_message",
        "unblock",
        "unmute",
        "unseen",
        "view_attachment",
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
            "{} lost its description across the version gap",
            tool.name
        );
        // meka resolves each tool's required permission from `readOnlyHint` when config does not
        // override it, so the annotation has to survive the version gap intact or the permission
        // model changes silently. The conversational surface is read-only on purpose -- classifying
        // the send tools as writes would leave a bridge at `read` unable to answer anybody -- while
        // the five that take irreversible action on somebody else's account ask for `write`.
        const NEEDS_WRITE: &[&str] = &[
            "delete_message",
            "moderate_member",
            "set_member_rights",
            "set_member_roles",
            "set_chat",
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
        .find(|tool| tool.name.as_ref() == "send_message")
        .expect("send_message is advertised");
    let schema = serde_json::to_value(&*send.input_schema).expect("schema serializes");
    let properties = schema
        .get("properties")
        .and_then(serde_json::Value::as_object)
        .expect("send_message has an object schema");
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
        ("send_message", "link_preview"),
        ("edit_message", "link_preview"),
        ("send_file", "link_preview"),
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
    let send_file = tools
        .iter()
        .find(|tool| tool.name.as_ref() == "send_file")
        .expect("send_file is advertised");
    let schema = serde_json::to_value(&*send_file.input_schema).expect("schema serializes");
    assert_eq!(
        schema["properties"]["paths"]["type"], "array",
        "send_file must take a list of paths: {schema}"
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
async fn the_history_cursor_survives_the_version_gap() {
    // `before` is a number, and every other tool argument this bridge takes is a string. Serde and
    // schemars agree locally, but meka's client is a major version behind and it is the one that
    // validates arguments in production, so the numeric form is exercised over a real socket.
    let harness = start().await;
    let client = connect(&harness).await;

    let tools = client.list_all_tools().await.expect("tools/list works");
    let read = tools
        .iter()
        .find(|tool| tool.name.as_ref() == "read_history")
        .expect("read_history is advertised");
    let schema = serde_json::to_value(&*read.input_schema).expect("schema serializes");
    assert_eq!(
        schema["properties"]["before"]["type"],
        serde_json::json!(["integer", "null"]),
        "an older client has to see the cursor as a number: {schema}"
    );

    let result = client
        .call_tool(tool_params(
            "read_history",
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
async fn the_policy_tools_round_trip_across_the_version_gap() {
    let harness = start().await;
    let client = connect(&harness).await;

    for (tool, expected) in [("mute", Policy::Mute), ("block", Policy::Block)] {
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
            "unblock",
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
    assert!(text.contains("list_conversations"), "got: {text}");

    client.cancel().await.expect("clean shutdown");
}

#[tokio::test]
async fn react_round_trips_across_the_version_gap() {
    let harness = start().await;
    let client = connect(&harness).await;

    let result = client
        .call_tool(tool_params(
            "react",
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
            "view_attachment",
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
            "view_attachment",
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
            "view_attachment",
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
            "download_attachment",
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
