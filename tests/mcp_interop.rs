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
    config::{McpConfig, McpTransport},
    mcp::{ConversationSummary, OutboundSink, SendOptions, SinkError, serve},
};
use rmcp2::{
    ServiceExt,
    model::{CallToolRequestParams, ClientInfo},
    transport::StreamableHttpClientTransport,
};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct RecordingSink {
    sent: Mutex<Vec<(String, String)>>,
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
        _path: &std::path::Path,
        _caption: Option<&str>,
        _as_photo: bool,
    ) -> Result<Vec<String>, SinkError> {
        Ok(vec!["2001".to_string()])
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
}

/// `CallToolRequestParams` is `#[non_exhaustive]`, so it has to be built through its constructor
/// rather than with a struct literal.
fn send_message_params(arguments: serde_json::Value) -> CallToolRequestParams {
    let mut params = CallToolRequestParams::new("send_message");
    params.arguments = arguments.as_object().cloned();
    params
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

/// Start the MCP server on an ephemeral loopback port.
async fn start() -> Harness {
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
            let result = serve::run(server, &config, sink, shutdown).await;
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
async fn all_tools_are_visible_to_an_older_client() {
    let harness = start().await;
    let client = connect(&harness).await;

    let tools = client.list_all_tools().await.expect("tools/list works");
    let mut names: Vec<&str> = tools.iter().map(|tool| tool.name.as_ref()).collect();
    names.sort_unstable();
    assert_eq!(names, vec![
        "get_conversation",
        "list_conversations",
        "send_file",
        "send_message",
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
        // override it. Every tool here is read-only on purpose: the send tools change nothing on
        // the machine, and classifying them as writes would leave a bridge run at `read` unable to
        // answer anybody. The annotation has to survive the version gap intact for that to hold.
        let read_only = tool
            .annotations
            .as_ref()
            .and_then(|annotations| annotations.read_only_hint);
        assert_eq!(read_only, Some(true), "readOnlyHint for {}", tool.name);
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
            let _ = serve::run(server, &config, sink, shutdown).await;
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
