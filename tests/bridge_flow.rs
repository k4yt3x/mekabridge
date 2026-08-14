//! End-to-end tests for the inbound and outbound paths.
//!
//! A stub meka (a real axum server speaking the real SSE wire format) stands in for the agent, and
//! a `MockChannel` stands in for Telegram. Together they let the whole loop run against real code:
//! events land in the durable queue, the drain loop batches them into a turn, and the sink delivers
//! what the agent asked for.

// Integration tests are their own crate, so clippy's `allow-*-in-tests` settings do not reach here.
// Assertions and timeout failures read better as `expect`/`panic` than as manual error plumbing.
#![allow(clippy::expect_used, clippy::panic)]

use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use axum::{Router, extract::State, response::IntoResponse, routing::post};
use chrono::Utc;
use mekabridge::{
    bridge::{
        BridgeSink,
        inbound::{self, DrainContext},
        turn::{Presence, TurnRunner},
    },
    channel::{
        Activity, Admission, Channel, ChannelCapabilities, ChannelError, ChannelId,
        ChannelIdentity, ChannelRegistry, ChatKind, ConversationId, FetchedFile, InboundEvent,
        InboundMessage, Platform, SendOptions, Sender,
    },
    config::{Config, DefaultPolicy, StorageConfig},
    mcp::{OutboundSink, ViewedAttachment},
    meka::MekaClient,
    store::{ConversationRecord, Policy, Store},
};
use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;

/// A 1x1 PNG, so attachment tests move real image bytes rather than an arbitrary blob.
const ONE_PIXEL_PNG: &[u8] = &[
    0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44, 0x52,
    0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x06, 0x00, 0x00, 0x00, 0x1F, 0x15, 0xC4,
    0x89, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x44, 0x41, 0x54, 0x78, 0xDA, 0x63, 0xFC, 0xCF, 0xC0, 0x50,
    0x0F, 0x00, 0x04, 0x85, 0x01, 0x80, 0x84, 0xA9, 0x8C, 0x21, 0x00, 0x00, 0x00, 0x00, 0x49, 0x45,
    0x4E, 0x44, 0xAE, 0x42, 0x60, 0x82,
];

/// How a scripted failure behaves. The three differ in exactly what the bridge is entitled to do
/// next, which is the whole of what these tests are about.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum FailureKind {
    /// Where a rate limit or an overload actually lands: meka's `RetryableProvider` has no arm of
    /// its own in the mapping onto a Problem Detail and falls through to `internal`. Nothing ran,
    /// so the batch may be handed over again.
    #[default]
    Transient,
    /// meka's bucket for an upstream failure its own agent loop has already tried and failed to
    /// repair. More attempts would only delay the notice saying an operator is needed.
    Unrepairable,
    /// The agent called a send tool and then the turn died. Nothing may be retried: the work is
    /// done and a second attempt would repeat it with the agent having no memory of the first.
    AfterActing,
}

/// What the stub meka observed, so tests can assert on the envelope the agent would have seen.
#[derive(Default)]
struct MekaRecorder {
    /// Bodies of turns meka actually accepted. A refusal never lands here, so a test asserting
    /// that a batch was delivered cannot be satisfied by one that was turned away.
    turns: Mutex<Vec<String>>,
    /// Every `POST /turn`, accepted or refused, for tests that measure submission rate.
    attempts: Mutex<usize>,
    /// Turns to fail before starting to succeed, for exercising the retry path.
    fail_first: Mutex<usize>,
    /// What those failures look like, which is what decides whether the batch may be tried again.
    failure: Mutex<FailureKind>,
    /// A conversation id that must appear in the envelope for a turn to be failed at all, so one
    /// chat can be kept in trouble while another is answered normally.
    fail_only: Mutex<Option<String>>,
    /// When each `POST /turn` arrived, for tests that measure the wait between attempts rather
    /// than how many there were.
    attempt_times: Mutex<Vec<std::time::Instant>>,
    /// Turns to answer with `session-not-found`, for exercising session recreation.
    forget_session_first: Mutex<usize>,
    /// Turns to answer with a stream that stops before any terminal event, simulating a dropped
    /// connection while the turn keeps running server-side.
    truncate_first: Mutex<usize>,
    /// What `GET /v1/sessions/{id}` reports for `turn_in_flight`.
    turn_in_flight: Mutex<bool>,
    /// Turns to refuse with a `turn-in-flight` 409, as meka does while it runs a turn of its own.
    busy_first: Mutex<usize>,
    /// Turns to answer with meka's empty-response stand-in and no tool calls.
    empty_first: Mutex<usize>,
    /// How long a turn takes to answer. The default of zero makes the suite fast; a test that
    /// needs something to happen *during* a turn sets it, since otherwise the turn is over
    /// before the test can act.
    turn_delay: Mutex<Duration>,
}

async fn create_session() -> impl IntoResponse {
    axum::Json(serde_json::json!({ "id": uuid::Uuid::new_v4() }))
}

async fn submit_turn(State(recorder): State<Arc<MekaRecorder>>, body: String) -> impl IntoResponse {
    let parsed = serde_json::from_str::<serde_json::Value>(&body).ok();
    let message = parsed
        .as_ref()
        .and_then(|value| {
            value
                .get("message")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        })
        .unwrap_or_default();
    *recorder
        .attempts
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) += 1;
    recorder
        .attempt_times
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .push(std::time::Instant::now());

    let should_forget = {
        let mut remaining = recorder
            .forget_session_first
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if *remaining > 0 {
            *remaining -= 1;
            true
        } else {
            false
        }
    };
    if should_forget {
        return (
            axum::http::StatusCode::NOT_FOUND,
            [(axum::http::header::CONTENT_TYPE, "application/problem+json")],
            r#"{"type":"https://meka.so/errors/session-not-found","title":"Session not found",
                "status":404,"detail":"gone"}"#
                .to_string(),
        )
            .into_response();
    }

    let should_refuse = {
        let mut remaining = recorder
            .busy_first
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if *remaining > 0 {
            *remaining -= 1;
            true
        } else {
            false
        }
    };
    if should_refuse {
        return (
            axum::http::StatusCode::CONFLICT,
            [(axum::http::header::CONTENT_TYPE, "application/problem+json")],
            r#"{"type":"https://meka.so/errors/turn-in-flight","title":"Turn already in flight",
                "status":409,"detail":"another turn is already in flight on this session"}"#
                .to_string(),
        )
            .into_response();
    }

    recorder
        .turns
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .push(message.clone());

    let delay = *recorder
        .turn_delay
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if !delay.is_zero() {
        tokio::time::sleep(delay).await;
    }

    let should_truncate = {
        let mut remaining = recorder
            .truncate_first
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if *remaining > 0 {
            *remaining -= 1;
            true
        } else {
            false
        }
    };
    if should_truncate {
        // A stream that stops mid-turn. meka would keep running the turn; the stub models that by
        // reporting `turn_in_flight` until the test flips it.
        *recorder
            .turn_in_flight
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
        return (
            [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
            "retry: 3000\n\n\
             event: turn.started\nid: 0\ndata: {\"turn_id\":\"t\",\"session_id\":\"s\"}\n\n\
             event: assistant_text.delta\nid: 1\ndata: {\"text\":\"partial\"}\n\n"
                .to_string(),
        )
            .into_response();
    }

    let should_be_empty = {
        let mut remaining = recorder
            .empty_first
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if *remaining > 0 {
            *remaining -= 1;
            true
        } else {
            false
        }
    };
    if should_be_empty {
        // What meka streams when the model comes back with no content at all.
        return (
            [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
            "retry: 3000\n\n\
             event: turn.started\nid: 0\ndata: {\"turn_id\":\"t\",\"session_id\":\"s\"}\n\n\
             event: assistant_text.delta\nid: 1\ndata: {\"text\":\"[The model returned an empty response.]\"}\n\n\
             event: turn.finished\nid: 2\ndata: {\"stop_reason\":\"end_turn\"}\n\n"
                .to_string(),
        )
            .into_response();
    }

    let should_fail = {
        let only = recorder
            .fail_only
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let eligible = only.is_none_or(|conversation| message.contains(&conversation));
        let mut remaining = recorder
            .fail_first
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if eligible && *remaining > 0 {
            *remaining -= 1;
            true
        } else {
            false
        }
    };

    let stream = if should_fail {
        let kind = *recorder
            .failure
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // The tool call is what makes the bridge treat the batch as spent: it counts sends off the
        // event stream, not off anything reaching a channel, so this is enough to stand in for an
        // agent that had already answered somebody when the turn died.
        let acted = if kind == FailureKind::AfterActing {
            "event: tool_call.executing\nid: 1\n\
             data: {\"id\":\"c1\",\"name\":\"mcp__mekabridge__send_message\"}\n\n"
        } else {
            ""
        };
        let error = if kind == FailureKind::Unrepairable {
            "{\"type\":\"https://meka.so/errors/provider\",\"title\":\"Provider failed\",\
             \"status\":502,\"detail\":\"upstream refused\"}"
        } else {
            "{\"type\":\"https://meka.so/errors/internal\",\"title\":\"Internal server error\",\
             \"status\":500,\
             \"detail\":\"provider temporarily unavailable: API returned status 429\"}"
        };
        format!(
            "retry: 3000\n\n\
             event: turn.started\nid: 0\ndata: {{\"turn_id\":\"t\",\"session_id\":\"s\"}}\n\n\
             {acted}\
             event: turn.failed\nid: 2\ndata: {{\"error\":{error}}}\n\n"
        )
    } else {
        "retry: 3000\n\n\
         event: turn.started\nid: 0\ndata: {\"turn_id\":\"t\",\"session_id\":\"s\"}\n\n\
         event: assistant_text.delta\nid: 1\ndata: {\"text\":\"ok\"}\n\n\
         event: turn.finished\nid: 2\ndata: {\"stop_reason\":\"end_turn\",\
         \"usage\":{\"input_tokens\":1,\"output_tokens\":1}}\n\n"
            .to_string()
    };

    (
        [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
        stream,
    )
        .into_response()
}

async fn cancel_turn() -> impl IntoResponse {
    axum::http::StatusCode::NO_CONTENT
}

async fn get_session(
    State(recorder): State<Arc<MekaRecorder>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let in_flight = *recorder
        .turn_in_flight
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    axum::Json(serde_json::json!({
        "id": id,
        "permission": "write",
        "title": "stub",
        "turn_in_flight": in_flight,
    }))
}

async fn info() -> impl IntoResponse {
    axum::Json(serde_json::json!({
        "version": "test",
        "model": "test-model",
        "vision": true,
    }))
}

/// Start a stub meka on an ephemeral port.
async fn start_meka(recorder: Arc<MekaRecorder>) -> (SocketAddr, CancellationToken) {
    let router = Router::new()
        .route("/v1/sessions", post(create_session))
        .route("/v1/sessions/{id}", axum::routing::get(get_session))
        .route("/v1/sessions/{id}/turn", post(submit_turn))
        .route("/v1/sessions/{id}/cancel", post(cancel_turn))
        .route("/v1/info", axum::routing::get(info))
        .with_state(recorder);
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("binds");
    let address = listener.local_addr().expect("address");
    let shutdown = CancellationToken::new();
    tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            let _ = axum::serve(listener, router)
                .with_graceful_shutdown(async move { shutdown.cancelled().await })
                .await;
        }
    });
    (address, shutdown)
}

/// A channel that records what it was asked to send and never talks to a network.
struct MockChannel {
    id: ChannelId,
    /// Whether this stands in for a platform that reports somebody else typing. Off by default,
    /// matching Telegram, which is the shape most of these tests want. Flipped after construction
    /// rather than passed in, so the harness constructors do not grow an eighth argument for it.
    typing_status: std::sync::atomic::AtomicBool,
    sent: Mutex<Vec<(String, String)>>,
    reactions: Mutex<Vec<(String, String, Option<String>)>>,
    activities: Mutex<Vec<Activity>>,
    /// Files this channel will hand back from `fetch`, keyed by reference.
    files: Mutex<std::collections::HashMap<String, Vec<u8>>>,
}

impl MockChannel {
    /// How many typing/upload indicators have been raised, for tests that watch presence.
    fn activity_count(&self) -> usize {
        self.activities
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len()
    }

    /// Stand in for a platform that does report typing, the way Discord does.
    fn report_typing(&self) {
        self.typing_status
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    fn new(id: &str) -> Self {
        Self {
            id: ChannelId::new(id),
            typing_status: std::sync::atomic::AtomicBool::new(false),
            sent: Mutex::new(Vec::new()),
            reactions: Mutex::new(Vec::new()),
            activities: Mutex::new(Vec::new()),
            files: Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// Make a file available to `fetch` under `file_ref`.
    fn put_file(&self, file_ref: &str, bytes: Vec<u8>) {
        self.files
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(file_ref.to_string(), bytes);
    }

    fn sent(&self) -> Vec<(String, String)> {
        self.sent
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

#[async_trait]
impl Channel for MockChannel {
    fn id(&self) -> &ChannelId {
        &self.id
    }

    fn platform(&self) -> Platform {
        Platform::Telegram
    }

    fn capabilities(&self) -> ChannelCapabilities {
        ChannelCapabilities {
            member_rights: false,
            member_roles: false,
            typing_indicator: true,
            files: true,
            photos: true,
            reactions: true,
            edit: true,
            admin: true,
            presence: false,
            typing_status: self.typing_status.load(std::sync::atomic::Ordering::SeqCst),
        }
    }

    async fn run(
        self: Arc<Self>,
        _sink: mpsc::Sender<InboundEvent>,
        shutdown: CancellationToken,
    ) -> Result<(), ChannelError> {
        shutdown.cancelled().await;
        Ok(())
    }

    async fn send_text(
        &self,
        conversation: &ConversationId,
        markdown: &str,
        _options: &SendOptions,
    ) -> Result<Vec<String>, ChannelError> {
        let mut sent = self
            .sent
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        sent.push((conversation.as_str().to_string(), markdown.to_string()));
        Ok(vec![format!("m{}", sent.len())])
    }

    async fn send_file(
        &self,
        conversation: &ConversationId,
        path: &std::path::Path,
        _caption: Option<&str>,
        _as_photo: bool,
    ) -> Result<Vec<String>, ChannelError> {
        let mut sent = self
            .sent
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        sent.push((
            conversation.as_str().to_string(),
            format!("<file {}>", path.display()),
        ));
        Ok(vec!["f1".to_string()])
    }

    async fn fetch(&self, file_ref: &str, max_bytes: u64) -> Result<FetchedFile, ChannelError> {
        let bytes = self
            .files
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(file_ref)
            .cloned()
            .ok_or_else(|| ChannelError::Delivery {
                channel: self.id.as_str().to_string(),
                message: format!("no such file {file_ref}"),
            })?;
        if bytes.len() as u64 > max_bytes {
            return Err(ChannelError::Delivery {
                channel: self.id.as_str().to_string(),
                message: format!(
                    "the file is {} bytes, over the configured limit of {max_bytes} bytes",
                    bytes.len()
                ),
            });
        }
        Ok(FetchedFile {
            bytes,
            media_type: Some("image/png".to_string()),
            extension: Some("png".to_string()),
        })
    }

    async fn react(
        &self,
        conversation: &ConversationId,
        message_id: &str,
        emoji: Option<&str>,
    ) -> Result<(), ChannelError> {
        let mut reactions = self
            .reactions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        reactions.push((
            conversation.as_str().to_string(),
            message_id.to_string(),
            emoji.map(str::to_string),
        ));
        Ok(())
    }

    async fn set_activity(
        &self,
        _conversation: &ConversationId,
        activity: Activity,
    ) -> Result<(), ChannelError> {
        self.activities
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(activity);
        Ok(())
    }

    async fn probe(&self) -> Result<ChannelIdentity, ChannelError> {
        Ok(ChannelIdentity {
            id: "1".to_string(),
            display_name: "Mock".to_string(),
            username: Some("mockbot".to_string()),
            reads_all_group_messages: true,
        })
    }
}

/// Build a sink with a scratch attachment directory and a meka client pointed nowhere.
///
/// The address is unreachable on purpose: only the fetch tools consult meka (for the vision flag),
/// and a failed probe degrades to "describe rather than show", which is the behaviour under test in
/// the cases that use this.
fn sink_for(store: Store, channels: Arc<ChannelRegistry>) -> BridgeSink {
    sink_with_storage(
        store,
        channels,
        std::env::temp_dir().join("mekabridge-test-attachments"),
        Arc::new(Presence::default()),
    )
}

fn sink_with_storage(
    store: Store,
    channels: Arc<ChannelRegistry>,
    attachment_dir: std::path::PathBuf,
    presence: Arc<Presence>,
) -> BridgeSink {
    let storage = StorageConfig {
        path: std::path::PathBuf::from("/tmp/mekabridge-unused.db"),
        attachment_dir,
        attachment_max_bytes: 20 * 1024 * 1024,
        attachment_retention: Duration::from_secs(86_400),
        history_retention: Duration::from_secs(86_400),
    };
    let meka = MekaClient::new(
        &config_for(
            ([127, 0, 0, 1], 1).into(),
            std::path::Path::new("/tmp/mekabridge-unused.db"),
            0,
            false,
            "20s",
        )
        .meka,
    )
    .expect("client builds");
    BridgeSink::new(
        store,
        channels,
        storage,
        DefaultPolicy {
            direct: Policy::Active,
            group: Policy::Mute,
            channel: Policy::Mute,
        },
        meka,
        presence,
    )
}

fn config_for(
    meka_address: SocketAddr,
    database: &std::path::Path,
    retries: u32,
    typing: bool,
    turn_timeout: &str,
) -> Config {
    let raw = format!(
        r#"
[meka]
base_url = "http://{meka_address}"
token = "test-token"
turn_timeout = "{turn_timeout}"

[bridge]
batch_max_messages = 32
max_queue_depth = 64
turn_retries = {retries}
typing_indicator = {typing}
[storage]
path = "{}"

[[channels.telegram]]
id = "mock"
token = "123:fake"
allowed_users = [1]
"#,
        database.display()
    );
    let mut config =
        Config::from_toml(&raw, std::path::Path::new("/tmp/config.toml")).expect("valid config");
    // The floor is set per test by `Setup`, since the suite pulls both ways on it; see there.
    //
    // The retry base is not in the file format for the same reason: at the shipped ten seconds
    // any test covering a retry would sleep through a real backoff, and every test that merely
    // tolerates one would take the same hit. Not scaled as far down as the floor, because a test
    // that asserts a wait happened has to be able to tell it from the round trip it replaced.
    config.bridge.retry_base = Duration::from_millis(250);
    config
}

fn message(text: &str, external_id: &str) -> InboundEvent {
    InboundEvent::Message(Box::new(InboundMessage {
        channel: ChannelId::new("mock"),
        platform: Platform::Telegram,
        conversation: ConversationId::parse("mock:1").expect("valid"),
        external_id: external_id.to_string(),
        message_id: external_id.to_string(),
        chat_kind: ChatKind::Direct,
        chat_title: None,
        sender: Sender {
            id: "1".to_string(),
            display_name: "Alice".to_string(),
            username: Some("alice".to_string()),
            is_bot: false,
            on_behalf_of_chat: false,
        },
        admission: Admission::User,
        sender_allowlisted: true,
        sender_roles: Vec::new(),
        addressed: false,
        text: text.to_string(),
        reply_to: None,
        edited_at: None,
        forwarded_from: None,
        group_id: None,
        notes: Vec::new(),
        arrived_mid_turn: false,
        attachments: Vec::new(),
        timestamp: Utc::now(),
    }))
}

/// Everything a test needs, wired the way the daemon wires it.
struct Harness {
    store: Store,
    sender: mpsc::Sender<InboundEvent>,
    channel: Arc<MockChannel>,
    recorder: Arc<MekaRecorder>,
    shutdown: CancellationToken,
    meka_shutdown: CancellationToken,
    _directory: tempfile::TempDir,
}

/// The parts of the config a test may want that the shared TOML does not cover. Most of the suite
/// takes the default, which is why these live here rather than as more positional arguments on a
/// constructor that already has several.
#[derive(Debug, Clone)]
struct Setup {
    /// Where a delivery failure gets reported in detail.
    owner: Option<String>,
    /// Whether the affected chat hears about one at all.
    notify_failures: bool,
    /// Not reachable from the file format on purpose, so it is set here instead. At the shipped
    /// second the suite would sleep through a real floor on every test that waits for a turn,
    /// which is most of them.
    ///
    /// The default is a compromise between two groups of tests pulling opposite ways. The floor is
    /// measured from the platform's send time, so it only coalesces a burst if the writer persists
    /// every part of it inside the window, and on a loaded machine that takes longer than it
    /// looks; meanwhile the tests that assert a message is *not* held measure against this
    /// same number. Burst tests take [`Setup::coalescing`] rather than pushing the default up.
    coalesce_floor: Duration,
    /// Scaled down from the shipped 3s/30s so the suite exercises the same logic without sleeping
    /// through a real quiet period on every test. Here rather than in the TOML because a test that
    /// asserts the quiet period was *not* applied needs it far enough from the floor to tell the
    /// two apart; see [`Setup::patient`].
    settle: Duration,
    settle_max: Duration,
    /// Whether the bridge announces a turn in the originating chats. Off for most of the suite, so
    /// it is not measuring presence it does not care about.
    typing_indicator: bool,
    /// `[meka].turn_timeout`, which also bounds how long `submit` retries a refusal before giving
    /// up and letting `deliver` release the batch.
    turn_timeout: String,
    /// Zero writes no message history at all, which is what makes an undeliverable message
    /// unrecoverable: there is no row to put back among what the agent has not seen.
    history_retention: Duration,
}

impl Default for Setup {
    fn default() -> Self {
        Self {
            owner: None,
            notify_failures: true,
            coalesce_floor: Duration::from_millis(100),
            settle: Duration::from_millis(150),
            settle_max: Duration::from_millis(600),
            typing_indicator: false,
            turn_timeout: "20s".to_string(),
            history_retention: Duration::from_secs(86_400),
        }
    }
}

impl Setup {
    /// A floor generous enough to hold a burst together while the writer persists it, for the tests
    /// that are about coalescing rather than about latency.
    fn coalescing() -> Self {
        Self {
            coalesce_floor: Duration::from_millis(600),
            // Clear of the floor, or the ceiling would release the burst at the very moment the
            // floor was meant to be holding it together.
            settle_max: Duration::from_secs(3),
            ..Self::default()
        }
    }

    /// A quiet period long enough that a test asserting it was skipped can say so without racing
    /// the floor. At the default the two are 50ms apart, which on a loaded machine is noise.
    fn patient() -> Self {
        Self {
            settle: Duration::from_secs(2),
            settle_max: Duration::from_secs(4),
            ..Self::default()
        }
    }
}

impl Harness {
    async fn start(retries: u32, fail_first: usize) -> Self {
        Self::start_with(retries, fail_first, 0).await
    }

    /// A harness whose turns take `delay` to answer, for testing what happens mid-turn.
    async fn start_slow(delay: Duration) -> Self {
        let harness = Self::start(1, 0).await;
        *harness
            .recorder
            .turn_delay
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = delay;
        harness
    }

    async fn start_with(retries: u32, fail_first: usize, forget_first: usize) -> Self {
        Self::start_full(retries, fail_first, forget_first, 0).await
    }

    /// A harness with the typing indicator on, which the others leave off so the suite is not
    /// measuring presence it does not care about.
    /// A harness whose turn budget is shorter than two retry intervals, so a persistent refusal
    /// makes `submit` give up and `deliver` genuinely release and rebuild the batch. Without that
    /// the envelope is built once and reused across `submit`'s internal retries, and anything about
    /// rebuilding it is untestable.
    async fn start_impatient(busy: usize) -> Self {
        let harness = Self::start_all(1, 0, 0, 0, 0, Setup {
            turn_timeout: "3s".to_string(),
            ..Setup::default()
        })
        .await;
        *harness
            .recorder
            .busy_first
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = busy;
        harness
    }

    /// A harness whose floor is generous enough to hold a burst together while the writer persists
    /// it, for the tests that are about coalescing rather than about latency.
    async fn start_coalescing() -> Self {
        Self::start_all(1, 0, 0, 0, 0, Setup::coalescing()).await
    }

    /// A harness whose quiet period is long enough to be unmistakable, for the tests about whether
    /// one was applied at all.
    async fn start_patient() -> Self {
        Self::start_all(1, 0, 0, 0, 0, Setup::patient()).await
    }

    async fn start_with_typing() -> Self {
        Self::start_all(1, 0, 0, 0, 0, Setup {
            typing_indicator: true,
            ..Setup::default()
        })
        .await
    }

    async fn start_full(
        retries: u32,
        fail_first: usize,
        forget_first: usize,
        truncate_first: usize,
    ) -> Self {
        Self::start_all(
            retries,
            fail_first,
            forget_first,
            truncate_first,
            0,
            Setup::default(),
        )
        .await
    }

    /// A harness set up to be watched failing: a scripted failure of a given shape, somewhere to
    /// report it, and a say in whether the chat itself hears about it.
    async fn start_failing(retries: u32, failure: FailureKind, setup: Setup) -> Self {
        // Far more failures scripted than any of these tests will get through, so the batch never
        // succeeds by running out of them.
        let harness = Self::start_all(retries, 64, 0, 0, 0, setup).await;
        *harness
            .recorder
            .failure
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = failure;
        harness
    }

    async fn start_all(
        retries: u32,
        fail_first: usize,
        forget_first: usize,
        truncate_first: usize,
        empty_first: usize,
        setup: Setup,
    ) -> Self {
        let directory = tempfile::tempdir().expect("tempdir");
        let database = directory.path().join("state.db");
        let recorder = Arc::new(MekaRecorder::default());
        *recorder
            .fail_first
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = fail_first;
        *recorder
            .forget_session_first
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = forget_first;
        *recorder
            .truncate_first
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = truncate_first;
        *recorder
            .empty_first
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = empty_first;

        let (meka_address, meka_shutdown) = start_meka(Arc::clone(&recorder)).await;
        let config = Arc::new(config_for(
            meka_address,
            &database,
            retries,
            setup.typing_indicator,
            &setup.turn_timeout,
        ));

        let mut config = config;
        // Set here rather than through `config_for`, which validates the owner against the
        // configured channels and would need the whole TOML threading through for two fields.
        {
            let config = Arc::get_mut(&mut config).expect("sole reference");
            config.bridge.owner_conversation = setup.owner;
            config.bridge.notify_failures = setup.notify_failures;
            config.bridge.coalesce_floor = setup.coalesce_floor;
            config.bridge.settle = setup.settle;
            config.bridge.settle_max = setup.settle_max;
            config.storage.history_retention = setup.history_retention;
        }

        let store = Store::open(&config.storage.path)
            .await
            .expect("store opens");
        let channel = Arc::new(MockChannel::new("mock"));
        let channels = Arc::new(ChannelRegistry::from_channels([
            Arc::clone(&channel) as Arc<dyn Channel>
        ]));
        let meka = MekaClient::new(&config.meka).expect("client builds");

        let shutdown = CancellationToken::new();
        let wake = Arc::new(Notify::new());
        let (sender, receiver) = mpsc::channel(16);
        let typing = Arc::new(inbound::TypingState::default());

        tokio::spawn({
            let store = store.clone();
            let config = Arc::clone(&config);
            let wake = Arc::clone(&wake);
            let typing = Arc::clone(&typing);
            async move { inbound::writer(store, config, receiver, wake, typing).await }
        });
        tokio::spawn({
            let context = DrainContext {
                typing,
                store: store.clone(),
                config: Arc::clone(&config),
                meka: meka.clone(),
                channels: Arc::clone(&channels),
                runner: TurnRunner::new(
                    meka,
                    channels,
                    // From the config rather than pinned off, so a test can exercise presence.
                    config.bridge.typing_indicator,
                    config.bridge.typing_max,
                    Arc::new(Presence::default()),
                ),
                identities: Arc::new(tokio::sync::OnceCell::new()),
                permission_checked: Arc::new(tokio::sync::OnceCell::new()),
                notices: inbound::NoticeLog::default(),
            };
            let shutdown = shutdown.clone();
            async move { inbound::drain_loop(context, wake, shutdown).await }
        });

        Self {
            store,
            sender,
            channel,
            recorder,
            shutdown,
            meka_shutdown,
            _directory: directory,
        }
    }

    fn attempts(&self) -> usize {
        *self
            .recorder
            .attempts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn turns(&self) -> Vec<String> {
        self.recorder
            .turns
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// Poll until `predicate` holds or the deadline passes.
    async fn wait_for(&self, label: &str, predicate: impl Fn(&Self) -> bool) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while tokio::time::Instant::now() < deadline {
            if predicate(self) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!(
            "timed out waiting for {label}; turns so far: {:?}",
            self.turns()
        );
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.shutdown.cancel();
        self.meka_shutdown.cancel();
    }
}

#[tokio::test]
async fn an_inbound_message_becomes_a_turn_carrying_its_routing_header() {
    let harness = Harness::start(1, 0).await;
    harness
        .sender
        .send(message("check the deploy logs", "1"))
        .await
        .expect("queued");

    harness
        .wait_for("the turn to be submitted", |harness| {
            !harness.turns().is_empty()
        })
        .await;

    let turns = harness.turns();
    assert_eq!(turns.len(), 1);
    let envelope = turns.first().expect("one turn");
    assert!(
        envelope.contains("conversation: mock:1"),
        "got:\n{envelope}"
    );
    assert!(
        envelope.contains("check the deploy logs"),
        "got:\n{envelope}"
    );
    assert!(
        envelope.contains("from: Alice (@alice, id 1)"),
        "got:\n{envelope}"
    );
    assert!(
        envelope.contains("[mekabridge] You are @mockbot on mock."),
        "got:\n{envelope}"
    );
}

#[tokio::test]
async fn the_queue_is_drained_after_a_successful_turn() {
    let harness = Harness::start(1, 0).await;
    harness
        .sender
        .send(message("hello", "1"))
        .await
        .expect("queued");
    harness
        .wait_for("the turn to complete", |harness| {
            !harness.turns().is_empty()
        })
        .await;

    // Give the drain loop a moment to record completion.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let stats = harness.store.queue_stats().await.expect("stats");
    assert_eq!(stats.pending, 0);
    assert_eq!(stats.in_flight, 0);
    assert_eq!(stats.done, 1);
    assert!(
        harness.store.last_turn_at().await.expect("read").is_some(),
        "a completed turn must be recorded for `mekabridge status`"
    );
}

#[tokio::test]
async fn every_turn_names_the_account_the_agent_appears_as() {
    // Stated per turn rather than once at session start, because a one-time orientation is an
    // ordinary user message and the first compaction summarises it away for good.
    let harness = Harness::start(1, 0).await;
    harness
        .sender
        .send(message("first", "1"))
        .await
        .expect("queued");
    harness
        .wait_for("the first turn", |harness| !harness.turns().is_empty())
        .await;
    harness
        .sender
        .send(message("second", "2"))
        .await
        .expect("queued");
    harness
        .wait_for("the second turn", |harness| harness.turns().len() >= 2)
        .await;

    let turns = harness.turns();
    for (index, turn) in turns.iter().take(2).enumerate() {
        assert!(
            turn.contains("[mekabridge] You are @mockbot on mock."),
            "turn {} must name the bot's own account:\n{}",
            index + 1,
            turn
        );
    }
}

#[tokio::test]
async fn duplicate_deliveries_are_ignored() {
    let harness = Harness::start(1, 0).await;
    // Telegram replays updates whose offset was never committed, so the same message id can arrive
    // twice after a crash.
    harness
        .sender
        .send(message("hello", "same-id"))
        .await
        .expect("queued");
    harness
        .sender
        .send(message("hello", "same-id"))
        .await
        .expect("queued");

    harness
        .wait_for("a turn", |harness| !harness.turns().is_empty())
        .await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let turns = harness.turns();
    let delivered: usize = turns
        .iter()
        .map(|envelope| envelope.matches("--- message").count())
        .sum();
    assert_eq!(
        delivered, 1,
        "the duplicate must not reach the agent: {turns:?}"
    );
}

#[tokio::test]
async fn a_failed_turn_is_retried_and_then_given_up_on() {
    // One retry configured, two failures scripted: the batch is attempted twice and then abandoned.
    let harness = Harness::start(1, 2).await;
    harness
        .sender
        .send(message("hello", "1"))
        .await
        .expect("queued");

    harness
        .wait_for("both attempts", |harness| harness.turns().len() >= 2)
        .await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let stats = harness.store.queue_stats().await.expect("stats");
    assert_eq!(
        stats.failed, 1,
        "the batch must end up failed, not retried forever"
    );
    assert_eq!(stats.pending, 0);
}

#[tokio::test]
async fn a_second_attempt_waits_rather_than_going_straight_back() {
    // The defect this pins: a failed batch used to be reoffered on the very next pass of the drain
    // loop, because readiness is measured from the platform's send time and that is long past by
    // the time a turn has failed. For the failure the budget exists for, an upstream out of quota,
    // both attempts then landed inside the same window and the message was declared undeliverable
    // seconds after it arrived.
    let harness = Harness::start_failing(3, FailureKind::Transient, Setup::default()).await;
    harness
        .sender
        .send(message("hello", "1"))
        .await
        .expect("queued");

    harness
        .wait_for("a third attempt", |harness| harness.attempts() >= 3)
        .await;

    let times = harness
        .recorder
        .attempt_times
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    let first = times[1].duration_since(times[0]);
    assert!(
        first >= Duration::from_millis(200),
        "the retry came back after only {first:?}, so nothing waited"
    );
    let second = times[2].duration_since(times[1]);
    assert!(
        second >= Duration::from_millis(450),
        "the second wait was {second:?}, so it did not double"
    );
}

#[tokio::test]
async fn traffic_in_another_chat_does_not_release_a_deferred_one() {
    // The ordering inside `readiness`. A batch waiting out a rate limit is by then long past
    // `settle_max`, since that is measured from when the message was sent, so a ceiling checked
    // first would release it into the very window it just bounced off. Nothing notices while the
    // drain loop is asleep on the deferral itself; it takes a message somewhere else to wake it
    // mid-wait, which is what this arranges.
    // A budget deep enough that the chat is still in the queue throughout, so any extra submission
    // is a release rather than the last attempt of a batch on its way out.
    let harness = Harness::start_failing(8, FailureKind::Transient, Setup::default()).await;
    *harness
        .recorder
        .fail_only
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some("mock:1".to_string());

    harness
        .sender
        .send(message("hello", "1"))
        .await
        .expect("queued");
    // Four attempts in, the next wait is two seconds: long enough that the chat spends nearly all
    // of it past the 600ms ceiling, which is the state the ordering has to survive.
    harness
        .wait_for("the wait to grow past the ceiling", |harness| {
            attempts_at(harness, "mock:1") >= 4
        })
        .await;
    let before = attempts_at(&harness, "mock:1");

    for index in 0..7 {
        harness
            .sender
            .send(message_elsewhere("unrelated", &format!("b{index}")))
            .await
            .expect("queued");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    assert_eq!(
        attempts_at(&harness, "mock:1"),
        before,
        "a message in another chat woke the drain loop and it released the deferred one"
    );
}

/// How many turns carried a message from `conversation`, failures included.
fn attempts_at(harness: &Harness, conversation: &str) -> usize {
    harness
        .turns()
        .iter()
        .filter(|envelope| envelope.contains(&format!("conversation: {conversation}")))
        .count()
}

#[tokio::test]
async fn an_unrepairable_failure_is_not_retried_at_all() {
    // meka maps both its `Provider` and `InvalidRequest` errors onto this type, and both are ones
    // its own agent loop has already tried and failed to repair. Spending the budget on them only
    // delays the notice that says an operator is needed.
    let harness = Harness::start_failing(3, FailureKind::Unrepairable, Setup::default()).await;
    harness
        .sender
        .send(message("hello", "1"))
        .await
        .expect("queued");

    harness
        .wait_for("the first attempt", |harness| harness.attempts() >= 1)
        .await;
    // Longer than three of the harness's backoffs, so a second attempt would have happened by now.
    tokio::time::sleep(Duration::from_millis(1200)).await;

    assert_eq!(
        harness.attempts(),
        1,
        "an error needing an operator must not be tried four times first"
    );
    let stats = harness.store.queue_stats().await.expect("stats");
    assert_eq!(stats.failed, 1);
    assert_eq!(stats.pending, 0);
}

#[tokio::test]
async fn a_turn_that_failed_after_the_agent_acted_is_not_retried() {
    // meka only retries an upstream failure while nothing has reached its frontend, so one that
    // gets as far as the bridge may well have a sent message and a shell command behind it.
    // Handing the batch over again would repeat both, and the agent would have no memory of the
    // first run to tell it from the second.
    let harness = Harness::start_failing(3, FailureKind::AfterActing, Setup::default()).await;
    harness
        .sender
        .send(message("hello", "1"))
        .await
        .expect("queued");

    harness
        .wait_for("the first attempt", |harness| harness.attempts() >= 1)
        .await;
    tokio::time::sleep(Duration::from_millis(1200)).await;

    assert_eq!(
        harness.attempts(),
        1,
        "the turn already had side effects, so it must not be run again"
    );
    let stats = harness.store.queue_stats().await.expect("stats");
    assert_eq!(stats.failed, 0);
    assert_eq!(stats.pending, 0);
}

#[tokio::test]
async fn an_undeliverable_message_is_owed_to_the_agent_again() {
    // A message is marked seen the moment it is queued, on the assumption that the agent is about
    // to be handed it. Running out of attempts is exactly where that assumption fails, and without
    // putting it back the message is neither delivered nor owed: absent from `unseen`, from the
    // missed-context lookback, and from the `mekabridge unseen` predicate.
    let harness = Harness::start_failing(0, FailureKind::Transient, Setup::default()).await;
    harness
        .sender
        .send(message("are you there?", "1"))
        .await
        .expect("queued");

    harness
        .wait_for("the batch to be given up on", |harness| {
            harness.attempts() >= 1
        })
        .await;
    tokio::time::sleep(Duration::from_millis(400)).await;

    let summary = harness
        .store
        .unseen_summary(Some("mock:1"))
        .await
        .expect("summary");
    assert_eq!(
        summary.count, 1,
        "a message that never reached the agent must still be owed to it"
    );
}

#[tokio::test]
async fn a_chat_is_told_that_something_broke_and_nothing_more() {
    // Whoever is in the chat did nothing wrong, cannot act on an upstream status code, and is not
    // necessarily somebody an operator would hand one to. The detail goes to the owner instead.
    let harness = Harness::start_failing(0, FailureKind::Transient, Setup::default()).await;
    harness
        .sender
        .send(message("hello", "1"))
        .await
        .expect("queued");

    harness
        .wait_for("the chat to be told", |harness| {
            !harness.channel.sent().is_empty()
        })
        .await;

    let sent = harness.channel.sent();
    assert_eq!(sent.len(), 1, "one apology, not one per attempt");
    let (conversation, text) = &sent[0];
    assert_eq!(
        conversation, "mock:1",
        "told the chat the message came from"
    );
    assert!(text.contains("went wrong"), "got {text:?}");
    for leak in ["429", "500", "internal", "provider", "meka"] {
        assert!(
            !text.to_ascii_lowercase().contains(leak),
            "the chat was told {leak:?}, which is the owner's business: {text:?}"
        );
    }
}

#[tokio::test]
async fn the_owner_hears_what_the_chat_is_spared() {
    let harness = Harness::start_failing(0, FailureKind::Transient, Setup {
        owner: Some("mock:99".to_string()),
        notify_failures: true,
        ..Setup::default()
    })
    .await;
    harness
        .sender
        .send(message("hello", "1"))
        .await
        .expect("queued");

    harness
        .wait_for("both notices", |harness| harness.channel.sent().len() >= 2)
        .await;

    let sent = harness.channel.sent();
    let owner = sent
        .iter()
        .find(|(conversation, _)| conversation == "mock:99")
        .expect("the owner must be told");
    assert!(
        owner.1.contains("429"),
        "the owner needs the actual error: {:?}",
        owner.1
    );
    assert!(
        owner.1.contains("mock:1"),
        "and which conversation lost something: {:?}",
        owner.1
    );
    assert!(
        sent.iter()
            .any(|(conversation, _)| conversation == "mock:1"),
        "the chat itself must still be told, vaguely"
    );
}

#[tokio::test]
async fn the_owner_is_not_promised_a_recovery_that_cannot_happen() {
    // With `[storage].history_retention` at zero nothing is written to the history, so there is no
    // row for `mark_unseen` to put back and the message really is gone. Telling the owner it will
    // come back as unseen context would send them looking for something that does not exist, in
    // the one configuration where a message is genuinely lost.
    let harness = Harness::start_failing(0, FailureKind::Transient, Setup {
        owner: Some("mock:99".to_string()),
        history_retention: Duration::ZERO,
        ..Setup::default()
    })
    .await;
    harness
        .sender
        .send(message("hello", "1"))
        .await
        .expect("queued");

    harness
        .wait_for("the owner to be told", |harness| {
            harness
                .channel
                .sent()
                .iter()
                .any(|(conversation, _)| conversation == "mock:99")
        })
        .await;

    let sent = harness.channel.sent();
    let owner = sent
        .iter()
        .find(|(conversation, _)| conversation == "mock:99")
        .expect("told");
    assert!(
        owner.1.contains("nothing will bring them back"),
        "the owner must be told the message is gone: {:?}",
        owner.1
    );
    assert!(
        !owner.1.contains("unseen"),
        "and not pointed at a backlog that was never written: {:?}",
        owner.1
    );
}

#[tokio::test]
async fn a_chat_hears_nothing_when_notify_failures_is_off() {
    // The bridge writing chat content of its own is a real exception to how it otherwise behaves,
    // so an operator who does not want the bot speaking unprompted in a group can say so.
    let harness = Harness::start_failing(0, FailureKind::Transient, Setup {
        owner: Some("mock:99".to_string()),
        notify_failures: false,
        ..Setup::default()
    })
    .await;
    harness
        .sender
        .send(message("hello", "1"))
        .await
        .expect("queued");

    harness
        .wait_for("the owner to be told", |harness| {
            !harness.channel.sent().is_empty()
        })
        .await;
    tokio::time::sleep(Duration::from_millis(400)).await;

    let sent = harness.channel.sent();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].0, "mock:99", "only the owner may hear about it");
}

#[tokio::test]
async fn a_chat_the_agent_answered_is_not_apologised_to() {
    // An apology after the agent has just replied contradicts what it said. The owner still hears,
    // because a turn that died after acting is exactly the kind worth looking into.
    let harness = Harness::start_failing(0, FailureKind::AfterActing, Setup {
        owner: Some("mock:99".to_string()),
        notify_failures: true,
        ..Setup::default()
    })
    .await;
    harness
        .sender
        .send(message("hello", "1"))
        .await
        .expect("queued");

    harness
        .wait_for("the owner to be told", |harness| {
            !harness.channel.sent().is_empty()
        })
        .await;
    tokio::time::sleep(Duration::from_millis(400)).await;

    let sent = harness.channel.sent();
    assert_eq!(sent.len(), 1, "got {sent:?}");
    assert_eq!(sent[0].0, "mock:99");
    // And told the right story about it. "Could not deliver" would be false here: the agent read
    // the messages and acted on them, and what needs looking at is the work it left half done.
    assert!(
        sent[0].1.contains("half finished"),
        "the owner was told the wrong thing: {:?}",
        sent[0].1
    );
    assert!(
        !sent[0].1.contains("could not deliver"),
        "the messages did reach the agent: {:?}",
        sent[0].1
    );
}

#[tokio::test]
async fn a_chat_is_not_apologised_to_twice_for_the_same_outage() {
    // An upstream out of quota for half an hour would otherwise write an apology into every
    // affected chat every time a batch ran out of attempts, which is worse than the silence it
    // replaced.
    let harness = Harness::start_failing(0, FailureKind::Transient, Setup::default()).await;
    harness
        .sender
        .send(message("hello", "1"))
        .await
        .expect("queued");
    harness
        .wait_for("the first apology", |harness| {
            !harness.channel.sent().is_empty()
        })
        .await;

    harness
        .sender
        .send(message("are you there?", "2"))
        .await
        .expect("queued");
    harness
        .wait_for("the second message to fail too", |harness| {
            harness.attempts() >= 2
        })
        .await;
    tokio::time::sleep(Duration::from_millis(400)).await;

    assert_eq!(
        harness.channel.sent().len(),
        1,
        "the same chat was apologised to twice inside the suppression window"
    );
}

#[tokio::test]
async fn a_transient_failure_is_recovered_by_the_retry() {
    let harness = Harness::start(1, 1).await;
    harness
        .sender
        .send(message("hello", "1"))
        .await
        .expect("queued");

    harness
        .wait_for("the retry to succeed", |harness| harness.turns().len() >= 2)
        .await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let stats = harness.store.queue_stats().await.expect("stats");
    assert_eq!(stats.done, 1);
    assert_eq!(stats.failed, 0);
}

#[tokio::test]
async fn messages_that_arrive_together_are_batched_into_one_turn() {
    let harness = Harness::start_coalescing().await;
    for index in 0..5 {
        harness
            .sender
            .send(message(&format!("part {index}"), &index.to_string()))
            .await
            .expect("queued");
    }

    harness
        .wait_for("a turn", |harness| !harness.turns().is_empty())
        .await;
    tokio::time::sleep(Duration::from_millis(400)).await;

    let turns = harness.turns();
    let total: usize = turns
        .iter()
        .map(|envelope| envelope.matches("--- message").count())
        .sum();
    assert_eq!(total, 5, "every message must be delivered exactly once");
    assert_eq!(
        turns.len(),
        1,
        "five messages sent together belong in one turn, got {} turns",
        turns.len()
    );
}

/// Alice has started composing in `mock:1`, as the connector reports it. The id matches the sender
/// of `message`, since a conversation is only held for whoever's message is already waiting.
fn typing() -> InboundEvent {
    InboundEvent::Typing {
        conversation: ConversationId::parse("mock:1").expect("valid"),
        author: "1".to_string(),
        timestamp: Utc::now(),
    }
}

#[tokio::test]
async fn a_chat_somebody_is_still_typing_in_waits_for_them() {
    // The point of the whole capability. The quiet period is 150ms here and the message would
    // otherwise be claimed the moment it expires; a typing notice has to hold it open past that,
    // which is what makes a second sentence land in the same turn as the first.
    let harness = Harness::start(1, 0).await;
    harness.channel.report_typing();
    harness
        .sender
        .send(message("hey", "1"))
        .await
        .expect("queued");
    harness.sender.send(typing()).await.expect("queued");

    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        harness.turns().is_empty(),
        "somebody is still typing, so nothing should have been claimed: {:?}",
        harness.turns()
    );

    harness
        .sender
        .send(message("can you check the deploy logs", "2"))
        .await
        .expect("queued");
    harness
        .wait_for("the turn", |harness| !harness.turns().is_empty())
        .await;
    let turns = harness.turns();
    assert!(
        turns[0].contains("hey") && turns[0].contains("deploy logs"),
        "both messages belong in one turn:\n{}",
        turns[0]
    );
}

#[tokio::test]
async fn sending_the_message_ends_the_wait_for_it() {
    // Neither platform reports that somebody stopped typing: the client just hides the indicator
    // when the message lands. So the message itself has to end the hold. Without that every
    // Discord message would be held until its author's last notice aged out, which is the whole
    // of the wait rather than a bound on it, and the feature would make the bridge slower than
    // having no debounce at all.
    let harness = Harness::start(1, 0).await;
    harness.channel.report_typing();
    harness.sender.send(typing()).await.expect("queued");
    tokio::time::sleep(Duration::from_millis(50)).await;

    let sent = tokio::time::Instant::now();
    harness
        .sender
        .send(message("one finished thought", "1"))
        .await
        .expect("queued");
    harness
        .wait_for("the turn", |harness| !harness.turns().is_empty())
        .await;
    // settle_max is 600ms here, so anything at or past that is the ceiling rescuing a stuck hold
    // rather than the rule working.
    assert!(
        sent.elapsed() < Duration::from_millis(500),
        "a finished message must not wait out the typing it produced, waited {:?}",
        sent.elapsed()
    );
}

#[tokio::test]
async fn a_chat_held_for_typing_does_not_hold_up_another() {
    // A typing hold makes the drain loop's own wait long, and a wait nothing can interrupt is a
    // wait every other conversation shares. That is the fault splitting readiness per conversation
    // was meant to end, and a long hold is exactly where it would come back.
    // Not the patient harness: the channel reports typing for every chat on it, so a long quiet
    // period would be one the second chat legitimately waits out too, and the test would be
    // measuring that rather than the hold on the first.
    let harness = Harness::start(1, 0).await;
    harness.channel.report_typing();
    harness
        .sender
        .send(message("hold this one", "held-1"))
        .await
        .expect("queued");
    harness.sender.send(typing()).await.expect("queued");
    tokio::time::sleep(Duration::from_millis(150)).await;

    let sent = tokio::time::Instant::now();
    let mut elsewhere = message("are you there?", "other-1");
    if let InboundEvent::Message(inner) = &mut elsewhere {
        inner.conversation = ConversationId::parse("mock:-100").expect("valid");
    }
    harness.sender.send(elsewhere).await.expect("queued");
    harness
        .wait_for("the other chat's turn", |harness| {
            harness
                .turns()
                .iter()
                .any(|turn| turn.contains("are you there?"))
        })
        .await;
    // Tight on purpose. With the wake branch this is the 150ms quiet period plus scheduling;
    // without it the loop sleeps however long the held chat asked for, which the harness ceiling
    // caps at roughly 450ms here and which in production is the full typing TTL.
    assert!(
        sent.elapsed() < Duration::from_millis(350),
        "an unrelated chat must not wait behind a typing hold, waited {:?}",
        sent.elapsed()
    );
}

#[tokio::test]
async fn typing_does_not_hold_a_chat_past_the_ceiling() {
    // Somebody who opens a compose box and walks away, or a client that stops sending the notice
    // without sending anything. Without the ceiling a conversation could be held indefinitely by a
    // signal that is only ever a heartbeat.
    let harness = Harness::start(1, 0).await;
    harness.channel.report_typing();
    harness
        .sender
        .send(message("hey", "1"))
        .await
        .expect("queued");
    for _ in 0..8 {
        harness.sender.send(typing()).await.expect("queued");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // settle_max is 600ms in the harness, so the ceiling is well past by now.
    let turns = harness.turns();
    assert!(
        !turns.is_empty(),
        "the ceiling has to win over a notice nobody stops sending"
    );
}

#[tokio::test]
async fn a_platform_that_cannot_report_typing_does_not_hold_a_message() {
    // The whole point of gating the quiet period on the capability. Telegram has no typing update
    // of any kind, so any wait there is a guess, and a guess long enough to catch somebody typing a
    // second sentence is far too long to impose on somebody who only ever meant to send one.
    //
    // A patient harness, so the two numbers are far apart: the quiet period is two seconds and the
    // floor a tenth of one, and anything under a second is unambiguously the floor. At the suite's
    // usual 150ms and 100ms the gap is smaller than the scheduling noise on a loaded machine.
    let harness = Harness::start_patient().await;
    let sent = tokio::time::Instant::now();
    harness
        .sender
        .send(message("one complete question", "1"))
        .await
        .expect("queued");
    harness
        .wait_for("the turn", |harness| !harness.turns().is_empty())
        .await;
    assert!(
        sent.elapsed() < Duration::from_secs(1),
        "a chat nobody can be seen typing in waited {:?}, which is the quiet period",
        sent.elapsed()
    );
}

#[tokio::test]
async fn the_parts_of_one_split_post_arrive_together() {
    // The floor's reason for existing, and nothing to do with typing. Telegram sends a multi-photo
    // album as one update per photo and the bridge has no album assembly of its own, so without a
    // floor the agent gets a photo and then a separate turn carrying the rest. Milliseconds apart,
    // which is why the floor is sized against the wire rather than against people.
    let harness = Harness::start_coalescing().await;
    for index in 0..3 {
        let mut event = message("", &index.to_string());
        if let InboundEvent::Message(inner) = &mut event {
            inner.group_id = Some("album-1".to_string());
        }
        harness.sender.send(event).await.expect("queued");
    }

    harness
        .wait_for("the turn", |harness| !harness.turns().is_empty())
        .await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let turns = harness.turns();
    assert_eq!(turns.len(), 1, "one post is one turn, got {turns:#?}");
    assert_eq!(
        turns[0].matches("album: album-1").count(),
        3,
        "every part of the post has to be in it:\n{}",
        turns[0]
    );
}

#[tokio::test]
async fn one_chat_mid_burst_does_not_hold_up_another() {
    // Readiness used to be one window over the whole queue, so any conversation still receiving
    // deferred delivery for every other conversation. With a quiet period long enough to be useful
    // that would mean a busy room stalling a direct message.
    let harness = Harness::start(1, 0).await;
    harness.channel.report_typing();

    let busy = tokio::spawn({
        let sender = harness.sender.clone();
        async move {
            for index in 0..40 {
                let mut event = message(&format!("chatter {index}"), &format!("busy-{index}"));
                if let InboundEvent::Message(inner) = &mut event {
                    inner.conversation = ConversationId::parse("mock:-100").expect("valid");
                }
                if sender.send(event).await.is_err() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(40)).await;
            }
        }
    });

    // Long enough that the busy conversation is unmistakably mid-burst.
    tokio::time::sleep(Duration::from_millis(120)).await;
    let sent = tokio::time::Instant::now();
    harness
        .sender
        .send(message("are you there?", "quiet-1"))
        .await
        .expect("queued");
    harness
        .wait_for("the quiet chat's turn", |harness| {
            harness
                .turns()
                .iter()
                .any(|turn| turn.contains("are you there?"))
        })
        .await;
    busy.abort();

    assert!(
        sent.elapsed() < Duration::from_millis(400),
        "a quiet chat must not wait on a busy one, waited {:?}",
        sent.elapsed()
    );
}

#[tokio::test]
async fn a_burst_typed_over_several_seconds_still_becomes_one_turn() {
    // The case the quiet period exists for: somebody types a thought across three messages, and
    // without it the first starts a turn on its own and the agent answers "hey" before it has read
    // the question. Only available where the platform reports typing, because only there does the
    // wait end when the person stops rather than when a guessed timer does.
    let harness = Harness::start(1, 0).await;
    harness.channel.report_typing();
    for (index, text) in [
        "hey",
        "can you check the deploy logs",
        "actually the staging ones",
    ]
    .iter()
    .enumerate()
    {
        harness
            .sender
            .send(message(text, &index.to_string()))
            .await
            .expect("queued");
        // Comfortably inside the harness's 150ms settle, the way a person's messages are inside
        // the shipped window.
        tokio::time::sleep(Duration::from_millis(60)).await;
    }

    harness
        .wait_for("a turn", |harness| !harness.turns().is_empty())
        .await;
    tokio::time::sleep(Duration::from_millis(400)).await;

    let turns = harness.turns();
    assert_eq!(turns.len(), 1, "got {} turns: {:#?}", turns.len(), turns);
    assert!(
        turns[0].contains("hey") && turns[0].contains("actually the staging ones"),
        "the whole thought must arrive together, got:\n{}",
        turns[0]
    );
}

#[tokio::test]
async fn a_chat_that_never_goes_quiet_is_released_by_the_ceiling() {
    // A steady stream keeps resetting the quiet period, so without a ceiling the batch would be
    // deferred indefinitely and the agent would never hear anything at all.
    let harness = Harness::start(1, 0).await;
    // The ceiling only binds where something is holding the conversation in the first place, which
    // on a platform that cannot report typing is nothing at all.
    harness.channel.report_typing();
    let sender = harness.sender.clone();
    let chatter = tokio::spawn(async move {
        for index in 0..40 {
            if sender
                .send(message(&format!("chatter {index}"), &index.to_string()))
                .await
                .is_err()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });

    // settle_max is 600ms in the harness, so a turn has to happen well before the stream ends.
    harness
        .wait_for("a turn despite the stream", |harness| {
            !harness.turns().is_empty()
        })
        .await;
    chatter.abort();

    let turns = harness.turns();
    assert!(
        turns[0].matches("--- message").count() > 1,
        "the ceiling should still release a batch rather than one message, got:\n{}",
        turns[0]
    );
}

#[tokio::test]
async fn queued_messages_survive_a_restart() {
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("state.db");

    // First run: queue a message and abandon it mid-flight, as a crash would.
    {
        let store = Store::open(&database).await.expect("opens");
        store
            .upsert_conversation(ConversationRecord {
                id: "mock:1".to_string(),
                channel_id: "mock".to_string(),
                platform: "telegram".to_string(),
                chat: "1".to_string(),
                thread: None,
                title: Some("Alice".to_string()),
                kind: "direct".to_string(),
                created_at: Utc::now(),
                last_inbound_at: Some(Utc::now()),
                last_outbound_at: None,
            })
            .await
            .expect("conversation");
        let payload = serde_json::to_string(&message("do not lose me", "1")).expect("encodes");
        store
            .enqueue("mock:1", "1", &payload, Utc::now(), 64)
            .await
            .expect("enqueued");
        store
            .claim_batch(&["mock:1".to_string()], 10)
            .await
            .expect("claimed");
        assert_eq!(store.pending_count().await.expect("count"), 0);
    }

    // Second run: startup recovery returns the stranded row to the queue and it gets delivered.
    let recorder = Arc::new(MekaRecorder::default());
    let (meka_address, meka_shutdown) = start_meka(Arc::clone(&recorder)).await;
    let config = Arc::new(config_for(meka_address, &database, 1, false, "20s"));
    let store = Store::open(&config.storage.path).await.expect("opens");
    let recovered = store.reset_in_flight().await.expect("recovers");
    assert_eq!(recovered, 1);

    let channel = Arc::new(MockChannel::new("mock"));
    let channels = Arc::new(ChannelRegistry::from_channels(
        [channel as Arc<dyn Channel>],
    ));
    let meka = MekaClient::new(&config.meka).expect("client");
    let shutdown = CancellationToken::new();
    let wake = Arc::new(Notify::new());
    tokio::spawn({
        let context = DrainContext {
            store: store.clone(),
            config: Arc::clone(&config),
            meka: meka.clone(),
            channels: Arc::clone(&channels),
            typing: Arc::new(inbound::TypingState::default()),
            runner: TurnRunner::new(
                meka,
                channels,
                false,
                Duration::from_secs(30),
                Arc::new(Presence::default()),
            ),
            identities: Arc::new(tokio::sync::OnceCell::new()),
            permission_checked: Arc::new(tokio::sync::OnceCell::new()),
            notices: inbound::NoticeLog::default(),
        };
        let shutdown = shutdown.clone();
        let wake = Arc::clone(&wake);
        async move { inbound::drain_loop(context, wake, shutdown).await }
    });
    wake.notify_one();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let turns = recorder
            .turns
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        if let Some(envelope) = turns.first() {
            assert!(envelope.contains("do not lose me"), "got:\n{envelope}");
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the recovered message was never delivered"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    shutdown.cancel();
    meka_shutdown.cancel();
}

#[tokio::test]
async fn the_sink_delivers_to_the_channel_and_records_the_send() {
    let directory = tempfile::tempdir().expect("tempdir");
    let store = Store::open(&directory.path().join("state.db"))
        .await
        .expect("opens");
    store
        .upsert_conversation(ConversationRecord {
            id: "mock:1".to_string(),
            channel_id: "mock".to_string(),
            platform: "telegram".to_string(),
            chat: "1".to_string(),
            thread: None,
            title: Some("Alice".to_string()),
            kind: "direct".to_string(),
            created_at: Utc::now(),
            last_inbound_at: Some(Utc::now()),
            last_outbound_at: None,
        })
        .await
        .expect("conversation");

    let channel = Arc::new(MockChannel::new("mock"));
    let channels = Arc::new(ChannelRegistry::from_channels([
        Arc::clone(&channel) as Arc<dyn Channel>
    ]));
    let sink = sink_for(store.clone(), channels);

    let ids = sink
        .send_text("mock:1", "**hello**", SendOptions::default())
        .await
        .expect("sends");
    assert_eq!(ids, vec!["m1".to_string()]);
    assert_eq!(channel.sent(), vec![(
        "mock:1".to_string(),
        "**hello**".to_string()
    )]);

    let record = store
        .conversation("mock:1")
        .await
        .expect("read")
        .expect("present");
    assert!(
        record.last_outbound_at.is_some(),
        "an outbound message must update the conversation's activity"
    );
}

/// A message that names the agent, which is what wakes a muted conversation.
fn mention(text: &str, external_id: &str) -> InboundEvent {
    let mut event = message(text, external_id);
    let InboundEvent::Message(inner) = &mut event else {
        panic!("a message was built just above");
    };
    inner.addressed = true;
    event
}

/// The same message, in a second direct chat.
fn message_elsewhere(text: &str, external_id: &str) -> InboundEvent {
    let mut event = message(text, external_id);
    let InboundEvent::Message(inner) = &mut event else {
        panic!("a message was built just above");
    };
    inner.conversation = ConversationId::parse("mock:2").expect("valid");
    inner.sender.id = "2".to_string();
    inner.sender.display_name = "Bob".to_string();
    inner.sender.username = Some("bob".to_string());
    event
}

/// A message in a group nobody has ruled on, which is the shape an upgrade inherits.
fn group_message(text: &str, external_id: &str, addressed: bool) -> InboundEvent {
    let mut event = message(text, external_id);
    let InboundEvent::Message(inner) = &mut event else {
        panic!("a message was built just above");
    };
    inner.conversation = ConversationId::parse("mock:-100").expect("valid");
    inner.chat_kind = ChatKind::Group;
    inner.chat_title = Some("Ops".to_string());
    inner.addressed = addressed;
    event
}

#[tokio::test]
async fn a_group_nobody_has_ruled_on_follows_the_configured_default() {
    // The behaviour every pre-existing group inherits on upgrade. There is no row for it in
    // `conversation_policy` and nothing backfills one, so it falls to
    // `[bridge.default_policy].group` the first time somebody speaks. Getting this wrong would
    // either keep costing a turn per message, which is the problem this release exists to fix,
    // or silence a group that was never meant to be quiet.
    let harness = Harness::start(1, 0).await;
    assert!(
        harness
            .store
            .policy("mock:-100")
            .await
            .expect("read")
            .is_none(),
        "the premise: nothing has ruled on this group"
    );

    for index in 0..3 {
        harness
            .sender
            .send(group_message(
                &format!("chatter {index}"),
                &index.to_string(),
                false,
            ))
            .await
            .expect("queued");
    }
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(
        harness.turns().is_empty(),
        "a group defaults to mentions only, so ordinary talk must not cost a turn: {:?}",
        harness.turns()
    );
    assert_eq!(
        harness
            .store
            .history("mock:-100", 10, None)
            .await
            .expect("read")
            .len(),
        3,
        "withheld, not discarded: the default is mute rather than block"
    );

    harness
        .sender
        .send(group_message("@bot are you there?", "9", true))
        .await
        .expect("queued");
    harness
        .wait_for("the turn a mention woke", |harness| {
            !harness.turns().is_empty()
        })
        .await;
    let envelope = harness.turns()[0].clone();
    assert!(envelope.contains("are you there?"), "got:\n{envelope}");
    assert!(
        envelope.contains("3 messages you have not seen"),
        "got:\n{envelope}"
    );
}

#[tokio::test]
async fn a_direct_chat_nobody_has_ruled_on_still_wakes_the_agent_for_everything() {
    // The other half of the default. A one-to-one chat has nobody else in it, so applying the group
    // default there would silence the agent against the only person talking to it.
    let harness = Harness::start(1, 0).await;
    harness
        .sender
        .send(message("just checking in", "1"))
        .await
        .expect("queued");
    harness
        .wait_for("the turn", |harness| !harness.turns().is_empty())
        .await;
    assert!(harness.turns()[0].contains("just checking in"));
}

#[tokio::test]
async fn a_blocked_conversation_never_reaches_the_agent_and_keeps_nothing() {
    let harness = Harness::start(1, 0).await;
    harness
        .store
        .set_policy("mock:1", Policy::Block, None, Some("too noisy"), Utc::now())
        .await
        .expect("block");

    for index in 0..5 {
        harness
            .sender
            .send(message("noise", &index.to_string()))
            .await
            .expect("queued");
    }
    // Nothing to wait for, so give the writer and drain loop a generous window to misbehave in.
    tokio::time::sleep(Duration::from_millis(400)).await;

    assert!(
        harness.turns().is_empty(),
        "a blocked chat must not wake the agent: {:?}",
        harness.turns()
    );
    let stats = harness.store.queue_stats().await.expect("stats");
    assert_eq!(
        stats.pending, 0,
        "blocked messages must not consume queue depth either"
    );
    assert!(
        harness
            .store
            .history("mock:1", 10, None)
            .await
            .expect("read")
            .is_empty(),
        "block keeps nothing, which is the whole difference from mute"
    );
    let policies = harness.store.list_policies().await.expect("list");
    assert_eq!(policies[0].dropped, 5);
}

#[tokio::test]
async fn a_chat_waiting_on_someone_elses_turn_is_told_the_agent_is_busy() {
    // meka only refuses because it is running a turn, and a backgrounded tool call delivers its
    // outcome as one. The agent is working; the chat should not look dead while it does.
    //
    // `turn_in_flight` is deliberately left false throughout, which is what the real meka reports
    // while it refuses: the field tracks a counter only `POST /turn` bumps, and the refusal comes
    // from a mutex that background-outcome turns hold without touching it.
    let harness = Harness::start_with_typing().await;
    *harness
        .recorder
        .busy_first
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = 1;

    harness
        .sender
        .send(message("are you there?", "1"))
        .await
        .expect("queued");

    harness
        .wait_for("the indicator to go up while meka is busy", |harness| {
            harness.channel.activity_count() > 0
        })
        .await;

    harness
        .wait_for("the batch to land once meka frees up", |harness| {
            harness
                .turns()
                .iter()
                .any(|turn| turn.contains("are you there?"))
        })
        .await;
}

#[tokio::test]
async fn a_session_that_refuses_while_reporting_itself_idle_is_retried_on_a_timer() {
    // The field failure this pins. meka answers `turn_in_flight` from an atomic that only
    // `POST /turn` increments, while the 409 comes from a session mutex that scheduled jobs and
    // background-task outcomes hold as well. For the length of one of those the session reports
    // idle and refuses anyway, so waiting for it to say it is free returns instantly and the
    // resubmission is refused just as fast: ~125 submissions a second for as long as the other turn
    // ran. Each one opened a typing indicator, and Discord's rate limiter went on replaying the
    // backlog for minutes after everything had gone quiet, which is what the chat actually showed.
    let harness = Harness::start_with_typing().await;
    // Effectively forever: this session never admits to being busy and never accepts a turn.
    *harness
        .recorder
        .busy_first
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = 100_000;

    harness
        .sender
        .send(message("are you there?", "1"))
        .await
        .expect("queued");
    harness
        .wait_for("the first submission", |harness| harness.attempts() > 0)
        .await;

    tokio::time::sleep(Duration::from_secs(3)).await;

    let submissions = harness.attempts();
    assert!(
        submissions <= 4,
        "{submissions} submissions in three seconds of refusals: the bridge is spinning on the \
         409 rather than waiting the other turn out"
    );
    let activities = harness.channel.activity_count();
    assert!(
        activities <= 4,
        "{activities} typing actions in three seconds: a refused submission is queueing indicators \
         faster than the platform will drain them"
    );
    assert!(
        activities > 0,
        "the chat was left silent while meka was busy with a turn of its own"
    );
}

#[tokio::test]
async fn a_busy_session_defers_a_batch_instead_of_giving_up_on_it() {
    // meka runs background tasks and scheduled wakes of its own, so it can refuse a submission with
    // a 409 for reasons that have nothing to do with this batch. Counting those against the retry
    // budget declared a message undeliverable that meka had never seen, which is what happened in
    // the field. Four refusals is more than `turn_retries + 1`, so under the old accounting this
    // message was dead before the fifth submission ever happened.
    let harness = Harness::start(1, 0).await;
    *harness
        .recorder
        .busy_first
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = 4;

    harness
        .sender
        .send(message("are you there?", "1"))
        .await
        .expect("queued");

    harness
        .wait_for(
            "the batch to survive the busy session and land",
            |harness| {
                harness
                    .turns()
                    .iter()
                    .any(|turn| turn.contains("are you there?"))
            },
        )
        .await;

    // Delivered, not failed, and the owner was never told it was undeliverable.
    let stats = harness.store.queue_stats().await.expect("stats");
    assert_eq!(stats.failed, 0, "a busy session is not a delivery failure");
    assert_eq!(stats.pending, 0, "the batch finished");
}

#[tokio::test]
async fn a_retracted_message_leaves_the_bridges_history_too() {
    // A platform that reports deletions is the only one that can do this, and the point is that the
    // agent can never be handed back something its author took down.
    let harness = Harness::start(1, 0).await;
    harness
        .sender
        .send(message("said too much", "77"))
        .await
        .expect("queued");
    await_history(&harness, 1, "the message to be recorded").await;

    harness
        .sender
        .send(InboundEvent::Retraction {
            conversation: ConversationId::parse("mock:1").expect("valid"),
            message_id: "77".to_string(),
            timestamp: Utc::now(),
        })
        .await
        .expect("queued");
    await_history(&harness, 0, "the recorded copy to go").await;
}

/// Poll until the recorded history for `mock:1` is exactly `expected` messages long.
///
/// The harness's own `wait_for` takes a synchronous predicate, and reading the store is not.
async fn await_history(harness: &Harness, expected: usize, label: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        let count = harness
            .store
            .history("mock:1", 10, None)
            .await
            .expect("read")
            .len();
        if count == expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("timed out waiting for {label}");
}

#[tokio::test]
async fn a_muted_conversation_records_everything_and_wakes_only_on_a_mention() {
    let harness = Harness::start(1, 0).await;
    harness
        .store
        .set_policy("mock:1", Policy::Mute, None, None, Utc::now())
        .await
        .expect("mute");

    for index in 0..3 {
        harness
            .sender
            .send(message(&format!("chatter {index}"), &index.to_string()))
            .await
            .expect("queued");
    }
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(
        harness.turns().is_empty(),
        "ordinary talk in a muted chat must not cost a turn: {:?}",
        harness.turns()
    );
    assert_eq!(
        harness
            .store
            .history("mock:1", 10, None)
            .await
            .expect("read")
            .len(),
        3,
        "withheld is not discarded: the agent has to be able to read it later"
    );

    harness
        .sender
        .send(mention("@bot what do you think about that?", "9"))
        .await
        .expect("queued");
    harness
        .wait_for("the turn a mention woke", |harness| {
            !harness.turns().is_empty()
        })
        .await;

    let turns = harness.turns();
    let envelope = turns.first().expect("one turn");
    assert_eq!(turns.len(), 1, "only the mention should have woken it");
    assert!(envelope.contains("what do you think"), "got:\n{envelope}");
    assert!(
        envelope.contains("3 messages you have not seen"),
        "the count of what was withheld has to be stated:\n{envelope}"
    );
    assert!(
        envelope.contains("chatter 2"),
        "the lookback is what makes a bare mention answerable:\n{envelope}"
    );
    assert!(
        envelope.contains("read_history"),
        "the agent has to be told how to reach the rest:\n{envelope}"
    );
}

#[tokio::test]
async fn a_listening_window_closing_actually_reaches_the_agent() {
    // The notice is written onto the message that discovers the expiry, and that message is
    // usually one nothing addressed, in a room that has just gone back to mentions only. Filed
    // rather than delivered, it lands in the history where nothing will read it, and the agent
    // goes on believing it can hear a room it cannot. That is the exact confusion the notice
    // exists to prevent, so discovering an expiry has to be able to wake the agent by itself.
    let harness = Harness::start(1, 0).await;
    // What `unmute(duration)` leaves behind once the window has closed.
    harness
        .store
        .set_policy(
            "mock:-100",
            Policy::Active,
            Some(Utc::now() - chrono::Duration::minutes(1)),
            Some("design discussion"),
            Utc::now() - chrono::Duration::minutes(21),
        )
        .await
        .expect("policy");

    harness
        .sender
        .send(group_message("unrelated chatter", "1", false))
        .await
        .expect("queued");
    harness
        .wait_for("the turn carrying the expiry notice", |harness| {
            !harness.turns().is_empty()
        })
        .await;
    let envelope = &harness.turns()[0];
    assert!(
        envelope.contains("hearing this chat in full") && envelope.contains("mentions only"),
        "the agent has to be told its window closed:\n{envelope}"
    );

    // And the room really is back on mentions only afterwards, rather than having been reopened by
    // the delivery.
    harness
        .sender
        .send(group_message("more chatter", "2", false))
        .await
        .expect("queued");
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(
        harness.turns().len(),
        1,
        "only the message that found the expiry is owed a turn: {:?}",
        harness.turns()
    );
}

#[tokio::test]
async fn a_muted_conversation_stays_quiet_even_moments_after_the_agent_speaks() {
    // There was a window here: for five minutes after the agent's own message, everything said in
    // the room woke it. In a busy chat that delivered the room, in envelopes indistinguishable
    // from a message addressed to the agent, and each reply it was nudged into making pushed the
    // window out again. Mention-only now means exactly that. Following a conversation on is
    // something the agent asks for rather than something it is handed.
    let harness = Harness::start(1, 0).await;
    harness
        .store
        .set_policy("mock:1", Policy::Mute, None, None, Utc::now())
        .await
        .expect("mute");

    // What `note_sent` does after the agent's own message lands, so this is the most favourable
    // possible moment for the old window: it opened a heartbeat ago.
    harness
        .store
        .touch_outbound(ConversationRecord {
            id: "mock:1".to_string(),
            channel_id: "mock".to_string(),
            platform: "telegram".to_string(),
            chat: "1".to_string(),
            thread: None,
            title: None,
            kind: ChatKind::Unknown.as_str().to_string(),
            created_at: Utc::now(),
            last_inbound_at: None,
            last_outbound_at: Some(Utc::now()),
        })
        .await
        .expect("outbound");

    harness
        .sender
        .send(message("no, I meant the other one", "1"))
        .await
        .expect("queued");
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(
        harness.turns().is_empty(),
        "speaking in a muted chat must not open it up again: {:?}",
        harness.turns()
    );

    // Withheld, not discarded. The distinction is the whole reason this is tolerable: the agent
    // was not interrupted, and the message is still there for it to find.
    let (owed, _) = harness
        .store
        .take_unseen("mock:1", Utc::now(), 10)
        .await
        .expect("read");
    assert_eq!(owed, 1, "the message has to survive for read_history");
}

#[tokio::test]
async fn a_refused_submission_does_not_spend_the_backlog_it_reported() {
    // The envelope for a refused turn is thrown away, and the backlog it reported has to survive
    // with it. Marking at read time meant the retry counted zero and, because the conversation is
    // muted, told the agent nothing had been said in a chat with four messages waiting.
    // Refused enough times that `submit` exhausts its budget and `deliver` releases the batch,
    // which is the only path that rebuilds the envelope and so the only one where spending the
    // backlog early is visible.
    // Three: `submit` refuses at t=0, t=2 and t=4 against a 3s budget, so the third gives up and
    // the batch is released. Two would be answered on the retry that follows the second sleep,
    // reusing the envelope built the first time and hiding the bug entirely.
    let harness = Harness::start_impatient(3).await;
    harness
        .store
        .set_policy("mock:1", Policy::Mute, None, None, Utc::now())
        .await
        .expect("mute");
    for index in 0..4 {
        harness
            .sender
            .send(message(&format!("while muted {index}"), &index.to_string()))
            .await
            .expect("queued");
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(harness.turns().is_empty(), "a mute withholds all four");

    let mut mention = message("@bot what do you make of that?", "9");
    if let InboundEvent::Message(inner) = &mut mention {
        inner.addressed = true;
    }
    harness.sender.send(mention).await.expect("queued");

    harness
        .wait_for("the batch to land on the retry", |harness| {
            !harness.turns().is_empty()
        })
        .await;

    let turns = harness.turns();
    let envelope = turns.first().expect("one turn");
    assert!(
        envelope.contains("while muted 3"),
        "the backlog was spent by the refused attempt:\n{envelope}"
    );
    assert!(
        !envelope.contains("Nothing else has been said"),
        "the retry told the agent a chat with four waiting messages was silent:\n{envelope}"
    );
}

#[tokio::test]
async fn unmuting_reports_the_backlog_once_and_then_stops() {
    // The trap: `unseen` is only ever cleared by the turn that reports it, so a conversation that
    // stops being muted with a backlog behind it would keep that count for the rest of its life and
    // `list_conversations` would go on quoting it.
    let harness = Harness::start(1, 0).await;
    harness
        .store
        .set_policy("mock:1", Policy::Mute, None, None, Utc::now())
        .await
        .expect("mute");
    for index in 0..4 {
        harness
            .sender
            .send(message(&format!("while muted {index}"), &index.to_string()))
            .await
            .expect("queued");
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(harness.turns().is_empty());

    harness
        .store
        .set_policy("mock:1", Policy::Active, None, None, Utc::now())
        .await
        .expect("unmute");
    harness
        .sender
        .send(message("now listening again", "9"))
        .await
        .expect("queued");
    harness
        .wait_for("the first turn after unmuting", |harness| {
            !harness.turns().is_empty()
        })
        .await;

    let envelope = harness.turns()[0].clone();
    assert!(
        envelope.contains("4 messages in mock:1 were recorded"),
        "the backlog has to be reported once:\n{envelope}"
    );
    assert!(
        !envelope.contains("only woken for mentions"),
        "it is not muted any more:\n{envelope}"
    );

    harness
        .sender
        .send(message("and again", "10"))
        .await
        .expect("queued");
    harness
        .wait_for("the second turn", |harness| harness.turns().len() > 1)
        .await;
    assert!(
        !harness.turns()[1].contains("were recorded"),
        "the backlog must be cleared by the turn that reported it: {:?}",
        harness.turns()[1]
    );
}

#[tokio::test]
async fn an_expired_block_lets_the_next_message_through_and_reports_the_damage() {
    let harness = Harness::start(1, 0).await;
    harness
        .store
        .set_policy("mock:1", Policy::Block, None, None, Utc::now())
        .await
        .expect("block");
    harness
        .sender
        .send(message("while blocked", "1"))
        .await
        .expect("queued");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(harness.turns().is_empty());

    // Backdate the expiry rather than sleeping through a real one. Re-ruling resets the tally, so
    // the drop is counted afterwards.
    harness
        .store
        .set_policy(
            "mock:1",
            Policy::Block,
            Some(Utc::now() - chrono::Duration::seconds(1)),
            None,
            Utc::now(),
        )
        .await
        .expect("block");
    harness
        .store
        .note_blocked_drop("mock:1")
        .await
        .expect("drop count");

    harness
        .sender
        .send(message("after the block", "2"))
        .await
        .expect("queued");
    harness
        .wait_for("the turn after the block expired", |harness| {
            !harness.turns().is_empty()
        })
        .await;

    let turns = harness.turns();
    let envelope = turns.first().expect("one turn");
    assert!(envelope.contains("after the block"), "got:\n{envelope}");
    assert!(
        envelope.contains("1 message was discarded while it was blocked"),
        "the agent has to be told the block did something, or it cannot judge whether to renew \
         it:\n{envelope}"
    );
    assert!(
        harness
            .store
            .policy("mock:1")
            .await
            .expect("read")
            .is_none(),
        "a lapsed policy must be cleared once it has been reported"
    );
}

#[tokio::test]
async fn the_sink_delivers_to_a_conversation_it_has_never_seen() {
    // Messaging first is the point: an id from the agent's system prompt is as valid as one from an
    // envelope, and whether the chat is writable is the platform's judgement rather than ours.
    let directory = tempfile::tempdir().expect("tempdir");
    let store = Store::open(&directory.path().join("state.db"))
        .await
        .expect("opens");
    let channel = Arc::new(MockChannel::new("mock"));
    let channels = Arc::new(ChannelRegistry::from_channels([
        Arc::clone(&channel) as Arc<dyn Channel>
    ]));
    let sink = sink_for(store.clone(), channels);

    sink.send_text("mock:999", "hello", SendOptions::default())
        .await
        .expect("an unseen id is deliverable");
    assert_eq!(channel.sent(), vec![(
        "mock:999".to_string(),
        "hello".to_string()
    )]);

    // And it joins the address book, or the agent could message somebody and then fail to find them
    // in `list_conversations` afterwards.
    let record = store
        .conversation("mock:999")
        .await
        .expect("read")
        .expect("the send registers the conversation");
    assert_eq!(record.chat, "999");
    assert_eq!(
        record.kind, "unknown",
        "nothing about the chat's shape is known from a send alone"
    );
    assert_eq!(record.title, None);
    assert!(record.last_outbound_at.is_some());
    assert!(record.last_inbound_at.is_none());
}

#[tokio::test]
async fn an_inbound_message_fills_in_what_a_send_could_not_know() {
    let directory = tempfile::tempdir().expect("tempdir");
    let store = Store::open(&directory.path().join("state.db"))
        .await
        .expect("opens");
    let channels = Arc::new(ChannelRegistry::from_channels([
        Arc::new(MockChannel::new("mock")) as Arc<dyn Channel>,
    ]));
    let sink = sink_for(store.clone(), channels);

    sink.send_text("mock:7", "first contact", SendOptions::default())
        .await
        .expect("sends");
    store
        .upsert_conversation(ConversationRecord {
            id: "mock:7".to_string(),
            channel_id: "mock".to_string(),
            platform: "telegram".to_string(),
            chat: "7".to_string(),
            thread: None,
            title: Some("Deploy Crew".to_string()),
            kind: "group".to_string(),
            created_at: Utc::now(),
            last_inbound_at: Some(Utc::now()),
            last_outbound_at: None,
        })
        .await
        .expect("inbound");

    // A second send must not drag the now-known title and kind back to their placeholders.
    sink.send_text("mock:7", "second", SendOptions::default())
        .await
        .expect("sends");
    let record = store
        .conversation("mock:7")
        .await
        .expect("read")
        .expect("present");
    assert_eq!(record.title.as_deref(), Some("Deploy Crew"));
    assert_eq!(record.kind, "group");
}

#[tokio::test]
async fn the_sink_still_refuses_ids_it_cannot_route() {
    let directory = tempfile::tempdir().expect("tempdir");
    let store = Store::open(&directory.path().join("state.db"))
        .await
        .expect("opens");
    let channel = Arc::new(MockChannel::new("mock"));
    let channels = Arc::new(ChannelRegistry::from_channels([
        Arc::clone(&channel) as Arc<dyn Channel>
    ]));
    let sink = sink_for(store, channels);

    // Unrestricted means "any chat", not "any string". These two fail here because no channel could
    // act on them at all, which is different from a chat the platform will reject.
    let malformed = sink
        .send_text("not-an-id", "hello", SendOptions::default())
        .await
        .expect_err("a malformed id must be refused");
    assert!(malformed.to_string().contains("not-an-id"), "{malformed}");

    let unconfigured = sink
        .send_text("discord:1", "hello", SendOptions::default())
        .await
        .expect_err("an unconfigured channel must be refused");
    assert!(
        unconfigured.to_string().contains("discord"),
        "{unconfigured}"
    );
    assert!(channel.sent().is_empty());
}

#[tokio::test]
async fn the_sink_lists_conversations_for_the_agent() {
    let directory = tempfile::tempdir().expect("tempdir");
    let store = Store::open(&directory.path().join("state.db"))
        .await
        .expect("opens");
    store
        .upsert_conversation(ConversationRecord {
            id: "mock:1".to_string(),
            channel_id: "mock".to_string(),
            platform: "telegram".to_string(),
            chat: "1".to_string(),
            thread: None,
            title: Some("Alice".to_string()),
            kind: "direct".to_string(),
            created_at: Utc::now(),
            last_inbound_at: Some(Utc::now()),
            last_outbound_at: None,
        })
        .await
        .expect("conversation");
    let channels = Arc::new(ChannelRegistry::from_channels([
        Arc::new(MockChannel::new("mock")) as Arc<dyn Channel>,
    ]));
    let sink = sink_for(store, channels);

    let listed = sink.conversations(None, 10).await.expect("lists");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, "mock:1");
    assert_eq!(listed[0].title.as_deref(), Some("Alice"));
}

#[tokio::test]
async fn a_forgotten_session_is_replaced_and_the_batch_is_replayed() {
    // meka losing the session (its row deleted, or its database replaced) must not lose the
    // message: a replacement session is bound and the same batch is submitted into it.
    let harness = Harness::start_with(1, 0, 1).await;
    harness
        .sender
        .send(message("still here?", "1"))
        .await
        .expect("queued");

    harness
        .wait_for("the replay", |harness| !harness.turns().is_empty())
        .await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let turns = harness.turns();
    // Only the replay lands: the first submission was answered with `session-not-found`, so it
    // never reached an agent and is not a delivered turn.
    assert!(turns[0].contains("still here?"), "got:\n{}", turns[0]);
    let stats = harness.store.queue_stats().await.expect("stats");
    assert_eq!(stats.done, 1, "the message must end up delivered");
    assert_eq!(stats.failed, 0);
}

#[tokio::test]
async fn a_dropped_stream_does_not_resubmit_the_turn() {
    // The turn was accepted and keeps running server-side, so resubmitting would duplicate a reply
    // the user is about to receive. The bridge waits for the session to go idle instead.
    let harness = Harness::start_full(1, 0, 0, 1).await;
    harness
        .sender
        .send(message("are you there?", "1"))
        .await
        .expect("queued");

    harness
        .wait_for("the truncated turn", |harness| !harness.turns().is_empty())
        .await;

    // The bridge is now polling. Report the turn as finished.
    tokio::time::sleep(Duration::from_millis(200)).await;
    *harness
        .recorder
        .turn_in_flight
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = false;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let stats = harness.store.queue_stats().await.expect("stats");
        if stats.done == 1 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the batch was never marked delivered; stats {stats:?}, turns {:?}",
            harness.turns()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert_eq!(
        harness.turns().len(),
        1,
        "the turn must not be resubmitted: {:?}",
        harness.turns()
    );
    let stats = harness.store.queue_stats().await.expect("stats");
    assert_eq!(
        stats.failed, 0,
        "an interrupted turn is not a delivery failure"
    );
    assert_eq!(stats.pending, 0);
}

#[tokio::test]
async fn attachments_are_announced_with_a_handle_and_nothing_is_downloaded() {
    // The core of the deferred model: the envelope tells the agent what arrived and how to reach
    // it, and no bytes move until the agent asks.
    let harness = Harness::start(1, 0).await;
    let directory = tempfile::tempdir().expect("tempdir");

    let mut event = message("what is this?", "1");
    {
        let InboundEvent::Message(inner) = &mut event else {
            panic!("a message was built just above");
        };
        inner.attachments = vec![mekabridge::channel::Attachment {
            kind: mekabridge::channel::AttachmentKind::Photo,
            file_name: Some("photo.jpg".to_string()),
            media_type: Some("image/jpeg".to_string()),
            bytes: Some(4096),
            file_ref: "AgACphoto".to_string(),
            thumb_ref: None,
            handle: None,
        }];
    }
    harness.sender.send(event).await.expect("queued");

    harness
        .wait_for("the turn", |harness| !harness.turns().is_empty())
        .await;

    let turns = harness.turns();
    assert!(
        turns[0].contains("attachment: photo, \"photo.jpg\", image/jpeg, 4.0 KiB ["),
        "the envelope must announce the file and its handle, got:\n{}",
        turns[0]
    );
    assert!(
        std::fs::read_dir(directory.path())
            .expect("readable")
            .next()
            .is_none(),
        "nothing should be written to disk before the agent asks for it"
    );
}

#[tokio::test]
async fn the_agent_can_view_an_attachment_as_an_image() {
    let store = Store::open_in_memory().await.expect("store");
    store
        .upsert_conversation(ConversationRecord {
            id: "mock:1".to_string(),
            channel_id: "mock".to_string(),
            platform: "telegram".to_string(),
            chat: "1".to_string(),
            thread: None,
            title: Some("Alice".to_string()),
            kind: "direct".to_string(),
            created_at: Utc::now(),
            last_inbound_at: Some(Utc::now()),
            last_outbound_at: None,
        })
        .await
        .expect("conversation");

    let channel = Arc::new(MockChannel::new("mock"));
    channel.put_file("AgACphoto", ONE_PIXEL_PNG.to_vec());
    let channels = Arc::new(ChannelRegistry::from_channels([
        Arc::clone(&channel) as Arc<dyn Channel>
    ]));

    let handle = store
        .register_attachment(mekabridge::store::AttachmentRecord {
            id: "mock:1:1:0".to_string(),
            conversation_id: "mock:1".to_string(),
            channel_id: "mock".to_string(),
            kind: "photo".to_string(),
            file_ref: "AgACphoto".to_string(),
            thumb_ref: None,
            file_name: Some("photo.png".to_string()),
            media_type: Some("image/png".to_string()),
            bytes: Some(ONE_PIXEL_PNG.len() as u64),
            path: None,
            created_at: Utc::now(),
        })
        .await
        .expect("registers");

    let directory = tempfile::tempdir().expect("tempdir");
    let sink = sink_with_storage(
        store.clone(),
        channels,
        directory.path().to_path_buf(),
        Arc::new(Presence::default()),
    );

    // meka is unreachable here, so the vision probe fails and the sink degrades to a description
    // rather than pretending the model can see.
    let viewed = sink.view_attachment(&handle).await.expect("resolves");
    assert!(
        matches!(viewed, ViewedAttachment::Description(ref text) if text.contains("no vision")),
        "got: {viewed:?}"
    );

    // Downloading works regardless of vision, and lands inside the configured directory.
    let downloaded = sink.download_attachment(&handle).await.expect("downloads");
    assert!(downloaded.path.starts_with(directory.path()));
    assert_eq!(downloaded.bytes, ONE_PIXEL_PNG.len() as u64);
    assert_eq!(
        std::fs::read(&downloaded.path).expect("readable"),
        ONE_PIXEL_PNG
    );

    // And the download is recorded, so the retention sweep can reclaim it later.
    let expired = store
        .take_expired_attachments(Utc::now() + chrono::Duration::days(1))
        .await
        .expect("sweep");
    assert_eq!(
        expired,
        vec![downloaded.path],
        "a downloaded file must be reclaimable"
    );
}

#[tokio::test]
async fn an_unknown_attachment_handle_is_a_clear_error() {
    let store = Store::open_in_memory().await.expect("store");
    let channel = Arc::new(MockChannel::new("mock"));
    let channels = Arc::new(ChannelRegistry::from_channels([
        Arc::clone(&channel) as Arc<dyn Channel>
    ]));
    let sink = sink_for(store, channels);

    let error = sink
        .view_attachment("9999")
        .await
        .expect_err("an invented handle must not resolve");
    assert!(error.to_string().contains("9999"), "got: {error}");
}

#[tokio::test]
async fn sending_a_message_stops_the_typing_indicator() {
    // The sink is the only thing that knows a reply actually went out, so this is the wiring that
    // keeps the refresh loop from re-arming an indicator Telegram has already cleared.
    let store = Store::open_in_memory().await.expect("store");
    store
        .upsert_conversation(ConversationRecord {
            id: "mock:1".to_string(),
            channel_id: "mock".to_string(),
            platform: "telegram".to_string(),
            chat: "1".to_string(),
            thread: None,
            title: Some("Alice".to_string()),
            kind: "direct".to_string(),
            created_at: Utc::now(),
            last_inbound_at: Some(Utc::now()),
            last_outbound_at: None,
        })
        .await
        .expect("conversation");

    let channel = Arc::new(MockChannel::new("mock"));
    let channels = Arc::new(ChannelRegistry::from_channels([
        Arc::clone(&channel) as Arc<dyn Channel>
    ]));
    let presence = Arc::new(Presence::default());
    let sink = sink_with_storage(
        store,
        channels,
        std::env::temp_dir().join("mekabridge-test-attachments"),
        Arc::clone(&presence),
    );

    let conversation = ConversationId::parse("mock:1").expect("valid");
    assert!(!presence.has_replied(&conversation));

    sink.send_text("mock:1", "hello", SendOptions::default())
        .await
        .expect("sends");
    assert!(
        presence.has_replied(&conversation),
        "a delivered message must silence the indicator for that conversation"
    );
}

#[tokio::test]
async fn sending_a_file_also_silences_the_typing_indicator() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("report.pdf");
    std::fs::write(&path, b"pdf").expect("write");

    let store = Store::open_in_memory().await.expect("store");
    store
        .upsert_conversation(ConversationRecord {
            id: "mock:1".to_string(),
            channel_id: "mock".to_string(),
            platform: "telegram".to_string(),
            chat: "1".to_string(),
            thread: None,
            title: Some("Alice".to_string()),
            kind: "direct".to_string(),
            created_at: Utc::now(),
            last_inbound_at: Some(Utc::now()),
            last_outbound_at: None,
        })
        .await
        .expect("conversation");

    let channel = Arc::new(MockChannel::new("mock"));
    let channels = Arc::new(ChannelRegistry::from_channels([
        Arc::clone(&channel) as Arc<dyn Channel>
    ]));
    let presence = Arc::new(Presence::default());
    let sink = sink_with_storage(
        store,
        channels,
        std::env::temp_dir().join("mekabridge-test-attachments"),
        Arc::clone(&presence),
    );

    sink.send_file("mock:1", &path, None, false)
        .await
        .expect("sends");
    assert!(
        presence.has_replied(&ConversationId::parse("mock:1").expect("valid")),
        "a file counts as a reply for indicator purposes"
    );
}

#[tokio::test]
async fn a_message_arriving_mid_turn_is_flagged_in_the_next_envelope() {
    // The bridge does not interrupt a running turn, so the follow-up lands in the turn after.
    // Saying when it arrived is what lets the agent notice its previous reply was premature and
    // correct itself, rather than answering as though nothing had changed.
    let harness = Harness::start_slow(Duration::from_millis(700)).await;
    harness
        .sender
        .send(message("check the prod logs", "1"))
        .await
        .expect("queued");

    harness
        .wait_for("the first turn", |harness| !harness.turns().is_empty())
        .await;

    // Sent immediately after the first turn began, the way an amendment actually arrives.
    harness
        .sender
        .send(message("actually, staging", "2"))
        .await
        .expect("queued");

    harness
        .wait_for("the second turn", |harness| harness.turns().len() > 1)
        .await;

    let turns = harness.turns();
    assert!(turns[1].contains("actually, staging"), "got:\n{}", turns[1]);
    assert!(
        turns[1].contains("late: this arrived while you were still working"),
        "the amendment must be marked as having landed mid-turn, got:\n{}",
        turns[1]
    );
}
