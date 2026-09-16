//! End-to-end tests for the inbound and outbound paths.
//!
//! A stub meka (a real axum server speaking the real inbox and feed wire formats) stands in for the
//! agent, and a `MockChannel` stands in for Telegram. Together they let the whole loop run against
//! real code: events land in the durable queue, the session task binds them into a hand-over and
//! posts it to the inbox, the session feed says what became of it, and the sink delivers what the
//! agent asked for.

// Integration tests are their own crate, so clippy's `allow-*-in-tests` settings do not reach here.
// Assertions and timeout failures read better as `expect`/`panic` than as manual error plumbing.
#![allow(clippy::expect_used, clippy::panic)]

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use axum::{Router, extract::State, response::IntoResponse, routing::post};
use chrono::Utc;
use mekabridge::{
    bridge::{
        BridgeSink, ChannelAccounts,
        feed::feed_reader,
        inbound::{self, SessionContext},
        turn::{Presence, TurnTypist},
    },
    channel::{
        Activity, Admission, Attachment, AttachmentKind, Channel, ChannelCapabilities,
        ChannelError, ChannelId, ChannelIdentity, ChannelRegistry, ChatKind, ConversationId,
        FetchedFile, FileOptions, FoundMessage, InboundEvent, InboundMessage, Platform,
        SendOptions, Sender, SentMessage,
    },
    config::{Config, DefaultPolicy, StorageConfig},
    mcp::{OutboundSink, ViewedAttachment},
    meka::MekaClient,
    store::{
        AccountId, AccountIdentity, ConversationRecord, HandOver, MessageKey, Policy, QueueState,
        QueueStats, Store,
    },
};
use tokio::sync::{Notify, broadcast, mpsc, watch};
use tokio_util::sync::CancellationToken;

/// A 1x1 PNG, so attachment tests move real image bytes rather than an arbitrary blob.
const ONE_PIXEL_PNG: &[u8] = &[
    0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44, 0x52,
    0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x06, 0x00, 0x00, 0x00, 0x1F, 0x15, 0xC4,
    0x89, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x44, 0x41, 0x54, 0x78, 0xDA, 0x63, 0xFC, 0xCF, 0xC0, 0x50,
    0x0F, 0x00, 0x04, 0x85, 0x01, 0x80, 0x84, 0xA9, 0x8C, 0x21, 0x00, 0x00, 0x00, 0x00, 0x49, 0x45,
    0x4E, 0x44, 0xAE, 0x42, 0x60, 0x82,
];

/// How the stub refuses an inbox post. Each one leaves the bridge entitled to do something
/// different next, which is the whole of what these tests are about.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum FailureKind {
    /// meka's own fault rather than the upstream's. Nothing was written, so the same batch is
    /// posted again under the same key.
    #[default]
    Transient,
    /// meka's classifier labelled an upstream failure transient and gave it a type saying so,
    /// which is the signal the backoff exists to act on. Carries the upstream's text and a
    /// `retry_after`, as a 502 does whenever `[serve] relay_provider_errors` is left on.
    RateLimited,
    /// The conversation no longer fits the model's window. Permanent, so more attempts only delay
    /// the notice saying an operator is needed.
    Unrepairable,
    /// A rotated token. Nothing to wait for.
    Auth,
    /// Another meka process holds the session: a wait rather than a failure.
    Locked,
}

impl FailureKind {
    /// The status and Problem Detail the stub answers a post with.
    fn response(self) -> (u16, &'static str) {
        match self {
            Self::Transient => (
                500,
                r#"{"type":"https://meka.so/errors/internal","title":"Internal server error",
                    "status":500,"detail":"provider temporarily unavailable: API returned 429"}"#,
            ),
            Self::RateLimited => (
                502,
                r#"{"type":"https://meka.so/errors/provider-unavailable",
                    "title":"Provider temporarily unavailable","status":502,
                    "detail":"the provider did not complete this turn",
                    "provider_response":"529 overloaded_error","retry_after":0.05}"#,
            ),
            Self::Unrepairable => (
                502,
                r#"{"type":"https://meka.so/errors/context-overflow",
                    "title":"Conversation exceeds the model's context window",
                    "status":502,"detail":"could not be compacted further; shorten it"}"#,
            ),
            Self::Auth => (
                401,
                r#"{"type":"https://meka.so/errors/auth","title":"Unauthorized",
                    "status":401,"detail":"the bearer token is not recognised"}"#,
            ),
            Self::Locked => (
                409,
                r#"{"type":"https://meka.so/errors/session-locked","title":"Session locked",
                    "status":409,"detail":"another meka process holds this session"}"#,
            ),
        }
    }
}

/// What the stub's inbox driver does with an item it accepted.
///
/// meka decides this, not the client: an item is read by a turn, or withdrawn, or given up on, and
/// the feed is the only place any of it is reported.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum TurnScript {
    /// The ordinary turn: the model reads the item and answers through a send tool.
    #[default]
    Answer,
    /// The model reads the item and says nothing at all, which is legal.
    Silent,
    /// The model reads the item and comes back with meka's empty-response stand-in, having called
    /// nothing. Provably inert, so the messages may be offered again.
    Empty,
    /// The model reads the item and the turn then fails, with nothing sent.
    FailAfterReading,
    /// The model reads the item, answers somebody, and the turn then fails. Nothing may be offered
    /// again: the work is done and a second run would repeat it with the agent unable to remember
    /// the first.
    FailAfterSending,
    /// The model reads the item and the turn is then cancelled.
    CancelAfterReading,
    /// The item is taken back before the model reads it, which is what cancelling the turn it
    /// opened does.
    Withdraw,
    /// meka gives up on the item after its retry ceiling.
    GiveUp,
    /// A turn that writes a send call's arguments slowly, so the composing window has real
    /// duration on the wire. The tool it calls is [`MekaRecorder::compose_tool`].
    Compose,
    /// Nothing happens: the item sits `pending` on meka's side, as one waiting out a turn the
    /// bridge cannot see does.
    Hold,
}

/// One frame on the stub's feed.
#[derive(Debug, Clone)]
struct Frame {
    id: u64,
    name: String,
    data: String,
}

impl Frame {
    fn encode(&self) -> String {
        format!(
            "event: {}\nid: {}\ndata: {}\n\n",
            self.name, self.id, self.data
        )
    }
}

/// The stub's session feed: everything it has ever emitted, and a broadcast to whoever is attached.
///
/// Ids run across the whole session and the ring spans turns, as meka's does, so an attach naming
/// an id from an earlier turn resumes across them.
#[derive(Debug)]
struct Feed {
    ring: Mutex<Vec<Frame>>,
    next_id: AtomicU64,
    turns: AtomicU64,
    sender: broadcast::Sender<Frame>,
    /// Closes every live connection, which is what a dropped socket looks like from the reader's
    /// side. Notified rather than flagged, so a stream parked on the broadcast wakes for it.
    cut: Arc<Notify>,
}

impl Default for Feed {
    fn default() -> Self {
        let (sender, _) = broadcast::channel(4096);
        Self {
            ring: Mutex::new(Vec::new()),
            next_id: AtomicU64::new(1),
            turns: AtomicU64::new(0),
            sender,
            cut: Arc::new(Notify::new()),
        }
    }
}

impl Feed {
    /// Emit one event, stamping in the `turn_id` and `session_id` every meka event carries.
    fn push(&self, name: &str, turn: Option<&str>, mut data: serde_json::Value) {
        if let Some(object) = data.as_object_mut() {
            if let Some(turn) = turn {
                object.insert("turn_id".to_string(), serde_json::json!(turn));
            }
            object.insert("session_id".to_string(), serde_json::json!("s"));
        }
        let frame = Frame {
            id: self.next_id.fetch_add(1, Ordering::SeqCst),
            name: name.to_string(),
            data: data.to_string(),
        };
        self.ring
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(frame.clone());
        // An error here means nobody is attached, which is ordinary between connections: the ring
        // still holds the frame and the next attach replays it, exactly as meka's does.
        let _ = self.sender.send(frame);
    }

    /// Drop every connection now, as a NAT losing state or a proxy recycling a worker does.
    fn cut(&self) {
        self.cut.notify_waiters();
    }

    /// A turn id nothing else will use.
    fn next_turn(&self) -> String {
        format!("t{}", self.turns.fetch_add(1, Ordering::SeqCst) + 1)
    }

    /// Everything after `last`, which is what an attach replays.
    fn since(&self, last: Option<u64>) -> Vec<Frame> {
        self.ring
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .filter(|frame| last.is_none_or(|last| frame.id > last))
            .cloned()
            .collect()
    }
}

/// What the stub meka observed, so tests can assert on what the agent would have been handed.
#[derive(Default)]
struct MekaRecorder {
    /// Every inbox post that was accepted, as `(key, body)`, in order. A refusal never lands here,
    /// so a test asserting that a batch was handed over cannot be satisfied by one that was turned
    /// away.
    items: Mutex<Vec<(String, String)>>,
    /// Every post, accepted or refused, for tests that measure the rate.
    attempts: Mutex<usize>,
    /// When each post arrived, for tests that measure the wait between them rather than how many
    /// there were.
    attempt_times: Mutex<Vec<std::time::Instant>>,
    /// `class` and `source` off each post, so a test can pin the contract rather than the body.
    classes: Mutex<Vec<(String, String)>>,
    /// The item each `Idempotency-Key` was answered with, so a replay answers the same item, as
    /// meka's row-scoped key does across a restart.
    by_key: Mutex<HashMap<String, String>>,
    /// Each item's state, which is what a replayed key reports.
    states: Mutex<HashMap<String, String>>,
    /// Posts to refuse before accepting anything.
    fail_first: Mutex<usize>,
    /// What those refusals look like, which is what decides what the bridge may do next.
    failure: Mutex<FailureKind>,
    /// A conversation id that must appear in the body for a post to be refused at all, so one chat
    /// can be kept in trouble while another is answered normally.
    fail_only: Mutex<Option<String>>,
    /// Posts to answer with `session-not-found`, for exercising session recreation.
    forget_session_first: Mutex<usize>,
    /// What the driver does with an item it accepted.
    script: Mutex<TurnScript>,
    /// How long the driver waits before running the turn. The default of zero makes the suite
    /// fast; a test that needs something to happen *during* a turn sets it.
    turn_delay: Mutex<Duration>,
    /// How long [`TurnScript::Compose`] holds the stream between `tool_call.composing` and
    /// `tool_call.executing`.
    ///
    /// The gap has to be real rather than two events in one chunk: the bridge raises the typing
    /// indicator on the first and drops it on the second, so with no wall-clock between them there
    /// is nothing for a test to observe and nothing a person would see either.
    compose_for: Mutex<Duration>,
    /// Which tool that call names. Anything other than a send tool is the "agent is off reading
    /// files" case, which must draw nothing.
    compose_tool: Mutex<String>,
    /// Whether the composing call is abandoned and replaced by a different one, which is what a
    /// caller sees when meka retries the provider round. `tool_call.composing` deliberately does
    /// not mark the attempt as having produced output, so the whole window it opens is one meka
    /// will retry from; the event already sent cannot be withdrawn, and the retry mints fresh ids.
    compose_retry: Mutex<bool>,
    /// How far the current scripted turn has got, so a test can wait for the phase it cares about
    /// rather than for the turn being entered: 1 once the composing window is open, 2 once the
    /// arguments are written, 3 once the turn has ended.
    phase: Mutex<usize>,
    /// The session feed.
    feed: Arc<Feed>,
    /// `Last-Event-ID` values seen on the feed, so a test can assert the bridge resumed from the
    /// right place rather than replaying from the start.
    attaches: Mutex<Vec<Option<u64>>>,
    /// Whether the next attach reports a replay hole, as meka does when its ring no longer reaches
    /// the client's position.
    feed_gap: Mutex<bool>,
    /// Feed attaches to refuse with a 404, standing in for a session meka has lost.
    feed_404_first: Mutex<usize>,
    /// Close the feed connection after this many frames, to exercise the reconnect.
    drop_feed_after: Mutex<Option<usize>>,
    /// Whether the feed carries a frame this build cannot parse: a known event name with a payload
    /// that is not JSON at all.
    garbled_frame: Mutex<bool>,
    /// Whether the turn runs before the 202 is answered, so the feed reports the item's whole life
    /// while the client is still waiting to learn its id. Ordinary on a fast local meka, and the
    /// one ordering a client cannot control.
    run_before_answering: Mutex<bool>,
    /// `POST /cancel` calls.
    cancels: Mutex<usize>,
    /// What `GET /v1/health/ready` answers with: the status line and the HTTP code.
    readiness: Mutex<Option<(u16, String)>>,
    /// What `GET /v1/sessions/{id}` reports for `turn_in_flight`.
    turn_in_flight: Mutex<bool>,
    /// The turn running right now, if any.
    ///
    /// meka runs one turn at a time per session and reads every item waiting for it into that
    /// turn. Whichever item's driver gets here first takes the lot; one that arrives while the
    /// turn runs is appended to it at the next round boundary rather than opening a turn of
    /// its own.
    in_turn: Mutex<Option<String>>,
    /// Each item as `(id, body)`, in the order meka accepted them, which is the order a turn reads
    /// them in.
    by_item: Mutex<Vec<(String, String)>>,
    /// What each turn read, in the order the turns ran: the turn's id and the bodies of the items
    /// it was handed, including any appended after it started.
    ///
    /// The batching assertion the suite actually cares about. One item per message is the contract
    /// with meka; one turn per burst is what it buys, and only the server can say whether it held.
    turns_read: Mutex<Vec<(String, Vec<String>)>>,
}

impl MekaRecorder {
    fn set<T>(&self, field: &Mutex<T>, value: T) {
        *field
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = value;
    }

    fn take_one(&self, field: &Mutex<usize>) -> bool {
        let mut remaining = field
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if *remaining > 0 {
            *remaining -= 1;
            true
        } else {
            false
        }
    }

    fn state_of(&self, item_id: &str) -> String {
        self.states
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(item_id)
            .cloned()
            .unwrap_or_else(|| "pending".to_string())
    }

    /// Note that `turn` read these items, which is what a batching assertion is really about.
    ///
    /// Keyed on the turn rather than appended blindly, so items meka hands to a turn already
    /// running land in that turn's list instead of reading as a turn of their own.
    fn read_in(&self, turn: &str, items: &[String]) {
        let bodies: Vec<String> = {
            let by_item = self
                .by_item
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            items
                .iter()
                .filter_map(|item| {
                    by_item
                        .iter()
                        .find(|(id, _)| id == item)
                        .map(|(_, body)| body.clone())
                })
                .collect()
        };
        let mut turns = self
            .turns_read
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match turns.iter_mut().find(|(id, _)| id == turn) {
            Some((_, read)) => read.extend(bodies),
            None => turns.push((turn.to_string(), bodies)),
        }
    }

    /// Take every item still waiting, stamping it with `state`, oldest first.
    ///
    /// Ordered by when meka accepted them rather than by id, which carries a uuid, because the
    /// order a turn reads them in is the order they were said in.
    fn take_waiting(&self, state: &str) -> Vec<String> {
        let accepted: Vec<String> = self
            .by_item
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .map(|(item, _)| item.clone())
            .collect();
        let mut states = self
            .states
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let waiting: Vec<String> = accepted
            .into_iter()
            .filter(|item| states.get(item).is_some_and(|held| held == "pending"))
            .collect();
        for item in &waiting {
            states.insert(item.clone(), state.to_string());
        }
        waiting
    }

    fn mark(&self, item_id: &str, state: &str) {
        self.states
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(item_id.to_string(), state.to_string());
    }
}

async fn create_session() -> impl IntoResponse {
    axum::Json(serde_json::json!({ "id": uuid::Uuid::new_v4() }))
}

/// `POST /v1/sessions/{id}/inbox`: the door every message goes through.
///
/// Never refuses for a turn in flight, which is the whole point of the inbox; the refusals it does
/// have are scripted, and each is one the bridge has to answer differently.
async fn enqueue_item(
    State(recorder): State<Arc<MekaRecorder>>,
    headers: axum::http::HeaderMap,
    body: String,
) -> impl IntoResponse {
    let parsed = serde_json::from_str::<serde_json::Value>(&body).ok();
    let field = |name: &str| -> String {
        parsed
            .as_ref()
            .and_then(|value| value.get(name))
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string()
    };
    let message = field("message");
    let key = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    recorder
        .classes
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .push((field("class"), field("source")));
    *recorder
        .attempts
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) += 1;
    recorder
        .attempt_times
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .push(std::time::Instant::now());

    if recorder.take_one(&recorder.forget_session_first) {
        return (
            axum::http::StatusCode::NOT_FOUND,
            [(axum::http::header::CONTENT_TYPE, "application/problem+json")],
            r#"{"type":"https://meka.so/errors/session-not-found","title":"Session not found",
                "status":404,"detail":"gone"}"#
                .to_string(),
        )
            .into_response();
    }

    let eligible = recorder
        .fail_only
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
        .is_none_or(|conversation| message.contains(&conversation));
    if eligible && recorder.take_one(&recorder.fail_first) {
        let kind = *recorder
            .failure
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let (status, problem) = kind.response();
        return (
            axum::http::StatusCode::from_u16(status).unwrap_or(axum::http::StatusCode::BAD_GATEWAY),
            [(axum::http::header::CONTENT_TYPE, "application/problem+json")],
            problem.to_string(),
        )
            .into_response();
    }

    // The key is recorded on the row rather than in memory, so a retry that lands after a restart
    // still finds its earlier item and is answered with that item's current state.
    let replayed = recorder
        .by_key
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(&key)
        .cloned();
    if let Some(item_id) = replayed {
        let state = recorder.state_of(&item_id);
        return (
            axum::http::StatusCode::ACCEPTED,
            axum::Json(serde_json::json!({
                "item_id": item_id,
                "session_id": "s",
                "class": "steer",
                "state": state,
                "replayed": true,
            })),
        )
            .into_response();
    }

    let item_id = format!("item-{}", uuid::Uuid::new_v4());
    recorder
        .by_key
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(key.clone(), item_id.clone());
    recorder.mark(&item_id, "pending");
    recorder
        .by_item
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .push((item_id.clone(), message.clone()));
    recorder
        .items
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .push((key, message));

    if *recorder
        .run_before_answering
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
    {
        run_turn(Arc::clone(&recorder)).await;
    }
    (
        axum::http::StatusCode::ACCEPTED,
        axum::Json(serde_json::json!({
            "item_id": item_id,
            "session_id": "s",
            "class": "steer",
            "state": "pending",
            "replayed": false,
        })),
    )
        .into_response()
}

/// meka's inbox drain: the turn everything waiting is read into, and what the feed says about it.
///
/// One turn takes every item waiting for it, which is what makes a burst of messages cost one
/// provider round trip rather than one each, and one that lands while a turn runs is appended to
/// that turn rather than opening another. The bridge posts an item per message and relies on both.
async fn run_turn(recorder: Arc<MekaRecorder>) {
    let script = *recorder
        .script
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let feed = Arc::clone(&recorder.feed);

    // Neither of these opens a turn: meka withdraws the item and says so, with nothing running.
    match script {
        TurnScript::Hold => return,
        TurnScript::Withdraw => {
            for item_id in recorder.take_waiting("withdrawn") {
                feed.push(
                    "inbox.withdrawn",
                    None,
                    serde_json::json!({ "item_id": item_id }),
                );
            }
            return;
        }
        TurnScript::GiveUp => {
            for item_id in recorder.take_waiting("withdrawn") {
                feed.push(
                    "inbox.failed",
                    None,
                    serde_json::json!({
                        "item_id": item_id,
                        "reason": "no turn could deliver it in 7 attempt(s) over the last hour",
                    }),
                );
            }
            return;
        }
        _ => {}
    }

    // Claimed under the lock that names the running turn, so two drains racing cannot open two
    // turns or read one item twice.
    let opened = {
        let mut in_turn = recorder
            .in_turn
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match in_turn.clone() {
            Some(turn) => Err(turn),
            None => {
                let waiting = recorder.take_waiting("delivered");
                let turn = (!waiting.is_empty()).then(|| feed.next_turn());
                if let Some(turn) = &turn {
                    *in_turn = Some(turn.clone());
                }
                Ok(turn.map(|turn| (turn, waiting)))
            }
        }
    };
    let (turn, items) = match opened {
        Ok(Some(opened)) => opened,
        // Nothing is waiting, which is what most ticks find.
        Ok(None) => return,
        Err(turn) => {
            // Appended to the turn already under way, which meka reads at its next round boundary.
            let appended = recorder.take_waiting("delivered");
            if !appended.is_empty() {
                recorder.read_in(&turn, &appended);
                feed.push(
                    "inbox.delivered",
                    Some(&turn),
                    serde_json::json!({ "item_ids": appended }),
                );
            }
            return;
        }
    };
    recorder.set(&recorder.phase, 0);
    let delay = *recorder
        .turn_delay
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if !delay.is_zero() {
        tokio::time::sleep(delay).await;
    }
    feed.push(
        "turn.started",
        Some(&turn),
        serde_json::json!({
            "started_at": Utc::now().to_rfc3339(),
            "source": "inbox",
            "item_ids": items.clone(),
        }),
    );
    // The provider accepted the request carrying the items, so the model has read them. Reported
    // before anything the model produced, which is when it actually happens.
    recorder.read_in(&turn, &items);
    feed.push(
        "inbox.delivered",
        Some(&turn),
        serde_json::json!({ "item_ids": items }),
    );

    if *recorder
        .garbled_frame
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
    {
        // Invalid JSON outright: every field accessor in the parser is lenient, defaulting a
        // missing or wrongly-typed member rather than failing, so a payload of the wrong *shape*
        // still parses.
        let frame = Frame {
            id: feed.next_id.fetch_add(1, Ordering::SeqCst),
            name: "assistant_text.delta".to_string(),
            data: "{\"text\":".to_string(),
        };
        feed.ring
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(frame.clone());
        let _ = feed.sender.send(frame);
    }

    let send_tool = "mcp__mekabridge__send_message";
    let sent = |id: &str| serde_json::json!({ "id": id, "name": send_tool, "input": {}, "display_summary": null });
    match script {
        TurnScript::Answer => {
            feed.push(
                "tool_call.composing",
                Some(&turn),
                serde_json::json!({ "id": "c1", "name": send_tool }),
            );
            feed.push("tool_call.executing", Some(&turn), sent("c1"));
            feed.push(
                "tool_call.completed",
                Some(&turn),
                serde_json::json!({ "id": "c1", "is_error": false }),
            );
            finish(&feed, &turn, "end_turn");
        }
        TurnScript::Silent => {
            feed.push(
                "assistant_text.delta",
                Some(&turn),
                serde_json::json!({ "text": "I will leave that one" }),
            );
            finish(&feed, &turn, "end_turn");
        }
        TurnScript::Empty => {
            feed.push(
                "assistant_text.delta",
                Some(&turn),
                serde_json::json!({ "text": "[The model returned an empty response.]" }),
            );
            finish(&feed, &turn, "end_turn");
        }
        TurnScript::FailAfterReading => {
            feed.push(
                "turn.failed",
                Some(&turn),
                serde_json::json!({
                    "error": {
                        "type": "https://meka.so/errors/provider-unavailable",
                        "title": "Provider temporarily unavailable",
                        "status": 502,
                        "detail": "the provider did not complete this turn",
                        "provider_response": "529 overloaded_error",
                    },
                }),
            );
        }
        TurnScript::FailAfterSending => {
            feed.push("tool_call.executing", Some(&turn), sent("c1"));
            feed.push(
                "turn.failed",
                Some(&turn),
                serde_json::json!({
                    "error": {
                        "type": "https://meka.so/errors/internal",
                        "title": "Internal server error",
                        "status": 500,
                        "detail": "provider temporarily unavailable: API returned 429",
                    },
                }),
            );
        }
        TurnScript::CancelAfterReading => {
            feed.push(
                "turn.canceled",
                Some(&turn),
                serde_json::json!({ "reason": "client" }),
            );
        }
        TurnScript::Compose => {
            let window = *recorder
                .compose_for
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let tool = {
                let name = recorder
                    .compose_tool
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .clone();
                if name.is_empty() {
                    send_tool.to_string()
                } else {
                    name
                }
            };
            // The agent working before it writes anything, which must draw nothing.
            feed.push(
                "assistant_text.delta",
                Some(&turn),
                serde_json::json!({ "text": "on it" }),
            );
            tokio::time::sleep(window).await;
            feed.push(
                "tool_call.composing",
                Some(&turn),
                serde_json::json!({ "id": "c1", "name": tool }),
            );
            recorder.set(&recorder.phase, 1);
            tokio::time::sleep(window).await;
            if *recorder
                .compose_retry
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
            {
                // The call being written is abandoned and a different one takes its place, which
                // is what meka's own retry looks like from here: no closing event for `c1`, ever.
                feed.push(
                    "tool_call.composing",
                    Some(&turn),
                    serde_json::json!({ "id": "c2", "name": "read_file" }),
                );
                feed.push(
                    "tool_call.executing",
                    Some(&turn),
                    serde_json::json!({
                        "id": "c2", "name": "read_file", "input": {}, "display_summary": null,
                    }),
                );
            } else {
                feed.push(
                    "tool_call.executing",
                    Some(&turn),
                    serde_json::json!({
                        "id": "c1", "name": tool, "input": {}, "display_summary": null,
                    }),
                );
                feed.push(
                    "tool_call.completed",
                    Some(&turn),
                    serde_json::json!({ "id": "c1", "is_error": false }),
                );
            }
            recorder.set(&recorder.phase, 2);
            tokio::time::sleep(window).await;
            finish(&feed, &turn, "end_turn");
        }
        TurnScript::Hold | TurnScript::Withdraw | TurnScript::GiveUp => unreachable!(),
    }
    recorder.set(&recorder.phase, 3);
    recorder.set(&recorder.in_turn, None);
}

fn finish(feed: &Feed, turn: &str, stop_reason: &str) {
    feed.push(
        "turn.finished",
        Some(turn),
        serde_json::json!({
            "stop_reason": stop_reason,
            "usage": { "input_tokens": 1, "output_tokens": 1 },
        }),
    );
}

/// `GET /v1/sessions/{id}/stream`: the session feed, which does not end with a turn.
///
/// Replays everything after `Last-Event-ID` out of the ring, then follows the live feed for as long
/// as the connection is held.
async fn open_feed(
    State(recorder): State<Arc<MekaRecorder>>,
    headers: axum::http::HeaderMap,
) -> axum::response::Response {
    let last_event_id = headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok());
    recorder
        .attaches
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .push(last_event_id);

    if recorder.take_one(&recorder.feed_404_first) {
        return (
            axum::http::StatusCode::NOT_FOUND,
            [(axum::http::header::CONTENT_TYPE, "application/problem+json")],
            r#"{"type":"https://meka.so/errors/session-not-found","title":"Session not found",
                "status":404,"detail":"gone"}"#,
        )
            .into_response();
    }

    // Subscribed before the ring is read, so a frame emitted between the two is not lost; the
    // filter below is what keeps it from being sent twice.
    let receiver = recorder.feed.sender.subscribe();
    let backlog = recorder.feed.since(last_event_id);
    let highest = backlog
        .last()
        .map_or(last_event_id.unwrap_or(0), |frame| frame.id);

    let mut opening = vec!["retry: 3000\n\n".to_string()];
    if *recorder
        .feed_gap
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
    {
        recorder.set(&recorder.feed_gap, false);
        // meka's wording for a ring that no longer reaches the client's position. Deliberately
        // carries no `id:`, so a client cannot move its position onto a notice.
        opening.push(
            "event: notice\ndata: {\"level\":\"warn\",\"text\":\"the replay does not reach your \
             Last-Event-ID, so events were dropped; read `GET /v1/sessions/{id}/messages` for the \
             full transcript\"}\n\n"
                .to_string(),
        );
    }
    opening.extend(backlog.iter().map(Frame::encode));

    let drop_after = *recorder
        .drop_feed_after
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    recorder.set(&recorder.drop_feed_after, None);
    let cut = Arc::clone(&recorder.feed.cut);
    let body = axum::body::Body::from_stream(futures::stream::unfold(
        (opening.into_iter(), receiver, 0_usize, highest),
        move |(mut opening, mut receiver, sent, highest)| {
            let cut = Arc::clone(&cut);
            async move {
                if drop_after.is_some_and(|limit| sent >= limit) {
                    return None;
                }
                if let Some(chunk) = opening.next() {
                    return Some((
                        Ok::<_, std::io::Error>(axum::body::Bytes::from(chunk)),
                        (opening, receiver, sent + 1, highest),
                    ));
                }
                loop {
                    let received = tokio::select! {
                        () = cut.notified() => return None,
                        received = receiver.recv() => received,
                    };
                    match received {
                        // Already in the replay: a frame emitted between subscribing and reading
                        // the ring is in both, and sending it twice would move a client's position
                        // over an event it has acted on.
                        Ok(frame) if frame.id <= highest => continue,
                        Ok(frame) => {
                            return Some((
                                Ok(axum::body::Bytes::from(frame.encode())),
                                (opening, receiver, sent + 1, highest),
                            ));
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => return None,
                    }
                }
            }
        },
    ));
    (
        [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
        body,
    )
        .into_response()
}

async fn cancel_turn(State(recorder): State<Arc<MekaRecorder>>) -> impl IntoResponse {
    *recorder
        .cancels
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) += 1;
    axum::http::StatusCode::NO_CONTENT
}

/// `GET /v1/health/ready`. meka answers 503 with the *same* body it sends on 200, naming which
/// subsystem is the blocker, rather than a Problem Detail.
async fn ready(State(recorder): State<Arc<MekaRecorder>>) -> axum::response::Response {
    let answer = recorder
        .readiness
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    match answer {
        Some((status, body)) => (
            axum::http::StatusCode::from_u16(status).unwrap_or(axum::http::StatusCode::OK),
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            body,
        )
            .into_response(),
        None => axum::Json(serde_json::json!({
            "status": "ok",
            "session_db": true,
            "profile_configured": true,
            "mcp_servers_healthy": true,
        }))
        .into_response(),
    }
}

async fn get_session(
    State(recorder): State<Arc<MekaRecorder>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let in_flight = *recorder
        .turn_in_flight
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    // `none` rather than the config's level, so the reconcile path is the one under test. It is
    // also what meka 0.46 migrates an `ask` session to, which is the case that most needs moving.
    axum::Json(serde_json::json!({
        "id": id,
        "permission": "none",
        "title": "stub",
        "turn_in_flight": in_flight,
        "inbox_pending": 0,
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
/// How long meka's inbox drain takes to notice an item.
///
/// A turn does not begin the instant an item lands: the drain has to wake, the session has to load,
/// and the provider round trip has to start. That gap is why a burst of messages the bridge posts
/// one after another is read by one turn, and a stub that opened a turn the instant it answered a
/// 202 would model a meka nobody runs.
const INBOX_DRAIN: Duration = Duration::from_millis(30);

async fn start_meka(recorder: Arc<MekaRecorder>) -> (SocketAddr, CancellationToken) {
    let drained = Arc::clone(&recorder);
    let router = Router::new()
        .route("/v1/sessions", post(create_session))
        .route("/v1/sessions/{id}", axum::routing::get(get_session))
        .route("/v1/sessions/{id}/inbox", post(enqueue_item))
        .route("/v1/sessions/{id}/stream", axum::routing::get(open_feed))
        .route("/v1/sessions/{id}/cancel", post(cancel_turn))
        .route("/v1/info", axum::routing::get(info))
        .route("/v1/health/ready", axum::routing::get(ready))
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
    tokio::spawn({
        let drain = Arc::clone(&drained);
        let shutdown = shutdown.clone();
        async move {
            loop {
                tokio::select! {
                    () = shutdown.cancelled() => return,
                    () = tokio::time::sleep(INBOX_DRAIN) => {}
                }
                run_turn(Arc::clone(&drain)).await;
            }
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
    /// Whether each outbound asked for a link preview, in send order. Recorded separately from
    /// `sent` so the existing assertions on message bodies stay readable.
    previews: Mutex<Vec<bool>>,
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
            previews: Mutex::new(Vec::new()),
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

/// Where [`MockChannel::send_text`] pretends the platform's length limit fell.
const SPLIT_MARKER: &str = "<split>";

/// A part [`MockChannel::send_text`] refuses, standing in for a platform rejecting one message of a
/// split reply after the earlier ones have already landed.
const REFUSED_PART: &str = "<refused>";

/// Stand in for the record a platform returns when it accepts a send.
///
/// The real connectors build this from the response, which is what makes the bridge's own history
/// rows look like everybody else's. The mock has to supply one too, or nothing downstream of the
/// channel is exercised.
fn echo(message_id: &str, text: &str, attachments: Vec<Attachment>) -> SentMessage {
    SentMessage {
        message_id: message_id.to_string(),
        text: text.to_string(),
        sender: Sender {
            id: "4242".to_string(),
            display_name: "Mica".to_string(),
            username: Some("micaagentbot".to_string()),
            is_bot: true,
            on_behalf_of_chat: false,
        },
        attachments,
        notes: Vec::new(),
        timestamp: Utc::now(),
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
        options: &SendOptions,
        sent: &mut Vec<SentMessage>,
    ) -> Result<(), ChannelError> {
        self.previews
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(options.link_preview);
        let mut log = self
            .sent
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        log.push((conversation.as_str().to_string(), markdown.to_string()));
        // A real connector splits text past its platform's limit into several messages with several
        // ids. Reproducing that here on an explicit marker, rather than on a length, keeps every
        // other test sending exactly one message while still giving the split one something to
        // assert against.
        let base = log.len();
        for (index, part) in markdown.split(SPLIT_MARKER).enumerate() {
            // A part the platform refuses, with the earlier ones already in the chat. The whole
            // point of the out-parameter is that the caller keeps those.
            if part == REFUSED_PART {
                return Err(ChannelError::Delivery {
                    channel: self.id.as_str().to_string(),
                    message: format!("part {} of the message was refused", index + 1),
                });
            }
            sent.push(echo(&format!("m{}", base + index), part, Vec::new()));
        }
        Ok(())
    }

    async fn send_files(
        &self,
        conversation: &ConversationId,
        paths: &[std::path::PathBuf],
        caption: Option<&str>,
        options: &FileOptions,
        sent: &mut Vec<SentMessage>,
    ) -> Result<(), ChannelError> {
        self.previews
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(options.send.link_preview);
        let mut log = self
            .sent
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // One entry naming every path, so a test can tell one grouped send from several.
        log.push((
            conversation.as_str().to_string(),
            format!(
                "<files {}>",
                paths
                    .iter()
                    .map(|path| path.display().to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        ));
        // One message per file, the shape a Telegram album comes back in, so a test can tell that
        // the bridge records a row per real message rather than one per call. The caption rides on
        // the first, which is where the platform puts it.
        sent.extend(paths.iter().enumerate().map(|(index, path)| {
            echo(
                &format!("f{}", index + 1),
                if index == 0 {
                    caption.unwrap_or("")
                } else {
                    ""
                },
                vec![Attachment {
                    kind: AttachmentKind::Document,
                    file_name: path
                        .file_name()
                        .map(|name| name.to_string_lossy().to_string()),
                    media_type: None,
                    bytes: None,
                    // Keyed by path so `fetch` can be primed with the same string, which is
                    // what lets a test read back a file the agent sent.
                    width: None,
                    height: None,
                    duration_secs: None,
                    file_ref: path.display().to_string(),
                    thumb_ref: None,
                    handle: None,
                }],
            )
        }));
        Ok(())
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

    async fn search_messages(
        &self,
        _conversation: &ConversationId,
        _query: &str,
        _limit: usize,
    ) -> Result<Vec<FoundMessage>, ChannelError> {
        // Stands in for Discord's guild search, which reaches back past anything the bridge
        // recorded and settles `own` from the connector's own account id.
        Ok(vec![FoundMessage {
            message_id: "from-the-platform".to_string(),
            sender_name: "Mica".to_string(),
            text: "said long before this bridge existed".to_string(),
            own: true,
            timestamp: Utc::now(),
        }])
    }

    async fn edit_text(
        &self,
        _conversation: &ConversationId,
        message_id: &str,
        markdown: &str,
        _link_preview: bool,
    ) -> Result<Option<SentMessage>, ChannelError> {
        // A revision keeps the id of the message it revises, which is what makes the two rows in
        // the history belong to one message.
        Ok(Some(echo(message_id, markdown, Vec::new())))
    }

    async fn delete_message(
        &self,
        _conversation: &ConversationId,
        _message_id: &str,
    ) -> Result<(), ChannelError> {
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
async fn sink_for(store: Store, channels: Arc<ChannelRegistry>) -> BridgeSink {
    sink_with_storage(
        store,
        channels,
        std::env::temp_dir().join("mekabridge-test-attachments"),
        Arc::new(Presence::default()),
    )
    .await
}

async fn sink_with_storage(
    store: Store,
    channels: Arc<ChannelRegistry>,
    attachment_dir: std::path::PathBuf,
    presence: Arc<Presence>,
) -> BridgeSink {
    sink_against_meka(
        store,
        channels,
        attachment_dir,
        presence,
        ([127, 0, 0, 1], 1).into(),
    )
    .await
}

/// The same, pointed at a meka that answers, so the vision probe succeeds and `view_attachment`
/// reaches the code that decides between an image and a description.
async fn sink_against_meka(
    store: Store,
    channels: Arc<ChannelRegistry>,
    attachment_dir: std::path::PathBuf,
    presence: Arc<Presence>,
    meka_address: SocketAddr,
) -> BridgeSink {
    let accounts = mock_accounts(&store).await;
    let storage = StorageConfig {
        path: std::path::PathBuf::from("/tmp/mekabridge-unused.db"),
        attachment_dir,
        attachment_max_bytes: 20 * 1024 * 1024,
        attachment_retention: Duration::from_secs(86_400),
        history_retention: Duration::from_secs(86_400),
    };
    let meka = MekaClient::new(
        &config_for(
            meka_address,
            std::path::Path::new("/tmp/mekabridge-unused.db"),
            false,
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
        accounts,
    )
}

fn config_for(meka_address: SocketAddr, database: &std::path::Path, typing: bool) -> Config {
    let raw = format!(
        r#"
[meka]
base_url = "http://{meka_address}"
token = "test-token"

[bridge]
batch_max_messages = 32
max_queue_depth = 64
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
    // Everything the file format deliberately does not carry -- the coalescing floor, the wait
    // before a hand-over is posted again, the hour a batch has to be accepted in -- is set per
    // test by `Setup`, since at the shipped values every test would sleep through one of them.
    Config::from_toml(&raw, std::path::Path::new("/tmp/config.toml")).expect("valid config")
}

fn message(text: &str, message_id: &str) -> InboundEvent {
    InboundEvent::Message(Box::new(InboundMessage {
        channel: ChannelId::new("mock"),
        platform: Platform::Telegram,
        conversation: ConversationId::parse("mock:1").expect("valid"),
        message_id: message_id.to_string(),
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
        attachments: Vec::new(),
        timestamp: Utc::now(),
    }))
}

/// Register the mock channel's account, as the daemon does for every channel at startup.
///
/// The identity is what [`MockChannel::probe`] reports, since that is where the daemon gets it.
/// Idempotent, so a test may call it as often as it needs the id: the same bot keeps its row.
async fn mock_account(store: &Store) -> AccountId {
    store
        .register_account(
            "mock",
            "telegram",
            &AccountIdentity {
                platform_id: "1".to_string(),
                username: Some("mockbot".to_string()),
                display_name: Some("Mock".to_string()),
            },
            Utc::now(),
        )
        .await
        .expect("account registers")
        .account
}

/// The map the daemon builds at startup, over the mock channel alone.
async fn mock_accounts(store: &Store) -> Arc<ChannelAccounts> {
    let account = mock_account(store).await;
    let mut accounts = ChannelAccounts::default();
    accounts.add("mock", account, "@mockbot".to_string());
    Arc::new(accounts)
}

/// Alice's direct chat, as the writer would have recorded it on her first message.
fn direct_conversation(address: &str) -> ConversationRecord {
    ConversationRecord {
        address: address.to_string(),
        channel: "mock".to_string(),
        platform: "telegram".to_string(),
        title: Some("Alice".to_string()),
        kind: "direct".to_string(),
        created_at: Utc::now(),
        last_inbound_at: Some(Utc::now()),
        last_outbound_at: None,
    }
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
    /// second the suite would sleep through a real floor on every test that waits for a hand-over,
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
    /// Far below the shipped four seconds, so a test can tell an indicator that was stopped from
    /// one that is still being renewed. At the default no refresh tick lands inside a window a
    /// test would care to sample, which is how two typing tests came to pass against the very
    /// regressions they were written for.
    typing_refresh: Duration,
    /// Whether the bridge announces a message being written in the originating chats. Off for most
    /// of the suite, so it is not measuring presence it does not care about.
    typing_indicator: bool,
    /// The first wait before a hand-over meka did not accept is posted again. Scaled down from the
    /// shipped ten seconds, but not as far as the floor: a test that asserts a wait happened has
    /// to be able to tell it from the round trip it replaced.
    retry_base: Duration,
    /// How long a batch may go unaccepted before it is given up on. The shipped hour would have
    /// every give-up test sleeping through it.
    hand_over_within: Duration,
    /// Extra offers of a message whose item was taken back unread, or whose turn came back empty.
    max_offers: u32,
    /// How often meka is asked about a hand-over it has held too long. The shipped five minutes
    /// would have the one test that needs the sweep sleeping through it.
    straggler_sweep: Duration,
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
            typing_refresh: Duration::from_millis(100),
            typing_indicator: false,
            retry_base: Duration::from_millis(250),
            hand_over_within: Duration::from_secs(2),
            max_offers: 1,
            straggler_sweep: Duration::from_millis(100),
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

    /// Somewhere to report a failure in detail, and a chat that hears about it too.
    fn with_owner() -> Self {
        Self {
            owner: Some("mock:99".to_string()),
            ..Self::default()
        }
    }
}

impl Harness {
    /// The plain one: every post accepted, every turn answers.
    async fn start() -> Self {
        Self::start_all(0, 0, TurnScript::Answer, Setup::default()).await
    }

    /// A harness whose first `fail_first` posts are refused with a transient failure and whose
    /// later ones succeed.
    async fn start_flaky(fail_first: usize) -> Self {
        Self::start_all(fail_first, 0, TurnScript::Answer, Setup::default()).await
    }

    /// A harness whose first post is answered `session-not-found`, for exercising the rebind.
    async fn start_forgetting(forget_first: usize) -> Self {
        Self::start_all(0, forget_first, TurnScript::Answer, Setup::default()).await
    }

    /// A harness set up to be watched failing at the door: a scripted refusal of a given shape,
    /// repeated far more often than any of these tests will get through, so the batch never
    /// succeeds by running out of them.
    async fn start_failing(failure: FailureKind, setup: Setup) -> Self {
        let harness = Self::start_all(10_000, 0, TurnScript::Answer, setup).await;
        harness.recorder.set(&harness.recorder.failure, failure);
        harness
    }

    /// The plain one, with something to say about the wiring around it.
    async fn start_with_setup(setup: Setup) -> Self {
        Self::start_all(0, 0, TurnScript::Answer, setup).await
    }

    /// A harness whose accepted items meet `script` on meka's side.
    async fn start_script(script: TurnScript) -> Self {
        Self::start_all(0, 0, script, Setup::default()).await
    }

    /// The same, with something to say about it.
    async fn start_script_with(script: TurnScript, setup: Setup) -> Self {
        Self::start_all(0, 0, script, setup).await
    }

    /// A harness whose turns take `delay` to begin, for testing what happens while one runs.
    async fn start_slow(delay: Duration) -> Self {
        let harness = Self::start().await;
        harness.recorder.set(&harness.recorder.turn_delay, delay);
        harness
    }

    /// A harness whose floor is generous enough to hold a burst together while the writer persists
    /// it, for the tests that are about coalescing rather than about latency.
    async fn start_coalescing() -> Self {
        Self::start_all(0, 0, TurnScript::Answer, Setup::coalescing()).await
    }

    /// A harness whose quiet period is long enough to be unmistakable, for the tests about whether
    /// one was applied at all.
    async fn start_patient() -> Self {
        Self::start_all(0, 0, TurnScript::Answer, Setup::patient()).await
    }

    /// A harness whose turns call `tool`, holding the composing window open for `window` so a test
    /// can see what the indicator does during it and either side.
    async fn start_composing(tool: &str, window: Duration) -> Self {
        let harness = Self::start_all(0, 0, TurnScript::Compose, Setup {
            typing_indicator: true,
            ..Setup::default()
        })
        .await;
        harness.recorder.set(&harness.recorder.compose_for, window);
        harness
            .recorder
            .set(&harness.recorder.compose_tool, tool.to_string());
        harness
    }

    async fn start_all(
        fail_first: usize,
        forget_first: usize,
        script: TurnScript,
        setup: Setup,
    ) -> Self {
        let directory = tempfile::tempdir().expect("tempdir");
        let database = directory.path().join("state.db");
        let recorder = Arc::new(MekaRecorder::default());
        recorder.set(&recorder.fail_first, fail_first);
        recorder.set(&recorder.forget_session_first, forget_first);
        recorder.set(&recorder.script, script);

        let (meka_address, meka_shutdown) = start_meka(Arc::clone(&recorder)).await;
        let mut config = Arc::new(config_for(meka_address, &database, setup.typing_indicator));
        // Set here rather than through `config_for`, which validates the owner against the
        // configured channels and would need the whole TOML threading through for two fields.
        {
            let config = Arc::get_mut(&mut config).expect("sole reference");
            config.bridge.owner_conversation = setup.owner;
            config.bridge.notify_failures = setup.notify_failures;
            config.bridge.coalesce_floor = setup.coalesce_floor;
            config.bridge.settle = setup.settle;
            config.bridge.settle_max = setup.settle_max;
            config.bridge.typing_refresh = setup.typing_refresh;
            config.bridge.retry_base = setup.retry_base;
            config.bridge.hand_over_within = setup.hand_over_within;
            config.bridge.max_offers = setup.max_offers;
            config.bridge.straggler_sweep = setup.straggler_sweep;
            config.storage.history_retention = setup.history_retention;
        }

        let store = Store::open(&config.storage.path)
            .await
            .expect("store opens");
        let accounts = mock_accounts(&store).await;
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
            let accounts = Arc::clone(&accounts);
            async move { inbound::writer(store, config, receiver, wake, typing, accounts).await }
        });
        spawn_session(
            store.clone(),
            Arc::clone(&config),
            meka,
            channels,
            accounts,
            typing,
            Arc::new(Presence::default()),
            Arc::clone(&wake),
            shutdown.clone(),
        );

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

    /// Posts made, accepted or refused.
    fn attempts(&self) -> usize {
        *self
            .recorder
            .attempts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// The inbox items meka accepted, in order, one per message.
    fn items(&self) -> Vec<String> {
        self.recorder
            .items
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .map(|(_, body)| body.clone())
            .collect()
    }

    /// What each turn read, in the order the turns ran.
    fn turns(&self) -> Vec<Vec<String>> {
        self.recorder
            .turns_read
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .map(|(_, read)| read.clone())
            .collect()
    }

    /// The `Idempotency-Key` each accepted envelope was posted under.
    fn keys(&self) -> Vec<String> {
        self.recorder
            .items
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .map(|(key, _)| key.clone())
            .collect()
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
            "timed out waiting for {label}; envelopes so far: {:?}",
            self.items()
        );
    }

    /// Poll the store until `predicate` holds, which `wait_for` cannot do: its predicate is
    /// synchronous and reading the store is not.
    async fn wait_until(&self, label: &str, predicate: impl AsyncFn(&Store) -> bool) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while tokio::time::Instant::now() < deadline {
            if predicate(&self.store).await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!(
            "timed out waiting for {label}; envelopes so far: {:?}",
            self.items()
        );
    }

    /// Wait for the queue to reach a given shape, which is how most outcomes are asserted.
    async fn wait_for_queue(&self, label: &str, predicate: impl Fn(QueueStats) -> bool) {
        self.wait_until(label, async |store| {
            store.queue_stats().await.is_ok_and(&predicate)
        })
        .await;
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.shutdown.cancel();
        self.meka_shutdown.cancel();
    }
}

/// Start the feed reader and the session task over one store, the way [`mekabridge::bridge::run`]
/// does.
#[allow(
    clippy::too_many_arguments,
    reason = "the daemon's own wiring, spelled out once"
)]
fn spawn_session(
    store: Store,
    config: Arc<Config>,
    meka: MekaClient,
    channels: Arc<ChannelRegistry>,
    accounts: Arc<ChannelAccounts>,
    typing: Arc<inbound::TypingState>,
    presence: Arc<Presence>,
    wake: Arc<Notify>,
    shutdown: CancellationToken,
) {
    let (session_sender, session_receiver) = watch::channel::<Option<uuid::Uuid>>(None);
    let position = Arc::new(AtomicU64::new(0));
    let (feed_sender, feed_receiver) = mpsc::channel(4096);
    tokio::spawn({
        let meka = meka.clone();
        let position = Arc::clone(&position);
        let shutdown = shutdown.clone();
        async move { feed_reader(meka, session_receiver, position, feed_sender, shutdown).await }
    });
    tokio::spawn({
        let typist = TurnTypist::new(
            Arc::clone(&channels),
            config.bridge.typing_indicator,
            config.bridge.typing_refresh,
            config.bridge.typing_max,
            presence,
        );
        let context = SessionContext {
            store,
            config,
            meka,
            channels,
            typing,
            accounts,
            permission_checked: Arc::new(tokio::sync::OnceCell::new()),
            pending_session: Arc::default(),
            notices: inbound::NoticeLog::default(),
            session: session_sender,
            position,
        };
        async move { inbound::session_loop(context, typist, feed_receiver, wake, shutdown).await }
    });
}

#[tokio::test]
async fn an_inbound_message_becomes_a_turn_carrying_its_routing_header() {
    let harness = Harness::start().await;
    harness
        .sender
        .send(message("check the deploy logs", "1"))
        .await
        .expect("queued");

    harness
        .wait_for("the turn to be submitted", |harness| {
            !harness.items().is_empty()
        })
        .await;

    let turns = harness.items();
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
async fn the_queue_is_drained_once_the_model_has_read_the_batch() {
    // Delivered means the model read it, which the feed says outright. Nothing is inferred from
    // the hand-over being accepted: an item meka has is an item meka may yet give up on.
    let harness = Harness::start().await;
    harness
        .sender
        .send(message("hello", "1"))
        .await
        .expect("queued");
    harness
        .wait_until(
            "the turn to be recorded for `mekabridge status`",
            async |store| store.last_turn_at().await.is_ok_and(|at| at.is_some()),
        )
        .await;

    let stats = harness.store.queue_stats().await.expect("stats");
    assert_eq!(stats.done, 1);
    assert_eq!(stats.pending, 0);
    assert_eq!(stats.in_flight, 0);
    assert_eq!(
        (stats.in_flight, stats.posted),
        (0, 0),
        "the hand-over is closed with its row: {stats:?}"
    );
}

#[tokio::test]
async fn every_turn_names_the_account_the_agent_appears_as() {
    // Stated per turn rather than once at session start, because a one-time orientation is an
    // ordinary user message and the first compaction summarises it away for good.
    let harness = Harness::start().await;
    harness
        .sender
        .send(message("first", "1"))
        .await
        .expect("queued");
    harness
        .wait_for("the first turn", |harness| !harness.items().is_empty())
        .await;
    harness
        .sender
        .send(message("second", "2"))
        .await
        .expect("queued");
    harness
        .wait_for("the second turn", |harness| harness.items().len() >= 2)
        .await;

    let turns = harness.items();
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
    let harness = Harness::start().await;
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
        .wait_for("a turn", |harness| !harness.items().is_empty())
        .await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let items = harness.items();
    assert_eq!(
        items.len(),
        1,
        "the duplicate must not reach the agent: {items:?}"
    );
}

#[tokio::test]
async fn a_hand_over_meka_never_accepts_is_given_up_on() {
    // meka out of reach for the whole hour a batch has to be accepted in, scaled down here. Every
    // post is refused, so nothing ever reaches the agent and the messages have to end up somewhere
    // rather than being posted for ever.
    let harness = Harness::start_failing(FailureKind::Transient, Setup {
        hand_over_within: Duration::from_millis(600),
        ..Setup::default()
    })
    .await;
    harness
        .sender
        .send(message("hello", "1"))
        .await
        .expect("queued");

    harness
        .wait_for("more than one post", |harness| harness.attempts() >= 2)
        .await;
    harness
        .wait_for_queue("the batch to be given up on", |stats| stats.failed == 1)
        .await;

    let stats = harness.store.queue_stats().await.expect("stats");
    assert_eq!(stats.pending, 0);
    assert_eq!(
        stats.in_flight, 0,
        "the hand-over is closed with its row: {stats:?}"
    );
}

#[tokio::test]
async fn a_second_post_waits_rather_than_going_straight_back() {
    // A hand-over meka refused used to be offered again on the very next pass. For the failure the
    // backoff exists for, meka out of reach, that spends every attempt inside the same outage and
    // the message is declared undeliverable seconds after it arrived.
    let harness = Harness::start_failing(FailureKind::Transient, Setup {
        hand_over_within: Duration::from_secs(30),
        ..Setup::default()
    })
    .await;
    harness
        .sender
        .send(message("hello", "1"))
        .await
        .expect("queued");

    harness
        .wait_for("a third post", |harness| harness.attempts() >= 3)
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
        "the second post came after only {first:?}, so nothing waited"
    );
    let second = times[2].duration_since(times[1]);
    assert!(
        second >= Duration::from_millis(450),
        "the second wait was {second:?}, so it did not double"
    );
}

#[tokio::test]
async fn an_item_taken_back_before_the_model_read_it_is_offered_again() {
    // Cancelling a turn the inbox opened withdraws the items it opened on, the way a cancelled
    // client turn loses its prompt. Nothing was read, so the messages go back into the queue and
    // are handed over afresh rather than counted as delivered.
    let harness = Harness::start_script(TurnScript::Withdraw).await;
    harness
        .sender
        .send(message("are you there?", "1"))
        .await
        .expect("queued");

    harness
        .wait_for("the second hand-over", |harness| harness.items().len() >= 2)
        .await;
    assert_eq!(
        harness.store.queue_stats().await.expect("stats").done,
        0,
        "an item nobody read must not be counted as delivered"
    );
    // A new hand-over each time, since meka refuses the same key with other words and the envelope
    // is rebuilt from whatever is waiting now.
    let keys = harness.keys();
    assert_ne!(keys[0], keys[1], "got {keys:?}");

    // And once the offers run out the message is owed to the agent rather than gone.
    harness
        .wait_for_queue("the offers to run out", |stats| stats.failed == 1)
        .await;
    let summary = harness
        .store
        .unseen_summary(Some("mock:1"))
        .await
        .expect("summary");
    assert_eq!(summary.count, 1, "the message must survive the withdrawal");
}

#[tokio::test]
async fn a_turn_cancelled_after_the_model_read_a_batch_does_not_offer_it_again() {
    // The other half, and the reason the rule is not simply "cancelled means offer it again". The
    // model read the messages before the turn was stopped, so they are accounted for: a second run
    // would repeat whatever the agent did with them, and it would have no memory of the first.
    let harness = Harness::start_script(TurnScript::CancelAfterReading).await;
    harness
        .sender
        .send(message("do the thing", "1"))
        .await
        .expect("queued");

    harness
        .wait_for_queue("the batch to be accounted for", |stats| stats.done == 1)
        .await;
    tokio::time::sleep(Duration::from_millis(400)).await;

    assert_eq!(
        harness.attempts(),
        1,
        "a cancellation after the model had read the batch must not hand it over again"
    );
    let stats = harness.store.queue_stats().await.expect("stats");
    assert_eq!(stats.pending, 0, "{stats:?}");
    assert_eq!(stats.failed, 0);
}

#[tokio::test]
async fn an_unrepairable_refusal_is_not_posted_again_at_all() {
    // A conversation that no longer fits the model's window will not fit on the next attempt
    // either, which is why meka gave it a type away from `provider`. Spending the hour on it only
    // delays the notice that says an operator is needed.
    let harness = Harness::start_failing(FailureKind::Unrepairable, Setup::default()).await;
    harness
        .sender
        .send(message("hello", "1"))
        .await
        .expect("queued");

    harness
        .wait_for_queue("the batch to be given up on", |stats| stats.failed == 1)
        .await;
    // Longer than several of the harness's backoffs, so a second post would have happened by now.
    tokio::time::sleep(Duration::from_millis(1200)).await;

    assert_eq!(
        harness.attempts(),
        1,
        "an error needing an operator must not be posted for the whole hour first"
    );
    assert_eq!(harness.store.queue_stats().await.expect("stats").pending, 0);
}

#[tokio::test]
async fn a_turn_that_failed_after_the_agent_acted_does_not_offer_the_batch_again() {
    // meka only retries an upstream failure while nothing has reached its frontend, so one that
    // gets as far as a `turn.failed` on the feed may well have a sent message and a shell command
    // behind it. Offering the batch again would repeat both, and the agent would have no memory of
    // the first run to tell it from the second.
    let harness = Harness::start_script(TurnScript::FailAfterSending).await;
    harness
        .sender
        .send(message("hello", "1"))
        .await
        .expect("queued");

    harness
        .wait_for_queue("the batch to be accounted for", |stats| stats.done == 1)
        .await;
    tokio::time::sleep(Duration::from_millis(800)).await;

    assert_eq!(
        harness.attempts(),
        1,
        "the turn already had side effects, so the batch must not go round again"
    );
    let stats = harness.store.queue_stats().await.expect("stats");
    assert_eq!(stats.failed, 0);
    assert_eq!(stats.pending, 0);
}

#[tokio::test]
async fn a_message_meka_gave_up_on_is_owed_to_the_agent_again() {
    // A message is marked seen the moment it is queued, on the assumption that the agent is about
    // to be handed it. meka giving up on the item is exactly where that assumption fails, and
    // without putting it back the message is neither delivered nor owed: absent from `unseen`,
    // from the missed-context lookback, and from the `mekabridge unseen` predicate.
    let harness = Harness::start_script(TurnScript::GiveUp).await;
    harness
        .sender
        .send(message("are you there?", "1"))
        .await
        .expect("queued");

    harness
        .wait_for_queue("the item to be given up on", |stats| stats.failed == 1)
        .await;

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
    let harness = Harness::start_script(TurnScript::GiveUp).await;
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
    for leak in ["429", "500", "internal", "provider", "meka", "inbox"] {
        assert!(
            !text.to_ascii_lowercase().contains(leak),
            "the chat was told {leak:?}, which is the owner's business: {text:?}"
        );
    }
}

#[tokio::test]
async fn the_owner_hears_what_the_chat_is_spared() {
    let harness = Harness::start_script_with(TurnScript::GiveUp, Setup::with_owner()).await;
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
        owner.1.contains("7 attempt(s)"),
        "the owner needs meka's own reason: {:?}",
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
    let harness = Harness::start_script_with(TurnScript::GiveUp, Setup {
        history_retention: Duration::ZERO,
        ..Setup::with_owner()
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
    let harness = Harness::start_script_with(TurnScript::GiveUp, Setup {
        notify_failures: false,
        ..Setup::with_owner()
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
    let harness =
        Harness::start_script_with(TurnScript::FailAfterSending, Setup::with_owner()).await;
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
    // affected chat every time a batch was given up on, which is worse than the silence it
    // replaced.
    let harness = Harness::start_script(TurnScript::GiveUp).await;
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
        .wait_for_queue("the second message to be given up on too", |stats| {
            stats.failed >= 2
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
async fn a_rate_limited_post_is_tried_again() {
    // The failure the backoff exists for, and the one a bridge reading this bucket as permanent
    // gives up on within seconds of the first refusal.
    let harness = Harness::start_failing(FailureKind::RateLimited, Setup {
        hand_over_within: Duration::from_secs(30),
        ..Setup::default()
    })
    .await;
    harness
        .sender
        .send(message("hello", "1"))
        .await
        .expect("queued");

    // `wait_for` panics on its deadline, so reaching a second post is the assertion. Asserting the
    // count afterwards would only restate what getting here already proved.
    harness
        .wait_for(
            "a transient upstream failure to be posted again rather than given up on",
            |harness| harness.attempts() >= 2,
        )
        .await;
}

#[tokio::test]
async fn a_transient_refusal_is_recovered_by_the_next_post() {
    // The same batch, under the same key and byte for byte, which is what lets meka answer a post
    // it has already seen with the item it already made rather than a second copy.
    let harness = Harness::start_flaky(1).await;
    harness
        .sender
        .send(message("hello", "1"))
        .await
        .expect("queued");

    harness
        .wait_for_queue("the batch to be delivered", |stats| stats.done == 1)
        .await;
    assert_eq!(harness.attempts(), 2, "one refusal and one that landed");
    assert_eq!(harness.store.queue_stats().await.expect("stats").failed, 0);
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
        .wait_for("a turn", |harness| !harness.items().is_empty())
        .await;
    tokio::time::sleep(Duration::from_millis(400)).await;

    assert_eq!(
        harness.items().len(),
        5,
        "every message is an item of its own, delivered exactly once"
    );
    let turns = harness.turns();
    assert_eq!(
        turns.len(),
        1,
        "five messages sent together belong in one turn, got {turns:#?}"
    );
    assert_eq!(turns[0].len(), 5, "and that turn reads all five");
}

/// Whether one turn read an item carrying each of `texts`.
fn read_all(read: &[String], texts: &[&str]) -> bool {
    texts
        .iter()
        .all(|text| read.iter().any(|item| item.contains(text)))
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
    let harness = Harness::start().await;
    harness.channel.report_typing();
    harness
        .sender
        .send(message("hey", "1"))
        .await
        .expect("queued");
    harness.sender.send(typing()).await.expect("queued");

    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        harness.items().is_empty(),
        "somebody is still typing, so nothing should have been claimed: {:?}",
        harness.items()
    );

    harness
        .sender
        .send(message("can you check the deploy logs", "2"))
        .await
        .expect("queued");
    harness
        .wait_for("the turn", |harness| {
            harness.turns().first().is_some_and(|read| read.len() == 2)
        })
        .await;
    let turns = harness.turns();
    assert!(
        read_all(&turns[0], &["hey", "deploy logs"]),
        "both messages belong in one turn:\n{:#?}",
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
    let harness = Harness::start().await;
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
        .wait_for("the turn", |harness| !harness.items().is_empty())
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
    let harness = Harness::start().await;
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
                .items()
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
    let harness = Harness::start().await;
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
    let turns = harness.items();
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
        .wait_for("the turn", |harness| !harness.items().is_empty())
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
        .wait_for("the turn", |harness| !harness.items().is_empty())
        .await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let turns = harness.turns();
    assert_eq!(turns.len(), 1, "one post is one turn, got {turns:#?}");
    assert_eq!(
        turns[0]
            .iter()
            .filter(|item| item.contains("album: album-1"))
            .count(),
        3,
        "every part of the post has to be in it:\n{:#?}",
        turns[0]
    );
}

#[tokio::test]
async fn one_chat_mid_burst_does_not_hold_up_another() {
    // Readiness used to be one window over the whole queue, so any conversation still receiving
    // deferred delivery for every other conversation. With a quiet period long enough to be useful
    // that would mean a busy room stalling a direct message.
    let harness = Harness::start().await;
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
                .items()
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
    let harness = Harness::start().await;
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
        .wait_for("a turn", |harness| !harness.items().is_empty())
        .await;
    tokio::time::sleep(Duration::from_millis(400)).await;

    let turns = harness.turns();
    assert_eq!(turns.len(), 1, "got {} turns: {:#?}", turns.len(), turns);
    assert!(
        read_all(&turns[0], &["hey", "actually the staging ones"]),
        "the whole thought must arrive together, got:\n{:#?}",
        turns[0]
    );
}

#[tokio::test]
async fn a_chat_that_never_goes_quiet_is_released_by_the_ceiling() {
    // A steady stream keeps resetting the quiet period, so without a ceiling the batch would be
    // deferred indefinitely and the agent would never hear anything at all.
    let harness = Harness::start().await;
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
            harness.turns().iter().any(|read| read.len() > 1)
        })
        .await;
    chatter.abort();

    let turns = harness.turns();
    assert!(
        turns[0].len() > 1,
        "the ceiling should still release what has piled up rather than one message, got:\n{:#?}",
        turns[0]
    );
}

#[tokio::test]
async fn a_hand_over_posted_before_a_restart_is_settled_by_the_replay() {
    // The whole reason the key and the body are written down. A bridge that stops between posting
    // an item and hearing what became of it has no way to tell, on the next start, whether the
    // agent read it: the feed events it missed are in meka's ring, and the item is answerable by
    // its key. So it posts the same bytes under the same key, meka answers with the item it
    // already made and that item's state, and the batch is settled without a second copy reaching
    // anybody.
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("state.db");
    let recorder = Arc::new(MekaRecorder::default());
    // Held on meka's side, so the first run stops with the item still waiting to be read.
    recorder.set(&recorder.script, TurnScript::Hold);
    let (meka_address, meka_shutdown) = start_meka(Arc::clone(&recorder)).await;
    let config = Arc::new(config_for(meka_address, &database, false));

    let first = run_bridge(&config).await;
    let payload = serde_json::to_string(&message("do not lose me", "1")).expect("encodes");
    first
        .store
        .upsert_conversation(direct_conversation("mock:1"))
        .await
        .expect("conversation");
    let account = mock_account(&first.store).await;
    first
        .store
        .enqueue(
            MessageKey {
                conversation: "mock:1",
                account,
                message_id: "1",
                revision: 0,
            },
            &payload,
            Utc::now(),
            64,
        )
        .await
        .expect("enqueued");
    first.wake.notify_one();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while recorder
        .items
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .is_empty()
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the first run never handed the batch over"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // Stopped with the item posted and its fate unknown, which is the state a crash leaves.
    first.shutdown.cancel();
    drop(first);
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Meanwhile the model reads it, so the second run is asking about something already settled.
    let item = recorder
        .by_key
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .values()
        .next()
        .cloned()
        .expect("one item");
    recorder.mark(&item, "delivered");

    let second = run_bridge(&config).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let stats = second.store.queue_stats().await.expect("stats");
        if stats.done == 1 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the hand-over was never settled after the restart; stats {stats:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let items = recorder
        .items
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    assert_eq!(
        items.len(),
        1,
        "the restart posted a second copy of the message: {items:?}"
    );
    second.shutdown.cancel();
    meka_shutdown.cancel();
}

/// One run of the daemon's own wiring over an existing database, for the restart tests.
struct Running {
    store: Store,
    wake: Arc<Notify>,
    shutdown: CancellationToken,
}

impl Drop for Running {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

async fn run_bridge(config: &Arc<Config>) -> Running {
    let store = Store::open(&config.storage.path).await.expect("opens");
    let accounts = mock_accounts(&store).await;
    let channels = Arc::new(ChannelRegistry::from_channels([
        Arc::new(MockChannel::new("mock")) as Arc<dyn Channel>,
    ]));
    let meka = MekaClient::new(&config.meka).expect("client");
    let shutdown = CancellationToken::new();
    let wake = Arc::new(Notify::new());
    spawn_session(
        store.clone(),
        Arc::clone(config),
        meka,
        channels,
        accounts,
        Arc::new(inbound::TypingState::default()),
        Arc::new(Presence::default()),
        Arc::clone(&wake),
        shutdown.clone(),
    );
    Running {
        store,
        wake,
        shutdown,
    }
}

#[tokio::test]
async fn the_sink_delivers_to_the_channel_and_records_the_send() {
    let directory = tempfile::tempdir().expect("tempdir");
    let store = Store::open(&directory.path().join("state.db"))
        .await
        .expect("opens");
    store
        .upsert_conversation(direct_conversation("mock:1"))
        .await
        .expect("conversation");

    let channel = Arc::new(MockChannel::new("mock"));
    let channels = Arc::new(ChannelRegistry::from_channels([
        Arc::clone(&channel) as Arc<dyn Channel>
    ]));
    let sink = sink_for(store.clone(), channels).await;

    let ids = sink
        .send_text("mock:1", "**hello**", SendOptions::default(), None)
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
fn mention(text: &str, message_id: &str) -> InboundEvent {
    let mut event = message(text, message_id);
    let InboundEvent::Message(inner) = &mut event else {
        panic!("a message was built just above");
    };
    inner.addressed = true;
    event
}

/// The same message, in a second direct chat.
fn message_elsewhere(text: &str, message_id: &str) -> InboundEvent {
    let mut event = message(text, message_id);
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
fn group_message(text: &str, message_id: &str, addressed: bool) -> InboundEvent {
    let mut event = message(text, message_id);
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
    let harness = Harness::start().await;
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
        harness.items().is_empty(),
        "a group defaults to mentions only, so ordinary talk must not cost a turn: {:?}",
        harness.items()
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
            !harness.items().is_empty()
        })
        .await;
    let envelope = harness.items()[0].clone();
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
    let harness = Harness::start().await;
    harness
        .sender
        .send(message("just checking in", "1"))
        .await
        .expect("queued");
    harness
        .wait_for("the turn", |harness| !harness.items().is_empty())
        .await;
    assert!(harness.items()[0].contains("just checking in"));
}

#[tokio::test]
async fn a_blocked_conversation_never_reaches_the_agent_and_keeps_nothing() {
    let harness = Harness::start().await;
    harness
        .store
        .set_policy(
            "mock:1",
            "telegram",
            Policy::Block,
            None,
            Some("too noisy"),
            Utc::now(),
        )
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
        harness.items().is_empty(),
        "a blocked chat must not wake the agent: {:?}",
        harness.items()
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
async fn the_indicator_is_up_only_while_the_model_writes_the_message() {
    // The whole point of the rework. The indicator used to be raised on `turn.started` and held for
    // the life of the turn, so a chat saw "typing" through a dozen tool calls and however long the
    // agent spent reading. It now tracks the one interval meka can actually vouch for: between
    // `tool_call.composing` and `tool_call.executing` on a send call.
    let harness =
        Harness::start_composing("mcp__mekabridge__send_message", Duration::from_millis(900)).await;
    harness
        .sender
        .send(message("what did the log say?", "1"))
        .await
        .expect("queued");

    // The turn is under way and the model is talking, but not yet writing a message.
    harness
        .wait_for("the turn to start", |harness| harness.attempts() > 0)
        .await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(
        harness.channel.activity_count(),
        0,
        "the indicator went up before the model had started writing anything"
    );

    harness
        .wait_for("the indicator during composition", |harness| {
            harness.channel.activity_count() > 0
        })
        .await;

    // And it stops when the arguments are finished, rather than running on to the end of the turn.
    // The stub holds the stream open for another window after `tool_call.executing` before sending
    // `turn.finished`, and the harness renews every 100ms, so an indicator still up here would tick
    // several times inside the sample. Waiting on the chunk count rather than on `turns()`, which
    // the stub pushes at the top of the handler and is therefore true from t=0.
    harness
        .wait_for("the arguments to be written", |harness| {
            *harness
                .recorder
                .phase
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                >= 2
        })
        .await;
    let settled = harness.channel.activity_count();
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(
        harness.channel.activity_count(),
        settled,
        "the indicator kept refreshing after the message was written"
    );
}

#[tokio::test]
async fn a_turn_that_writes_no_message_shows_nothing() {
    // A question the agent answers by doing nothing, or a mention in a group it decides not to
    // join. Under the old rule this still drew a typing indicator for as long as the turn ran,
    // which promised a reply that was never coming.
    // A turn with real duration that calls a tool which is not a send. Duration matters: the stub
    // otherwise answers in microseconds, and the old raise-on-`turn.started` behaviour would have
    // had no time to draw anything either, so the test would pass against the code it rules out.
    let harness = Harness::start_composing("read_file", Duration::from_millis(700)).await;
    harness
        .sender
        .send(message("just so you know", "1"))
        .await
        .expect("queued");

    // Waiting for the whole stream, not for the handler being entered. `turns()` is pushed at the
    // top of the handler, so the original `wait_for` returned at t=0 and the assertion ran before
    // the stub had reached its `tool_call.composing` chunk at all: the test passed against a build
    // with the send-tool guard deleted, which is the one regression it exists to catch.
    harness
        .wait_for("the whole turn to run", |harness| {
            *harness
                .recorder
                .phase
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                >= 3
        })
        .await;
    tokio::time::sleep(Duration::from_millis(400)).await;

    assert_eq!(
        harness.channel.activity_count(),
        0,
        "a turn that never wrote a message must not claim it was typing one"
    );
}

#[tokio::test]
async fn a_retracted_message_is_marked_rather_than_erased() {
    // Only a platform that reports deletions can do this. The row stays: a message the agent was
    // woken for is already in its session for good, so deleting the record would take away the one
    // thing able to tell it later that what it acted on had been withdrawn. What must not happen is
    // the retraction passing unrecorded, leaving the old text looking current.
    let harness = Harness::start().await;
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

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let history = harness
            .store
            .history("mock:1", 10, None)
            .await
            .expect("read");
        assert_eq!(history.len(), 1, "the row must not be removed");
        let record = history.first().expect("one row");
        if record.deleted_at.is_some() {
            assert_eq!(
                record.text, "said too much",
                "the text is kept, so the agent can see what it was"
            );
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for the retraction to be recorded"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
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
async fn every_envelope_is_posted_as_a_steer_from_this_bridge_under_a_key() {
    // The whole contract with the inbox in one assertion. `steer` is what reaches a turn that is
    // already running, at its next round boundary, rather than waiting for it to end; `source` is
    // what meka writes into the header above the envelope, so the agent reads one name for this
    // bridge; and the key is what makes a post safe to repeat after an ambiguous failure or a
    // restart on either side.
    let harness = Harness::start().await;
    harness
        .sender
        .send(message("are you there?", "1"))
        .await
        .expect("queued");
    harness
        .wait_for("the hand-over", |harness| !harness.items().is_empty())
        .await;

    let posted = harness
        .recorder
        .classes
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    assert_eq!(posted, vec![(
        "steer".to_string(),
        "mekabridge".to_string()
    )]);
    let key = harness.keys().first().cloned().expect("one key");
    assert!(!key.is_empty(), "every post carries a key");
    assert!(
        key.is_ascii() && key.len() <= 255,
        "meka refuses a key outside that: {key:?}"
    );
}

#[tokio::test]
async fn a_backlog_survives_a_batch_the_model_never_read() {
    // A conversation's backlog is announced once, in the envelope, and it used to be spent as soon
    // as meka took the turn. An item taken back before the model read it leaves nobody having been
    // told, so spending it there loses those messages from every path that would have found them
    // again.
    let harness = Harness::start_script_with(TurnScript::Withdraw, Setup {
        max_offers: 3,
        ..Setup::default()
    })
    .await;
    harness
        .store
        .set_policy("mock:1", "telegram", Policy::Mute, None, None, Utc::now())
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
        harness.items().is_empty(),
        "a muted chat costs no hand-over"
    );

    harness
        .sender
        .send(mention("@bot anything from the others?", "9"))
        .await
        .expect("queued");
    harness
        .wait_for("the first hand-over", |harness| !harness.items().is_empty())
        .await;
    // Taken back, so the messages go round again. The envelope built for the second attempt has to
    // make the statement the first one's never reached anybody.
    harness
        .recorder
        .set(&harness.recorder.script, TurnScript::Answer);

    harness
        .wait_for_queue("the batch to be delivered", |stats| stats.done >= 1)
        .await;
    let items = harness.items();
    let envelope = items.last().expect("the delivered envelope");
    assert!(
        envelope.contains("3 messages you have not seen"),
        "the withdrawn envelope took the only statement of the backlog with it, so the next one \
         has to make it again:\n{envelope}"
    );
    assert!(
        envelope.contains("chatter 2"),
        "and the lookback with it:\n{envelope}"
    );
}

#[tokio::test]
async fn a_muted_conversation_records_everything_and_wakes_only_on_a_mention() {
    let harness = Harness::start().await;
    harness
        .store
        .set_policy("mock:1", "telegram", Policy::Mute, None, None, Utc::now())
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
        harness.items().is_empty(),
        "ordinary talk in a muted chat must not cost a turn: {:?}",
        harness.items()
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
            !harness.items().is_empty()
        })
        .await;

    let turns = harness.items();
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
    let harness = Harness::start().await;
    // What `unmute(duration)` leaves behind once the window has closed.
    harness
        .store
        .set_policy(
            "mock:-100",
            "telegram",
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
            !harness.items().is_empty()
        })
        .await;
    let envelope = &harness.items()[0];
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
        harness.items().len(),
        1,
        "only the message that found the expiry is owed a turn: {:?}",
        harness.items()
    );
}

#[tokio::test]
async fn a_muted_conversation_stays_quiet_even_moments_after_the_agent_speaks() {
    // There was a window here: for five minutes after the agent's own message, everything said in
    // the room woke it. In a busy chat that delivered the room, in envelopes indistinguishable
    // from a message addressed to the agent, and each reply it was nudged into making pushed the
    // window out again. Mention-only now means exactly that. Following a conversation on is
    // something the agent asks for rather than something it is handed.
    let harness = Harness::start().await;
    harness
        .store
        .set_policy("mock:1", "telegram", Policy::Mute, None, None, Utc::now())
        .await
        .expect("mute");

    // What `note_sent` does after the agent's own message lands, so this is the most favourable
    // possible moment for the old window: it opened a heartbeat ago.
    harness
        .store
        .touch_outbound("mock:1", "telegram", Utc::now())
        .await
        .expect("outbound");

    harness
        .sender
        .send(message("no, I meant the other one", "1"))
        .await
        .expect("queued");
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(
        harness.items().is_empty(),
        "speaking in a muted chat must not open it up again: {:?}",
        harness.items()
    );

    // Withheld, not discarded. The distinction is the whole reason this is tolerable: the agent
    // was not interrupted, and the message is still there for it to find.
    let (owed, ..) = harness
        .store
        .take_unseen("mock:1", Utc::now(), 10)
        .await
        .expect("read");
    assert_eq!(owed, 1, "the message has to survive for read_history");
}

#[tokio::test]
async fn a_batch_that_never_reached_the_model_owes_its_backlog_back() {
    // The backlog and the count of what the queue shed are each stated once, in the envelope, and
    // both used to be spent the moment it was rendered. A batch meka never accepted was read by
    // nobody, so both are owed again: otherwise the next envelope tells the agent "nothing has
    // been said there since you last looked" about a chat with four messages waiting, and they are
    // then gone from `unseen` and from every future lookback with nothing to point at them.
    let harness = Harness::start_failing(FailureKind::Transient, Setup {
        hand_over_within: Duration::from_millis(600),
        ..Setup::default()
    })
    .await;
    harness.store.note_dropped(3).await.expect("shed three");
    harness
        .store
        .set_policy("mock:1", "telegram", Policy::Mute, None, None, Utc::now())
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
    assert!(harness.items().is_empty(), "a mute withholds all four");

    harness
        .sender
        .send(mention("@bot what do you make of that?", "9"))
        .await
        .expect("queued");
    harness
        .wait_for_queue("the batch to be given up on", |stats| stats.failed == 1)
        .await;

    assert_eq!(
        harness.store.peek_dropped().await.expect("peek"),
        3,
        "the overflow notice died with the envelope nobody read"
    );
    let summary = harness
        .store
        .unseen_summary(Some("mock:1"))
        .await
        .expect("summary");
    assert_eq!(
        summary.count, 5,
        "the backlog was spent by a hand-over meka never accepted"
    );
}

#[tokio::test]
async fn unmuting_reports_the_backlog_once_and_then_stops() {
    // The trap: `unseen` is only ever cleared by the turn that reports it, so a conversation that
    // stops being muted with a backlog behind it would keep that count for the rest of its life and
    // `list_conversations` would go on quoting it.
    let harness = Harness::start().await;
    harness
        .store
        .set_policy("mock:1", "telegram", Policy::Mute, None, None, Utc::now())
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
    assert!(harness.items().is_empty());

    harness
        .store
        .set_policy("mock:1", "telegram", Policy::Active, None, None, Utc::now())
        .await
        .expect("unmute");
    harness
        .sender
        .send(message("now listening again", "9"))
        .await
        .expect("queued");
    harness
        .wait_for("the first turn after unmuting", |harness| {
            !harness.items().is_empty()
        })
        .await;

    let envelope = harness.items()[0].clone();
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
        .wait_for("the second turn", |harness| harness.items().len() > 1)
        .await;
    assert!(
        !harness.items()[1].contains("were recorded"),
        "the backlog must be cleared by the turn that reported it: {:?}",
        harness.items()[1]
    );
}

#[tokio::test]
async fn an_expired_block_lets_the_next_message_through_and_reports_the_damage() {
    let harness = Harness::start().await;
    harness
        .store
        .set_policy("mock:1", "telegram", Policy::Block, None, None, Utc::now())
        .await
        .expect("block");
    harness
        .sender
        .send(message("while blocked", "1"))
        .await
        .expect("queued");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(harness.items().is_empty());

    // Backdate the expiry rather than sleeping through a real one. Re-ruling resets the tally, so
    // the drop is counted afterwards.
    harness
        .store
        .set_policy(
            "mock:1",
            "telegram",
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
            !harness.items().is_empty()
        })
        .await;

    let turns = harness.items();
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
    let sink = sink_for(store.clone(), channels).await;

    sink.send_text("mock:999", "hello", SendOptions::default(), None)
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
    assert_eq!(record.channel, "mock");
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
    let sink = sink_for(store.clone(), channels).await;

    sink.send_text("mock:7", "first contact", SendOptions::default(), None)
        .await
        .expect("sends");
    store
        .upsert_conversation(ConversationRecord {
            address: "mock:7".to_string(),
            channel: "mock".to_string(),
            platform: "telegram".to_string(),
            title: Some("Deploy Crew".to_string()),
            kind: "group".to_string(),
            created_at: Utc::now(),
            last_inbound_at: Some(Utc::now()),
            last_outbound_at: None,
        })
        .await
        .expect("inbound");

    // A second send must not drag the now-known title and kind back to their placeholders.
    sink.send_text("mock:7", "second", SendOptions::default(), None)
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
    let sink = sink_for(store, channels).await;

    // Unrestricted means "any chat", not "any string". These two fail here because no channel could
    // act on them at all, which is different from a chat the platform will reject.
    let malformed = sink
        .send_text("not-an-id", "hello", SendOptions::default(), None)
        .await
        .expect_err("a malformed id must be refused");
    assert!(malformed.to_string().contains("not-an-id"), "{malformed}");

    let unconfigured = sink
        .send_text("discord:1", "hello", SendOptions::default(), None)
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
        .upsert_conversation(direct_conversation("mock:1"))
        .await
        .expect("conversation");
    let channels = Arc::new(ChannelRegistry::from_channels([
        Arc::new(MockChannel::new("mock")) as Arc<dyn Channel>,
    ]));
    let sink = sink_for(store, channels).await;

    let listed = sink.conversations(None, 10).await.expect("lists");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, "mock:1");
    assert_eq!(listed[0].title.as_deref(), Some("Alice"));
}

#[tokio::test]
async fn every_feed_event_names_the_session_it_was_read_from() {
    // Event ids are numbered per session, and the channel between the reader and the session task
    // is thousands deep, so a rebind leaves the old session's feed still draining into a task that
    // has moved on. Without the name there is nothing to tell those events from the new session's,
    // and one of their ids reaching the feed position would reopen the new feed from a number that
    // means nothing in its numbering -- meka would then replay nothing until the new session's own
    // ids overtook it.
    let recorder = Arc::new(MekaRecorder::default());
    let (address, shutdown) = start_meka(Arc::clone(&recorder)).await;
    let meka = MekaClient::new(
        &config_for(
            address,
            &std::path::PathBuf::from("/tmp/mekabridge-unused.db"),
            false,
        )
        .meka,
    )
    .expect("client");

    let first = uuid::Uuid::new_v4();
    let second = uuid::Uuid::new_v4();
    let (session, session_receiver) = watch::channel(Some(first));
    let (feed_sender, mut feed) = mpsc::channel(64);
    let reader = CancellationToken::new();
    tokio::spawn({
        let meka = meka.clone();
        let reader = reader.clone();
        async move {
            feed_reader(
                meka,
                session_receiver,
                Arc::new(AtomicU64::new(0)),
                feed_sender,
                reader,
            )
            .await;
        }
    });

    let named = async |feed: &mut mpsc::Receiver<mekabridge::bridge::feed::FeedEvent>| {
        match tokio::time::timeout(Duration::from_secs(5), feed.recv()).await {
            Ok(Some(mekabridge::bridge::feed::FeedEvent::Connected { session, .. }))
            | Ok(Some(mekabridge::bridge::feed::FeedEvent::Item { session, .. })) => Some(session),
            other => panic!("expected a named feed event, got {other:?}"),
        }
    };

    assert_eq!(named(&mut feed).await, Some(first), "the open names it");
    recorder
        .feed
        .push("turn.started", Some("t1"), serde_json::json!({}));
    assert_eq!(
        named(&mut feed).await,
        Some(first),
        "and so does each event"
    );

    // The rebind. The reader drops the connection it holds and opens the replacement's.
    session.send_replace(Some(second));
    recorder
        .feed
        .push("turn.finished", Some("t1"), serde_json::json!({}));
    let mut seen = Vec::new();
    for _ in 0..2 {
        seen.push(named(&mut feed).await);
    }
    assert!(
        seen.iter().all(|session| *session == Some(second)),
        "an event read from the replacement's feed was credited to the session it replaced: \
         {seen:?}"
    );

    reader.cancel();
    shutdown.cancel();
}

#[tokio::test]
async fn a_forgotten_session_is_replaced_and_the_batch_handed_to_the_new_one() {
    // meka losing the session (its row deleted, or its database replaced) must not lose the
    // message: a replacement is bound and the same batch is posted into it, under the same key,
    // since meka scopes a key per session and the new one has never seen it.
    let harness = Harness::start_forgetting(1).await;
    harness
        .sender
        .send(message("still here?", "1"))
        .await
        .expect("queued");

    harness
        .wait_for_queue("the batch to be delivered", |stats| stats.done == 1)
        .await;

    // One item, however many requests it took. A rebind reopens the feed, and a connection that
    // opens after a hand-over went out asks about it, since its outcome may have been reported
    // while nothing was listening; meka answers that with the item it already has rather than a
    // second copy, which is the whole reason the key is on the row.
    let items = harness.items();
    assert_eq!(items.len(), 1, "the replacement was sent a second copy");
    assert!(items[0].contains("still here?"), "got:\n{}", items[0]);
    assert!(
        harness.attempts() >= 2,
        "the refusal and the post into the replacement"
    );
    assert_eq!(harness.store.queue_stats().await.expect("stats").failed, 0);
}

#[tokio::test]
async fn a_video_preview_is_not_refused_on_the_size_of_the_video() {
    // The still frame is what "show me" resolves to for a video, and it is tens of kilobytes. The
    // pre-fetch size check reads `record.bytes`, which is the size of the *main* file, so checking
    // one against the other refused every video over the ceiling on the strength of a number
    // describing something else: "what is in this clip?" answered "too large to show inline" while
    // the frame that would have answered it sat one fetch away.
    let recorder = Arc::new(MekaRecorder::default());
    let (address, shutdown) = start_meka(Arc::clone(&recorder)).await;
    let directory = tempfile::tempdir().expect("tempdir");
    let store = Store::open(&directory.path().join("state.db"))
        .await
        .expect("store");
    store
        .upsert_conversation(direct_conversation("mock:1"))
        .await
        .expect("conversation");

    let channel = Arc::new(MockChannel::new("mock"));
    channel.put_file("AgACthumb", ONE_PIXEL_PNG.to_vec());
    let channels = Arc::new(ChannelRegistry::from_channels([
        Arc::clone(&channel) as Arc<dyn Channel>
    ]));

    let account = mock_account(&store).await;
    let handle = store
        .register_attachment(mekabridge::store::AttachmentRecord {
            conversation: "mock:1".to_string(),
            account,
            message_id: "1".to_string(),
            revision: 0,
            position: 0,
            kind: "video".to_string(),
            file_ref: "AgACvideo".to_string(),
            thumb_ref: Some("AgACthumb".to_string()),
            file_name: Some("clip.mp4".to_string()),
            media_type: Some("video/mp4".to_string()),
            // Well past `MAX_VIEW_BYTES`, which is the whole point: the thumbnail is not.
            bytes: Some(12 * 1024 * 1024),
            path: None,
            created_at: Utc::now(),
        })
        .await
        .expect("registers");

    let sink = sink_against_meka(
        store.clone(),
        channels,
        directory.path().to_path_buf(),
        Arc::new(Presence::default()),
        address,
    )
    .await;
    let viewed = sink.view_attachment(&handle).await.expect("resolves");
    shutdown.cancel();

    match viewed {
        // The note is what says this is a frame rather than the file, which is the whole reason a
        // preview is allowed to stand in for something that cannot be shown.
        ViewedAttachment::Image { note, .. } => {
            assert!(note.is_some(), "a still frame must arrive with its caveat");
        }
        other => panic!("the preview frame was refused on the video's size: {other:?}"),
    }
}

#[tokio::test]
async fn meka_being_unreachable_is_posted_again_rather_than_given_up_on() {
    // The case the whole backoff exists for. Every other failure test scripts an HTTP-level
    // Problem Detail, so which *errors* reach the give-up path was never checked -- and that path
    // marks every row failed: unseen, the chat apologised to, the owner told a message could not
    // be delivered. Classifying a transport error as permanent therefore declares every queued
    // message undeliverable within a second of meka restarting.
    let harness = Harness::start_with_setup(Setup {
        hand_over_within: Duration::from_secs(30),
        ..Setup::default()
    })
    .await;
    // Stopped once the session is bound, which is where a restart in the middle of a working day
    // leaves things. A meka that was never reachable is a different case: nothing is bound, so
    // nothing is claimed and there is no hand-over to charge.
    harness
        .wait_until("the session to be bound", async |store| {
            store.session_id().await.is_ok_and(|id| id.is_some())
        })
        .await;
    // Stop the stub so the connection itself is refused, which is what a restarting meka looks
    // like from here. Nothing is scripted to fail: the failure is the socket.
    harness.meka_shutdown.cancel();
    tokio::time::sleep(Duration::from_millis(150)).await;

    harness
        .sender
        .send(message("are you there?", "1"))
        .await
        .expect("queued");

    // Waiting for evidence that a post was actually made and charged, rather than for a fixed
    // interval: on a loaded machine a bare sleep can end before the session task has tried
    // anything, and the assertion below would then hold for the wrong reason.
    harness
        .wait_until(
            "a post to be charged against the hand-over",
            async |store| {
                store
                    .rows_in(QueueState::InFlight)
                    .await
                    .is_ok_and(|rows| rows.iter().any(|row| row.posts > 0))
            },
        )
        .await;

    let stats = harness.store.queue_stats().await.expect("stats");
    assert_eq!(stats.failed, 0, "the batch was written off: {stats:?}");
    assert!(
        harness.channel.sent().is_empty(),
        "a chat was apologised to before the hour was up: {:?}",
        harness.channel.sent()
    );
}

#[tokio::test]
async fn a_frame_this_build_cannot_read_is_skipped_and_the_feed_carries_on() {
    // A frame the parser cannot read decides nothing on its own, and reconnecting would replay it
    // out of meka's ring to fail on it identically. Skipping it costs at most one event; dropping
    // the feed over it costs every outcome after it, which for a session that runs turns nobody
    // here started is everything.
    let harness = Harness::start().await;
    harness.recorder.set(&harness.recorder.garbled_frame, true);

    harness
        .sender
        .send(message("are you there?", "1"))
        .await
        .expect("queued");
    harness
        .wait_for_queue("the outcome after the garbled frame", |stats| {
            stats.done == 1
        })
        .await;

    let attaches = harness
        .recorder
        .attaches
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .len();
    assert_eq!(attaches, 1, "the feed was reconnected over one bad frame");
    assert_eq!(
        *harness
            .recorder
            .cancels
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()),
        0,
        "and nothing was cancelled over it either"
    );
}

#[tokio::test]
async fn a_degraded_readiness_is_read_rather_than_reported_as_a_broken_probe() {
    // meka answers 503 with the same body it sends on 200, naming which subsystem is the blocker,
    // and not as a Problem Detail. Handed to the shared decode path that becomes "unexpected body",
    // retried on the way, so the one response worth reading is the one that could not be read.
    let recorder = Arc::new(MekaRecorder::default());
    *recorder
        .readiness
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some((
        503,
        r#"{"status":"degraded","session_db":false,"profile_configured":true,
            "mcp_servers_healthy":true}"#
            .to_string(),
    ));
    let (address, shutdown) = start_meka(Arc::clone(&recorder)).await;
    let directory = tempfile::tempdir().expect("tempdir");
    let config = config_for(address, &directory.path().join("state.db"), false);
    let meka = mekabridge::meka::MekaClient::new(&config.meka).expect("client");

    let ready = meka
        .ready()
        .await
        .expect("meka's own 503 carries an answer");
    assert_eq!(ready.status, "degraded");
    assert!(
        !ready.session_db,
        "the blocker meka named was lost on the way"
    );
    assert!(ready.profile_configured);

    // A 503 that is *not* meka's own answer is a different thing entirely: something in front of
    // meka, which is worth retrying rather than reporting as a considered verdict.
    *recorder
        .readiness
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) =
        Some((503, "<html>502 Bad Gateway</html>".to_string()));
    meka.ready()
        .await
        .expect_err("a proxy's 503 is not a readiness answer");

    // The one that actually gets through the door: a load balancer answering in JSON. Every field
    // of `ReadyStatus` defaults, so requiring merely a non-empty `status` accepted this and turned
    // it into a confident report that every subsystem was down.
    *recorder
        .readiness
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some((
        503,
        r#"{"status":"Service Unavailable","message":"no healthy upstream"}"#.to_string(),
    ));
    meka.ready()
        .await
        .expect_err("a proxy's JSON 503 was read as meka's own diagnosis");

    shutdown.cancel();
}

#[tokio::test]
async fn an_abandoned_composing_call_still_closes_the_indicator() {
    // meka emits `tool_call.composing` without marking the attempt as having produced output, on
    // purpose: a call whose arguments never finish is still safe to retry. That makes the composing
    // window exactly the window meka retries, and a retry comes back with fresh ids -- so the
    // `executing` matching the id that opened the window never arrives. Waiting for it left the
    // indicator refreshing until the turn ended, which is the behaviour this whole rework removed.
    let harness =
        Harness::start_composing("mcp__mekabridge__send_message", Duration::from_millis(700)).await;
    harness.recorder.set(&harness.recorder.compose_retry, true);
    harness
        .sender
        .send(message("write me something long", "1"))
        .await
        .expect("queued");

    // Up while the send call is being written, which is the part that must keep working.
    harness
        .wait_for("the indicator during composition", |harness| {
            harness.channel.activity_count() > 0
        })
        .await;

    // The replacement call has been announced and has begun running. Nothing is being written to
    // anybody now, so nothing should still be claiming otherwise.
    harness
        .wait_for("the call that replaced it", |harness| {
            *harness
                .recorder
                .phase
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                >= 2
        })
        .await;
    let settled = harness.channel.activity_count();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        harness.channel.activity_count(),
        settled,
        "the indicator went on refreshing for a call the model had abandoned"
    );
}

#[tokio::test]
async fn attachments_are_announced_with_a_handle_and_nothing_is_downloaded() {
    // The core of the deferred model: the envelope tells the agent what arrived and how to reach
    // it, and no bytes move until the agent asks.
    let harness = Harness::start().await;
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
            width: None,
            height: None,
            duration_secs: None,
            file_ref: "AgACphoto".to_string(),
            thumb_ref: None,
            handle: None,
        }];
    }
    harness.sender.send(event).await.expect("queued");

    harness
        .wait_for("the turn", |harness| !harness.items().is_empty())
        .await;

    let turns = harness.items();
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
        .upsert_conversation(direct_conversation("mock:1"))
        .await
        .expect("conversation");

    let channel = Arc::new(MockChannel::new("mock"));
    channel.put_file("AgACphoto", ONE_PIXEL_PNG.to_vec());
    let channels = Arc::new(ChannelRegistry::from_channels([
        Arc::clone(&channel) as Arc<dyn Channel>
    ]));

    let account = mock_account(&store).await;
    let handle = store
        .register_attachment(mekabridge::store::AttachmentRecord {
            conversation: "mock:1".to_string(),
            account,
            message_id: "1".to_string(),
            revision: 0,
            position: 0,
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
    )
    .await;

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
    let sink = sink_for(store, channels).await;

    let error = sink
        .view_attachment("9999")
        .await
        .expect_err("an invented handle must not resolve");
    assert!(error.to_string().contains("9999"), "got: {error}");
}

#[tokio::test]
async fn a_file_send_with_no_files_is_refused_by_the_sink() {
    // The contract every channel is handed. Enforced once here rather than in each connector,
    // because a channel that does not index its input, as Discord's does not, would otherwise send
    // a caption-only message and report a file delivered.
    let store = Store::open_in_memory().await.expect("store");
    let channel = Arc::new(MockChannel::new("mock"));
    let channels = Arc::new(ChannelRegistry::from_channels([
        Arc::clone(&channel) as Arc<dyn Channel>
    ]));
    let sink = sink_with_storage(
        store,
        channels,
        std::env::temp_dir().join("mekabridge-test-attachments"),
        Arc::new(Presence::default()),
    )
    .await;

    let error = sink
        .send_file("mock:1", &[], None, FileOptions::default(), None)
        .await
        .expect_err("an empty file list must be refused");
    assert!(error.to_string().contains("no files"), "got: {error}");
    assert!(
        channel
            .sent
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_empty(),
        "nothing should have reached the channel"
    );
}

#[tokio::test]
async fn the_preview_switch_survives_the_whole_outbound_path() {
    // The unit tests stop at the sink. This one runs the real `BridgeSink` down to a `Channel`, so
    // it covers the layer between: the sink resolves the conversation, checks capabilities and
    // rebuilds the options on the way through, and a field dropped there would look correct from
    // both ends.
    let store = Store::open_in_memory().await.expect("store");
    let channel = Arc::new(MockChannel::new("mock"));
    let channels = Arc::new(ChannelRegistry::from_channels([
        Arc::clone(&channel) as Arc<dyn Channel>
    ]));
    let sink = sink_with_storage(
        store,
        channels,
        std::env::temp_dir().join("mekabridge-test-attachments"),
        Arc::new(Presence::default()),
    )
    .await;

    sink.send_text(
        "mock:1",
        "see https://example.com",
        SendOptions {
            link_preview: true,
            ..SendOptions::default()
        },
        None,
    )
    .await
    .expect("send succeeds");

    let directory = tempfile::tempdir().expect("temp dir");
    let first = directory.path().join("chart.png");
    let second = directory.path().join("table.png");
    std::fs::write(&first, b"png").expect("write");
    std::fs::write(&second, b"png").expect("write");
    // Two files in one call, so this also covers the sink handing the whole group to the channel
    // rather than the first of it.
    sink.send_file(
        "mock:1",
        &[first.clone(), second.clone()],
        Some("see https://example.com"),
        FileOptions {
            as_photo: false,
            send: SendOptions {
                link_preview: true,
                ..SendOptions::default()
            },
        },
        None,
    )
    .await
    .expect("file send succeeds");

    let sent = channel
        .sent
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let files = sent
        .iter()
        .find(|(_, body)| body.starts_with("<files"))
        .map(|(_, body)| body.clone())
        .expect("the file send reached the channel");
    assert!(files.contains("chart.png"), "{files}");
    assert!(
        files.contains("table.png"),
        "only the first path reached the channel: {files}"
    );
    drop(sent);

    assert_eq!(
        channel
            .previews
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_slice(),
        [true, true],
        "a preview the agent asked for was lost between the sink and the channel"
    );
}

#[tokio::test]
async fn sending_a_message_stops_the_typing_indicator() {
    // The sink is the only thing that knows a reply actually went out, so this is the wiring that
    // keeps the refresh loop from re-arming an indicator Telegram has already cleared.
    let store = Store::open_in_memory().await.expect("store");
    store
        .upsert_conversation(direct_conversation("mock:1"))
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
    )
    .await;

    let conversation = ConversationId::parse("mock:1").expect("valid");
    assert!(!presence.has_replied(&conversation));

    sink.send_text("mock:1", "hello", SendOptions::default(), None)
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
        .upsert_conversation(direct_conversation("mock:1"))
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
    )
    .await;

    sink.send_file(
        "mock:1",
        std::slice::from_ref(&path),
        None,
        FileOptions::default(),
        None,
    )
    .await
    .expect("sends");
    assert!(
        presence.has_replied(&ConversationId::parse("mock:1").expect("valid")),
        "a file counts as a reply for indicator purposes"
    );
}

#[tokio::test]
async fn a_message_arriving_mid_turn_is_handed_over_without_waiting_for_the_turn() {
    // What the inbox replaces. A turn already running used to mean the next message waited for it
    // to end, and the envelope after carried a `late:` line saying the reply had been written
    // without it. Now it is handed over while the turn runs, and meka reads it at that turn's next
    // round boundary under a header of its own.
    let harness = Harness::start_slow(Duration::from_millis(700)).await;
    harness
        .sender
        .send(message("check the prod logs", "1"))
        .await
        .expect("queued");
    harness
        .wait_for("the first hand-over", |harness| !harness.items().is_empty())
        .await;

    // Sent immediately after the first turn began, the way an amendment actually arrives.
    let sent = tokio::time::Instant::now();
    harness
        .sender
        .send(message("actually, staging", "2"))
        .await
        .expect("queued");
    harness
        .wait_for("the second hand-over", |harness| harness.items().len() > 1)
        .await;

    assert!(
        sent.elapsed() < Duration::from_millis(600),
        "the amendment waited {:?} for a turn it never had to wait for",
        sent.elapsed()
    );
    let items = harness.items();
    assert!(items[1].contains("actually, staging"), "got:\n{}", items[1]);
    assert!(
        !items[1].contains("late:"),
        "nothing is late when it reaches the running turn:\n{}",
        items[1]
    );
}

/// A sink whose history retention can be turned off, for the one test that needs it.
async fn sink_with_retention(
    store: Store,
    channels: Arc<ChannelRegistry>,
    history_retention: Duration,
) -> BridgeSink {
    let accounts = mock_accounts(&store).await;
    let storage = StorageConfig {
        path: std::path::PathBuf::from("/tmp/mekabridge-unused.db"),
        attachment_dir: std::env::temp_dir().join("mekabridge-test-attachments"),
        attachment_max_bytes: 20 * 1024 * 1024,
        attachment_retention: Duration::from_secs(86_400),
        history_retention,
    };
    let meka = MekaClient::new(
        &config_for(
            ([127, 0, 0, 1], 1).into(),
            std::path::Path::new("/tmp/mekabridge-unused.db"),
            false,
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
        Arc::new(Presence::default()),
        accounts,
    )
}

/// A sink over a fresh mock channel, returned alongside the channel and the store.
async fn own_message_harness() -> (BridgeSink, Arc<MockChannel>, Store) {
    let store = Store::open_in_memory().await.expect("store");
    let channel = Arc::new(MockChannel::new("mock"));
    let channels = Arc::new(ChannelRegistry::from_channels([
        Arc::clone(&channel) as Arc<dyn Channel>
    ]));
    let sink = sink_with_storage(
        store.clone(),
        channels,
        std::env::temp_dir().join("mekabridge-test-attachments"),
        Arc::new(Presence::default()),
    )
    .await;
    (sink, channel, store)
}

#[tokio::test]
async fn what_the_agent_sends_is_recorded_one_row_per_platform_message() {
    // The failure this exists for: a scheduled session sent somebody a message, and the session
    // asked about it afterwards could find no trace of it, because the bridge recorded only what
    // other people said.
    //
    // One row per real message rather than one per call, because text past the platform's limit
    // becomes several messages with several ids, and `message_id` can only hold one of them. A
    // single row would hand back an id that edits or reacts to the first part alone.
    let (sink, _channel, store) = own_message_harness().await;

    let ids = sink
        .send_text(
            "mock:1",
            "first<split>second<split>third",
            SendOptions::default(),
            Some("scheduled-news"),
        )
        .await
        .expect("send succeeds");
    assert_eq!(ids, vec!["m1", "m2", "m3"], "the caller is told every id");

    let history = store.history("mock:1", 10, None).await.expect("read");
    assert_eq!(
        history.len(),
        3,
        "one row per message that reached the chat"
    );
    let rows: Vec<(&str, &str)> = history
        .iter()
        .map(|row| (row.message_id.as_str(), row.text.as_str()))
        .collect();
    assert_eq!(
        rows,
        vec![("m1", "first"), ("m2", "second"), ("m3", "third")],
        "each row carries the id and the text of its own message"
    );
    assert!(
        history.iter().all(|row| row.own),
        "these are the bridge's own messages"
    );
    assert!(
        history
            .iter()
            .all(|row| row.session_id.as_deref() == Some("scheduled-news")),
        "and each says which session sent it"
    );
}

#[tokio::test]
async fn the_agents_own_messages_are_never_a_backlog() {
    // `seen` is what the bridge owes the agent. Recording its own output unseen would have it
    // reporting a backlog made of things it said itself, and handing them back as missed context.
    let (sink, _channel, store) = own_message_harness().await;

    sink.send_text("mock:1", "on it", SendOptions::default(), None)
        .await
        .expect("send succeeds");

    let summary = store.unseen_summary(Some("mock:1")).await.expect("summary");
    assert_eq!(summary.count, 0, "nothing is owed after speaking");
    let (count, context, _) = store
        .take_unseen("mock:1", Utc::now() + chrono::Duration::hours(1), 10)
        .await
        .expect("take");
    assert_eq!(count, 0);
    assert!(context.is_empty(), "and nothing is offered back as missed");
}

#[tokio::test]
async fn a_file_the_agent_sent_can_be_opened_again() {
    // The case that matters is not the sending session, which still has the local path, but every
    // other one: a file sent by a scheduled job is otherwise a history row nobody can open.
    let (sink, channel, store) = own_message_harness().await;
    let directory = tempfile::tempdir().expect("temp dir");
    let path = directory.path().join("chart.png");
    std::fs::write(&path, ONE_PIXEL_PNG).expect("write");
    // What the platform will hand back when the handle is redeemed.
    channel.put_file(&path.display().to_string(), ONE_PIXEL_PNG.to_vec());

    sink.send_file(
        "mock:1",
        std::slice::from_ref(&path),
        Some("here it is"),
        FileOptions::default(),
        None,
    )
    .await
    .expect("send succeeds");

    let history = store.history("mock:1", 10, None).await.expect("read");
    let row = history.first().expect("one row");
    let handle = row
        .attachments
        .first()
        .expect("the file the agent sent has a handle");
    // Downloaded rather than viewed, because viewing branches on whether the model has vision and
    // this is about the handle reaching the platform and coming back with the bytes.
    let downloaded = sink
        .download_attachment(handle)
        .await
        .expect("the handle resolves to the file");
    assert_eq!(
        std::fs::read(&downloaded.path).expect("the file landed"),
        ONE_PIXEL_PNG,
        "the bytes came back from the platform"
    );
}

#[tokio::test]
async fn the_agent_editing_its_own_message_supersedes_the_old_wording() {
    let (sink, _channel, store) = own_message_harness().await;
    sink.send_text("mock:1", "meet at four", SendOptions::default(), None)
        .await
        .expect("send succeeds");

    sink.edit_message("mock:1", "m1", "meet at five", false, None)
        .await
        .expect("edit succeeds");

    let history = store.history("mock:1", 10, None).await.expect("read");
    assert_eq!(history.len(), 2, "both wordings are kept");
    let live: Vec<&str> = history
        .iter()
        .filter(|row| row.superseded_at.is_none())
        .map(|row| row.text.as_str())
        .collect();
    assert_eq!(live, vec!["meet at five"], "only the revision is current");
    assert!(
        history.iter().all(|row| row.message_id == "m1"),
        "both rows belong to the one message"
    );
}

#[tokio::test]
async fn the_agent_deleting_its_own_message_marks_the_row() {
    // Symmetry with a deletion reported by the platform. Without it, the agent retracting something
    // leaves a history that still reads as though it stands.
    let (sink, _channel, store) = own_message_harness().await;
    sink.send_text("mock:1", "spoke too soon", SendOptions::default(), None)
        .await
        .expect("send succeeds");

    sink.delete_message("mock:1", "m1")
        .await
        .expect("delete succeeds");

    let history = store.history("mock:1", 10, None).await.expect("read");
    let row = history.first().expect("the row survives");
    assert!(row.deleted_at.is_some(), "and says it was deleted");
    assert_eq!(row.text, "spoke too soon", "with the text still readable");
}

#[tokio::test]
async fn history_switched_off_records_nothing_the_agent_sent() {
    // `history_retention = 0` means the bridge keeps no record of a conversation. Recording its own
    // half anyway would make that setting a half-measure nobody asked for.
    let store = Store::open_in_memory().await.expect("store");
    let channel = Arc::new(MockChannel::new("mock"));
    let channels = Arc::new(ChannelRegistry::from_channels([
        Arc::clone(&channel) as Arc<dyn Channel>
    ]));
    let sink = sink_with_retention(store.clone(), channels, Duration::ZERO).await;

    sink.send_text("mock:1", "on it", SendOptions::default(), None)
        .await
        .expect("send succeeds");

    assert!(
        store
            .history("mock:1", 10, None)
            .await
            .expect("read")
            .is_empty(),
        "nothing is recorded when history is off"
    );
}

#[tokio::test]
async fn an_edit_from_the_platform_supersedes_the_wording_it_replaced() {
    // An edit arrives as a second row, under an id of its own, because the queue needs a distinct
    // key to deliver it as an event rather than discard it as a redelivery. Nothing used to connect
    // the two, so `read_history` returned the pre-edit and post-edit wordings as two messages that
    // both looked current, and an agent reading back could act on the retracted one.
    let harness = Harness::start().await;
    harness
        .sender
        .send(message("meet at four", "12"))
        .await
        .expect("queued");
    await_history(&harness, 1, "the original to be recorded").await;

    let InboundEvent::Message(mut revision) = message("meet at five", "12") else {
        panic!("the builder makes a message");
    };
    revision.edited_at = Some(Utc::now());
    harness
        .sender
        .send(InboundEvent::Message(revision))
        .await
        .expect("queued");
    await_history(&harness, 2, "the revision to be recorded").await;

    let history = harness
        .store
        .history("mock:1", 10, None)
        .await
        .expect("read");
    let live: Vec<&str> = history
        .iter()
        .filter(|row| row.superseded_at.is_none())
        .map(|row| row.text.as_str())
        .collect();
    assert_eq!(
        live,
        vec!["meet at five"],
        "only the revision is the current wording"
    );
}

#[tokio::test]
async fn a_platform_search_hit_the_bot_wrote_is_marked_as_its_own() {
    // This path is the one that reaches back past anything the bridge recorded, so it is exactly
    // where "have I already said this?" is hardest and where the mark matters most. The connector
    // settles it from its own account id, and the bridge has to carry that through rather than
    // filling in a default: the field is omitted when false, so the agent cannot tell a hit that
    // was not the bot's from one nobody bothered to check.
    let (sink, _channel, store) = own_message_harness().await;
    store
        .upsert_conversation(direct_conversation("mock:1"))
        .await
        .expect("conversation");
    store
        .record_message(mekabridge::store::MessageRecord {
            id: 0,
            conversation: "mock:1".to_string(),
            account: mock_account(&store).await,
            message_id: "local".to_string(),
            revision: 0,
            sender_id: Some("1".to_string()),
            sender_name: "Alice".to_string(),
            text: "a locally recorded match".to_string(),
            notes: None,
            attachments: Vec::new(),
            addressed: false,
            seen: true,
            own: false,
            session_id: None,
            deleted_at: None,
            superseded_at: None,
            timestamp: Utc::now(),
            previous_account: false,
        })
        .await
        .expect("record");

    let entries = sink
        .search_history("match", Some("mock:1"), 20)
        .await
        .expect("search runs");
    let from_platform = entries
        .iter()
        .find(|entry| entry.message_id == "from-the-platform")
        .expect("the platform's own record is merged in");
    assert!(
        from_platform.own,
        "a hit the connector attributed to the bot has to stay attributed to it"
    );
    assert!(
        entries.iter().any(|entry| entry.message_id == "local"),
        "the bridge's own record is still there alongside it"
    );
}

#[tokio::test]
async fn a_half_sent_reply_records_the_parts_that_landed() {
    // Splitting means a send can half succeed: three parts go out as three requests and the second
    // can be refused with the first already in the chat. Reporting only the error left the chat
    // holding words the history had no record of, so "have I already said this?" answered no about
    // something the person could see on their screen. The agent still learns the send failed; what
    // changes is that the record matches the chat either way.
    let (sink, _channel, store) = own_message_harness().await;

    let error = sink
        .send_text(
            "mock:1",
            "this part landed<split><refused><split>never reached",
            SendOptions::default(),
            None,
        )
        .await
        .expect_err("the refused part has to fail the call");
    assert!(error.to_string().contains("refused"), "got: {error}");

    let history = store.history("mock:1", 10, None).await.expect("read");
    let texts: Vec<&str> = history.iter().map(|row| row.text.as_str()).collect();
    assert_eq!(
        texts,
        vec!["this part landed"],
        "what the chat received is recorded, and what it did not is absent"
    );
    assert!(history.iter().all(|row| row.own));
}

#[tokio::test]
async fn a_reply_that_never_started_leaves_no_trace() {
    // The other half of the same rule. Nothing reached the chat, so nothing is recorded and the
    // conversation is not minted into the address book with a time the agent last spoke in it.
    let (sink, _channel, store) = own_message_harness().await;

    sink.send_text("mock:1", "<refused>", SendOptions::default(), None)
        .await
        .expect_err("the first part failing fails the call");

    assert!(
        store
            .history("mock:1", 10, None)
            .await
            .expect("read")
            .is_empty(),
        "a chat that received nothing has nothing recorded"
    );
    assert!(
        store.conversation("mock:1").await.expect("read").is_none(),
        "and is not stamped as one the agent has written in"
    );
}

#[tokio::test]
async fn the_bridges_own_apology_is_recorded_like_anything_else_it_says() {
    // The one message the bridge writes itself, and the one send that reaches a platform without
    // going through the sink. Leaving it unrecorded made it the single thing the bridge puts in a
    // chat that its own record of the chat does not contain, so somebody replying to the apology
    // would have the agent reading a reply to nothing.
    //
    // It is marked `own` with no session behind it, which is the distinction that makes it
    // representable: the account spoke, and no session did.
    let harness = Harness::start_script(TurnScript::GiveUp).await;
    harness
        .sender
        .send(message("hello", "1"))
        .await
        .expect("queued");

    harness
        .wait_for("the chat to be told", |harness| {
            harness
                .channel
                .sent()
                .iter()
                .any(|(conversation, _)| conversation == "mock:1")
        })
        .await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let recorded = harness
            .store
            .history("mock:1", 10, None)
            .await
            .expect("read")
            .into_iter()
            .find(|row| row.own);
        if let Some(row) = recorded {
            assert!(
                row.text.contains("went wrong"),
                "the row has to hold what the chat was actually told, got {:?}",
                row.text
            );
            assert_eq!(
                row.session_id, None,
                "no session wrote this; the bridge did"
            );
            assert!(row.seen, "and it is not a backlog owed to the agent");
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for the notice to reach the history"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn an_item_the_feed_reports_before_its_receipt_is_still_settled() {
    // The ordering no client controls. meka answers the post once the item is durable, and the
    // turn it opens can be underway, delivered and finished before that answer has travelled back
    // over a second connection. The feed reader hands its events straight on, so they are waiting
    // in front of the session task before it has learned the item's id at all.
    //
    // What makes it safe is that one task does both: the 202 is written down before the next feed
    // event is looked at, so an outcome that arrived first is matched to its batch rather than
    // filed against an item nobody has heard of.
    let harness = Harness::start().await;
    harness
        .recorder
        .set(&harness.recorder.run_before_answering, true);

    harness
        .sender
        .send(message("did that land?", "1"))
        .await
        .expect("queued");
    harness
        .wait_for_queue("the batch to be delivered", |stats| stats.done == 1)
        .await;
    assert_eq!(harness.attempts(), 1, "and handed over exactly once");
}

#[tokio::test]
async fn a_hand_over_meka_never_saw_is_posted_again_after_a_restart() {
    // A bridge that stops between binding a batch and posting it has rendered an envelope nobody
    // has read. The bytes and the key are on the row, so the next start posts exactly those rather
    // than rebuilding an envelope that would now say something different: the backlog it reported
    // has not been spent, and rebuilding it would report it twice.
    let directory = tempfile::tempdir().expect("tempdir");
    let database = directory.path().join("state.db");
    let recorder = Arc::new(MekaRecorder::default());
    let (meka_address, meka_shutdown) = start_meka(Arc::clone(&recorder)).await;
    let config = Arc::new(config_for(meka_address, &database, false));

    // What the previous run left: rows claimed, an envelope rendered, and nothing posted.
    let key = {
        let store = Store::open(&database).await.expect("opens");
        store
            .upsert_conversation(direct_conversation("mock:1"))
            .await
            .expect("conversation");
        let account = mock_account(&store).await;
        let payload = serde_json::to_string(&message("hello", "1")).expect("encodes");
        store
            .enqueue(
                MessageKey {
                    conversation: "mock:1",
                    account,
                    message_id: "1",
                    revision: 0,
                },
                &payload,
                Utc::now(),
                64,
            )
            .await
            .expect("enqueued");
        let claimed = store
            .claim(&["mock:1".to_string()], 10)
            .await
            .expect("claimed");
        let seq = claimed.first().expect("a row was claimed").seq;
        let key = format!("mekabridge-{}", uuid::Uuid::new_v4());
        assert!(
            store
                .hold(
                    seq,
                    HandOver {
                        key: &key,
                        body: "rendered before the restart",
                        session_id: uuid::Uuid::nil(),
                        dropped: 0,
                        accounted: None,
                    },
                    Utc::now(),
                )
                .await
                .expect("held")
        );
        key
    };

    let running = run_bridge(&config).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let stats = running.store.queue_stats().await.expect("stats");
        if stats.done == 1 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the unposted hand-over was never handed over; stats {stats:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let posted = recorder
        .items
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    assert_eq!(posted.len(), 1, "got {posted:?}");
    assert_eq!(posted[0].0, key, "a new key would make a second item");
    assert_eq!(
        posted[0].1, "rendered before the restart",
        "the item was rebuilt rather than posted as it stood"
    );
    running.shutdown.cancel();
    meka_shutdown.cancel();
}

#[tokio::test]
async fn a_replay_that_lost_events_makes_the_bridge_ask_about_what_it_has_out() {
    // meka's ring is bounded, so a resume can come back saying events are gone rather than handing
    // over a transcript that silently skips. Among them can be the one event that says a batch was
    // read, and nothing else ever will: the turn is over, and waiting for an outcome that has been
    // and gone leaves the messages in flight for ever. Asking meka about the item under its own
    // key is what settles it, and costs one request that changes nothing on meka's side.
    let harness = Harness::start_script(TurnScript::Hold).await;
    harness
        .sender
        .send(message("are you there?", "1"))
        .await
        .expect("queued");
    harness
        .wait_for("the hand-over", |harness| !harness.items().is_empty())
        .await;

    // The model reads it and the event that said so never reaches the bridge.
    let item = harness
        .recorder
        .by_key
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .values()
        .next()
        .cloned()
        .expect("one item");
    harness.recorder.mark(&item, "delivered");
    harness.recorder.feed.push(
        "notice",
        None,
        serde_json::json!({
            "level": "warn",
            "text": "the replay does not reach your Last-Event-ID, so events were dropped; read \
                     `GET /v1/sessions/{id}/messages` for the full transcript",
        }),
    );

    harness
        .wait_for_queue("the batch to be settled by the reconciliation", |stats| {
            stats.done == 1
        })
        .await;
    assert_eq!(
        harness.attempts(),
        2,
        "the hand-over and the one question about it"
    );
}

#[tokio::test]
async fn a_dropped_feed_is_reopened_from_where_it_left_off() {
    // The feed is the only thing that says what became of a batch, so losing it is not something
    // to be reported and given up on. It is reopened from the last event the session task acted
    // on, and meka replays what was missed across turns rather than from the beginning, which is
    // what stops a reconnect re-reading outcomes the bridge has already acted on.
    let harness = Harness::start().await;
    harness
        .sender
        .send(message("first", "1"))
        .await
        .expect("queued");
    harness
        .wait_for_queue("the first batch", |stats| stats.done == 1)
        .await;

    harness.recorder.feed.cut();
    harness
        .wait_for("the feed to be reopened", |harness| {
            harness
                .recorder
                .attaches
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .len()
                >= 2
        })
        .await;
    let attaches = harness
        .recorder
        .attaches
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    assert_eq!(attaches[0], None, "nothing had been handled at startup");
    assert!(
        attaches[1].is_some_and(|last| last > 0),
        "the feed was reopened from the start rather than resumed: {attaches:?}"
    );

    // And it still works afterwards, which is the half a resume that merely reconnects would miss.
    harness
        .sender
        .send(message("second", "2"))
        .await
        .expect("queued");
    harness
        .wait_for_queue("the second batch", |stats| stats.done == 2)
        .await;
}

#[tokio::test]
async fn a_model_that_came_back_empty_has_its_batch_offered_again() {
    // meka streams a bracketed stand-in when a turn yields no content at all. Paired with no tool
    // calls that is provably inert: nothing was sent and nothing ran, so offering the messages
    // again repeats nothing, and is far better than leaving somebody who has just messaged the bot
    // in silence.
    let harness = Harness::start_script_with(TurnScript::Empty, Setup {
        max_offers: 3,
        ..Setup::default()
    })
    .await;
    // Slow enough that the flip below lands between two rounds rather than racing several.
    harness
        .recorder
        .set(&harness.recorder.turn_delay, Duration::from_millis(200));
    harness
        .sender
        .send(message("are you there?", "1"))
        .await
        .expect("queued");

    harness
        .wait_for("the batch to be handed over again", |harness| {
            harness.items().len() >= 2
        })
        .await;
    harness
        .recorder
        .set(&harness.recorder.script, TurnScript::Answer);
    harness
        .wait_for_queue("the answer", |stats| stats.done == 1)
        .await;
    assert_eq!(
        harness.store.queue_stats().await.expect("stats").failed,
        0,
        "the messages were given up on rather than answered"
    );
}

#[tokio::test]
async fn a_turn_that_read_a_batch_and_then_failed_still_tells_the_chat() {
    // The model read the messages and the turn died before it said anything. They are accounted
    // for, since it read them, so they are not offered again; but nobody in the chat has heard a
    // word, so this is the one failure after a delivery that the chat itself is told about.
    let harness =
        Harness::start_script_with(TurnScript::FailAfterReading, Setup::with_owner()).await;
    harness
        .sender
        .send(message("hello", "1"))
        .await
        .expect("queued");

    harness
        .wait_for("both notices", |harness| harness.channel.sent().len() >= 2)
        .await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let sent = harness.channel.sent();
    assert!(
        sent.iter()
            .any(|(conversation, text)| conversation == "mock:1"
                && text.contains("may not have finished")),
        "a chat the agent never answered must still be told: {sent:?}"
    );
    let owner = sent
        .iter()
        .find(|(conversation, _)| conversation == "mock:99")
        .expect("the owner must be told");
    assert!(
        owner.1.contains("529 overloaded_error"),
        "the owner needs the upstream's own words: {:?}",
        owner.1
    );
    let stats = harness.store.queue_stats().await.expect("stats");
    assert_eq!(
        stats.done, 1,
        "and the messages are accounted for: {stats:?}"
    );
    assert_eq!(harness.attempts(), 1, "rather than handed over again");
}

#[tokio::test]
async fn a_turn_the_agent_read_and_said_nothing_in_is_still_a_delivery() {
    // The agent is allowed to read something and answer nobody. From the other end that looks
    // exactly like a broken bridge, which is why it is logged, but it is not a delivery failure
    // and the messages must not go round again.
    let harness = Harness::start_script(TurnScript::Silent).await;
    harness
        .sender
        .send(message("just so you know", "1"))
        .await
        .expect("queued");

    harness
        .wait_for_queue("the batch to be delivered", |stats| stats.done == 1)
        .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(harness.attempts(), 1, "a silent turn is not a failure");
    assert!(
        harness.channel.sent().is_empty(),
        "and nobody is apologised to for it: {:?}",
        harness.channel.sent()
    );
}

#[tokio::test]
async fn a_refusal_that_needs_an_operator_is_given_up_on_at_once() {
    // A rotated token will not come back on its own, so spending the hour on it only delays the
    // notice that says an operator is needed.
    let harness = Harness::start_failing(FailureKind::Auth, Setup::with_owner()).await;
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
    assert_eq!(harness.attempts(), 1, "an operator's problem is not a wait");
    assert_eq!(harness.store.queue_stats().await.expect("stats").failed, 1);
    let sent = harness.channel.sent();
    let owner = sent
        .iter()
        .find(|(conversation, _)| conversation == "mock:99")
        .expect("the owner must be told");
    assert!(
        owner.1.contains("needs you rather than another attempt"),
        "the owner has to be told it will not clear on its own: {:?}",
        owner.1
    );
}

#[tokio::test]
async fn a_session_another_process_holds_is_waited_out_rather_than_failed() {
    // A REPL open on the bridge's session is the ordinary way here: meka refuses the post before
    // anything is written, and its own driver asks a held session again every ten seconds until
    // the holder lets go. A wait, not a delivery failure, so the batch keeps its place.
    let harness = Harness::start_failing(FailureKind::Locked, Setup {
        hand_over_within: Duration::from_secs(30),
        ..Setup::default()
    })
    .await;
    harness
        .sender
        .send(message("are you there?", "1"))
        .await
        .expect("queued");

    harness
        .wait_for("a second post", |harness| harness.attempts() >= 2)
        .await;
    let stats = harness.store.queue_stats().await.expect("stats");
    assert_eq!(
        stats.failed, 0,
        "a held session is not a failure: {stats:?}"
    );
    assert_eq!(stats.in_flight, 1, "the hand-over keeps its place");
    assert!(
        harness.channel.sent().is_empty(),
        "and nobody is told anything: {:?}",
        harness.channel.sent()
    );
}

#[tokio::test]
async fn nothing_overtakes_a_hand_over_meka_has_not_accepted() {
    // The queue is claimed in order and one hand-over is offered at a time, so a chat whose
    // envelope is waiting on an unreachable meka is not passed by a chat that arrived after it.
    // Releasing the later one would hand the agent the second half of the morning before the
    // first, and there is nothing to be gained by it: whatever is refusing the first will refuse
    // the second.
    let harness = Harness::start_failing(FailureKind::Transient, Setup {
        hand_over_within: Duration::from_millis(600),
        ..Setup::default()
    })
    .await;
    harness
        .recorder
        .set(&harness.recorder.fail_only, Some("mock:1".to_string()));

    harness
        .sender
        .send(message("first, and stuck", "1"))
        .await
        .expect("queued");
    harness
        .wait_for("the first post", |harness| harness.attempts() >= 1)
        .await;
    harness
        .sender
        .send(message_elsewhere("second, and fine", "2"))
        .await
        .expect("queued");
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        harness.items().is_empty(),
        "a later chat was handed over ahead of the one waiting: {:?}",
        harness.items()
    );

    // And once the first is given up on, the second goes at once rather than waiting for it.
    harness
        .wait_for_queue("the first to be given up on", |stats| stats.failed == 1)
        .await;
    harness
        .wait_for("the second chat's hand-over", |harness| {
            harness
                .items()
                .iter()
                .any(|envelope| envelope.contains("second, and fine"))
        })
        .await;
}

#[tokio::test]
async fn a_backlog_is_stated_once_and_owed_back_if_its_item_dies() {
    // Two hand-overs can be out at once now, which is what the inbox is for, so what a chat has
    // already been reported is a set rather than a watermark: the item that states a backlog marks
    // exactly those rows seen against itself as it is rendered, a later item counts only what is
    // left, and an item that dies gives back its own slice and nothing else.
    let harness = Harness::start_script(TurnScript::Hold).await;
    harness
        .store
        .set_policy("mock:1", "telegram", Policy::Mute, None, None, Utc::now())
        .await
        .expect("mute");
    for index in 0..3 {
        harness
            .sender
            .send(message(&format!("chatter {index}"), &index.to_string()))
            .await
            .expect("queued");
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    harness
        .sender
        .send(mention("@bot one", "9"))
        .await
        .expect("queued");
    harness
        .wait_for("the first hand-over", |harness| !harness.items().is_empty())
        .await;
    harness
        .sender
        .send(mention("@bot two", "10"))
        .await
        .expect("queued");
    harness
        .wait_for("the second hand-over", |harness| harness.items().len() >= 2)
        .await;

    let items = harness.items();
    assert!(
        items[0].contains("3 messages you have not seen") && items[0].contains("chatter 2"),
        "the first envelope has to state it:\n{}",
        items[0]
    );
    assert!(
        !items[1].contains("you have not seen") && !items[1].contains("chatter 2"),
        "the backlog was stated twice, so the agent sees the same messages in both:\n{}",
        items[1]
    );
    assert!(items[1].contains("@bot two"), "got:\n{}", items[1]);

    // And a hand-over that fails leaves it owed rather than spent, so the next one states it.
    harness
        .recorder
        .set(&harness.recorder.script, TurnScript::GiveUp);
    harness
        .wait_for_queue("both hand-overs to be given up on", |stats| {
            stats.failed == 2
        })
        .await;
    assert_eq!(
        harness
            .store
            .unseen_summary(Some("mock:1"))
            .await
            .expect("summary")
            .count,
        5,
        "the three withheld messages and the two mentions are all owed again"
    );
}

#[tokio::test]
async fn two_messages_in_one_pass_state_their_chat_s_backlog_once() {
    // The other half of the accounting, and the half a single hand-over cannot show. A claimed pass
    // renders an item per message, so two mentions that settle together are two items; the backlog
    // belongs on the first of them and marking it seen there is what keeps the second from
    // restating what the agent is already being shown in the same turn.
    let harness = Harness::start_script(TurnScript::Hold).await;
    harness
        .store
        .set_policy("mock:1", "telegram", Policy::Mute, None, None, Utc::now())
        .await
        .expect("mute");
    for index in 0..3 {
        harness
            .sender
            .send(message(&format!("chatter {index}"), &index.to_string()))
            .await
            .expect("queued");
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    harness
        .sender
        .send(mention("@bot one", "9"))
        .await
        .expect("queued");
    harness
        .sender
        .send(mention("@bot two", "10"))
        .await
        .expect("queued");
    harness
        .wait_for("both items", |harness| harness.items().len() >= 2)
        .await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let items = harness.items();
    assert_eq!(items.len(), 2, "one item per message: {items:#?}");
    assert!(
        items[0].contains("3 messages you have not seen") && items[0].contains("chatter 2"),
        "the first item of the chat states its backlog:\n{}",
        items[0]
    );
    assert!(
        !items[1].contains("you have not seen") && !items[1].contains("chatter 2"),
        "the second states it again, so the agent reads the same three messages twice in one \
         turn:\n{}",
        items[1]
    );
    // Nor the empty version of the block. Once the first item has marked the backlog seen, asking
    // again answers "nothing since you last looked", which is true and worth nothing: the agent is
    // reading both items in the same turn and was told about the chat a few lines above.
    assert!(
        !items[1].contains("only woken in mock:1"),
        "the second item repeats the mute notice the first one just gave:\n{}",
        items[1]
    );
    assert!(items[1].contains("@bot two"), "got:\n{}", items[1]);
    assert_eq!(
        harness
            .store
            .unseen_summary(Some("mock:1"))
            .await
            .expect("summary")
            .count,
        0,
        "the backlog is spent by the item that states it, not by the turn that reads it"
    );
}

#[tokio::test]
async fn one_hand_over_dying_leaves_the_other_holding_its_own_slice() {
    // The half of the accounting a watermark could not express. Two items are outstanding for one
    // chat: the first states the backlog, the second states nothing because there is nothing left.
    // The first dies. Exactly its rows come back, and the live one keeps what it took, so the agent
    // is neither shown the same messages twice nor left owing what it has already been handed.
    let harness = Harness::start_script(TurnScript::Hold).await;
    harness
        .store
        .set_policy("mock:1", "telegram", Policy::Mute, None, None, Utc::now())
        .await
        .expect("mute");
    for index in 0..3 {
        harness
            .sender
            .send(message(&format!("chatter {index}"), &index.to_string()))
            .await
            .expect("queued");
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    harness
        .sender
        .send(mention("@bot one", "9"))
        .await
        .expect("queued");
    harness
        .wait_for("the first hand-over", |harness| !harness.items().is_empty())
        .await;
    harness
        .sender
        .send(mention("@bot two", "10"))
        .await
        .expect("queued");
    harness
        .wait_for("the second hand-over", |harness| harness.items().len() >= 2)
        .await;

    // meka gives up on the first item alone, which is what its own hour of retries running out
    // looks like on the feed.
    let first = harness
        .recorder
        .by_item
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .first()
        .map(|(item, _)| item.clone())
        .expect("an item was accepted");
    harness.recorder.mark(&first, "withdrawn");
    harness.recorder.feed.push(
        "inbox.failed",
        None,
        serde_json::json!({ "item_id": first, "reason": "no turn could deliver it" }),
    );

    harness
        .wait_for_queue("the first hand-over to be given up on", |stats| {
            stats.failed == 1
        })
        .await;
    assert_eq!(
        harness
            .store
            .unseen_summary(Some("mock:1"))
            .await
            .expect("summary")
            .count,
        4,
        "the three withheld messages and the mention that died, and not the one still with meka"
    );
}

#[tokio::test]
async fn a_turn_is_told_apart_from_a_batch_settled_a_moment_before_it_reported() {
    // The association between a turn and the batches it read is what the owner notice and the
    // second offer both hang on, and nothing else carries it. A reconnect that spans a delivery
    // settles the batch by asking meka, so the `inbox.delivered` that follows in the replay finds
    // it already closed; taking that as "this turn read nothing of ours" loses the notice for a
    // turn that died half-way through work it had really been given.
    let harness = Harness::start_script_with(TurnScript::Hold, Setup::with_owner()).await;
    harness
        .sender
        .send(message("what did the log say?", "1"))
        .await
        .expect("queued");
    harness
        .wait_for("the hand-over", |harness| !harness.items().is_empty())
        .await;

    // The model reads it while the feed is down, so the event saying so never arrives.
    let item = harness
        .recorder
        .by_key
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .values()
        .next()
        .cloned()
        .expect("one item");
    harness.recorder.mark(&item, "delivered");
    harness.recorder.feed.cut();
    harness
        .wait_for_queue("the reconnect to settle it", |stats| stats.done == 1)
        .await;

    // And now the replay hands over the turn it was read by, which then dies.
    let feed = Arc::clone(&harness.recorder.feed);
    feed.push(
        "turn.started",
        Some("t1"),
        serde_json::json!({"source": "inbox", "item_ids": [item]}),
    );
    feed.push(
        "inbox.delivered",
        Some("t1"),
        serde_json::json!({"item_ids": [item]}),
    );
    feed.push(
        "turn.failed",
        Some("t1"),
        serde_json::json!({"error": {"type": "https://meka.so/errors/provider",
                                     "title": "Provider", "status": 502,
                                     "detail": "the provider refused"}}),
    );

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
        owner.1.contains("half finished"),
        "the owner has to hear that the turn died on work it had been given: {:?}",
        owner.1
    );
}

#[tokio::test]
async fn a_batch_meka_has_held_too_long_is_asked_about_without_a_reconnect() {
    // The outcome of a hand-over arrives once. A store error while writing it down is logged and
    // nothing re-drives it, the feed position moves past the event, and meka never sends it again.
    // On a connection that stays up for days those rows would sit in flight for days: invisible to
    // the queue's own readiness, still charged against the depth, reaching nobody. meka gives up
    // on an item within the hour, so anything older than that has an outcome that has been and
    // gone, and is worth one question.
    let harness = Harness::start_script_with(TurnScript::Hold, Setup {
        hand_over_within: Duration::from_millis(300),
        ..Setup::default()
    })
    .await;
    harness
        .sender
        .send(message("are you there?", "1"))
        .await
        .expect("queued");
    harness
        .wait_for("the hand-over", |harness| !harness.items().is_empty())
        .await;

    // Read by the model, with the event lost rather than merely late.
    let item = harness
        .recorder
        .by_key
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .values()
        .next()
        .cloned()
        .expect("one item");
    harness.recorder.mark(&item, "delivered");
    tokio::time::sleep(Duration::from_millis(400)).await;

    // Anything that wakes the loop; the sweep runs on the round rather than on a timer of its own.
    harness
        .sender
        .send(message_elsewhere("unrelated", "2"))
        .await
        .expect("queued");
    harness
        .wait_for_queue("the straggler to be settled", |stats| stats.done >= 1)
        .await;
    assert_eq!(
        harness.store.queue_stats().await.expect("stats").in_flight,
        0,
        "the rows were left in flight with nothing able to move them"
    );
}
