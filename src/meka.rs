//! Client for meka's HTTP API.
//!
//! Turns are always submitted with `stream: true`. The bridge does not relay deltas to anyone, but
//! streaming buys three things a blocking POST cannot: liveness on a turn that runs for minutes
//! (which matters behind proxies with read timeouts), visibility of tool calls in the log, and a
//! clean terminal event instead of a connection that may or may not still be alive.
//!
//! Retries are deliberately asymmetric. Read-only endpoints retry on connect failures, 5xx, and
//! 429. Session creation and turn submission never do: a retried `POST /turn` can bill a second
//! time and send a second round of messages, so turn-level retry belongs to the queue's attempt
//! counter in [`crate::store`], where it is bounded and observable.

pub mod sse;

use std::{path::Path, time::Duration};

use futures::{Stream, StreamExt};
use serde::Deserialize;
use url::Url;
use uuid::Uuid;

use crate::{
    config::{MekaConfig, Permission, secret::Secret},
    meka::sse::TurnEvent,
};

/// Problem Detail (RFC 9457) as returned by every meka error response.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct ProblemDetail {
    #[serde(rename = "type")]
    pub type_uri: String,
    pub title: String,
    pub status: u16,
    pub detail: String,
    pub instance: Option<String>,
}

impl ProblemDetail {
    /// Classify the stable `type` URI. meka's docs are explicit that clients should route on this
    /// rather than on the status code or the human-readable detail.
    pub fn kind(&self) -> ProblemKind {
        match self.type_uri.rsplit('/').next().unwrap_or_default() {
            "auth" => ProblemKind::Auth,
            "auth-scope" => ProblemKind::AuthScope,
            "session-not-found" => ProblemKind::SessionNotFound,
            "session-locked" => ProblemKind::SessionLocked,
            "turn-in-flight" => ProblemKind::TurnInFlight,
            "turn-cancelled" => ProblemKind::TurnCancelled,
            "concurrency-limit" => ProblemKind::ConcurrencyLimit,
            "sse-lag" => ProblemKind::SseLag,
            "provider" => ProblemKind::Provider,
            "invalid-body" => ProblemKind::InvalidBody,
            "payload-too-large" => ProblemKind::PayloadTooLarge,
            "idempotency" => ProblemKind::Idempotency,
            "internal" => ProblemKind::Internal,
            _ => ProblemKind::Other,
        }
    }
}

impl std::fmt::Display for ProblemDetail {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.detail.is_empty() {
            write!(formatter, "{} ({})", self.title, self.status)
        } else {
            write!(
                formatter,
                "{} ({}): {}",
                self.title, self.status, self.detail
            )
        }
    }
}

/// Machine-readable meka error categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProblemKind {
    Auth,
    AuthScope,
    SessionNotFound,
    SessionLocked,
    TurnInFlight,
    TurnCancelled,
    ConcurrencyLimit,
    SseLag,
    Provider,
    InvalidBody,
    PayloadTooLarge,
    Idempotency,
    Internal,
    Other,
}

#[derive(Debug, thiserror::Error)]
pub enum MekaError {
    #[error("could not reach meka: {0}")]
    Transport(#[from] reqwest::Error),

    #[error("meka rejected the request: {0}")]
    Problem(ProblemDetail),

    #[error("meka returned HTTP {status} with an unexpected body: {body}")]
    UnexpectedStatus { status: u16, body: String },

    /// The turn was accepted and then the connection failed.
    ///
    /// meka keeps running the turn regardless: it holds the runtime lock and the spawned task
    /// completes. Every stream-level failure lands here because the stream only exists after the
    /// submission was accepted, so there is no case where this means "the turn never happened".
    #[error("the turn stream was interrupted after the turn had started: {reason}")]
    StreamInterrupted { reason: String },

    #[error("turn exceeded the configured timeout of {}s", .0.as_secs())]
    Timeout(Duration),

    #[error("could not decode a meka response: {0}")]
    Decode(String),

    #[error("could not build the meka client: {0}")]
    Build(String),
}

impl MekaError {
    /// Whether retrying the same request could plausibly succeed.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Transport(error) => {
                error.is_timeout() || error.is_connect() || error.is_request()
            }
            Self::Problem(problem) => matches!(
                problem.kind(),
                ProblemKind::ConcurrencyLimit
                    | ProblemKind::Provider
                    | ProblemKind::Internal
                    | ProblemKind::SessionLocked
            ),
            _ => false,
        }
    }

    /// Whether meka says the session id the bridge holds no longer exists, which is the cue to
    /// create a replacement.
    pub fn is_session_missing(&self) -> bool {
        matches!(self, Self::Problem(problem) if problem.kind() == ProblemKind::SessionNotFound)
    }

    /// Whether meka rejected the submission because a turn is already running on the session.
    ///
    /// This can only happen before the turn is accepted, so the batch was never handed over.
    pub fn is_turn_in_flight(&self) -> bool {
        matches!(self, Self::Problem(problem) if problem.kind() == ProblemKind::TurnInFlight)
    }

    /// Whether the turn was accepted and may still be running server-side despite this error.
    ///
    /// A dropped stream does not cancel the turn: meka keeps the runtime lock and the spawned task
    /// runs to completion. Resubmitting would duplicate a reply the user is about to receive, so
    /// the caller should wait for the session to go idle rather than retry.
    pub const fn turn_may_still_be_running(&self) -> bool {
        matches!(self, Self::StreamInterrupted { .. })
    }
}

type Result<T> = std::result::Result<T, MekaError>;

/// Session metadata as returned by `GET /v1/sessions/{id}`.
#[derive(Debug, Clone, Deserialize)]
pub struct SessionInfo {
    pub id: Uuid,
    pub permission: String,
    pub title: String,
    /// Omitted by meka when the session has no working directory of its own.
    #[serde(default)]
    pub cwd: Option<String>,
    /// Whether a turn is running on this session right now.
    ///
    /// Lets a client whose stream dropped tell "my turn is still running" from "my turn died"
    /// without submitting a speculative turn and reading the 409.
    pub turn_in_flight: bool,
}

/// Server metadata as returned by `GET /v1/info`.
#[derive(Debug, Clone, Deserialize)]
pub struct ServerInfo {
    pub version: String,
    /// `null` when no model is configured. meka serializes the field either way, so this has to be
    /// an `Option`: a bare `String` fails to deserialize the null and takes `doctor` and the
    /// vision check down with it.
    pub model: Option<String>,
    /// Whether the active provider profile can look at images at all.
    ///
    /// The bridge attaches nothing to a turn, so this gates `view_attachment` instead: with vision
    /// off, meka would replace the image block in the tool result with a placeholder, and
    /// returning a description that names the file is more use to the agent than a picture it
    /// cannot see.
    pub vision: bool,
}

/// Readiness as returned by `GET /v1/health/ready`.
#[derive(Debug, Clone, Deserialize)]
pub struct ReadyStatus {
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub session_db: bool,
    #[serde(default)]
    pub provider_configured: bool,
    #[serde(default)]
    pub mcp_servers_healthy: bool,
}

/// How a turn ended.
#[derive(Debug, Clone, PartialEq)]
pub enum TurnOutcome {
    Finished {
        stop_reason: String,
        refusal_text: Option<String>,
        usage: sse::Usage,
    },
    Cancelled {
        reason: String,
    },
}

/// HTTP client for one `meka serve` instance.
#[derive(Clone)]
pub struct MekaClient {
    http: reqwest::Client,
    base_url: Url,
    token: Secret,
    turn_timeout: Duration,
    max_retries: u32,
}

impl MekaClient {
    /// Build a client from the resolved `[meka]` config.
    pub fn new(config: &MekaConfig) -> Result<Self> {
        let http = reqwest::Client::builder()
            .connect_timeout(config.connect_timeout)
            // No overall request timeout: a streaming turn legitimately runs for minutes, and the
            // per-turn budget is applied around the stream instead.
            .build()
            .map_err(|error| MekaError::Build(error.to_string()))?;
        Ok(Self {
            http,
            base_url: config.base_url.clone(),
            token: config.token.clone(),
            turn_timeout: config.turn_timeout,
            max_retries: config.max_retries,
        })
    }

    fn endpoint(&self, path: &str) -> Result<Url> {
        self.base_url
            .join(path)
            .map_err(|error| MekaError::Build(format!("invalid endpoint {path}: {error}")))
    }

    /// `POST /v1/sessions`. Never retried: a retry after an ambiguous failure would leave an orphan
    /// session holding a conversation the bridge no longer tracks.
    pub async fn create_session(&self, cwd: Option<&Path>, permission: Permission) -> Result<Uuid> {
        #[derive(Deserialize)]
        struct CreateResponse {
            id: Uuid,
        }

        let mut body = serde_json::json!({
            "permission": permission.as_str(),
            "capabilities": {
                // The bridge never relays reasoning, so asking for it would only waste bandwidth.
                "supports_reasoning_stream": false,
                // There is no interface here to show an approval prompt on. Declaring that makes
                // meka deny a gated tool immediately with a notice, instead of parking the turn on
                // the SSE channel for a minute waiting for an answer that can never arrive.
                "supports_permission_prompts": false,
            },
        });
        if let Some(cwd) = cwd
            && let Some(object) = body.as_object_mut()
        {
            object.insert(
                "cwd".to_string(),
                serde_json::Value::String(cwd.to_string_lossy().into_owned()),
            );
        }

        let response = self
            .http
            .post(self.endpoint("/v1/sessions")?)
            .bearer_auth(self.token.expose())
            .json(&body)
            .send()
            .await?;
        let created: CreateResponse = decode(response).await?;
        Ok(created.id)
    }

    /// `PATCH /v1/sessions/{id}`: change the session's permission level.
    ///
    /// Used to reconcile a running session with `[session].permission` after an operator edits the
    /// config. Without it the level is fixed at creation time and a config change silently does
    /// nothing, which is a confusing way to be stuck.
    pub async fn set_session_permission(
        &self,
        session_id: Uuid,
        permission: Permission,
    ) -> Result<()> {
        let url = self.endpoint(&format!("/v1/sessions/{session_id}"))?;
        let response = self
            .http
            .patch(url)
            .bearer_auth(self.token.expose())
            .json(&serde_json::json!({ "permission": permission.as_str() }))
            .send()
            .await?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(problem_from(response).await)
        }
    }

    /// `GET /v1/sessions/{id}`.
    pub async fn session(&self, session_id: Uuid) -> Result<SessionInfo> {
        let url = self.endpoint(&format!("/v1/sessions/{session_id}"))?;
        self.get_with_retries(url).await
    }

    /// `GET /v1/info`.
    pub async fn info(&self) -> Result<ServerInfo> {
        let url = self.endpoint("/v1/info")?;
        self.get_with_retries(url).await
    }

    /// `GET /v1/health/ready`.
    pub async fn ready(&self) -> Result<ReadyStatus> {
        let url = self.endpoint("/v1/health/ready")?;
        self.get_with_retries(url).await
    }

    /// `POST /v1/sessions/{id}/cancel`. Idempotent server-side, so a retry is safe.
    pub async fn cancel_turn(&self, session_id: Uuid) -> Result<()> {
        let url = self.endpoint(&format!("/v1/sessions/{session_id}/cancel"))?;
        let mut attempt = 0;
        loop {
            let response = self
                .http
                .post(url.clone())
                .bearer_auth(self.token.expose())
                .send()
                .await;
            let outcome = match response {
                Ok(response) if response.status().is_success() => return Ok(()),
                Ok(response) => Err(problem_from(response).await),
                Err(error) => Err(MekaError::Transport(error)),
            };
            match outcome {
                Ok(()) => return Ok(()),
                Err(error) => {
                    if attempt >= self.max_retries || !error.is_retryable() {
                        return Err(error);
                    }
                    backoff(attempt).await;
                    attempt += 1;
                }
            }
        }
    }

    /// Poll the session until no turn is running, or `budget` elapses.
    ///
    /// Returns whether the session actually went idle. Used after a dropped stream, where the turn
    /// is still running and resubmitting would duplicate a reply the user is about to receive.
    pub async fn wait_until_idle(&self, session_id: Uuid, budget: Duration) -> Result<bool> {
        const POLL_INTERVAL: Duration = Duration::from_secs(2);
        let deadline = tokio::time::Instant::now() + budget;
        loop {
            match self.session(session_id).await {
                Ok(info) if !info.turn_in_flight => return Ok(true),
                Ok(_) => {}
                // A session that no longer exists cannot be running a turn.
                Err(error) if error.is_session_missing() => return Err(error),
                Err(error) => {
                    tracing::debug!("while waiting for the session to go idle: {}", error);
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Ok(false);
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    async fn get_with_retries<T: serde::de::DeserializeOwned>(&self, url: Url) -> Result<T> {
        let mut attempt = 0;
        loop {
            let result = async {
                let response = self
                    .http
                    .get(url.clone())
                    .bearer_auth(self.token.expose())
                    .send()
                    .await?;
                decode(response).await
            }
            .await;
            match result {
                Ok(value) => return Ok(value),
                Err(error) => {
                    if attempt >= self.max_retries || !error.is_retryable() {
                        return Err(error);
                    }
                    tracing::debug!(
                        "retrying {} after {}: attempt {} of {}",
                        url,
                        error,
                        attempt + 1,
                        self.max_retries
                    );
                    backoff(attempt).await;
                    attempt += 1;
                }
            }
        }
    }

    /// Open a turn's SSE stream.
    ///
    /// The returned stream yields every event meka emits and ends after a terminal one. Errors
    /// before the first event (auth, unknown session, a turn already in flight) surface here rather
    /// than inside the stream, so the caller can distinguish "never started" from "died partway".
    ///
    /// The body carries text and nothing else, deliberately. meka's turn API does accept image
    /// attachments, and an earlier version of this client used them, but images now reach the model
    /// only when the agent asks for one with `view_attachment`, which returns an MCP image block
    /// that meka forwards to the provider as multimodal content. The reason is that this bridge
    /// owns one permanent session: an image attached to a turn stays in that context for the life
    /// of the session, so pushing every photo anyone sends would fill it with pictures nobody
    /// ever needed. Reading this and concluding that images cannot reach the model is a mistake
    /// worth heading off; the pull path is in `mcp.rs` and `bridge.rs`.
    async fn open_turn(
        &self,
        session_id: Uuid,
        message: &str,
    ) -> Result<impl Stream<Item = Result<TurnEvent>> + Send + use<>> {
        let url = self.endpoint(&format!("/v1/sessions/{session_id}/turn"))?;
        let response = self
            .http
            .post(url)
            .bearer_auth(self.token.expose())
            .json(&serde_json::json!({
                "message": message,
                "stream": true,
            }))
            .send()
            .await?;
        if !response.status().is_success() {
            return Err(problem_from(response).await);
        }

        let events = response.bytes_stream();
        let stream = futures::stream::unfold(
            (
                Box::pin(eventsource_stream::Eventsource::eventsource(events)),
                false,
            ),
            |(mut events, finished)| async move {
                if finished {
                    return None;
                }
                loop {
                    let next = events.next().await;
                    match next {
                        None => return None,
                        Some(Err(error)) => {
                            return Some((
                                Err(MekaError::StreamInterrupted {
                                    reason: error.to_string(),
                                }),
                                (events, true),
                            ));
                        }
                        Some(Ok(frame)) => match sse::parse(&frame.event, &frame.data) {
                            Ok(None) => continue,
                            Ok(Some(event)) => {
                                let terminal = event.is_terminal();
                                return Some((Ok(event), (events, terminal)));
                            }
                            Err(error) => {
                                return Some((
                                    Err(MekaError::StreamInterrupted {
                                        reason: format!("event {:?}: {error}", frame.event),
                                    }),
                                    (events, true),
                                ));
                            }
                        },
                    }
                }
            },
        );
        Ok(stream)
    }

    /// Run a turn to completion, handing every event to `observer`.
    ///
    /// Applies the configured turn timeout around the whole stream. On timeout the turn is
    /// cancelled server-side so meka is not left burning provider tokens for a stream nobody is
    /// reading.
    pub async fn run_turn<F>(
        &self,
        session_id: Uuid,
        message: &str,
        mut observer: F,
    ) -> Result<TurnOutcome>
    where
        F: FnMut(&TurnEvent) + Send,
    {
        let stream = self.open_turn(session_id, message).await?;
        let drive = async {
            let mut stream = Box::pin(stream);
            while let Some(event) = stream.next().await {
                let event = event?;
                observer(&event);
                match event {
                    TurnEvent::Finished {
                        stop_reason,
                        refusal_text,
                        usage,
                    } => {
                        return Ok(TurnOutcome::Finished {
                            stop_reason,
                            refusal_text,
                            usage,
                        });
                    }
                    TurnEvent::Cancelled { reason } => {
                        return Ok(TurnOutcome::Cancelled { reason });
                    }
                    TurnEvent::Failed { error } => {
                        let problem: ProblemDetail =
                            serde_json::from_value(error).unwrap_or_default();
                        return Err(MekaError::Problem(problem));
                    }
                    _ => {}
                }
            }
            Err(MekaError::StreamInterrupted {
                reason: "the stream ended without a terminal event".to_string(),
            })
        };

        match tokio::time::timeout(self.turn_timeout, drive).await {
            Ok(result) => result,
            Err(_elapsed) => {
                if let Err(error) = self.cancel_turn(session_id).await {
                    tracing::warn!(
                        "turn timed out and the follow-up cancel also failed: {}",
                        error
                    );
                }
                Err(MekaError::Timeout(self.turn_timeout))
            }
        }
    }
}

/// Decode a successful JSON body, or convert an error response into a [`MekaError::Problem`].
async fn decode<T: serde::de::DeserializeOwned>(response: reqwest::Response) -> Result<T> {
    if !response.status().is_success() {
        return Err(problem_from(response).await);
    }
    let bytes = response.bytes().await?;
    serde_json::from_slice(&bytes).map_err(|error| {
        MekaError::Decode(format!(
            "{error}; body was {}",
            String::from_utf8_lossy(&bytes)
                .chars()
                .take(256)
                .collect::<String>()
        ))
    })
}

/// Turn an error response into a [`MekaError`], preferring the Problem Detail body when meka sent
/// one and falling back to the raw text when it did not (a reverse proxy returning 502 HTML, say).
async fn problem_from(response: reqwest::Response) -> MekaError {
    let status = response.status().as_u16();
    let bytes = match response.bytes().await {
        Ok(bytes) => bytes,
        Err(error) => return MekaError::Transport(error),
    };
    match serde_json::from_slice::<ProblemDetail>(&bytes) {
        Ok(mut problem) if !problem.type_uri.is_empty() => {
            if problem.status == 0 {
                problem.status = status;
            }
            MekaError::Problem(problem)
        }
        _ => MekaError::UnexpectedStatus {
            status,
            body: String::from_utf8_lossy(&bytes)
                .chars()
                .take(256)
                .collect::<String>(),
        },
    }
}

/// Exponential backoff with jitter: 250ms, 500ms, 1s, 2s, capped at 8s.
///
/// The jitter matters because the bridge and meka restart together under systemd, and a fixed
/// schedule would have every retry land in lockstep.
async fn backoff(attempt: u32) {
    use rand::RngExt as _;

    let base = Duration::from_millis(250) * 2_u32.saturating_pow(attempt.min(5));
    let capped = base.min(Duration::from_secs(8));
    let jitter = rand::rng().random_range(0..=(capped.as_millis() / 4) as u64);
    tokio::time::sleep(capped + Duration::from_millis(jitter)).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn problem(type_uri: &str) -> ProblemDetail {
        ProblemDetail {
            type_uri: type_uri.to_string(),
            title: "t".to_string(),
            status: 409,
            detail: "d".to_string(),
            instance: None,
        }
    }

    #[test]
    fn problem_kinds_route_on_the_type_uri() {
        assert_eq!(
            problem("https://meka.so/errors/session-not-found").kind(),
            ProblemKind::SessionNotFound
        );
        assert_eq!(
            problem("https://meka.so/errors/turn-in-flight").kind(),
            ProblemKind::TurnInFlight
        );
        assert_eq!(
            problem("https://meka.so/errors/provider").kind(),
            ProblemKind::Provider
        );
        assert_eq!(
            problem("https://example.com/errors/new").kind(),
            ProblemKind::Other
        );
    }

    #[test]
    fn session_missing_is_detected() {
        let error = MekaError::Problem(problem("https://meka.so/errors/session-not-found"));
        assert!(error.is_session_missing());
        let other = MekaError::Problem(problem("https://meka.so/errors/auth"));
        assert!(!other.is_session_missing());
    }

    #[test]
    fn retryable_classification_covers_transient_failures() {
        assert!(MekaError::Problem(problem("https://meka.so/errors/provider")).is_retryable());
        assert!(
            MekaError::Problem(problem("https://meka.so/errors/concurrency-limit")).is_retryable()
        );
    }

    #[test]
    fn permanent_failures_are_not_retried() {
        // Retrying an auth or validation failure just burns time; both need operator action.
        assert!(!MekaError::Problem(problem("https://meka.so/errors/auth")).is_retryable());
        assert!(!MekaError::Problem(problem("https://meka.so/errors/invalid-body")).is_retryable());
        assert!(
            !MekaError::Problem(problem("https://meka.so/errors/session-not-found")).is_retryable()
        );
        assert!(!MekaError::Timeout(Duration::from_secs(1)).is_retryable());
        // A dropped stream is not retried either: the turn is still running, so the caller waits
        // for the session to go idle rather than submitting a duplicate.
        assert!(
            !MekaError::StreamInterrupted {
                reason: "reset".to_string()
            }
            .is_retryable()
        );
    }

    #[test]
    fn server_info_tolerates_a_null_model() {
        // meka serializes `model` unconditionally and it is an `Option` on its side, so a server
        // with no model configured sends an explicit null.
        let info: ServerInfo =
            serde_json::from_str(r#"{"version":"0.36.0","model":null,"vision":true}"#)
                .expect("a null model must deserialize");
        assert_eq!(info.model, None);
        assert!(info.vision);
    }

    #[test]
    fn problem_detail_displays_title_status_and_detail() {
        let rendered = problem("https://meka.so/errors/auth").to_string();
        assert_eq!(rendered, "t (409): d");
    }
}
