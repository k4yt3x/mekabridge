//! Client for meka's HTTP API.
//!
//! Turns are always submitted with `stream: true`. The bridge does not relay deltas to anyone, but
//! streaming buys three things a blocking POST cannot: liveness on a turn that runs for minutes
//! (which matters behind proxies with read timeouts), visibility of tool calls in the log, and a
//! clean terminal event instead of a connection that may or may not still be alive.
//!
//! Retries are deliberately asymmetric. Read-only endpoints retry what [`MekaError::is_retryable`]
//! admits, which is meka's own error taxonomy by `type` URI rather than by status code, plus a bare
//! 429 or 5xx from whatever sits between the two processes. Session creation and turn submission
//! never retry: a retried `POST /turn` can bill a second time and send a second round of messages,
//! so turn-level retry belongs to the queue's attempt counter in [`crate::store`], where it is
//! bounded, spaced out, and observable.

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

/// How many separate drops one turn may ride out before its outcome is called unknown.
///
/// Generous because each rejoin is one cheap request against a turn that is still running and still
/// costing provider tokens, and because a rejoin that succeeds is the bridge working rather than
/// struggling: a half-hour turn over a domestic connection can legitimately need several. Bounded
/// at all only because a stream that dies the instant it opens would otherwise spin. The turn
/// timeout is the real backstop.
const MAX_REJOINS: u32 = 20;

/// How many requests may be spent getting back on after a single drop.
///
/// This is the budget that has to fit inside meka's `[serve].stream_reattach_grace`, since none of
/// these attempts is resetting that clock. At [`REJOIN_RETRY_DELAY`] apart it spends a few seconds
/// against a default of thirty.
const MAX_REJOIN_ATTEMPTS: u32 = 5;

/// Longest silence tolerated on an open response before it is treated as dead.
///
/// Applies to the gap between reads, not to the whole response, so a turn that runs for half an
/// hour is unaffected: meka sends a keep-alive comment every twenty seconds on both SSE endpoints,
/// and this is three of those. It is what turns a connection that stopped speaking without closing
/// into an error the rejoin can act on.
const STREAM_READ_TIMEOUT: Duration = Duration::from_secs(60);

/// Ceiling on one request that is not a stream.
///
/// Every one of these is a small JSON round trip that meka answers immediately or not at all, so
/// the generous figure is about a loaded server rather than a slow reply. Separate from
/// [`STREAM_READ_TIMEOUT`] because that one only bounds silence: a peer trickling a byte a minute
/// would satisfy it forever, and `doctor` should not hang on that.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// How long to wait before asking again when the rejoin request itself fails.
///
/// Small deliberately. The whole per-drop budget has to fit inside `[serve].stream_reattach_grace`,
/// which meka defaults to thirty seconds.
const REJOIN_RETRY_DELAY: Duration = Duration::from_secs(1);

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
    /// Seconds the upstream asked the caller to wait, as an RFC 9457 extension member.
    ///
    /// meka sends this on the 429s it raises itself, its concurrency limit and its idempotency
    /// cap. What it does *not* send it on is the case that matters most: `RetryableProvider`
    /// carries the provider's own `Retry-After` and then drops it at the boundary that turns
    /// an error into a Problem Detail, so a rate limit arrives here with the one number that
    /// is not a guess already discarded.
    pub retry_after: Option<f64>,
}

impl ProblemDetail {
    /// Classify the stable `type` URI. meka's docs are explicit that clients should route on this
    /// rather than on the status code or the human-readable detail.
    pub fn kind(&self) -> ProblemKind {
        match self.type_uri.rsplit('/').next().unwrap_or_default() {
            "auth" => ProblemKind::Auth,
            "auth-scope" => ProblemKind::AuthScope,
            "session-not-found" => ProblemKind::SessionNotFound,
            "not-found" => ProblemKind::NotFound,
            "session-locked" => ProblemKind::SessionLocked,
            "turn-in-flight" => ProblemKind::TurnInFlight,
            "turn-cancelled" => ProblemKind::TurnCancelled,
            "concurrency-limit" => ProblemKind::ConcurrencyLimit,
            "sse-lag" => ProblemKind::SseLag,
            "stream-detached" => ProblemKind::StreamDetached,
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
    /// Something other than a session was not there: for this bridge, a turn stream with nothing
    /// joinable. Distinct from [`Self::SessionNotFound`], which is the cue to build a new session.
    NotFound,
    SessionLocked,
    TurnInFlight,
    TurnCancelled,
    ConcurrencyLimit,
    SseLag,
    /// A rejoined stream ended with no outcome recorded for the turn. Arrives only inside a
    /// terminal `turn.failed`, never as an HTTP response.
    StreamDetached,
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

    /// The turn was accepted, the connection failed, and rejoining it did not work either.
    ///
    /// Every stream-level failure lands here because the stream only exists after the submission
    /// was accepted, so this never means "the turn never happened". What it does *not* mean any
    /// more is that the turn will *finish*. meka once let a disconnected turn run to completion; it
    /// now stops the agent loop once its stream has had no subscriber for
    /// `[serve].stream_reattach_grace`. That check happens at a provider-round boundary rather than
    /// on a clock, so a turn sitting inside one long tool call is not stopped until the call
    /// returns. Either way the turn is normally still running and still able to send when this is
    /// returned, which is why the rejoin gives up by cancelling it rather than leaving it be.
    /// Treating a session that goes idle afterwards as one whose turn finished is how a message
    /// gets marked delivered without ever having been answered.
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
    ///
    /// `provider` is deliberately absent. meka maps both its `Provider` and `InvalidRequest`
    /// variants onto that type URI and both are its explicitly non-retryable bucket: by the time a
    /// request is malformed enough for the upstream to reject it, meka's own agent loop has already
    /// tried to repair it. What a rate limit or an overload becomes is `internal`, because meka's
    /// `RetryableProvider` has no arm of its own in the mapping and falls through.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Transport(error) => {
                error.is_timeout() || error.is_connect() || error.is_request()
            }
            Self::Problem(problem) => match problem.kind() {
                ProblemKind::ConcurrencyLimit
                | ProblemKind::Internal
                | ProblemKind::SessionLocked
                // meka emits this when its broadcast to *this* client fell behind, then cancels
                // the turn and says "Retry the turn" in the detail: nothing about it says the work
                // would fail again. Classified here for callers that ask, but the turn path does
                // not reach this. It marks the counters incomplete first, on the grounds that the
                // events meka dropped may have included a send, and a turn that may have acted is
                // closed out rather than offered again.
                | ProblemKind::SseLag
                // The turn ran and meka cannot say how it ended. Worth another attempt for the
                // same reason a dropped stream is: the alternative is calling a message delivered
                // on the strength of not knowing.
                | ProblemKind::StreamDetached => true,
                // A `type` this build has never heard of, where the status is the only thing left
                // to go on. Reading those as permanent is a trap rather than a conservative
                // default: this bridge asks meka to give `RetryableProvider` an arm of its own
                // instead of letting it fall through to `internal`, and the natural way to grant
                // that is a new URI. Landing it would otherwise turn every rate limit into a
                // message given up on at the first attempt, which is the fault the whole retry
                // budget exists to prevent.
                ProblemKind::Other => problem.status == 429 || (500..600).contains(&problem.status),
                _ => false,
            },
            // No Problem Detail body, so nothing to route on but the status. This is what a reverse
            // proxy in front of meka produces: nginx's 503 while meka restarts, or a 429 from
            // whatever is metering the hop. Treating those as permanent meant `doctor` and `status`
            // giving up on the first hiccup.
            Self::UnexpectedStatus { status, .. } => *status == 429 || (500..600).contains(status),
            // A body that would not parse, which is a truncated or garbled response rather than a
            // considered refusal. It is also what a failure of the bridge's *own* database is
            // laundered into on the way out of `ensure_session`, and a busy SQLite file is the most
            // transient thing here: calling that permanent would declare a message undeliverable
            // over a lock that cleared a second later.
            Self::Decode(_) => true,
            _ => false,
        }
    }

    /// How long the upstream asked the caller to wait, when it said.
    ///
    /// `try_from` rather than `from`: the plain conversion panics on a value too large for a
    /// `Duration`, and a JSON number is whatever the other side put there.
    pub fn retry_after(&self) -> Option<Duration> {
        match self {
            Self::Problem(problem) => problem
                .retry_after
                .filter(|seconds| *seconds >= 0.0)
                .and_then(|seconds| Duration::try_from_secs_f64(seconds).ok()),
            _ => None,
        }
    }

    /// Whether meka says the session id the bridge holds no longer exists, which is the cue to
    /// create a replacement.
    pub fn is_session_missing(&self) -> bool {
        matches!(self, Self::Problem(problem) if problem.kind() == ProblemKind::SessionNotFound)
    }

    /// Whether meka says there is no turn stream to join on a session that does exist.
    ///
    /// The session is fine; the turn's stream is simply over. Worth telling apart from a rejoin
    /// that failed on the way, because there is nothing left to cancel.
    pub fn is_stream_missing(&self) -> bool {
        matches!(self, Self::Problem(problem) if problem.kind() == ProblemKind::NotFound)
    }

    /// Whether meka rejected the submission because a turn is already running on the session.
    ///
    /// This can only happen before the turn is accepted, so the batch was never handed over.
    pub fn is_turn_in_flight(&self) -> bool {
        matches!(self, Self::Problem(problem) if problem.kind() == ProblemKind::TurnInFlight)
    }

    /// Whether the turn was accepted and its outcome is unknown rather than failed.
    ///
    /// The turn ran. Whether it finished, and what it did before the connection went, cannot be
    /// established from here: [`MekaClient::run_turn`] has already tried the one thing that would
    /// have answered it, which is rejoining the stream. Resubmitting may duplicate work the agent
    /// already did; not resubmitting may drop a message nobody ever answered.
    ///
    /// `stream-detached` is meka saying the same thing in its own words: a rejoin succeeded, the
    /// stream ended, and the task that would have recorded an outcome is gone. Classifying it by
    /// its status instead happens to land in the same place today, and would stop doing so the
    /// moment meka decided that a turn nobody can report on is a 4xx.
    pub fn turn_outcome_unknown(&self) -> bool {
        match self {
            Self::StreamInterrupted { .. } => true,
            Self::Problem(problem) => problem.kind() == ProblemKind::StreamDetached,
            _ => false,
        }
    }

    /// Whether this error means events went missing from the caller's view of the stream.
    ///
    /// meka says so outright with `sse-lag`: its broadcast to this client overran and it names how
    /// many events were dropped before cancelling the turn. Anything counted off that stream is a
    /// floor rather than a total, so a caller asking "did the agent already send something" cannot
    /// read its own zero as an answer.
    pub fn dropped_events(&self) -> bool {
        matches!(self, Self::Problem(problem) if problem.kind() == ProblemKind::SseLag)
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
    /// The bridge attaches nothing to a turn, so this gates `view_attachment` instead. Worth being
    /// precise about why, because the obvious reason is wrong: meka checks `vision` only on the
    /// paths that bring an image *in*, and forwards an MCP tool result's image block to the
    /// provider whatever the setting says. So this check is not belt and braces over one of
    /// meka's, it is the only thing standing between a non-vision profile and an image block
    /// committed to the session's history, which the provider then rejects on every later
    /// request in that session.
    pub vision: bool,
    /// The permission levels this meka will create a session at, from `[permissions].enabled`.
    ///
    /// Asking for one outside the set is a 422 at session creation, which happens on the first
    /// message rather than at startup, so without this the misconfiguration surfaces as a message
    /// that never gets answered. `ask` is the one to watch: it is not in meka's default set.
    ///
    /// Defaulted rather than required, so an older meka that does not send it reads as "no
    /// opinion" and the check is skipped instead of taking `doctor` down.
    #[serde(default)]
    pub enabled_permissions: Vec<String>,
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
            // per-turn budget is applied around the stream instead. `read_timeout` is the one that
            // works on a stream, because it bounds the gap between reads rather than the whole
            // response, and meka sends a keep-alive comment every twenty seconds on both of its SSE
            // endpoints. Without it a connection that goes silent without closing -- a NAT dropping
            // state, a machine pulled off the network -- is indistinguishable from a turn thinking,
            // and the drain loop waits out the entire turn budget for a stream that will never
            // speak again.
            .read_timeout(STREAM_READ_TIMEOUT)
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
            .timeout(REQUEST_TIMEOUT)
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
            .timeout(REQUEST_TIMEOUT)
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
    ///
    /// The only endpoint here whose failure status carries an answer rather than an error. meka
    /// replies 503 with the same `ReadyStatus` body it sends on 200, naming which subsystem is the
    /// blocker, and not a Problem Detail. Handing that to the shared decode path turned the one
    /// response worth reading into "unexpected body", retried three times on the way, so `doctor`
    /// could report that the probe had failed but never which dependency was down.
    pub async fn ready(&self) -> Result<ReadyStatus> {
        let url = self.endpoint("/v1/health/ready")?;
        let mut attempt = 0;
        loop {
            let result = async {
                let response = self
                    .http
                    .get(url.clone())
                    .bearer_auth(self.token.expose())
                    .timeout(REQUEST_TIMEOUT)
                    .send()
                    .await?;
                if response.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE {
                    let body = response.text().await?;
                    // A 503 that is not meka's own answer came from something in between, a proxy
                    // while meka restarts, and is the transient case worth retrying.
                    //
                    // Told apart by the two words meka can put in `status`, not merely by it being
                    // present. Every field here defaults, so requiring a non-empty string admitted
                    // any JSON object carrying one: a load balancer's
                    // `{"status":"Service Unavailable","message":...}` parsed into all three flags
                    // false, and `doctor` reported a fabricated "session database unreachable" and
                    // exited non-zero on the one command run to find out what is actually wrong.
                    return match serde_json::from_str::<ReadyStatus>(&body) {
                        Ok(ready) if ready.status == "ok" || ready.status == "degraded" => {
                            Ok(ready)
                        }
                        _ => Err(MekaError::UnexpectedStatus {
                            status: 503,
                            // Capped like every other error body. A proxy answering with a full
                            // HTML page would otherwise carry the whole thing into the log line.
                            body: body.chars().take(256).collect(),
                        }),
                    };
                }
                decode(response).await
            }
            .await;
            match result {
                Ok(value) => return Ok(value),
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

    /// Stop a turn this call has given up on, best effort.
    ///
    /// Every give-up path goes through here. Left alone the turn keeps running, keeps spending
    /// provider tokens, and keeps able to send messages nothing on this side will account for, so
    /// trying and failing is strictly better than not trying; the call is idempotent server-side.
    ///
    /// The hazard, in one place rather than repeated at each caller: **meka scopes cancellation to
    /// the session, not to the turn.** Whatever token that session currently holds is the one that
    /// fires, and meka's own scheduled work publishes into the same slot. Each call here is made
    /// while this bridge has good reason to believe its turn is the one running, which is nearly
    /// certain the instant a stream dies and less so the longer giving up took. A cancel that lands
    /// after meka has moved on stops something the bridge never started. Nothing in meka's API can
    /// currently tell the two apart; a `turn_id` on the cancel endpoint would.
    async fn abandon_turn(&self, session_id: Uuid, why: &str) {
        if let Err(error) = self.cancel_turn(session_id).await {
            tracing::warn!("{why}, and the turn could not be cancelled: {error}");
        }
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
                .timeout(REQUEST_TIMEOUT)
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

    async fn get_with_retries<T: serde::de::DeserializeOwned>(&self, url: Url) -> Result<T> {
        let mut attempt = 0;
        loop {
            let result = async {
                let response = self
                    .http
                    .get(url.clone())
                    .bearer_auth(self.token.expose())
                    .timeout(REQUEST_TIMEOUT)
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
    ) -> Result<impl Stream<Item = Result<StreamItem>> + Send + use<>> {
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
        Ok(events_from(response))
    }

    /// `GET /v1/sessions/{id}/stream`: rejoin the turn already running on this session.
    ///
    /// Two things happen at once here, and the second is the reason this exists. meka replays what
    /// was missed after `resume_from` and then follows the live turn, so the caller learns how the
    /// turn actually ended rather than inferring it. And *being subscribed at all* is what keeps
    /// the turn alive: meka stops the agent loop once its stream has had no subscriber for
    /// `[serve].stream_reattach_grace`, on the reasoning that nobody is listening. A client that
    /// answers a dropped connection by polling instead of rejoining is the case that reasoning
    /// describes, so it gets its turn killed roughly thirty seconds later.
    ///
    /// The resumed stream opens with a synthesised `turn.started` carrying no id. That is
    /// deliberate on meka's side, so a resume cannot move the caller's position backwards before
    /// the replay has run, and it is why [`StreamItem::id`] is an `Option`.
    async fn reattach_turn(
        &self,
        session_id: Uuid,
        resume_from: Option<u64>,
    ) -> Result<impl Stream<Item = Result<StreamItem>> + Send + use<>> {
        let url = self.endpoint(&format!("/v1/sessions/{session_id}/stream"))?;
        let mut request = self.http.get(url).bearer_auth(self.token.expose());
        if let Some(resume_from) = resume_from {
            request = request.header("Last-Event-ID", resume_from.to_string());
        }
        let response = request.send().await?;
        if !response.status().is_success() {
            return Err(problem_from(response).await);
        }
        Ok(events_from(response))
    }

    /// Run a turn to completion, handing every event to `observer`.
    ///
    /// Applies the configured turn timeout around the whole stream. On timeout the turn is
    /// cancelled server-side so meka is not left burning provider tokens for a stream nobody is
    /// reading.
    ///
    /// A connection that drops partway is rejoined rather than abandoned, a bounded number of
    /// times. That is not only about learning how the turn ended: meka stops a turn whose stream
    /// has had no subscriber for `[serve].stream_reattach_grace`, checked when the agent loop
    /// next comes round, so a client that gives up on the connection is also giving up on the
    /// turn, and would then be told the session had gone idle as though the work had finished.
    /// `observer` sees the replayed events exactly once, because the rejoin resumes strictly
    /// after the last id already delivered.
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
            let mut stream = Box::pin(stream)
                as std::pin::Pin<Box<dyn Stream<Item = Result<StreamItem>> + Send>>;
            let mut resume_from: Option<u64> = None;
            let mut rejoins = 0_u32;
            // The turn this call is following. meka retains only the most recent turn's stream, so
            // a rejoin that lands after a newer one started -- a scheduled job, or a backgrounded
            // tool call delivering its outcome -- hands back *that* turn instead. Its ids are all
            // above `resume_from`, being session-scoped, so nothing is filtered and its events
            // would be counted as this turn's and its terminal read as this turn's outcome. meka
            // documents the id on the re-issued `turn.started` as the only way to tell.
            let mut following: Option<String> = None;
            loop {
                let item = match stream.next().await {
                    Some(Ok(item)) => item,
                    // A frame this build cannot parse is a contract mismatch, not a dropped
                    // connection. Rejoining replays the same frame out of the ring and fails
                    // identically every time, so it is reported rather than retried.
                    Some(Err(MekaError::Decode(reason))) => {
                        // Stopped for the same reason the other give-up paths stop it: the turn is
                        // still running and can still send messages nothing here will account for.
                        // This was the one exit that walked away and left it going.
                        self.abandon_turn(session_id, "a frame could not be parsed")
                            .await;
                        return Err(MekaError::Decode(reason));
                    }
                    other => {
                        let reason = match other {
                            Some(Err(error)) => error.to_string(),
                            _ => "the stream ended without a terminal event".to_string(),
                        };
                        // Two budgets, because they bound two different things. `attempts` is the
                        // requests spent getting back on after *this* drop, which is what has to
                        // fit inside meka's reattach grace; it starts fresh each time, since a
                        // successful attach resets that clock on meka's side. `rejoins` is the
                        // drops ridden out over the whole turn, which only bounds a connection that
                        // dies the instant it opens. Sharing one counter between them meant a long
                        // turn that had been rejoined cleanly four times had no allowance left for
                        // the fifth drop, and one transient 502 there cancelled a healthy turn.
                        let mut attempts = 0_u32;
                        let resumed = loop {
                            if rejoins >= MAX_REJOINS || attempts >= MAX_REJOIN_ATTEMPTS {
                                // Left alone, the turn keeps running until its grace expires and
                                // can still send messages nobody is accounted for. This is the call
                                // most exposed to the session-scope hazard in `abandon_turn`: more
                                // time has passed here than on any other give-up path.
                                self.abandon_turn(session_id, "the rejoin budget ran out")
                                    .await;
                                return Err(MekaError::StreamInterrupted { reason });
                            }
                            attempts += 1;
                            tracing::warn!(
                                resume_from,
                                attempt = attempts,
                                rejoins,
                                "lost the turn stream ({reason}); rejoining it"
                            );
                            match self.reattach_turn(session_id, resume_from).await {
                                Ok(resumed) => {
                                    rejoins += 1;
                                    break resumed;
                                }
                                // The request itself failed on something transient. Round again;
                                // the bound at the top of this loop is what stops it.
                                Err(error) if error.is_retryable() => {
                                    tracing::warn!(
                                        "could not rejoin the turn stream ({error}); trying again"
                                    );
                                    tokio::time::sleep(REJOIN_RETRY_DELAY).await;
                                }
                                // Nothing to rejoin, or meka is unreachable. Report the
                                // interruption rather than the rejoin's own error: the caller's
                                // question is what happened to the turn, and the answer is still
                                // that it is unknown.
                                Err(error) => {
                                    tracing::warn!("could not rejoin the turn stream: {}", error);
                                    // A 404 means the stream is already over, so there is nothing
                                    // left to stop, and cancelling anyway is not free: meka scopes
                                    // cancel to the session rather than to a turn, so it would fire
                                    // at whatever that session is running now, which may be a
                                    // scheduled job this bridge never started.
                                    if !error.is_stream_missing() {
                                        self.abandon_turn(session_id, "the rejoin was refused")
                                            .await;
                                    }
                                    return Err(MekaError::StreamInterrupted { reason });
                                }
                            }
                        };
                        stream = Box::pin(resumed);
                        continue;
                    }
                };
                if let TurnEvent::Started { turn_id, .. } = &item.event
                    && !turn_id.is_empty()
                {
                    match &following {
                        None => following = Some(turn_id.clone()),
                        Some(following) if following != turn_id => {
                            return Err(MekaError::StreamInterrupted {
                                reason: format!(
                                    "rejoined turn {turn_id} rather than {following}, so the \
                                     stream for this turn is gone"
                                ),
                            });
                        }
                        Some(_) => {}
                    }
                }
                observer(&item.event);
                // Only after the event has been handed over, and only when it carries one. `max`
                // rather than assignment because ids arriving in order is a property of meka's
                // emitter rather than something guaranteed at this end, and resuming from anything
                // lower than what the observer has seen would replay it.
                if let Some(id) = item.id {
                    resume_from = Some(resume_from.map_or(id, |last| last.max(id)));
                }
                match item.event {
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
        };

        match tokio::time::timeout(self.turn_timeout, drive).await {
            Ok(result) => result,
            Err(_elapsed) => {
                self.abandon_turn(session_id, "the turn ran past its budget")
                    .await;
                Err(MekaError::Timeout(self.turn_timeout))
            }
        }
    }
}

/// One event off a turn's SSE stream, with the id needed to resume after it.
#[derive(Debug, Clone, PartialEq)]
pub struct StreamItem {
    /// `None` only until the first id of a connection has been seen.
    ///
    /// Not "whenever meka omits one", which is the obvious reading and is wrong:
    /// `eventsource-stream` implements the spec's persistent last-event-ID buffer, so an event
    /// sent without an `id:` line is reported carrying the previous one. meka does send
    /// several that way, including the synthesised `turn.started` that opens a resumed stream
    /// and the notice about a replay hole. Inheriting the previous id is harmless here, since
    /// resuming from it is idempotent.
    pub id: Option<u64>,
    pub event: TurnEvent,
}

/// Turn an SSE response body into parsed events, tagged with their ids.
///
/// Shared by the initial submission and the rejoin, which differ only in how the response was
/// obtained: the wire format either side of a dropped connection is the same stream.
fn events_from(
    response: reqwest::Response,
) -> impl Stream<Item = Result<StreamItem>> + Send + use<> {
    let events = response.bytes_stream();
    futures::stream::unfold(
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
                            let id = frame.id.trim().parse::<u64>().ok();
                            return Some((Ok(StreamItem { id, event }), (events, terminal)));
                        }
                        // `Decode` rather than `StreamInterrupted`: a frame this build cannot
                        // parse is a contract mismatch, not a lost connection, and the difference
                        // decides what happens next. Rejoining replays the same frame out of meka's
                        // ring and fails on it identically every time.
                        Err(error) => {
                            return Some((
                                Err(MekaError::Decode(format!(
                                    "event {:?}: {error}",
                                    frame.event
                                ))),
                                (events, true),
                            ));
                        }
                    },
                }
            }
        },
    )
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
            retry_after: None,
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
        assert!(
            MekaError::Problem(problem("https://meka.so/errors/concurrency-limit")).is_retryable()
        );
        // Where a provider rate limit or overload lands once meka's own retries are spent: its
        // `RetryableProvider` has no arm in the Problem Detail mapping and falls through to this.
        assert!(MekaError::Problem(problem("https://meka.so/errors/internal")).is_retryable());
    }

    #[test]
    fn the_read_timeout_leaves_room_for_mekas_keep_alive() {
        // meka sends a keep-alive comment every twenty seconds on both SSE endpoints, and that is
        // the only traffic a turn spending ten minutes inside one tool call produces. Set at or
        // under that interval this stops being a liveness check and becomes a timer that kills
        // every long turn, which would look exactly like a flaky network rather than a
        // config mistake.
        const MEKA_KEEP_ALIVE: Duration = Duration::from_secs(20);
        assert!(
            STREAM_READ_TIMEOUT >= MEKA_KEEP_ALIVE * 2,
            "a read timeout of {STREAM_READ_TIMEOUT:?} gives meka's {MEKA_KEEP_ALIVE:?} keep-alive \
             no margin"
        );
        // The rejoin budget has to fit inside meka's reattach grace, which defaults to thirty
        // seconds, or the bridge spends the whole window it is trying to beat.
        const MEKA_REATTACH_GRACE: Duration = Duration::from_secs(30);
        assert!(
            REJOIN_RETRY_DELAY * MAX_REJOIN_ATTEMPTS < MEKA_REATTACH_GRACE,
            "the per-drop rejoin budget outlives the turn it is trying to catch"
        );
    }

    #[test]
    fn a_lagged_stream_admits_its_own_counters_are_short() {
        // meka drops events out of one client's view and names how many before cancelling the turn.
        // The ones it drops can be the `tool_call.executing` for a send, so a caller counting sends
        // off that stream has to know its total is a floor. Only `sse-lag` says this; the ordinary
        // failures leave the counting intact.
        assert!(MekaError::Problem(problem("https://meka.so/errors/sse-lag")).dropped_events());
        assert!(!MekaError::Problem(problem("https://meka.so/errors/internal")).dropped_events());
        assert!(
            !MekaError::StreamInterrupted {
                reason: "reset".to_string()
            }
            .dropped_events()
        );
    }

    #[test]
    fn a_detached_stream_is_an_unknown_outcome_rather_than_a_failure() {
        // meka's own words for it are "the turn's stream closed without recording an outcome",
        // which is the same thing a dropped connection leaves behind. Routing it by status instead
        // lands in the same place only for as long as the status stays 5xx.
        assert!(
            MekaError::Problem(problem("https://meka.so/errors/stream-detached"))
                .turn_outcome_unknown()
        );
        assert!(
            !MekaError::Problem(problem("https://meka.so/errors/internal")).turn_outcome_unknown()
        );
    }

    #[test]
    fn a_missing_stream_is_told_apart_from_a_missing_session() {
        // Different remedies. No session means build one; no stream means the turn is simply over,
        // and cancelling anyway would fire at whatever that session is running now, which meka
        // scopes per session rather than per turn.
        let missing_stream = MekaError::Problem(problem("https://meka.so/errors/not-found"));
        assert!(missing_stream.is_stream_missing());
        assert!(!missing_stream.is_session_missing());
        let missing_session =
            MekaError::Problem(problem("https://meka.so/errors/session-not-found"));
        assert!(!missing_session.is_stream_missing());
        assert!(missing_session.is_session_missing());
    }

    #[test]
    fn a_type_this_build_has_never_heard_of_falls_back_to_the_status() {
        // Forward compatibility with the very change this bridge asks meka for. Its
        // `RetryableProvider` currently has no arm in the mapping onto a Problem Detail and lands
        // on `internal`; giving it one means a new URI, and reading an unknown URI as permanent
        // would turn every rate limit into a message abandoned on the first attempt the day that
        // improvement shipped.
        let retryable = |status| {
            let mut detail = problem("https://meka.so/errors/retryable-provider");
            detail.status = status;
            MekaError::Problem(detail).is_retryable()
        };
        assert!(retryable(429));
        assert!(retryable(503));
        assert!(retryable(529), "Anthropic's overloaded status");
        // An unknown type on a 4xx is still somebody's mistake to fix rather than a wait.
        assert!(!retryable(403));
        assert!(!retryable(422));
    }

    #[test]
    fn a_bare_status_from_something_between_the_two_processes_is_retried() {
        // A reverse proxy answering while meka restarts sends HTML, not RFC 9457, so there is no
        // `type` to route on. Reading those as permanent had `doctor` and `status` give up on the
        // first hiccup.
        for status in [429, 500, 502, 503, 504] {
            assert!(
                MekaError::UnexpectedStatus {
                    status,
                    body: "<html>".to_string()
                }
                .is_retryable(),
                "status {status} should be retryable"
            );
        }
        assert!(
            !MekaError::UnexpectedStatus {
                status: 404,
                body: "<html>".to_string()
            }
            .is_retryable()
        );
    }

    #[test]
    fn a_retry_after_hint_is_read_when_meka_sends_one() {
        let mut detail = problem("https://meka.so/errors/internal");
        detail.retry_after = Some(45.0);
        assert_eq!(
            MekaError::Problem(detail).retry_after(),
            Some(Duration::from_secs(45))
        );
        assert_eq!(
            MekaError::Problem(problem("https://meka.so/errors/internal")).retry_after(),
            None
        );
        // Nonsense rather than an instruction to wait forever. The last two matter more than they
        // look: `Duration::from_secs_f64` *panics* on a value it cannot represent, and this reads
        // whatever number the other side put in a JSON body.
        for nonsense in [-1.0, f64::NAN, f64::INFINITY, 1e300] {
            let mut detail = problem("https://meka.so/errors/internal");
            detail.retry_after = Some(nonsense);
            assert_eq!(
                MekaError::Problem(detail).retry_after(),
                None,
                "{nonsense} must not be read as a wait"
            );
        }
    }

    #[test]
    fn a_concurrency_limit_carries_a_retry_after_today() {
        // Contradicting an earlier comment here that said meka never sends this. `TurnGuard`
        // answers its 429 with `with_retry_after(1)`, which sets both the header and a
        // `retry_after` body extension that flattens to the top level. The value is an
        // integer there, so the field has to tolerate one.
        let body = r#"{"type":"https://meka.so/errors/concurrency-limit",
                       "title":"Too many concurrent turns","status":429,
                       "detail":"retry shortly","retry_after":1}"#;
        let detail: ProblemDetail = serde_json::from_str(body).expect("parses");
        assert_eq!(
            MekaError::Problem(detail).retry_after(),
            Some(Duration::from_secs(1))
        );
    }

    #[test]
    fn permanent_failures_are_not_retried() {
        // Retrying an auth or validation failure just burns time; both need operator action.
        assert!(!MekaError::Problem(problem("https://meka.so/errors/auth")).is_retryable());
        assert!(!MekaError::Problem(problem("https://meka.so/errors/invalid-body")).is_retryable());
        // meka's non-retryable upstream bucket, despite the name reading like a transient fault:
        // both `Provider` and `InvalidRequest` map onto this URI and the agent loop has already
        // tried to repair what it could.
        assert!(!MekaError::Problem(problem("https://meka.so/errors/provider")).is_retryable());
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
