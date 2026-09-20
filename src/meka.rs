//! Client for meka's HTTP API.
//!
//! Two doors matter here. Messages go in through the session inbox, `POST /v1/sessions/{id}/inbox`,
//! which answers the moment the item is durable on meka's side and never refuses for a turn in
//! flight. What became of them comes back on the session feed, `GET /v1/sessions/{id}/stream`,
//! one connection per session that carries every turn's events, whoever started the turn, and does
//! not end of its own accord.
//!
//! Retries are deliberately asymmetric. Read-only endpoints retry what [`MekaError::is_retryable`]
//! admits, which is meka's own error taxonomy by `type` URI rather than by status code, plus a bare
//! 429 or 5xx from whatever sits between the two processes. Session creation and an inbox post
//! never retry here: the post is retried by the session task, where the schedule is written down
//! and observable, and it carries an `Idempotency-Key` so a retry after an ambiguous failure finds
//! the item it already made rather than making a second.

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

/// Longest silence tolerated on the open feed before it is treated as dead.
///
/// Bounds the gap between reads, not the whole response, so a feed held open for days is
/// unaffected: meka sends a keep-alive comment every twenty seconds and this is three of those. It
/// is what turns a connection that stopped speaking without closing into an error the reader can
/// reconnect on.
const STREAM_READ_TIMEOUT: Duration = Duration::from_secs(60);

/// Ceiling on one request that is not a stream.
///
/// Separate from [`STREAM_READ_TIMEOUT`] because that one only bounds silence: a peer trickling a
/// byte a minute would satisfy it forever, and `doctor` should not hang on that.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// How much of the upstream's own response is repeated when an error is rendered.
///
/// meka bounds what it sends at 4 KiB, which is sized for its log rather than for a chat: the
/// owner's notice carries this in the middle and appends the attempt count and the affected
/// conversations after it, so an unbounded one pushes those past a platform's length limit and into
/// a second message. The actionable part is the error type, which sits at the start, and meka's own
/// log keeps the whole thing either way.
const PROVIDER_RESPONSE_PREVIEW_CHARS: usize = 400;

/// Problem Detail (RFC 9457) as returned by every meka error response.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct ProblemDetail {
    #[serde(rename = "type")]
    pub type_uri: String,
    pub title: String,
    pub status: u16,
    pub detail: String,
    /// Seconds the upstream asked the caller to wait, as an RFC 9457 extension member.
    ///
    /// meka sends this on the 429s it raises itself, its concurrency limit and its idempotency
    /// cap, and since 0.44 on a `RetryableProvider` too, which is the case that matters most: a
    /// rate limit now arrives carrying the one number that is not a guess. It used to be dropped
    /// at the boundary that turns an error into a Problem Detail.
    ///
    /// Never the discriminator between a transient upstream failure and a permanent one, which is
    /// what [`ProblemKind::ProviderUnavailable`] is for. Most transient failures carry none: a
    /// dropped connection never received a response to hold a header, and a mid-stream overload
    /// has no headers at all.
    pub retry_after: Option<f64>,

    /// The upstream's own error text, relayed as an RFC 9457 extension member.
    ///
    /// meka's `detail` on a 502 names its server log rather than the failure, so without this the
    /// owner's notice says only that the provider refused and an operator has to go and read
    /// meka's log to learn whether a credential died or a quota ran out. Absent when the operator
    /// set `[serve] relay_provider_errors = false`, and from any meka before it had the key.
    pub provider_response: Option<String>,
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
            // Respelled in 0.46. Both are matched for the reason the SSE terminal is: an
            // unrecognised type falls to `Other`, where the status alone decides, and that decides
            // wrongly without saying so.
            "turn-canceled" | "turn-cancelled" => ProblemKind::TurnCancelled,
            "concurrency-limit" => ProblemKind::ConcurrencyLimit,
            "sse-lag" => ProblemKind::SseLag,
            "stream-detached" => ProblemKind::StreamDetached,
            "provider" => ProblemKind::Provider,
            "provider-unavailable" => ProblemKind::ProviderUnavailable,
            "context-overflow" => ProblemKind::ContextOverflow,
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
            write!(formatter, "{} ({})", self.title, self.status)?;
        } else {
            write!(
                formatter,
                "{} ({}): {}",
                self.title, self.status, self.detail
            )?;
        }
        // Appended rather than replacing `detail`, which stays meka's own sentence. This reaches
        // the owner's notice, which is the reader meka relayed it for; the affected chat is told
        // one line carrying none of it.
        if let Some(response) = &self.provider_response {
            let preview: String = response
                .chars()
                .take(PROVIDER_RESPONSE_PREVIEW_CHARS)
                .collect();
            let elided = if preview.len() < response.len() {
                "..."
            } else {
                ""
            };
            write!(formatter, ". The provider said: {preview}{elided}")?;
        }
        Ok(())
    }
}

/// Machine-readable meka error categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProblemKind {
    Auth,
    AuthScope,
    SessionNotFound,
    /// Something other than a session was not there: an inbox item, a skill, a blob. Distinct from
    /// [`Self::SessionNotFound`], which is the cue to build a new session.
    NotFound,
    SessionLocked,
    TurnInFlight,
    TurnCancelled,
    ConcurrencyLimit,
    SseLag,
    /// The feed closed under a turn with no outcome recorded for it. Arrives only inside a
    /// terminal `turn.failed`, never as an HTTP response.
    StreamDetached,
    Provider,
    /// The transient half of the same 502: a failure meka's own classifier recognised, covering an
    /// overload, a 5xx, a dropped connection and a stalled stream.
    ///
    /// Split out of [`Self::Provider`] because a relayed `Retry-After` was the only thing
    /// separating the two and most of these carry none, so the 529 a bridge sees most often
    /// arrived byte-identical to a dead credential.
    ProviderUnavailable,
    /// The conversation no longer fits the model's window and meka could not compact it far
    /// enough. A 502 like [`Self::Provider`], and deliberately a separate type: the same
    /// conversation will not fit on the next attempt either, so this is the one upstream failure
    /// where retrying is certain to waste the budget.
    ContextOverflow,
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

    /// The feed connection ended.
    ///
    /// Never an outcome in itself. The feed carries on server-side and a turn is never stopped for
    /// want of a reader, so the reader reconnects from the last id it handled and meka replays what
    /// it missed.
    #[error("the session feed ended: {reason}")]
    FeedEnded { reason: String },

    #[error("could not decode a meka response: {0}")]
    Decode(String),

    #[error("could not build the meka client: {0}")]
    Build(String),
}

impl MekaError {
    /// Whether retrying the same request could plausibly succeed.
    ///
    /// [`ProblemKind::ProviderUnavailable`] is the transient upstream failure and the one this
    /// exists for. [`ProblemKind::Provider`] is retryable too, despite reading like a refusal: meka
    /// describes it as a catch-all meaning "no reason to expect a retry to help" rather than "a
    /// retry cannot help", and asks clients not to be built so that they never retry it. The queue
    /// bounds the attempts, which is the part meka does warn against leaving open.
    ///
    /// On meka 0.44 those two shared the `provider` URI, so a bridge reading that bucket as
    /// permanent gave up on every rate limit at the first attempt. Retrying both keeps that from
    /// coming back on an older meka while the split does the work on a newer one.
    ///
    /// [`ProblemKind::ContextOverflow`] is the exception, and why meka split it out as well: it
    /// will refuse the same conversation forever.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Transport(error) => {
                error.is_timeout() || error.is_connect() || error.is_request()
            }
            Self::Problem(problem) => match problem.kind() {
                ProblemKind::ConcurrencyLimit
                | ProblemKind::Internal
                | ProblemKind::Provider
                | ProblemKind::ProviderUnavailable
                | ProblemKind::SessionLocked
                // Both arrive only as a turn's terminal on the feed, where nothing here retries
                // anything: what a hand-over is owed is decided by the inbox events, not by how
                // turn ended. Classified for the callers that ask.
                | ProblemKind::SseLag
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

    /// Whether the conversation has outgrown the model's window.
    ///
    /// Worth telling apart from every other turn failure because it is not about the message that
    /// happened to hit it. This bridge runs one permanent session, so a window the conversation no
    /// longer fits refuses the next message too, and the one after that, until somebody shortens
    /// it. Every other failure here is per-message.
    pub fn is_context_overflow(&self) -> bool {
        matches!(self, Self::Problem(problem) if problem.kind() == ProblemKind::ContextOverflow)
    }

    /// Whether another meka process holds the session, which is a wait rather than a failure.
    ///
    /// A REPL opened on the bridge's session is the ordinary way here. meka refuses an inbox post
    /// on it before anything is written, and its own driver asks a held session again every ten
    /// seconds until the holder lets go, so the post is simply made again later.
    pub fn is_session_locked(&self) -> bool {
        matches!(self, Self::Problem(problem) if problem.kind() == ProblemKind::SessionLocked)
    }
}

type Result<T> = std::result::Result<T, MekaError>;

/// Session metadata as returned by `GET /v1/sessions/{id}`.
#[derive(Debug, Clone, Deserialize)]
pub struct SessionInfo {
    pub id: Uuid,
    /// Omitted since meka 0.46 for a row that records no level, rather than sent as `""`.
    ///
    /// Never this bridge's own session, which is created naming a level, so a session that answers
    /// without one was made by another hand and there is nothing to check it against.
    #[serde(default)]
    pub permission: Option<String>,
    pub title: String,
    /// Omitted by meka when the session has no working directory of its own.
    #[serde(default)]
    pub cwd: Option<String>,
    /// Whether a turn is running on this session right now.
    ///
    /// Read by `mekabridge session show` and by nothing that decides anything: what became of a
    /// hand-over is what the feed says, and a session busy with somebody else's turn is no
    /// obstacle to putting a message in its inbox.
    pub turn_in_flight: bool,
    /// Inbox items waiting to be appended. Reported for a loaded session only, and by no meka
    /// before 0.55, so it is optional rather than defaulted to a zero that would read as "nothing
    /// waiting".
    #[serde(default)]
    pub inbox_pending: Option<u64>,
}

/// Server metadata as returned by `GET /v1/info`.
#[derive(Debug, Clone, Deserialize)]
pub struct ServerInfo {
    pub version: String,
    /// Whether the active provider profile can look at images at all.
    ///
    /// The bridge attaches nothing to a turn, so this gates `attachment_view` instead. Worth being
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
    /// that never gets answered.
    ///
    /// Defaulted rather than required, so an older meka that does not send it reads as "no
    /// opinion" and the check is skipped instead of taking `doctor` down.
    #[serde(default)]
    pub enabled_permissions: Vec<String>,
    /// The scopes `[meka].token` holds, which meka reports from 0.57.
    ///
    /// The one thing about the token nothing else here could check. Every call `doctor` makes
    /// needs a read scope only, so a token that cannot hand a message over answers all of them and
    /// the gap surfaces at the first message instead. Empty on an older meka, which reads as "no
    /// opinion" the same way [`Self::enabled_permissions`] does.
    #[serde(default)]
    pub scopes: Vec<String>,
}

/// One configured profile, as returned by `GET /v1/profiles`.
///
/// Where the model came from once meka 0.44 dropped `model` and `provider` from `GET /v1/info`.
/// The two words meant different things on the two endpoints -- `provider` on `/v1/info` was a
/// backend, `provider` on `POST /v1/sessions` was a profile -- and this endpoint names them apart.
#[derive(Debug, Clone, Deserialize)]
pub struct Profile {
    /// The profile name, which is what `POST /v1/sessions` would take.
    pub name: String,
    /// The account it bills, which is where the credential and the backend live since meka 0.46
    /// split one `[providers.<name>]` table into two.
    #[serde(default)]
    pub account: String,
    /// The wire protocol the account speaks, such as `anthropic-messages`.
    ///
    /// Omitted when the profile names an account `config.toml` does not have, which is a
    /// misconfiguration no meka will start a session on.
    #[serde(default)]
    pub backend: Option<String>,
    /// Omitted by meka when the profile configures none.
    #[serde(default)]
    pub model: Option<String>,
    /// Whether a session naming no profile gets this one. The bridge never names one, so this
    /// marks the profile its session actually runs on.
    #[serde(default)]
    pub active: bool,
}

/// Readiness as returned by `GET /v1/health/ready`.
#[derive(Debug, Clone, Deserialize)]
pub struct ReadyStatus {
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub session_db: bool,
    /// Whether `config.toml` names at least one profile. Not whether its credential works, which
    /// meka only finds out when a session first needs it.
    ///
    /// `provider_configured` until 0.46 renamed it. Aliased rather than simply renamed because
    /// every field here defaults: a rename alone reads a 0.45 body as `false` and `doctor` calls a
    /// healthy deployment unservable, which is the failure this flag exists to report.
    #[serde(default, alias = "provider_configured")]
    pub profile_configured: bool,
    #[serde(default)]
    pub mcp_servers_healthy: bool,
}

/// What `POST /v1/sessions/{id}/inbox` answered.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct InboxReceipt {
    pub item_id: String,
    /// `pending` on a fresh item. On a replayed `Idempotency-Key`, whatever the earlier item has
    /// since become, which is how an item posted before a restart learns its fate without an
    /// event.
    pub state: InboxState,
    /// Whether the `Idempotency-Key` had been used before, so the item is the earlier one.
    #[serde(default)]
    pub replayed: bool,
}

/// Where an inbox item is in its life, as meka names it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(from = "String")]
pub enum InboxState {
    /// Waiting for a turn to read it.
    Pending,
    /// Its text is in the conversation, and no request carrying it has been accepted yet.
    Appended,
    /// The provider accepted a request carrying it, so the model has read it.
    Delivered,
    /// Taken back: by a `DELETE`, by the cancellation of the turn it opened, or by meka giving up
    /// on it.
    Withdrawn,
    /// A name this build does not know, kept rather than failing the post over it.
    Other(String),
}

impl From<String> for InboxState {
    fn from(name: String) -> Self {
        match name.as_str() {
            "pending" => Self::Pending,
            "appended" => Self::Appended,
            "delivered" => Self::Delivered,
            "withdrawn" => Self::Withdrawn,
            _ => Self::Other(name),
        }
    }
}

impl std::fmt::Display for InboxState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => formatter.write_str("pending"),
            Self::Appended => formatter.write_str("appended"),
            Self::Delivered => formatter.write_str("delivered"),
            Self::Withdrawn => formatter.write_str("withdrawn"),
            Self::Other(name) => formatter.write_str(name),
        }
    }
}

/// The class every item is posted as. A `followup` would reproduce the wait the inbox exists to
/// end, and an `interrupt` would cut a reply to one person because another wrote; a `steer` is
/// read at the running turn's next round boundary, or rides the next turn's opening.
pub const INBOX_CLASS: &str = "steer";

/// Who the header meka writes above each item names as the sender. The same name the MCP
/// instructions use for this bridge, so the agent reads one name for it rather than whatever the
/// operator described the token as.
pub const INBOX_SOURCE: &str = "mekabridge";

/// HTTP client for one `meka serve` instance.
#[derive(Clone)]
pub struct MekaClient {
    http: reqwest::Client,
    base_url: Url,
    token: Secret,
    max_retries: u32,
}

impl MekaClient {
    /// Build a client from the resolved `[meka]` config.
    pub fn new(config: &MekaConfig) -> Result<Self> {
        let http = reqwest::Client::builder()
            .connect_timeout(config.connect_timeout)
            // No overall request timeout: the feed is held open for as long as the bridge runs.
            // `read_timeout` is the one that works on a stream, because it bounds the gap between
            // reads rather than the whole response, and meka sends a keep-alive comment every
            // twenty seconds. Without it a connection that goes silent without closing -- a NAT
            // dropping state, a machine pulled off the network -- is indistinguishable from a quiet
            // session, and the reader would wait forever on a feed that will never speak again.
            .read_timeout(STREAM_READ_TIMEOUT)
            .build()
            .map_err(|error| MekaError::Build(error.to_string()))?;
        Ok(Self {
            http,
            base_url: config.base_url.clone(),
            token: config.token.clone(),
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

    /// `GET /v1/profiles`: the configured profiles, one of them marked as the default.
    ///
    /// `GET /v1/providers` until meka 0.46 renamed the endpoint and the key together. Not read
    /// under both names: the old path answers 404 on a new meka and the new one answers 404 on an
    /// old one, and `doctor` prints that either way rather than quietly reporting the wrong thing.
    pub async fn profiles(&self) -> Result<Vec<Profile>> {
        #[derive(Deserialize)]
        struct ProfilesResponse {
            profiles: Vec<Profile>,
        }

        let url = self.endpoint("/v1/profiles")?;
        let response: ProfilesResponse = self.get_with_retries(url).await?;
        Ok(response.profiles)
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

    /// `POST /v1/sessions/{id}/cancel`. Idempotent server-side, so a retry is safe.
    ///
    /// With a `turn_id` meka stops only that turn and answers `409 turn-mismatch` when another is
    /// running, so a caller that watched one turn cannot stop the scheduled fire that replaced it.
    /// Without one it stops whatever runs, which is what the operator's `mekabridge cancel` means.
    pub async fn cancel_turn(&self, session_id: Uuid, turn_id: Option<&str>) -> Result<()> {
        let url = self.endpoint(&format!("/v1/sessions/{session_id}/cancel"))?;
        let body = turn_id.map(|turn_id| serde_json::json!({ "turn_id": turn_id }));
        let mut attempt = 0;
        loop {
            let mut request = self
                .http
                .post(url.clone())
                .bearer_auth(self.token.expose())
                .timeout(REQUEST_TIMEOUT);
            if let Some(body) = &body {
                request = request.json(body);
            }
            let outcome = match request.send().await {
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

    /// `POST /v1/sessions/{id}/inbox`: hand the agent a message without waiting for it.
    ///
    /// Answers as soon as the item is durable on meka's side, whether or not a turn is running: a
    /// running one reads it at its next round boundary, and an idle session opens a turn for it.
    /// meka retries a turn that fails before the model produced anything, and reports the item's
    /// fate on the feed, so nothing about the turn is waited on here.
    ///
    /// `key` is recorded on the item. A retry after an ambiguous failure, or after a restart on
    /// either side, is answered with the item it already made and that item's current state rather
    /// than a second copy, which is also how a hand-over whose events were missed is reconciled.
    /// body has to be byte-identical on a replay: meka refuses the same key with other words.
    ///
    /// Never retried here. The session task schedules the retry, where the wait is written down
    /// and survives a restart.
    pub async fn post_inbox(
        &self,
        session_id: Uuid,
        body: &str,
        key: &str,
    ) -> Result<InboxReceipt> {
        let url = self.endpoint(&format!("/v1/sessions/{session_id}/inbox"))?;
        let response = self
            .http
            .post(url)
            .bearer_auth(self.token.expose())
            .timeout(REQUEST_TIMEOUT)
            .header("Idempotency-Key", key)
            .json(&serde_json::json!({
                "message": body,
                "class": INBOX_CLASS,
                "source": INBOX_SOURCE,
            }))
            .send()
            .await?;
        decode(response).await
    }

    /// `GET /v1/sessions/{id}/stream`: the session's event feed.
    ///
    /// Every event of every turn on the session, whoever started it, for as long as the connection
    /// is held; it does not end with a turn. `last_event_id` is the last id already handled, and
    /// meka replays what came after it before following the live feed, across turns. A resume that
    /// outruns meka's replay ring is reported on the feed as a `notice` rather than as a silent
    /// hole, which is the caller's cue to reconcile what it was waiting on.
    ///
    /// Opening the feed loads the session, so the bridge subscribes before it has anything to hand
    /// over, and a session evicted for idleness comes back rather than answering 404. It is also
    /// what keeps the session resident: meka does not evict one with a live subscriber.
    ///
    /// Errors before the first byte surface here rather than inside the stream, so the caller can
    /// tell a session that is gone from a connection that was lost.
    pub async fn open_feed(
        &self,
        session_id: Uuid,
        last_event_id: Option<u64>,
    ) -> Result<impl Stream<Item = Result<StreamItem>> + Send + use<>> {
        let url = self.endpoint(&format!("/v1/sessions/{session_id}/stream"))?;
        let mut request = self.http.get(url).bearer_auth(self.token.expose());
        if let Some(last_event_id) = last_event_id {
            request = request.header("Last-Event-ID", last_event_id.to_string());
        }
        let response = request.send().await?;
        if !response.status().is_success() {
            return Err(problem_from(response).await);
        }
        Ok(events_from(response))
    }
}

/// One event off the session feed, with the id needed to resume after it and the turn it belongs
/// to.
#[derive(Debug, Clone, PartialEq)]
pub struct StreamItem {
    /// `None` only until the first id of a connection has been seen.
    ///
    /// Not "whenever meka omits one", which is the obvious reading and is wrong:
    /// `eventsource-stream` implements the spec's persistent last-event-ID buffer, so an event
    /// sent without an `id:` line is reported carrying the previous one. meka does send
    /// several that way, including the synthesised `turn.started` that opens a resumed feed
    /// and the notice about a replay hole. Inheriting the previous id is harmless here, since
    /// resuming from it is idempotent.
    pub id: Option<u64>,
    /// Which turn the event belongs to, off the payload. Every event meka sends carries one;
    /// `None` on a frame that names none, such as the notice about a replay hole.
    pub turn_id: Option<String>,
    pub event: TurnEvent,
}

/// Turn an SSE response body into parsed events, tagged with their ids.
///
/// The stream ends when the connection does, and not before: the feed carries on past every
/// terminal. A frame this build cannot parse is reported as [`MekaError::Decode`] and the stream
/// carries on, since the frame decides nothing on its own and reconnecting would replay it out of
/// meka's ring to fail identically.
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
                            Err(MekaError::FeedEnded {
                                reason: error.to_string(),
                            }),
                            (events, true),
                        ));
                    }
                    Some(Ok(frame)) => match sse::parse(&frame.event, &frame.data) {
                        Ok(None) => continue,
                        Ok(Some(parsed)) => {
                            let id = frame.id.trim().parse::<u64>().ok();
                            return Some((
                                Ok(StreamItem {
                                    id,
                                    turn_id: parsed.turn_id,
                                    event: parsed.event,
                                }),
                                (events, false),
                            ));
                        }
                        Err(error) => {
                            return Some((
                                Err(MekaError::Decode(format!(
                                    "event {:?}: {error}",
                                    frame.event
                                ))),
                                (events, false),
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
            retry_after: None,
            provider_response: None,
        }
    }

    #[test]
    fn problem_kinds_route_on_the_type_uri() {
        assert_eq!(
            problem("https://meka.run/errors/session-not-found").kind(),
            ProblemKind::SessionNotFound
        );
        assert_eq!(
            problem("https://meka.run/errors/turn-in-flight").kind(),
            ProblemKind::TurnInFlight
        );
        assert_eq!(
            problem("https://meka.run/errors/provider").kind(),
            ProblemKind::Provider
        );
        assert_eq!(
            problem("https://example.com/errors/new").kind(),
            ProblemKind::Other
        );
    }

    /// meka 0.59 moved every `type` URI from `meka.so` to `meka.run`, keeping the segment after the
    /// last slash. Both hosts are in support at once, since the floor is 0.55, and routing on that
    /// last segment is what makes the move invisible here rather than a table to keep in step.
    #[test]
    fn the_host_a_type_uri_sits_under_decides_nothing() {
        for host in ["https://meka.so", "https://meka.run"] {
            assert_eq!(
                problem(&format!("{host}/errors/context-overflow")).kind(),
                ProblemKind::ContextOverflow,
                "{host} must route like the other, or one of the two mekas this build supports \
                 has every error fall to `Other`"
            );
        }
    }

    #[test]
    fn session_missing_is_detected() {
        let error = MekaError::Problem(problem("https://meka.run/errors/session-not-found"));
        assert!(error.is_session_missing());
        let other = MekaError::Problem(problem("https://meka.run/errors/auth"));
        assert!(!other.is_session_missing());
    }

    #[test]
    fn retryable_classification_covers_transient_failures() {
        assert!(
            MekaError::Problem(problem("https://meka.run/errors/concurrency-limit")).is_retryable()
        );
        assert!(MekaError::Problem(problem("https://meka.run/errors/internal")).is_retryable());
    }

    /// Where a provider rate limit or an overload lands once meka's own retries are spent.
    ///
    /// Up to meka 0.43 it was `internal`, which the test above covers. 0.44 gave
    /// `RetryableProvider` an arm of its own and pointed it at the existing `provider` URI, so this
    /// bucket stopped being purely permanent and reading it that way turned every rate limit into a
    /// message given up on at the first attempt.
    #[test]
    fn a_provider_failure_is_worth_another_attempt() {
        assert!(MekaError::Problem(problem("https://meka.run/errors/provider")).is_retryable());
    }

    /// The type meka minted so a transient upstream failure stops arriving byte-identical to a dead
    /// credential. Unrecognised it would fall to [`ProblemKind::Other`], which retries a 502 by its
    /// status alone and would answer correctly today for the wrong reason, then stop the moment
    /// meka gave one of these a 4xx.
    #[test]
    fn an_upstream_meka_calls_transient_is_worth_another_attempt() {
        let detail = problem("https://meka.run/errors/provider-unavailable");
        assert_eq!(detail.kind(), ProblemKind::ProviderUnavailable);
        assert!(MekaError::Problem(detail).is_retryable());
    }

    /// The one upstream refusal that must not be retried, and the reason meka split it out of
    /// `provider`: the conversation that did not fit will not fit on the next attempt either, so
    /// the whole retry budget is spent proving it.
    #[test]
    fn a_context_overflow_is_never_worth_another_attempt() {
        let error = MekaError::Problem(problem("https://meka.run/errors/context-overflow"));
        assert_eq!(
            problem("https://meka.run/errors/context-overflow").kind(),
            ProblemKind::ContextOverflow,
            "an unrecognised type would fall to `Other`, whose 5xx rule retries it"
        );
        assert!(!error.is_retryable());
        assert!(error.is_context_overflow());
        assert!(
            !MekaError::Problem(problem("https://meka.run/errors/provider")).is_context_overflow(),
            "the two share a status and must not share a remedy"
        );
    }

    /// The status a context overflow actually arrives with. `problem()` builds a 409, so without
    /// this the test above would pass even if the type fell through to `Other`, whose rule is the
    /// status alone.
    #[test]
    fn a_context_overflow_is_not_retried_at_the_status_it_really_carries() {
        let mut detail = problem("https://meka.run/errors/context-overflow");
        detail.status = 502;
        assert!(!MekaError::Problem(detail).is_retryable());
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
    }

    #[test]
    fn a_type_this_build_has_never_heard_of_falls_back_to_the_status() {
        // Forward compatibility with the very change this bridge asks meka for. Its
        // `RetryableProvider` currently has no arm in the mapping onto a Problem Detail and lands
        // on `internal`; giving it one means a new URI, and reading an unknown URI as permanent
        // would turn every rate limit into a message abandoned on the first attempt the day that
        // improvement shipped.
        let retryable = |status| {
            let mut detail = problem("https://meka.run/errors/retryable-provider");
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
        let mut detail = problem("https://meka.run/errors/internal");
        detail.retry_after = Some(45.0);
        assert_eq!(
            MekaError::Problem(detail).retry_after(),
            Some(Duration::from_secs(45))
        );
        assert_eq!(
            MekaError::Problem(problem("https://meka.run/errors/internal")).retry_after(),
            None
        );
        // Nonsense rather than an instruction to wait forever. The last two matter more than they
        // look: `Duration::from_secs_f64` *panics* on a value it cannot represent, and this reads
        // whatever number the other side put in a JSON body.
        for nonsense in [-1.0, f64::NAN, f64::INFINITY, 1e300] {
            let mut detail = problem("https://meka.run/errors/internal");
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
        let body = r#"{"type":"https://meka.run/errors/concurrency-limit",
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
        assert!(!MekaError::Problem(problem("https://meka.run/errors/auth")).is_retryable());
        assert!(
            !MekaError::Problem(problem("https://meka.run/errors/invalid-body")).is_retryable()
        );
        // `provider` used to be asserted here, on the grounds that meka's `Provider` and
        // `InvalidRequest` both mapped onto it and the agent loop had already tried to repair
        // them. meka 0.44 pointed `RetryableProvider` at the same URI, so the bucket now holds
        // transient failures too and is retried; see `a_provider_failure_is_worth_another_attempt`.
        assert!(
            !MekaError::Problem(problem("https://meka.run/errors/session-not-found"))
                .is_retryable()
        );
    }

    /// The one 409 that is a wait rather than a refusal: another meka process holds the session,
    /// and an inbox post is refused on it before anything is written.
    #[test]
    fn a_locked_session_is_a_wait_rather_than_a_failure() {
        let locked = MekaError::Problem(problem("https://meka.run/errors/session-locked"));
        assert!(locked.is_session_locked());
        assert!(locked.is_retryable());
        assert!(
            !MekaError::Problem(problem("https://meka.run/errors/session-not-found"))
                .is_session_locked()
        );
    }

    /// What the inbox answers, and every state a replayed key can report. An unknown state is kept
    /// by name rather than failing the post: the item was recorded either way.
    #[test]
    fn an_inbox_receipt_reads_every_state_meka_names() {
        let fresh = r#"{"item_id":"i1","session_id":"s","class":"steer","state":"pending",
                        "replayed":false}"#;
        let receipt: InboxReceipt = serde_json::from_str(fresh).expect("parses");
        assert_eq!(receipt.item_id, "i1");
        assert_eq!(receipt.state, InboxState::Pending);
        assert!(!receipt.replayed);
        for (name, state) in [
            ("appended", InboxState::Appended),
            ("delivered", InboxState::Delivered),
            ("withdrawn", InboxState::Withdrawn),
        ] {
            let replayed = format!(
                r#"{{"item_id":"i1","session_id":"s","class":"steer","state":"{name}",
                    "replayed":true}}"#
            );
            let receipt: InboxReceipt = serde_json::from_str(&replayed).expect("parses");
            assert_eq!(receipt.state, state, "{name}");
            assert!(receipt.replayed);
        }
        assert_eq!(
            InboxState::from("archived".to_string()),
            InboxState::Other("archived".to_string())
        );
        assert_eq!(InboxState::Delivered.to_string(), "delivered");
    }

    /// The count is new in 0.55 and reported for a loaded session only, so both "not sent" and
    /// "sent as zero" have to read back as what they are.
    #[test]
    fn session_info_reads_the_inbox_count_when_meka_sends_one() {
        let counted = r#"{"id":"6f1a4b9c-0000-4000-8000-000000000000","title":"",
                          "turn_in_flight":true,"inbox_pending":2}"#;
        let info: SessionInfo = serde_json::from_str(counted).expect("must deserialize");
        assert_eq!(info.inbox_pending, Some(2));
        let uncounted = r#"{"id":"6f1a4b9c-0000-4000-8000-000000000000","title":"",
                            "turn_in_flight":false}"#;
        let info: SessionInfo = serde_json::from_str(uncounted).expect("must deserialize");
        assert_eq!(info.inbox_pending, None);
    }

    /// `/v1/info` has to be read across the versions this bridge supports, and it changed in both
    /// directions at once: meka 0.44 dropped `model` and `provider` and added `default_permission`.
    /// A field arriving that this build has never heard of is the easy half; the half that bit is
    /// that a field going missing is silent, because a missing `Option` deserializes to `None`
    /// rather than failing. `doctor` reported "(none configured)" for every deployment on 0.44
    /// while nothing looked wrong.
    #[test]
    fn server_info_reads_every_meka_this_bridge_supports() {
        let recent = r#"{"version":"0.44.0","default_permission":"read",
                         "enabled_permissions":["read","workspace"],"vision":true}"#;
        let info: ServerInfo = serde_json::from_str(recent).expect("0.44 must deserialize");
        assert_eq!(info.version, "0.44.0");
        assert!(info.vision);
        assert_eq!(info.enabled_permissions, ["read", "workspace"]);

        let older = r#"{"version":"0.43.0","model":null,"provider":"anthropic-messages",
                        "vision":false,"enabled_permissions":[]}"#;
        let info: ServerInfo = serde_json::from_str(older).expect("0.43 must deserialize");
        assert!(
            !info.vision,
            "the fields it dropped must not shadow the ones it kept"
        );
        assert!(
            info.scopes.is_empty(),
            "a meka that reports no scopes has said nothing, not that the token holds nothing"
        );

        let scoped = r#"{"version":"0.57.0","default_permission":"read",
                         "enabled_permissions":["read"],"vision":true,
                         "scopes":["sessions:r","sessions:w"]}"#;
        let info: ServerInfo = serde_json::from_str(scoped).expect("0.57 must deserialize");
        assert_eq!(info.scopes, ["sessions:r", "sessions:w"]);
    }

    /// The rename that reads as a healthy deployment being unservable. Every field on
    /// [`ReadyStatus`] defaults, so a name meka no longer sends is `false` rather than an error,
    /// and `doctor` fails the run over it.
    #[test]
    fn readiness_reads_the_configured_profile_flag_under_either_name() {
        let new = r#"{"status":"ok","session_db":true,"profile_configured":true,
                      "mcp_servers_healthy":true}"#;
        let ready: ReadyStatus = serde_json::from_str(new).expect("0.46 must deserialize");
        assert!(ready.profile_configured);

        let old = r#"{"status":"ok","session_db":true,"provider_configured":true,
                      "mcp_servers_healthy":true}"#;
        let ready: ReadyStatus = serde_json::from_str(old).expect("0.45 must deserialize");
        assert!(
            ready.profile_configured,
            "the old spelling must not read as no profile configured"
        );
    }

    /// meka 0.46 omits the level for a row that records none, where it used to send `""`. A
    /// required field here would turn `doctor` reporting on somebody else's session into a decode
    /// error.
    #[test]
    fn a_session_that_records_no_level_still_reads() {
        let body = r#"{"id":"6f1a4b9c-0000-4000-8000-000000000000","title":"",
                       "approvals":false,"profile":"work","turn_in_flight":false}"#;
        let info: SessionInfo = serde_json::from_str(body).expect("must deserialize");
        assert_eq!(info.permission, None);
        assert!(!info.turn_in_flight);
    }

    /// Where the model went. The bridge names no profile, so the one marked `active` is the one its
    /// session runs on.
    #[test]
    fn the_active_profile_is_the_one_the_bridge_gets() {
        #[derive(serde::Deserialize)]
        struct Response {
            profiles: Vec<Profile>,
        }

        let body = r#"{"profiles":[
            {"name":"cheap","account":"personal","backend":"openai-responses","active":false},
            {"name":"work","account":"work","backend":"anthropic-messages",
             "model":"claude-opus-5","active":true}]}"#;
        let parsed: Response = serde_json::from_str(body).expect("profiles must deserialize");
        let active = parsed
            .profiles
            .iter()
            .find(|profile| profile.active)
            .expect("one profile is the default");
        assert_eq!(active.name, "work");
        assert_eq!(active.account, "work");
        assert_eq!(active.backend.as_deref(), Some("anthropic-messages"));
        assert_eq!(active.model.as_deref(), Some("claude-opus-5"));
        // meka omits `model` for a profile that configures none, rather than sending null.
        assert_eq!(parsed.profiles[0].model, None);
    }

    /// A profile naming an account `config.toml` does not have. meka lists it with no `backend`
    /// rather than dropping it, and reports the dangling account beside the table, so a required
    /// field here would turn the row that explains the misconfiguration into a decode error.
    #[test]
    fn a_profile_on_a_missing_account_still_reads() {
        let body = r#"{"name":"work","account":"gone","active":true}"#;
        let profile: Profile = serde_json::from_str(body).expect("must deserialize");
        assert_eq!(profile.backend, None);
        assert_eq!(profile.account, "gone");
    }

    #[test]
    fn problem_detail_displays_title_status_and_detail() {
        let rendered = problem("https://meka.run/errors/auth").to_string();
        assert_eq!(rendered, "t (409): d");
    }

    /// meka's own `detail` for a 502 points at its server log, so the owner's notice is only worth
    /// reading if the relayed upstream text rides along with it.
    #[test]
    fn the_upstream_response_reaches_whoever_reads_the_failure() {
        let mut detail = problem("https://meka.run/errors/provider");
        detail.provider_response = Some("401 invalid_api_key".to_string());
        assert_eq!(
            detail.to_string(),
            "t (409): d. The provider said: 401 invalid_api_key"
        );
        // Absent where the operator turned relaying off, or on a meka predating the key.
        assert_eq!(
            problem("https://meka.run/errors/provider").to_string(),
            "t (409): d"
        );
    }

    /// meka bounds this at 4 KiB for its log. The owner's notice puts it mid-message and appends
    /// the attempt count and affected conversations after it, so an unbounded one pushes those past
    /// Telegram's 4096 characters and into a second message the operator may never read.
    #[test]
    fn a_vast_upstream_response_does_not_displace_the_rest_of_the_notice() {
        let mut detail = problem("https://meka.run/errors/provider");
        // A multi-byte character throughout, so a byte-wise cut would land mid-character and panic.
        detail.provider_response = Some("é".repeat(4 * 1024));
        let rendered = detail.to_string();
        assert!(rendered.ends_with("..."), "the cut is not marked");
        assert!(
            rendered.chars().count() < 600,
            "{} characters would crowd out the lines after it",
            rendered.chars().count()
        );
        // The start survives, which is where the provider puts the error type.
        assert!(rendered.contains("The provider said: éé"));
    }

    /// The whole body meka sends, rather than a hand-built struct, so a rename on either side is
    /// caught here instead of by an operator reading a notice that stopped naming the cause.
    #[test]
    fn a_real_502_body_parses_into_both_new_fields() {
        let body = r#"{"type":"https://meka.run/errors/provider-unavailable",
                       "title":"Provider temporarily unavailable","status":502,
                       "detail":"its response is in the server log",
                       "provider_response":"529 overloaded_error","retry_after":30}"#;
        let detail: ProblemDetail = serde_json::from_str(body).expect("a 502 body must parse");
        assert_eq!(detail.kind(), ProblemKind::ProviderUnavailable);
        assert_eq!(
            detail.provider_response.as_deref(),
            Some("529 overloaded_error")
        );
        assert_eq!(
            MekaError::Problem(detail).retry_after(),
            Some(Duration::from_secs(30))
        );
    }
}
