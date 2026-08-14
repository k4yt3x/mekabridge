//! Running one turn against meka, and the presence signalling that goes with it.
//!
//! The bridge relays no deltas: nothing the agent streams reaches a user unless the agent calls
//! `send_message`. Streaming is still the transport, because a multi-minute turn on a blocking POST
//! is fragile behind proxies with read timeouts, and because the event stream is the only place
//! tool activity is visible for logs.
//!
//! Typing indicators are the one thing the bridge emits on its own. That is presence rather than
//! content, the same signal a person's phone shows while they type, so it does not compete with the
//! agent's decision about whether to reply at all.
//!
//! It is held for as long as the turn runs: it opens once meka accepts the turn, stops the moment a
//! reply actually lands, and stops again when the turn ends. `[bridge].typing_max` is a ceiling on
//! top of that, defaulting to the turn budget, so it only fires if a turn somehow outlives both.
//!
//! Cancelling it has to abandon any request still in flight, not merely stop making new ones. Both
//! platforms queue rate-limited calls rather than refusing them, so an indicator that is allowed to
//! finish sending after its turn has ended goes on being drawn for as long as the backlog takes to
//! drain, which is a chat that claims the agent is working when it has been idle for minutes.
//!
//! Holding it that long slightly overstates the case, since an agent grinding through tool calls is
//! working rather than composing, and it may decide to say nothing at all. Stopping early is worse.
//! A cap shorter than the turn produces a chat that shows "typing" briefly and is then silent for
//! minutes, which reads as a bot that has died rather than one that is thinking, and the user has
//! no way to tell those apart. Neither platform limits how long the indicator may be renewed.

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    channel::{ChannelRegistry, ConversationId},
    meka::{MekaClient, MekaError, TurnOutcome, sse::TurnEvent},
};

/// How often the typing indicator is refreshed.
///
/// Telegram clears the status after about five seconds and Discord after ten, so the interval has
/// to sit under the shorter of the two to look continuous on both. Neither platform caps how many
/// times it may be renewed, so holding the indicator for as long as the work lasts costs one cheap
/// call every few seconds.
///
/// Cheap, but not free: Discord meters the typing endpoint on its own bucket, and twilight queues
/// rather than refuses once that is exhausted. One refresh loop per conversation stays well inside
/// it; anything that opens indicators in a loop does not.
const TYPING_REFRESH: Duration = Duration::from_secs(4);

/// Which conversations the agent has already sent to during the current turn.
///
/// Shared with the outbound sink, which is the only thing that knows a message actually went out.
/// Telegram clears the typing status when a message from the bot arrives, so without this the
/// refresh loop re-arms it seconds later and the user, having just been answered, waits for a
/// second message that is never coming.
#[derive(Debug, Default)]
pub struct Presence {
    replied: std::sync::Mutex<std::collections::HashSet<ConversationId>>,
}

impl Presence {
    /// Record that a message reached `conversation`.
    pub fn note_sent(&self, conversation: &ConversationId) {
        self.replied
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(conversation.clone());
    }

    /// Whether the agent has already sent to `conversation` this turn.
    pub fn has_replied(&self, conversation: &ConversationId) -> bool {
        self.replied
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .contains(conversation)
    }

    /// Forget everything, so the next turn starts from silence.
    pub fn reset(&self) {
        self.replied
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
    }
}

/// The MCP tool name meka exposes for this bridge's `send_message`, used to tell "the agent
/// replied" apart from "the agent stayed quiet". meka namespaces MCP tools as
/// `mcp__<server>__<tool>`, and the server segment is whatever the operator named this bridge in
/// meka's config, so the match is on the suffix.
const SEND_TOOL_SUFFIX: &str = "__send_message";

/// How much assistant text to keep for diagnostics. Enough to hold any of meka's empty-turn
/// stand-ins whole, and to show the opening of a real answer that never got delivered.
const TEXT_PREVIEW_CHARS: usize = 240;

/// What a turn did, for logging, for deciding whether to warn about a silent turn, and for deciding
/// whether a failed one may be handed over again.
///
/// Reported for a failed turn as much as a finished one, which is the whole reason the outcome is a
/// `Result` in here rather than the return type. A failure reaching the bridge means meka's own
/// retries ran out, and those are scoped to a single provider call, so the turn may well have made
/// several already: a rate limit on the third call of a tool loop arrives with the first two
/// iterations' tool calls behind it. Whether anything actually happened is the difference between a
/// batch that is safe to offer again and one whose retry would repeat work the agent cannot
/// remember doing.
#[derive(Debug)]
pub struct TurnReport {
    pub outcome: Result<TurnOutcome, MekaError>,
    /// Times the agent called a send tool during the turn.
    pub sends: usize,
    /// Total tool calls, including sends.
    pub tool_calls: usize,
    /// Characters of assistant text produced. Not relayed anywhere; useful for spotting a turn
    /// that wrote a long answer and then failed to deliver it.
    pub text_length: usize,
    /// Leading slice of that text, kept so a turn that delivered nothing can say what the agent
    /// produced instead. Without it an operator has to query meka's message API to find out why a
    /// message went unanswered, which is a poor thing to need at 3am.
    pub text_preview: String,
}

impl TurnReport {
    /// A turn that never got as far as running, so nothing was produced and nothing was done.
    pub fn failed(error: MekaError) -> Self {
        Self {
            outcome: Err(error),
            sends: 0,
            tool_calls: 0,
            text_length: 0,
            text_preview: String::new(),
        }
    }

    /// A turn that produced text but sent nothing.
    ///
    /// Legal, and sometimes correct, but almost always worth a log line: from the user's side it is
    /// indistinguishable from the bridge being broken.
    pub const fn is_silent(&self) -> bool {
        self.sends == 0
    }

    /// Whether anything happened that a second attempt would repeat.
    ///
    /// Sends and tool calls, not text. Text costs tokens and nothing else, so a turn that wrote a
    /// paragraph and then died can be handed over again without anybody noticing; a turn that ran a
    /// shell command cannot, and the agent would have no memory of the first run to tell it apart
    /// from the second.
    pub const fn had_side_effects(&self) -> bool {
        self.sends > 0 || self.tool_calls > 0
    }

    /// Whether the turn did nothing at all because the model came back empty.
    ///
    /// meka substitutes a bracketed stand-in when a turn yields no content, and streams it as the
    /// assistant text. Paired with zero tool calls that is provably inert: nothing was sent,
    /// nothing was executed, so the batch can be handed over again without any risk of doing
    /// something twice. Refusals are excluded because those are a real answer, just not one
    /// anybody likes.
    ///
    /// Matching meka's wording is a little brittle. It degrades safely: if the phrasing changes the
    /// turn is simply treated as silent and logged, rather than retried.
    pub fn produced_nothing(&self) -> bool {
        if self.tool_calls > 0
            || matches!(self.outcome, Ok(TurnOutcome::Finished { ref stop_reason, .. }) if stop_reason == "refusal")
        {
            return false;
        }
        let text = self.text_preview.trim();
        text.starts_with("[The model ") && text.ends_with(']')
    }
}

/// Runs turns and keeps the originating conversations looking alive while they run.
pub struct TurnRunner {
    meka: MekaClient,
    channels: Arc<ChannelRegistry>,
    typing_enabled: bool,
    /// Ceiling on how long the indicator is held, from
    /// [`crate::config::BridgeConfig::typing_max`].
    typing_max: Duration,
    /// Shared with the outbound sink so the indicator can stop once a reply has actually landed.
    presence: Arc<Presence>,
}

impl TurnRunner {
    pub const fn new(
        meka: MekaClient,
        channels: Arc<ChannelRegistry>,
        typing_enabled: bool,
        typing_max: Duration,
        presence: Arc<Presence>,
    ) -> Self {
        Self {
            meka,
            channels,
            typing_enabled,
            typing_max,
            presence,
        }
    }

    /// Submit `message` and drive the turn to completion.
    ///
    /// `conversations` are the ones the batch came from; each gets a typing indicator until the
    /// agent replies there or the window lapses.
    pub async fn run(
        &self,
        session_id: Uuid,
        message: &str,
        conversations: &BTreeSet<ConversationId>,
    ) -> TurnReport {
        // Sends from an earlier turn must not suppress this turn's indicator.
        self.presence.reset();
        // Opened on `Started` rather than here, because until meka accepts the turn nothing has
        // reached the agent and there is nothing to be composing. That matters more than it reads:
        // a refused submission is retried rather than dropped, so announcing it here put out one
        // flash per attempt, which on a platform whose indicator outlives the retry interval is
        // indistinguishable from a bot that types forever.
        let mut typing: Option<CancellationToken> = None;

        let mut sends = 0_usize;
        let mut tool_calls = 0_usize;
        let mut text_length = 0_usize;
        let mut text_preview = String::new();

        let result = self
            .meka
            .run_turn(session_id, message, |event| match event {
                TurnEvent::Started { .. } => {
                    // Replacing rather than assuming there is only ever one. A second `Started`
                    // would otherwise strand the first token's refresh loop, which nothing would
                    // then cancel and which would run until the ceiling.
                    if let Some(previous) = typing.replace(self.start_typing(conversations)) {
                        previous.cancel();
                    }
                }
                TurnEvent::AssistantText { text } => {
                    text_length += text.chars().count();
                    // Bounded: a long answer would otherwise be held in memory for a log line.
                    if text_preview.chars().count() < TEXT_PREVIEW_CHARS {
                        text_preview.push_str(text);
                    }
                }
                TurnEvent::ToolCallStarted { name, .. } => {
                    tool_calls += 1;
                    if name.ends_with(SEND_TOOL_SUFFIX) {
                        sends += 1;
                    }
                    tracing::debug!(tool = %name, "agent tool call");
                }
                TurnEvent::Notice { level, text } => {
                    tracing::warn!(level = %level, "meka notice: {}", text);
                }
                TurnEvent::PermissionRequired { tool_name, .. } => {
                    // Sessions declare `supports_permission_prompts: false`, so meka denies a gated
                    // tool immediately rather than emitting this. Reaching here means the turn is
                    // about to stall for the full timeout with nothing able to answer.
                    tracing::error!(
                        tool = %tool_name,
                        "meka asked for permission, but this bridge has no approval channel; the \
                         turn will stall and deny. Set [session].permission to \"write\"."
                    );
                }
                _ => {}
            })
            .await;

        if let Some(typing) = typing {
            typing.cancel();
        }

        // The counters are kept on the error path too. They used to be dropped with the `Ok`, which
        // left the caller unable to tell a turn that failed before the agent did anything from one
        // that failed after it had already answered somebody.
        TurnReport {
            outcome: result,
            sends,
            tool_calls,
            text_length,
            text_preview,
        }
    }

    /// Open the indicator in each conversation until the returned token is cancelled, the agent
    /// replies there, or the ceiling is reached.
    ///
    /// Visible to the drain loop so it can cover the one stretch a turn stream cannot: meka running
    /// a turn this bridge did not submit, which is how a backgrounded tool call delivers its
    /// outcome. The agent is genuinely running then, and the batch waiting on it belongs to a
    /// conversation we know by name, so the claim is as true there as it is during our own turn.
    pub(super) fn start_typing(
        &self,
        conversations: &BTreeSet<ConversationId>,
    ) -> CancellationToken {
        let token = CancellationToken::new();
        if !self.typing_enabled {
            return token;
        }
        for conversation in conversations {
            let Ok(channel) = self.channels.resolve(conversation) else {
                continue;
            };
            if !channel.capabilities().typing_indicator {
                continue;
            }
            let channel = Arc::clone(channel);
            let conversation = conversation.clone();
            let presence = Arc::clone(&self.presence);
            let typing_max = self.typing_max;
            let token = token.child_token();
            tokio::spawn(async move {
                let deadline = tokio::time::Instant::now() + typing_max;
                loop {
                    // Checked before re-arming rather than after sending, so the burst that follows
                    // a reply never happens in the first place.
                    if presence.has_replied(&conversation) {
                        return;
                    }
                    if tokio::time::Instant::now() >= deadline {
                        tracing::debug!(
                            conversation = %conversation,
                            "the turn has run past the typing window; letting the indicator lapse"
                        );
                        return;
                    }
                    // Raced against the token rather than simply awaited. A rate-limited platform
                    // parks the request instead of refusing it -- twilight queues typing calls
                    // behind the channel's bucket -- so a request that outlives its turn has to be
                    // abandoned, not merely followed by no more. Otherwise a burst of indicators
                    // goes on arriving long after the agent stopped, which is exactly how a spin on
                    // a refused submission turned into minutes of phantom typing.
                    let sent = tokio::select! {
                        biased;
                        () = token.cancelled() => return,
                        result = channel
                            .set_activity(&conversation, crate::channel::Activity::Typing) => result,
                    };
                    if let Err(error) = sent {
                        // Presence is cosmetic; a chat that will not accept a typing action should
                        // never take down the turn that is actually doing the work.
                        tracing::debug!(
                            conversation = %conversation,
                            "typing indicator failed: {}",
                            error
                        );
                        return;
                    }
                    tokio::select! {
                        () = token.cancelled() => return,
                        () = tokio::time::sleep(TYPING_REFRESH) => {}
                    }
                }
            });
        }
        token
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;

    use super::*;
    use crate::{
        channel::{
            Activity, Channel, ChannelCapabilities, ChannelError, ChannelId, ChannelIdentity,
            FetchedFile, InboundEvent, Platform, SendOptions,
        },
        meka::sse::Usage,
    };

    /// A short ceiling, so the test that proves the indicator lapses does not take half an hour.
    /// Production follows `[meka].turn_timeout` unless the operator pins it.
    const TEST_TYPING_MAX: Duration = Duration::from_secs(30);

    /// Records the activity actions a turn asks for, so the indicator's timing can be asserted.
    struct SpyChannel {
        id: ChannelId,
        activities: Mutex<Vec<Activity>>,
        /// How long `set_activity` takes to reach the platform, standing in for a request parked
        /// behind a rate limit bucket.
        latency: Duration,
    }

    impl SpyChannel {
        fn new() -> Self {
            Self {
                id: ChannelId::new("spy"),
                activities: Mutex::new(Vec::new()),
                latency: Duration::ZERO,
            }
        }

        /// A channel whose typing calls sit in a queue before they go out.
        fn slow(latency: Duration) -> Self {
            Self {
                latency,
                ..Self::new()
            }
        }

        fn activity_count(&self) -> usize {
            self.activities
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .len()
        }
    }

    #[async_trait]
    impl Channel for SpyChannel {
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
                typing_status: false,
                files: true,
                photos: true,
                reactions: true,
                edit: true,
                admin: true,
                presence: false,
            }
        }

        async fn run(
            self: Arc<Self>,
            _sink: tokio::sync::mpsc::Sender<InboundEvent>,
            shutdown: CancellationToken,
        ) -> Result<(), ChannelError> {
            shutdown.cancelled().await;
            Ok(())
        }

        async fn send_text(
            &self,
            _conversation: &ConversationId,
            _markdown: &str,
            _options: &SendOptions,
        ) -> Result<Vec<String>, ChannelError> {
            Ok(vec!["m1".to_string()])
        }

        async fn send_file(
            &self,
            _conversation: &ConversationId,
            _path: &std::path::Path,
            _caption: Option<&str>,
            _as_photo: bool,
        ) -> Result<Vec<String>, ChannelError> {
            Ok(vec!["f1".to_string()])
        }

        async fn fetch(
            &self,
            _file_ref: &str,
            _max_bytes: u64,
        ) -> Result<FetchedFile, ChannelError> {
            Ok(FetchedFile {
                bytes: Vec::new(),
                media_type: None,
                extension: None,
            })
        }

        async fn react(
            &self,
            _conversation: &ConversationId,
            _message_id: &str,
            _emoji: Option<&str>,
        ) -> Result<(), ChannelError> {
            Ok(())
        }

        async fn set_activity(
            &self,
            _conversation: &ConversationId,
            activity: Activity,
        ) -> Result<(), ChannelError> {
            if !self.latency.is_zero() {
                tokio::time::sleep(self.latency).await;
            }
            self.activities
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(activity);
            Ok(())
        }

        async fn probe(&self) -> Result<ChannelIdentity, ChannelError> {
            Ok(ChannelIdentity {
                id: "1".to_string(),
                display_name: "Spy".to_string(),
                username: None,
                reads_all_group_messages: true,
            })
        }
    }

    fn runner_with(channel: Arc<SpyChannel>, presence: Arc<Presence>) -> TurnRunner {
        let meka = crate::meka::MekaClient::new(&crate::config::MekaConfig {
            base_url: "http://127.0.0.1:1".parse().expect("literal parses"),
            token: crate::config::secret::Secret::new("test", "test"),
            connect_timeout: Duration::from_secs(1),
            turn_timeout: Duration::from_secs(1),
            max_retries: 0,
        })
        .expect("client builds");
        let channels = Arc::new(crate::channel::ChannelRegistry::from_channels([
            channel as Arc<dyn Channel>
        ]));
        TurnRunner::new(meka, channels, true, TEST_TYPING_MAX, presence)
    }

    fn conversations() -> BTreeSet<ConversationId> {
        let mut set = BTreeSet::new();
        set.insert(ConversationId::parse("spy:1").expect("valid"));
        set
    }

    #[test]
    fn presence_tracks_which_conversations_were_answered() {
        let presence = Presence::default();
        let conversation = ConversationId::parse("spy:1").expect("valid");
        assert!(!presence.has_replied(&conversation));
        presence.note_sent(&conversation);
        assert!(presence.has_replied(&conversation));
        presence.reset();
        assert!(
            !presence.has_replied(&conversation),
            "a new turn must start from silence, not inherit the last one's sends"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn typing_stops_re_arming_once_the_agent_has_replied() {
        // The bug this pins: Telegram clears the typing status when the bot's message arrives, so
        // re-arming afterwards tells a user who was just answered that a second message is coming.
        let channel = Arc::new(SpyChannel::new());
        let presence = Arc::new(Presence::default());
        let runner = runner_with(Arc::clone(&channel), Arc::clone(&presence));

        let token = runner.start_typing(&conversations());
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(
            channel.activity_count(),
            1,
            "the opening burst still happens"
        );

        presence.note_sent(&ConversationId::parse("spy:1").expect("valid"));
        tokio::time::sleep(TYPING_REFRESH * 3).await;
        assert_eq!(
            channel.activity_count(),
            1,
            "nothing more may be sent after the reply landed"
        );
        token.cancel();
    }

    #[tokio::test(start_paused = true)]
    async fn cancelling_stops_the_indicator_promptly() {
        // What actually ends the indicator on a normal turn. Nothing covered it, and the ceiling
        // used to be 30s, so a leak here would have looked like a brief tail rather than the
        // half-hour one the turn budget now allows.
        let channel = Arc::new(SpyChannel::new());
        let runner = runner_with(Arc::clone(&channel), Arc::new(Presence::default()));

        let token = runner.start_typing(&conversations());
        tokio::time::sleep(TYPING_REFRESH * 3).await;
        assert!(
            channel.activity_count() > 1,
            "the indicator must be live to begin with"
        );

        token.cancel();
        // Settled past one refresh interval before measuring, so what follows proves the loop has
        // stopped for good rather than merely slowed.
        tokio::time::sleep(TYPING_REFRESH).await;
        let after_cancel = channel.activity_count();
        tokio::time::sleep(TYPING_REFRESH * 20).await;
        assert_eq!(
            channel.activity_count(),
            after_cancel,
            "the indicator kept refreshing after the turn ended"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn cancelling_abandons_a_typing_request_that_has_not_gone_out_yet() {
        // Discord queues typing calls behind the channel's rate limit bucket, so one can sit for
        // seconds before it is sent. Cancelling has to drop the request, not just decline to make
        // the next one: a bridge that spun on a refused submission left thousands of them queued,
        // and the platform went on drawing the indicator for minutes after meka had gone idle.
        let channel = Arc::new(SpyChannel::slow(Duration::from_secs(10)));
        let runner = runner_with(Arc::clone(&channel), Arc::new(Presence::default()));

        let token = runner.start_typing(&conversations());
        tokio::time::sleep(Duration::from_secs(1)).await;
        token.cancel();

        tokio::time::sleep(Duration::from_secs(30)).await;
        assert_eq!(
            channel.activity_count(),
            0,
            "a cancelled indicator still reached the platform"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn typing_lapses_once_the_turn_outlives_the_window() {
        // A turn grinding through tool calls is working, not composing. Holding the indicator for
        // its whole duration claims something no person would.
        let channel = Arc::new(SpyChannel::new());
        let runner = runner_with(Arc::clone(&channel), Arc::new(Presence::default()));

        let token = runner.start_typing(&conversations());
        tokio::time::sleep(TEST_TYPING_MAX * 4).await;

        let sent = channel.activity_count();
        let ceiling = (TEST_TYPING_MAX.as_secs() / TYPING_REFRESH.as_secs()) as usize + 1;
        assert!(
            sent <= ceiling,
            "held the indicator for {sent} refreshes, past the {ceiling} the window allows"
        );
        assert!(sent > 1, "the window must still cover a normal turn");
        token.cancel();
    }

    #[tokio::test]
    async fn a_refused_submission_does_not_announce_typing() {
        // The indicator claims the agent is composing. A submission meka refuses never reached the
        // agent at all, so announcing it is a plain falsehood -- and since a refused batch is now
        // retried rather than dropped, one flash per attempt reads as a permanently typing bot.
        let channel = Arc::new(SpyChannel::new());
        // Nothing listens on this port, so the submission fails before meka ever sees it.
        let meka = crate::meka::MekaClient::new(&crate::config::MekaConfig {
            base_url: "http://127.0.0.1:1".parse().expect("literal parses"),
            token: crate::config::secret::Secret::new("test", "test"),
            connect_timeout: Duration::from_millis(50),
            turn_timeout: Duration::from_secs(1),
            max_retries: 0,
        })
        .expect("client builds");
        let channels = Arc::new(crate::channel::ChannelRegistry::from_channels([
            Arc::clone(&channel) as Arc<dyn Channel>,
        ]));
        let runner = TurnRunner::new(
            meka,
            channels,
            true,
            TEST_TYPING_MAX,
            Arc::new(Presence::default()),
        );

        let report = runner
            .run(uuid::Uuid::new_v4(), "hello", &conversations())
            .await;
        assert!(report.outcome.is_err(), "nothing is listening on that port");
        assert_eq!(
            channel.activity_count(),
            0,
            "typing was announced for a turn that was never accepted"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn typing_is_not_emitted_at_all_when_disabled() {
        let channel = Arc::new(SpyChannel::new());
        let meka = crate::meka::MekaClient::new(&crate::config::MekaConfig {
            base_url: "http://127.0.0.1:1".parse().expect("literal parses"),
            token: crate::config::secret::Secret::new("test", "test"),
            connect_timeout: Duration::from_secs(1),
            turn_timeout: Duration::from_secs(1),
            max_retries: 0,
        })
        .expect("client builds");
        let channels = Arc::new(crate::channel::ChannelRegistry::from_channels([
            Arc::clone(&channel) as Arc<dyn Channel>,
        ]));
        let runner = TurnRunner::new(
            meka,
            channels,
            false,
            TEST_TYPING_MAX,
            Arc::new(Presence::default()),
        );

        let token = runner.start_typing(&conversations());
        tokio::time::sleep(TYPING_REFRESH * 2).await;
        assert_eq!(channel.activity_count(), 0);
        token.cancel();
    }

    fn report(sends: usize) -> TurnReport {
        TurnReport {
            outcome: Ok(TurnOutcome::Finished {
                stop_reason: "end_turn".to_string(),
                refusal_text: None,
                usage: Usage::default(),
            }),
            sends,
            tool_calls: sends,
            text_length: 10,
            text_preview: "hello".to_string(),
        }
    }

    #[test]
    fn a_turn_without_sends_is_reported_as_silent() {
        assert!(report(0).is_silent());
        assert!(!report(1).is_silent());
    }

    #[test]
    fn a_failed_turn_still_says_whether_the_agent_had_acted() {
        // The distinction the whole retry decision rests on. meka only retries a provider failure
        // while nothing has reached the frontend, so a failure that gets this far may well have a
        // sent message and a shell command behind it, and handing that batch over again would
        // repeat both with the agent none the wiser.
        let failed = |sends, tool_calls| TurnReport {
            outcome: Err(MekaError::Timeout(Duration::from_secs(1))),
            sends,
            tool_calls,
            text_length: 400,
            text_preview: "I'll take a look".to_string(),
        };
        assert!(failed(1, 1).had_side_effects());
        assert!(
            failed(0, 3).had_side_effects(),
            "tool calls count even when nothing was sent"
        );
        assert!(
            !failed(0, 0).had_side_effects(),
            "text alone costs tokens and nothing else"
        );
    }

    fn empty_turn(stop_reason: &str, tool_calls: usize, text: &str) -> TurnReport {
        TurnReport {
            outcome: Ok(TurnOutcome::Finished {
                stop_reason: stop_reason.to_string(),
                refusal_text: None,
                usage: Usage::default(),
            }),
            sends: 0,
            tool_calls,
            text_length: text.chars().count(),
            text_preview: text.to_string(),
        }
    }

    #[test]
    fn a_model_that_returned_nothing_is_recognised() {
        // meka's stand-in for a turn with no content. Nothing ran, so the batch can be retried.
        assert!(
            empty_turn("end_turn", 0, "[The model returned an empty response.]").produced_nothing()
        );
        assert!(
            empty_turn(
                "end_turn",
                0,
                "[The model returned an empty response (stop reason: length).]"
            )
            .produced_nothing()
        );
        assert!(
            empty_turn(
                "max_tokens",
                0,
                "[The model reached its output limit before producing a response.]"
            )
            .produced_nothing()
        );
    }

    #[test]
    fn a_turn_that_actually_did_something_is_not_treated_as_empty() {
        // A real answer that simply was not sent must not be replayed: the agent may have acted.
        assert!(!empty_turn("end_turn", 0, "Sure, here is what I found.").produced_nothing());
        // Tool calls mean side effects may have happened, so replaying is not safe.
        assert!(
            !empty_turn("end_turn", 2, "[The model returned an empty response.]")
                .produced_nothing()
        );
        // A refusal is a real answer, not a failure to produce one.
        assert!(!empty_turn("refusal", 0, "[The model declined to respond.]").produced_nothing());
    }

    #[test]
    fn send_tool_detection_matches_mekas_namespacing() {
        // meka rewrites MCP tools as `mcp__<server>__<tool>`, and the server segment is whatever
        // the operator called this bridge, so only the suffix is stable.
        assert!("mcp__mekabridge__send_message".ends_with(SEND_TOOL_SUFFIX));
        assert!("mcp__my_bridge__send_message".ends_with(SEND_TOOL_SUFFIX));
        assert!(!"mcp__mekabridge__send_file".ends_with(SEND_TOOL_SUFFIX));
        assert!(!"mcp__mekabridge__list_conversations".ends_with(SEND_TOOL_SUFFIX));
        assert!(!"read_file".ends_with(SEND_TOOL_SUFFIX));
    }
}
