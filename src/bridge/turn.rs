//! What a turn on the feed is doing, and the presence signalling that goes with it.
//!
//! The typing window is closed by the next `tool_call.executing` or `tool_call.composing`, not by
//! one carrying the id that opened it. meka emits `composing` without marking the attempt as having
//! produced output, so a call whose arguments never finish is safe to retry, and a retry mints
//! fresh tool ids while the `composing` already on the wire cannot be taken back. Matching on the
//! id waits for one that never arrives, leaving the indicator up until the turn ends.
//!
//! A turn that dies mid-call emits no closing event either, which is why the window is closed when
//! the turn ends rather than only by an event.
//!
//! Cancelling has to abandon a request already in flight, not merely stop starting new ones: both
//! platforms queue rate-limited calls rather than refusing them, so one allowed to finish keeps the
//! indicator drawn for as long as the backlog takes to drain.

use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
    time::Duration,
};

use tokio_util::sync::CancellationToken;

use crate::{
    channel::{ChannelRegistry, ConversationId},
    meka::sse::TurnEvent,
};

/// Which conversations the agent has sent to since the current composing window opened.
///
/// Telegram clears the typing status when a message from the bot arrives, so without this the
/// refresh loop re-arms it seconds later and somebody who has just been answered waits for a second
/// message that is never coming.
///
/// Scoped to the window rather than the turn: the indicator drops at `tool_call.executing`, which
/// is *before* the send tool runs and records itself here, so a per-turn record could only suppress
/// a later window rather than the one it was written for.
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

    /// Whether the agent has already sent to `conversation` this window.
    pub fn has_replied(&self, conversation: &ConversationId) -> bool {
        self.replied
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .contains(conversation)
    }

    /// Forget everything, so the next window starts from silence.
    pub fn reset(&self) {
        self.replied
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
    }
}

/// Whether a meka `notice` is telling the caller that events went missing.
///
/// Two situations mean it: the replay ring no longer reaching the position asked for, and the
/// re-attached subscription falling behind afterwards. Matched on prose because the event carries
/// no code, which is fragile only in the cheap direction: a rewording means one reconciliation is
/// not run on the notice, and every reconnect runs one anyway.
///
/// The gap notice is matched on the clause it has kept across rewordings rather than on its
/// opening, which is what went stale: meka 0.46 replaced "Replay buffer does not reach" with "the
/// replay does not reach", so a match on the first two words stopped firing a release before this
/// bridge's own floor and nothing failed to say so.
pub fn notice_reports_lost_events(text: &str) -> bool {
    text.contains("does not reach your Last-Event-ID") || text.contains("Fell behind")
}

/// The MCP tool name meka exposes for this bridge's `message_send`, used to tell "the agent
/// replied" apart from "the agent stayed quiet". meka namespaces MCP tools as
/// `mcp__<server>__<tool>`, and the server segment is whatever the operator named this bridge in
/// meka's config, so the match is on the suffix.
pub const SEND_TOOL_SUFFIX: &str = "__message_send";

/// How much assistant text to keep for diagnostics. Enough to hold any of meka's empty-turn
/// stand-ins whole, and to show the opening of a real answer that never got delivered.
const TEXT_PREVIEW_CHARS: usize = 240;

/// Ceiling on how many turns are tracked at once, past which what is held is dropped.
///
/// A turn is forgotten when its terminal arrives, and a terminal lost to a replay hole would
/// otherwise leave its entry behind for the life of the process. Far above the one turn a session
/// runs at a time, so reaching it means terminals are going missing rather than that a session is
/// busy.
pub const MAX_TRACKED_TURNS: usize = 1024;

/// What one turn has done so far, tallied off the feed.
///
/// For logging, for deciding whether to warn about a silent turn, and for the one case where a
/// delivered message is offered again: the model came back with nothing at all.
#[derive(Debug, Default)]
pub struct TurnTally {
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
    /// This bridge's messages the model read in this turn, by queue sequence. Filled from
    /// `inbox.delivered`, not from the turn's opening: a turn opened on an item and failed before
    /// the provider accepted the request never showed the model anything.
    pub read: Vec<i64>,
}

impl TurnTally {
    /// Count what `event` says the agent did.
    pub fn note(&mut self, event: &TurnEvent) {
        match event {
            TurnEvent::AssistantText { text } => {
                self.text_length += text.chars().count();
                // Bounded: a long answer would otherwise be held in memory for a log line.
                if self.text_preview.chars().count() < TEXT_PREVIEW_CHARS {
                    self.text_preview.push_str(text);
                }
            }
            TurnEvent::ToolCallStarted { name, .. } => {
                self.tool_calls += 1;
                if name.ends_with(SEND_TOOL_SUFFIX) {
                    self.sends += 1;
                }
            }
            _ => {}
        }
    }

    /// A turn that produced text but sent nothing.
    ///
    /// Legal, and sometimes correct, but almost always worth a log line: from the user's side it is
    /// indistinguishable from the bridge being broken.
    pub const fn is_silent(&self) -> bool {
        self.sends == 0
    }

    /// Whether the turn did nothing at all because the model came back empty.
    ///
    /// meka streams a bracketed stand-in as the assistant text when a turn yields no content, and
    /// paired with zero tool calls that is provably inert, so what it read can be offered again.
    /// Refusals are excluded, being a real answer.
    ///
    /// Matching meka's wording is brittle but degrades safely: a rewording leaves the turn treated
    /// as silent and logged rather than offered again.
    pub fn produced_nothing(&self, stop_reason: &str) -> bool {
        if self.tool_calls > 0 || stop_reason == "refusal" {
            return false;
        }
        let text = self.text_preview.trim();
        text.starts_with("[The model ") && text.ends_with(']')
    }
}

/// Draws the typing indicator in the chats a turn is answering, while the model writes a message.
///
/// Keyed by turn because the feed carries every turn on the session, and a window opened by one
/// must not be closed by the events of the next. The conversations a turn is answering are those
/// of the items it opened on or read, which is the best guess available: `tool_call.composing`
/// carries the tool name and nothing else, so the message's actual target is not knowable until
/// the call runs.
pub struct TurnTypist {
    channels: Arc<ChannelRegistry>,
    enabled: bool,
    /// How often the indicator is renewed, from
    /// [`crate::config::BridgeConfig::typing_refresh`].
    refresh: Duration,
    /// Ceiling on how long the indicator is held, from
    /// [`crate::config::BridgeConfig::typing_max`].
    max: Duration,
    /// Shared with the outbound sink so the indicator can stop once a reply has actually landed.
    presence: Arc<Presence>,
    /// The open composing window of each turn.
    windows: HashMap<String, CancellationToken>,
    /// Which conversations each turn is answering.
    targets: HashMap<String, BTreeSet<ConversationId>>,
}

impl TurnTypist {
    pub fn new(
        channels: Arc<ChannelRegistry>,
        enabled: bool,
        refresh: Duration,
        max: Duration,
        presence: Arc<Presence>,
    ) -> Self {
        Self {
            channels,
            enabled,
            refresh,
            max,
            presence,
            windows: HashMap::new(),
            targets: HashMap::new(),
        }
    }

    /// A turn began. Sends from an earlier turn must not suppress this one's indicator.
    pub fn begin(&mut self, turn: &str) {
        self.presence.reset();
        // Bounded by dropping what is held, the same rule the notice log uses. Every window is
        // cancelled on the way out, so nothing is left drawing in a chat; what is lost is the
        // record of which chats a turn already under way is answering, and the worst of that is an
        // indicator that does not appear.
        if self.targets.len() >= MAX_TRACKED_TURNS {
            tracing::warn!(
                "tracking {} turns, so terminals are going missing; forgetting what is held",
                self.targets.len()
            );
            for (_, window) in self.windows.drain() {
                window.cancel();
            }
            self.targets.clear();
        }
        self.targets.entry(turn.to_string()).or_default();
    }

    /// Note that `turn` is answering `conversations`, on top of any it already was.
    pub fn expect(&mut self, turn: &str, conversations: impl IntoIterator<Item = ConversationId>) {
        self.targets
            .entry(turn.to_string())
            .or_default()
            .extend(conversations);
    }

    /// Open or close a window on a tool-call event of `turn`.
    pub fn observe(&mut self, turn: &str, event: &TurnEvent) {
        match event {
            TurnEvent::ToolCallComposing { name, .. } => {
                // Closes first, unconditionally. A second `composing` means the previous call
                // is over one way or another: either its arguments finished, or meka retried
                // the round and the call being announced now is its replacement. Clearing the
                // presence record belongs to `start_typing`, which owns the window it opens.
                if let Some(previous) = self.windows.remove(turn) {
                    previous.cancel();
                }
                if !name.ends_with(SEND_TOOL_SUFFIX) {
                    return;
                }
                let Some(conversations) = self.targets.get(turn) else {
                    // A turn this bridge handed nothing to has nobody waiting on it.
                    return;
                };
                let token = self.start_typing(conversations);
                self.windows.insert(turn.to_string(), token);
            }
            TurnEvent::ToolCallStarted { .. } => {
                // The arguments are written, so whatever was being composed is composed. Closed
                // here rather than left to the send landing, because a send that fails never
                // lands and would leave the indicator up until the turn ended.
                if let Some(window) = self.windows.remove(turn) {
                    window.cancel();
                }
            }
            _ => {}
        }
    }

    /// The turn ended; nothing it was composing is coming.
    pub fn close(&mut self, turn: &str) {
        if let Some(window) = self.windows.remove(turn) {
            window.cancel();
        }
        self.targets.remove(turn);
    }

    /// Open the indicator in each conversation until the returned token is cancelled, the agent
    /// replies there, or the ceiling is reached.
    ///
    /// Called only while the model is writing a send call's arguments.
    fn start_typing(&self, conversations: &BTreeSet<ConversationId>) -> CancellationToken {
        let token = CancellationToken::new();
        if !self.enabled {
            return token;
        }
        // Whatever a previous window recorded is stale the moment a new one opens. `Presence` stops
        // the refresh loop re-arming after a reply has landed, which was right when one indicator
        // covered a whole turn; per message it reads backwards, because a fresh composing event on
        // a send tool is exactly the evidence that another message is coming. Left standing, the
        // record would silence every send after the first: the token is dropped at
        // `tool_call.executing`, before the send tool runs and records itself, so it can only ever
        // suppress a later window rather than the one it was written for.
        self.presence.reset();
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
            let typing_max = self.max;
            let typing_refresh = self.refresh;
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
                            "the window has run past the typing ceiling; letting the indicator lapse"
                        );
                        return;
                    }
                    // Raced against the token rather than simply awaited. A rate-limited platform
                    // parks the request instead of refusing it -- twilight queues typing calls
                    // behind the channel's bucket -- so a request that outlives its window has to
                    // be abandoned, not merely followed by no more. Otherwise a burst of indicators
                    // goes on arriving long after the agent stopped.
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
                        () = tokio::time::sleep(typing_refresh) => {}
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
    use crate::channel::{
        Activity, Channel, ChannelCapabilities, ChannelError, ChannelId, ChannelIdentity,
        FetchedFile, InboundEvent, Platform, SendOptions,
    };

    /// Every wording meka has emitted, copied from the emitting sites rather than paraphrased.
    /// A drift in any of them means one reconciliation is not run on the notice; the reconnect
    /// runs one regardless, which is why this is cheap to get wrong and still worth pinning.
    ///
    /// Both spellings of the gap notice are here because pinning only the one this bridge was
    /// written against is what hid the drift: meka reworded it in 0.46, the assertion and the
    /// matcher agreed with each other on a string meka no longer sent, and the test went on
    /// passing.
    #[test]
    fn every_wording_of_mekas_lost_event_notices_is_recognised() {
        // meka 0.46 onwards, which is this bridge's floor.
        assert!(notice_reports_lost_events(
            "the replay does not reach your Last-Event-ID, so events were dropped; read `GET \
             /v1/sessions/{id}/messages` for the full transcript"
        ));
        // Before 0.46. Kept because reading a gap notice wrongly is the expensive direction, and
        // matching a string no meka still sends costs nothing.
        assert!(notice_reports_lost_events(
            "Replay buffer does not reach your Last-Event-ID; some events were dropped."
        ));
        assert!(notice_reports_lost_events(
            "Fell behind; 12 event(s) were dropped from this replay."
        ));
        // An ordinary notice must not trigger a reconciliation per notice.
        assert!(!notice_reports_lost_events(
            "Session was compacted before this turn."
        ));
    }

    /// A short ceiling, so the test that proves the indicator lapses does not take two minutes.
    const TEST_TYPING_MAX: Duration = Duration::from_secs(30);

    /// What the shipped default is; these tests assert against the interval they configure.
    const TYPING_REFRESH: Duration = Duration::from_secs(4);

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

    /// Stand in for the record a platform returns when it accepts a send.
    fn echo(message_id: &str, text: &str) -> crate::channel::SentMessage {
        crate::channel::SentMessage {
            message_id: message_id.to_string(),
            text: text.to_string(),
            sender: crate::channel::Sender {
                id: "4242".to_string(),
                display_name: "Mica".to_string(),
                username: None,
                is_bot: true,
                on_behalf_of_chat: false,
            },
            attachments: Vec::new(),
            notes: Vec::new(),
            timestamp: chrono::Utc::now(),
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
            markdown: &str,
            _options: &SendOptions,
            sent: &mut Vec<crate::channel::SentMessage>,
        ) -> Result<(), ChannelError> {
            sent.push(echo("m1", markdown));
            Ok(())
        }

        async fn send_files(
            &self,
            _conversation: &ConversationId,
            _paths: &[std::path::PathBuf],
            caption: Option<&str>,
            _options: &crate::channel::FileOptions,
            sent: &mut Vec<crate::channel::SentMessage>,
        ) -> Result<(), ChannelError> {
            sent.push(echo("f1", caption.unwrap_or("")));
            Ok(())
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

    fn typist_with(channel: Arc<SpyChannel>, presence: Arc<Presence>, enabled: bool) -> TurnTypist {
        let channels = Arc::new(crate::channel::ChannelRegistry::from_channels([
            channel as Arc<dyn Channel>
        ]));
        TurnTypist::new(channels, enabled, TYPING_REFRESH, TEST_TYPING_MAX, presence)
    }

    fn conversations() -> BTreeSet<ConversationId> {
        let mut set = BTreeSet::new();
        set.insert(ConversationId::parse("spy:1").expect("valid"));
        set
    }

    fn composing(name: &str) -> TurnEvent {
        TurnEvent::ToolCallComposing {
            id: "c1".to_string(),
            name: name.to_string(),
        }
    }

    fn executing(name: &str) -> TurnEvent {
        TurnEvent::ToolCallStarted {
            id: "c1".to_string(),
            name: name.to_string(),
            display_summary: None,
        }
    }

    /// A turn is forgotten on its terminal, and a terminal lost to a replay hole would otherwise
    /// leave its entry behind for good. Bounded by dropping the lot, with every window cancelled
    /// so nothing is left claiming somebody is being written to.
    #[tokio::test(start_paused = true)]
    async fn tracking_is_bounded_when_terminals_go_missing() {
        let channel = Arc::new(SpyChannel::new());
        let mut typist = typist_with(Arc::clone(&channel), Arc::new(Presence::default()), true);
        typist.begin("t0");
        typist.expect("t0", conversations());
        typist.observe("t0", &composing("mcp__mekabridge__message_send"));
        tokio::time::sleep(Duration::from_millis(10)).await;
        let drawn = channel.activity_count();
        assert!(drawn >= 1, "the window has to be open to begin with");

        for index in 0..MAX_TRACKED_TURNS {
            typist.begin(&format!("spare{index}"));
        }
        assert!(typist.targets.len() <= MAX_TRACKED_TURNS, "unbounded");
        tokio::time::sleep(TYPING_REFRESH * 3).await;
        assert_eq!(
            channel.activity_count(),
            drawn,
            "a window dropped with its turn went on drawing"
        );
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
            "a new window must start from silence, not inherit the last one's sends"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn typing_stops_re_arming_once_the_agent_has_replied() {
        // The bug this pins: Telegram clears the typing status when the bot's message arrives, so
        // re-arming afterwards tells a user who was just answered that a second message is coming.
        let channel = Arc::new(SpyChannel::new());
        let presence = Arc::new(Presence::default());
        let typist = typist_with(Arc::clone(&channel), Arc::clone(&presence), true);

        let token = typist.start_typing(&conversations());
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
    async fn a_new_window_draws_even_in_a_chat_just_answered() {
        // The gate stops a *live* loop re-arming after a reply; it must not stop the next message
        // being announced, since the indicator is dropped before the send tool runs and records
        // itself, so every window after the first would otherwise draw nothing.
        let channel = Arc::new(SpyChannel::new());
        let presence = Arc::new(Presence::default());
        let typist = typist_with(Arc::clone(&channel), Arc::clone(&presence), true);

        let first = typist.start_typing(&conversations());
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(channel.activity_count(), 1);
        presence.note_sent(&ConversationId::parse("spy:1").expect("valid"));
        first.cancel();

        let second = typist.start_typing(&conversations());
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(
            channel.activity_count(),
            2,
            "the second message in a turn was never announced"
        );
        second.cancel();
    }

    #[tokio::test(start_paused = true)]
    async fn cancelling_stops_the_indicator_promptly() {
        let channel = Arc::new(SpyChannel::new());
        let typist = typist_with(Arc::clone(&channel), Arc::new(Presence::default()), true);

        let token = typist.start_typing(&conversations());
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
            "the indicator kept refreshing after the window closed"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn cancelling_abandons_a_typing_request_that_has_not_gone_out_yet() {
        // Discord queues typing calls behind the channel's rate limit bucket, so one can sit for
        // seconds before it is sent. Cancelling has to drop the request, not just decline to make
        // the next one, or the platform goes on drawing the indicator for minutes after the agent
        // has gone quiet.
        let channel = Arc::new(SpyChannel::slow(Duration::from_secs(10)));
        let typist = typist_with(Arc::clone(&channel), Arc::new(Presence::default()), true);

        let token = typist.start_typing(&conversations());
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
    async fn typing_lapses_once_the_window_outlives_the_ceiling() {
        // A turn grinding through tool calls is working, not composing. Holding the indicator for
        // its whole duration claims something no person would.
        let channel = Arc::new(SpyChannel::new());
        let typist = typist_with(Arc::clone(&channel), Arc::new(Presence::default()), true);

        let token = typist.start_typing(&conversations());
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

    #[tokio::test(start_paused = true)]
    async fn typing_is_not_emitted_at_all_when_disabled() {
        let channel = Arc::new(SpyChannel::new());
        let typist = typist_with(Arc::clone(&channel), Arc::new(Presence::default()), false);

        let token = typist.start_typing(&conversations());
        tokio::time::sleep(TYPING_REFRESH * 2).await;
        assert_eq!(channel.activity_count(), 0);
        token.cancel();
    }

    /// The window is opened by a send tool composing and closed by the next call announced,
    /// whichever it is, and only for a turn that is answering somebody.
    #[tokio::test(start_paused = true)]
    async fn a_composing_send_opens_a_window_the_next_call_closes() {
        let channel = Arc::new(SpyChannel::new());
        let mut typist = typist_with(Arc::clone(&channel), Arc::new(Presence::default()), true);

        // A turn nobody here is waiting on draws nothing, whatever it composes.
        typist.begin("t0");
        typist.observe("t0", &composing("mcp__mekabridge__message_send"));
        tokio::time::sleep(TYPING_REFRESH * 2).await;
        assert_eq!(channel.activity_count(), 0, "no target, no indicator");
        typist.close("t0");

        typist.begin("t1");
        typist.expect("t1", conversations());
        // Reading files is not writing a message.
        typist.observe("t1", &composing("read_file"));
        typist.observe("t1", &executing("read_file"));
        tokio::time::sleep(TYPING_REFRESH * 2).await;
        assert_eq!(channel.activity_count(), 0, "a non-send call draws nothing");

        typist.observe("t1", &composing("mcp__mekabridge__message_send"));
        tokio::time::sleep(TYPING_REFRESH * 2 + Duration::from_millis(10)).await;
        let while_composing = channel.activity_count();
        assert!(
            while_composing >= 2,
            "the window was not drawn: {while_composing}"
        );

        // The arguments are finished: the next event on the turn closes the window, and a
        // different turn's events leave it alone.
        typist.observe("t2", &executing("mcp__mekabridge__message_send"));
        typist.observe("t1", &executing("mcp__mekabridge__message_send"));
        tokio::time::sleep(TYPING_REFRESH * 3).await;
        assert_eq!(
            channel.activity_count(),
            while_composing,
            "the window stayed open past the call that closed it"
        );
        typist.close("t1");
    }

    /// A composing call meka abandoned and replaced, which is what its provider retry looks like:
    /// no closing event for the first id ever arrives, so the replacement has to close it.
    #[tokio::test(start_paused = true)]
    async fn an_abandoned_composing_call_is_closed_by_its_replacement() {
        let channel = Arc::new(SpyChannel::new());
        let mut typist = typist_with(Arc::clone(&channel), Arc::new(Presence::default()), true);
        typist.begin("t1");
        typist.expect("t1", conversations());
        typist.observe("t1", &composing("mcp__mekabridge__message_send"));
        tokio::time::sleep(TYPING_REFRESH + Duration::from_millis(10)).await;
        let drawn = channel.activity_count();
        assert!(drawn >= 1);

        typist.observe("t1", &composing("read_file"));
        tokio::time::sleep(TYPING_REFRESH * 3).await;
        assert_eq!(
            channel.activity_count(),
            drawn,
            "the abandoned window kept drawing"
        );

        // And a turn ending closes whatever was left open.
        typist.observe("t1", &composing("mcp__mekabridge__message_send"));
        tokio::time::sleep(Duration::from_millis(10)).await;
        let reopened = channel.activity_count();
        typist.close("t1");
        tokio::time::sleep(TYPING_REFRESH * 3).await;
        assert_eq!(
            channel.activity_count(),
            reopened,
            "the turn ended with a window open"
        );
    }

    fn tally(sends: usize, tool_calls: usize, text: &str) -> TurnTally {
        let mut tally = TurnTally::default();
        for _ in 0..sends {
            tally.note(&executing("mcp__mekabridge__message_send"));
        }
        for _ in sends..tool_calls {
            tally.note(&executing("read_file"));
        }
        tally.note(&TurnEvent::AssistantText {
            text: text.to_string(),
        });
        tally
    }

    #[test]
    fn a_turn_without_sends_is_reported_as_silent() {
        assert!(tally(0, 0, "hello").is_silent());
        assert!(tally(0, 3, "hello").is_silent());
        assert!(!tally(1, 1, "hello").is_silent());
    }

    #[test]
    fn a_model_that_returned_nothing_is_recognised() {
        // meka's stand-in for a turn with no content. Nothing ran, so what it read can be offered
        // again.
        assert!(
            tally(0, 0, "[The model returned an empty response.]").produced_nothing("end_turn")
        );
        assert!(
            tally(
                0,
                0,
                "[The model returned an empty response (stop reason: length).]"
            )
            .produced_nothing("end_turn")
        );
        assert!(
            tally(
                0,
                0,
                "[The model reached its output limit before producing a response.]"
            )
            .produced_nothing("max_tokens")
        );
    }

    #[test]
    fn a_turn_that_actually_did_something_is_not_treated_as_empty() {
        // A real answer that simply was not sent must not be offered again: the agent may have
        // acted on it in ways the tally cannot see.
        assert!(!tally(0, 0, "Sure, here is what I found.").produced_nothing("end_turn"));
        // Tool calls mean side effects may have happened, so offering again is not safe.
        assert!(
            !tally(0, 2, "[The model returned an empty response.]").produced_nothing("end_turn")
        );
        // A refusal is a real answer, not a failure to produce one.
        assert!(!tally(0, 0, "[The model declined to respond.]").produced_nothing("refusal"));
    }

    #[test]
    fn the_text_preview_is_bounded() {
        let mut tally = TurnTally::default();
        for _ in 0..100 {
            tally.note(&TurnEvent::AssistantText {
                text: "0123456789".to_string(),
            });
        }
        assert_eq!(tally.text_length, 1000);
        assert!(tally.text_preview.chars().count() <= TEXT_PREVIEW_CHARS + 10);
    }

    #[test]
    fn send_tool_detection_matches_mekas_namespacing() {
        // meka rewrites MCP tools as `mcp__<server>__<tool>`, and the server segment is whatever
        // the operator called this bridge, so only the suffix is stable.
        assert!("mcp__mekabridge__message_send".ends_with(SEND_TOOL_SUFFIX));
        assert!("mcp__my_bridge__message_send".ends_with(SEND_TOOL_SUFFIX));
        assert!(!"mcp__mekabridge__file_send".ends_with(SEND_TOOL_SUFFIX));
        assert!(!"mcp__mekabridge__conversation_list".ends_with(SEND_TOOL_SUFFIX));
        assert!(!"read_file".ends_with(SEND_TOOL_SUFFIX));
    }
}
