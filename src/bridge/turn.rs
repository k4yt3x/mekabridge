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
//! It is raised while the model is writing the arguments to a send tool, and at no other time.
//! meka opens that window with `tool_call.composing`, so the interval is roughly the time spent
//! generating the message: long for a long reply, brief for a short one, absent while the agent is
//! reading, searching or thinking.
//!
//! The window is closed by the next `tool_call.executing` or `tool_call.composing`, whichever comes
//! first, and not by one carrying the id that opened it. meka emits `composing` *without* marking
//! the attempt as having produced output, deliberately, so that a call whose arguments never finish
//! is still safe to retry. That makes the composing window exactly the window in which meka retries
//! the whole provider round, and a retry mints fresh tool ids while the `composing` already on the
//! wire cannot be taken back. Waiting for a matching id there is waiting for one that will never
//! arrive, and the indicator stays up, refreshing, until the turn ends. Matching gains nothing in
//! return: meka accumulates one call at a time, so the only thing that can close a window is that
//! call or the one that displaced it.
//!
//! This used to be held for the whole turn, on the reasoning that a chat which shows typing briefly
//! and then falls silent for minutes reads as a bot that has died rather than one that is thinking.
//! That cost is real and is now accepted, because the other side of it turned out to be worse: an
//! indicator that is up for every one of a dozen tool calls is a claim of "a reply is seconds away"
//! that is wrong nearly all of the time, and a signal that is wrong nearly all of the time teaches
//! people to stop reading it. Silence during a long tool run is at least true.
//!
//! Two consequences worth knowing. On a provider backend that does not stream a call as it is
//! written, `openai-chat-completions` today, meka resolves each call's name and arguments together
//! and the two events arrive back to back, so the window is empty and the indicator is at most a
//! flicker. And a turn that dies mid-call emits no closing event, which is why the token is also
//! cancelled when the turn ends.
//!
//! Cancelling has to abandon any request still in flight, not merely stop making new ones. Both
//! platforms queue rate-limited calls rather than refusing them, so an indicator that is allowed to
//! finish sending after its window has closed goes on being drawn for as long as the backlog takes
//! to drain, which is a chat that claims the agent is working when it has been idle for minutes.

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    channel::{ChannelRegistry, ConversationId},
    meka::{MekaClient, MekaError, TurnOutcome, sse::TurnEvent},
};

/// Which conversations the agent has sent to since the current composing window opened.
///
/// Shared with the outbound sink, which is the only thing that knows a message actually went out.
/// Telegram clears the typing status when a message from the bot arrives, so without this the
/// refresh loop re-arms it seconds later and the user, having just been answered, waits for a
/// second message that is never coming.
///
/// Scoped to the window rather than the turn, and cleared when each one opens. Per turn it would
/// silence every send after the first: the indicator is now dropped at `tool_call.executing`, which
/// is *before* the send tool runs and records itself here, so the record could only ever suppress a
/// later window rather than the one it was written for.
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

/// Whether a meka `notice` is telling the caller that events went missing.
///
/// Two wordings mean it, and they are different situations: the replay ring no longer reaching the
/// position asked for, and the re-attached subscription itself falling behind afterwards. Matched
/// on prose because the event carries no code, which is fragile in the safe direction. A rewording
/// on meka's side means the counters are believed again, which is where they stood before any of
/// this existed, rather than a batch being wrongly discarded. Worth asking meka for a flag on the
/// event.
fn notice_reports_lost_events(text: &str) -> bool {
    text.contains("Replay buffer") || text.contains("Fell behind")
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
    /// Whether meka took the turn at all.
    ///
    /// A refused submission and a turn that ran and failed are both an `Err`, and they mean
    /// opposite things to a caller holding state it spent on the envelope: a turn that ran reached
    /// the agent, a refused one reached nobody. Read off the events rather than off the error,
    /// because the error cannot always tell you: meka's `internal` covers both a submission it
    /// threw out and a provider failure halfway through the work. Any accepted turn opens with
    /// `turn.started`, and a refusal produces no events at all, so the first event of any kind is
    /// the answer.
    ///
    /// A stream that dies before its first event reads as not accepted when it was. That errs
    /// toward showing the agent a backlog twice rather than losing it, which is the right way
    /// round.
    pub accepted: bool,
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
    /// Whether the counters above are known to be missing events.
    ///
    /// Set when a rejoin reports a replay hole: meka retains a bounded ring, so a stream resumed
    /// after a long enough gap comes back with a `notice` saying some events are gone rather than
    /// a transcript that silently skips. Those events can include a send, which makes
    /// [`Self::had_side_effects`] answer "no" for a turn that did act.
    pub counters_incomplete: bool,
}

impl TurnReport {
    /// A turn that never got as far as running, so nothing was produced and nothing was done.
    pub fn failed(error: MekaError) -> Self {
        Self {
            outcome: Err(error),
            accepted: false,
            sends: 0,
            tool_calls: 0,
            text_length: 0,
            text_preview: String::new(),
            counters_incomplete: false,
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
        // A hole in the accounting counts as "it acted". The two readings are not symmetric: a
        // turn wrongly held to have acted costs one unanswered message that is still owed to the
        // agent and reported to whoever is waiting, while one wrongly held to have done nothing is
        // replayed, and the agent repeats work it cannot remember doing.
        self.counters_incomplete || self.sends > 0 || self.tool_calls > 0
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
        // Only a turn that ran to its own end can be called inert. One that was stopped partway
        // produced the stand-in for the round it managed rather than for the turn, and what to do
        // with its batch is the cancellation's decision, not this one's.
        let Ok(TurnOutcome::Finished { stop_reason, .. }) = &self.outcome else {
            return false;
        };
        // Provably is the operative word, and a hole in the accounting is exactly the case where
        // nothing is provable: the counters are a floor, so zero tool calls is not evidence that
        // none ran, and the stand-in describes only the part of the stream that came back. Read
        // without this the turn looks inert, the batch is handed over at once, and the agent
        // repeats a send it cannot remember making. This is the same guard `had_side_effects` makes
        // on the failure path, which is worth nothing if the success path answers first.
        if self.counters_incomplete || self.tool_calls > 0 || stop_reason == "refusal" {
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
    /// How often the indicator is renewed, from
    /// [`crate::config::BridgeConfig::typing_refresh`].
    typing_refresh: Duration,
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
        typing_refresh: Duration,
        typing_max: Duration,
        presence: Arc<Presence>,
    ) -> Self {
        Self {
            meka,
            channels,
            typing_enabled,
            typing_refresh,
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
        // Nothing is raised until the model starts writing a send call. Opening it any earlier,
        // on submission or on `Started`, is a claim that a reply is being written when the agent
        // may be about to spend two minutes reading files.
        let mut typing: Option<CancellationToken> = None;

        let mut sends = 0_usize;
        let mut tool_calls = 0_usize;
        let mut text_length = 0_usize;
        let mut text_preview = String::new();
        let mut counters_incomplete = false;

        let mut accepted = false;
        let result = self
            .meka
            .run_turn(session_id, message, |event| {
                // Any event at all means meka took the turn: a refused submission never opens a
                // stream, so nothing reaches here. `turn.started` is always the first, but this
                // deliberately does not name it -- the point is "something arrived", and tying it to
                // one variant would go quiet the day meka reorders its opening events.
                accepted = true;
                match event {
                TurnEvent::AssistantText { text } => {
                    text_length += text.chars().count();
                    // Bounded: a long answer would otherwise be held in memory for a log line.
                    if text_preview.chars().count() < TEXT_PREVIEW_CHARS {
                        text_preview.push_str(text);
                    }
                }
                TurnEvent::ToolCallComposing { name, .. } => {
                    // Closes first, unconditionally. A second `composing` means the previous call
                    // is over one way or another: either its arguments finished, or meka retried
                    // the round and the call being announced now is its replacement. Clearing the
                    // presence record belongs to `start_typing`, which owns the window it opens.
                    if let Some(previous) = typing.take() {
                        previous.cancel();
                    }
                    if !name.ends_with(SEND_TOOL_SUFFIX) {
                        return;
                    }
                    typing = Some(self.start_typing(conversations));
                }
                TurnEvent::ToolCallStarted { name, .. } => {
                    tool_calls += 1;
                    if name.ends_with(SEND_TOOL_SUFFIX) {
                        sends += 1;
                    }
                    // The arguments are written, so whatever was being composed is composed. Closed
                    // here rather than left to the send landing, because a send that fails never
                    // lands and would leave the indicator up until the turn ended.
                    if let Some(typing) = typing.take() {
                        typing.cancel();
                    }
                    tracing::debug!(tool = %name, "agent tool call");
                }
                TurnEvent::Notice { level, text } => {
                    if notice_reports_lost_events(text) {
                        counters_incomplete = true;
                    }
                    tracing::warn!(level = %level, "meka notice: {}", text);
                }
                TurnEvent::ContextCompacted {
                    source,
                    replaced_count,
                    generation,
                } => {
                    // Worth a line at warn because of what this session is. One permanent context
                    // holds everyone the agent has ever spoken to, on every platform, so a
                    // compaction is the moment its memory of conversations nobody is currently
                    // having becomes a summary. Nothing here can prevent it; an operator wondering
                    // why the agent forgot somebody should be able to find when.
                    tracing::warn!(
                        source = %source,
                        replaced = replaced_count,
                        generation,
                        "meka compacted the session; earlier conversations are now a summary"
                    );
                }
                TurnEvent::PermissionRequired { tool_name, .. } => {
                    // Sessions declare `supports_permission_prompts: false`, so meka denies a gated
                    // tool immediately rather than emitting this. Reaching here means the turn is
                    // about to stall for the full timeout with nothing able to answer.
                    tracing::error!(
                        tool = %tool_name,
                        "meka asked for permission, but this bridge has no approval channel; the \
                         turn will stall and deny. meka only prompts at [session].permission = \
                         \"ask\", so that is what to change; use \"read\" or \"unrestricted\"."
                    );
                }
                _ => {}
                }
            })
            .await;

        if let Some(typing) = typing {
            typing.cancel();
        }

        // The counters are kept on the error path too. They used to be dropped with the `Ok`, which
        // left the caller unable to tell a turn that failed before the agent did anything from one
        // that failed after it had already answered somebody.
        //
        // A turn that ended in `sse-lag` is the case those counters cannot describe: meka dropped
        // events from this client's view before cancelling, so a send may have happened and left no
        // trace here. Counting that as "did nothing" is what hands the batch back for a second
        // delivery.
        let counters_incomplete =
            counters_incomplete || result.as_ref().is_err_and(MekaError::dropped_events);
        TurnReport {
            outcome: result,
            accepted,
            sends,
            tool_calls,
            text_length,
            text_preview,
            counters_incomplete,
        }
    }

    /// Open the indicator in each conversation until the returned token is cancelled, the agent
    /// replies there, or the ceiling is reached.
    ///
    /// Called only while the model is writing a send call's arguments. `conversations` is the batch
    /// the turn was submitted for rather than the message's actual target, which is not knowable
    /// yet: `tool_call.composing` carries the tool name and nothing else, because no argument has
    /// streamed. For the single-conversation batch that per-conversation readiness makes the common
    /// case, the two are the same; for a batch spanning several chats it briefly shows the
    /// indicator in one that is not being answered.
    fn start_typing(&self, conversations: &BTreeSet<ConversationId>) -> CancellationToken {
        let token = CancellationToken::new();
        if !self.typing_enabled {
            return token;
        }
        // Whatever a previous window recorded is stale the moment a new one opens. `Presence` stops
        // the refresh loop re-arming after a reply has landed, which was right when one indicator
        // covered a whole turn; per message it reads backwards, because a fresh composing event on
        // a send tool is exactly the evidence that another message is coming. Left standing, the
        // record would silence every send after the first: the token is now dropped at
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
            let typing_max = self.typing_max;
            let typing_refresh = self.typing_refresh;
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
    use crate::{
        channel::{
            Activity, Channel, ChannelCapabilities, ChannelError, ChannelId, ChannelIdentity,
            FetchedFile, InboundEvent, Platform, SendOptions,
        },
        meka::sse::Usage,
    };

    /// Both of meka's wordings, copied from the emitting sites rather than paraphrased. They are
    /// the only signal that a rejoin lost events, so a drift in either silently restores the bug
    /// where a turn that had already sent gets handed back to send again.
    #[test]
    fn both_of_mekas_lost_event_notices_are_recognised() {
        assert!(notice_reports_lost_events(
            "Replay buffer does not reach your Last-Event-ID; some events were dropped."
        ));
        assert!(notice_reports_lost_events(
            "Fell behind; 12 event(s) were dropped from this replay."
        ));
        // An ordinary notice must not make the counters look untrustworthy: that would mark every
        // batch spent and stop anything ever being retried.
        assert!(!notice_reports_lost_events(
            "Session was compacted before this turn."
        ));
    }

    /// A short ceiling, so the test that proves the indicator lapses does not take half an hour.
    /// Production follows `[meka].turn_timeout` unless the operator pins it.
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
        TurnRunner::new(
            meka,
            channels,
            true,
            TYPING_REFRESH,
            TEST_TYPING_MAX,
            presence,
        )
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
    async fn a_new_window_draws_even_in_a_chat_just_answered() {
        // The regression the per-message rework introduced. The gate stops a *live* loop re-arming
        // after a reply; it must not stop the next message being announced, and under the new
        // scheme every window after the first would otherwise draw nothing, because the indicator
        // is dropped before the send tool runs and records itself.
        let channel = Arc::new(SpyChannel::new());
        let presence = Arc::new(Presence::default());
        let runner = runner_with(Arc::clone(&channel), Arc::clone(&presence));

        let first = runner.start_typing(&conversations());
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(channel.activity_count(), 1);
        presence.note_sent(&ConversationId::parse("spy:1").expect("valid"));
        first.cancel();

        let second = runner.start_typing(&conversations());
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
            TYPING_REFRESH,
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
            TYPING_REFRESH,
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
            accepted: true,
            outcome: Ok(TurnOutcome::Finished {
                stop_reason: "end_turn".to_string(),
                refusal_text: None,
                usage: Usage::default(),
            }),
            sends,
            tool_calls: sends,
            text_length: 10,
            text_preview: "hello".to_string(),
            counters_incomplete: false,
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
            accepted: true,
            outcome: Err(MekaError::Timeout(Duration::from_secs(1))),
            sends,
            tool_calls,
            text_length: 400,
            text_preview: "I'll take a look".to_string(),
            counters_incomplete: false,
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
            accepted: true,
            outcome: Ok(TurnOutcome::Finished {
                stop_reason: stop_reason.to_string(),
                refusal_text: None,
                usage: Usage::default(),
            }),
            sends: 0,
            tool_calls,
            text_length: text.chars().count(),
            text_preview: text.to_string(),
            counters_incomplete: false,
        }
    }

    #[test]
    fn a_turn_with_a_hole_in_its_accounting_is_never_called_inert() {
        // The two predicates have to agree, and here they did not. `had_side_effects` treats a
        // reported replay hole as "it acted"; `produced_nothing` looked only at the counters, so
        // both answered yes for the same report and the caller's first arm -- the one that hands
        // the batch straight back -- won. The path is ordinary: the agent answers through
        // send_message and writes no text, so meka's final round emits the empty-response
        // stand-in, and a rejoin that outran the replay ring leaves `tool_calls` at zero
        // for a turn that had already sent.
        let mut report = empty_turn("end_turn", 0, "[The model returned an empty response.]");
        assert!(report.produced_nothing());
        report.counters_incomplete = true;
        assert!(
            !report.produced_nothing(),
            "a turn whose events are known to be missing cannot be called provably inert"
        );
        assert!(report.had_side_effects(), "and the two must not disagree");
    }

    #[test]
    fn a_cancelled_turn_is_not_diagnosed_as_an_empty_response() {
        // Stopped partway, so the stand-in describes the round it managed rather than the turn.
        // Taken for an empty response the batch is requeued with no backoff and the log, the stored
        // reason and the owner's notice all name the wrong cause.
        let report = TurnReport {
            accepted: true,
            outcome: Ok(TurnOutcome::Cancelled {
                reason: "client".to_string(),
            }),
            sends: 0,
            tool_calls: 0,
            text_length: 0,
            text_preview: "[The model returned an empty response.]".to_string(),
            counters_incomplete: false,
        };
        assert!(!report.produced_nothing());
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
    fn a_turn_whose_accounting_has_a_hole_counts_as_having_acted() {
        // A rejoin that outran meka's replay ring comes back saying some events are gone. Those can
        // include a send, so the counters understate what happened, and the two readings are not
        // symmetric: held to have acted, the batch is one unanswered message still owed to the
        // agent; held to have done nothing, it is replayed and the agent repeats work it cannot
        // remember doing.
        let mut report = report(0);
        report.tool_calls = 0;
        assert!(!report.had_side_effects());
        report.counters_incomplete = true;
        assert!(report.had_side_effects());
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
