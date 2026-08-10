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

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    channel::{ChannelRegistry, ConversationId},
    meka::{MekaClient, MekaError, TurnImage, TurnOutcome, sse::TurnEvent},
};

/// How often the typing indicator is refreshed. Telegram clears it after about five seconds, so the
/// interval has to sit under that to look continuous.
const TYPING_REFRESH: Duration = Duration::from_secs(4);

/// The MCP tool name meka exposes for this bridge's `send_message`, used to tell "the agent
/// replied" apart from "the agent stayed quiet". meka namespaces MCP tools as
/// `mcp__<server>__<tool>`, and the server segment is whatever the operator named this bridge in
/// meka's config, so the match is on the suffix.
const SEND_TOOL_SUFFIX: &str = "__send_message";

/// How much assistant text to keep for diagnostics. Enough to hold any of meka's empty-turn
/// stand-ins whole, and to show the opening of a real answer that never got delivered.
const TEXT_PREVIEW_CHARS: usize = 240;

/// What a completed turn did, for logging and for deciding whether to warn about a silent turn.
#[derive(Debug, Clone, PartialEq)]
pub struct TurnReport {
    pub outcome: TurnOutcome,
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
    /// A turn that produced text but sent nothing.
    ///
    /// Legal, and sometimes correct, but almost always worth a log line: from the user's side it is
    /// indistinguishable from the bridge being broken.
    pub const fn is_silent(&self) -> bool {
        self.sends == 0
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
            || matches!(self.outcome, TurnOutcome::Finished { ref stop_reason, .. } if stop_reason == "refusal")
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
}

impl TurnRunner {
    pub const fn new(
        meka: MekaClient,
        channels: Arc<ChannelRegistry>,
        typing_enabled: bool,
    ) -> Self {
        Self {
            meka,
            channels,
            typing_enabled,
        }
    }

    /// Submit `message` and drive the turn to completion.
    ///
    /// `conversations` are the ones the batch came from; each gets a typing indicator for the
    /// duration.
    pub async fn run(
        &self,
        session_id: Uuid,
        message: &str,
        images: &[TurnImage],
        conversations: &BTreeSet<ConversationId>,
    ) -> Result<TurnReport, MekaError> {
        let typing = self.start_typing(conversations);

        let mut sends = 0_usize;
        let mut tool_calls = 0_usize;
        let mut text_length = 0_usize;
        let mut text_preview = String::new();

        let result = self
            .meka
            .run_turn(session_id, message, images, |event| match event {
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

        typing.cancel();

        result.map(|outcome| TurnReport {
            outcome,
            sends,
            tool_calls,
            text_length,
            text_preview,
        })
    }

    /// Keep a typing indicator alive in each conversation until the returned token is cancelled.
    fn start_typing(&self, conversations: &BTreeSet<ConversationId>) -> CancellationToken {
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
            let token = token.child_token();
            tokio::spawn(async move {
                loop {
                    if let Err(error) = channel.set_typing(&conversation).await {
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
    use super::*;
    use crate::meka::sse::Usage;

    fn report(sends: usize) -> TurnReport {
        TurnReport {
            outcome: TurnOutcome::Finished {
                stop_reason: "end_turn".to_string(),
                refusal_text: None,
                usage: Usage::default(),
            },
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

    fn empty_turn(stop_reason: &str, tool_calls: usize, text: &str) -> TurnReport {
        TurnReport {
            outcome: TurnOutcome::Finished {
                stop_reason: stop_reason.to_string(),
                refusal_text: None,
                usage: Usage::default(),
            },
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
