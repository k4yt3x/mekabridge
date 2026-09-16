//! Turn events, and the mapping from meka's SSE wire format onto them.
//!
//! meka names each event (`assistant_text.delta`, `tool_call.executing`, `inbox.delivered`, ...)
//! and puts a JSON object in `data`. Unrecognised names become [`TurnEvent::Unknown`] rather than
//! an error, so a meka that grows a new event type does not break a running bridge.
//!
//! The events come off a session feed rather than one turn's stream: every payload carries the
//! `turn_id` of the turn it belongs to, and the feed carries turns nobody here started, so the id
//! travels beside the event rather than being implied by the connection.

use serde::Deserialize;

/// Who started a turn, off `turn.started`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TurnSource {
    /// A `POST /turn`.
    Client,
    /// meka opened the turn on waiting inbox items, which it names.
    Inbox { item_ids: Vec<String> },
    /// A scheduled job fired.
    Schedule { job_id: String },
    /// A background task reported back and earned a turn of its own.
    Background,
    /// A name this build does not know, or none at all: the `turn.started` meka synthesises when a
    /// feed attaches mid-turn carries no source.
    Unknown(String),
}

/// One event from the session feed.
#[derive(Debug, Clone, PartialEq)]
pub enum TurnEvent {
    Started {
        started_at: String,
        source: TurnSource,
        /// Whether this is the announcement meka synthesises for a turn already running when the
        /// feed attached, rather than a turn beginning. It carries no id, no source and no time,
        /// and everything after it is the real thing.
        resumed: bool,
    },
    AssistantText {
        text: String,
    },
    Thinking {
        text: String,
    },
    /// The model has begun writing a tool call's arguments.
    ///
    /// The only thing on the feed that separates the agent writing a message from the agent doing
    /// anything else. Assistant text is narration around a call rather than the reply itself, and
    /// by [`TurnEvent::ToolCallStarted`] the arguments are already finished. Carries no input,
    /// because none has streamed yet: which conversation a message is for is not known until
    /// the call runs.
    ToolCallComposing {
        id: String,
        name: String,
    },
    ToolCallStarted {
        id: String,
        name: String,
        display_summary: Option<String>,
    },
    ToolCallCompleted {
        id: String,
        is_error: bool,
    },
    Notice {
        level: String,
        text: String,
    },
    /// The conversation was summarised and part of the window replaced.
    ///
    /// The one event on the feed that is not additive. It matters more here than to a client
    /// rendering a transcript: this bridge owns one permanent session shared by everyone on every
    /// platform, so a compaction is the moment its memory of people who are not in the current
    /// conversation becomes a summary.
    ContextCompacted {
        source: String,
        replaced_count: u64,
        generation: u64,
    },
    /// Emitted when a gated tool needs approval, which for this bridge means the session has
    /// meka's `approvals` switch on.
    ///
    /// The turn does not stall on it. Every session here is created with
    /// `supports_permission_prompts: false`, which meka answers by denying the call at once with a
    /// notice rather than parking it on the SSE channel for the 30 minutes a request stays
    /// answerable. So this arrives as a record of a call that was refused, and the level is what
    /// needs raising.
    PermissionRequired {
        request_id: String,
        tool_name: String,
        expires_in_seconds: u64,
    },
    /// The provider accepted a request carrying these inbox items, so the model has read them.
    ///
    /// Arrives inside the turn that read them, before its terminal. This is the one event that
    /// says an item was delivered; nothing is inferred from the turn ending.
    InboxDelivered {
        item_ids: Vec<String>,
    },
    /// meka gave up on an inbox item after its retry ceiling and withdrew it.
    InboxFailed {
        item_id: String,
        reason: String,
    },
    /// An inbox item was taken back before the model read it: a `DELETE`, or the cancellation of
    /// the turn it opened.
    InboxWithdrawn {
        item_id: String,
    },
    Finished {
        stop_reason: String,
        refusal_text: Option<String>,
        usage: Usage,
    },
    Failed {
        error: serde_json::Value,
    },
    Cancelled {
        reason: String,
    },
    /// An event name this build does not know about.
    Unknown {
        event: String,
    },
}

impl TurnEvent {
    /// Whether this event ends a turn. `turn.finished`, `turn.failed`, and `turn.canceled` are
    /// terminal per meka's HTTP API. The feed itself carries on past them.
    pub const fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Finished { .. } | Self::Failed { .. } | Self::Cancelled { .. }
        )
    }
}

/// Token counters reported on `turn.finished`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct Usage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_creation_input_tokens: u64,
    pub cache_read_input_tokens: u64,
}

/// One parsed frame: the event, and which turn it belongs to.
#[derive(Debug, Clone, PartialEq)]
pub struct Parsed {
    /// Off the payload's `turn_id`, which every event meka sends carries; `None` on a frame that
    /// names none, such as the notice about a replay hole.
    pub turn_id: Option<String>,
    pub event: TurnEvent,
}

/// Parse one SSE frame.
///
/// Returns `Ok(None)` for frames that carry no turn event, such as the keep-alive comments and the
/// initial `retry:` directive, which arrive with an empty event name and empty data.
pub fn parse(event_name: &str, data: &str) -> Result<Option<Parsed>, serde_json::Error> {
    if event_name.is_empty() && data.trim().is_empty() {
        return Ok(None);
    }
    let value: serde_json::Value = if data.trim().is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_str(data)?
    };

    let string = |key: &str| -> String {
        value
            .get(key)
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string()
    };
    let optional_string = |key: &str| -> Option<String> {
        value
            .get(key)
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
    };
    let number = |key: &str| -> u64 {
        value
            .get(key)
            .and_then(serde_json::Value::as_u64)
            .unwrap_or_default()
    };
    let strings = |key: &str| -> Vec<String> {
        value
            .get(key)
            .and_then(serde_json::Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(serde_json::Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default()
    };

    let event = match event_name {
        "turn.started" => TurnEvent::Started {
            started_at: string("started_at"),
            source: match optional_string("source").as_deref() {
                Some("client") => TurnSource::Client,
                Some("inbox") => TurnSource::Inbox {
                    item_ids: strings("item_ids"),
                },
                Some("schedule") => TurnSource::Schedule {
                    job_id: string("job_id"),
                },
                Some("background") => TurnSource::Background,
                other => TurnSource::Unknown(other.unwrap_or_default().to_string()),
            },
            resumed: value
                .get("resumed")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false),
        },
        "assistant_text.delta" => TurnEvent::AssistantText {
            text: string("text"),
        },
        "thinking.delta" => TurnEvent::Thinking {
            text: string("text"),
        },
        "tool_call.composing" => TurnEvent::ToolCallComposing {
            id: string("id"),
            name: string("name"),
        },
        "tool_call.executing" => TurnEvent::ToolCallStarted {
            id: string("id"),
            name: string("name"),
            display_summary: optional_string("display_summary"),
        },
        "tool_call.completed" => TurnEvent::ToolCallCompleted {
            id: string("id"),
            is_error: value
                .get("is_error")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false),
        },
        "notice" => TurnEvent::Notice {
            level: string("level"),
            text: string("text"),
        },
        "context.compacted" => TurnEvent::ContextCompacted {
            source: string("source"),
            replaced_count: number("replaced_count"),
            generation: number("generation"),
        },
        "permission_required" => TurnEvent::PermissionRequired {
            request_id: string("request_id"),
            tool_name: string("tool_name"),
            expires_in_seconds: value
                .get("expires_in_seconds")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0),
        },
        "inbox.delivered" => TurnEvent::InboxDelivered {
            item_ids: strings("item_ids"),
        },
        "inbox.failed" => TurnEvent::InboxFailed {
            item_id: string("item_id"),
            reason: string("reason"),
        },
        "inbox.withdrawn" => TurnEvent::InboxWithdrawn {
            item_id: string("item_id"),
        },
        "turn.finished" => TurnEvent::Finished {
            stop_reason: string("stop_reason"),
            refusal_text: optional_string("refusal_text"),
            usage: value
                .get("usage")
                .cloned()
                .map(serde_json::from_value)
                .transpose()
                .unwrap_or_default()
                .unwrap_or_default(),
        },
        "turn.failed" => TurnEvent::Failed {
            error: value
                .get("error")
                .cloned()
                .unwrap_or(serde_json::Value::Null),
        },
        // meka 0.46 respelled this with one `l` and kept no alias. Both are matched because a
        // terminal event that falls through to `Unknown` fails silently rather than loudly:
        // `is_terminal` says no, and the turn's tally is never closed out.
        "turn.canceled" | "turn.cancelled" => TurnEvent::Cancelled {
            reason: string("reason"),
        },
        other => TurnEvent::Unknown {
            event: other.to_string(),
        },
    };
    Ok(Some(Parsed {
        turn_id: optional_string("turn_id").filter(|id| !id.is_empty()),
        event,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(name: &str, data: &str) -> TurnEvent {
        parse(name, data).expect("parses").expect("event").event
    }

    #[test]
    fn parses_assistant_text_delta() {
        assert_eq!(
            event("assistant_text.delta", r#"{"text":"hello"}"#),
            TurnEvent::AssistantText {
                text: "hello".to_string()
            }
        );
    }

    #[test]
    fn parses_finished_with_usage() {
        let data = r#"{"stop_reason":"end_turn","usage":{"input_tokens":10,"output_tokens":3}}"#;
        match event("turn.finished", data) {
            TurnEvent::Finished {
                stop_reason,
                usage,
                refusal_text,
            } => {
                assert_eq!(stop_reason, "end_turn");
                assert_eq!(usage.input_tokens, 10);
                assert_eq!(usage.output_tokens, 3);
                assert_eq!(usage.cache_read_input_tokens, 0);
                assert_eq!(refusal_text, None);
            }
            other => panic!("expected Finished, got {other:?}"),
        }
    }

    #[test]
    fn finished_without_usage_defaults_to_zero() {
        match event("turn.finished", r#"{"stop_reason":"end_turn"}"#) {
            TurnEvent::Finished { usage, .. } => assert_eq!(usage, Usage::default()),
            other => panic!("expected Finished, got {other:?}"),
        }
    }

    #[test]
    fn parses_refusal_text() {
        let data = r#"{"stop_reason":"refusal","refusal_text":"policy"}"#;
        match event("turn.finished", data) {
            TurnEvent::Finished { refusal_text, .. } => {
                assert_eq!(refusal_text.as_deref(), Some("policy"));
            }
            other => panic!("expected Finished, got {other:?}"),
        }
    }

    #[test]
    fn parses_the_composing_window() {
        // The pair the typing indicator is drawn between. `tool_call.composing` carries no input
        // because none has streamed; `tool_call.executing` closes it on the same id.
        assert_eq!(
            event(
                "tool_call.composing",
                r#"{"id":"tu_1","name":"mcp__mekabridge__send_message"}"#,
            ),
            TurnEvent::ToolCallComposing {
                id: "tu_1".to_string(),
                name: "mcp__mekabridge__send_message".to_string()
            }
        );
    }

    #[test]
    fn parses_a_compaction() {
        let data = r#"{"source":"summarizer","replaced_count":42,"generation":3}"#;
        assert_eq!(
            event("context.compacted", data),
            TurnEvent::ContextCompacted {
                source: "summarizer".to_string(),
                replaced_count: 42,
                generation: 3
            }
        );
    }

    /// Every shape meka's `turn.started` takes, flattened into the payload beside `turn_id`. The
    /// inbox one is the only one this bridge acts on: its `item_ids` are what tie a turn back to
    /// the items that woke it.
    #[test]
    fn a_turn_says_who_started_it() {
        let inbox = r#"{"turn_id":"t1","session_id":"s","started_at":"2026-09-16T00:00:00Z",
                        "source":"inbox","item_ids":["i1","i2"]}"#;
        let parsed = parse("turn.started", inbox)
            .expect("parses")
            .expect("event");
        assert_eq!(parsed.turn_id.as_deref(), Some("t1"));
        assert_eq!(parsed.event, TurnEvent::Started {
            started_at: "2026-09-16T00:00:00Z".to_string(),
            source: TurnSource::Inbox {
                item_ids: vec!["i1".to_string(), "i2".to_string()]
            },
            resumed: false,
        });
        assert!(matches!(
            event("turn.started", r#"{"turn_id":"t2","source":"client"}"#),
            TurnEvent::Started {
                source: TurnSource::Client,
                ..
            }
        ));
        assert!(matches!(
            event(
                "turn.started",
                r#"{"turn_id":"t3","source":"schedule","job_id":"j9"}"#
            ),
            TurnEvent::Started {
                source: TurnSource::Schedule { job_id },
                ..
            } if job_id == "j9"
        ));
        assert!(matches!(
            event("turn.started", r#"{"turn_id":"t4","source":"background"}"#),
            TurnEvent::Started {
                source: TurnSource::Background,
                ..
            }
        ));
        // A source this build has not heard of is kept by name rather than misfiled.
        assert!(matches!(
            event("turn.started", r#"{"turn_id":"t5","source":"webhook"}"#),
            TurnEvent::Started {
                source: TurnSource::Unknown(name),
                ..
            } if name == "webhook"
        ));
    }

    /// What a feed attaching mid-turn opens with: the id of the turn in flight and `resumed`, with
    /// no source and no time, because it is synthesised rather than replayed.
    #[test]
    fn a_resumed_turn_is_told_apart_from_one_beginning() {
        let parsed = parse(
            "turn.started",
            r#"{"turn_id":"t1","session_id":"s","resumed":true}"#,
        )
        .expect("parses")
        .expect("event");
        assert_eq!(parsed.turn_id.as_deref(), Some("t1"));
        assert_eq!(parsed.event, TurnEvent::Started {
            started_at: String::new(),
            source: TurnSource::Unknown(String::new()),
            resumed: true,
        });
    }

    /// The three events that say what became of an item. `inbox.delivered` is the only one that
    /// names several items at once, since a turn reads everything waiting in one request.
    #[test]
    fn inbox_outcomes_are_parsed() {
        let delivered = parse(
            "inbox.delivered",
            r#"{"item_ids":["i1","i2"],"turn_id":"t1","session_id":"s"}"#,
        )
        .expect("parses")
        .expect("event");
        assert_eq!(delivered.turn_id.as_deref(), Some("t1"));
        assert_eq!(delivered.event, TurnEvent::InboxDelivered {
            item_ids: vec!["i1".to_string(), "i2".to_string()]
        });
        assert_eq!(
            event(
                "inbox.failed",
                r#"{"item_id":"i1","session_id":"s","reason":"no turn could deliver it in 7 attempt(s) over the last hour"}"#
            ),
            TurnEvent::InboxFailed {
                item_id: "i1".to_string(),
                reason: "no turn could deliver it in 7 attempt(s) over the last hour".to_string(),
            }
        );
        assert_eq!(
            event("inbox.withdrawn", r#"{"item_id":"i1","session_id":"s"}"#),
            TurnEvent::InboxWithdrawn {
                item_id: "i1".to_string()
            }
        );
    }

    /// Every payload on the feed carries the turn it belongs to, and a bridge holding one feed
    /// for a session that runs turns it never started has to file by it.
    #[test]
    fn the_turn_id_rides_every_event() {
        let parsed = parse(
            "assistant_text.delta",
            r#"{"turn_id":"t7","session_id":"s","text":"x"}"#,
        )
        .expect("parses")
        .expect("event");
        assert_eq!(parsed.turn_id.as_deref(), Some("t7"));
        // A frame naming no turn, such as the replay-hole notice, reads as none rather than as
        // an empty one that would file under a turn called "".
        let notice = parse(
            "notice",
            r#"{"level":"warn","text":"the replay does not reach"}"#,
        )
        .expect("parses")
        .expect("event");
        assert_eq!(notice.turn_id, None);
    }

    #[test]
    fn unknown_events_do_not_fail_the_stream() {
        assert_eq!(
            event("some.future.event", r#"{"a":1}"#),
            TurnEvent::Unknown {
                event: "some.future.event".to_string()
            }
        );
    }

    #[test]
    fn keep_alive_frames_are_ignored() {
        assert_eq!(parse("", "").expect("parses"), None);
        assert_eq!(parse("", "   ").expect("parses"), None);
    }

    #[test]
    fn terminal_events_are_recognised() {
        assert!(event("turn.finished", r#"{"stop_reason":"end_turn"}"#).is_terminal());
        assert!(event("turn.canceled", r#"{"reason":"client"}"#).is_terminal());
        assert!(event("turn.failed", r#"{"error":{}}"#).is_terminal());
        assert!(!event("assistant_text.delta", r#"{"text":"x"}"#).is_terminal());
        // The feed carries on past a terminal, and so must the reader: an inbox outcome is not
        // one, however final it is for the item.
        assert!(!event("inbox.failed", r#"{"item_id":"i","reason":"r"}"#).is_terminal());
    }

    /// The spelling meka 0.46 moved to, and the one it moved from. Reading only one of them is not
    /// a parse failure but a leak: an unmatched terminal is `Unknown`, which is not terminal, so
    /// the turn's tally and its typing window are never closed out.
    #[test]
    fn both_spellings_of_a_cancelled_turn_are_terminal() {
        for name in ["turn.canceled", "turn.cancelled"] {
            let parsed = event(name, r#"{"reason":"sse_lag"}"#);
            assert_eq!(
                parsed,
                TurnEvent::Cancelled {
                    reason: "sse_lag".to_string(),
                },
                "{name} must not fall through to Unknown"
            );
            assert!(parsed.is_terminal(), "{name} must end the turn");
        }
    }

    #[test]
    fn malformed_json_is_an_error() {
        parse("assistant_text.delta", "{not json").expect_err("must fail");
    }

    #[test]
    fn missing_fields_degrade_to_empty_rather_than_failing() {
        // meka is the only producer, but a partial frame should not take down a running bridge.
        assert_eq!(
            event("tool_call.executing", "{}"),
            TurnEvent::ToolCallStarted {
                id: String::new(),
                name: String::new(),
                display_summary: None,
            }
        );
    }
}
