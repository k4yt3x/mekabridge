//! Turn events, and the mapping from meka's SSE wire format onto them.
//!
//! meka names each event (`assistant_text.delta`, `tool_call.executing`, `turn.finished`, ...) and
//! puts a JSON object in `data`. Unrecognised names become [`TurnEvent::Unknown`] rather than an
//! error, so a meka that grows a new event type does not break a running bridge.

use serde::Deserialize;

/// One event from a turn's SSE stream.
#[derive(Debug, Clone, PartialEq)]
pub enum TurnEvent {
    Started {
        turn_id: String,
        started_at: String,
    },
    AssistantText {
        text: String,
    },
    Thinking {
        text: String,
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
    /// Emitted when a gated tool needs approval. The bridge does not offer an approval UI, so this
    /// only ever means the session was configured at `ask`, where the turn will stall for 60
    /// seconds and then auto-deny.
    PermissionRequired {
        request_id: String,
        tool_name: String,
        expires_in_seconds: u64,
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
    /// Whether this event ends the stream. `turn.finished`, `turn.failed`, and `turn.cancelled` are
    /// terminal per meka's HTTP API.
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

/// Parse one SSE frame.
///
/// Returns `Ok(None)` for frames that carry no turn event, such as the keep-alive comments and the
/// initial `retry:` directive, which arrive with an empty event name and empty data.
pub fn parse(event_name: &str, data: &str) -> Result<Option<TurnEvent>, serde_json::Error> {
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

    let parsed = match event_name {
        "turn.started" => TurnEvent::Started {
            turn_id: string("turn_id"),
            started_at: string("started_at"),
        },
        "assistant_text.delta" => TurnEvent::AssistantText {
            text: string("text"),
        },
        "thinking.delta" => TurnEvent::Thinking {
            text: string("text"),
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
        "permission_required" => TurnEvent::PermissionRequired {
            request_id: string("request_id"),
            tool_name: string("tool_name"),
            expires_in_seconds: value
                .get("expires_in_seconds")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0),
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
        "turn.cancelled" => TurnEvent::Cancelled {
            reason: string("reason"),
        },
        other => TurnEvent::Unknown {
            event: other.to_string(),
        },
    };
    Ok(Some(parsed))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_assistant_text_delta() {
        let event = parse("assistant_text.delta", r#"{"text":"hello"}"#)
            .expect("parses")
            .expect("event");
        assert_eq!(event, TurnEvent::AssistantText {
            text: "hello".to_string()
        });
    }

    #[test]
    fn parses_finished_with_usage() {
        let data = r#"{"stop_reason":"end_turn","usage":{"input_tokens":10,"output_tokens":3}}"#;
        let event = parse("turn.finished", data)
            .expect("parses")
            .expect("event");
        match event {
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
        let event = parse("turn.finished", r#"{"stop_reason":"end_turn"}"#)
            .expect("parses")
            .expect("event");
        match event {
            TurnEvent::Finished { usage, .. } => assert_eq!(usage, Usage::default()),
            other => panic!("expected Finished, got {other:?}"),
        }
    }

    #[test]
    fn parses_refusal_text() {
        let data = r#"{"stop_reason":"refusal","refusal_text":"policy"}"#;
        let event = parse("turn.finished", data)
            .expect("parses")
            .expect("event");
        match event {
            TurnEvent::Finished { refusal_text, .. } => {
                assert_eq!(refusal_text.as_deref(), Some("policy"));
            }
            other => panic!("expected Finished, got {other:?}"),
        }
    }

    #[test]
    fn unknown_events_do_not_fail_the_stream() {
        let event = parse("some.future.event", r#"{"a":1}"#)
            .expect("parses")
            .expect("event");
        assert_eq!(event, TurnEvent::Unknown {
            event: "some.future.event".to_string()
        });
    }

    #[test]
    fn keep_alive_frames_are_ignored() {
        assert_eq!(parse("", "").expect("parses"), None);
        assert_eq!(parse("", "   ").expect("parses"), None);
    }

    #[test]
    fn terminal_events_are_recognised() {
        assert!(
            parse("turn.finished", r#"{"stop_reason":"end_turn"}"#)
                .expect("parses")
                .expect("event")
                .is_terminal()
        );
        assert!(
            parse("turn.cancelled", r#"{"reason":"client"}"#)
                .expect("parses")
                .expect("event")
                .is_terminal()
        );
        assert!(
            parse("turn.failed", r#"{"error":{}}"#)
                .expect("parses")
                .expect("event")
                .is_terminal()
        );
        assert!(
            !parse("assistant_text.delta", r#"{"text":"x"}"#)
                .expect("parses")
                .expect("event")
                .is_terminal()
        );
    }

    #[test]
    fn malformed_json_is_an_error() {
        parse("assistant_text.delta", "{not json").expect_err("must fail");
    }

    #[test]
    fn missing_fields_degrade_to_empty_rather_than_failing() {
        // meka is the only producer, but a partial frame should not take down a running bridge.
        let event = parse("tool_call.executing", "{}")
            .expect("parses")
            .expect("event");
        assert_eq!(event, TurnEvent::ToolCallStarted {
            id: String::new(),
            name: String::new(),
            display_summary: None,
        });
    }
}
