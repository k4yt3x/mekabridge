//! The platform-neutral intermediate from [`crate::render`], emitted as Discord Markdown.
//!
//! Discord speaks Markdown rather than a markup subset, which sounds like it should make this a
//! passthrough and does not. Discord's dialect has no tables, no images, and no horizontal rules,
//! its headings stop at three levels, and, most importantly, text that was never meant as markup is
//! still read as markup: `snake_case_names` come out italicised and `2 * 3 * 4` turns into bold.
//! So agent output is parsed, and re-emitted with everything that is not deliberate formatting
//! escaped.
//!
//! The limit is 2000 characters against Telegram's 4096, so replies split about twice as often.

use crate::render::{Block, Span, into_messages};

/// Discord's per-message character limit.
pub const MESSAGE_LIMIT: usize = 2000;

/// Render Markdown into one or more Discord messages, each within `limit` characters.
///
/// Returns an empty vector for input that renders to nothing, so callers do not send blank
/// messages.
pub fn to_markdown(markdown: &str, limit: usize) -> Vec<String> {
    // Every character counts here, escapes and fences included: Discord's limit applies to what is
    // sent, not to what it renders as.
    into_messages(markdown, limit, render_group, |text| text.chars().count())
}

fn render_group(blocks: &[Block]) -> String {
    let mut out = String::new();
    for (index, block) in blocks.iter().enumerate() {
        if index > 0 {
            let previous_tight = blocks.get(index - 1).is_some_and(Block::is_tight);
            out.push_str(if previous_tight || block.is_tight() {
                "\n"
            } else {
                "\n\n"
            });
        }
        match block {
            Block::Text { spans, quoted, .. } => {
                let mut body = String::new();
                for span in spans {
                    render_span(span, &mut body);
                }
                push_quoted(&body, *quoted, &mut out);
            }
            Block::Heading {
                level,
                spans,
                quoted,
            } => {
                let mut body = String::new();
                // Discord stops at three levels. Deeper ones become bold, which is what they would
                // have to degrade to anyway and is still visibly a heading.
                if *level <= 3 {
                    for _ in 0..*level {
                        body.push('#');
                    }
                    body.push(' ');
                    for span in spans {
                        render_span(span, &mut body);
                    }
                } else {
                    body.push_str("**");
                    for span in spans {
                        let mut span = span.clone();
                        span.style.bold = false;
                        render_span(&span, &mut body);
                    }
                    body.push_str("**");
                }
                push_quoted(&body, *quoted, &mut out);
            }
            Block::Pre { language, text } => {
                out.push_str("```");
                if let Some(language) = language {
                    out.push_str(&sanitize_language(language));
                }
                out.push('\n');
                // A fence inside the body would close the block early. Breaking the run with a
                // zero-width space keeps the text readable and the block intact, which beats
                // dropping the content or emitting markup Discord will mis-parse.
                out.push_str(&text.replace("```", "`\u{200b}``"));
                out.push_str("\n```");
            }
        }
    }
    out
}

/// Append `body`, quoting every line of it when it belongs inside a blockquote.
///
/// Discord's `>` applies to one line, so a wrapped quote needs the marker repeated rather than the
/// `>>>` block form, which would swallow everything after it in the same message.
fn push_quoted(body: &str, quoted: bool, out: &mut String) {
    if !quoted {
        out.push_str(body);
        return;
    }
    for (index, line) in body.split('\n').enumerate() {
        if index > 0 {
            out.push('\n');
        }
        out.push_str("> ");
        out.push_str(line);
    }
}

fn render_span(span: &Span, out: &mut String) {
    let opening = if span.link.is_some() { "[" } else { "" };
    out.push_str(opening);

    if span.style.code {
        // Nothing inside a code span is markup, so the other styles have nowhere to go and the text
        // is passed through unescaped. A backtick in the body is fenced with a longer run, which is
        // Discord's own escape for this.
        let fence = if span.text.contains("``") {
            "```"
        } else if span.text.contains('`') {
            "``"
        } else {
            "`"
        };
        out.push_str(fence);
        if fence.len() > 1 {
            out.push(' ');
        }
        out.push_str(&span.text);
        if fence.len() > 1 {
            out.push(' ');
        }
        out.push_str(fence);
    } else {
        // A fixed open order means the matching close order is fixed too, so nesting can never
        // interleave incorrectly.
        if span.style.strikethrough {
            out.push_str("~~");
        }
        if span.style.bold {
            out.push_str("**");
        }
        if span.style.italic {
            out.push('*');
        }
        escape(&span.text, out);
        if span.style.italic {
            out.push('*');
        }
        if span.style.bold {
            out.push_str("**");
        }
        if span.style.strikethrough {
            out.push_str("~~");
        }
    }

    if let Some(link) = &span.link {
        out.push_str("](");
        // Only the delimiters need handling: percent-encoding the whole URL would corrupt one that
        // is already encoded, and Discord accepts spaces inside angle brackets.
        out.push_str(&link.replace(')', "%29"));
        out.push(')');
    }
}

/// Escape every character Discord would otherwise read as markup.
///
/// Line-leading `#`, `>`, and `-` are escaped too, because a span can start a line even when it did
/// not start the block, and an unescaped one turns the rest of the line into a heading, a quote, or
/// a list item.
fn escape(text: &str, out: &mut String) {
    let mut at_line_start = out.is_empty() || out.ends_with('\n');
    for character in text.chars() {
        match character {
            '\n' => {
                out.push('\n');
                at_line_start = true;
                continue;
            }
            // `<` is deliberately absent. `<@id>` is the only way the agent can write a mention it
            // means, since it is never given a name-to-id path, and whether that ping actually
            // notifies is decided by the `allowed_mentions` on the send rather than here.
            '*' | '_' | '~' | '`' | '|' | '\\' | '[' | ']' => {
                out.push('\\');
                out.push(character);
            }
            '#' | '>' | '-' if at_line_start => {
                out.push('\\');
                out.push(character);
            }
            other => out.push(other),
        }
        at_line_start = false;
    }
}

/// Keep a code fence's language tag to something Discord will treat as one.
fn sanitize_language(language: &str) -> String {
    language
        .chars()
        .filter(|character| {
            character.is_ascii_alphanumeric() || *character == '+' || *character == '-'
        })
        .take(32)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn render(markdown: &str) -> String {
        to_markdown(markdown, MESSAGE_LIMIT).join("\n---\n")
    }

    #[test]
    fn plain_text_passes_through() {
        assert_eq!(render("hello world"), "hello world");
    }

    #[test]
    fn inline_styles_map_to_discords_dialect() {
        assert_eq!(render("**bold**"), "**bold**");
        assert_eq!(render("*italic*"), "*italic*");
        assert_eq!(render("~~gone~~"), "~~gone~~");
        assert_eq!(render("`code`"), "`code`");
    }

    #[test]
    fn headings_survive_as_headings() {
        assert_eq!(render("# Title"), "# Title");
        assert_eq!(render("### Smaller"), "### Smaller");
    }

    #[test]
    fn headings_past_the_third_level_become_bold() {
        // Discord has no h4, and bold is what it would have to degrade to anyway.
        assert_eq!(render("#### Deep"), "**Deep**");
    }

    #[test]
    fn a_snake_case_word_does_not_come_out_italicised() {
        // The whole reason this module escapes rather than passing Markdown through.
        assert_eq!(
            render("call send_message_now here"),
            "call send\\_message\\_now here"
        );
    }

    #[test]
    fn literal_asterisks_stay_literal() {
        assert_eq!(render(r"2 \* 3 \* 4"), "2 \\* 3 \\* 4");
    }

    #[test]
    fn a_code_span_is_not_escaped_inside() {
        assert_eq!(render("`send_message(a_b)`"), "`send_message(a_b)`");
    }

    #[test]
    fn a_backtick_inside_a_code_span_widens_the_fence() {
        assert_eq!(render("``a ` b``"), "`` a ` b ``");
    }

    #[test]
    fn a_fenced_block_keeps_its_language() {
        assert_eq!(
            render("```rust\nfn main() {}\n```"),
            "```rust\nfn main() {}\n```"
        );
    }

    #[test]
    fn a_fence_inside_a_code_block_cannot_close_it_early() {
        let rendered = render("````\nsee ``` here\n````");
        assert_eq!(rendered.matches("```\n").count(), 1);
        assert!(rendered.ends_with("\n```"));
    }

    #[test]
    fn a_quote_marks_every_line_it_wraps_onto() {
        let rendered = render("> first\n> second");
        for line in rendered.lines() {
            assert!(
                line.starts_with("> "),
                "unquoted line {line:?} in {rendered:?}"
            );
        }
    }

    #[test]
    fn an_explicit_mention_the_agent_wrote_survives() {
        // The only way the agent can deliberately ping somebody: it is never given a name-to-id
        // path, so escaping the `<` here would leave it no way at all. `allowed_mentions` on the
        // send is what decides whether the ping actually notifies.
        assert_eq!(
            render("ping <@245119312739729408> about it"),
            "ping <@245119312739729408> about it"
        );
        assert_eq!(
            render("see <#1183429847290374144>"),
            "see <#1183429847290374144>"
        );
    }

    #[test]
    fn a_heading_inside_a_quote_stays_inside_the_quote() {
        let rendered = render("> # Title\n>\n> body");
        for line in rendered.lines().filter(|line| !line.trim().is_empty()) {
            assert!(line.starts_with("> "), "escaped the quote: {rendered:?}");
        }
    }

    #[test]
    fn a_link_becomes_an_inline_link() {
        assert_eq!(
            render("[docs](https://example.com/a_b)"),
            "[docs](https://example.com/a_b)"
        );
    }

    #[test]
    fn a_table_degrades_to_a_code_block() {
        // Discord has no tables, and monospace at least keeps the columns lined up.
        let rendered = render("| a | b |\n|---|---|\n| 1 | 2 |");
        assert!(rendered.starts_with("```\n"), "got {rendered:?}");
        assert!(rendered.contains("a | b"), "got {rendered:?}");
    }

    #[test]
    fn a_list_becomes_bulleted_lines() {
        assert_eq!(render("- one\n- two"), "\u{2022} one\n\u{2022} two");
    }

    #[test]
    fn a_long_reply_splits_evenly_rather_than_into_walls_and_stragglers() {
        // Shrinking the budget for only the group that overflowed produced a full message followed
        // by a single word, over and over. Correct by the letter of the limit, unreadable in a
        // chat.
        let chunks = to_markdown(&"snake_case_identifier ".repeat(200), MESSAGE_LIMIT);
        let shortest = chunks.iter().map(|c| c.chars().count()).min().unwrap_or(0);
        let longest = chunks.iter().map(|c| c.chars().count()).max().unwrap_or(0);
        assert!(chunks.len() >= 2, "this should split at all");
        assert!(
            shortest * 4 >= longest,
            "parts are {:?} characters, which reads as a wall and then scraps",
            chunks.iter().map(|c| c.chars().count()).collect::<Vec<_>>()
        );
    }

    #[test]
    fn every_chunk_stays_within_the_limit() {
        // Discord counts the markup, not the text under it, so escapes and fences are charged to
        // the same 2000 characters. Anything over is rejected by the client library before it is
        // sent, which loses the whole reply rather than truncating it.
        for source in [
            "word ".repeat(3000),
            "snake_case_identifier ".repeat(200),
            format!("```\n{}\n```", "x".repeat(1995)),
            "[x] ".repeat(500),
            "*".repeat(2400),
            format!("> {}", "quoted words ".repeat(400)),
            format!("# {}", "heading ".repeat(400)),
        ] {
            for chunk in to_markdown(&source, MESSAGE_LIMIT) {
                assert!(
                    chunk.chars().count() <= MESSAGE_LIMIT,
                    "a chunk of {} characters would be refused; source began {:?}",
                    chunk.chars().count(),
                    &source[..40.min(source.len())]
                );
            }
        }
    }

    #[test]
    fn input_that_renders_to_nothing_produces_no_messages() {
        assert!(to_markdown("   \n\n  ", MESSAGE_LIMIT).is_empty());
    }
}
