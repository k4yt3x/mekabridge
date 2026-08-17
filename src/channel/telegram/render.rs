//! The platform-neutral intermediate from [`crate::render`], emitted as Telegram HTML.
//!
//! Telegram accepts a small HTML subset (`b`, `i`, `u`, `s`, `code`, `pre`, `a`, `blockquote`,
//! `tg-spoiler`) and nothing else: no headings, lists, tables, or images. Agent output uses all of
//! those, so this emitter maps them onto what does exist rather than emitting markup Telegram will
//! reject with a 400.
//!
//! The 4096 limit counts characters after entity parsing, so lengths are measured on visible text
//! and markup overhead is not charged against the budget.

use crate::render::{Block, Span, block_separator, into_messages};

/// Telegram's per-message character limit, counted after entity parsing.
pub const MESSAGE_LIMIT: usize = 4096;

/// Caption limit for photos and documents, which is much smaller than the message limit.
pub const CAPTION_LIMIT: usize = 1024;

/// Render Markdown into one or more Telegram HTML messages, each within `limit` visible characters.
///
/// Returns an empty vector for input that renders to nothing, so callers do not send blank
/// messages.
pub fn to_html(markdown: &str, limit: usize) -> Vec<String> {
    into_messages(markdown, limit, render_group, visible_length)
}

/// Visible character count, ignoring markup, which is what Telegram's limit applies to.
///
/// Telegram counts a message after entity parsing, so tags cost nothing against the 4096 and an
/// escaped `&amp;` counts as the one character it renders as. Charging the markup would split
/// replies far shorter than Telegram would actually accept.
fn visible_length(html: &str) -> usize {
    let mut count = 0;
    let mut in_tag = false;
    let mut chars = html.chars().peekable();
    while let Some(character) = chars.next() {
        match character {
            '<' => in_tag = true,
            '>' => in_tag = false,
            '&' if !in_tag => {
                // Consume the entity so `&amp;` counts as the single character it renders as.
                for entity in chars.by_ref() {
                    if entity == ';' {
                        break;
                    }
                }
                // An entity renders as one BMP character, so one unit.
                count += 1;
            }
            // Telegram measures a message the way it measures entity offsets, in UTF-16 code
            // units, so anything outside the BMP costs two. Counted as characters, an emoji-heavy
            // reply was split at 4096 characters and rejected at 8192 units -- and because the
            // splitter aims at the limit exactly, it failed on the *first* part, so nothing arrived
            // at all.
            _ if !in_tag => count += character.len_utf16(),
            _ => {}
        }
    }
    count
}

fn render_group(blocks: &[Block]) -> String {
    let mut out = String::new();
    for (index, block) in blocks.iter().enumerate() {
        if let Some(previous) = index
            .checked_sub(1)
            .and_then(|previous| blocks.get(previous))
        {
            out.push_str(block_separator(previous, block));
        }
        match block {
            Block::Text { spans, quoted, .. } => {
                if *quoted {
                    out.push_str("<blockquote>");
                }
                for span in spans {
                    render_span(span, false, &mut out);
                }
                if *quoted {
                    out.push_str("</blockquote>");
                }
            }
            // Telegram has no headings, and bold is the closest thing that survives. The wrapper
            // already covers the whole line, so bold inside it is suppressed rather than emitted as
            // a nested `<b>` that renders identically and only adds markup.
            Block::Heading { spans, .. } => {
                out.push_str("<b>");
                for span in spans {
                    render_span(span, true, &mut out);
                }
                out.push_str("</b>");
            }
            Block::Pre { language, text } => {
                match language {
                    Some(language) => {
                        out.push_str("<pre><code class=\"language-");
                        escape_attribute(language, &mut out);
                        out.push_str("\">");
                    }
                    None => out.push_str("<pre>"),
                }
                escape_text(text, &mut out);
                out.push_str(if language.is_some() {
                    "</code></pre>"
                } else {
                    "</pre>"
                });
            }
        }
    }
    out
}

/// Emit one span. `bold_already_open` suppresses this span's own bold, for a caller that has
/// wrapped the whole block in `<b>` already.
fn render_span(span: &Span, bold_already_open: bool, out: &mut String) {
    let bold = span.style.bold && !bold_already_open;
    if let Some(link) = &span.link {
        out.push_str("<a href=\"");
        escape_attribute(link, out);
        out.push_str("\">");
    }
    // A fixed open order means the matching close order is fixed too, so nesting can never
    // interleave incorrectly.
    if bold {
        out.push_str("<b>");
    }
    if span.style.italic {
        out.push_str("<i>");
    }
    if span.style.strikethrough {
        out.push_str("<s>");
    }
    if span.style.code {
        out.push_str("<code>");
    }
    escape_text(&span.text, out);
    if span.style.code {
        out.push_str("</code>");
    }
    if span.style.strikethrough {
        out.push_str("</s>");
    }
    if span.style.italic {
        out.push_str("</i>");
    }
    if bold {
        out.push_str("</b>");
    }
    if span.link.is_some() {
        out.push_str("</a>");
    }
}

fn escape_text(text: &str, out: &mut String) {
    for character in text.chars() {
        match character {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            other => out.push(other),
        }
    }
}

fn escape_attribute(text: &str, out: &mut String) {
    for character in text.chars() {
        match character {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            other => out.push(other),
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn the_budget_is_counted_the_way_telegram_counts_it() {
        // Telegram measures a message in UTF-16 code units, the same unit its entity offsets use,
        // so anything outside the BMP costs two. Counted as characters an emoji-heavy reply was cut
        // at 4096 characters and refused at 8192 units, and because the splitter aims at the limit
        // exactly it failed on the *first* part, so nothing arrived at all.
        let emoji = "\u{1f600}";
        assert_eq!(emoji.chars().count(), 1, "one character");
        assert_eq!(visible_length(emoji), 2, "but two of what Telegram counts");
        // Markup is still free, and an entity still costs the one character it renders as.
        assert_eq!(visible_length("<b>hi</b>"), 2);
        assert_eq!(visible_length("&amp;"), 1);

        let chunks = to_html(&emoji.repeat(40), 20);
        for chunk in &chunks {
            let units: usize = chunk.chars().map(char::len_utf16).sum();
            assert!(
                units <= 20,
                "a chunk of {units} UTF-16 units was emitted against a limit of 20: {chunk:?}"
            );
        }
    }

    use super::*;

    fn render(markdown: &str) -> String {
        to_html(markdown, MESSAGE_LIMIT).join("\n---\n")
    }

    /// Every opening tag has a matching close, in the right order.
    fn assert_well_formed(html: &str) {
        let mut stack: Vec<String> = Vec::new();
        let mut rest = html;
        while let Some(start) = rest.find('<') {
            let after = rest.get(start + 1..).unwrap_or("");
            let Some(end) = after.find('>') else {
                panic!("unterminated tag in {html:?}");
            };
            let raw = after.get(..end).unwrap_or("");
            if let Some(name) = raw.strip_prefix('/') {
                let popped = stack.pop();
                assert_eq!(
                    popped.as_deref(),
                    Some(name),
                    "mismatched close </{name}> in {html:?}"
                );
            } else {
                let name = raw.split_whitespace().next().unwrap_or("").to_string();
                stack.push(name);
            }
            rest = after.get(end + 1..).unwrap_or("");
        }
        assert!(stack.is_empty(), "unclosed tags {stack:?} in {html:?}");
    }

    #[test]
    fn a_list_keeps_its_paragraphs_apart() {
        // A list packs tightly against itself, not against whatever sits either side of it. Testing
        // one side rather than both ran the intro and the conclusion into the bullets, and a reply
        // built from all three arrived as a single wall of text.
        assert_eq!(
            render("Intro.\n\n- one\n- two\n\nOutro."),
            "Intro.\n\n\u{2022} one\n\u{2022} two\n\nOutro."
        );
        assert_eq!(render("Before.\n\n- one"), "Before.\n\n\u{2022} one");
        assert_eq!(render("- one\n\nAfter."), "\u{2022} one\n\nAfter.");
    }

    #[test]
    fn ordinary_paragraphs_keep_their_blank_line() {
        assert_eq!(render("A.\n\nB.\n\nC."), "A.\n\nB.\n\nC.");
        // Any run of blank lines is one paragraph break in Markdown, so this is not a bug to fix.
        assert_eq!(render("A.\n\n\n\nB."), "A.\n\nB.");
        // A single newline is a soft break inside one paragraph, which is what it looks like.
        assert_eq!(render("A.\nB."), "A.\nB.");
    }

    #[test]
    fn a_loose_list_keeps_the_spacing_it_was_written_with() {
        // CommonMark reports looseness only by wrapping each item in a paragraph, and the parser
        // used to discard that, so a list written with blank lines between its items arrived packed
        // as tight as one written without.
        assert_eq!(render("- one\n- two"), "\u{2022} one\n\u{2022} two");
        assert_eq!(render("- one\n\n- two"), "\u{2022} one\n\n\u{2022} two");
        assert_eq!(render("1. one\n\n2. two"), "1. one\n\n2. two");
    }

    #[test]
    fn a_second_paragraph_in_an_item_is_not_mistaken_for_the_next_item() {
        // The worse half of the same bug: a continuation paragraph was joined to its bullet by a
        // single newline, so it read as a malformed item rather than as more of the one above it.
        assert_eq!(
            render("- first para\n\n  second para\n- next item"),
            "\u{2022} first para\n\nsecond para\n\n\u{2022} next item"
        );
    }

    #[test]
    fn plain_text_passes_through() {
        assert_eq!(render("hello world"), "hello world");
    }

    #[test]
    fn inline_styles_map_to_the_telegram_subset() {
        assert_eq!(render("**bold**"), "<b>bold</b>");
        assert_eq!(render("*italic*"), "<i>italic</i>");
        assert_eq!(render("~~gone~~"), "<s>gone</s>");
        assert_eq!(render("`code`"), "<code>code</code>");
    }

    #[test]
    fn nested_styles_nest_in_a_fixed_order() {
        let html = render("***both***");
        assert_well_formed(&html);
        assert_eq!(html, "<b><i>both</i></b>");
    }

    #[test]
    fn html_metacharacters_are_escaped() {
        // Unescaped, this would be a Telegram 400 at best and tag injection at worst.
        assert_eq!(render("5 < 6 & 7 > 2"), "5 &lt; 6 &amp; 7 &gt; 2");
    }

    #[test]
    fn raw_html_in_agent_output_is_shown_as_text() {
        let html = render("<script>alert(1)</script>");
        assert!(!html.contains("<script>"), "got: {html}");
        assert!(html.contains("&lt;script&gt;"), "got: {html}");
    }

    #[test]
    fn links_render_with_escaped_attributes() {
        let html = render("[docs](https://example.com/a?b=1&c=2)");
        assert_eq!(
            html,
            "<a href=\"https://example.com/a?b=1&amp;c=2\">docs</a>"
        );
        assert_well_formed(&html);
    }

    #[test]
    fn headings_become_bold_because_telegram_has_none() {
        assert_eq!(render("## Section"), "<b>Section</b>");
    }

    #[test]
    fn unordered_lists_become_bulleted_lines() {
        let html = render("- one\n- two");
        assert_eq!(html, "\u{2022} one\n\u{2022} two");
    }

    #[test]
    fn ordered_lists_keep_their_numbering() {
        let html = render("1. first\n2. second");
        assert_eq!(html, "1. first\n2. second");
    }

    #[test]
    fn ordered_lists_respect_a_custom_start() {
        let html = render("5. five\n6. six");
        assert!(html.starts_with("5. five"), "got: {html}");
        assert!(html.contains("6. six"), "got: {html}");
    }

    #[test]
    fn nested_lists_are_indented() {
        let html = render("- outer\n  - inner");
        assert!(html.contains("\u{2022} outer"), "got: {html}");
        assert!(html.contains("  \u{2022} inner"), "got: {html}");
    }

    #[test]
    fn fenced_code_blocks_keep_their_language() {
        let html = render("```rust\nfn main() {}\n```");
        assert_eq!(
            html,
            "<pre><code class=\"language-rust\">fn main() {}</code></pre>"
        );
        assert_well_formed(&html);
    }

    #[test]
    fn code_blocks_without_a_language_use_bare_pre() {
        let html = render("```\nraw\n```");
        assert_eq!(html, "<pre>raw</pre>");
    }

    #[test]
    fn code_block_contents_are_escaped() {
        let html = render("```\nif a < b && c > d {}\n```");
        assert!(html.contains("&lt; b &amp;&amp; c &gt;"), "got: {html}");
    }

    #[test]
    fn block_quotes_use_the_blockquote_tag() {
        let html = render("> quoted");
        assert_eq!(html, "<blockquote>quoted</blockquote>");
        assert_well_formed(&html);
    }

    #[test]
    fn tables_degrade_to_aligned_preformatted_text() {
        let html = render("| a | bb |\n|---|----|\n| 1 | 2 |");
        assert!(html.starts_with("<pre>"), "got: {html}");
        assert!(html.contains("a | bb"), "got: {html}");
        assert!(html.contains("1 | 2"), "got: {html}");
        assert_well_formed(&html);
    }

    #[test]
    fn images_become_links() {
        let html = render("![alt text](https://example.com/x.png)");
        assert!(
            html.contains("href=\"https://example.com/x.png\""),
            "got: {html}"
        );
        assert!(html.contains("alt text"), "got: {html}");
        assert_well_formed(&html);
    }

    #[test]
    fn task_lists_render_check_boxes() {
        let html = render("- [x] done\n- [ ] todo");
        assert!(html.contains('\u{2611}'), "got: {html}");
        assert!(html.contains('\u{2610}'), "got: {html}");
    }

    #[test]
    fn empty_input_produces_no_messages() {
        assert!(to_html("", MESSAGE_LIMIT).is_empty());
        assert!(to_html("   \n  ", MESSAGE_LIMIT).is_empty());
    }

    #[test]
    fn long_text_splits_into_several_messages_each_within_the_limit() {
        let markdown = "word ".repeat(3000);
        let chunks = to_html(&markdown, MESSAGE_LIMIT);
        assert!(chunks.len() > 1, "expected a split, got {}", chunks.len());
        for chunk in &chunks {
            assert_well_formed(chunk);
            assert!(
                visible_length(chunk) <= MESSAGE_LIMIT,
                "chunk of {} visible chars exceeds the limit",
                visible_length(chunk)
            );
        }
    }

    #[test]
    fn splitting_never_breaks_a_tag_in_half() {
        // Styling that spans the split point is the case that produces malformed HTML if the
        // rendered string is sliced instead of the structure.
        // The trailing space matters: CommonMark will not close emphasis after whitespace, so
        // `**bold **` is literal text rather than a styled run.
        let markdown = format!("**{}**", "bold ".repeat(2000).trim_end());
        let chunks = to_html(&markdown, 100);
        assert!(chunks.len() > 1);
        for chunk in &chunks {
            assert_well_formed(chunk);
            assert!(chunk.starts_with("<b>"), "styling must reopen: {chunk}");
            assert!(chunk.ends_with("</b>"), "styling must close: {chunk}");
        }
    }

    #[test]
    fn oversized_code_blocks_split_into_several_pre_blocks() {
        let body = (0..500)
            .map(|index| format!("line {index}"))
            .collect::<Vec<_>>()
            .join("\n");
        let chunks = to_html(&format!("```\n{body}\n```"), 200);
        assert!(chunks.len() > 1);
        for chunk in &chunks {
            assert_well_formed(chunk);
            assert!(chunk.starts_with("<pre>"), "got: {chunk}");
            assert!(chunk.ends_with("</pre>"), "got: {chunk}");
            assert!(visible_length(chunk) <= 200);
        }
    }

    #[test]
    fn a_single_line_longer_than_the_limit_is_cut_mid_line() {
        let chunks = to_html(&format!("```\n{}\n```", "x".repeat(500)), 100);
        assert!(chunks.len() >= 5);
        for chunk in &chunks {
            assert_well_formed(chunk);
            assert!(visible_length(chunk) <= 100);
        }
    }

    #[test]
    fn splitting_prefers_word_boundaries() {
        let markdown = "alpha bravo charlie delta echo foxtrot golf hotel india juliet";
        let chunks = to_html(markdown, 20);
        assert!(chunks.len() > 1);
        for chunk in &chunks {
            // A word cut in half would leave a chunk that neither starts nor ends on whitespace in
            // the source; checking the reassembly is the durable version of that assertion.
            assert!(!chunk.is_empty());
        }
        let rejoined = chunks.join(" ");
        for word in markdown.split_whitespace() {
            assert!(rejoined.contains(word), "lost {word:?} in {chunks:?}");
        }
    }

    #[test]
    fn multibyte_text_splits_on_character_boundaries() {
        // Slicing by byte index here would panic or produce invalid UTF-8.
        let markdown = "日本語のテキスト".repeat(500);
        let chunks = to_html(&markdown, 100);
        assert!(chunks.len() > 1);
        for chunk in &chunks {
            assert!(visible_length(chunk) <= 100);
        }
    }

    #[test]
    fn emoji_are_not_split_apart() {
        let markdown = "🎉🎊".repeat(200);
        let chunks = to_html(&markdown, 50);
        // Guarded: `for` over an empty vector asserts nothing, and the length check below is the
        // only thing here that a wrong measure would trip.
        assert!(
            !chunks.is_empty(),
            "the emoji vanished instead of splitting"
        );
        for chunk in &chunks {
            assert!(
                visible_length(chunk) <= 50,
                "a chunk ran past the limit: {chunk:?}"
            );
            assert!(!chunk.contains('\u{fffd}'), "replacement char in {chunk}");
        }
    }

    #[test]
    fn mixed_document_stays_well_formed_when_split() {
        let markdown = format!(
            "# Title\n\nSome **bold** and a [link](https://example.com).\n\n\
             ```python\n{}\n```\n\n- item one\n- item two\n\n> a quote\n",
            "print('hello')\n".repeat(200)
        );
        let chunks = to_html(&markdown, 500);
        assert!(chunks.len() > 1);
        for chunk in &chunks {
            assert_well_formed(chunk);
            assert!(visible_length(chunk) <= 500);
        }
    }

    #[test]
    fn a_limit_of_zero_does_not_hang_or_panic() {
        let chunks = to_html("some text here", 0);
        assert!(chunks.iter().all(|chunk| !chunk.is_empty()));
    }
}
