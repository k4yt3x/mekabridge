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

use crate::render::{Block, Span, block_separator, into_messages};

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
        if let Some(previous) = index
            .checked_sub(1)
            .and_then(|previous| blocks.get(previous))
        {
            out.push_str(block_separator(previous, block));
        }
        match block {
            Block::Text { spans, quoted, .. } => {
                let mut body = String::new();
                render_spans(spans, &mut body);
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
                    render_spans(spans, &mut body);
                } else {
                    let bold: Vec<Span> = spans
                        .iter()
                        .map(|span| {
                            let mut span = span.clone();
                            span.style.bold = true;
                            span
                        })
                        .collect();
                    render_spans(&bold, &mut body);
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

/// One delimiter pair Discord reads around a run of text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mark<'a> {
    Link(&'a str),
    Strikethrough,
    Bold,
    Italic,
}

impl Mark<'_> {
    /// The marks a span carries, in the order they open when nothing else decides it.
    ///
    /// Less any style its own code would cut short. Discord's emphasis rules know nothing of code
    /// spans, so `**` inside code inside bold closes the bold there and prints the rest as
    /// asterisks. Such a span goes out unstyled instead, with the style closed around it and
    /// reopened after, which loses the emphasis on that one span rather than the markup of the
    /// whole run. A link's address cannot clash, since [`push_link_target`] encodes every
    /// delimiter in it.
    fn of(span: &Span) -> Vec<Mark<'_>> {
        let code = span.style.code.then_some(span.text.as_str());
        let clashes = |delimiter: &str| code.is_some_and(|code| code.contains(delimiter));
        let mut marks = Vec::new();
        if let Some(link) = &span.link {
            marks.push(Mark::Link(link));
        }
        if span.style.strikethrough && !clashes("~~") {
            marks.push(Mark::Strikethrough);
        }
        if span.style.bold && !clashes("**") {
            marks.push(Mark::Bold);
        }
        // Any `*`, not only a pair: italic's closing rule accepts `**` inside but stops at one.
        if span.style.italic && !clashes("*") {
            marks.push(Mark::Italic);
        }
        marks
    }

    /// `after_close` is whether the last thing written was a closing delimiter.
    fn open(self, after_close: bool, out: &mut String) {
        // An opening `*` straight after a closing one runs the two into one delimiter, so
        // `**a***b*` reads as nothing Discord can match and the asterisks are printed. A zero-width
        // space keeps them apart without showing. Two that both open are left alone: `***` is
        // bold and italic at once, and Discord reads it that way.
        if after_close && matches!(self, Mark::Bold | Mark::Italic) && out.ends_with('*') {
            out.push('\u{200b}');
        }
        match self {
            Mark::Link(_) => out.push('['),
            Mark::Strikethrough => out.push_str("~~"),
            Mark::Bold => out.push_str("**"),
            Mark::Italic => out.push('*'),
        }
    }

    fn close(self, out: &mut String) {
        match self {
            Mark::Link(link) => {
                out.push_str("](");
                push_link_target(link, out);
                out.push(')');
            }
            Mark::Strikethrough => out.push_str("~~"),
            Mark::Bold => out.push_str("**"),
            Mark::Italic => out.push('*'),
        }
    }
}

/// Emit a block's spans with each style opened once for as long as it runs.
///
/// Delimiting every span on its own closed a style and reopened it wherever a neighbour shared it,
/// so `**bold *italic* bold**` went out as `**bold *****italic***** bold**`, a run Discord cannot
/// match and prints as asterisks. Here a mark stays open while the spans around it carry it, and
/// the marks a span opens go longest-running first, so the one that ends soonest is innermost and
/// what closes always closes in order.
fn render_spans(spans: &[Span], out: &mut String) {
    let wanted: Vec<Vec<Mark<'_>>> = spans.iter().map(Mark::of).collect();
    let mut open: Vec<Mark<'_>> = Vec::new();
    for ((index, span), here) in spans.iter().enumerate().zip(&wanted) {
        // Closed from the first open mark this span does not carry, since only the top of the
        // stack can close. A mark above it that this span does carry reopens below.
        let kept = open
            .iter()
            .position(|mark| !here.contains(mark))
            .unwrap_or(open.len());
        let mut after_close = false;
        if open.len() > kept {
            let trailing = take_trailing_space(out);
            while open.len() > kept {
                if let Some(mark) = open.pop() {
                    mark.close(out);
                    after_close = true;
                }
            }
            if !trailing.is_empty() {
                out.push_str(&trailing);
                after_close = false;
            }
        }
        let ahead = wanted.get(index..).unwrap_or_default();
        let mut opening: Vec<Mark<'_>> = here
            .iter()
            .copied()
            .filter(|mark| !open.contains(mark))
            .collect();
        // Stable, so marks running equally far keep the order `Mark::of` gives them.
        opening.sort_by_key(|mark| {
            std::cmp::Reverse(
                ahead
                    .iter()
                    .take_while(|marks| marks.contains(mark))
                    .count(),
            )
        });
        // A span of nothing but space opens nothing: anything opened around it would close again
        // with nothing inside once the space moved out, and `****` prints as it is. The next span
        // opens what it needs.
        if !span.style.code && span.text.trim_start_matches(EDGE_SPACE).is_empty() {
            opening.clear();
        }
        // Space at the start of the span goes before what opens rather than inside it, for the
        // same reason as at a close; see `take_trailing_space`.
        let mut body = span.text.as_str();
        if !opening.is_empty() && !span.style.code {
            let trimmed = body.trim_start_matches(EDGE_SPACE);
            let leading = body.get(..body.len() - trimmed.len()).unwrap_or_default();
            if !leading.is_empty() {
                escape(leading, out);
                after_close = false;
            }
            body = trimmed;
        }
        for mark in opening {
            mark.open(after_close, out);
            after_close = false;
            open.push(mark);
        }
        if span.style.code {
            push_code(body, out);
        } else {
            escape(body, out);
        }
    }
    let trailing = take_trailing_space(out);
    while let Some(mark) = open.pop() {
        mark.close(out);
    }
    out.push_str(&trailing);
}

/// The space characters moved out from beside a delimiter. Newlines among them, since a message
/// is preferably split at one and a delimiter left against it fails the same way.
const EDGE_SPACE: [char; 3] = [' ', '\t', '\n'];

/// Remove and return the spaces at the end of `out`, so a closing delimiter goes before them.
///
/// Discord's italic and strikethrough will not close right after a space, nor open right before
/// one, so `*see *` and `~~old ~~` print their delimiters. Bold tolerates it, but nothing loses by
/// moving the space outside, and a style that closes beside a code span or at the end of a split
/// message would otherwise leave exactly that.
fn take_trailing_space(out: &mut String) -> String {
    let kept = out.trim_end_matches(EDGE_SPACE).len();
    out.split_off(kept)
}

/// Nothing inside a code span is markup, so its text is passed through unescaped. A backtick in
/// the body is fenced with a longer run, which is Discord's own escape for this.
fn push_code(text: &str, out: &mut String) {
    // Two code spans with nothing between them would run their fences together into one.
    if out.ends_with('`') {
        out.push('\u{200b}');
    }
    let fence = if text.contains("``") {
        "```"
    } else if text.contains('`') {
        "``"
    } else {
        "`"
    };
    out.push_str(fence);
    if fence.len() > 1 {
        out.push(' ');
    }
    out.push_str(text);
    if fence.len() > 1 {
        out.push(' ');
    }
    out.push_str(fence);
}

/// A link's address, with every character that could end it early or close a style around it
/// percent-encoded.
///
/// Only those: encoding the whole address would corrupt one that is already encoded, and Discord
/// accepts spaces inside it. `(` and `)` because Discord's link rule matches parentheses and stops
/// at an unpaired one, and `*` and `~` because its emphasis rules do not know where a link's
/// address begins, so a `*` there would close an italic around the link.
fn push_link_target(link: &str, out: &mut String) {
    for character in link.chars() {
        match character {
            '(' => out.push_str("%28"),
            ')' => out.push_str("%29"),
            '*' => out.push_str("%2A"),
            '~' => out.push_str("%7E"),
            other => out.push(other),
        }
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

    /// Each span used to open and close its own delimiters, so a style shared with a neighbour was
    /// closed and reopened at the seam: `**bold *****italic***** bold**`, which Discord cannot
    /// match and prints as asterisks. Reported from the field for the first two.
    #[test]
    fn a_style_stays_open_across_what_is_nested_inside_it() {
        assert_eq!(render("**bold *italic* bold**"), "**bold *italic* bold**");
        assert_eq!(render("**bold `code` bold**"), "**bold `code` bold**");
        assert_eq!(
            render("*italic **bold** italic*"),
            "*italic **bold** italic*"
        );
        assert_eq!(
            render("~~strike **bold** strike~~"),
            "~~strike **bold** strike~~"
        );
        assert_eq!(render("***both***"), "***both***");
    }

    #[test]
    fn code_holding_the_styles_own_delimiter_is_set_outside_it() {
        // Discord reads emphasis without knowing about code, so the `**` inside closed the bold
        // early and printed the rest as asterisks. The code loses its bold; the run keeps its
        // markup.
        assert_eq!(
            render("**Run `ls src/**/*.rs` first**"),
            "**Run** `ls src/**/*.rs` **first**"
        );
        assert_eq!(render("*see `a*b` here*"), "*see* `a*b` *here*");
        assert_eq!(render("~~old `x~~y` way~~"), "~~old~~ `x~~y` ~~way~~");
        // One `*` cannot close bold, so bold keeps it.
        assert_eq!(render("**see `a*b` here**"), "**see `a*b` here**");
    }

    #[test]
    fn a_link_address_can_neither_end_the_link_nor_a_style_early() {
        assert_eq!(
            render("*see [it](https://e.com/?q=*) now*"),
            "*see [it](https://e.com/?q=%2A) now*"
        );
        assert_eq!(
            render("**[glob](https://e.com/**/x)**"),
            "[**glob**](https://e.com/%2A%2A/x)"
        );
        // An unpaired parenthesis in the address used to swallow what came after the link.
        assert_eq!(
            render("[0](https://e.com/x_(y)) `fn()`"),
            "[0](https://e.com/x_%28y%29) `fn()`"
        );
    }

    #[test]
    fn a_span_of_only_space_opens_nothing() {
        // The style closes around each code span, and opening it around the space between them
        // left `****`, which Discord prints.
        assert_eq!(render("**x `**` `**` y**"), "**x** `**` `**` **y**");
        assert_eq!(render("*see `a*b` `c*d` now*"), "*see* `a*b` `c*d` *now*");
        assert_eq!(render("~~`~~a` `~~b`~~"), "`~~a` `~~b`");
    }

    #[test]
    fn code_spans_left_touching_keep_their_own_fences() {
        assert_eq!(render("`a`*`*`*`b`"), "`a`\u{200b}`*`\u{200b}`b`");
    }

    #[test]
    fn a_delimiter_never_sits_against_a_space_it_cannot_close_or_open_beside() {
        // Discord's italic and strikethrough refuse `*see *` and `* now*`, printing the asterisks.
        // A long italic run split across messages left exactly that at every cut.
        for source in [
            format!("*{}end*", "word ".repeat(600)),
            format!("*{}end*", "line of words\n".repeat(200)),
        ] {
            for chunk in to_markdown(&source, MESSAGE_LIMIT) {
                assert!(
                    !chunk.ends_with(" *")
                        && !chunk.ends_with("\n*")
                        && !chunk.starts_with("* ")
                        && !chunk.starts_with("*\n"),
                    "a delimiter against a space at {:?} .. {:?}",
                    chunk.chars().take(12).collect::<String>(),
                    chunk.chars().rev().take(12).collect::<String>()
                );
            }
        }
    }

    #[test]
    fn a_code_span_keeps_the_style_around_it() {
        // Code used to drop every other style, so bold around it vanished without a trace.
        assert_eq!(render("**`code`**"), "**`code`**");
    }

    #[test]
    fn a_styled_word_inside_a_link_leaves_it_one_link() {
        assert_eq!(
            render("[**a** b](https://example.com)"),
            "[**a** b](https://example.com)"
        );
    }

    #[test]
    fn a_style_closing_where_another_opens_is_kept_apart() {
        // `*a***b**` is a run of three Discord cannot split back into a close and an open.
        assert_eq!(render("_a_**b**"), "*a*\u{200b}**b**");
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
        assert_eq!(render("`message_send(a_b)`"), "`message_send(a_b)`");
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
