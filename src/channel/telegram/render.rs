//! Markdown to Telegram HTML, with length-safe splitting.
//!
//! Telegram accepts a small HTML subset (`b`, `i`, `u`, `s`, `code`, `pre`, `a`, `blockquote`,
//! `tg-spoiler`) and nothing else: no headings, lists, tables, or images. Agent output uses all of
//! those, so this module maps them onto what does exist rather than emitting markup Telegram will
//! reject with a 400.
//!
//! Splitting is the reason the conversion goes through a structured intermediate instead of
//! building an HTML string and slicing it. Slicing rendered HTML can cut a tag in half or leave one
//! unclosed, and Telegram rejects the whole message when that happens. Here the markdown is parsed
//! into spans that carry their own styling, splitting happens on span boundaries, and tags are
//! emitted afterwards, so every chunk is well formed by construction.
//!
//! The 4096 limit counts characters after entity parsing, so lengths are measured on visible text
//! and markup overhead is not charged against the budget.

use pulldown_cmark::{CodeBlockKind, Event, Options, Parser, Tag, TagEnd};

/// Telegram's per-message character limit, counted after entity parsing.
pub const MESSAGE_LIMIT: usize = 4096;

/// Caption limit for photos and documents, which is much smaller than the message limit.
pub const CAPTION_LIMIT: usize = 1024;

/// Inline styling flags that survive into Telegram's HTML subset.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Style {
    bold: bool,
    italic: bool,
    strikethrough: bool,
    code: bool,
}

/// A run of text sharing one style and at most one link.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Span {
    text: String,
    style: Style,
    link: Option<String>,
}

impl Span {
    fn plain(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            style: Style::default(),
            link: None,
        }
    }

    fn visible_length(&self) -> usize {
        self.text.chars().count()
    }
}

/// A block-level unit. Blocks are never split across messages unless they are individually too
/// large, which keeps a code block or a list item intact wherever possible.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Block {
    Text {
        spans: Vec<Span>,
        quoted: bool,
        /// Set for list items, which read better separated by one newline than by a blank line.
        tight: bool,
    },
    Pre {
        language: Option<String>,
        text: String,
    },
}

impl Block {
    fn visible_length(&self) -> usize {
        match self {
            Self::Text { spans, .. } => spans.iter().map(Span::visible_length).sum(),
            Self::Pre { text, .. } => text.chars().count(),
        }
    }

    const fn is_tight(&self) -> bool {
        matches!(self, Self::Text { tight: true, .. })
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::Text { spans, .. } => spans.iter().all(|span| span.text.trim().is_empty()),
            Self::Pre { text, .. } => text.trim().is_empty(),
        }
    }
}

/// Render Markdown into one or more Telegram HTML messages, each within `limit` visible characters.
///
/// Returns an empty vector for input that renders to nothing, so callers do not send blank
/// messages.
pub fn to_html(markdown: &str, limit: usize) -> Vec<String> {
    let limit = limit.max(1);
    let blocks = parse_blocks(markdown);
    group_blocks(blocks, limit)
        .into_iter()
        .map(|group| render_group(&group))
        .filter(|rendered| !rendered.trim().is_empty())
        .collect()
}

/// Split Markdown into plain-text messages, leaving the source formatting untouched.
///
/// The escape hatch behind `parse_mode = "none"`, for when HTML rendering misbehaves against a
/// particular message and sending something readable matters more than sending it styled.
pub fn to_plain(markdown: &str, limit: usize) -> Vec<String> {
    let limit = limit.max(1);
    let trimmed = markdown.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }
    let mut chunks = Vec::new();
    let mut rest = trimmed;
    while rest.chars().count() > limit {
        let (head, tail) = split_at_visible(rest, limit);
        if head.trim().is_empty() {
            break;
        }
        chunks.push(head.trim_end().to_string());
        rest = tail.trim_start_matches('\n');
    }
    if !rest.trim().is_empty() {
        chunks.push(rest.to_string());
    }
    chunks
}

fn parse_blocks(markdown: &str) -> Vec<Block> {
    let mut options = Options::empty();
    options.insert(Options::ENABLE_TABLES);
    options.insert(Options::ENABLE_STRIKETHROUGH);
    options.insert(Options::ENABLE_TASKLISTS);

    let mut state = ParseState::default();
    for event in Parser::new_ext(markdown, options) {
        state.handle(event);
    }
    state.finish()
}

#[derive(Default)]
struct ParseState {
    blocks: Vec<Block>,
    spans: Vec<Span>,
    style: Style,
    link: Option<String>,
    quote_depth: usize,
    /// One entry per open list; `Some(n)` carries the next number for an ordered list.
    lists: Vec<Option<u64>>,
    code_block: Option<(Option<String>, String)>,
    table: Option<TableState>,
}

#[derive(Default)]
struct TableState {
    rows: Vec<Vec<String>>,
    current_row: Vec<String>,
    current_cell: String,
    in_cell: bool,
}

impl ParseState {
    fn handle(&mut self, event: Event<'_>) {
        match event {
            Event::Start(tag) => self.start(tag),
            Event::End(tag) => self.end(tag),
            Event::Text(text) => self.push_text(&text),
            Event::Code(text) => {
                let mut style = self.style;
                style.code = true;
                self.push_span(Span {
                    text: text.to_string(),
                    style,
                    link: self.link.clone(),
                });
            }
            // Raw HTML in agent output is far more often an accident than an intent, and passing it
            // through would either be rejected by Telegram or, worse, injected. It is shown as
            // text.
            Event::Html(text) | Event::InlineHtml(text) => self.push_text(&text),
            Event::SoftBreak | Event::HardBreak => self.push_text("\n"),
            Event::Rule => {
                self.flush(false);
                self.blocks.push(Block::Text {
                    spans: vec![Span::plain("──────────")],
                    quoted: false,
                    tight: false,
                });
            }
            Event::TaskListMarker(checked) => {
                self.push_text(if checked { "\u{2611} " } else { "\u{2610} " });
            }
            Event::FootnoteReference(_) | Event::InlineMath(_) | Event::DisplayMath(_) => {}
        }
    }

    fn start(&mut self, tag: Tag<'_>) {
        match tag {
            Tag::Paragraph => {}
            Tag::Heading { .. } => {
                self.flush(false);
                // Telegram has no headings; bold is the closest thing that survives.
                self.style.bold = true;
            }
            Tag::BlockQuote(_) => {
                self.flush(false);
                self.quote_depth += 1;
            }
            Tag::CodeBlock(kind) => {
                self.flush(false);
                let language = match kind {
                    CodeBlockKind::Fenced(info) => {
                        let language = info.split_whitespace().next().unwrap_or_default();
                        (!language.is_empty()).then(|| language.to_string())
                    }
                    CodeBlockKind::Indented => None,
                };
                self.code_block = Some((language, String::new()));
            }
            Tag::List(start) => {
                self.flush(false);
                self.lists.push(start);
            }
            Tag::Item => {
                self.flush(false);
                let depth = self.lists.len().saturating_sub(1);
                let indent = "  ".repeat(depth);
                let marker = match self.lists.last_mut() {
                    Some(Some(number)) => {
                        let current = *number;
                        *number = number.saturating_add(1);
                        format!("{current}. ")
                    }
                    _ => "\u{2022} ".to_string(),
                };
                self.spans.push(Span::plain(format!("{indent}{marker}")));
            }
            Tag::Emphasis => self.style.italic = true,
            Tag::Strong => self.style.bold = true,
            Tag::Strikethrough => self.style.strikethrough = true,
            Tag::Link { dest_url, .. } => self.link = Some(dest_url.to_string()),
            Tag::Image { dest_url, .. } => {
                // Telegram cannot inline a remote image from HTML, so it becomes a link and the alt
                // text that follows becomes the label.
                self.link = Some(dest_url.to_string());
            }
            Tag::Table(_) => {
                self.flush(false);
                self.table = Some(TableState::default());
            }
            Tag::TableHead | Tag::TableRow => {
                if let Some(table) = &mut self.table {
                    table.current_row.clear();
                }
            }
            Tag::TableCell => {
                if let Some(table) = &mut self.table {
                    table.current_cell.clear();
                    table.in_cell = true;
                }
            }
            Tag::HtmlBlock | Tag::FootnoteDefinition(_) | Tag::MetadataBlock(_) => {}
            Tag::DefinitionList
            | Tag::DefinitionListTitle
            | Tag::DefinitionListDefinition
            | Tag::Superscript
            | Tag::Subscript => {}
        }
    }

    fn end(&mut self, tag: TagEnd) {
        match tag {
            TagEnd::Paragraph => self.flush(false),
            TagEnd::Heading(_) => {
                self.flush(false);
                self.style.bold = false;
            }
            TagEnd::BlockQuote(_) => {
                self.flush(false);
                self.quote_depth = self.quote_depth.saturating_sub(1);
            }
            TagEnd::CodeBlock => {
                if let Some((language, text)) = self.code_block.take() {
                    self.blocks.push(Block::Pre {
                        language,
                        text: text.trim_end_matches('\n').to_string(),
                    });
                }
            }
            TagEnd::List(_) => {
                self.flush(true);
                self.lists.pop();
            }
            // Tight lists emit item text without a wrapping paragraph, so the item end has to flush
            // too. The flush is a no-op when a paragraph already did it.
            TagEnd::Item => self.flush(true),
            TagEnd::Emphasis => self.style.italic = false,
            TagEnd::Strong => self.style.bold = false,
            TagEnd::Strikethrough => self.style.strikethrough = false,
            TagEnd::Link | TagEnd::Image => self.link = None,
            TagEnd::Table => {
                if let Some(table) = self.table.take() {
                    self.push_table(table);
                }
            }
            TagEnd::TableHead | TagEnd::TableRow => {
                if let Some(table) = &mut self.table {
                    let row = std::mem::take(&mut table.current_row);
                    if !row.is_empty() {
                        table.rows.push(row);
                    }
                }
            }
            TagEnd::TableCell => {
                if let Some(table) = &mut self.table {
                    let cell = std::mem::take(&mut table.current_cell);
                    table.current_row.push(cell.trim().to_string());
                    table.in_cell = false;
                }
            }
            TagEnd::HtmlBlock | TagEnd::FootnoteDefinition | TagEnd::MetadataBlock(_) => {}
            TagEnd::DefinitionList
            | TagEnd::DefinitionListTitle
            | TagEnd::DefinitionListDefinition
            | TagEnd::Superscript
            | TagEnd::Subscript => {}
        }
    }

    fn push_text(&mut self, text: &str) {
        if let Some((_, buffer)) = &mut self.code_block {
            buffer.push_str(text);
            return;
        }
        if let Some(table) = &mut self.table
            && table.in_cell
        {
            table.current_cell.push_str(text);
            return;
        }
        self.push_span(Span {
            text: text.to_string(),
            style: self.style,
            link: self.link.clone(),
        });
    }

    fn push_span(&mut self, span: Span) {
        if span.text.is_empty() {
            return;
        }
        // Merging adjacent runs keeps the emitted HTML from alternating `</b><b>` across every
        // word, which both bloats the payload and reads badly in Telegram's own editor.
        if let Some(last) = self.spans.last_mut()
            && last.style == span.style
            && last.link == span.link
        {
            last.text.push_str(&span.text);
            return;
        }
        self.spans.push(span);
    }

    /// Emit whatever spans have accumulated as one block.
    fn flush(&mut self, tight: bool) {
        if self.spans.is_empty() {
            return;
        }
        let spans = std::mem::take(&mut self.spans);
        let block = Block::Text {
            spans,
            quoted: self.quote_depth > 0,
            tight: tight || !self.lists.is_empty(),
        };
        if !block.is_empty() {
            self.blocks.push(block);
        }
    }

    /// Telegram has no table markup, so a table becomes a preformatted block where column alignment
    /// at least survives in a monospace font.
    fn push_table(&mut self, table: TableState) {
        if table.rows.is_empty() {
            return;
        }
        let column_count = table.rows.iter().map(Vec::len).max().unwrap_or(0);
        let mut widths = vec![0_usize; column_count];
        for row in &table.rows {
            for (index, cell) in row.iter().enumerate() {
                if let Some(width) = widths.get_mut(index) {
                    *width = (*width).max(cell.chars().count());
                }
            }
        }
        let mut rendered = String::new();
        for row in &table.rows {
            let mut line = String::new();
            for index in 0..column_count {
                let cell = row.get(index).map(String::as_str).unwrap_or_default();
                let width = widths.get(index).copied().unwrap_or(0);
                if index > 0 {
                    line.push_str(" | ");
                }
                line.push_str(cell);
                for _ in cell.chars().count()..width {
                    line.push(' ');
                }
            }
            rendered.push_str(line.trim_end());
            rendered.push('\n');
        }
        self.blocks.push(Block::Pre {
            language: None,
            text: rendered.trim_end().to_string(),
        });
    }

    fn finish(mut self) -> Vec<Block> {
        if let Some((language, text)) = self.code_block.take() {
            self.blocks.push(Block::Pre {
                language,
                text: text.trim_end_matches('\n').to_string(),
            });
        }
        self.flush(false);
        self.blocks
    }
}

/// Pack blocks into groups that each fit within `limit` visible characters.
fn group_blocks(blocks: Vec<Block>, limit: usize) -> Vec<Vec<Block>> {
    let mut groups: Vec<Vec<Block>> = Vec::new();
    let mut current: Vec<Block> = Vec::new();
    let mut current_length = 0_usize;

    for block in blocks {
        for piece in split_block(block, limit) {
            let piece_length = piece.visible_length();
            let separator = if current.is_empty() {
                0
            } else if current.last().is_some_and(Block::is_tight) || piece.is_tight() {
                1
            } else {
                2
            };
            if !current.is_empty() && current_length + separator + piece_length > limit {
                groups.push(std::mem::take(&mut current));
                current_length = 0;
            }
            let separator = if current.is_empty() { 0 } else { separator };
            current_length += separator + piece_length;
            current.push(piece);
        }
    }
    if !current.is_empty() {
        groups.push(current);
    }
    groups
}

/// Break a single oversized block into pieces that each fit.
fn split_block(block: Block, limit: usize) -> Vec<Block> {
    if block.visible_length() <= limit {
        return vec![block];
    }
    match block {
        Block::Pre { language, text } => split_pre(&language, &text, limit),
        Block::Text {
            spans,
            quoted,
            tight,
        } => split_spans(spans, limit)
            .into_iter()
            .map(|spans| Block::Text {
                spans,
                quoted,
                tight,
            })
            .collect(),
    }
}

/// Split a code block on line boundaries so each piece stays independently readable.
fn split_pre(language: &Option<String>, text: &str, limit: usize) -> Vec<Block> {
    let mut pieces = Vec::new();
    let mut current = String::new();
    for line in text.split_inclusive('\n') {
        if !current.is_empty() && current.chars().count() + line.chars().count() > limit {
            pieces.push(Block::Pre {
                language: language.clone(),
                text: current.trim_end_matches('\n').to_string(),
            });
            current = String::new();
        }
        if line.chars().count() > limit {
            // A single line longer than a whole message has to be cut mid-line.
            let mut rest = line;
            while rest.chars().count() > limit {
                let (head, tail) = split_at_visible(rest, limit);
                pieces.push(Block::Pre {
                    language: language.clone(),
                    text: head.to_string(),
                });
                rest = tail;
            }
            current.push_str(rest);
        } else {
            current.push_str(line);
        }
    }
    if !current.trim().is_empty() {
        pieces.push(Block::Pre {
            language: language.clone(),
            text: current.trim_end_matches('\n').to_string(),
        });
    }
    pieces
}

/// Split a run of spans, preferring span boundaries and falling back to cutting inside one.
fn split_spans(spans: Vec<Span>, limit: usize) -> Vec<Vec<Span>> {
    let mut groups: Vec<Vec<Span>> = Vec::new();
    let mut current: Vec<Span> = Vec::new();
    let mut current_length = 0_usize;

    for span in spans {
        let mut remaining = span;
        loop {
            let available = limit.saturating_sub(current_length);
            if remaining.visible_length() <= available {
                if remaining.visible_length() > 0 {
                    current_length += remaining.visible_length();
                    current.push(remaining);
                }
                break;
            }
            if available == 0 {
                groups.push(std::mem::take(&mut current));
                current_length = 0;
                continue;
            }
            let (head, tail) = split_at_visible(&remaining.text, available);
            if head.trim().is_empty() {
                // No usable break within the budget; start a new message rather than emitting a
                // fragment made only of whitespace.
                groups.push(std::mem::take(&mut current));
                current_length = 0;
                continue;
            }
            current.push(Span {
                text: head.to_string(),
                style: remaining.style,
                link: remaining.link.clone(),
            });
            groups.push(std::mem::take(&mut current));
            current_length = 0;
            remaining = Span {
                text: tail.trim_start_matches(['\n', ' ']).to_string(),
                style: remaining.style,
                link: remaining.link.clone(),
            };
            if remaining.text.is_empty() {
                break;
            }
        }
    }
    if !current.is_empty() {
        groups.push(current);
    }
    groups
}

/// Cut `text` at no more than `limit` characters, preferring a newline and then a space so words
/// stay whole. Always cuts on a character boundary.
fn split_at_visible(text: &str, limit: usize) -> (&str, &str) {
    let mut boundary = text.len();
    for (count, (index, _)) in text.char_indices().enumerate() {
        if count == limit {
            boundary = index;
            break;
        }
    }
    if boundary >= text.len() {
        return (text, "");
    }
    let head = text.get(..boundary).unwrap_or("");
    // Only accept a break point in the last quarter of the budget; otherwise a paragraph with one
    // early newline would produce a nearly empty message.
    let minimum = boundary.saturating_sub(boundary / 4);
    let cut = head
        .rfind('\n')
        .filter(|index| *index >= minimum)
        .or_else(|| head.rfind(' ').filter(|index| *index >= minimum))
        .map_or(boundary, |index| index + 1);
    (text.get(..cut).unwrap_or(""), text.get(cut..).unwrap_or(""))
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
                if *quoted {
                    out.push_str("<blockquote>");
                }
                for span in spans {
                    render_span(span, &mut out);
                }
                if *quoted {
                    out.push_str("</blockquote>");
                }
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

fn render_span(span: &Span, out: &mut String) {
    if let Some(link) = &span.link {
        out.push_str("<a href=\"");
        escape_attribute(link, out);
        out.push_str("\">");
    }
    // A fixed open order means the matching close order is fixed too, so nesting can never
    // interleave incorrectly.
    if span.style.bold {
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
    if span.style.bold {
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
            assert!(std::str::from_utf8(chunk.as_bytes()).is_ok());
        }
    }

    #[test]
    fn emoji_are_not_split_apart() {
        let markdown = "🎉🎊".repeat(200);
        let chunks = to_html(&markdown, 50);
        for chunk in &chunks {
            assert!(std::str::from_utf8(chunk.as_bytes()).is_ok());
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
    fn plain_mode_leaves_markdown_untouched() {
        let chunks = to_plain("**bold** and `code`", MESSAGE_LIMIT);
        assert_eq!(chunks, vec!["**bold** and `code`"]);
    }

    #[test]
    fn plain_mode_still_respects_the_limit() {
        let chunks = to_plain(&"word ".repeat(3000), 100);
        assert!(chunks.len() > 1);
        for chunk in &chunks {
            assert!(chunk.chars().count() <= 100);
        }
    }

    #[test]
    fn plain_mode_drops_empty_input() {
        assert!(to_plain("  \n ", MESSAGE_LIMIT).is_empty());
    }

    #[test]
    fn a_limit_of_zero_does_not_hang_or_panic() {
        let chunks = to_html("some text here", 0);
        assert!(chunks.iter().all(|chunk| !chunk.is_empty()));
    }

    /// Visible character count, ignoring markup, which is what Telegram's limit applies to.
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
                    count += 1;
                }
                _ if !in_tag => count += 1,
                _ => {}
            }
        }
        count
    }
}
