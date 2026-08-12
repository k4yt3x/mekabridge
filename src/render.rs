//! Markdown to a platform-neutral intermediate, with length-safe splitting.
//!
//! Every chat platform accepts some small subset of the formatting agent output uses, and none of
//! them accept all of it. Rather than each connector parsing Markdown itself, the parse happens
//! once here into [`Block`]s and [`Span`]s that carry their own styling, and each connector
//! supplies only the emitter that turns a group of blocks into whatever its platform speaks.
//!
//! Splitting is the reason the conversion goes through a structured intermediate instead of
//! building a string and slicing it. Slicing rendered markup can cut a tag in half or leave one
//! unclosed, and a platform rejects the whole message when that happens. Here splitting is done on
//! span boundaries and markup is emitted afterwards, so every chunk is well formed by construction.
//!
//! Lengths are counted in visible characters, because that is what platforms charge against their
//! limits: markup overhead is not part of the budget.

use pulldown_cmark::{CodeBlockKind, Event, Options, Parser, Tag, TagEnd};

/// Inline styling that survives into every platform's markup.
///
/// Deliberately the intersection rather than the union. Anything a single platform supports and the
/// others do not belongs in that platform's emitter, not here, where every other emitter would have
/// to decide what to do about it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Style {
    pub bold: bool,
    pub italic: bool,
    pub strikethrough: bool,
    pub code: bool,
}

/// A run of text sharing one style and at most one link.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Span {
    pub text: String,
    pub style: Style,
    pub link: Option<String>,
}

impl Span {
    fn plain(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            style: Style::default(),
            link: None,
        }
    }

    pub fn visible_length(&self) -> usize {
        self.text.chars().count()
    }
}

/// A block-level unit. Blocks are never split across messages unless they are individually too
/// large, which keeps a code block or a list item intact wherever possible.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Block {
    Text {
        spans: Vec<Span>,
        quoted: bool,
        /// Set for list items, which read better separated by one newline than by a blank line.
        tight: bool,
    },
    /// A heading and its level, 1 through 6.
    ///
    /// Kept distinct from bold text even though several platforms can only render it as bold,
    /// because Discord has real headings and flattening here would take that away from every
    /// emitter at once.
    Heading {
        level: u8,
        spans: Vec<Span>,
        /// Carried for the same reason `Text` carries it: before headings were their own variant a
        /// quoted heading was a quoted `Text`, and dropping it here would let one escape the
        /// blockquote it was written inside.
        quoted: bool,
    },
    Pre {
        language: Option<String>,
        text: String,
    },
}

impl Block {
    pub fn visible_length(&self) -> usize {
        match self {
            Self::Text { spans, .. } | Self::Heading { spans, .. } => {
                spans.iter().map(Span::visible_length).sum()
            }
            Self::Pre { text, .. } => text.chars().count(),
        }
    }

    pub const fn is_tight(&self) -> bool {
        matches!(self, Self::Text { tight: true, .. })
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Self::Text { spans, .. } | Self::Heading { spans, .. } => {
                spans.iter().all(|span| span.text.trim().is_empty())
            }
            Self::Pre { text, .. } => text.trim().is_empty(),
        }
    }
}

/// Parse Markdown and emit one or more messages, each within `limit` characters of output.
///
/// The budget is measured on what `emit` produced, not on the text underneath it. That distinction
/// is the whole reason this is not a one-line pipeline: Telegram counts characters after its markup
/// is parsed away, so markup is free, while Discord counts the markup itself, and there escaping
/// `snake_case` or wrapping a code fence is charged to the same allowance as the words. Budgeting
/// on visible length alone overshoots on Discord by however much the emitter added, and a message
/// over the limit is refused outright rather than trimmed.
///
/// So `measure` belongs to the emitter, which is the only thing that knows how its platform counts,
/// and blocks are grouped, emitted, measured, and regrouped against a tighter budget until they
/// fit. Emitters are pure and cheap, and the retry only touches the groups that need it.
///
/// Returns an empty vector for input that renders to nothing, so callers do not send blank
/// messages.
pub fn into_messages(
    markdown: &str,
    limit: usize,
    emit: impl Fn(&[Block]) -> String,
    measure: impl Fn(&str) -> usize,
) -> Vec<String> {
    let limit = limit.max(1);
    let blocks = parse_blocks(markdown);
    let mut budget = limit;
    loop {
        let rendered: Vec<String> = group_blocks(blocks.clone(), budget)
            .iter()
            .map(|group| emit(group))
            .collect();
        let worst = rendered.iter().map(|body| measure(body)).max().unwrap_or(0);
        // At a budget of one visible character per message there is nothing left to give up.
        if worst <= limit || budget <= 1 {
            return rendered
                .into_iter()
                .filter(|body| !body.trim().is_empty())
                .collect();
        }
        // Shrink the budget for the whole document rather than for the group that overflowed.
        // Regrouping one group in isolation also fits, but it packs the overflow into a full
        // message and a stray remainder, so a long reply arrives as alternating walls and single
        // words. Grouping the whole thing at one budget keeps the parts even.
        budget = budget
            .saturating_mul(limit)
            .checked_div(worst)
            .unwrap_or(1)
            .clamp(1, budget - 1);
    }
}

/// Split Markdown into plain-text messages, leaving the source formatting untouched.
///
/// The escape hatch for when markup rendering misbehaves against a particular message and sending
/// something readable matters more than sending it styled.
pub fn plain(markdown: &str, limit: usize) -> Vec<String> {
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
    /// Level of the heading currently being collected, if any.
    heading: Option<u8>,
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
            // through would either be rejected by the platform or, worse, injected. It is shown as
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
            Tag::Heading { level, .. } => {
                self.flush(false);
                self.heading = Some(level as u8);
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
                // No platform here can inline a remote image from markup, so it becomes a link and
                // the alt text that follows becomes the label.
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
                self.heading = None;
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
        // Merging adjacent runs keeps the emitted markup from alternating open and close tags
        // across every word, which both bloats the payload and reads badly in a platform's
        // own editor.
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
        let block = match self.heading {
            Some(level) => Block::Heading {
                level,
                spans,
                // Carried for the same reason `Text` carries it: before headings were their own
                // variant a quoted one was a quoted `Text`, and dropping it here would let a
                // heading escape the blockquote it was written inside.
                quoted: self.quote_depth > 0,
            },
            None => Block::Text {
                spans,
                quoted: self.quote_depth > 0,
                tight: tight || !self.lists.is_empty(),
            },
        };
        if !block.is_empty() {
            self.blocks.push(block);
        }
    }

    /// No platform here has table markup, so a table becomes a preformatted block where column
    /// alignment at least survives in a monospace font.
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
        // Only the first piece keeps the heading, since a platform with real headings would
        // otherwise repeat the marker on every continuation as though each were a new section.
        Block::Heading {
            level,
            spans,
            quoted,
        } => split_spans(spans, limit)
            .into_iter()
            .enumerate()
            .map(|(index, spans)| {
                if index == 0 {
                    Block::Heading {
                        level,
                        spans,
                        quoted,
                    }
                } else {
                    Block::Text {
                        spans,
                        quoted,
                        tight: false,
                    }
                }
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
pub fn split_at_visible(text: &str, limit: usize) -> (&str, &str) {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn count_chars(text: &str) -> usize {
        text.chars().count()
    }

    /// Emitter that keeps only what the intermediate carries, so these tests exercise the parse and
    /// the splitting rather than any one platform's markup.
    fn debug_emit(blocks: &[Block]) -> String {
        let mut out = String::new();
        for (index, block) in blocks.iter().enumerate() {
            if index > 0 {
                out.push('\n');
            }
            match block {
                Block::Text { spans, quoted, .. } => {
                    if *quoted {
                        out.push_str("> ");
                    }
                    for span in spans {
                        out.push_str(&span.text);
                    }
                }
                Block::Heading { level, spans, .. } => {
                    for _ in 0..*level {
                        out.push('#');
                    }
                    out.push(' ');
                    for span in spans {
                        out.push_str(&span.text);
                    }
                }
                Block::Pre { text, .. } => out.push_str(text),
            }
        }
        out
    }

    #[test]
    fn a_heading_survives_the_parse_as_a_heading() {
        let blocks = parse_blocks("# Title\n\nbody");
        assert_eq!(
            blocks.first(),
            Some(&Block::Heading {
                level: 1,
                spans: vec![Span::plain("Title")],
                quoted: false,
            })
        );
    }

    #[test]
    fn emphasis_inside_a_heading_no_longer_leaks_past_it() {
        // The heading level used to be applied by turning bold on for the duration, so the closing
        // `**` switched it back off and the rest of the heading came out unstyled.
        let blocks = parse_blocks("# plain **bold** plain");
        let Some(Block::Heading { spans, .. }) = blocks.first() else {
            panic!("expected a heading, got {blocks:?}");
        };
        let styled: Vec<(&str, bool)> = spans
            .iter()
            .map(|span| (span.text.as_str(), span.style.bold))
            .collect();
        assert_eq!(styled, vec![
            ("plain ", false),
            ("bold", true),
            (" plain", false)
        ]);
    }

    #[test]
    fn heading_levels_are_reported_verbatim() {
        let blocks = parse_blocks("###### six");
        assert!(matches!(
            blocks.first(),
            Some(Block::Heading { level: 6, .. })
        ));
    }

    #[test]
    fn an_oversized_heading_keeps_the_marker_only_on_its_first_piece() {
        let long = "word ".repeat(200);
        let pieces = split_block(
            Block::Heading {
                level: 2,
                spans: vec![Span::plain(long)],
                quoted: false,
            },
            100,
        );
        assert!(pieces.len() > 1);
        assert!(matches!(
            pieces.first(),
            Some(Block::Heading { level: 2, .. })
        ));
        assert!(
            pieces[1..]
                .iter()
                .all(|piece| matches!(piece, Block::Text { .. }))
        );
    }

    #[test]
    fn a_table_becomes_a_preformatted_block() {
        let blocks = parse_blocks("| a | b |\n|---|---|\n| 1 | 2 |");
        assert!(matches!(blocks.first(), Some(Block::Pre { .. })));
    }

    #[test]
    fn every_rendered_chunk_stays_within_the_limit() {
        let chunks = into_messages(&"word ".repeat(3000), 100, debug_emit, count_chars);
        assert!(chunks.len() > 1);
        for chunk in &chunks {
            assert!(chunk.chars().count() <= 100, "chunk too long: {chunk:?}");
        }
    }

    #[test]
    fn input_that_renders_to_nothing_produces_no_messages() {
        assert!(into_messages("   \n\n  ", 4096, debug_emit, count_chars).is_empty());
    }

    #[test]
    fn a_zero_limit_does_not_loop_forever() {
        let chunks = into_messages("hello world", 0, debug_emit, count_chars);
        assert!(!chunks.is_empty());
    }

    #[test]
    fn plain_leaves_the_markdown_alone() {
        let chunks = plain("**bold** and `code`", 4096);
        assert_eq!(chunks, vec!["**bold** and `code`".to_string()]);
    }

    #[test]
    fn plain_splits_on_the_limit() {
        let chunks = plain(&"word ".repeat(3000), 100);
        assert!(chunks.len() > 1);
        for chunk in &chunks {
            assert!(chunk.chars().count() <= 100, "chunk too long: {chunk:?}");
        }
    }

    #[test]
    fn plain_drops_whitespace_only_input() {
        assert!(plain("  \n ", 4096).is_empty());
    }
}
