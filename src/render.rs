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
    // The rendering at the full budget, kept for the case where shrinking never succeeds.
    //
    // Some overflow does not shrink. A link target is copied into every piece the splitter cuts, so
    // the widest body stays the same size however small the budget gets, and the loop runs it down
    // to one visible character per message. That does nothing for the offending block and chops
    // every *other* block in the document into single characters: a page of prose next to one long
    // URL came out as a thousand one-letter messages, each of which the connector dutifully sends.
    //
    // When shrinking cannot win, the best thing to cut up is therefore the *least* cut rendering
    // there is. Its blocks are whole and only the irreducible body needs hard-splitting, so the
    // damage stays where the problem is.
    let mut whole: Option<Vec<String>> = None;
    loop {
        let rendered: Vec<String> = group_blocks(blocks.clone(), budget)
            .iter()
            .map(|group| emit(group))
            .collect();
        let worst = rendered.iter().map(|body| measure(body)).max().unwrap_or(0);
        let fits = worst <= limit;
        // At one visible character per message there is nothing left to give up.
        if fits || budget <= 1 {
            let shrunk: Vec<String> = rendered
                .into_iter()
                .filter(|body| !body.trim().is_empty())
                .collect();
            // Nothing was ever cut, so this is the whole document at the full budget and there is
            // nothing to compare it against.
            let Some(whole) = whole else {
                return shrunk;
            };
            let bodies: Vec<String> = whole
                .into_iter()
                .filter(|body| !body.trim().is_empty())
                .collect();
            // Out of budget and still over. Returning as-is meant handing the connector a body the
            // platform refuses outright -- twilight rejects it client-side, Telegram answers 400 --
            // and since the parts before it have already been sent, the reply arrives half
            // delivered. Cutting is worse markup than the splitter wanted and better than a message
            // nobody receives; it only happens for content that cannot be broken at all, such as a
            // single link longer than the whole limit.
            // Filtered again on the way out: `hard_split`'s trailing remainder is whatever is left
            // past the last cut, which can be nothing but whitespace, and a blank body is refused
            // by the platform and aborts the rest of the send.
            let cut: Vec<String> = bodies
                .into_iter()
                .flat_map(|body| hard_split(&body, limit, &measure))
                .filter(|body| !body.trim().is_empty())
                .collect();
            // Fitting is not the same as being worth sending, and this is the comparison that says
            // so. Some overflow does not shrink: a link target is copied into every piece the
            // splitter cuts, so the budget falls until the link is alone in its message and *then*
            // fits -- at one visible character each, which chops the rest of the document into
            // single letters. Thirty-six thousand characters of prose beside one 1995-character URL
            // came out as 28,817 messages, and the connector sends every one of them. Checking only
            // whether the widest body fits cannot see that, because it does fit.
            if fits && shrunk.len() <= cut.len() {
                return shrunk;
            }
            return cut;
        }
        whole = whole.or(Some(rendered));
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

/// Last resort: cut a body that no amount of regrouping brought under the limit.
///
/// Measured with the emitter's own `measure`, since that is the only thing that knows how the
/// platform counts, and stepped one character at a time because the relationship between characters
/// and whatever it counts is the emitter's business. Only ever reached for something indivisible.
fn hard_split(body: &str, limit: usize, measure: &impl Fn(&str) -> usize) -> Vec<String> {
    if measure(body) <= limit {
        return vec![body.to_string()];
    }
    let mut parts = Vec::new();
    let mut current = String::new();
    for character in body.chars() {
        let mut candidate = current.clone();
        candidate.push(character);
        if !current.is_empty() && measure(&candidate) > limit {
            parts.push(std::mem::take(&mut current));
            current.push(character);
        } else {
            current = candidate;
        }
    }
    if !current.is_empty() {
        parts.push(current);
    }
    parts
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
    lists: Vec<ListState>,
    code_block: Option<(Option<String>, String)>,
    table: Option<TableState>,
}

/// One open list, innermost last.
struct ListState {
    /// Next number for an ordered list, or `None` for a bulleted one.
    next_number: Option<u64>,
    /// Whether the list was written with blank lines between its items, which CommonMark reports
    /// only by wrapping each item's content in a paragraph.
    loose: bool,
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
            // A list whose items wrap their content in paragraphs is a loose one, which is the
            // only signal CommonMark gives and the only one pulldown-cmark passes on. Noted on the
            // innermost list so its items are spaced the way they were written.
            Tag::Paragraph => {
                if let Some(list) = self.lists.last_mut() {
                    list.loose = true;
                }
            }
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
                self.lists.push(ListState {
                    next_number: start,
                    loose: false,
                });
            }
            Tag::Item => {
                self.flush(false);
                let depth = self.lists.len().saturating_sub(1);
                let indent = "  ".repeat(depth);
                let marker = match self
                    .lists
                    .last_mut()
                    .and_then(|list| list.next_number.as_mut())
                {
                    Some(number) => {
                        let current = *number;
                        *number = number.saturating_add(1);
                        format!("{current}. ")
                    }
                    None => "\u{2022} ".to_string(),
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
                // Only a tight list packs its items together. A loose one was written with blank
                // lines between its items, and flattening that also merged the paragraphs of a
                // multi-paragraph item into its bullet, where a continuation was indistinguishable
                // from the next item.
                tight: tight || self.lists.last().is_some_and(|list| !list.loose),
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

/// What separates two adjacent blocks: a newline, or a blank line.
///
/// Only list items pack tightly, and only against each other. Testing either side rather than both
/// let a list's tightness leak onto its neighbours, so a paragraph before or after a list was run
/// into it and a reply built from an intro, a list and a conclusion arrived as one wall of text.
///
/// Shared because the emitters and [`group_blocks`] have to agree: the packer budgets for this
/// separator's length, and if it assumed one character where an emitter wrote two, a message could
/// be packed a character over the platform's limit and refused.
pub(crate) fn block_separator(previous: &Block, next: &Block) -> &'static str {
    if previous.is_tight() && next.is_tight() {
        "\n"
    } else {
        "\n\n"
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
            let separator = current
                .last()
                .map_or(0, |last| block_separator(last, &piece).len());
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
    // Never zero. At zero the budget can never grow, because flushing sets the length back to a
    // limit that is still zero, and the loop below has no other way to make room. Callers arrive
    // through `into_messages`, which already clamps, so this only holds the invariant locally.
    let limit = limit.max(1);
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
                if !current.is_empty() {
                    // No usable break within the budget; start a new message rather than emitting a
                    // fragment made only of whitespace.
                    groups.push(std::mem::take(&mut current));
                    current_length = 0;
                    continue;
                }
                // Nothing to flush, so flushing achieves nothing: the group is already empty and
                // the budget is already the whole limit, which made the next pass
                // identical to this one. That spun forever without consuming a
                // byte, allocating, or reaching an await, so the task could not
                // even be cancelled and the worker thread was gone for good.
                //
                // Dropping the leading whitespace is what makes progress here, and it is the right
                // rendering anyway, since no message wants to begin with it. `head` is non-empty
                // and entirely whitespace, so the text does start with some and
                // this always shortens it. The equality check is a backstop rather
                // than a live branch: it costs a comparison and it means a future
                // change to `split_at_visible` cannot bring the hang back.
                let trimmed = remaining.text.trim_start();
                if trimmed.len() == remaining.text.len() {
                    break;
                }
                remaining.text = trimmed.to_string();
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
    fn a_span_too_long_to_break_does_not_spin() {
        // Run on its own thread with a deadline, because the regression is a loop with no
        // allocation and no yield: called directly it would hang the suite rather than fail it. A
        // plain thread rather than `spawn_blocking`, because dropping a tokio runtime waits for its
        // blocking tasks, so the panic would land and then teardown would hang anyway. The test
        // harness exits the process when it finishes, so the spinning thread is abandoned.
        //
        // Driven straight at `split_spans` because the state that triggers it is precise: an empty
        // group, and a span whose first `available` characters are all whitespace. Reaching it
        // through `into_messages` needs the budget already shrunk to near nothing, which happens
        // when a span is cheap to measure but expensive to emit -- a Markdown link whose URL runs
        // past the platform limit is the real-world case, and it took a whole reply down with it.
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        std::thread::spawn(move || {
            let _ = sender.send(split_spans(vec![Span::plain(" now")], 1));
        });
        let groups = receiver
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("split_spans spun on a span it could not break");
        let text: String = groups
            .iter()
            .flat_map(|group| group.iter().map(|span| span.text.as_str()))
            .collect();
        assert_eq!(
            text, "now",
            "the text was dropped instead of split: {groups:?}"
        );
    }

    /// Emitter that charges for its own markup, as Discord's does: a link's target counts against
    /// the limit. `debug_emit` drops targets entirely, so with it the budget never shrinks and the
    /// give-up path this exercises is unreachable.
    fn link_emit(blocks: &[Block]) -> String {
        let mut out = String::new();
        for block in blocks {
            match block {
                Block::Text { spans, .. } | Block::Heading { spans, .. } => {
                    for span in spans {
                        match &span.link {
                            Some(target) => {
                                out.push('[');
                                out.push_str(&span.text);
                                out.push_str("](");
                                out.push_str(target);
                                out.push(')');
                            }
                            None => out.push_str(&span.text),
                        }
                    }
                }
                Block::Pre { text, .. } => {
                    out.push_str("```\n");
                    out.push_str(text);
                    out.push_str("\n```");
                }
            }
        }
        out
    }

    #[test]
    fn nothing_over_the_limit_is_ever_returned() {
        // The splitter gave up once the budget hit one and returned whatever it had, even when that
        // was still over. The platform then refuses the body outright -- twilight rejects it before
        // it leaves the process, Telegram answers 400 -- and the parts before it have already been
        // sent, so the reply lands half delivered with no record of where it stopped.
        // A target that lands *just under* the limit rather than over it. This is the case that
        // actually floods: the budget falls until the link is alone in its message and then fits,
        // so a check on whether the widest body fits never fires, and every other block has been
        // chopped to single characters by then. An unbreakable run that is merely *over* the limit
        // exercises the other arm and cannot see this.
        let snug = format!("[t](https://e.test/{})", "y".repeat(80));
        assert_eq!(
            snug.chars().count(),
            100,
            "the emitted link must be exactly the limit"
        );
        let unbreakable = "x".repeat(300);
        // Prose alongside the link matters: the link is what stops the budget shrinking, and the
        // prose is what gets chopped into single characters when it runs to the floor.
        let prose = "the quick brown fox jumps over the lazy dog. ".repeat(14);
        for markdown in [
            format!("{prose}{snug}\n\n{prose}"),
            format!("{prose}[the docs](https://example.test/{unbreakable})\n\n{prose}"),
            format!("- [x]({unbreakable})\n- second item"),
            format!("see {unbreakable} now"),
            format!("`{unbreakable}`"),
        ] {
            let chunks = into_messages(&markdown, 100, link_emit, count_chars);
            assert!(!chunks.is_empty(), "{markdown:?} rendered to nothing");
            // Bounding the count as well as each length. Keeping every chunk under the limit is
            // trivially satisfied by chopping the whole document into single characters, which is
            // what happens when the overflow is a constant per-piece cost the budget cannot shrink
            // away, and the connector then sends every one of them.
            assert!(
                chunks.iter().all(|chunk| !chunk.trim().is_empty()),
                "{markdown:?} produced a blank message, which the platform refuses"
            );
            assert!(
                chunks.len() <= 20,
                "{markdown:?} produced {} chunks, so the reply arrives as a flood",
                chunks.len()
            );
            for chunk in &chunks {
                assert!(
                    count_chars(chunk) <= 100,
                    "a chunk of {} characters was emitted against a limit of 100",
                    count_chars(chunk)
                );
            }
        }
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
