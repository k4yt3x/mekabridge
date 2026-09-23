//! A workaround for emphasis in CJK prose, written to be deleted whole.
//!
//! CommonMark only lets `**` close emphasis when its delimiter run is right-flanking: not preceded
//! by whitespace, and either not preceded by punctuation or followed by whitespace or punctuation.
//! In `**今天下雨。**明天转晴` the closing run is preceded by `。` and followed by `明`, so neither
//! branch holds, the run never closes, and the asterisks arrive as literal text. English rarely
//! notices because a space normally follows the punctuation. Chinese and Japanese set punctuation
//! hard against the next character, so it happens in nearly every emphasised sentence. Symbols
//! count as punctuation since CommonMark 0.31, so an emoji inside the emphasis blocks the run the
//! same way a full stop does.
//!
//! Upstream's fix is to stop counting CJK punctuation as punctuation for that test, and to let a
//! CJK character on the far side of the run satisfy the second branch:
//! commonmark/commonmark-spec#650, spec PR commonmark/commonmark-spec#839, implemented in
//! pulldown-cmark/pulldown-cmark#1059 as `Options::ENABLE_CJK_FRIENDLY_EMPHASIS` and merged on
//! 2026-07-30. No release carries it as of 0.13.4. When one does, delete this file, set that option
//! in [`super::parse_blocks`], and drop the three calls that reach in here: two in `parse_blocks`
//! and one in `ParseState::push_table`.
//!
//! Until then the same effect comes from rewriting the source. A U+2060 WORD JOINER between the run
//! and the character blocking it satisfies "not preceded by punctuation" outright, because a word
//! joiner is neither whitespace nor punctuation, and [`restore`] takes it back out of the parsed
//! spans so it never reaches a platform.
//!
//! What keeps this from breaking markdown that already worked is that a sentinel goes in on both
//! sides of a run or on neither. Putting one in satisfies "not preceded by punctuation" on that
//! side, which is what a blocked run needs, but it also takes away "followed by punctuation", which
//! the run's other direction may be leaning on, so a one-sided insertion can turn flanking off as
//! readily as on. The two sides where skipping is safe are the two where that cannot happen:
//! whitespace, where the run has no flanking in that direction to lose and must not be handed any,
//! and alphanumerics, which are not punctuation, so the branch on that side holds already.
//!
//! Three things are left alone outright, each because a sentinel beside them would change what they
//! are rather than how they flank. A run touching another delimiter: separating the two changes
//! which of them wins, and `正文**_重点。_**继续` comes out bold instead of italic. A run touching
//! a backslash: the backslash escapes whatever follows it, so a sentinel between them both strands
//! the backslash on screen and turns a deliberately literal asterisk into markup. And a
//! one-character `~`, which pulldown-cmark restricts the way it restricts `_`, opening and closing
//! only against the punctuation a sentinel replaces.
//!
//! Nothing is inserted at all unless a CJK character sits on one side of the run or the other, so a
//! document without CJK in it parses as it did before.
//!
//! The equivalence was checked rather than argued, against a build of upstream's branch with
//! `ENABLE_CJK_FRIENDLY_EMPHASIS` set. Across 110878 arrangements of CJK, kana, hangul, halfwidth
//! and fullwidth punctuation, Latin, an emoji, whitespace, backslashes and delimiter runs one to
//! three long, the visible text never came out different from an unrewritten parse, and every
//! arrangement where this module takes emphasis away is one where upstream's own flag takes the
//! same emphasis away: 696 of 77022 with two runs against upstream's 760, and 108 of 33856 with
//! four runs packed together against upstream's 680. It does less than upstream in 659 of the
//! 77022, and a backslash is involved in every one of them.

use std::{borrow::Cow, ops::RangeInclusive};

use super::Block;

/// Stands in for the whitespace or punctuation CommonMark wants next to the run. Invisible, not
/// whitespace, and not punctuation, which are the three things the flanking test looks at.
const SENTINEL: char = '\u{2060}';

/// `_` is deliberately absent. Its own rule asks to be "preceded by a punctuation character" before
/// it may open emphasis inside a word, and a sentinel replaces exactly that neighbour, so rewriting
/// an underscore run would turn off pairs that work today. Upstream leaves the underscore
/// restrictions alone for the same reason, and CJK prose writes emphasis with `**`.
const DELIMITERS: [char; 2] = ['*', '~'];

/// Rewrite the source so CJK-adjacent delimiter runs can open and close emphasis.
///
/// Any sentinel already in the input is dropped first, so the character is never ambiguous between
/// the author's and ours. It carries no meaning in a chat message and nothing downstream can tell
/// the difference.
pub(super) fn normalize(markdown: &str) -> Cow<'_, str> {
    if !markdown
        .chars()
        .any(|character| character == SENTINEL || is_cjk(character))
    {
        return Cow::Borrowed(markdown);
    }

    let source: Vec<char> = markdown
        .chars()
        .filter(|character| *character != SENTINEL)
        .collect();
    let mut normalized = String::with_capacity(markdown.len());
    let mut index = 0;
    while let Some(&character) = source.get(index) {
        if !DELIMITERS.contains(&character) {
            normalized.push(character);
            index += 1;
            continue;
        }
        // A delimiter run is the whole span of one repeated character, and its length decides what
        // it can pair with, so it is measured and emitted as a unit rather than per character.
        let start = index;
        while source.get(index) == Some(&character) {
            index += 1;
        }
        let before = start
            .checked_sub(1)
            .and_then(|previous| source.get(previous));
        let after = source.get(index);
        // A one-character `~` carries the restriction `_` carries rather than the one `*` carries:
        // pulldown-cmark will only open or close it against punctuation, which is the neighbour a
        // sentinel replaces. Only `~~` is a run this can do anything for.
        let rewritable = character != '~' || index - start == 2;
        if rewritable && rescues(before, after) {
            normalized.push(SENTINEL);
        }
        for _ in start..index {
            normalized.push(character);
        }
        if rewritable && rescues(after, before) {
            normalized.push(SENTINEL);
        }
    }
    Cow::Owned(normalized)
}

/// Take every sentinel back out of the parsed blocks.
pub(super) fn restore(blocks: &mut Vec<Block>) {
    blocks.retain_mut(|block| match block {
        Block::Text { spans, .. } | Block::Heading { spans, .. } => {
            for span in spans.iter_mut() {
                clear(&mut span.text);
                if let Some(link) = &mut span.link {
                    clear(link);
                }
            }
            let carried_something = !spans.is_empty();
            // A span that held nothing but a sentinel would otherwise reach an emitter as an empty
            // pair of tags, which Telegram refuses outright. Dropping the block too, but only when
            // it had spans and every one of them was ours, since an empty block the parser made on
            // its own is the parser's business.
            spans.retain(|span| !span.text.is_empty());
            !carried_something || !spans.is_empty()
        }
        Block::Pre { language, text } => {
            clear(text);
            if let Some(language) = language {
                clear(language);
            }
            true
        }
    });
}

/// Drop every sentinel from one string.
pub(super) fn clear(text: &mut String) {
    if text.contains(SENTINEL) {
        text.retain(|character| character != SENTINEL);
    }
}

/// Whether a sentinel belongs between a delimiter run and `neighbour`, given `across` on the run's
/// other side.
fn rescues(neighbour: Option<&char>, across: Option<&char>) -> bool {
    let Some(&neighbour) = neighbour else {
        return false;
    };
    // Whitespace is the one neighbour that must never be separated from the run. A run preceded by
    // a space cannot close emphasis today, and a sentinel there would let it, which is a change to
    // text that has nothing to do with CJK. An alphanumeric neighbour is skipped because it is not
    // punctuation, so the flanking branch it sits on already holds and a sentinel would be noise.
    if neighbour.is_whitespace() || neighbour.is_alphanumeric() {
        return false;
    }
    // Both sides have to agree on this skip or the skip itself takes emphasis away. Leaving one
    // side alone is safe when that side is alphanumeric, which is not punctuation and so satisfies
    // the flanking test already, but these are all punctuation: skipping one of them on one side
    // while inserting on the other strips a run of the "followed by punctuation" it was closing on.
    if is_bound_to_a_run(neighbour) || across.is_some_and(|&across| is_bound_to_a_run(across)) {
        return false;
    }
    // Having ruled out the alphanumerics, a CJK neighbour here is CJK punctuation: the case
    // upstream rescues by not counting it as punctuation at all. The other arm is upstream's
    // second: punctuation of any kind, including an emoji, with a CJK character across the run.
    is_cjk(neighbour) || across.is_some_and(|&across| is_cjk(across))
}

/// The blocks CJK prose is written out of, punctuation included.
///
/// Ranges rather than a Unicode table, because the question here is only which characters are
/// written without spaces around them, and that is the East Asian blocks and the punctuation that
/// belongs to them. Halfwidth forms are left out: they are narrow, and they take spaces the way
/// Latin does.
const CJK_RANGES: &[RangeInclusive<u32>] = &[
    0x1100..=0x11FF,   // Hangul Jamo
    0x2E80..=0x2EFF,   // CJK Radicals Supplement
    0x2F00..=0x2FDF,   // Kangxi Radicals
    0x2FF0..=0x2FFF,   // Ideographic Description Characters
    0x3000..=0x303F,   // CJK Symbols and Punctuation
    0x3040..=0x30FF,   // Hiragana and Katakana
    0x3100..=0x312F,   // Bopomofo
    0x3130..=0x318F,   // Hangul Compatibility Jamo
    0x3190..=0x319F,   // Kanbun
    0x31A0..=0x31BF,   // Bopomofo Extended
    0x31C0..=0x31EF,   // CJK Strokes
    0x31F0..=0x31FF,   // Katakana Phonetic Extensions
    0x3200..=0x32FF,   // Enclosed CJK Letters and Months
    0x3300..=0x33FF,   // CJK Compatibility
    0x3400..=0x4DBF,   // CJK Unified Ideographs Extension A
    0x4E00..=0x9FFF,   // CJK Unified Ideographs
    0xA960..=0xA97F,   // Hangul Jamo Extended-A
    0xAC00..=0xD7AF,   // Hangul Syllables
    0xD7B0..=0xD7FF,   // Hangul Jamo Extended-B
    0xF900..=0xFAFF,   // CJK Compatibility Ideographs
    0xFE10..=0xFE1F,   // Vertical Forms
    0xFE30..=0xFE4F,   // CJK Compatibility Forms
    0xFE50..=0xFE6F,   // Small Form Variants
    0xFF01..=0xFF9F,   // Fullwidth ASCII, halfwidth CJK punctuation and katakana
    0xFFE0..=0xFFE6,   // Fullwidth symbols
    0x1F200..=0x1F2FF, // Enclosed Ideographic Supplement
    0x20000..=0x3FFFD, // CJK Unified Ideographs, extensions B onwards
];

/// Characters that mean something by sitting exactly where they are: another emphasis delimiter
/// pairs with the run it touches, and a backslash escapes the character after it. A sentinel
/// between one of these and a run changes what the run is, so a run touching one is left alone.
fn is_bound_to_a_run(character: char) -> bool {
    matches!(character, '*' | '_' | '~' | '\\')
}

fn is_cjk(character: char) -> bool {
    let code = u32::from(character);
    // Latin, and everything else below the first block, is answered without touching the table.
    code >= 0x1100 && CJK_RANGES.iter().any(|range| range.contains(&code))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::render::{Style, collect_blocks, parse_blocks};

    /// The visible text of a parse, one entry per character, with the style it carries.
    fn styled(blocks: &[Block]) -> Vec<(char, Style)> {
        let mut out = Vec::new();
        for block in blocks {
            match block {
                Block::Text { spans, .. } | Block::Heading { spans, .. } => {
                    for span in spans {
                        out.extend(span.text.chars().map(|character| (character, span.style)));
                    }
                }
                Block::Pre { text, .. } => {
                    out.extend(text.chars().map(|character| (character, Style::default())));
                }
            }
        }
        out
    }

    fn rendered(markdown: &str) -> Vec<(char, Style)> {
        styled(&parse_blocks(markdown))
    }

    fn carrying(markdown: &str, wanted: fn(&Style) -> bool) -> String {
        rendered(markdown)
            .into_iter()
            .filter(|(_, style)| wanted(style))
            .map(|(character, _)| character)
            .collect()
    }

    fn bold(markdown: &str) -> String {
        carrying(markdown, |style| style.bold)
    }

    fn text(markdown: &str) -> String {
        rendered(markdown)
            .into_iter()
            .map(|(character, _)| character)
            .collect()
    }

    #[test]
    fn a_sentence_ending_in_cjk_punctuation_still_closes_its_emphasis() {
        // The shape this module was written for: bold that ends on a full stop and runs straight
        // into the next sentence, which is how Chinese is written.
        assert_eq!(
            bold("**记得 9/30 是最后一天。**表格请在 8/16 之前交上来"),
            "记得 9/30 是最后一天。"
        );
    }

    #[test]
    fn every_shape_of_blocked_run_in_cjk_prose_closes() {
        let cases = [
            ("**今天下雨。**明天转晴", "今天下雨。"),
            ("**气温.**明天回升", "气温."),
            ("**备注(1)**见下方", "备注(1)"),
            ("以下**(备注)**参照", "(备注)"),
            ("**完成🎉**下一步", "完成🎉"),
            ("**「見出し」**です", "「見出し」"),
            ("**\"見出し\"**です", "\"見出し\""),
            ("**首先，**Then", "首先，"),
            ("**한국어입니다.**계속", "한국어입니다."),
            ("**半角｡**a", "半角｡"),
        ];
        for (markdown, expected) in cases {
            assert_eq!(bold(markdown), expected, "for {markdown:?}");
        }
    }

    #[test]
    fn italic_and_strikethrough_are_rescued_the_same_way() {
        assert_eq!(
            carrying("*今天下雨。*明天转晴", |style| style.italic),
            "今天下雨。"
        );
        assert_eq!(
            carrying("~~今天下雨。~~明天转晴", |style| style
                .strikethrough),
            "今天下雨。"
        );
    }

    #[test]
    fn an_underscore_run_is_not_a_run_this_rewrites() {
        // Underscore is excluded because its opening rule asks to be preceded by punctuation, which
        // is the neighbour a sentinel would replace. Asserted on the rewrite rather than on the
        // parse, since the parse would be unchanged whether or not this module knew what an
        // underscore was.
        assert_eq!(normalize("。_重点_。"), "。_重点_。");
        assert_eq!(carrying("。_重点_。", |style| style.italic), "重点");
    }

    #[test]
    fn an_escaped_delimiter_stays_escaped() {
        // A backslash escapes the character after it, so a sentinel between the two both leaves the
        // backslash on screen and turns a deliberately literal asterisk into live markup.
        assert_eq!(text(r"用 \*星号\* 表示乘法"), "用 *星号* 表示乘法");
        assert_eq!(bold(r"用 \*星号\* 表示乘法"), "");
        assert_eq!(text(r"甲\*乙*"), "甲*乙*");
        assert_eq!(carrying(r"甲\*乙*", |style| style.italic), "");
    }

    #[test]
    fn a_lone_tilde_is_left_to_the_parser() {
        // One `~` is restricted the way `_` is rather than the way `*` is: it opens and closes only
        // against punctuation, so rewriting it takes strikethrough away instead of granting it. Two
        // of them carry no such restriction and are rescued as normal.
        assert_eq!(
            carrying("（~备注~）正文", |style| style.strikethrough),
            "备注"
        );
        assert_eq!(carrying("。~划掉~", |style| style.strikethrough), "划掉");
        assert_eq!(
            carrying("（~~备注~~）正文", |style| style.strikethrough),
            "备注"
        );
    }

    #[test]
    fn an_empty_code_block_is_still_a_code_block() {
        // A block is dropped only when it held spans and every one of them was a sentinel of ours.
        // An empty fence is the parser's own and used to reach the platform as an empty `pre`.
        for markdown in ["```\n```", "```rust\n\n```"] {
            assert_eq!(
                parse_blocks(markdown),
                collect_blocks(markdown),
                "for {markdown:?}"
            );
        }
    }

    #[test]
    fn text_without_cjk_is_not_touched_at_all() {
        for markdown in [
            "**bold.**tail",
            "a **b** c",
            "*em* and _em_ and ~~gone~~",
            "# heading\n\n- one\n- two\n\n```rust\nlet x = *y;\n```",
            "[link](https://example.com/a_b~c*d)",
            "snake_case_word and 2**3**4",
        ] {
            assert!(
                matches!(normalize(markdown), Cow::Borrowed(_)),
                "rewrote {markdown:?}"
            );
        }
    }

    #[test]
    fn the_sentinel_never_reaches_a_span() {
        let markdown = "**标题。**正文\n\n> **摘录。**接着\n\n# **标题。**收尾\n\n`代码。*x*` 和 **加粗（1）**结束\n\n```\n正文。*x*\n```\n\n| 项目。 | *数量* |\n| --- | --- |\n| 甲。 | 乙 |";
        for block in parse_blocks(markdown) {
            let carries = match &block {
                Block::Text { spans, .. } | Block::Heading { spans, .. } => {
                    spans.iter().any(|span| {
                        span.text.contains(SENTINEL)
                            || span
                                .link
                                .as_deref()
                                .is_some_and(|link| link.contains(SENTINEL))
                    })
                }
                Block::Pre { language, text } => {
                    text.contains(SENTINEL)
                        || language
                            .as_deref()
                            .is_some_and(|language| language.contains(SENTINEL))
                }
            };
            assert!(!carries, "sentinel survived in {block:?}");
        }
    }

    #[test]
    fn a_sentinel_written_by_the_author_is_dropped_rather_than_trusted() {
        // Ours and theirs are the same character, so the input is cleared of it before anything is
        // inserted. It renders as nothing either way.
        assert_eq!(text("甲\u{2060}乙"), "甲乙");
        assert_eq!(bold("**今天下雨。\u{2060}**明天转晴"), "今天下雨。");
    }

    #[test]
    fn a_link_target_survives_the_rewrite() {
        let blocks = parse_blocks("见[条目](https://example.com/wiki/词条_(消歧义))。");
        let Some(Block::Text { spans, .. }) = blocks.first() else {
            panic!("expected text, got {blocks:?}");
        };
        let link = spans.iter().find_map(|span| span.link.clone());
        assert_eq!(
            link.as_deref(),
            Some("https://example.com/wiki/词条_(消歧义)")
        );
    }

    #[test]
    fn block_structure_that_leads_with_a_delimiter_is_left_alone() {
        // A bullet, a thematic break and a tilde fence all open a line with a delimiter run, and a
        // sentinel in front of one would turn it into ordinary text.
        assert_eq!(text("* 项目一\n* 项目二"), "• 项目一• 项目二");
        assert!(text("正文\n\n***\n\n继续").contains("──────────"));
        let blocks = parse_blocks("~~~文本\n代码。\n~~~");
        assert_eq!(blocks.len(), 1, "{blocks:?}");
        assert!(
            matches!(blocks.first(), Some(Block::Pre { .. })),
            "{blocks:?}"
        );
    }

    #[test]
    fn a_run_against_whitespace_is_never_separated_from_it() {
        // ` **正文` cannot close emphasis and must not start doing so, which is what a sentinel
        // between the space and the run would cause.
        assert_eq!(bold("正文 **重点** 正文"), "重点");
        assert_eq!(bold("正文 ** 不是重点 ** 正文"), "");
        // Paired with the line above so an empty parse cannot pass for an unemphasised one.
        assert_eq!(text("正文 ** 不是重点 ** 正文"), "正文 ** 不是重点 ** 正文");
    }

    #[test]
    fn the_rewrite_only_ever_adds_emphasis() {
        // The safety argument in one property: across every arrangement of CJK, Latin, punctuation,
        // an emoji, whitespace, a backslash and delimiter runs, the rewrite leaves the visible text
        // alone apart from the delimiters the parse consumed, and no character loses a style it
        // already carried.
        //
        // The exceptions are counted rather than waved past. Each one is CommonMark's rule of
        // three, where letting a run close makes it able to do both jobs and the pair is then
        // refused for its lengths; `a_mismatched_pair_falls_to_the_rule_of_three` works one
        // through. Pinning the number means a loss arriving for any other reason shows up here even
        // in a shape where the rule could have fired.
        let fillers = ["中", "。", "a", ".", "🎉", " ", "", "_", "\\", "\\*"];
        let runs = ["*", "**", "***", "~", "~~"];
        let mut lost = Vec::new();
        for first in fillers {
            for opening in runs {
                for middle in fillers {
                    for closing in runs {
                        for last in fillers {
                            let markdown = format!("{first}{opening}{middle}{closing}{last}");
                            let before = styled(&collect_blocks(&markdown));
                            let after = rendered(&markdown);
                            assert_eq!(
                                visible(&before),
                                visible(&after),
                                "text changed for {markdown:?}"
                            );
                            let lost_one = styles(&before).into_iter().zip(styles(&after)).any(
                                |(was, now)| {
                                    (was.bold && !now.bold)
                                        || (was.italic && !now.italic)
                                        || (was.strikethrough && !now.strikethrough)
                                        || was.code != now.code
                                },
                            );
                            if lost_one {
                                assert!(
                                    rule_of_three(&markdown),
                                    "{markdown:?} lost a style where the rule of three cannot fire"
                                );
                                lost.push(markdown);
                            }
                        }
                    }
                }
            }
        }
        assert_eq!(lost.len(), 108, "{lost:?}");
    }

    /// The characters a parse left visible, with the delimiters it consumed taken out of both sides
    /// so the two are comparable.
    fn visible(carried: &[(char, Style)]) -> String {
        carried
            .iter()
            .filter(|(character, _)| !matches!(character, '*' | '_' | '~'))
            .map(|(character, _)| *character)
            .collect()
    }

    fn styles(carried: &[(char, Style)]) -> Vec<Style> {
        carried
            .iter()
            .filter(|(character, _)| !matches!(character, '*' | '_' | '~'))
            .map(|(_, style)| *style)
            .collect()
    }

    /// Whether CommonMark's rule of three can govern any pair of runs in `markdown`: their lengths
    /// sum to a multiple of three without both being multiples of three. Necessary rather than
    /// sufficient, which is why the count above is pinned as well.
    fn rule_of_three(markdown: &str) -> bool {
        let runs = delimiter_runs(markdown);
        runs.iter()
            .enumerate()
            .any(|(index, (character, opening))| {
                runs[index + 1..].iter().any(|(other, closing)| {
                    other == character
                        && (opening + closing) % 3 == 0
                        && !(opening % 3 == 0 && closing % 3 == 0)
                })
            })
    }

    /// The delimiter runs a parse sees, which is not what the source looks like: an escaped
    /// delimiter is text, and it splits the run it sits in rather than lengthening it.
    fn delimiter_runs(markdown: &str) -> Vec<(char, usize)> {
        let mut runs: Vec<(char, usize)> = Vec::new();
        let mut escaped = false;
        for character in markdown.chars() {
            let delimiter = !escaped && matches!(character, '*' | '_' | '~');
            match runs.last_mut() {
                Some((last, length)) if delimiter && *last == character => *length += 1,
                _ if delimiter => runs.push((character, 1)),
                // Anything else ends whatever run was open, so the next one starts fresh.
                _ => runs.push(('\0', 0)),
            }
            escaped = !escaped && character == '\\';
        }
        runs.retain(|(_, length)| *length > 0);
        runs
    }

    #[test]
    fn a_quoted_line_reaches_both_platforms_as_bold() {
        // Everything else here works on the intermediate. This is the whole path, from what the
        // agent wrote to what the platform is handed.
        let markdown = "> **记得 9/30 是最后一天。**表格请在 8/16 之前交上来，逾期不再受理。";
        assert_eq!(
            crate::channel::telegram::render::to_html(markdown, 4096),
            vec![
                "<blockquote><b>记得 9/30 是最后一天。</b>表格请在 8/16 之前交上来，逾期不再受理。</blockquote>"
            ]
        );
        assert_eq!(
            crate::channel::discord::render::to_markdown(markdown, 2000),
            vec!["> **记得 9/30 是最后一天。**表格请在 8/16 之前交上来，逾期不再受理。"]
        );
    }

    #[test]
    fn emphasis_packed_against_emphasis_is_left_to_the_parser() {
        // Separating two runs that touch changes which of them wins, and the loser's styling goes:
        // `正文**_重点。_**继续` is italic before the rewrite and would come out bold and not
        // italic after it. Runs against runs are handed to the parser exactly as they were
        // written.
        for markdown in [
            "正文**_重点。_**继续",
            "正文_**重点。**_继续",
            "正文**~~划掉。~~**继续",
            "正文*~~划。~~*继续",
            "正文**甲_**。",
        ] {
            assert_eq!(
                styled(&parse_blocks(markdown)),
                styled(&collect_blocks(markdown)),
                "for {markdown:?}"
            );
        }
    }

    #[test]
    fn a_table_column_is_no_wider_for_holding_emphasis() {
        // Column widths are measured while the table is parsed, before the sentinel comes out, so a
        // cell that carried one used to pad its whole column a character wider than anything in it.
        let table = |cell: &str| {
            parse_blocks(&format!(
                "| 项目 | 数量 |\n| --- | --- |\n| {cell} | 一 |\n| 普通 | 二 |"
            ))
        };
        assert_eq!(table("**加粗。**"), table("加粗。"));
    }

    #[test]
    fn a_mismatched_pair_falls_to_the_rule_of_three() {
        // `。*甲**。` is emphasised today only because its opening run, sitting against CJK
        // punctuation, is unable to close. CommonMark refuses a pair whose run lengths sum to a
        // multiple of three when either delimiter could do both jobs, so letting that run close,
        // which is the point of the whole module, brings the rule into play and the pair is
        // refused. Upstream's own flag does the same to this input, and reaching it takes a `*`
        // opened against a `**`, which is malformed either way.
        let carried = rendered("。*甲**。");
        assert!(
            carried.iter().all(|(_, style)| !style.italic),
            "{carried:?}"
        );
        assert_eq!(text("。*甲**。"), "。*甲**。");
    }
}
