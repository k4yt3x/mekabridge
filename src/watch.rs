//! Watches: the agent's own standing reason for a muted conversation to wake it.
//!
//! A conversation on [`crate::store::Policy::Mute`] delivers a message when the platform says it
//! was addressed to the agent. That is two facts about a message, both of them somebody else's
//! decision. A watch is the third reason and the agent's own: a pattern it set, over one named
//! field of the message. It is the keyword notification every chat client already has, which is
//! why it belongs to the bridge's attention vocabulary rather than to whatever is reading the
//! messages.
//!
//! What a watch is *not* is a judgement. It decides that a message is worth a turn, and nothing
//! about what the message means; the turn it wakes is where that is settled.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use regex::{Regex, RegexSet, RegexSetBuilder};
use serde::{Deserialize, Serialize};

use crate::{channel::InboundMessage, store::WatchRecord};

/// Most watches one deployment may hold.
///
/// A ceiling rather than a tuning knob: every pattern is matched against every message in a muted
/// conversation, and an agent that can add rules at runtime is an agent that can add them without
/// limit. Generous enough that a real rule set is nowhere near it.
pub const MAX_WATCHES: usize = 500;

/// Longest pattern accepted, in characters.
///
/// Patterns come from the model, so they are untrusted input twice over: whatever the agent wrote,
/// which is whatever the last person talking to it talked it into.
pub const MAX_PATTERN_CHARS: usize = 256;

/// Compiled size ceiling for one pattern, and for a whole set.
///
/// The `regex` crate is linear in the length of the haystack whatever the pattern does, so the
/// exposure here is memory rather than time: a small pattern with large bounded repetitions
/// (`(?:a{1000}){1000}`) compiles to an enormous program. These bound that.
const PATTERN_SIZE_LIMIT: usize = 256 * 1024;
const SET_SIZE_LIMIT: usize = 16 * 1024 * 1024;

/// Which part of a message a watch reads.
///
/// One watch inspects one field. The wake line the agent is shown names the field that matched, so
/// a rule that could have fired on any of three would leave it guessing which; and a pattern
/// written for a name is rarely the one you would point at a message body.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WatchField {
    /// The message body, which is also where a photo's caption arrives.
    Text,
    /// The sender's display name and username, either of which counts.
    Sender,
    /// The sender's platform id, for watching one person rather than a turn of phrase.
    SenderId,
}

impl WatchField {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Sender => "sender",
            Self::SenderId => "sender_id",
        }
    }

    /// How the wake line names this field, as the tail of "matched your watch #3 ...".
    pub const fn describe(self) -> &'static str {
        match self {
            Self::Text => "in the text",
            Self::Sender => "in the sender's name",
            Self::SenderId => "on the sender's id",
        }
    }

    /// Parse the stored spelling. The CHECK constraint keeps anything else out of the column, so an
    /// unrecognised value means a hand-edited database and is reported rather than guessed at.
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "text" => Some(Self::Text),
            "sender" => Some(Self::Sender),
            "sender_id" => Some(Self::SenderId),
            _ => None,
        }
    }
}

/// How a watch's patterns combine.
///
/// `All` exists because "these terms, in any order" is a rule people genuinely write, and its
/// obvious spelling is lookahead (`(?=.*A)(?=.*B)`), which is exactly what the `regex` crate
/// refuses. Saying it in the rule rather than in the pattern keeps both the rule and the
/// linear-time guarantee that refusal buys.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WatchMode {
    /// Fires when any one pattern matches. What a keyword list means.
    #[default]
    Any,
    /// Fires only when every pattern matches, in any order and anywhere in the field.
    All,
}

impl WatchMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Any => "any",
            Self::All => "all",
        }
    }

    /// Parse the stored spelling. The CHECK constraint keeps anything else out of the column, so an
    /// unrecognised value means a hand-edited database and is reported rather than guessed at.
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "any" => Some(Self::Any),
            "all" => Some(Self::All),
            _ => None,
        }
    }
}

/// Longest watch name accepted, in characters.
pub const MAX_NAME_CHARS: usize = 64;

/// Most patterns one deployment may hold across every watch.
///
/// The ceiling that actually matters now that a watch holds a list: [`MAX_WATCHES`] bounds the
/// rules, and this bounds the work, since every pattern of a field is compiled into that field's
/// one set.
pub const MAX_PATTERNS: usize = 2000;

/// Check a watch name, or say what is wrong with it.
///
/// A name is an identifier the agent chooses and then refers to, and it is printed inside quotes
/// on the wake line, so it has to be one line and it has to be something a model can reproduce
/// exactly. Nothing stricter than that: rejecting spaces or punctuation would only make the agent
/// guess at a scheme nobody wrote down.
pub fn validate_name(name: &str) -> Result<(), String> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Err("the name is empty".to_string());
    }
    if trimmed != name {
        return Err("the name has leading or trailing whitespace".to_string());
    }
    let length = name.chars().count();
    if length > MAX_NAME_CHARS {
        return Err(format!(
            "the name is {length} characters, and at most {MAX_NAME_CHARS} are accepted"
        ));
    }
    if name.chars().any(|character| {
        character.is_control() || matches!(character, '\u{2028}' | '\u{2029}' | '\u{85}')
    }) {
        return Err("the name contains a line break or a control character".to_string());
    }
    Ok(())
}

/// What made a watch fire, as the wake line reports it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WatchHit {
    /// The pattern that matched, under [`WatchMode::Any`]. The first one in the rule's own order,
    /// where several did, because that is the one the person reading the rule will look for first.
    Pattern(String),
    /// How many patterns had to match, under [`WatchMode::All`]. Naming one of them would be
    /// arbitrary and naming all of them would put a rule set on a header line.
    All(usize),
}

impl Default for WatchHit {
    /// What an item queued by 0.16.0 decodes to. It reports nothing about the hit, which is
    /// honest: that release recorded a pattern under a numeric id this one no longer uses.
    fn default() -> Self {
        Self::All(0)
    }
}

/// One watch that fired on one message.
///
/// Carried on the message through the queue, so the item the agent reads can name the rule that
/// woke it. Without that the agent is handed a message from a muted room with no way to tell which
/// of its own rules is responsible, which is the one thing it needs in order to retune them.
///
/// `name` and `hit` default on decode so an item already queued when the bridge was upgraded still
/// reads back. Such an item renders without naming its rule, which is the price of not draining
/// the queue across an upgrade and lasts exactly as long as the items that predate it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatchMatch {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub hit: WatchHit,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub field: WatchField,
}

/// Compile one pattern the way the matcher will, or say why it cannot be.
///
/// The error is handed to whoever wrote the pattern, verbatim: `regex` explains what is wrong with
/// a caret in the wrong place far better than "invalid pattern" does, and the agent can act on the
/// explanation.
///
/// Case-insensitive by default, because the agent is writing something closer to a keyword than to
/// a parser, and a rule that misses on a capital letter is a rule that looks like it works. A
/// pattern that means it can say `(?-i)`.
pub fn compile_pattern(pattern: &str) -> Result<Regex, String> {
    if pattern.trim().is_empty() {
        return Err("the pattern is empty".to_string());
    }
    let length = pattern.chars().count();
    if length > MAX_PATTERN_CHARS {
        return Err(format!(
            "the pattern is {length} characters, and at most {MAX_PATTERN_CHARS} are accepted"
        ));
    }
    regex::RegexBuilder::new(pattern)
        .case_insensitive(true)
        .size_limit(PATTERN_SIZE_LIMIT)
        .build()
        .map_err(|error| error.to_string())
}

/// The watches in force, compiled once and matched against every message a muted conversation
/// offers.
///
/// One [`RegexSet`] per field rather than one [`Regex`] per row: a set matches every pattern in a
/// single pass over the haystack, which is what keeps a busy room affordable when the agent has
/// accumulated a hundred rules. The sets are parallel to `rows` through `indices`, since a set
/// reports the positions that matched and nothing else.
pub struct Watchlist {
    rows: Vec<WatchRecord>,
    fields: Vec<FieldSet>,
}

/// One field's patterns, and which rule each came from.
struct FieldSet {
    field: WatchField,
    set: RegexSet,
    /// For each pattern in `set`, in the same order: its row in [`Watchlist::rows`], and its
    /// position within that row's own list. Several entries point at one row now that a watch
    /// holds many patterns, which is what turns a set hit back into a rule.
    sources: Vec<(usize, usize)>,
    /// How many patterns each row contributed, by row index. What [`WatchMode::All`] compares
    /// against: a rule is satisfied when every pattern that *compiled* matched, and a row whose
    /// patterns did not all compile is not in here at all.
    counts: HashMap<usize, usize>,
}

impl Watchlist {
    /// Build from the rows the store holds.
    ///
    /// A row whose pattern will not compile is skipped rather than failing the whole list: every
    /// pattern was compiled once before it was stored, so one that no longer compiles means a
    /// hand-edited database, and dropping every other watch over it would turn one bad row into a
    /// silently deaf bridge. The log line is how anybody finds out.
    pub fn new(rows: Vec<WatchRecord>) -> Self {
        let mut fields = Vec::new();
        for field in [WatchField::Text, WatchField::Sender, WatchField::SenderId] {
            let mut patterns = Vec::new();
            let mut sources = Vec::new();
            let mut counts: HashMap<usize, usize> = HashMap::new();
            for (index, row) in rows.iter().enumerate() {
                if row.field != field {
                    continue;
                }
                let mut compiled = Vec::new();
                let mut broken = false;
                for (position, pattern) in row.patterns.iter().enumerate() {
                    match compile_pattern(pattern) {
                        Ok(_) => compiled.push((position, pattern.clone())),
                        Err(error) => {
                            broken = true;
                            tracing::error!(
                                watch = %row.name,
                                pattern = %pattern,
                                "a stored watch pattern does not compile and is being ignored: {}",
                                error
                            );
                        }
                    }
                }
                // Under `all` a skipped pattern is a term the rule no longer requires, so dropping
                // it would make the rule easier to satisfy and wake the agent on messages it
                // never asked about. Under `any` the same skip only costs one of several ways to
                // fire, so the rest of the rule still stands.
                if broken && row.mode == WatchMode::All {
                    tracing::error!(
                        watch = %row.name,
                        "a pattern of this `all` watch does not compile, so the whole watch is \
                         disabled rather than being made easier to satisfy"
                    );
                    continue;
                }
                if compiled.is_empty() {
                    continue;
                }
                counts.insert(index, compiled.len());
                for (position, pattern) in compiled {
                    patterns.push(pattern);
                    sources.push((index, position));
                }
            }
            if patterns.is_empty() {
                continue;
            }
            match RegexSetBuilder::new(&patterns)
                .case_insensitive(true)
                .size_limit(SET_SIZE_LIMIT)
                .build()
            {
                Ok(set) => fields.push(FieldSet {
                    field,
                    set,
                    sources,
                    counts,
                }),
                // Every pattern compiled on its own just above, so this is the combined size
                // ceiling. Reported rather than retried one pattern at a time: the ceiling is
                // sixteen megabytes of compiled program, and a rule set that reaches it is a
                // configuration problem rather than something to work around silently.
                Err(error) => tracing::error!(
                    field = field.as_str(),
                    "the watches for this field could not be compiled together, so none of them \
                     will match: {}",
                    error
                ),
            }
        }
        Self { rows, fields }
    }

    /// Whether anything is being watched at all, so the gate can skip the work entirely.
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// Every watch that fires on `message`, in the order they were created.
    ///
    /// A watch scoped to a conversation is checked only there; one with no scope is checked
    /// everywhere. The scope is filtered after matching rather than before, because the set is what
    /// makes matching one pass and it has no notion of which rows are relevant.
    pub fn matches(&self, message: &InboundMessage) -> Vec<WatchMatch> {
        // Row to the positions of its patterns that matched, unioned across the field's haystacks.
        // A set is one pass over one string, so a sender rule is two passes and everything else is
        // one, whatever the rule count.
        let mut matched: BTreeMap<usize, (WatchField, BTreeSet<usize>)> = BTreeMap::new();
        for field in &self.fields {
            for haystack in haystacks(field.field, message) {
                if haystack.is_empty() {
                    continue;
                }
                for index in field.set.matches(haystack).into_iter() {
                    let Some(&(row, position)) = field.sources.get(index) else {
                        continue;
                    };
                    matched
                        .entry(row)
                        .or_insert_with(|| (field.field, BTreeSet::new()))
                        .1
                        .insert(position);
                }
            }
        }
        // `BTreeMap` iterates in row order, which is the order the store read the rules in, which
        // is the order they were created.
        matched
            .into_iter()
            .filter_map(|(row, (field, positions))| {
                let record = self.rows.get(row)?;
                let scoped_elsewhere = record
                    .conversation
                    .as_deref()
                    .is_some_and(|address| address != message.conversation.as_str());
                if scoped_elsewhere {
                    return None;
                }
                let hit = match record.mode {
                    // The earliest pattern in the rule's own order, which is the one somebody
                    // reading the rule looks for first.
                    WatchMode::Any => {
                        WatchHit::Pattern(record.patterns.get(*positions.first()?)?.clone())
                    }
                    // Every pattern that compiled, and `new` refuses to compile an `all` rule
                    // partly, so this is every pattern the rule was written with.
                    WatchMode::All => {
                        let required = self
                            .fields
                            .iter()
                            .find(|candidate| candidate.field == field)
                            .and_then(|candidate| candidate.counts.get(&row).copied())?;
                        if positions.len() < required {
                            return None;
                        }
                        WatchHit::All(required)
                    }
                };
                Some(WatchMatch {
                    name: record.name.clone(),
                    hit,
                    reason: record.reason.clone(),
                    field,
                })
            })
            .collect()
    }
}

/// What a watch on `field` reads from `message`.
///
/// A sender is two strings because the platforms disagree about which one a person is known by, and
/// a rule written for either should fire. Both are checked against the same pattern rather than the
/// two being joined, so a pattern anchored with `^` still means what it says.
fn haystacks(field: WatchField, message: &InboundMessage) -> Vec<&str> {
    match field {
        WatchField::Text => vec![message.text.as_str()],
        WatchField::Sender => {
            let mut names = vec![message.sender.display_name.as_str()];
            if let Some(username) = &message.sender.username {
                names.push(username.as_str());
            }
            names
        }
        WatchField::SenderId => vec![message.sender.id.as_str()],
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;
    use crate::channel::{
        Admission, ChannelId, ChatKind, ConversationId, InboundMessage, Platform, Sender,
    };

    fn watch(name: &str, field: WatchField, patterns: &[&str]) -> WatchRecord {
        WatchRecord {
            id: 0,
            name: name.to_string(),
            conversation: None,
            field,
            mode: WatchMode::Any,
            patterns: patterns.iter().map(|p| (*p).to_string()).collect(),
            reason: None,
            until: None,
            created_at: Utc::now(),
        }
    }

    fn all_of(name: &str, field: WatchField, patterns: &[&str]) -> WatchRecord {
        WatchRecord {
            mode: WatchMode::All,
            ..watch(name, field, patterns)
        }
    }

    fn message(conversation: &str, text: &str, name: &str, sender_id: &str) -> InboundMessage {
        InboundMessage {
            channel: ChannelId::new("telegram"),
            platform: Platform::Telegram,
            conversation: ConversationId::parse(conversation).expect("valid"),
            message_id: "1".to_string(),
            chat_kind: ChatKind::Group,
            chat_title: None,
            sender: Sender {
                id: sender_id.to_string(),
                display_name: name.to_string(),
                username: Some("handle".to_string()),
                is_bot: false,
                on_behalf_of_chat: false,
            },
            admission: Admission::Chat,
            sender_allowlisted: false,
            addressed: false,
            matches: Vec::new(),
            sender_roles: Vec::new(),
            text: text.to_string(),
            reply_to: None,
            edited_at: None,
            forwarded_from: None,
            group_id: None,
            notes: Vec::new(),
            attachments: Vec::new(),
            timestamp: Utc::now(),
        }
    }

    #[test]
    fn a_pattern_is_matched_against_the_field_it_names_and_no_other() {
        // The whole reason a watch names one field: the same word means different things in a
        // message and in a name, and a rule written for one firing on the other is a false wake
        // the agent cannot explain.
        let list = Watchlist::new(vec![
            watch("rule-1", WatchField::Text, &["deploy"]),
            watch("rule-2", WatchField::Sender, &["deploy"]),
        ]);
        let in_text = list.matches(&message("telegram:-100", "deploy is stuck", "Alice", "7"));
        assert_eq!(in_text.len(), 1, "got {in_text:?}");
        assert_eq!(in_text[0].name, "rule-1");
        assert_eq!(in_text[0].field, WatchField::Text);

        let in_name = list.matches(&message("telegram:-100", "hello", "Deploy Bot", "7"));
        assert_eq!(in_name.len(), 1, "got {in_name:?}");
        assert_eq!(in_name[0].name, "rule-2");
    }

    #[test]
    fn a_sender_watch_reads_the_username_as_well_as_the_display_name() {
        // Which of the two a person is known by differs per platform and per person, so a rule
        // written for either has to fire.
        let list = Watchlist::new(vec![watch("rule-1", WatchField::Sender, &["^handle$"])]);
        let hit = list.matches(&message("telegram:-100", "hello", "Someone Else", "7"));
        assert_eq!(
            hit.len(),
            1,
            "an anchored pattern must still see the username: {hit:?}"
        );
    }

    #[test]
    fn matching_ignores_case_unless_the_pattern_says_otherwise() {
        // The agent is writing something closer to a keyword than to a parser, and a rule that
        // misses on a capital letter is a rule that looks like it works.
        let list = Watchlist::new(vec![watch("rule-1", WatchField::Text, &["deploy"])]);
        assert_eq!(
            list.matches(&message("telegram:-100", "DEPLOY", "A", "7"))
                .len(),
            1
        );

        let exact = Watchlist::new(vec![watch("rule-1", WatchField::Text, &["(?-i)deploy"])]);
        assert!(
            exact
                .matches(&message("telegram:-100", "DEPLOY", "A", "7"))
                .is_empty(),
            "a pattern that asks for case sensitivity has to get it"
        );
    }

    #[test]
    fn a_scoped_watch_fires_only_in_its_own_conversation() {
        let mut scoped = watch("rule-1", WatchField::Text, &["deploy"]);
        scoped.conversation = Some("telegram:-100".to_string());
        let list = Watchlist::new(vec![scoped]);
        assert_eq!(
            list.matches(&message("telegram:-100", "deploy", "A", "7"))
                .len(),
            1
        );
        assert!(
            list.matches(&message("telegram:-200", "deploy", "A", "7"))
                .is_empty(),
            "a watch confined to one room must not wake the agent in another"
        );
    }

    #[test]
    fn every_matching_watch_is_reported_in_the_order_it_was_created() {
        // The agent is told which rules fired so it can retune them, and being told about one of
        // three would send it removing a rule that was not the noisy one.
        let list = Watchlist::new(vec![
            watch("rule-3", WatchField::Text, &["deploy"]),
            watch("rule-7", WatchField::Text, &["stuck"]),
            watch("rule-9", WatchField::SenderId, &["^7$"]),
        ]);
        let hits = list.matches(&message("telegram:-100", "deploy is stuck", "A", "7"));
        assert_eq!(
            hits.iter().map(|hit| hit.name.as_str()).collect::<Vec<_>>(),
            vec!["rule-3", "rule-7", "rule-9"]
        );
    }

    #[test]
    fn an_any_watch_fires_on_one_pattern_and_reports_which() {
        // The common rule: a list of spellings for one thing. The agent is told which spelling hit
        // so it can narrow the rule without reading the whole list back.
        let list = Watchlist::new(vec![watch("spam", WatchField::Text, &[
            "看我简介",
            "跑分",
            "日入过万",
        ])]);
        let hits = list.matches(&message("telegram:-100", "来跑分吧", "A", "7"));
        assert_eq!(
            hits.len(),
            1,
            "one rule fired, not one per pattern: {hits:?}"
        );
        assert_eq!(hits[0].name, "spam");
        assert_eq!(hits[0].hit, WatchHit::Pattern("跑分".to_string()));
    }

    #[test]
    fn an_any_watch_hit_by_several_patterns_reports_the_first_in_the_rule() {
        // Arbitrary either way, so it is the rule's own order: that is what somebody reading the
        // rule scans, and it does not move when the message changes.
        let list = Watchlist::new(vec![watch("spam", WatchField::Text, &["跑分", "日入过万"])]);
        let hits = list.matches(&message("telegram:-100", "日入过万 跑分", "A", "7"));
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].hit, WatchHit::Pattern("跑分".to_string()));
    }

    #[test]
    fn an_all_watch_waits_for_every_term() {
        // What replaces the lookahead the engine refuses: "these terms, in any order, anywhere".
        let list = Watchlist::new(vec![all_of("pump", WatchField::Text, &[
            "购买", "止盈", "止损",
        ])]);
        assert!(
            list.matches(&message("telegram:-100", "购买 止盈", "A", "7"))
                .is_empty(),
            "two of three is not the rule"
        );
        let hits = list.matches(&message(
            "telegram:-100",
            "止损 先 购买 然后 止盈",
            "A",
            "7",
        ));
        assert_eq!(hits.len(), 1, "order must not matter: {hits:?}");
        assert_eq!(hits[0].hit, WatchHit::All(3));
    }

    #[test]
    fn an_all_watch_on_a_sender_may_take_its_terms_from_either_string() {
        // A sender is a display name and a username, and `all` asks that every pattern match the
        // field rather than that all of them match the same string. Worth pinning down, because
        // the other reading is just as defensible and the two differ exactly here.
        let list = Watchlist::new(vec![all_of("split", WatchField::Sender, &[
            "^Free", "handle$",
        ])]);
        let hits = list.matches(&message("telegram:-100", "hello", "Free Money", "7"));
        assert_eq!(hits.len(), 1, "got {hits:?}");
        assert_eq!(hits[0].hit, WatchHit::All(2));
    }

    #[test]
    fn a_broken_pattern_disables_an_all_watch_rather_than_loosening_it() {
        // Skipping a term of an `all` rule would leave a rule that fires on strictly more than it
        // was written to, which is the one direction a silent degradation must not go. Under
        // `any` the same skip only costs one of several ways to fire.
        let loosened = Watchlist::new(vec![all_of("pump", WatchField::Text, &[
            "(unclosed",
            "止盈",
        ])]);
        assert!(
            loosened
                .matches(&message("telegram:-100", "止盈", "A", "7"))
                .is_empty(),
            "the surviving term must not fire the rule on its own"
        );

        let narrowed = Watchlist::new(vec![watch("spam", WatchField::Text, &[
            "(unclosed",
            "止盈",
        ])]);
        assert_eq!(
            narrowed
                .matches(&message("telegram:-100", "止盈", "A", "7"))
                .len(),
            1,
            "an `any` rule keeps the patterns that still compile"
        );
    }

    #[test]
    fn a_match_recorded_by_the_previous_release_still_decodes() {
        // A message can sit in the queue across an upgrade, and its payload was written when a
        // match was a numeric id and one pattern. Refusing to decode it would strand the message
        // rather than the field, so the two fields that changed carry defaults.
        let queued = serde_json::json!({
            "id": 12,
            "pattern": "看我简介",
            "reason": "spam signature",
            "field": "text"
        });
        let decoded: WatchMatch = serde_json::from_value(queued).expect("an old item must decode");
        assert_eq!(decoded.field, WatchField::Text);
        assert_eq!(decoded.reason.as_deref(), Some("spam signature"));
        assert!(decoded.name.is_empty(), "there was no name to recover");
        assert_eq!(decoded.hit, WatchHit::All(0));
    }

    #[test]
    fn a_match_round_trips_through_the_queue_payload() {
        let original = WatchMatch {
            name: "spam-signatures".to_string(),
            hit: WatchHit::Pattern("跑分".to_string()),
            reason: Some("spam".to_string()),
            field: WatchField::Sender,
        };
        let encoded = serde_json::to_string(&original).expect("encodes");
        let decoded: WatchMatch = serde_json::from_str(&encoded).expect("decodes");
        assert_eq!(decoded, original);
    }

    #[test]
    fn a_name_has_to_be_one_usable_line() {
        assert!(validate_name("spam-signatures").is_ok());
        assert!(
            validate_name("").is_err(),
            "an empty name identifies nothing"
        );
        assert!(
            validate_name(" padded").is_err(),
            "a name is what the agent types back"
        );
        assert!(
            validate_name("two\nlines").is_err(),
            "the name is printed on a header line"
        );
        assert!(validate_name(&"n".repeat(MAX_NAME_CHARS + 1)).is_err());
    }

    #[test]
    fn a_pattern_that_no_longer_compiles_costs_only_itself() {
        // Every pattern was compiled before it was stored, so one that will not compile now means
        // a hand-edited database. Failing the whole list over it would turn one bad row into a
        // bridge that silently stops watching for anything.
        let list = Watchlist::new(vec![
            watch("rule-1", WatchField::Text, &["(unclosed"]),
            watch("rule-2", WatchField::Text, &["deploy"]),
        ]);
        let hits = list.matches(&message("telegram:-100", "deploy", "A", "7"));
        assert_eq!(hits.len(), 1, "got {hits:?}");
        assert_eq!(hits[0].name, "rule-2");
    }

    #[test]
    fn nothing_matches_an_empty_field() {
        // A photo with no caption has no text, and a channel post has no sender id. Neither should
        // be matched by a pattern that happens to accept the empty string, because the agent meant
        // "a message that says this" rather than "a message with nothing in it".
        let list = Watchlist::new(vec![watch("rule-1", WatchField::Text, &["x*"])]);
        assert!(
            list.matches(&message("telegram:-100", "", "A", "7"))
                .is_empty()
        );
    }

    #[test]
    fn an_unusable_pattern_is_refused_with_the_reason() {
        // The text goes to whoever wrote the pattern. `regex` explains an unclosed group far
        // better than "invalid pattern" does, and the explanation is what they can act on.
        let error = compile_pattern("(unclosed").expect_err("must be refused");
        assert!(error.contains("unclosed group"), "got: {error}");
        assert!(
            compile_pattern("   ").is_err(),
            "an empty pattern matches everything"
        );

        let long = "a".repeat(MAX_PATTERN_CHARS + 1);
        let error = compile_pattern(&long).expect_err("must be refused");
        assert!(
            error.contains(&MAX_PATTERN_CHARS.to_string()),
            "got: {error}"
        );
    }

    #[test]
    fn an_empty_list_is_reported_as_empty_rather_than_matching_nothing_slowly() {
        // The gate checks this before doing any work, which is what keeps a deployment that has
        // never set a watch from paying for the feature on every message.
        assert!(Watchlist::new(Vec::new()).is_empty());
        assert!(!Watchlist::new(vec![watch("rule-1", WatchField::Text, &["x"])]).is_empty());
    }
}
