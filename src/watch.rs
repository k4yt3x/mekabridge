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

/// One watch that fired on one message.
///
/// Carried on the message through the queue, so the item the agent reads can name the rule that
/// woke it. Without that the agent is handed a message from a muted room with no way to tell which
/// of its own rules is responsible, which is the one thing it needs in order to retune them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatchMatch {
    pub id: i64,
    pub pattern: String,
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

/// One field's patterns, and which row each came from.
struct FieldSet {
    field: WatchField,
    set: RegexSet,
    /// Index into [`Watchlist::rows`] for each pattern in `set`, in the same order.
    rows: Vec<usize>,
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
            for (index, row) in rows.iter().enumerate() {
                if row.field != field {
                    continue;
                }
                match compile_pattern(&row.pattern) {
                    Ok(_) => {
                        patterns.push(row.pattern.clone());
                        sources.push(index);
                    }
                    Err(error) => tracing::error!(
                        watch = row.id,
                        pattern = %row.pattern,
                        "a stored watch pattern does not compile and is being ignored: {}",
                        error
                    ),
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
                    rows: sources,
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
        let mut hits: Vec<(usize, WatchField)> = Vec::new();
        for field in &self.fields {
            for haystack in haystacks(field.field, message) {
                if haystack.is_empty() {
                    continue;
                }
                for position in field.set.matches(haystack).into_iter() {
                    let Some(&row) = field.rows.get(position) else {
                        continue;
                    };
                    if !hits.iter().any(|(existing, _)| *existing == row) {
                        hits.push((row, field.field));
                    }
                }
            }
        }
        hits.sort_unstable_by_key(|(row, _)| *row);
        hits.into_iter()
            .filter_map(|(row, field)| {
                let record = self.rows.get(row)?;
                let scoped_elsewhere = record
                    .conversation
                    .as_deref()
                    .is_some_and(|address| address != message.conversation.as_str());
                if scoped_elsewhere {
                    return None;
                }
                Some(WatchMatch {
                    id: record.id,
                    pattern: record.pattern.clone(),
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

    fn watch(id: i64, field: WatchField, pattern: &str) -> WatchRecord {
        WatchRecord {
            id,
            conversation: None,
            field,
            pattern: pattern.to_string(),
            reason: None,
            until: None,
            created_at: Utc::now(),
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
            watch(1, WatchField::Text, "deploy"),
            watch(2, WatchField::Sender, "deploy"),
        ]);
        let in_text = list.matches(&message("telegram:-100", "deploy is stuck", "Alice", "7"));
        assert_eq!(in_text.len(), 1, "got {in_text:?}");
        assert_eq!(in_text[0].id, 1);
        assert_eq!(in_text[0].field, WatchField::Text);

        let in_name = list.matches(&message("telegram:-100", "hello", "Deploy Bot", "7"));
        assert_eq!(in_name.len(), 1, "got {in_name:?}");
        assert_eq!(in_name[0].id, 2);
    }

    #[test]
    fn a_sender_watch_reads_the_username_as_well_as_the_display_name() {
        // Which of the two a person is known by differs per platform and per person, so a rule
        // written for either has to fire.
        let list = Watchlist::new(vec![watch(1, WatchField::Sender, "^handle$")]);
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
        let list = Watchlist::new(vec![watch(1, WatchField::Text, "deploy")]);
        assert_eq!(
            list.matches(&message("telegram:-100", "DEPLOY", "A", "7"))
                .len(),
            1
        );

        let exact = Watchlist::new(vec![watch(1, WatchField::Text, "(?-i)deploy")]);
        assert!(
            exact
                .matches(&message("telegram:-100", "DEPLOY", "A", "7"))
                .is_empty(),
            "a pattern that asks for case sensitivity has to get it"
        );
    }

    #[test]
    fn a_scoped_watch_fires_only_in_its_own_conversation() {
        let mut scoped = watch(1, WatchField::Text, "deploy");
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
            watch(3, WatchField::Text, "deploy"),
            watch(7, WatchField::Text, "stuck"),
            watch(9, WatchField::SenderId, "^7$"),
        ]);
        let hits = list.matches(&message("telegram:-100", "deploy is stuck", "A", "7"));
        assert_eq!(hits.iter().map(|hit| hit.id).collect::<Vec<_>>(), vec![
            3, 7, 9
        ]);
    }

    #[test]
    fn a_pattern_that_no_longer_compiles_costs_only_itself() {
        // Every pattern was compiled before it was stored, so one that will not compile now means
        // a hand-edited database. Failing the whole list over it would turn one bad row into a
        // bridge that silently stops watching for anything.
        let list = Watchlist::new(vec![
            watch(1, WatchField::Text, "(unclosed"),
            watch(2, WatchField::Text, "deploy"),
        ]);
        let hits = list.matches(&message("telegram:-100", "deploy", "A", "7"));
        assert_eq!(hits.len(), 1, "got {hits:?}");
        assert_eq!(hits[0].id, 2);
    }

    #[test]
    fn nothing_matches_an_empty_field() {
        // A photo with no caption has no text, and a channel post has no sender id. Neither should
        // be matched by a pattern that happens to accept the empty string, because the agent meant
        // "a message that says this" rather than "a message with nothing in it".
        let list = Watchlist::new(vec![watch(1, WatchField::Text, "x*")]);
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
        assert!(!Watchlist::new(vec![watch(1, WatchField::Text, "x")]).is_empty());
    }
}
