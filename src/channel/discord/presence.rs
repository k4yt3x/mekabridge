//! Who is online, accumulated from the gateway.
//!
//! Presence is the one thing Discord will not answer over HTTP: there is no endpoint to ask whether
//! somebody is around, only a stream of updates to keep up with. So this is a running tally rather
//! than a lookup, and everything awkward about it follows from that.
//!
//! **Only status is kept.** Discord sends what somebody is playing, listening to, and their custom
//! status alongside it. None of that is stored, logged, or exposed. The bridge ingests presence for
//! every member of every server it is in, involuntarily on their part, and the narrowest thing that
//! answers "can I give this person work" is the status alone.
//!
//! **Absence is not offline.** A member the cache has never heard of reads as
//! [`PresenceStatus::Unknown`], never as offline. In the seconds after startup that is every
//! member, and reporting a server full of offline people would be a confident lie at exactly the
//! moment the bridge knows least.
//!
//! **A reconnect replaces a guild's entry rather than merging into it.** Updates missed while the
//! gateway was away are unrecoverable, so anything still in the map afterwards is a claim nobody
//! checked. Merging would leave whoever went offline during the gap looking permanently available.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use twilight_model::{
    gateway::presence::Status as TwilightStatus,
    id::{
        Id,
        marker::{GuildMarker, UserMarker},
    },
};

use crate::channel::{Presence, PresenceStatus};

/// Per-guild presence, replaced wholesale whenever the guild is re-seeded.
#[derive(Debug, Default)]
struct GuildPresence {
    members: HashMap<Id<UserMarker>, PresenceStatus>,
    /// When this guild was last seeded or updated, which is what makes a stale answer legible as
    /// stale rather than passing for current.
    as_of: Option<DateTime<Utc>>,
}

/// Online status by guild and member.
#[derive(Debug, Default)]
pub struct PresenceCache {
    guilds: std::sync::RwLock<HashMap<Id<GuildMarker>, GuildPresence>>,
}

impl PresenceCache {
    /// Replace everything known about one guild.
    ///
    /// Called on `GUILD_CREATE`, which arrives both on first connect and again after a resume that
    /// could not be continued. Replacing rather than merging is the point: see the module note.
    pub fn seed(
        &self,
        guild_id: Id<GuildMarker>,
        presences: impl IntoIterator<Item = (Id<UserMarker>, TwilightStatus)>,
        now: DateTime<Utc>,
    ) {
        let members = presences
            .into_iter()
            .map(|(user, status)| (user, translate(status)))
            .collect();
        let mut guilds = self.write();
        guilds.insert(guild_id, GuildPresence {
            members,
            as_of: Some(now),
        });
    }

    /// Apply one `PRESENCE_UPDATE`.
    ///
    /// Ignored for a guild that has not been seeded. An update on its own says nothing about the
    /// rest of the server, and a map holding one person would otherwise report everybody else as
    /// unknown while looking, from `as_of`, freshly populated.
    pub fn update(
        &self,
        guild_id: Id<GuildMarker>,
        user: Id<UserMarker>,
        status: TwilightStatus,
        now: DateTime<Utc>,
    ) {
        let mut guilds = self.write();
        let Some(guild) = guilds.get_mut(&guild_id) else {
            return;
        };
        guild.members.insert(user, translate(status));
        guild.as_of = Some(now);
    }

    /// Forget a guild the bot is no longer in.
    pub fn forget(&self, guild_id: Id<GuildMarker>) {
        self.write().remove(&guild_id);
    }

    /// What is known about one member, which may be that nothing is.
    pub fn get(&self, guild_id: Id<GuildMarker>, user: Id<UserMarker>) -> Presence {
        let guilds = self.read();
        let Some(guild) = guilds.get(&guild_id) else {
            return Presence::unknown();
        };
        Presence {
            // Seeded guilds omit anyone offline, so a member who is present in the server but
            // absent from the map is offline rather than unknown. The distinction only holds
            // because the guild itself has been seen.
            status: guild
                .members
                .get(&user)
                .copied()
                .unwrap_or(PresenceStatus::Offline),
            as_of: guild.as_of,
        }
    }

    /// Whether a guild has been seeded at all.
    ///
    /// This is the line between "nothing is known" and "nobody is online", and the two are reported
    /// differently, so it is worth being able to assert on directly.
    pub fn is_primed(&self, guild_id: Id<GuildMarker>) -> bool {
        self.read()
            .get(&guild_id)
            .is_some_and(|guild| guild.as_of.is_some())
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, HashMap<Id<GuildMarker>, GuildPresence>> {
        self.guilds
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, HashMap<Id<GuildMarker>, GuildPresence>> {
        self.guilds
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Discord's status strings, narrowed to what the bridge reports.
///
/// `Invisible` is deliberately reported as offline. It is what the person chose to appear as, and
/// the bridge relaying "actually they are around" would defeat a privacy setting on somebody who
/// never agreed to be watched by a bot.
const fn translate(status: TwilightStatus) -> PresenceStatus {
    match status {
        TwilightStatus::Online => PresenceStatus::Online,
        TwilightStatus::Idle => PresenceStatus::Idle,
        TwilightStatus::DoNotDisturb => PresenceStatus::DoNotDisturb,
        TwilightStatus::Offline | TwilightStatus::Invisible => PresenceStatus::Offline,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn guild() -> Id<GuildMarker> {
        Id::new(1)
    }

    fn user(id: u64) -> Id<UserMarker> {
        Id::new(id)
    }

    #[test]
    fn an_unseeded_guild_reports_unknown_rather_than_offline() {
        // The failure this exists to stop: moments after startup nothing has been seeded, and
        // "everybody is offline" would send an agent looking for somebody to assign work to away
        // empty-handed, confidently and wrongly.
        let cache = PresenceCache::default();
        let presence = cache.get(guild(), user(7));
        assert_eq!(presence.status, PresenceStatus::Unknown);
        assert!(presence.as_of.is_none());
        assert!(!cache.is_primed(guild()));
    }

    #[test]
    fn a_seeded_guild_reports_a_missing_member_as_offline() {
        // Discord omits offline members from the seed, so absence within a known guild is real
        // information, unlike absence of the guild itself.
        let cache = PresenceCache::default();
        cache.seed(guild(), [(user(1), TwilightStatus::Online)], Utc::now());
        assert_eq!(cache.get(guild(), user(1)).status, PresenceStatus::Online);
        assert_eq!(cache.get(guild(), user(2)).status, PresenceStatus::Offline);
        assert!(cache.is_primed(guild()));
    }

    #[test]
    fn reseeding_replaces_rather_than_merges() {
        // A reconnect loses whatever happened while the gateway was away. Anyone carried over from
        // the previous seed would be a claim nobody rechecked, and someone who went offline during
        // the gap would read as available forever.
        let cache = PresenceCache::default();
        cache.seed(
            guild(),
            [
                (user(1), TwilightStatus::Online),
                (user(2), TwilightStatus::Online),
            ],
            Utc::now(),
        );
        cache.seed(guild(), [(user(1), TwilightStatus::Idle)], Utc::now());

        assert_eq!(cache.get(guild(), user(1)).status, PresenceStatus::Idle);
        assert_eq!(
            cache.get(guild(), user(2)).status,
            PresenceStatus::Offline,
            "a member dropped by the reseed must not survive from the previous one"
        );
    }

    #[test]
    fn an_update_before_the_seed_is_ignored() {
        // One person's update says nothing about the rest of the server. Accepting it would stamp
        // `as_of`, making a map of exactly one member look freshly complete.
        let cache = PresenceCache::default();
        cache.update(guild(), user(1), TwilightStatus::Online, Utc::now());
        assert_eq!(cache.get(guild(), user(1)).status, PresenceStatus::Unknown);
        assert!(!cache.is_primed(guild()));
    }

    #[test]
    fn going_offline_is_recorded_rather_than_dropped() {
        let cache = PresenceCache::default();
        cache.seed(guild(), [(user(1), TwilightStatus::Online)], Utc::now());
        cache.update(guild(), user(1), TwilightStatus::Offline, Utc::now());
        assert_eq!(cache.get(guild(), user(1)).status, PresenceStatus::Offline);
    }

    #[test]
    fn invisible_is_honoured_as_offline() {
        // Somebody who set themselves invisible chose that. Reporting them as around would defeat
        // the setting for the benefit of a bot they never opted into.
        let cache = PresenceCache::default();
        cache.seed(guild(), [(user(1), TwilightStatus::Invisible)], Utc::now());
        assert_eq!(cache.get(guild(), user(1)).status, PresenceStatus::Offline);
    }

    #[test]
    fn a_forgotten_guild_goes_back_to_unknown() {
        let cache = PresenceCache::default();
        cache.seed(guild(), [(user(1), TwilightStatus::Online)], Utc::now());
        cache.forget(guild());
        assert_eq!(cache.get(guild(), user(1)).status, PresenceStatus::Unknown);
    }
}
