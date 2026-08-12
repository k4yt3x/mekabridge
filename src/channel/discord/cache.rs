//! Guild, channel, and role names, kept from the gateway's own events.
//!
//! A Discord conversation id is a bare channel snowflake, which tells the agent nothing about where
//! it is, and raw message content refers to people, roles, and channels by id. Both need names, and
//! fetching them over REST per message would be several round trips for every line of chat.
//!
//! The gateway supplies them free. With the `GUILDS` intent, connecting delivers one `GUILD_CREATE`
//! per server carrying its name, every channel, and every role, and the create/update/delete events
//! keep that current afterwards. So this is not a cache in the sense of something that can be stale
//! and needs invalidating: it is the gateway's own state, mirrored.
//!
//! Deliberately holds nothing about people. Member names come from the message that mentions them,
//! which is always present, and keeping a member list would need a second privileged intent for
//! data the bridge already has.

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use twilight_model::{
    channel::{Channel, ChannelType},
    guild::{Guild, Role},
    id::{
        Id,
        marker::{ChannelMarker, GuildMarker, RoleMarker},
    },
};

/// What is known about one server.
#[derive(Debug, Default)]
struct GuildEntry {
    name: String,
    roles: HashMap<Id<RoleMarker>, String>,
}

/// What is known about one channel, thread, or forum post.
#[derive(Debug, Clone)]
struct ChannelEntry {
    /// Absent for direct messages, which have no name of their own.
    name: Option<String>,
    guild: Option<Id<GuildMarker>>,
    /// The channel a thread hangs off, so a forum post can name the forum it is in.
    parent: Option<Id<ChannelMarker>>,
    kind: ChannelType,
}

/// The gateway's view of every server the bot is in.
#[derive(Debug, Default)]
pub struct NameCache {
    guilds: RwLock<HashMap<Id<GuildMarker>, GuildEntry>>,
    channels: RwLock<HashMap<Id<ChannelMarker>, ChannelEntry>>,
}

impl NameCache {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Take everything a `GUILD_CREATE` carries.
    pub fn insert_guild(&self, guild: &Guild) {
        if let Ok(mut guilds) = self.guilds.write() {
            guilds.insert(guild.id, GuildEntry {
                name: guild.name.clone(),
                roles: guild
                    .roles
                    .iter()
                    .map(|role| (role.id, role.name.clone()))
                    .collect(),
            });
        }
        for channel in guild.channels.iter().chain(guild.threads.iter()) {
            self.insert_channel(channel);
        }
    }

    /// Forget a server the bot was removed from, and every channel that belonged to it.
    pub fn remove_guild(&self, guild_id: Id<GuildMarker>) {
        if let Ok(mut guilds) = self.guilds.write() {
            guilds.remove(&guild_id);
        }
        if let Ok(mut channels) = self.channels.write() {
            channels.retain(|_, entry| entry.guild != Some(guild_id));
        }
    }

    /// Rename a server, keeping its roles.
    pub fn rename_guild(&self, guild_id: Id<GuildMarker>, name: &str) {
        if let Ok(mut guilds) = self.guilds.write() {
            guilds.entry(guild_id).or_default().name = name.to_string();
        }
    }

    pub fn insert_role(&self, guild_id: Id<GuildMarker>, role: &Role) {
        if let Ok(mut guilds) = self.guilds.write() {
            guilds
                .entry(guild_id)
                .or_default()
                .roles
                .insert(role.id, role.name.clone());
        }
    }

    pub fn remove_role(&self, guild_id: Id<GuildMarker>, role_id: Id<RoleMarker>) {
        if let Ok(mut guilds) = self.guilds.write()
            && let Some(guild) = guilds.get_mut(&guild_id)
        {
            guild.roles.remove(&role_id);
        }
    }

    pub fn insert_channel(&self, channel: &Channel) {
        if let Ok(mut channels) = self.channels.write() {
            channels.insert(channel.id, ChannelEntry {
                name: channel.name.clone(),
                guild: channel.guild_id,
                parent: channel.parent_id,
                kind: channel.kind,
            });
        }
    }

    pub fn remove_channel(&self, channel_id: Id<ChannelMarker>) {
        if let Ok(mut channels) = self.channels.write() {
            channels.remove(&channel_id);
        }
    }

    /// Which server a channel belongs to, or `None` for a direct message.
    pub fn guild_of(&self, channel_id: Id<ChannelMarker>) -> Option<Id<GuildMarker>> {
        let channels = self.channels.read().ok()?;
        channels.get(&channel_id).and_then(|entry| entry.guild)
    }

    /// What shape of room a channel is.
    ///
    /// The released gateway payload does not carry `channel_type` on a message, so this is where a
    /// direct message is told apart from a server channel.
    pub fn kind_of(&self, channel_id: Id<ChannelMarker>) -> Option<ChannelType> {
        let channels = self.channels.read().ok()?;
        channels.get(&channel_id).map(|entry| entry.kind)
    }

    /// The channel a thread hangs off, so a thread can inherit its parent's allowlist standing.
    pub fn parent_of(&self, channel_id: Id<ChannelMarker>) -> Option<Id<ChannelMarker>> {
        let channels = self.channels.read().ok()?;
        let entry = channels.get(&channel_id)?;
        is_thread(entry.kind).then_some(entry.parent).flatten()
    }

    pub fn guild_name(&self, guild_id: Id<GuildMarker>) -> Option<String> {
        let guilds = self.guilds.read().ok()?;
        guilds.get(&guild_id).map(|guild| guild.name.clone())
    }

    pub fn role_name(&self, guild_id: Id<GuildMarker>, role_id: Id<RoleMarker>) -> Option<String> {
        let guilds = self.guilds.read().ok()?;
        guilds
            .get(&guild_id)
            .and_then(|guild| guild.roles.get(&role_id))
            .cloned()
    }

    /// Names for a member's roles, in the order the member object listed them.
    ///
    /// A role the cache has never heard of is skipped rather than rendered as its id: an id in a
    /// list of names reads as though somebody is called `847362…`.
    pub fn role_names(&self, guild_id: Id<GuildMarker>, roles: &[Id<RoleMarker>]) -> Vec<String> {
        let Ok(guilds) = self.guilds.read() else {
            return Vec::new();
        };
        let Some(guild) = guilds.get(&guild_id) else {
            return Vec::new();
        };
        roles
            .iter()
            .filter_map(|role| guild.roles.get(role).cloned())
            .collect()
    }

    /// A role id looked up by name, case insensitively.
    ///
    /// The reverse direction, for `set_member_roles`, which takes the names the agent was shown
    /// rather than ids it was never given.
    pub fn role_by_name(&self, guild_id: Id<GuildMarker>, name: &str) -> Option<Id<RoleMarker>> {
        let guilds = self.guilds.read().ok()?;
        guilds.get(&guild_id).and_then(|guild| {
            guild
                .roles
                .iter()
                .find(|(_, role_name)| role_name.eq_ignore_ascii_case(name))
                .map(|(id, _)| *id)
        })
    }

    /// Every role name in a server, for telling the agent what it could have asked for.
    pub fn role_catalogue(&self, guild_id: Id<GuildMarker>) -> Vec<String> {
        let Ok(guilds) = self.guilds.read() else {
            return Vec::new();
        };
        let mut names: Vec<String> = guilds
            .get(&guild_id)
            .map(|guild| guild.roles.values().cloned().collect())
            .unwrap_or_default();
        names.sort();
        names
    }

    /// A channel's own name with its `#`, or `None` for a direct message.
    pub fn channel_name(&self, channel_id: Id<ChannelMarker>) -> Option<String> {
        let channels = self.channels.read().ok()?;
        let entry = channels.get(&channel_id)?;
        entry.name.as_ref().map(|name| format!("#{name}"))
    }

    /// Where a conversation is, as a person would say it.
    ///
    /// `#deploys in Acme Corp` for a channel, `#deploys › rollback tonight in Acme Corp` for a
    /// thread, and `None` for a direct message or anything the cache has not been told about, so
    /// the caller can say nothing rather than something wrong.
    pub fn describe(&self, channel_id: Id<ChannelMarker>) -> Option<String> {
        let (located, guild) = {
            let channels = self.channels.read().ok()?;
            let entry = channels.get(&channel_id)?;
            let name = entry.name.clone()?;
            let located = match entry.parent.and_then(|parent| channels.get(&parent)) {
                // A thread's own name is the topic; the parent names the room it was started in.
                Some(parent) if is_thread(entry.kind) => match &parent.name {
                    Some(parent_name) => format!("#{parent_name} \u{203a} {name}"),
                    None => format!("#{name}"),
                },
                _ => format!("#{name}"),
            };
            (located, entry.guild)
        };
        match guild.and_then(|guild| self.guild_name(guild)) {
            Some(guild) => Some(format!("{located} in {guild}")),
            None => Some(located),
        }
    }

    /// How many servers and channels are known, for `doctor`.
    pub fn size(&self) -> (usize, usize) {
        let guilds = self.guilds.read().map(|guilds| guilds.len()).unwrap_or(0);
        let channels = self
            .channels
            .read()
            .map(|channels| channels.len())
            .unwrap_or(0);
        (guilds, channels)
    }
}

const fn is_thread(kind: ChannelType) -> bool {
    matches!(
        kind,
        ChannelType::AnnouncementThread | ChannelType::PublicThread | ChannelType::PrivateThread
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn channel(id: u64, name: &str, kind: ChannelType) -> Channel {
        let mut channel = blank_channel(id);
        channel.name = Some(name.to_string());
        channel.kind = kind;
        channel.guild_id = Some(Id::new(900));
        channel
    }

    fn blank_channel(id: u64) -> Channel {
        serde_json::from_value(serde_json::json!({
            "id": id.to_string(),
            "type": 0,
        }))
        .expect("a channel with only the required fields deserializes")
    }

    fn cache_with_guild() -> Arc<NameCache> {
        let cache = NameCache::new();
        cache.rename_guild(Id::new(900), "Acme Corp");
        cache
    }

    #[test]
    fn a_channel_is_described_with_its_server() {
        let cache = cache_with_guild();
        cache.insert_channel(&channel(1, "deploys", ChannelType::GuildText));
        assert_eq!(
            cache.describe(Id::new(1)).as_deref(),
            Some("#deploys in Acme Corp")
        );
    }

    #[test]
    fn a_thread_names_the_channel_it_hangs_off() {
        let cache = cache_with_guild();
        cache.insert_channel(&channel(1, "deploys", ChannelType::GuildText));
        let mut thread = channel(2, "rollback tonight", ChannelType::PublicThread);
        thread.parent_id = Some(Id::new(1));
        cache.insert_channel(&thread);
        assert_eq!(
            cache.describe(Id::new(2)).as_deref(),
            Some("#deploys \u{203a} rollback tonight in Acme Corp")
        );
    }

    #[test]
    fn an_unknown_channel_is_described_as_nothing_rather_than_guessed() {
        let cache = cache_with_guild();
        assert_eq!(cache.describe(Id::new(7)), None);
    }

    #[test]
    fn leaving_a_server_forgets_its_channels_too() {
        let cache = cache_with_guild();
        cache.insert_channel(&channel(1, "deploys", ChannelType::GuildText));
        cache.remove_guild(Id::new(900));
        assert_eq!(cache.describe(Id::new(1)), None);
        assert_eq!(cache.size(), (0, 0));
    }

    #[test]
    fn a_role_the_cache_has_never_seen_is_skipped_rather_than_shown_as_an_id() {
        let cache = cache_with_guild();
        let role: Role = serde_json::from_value(serde_json::json!({
            "id": "555",
            "name": "Moderators",
            "color": 0,
            "colors": {"primary_color": 0},
            "flags": 0,
            "hoist": false,
            "managed": false,
            "mentionable": true,
            "permissions": "0",
            "position": 1,
        }))
        .expect("a role with only the required fields deserializes");
        cache.insert_role(Id::new(900), &role);
        let names = cache.role_names(Id::new(900), &[Id::new(555), Id::new(556)]);
        assert_eq!(names, vec!["Moderators".to_string()]);
    }

    #[test]
    fn a_role_can_be_found_back_from_the_name_the_agent_was_shown() {
        let cache = cache_with_guild();
        let role: Role = serde_json::from_value(serde_json::json!({
            "id": "555",
            "name": "Release Team",
            "color": 0,
            "colors": {"primary_color": 0},
            "flags": 0,
            "hoist": false,
            "managed": false,
            "mentionable": true,
            "permissions": "0",
            "position": 1,
        }))
        .expect("a role with only the required fields deserializes");
        cache.insert_role(Id::new(900), &role);
        assert_eq!(
            cache.role_by_name(Id::new(900), "release team"),
            Some(Id::new(555))
        );
        assert_eq!(cache.role_by_name(Id::new(900), "Nobody"), None);
    }
}
