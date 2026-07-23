// CONCEPT:EG-KG.coordination.dynamic-channels — Dynamic Communication Channels
//
// Ephemeral P2P and group channels for inter-agent communication.
// Channels have a lifecycle: Create → Join → Leave → Close.
// On close, the channel's content is vectorized and persisted
// as a KG imprint (embedding + participant edges).

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

use crate::protocol::ChannelType;

/// A single message in a channel.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelMessage {
    pub sender: String,
    pub payload: String,
    pub timestamp: u64,
}

/// KG imprint created when a channel is closed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelImprint {
    pub channel_id: String,
    pub channel_type: ChannelType,
    pub creator: String,
    pub participants: Vec<String>,
    pub message_count: usize,
    pub created_at: u64,
    pub closed_at: u64,
    pub summary_embedding: Option<Vec<f32>>,
    pub topic_metadata: Option<String>,
}

/// A live communication channel.
#[derive(Debug, Clone)]
pub struct Channel {
    pub id: String,
    /// Opaque verified tenant owner.
    pub tenant_scope: String,
    pub channel_type: ChannelType,
    pub creator: String,
    pub members: HashSet<String>,
    pub messages: Vec<ChannelMessage>,
    pub created_at: u64,
}

impl Channel {
    pub fn new(
        id: String,
        tenant_scope: String,
        channel_type: ChannelType,
        creator: String,
        initial_members: Vec<String>,
    ) -> Self {
        let now = crate::server::authoritative_now_secs();
        let mut members: HashSet<String> = initial_members.into_iter().collect();
        members.insert(creator.clone());
        Channel {
            id,
            tenant_scope,
            channel_type,
            creator,
            members,
            messages: Vec::new(),
            created_at: now,
        }
    }
}

/// Manages all active channels.
pub struct ChannelManager {
    channels: HashMap<String, Channel>,
    /// Closed channel imprints awaiting KG persistence.
    pub imprints: Vec<ChannelImprint>,
}

impl Default for ChannelManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ChannelManager {
    pub fn new() -> Self {
        ChannelManager {
            channels: HashMap::new(),
            imprints: Vec::new(),
        }
    }

    /// Create a channel owned by a verified tenant.
    pub fn create_channel_scoped(
        &mut self,
        channel_id: &str,
        tenant_scope: &str,
        channel_type: ChannelType,
        creator: &str,
        initial_members: Vec<String>,
    ) -> Result<(), String> {
        if tenant_scope.trim().is_empty() {
            return Err("Channel tenant scope is required".to_string());
        }
        if self.channels.contains_key(channel_id) {
            return Err(format!("Channel '{}' already exists", channel_id));
        }
        // P2P channels must have exactly 2 members.
        if channel_type == ChannelType::PeerToPeer {
            let mut all_members: HashSet<String> = initial_members.iter().cloned().collect();
            all_members.insert(creator.to_string());
            if all_members.len() != 2 {
                return Err("PeerToPeer channels require exactly 2 members".to_string());
            }
        }
        self.channels.insert(
            channel_id.to_string(),
            Channel::new(
                channel_id.to_string(),
                tenant_scope.to_string(),
                channel_type,
                creator.to_string(),
                initial_members,
            ),
        );
        Ok(())
    }

    /// Uniform non-enumerating membership check for served channel operations.
    pub fn authorize_member(
        &self,
        channel_id: &str,
        tenant_scope: &str,
        agent_id: &str,
    ) -> Result<(), String> {
        let authorized = self.channels.get(channel_id).is_some_and(|channel| {
            channel.tenant_scope == tenant_scope && channel.members.contains(agent_id)
        });
        if authorized {
            Ok(())
        } else {
            Err("Channel not found or access denied".to_string())
        }
    }

    pub fn authorize_tenant(&self, channel_id: &str, tenant_scope: &str) -> Result<(), String> {
        if self
            .channels
            .get(channel_id)
            .is_some_and(|channel| channel.tenant_scope == tenant_scope)
        {
            Ok(())
        } else {
            Err("Channel not found or access denied".to_string())
        }
    }

    pub fn authorize_creator(
        &self,
        channel_id: &str,
        tenant_scope: &str,
        agent_id: &str,
    ) -> Result<(), String> {
        let authorized = self.channels.get(channel_id).is_some_and(|channel| {
            channel.tenant_scope == tenant_scope && channel.creator == agent_id
        });
        if authorized {
            Ok(())
        } else {
            Err("Channel not found or access denied".to_string())
        }
    }

    /// Join an existing channel.
    pub fn join_channel(&mut self, channel_id: &str, agent_id: &str) -> Result<(), String> {
        let channel = self
            .channels
            .get_mut(channel_id)
            .ok_or_else(|| format!("Channel '{}' not found", channel_id))?;
        if channel.channel_type == ChannelType::PeerToPeer {
            return Err("Cannot join a PeerToPeer channel after creation".to_string());
        }
        channel.members.insert(agent_id.to_string());
        Ok(())
    }

    /// Leave a channel. If all members leave, the channel auto-closes.
    pub fn leave_channel(
        &mut self,
        channel_id: &str,
        agent_id: &str,
    ) -> Result<Option<ChannelImprint>, String> {
        let channel = self
            .channels
            .get_mut(channel_id)
            .ok_or_else(|| format!("Channel '{}' not found", channel_id))?;
        channel.members.remove(agent_id);
        if channel.members.is_empty() {
            // Auto-close: create imprint.
            return self.close_channel(channel_id, None, None);
        }
        Ok(None)
    }

    /// Close a channel and create a KG imprint.
    pub fn close_channel(
        &mut self,
        channel_id: &str,
        summary_embedding: Option<Vec<f32>>,
        topic_metadata: Option<String>,
    ) -> Result<Option<ChannelImprint>, String> {
        let channel = self
            .channels
            .remove(channel_id)
            .ok_or_else(|| format!("Channel '{}' not found", channel_id))?;
        let now = crate::server::authoritative_now_secs();
        let imprint = ChannelImprint {
            channel_id: channel.id,
            channel_type: channel.channel_type,
            creator: channel.creator,
            participants: channel.members.into_iter().collect(),
            message_count: channel.messages.len(),
            created_at: channel.created_at,
            closed_at: now,
            summary_embedding,
            topic_metadata,
        };
        self.imprints.push(imprint.clone());
        Ok(Some(imprint))
    }

    /// Send a message to a channel.
    pub fn send_message(
        &mut self,
        channel_id: &str,
        sender: &str,
        payload: &str,
    ) -> Result<(), String> {
        let channel = self
            .channels
            .get_mut(channel_id)
            .ok_or_else(|| format!("Channel '{}' not found", channel_id))?;
        if !channel.members.contains(sender) {
            return Err(format!(
                "Agent '{}' is not a member of channel '{}'",
                sender, channel_id
            ));
        }
        let now = crate::server::authoritative_now_secs();
        channel.messages.push(ChannelMessage {
            sender: sender.to_string(),
            payload: payload.to_string(),
            timestamp: now,
        });
        Ok(())
    }

    /// Get messages from a channel.
    pub fn get_messages(
        &self,
        channel_id: &str,
        limit: Option<usize>,
    ) -> Result<Vec<&ChannelMessage>, String> {
        let channel = self
            .channels
            .get(channel_id)
            .ok_or_else(|| format!("Channel '{}' not found", channel_id))?;
        let msgs = match limit {
            Some(n) => channel
                .messages
                .iter()
                .rev()
                .take(n)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect(),
            None => channel.messages.iter().collect(),
        };
        Ok(msgs)
    }

    /// List all active channels.
    pub fn list_channels(&self) -> Vec<(String, ChannelType, usize)> {
        self.channels
            .values()
            .map(|c| (c.id.clone(), c.channel_type, c.members.len()))
            .collect()
    }

    /// List only channels the verified actor is a member of in its tenant.
    pub fn list_channels_for(
        &self,
        tenant_scope: &str,
        agent_id: &str,
    ) -> Vec<(String, ChannelType, usize)> {
        self.channels
            .values()
            .filter(|channel| {
                channel.tenant_scope == tenant_scope && channel.members.contains(agent_id)
            })
            .map(|channel| {
                (
                    channel.id.clone(),
                    channel.channel_type,
                    channel.members.len(),
                )
            })
            .collect()
    }

    /// Get members of a channel.
    pub fn get_members(&self, channel_id: &str) -> Result<Vec<String>, String> {
        let channel = self
            .channels
            .get(channel_id)
            .ok_or_else(|| format!("Channel '{}' not found", channel_id))?;
        Ok(channel.members.iter().cloned().collect())
    }

    /// Drain pending imprints for KG persistence.
    pub fn drain_imprints(&mut self) -> Vec<ChannelImprint> {
        std::mem::take(&mut self.imprints)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_p2p_channel() {
        let mut mgr = ChannelManager::new();
        mgr.create_channel_scoped(
            "channel:p2p:a:b",
            "tenant-test",
            ChannelType::PeerToPeer,
            "agent:a",
            vec!["agent:b".to_string()],
        )
        .unwrap();
        assert_eq!(mgr.list_channels().len(), 1);
        let members = mgr.get_members("channel:p2p:a:b").unwrap();
        assert_eq!(members.len(), 2);
    }

    #[test]
    fn scoped_channel_reads_require_same_tenant_membership() {
        let mut mgr = ChannelManager::new();
        mgr.create_channel_scoped(
            "shared-name",
            "tenant-a",
            ChannelType::Group,
            "alice",
            vec!["bob".to_string()],
        )
        .unwrap();
        assert!(mgr
            .authorize_member("shared-name", "tenant-a", "alice")
            .is_ok());
        assert!(mgr
            .authorize_member("shared-name", "tenant-a", "bob")
            .is_ok());
        assert!(mgr
            .authorize_member("shared-name", "tenant-a", "mallory")
            .is_err());
        assert!(mgr
            .authorize_member("shared-name", "tenant-b", "alice")
            .is_err());
        assert_eq!(mgr.list_channels_for("tenant-a", "bob").len(), 1);
        assert!(mgr.list_channels_for("tenant-b", "alice").is_empty());
    }

    #[test]
    fn test_p2p_rejects_three_members() {
        let mut mgr = ChannelManager::new();
        let result = mgr.create_channel_scoped(
            "bad",
            "tenant-test",
            ChannelType::PeerToPeer,
            "a",
            vec!["b".to_string(), "c".to_string()],
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_group_channel_join_leave() {
        let mut mgr = ChannelManager::new();
        mgr.create_channel_scoped("group:1", "tenant-test", ChannelType::Group, "a", vec![])
            .unwrap();
        mgr.join_channel("group:1", "b").unwrap();
        mgr.join_channel("group:1", "c").unwrap();
        assert_eq!(mgr.get_members("group:1").unwrap().len(), 3);

        mgr.leave_channel("group:1", "c").unwrap();
        assert_eq!(mgr.get_members("group:1").unwrap().len(), 2);
    }

    #[test]
    fn test_send_and_get_messages() {
        let mut mgr = ChannelManager::new();
        mgr.create_channel_scoped(
            "ch",
            "tenant-test",
            ChannelType::Group,
            "a",
            vec!["b".to_string()],
        )
        .unwrap();
        mgr.send_message("ch", "a", "hello").unwrap();
        mgr.send_message("ch", "b", "world").unwrap();

        let msgs = mgr.get_messages("ch", None).unwrap();
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].payload, "hello");
    }

    #[test]
    fn test_non_member_send_denied() {
        let mut mgr = ChannelManager::new();
        mgr.create_channel_scoped("ch", "tenant-test", ChannelType::Group, "a", vec![])
            .unwrap();
        assert!(mgr.send_message("ch", "outsider", "nope").is_err());
    }

    #[test]
    fn test_close_creates_imprint() {
        let mut mgr = ChannelManager::new();
        mgr.create_channel_scoped(
            "ch",
            "tenant-test",
            ChannelType::Group,
            "a",
            vec!["b".to_string()],
        )
        .unwrap();
        mgr.send_message("ch", "a", "msg1").unwrap();

        let imprint = mgr
            .close_channel("ch", None, Some("test topic".into()))
            .unwrap()
            .unwrap();
        assert_eq!(imprint.message_count, 1);
        assert_eq!(imprint.topic_metadata, Some("test topic".into()));
        assert!(mgr.list_channels().is_empty());
    }

    #[test]
    fn test_auto_close_on_all_leave() {
        let mut mgr = ChannelManager::new();
        mgr.create_channel_scoped("ch", "tenant-test", ChannelType::Group, "a", vec![])
            .unwrap();
        let imprint = mgr.leave_channel("ch", "a").unwrap();
        assert!(imprint.is_some());
        assert!(mgr.list_channels().is_empty());
    }

    #[test]
    fn test_drain_imprints() {
        let mut mgr = ChannelManager::new();
        mgr.create_channel_scoped("ch", "tenant-test", ChannelType::Group, "a", vec![])
            .unwrap();
        mgr.close_channel("ch", None, None).unwrap();
        let imprints = mgr.drain_imprints();
        assert_eq!(imprints.len(), 1);
        assert!(mgr.drain_imprints().is_empty());
    }
}
