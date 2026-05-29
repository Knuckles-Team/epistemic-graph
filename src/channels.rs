// CONCEPT:KG-2.19 — Dynamic Communication Channels
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
    pub channel_type: ChannelType,
    pub creator: String,
    pub members: HashSet<String>,
    pub messages: Vec<ChannelMessage>,
    pub created_at: u64,
}

impl Channel {
    pub fn new(
        id: String,
        channel_type: ChannelType,
        creator: String,
        initial_members: Vec<String>,
    ) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let mut members: HashSet<String> = initial_members.into_iter().collect();
        members.insert(creator.clone());
        Channel {
            id,
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

impl ChannelManager {
    pub fn new() -> Self {
        ChannelManager {
            channels: HashMap::new(),
            imprints: Vec::new(),
        }
    }

    /// Create a new channel.
    pub fn create_channel(
        &mut self,
        channel_id: &str,
        channel_type: ChannelType,
        creator: &str,
        initial_members: Vec<String>,
    ) -> Result<(), String> {
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
                channel_type,
                creator.to_string(),
                initial_members,
            ),
        );
        Ok(())
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
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
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
            return Err(format!("Agent '{}' is not a member of channel '{}'", sender, channel_id));
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        channel.messages.push(ChannelMessage {
            sender: sender.to_string(),
            payload: payload.to_string(),
            timestamp: now,
        });
        Ok(())
    }

    /// Get messages from a channel.
    pub fn get_messages(&self, channel_id: &str, limit: Option<usize>) -> Result<Vec<&ChannelMessage>, String> {
        let channel = self
            .channels
            .get(channel_id)
            .ok_or_else(|| format!("Channel '{}' not found", channel_id))?;
        let msgs = match limit {
            Some(n) => channel.messages.iter().rev().take(n).collect::<Vec<_>>().into_iter().rev().collect(),
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
        mgr.create_channel(
            "channel:p2p:a:b",
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
    fn test_p2p_rejects_three_members() {
        let mut mgr = ChannelManager::new();
        let result = mgr.create_channel(
            "bad",
            ChannelType::PeerToPeer,
            "a",
            vec!["b".to_string(), "c".to_string()],
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_group_channel_join_leave() {
        let mut mgr = ChannelManager::new();
        mgr.create_channel("group:1", ChannelType::Group, "a", vec![])
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
        mgr.create_channel("ch", ChannelType::Group, "a", vec!["b".to_string()])
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
        mgr.create_channel("ch", ChannelType::Group, "a", vec![])
            .unwrap();
        assert!(mgr.send_message("ch", "outsider", "nope").is_err());
    }

    #[test]
    fn test_close_creates_imprint() {
        let mut mgr = ChannelManager::new();
        mgr.create_channel("ch", ChannelType::Group, "a", vec!["b".to_string()])
            .unwrap();
        mgr.send_message("ch", "a", "msg1").unwrap();

        let imprint = mgr.close_channel("ch", None, Some("test topic".into())).unwrap().unwrap();
        assert_eq!(imprint.message_count, 1);
        assert_eq!(imprint.topic_metadata, Some("test topic".into()));
        assert!(mgr.list_channels().is_empty());
    }

    #[test]
    fn test_auto_close_on_all_leave() {
        let mut mgr = ChannelManager::new();
        mgr.create_channel("ch", ChannelType::Group, "a", vec![])
            .unwrap();
        let imprint = mgr.leave_channel("ch", "a").unwrap();
        assert!(imprint.is_some());
        assert!(mgr.list_channels().is_empty());
    }

    #[test]
    fn test_drain_imprints() {
        let mut mgr = ChannelManager::new();
        mgr.create_channel("ch", ChannelType::Group, "a", vec![]).unwrap();
        mgr.close_channel("ch", None, None).unwrap();
        let imprints = mgr.drain_imprints();
        assert_eq!(imprints.len(), 1);
        assert!(mgr.drain_imprints().is_empty());
    }
}
