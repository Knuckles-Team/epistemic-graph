use super::{ChannelManager, ChannelMessage, ChannelType};

impl ChannelManager {
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
            .map(|channel| {
                (
                    channel.id.clone(),
                    channel.channel_type,
                    channel.members.len(),
                )
            })
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
}
