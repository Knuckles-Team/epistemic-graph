//! Fleet catalog reads over the AgentComponent owner (EH-345).
//!
//! The fleet catalog stores no content of its own: a connector's tools,
//! prompts, resources and skills are the `AgentComponent` records its pack
//! published, keyed `mcp:<connector>/<kind>/<name>`. This is the one read the
//! projection needs from the owner -- every HEAD revision under one exact key
//! prefix of one tenant -- decoded by the component layer's own decoder, so the
//! fleet catalog cannot come to disagree with `AgentComponent.Current` about
//! what a record says.

use eg_types::agent_component::AgentComponentEntry;

use super::agent_component::{component_tables, ComponentLayer};
use super::agent_library::AgentLibraryStore;
use super::agent_revision::decode_revision;

impl AgentLibraryStore {
    /// Every HEAD revision whose id starts with `prefix` in `tenant_id`, in id
    /// order, whatever its lifecycle -- the caller decides what is served.
    ///
    /// Refuses by name past `max_rows` rather than returning a truncated set:
    /// the fleet projection digests and counts what this returns, and a silent
    /// truncation would page a catalog that does not exist.
    pub fn component_heads_with_prefix(
        &self,
        tenant_id: &str,
        prefix: &str,
        max_rows: usize,
    ) -> Result<Vec<AgentComponentEntry>, String> {
        let tables = component_tables();
        let read = self.read()?;
        let heads = read.open_owner_table(tables.heads)?;
        let revisions = read.open_owner_table(tables.revisions)?;
        let mut entries = Vec::new();
        for row in heads
            .range((tenant_id, prefix)..)
            .map_err(|error| error.to_string())?
        {
            let (key, head_revision) = row.map_err(|error| error.to_string())?;
            let (row_tenant, component_id) = key.value();
            if row_tenant != tenant_id || !component_id.starts_with(prefix) {
                break;
            }
            if entries.len() == max_rows {
                return Err(format!(
                    "FLEET_SNAPSHOT_TOO_LARGE: more than {max_rows} components under '{prefix}'"
                ));
            }
            let revision = revisions
                .get((row_tenant, component_id, head_revision.value()))
                .map_err(|error| error.to_string())?
                .ok_or_else(|| "agent component head points to a missing revision".to_string())?;
            entries.push(decode_revision::<ComponentLayer>(revision.value())?);
        }
        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_owner_answers_no_members_rather_than_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let store = AgentLibraryStore::open(dir.path().to_str().unwrap()).unwrap();
        let members = store
            .component_heads_with_prefix("tenant-a", "mcp:github/tool/", 16)
            .unwrap();
        assert!(members.is_empty());
    }
}
