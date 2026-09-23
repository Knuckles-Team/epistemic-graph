//! The batch-aware pin resolver of a pack commit (PA2, pack design §3.4).
//!
//! Planning resolved every intra-pack reference against a snapshot. Inside the
//! one Agent Library write, every pin a written revision carries is resolved
//! AGAIN: first against the revisions staged in this very batch, then against
//! the committed tables. A pin that names neither -- a sibling withdrawn or
//! retired by someone else since planning, or revised underneath it -- aborts
//! the whole write as `PACK_PLAN_STALE`. Staged rows are consulted directly
//! rather than read back, so nothing relies on read-your-writes inside the
//! owner window.

use std::collections::BTreeSet;

use eg_types::agent_library::AgentLibraryLifecycle;
use redb::ReadableTable;

use super::admitted::PackOwnerWrite;
use super::commit::ConnectorPackComponentCommit;

/// Resolve every pin of every revision this batch writes.
pub(super) fn resolve_pack_pins_in_write(
    write: &PackOwnerWrite<'_>,
    tenant_id: &str,
    components: &[ConnectorPackComponentCommit],
) -> Result<(), String> {
    let staged: BTreeSet<(&str, &str)> = components
        .iter()
        .map(|component| {
            (
                component.entry.component_id.as_str(),
                component.entry.definition_digest.as_str(),
            )
        })
        .collect();
    let heads = write.open_table(eg_storage::AGENT_COMPONENT_HEADS)?;
    let revisions = write.open_table(eg_storage::AGENT_COMPONENT_REVISIONS)?;
    for component in components {
        if component.entry.lifecycle != AgentLibraryLifecycle::Published {
            continue;
        }
        let draft = component.entry.as_draft();
        for pin in draft.pinned_components() {
            let key = (pin.component_id.as_str(), pin.definition_digest.as_str());
            if staged.contains(&key) {
                continue;
            }
            let head_revision = heads
                .get((tenant_id, key.0))
                .map_err(|error| error.to_string())?
                .map(|value| value.value())
                .ok_or_else(|| stale(&component.entry.component_id, key.0))?;
            let head = revisions
                .get((tenant_id, key.0, head_revision))
                .map_err(|error| error.to_string())?
                .ok_or_else(|| "agent component head points to a missing revision".to_string())?;
            let head: eg_types::agent_component::AgentComponentEntry =
                crate::server::persistence::agent_row::decode(head.value(), "agent component")?;
            if head.lifecycle != AgentLibraryLifecycle::Published || head.definition_digest != key.1
            {
                return Err(stale(&component.entry.component_id, key.0));
            }
        }
    }
    Ok(())
}

fn stale(subject: &str, pinned: &str) -> String {
    format!(
        "PACK_PLAN_STALE: '{subject}' pins '{pinned}', which is no longer the published \
         revision the import was planned against"
    )
}
