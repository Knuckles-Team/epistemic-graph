//! The tenant's published `GuardrailRule` set (EG-DECISION-ENGINE-R127.2.1).
//!
//! A guardrail rule is an Agent Library component of kind
//! [`AgentComponentKind::GuardrailRule`], published through the ordinary
//! component publish path: the component id is the rule id and the governed
//! task class and source generation travel as attributes, which the draft
//! validator refuses at publish when missing or malformed. This module is the
//! read half: one snapshot of the tenant's current, published rule heads,
//! decoded back into [`GuardrailRule`]s for guardrail entailment
//! (EG-DECISION-ENGINE-R127) to reason over. It adds no served query.

use eg_types::agent_component::AgentComponentKind;
use eg_types::agent_library::AgentLibraryLifecycle;
use eg_types::decision::guardrail::GuardrailRule;

use super::agent_component::ComponentLayer;
use super::agent_library::AgentLibraryStore;
use super::agent_revision::decode_revision;

/// Head rows one rule-set read may examine. A tenant larger than this is
/// refused by name rather than answered over a silently partial rule set.
const MAX_GUARDRAIL_SCAN: usize = 4_096;

impl AgentLibraryStore {
    /// Every current, published `GuardrailRule` of `tenant_id`, sorted by
    /// rule id, read from one snapshot. Retired and withdrawn rules are not
    /// in force and are left out.
    pub fn published_guardrail_rules(&self, tenant_id: &str) -> Result<Vec<GuardrailRule>, String> {
        let read = self.read()?;
        let heads = read.open_owner_table(eg_storage::AGENT_COMPONENT_HEADS)?;
        let revisions = read.open_owner_table(eg_storage::AGENT_COMPONENT_REVISIONS)?;
        let range = heads
            .range((tenant_id, "")..)
            .map_err(|error| error.to_string())?;
        let mut rules = Vec::new();
        for (scanned, row) in range.enumerate() {
            let (key, head) = row.map_err(|error| error.to_string())?;
            let (row_tenant, component_id) = key.value();
            if row_tenant != tenant_id {
                break;
            }
            if scanned >= MAX_GUARDRAIL_SCAN {
                return Err(format!(
                    "GUARDRAIL_SCOPE_TOO_LARGE: the tenant holds more than \
                     {MAX_GUARDRAIL_SCAN} components"
                ));
            }
            let stored = revisions
                .get((tenant_id, component_id, head.value()))
                .map_err(|error| error.to_string())?
                .ok_or_else(|| "agent component head points to a missing revision".to_string())?;
            let entry = decode_revision::<ComponentLayer>(stored.value())?;
            if entry.kind != AgentComponentKind::GuardrailRule
                || entry.lifecycle != AgentLibraryLifecycle::Published
            {
                continue;
            }
            rules.push(GuardrailRule::from_attributes(
                &entry.component_id,
                &entry.attributes,
            )?);
        }
        Ok(rules)
    }
}

#[cfg(test)]
mod tests;
