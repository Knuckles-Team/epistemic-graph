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
//!
//! It is also the promotion gate (EG-DECISION-ENGINE-R129.3/.4): a published
//! rule component that names a forbidden rule (`forbids_rule_id`) is a stored
//! invariant, and publishing a rule that contradicts one is refused with
//! `RULE_CONTRADICTS_INVARIANT` before anything is written.

use eg_types::agent_component::AgentComponentKind;
use eg_types::agent_library::AgentLibraryLifecycle;
use std::collections::BTreeMap;

use eg_types::agent_component::AgentComponentDraft;
use eg_types::decision::guardrail::{
    promote_on_consensus, GuardrailRule, RuleInvariant, RulePromotionVerdict,
};

use super::agent_component::ComponentLayer;
use super::agent_library::AgentLibraryStore;
use super::agent_revision::decode_revision;

/// Head rows one rule-set read may examine. A tenant larger than this is
/// refused by name rather than answered over a silently partial rule set.
const MAX_GUARDRAIL_SCAN: usize = 4_096;

/// Agreement an explicit publish carries: the publishing principal's own
/// approval is the whole of the human decision, so it counts as full
/// consensus. Promotion still requires it to clear the threshold, and the
/// invariant gate runs before either is compared (R129.4).
const PUBLISH_AGREEMENT: f64 = 1.0;
/// Consensus a rule must reach to be promoted.
const PROMOTION_THRESHOLD: f64 = 1.0;

/// A published rule paired with its stored component metadata.
type PublishedRuleComponent = (GuardrailRule, BTreeMap<String, String>);

impl AgentLibraryStore {
    /// Every current, published `GuardrailRule` of `tenant_id`, sorted by
    /// rule id, read from one snapshot. Retired and withdrawn rules are not
    /// in force and are left out.
    pub fn published_guardrail_rules(&self, tenant_id: &str) -> Result<Vec<GuardrailRule>, String> {
        Ok(self
            .published_rule_components(tenant_id)?
            .into_iter()
            .map(|(rule, _)| rule)
            .collect())
    }

    /// Every stored invariant of `tenant_id`, sorted by invariant IRI.
    pub fn published_rule_invariants(&self, tenant_id: &str) -> Result<Vec<RuleInvariant>, String> {
        let mut invariants = Vec::new();
        for (rule, attributes) in self.published_rule_components(tenant_id)? {
            if let Some(invariant) = RuleInvariant::from_rule_component(&rule, &attributes)? {
                invariants.push(invariant);
            }
        }
        Ok(invariants)
    }

    /// Refuse to publish a guardrail rule that contradicts a stored
    /// invariant. Promotion is decided by [`promote_on_consensus`], whose
    /// invariant check runs before the consensus threshold, so even the full
    /// agreement of an explicit publish cannot override an invariant.
    pub(crate) fn require_rule_promotable(
        &self,
        component: &AgentComponentDraft,
    ) -> Result<(), String> {
        let rule = GuardrailRule::from_attributes(&component.component_id, &component.attributes)?;
        if RuleInvariant::from_rule_component(&rule, &component.attributes)?.is_some() {
            return Ok(());
        }
        let invariants = self.published_rule_invariants(&component.tenant_id)?;
        match promote_on_consensus(&rule, &invariants, PUBLISH_AGREEMENT, PROMOTION_THRESHOLD) {
            Some(RulePromotionVerdict::Promotable) => Ok(()),
            Some(RulePromotionVerdict::RejectedInvariant { invariant_iri }) => Err(format!(
                "RULE_CONTRADICTS_INVARIANT: guardrail rule '{}' on '{}' contradicts \
                 invariant '{invariant_iri}'",
                rule.rule_id, rule.applies_to_class
            )),
            None => Err(format!(
                "RULE_CONTRADICTS_INVARIANT: guardrail rule '{}' did not reach the \
                 promotion threshold",
                rule.rule_id
            )),
        }
    }

    fn published_rule_components(
        &self,
        tenant_id: &str,
    ) -> Result<Vec<PublishedRuleComponent>, String> {
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
            let rule = GuardrailRule::from_attributes(&entry.component_id, &entry.attributes)?;
            rules.push((rule, entry.attributes));
        }
        Ok(rules)
    }
}

#[cfg(test)]
mod tests;
