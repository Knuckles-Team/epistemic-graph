//! Decide-layer fixtures shared by eg-types' own derivation tests and
//! eg-compute's assembly tests.

use crate::agent_component::{
    AgentComponentEntry, AgentComponentFacts, AgentComponentKind, ComponentProvenance, CostFacts,
    DeclaredCost, DeclaredLatency, FactQuality, PriceSource, ToolEffect,
    AGENT_COMPONENT_SCHEMA_VERSION,
};
use crate::agent_library::AgentLibraryLifecycle;
use crate::contract::BoundedVec;
use crate::decision::CandidateFacts;

/// A published candidate with opaque facts, classified under
/// `classification`, requiring and declaring nothing, and resting on no
/// premise. Fixtures add premises or requirements with struct-update syntax.
pub fn published_candidate(
    component_id: &str,
    kind: AgentComponentKind,
    definition_digest: &str,
    classification: &[&str],
) -> CandidateFacts {
    CandidateFacts {
        component_id: component_id.to_string(),
        kind,
        entry_revision: 1,
        definition_digest: definition_digest.to_string(),
        lifecycle: AgentLibraryLifecycle::Published,
        classification: BoundedVec::new(classification.iter().map(|t| t.to_string()).collect())
            .expect("fixture classification fits its bound"),
        required_capabilities: BoundedVec::default(),
        declared_capabilities: BoundedVec::default(),
        requires: BoundedVec::default(),
        facts: AgentComponentFacts::Opaque,
        fact_premises: BoundedVec::default(),
    }
}

/// A published `Tool` entry with a declared per-call cost and p95 latency,
/// classified under `classification` (EG-DECISION-ENGINE-R061).
///
/// The one fixture both consuming paths read: the exact-solver path
/// (`eg-compute::assemble::facts`, over `CandidateFacts::from_entry(&entry)`)
/// and the statistical candidate path (`eg-numeric`'s
/// `CandidateView::from_component(&entry)`) each derive their own typed view
/// from this SAME `AgentComponentEntry`, so a test comparing the two derived
/// views is a test that the same declared cost/latency/capability facts read
/// the same values on both paths.
pub fn tool_entry_with_cost_latency(
    component_id: &str,
    classification: &[&str],
    currency: &str,
    per_call_micros: u64,
    p95_ms: u32,
) -> AgentComponentEntry {
    AgentComponentEntry {
        schema_version: AGENT_COMPONENT_SCHEMA_VERSION,
        component_id: component_id.to_string(),
        kind: AgentComponentKind::Tool,
        version: "v1".to_string(),
        content_digest: "sha256:fixture-content".to_string(),
        content_ref: None,
        facts: AgentComponentFacts::Tool {
            effect: ToolEffect::Read,
            required_scopes: Vec::new(),
            input_schema_digest: None,
            output_schema_digest: None,
            read_only_hint: None,
            destructive_hint: None,
            idempotent_hint: None,
            open_world_hint: None,
            modalities: Default::default(),
            cost: Some(CostFacts {
                declared: DeclaredCost {
                    currency: currency.to_string(),
                    per_call_micros: Some(per_call_micros),
                    input_per_mtok_micros: None,
                    output_per_mtok_micros: None,
                },
                price_source: PriceSource::Publisher,
                quality: FactQuality::Declared,
            }),
            latency_declared: Some(DeclaredLatency {
                p50_ms: p95_ms / 2,
                p95_ms,
            }),
        },
        provenance: ComponentProvenance::Native,
        summary: format!("fixture tool {component_id}"),
        classification: classification.iter().map(|t| t.to_string()).collect(),
        requires: Vec::new(),
        declared_capabilities: Vec::new(),
        required_capabilities: Vec::new(),
        declared_required_capabilities: Vec::new(),
        attributes: Default::default(),
        tenant_id: "tenant-fixture".to_string(),
        actor_scope: "fixture".to_string(),
        purpose_id: "fixture".to_string(),
        policy_digest: "sha256:fixture-policy".to_string(),
        source_revision: "fixture".to_string(),
        source_revision_digest: "sha256:fixture-source".to_string(),
        entry_revision: 1,
        lifecycle: AgentLibraryLifecycle::Published,
        definition_digest: "sha256:fixture-definition".to_string(),
        created_at_ms: 0,
        updated_at_ms: 0,
    }
}

/// The BM25 text feature over a component's `summary`, scored against the
/// request's `query` parameter; abstains when either is missing.
pub fn summary_text_feature() -> crate::decision::statistical::features::FeatureSpec {
    use crate::decision::statistical::features::{FeatureKind, FeatureSpec, MissingValue};
    FeatureSpec {
        name: "text".to_string(),
        kind: FeatureKind::TextBm25 {
            key: "summary".to_string(),
            param: "query".to_string(),
        },
        missing: MissingValue::Abstain,
    }
}

#[cfg(test)]
mod tests {
    use super::tool_entry_with_cost_latency;
    use crate::decision::CandidateFacts;

    // spec: EG-DECISION-ENGINE-R061.1
    #[test]
    fn the_fixture_entry_converts_to_candidate_facts_with_its_declared_cost_intact() {
        let entry = tool_entry_with_cost_latency(
            "fixture-tool",
            &["eg:capability/retrieval"],
            "USD",
            1_200,
            450,
        );
        let facts = CandidateFacts::from_entry(&entry).expect("fixture entry is well-formed");
        assert_eq!(facts.component_id, "fixture-tool");
        assert_eq!(
            facts.classification.as_slice(),
            ["eg:capability/retrieval".to_string()]
        );
        let crate::agent_component::AgentComponentFacts::Tool {
            cost,
            latency_declared,
            ..
        } = &facts.facts
        else {
            panic!("fixture facts must be Tool");
        };
        assert_eq!(cost.as_ref().unwrap().declared.per_call_micros, Some(1_200));
        assert_eq!(latency_declared.unwrap().p95_ms, 450);
    }
}
