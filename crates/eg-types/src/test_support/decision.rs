//! Decide-layer fixtures shared by eg-types' own derivation tests and
//! eg-compute's assembly tests.

use crate::agent_component::{AgentComponentFacts, AgentComponentKind};
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
