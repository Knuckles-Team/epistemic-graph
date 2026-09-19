//! Valid sample values for the Decide layer's wire types.
//!
//! Built by construction rather than decoded from a fixture, so a field added
//! to a record makes this file fail to compile instead of making a test pass
//! against a value that no longer has the shape it claims.

use crate::agent_component::{AgentComponentFacts, AgentComponentKind, ComponentDependency};
use crate::agent_library::{AgentLibraryLifecycle, AgentLibraryMutationContext};
use crate::contract::Nonce;
use crate::decision::{
    AbstainReason, AssemblyConstraints, AssemblyRequest, AssemblyRequirements, CandidateFacts,
    CandidateSourceRecord, ColdStart, CoverageDerivation, DecideRequest, DecisionCommitRequest,
    DecisionInputs, DecisionOutcome, DecisionPolicy, DecisionPolicyRef, DecisionQuestion,
    DecisionRecord, DerivationClass, DerivationEdge, EdgeSource, Elimination, EvidenceClass,
    LibraryCandidateScope, ObjectiveLevelKind, ObjectiveOrder, PremiseClass, PremiseProvenance,
    PremiseRef, ResolutionKind, SlotAssignment, SolverIdentity, TraceFidelity, UnitRationalWire,
    UnknownCostRule, Violation, WhyNot, DECISION_POLICY_SCHEMA_VERSION,
    DECISION_RECORD_SCHEMA_VERSION,
};
use crate::solve::{Algorithm, ObjectiveValue, Scalar};

use super::{bounded, digest_text, solver};

/// A pinned component reference.
pub fn dependency(component_id: &str, kind: AgentComponentKind) -> ComponentDependency {
    ComponentDependency {
        component_id: component_id.to_string(),
        kind,
        definition_digest: digest_text(0x11),
    }
}

/// The mutation context every agent-library write carries.
pub fn mutation_context() -> AgentLibraryMutationContext {
    AgentLibraryMutationContext {
        request_id: 1,
        principal: "principal-a".to_string(),
        caller_principal: "caller-a".to_string(),
        attempt_nonce: Nonce::from_bytes([7; 32]),
        tenant_id: "tenant-a".to_string(),
        actor_scope: "scope-a".to_string(),
        purpose_id: "purpose-a".to_string(),
        policy_revision: "policy-1".to_string(),
        policy_digest: digest_text(0x22),
        policy_decision_id: "decision-1".to_string(),
        idempotency_key: "idempotency-1".to_string(),
        expected_revision: Some(1),
        trace_id: Some("trace-1".to_string()),
        created_at_ms: 1_700_000_000_000,
    }
}

/// The engine-default-shaped policy.
pub fn policy() -> DecisionPolicy {
    DecisionPolicy {
        schema_version: DECISION_POLICY_SCHEMA_VERSION,
        objective: ObjectiveOrder::Lexicographic {
            levels: bounded(vec![
                ObjectiveLevelKind::Uncovered,
                ObjectiveLevelKind::Components,
                ObjectiveLevelKind::DeclaredCost,
                ObjectiveLevelKind::DeclaredP95Latency,
            ]),
        },
        unknown_cost: UnknownCostRule::ExcludeWhenStrict,
        accepted_gap: Scalar::new(0),
        node_budget: 100_000,
        max_templates: 8,
        max_slots: 6,
        max_nogood_rounds: 8,
        max_why_not_per_slot: 4,
        a2a_requires_observation: true,
        cold_start: ColdStart::DeterministicOnly,
        statistical: None,
    }
}

/// One assembly question.
pub fn assembly_request() -> AssemblyRequest {
    AssemblyRequest {
        tenant_id: "tenant-a".to_string(),
        requirements: AssemblyRequirements {
            tasks: bounded(vec!["eg:task/research".to_string()]),
            capabilities: bounded(vec!["eg:capability/retrieval".to_string()]),
            task_mappings: bounded(Vec::new()),
            unmapped_task_digests: bounded(Vec::new()),
            constraints: AssemblyConstraints::default(),
            pins: bounded(Vec::new()),
            denies: bounded(vec!["component-denied".to_string()]),
        },
        candidates: LibraryCandidateScope {
            kinds: bounded(vec![AgentComponentKind::Tool]),
            classification_under: Some("eg:capability".to_string()),
        },
        templates: bounded(Vec::new()),
        policy: DecisionPolicyRef::Default,
        solver: None,
    }
}

/// One premise of every class the record vocabulary has.
pub fn premises() -> Vec<PremiseRef> {
    [
        (
            PremiseClass::Definition,
            PremiseProvenance::NativeOntology {
                ontology_digest: digest_text(0x31),
            },
        ),
        (
            PremiseClass::Proof,
            PremiseProvenance::Policy {
                policy_digest: digest_text(0x32),
            },
        ),
        (
            PremiseClass::Claim,
            PremiseProvenance::Publisher {
                component_id: "component-a".to_string(),
                definition_digest: digest_text(0x33),
            },
        ),
        (
            PremiseClass::Claim,
            PremiseProvenance::ConnectorPack {
                connector: "connector-a".to_string(),
                binding_revision: 3,
                pack_digest: digest_text(0x34),
                entry_digest: digest_text(0x35),
            },
        ),
        (
            PremiseClass::Claim,
            PremiseProvenance::ClaimedMapping {
                text_digest: digest_text(0x36),
                producer: "mapper-a".to_string(),
            },
        ),
        (
            PremiseClass::Observation,
            PremiseProvenance::Observation {
                evaluation_id: "evaluation-a".to_string(),
            },
        ),
    ]
    .into_iter()
    .map(|(class, provenance)| PremiseRef {
        subject: "component-a".to_string(),
        fact: "provides".to_string(),
        class,
        provenance,
    })
    .collect()
}

/// Every elimination rule the record vocabulary has.
pub fn every_violation() -> Vec<Violation> {
    vec![
        Violation::Denied,
        Violation::Withdrawn,
        Violation::Retired,
        Violation::IneligibleExternalAgent,
        Violation::ContextWindowTooSmall {
            required: 64_000,
            available: 8_000,
        },
        Violation::MissingToolSupport,
        Violation::MissingStructuredOutput,
        Violation::MissingModality {
            iri: "eg:modality/image".to_string(),
        },
        Violation::UnknownCostUnderStrictBudget,
        Violation::OverBudget {
            level: ObjectiveLevelKind::DeclaredCost,
        },
        Violation::TemplateValidation {
            code: "SLOT_KIND_MISMATCH".to_string(),
        },
    ]
}

/// Every abstention reason the decision vocabulary has.
pub fn every_abstain_reason() -> Vec<AbstainReason> {
    vec![
        AbstainReason::UncoveredCapability {
            iri: "eg:capability/retrieval".to_string(),
        },
        AbstainReason::UnresolvedCapabilityIri {
            iri: "urn:vendor:thing".to_string(),
        },
        AbstainReason::UnmappedTask {
            text_digest: digest_text(0x41),
        },
        AbstainReason::Infeasible {
            constraints: bounded(vec!["cover:eg:capability/retrieval".to_string()]),
        },
        AbstainReason::BudgetExhausted {
            incumbent: Some("component-a".to_string()),
            lower_bound: Scalar::new(12),
        },
        AbstainReason::UnknownFact {
            component_id: "component-a".to_string(),
            field: "cost".to_string(),
        },
        AbstainReason::IneligibleExternalAgent {
            component_id: "component-b".to_string(),
        },
        AbstainReason::RecordTooLarge { bytes: 65_537 },
        AbstainReason::PolicyLoosening {
            field: "node_budget".to_string(),
        },
        AbstainReason::InsufficientConfidence,
    ]
}

/// Both conclusions an assembly can reach.
pub fn every_decision_outcome() -> Vec<DecisionOutcome> {
    vec![
        DecisionOutcome::Solved {
            graph_digest: digest_text(0x51),
            slots: bounded(vec![SlotAssignment {
                slot: "tool".to_string(),
                component: dependency("component-a", AgentComponentKind::Tool),
            }]),
            certificate: Box::new(solver::certificate()),
        },
        DecisionOutcome::Abstained {
            reasons: bounded(every_abstain_reason()),
        },
    ]
}

fn candidate_facts() -> CandidateFacts {
    CandidateFacts {
        component_id: "component-a".to_string(),
        kind: AgentComponentKind::Tool,
        entry_revision: 4,
        definition_digest: digest_text(0x61),
        lifecycle: AgentLibraryLifecycle::Published,
        classification: bounded(vec!["eg:capability/retrieval".to_string()]),
        required_capabilities: bounded(vec!["eg:capability/action".to_string()]),
        declared_capabilities: bounded(vec!["urn:vendor:search".to_string()]),
        requires: bounded(vec![dependency(
            "component-b",
            AgentComponentKind::ModelProfile,
        )]),
        facts: AgentComponentFacts::Opaque,
        fact_premises: bounded(premises()),
    }
}

fn inputs() -> DecisionInputs {
    DecisionInputs {
        request: assembly_request(),
        candidates: bounded(vec![candidate_facts()]),
        ontology_digest: digest_text(0x71),
        policy: policy(),
        policy_digest: digest_text(0x72),
        catalog_digest: digest_text(0x73),
        solver: SolverIdentity {
            algorithm: Algorithm::DepthFirstDualAscent,
            node_budget: 100_000,
        },
    }
}

/// One complete assembly record, carrying `outcome`.
pub fn record(outcome: DecisionOutcome) -> DecisionRecord {
    DecisionRecord {
        schema_version: DECISION_RECORD_SCHEMA_VERSION,
        record_id: "decision:0011".to_string(),
        tenant_id: "tenant-a".to_string(),
        caller_principal: "caller-a".to_string(),
        created_at_ms: 1_700_000_000_000,
        question: DecisionQuestion::Assemble,
        candidate_source: CandidateSourceRecord::AgentLibrary {
            kinds: bounded(vec![AgentComponentKind::Tool]),
            classification_under: Some("eg:capability".to_string()),
        },
        inputs: inputs(),
        inputs_digest: digest_text(0x81),
        resolution_kind: ResolutionKind::Optimization,
        evidence_class: EvidenceClass::Claim,
        derivation_class: DerivationClass::Proof,
        trace_fidelity: TraceFidelity::Truncated {
            dropped: bounded(vec!["why_not".to_string()]),
        },
        premises: bounded(premises()),
        eliminated: bounded(
            every_violation()
                .into_iter()
                .map(|violation| Elimination {
                    component_id: "component-c".to_string(),
                    violation,
                })
                .collect(),
        ),
        derivations: bounded(vec![CoverageDerivation {
            required: "eg:capability/retrieval".to_string(),
            covered_by: Some("component-a".to_string()),
            chain: bounded(vec![
                DerivationEdge {
                    narrower: "eg:capability/retrieval/web-search".to_string(),
                    broader: "eg:capability/retrieval".to_string(),
                    source: EdgeSource::NativeOntology,
                    class: PremiseClass::Definition,
                },
                DerivationEdge {
                    narrower: "urn:vendor:search".to_string(),
                    broader: "eg:capability/retrieval".to_string(),
                    source: EdgeSource::ComponentClassification {
                        component_id: "component-a".to_string(),
                    },
                    class: PremiseClass::Claim,
                },
            ]),
        }]),
        outcome,
        why_not: bounded(vec![WhyNot {
            component_id: "component-c".to_string(),
            slot: "tool".to_string(),
            forced_objective: Some(ObjectiveValue {
                levels: Vec::new(),
                scalar: Scalar::new(9),
            }),
            violation: Some(Violation::Denied),
        }]),
        record_digest: digest_text(0x91),
    }
}

/// One commit of the solved record.
pub fn commit_request() -> DecisionCommitRequest {
    let solved = every_decision_outcome()
        .into_iter()
        .next()
        .expect("the solved outcome is first");
    DecisionCommitRequest {
        context: mutation_context(),
        record: record(solved),
        expected_catalog_digest: digest_text(0x73),
    }
}

/// A unit rational that is inside every bound.
pub fn unit_rational(numerator: u64, denominator: u64) -> UnitRationalWire {
    UnitRationalWire::new(numerator, denominator).expect("sample rational is inside its bounds")
}

/// One evaluate-only statistical question.
pub fn decide_request() -> DecideRequest {
    super::statistical::decide_request()
}
