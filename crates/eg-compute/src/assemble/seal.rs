//! Sealing a decision into its durable record: identity fields, digests, the
//! record id and the 64 KiB bound (DECIDE-LAYER-DESIGN §4.5, EH-067).
//!
//! The bound is applied in two steps. First the why-not explanations are
//! dropped and the record says so (`trace_fidelity = truncated [why_not]`);
//! if the record is still over the bound, the answer itself is withdrawn and
//! the record abstains with `RecordTooLarge { bytes }` -- a committed record is
//! never silently cut.

use eg_types::contract::BoundedVec;
use eg_types::decision::{
    digest, AbstainReason, CandidateSourceRecord, DecisionErrorCode, DecisionInputs,
    DecisionOutcome, DecisionQuestion, DecisionRecord, DerivationClass, TraceFidelity,
    DECISION_RECORD_SCHEMA_VERSION, MAX_DECISION_RECORD_BYTES,
};

use super::conclude::Decided;
use super::{AssembleError, Assembly};

/// The record fields that are recorded and never read by the math.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordIdentity {
    pub tenant_id: String,
    /// Privacy-safe persistence id of the caller.
    pub caller_principal: String,
    pub created_at_ms: u64,
}

impl RecordIdentity {
    /// The identity a stored record was sealed with.
    pub fn of(record: &DecisionRecord) -> Self {
        Self {
            tenant_id: record.tenant_id.clone(),
            caller_principal: record.caller_principal.clone(),
            created_at_ms: record.created_at_ms,
        }
    }
}

fn bounded<T, const N: usize>(
    values: Vec<T>,
    what: &str,
) -> Result<BoundedVec<T, N>, AssembleError> {
    BoundedVec::new(values).map_err(|error| {
        AssembleError::new(
            DecisionErrorCode::RecordTooLarge,
            format!("{what}: {error}"),
        )
    })
}

/// The encoded size a record is bounded by: its canonical JSON.
pub fn encoded_len(record: &DecisionRecord) -> usize {
    serde_json::to_vec(record).map_or(usize::MAX, |bytes| bytes.len())
}

pub(super) fn seal(
    inputs: DecisionInputs,
    identity: RecordIdentity,
    decided: Decided,
) -> Result<Assembly, AssembleError> {
    let mut record = unsealed(inputs, identity, &decided)?;
    let mut assembly_parts = (decided.model, decided.agents, decided.graph);
    if encoded_len(&record) > MAX_DECISION_RECORD_BYTES {
        record.why_not = BoundedVec::default();
        record.trace_fidelity = truncated(&["why_not"])?;
    }
    let bytes = encoded_len(&record);
    if bytes > MAX_DECISION_RECORD_BYTES {
        record.outcome = DecisionOutcome::Abstained {
            reasons: bounded(
                vec![AbstainReason::RecordTooLarge {
                    bytes: bytes as u64,
                }],
                "reasons",
            )?,
        };
        record.resolution_kind = eg_types::decision::ResolutionKind::Abstention;
        record.trace_fidelity = truncated(&["why_not", "outcome"])?;
        assembly_parts = (None, Vec::new(), None);
    }
    record.record_digest = digest::record_digest(&record);
    record.record_id = digest::record_id(&record.record_digest);
    let (model, agents, graph) = assembly_parts;
    Ok(Assembly {
        record,
        model,
        agents,
        graph,
    })
}

fn truncated(parts: &[&str]) -> Result<TraceFidelity, AssembleError> {
    Ok(TraceFidelity::Truncated {
        dropped: bounded(
            parts.iter().map(|part| part.to_string()).collect(),
            "dropped",
        )?,
    })
}

fn unsealed(
    inputs: DecisionInputs,
    identity: RecordIdentity,
    decided: &Decided,
) -> Result<DecisionRecord, AssembleError> {
    let candidate_source = CandidateSourceRecord::AgentLibrary {
        kinds: inputs.request.candidates.kinds.clone(),
        classification_under: inputs.request.candidates.classification_under.clone(),
    };
    Ok(DecisionRecord {
        schema_version: DECISION_RECORD_SCHEMA_VERSION,
        record_id: String::new(),
        tenant_id: identity.tenant_id,
        caller_principal: identity.caller_principal,
        created_at_ms: identity.created_at_ms,
        question: DecisionQuestion::Assemble,
        candidate_source,
        inputs_digest: digest::inputs_digest(&inputs),
        inputs,
        resolution_kind: decided.resolution_kind,
        evidence_class: decided.evidence_class,
        derivation_class: DerivationClass::Proof,
        trace_fidelity: TraceFidelity::FullStep,
        premises: bounded(decided.premises.clone(), "premises")?,
        eliminated: bounded(decided.eliminated.clone(), "eliminated")?,
        derivations: bounded(decided.derivations.clone(), "derivations")?,
        outcome: decided.outcome.clone(),
        why_not: bounded(decided.why_not.clone(), "why_not")?,
        record_digest: String::new(),
    })
}
