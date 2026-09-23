//! Refuse inputs that are not self-consistent before any step reads them.
//!
//! A record is re-derived from its stored inputs on commit, so the inputs have
//! to be a closed, checkable value: every digest they carry must be the digest
//! of what they carry, and the candidate list must be the canonical one. A
//! caller-built record whose inputs disagree with themselves is refused here
//! rather than decided.

use eg_types::agent_component::AgentComponentKind;
use eg_types::decision::digest;
use eg_types::decision::request::{effective_solver_budget, loosened_solver_field};
use eg_types::decision::{
    CandidateFacts, DecisionErrorCode, DecisionInputs, MAX_ASSEMBLY_CANDIDATES,
    MAX_ASSEMBLY_TEMPLATES, MAX_TEMPLATE_SLOTS,
};
use eg_types::solve::Algorithm;

use super::seal::RecordIdentity;
use super::AssembleError;

fn invalid(detail: impl Into<String>) -> AssembleError {
    AssembleError::new(DecisionErrorCode::AssemblyInputsInvalid, detail)
}

/// Every consistency rule the decision function relies on.
pub(super) fn inputs(
    inputs: &DecisionInputs,
    identity: &RecordIdentity,
) -> Result<(), AssembleError> {
    if identity.tenant_id != inputs.request.tenant_id {
        return Err(invalid("the record tenant differs from the request tenant"));
    }
    inputs
        .policy
        .clone()
        .checked()
        .map_err(|code| AssembleError::new(code, "the decision policy is not usable"))?;
    check_digests(inputs)?;
    check_candidates(
        inputs.candidates.as_slice(),
        &inputs.request.candidates.kinds,
    )?;
    check_templates(inputs)?;
    check_solver(inputs)
}

/// The stored templates are exactly the ones the request named, in order,
/// each a valid shape with between one and the policy's slot bound `Agent`
/// nodes (§7.3: at most eight templates of at most six slots).
fn check_templates(inputs: &DecisionInputs) -> Result<(), AssembleError> {
    let named = inputs.request.templates.as_slice();
    let stored = inputs.templates.as_slice();
    let max_templates = usize::from(inputs.policy.max_templates).min(MAX_ASSEMBLY_TEMPLATES);
    if stored.len() != named.len() || stored.len() > max_templates {
        return Err(invalid(
            "the stored templates are not the ones the request named",
        ));
    }
    let max_slots = usize::from(inputs.policy.max_slots).min(MAX_TEMPLATE_SLOTS);
    for (reference, template) in named.iter().zip(stored) {
        let same = reference.graph_id == template.graph_id
            && reference.entry_revision == template.entry_revision
            && reference.definition_digest == template.definition_digest;
        if !same {
            return Err(invalid(format!(
                "template '{}' is not the named revision",
                template.graph_id
            )));
        }
        template.shape.validate().map_err(invalid)?;
        let slots = template.slot_nodes().len();
        if slots == 0 || slots > max_slots {
            return Err(invalid(format!(
                "template '{}' has {slots} agent slots; 1..={max_slots} are allowed",
                template.graph_id
            )));
        }
    }
    Ok(())
}

fn check_digests(inputs: &DecisionInputs) -> Result<(), AssembleError> {
    if inputs.policy_digest != digest::policy_digest(&inputs.policy) {
        return Err(invalid(
            "policy_digest is not the digest of the stored policy",
        ));
    }
    if inputs.ontology_digest != eg_types::agent_ontology::ontology_digest() {
        return Err(invalid(
            "ontology_digest names a vocabulary this build does not carry",
        ));
    }
    if inputs.catalog_digest != digest::candidates_catalog_digest(inputs.candidates.as_slice()) {
        return Err(invalid(
            "catalog_digest is not the digest of the stored candidates",
        ));
    }
    Ok(())
}

/// Sorted strictly by id, in the request's kind scope, and each of a kind an
/// agent can be assembled from (or an external agent card, which step 1b
/// then refuses by name).
fn check_candidates(
    candidates: &[CandidateFacts],
    kinds: &eg_types::contract::BoundedVec<AgentComponentKind, 16>,
) -> Result<(), AssembleError> {
    if candidates.len() > MAX_ASSEMBLY_CANDIDATES {
        return Err(AssembleError::new(
            DecisionErrorCode::CandidateScopeTooLarge,
            format!("at most {MAX_ASSEMBLY_CANDIDATES} candidates per record"),
        ));
    }
    if candidates
        .windows(2)
        .any(|pair| pair[0].component_id >= pair[1].component_id)
    {
        return Err(invalid(
            "candidates must be sorted by component id, without repeats",
        ));
    }
    for candidate in candidates {
        if !kinds.iter().any(|kind| *kind == candidate.kind) {
            return Err(invalid(format!(
                "candidate '{}' is outside the request's kind scope",
                candidate.component_id
            )));
        }
    }
    for kind in kinds {
        if !assemblable(*kind) {
            return Err(invalid(format!(
                "no agent slot accepts a {} component",
                kind.as_str()
            )));
        }
    }
    Ok(())
}

/// The kinds a one-agent assembly can place, plus the external agent card,
/// which is admitted to the scope only so that step 1b can refuse it by name.
pub(super) fn assemblable(kind: AgentComponentKind) -> bool {
    matches!(
        kind,
        AgentComponentKind::ModelProfile
            | AgentComponentKind::SystemPrompt
            | AgentComponentKind::Tool
            | AgentComponentKind::Toolset
            | AgentComponentKind::Skill
            | AgentComponentKind::Ontology
            | AgentComponentKind::A2aAgentCard
    )
}

fn check_solver(inputs: &DecisionInputs) -> Result<(), AssembleError> {
    if let Some(requested) = &inputs.request.solver {
        if let Some(field) = loosened_solver_field(&inputs.policy, requested) {
            return Err(AssembleError::new(
                DecisionErrorCode::PolicyLoosening,
                format!("the request widens the policy's {field}"),
            ));
        }
    }
    let budget = effective_solver_budget(&inputs.policy, &inputs.request);
    let identity = &inputs.solver;
    if identity.algorithm != Algorithm::DepthFirstDualAscent
        || identity.node_budget != budget.node_budget
    {
        return Err(invalid(
            "the solver identity is not the one this request runs under",
        ));
    }
    Ok(())
}
