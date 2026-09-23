//! Entailment with its working shown (DECIDE-LAYER-DESIGN §3.2, §7.1, WP A2).
//!
//! Three questions, each answered as data a reader can re-check rather than a
//! boolean it has to trust:
//!
//! * **What must the assembly cover?** [`required_capabilities`] closes the
//!   request's typed tasks over the native `requires` relation and adds the
//!   capabilities a caller named directly or through a claimed free-text
//!   mapping. Every required IRI carries the premise that made it required, and
//!   a mapping is a CLAIM, so a record that leans on one is classified by it.
//! * **Why does this component cover that capability?** [`coverage_chain`]
//!   returns the `is_a` walk from the component through one of its declared
//!   classification terms up to the requirement, one edge per step, each edge
//!   naming its source and premise class. The first edge is the publisher's
//!   classification (a claim); the rest are native vocabulary (definitions).
//! * **How far can the conclusion be trusted?** [`weakest`] folds premise
//!   classes into the record's evidence class: the WEAKEST premise wins, and a
//!   definition never weakens anything.
//!
//! [`verify_coverage`] and [`verify_record`] are the independent halves: they
//! re-check a stored derivation against the vocabulary this build carries and
//! the candidate facts the record stored, without running any of the code
//! above. `epistemic_graph.decision` repeats them in pure Python.

use std::collections::BTreeMap;

use super::record::{
    AbstainReason, CandidateFacts, CoverageDerivation, DecisionRecord, DerivationEdge, EdgeSource,
    EvidenceClass, PremiseClass, PremiseProvenance, PremiseRef,
};
use super::request::AssemblyRequirements;
use crate::agent_ontology;
use crate::contract::BoundedVec;

/// Most edges one stored coverage chain may carry.
pub const MAX_DERIVATION_EDGES: usize = 16;

/// Where a direct capability requirement is recorded as coming from.
const DIRECT_CAPABILITY_FIELD: &str = "requirements.capabilities";

/// One capability the assembly must cover, and the premise that requires it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequiredCapability {
    pub iri: String,
    pub because: PremiseRef,
}

/// Why a stored derivation or evidence class does not re-check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DerivationDefect {
    /// `covered_by` names a component the record's candidates do not hold.
    UnknownCoveringComponent { component_id: String },
    /// A covered requirement with no chain, or an uncovered one with a chain.
    ChainShape { required: String },
    /// The first edge is not the covering component's own classification.
    NotRootedInComponent { required: String },
    /// An edge after the first is not a declared native `broader` link, or
    /// is labelled with the wrong source or class.
    EdgeNotInOntology { required: String, index: usize },
    /// Consecutive edges do not share their middle term.
    Discontinuous { required: String, index: usize },
    /// The chain ends somewhere other than the requirement.
    MissesRequirement { required: String },
    /// The record's evidence class is not the weakest of its premises.
    EvidenceClassMismatch {
        recorded: EvidenceClass,
        derived: EvidenceClass,
    },
}

impl std::fmt::Display for DerivationDefect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "derivation does not re-check: {self:?}")
    }
}

impl std::error::Error for DerivationDefect {}

/// The capabilities `requirements` needs, each with the premise requiring it,
/// sorted by IRI; or every reason the request cannot be decided at all.
///
/// When one IRI is required for several reasons the STRONGEST premise is kept:
/// requirements are a disjunction of justifications, so a capability both
/// named directly and implied by a claimed mapping does not rest on the claim.
pub fn required_capabilities(
    requirements: &AssemblyRequirements,
    ontology_digest: &str,
) -> Result<Vec<RequiredCapability>, Vec<AbstainReason>> {
    let mut required: BTreeMap<String, PremiseRef> = BTreeMap::new();
    let mut reasons = Vec::new();
    for iri in &requirements.capabilities {
        if agent_ontology::is_capability(iri) {
            offer(&mut required, iri, direct_premise(iri));
        } else {
            reasons.push(unresolved(iri));
        }
    }
    let native = PremiseProvenance::NativeOntology {
        ontology_digest: ontology_digest.to_string(),
    };
    for task in &requirements.tasks {
        close_task(
            &mut required,
            &mut reasons,
            task,
            &native,
            PremiseClass::Definition,
        );
    }
    for mapping in &requirements.task_mappings {
        let claimed = PremiseProvenance::ClaimedMapping {
            text_digest: mapping.text_digest.clone(),
            producer: mapping.provenance.producer.clone(),
        };
        for task in &mapping.task_iris {
            close_task(
                &mut required,
                &mut reasons,
                task,
                &claimed,
                PremiseClass::Claim,
            );
        }
    }
    reasons.extend(
        requirements
            .unmapped_task_digests
            .iter()
            .map(|text_digest| AbstainReason::UnmappedTask {
                text_digest: text_digest.clone(),
            }),
    );
    if !reasons.is_empty() {
        reasons.dedup();
        return Err(reasons);
    }
    Ok(required
        .into_iter()
        .map(|(iri, because)| RequiredCapability { iri, because })
        .collect())
}

fn unresolved(iri: &str) -> AbstainReason {
    AbstainReason::UnresolvedCapabilityIri {
        iri: iri.to_string(),
    }
}

fn direct_premise(iri: &str) -> PremiseRef {
    PremiseRef {
        subject: iri.to_string(),
        fact: "required".to_string(),
        class: PremiseClass::Definition,
        provenance: PremiseProvenance::Request {
            field: DIRECT_CAPABILITY_FIELD.to_string(),
        },
    }
}

/// Add every capability `task` requires, under `provenance` and `class`.
fn close_task(
    required: &mut BTreeMap<String, PremiseRef>,
    reasons: &mut Vec<AbstainReason>,
    task: &str,
    provenance: &PremiseProvenance,
    class: PremiseClass,
) {
    if !agent_ontology::is_task(task) {
        reasons.push(unresolved(task));
        return;
    }
    for capability in agent_ontology::capabilities_for_task(task) {
        let premise = PremiseRef {
            subject: (*capability).to_string(),
            fact: format!("required_by:{task}"),
            class,
            provenance: provenance.clone(),
        };
        offer(required, capability, premise);
    }
}

/// Keep `premise` for `iri` unless a premise at least as strong is held.
fn offer(required: &mut BTreeMap<String, PremiseRef>, iri: &str, premise: PremiseRef) {
    let stronger = required
        .get(iri)
        .is_none_or(|held| premise.class.strength() > held.class.strength());
    if stronger {
        required.insert(iri.to_string(), premise);
    }
}

/// The shortest `is_a` chain from `candidate` to `required`, or `None` when no
/// declared classification term of the candidate is subsumed by it.
///
/// Ties break on the classification term, so the chain is a function of the
/// candidate facts alone.
pub fn coverage_chain(required: &str, candidate: &CandidateFacts) -> Option<Vec<DerivationEdge>> {
    let (term, native) = candidate
        .classification
        .iter()
        .filter_map(|term| {
            agent_ontology::broader_chain(term, required).map(|chain| (term.as_str(), chain))
        })
        .min_by(|a, b| a.1.len().cmp(&b.1.len()).then(a.0.cmp(b.0)))?;
    let mut edges = vec![DerivationEdge {
        narrower: candidate.component_id.clone(),
        broader: term.to_string(),
        source: EdgeSource::ComponentClassification {
            component_id: candidate.component_id.clone(),
        },
        class: PremiseClass::Claim,
    }];
    edges.extend(
        native
            .into_iter()
            .map(|(narrower, broader)| DerivationEdge {
                narrower: narrower.to_string(),
                broader: broader.to_string(),
                source: EdgeSource::NativeOntology,
                class: PremiseClass::Definition,
            }),
    );
    Some(edges)
}

/// The stored derivation for `required`, covered by `covered_by` when some
/// candidate covers it.
pub fn coverage_derivation(
    required: &str,
    covered_by: Option<&CandidateFacts>,
) -> Option<CoverageDerivation> {
    let chain = match covered_by {
        Some(candidate) => coverage_chain(required, candidate)?,
        None => Vec::new(),
    };
    Some(CoverageDerivation {
        required: required.to_string(),
        covered_by: covered_by.map(|candidate| candidate.component_id.clone()),
        chain: BoundedVec::new(chain).ok()?,
    })
}

/// The evidence class of a conclusion resting on `classes`: the weakest one.
/// A definition does not weaken; no premise at all is a proof.
pub fn weakest(classes: impl IntoIterator<Item = PremiseClass>) -> EvidenceClass {
    let floor = classes
        .into_iter()
        .map(PremiseClass::strength)
        .min()
        .unwrap_or(PremiseClass::Definition.strength());
    if floor == PremiseClass::Claim.strength() {
        EvidenceClass::Claim
    } else if floor == PremiseClass::Observation.strength() {
        EvidenceClass::Observation
    } else {
        EvidenceClass::Proof
    }
}

/// Every premise class a record's conclusion rests on: its premises and each
/// edge of each derivation.
pub fn record_premise_classes(record: &DecisionRecord) -> impl Iterator<Item = PremiseClass> + '_ {
    record.premises.iter().map(|premise| premise.class).chain(
        record
            .derivations
            .iter()
            .flat_map(|derivation| derivation.chain.iter().map(|edge| edge.class)),
    )
}

/// Re-check one stored coverage derivation against this build's vocabulary
/// and the candidate facts the record stored.
pub fn verify_coverage(
    derivation: &CoverageDerivation,
    candidates: &[CandidateFacts],
) -> Result<(), DerivationDefect> {
    let required = derivation.required.clone();
    let Some(component_id) = &derivation.covered_by else {
        return match derivation.chain.is_empty() {
            true => Ok(()),
            false => Err(DerivationDefect::ChainShape { required }),
        };
    };
    let candidate = candidates
        .iter()
        .find(|candidate| &candidate.component_id == component_id)
        .ok_or_else(|| DerivationDefect::UnknownCoveringComponent {
            component_id: component_id.clone(),
        })?;
    let (first, rest) =
        derivation
            .chain
            .as_slice()
            .split_first()
            .ok_or_else(|| DerivationDefect::ChainShape {
                required: required.clone(),
            })?;
    check_root_edge(first, candidate, &required)?;
    check_native_edges(first, rest, &required)?;
    let last = rest.last().unwrap_or(first);
    match last.broader == derivation.required {
        true => Ok(()),
        false => Err(DerivationDefect::MissesRequirement { required }),
    }
}

fn check_root_edge(
    first: &DerivationEdge,
    candidate: &CandidateFacts,
    required: &str,
) -> Result<(), DerivationDefect> {
    let rooted = first.narrower == candidate.component_id
        && first.class == PremiseClass::Claim
        && first.source
            == (EdgeSource::ComponentClassification {
                component_id: candidate.component_id.clone(),
            })
        && candidate
            .classification
            .iter()
            .any(|term| term == &first.broader);
    match rooted {
        true => Ok(()),
        false => Err(DerivationDefect::NotRootedInComponent {
            required: required.to_string(),
        }),
    }
}

fn check_native_edges(
    first: &DerivationEdge,
    rest: &[DerivationEdge],
    required: &str,
) -> Result<(), DerivationDefect> {
    let mut previous = first;
    for (offset, edge) in rest.iter().enumerate() {
        let index = offset + 1;
        if edge.narrower != previous.broader {
            return Err(DerivationDefect::Discontinuous {
                required: required.to_string(),
                index,
            });
        }
        let native = edge.source == EdgeSource::NativeOntology
            && edge.class == PremiseClass::Definition
            && agent_ontology::is_direct_broader(&edge.narrower, &edge.broader);
        if !native {
            return Err(DerivationDefect::EdgeNotInOntology {
                required: required.to_string(),
                index,
            });
        }
        previous = edge;
    }
    Ok(())
}

/// Re-check every derivation a record stores and its evidence class.
pub fn verify_record(record: &DecisionRecord) -> Result<(), DerivationDefect> {
    let candidates = record.inputs.candidates.as_slice();
    for derivation in &record.derivations {
        verify_coverage(derivation, candidates)?;
    }
    let derived = weakest(record_premise_classes(record));
    if derived != record.evidence_class {
        return Err(DerivationDefect::EvidenceClassMismatch {
            recorded: record.evidence_class,
            derived,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests;
