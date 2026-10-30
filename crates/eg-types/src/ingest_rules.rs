//! Pure admission and refusal rules for the ingest worker's typed records.
//!
//! Every function here decides before any write: a refusal is a value, so a
//! refused item can never leave partial state behind. The served wiring that
//! calls these rules is carried by the rows that own the wire methods.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use crate::extracted_fact::{CandidateClaim, ExtractedFact, GroundingResult};
use crate::knowledge_atom::KnowledgeAtom;
use crate::ontology_proposal::{
    score_proposal, OntologyProposal, ProposalCheck, ProposalState, ProposalThresholds,
};
use crate::source_governance::{Classification, RecordGovernance, SourceContainer, SourceGroup};

/// Deepest container nesting a source tree may carry.
pub const MAX_CONTAINER_DEPTH: usize = 32;
/// Most members one source group may hold.
pub const MAX_GROUP_MEMBERS: usize = 100_000;
/// Share of a source's held records one reconcile plan may delete (percent).
pub const MASS_DELETE_PERCENT: usize = 20;

/// A stable refusal code with bounded detail.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum IngestRefusal {
    #[error("SOURCE_GOVERNANCE_CONFLICT: classification {classification:?} disagrees with access_public={access_public}")]
    GovernanceConflict {
        classification: Classification,
        access_public: bool,
    },
    #[error("SOURCE_RETENTION_MISSING: retention is required by policy")]
    RetentionMissing,
    #[error("ROW_VERSION_INVALID: {0}")]
    RowVersionInvalid(String),
    #[error("SOURCE_RECORD_UNKNOWN: {0}")]
    RecordUnknown(String),
    #[error("SOURCE_GROUP_TOO_LARGE: {members} members exceed {MAX_GROUP_MEMBERS}")]
    GroupTooLarge { members: usize },
    #[error("SOURCE_MASS_DELETE_GUARD: {missing} of {held} held records would be deleted")]
    MassDeleteGuard { missing: usize, held: usize },
    #[error("SOURCE_CONTAINER_CYCLE: {0}")]
    ContainerCycle(String),
    #[error("SOURCE_CONTAINER_TOO_DEEP: {0}")]
    ContainerTooDeep(String),
    #[error("PROPOSAL_NOT_ADMITTED: failed {0:?} check")]
    ProposalNotAdmitted(ProposalCheck),
    #[error("ATOM_PROVENANCE_MISSING: atom {0} has no provenance key")]
    AtomProvenanceMissing(String),
}

impl IngestRefusal {
    /// The stable wire code, the text before the first colon of the message.
    pub fn code(&self) -> String {
        let text = self.to_string();
        text.split(':').next().unwrap_or_default().to_string()
    }
}

impl RecordGovernance {
    /// Admit a record's governance, or refuse it before anything is written.
    /// A public classification must match `access_public`, and a policy that
    /// requires retention refuses a record that sent none.
    pub fn admit(&self, retention_required: bool) -> Result<(), IngestRefusal> {
        if (self.classification == Classification::Public) != self.access_public {
            return Err(IngestRefusal::GovernanceConflict {
                classification: self.classification,
                access_public: self.access_public,
            });
        }
        if retention_required && self.retention.is_none() {
            return Err(IngestRefusal::RetentionMissing);
        }
        Ok(())
    }
}

/// What a valid row version means for the held evidence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RowVersionDecision {
    First,
    Supersedes,
    Replay,
}

/// Validate an incoming row version against the highest stored one. Only
/// non-negative integers are accepted, and a version may never decrease.
pub fn check_row_version(
    stored: Option<u64>,
    incoming: &serde_json::Value,
) -> Result<RowVersionDecision, IngestRefusal> {
    let version = incoming.as_u64().ok_or_else(|| {
        IngestRefusal::RowVersionInvalid(format!("version {incoming} is not an integer"))
    })?;
    match stored {
        None => Ok(RowVersionDecision::First),
        Some(held) if version > held => Ok(RowVersionDecision::Supersedes),
        Some(held) if version == held => Ok(RowVersionDecision::Replay),
        Some(held) => Err(IngestRefusal::RowVersionInvalid(format!(
            "version {version} is below stored version {held}"
        ))),
    }
}

/// Refuse an access update for a record the engine does not hold.
pub fn check_access_target(held: &BTreeSet<String>, record_ref: &str) -> Result<(), IngestRefusal> {
    if held.contains(record_ref) {
        Ok(())
    } else {
        Err(IngestRefusal::RecordUnknown(record_ref.to_string()))
    }
}

/// Refuse a group whose member set is past the bound.
pub fn check_group_size(group: &SourceGroup) -> Result<(), IngestRefusal> {
    if group.members.len() > MAX_GROUP_MEMBERS {
        return Err(IngestRefusal::GroupTooLarge {
            members: group.members.len(),
        });
    }
    Ok(())
}

/// Plan the deletions a source enumeration implies. An incomplete enumeration
/// never deletes, so it yields an empty plan. A plan that would delete more
/// than 20 percent of the held records is refused unless an admin overrides.
pub fn plan_reconcile(
    held: &BTreeSet<String>,
    enumerated: &BTreeSet<String>,
    complete: bool,
    admin_override: bool,
) -> Result<BTreeSet<String>, IngestRefusal> {
    if !complete {
        return Ok(BTreeSet::new());
    }
    let missing: BTreeSet<String> = held.difference(enumerated).cloned().collect();
    if !admin_override && missing.len() * 100 > held.len() * MASS_DELETE_PERCENT {
        return Err(IngestRefusal::MassDeleteGuard {
            missing: missing.len(),
            held: held.len(),
        });
    }
    Ok(missing)
}

/// Validate a container batch: no parent cycle and nesting within the bound.
/// A parent outside the batch is a root of this batch.
pub fn check_container_tree(containers: &[SourceContainer]) -> Result<(), IngestRefusal> {
    let parents: BTreeMap<&str, Option<&str>> = containers
        .iter()
        .map(|c| (c.container_id.as_str(), c.parent_id.as_deref()))
        .collect();
    for start in parents.keys() {
        let mut seen = HashSet::new();
        let mut cursor = Some(*start);
        let mut depth = 0usize;
        while let Some(id) = cursor {
            if !seen.insert(id) {
                return Err(IngestRefusal::ContainerCycle((*start).to_string()));
            }
            depth += 1;
            if depth > MAX_CONTAINER_DEPTH {
                return Err(IngestRefusal::ContainerTooDeep((*start).to_string()));
            }
            cursor = parents.get(id).copied().flatten();
        }
    }
    Ok(())
}

/// The ontology cache the grounder consults: predicate to (domain, range)
/// classes, and entity name to class.
#[derive(Clone, Debug, Default)]
pub struct GroundingCache {
    pub predicates: BTreeMap<String, (String, String)>,
    pub classes: BTreeMap<String, String>,
}

/// Ground one fact. Direction is repaired when only the swapped pair satisfies
/// the predicate's domain and range; a fact that fits neither way stays an
/// ungrounded claim.
pub fn ground_fact(fact: ExtractedFact, cache: &GroundingCache) -> CandidateClaim {
    let grounding = ground(&fact, cache);
    CandidateClaim { fact, grounding }
}

fn ground(fact: &ExtractedFact, cache: &GroundingCache) -> GroundingResult {
    let Some((domain, range)) = cache.predicates.get(&fact.predicate) else {
        return GroundingResult::Ungrounded;
    };
    let subject = cache.classes.get(&fact.subject);
    let object = cache.classes.get(&fact.object);
    let fits = |s: Option<&String>, o: Option<&String>| s == Some(domain) && o == Some(range);
    if fits(subject, object) {
        grounded(domain, &fact.predicate, range, false)
    } else if fits(object, subject) {
        grounded(domain, &fact.predicate, range, true)
    } else {
        GroundingResult::Ungrounded
    }
}

fn grounded(domain: &str, property: &str, range: &str, repaired: bool) -> GroundingResult {
    GroundingResult::Grounded {
        subject_class: domain.to_string(),
        property: property.to_string(),
        object_class: range.to_string(),
        direction_repaired: repaired,
    }
}

/// Incremental duplicate filter: an O(1) hash-set probe per fact.
#[derive(Debug, Default)]
pub struct FactDeduper {
    seen: HashSet<(String, String, String)>,
}

impl FactDeduper {
    /// True when the fact is new; false when an equal triple was seen before.
    pub fn admit(&mut self, fact: &ExtractedFact) -> bool {
        self.seen.insert((
            fact.subject.clone(),
            fact.predicate.clone(),
            fact.object.clone(),
        ))
    }
}

/// The result of judging an induced proposal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdmissionDecision {
    pub state: ProposalState,
    /// Instances may populate against a proposal only once it is admitted.
    pub instances_allowed: bool,
}

/// Judge a proposal. A failing check refuses with `PROPOSAL_NOT_ADMITTED` and
/// the failing check; the caller leaves the proposal a draft.
pub fn judge_proposal(
    proposal: &OntologyProposal,
    observed: &[String],
    thresholds: &ProposalThresholds,
) -> Result<AdmissionDecision, IngestRefusal> {
    match score_proposal(proposal, observed).failing_check(thresholds) {
        Some(check) => Err(IngestRefusal::ProposalNotAdmitted(check)),
        None => Ok(AdmissionDecision {
            state: ProposalState::Admitted,
            instances_allowed: true,
        }),
    }
}

/// Refuse an atom that carries no provenance key; such an atom is not stored.
pub fn check_atom_provenance(atom: &KnowledgeAtom) -> Result<(), IngestRefusal> {
    if atom.provenance_key.trim().is_empty() {
        return Err(IngestRefusal::AtomProvenanceMissing(atom.atom_id.clone()));
    }
    Ok(())
}

/// Select the atoms whose text shares a term with the query, best first, up to
/// a token budget (whitespace words). No relevant atom yields an empty context.
pub fn select_atoms<'a>(
    query: &str,
    atoms: &'a [KnowledgeAtom],
    token_budget: usize,
) -> Vec<&'a KnowledgeAtom> {
    let terms: HashSet<String> = query.split_whitespace().map(str::to_lowercase).collect();
    let mut scored: Vec<(usize, &KnowledgeAtom)> = atoms
        .iter()
        .map(|atom| {
            let hits = atom
                .text
                .split_whitespace()
                .filter(|word| terms.contains(&word.to_lowercase()))
                .count();
            (hits, atom)
        })
        .filter(|(hits, _)| *hits > 0)
        .collect();
    scored.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.atom_id.cmp(&b.1.atom_id)));
    let mut used = 0usize;
    let mut chosen = Vec::new();
    for (_, atom) in scored {
        let tokens = atom.text.split_whitespace().count();
        if used + tokens > token_budget {
            break;
        }
        used += tokens;
        chosen.push(atom);
    }
    chosen
}
