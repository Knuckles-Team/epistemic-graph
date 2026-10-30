//! Edit concentration over an RDF data graph (EG-DECISION-ENGINE-R173.3).
//!
//! One reading of the graph serves both paths: the native SHACL constraint
//! `kg:editConcentration true` (evaluated by [`crate::validate`]) and the
//! served `Decide` arm for `QuestionKind::EditConcentration`. Both call
//! [`judge_edit`], which reads the candidate edit, the concentration policy
//! node the edit names and every edit governed by that policy, then applies
//! `eg_types::decision::concentration::concentration_judgement`. The verdict
//! is therefore the same from `Decide` and from `ShaclValidate` by
//! construction.
//!
//! Vocabulary (`http://knuckles.team/kg#`, harness ontology):
//! an edit is a subject of `kg:governedByConcentrationPolicy` with
//! `kg:targetsDimension` (IRI), `kg:editRound` (integer), `kg:editStatus`
//! (`proposed` | `shipped` | `rejected` | `reverted`) and
//! `kg:judgedUnderRevision` (integer). The policy node carries
//! `kg:minShipped`, `kg:windowRounds` and `kg:concentrationRevision`.

use eg_rdf::oxrdf::{Graph, NamedNodeRef, NamedOrBlankNodeRef, Term, TermRef};
use eg_types::decision::concentration::{
    concentration_judgement, ConcentrationAbstention, ConcentrationEvidence, ConcentrationPolicy,
    ConcentrationVerdict, EditFact, EditStatus,
};

/// The KG namespace the harness ontology uses.
pub const KG: &str = "http://knuckles.team/kg#";
/// The shape parameter that turns the constraint on.
pub const EDIT_CONCENTRATION: &str = "http://knuckles.team/kg#editConcentration";
/// The constraint component IRI reported in a validation result.
pub const CC_EDIT_CONCENTRATION: &str =
    "http://knuckles.team/kg#EditConcentrationConstraintComponent";

const GOVERNED_BY: &str = "http://knuckles.team/kg#governedByConcentrationPolicy";
const TARGETS_DIMENSION: &str = "http://knuckles.team/kg#targetsDimension";
const EDIT_ROUND: &str = "http://knuckles.team/kg#editRound";
const EDIT_STATUS: &str = "http://knuckles.team/kg#editStatus";
const JUDGED_UNDER: &str = "http://knuckles.team/kg#judgedUnderRevision";
const MIN_SHIPPED: &str = "http://knuckles.team/kg#minShipped";
const WINDOW_ROUNDS: &str = "http://knuckles.team/kg#windowRounds";
const POLICY_REVISION: &str = "http://knuckles.team/kg#concentrationRevision";

/// Why an edit could not be judged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JudgeError {
    /// The node is not a well-formed edit fact; the detail names the field.
    MalformedEdit(String),
    /// A typed abstention (R173.4).
    Abstained(ConcentrationAbstention),
}

/// A judged edit: the verdict and what it counted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EditJudgement {
    pub verdict: ConcentrationVerdict,
    pub evidence: ConcentrationEvidence,
}

fn nn(iri: &str) -> NamedNodeRef<'_> {
    NamedNodeRef::new_unchecked(iri)
}

fn subject(term: &Term) -> Option<NamedOrBlankNodeRef<'_>> {
    match term {
        Term::NamedNode(n) => Some(NamedOrBlankNodeRef::NamedNode(n.as_ref())),
        Term::BlankNode(b) => Some(NamedOrBlankNodeRef::BlankNode(b.as_ref())),
        _ => None,
    }
}

fn term_key(term: &Term) -> String {
    match term {
        Term::NamedNode(n) => n.as_str().to_string(),
        other => other.to_string(),
    }
}

fn object(data: &Graph, node: &Term, predicate: &str) -> Option<Term> {
    let s = subject(node)?;
    data.object_for_subject_predicate(s, nn(predicate))
        .map(TermRef::into_owned)
}

fn literal<T: std::str::FromStr>(data: &Graph, node: &Term, predicate: &str) -> Option<T> {
    match object(data, node, predicate)? {
        Term::Literal(l) => l.value().trim().parse().ok(),
        _ => None,
    }
}

fn status(text: &str) -> Option<EditStatus> {
    match text {
        "proposed" => Some(EditStatus::Proposed),
        "shipped" => Some(EditStatus::Shipped),
        "rejected" | "reverted" => Some(EditStatus::Reverted),
        _ => None,
    }
}

/// One edit fact read from the graph.
fn edit_fact(data: &Graph, edit: &Term) -> Result<EditFact, JudgeError> {
    let missing = |field: &str| JudgeError::MalformedEdit(format!("{edit} has no valid {field}"));
    let dimension = match object(data, edit, TARGETS_DIMENSION) {
        Some(Term::NamedNode(n)) => n.as_str().to_string(),
        _ => return Err(missing("kg:targetsDimension")),
    };
    let status_text = match object(data, edit, EDIT_STATUS) {
        Some(Term::Literal(l)) => l.value().to_string(),
        _ => return Err(missing("kg:editStatus")),
    };
    Ok(EditFact {
        edit_id: term_key(edit),
        dimension_iri: dimension,
        round: literal(data, edit, EDIT_ROUND).ok_or_else(|| missing("kg:editRound"))?,
        status: status(&status_text).ok_or_else(|| missing("kg:editStatus"))?,
        policy_revision: literal(data, edit, JUDGED_UNDER)
            .ok_or_else(|| missing("kg:judgedUnderRevision"))?,
    })
}

/// The policy node, when it carries every field with positive bounds.
fn policy(data: &Graph, node: &Term) -> Option<ConcentrationPolicy> {
    let min_shipped: u32 = literal(data, node, MIN_SHIPPED)?;
    let window_rounds: u32 = literal(data, node, WINDOW_ROUNDS)?;
    if min_shipped == 0 || window_rounds == 0 {
        return None;
    }
    Some(ConcentrationPolicy {
        policy_iri: term_key(node),
        min_shipped,
        window_rounds,
        revision: literal(data, node, POLICY_REVISION)?,
    })
}

/// Judge one edit node of `data` (R173.3). The facts are every well-formed
/// edit governed by the same policy node; a malformed sibling is not a fact.
pub fn judge_edit(data: &Graph, edit: &Term) -> Result<EditJudgement, JudgeError> {
    let candidate = edit_fact(data, edit)?;
    let Some(policy_node) = object(data, edit, GOVERNED_BY) else {
        return Err(JudgeError::Abstained(
            ConcentrationAbstention::NoConcentrationPolicy,
        ));
    };
    let policy = policy(data, &policy_node);
    let facts: Vec<EditFact> = data
        .subjects_for_predicate_object(nn(GOVERNED_BY), policy_node.as_ref())
        .map(|s| Term::from(s.into_owned()))
        .filter(|s| term_key(s) != candidate.edit_id)
        .filter_map(|s| edit_fact(data, &s).ok())
        .collect();
    concentration_judgement(&facts, &candidate, policy.as_ref())
        .map(|(verdict, evidence)| EditJudgement { verdict, evidence })
        .map_err(JudgeError::Abstained)
}

/// Judge an edit named by its IRI.
pub fn judge_edit_iri(data: &Graph, edit_iri: &str) -> Result<EditJudgement, JudgeError> {
    let node = eg_rdf::oxrdf::NamedNode::new(edit_iri)
        .map_err(|error| JudgeError::MalformedEdit(format!("bad edit iri {edit_iri}: {error}")))?;
    judge_edit(data, &Term::NamedNode(node))
}

/// The SHACL message for a judged edit, `None` when it conforms (`keep`).
pub(crate) fn violation_message(result: &Result<EditJudgement, JudgeError>) -> Option<String> {
    use eg_types::decision::concentration::ConcentrationAction;
    match result {
        Ok(j) if j.verdict.action == ConcentrationAction::Keep => None,
        Ok(j) => Some(format!(
            "revert: {} shipped edits on {} in rounds {}..{} reach min_shipped {} (policy {} revision {})",
            j.verdict.count,
            j.verdict.dimension_iri,
            j.evidence.window_start,
            j.evidence.window_end,
            j.evidence.min_shipped,
            j.evidence.policy_iri,
            j.evidence.policy_revision,
        )),
        Err(JudgeError::Abstained(ConcentrationAbstention::NoConcentrationPolicy)) => {
            Some("no_concentration_policy".to_string())
        }
        Err(JudgeError::Abstained(ConcentrationAbstention::MixedPolicyRevision { revisions })) => {
            Some(format!("mixed_policy_revision: {revisions:?}"))
        }
        Err(JudgeError::MalformedEdit(detail)) => Some(format!("malformed edit: {detail}")),
    }
}
