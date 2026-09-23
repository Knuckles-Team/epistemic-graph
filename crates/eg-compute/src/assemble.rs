//! The assembly decision function (DECIDE-LAYER-DESIGN §3, §7, WP A5).
//!
//! [`assemble`] is a pure function of a [`DecisionInputs`] value and the
//! record identity (tenant, caller, time) that is recorded but never read by
//! the math. It runs the resolution ladder in its ruled order:
//!
//! 1. **1a visibility** happened before this function: the inputs hold only
//!    the candidates the tenant-bound library read returned for the request's
//!    scope, and nothing outside them can appear in the answer.
//! 2. **1b constraints** ([`eliminate`]): denies, lifecycle, external agents,
//!    hard model facts and budgets remove candidates, each with its violation.
//! 3. **2 entailment** (`eg_types::decision::derivation`): the request's
//!    typed tasks close over the native vocabulary into required capabilities,
//!    and a candidate covers one only through a checkable `is_a` chain.
//! 4. **3 optimisation** ([`model`], [`objective`], [`conclude`]): the legal
//!    remainder becomes a bounded 0-1 programme under the policy's objective,
//!    solved by `crate::solve` with a verifiable certificate, then checked by
//!    the validators the solver does not encode, with a bounded no-good loop.
//! 5. Abstention with typed reasons whenever a step cannot decide.
//!
//! **Monotone safety** holds by construction: step 1b builds the legal set and
//! every later step reads only that set, so nothing after it can reinstate an
//! eliminated option; the solver can choose among legal options or fail.
//!
//! Everything is integer arithmetic over sorted inputs. Replaying a stored
//! record's inputs through [`assemble`] reproduces it byte for byte, which is
//! what [`replay_check`] asserts before a record may be committed.

mod agent;
mod conclude;
mod eliminate;
mod facts;
mod model;
mod objective;
mod seal;
mod search;
mod template;
mod validate;
mod why_not;

use eg_types::agent_graph::AgentGraphDraft;
use eg_types::agent_library::AgentLibraryEntryDraft;
use eg_types::decision::{DecisionErrorCode, DecisionInputs, DecisionOutcome, DecisionRecord};
use eg_types::solve::ModelSpec;

pub use seal::{encoded_len, RecordIdentity};

/// One decided assembly: the record, and -- only when it is `Solved` -- the
/// agent and graph drafts it proves plus the exact model its certificate
/// covers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Assembly {
    pub record: DecisionRecord,
    pub model: Option<ModelSpec>,
    /// One agent per slot; one for the one-agent graph.
    pub agents: Vec<AgentLibraryEntryDraft>,
    pub graph: Option<AgentGraphDraft>,
}

/// A typed refusal: the closed code a caller branches on, plus detail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssembleError {
    pub code: DecisionErrorCode,
    pub detail: String,
}

impl AssembleError {
    pub(crate) fn new(code: DecisionErrorCode, detail: impl Into<String>) -> Self {
        Self {
            code,
            detail: detail.into(),
        }
    }
}

impl std::fmt::Display for AssembleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code.as_str(), self.detail)
    }
}

impl std::error::Error for AssembleError {}

/// The complete inputs of one assembly over `candidates` (any order) under
/// `policy`: candidates sorted, and every digest computed from what it names.
pub fn inputs(
    request: eg_types::decision::AssemblyRequest,
    mut candidates: Vec<eg_types::decision::CandidateFacts>,
    templates: Vec<eg_types::decision::TemplateFacts>,
    policy: eg_types::decision::DecisionPolicy,
) -> Result<DecisionInputs, AssembleError> {
    use eg_types::decision::{digest, request::effective_solver_budget, SolverIdentity};
    candidates.sort_by(|a, b| a.component_id.cmp(&b.component_id));
    let node_budget = effective_solver_budget(&policy, &request).node_budget;
    Ok(DecisionInputs {
        request,
        catalog_digest: digest::candidates_catalog_digest(&candidates),
        candidates: eg_types::contract::BoundedVec::new(candidates).map_err(|error| {
            AssembleError::new(DecisionErrorCode::CandidateScopeTooLarge, error)
        })?,
        ontology_digest: eg_types::agent_ontology::ontology_digest(),
        policy_digest: digest::policy_digest(&policy),
        policy,
        solver: SolverIdentity {
            algorithm: eg_types::solve::Algorithm::DepthFirstDualAscent,
            node_budget,
        },
        templates: eg_types::contract::BoundedVec::new(templates)
            .map_err(|error| AssembleError::new(DecisionErrorCode::AssemblyInputsInvalid, error))?,
    })
}

/// Decide one assembly from its complete inputs.
pub fn assemble(
    inputs: DecisionInputs,
    identity: RecordIdentity,
) -> Result<Assembly, AssembleError> {
    validate::inputs(&inputs, &identity)?;
    let decided = conclude::decide(&inputs)?;
    seal::seal(inputs, identity, decided)
}

/// Re-derive a stored record from its own inputs and refuse it unless the
/// engine reaches exactly the same bytes and its certificate verifies.
///
/// This is the whole of what makes a record arriving on the wire safe to
/// commit: a caller cannot store an outcome the engine would not compute from
/// the inputs it stores, and cannot store inputs whose certificate does not
/// check. The candidate facts themselves are compared with the catalog by the
/// commit, which is the only place that can read it.
pub fn replay_check(record: &DecisionRecord) -> Result<Assembly, AssembleError> {
    let record = record
        .clone()
        .checked()
        .map_err(|code| AssembleError::new(code, "the record's version is not served"))?;
    eg_types::decision::derivation::verify_record(&record).map_err(|defect| {
        AssembleError::new(DecisionErrorCode::DerivationRejected, defect.to_string())
    })?;
    let again = assemble(record.inputs.clone(), RecordIdentity::of(&record))?;
    if again.record != record {
        return Err(AssembleError::new(
            DecisionErrorCode::DecisionReplayMismatch,
            "re-deriving the record from its stored inputs reached a different record",
        ));
    }
    verify_solved(&again)?;
    Ok(again)
}

/// Verify a solved record's certificate against the model re-built from its
/// inputs, independently of the search that produced it.
fn verify_solved(assembly: &Assembly) -> Result<(), AssembleError> {
    let DecisionOutcome::Solved { certificate, .. } = &assembly.record.outcome else {
        return Ok(());
    };
    let spec = assembly.model.clone().ok_or_else(|| {
        AssembleError::new(
            DecisionErrorCode::CertificateRejected,
            "a solved record has no model",
        )
    })?;
    let model = crate::solve::Model::try_from(spec).map_err(|error| {
        AssembleError::new(DecisionErrorCode::AssemblyModelInvalid, error.to_string())
    })?;
    match crate::solve::verify(&model, certificate) {
        Ok(verdict) if conclude::verdict_supports_solved(&verdict) => Ok(()),
        Ok(verdict) => Err(AssembleError::new(
            DecisionErrorCode::CertificateRejected,
            format!("the certificate proves {verdict:?}, not an assembly"),
        )),
        Err(error) => Err(AssembleError::new(
            DecisionErrorCode::CertificateRejected,
            format!("{error:?}"),
        )),
    }
}

#[cfg(test)]
mod tests;
