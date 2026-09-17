//! Independent certificate verifier.
//!
//! The verifier trusts the [`Model`] (the problem statement) and nothing the
//! solver produced: it re-checks the incumbent row by row, recomputes its
//! objective, evaluates every dual itself with checked arithmetic, walks the
//! proof tree with its own path bookkeeping, and only then asks whether the
//! claimed status follows. It imports no part of the search, so a search
//! defect cannot be mirrored here; moving it next to the wire types (or into a
//! client) needs only the model and certificate types.

mod dual;
mod error;
mod rows;
mod status;
mod tree;

pub use error::{StatusDefect, Verdict, VerifyError};

use super::certificate::{BoundProof, Certificate, Incumbent};
use super::model::Model;

/// Verify `certificate` against `model`.
pub fn verify(model: &Model, certificate: &Certificate) -> Result<Verdict, VerifyError> {
    if certificate.model_digest != model.digest() {
        return Err(VerifyError::ModelDigestMismatch);
    }
    if certificate.nodes_expanded > certificate.config.node_budget() {
        return Err(VerifyError::Status(StatusDefect::NodeCountMismatch));
    }
    let incumbent = match &certificate.incumbent {
        Some(claimed) => Some(check_incumbent(model, claimed)?),
        None => None,
    };
    let (evidence, tree) = match &certificate.proof {
        BoundProof::Tree { nodes } => (tree::walk(model, nodes)?, true),
        BoundProof::Root { dual } => (tree::root(model, dual)?, false),
    };
    status::judge(
        certificate,
        &status::Verified {
            incumbent,
            evidence,
            tree,
        },
    )
}

/// A feasible selection whose claimed objective matches its recomputed one.
fn check_incumbent(model: &Model, claimed: &Incumbent) -> Result<i128, VerifyError> {
    rows::check_selection(model, &claimed.selected)?;
    match model.objective_value(&claimed.selected) {
        Some(value) if value == claimed.objective => Ok(value.scalar.get()),
        _ => Err(VerifyError::ObjectiveMismatch),
    }
}
