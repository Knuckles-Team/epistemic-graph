//! Does the claimed status follow from the verified evidence?

use super::error::{StatusDefect, Verdict, VerifyError};
use super::tree::Evidence;
use crate::solve::certificate::{Certificate, SolveStatus};
use crate::solve::model::RowId;
use crate::solve::scalar::Scalar;

/// Facts established independently of the claimed status.
pub(super) struct Verified {
    /// The incumbent's recomputed scalar objective, when it is feasible.
    pub(super) incumbent: Option<i128>,
    pub(super) evidence: Evidence,
    /// Whether the proof was a full tree (rather than a root dual).
    pub(super) tree: bool,
}

fn require(condition: bool, defect: StatusDefect) -> Result<(), VerifyError> {
    if condition {
        Ok(())
    } else {
        Err(VerifyError::Status(defect))
    }
}

pub(super) fn judge(
    certificate: &Certificate,
    verified: &Verified,
) -> Result<Verdict, VerifyError> {
    let claimed = certificate.lower_bound.map(Scalar::get);
    match &certificate.status {
        SolveStatus::Optimal => optimal(claimed, verified),
        SolveStatus::OptimalByDeterministicSearch { nodes_expanded } => {
            require(
                *nodes_expanded == certificate.nodes_expanded,
                StatusDefect::NodeCountMismatch,
            )?;
            optimal_by_search(claimed, verified)
        }
        SolveStatus::FeasibleWithGap { gap } => with_gap(certificate, gap.get(), verified),
        SolveStatus::Infeasible { core } => infeasible(claimed, core, verified),
        SolveStatus::InfeasibleByDeterministicSearch { nodes_expanded } => {
            require(
                *nodes_expanded == certificate.nodes_expanded,
                StatusDefect::NodeCountMismatch,
            )?;
            infeasible_by_search(claimed, verified)
        }
        SolveStatus::BudgetExhausted => exhausted(certificate, claimed, verified),
    }
}

fn incumbent(verified: &Verified) -> Result<i128, VerifyError> {
    verified
        .incumbent
        .ok_or(VerifyError::Status(StatusDefect::MissingIncumbent))
}

/// A claimed lower bound that the verified leaves support.
fn supported_bound(claimed: Option<i128>, verified: &Verified) -> Result<i128, VerifyError> {
    let bound = claimed.ok_or(VerifyError::Status(StatusDefect::LowerBoundMismatch))?;
    let proven = verified
        .evidence
        .min_bound
        .ok_or(VerifyError::Status(StatusDefect::NoBoundLeaf))?;
    require(bound <= proven, StatusDefect::LowerBoundAboveProof)?;
    Ok(bound)
}

fn optimal(claimed: Option<i128>, verified: &Verified) -> Result<Verdict, VerifyError> {
    let best = incumbent(verified)?;
    let proven = verified
        .evidence
        .min_bound
        .ok_or(VerifyError::Status(StatusDefect::NoBoundLeaf))?;
    require(proven >= best, StatusDefect::BoundBelowIncumbent)?;
    require(claimed == Some(best), StatusDefect::LowerBoundMismatch)?;
    Ok(Verdict::ProvenOptimal {
        objective: Scalar::new(best),
    })
}

fn optimal_by_search(claimed: Option<i128>, verified: &Verified) -> Result<Verdict, VerifyError> {
    require(!verified.tree, StatusDefect::WrongProofKind)?;
    let best = incumbent(verified)?;
    let bound = supported_bound(claimed, verified)?;
    require(bound <= best, StatusDefect::LowerBoundMismatch)?;
    Ok(Verdict::OptimalityRequiresReplay {
        objective: Scalar::new(best),
        lower_bound: Scalar::new(bound),
    })
}

fn with_gap(
    certificate: &Certificate,
    gap: i128,
    verified: &Verified,
) -> Result<Verdict, VerifyError> {
    let budget = certificate.config.node_budget();
    require(
        certificate.nodes_expanded == budget,
        StatusDefect::NodeCountMismatch,
    )?;
    let best = incumbent(verified)?;
    let bound = supported_bound(certificate.lower_bound.map(Scalar::get), verified)?;
    require(
        gap > 0 && best.checked_sub(bound) == Some(gap),
        StatusDefect::GapMismatch,
    )?;
    require(
        gap <= certificate.config.accepted_gap(),
        StatusDefect::GapNotAccepted,
    )?;
    Ok(Verdict::ProvenGap {
        incumbent: Scalar::new(best),
        lower_bound: Scalar::new(bound),
    })
}

fn no_incumbent_no_bound(claimed: Option<i128>, verified: &Verified) -> Result<(), VerifyError> {
    require(
        verified.incumbent.is_none(),
        StatusDefect::UnexpectedIncumbent,
    )?;
    require(claimed.is_none(), StatusDefect::LowerBoundMismatch)
}

fn infeasible(
    claimed: Option<i128>,
    core: &[RowId],
    verified: &Verified,
) -> Result<Verdict, VerifyError> {
    no_incumbent_no_bound(claimed, verified)?;
    require(verified.tree, StatusDefect::WrongProofKind)?;
    require(
        verified.evidence.bound_leaves == 0,
        StatusDefect::WrongProofKind,
    )?;
    require(
        core == verified.evidence.infeasibility_rows,
        StatusDefect::CoreMismatch,
    )?;
    Ok(Verdict::ProvenInfeasible {
        core: core.to_vec(),
    })
}

fn infeasible_by_search(
    claimed: Option<i128>,
    verified: &Verified,
) -> Result<Verdict, VerifyError> {
    no_incumbent_no_bound(claimed, verified)?;
    require(!verified.tree, StatusDefect::WrongProofKind)?;
    Ok(Verdict::InfeasibilityRequiresReplay)
}

fn exhausted(
    certificate: &Certificate,
    claimed: Option<i128>,
    verified: &Verified,
) -> Result<Verdict, VerifyError> {
    let budget = certificate.config.node_budget();
    require(
        certificate.nodes_expanded == budget,
        StatusDefect::NodeCountMismatch,
    )?;
    let bound = match claimed {
        Some(_) => Some(supported_bound(claimed, verified)?),
        None => None,
    };
    if let (Some(best), Some(bound)) = (verified.incumbent, bound) {
        let gap = best.checked_sub(bound).unwrap_or(i128::MAX);
        require(
            gap > certificate.config.accepted_gap(),
            StatusDefect::GapAccepted,
        )?;
    }
    Ok(Verdict::Unresolved {
        incumbent: verified.incumbent.map(Scalar::new),
        lower_bound: bound.map(Scalar::new),
    })
}
