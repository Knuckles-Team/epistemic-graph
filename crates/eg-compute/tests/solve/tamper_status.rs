//! The verifier rejects status claims the evidence does not support.

use eg_compute::solve::model::{Model, ModelSpec};
use eg_compute::solve::{
    BoundProof, Certificate, Incumbent, LagrangeDual, LeafProof, ProofNode, Scalar, SolveStatus,
    SolverConfig, StatusDefect, VerifyError,
};

use super::support::{
    config, model, odd_cycle_cover, parity_equality, rejection, solve_and_verify,
};

fn solved(spec: &ModelSpec, solver: &SolverConfig) -> (Model, Certificate) {
    let instance = model(spec);
    let (certificate, _) = solve_and_verify(&instance, solver);
    (instance, certificate)
}

fn assert_rejects(
    base: &(Model, Certificate),
    mutate: &dyn Fn(&mut Certificate),
    expected: VerifyError,
) {
    assert_eq!(rejection(&base.0, &base.1, mutate), expected);
}

fn defect(defect: StatusDefect) -> VerifyError {
    VerifyError::Status(defect)
}

#[test]
fn infeasibility_claims_need_the_exact_core_and_true_infeasible_leaves() {
    let base = solved(&parity_equality(5), &config(1_000_000, 1 << 16, 0, 1));
    assert!(matches!(base.1.status, SolveStatus::Infeasible { .. }));
    assert_rejects(
        &base,
        &|c| c.status = SolveStatus::Infeasible { core: Vec::new() },
        defect(StatusDefect::CoreMismatch),
    );
    let nodes_expanded = base.1.nodes_expanded;
    let by_search = move |c: &mut Certificate| {
        c.status = SolveStatus::InfeasibleByDeterministicSearch { nodes_expanded }
    };
    assert_rejects(&base, &by_search, defect(StatusDefect::WrongProofKind));
    let fake = |c: &mut Certificate| {
        let selected = vec![false; 5];
        let objective = model(&parity_equality(5))
            .objective_value(&selected)
            .expect("full length");
        c.incumbent = Some(Incumbent {
            selected,
            objective,
        });
    };
    assert_rejects(
        &base,
        &fake,
        VerifyError::RowViolated {
            row: eg_compute::solve::model::RowId(0),
        },
    );
    let BoundProof::Tree { nodes } = &base.1.proof else {
        panic!("tree expected")
    };
    let leaf = nodes
        .iter()
        .position(|node| {
            matches!(
                node,
                ProofNode::Leaf {
                    proof: LeafProof::Infeasible { .. }
                }
            )
        })
        .expect("an infeasible leaf");
    let negate = move |c: &mut Certificate| {
        if let BoundProof::Tree { nodes } = &mut c.proof {
            if let ProofNode::Leaf {
                proof: LeafProof::Infeasible { dual },
            } = &mut nodes[leaf]
            {
                dual.entries[0].numerator = Scalar::new(-dual.entries[0].numerator.get());
            }
        }
    };
    assert_rejects(
        &base,
        &negate,
        VerifyError::LeafNotInfeasible { node: leaf },
    );
}

#[test]
fn gap_claims_must_match_the_bound_and_the_acceptance_policy() {
    let base = solved(&odd_cycle_cover(12, 5), &config(200, 1 << 16, 1_000, 1));
    let SolveStatus::FeasibleWithGap { gap } = base.1.status.clone() else {
        panic!("{:?}", base.1.status)
    };
    let widened = move |c: &mut Certificate| {
        c.status = SolveStatus::FeasibleWithGap {
            gap: Scalar::new(gap.get() + 1),
        }
    };
    assert_rejects(&base, &widened, defect(StatusDefect::GapMismatch));
    let strict = move |c: &mut Certificate| c.config = config(200, 1 << 16, gap.get() - 1, 1);
    assert_rejects(&base, &strict, defect(StatusDefect::GapNotAccepted));
    assert_rejects(
        &base,
        &|c| c.status = SolveStatus::BudgetExhausted,
        defect(StatusDefect::GapAccepted),
    );
    assert_rejects(
        &base,
        &|c| c.status = SolveStatus::Optimal,
        defect(StatusDefect::BoundBelowIncumbent),
    );
    // The optimum (36) lies in some leaf, whose bound is at most 36.
    let raised = |c: &mut Certificate| c.lower_bound = Some(Scalar::new(37));
    assert_rejects(&base, &raised, defect(StatusDefect::LowerBoundAboveProof));
}

#[test]
fn search_claims_need_a_root_bound_that_supports_the_reported_bound() {
    let base = solved(&odd_cycle_cover(4, 5), &config(1_000_000, 1, 0, 1));
    assert!(matches!(
        base.1.status,
        SolveStatus::OptimalByDeterministicSearch { .. }
    ));
    let empty = |c: &mut Certificate| {
        c.proof = BoundProof::Root {
            dual: LagrangeDual {
                denominator: 1,
                entries: Vec::new(),
            },
        }
    };
    assert_rejects(&base, &empty, defect(StatusDefect::LowerBoundAboveProof));
    assert_rejects(
        &base,
        &|c| c.status = SolveStatus::Optimal,
        defect(StatusDefect::BoundBelowIncumbent),
    );
    let miscounted = |c: &mut Certificate| {
        c.status = SolveStatus::OptimalByDeterministicSearch { nodes_expanded: 0 }
    };
    assert_rejects(&base, &miscounted, defect(StatusDefect::NodeCountMismatch));
    assert_rejects(
        &base,
        &|c| c.incumbent = None,
        defect(StatusDefect::MissingIncumbent),
    );
}
