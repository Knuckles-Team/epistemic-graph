//! Worst cases for the search: loose bounds, parity, and certificate limits.

use eg_compute::solve::model::RowId;
use eg_compute::solve::{BoundProof, LeafProof, ProofNode, Scalar, SolveStatus, Verdict};

use super::support::{config, model, odd_cycle_cover, parity_equality, solve_and_verify};

#[test]
fn exhausted_budget_above_the_accepted_gap_is_unresolved() {
    let cover = model(&odd_cycle_cover(12, 5));
    let (certificate, verdict) = solve_and_verify(&cover, &config(200, 1 << 16, 0, 1));
    assert_eq!(certificate.status, SolveStatus::BudgetExhausted);
    assert_eq!(certificate.nodes_expanded, 200);
    let incumbent = certificate
        .incumbent
        .as_ref()
        .expect("greedy covers every cycle")
        .objective
        .scalar;
    assert_eq!(incumbent, Scalar::new(36));
    let lower_bound = certificate.lower_bound.expect("open leaves carry bounds");
    assert!(lower_bound < incumbent);
    assert_eq!(
        verdict,
        Verdict::Unresolved {
            incumbent: Some(incumbent),
            lower_bound: Some(lower_bound)
        }
    );
}

#[test]
fn exhausted_budget_within_the_accepted_gap_reports_a_certified_gap() {
    let cover = model(&odd_cycle_cover(12, 5));
    let (certificate, verdict) = solve_and_verify(&cover, &config(200, 1 << 16, 1_000, 1));
    let lower_bound = certificate
        .lower_bound
        .expect("open leaves carry bounds")
        .get();
    assert_eq!(
        certificate.status,
        SolveStatus::FeasibleWithGap {
            gap: Scalar::new(36 - lower_bound)
        }
    );
    assert_eq!(
        verdict,
        Verdict::ProvenGap {
            incumbent: Scalar::new(36),
            lower_bound: Scalar::new(lower_bound)
        }
    );
}

#[test]
fn parity_exhausts_the_budget_without_an_incumbent() {
    let parity = model(&parity_equality(41));
    let (certificate, verdict) = solve_and_verify(&parity, &config(500, 1 << 16, 0, 1));
    assert_eq!(certificate.status, SolveStatus::BudgetExhausted);
    assert_eq!(certificate.nodes_expanded, 500);
    assert!(certificate.incumbent.is_none());
    assert!(matches!(
        verdict,
        Verdict::Unresolved {
            incumbent: None,
            ..
        }
    ));
}

#[test]
fn small_parity_is_proven_infeasible_with_its_core() {
    let parity = model(&parity_equality(9));
    let (certificate, verdict) = solve_and_verify(&parity, &config(1_000_000, 1 << 16, 0, 1));
    assert_eq!(
        certificate.status,
        SolveStatus::Infeasible {
            core: vec![RowId(0)]
        }
    );
    assert_eq!(
        verdict,
        Verdict::ProvenInfeasible {
            core: vec![RowId(0)]
        }
    );
}

#[test]
fn infeasibility_beyond_the_certificate_limit_requires_replay() {
    let parity = model(&parity_equality(9));
    let (certificate, verdict) = solve_and_verify(&parity, &config(1_000_000, 2, 0, 1));
    let expanded = certificate.nodes_expanded;
    assert_eq!(
        certificate.status,
        SolveStatus::InfeasibleByDeterministicSearch {
            nodes_expanded: expanded
        }
    );
    assert!(matches!(certificate.proof, BoundProof::Root { .. }));
    assert_eq!(verdict, Verdict::InfeasibilityRequiresReplay);
}

#[test]
fn optimality_beyond_the_certificate_limit_requires_replay() {
    let cover = model(&odd_cycle_cover(4, 5));
    let (certificate, verdict) = solve_and_verify(&cover, &config(1_000_000, 1, 0, 1));
    let expanded = certificate.nodes_expanded;
    assert_eq!(
        certificate.status,
        SolveStatus::OptimalByDeterministicSearch {
            nodes_expanded: expanded
        }
    );
    assert!(
        matches!(verdict, Verdict::OptimalityRequiresReplay { objective, .. } if objective == Scalar::new(12))
    );
}

#[test]
fn a_rational_denominator_certifies_an_odd_cycle_at_the_root() {
    let cycle = model(&odd_cycle_cover(1, 41));
    let (integral, _) = solve_and_verify(&cycle, &config(1_000_000, 1 << 16, 0, 1));
    let (halves, verdict) = solve_and_verify(&cycle, &config(1_000_000, 1 << 16, 0, 2));
    assert_eq!(
        verdict,
        Verdict::ProvenOptimal {
            objective: Scalar::new(21)
        }
    );
    assert_eq!(halves.nodes_expanded, 1);
    assert!(integral.nodes_expanded > halves.nodes_expanded);
    let BoundProof::Tree { nodes } = &halves.proof else {
        panic!("a one-leaf tree fits any limit")
    };
    assert!(
        matches!(&nodes[..], [ProofNode::Leaf { proof: LeafProof::Bound { dual } }] if dual.denominator == 2)
    );
}
