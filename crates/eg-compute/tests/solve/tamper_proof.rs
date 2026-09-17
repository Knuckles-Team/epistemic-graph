//! The verifier rejects tampered incumbents, trees and duals.
//!
//! Base: a two-cycle vertex cover solved with room for a full tree, which has
//! branches, forced nodes, bound leaves and completion leaves.

use eg_compute::solve::model::{Model, RowId, VarId};
use eg_compute::solve::{
    verify, BoundProof, Certificate, DualEntry, Incumbent, LagrangeDual, LeafProof, ProofNode,
    Scalar, SolveStatus, StatusDefect, VerifyError,
};

use super::support::{model, odd_cycle_cover, rejection, solve_and_verify, wide_config};

fn base() -> (Model, Certificate) {
    let cover = model(&odd_cycle_cover(2, 5));
    let (certificate, _) = solve_and_verify(&cover, &wide_config());
    assert_eq!(certificate.status, SolveStatus::Optimal);
    (cover, certificate)
}

fn nodes(certificate: &mut Certificate) -> &mut Vec<ProofNode> {
    match &mut certificate.proof {
        BoundProof::Tree { nodes } => nodes,
        BoundProof::Root { .. } => panic!("the base certificate carries a tree"),
    }
}

fn position(certificate: &Certificate, wanted: fn(&ProofNode) -> bool) -> usize {
    let BoundProof::Tree { nodes } = &certificate.proof else {
        panic!("tree expected")
    };
    nodes
        .iter()
        .position(wanted)
        .expect("the base tree has this node kind")
}

fn is_branch(node: &ProofNode) -> bool {
    matches!(node, ProofNode::Branch { .. })
}

fn is_forced(node: &ProofNode) -> bool {
    matches!(node, ProofNode::Forced { .. })
}

fn is_dual_leaf(node: &ProofNode) -> bool {
    matches!(node, ProofNode::Leaf { proof: LeafProof::Bound { dual } } if !dual.entries.is_empty())
}

fn leaf_dual(certificate: &mut Certificate, at: usize) -> &mut LagrangeDual {
    match &mut nodes(certificate)[at] {
        ProofNode::Leaf {
            proof: LeafProof::Bound { dual } | LeafProof::Infeasible { dual },
        } => dual,
        other => panic!("not a leaf: {other:?}"),
    }
}

fn incumbent(certificate: &mut Certificate) -> &mut Incumbent {
    certificate
        .incumbent
        .as_mut()
        .expect("the base has an incumbent")
}

type Case = (&'static str, Box<dyn Fn(&mut Certificate)>, VerifyError);

fn case(
    name: &'static str,
    mutate: impl Fn(&mut Certificate) + 'static,
    expected: VerifyError,
) -> Case {
    (name, Box::new(mutate), expected)
}

fn assignment_cases(cover: &Model) -> Vec<Case> {
    let everything = vec![true; cover.variable_count()];
    let worse = Incumbent {
        objective: cover.objective_value(&everything).expect("full length"),
        selected: everything,
    };
    vec![
        case(
            "drop a selected vertex",
            |c| {
                let at = incumbent(c)
                    .selected
                    .iter()
                    .position(|&on| on)
                    .expect("non-empty cover");
                incumbent(c).selected[at] = false;
            },
            VerifyError::RowViolated { row: RowId(0) },
        ),
        case(
            "inflate the scalar objective",
            |c| {
                let scalar = &mut incumbent(c).objective.scalar;
                *scalar = Scalar::new(scalar.get() + 1);
            },
            VerifyError::ObjectiveMismatch,
        ),
        case(
            "shorten the selection",
            |c| {
                incumbent(c).selected.pop();
            },
            VerifyError::AssignmentLength {
                expected: 10,
                found: 9,
            },
        ),
        case(
            "claim a worse feasible cover is optimal",
            move |c| {
                c.incumbent = Some(worse.clone());
                c.lower_bound = Some(worse.objective.scalar);
            },
            VerifyError::Status(StatusDefect::BoundBelowIncumbent),
        ),
        case(
            "understate the lower bound",
            |c| {
                c.lower_bound = c.lower_bound.map(|bound| Scalar::new(bound.get() - 1));
            },
            VerifyError::Status(StatusDefect::LowerBoundMismatch),
        ),
        case(
            "claim more nodes than the budget",
            |c| {
                c.nodes_expanded = c.config.node_budget() + 1;
            },
            VerifyError::Status(StatusDefect::NodeCountMismatch),
        ),
    ]
}

fn tree_cases(certificate: &Certificate) -> Vec<Case> {
    let branch = position(certificate, is_branch);
    let forced = position(certificate, is_forced);
    let leaf = position(certificate, is_dual_leaf);
    vec![
        case(
            "drop the last node",
            |c| {
                nodes(c).pop();
            },
            VerifyError::TreeIncomplete,
        ),
        case(
            "append a leaf",
            |c| {
                let dual = LagrangeDual {
                    denominator: 1,
                    entries: Vec::new(),
                };
                nodes(c).push(ProofNode::Leaf {
                    proof: LeafProof::Bound { dual },
                });
            },
            VerifyError::TrailingNodes {
                node: count(certificate),
            },
        ),
        case(
            "branch on a missing variable",
            move |c| {
                nodes(c)[branch] = ProofNode::Branch {
                    var: VarId(99),
                    first: true,
                };
            },
            VerifyError::VariableOutOfRange {
                node: branch,
                var: VarId(99),
            },
        ),
        case(
            "flip a forced value",
            move |c| {
                if let ProofNode::Forced { value, .. } = &mut nodes(c)[forced] {
                    *value = !*value;
                }
            },
            VerifyError::ForcedUnjustified { node: forced },
        ),
        case(
            "force from an unrelated row",
            move |c| {
                if let ProofNode::Forced { var, row, .. } = &mut nodes(c)[forced] {
                    *row = RowId(((var.0 as usize / 5) * 5 + 7) as u32 % 10);
                }
            },
            VerifyError::ForcedUnjustified { node: forced },
        ),
        case(
            "zero denominator",
            move |c| {
                leaf_dual(c, leaf).denominator = 0;
            },
            VerifyError::DualMalformed { node: leaf },
        ),
        case(
            "negative multiplier on a covering row",
            move |c| {
                let entry = &mut leaf_dual(c, leaf).entries[0];
                entry.numerator = Scalar::new(-entry.numerator.get());
            },
            VerifyError::DualMalformed { node: leaf },
        ),
        case(
            "multiplier above the cap",
            move |c| {
                leaf_dual(c, leaf).entries[0].numerator = Scalar::new((1 << 56) + 1);
            },
            VerifyError::DualMalformed { node: leaf },
        ),
        case(
            "repeated dual row",
            move |c| {
                let dual = leaf_dual(c, leaf);
                let entry: DualEntry = dual.entries[0];
                dual.entries.insert(0, entry);
            },
            VerifyError::DualMalformed { node: leaf },
        ),
        case(
            "dual row out of range",
            move |c| {
                let dual = leaf_dual(c, leaf);
                dual.entries.push(DualEntry {
                    row: RowId(500),
                    numerator: Scalar::new(1),
                });
            },
            VerifyError::RowOutOfRange {
                node: leaf,
                row: RowId(500),
            },
        ),
    ]
}

/// Number of nodes in the base tree (the index an appended node gets).
fn count(certificate: &Certificate) -> usize {
    match &certificate.proof {
        BoundProof::Tree { nodes } => nodes.len(),
        BoundProof::Root { .. } => 0,
    }
}

#[test]
fn every_tampered_assignment_or_tree_is_rejected_with_its_defect() {
    let (cover, certificate) = base();
    let mut cases = assignment_cases(&cover);
    cases.extend(tree_cases(&certificate));
    for (name, mutate, expected) in cases {
        assert_eq!(
            rejection(&cover, &certificate, mutate.as_ref()),
            expected,
            "{name}"
        );
    }
}

#[test]
fn a_certificate_does_not_verify_against_another_model() {
    let (_, certificate) = base();
    let other = model(&odd_cycle_cover(1, 10));
    assert_eq!(
        verify(&other, &certificate),
        Err(VerifyError::ModelDigestMismatch)
    );
}

#[test]
fn the_verifier_shares_no_code_with_the_search() {
    let sources = [
        include_str!("../../src/solve/verify/mod.rs"),
        include_str!("../../src/solve/verify/dual.rs"),
        include_str!("../../src/solve/verify/error.rs"),
        include_str!("../../src/solve/verify/rows.rs"),
        include_str!("../../src/solve/verify/status.rs"),
        include_str!("../../src/solve/verify/tree.rs"),
    ];
    for source in sources {
        assert!(!source.contains("search::") && !source.contains("mod search"));
    }
}
