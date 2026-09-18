//! Valid sample values for the solver wire types.
//!
//! One tiny model with two variables, a covering row and an exactly-one row,
//! plus a certificate whose proof tree closes both branches. Small on purpose:
//! the point is that every field shape round-trips, not that the numbers mean
//! anything.

use crate::solve::{
    Algorithm, BoundProof, Certificate, Coefficient, ConstraintBody, ConstraintSpec, DualEntry,
    Incumbent, LagrangeDual, LeafProof, LevelValue, ModelSpec, ObjectiveLevelSpec, ObjectiveTerm,
    ObjectiveValue, ProofNode, Relation, RowId, Scalar, Sha256Digest, SolveRequest, SolveStatus,
    SolverConfig, SolverConfigSpec, Term, VarId,
};

/// A model with one of every constraint body shape.
pub fn model() -> ModelSpec {
    ModelSpec {
        variables: vec!["component-a".to_string(), "component-b".to_string()],
        constraints: vec![
            ConstraintSpec {
                label: "cover:eg:capability/retrieval".to_string(),
                body: ConstraintBody::AtLeast {
                    vars: vec![VarId(0), VarId(1)],
                    k: 1,
                },
            },
            ConstraintSpec {
                label: "slot:tool".to_string(),
                body: ConstraintBody::ExactlyOne {
                    vars: vec![VarId(0), VarId(1)],
                },
            },
            ConstraintSpec {
                label: "budget:components".to_string(),
                body: ConstraintBody::Linear {
                    terms: vec![
                        Term {
                            var: VarId(0),
                            coefficient: 1,
                        },
                        Term {
                            var: VarId(1),
                            coefficient: 1,
                        },
                    ],
                    relation: Relation::LessEqual,
                    rhs: 1,
                },
            },
        ],
        objective: vec![ObjectiveLevelSpec {
            label: "components".to_string(),
            terms: vec![
                ObjectiveTerm {
                    var: VarId(0),
                    coefficient: Coefficient::Known(1),
                },
                ObjectiveTerm {
                    var: VarId(1),
                    coefficient: Coefficient::Unknown,
                },
            ],
        }],
    }
}

/// Every other constraint body shape, so none of them is unexercised.
pub fn every_constraint_body() -> Vec<ConstraintBody> {
    vec![
        ConstraintBody::Implication {
            antecedent: VarId(0),
            consequent: VarId(1),
        },
        ConstraintBody::ImpliesAny {
            antecedent: VarId(0),
            consequents: vec![VarId(1)],
        },
        ConstraintBody::AtMost {
            vars: vec![VarId(0), VarId(1)],
            k: 1,
        },
        ConstraintBody::Fix {
            var: VarId(1),
            value: false,
        },
    ]
}

/// The default solver limits, in wire form.
pub fn config_spec() -> SolverConfigSpec {
    SolverConfigSpec::from(SolverConfig::default())
}

/// One request against [`model`].
pub fn request() -> SolveRequest {
    SolveRequest {
        model: model(),
        config: Some(config_spec()),
    }
}

fn dual(numerator: i128) -> LagrangeDual {
    LagrangeDual {
        denominator: 1,
        entries: vec![DualEntry {
            row: RowId(0),
            numerator: Scalar::new(numerator),
        }],
    }
}

/// A certificate whose pre-order tree carries every node and leaf shape.
pub fn certificate() -> Certificate {
    Certificate {
        model_digest: Sha256Digest::of_json("eg-solve/model/v1", &model()),
        algorithm: Algorithm::DepthFirstDualAscent,
        config: SolverConfig::default(),
        status: SolveStatus::Optimal,
        incumbent: Some(Incumbent {
            selected: vec![true, false],
            objective: ObjectiveValue {
                levels: vec![LevelValue {
                    known: Scalar::new(1),
                    unknown_selected: 0,
                }],
                scalar: Scalar::new(1),
            },
        }),
        lower_bound: Some(Scalar::new(1)),
        proof: BoundProof::Tree {
            nodes: vec![
                ProofNode::Branch {
                    var: VarId(0),
                    first: true,
                },
                ProofNode::Leaf {
                    proof: LeafProof::Bound { dual: dual(1) },
                },
                ProofNode::Forced {
                    var: VarId(1),
                    value: true,
                    row: RowId(1),
                },
                ProofNode::Leaf {
                    proof: LeafProof::Infeasible { dual: dual(2) },
                },
            ],
        },
        nodes_expanded: 4,
    }
}

/// Every other outcome class and the root-only proof shape.
pub fn every_solve_status() -> Vec<SolveStatus> {
    vec![
        SolveStatus::Optimal,
        SolveStatus::OptimalByDeterministicSearch { nodes_expanded: 9 },
        SolveStatus::FeasibleWithGap {
            gap: Scalar::new(3),
        },
        SolveStatus::Infeasible {
            core: vec![RowId(0), RowId(1)],
        },
        SolveStatus::InfeasibleByDeterministicSearch { nodes_expanded: 11 },
        SolveStatus::BudgetExhausted,
    ]
}

/// The root-dual proof shape.
pub fn root_proof() -> BoundProof {
    BoundProof::Root { dual: dual(5) }
}
