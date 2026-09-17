//! Model, scalar and config validation, and exactness of the scalarisation.

use eg_compute::solve::model::{
    Coefficient, ConstraintBody, ConstraintSpec, Location, Model, ModelError, ModelSpec,
    ObjectiveLevelSpec, ObjectiveTerm, Relation, Term, VarId,
};
use eg_compute::solve::{ConfigError, Scalar, Sha256Digest, SolverConfig, SolverConfigSpec};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

use super::support::{level_key, model, random_model};

fn base() -> ModelSpec {
    let a = VarId(0);
    let b = VarId(1);
    ModelSpec {
        variables: vec!["a".into(), "b".into(), "c".into()],
        constraints: vec![
            ConstraintSpec {
                label: "budget".into(),
                body: ConstraintBody::Linear {
                    terms: vec![
                        Term {
                            var: a,
                            coefficient: 1,
                        },
                        Term {
                            var: b,
                            coefficient: 2,
                        },
                    ],
                    relation: Relation::LessEqual,
                    rhs: 2,
                },
            },
            ConstraintSpec {
                label: "pick".into(),
                body: ConstraintBody::ExactlyOne { vars: vec![a, b] },
            },
        ],
        objective: vec![ObjectiveLevelSpec {
            label: "cost".into(),
            terms: vec![
                ObjectiveTerm {
                    var: a,
                    coefficient: Coefficient::Known(3),
                },
                ObjectiveTerm {
                    var: b,
                    coefficient: Coefficient::Unknown,
                },
            ],
        }],
    }
}

fn body(spec: &mut ModelSpec, index: usize) -> &mut ConstraintBody {
    &mut spec.constraints[index].body
}

fn linear_terms(spec: &mut ModelSpec) -> &mut Vec<Term> {
    match body(spec, 0) {
        ConstraintBody::Linear { terms, .. } => terms,
        other => panic!("constraint 0 is linear, not {other:?}"),
    }
}

fn level(max: i64) -> ObjectiveLevelSpec {
    let terms = (0..3)
        .map(|v| ObjectiveTerm {
            var: VarId(v),
            coefficient: Coefficient::Known(max),
        })
        .collect();
    ObjectiveLevelSpec {
        label: "wide".into(),
        terms,
    }
}

type Case = (&'static str, fn(&mut ModelSpec), ModelError);

const CASES: [Case; 15] = [
    (
        "no variables",
        |s| {
            *s = ModelSpec {
                variables: Vec::new(),
                ..base()
            }
        },
        ModelError::NoVariables,
    ),
    (
        "empty name",
        |s| s.variables[0].clear(),
        ModelError::EmptyName { var: VarId(0) },
    ),
    (
        "duplicate name",
        |s| s.variables[2] = "a".into(),
        ModelError::DuplicateName { var: VarId(2) },
    ),
    (
        "long label",
        |s| s.constraints[0].label = "x".repeat(257),
        ModelError::LabelTooLong {
            location: Location::Constraint(0),
            bytes: 257,
            max: 256,
        },
    ),
    (
        "unknown variable",
        |s| {
            *body(s, 1) = ConstraintBody::ExactlyOne {
                vars: vec![VarId(9)],
            }
        },
        ModelError::UnknownVariable {
            location: Location::Constraint(1),
            var: VarId(9),
        },
    ),
    (
        "repeated variable",
        |s| {
            *body(s, 1) = ConstraintBody::ExactlyOne {
                vars: vec![VarId(0), VarId(0)],
            }
        },
        ModelError::DuplicateVariable {
            location: Location::Constraint(1),
            var: VarId(0),
        },
    ),
    (
        "zero coefficient",
        |s| linear_terms(s)[1].coefficient = 0,
        ModelError::ZeroCoefficient {
            location: Location::Constraint(0),
            var: VarId(1),
        },
    ),
    (
        "huge coefficient",
        |s| linear_terms(s)[0].coefficient = (1 << 40) + 1,
        ModelError::CoefficientOutOfRange {
            location: Location::Constraint(0),
            var: VarId(0),
            value: (1 << 40) + 1,
        },
    ),
    (
        "huge rhs",
        |s| {
            *body(s, 0) = ConstraintBody::Linear {
                terms: vec![Term {
                    var: VarId(0),
                    coefficient: 1,
                }],
                relation: Relation::Equal,
                rhs: -(1 << 53),
            }
        },
        ModelError::RhsOutOfRange {
            constraint: 0,
            value: -(1 << 53),
        },
    ),
    (
        "empty row",
        |s| linear_terms(s).clear(),
        ModelError::EmptyRow { constraint: 0 },
    ),
    (
        "cardinality above size",
        |s| {
            *body(s, 1) = ConstraintBody::AtLeast {
                vars: vec![VarId(0)],
                k: 2,
            }
        },
        ModelError::CardinalityOutOfRange {
            constraint: 1,
            k: 2,
            len: 1,
        },
    ),
    (
        "self implication",
        |s| {
            *body(s, 1) = ConstraintBody::ImpliesAny {
                antecedent: VarId(2),
                consequents: vec![VarId(1), VarId(2)],
            }
        },
        ModelError::SelfImplication {
            constraint: 1,
            var: VarId(2),
        },
    ),
    (
        "objective variable",
        |s| s.objective[0].terms[0].var = VarId(3),
        ModelError::UnknownVariable {
            location: Location::ObjectiveLevel(0),
            var: VarId(3),
        },
    ),
    (
        "too many levels",
        |s| s.objective = vec![level(1); 17],
        ModelError::TooManyObjectiveLevels { count: 17, max: 16 },
    ),
    (
        "scalarisation overflow",
        |s| s.objective = vec![level(1 << 40); 16],
        ModelError::ObjectiveRangeOverflow,
    ),
];

#[test]
fn every_invalid_spec_is_refused_with_its_typed_error() {
    assert!(Model::try_from(base()).is_ok());
    for (name, mutate, expected) in CASES {
        let mut spec = base();
        mutate(&mut spec);
        assert_eq!(Model::try_from(spec), Err(expected), "{name}");
    }
}

#[test]
fn wire_types_refuse_unknown_fields_and_non_canonical_scalars() {
    let mut value = serde_json::to_value(base()).expect("encode");
    value["extra"] = serde_json::json!(1);
    assert!(serde_json::from_value::<Model>(value).is_err());
    for text in ["01", "+5", "1.0", "", " 7"] {
        assert!(
            serde_json::from_value::<Scalar>(serde_json::json!(text)).is_err(),
            "{text:?}"
        );
    }
    assert_eq!(
        serde_json::from_value::<Scalar>(serde_json::json!(
            "-170141183460469231731687303715884105728"
        ))
        .expect("i128::MIN is a canonical decimal string")
        .get(),
        i128::MIN
    );
    let upper = "AB".repeat(32);
    assert!(serde_json::from_value::<Sha256Digest>(serde_json::json!(upper)).is_err());
    assert!(serde_json::from_value::<Sha256Digest>(serde_json::json!("ab".repeat(32))).is_ok());
}

#[test]
fn solver_configs_are_validated() {
    let valid = SolverConfigSpec {
        node_budget: 1,
        max_certificate_leaves: 1,
        bound_denominator: 1,
        accepted_gap: Scalar::new(0),
    };
    let cases = [
        (
            SolverConfigSpec {
                node_budget: 0,
                ..valid
            },
            ConfigError::NodeBudgetOutOfRange { value: 0 },
        ),
        (
            SolverConfigSpec {
                max_certificate_leaves: 0,
                ..valid
            },
            ConfigError::CertificateLeavesOutOfRange { value: 0 },
        ),
        (
            SolverConfigSpec {
                bound_denominator: (1 << 16) + 1,
                ..valid
            },
            ConfigError::DenominatorOutOfRange {
                value: (1 << 16) + 1,
            },
        ),
        (
            SolverConfigSpec {
                accepted_gap: Scalar::new(-1),
                ..valid
            },
            ConfigError::NegativeAcceptedGap,
        ),
    ];
    assert!(SolverConfig::try_from(valid).is_ok());
    for (spec, expected) in cases {
        assert_eq!(SolverConfig::try_from(spec), Err(expected));
    }
}

#[test]
fn the_scalar_objective_orders_selections_exactly_lexicographically() {
    for seed in 0..60u64 {
        let spec = random_model(seed, 12, 1);
        let instance = model(&spec);
        let mut rng = ChaCha8Rng::seed_from_u64(seed ^ 0x5eed);
        for _ in 0..200 {
            let left: Vec<bool> = (0..12).map(|_| rng.gen_bool(0.5)).collect();
            let right: Vec<bool> = (0..12).map(|_| rng.gen_bool(0.5)).collect();
            let scalar = |s: &[bool]| instance.objective_value(s).expect("full length").scalar;
            let by_levels = level_key(&spec, &left).cmp(&level_key(&spec, &right));
            assert_eq!(scalar(&left).cmp(&scalar(&right)), by_levels, "seed {seed}");
        }
    }
    assert_eq!(model(&base()).objective_value(&[true]), None);
}
