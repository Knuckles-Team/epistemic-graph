//! Shared solver-test fixtures: configs, an exhaustive oracle that reads the
//! typed spec directly (not the lowered rows or the scalar weights), random
//! general models, and small structured instances.

use eg_compute::solve::model::{
    Coefficient, ConstraintBody, ConstraintSpec, Model, ModelSpec, ObjectiveLevelSpec,
    ObjectiveTerm, Relation, Term, VarId,
};
use eg_compute::solve::{
    solve, verify, Certificate, Scalar, SolverConfig, SolverConfigSpec, Verdict, VerifyError,
};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

use super::generators::shuffled;

/// Per level, `(selected unknown-cost variables, known sum)`: compared
/// lexicographically this is the objective order, independent of scalarisation.
pub type LevelKey = Vec<(u32, i128)>;

pub fn config(
    node_budget: u64,
    max_certificate_leaves: u32,
    accepted_gap: i128,
    bound_denominator: u64,
) -> SolverConfig {
    let spec = SolverConfigSpec {
        node_budget,
        max_certificate_leaves,
        bound_denominator,
        accepted_gap: Scalar::new(accepted_gap),
    };
    SolverConfig::try_from(spec).expect("test configs are valid")
}

/// Enough budget and certificate room to finish and certify small models.
pub fn wide_config() -> SolverConfig {
    config(
        eg_compute::solve::certificate::MAX_NODE_BUDGET,
        1 << 20,
        0,
        1,
    )
}

pub fn model(spec: &ModelSpec) -> Model {
    Model::try_from(spec.clone()).expect("fixture specs are valid")
}

/// Solve, then verify; a verifier rejection of solver output fails the test.
pub fn solve_and_verify(model: &Model, config: &SolverConfig) -> (Certificate, Verdict) {
    let certificate = solve(model, config);
    let verdict = verify(model, &certificate).unwrap_or_else(|error| {
        panic!("verifier rejected solver output: {error}: {certificate:?}")
    });
    (certificate, verdict)
}

/// Apply `mutate` to a copy of `certificate` and return the verifier's rejection.
pub fn rejection(
    model: &Model,
    certificate: &Certificate,
    mutate: &dyn Fn(&mut Certificate),
) -> VerifyError {
    let mut tampered = certificate.clone();
    mutate(&mut tampered);
    verify(model, &tampered).expect_err("a tampered certificate must be rejected")
}

fn selected(mask: u64, var: VarId) -> bool {
    (mask >> var.0) & 1 == 1
}

fn count_on(mask: u64, vars: &[VarId]) -> i64 {
    vars.iter().filter(|&&var| selected(mask, var)).count() as i64
}

fn body_satisfied(body: &ConstraintBody, mask: u64) -> bool {
    match body {
        ConstraintBody::Linear {
            terms,
            relation,
            rhs,
        } => {
            let lhs: i64 = terms
                .iter()
                .filter(|t| selected(mask, t.var))
                .map(|t| t.coefficient)
                .sum();
            match relation {
                Relation::GreaterEqual => lhs >= *rhs,
                Relation::Equal => lhs == *rhs,
                Relation::LessEqual => lhs <= *rhs,
            }
        }
        ConstraintBody::Implication {
            antecedent,
            consequent,
        } => !selected(mask, *antecedent) || selected(mask, *consequent),
        ConstraintBody::ImpliesAny {
            antecedent,
            consequents,
        } => !selected(mask, *antecedent) || count_on(mask, consequents) > 0,
        ConstraintBody::ExactlyOne { vars } => count_on(mask, vars) == 1,
        ConstraintBody::AtMost { vars, k } => count_on(mask, vars) <= i64::from(*k),
        ConstraintBody::AtLeast { vars, k } => count_on(mask, vars) >= i64::from(*k),
        ConstraintBody::Fix { var, value } => selected(mask, *var) == *value,
    }
}

fn mask_key(spec: &ModelSpec, mask: u64) -> LevelKey {
    spec.objective
        .iter()
        .map(|level| {
            level
                .terms
                .iter()
                .filter(|term| selected(mask, term.var))
                .fold((0, 0), |(unknown, known), term| match term.coefficient {
                    Coefficient::Unknown => (unknown + 1, known),
                    Coefficient::Known(value) => (unknown, known + i128::from(value)),
                })
        })
        .collect()
}

fn to_mask(selection: &[bool]) -> u64 {
    selection
        .iter()
        .enumerate()
        .filter(|(_, &on)| on)
        .fold(0, |mask, (var, _)| mask | (1 << var))
}

/// The level key of a selection.
pub fn level_key(spec: &ModelSpec, selection: &[bool]) -> LevelKey {
    mask_key(spec, to_mask(selection))
}

/// The best level key over all feasible selections, or `None` when infeasible.
pub fn exhaustive_optimum(spec: &ModelSpec) -> Option<LevelKey> {
    let variables = spec.variables.len();
    assert!(
        variables <= 20,
        "exhaustive oracle is for at most 20 variables"
    );
    (0u64..1 << variables)
        .filter(|&mask| {
            spec.constraints
                .iter()
                .all(|c| body_satisfied(&c.body, mask))
        })
        .map(|mask| mask_key(spec, mask))
        .min()
}

fn distinct(rng: &mut ChaCha8Rng, variables: usize, max: usize) -> Vec<VarId> {
    let take = rng.gen_range(1..=max.min(variables));
    shuffled(variables, rng)
        .into_iter()
        .take(take)
        .map(|v| VarId(v as u32))
        .collect()
}

fn random_body(rng: &mut ChaCha8Rng, variables: usize) -> ConstraintBody {
    let vars = distinct(rng, variables, 4);
    let len = vars.len() as u32;
    match rng.gen_range(0..7) {
        0 => random_linear(rng, vars),
        1 if len >= 2 => ConstraintBody::Implication {
            antecedent: vars[0],
            consequent: vars[1],
        },
        2 if len >= 2 => ConstraintBody::ImpliesAny {
            antecedent: vars[0],
            consequents: vars[1..].to_vec(),
        },
        3 => ConstraintBody::ExactlyOne { vars },
        4 => ConstraintBody::AtMost {
            k: rng.gen_range(0..=len),
            vars,
        },
        5 => ConstraintBody::AtLeast {
            k: rng.gen_range(1..=len),
            vars,
        },
        _ => ConstraintBody::Fix {
            var: vars[0],
            value: rng.gen_bool(0.5),
        },
    }
}

fn random_linear(rng: &mut ChaCha8Rng, vars: Vec<VarId>) -> ConstraintBody {
    let terms = vars
        .into_iter()
        .map(|var| Term {
            var,
            coefficient: [-5, -3, -2, -1, 1, 2, 3, 5][rng.gen_range(0..8)],
        })
        .collect();
    let relation =
        [Relation::LessEqual, Relation::GreaterEqual, Relation::Equal][rng.gen_range(0..3)];
    ConstraintBody::Linear {
        terms,
        relation,
        rhs: rng.gen_range(-4..=6),
    }
}

fn random_level(rng: &mut ChaCha8Rng, variables: usize, index: usize) -> ObjectiveLevelSpec {
    let terms = distinct(rng, variables, variables)
        .into_iter()
        .map(|var| {
            let coefficient = if rng.gen_range(0..10) == 0 {
                Coefficient::Unknown
            } else {
                Coefficient::Known(rng.gen_range(-9..=9))
            };
            ObjectiveTerm { var, coefficient }
        })
        .collect();
    ObjectiveLevelSpec {
        label: format!("level-{index}"),
        terms,
    }
}

/// A random general model: every constraint kind, mixed-sign coefficients,
/// unknown costs and up to three objective levels.
pub fn random_model(seed: u64, variables: usize, constraints: usize) -> ModelSpec {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let bodies = (0..rng.gen_range(1..=constraints)).map(|index| ConstraintSpec {
        label: format!("row-{index}"),
        body: random_body(&mut rng, variables),
    });
    let constraints: Vec<ConstraintSpec> = bodies.collect();
    let levels = rng.gen_range(1..=3);
    ModelSpec {
        variables: (0..variables).map(|v| format!("x{v}")).collect(),
        constraints,
        objective: (0..levels)
            .map(|index| random_level(&mut rng, variables, index))
            .collect(),
    }
}

fn unit_objective(variables: usize) -> Vec<ObjectiveLevelSpec> {
    let terms = (0..variables).map(|v| ObjectiveTerm {
        var: VarId(v as u32),
        coefficient: Coefficient::Known(1),
    });
    vec![ObjectiveLevelSpec {
        label: "count".into(),
        terms: terms.collect(),
    }]
}

/// Minimum vertex cover of `cycles` disjoint cycles of odd `length`: each needs
/// `(length + 1) / 2` vertices while the integer matching bound gives
/// `(length − 1) / 2`, so the bound stays loose and the tree grows.
pub fn odd_cycle_cover(cycles: usize, length: usize) -> ModelSpec {
    let variables = cycles * length;
    let edge = |cycle: usize, at: usize| {
        let (from, to) = (cycle * length + at, cycle * length + (at + 1) % length);
        ConstraintSpec {
            label: format!("edge-{from}-{to}"),
            body: ConstraintBody::AtLeast {
                vars: vec![VarId(from as u32), VarId(to as u32)],
                k: 1,
            },
        }
    };
    ModelSpec {
        variables: (0..variables).map(|v| format!("v{v}")).collect(),
        constraints: (0..cycles)
            .flat_map(|c| (0..length).map(move |at| (c, at)))
            .map(|(c, at)| edge(c, at))
            .collect(),
        objective: unit_objective(variables),
    }
}

/// `Σ 2·x = variables` for odd `variables`: infeasible by parity, which interval
/// propagation cannot see, so refuting it takes exponentially many nodes.
pub fn parity_equality(variables: usize) -> ModelSpec {
    let terms = (0..variables)
        .map(|v| Term {
            var: VarId(v as u32),
            coefficient: 2,
        })
        .collect();
    let body = ConstraintBody::Linear {
        terms,
        relation: Relation::Equal,
        rhs: variables as i64,
    };
    ModelSpec {
        variables: (0..variables).map(|v| format!("p{v}")).collect(),
        constraints: vec![ConstraintSpec {
            label: "parity".into(),
            body,
        }],
        objective: unit_objective(variables),
    }
}
