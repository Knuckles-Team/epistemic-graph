//! Synthetic 0-1 instances whose ground truth is known by construction.
//!
//! Shared by the solver tests and the `solve_cover` bench (through `#[path]`),
//! so it depends only on `eg_compute` and the seeded RNG. Every instance is
//! SYNTHETIC; nothing here claims real-world calibration.

use eg_compute::solve::model::{
    Coefficient, ConstraintBody, ConstraintSpec, ModelSpec, ObjectiveLevelSpec, ObjectiveTerm,
    Relation, Term, VarId,
};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

/// Cost of one capability inside a planted component.
pub const UNIT: i64 = 10;
/// Cost of the cheapest model profile.
pub const MODEL_BASE: i64 = 7;

/// Size of a planted assembly instance.
#[derive(Debug, Clone, Copy)]
pub struct AssemblyShape {
    pub components: usize,
    pub capabilities: usize,
    pub models: usize,
    pub unknown_cost_decoys: usize,
}

/// A generated instance and its unique optimum.
pub struct Planted {
    pub spec: ModelSpec,
    pub optimum: Vec<bool>,
}

/// Planted agent assembly.
///
/// Capabilities are partitioned into groups; one planted component covers each
/// group at cost `UNIT·|group|`. Decoy components cover random subsets at a
/// strictly higher cost per capability, or an unknown cost. Model profiles form
/// an exactly-one group; model 0 is the cheapest. Every selection pays at least
/// `UNIT` per capability, with equality only for disjoint planted components,
/// and at least `MODEL_BASE` for its model; unknown costs rank in a worse tier.
/// The planted partition with model 0 is therefore the unique optimum of the
/// first objective level. Budget, cardinality, requirement and implication
/// rows are added so that the planted optimum satisfies all of them.
pub fn planted_assembly(shape: AssemblyShape, seed: u64) -> Planted {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let groups = partition(shape.capabilities, &mut rng);
    let planted = groups.len();
    assert!(planted < shape.components, "shape leaves no decoys");
    let mut covers: Vec<Vec<usize>> = groups;
    let mut costs: Vec<Coefficient> = covers
        .iter()
        .map(|g| known(UNIT * g.len() as i64))
        .collect();
    for decoy in 0..shape.components - planted {
        let subset = random_subset(shape.capabilities, &mut rng);
        let premium = rng.gen_range(1..=UNIT);
        let unknown = decoy < shape.unknown_cost_decoys;
        costs.push(if unknown {
            Coefficient::Unknown
        } else {
            known(UNIT * subset.len() as i64 + premium)
        });
        covers.push(subset);
    }
    let order = shuffled(shape.components, &mut rng);
    let mut spec = empty_spec(shape);
    let mut optimum = vec![false; shape.components + shape.models];
    for (slot, &original) in order.iter().enumerate() {
        optimum[slot] = original < planted;
    }
    optimum[shape.components] = true;
    add_rows(&mut spec, shape, (&order, &covers), planted, &mut rng);
    add_objective(&mut spec, shape, &order, &costs, &mut rng);
    Planted { spec, optimum }
}

fn known(value: i64) -> Coefficient {
    Coefficient::Known(value)
}

fn partition(capabilities: usize, rng: &mut ChaCha8Rng) -> Vec<Vec<usize>> {
    let mut groups = Vec::new();
    let mut next = 0;
    while next < capabilities {
        let size = rng.gen_range(1..=4).min(capabilities - next);
        groups.push((next..next + size).collect());
        next += size;
    }
    groups
}

fn random_subset(capabilities: usize, rng: &mut ChaCha8Rng) -> Vec<usize> {
    let size = rng.gen_range(1..=6.min(capabilities));
    let mut chosen: Vec<usize> = shuffled(capabilities, rng).into_iter().take(size).collect();
    chosen.sort_unstable();
    chosen
}

/// A seeded permutation of `0..len` (Fisher-Yates).
pub fn shuffled(len: usize, rng: &mut ChaCha8Rng) -> Vec<usize> {
    let mut items: Vec<usize> = (0..len).collect();
    for index in (1..len).rev() {
        items.swap(index, rng.gen_range(0..=index));
    }
    items
}

fn empty_spec(shape: AssemblyShape) -> ModelSpec {
    let components = (0..shape.components).map(|c| format!("component-{c}"));
    let models = (0..shape.models).map(|m| format!("model-{m}"));
    ModelSpec {
        variables: components.chain(models).collect(),
        constraints: Vec::new(),
        objective: Vec::new(),
    }
}

fn constraint(label: String, body: ConstraintBody) -> ConstraintSpec {
    ConstraintSpec { label, body }
}

fn vars(indices: impl IntoIterator<Item = usize>) -> Vec<VarId> {
    indices
        .into_iter()
        .map(|index| VarId(index as u32))
        .collect()
}

/// `order[slot]` is the original component placed at variable `slot`.
fn add_rows(
    spec: &mut ModelSpec,
    shape: AssemblyShape,
    (order, covers): (&[usize], &[Vec<usize>]),
    planted: usize,
    rng: &mut ChaCha8Rng,
) {
    let models = vars(shape.components..shape.components + shape.models);
    for capability in 0..shape.capabilities {
        let coverers =
            vars((0..shape.components).filter(|&slot| covers[order[slot]].contains(&capability)));
        spec.constraints.push(constraint(
            format!("cover-{capability}"),
            ConstraintBody::AtLeast {
                vars: coverers,
                k: 1,
            },
        ));
    }
    spec.constraints.push(constraint(
        "one-model".into(),
        ConstraintBody::ExactlyOne {
            vars: models.clone(),
        },
    ));
    let limit = (planted + 2).min(shape.components) as u32;
    let all_components = vars(0..shape.components);
    spec.constraints.push(constraint(
        "cardinality".into(),
        ConstraintBody::AtMost {
            vars: all_components,
            k: limit,
        },
    ));
    for slot in (0..shape.components)
        .filter(|&slot| order[slot] >= planted)
        .take(4)
    {
        let antecedent = VarId(slot as u32);
        let premium_model = models[1 % models.len()];
        if premium_model != models[0] {
            let body = ConstraintBody::Implication {
                antecedent,
                consequent: premium_model,
            };
            spec.constraints
                .push(constraint(format!("needs-tools-{slot}"), body));
        }
        let capability = rng.gen_range(0..shape.capabilities);
        let consequents = vars(
            (0..shape.components)
                .filter(|&other| other != slot && covers[order[other]].contains(&capability)),
        );
        if !consequents.is_empty() {
            let body = ConstraintBody::ImpliesAny {
                antecedent,
                consequents,
            };
            spec.constraints
                .push(constraint(format!("requires-{slot}"), body));
        }
    }
    let budget_terms: Vec<Term> = models
        .iter()
        .enumerate()
        .map(|(rank, &var)| Term {
            var,
            coefficient: MODEL_BASE + 3 * rank as i64,
        })
        .collect();
    let budget = ConstraintBody::Linear {
        terms: budget_terms,
        relation: Relation::LessEqual,
        rhs: MODEL_BASE + 64,
    };
    spec.constraints
        .push(constraint("model-budget".into(), budget));
}

fn add_objective(
    spec: &mut ModelSpec,
    shape: AssemblyShape,
    order: &[usize],
    costs: &[Coefficient],
    rng: &mut ChaCha8Rng,
) {
    let mut cost: Vec<ObjectiveTerm> = (0..shape.components)
        .map(|slot| ObjectiveTerm {
            var: VarId(slot as u32),
            coefficient: costs[order[slot]],
        })
        .collect();
    cost.extend((0..shape.models).map(|rank| ObjectiveTerm {
        var: VarId((shape.components + rank) as u32),
        coefficient: known(MODEL_BASE + 3 * rank as i64),
    }));
    let count = (0..shape.components)
        .map(|slot| ObjectiveTerm {
            var: VarId(slot as u32),
            coefficient: known(1),
        })
        .collect();
    let latency = (0..shape.components + shape.models)
        .map(|var| ObjectiveTerm {
            var: VarId(var as u32),
            coefficient: known(rng.gen_range(0..500)),
        })
        .collect();
    spec.objective = vec![
        ObjectiveLevelSpec {
            label: "cost".into(),
            terms: cost,
        },
        ObjectiveLevelSpec {
            label: "components".into(),
            terms: count,
        },
        ObjectiveLevelSpec {
            label: "latency-p95-ms".into(),
            terms: latency,
        },
    ];
}
