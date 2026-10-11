//! Independent bounded topology oracle for R110.1. No solver, row evaluator or
//! production choice-table helper is used to compute the expected answer.

use eg_types::decision::{LeaseDemand, SolverBudget, StopRule};

use super::*;
use crate::SplitMix64;

#[derive(Clone, Debug)]
struct Case {
    width: u8,
    lead_rounds: u8,
    worker_rounds: u8,
    subtasks: u32,
    per_agent: u32,
    available: u64,
    p95: [u32; 2],
    lease: [u64; 2],
    tokens: [u64; 2],
    deadline: Option<u32>,
    token_cap: Option<u64>,
}

impl Case {
    /// All ten component bits are forced by the five per-slot kind rules.
    /// Enumerating one choice per slot therefore covers every potentially
    /// feasible assignment of the entire model, not just sampled bit masks.
    /// At most 2 * (4 * 2) = 16 tuples; at most 20 actual binary variables.
    fn feasible(&self) -> Vec<([u8; 4], (u64, u64))> {
        let mut feasible = Vec::new();
        for lead_rounds in 1..=self.lead_rounds {
            for width in 1..=self.width {
                for rounds in 1..=self.worker_rounds {
                    if u64::from(width) * u64::from(self.per_agent) < u64::from(self.subtasks) {
                        continue;
                    }
                    // Count batches directly, independently of production tables.
                    let turns = (0..self.subtasks).step_by(usize::from(width)).count() as u64;
                    let makespan = u64::from(lead_rounds) * u64::from(self.p95[0])
                        + u64::from(rounds) * turns * u64::from(self.p95[1]);
                    let lease = self.lease[0] + u64::from(width) * self.lease[1];
                    let tokens = u64::from(lead_rounds) * self.tokens[0]
                        + u64::from(width) * u64::from(rounds) * turns * self.tokens[1];
                    if lease <= self.available
                        && self
                            .deadline
                            .is_none_or(|limit| makespan <= u64::from(limit))
                        && self.token_cap.is_none_or(|limit| tokens <= limit)
                    {
                        feasible.push(([1, lead_rounds, width, rounds], (makespan, lease)));
                    }
                }
            }
        }
        feasible
    }

    fn decide(&self) -> Assembly {
        let mut topology = facts();
        let mut slots = topology.slots.as_slice().to_vec();
        slots[1].max_width = self.width;
        for (index, slot) in slots.iter_mut().enumerate() {
            slot.max_rounds = [self.lead_rounds, self.worker_rounds][index];
            slot.p95_ms = Some(self.p95[index]);
            slot.tokens = Some(self.tokens[index]);
            slot.lease = bounded(vec![LeaseDemand {
                class: CapacityResourceClass::LlmGenerator,
                amount: self.lease[index],
            }]);
        }
        topology.slots = bounded(slots);
        topology.stop = StopRule::MaxRounds { n: 2 };
        let template = fan(topology);
        let mut asked = request(&[], &[]);
        asked.templates = bounded(vec![reference(&template)]);
        let mut topology = requirements(self.subtasks);
        topology.per_agent_subtasks = self.per_agent;
        topology.deadline_ms = self.deadline;
        topology.caps.max_tokens = self.token_cap;
        asked.requirements.topology = Some(topology);
        asked.solver = Some(SolverBudget {
            node_budget: 100_000,
            max_why_not_per_slot: 0,
        });
        let mut library = agent_basics();
        library.extend([
            cheap_model("model"),
            prompt("prompt", 100),
            tool("tool", &[], Some(1)),
        ]);
        assert_eq!(library.len(), 5);
        let mut inputs = build_inputs(
            asked,
            library,
            vec![template],
            DecisionPolicy::engine_default(),
        )
        .expect("valid fixture inputs");
        inputs.topology = Some(read(self.available, admitted()));
        assemble(inputs, identity()).expect("topology assembly")
    }
}

fn cases() -> Vec<Case> {
    let mut rng = SplitMix64::new(0x110_2026);
    let boundary = Case {
        width: 4,
        lead_rounds: 2,
        worker_rounds: 2,
        subtasks: 4,
        per_agent: 2,
        available: 20,
        p95: [100, 200],
        lease: [1, 2],
        tokens: [10, 20],
        deadline: None,
        token_cap: None,
    };
    let mut cases = vec![
        boundary.clone(),
        Case {
            available: 0,
            ..boundary
        },
    ];
    for _ in 0..64 {
        cases.push(Case {
            width: 1 + rng.below(4) as u8,
            lead_rounds: 1 + rng.below(2) as u8,
            worker_rounds: 1 + rng.below(2) as u8,
            subtasks: 1 + rng.below(8) as u32,
            per_agent: 1 + rng.below(4) as u32,
            available: rng.below(20) as u64,
            p95: [1 + rng.below(100) as u32, 1 + rng.below(200) as u32],
            lease: [1 + rng.below(3) as u64, 1 + rng.below(3) as u64],
            tokens: [1 + rng.below(20) as u64, 1 + rng.below(40) as u64],
            deadline: (rng.below(2) == 0).then(|| rng.below(800) as u32),
            token_cap: (rng.below(2) == 0).then(|| rng.below(400) as u64),
        });
    }
    cases
}

// spec: EG-DECISION-ENGINE-R110.1
#[test]
fn property_oracle_agrees_with_the_solver_for_instances_up_to_twenty_variables() {
    let mut solved = 0;
    let mut infeasible = 0;
    let mut maximum_variables = 0;
    // Exactly 66 cases, each with at most 16 independently enumerated tuples.
    for (index, case) in cases().iter().enumerate() {
        let feasible = case.feasible();
        let optimum = feasible.iter().map(|(_, objective)| *objective).min();
        let assembly = case.decide();
        match (&assembly.record.outcome, optimum) {
            (DecisionOutcome::Solved { certificate, .. }, Some(expected)) => {
                solved += 1;
                let spec = assembly.model.as_ref().expect("actual solved model");
                let incumbent = certificate.incumbent.as_ref().expect("incumbent");
                let n = spec.variables.len();
                assert!(n <= 20, "case {index}: {n} actual variables");
                assert_eq!(incumbent.selected.len(), n);
                assert_eq!(
                    spec.variables.iter().filter(|v| v.starts_with('s')).count(),
                    10
                );
                assert_eq!(
                    n,
                    10 + usize::from(case.lead_rounds)
                        + usize::from(case.width) * usize::from(case.worker_rounds)
                );
                maximum_variables = maximum_variables.max(n);
                let plan = plan(&assembly);
                let slots = plan.slots.as_slice();
                let choice = [
                    slots[0].width,
                    slots[0].rounds,
                    slots[1].width,
                    slots[1].rounds,
                ];
                assert!(
                    feasible.contains(&(choice, expected)),
                    "case {index}: {case:?}"
                );
                assert_eq!(plan.makespan_ms, Some(expected.0));
                assert_eq!(plan.lease.per_cell.as_slice()[0].amount, expected.1);
                for (level, expected) in incumbent
                    .objective
                    .levels
                    .iter()
                    .take(2)
                    .zip([expected.0, expected.1])
                {
                    assert_eq!(level.unknown_selected, 0);
                    assert_eq!(level.known.get(), i128::from(expected));
                }
                assert!(incumbent.objective.levels.len() >= 2);
                replay_check(&assembly.record).expect("certificate and actual path replay");
            }
            (DecisionOutcome::Abstained { reasons }, None) => {
                infeasible += 1;
                assert!(!reasons.is_empty());
                assert!(
                    reasons
                        .iter()
                        .all(|reason| matches!(reason, AbstainReason::Infeasible { .. })),
                    "case {index}: unexpected refusal {reasons:?}"
                );
            }
            (outcome, expected) => panic!("case {index}: {case:?}: {outcome:?} != {expected:?}"),
        }
    }
    assert!(
        solved > 1 && infeasible > 1,
        "both generated outcomes exercised"
    );
    assert_eq!(
        maximum_variables, 20,
        "actual model/certificate boundary exercised"
    );
}
