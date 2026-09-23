//! Why-not explanations (DECIDE-LAYER-DESIGN §7.4, EH-008).
//!
//! An explanation is not prose: it is a re-solve with the excluded option
//! FORCED in, recorded as the objective that forcing costs or as the fact
//! that forcing it leaves no feasible answer. Each costs one bounded solve,
//! so they are budgeted: at most `limit` per slot, taken in order of the
//! option's own objective weight (cheapest first, ties by candidate order).
//! Options step 1b removed are not re-solved -- their violation is already in
//! the record's `eliminated` list.

use eg_types::agent_component::AgentComponentKind;
use eg_types::decision::{Violation, WhyNot};
use eg_types::solve::{ConstraintBody, ConstraintSpec, SolveStatus, VarId};

use super::agent::slot_name;
use super::conclude::slot_label;
use super::model::var_name;
use super::search::SolveContext;
use crate::solve::{solve, Model};

/// The slot order explanations are produced in.
const SLOTS: [AgentComponentKind; 6] = [
    AgentComponentKind::ModelProfile,
    AgentComponentKind::SystemPrompt,
    AgentComponentKind::Tool,
    AgentComponentKind::Toolset,
    AgentComponentKind::Skill,
    AgentComponentKind::Ontology,
];

/// Most explanations one record carries in total.
const MAX_WHY_NOT: usize = 64;

pub(super) fn explain(
    context: &SolveContext<'_>,
    model: &Model,
    selected: &[bool],
    limit: u8,
) -> Vec<WhyNot> {
    let mut out = Vec::new();
    for kind in SLOTS {
        let mut excluded: Vec<VarId> = (0..selected.len())
            .map(|position| VarId(position as u32))
            .filter(|var| !selected[var.index()] && context.kind_of(*var) == kind)
            .collect();
        excluded.sort_by_key(|var| (model.weights()[var.index()], *var));
        for var in excluded.into_iter().take(usize::from(limit)) {
            if out.len() >= MAX_WHY_NOT {
                return out;
            }
            out.push(forced(context, var, kind));
        }
    }
    out
}

/// Re-solve with `var` forced in.
fn forced(context: &SolveContext<'_>, var: VarId, kind: AgentComponentKind) -> WhyNot {
    let placed = context.built.vars[var.index()];
    let mut spec = context.built.spec.clone();
    spec.constraints.push(ConstraintSpec {
        label: format!("why-not:{}", var_name(placed)),
        body: ConstraintBody::Fix { var, value: true },
    });
    let certificate = Model::try_from(spec).map(|model| solve(&model, &context.config));
    let (forced_objective, violation) = match certificate {
        Ok(certificate) => match certificate.status {
            SolveStatus::Infeasible { .. }
            | SolveStatus::InfeasibleByDeterministicSearch { .. } => {
                (None, Some(Violation::InfeasibleWhenForced))
            }
            SolveStatus::Optimal
            | SolveStatus::OptimalByDeterministicSearch { .. }
            | SolveStatus::FeasibleWithGap { .. }
            | SolveStatus::BudgetExhausted => (
                certificate.incumbent.map(|incumbent| incumbent.objective),
                None,
            ),
        },
        Err(_) => (None, None),
    };
    WhyNot {
        component_id: context.candidate(var).component_id.clone(),
        slot: slot_label(context.template, placed.slot, slot_name(kind)),
        forced_objective,
        violation,
    }
}
