//! Step 3's problem statement: the legal remainder as a bounded 0-1 programme.
//!
//! One binary variable per (slot, legal candidate), named `s<j>.c<i>` after
//! the slot and the candidate's position in the record's sorted candidate
//! list, so every constraint label a record reports (`cover:<iri>`,
//! `s0.requires:c3:0`, `pin:c7`) resolves against the record's own inputs. A
//! one-agent assembly has one slot; a graph template has one slot per `Agent`
//! node (§7.3 topology).
//!
//! Rows (DECIDE-LAYER-DESIGN §7.3):
//!
//! * **covering** -- every required capability is covered by some selected
//!   candidate, in any slot, whose classification is subsumed by it;
//! * **one agent per slot** -- exactly one model profile and one system prompt,
//!   and at least one tool, skill and ontology, because each slot is one
//!   runnable Agent Library entry (§4.6) and those are its slot rules;
//! * **needs** -- a selected candidate's own `required_capabilities` are
//!   covered by some OTHER candidate selected in the same slot;
//! * **requires** -- a selected candidate's pinned dependencies are selected in
//!   its slot, at the pinned revision, or it cannot be selected there at all;
//! * **feature compatibility** -- a selected tool needs its slot's model to
//!   support tools; a selected prompt must fit its slot's model window;
//! * **knapsacks** -- declared per-call cost and declared p95 latency, summed
//!   over every slot, within the request's budgets;
//! * **cardinality** and **pins** (a pin must be selected in some slot).

use eg_types::agent_component::AgentComponentKind;
use eg_types::agent_ontology::is_capability;
use eg_types::decision::derivation::RequiredCapability;
use eg_types::decision::{AbstainReason, CandidateFacts, DecisionInputs};
use eg_types::solve::{ConstraintBody, ConstraintSpec, ModelSpec, Relation, Term, VarId};

use super::facts::{self, Declared};
use super::{objective, AssembleError};

/// How many components of one kind an agent entry holds.
#[derive(Clone, Copy)]
enum SlotArity {
    ExactlyOne,
    AtLeastOne,
}

/// The slots of one runnable agent (`AgentLibraryEntryDraft` validation).
const ONE_AGENT_SLOTS: [(AgentComponentKind, &str, SlotArity); 5] = [
    (
        AgentComponentKind::ModelProfile,
        "one:model_profile",
        SlotArity::ExactlyOne,
    ),
    (
        AgentComponentKind::SystemPrompt,
        "one:system_prompt",
        SlotArity::ExactlyOne,
    ),
    (AgentComponentKind::Tool, "one:tool", SlotArity::AtLeastOne),
    (
        AgentComponentKind::Skill,
        "one:skill",
        SlotArity::AtLeastOne,
    ),
    (
        AgentComponentKind::Ontology,
        "one:ontology",
        SlotArity::AtLeastOne,
    ),
];

/// One variable: candidate `candidate` placed in slot `slot`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct VarRef {
    pub slot: usize,
    pub candidate: usize,
}

/// A built programme and what each of its variables stands for.
pub(super) struct Built {
    pub spec: ModelSpec,
    pub vars: Vec<VarRef>,
}

pub(super) enum Building {
    Ready(Built),
    /// A step decided before any search: nothing covers a requirement, an
    /// agent slot has no legal candidate, or a pin names an illegal option.
    Abstain(Vec<AbstainReason>),
}

/// The variable name of `var`.
pub(super) fn var_name(var: VarRef) -> String {
    format!("s{}.c{}", var.slot, var.candidate)
}

pub(super) fn build(
    inputs: &DecisionInputs,
    required: &[RequiredCapability],
    (legal, slots): (&[usize], usize),
    currency: Option<&str>,
) -> Result<Building, AssembleError> {
    let vars: Vec<VarRef> = (0..slots)
        .flat_map(|slot| {
            legal
                .iter()
                .map(move |&candidate| VarRef { slot, candidate })
        })
        .collect();
    let mut rows = Rows {
        candidates: inputs.candidates.as_slice(),
        vars: &vars,
        slots,
        constraints: Vec::new(),
        uncovered: Vec::new(),
        infeasible: Vec::new(),
    };
    rows.pins(inputs);
    rows.cover(required);
    rows.one_agent();
    rows.needs();
    rows.requires();
    rows.tools_need_a_tool_model();
    rows.prompt_fits_model();
    rows.budgets(inputs, currency);
    rows.max_components(inputs.request.requirements.constraints.max_components);
    if let Some(reasons) = rows.abstention() {
        return Ok(Building::Abstain(reasons));
    }
    let constraints = rows.constraints;
    let placed: Vec<usize> = vars.iter().map(|var| var.candidate).collect();
    let objective = objective::levels(inputs, &placed, currency)?;
    Ok(Building::Ready(Built {
        spec: ModelSpec {
            variables: vars.iter().map(|&var| var_name(var)).collect(),
            constraints,
            objective,
        },
        vars,
    }))
}

struct Rows<'a> {
    candidates: &'a [CandidateFacts],
    vars: &'a [VarRef],
    slots: usize,
    constraints: Vec<ConstraintSpec>,
    uncovered: Vec<String>,
    infeasible: Vec<String>,
}

impl Rows<'_> {
    fn candidate(&self, var: VarId) -> &CandidateFacts {
        &self.candidates[self.vars[var.index()].candidate]
    }

    fn var_ids(&self) -> impl Iterator<Item = VarId> + '_ {
        (0..self.vars.len()).map(|position| VarId(position as u32))
    }

    fn slot_of(&self, var: VarId) -> usize {
        self.vars[var.index()].slot
    }

    /// Variables in `slot` (or in every slot, for `None`) whose candidate
    /// satisfies `keep`.
    fn vars_where(
        &self,
        slot: Option<usize>,
        keep: impl Fn(&CandidateFacts) -> bool,
    ) -> Vec<VarId> {
        self.var_ids()
            .filter(|&var| slot.is_none_or(|slot| self.slot_of(var) == slot))
            .filter(|&var| keep(self.candidate(var)))
            .collect()
    }

    fn label(&self, var: VarId) -> String {
        var_name(self.vars[var.index()])
    }

    fn push(&mut self, label: String, body: ConstraintBody) {
        self.constraints.push(ConstraintSpec { label, body });
    }

    /// A pin names an option that must be selected in some slot; it can only
    /// name one that is still legal, at the pinned revision (§3.4).
    fn pins(&mut self, inputs: &DecisionInputs) {
        for pin in &inputs.request.requirements.pins {
            let pinned = self.vars_where(None, |candidate| {
                candidate.component_id == pin.component_id
                    && candidate.kind == pin.kind
                    && candidate.definition_digest == pin.definition_digest
            });
            let Some(&first) = pinned.first() else {
                self.infeasible.push(format!("pin:{}", pin.component_id));
                continue;
            };
            let label = format!("pin:c{}", self.vars[first.index()].candidate);
            self.push(label, ConstraintBody::AtLeast { vars: pinned, k: 1 });
        }
    }

    fn cover(&mut self, required: &[RequiredCapability]) {
        for requirement in required {
            let covering = self.vars_where(None, |candidate| {
                candidate.classified_under(&requirement.iri)
            });
            if covering.is_empty() {
                self.uncovered.push(requirement.iri.clone());
                continue;
            }
            let body = ConstraintBody::AtLeast {
                vars: covering,
                k: 1,
            };
            self.push(format!("cover:{}", requirement.iri), body);
        }
    }

    /// The agent-library entry's own slot rules, stated as rows so the solver
    /// never proposes an agent the library would refuse.
    fn one_agent(&mut self) {
        for (kind, label, arity) in ONE_AGENT_SLOTS {
            if self
                .vars_where(None, |candidate| candidate.kind == kind)
                .is_empty()
            {
                self.infeasible.push(label.to_string());
                continue;
            }
            for slot in 0..self.slots {
                let vars = self.vars_where(Some(slot), |candidate| candidate.kind == kind);
                let body = match arity {
                    SlotArity::ExactlyOne => ConstraintBody::ExactlyOne { vars },
                    SlotArity::AtLeastOne => ConstraintBody::AtLeast { vars, k: 1 },
                };
                self.push(format!("s{slot}.{label}"), body);
            }
        }
    }

    fn needs(&mut self) {
        for var in self.var_ids().collect::<Vec<_>>() {
            let needs: Vec<String> = self
                .candidate(var)
                .required_capabilities
                .iter()
                .filter(|iri| is_capability(iri))
                .cloned()
                .collect();
            for need in needs {
                let others: Vec<VarId> = self
                    .vars_where(Some(self.slot_of(var)), |candidate| {
                        candidate.classified_under(&need)
                    })
                    .into_iter()
                    .filter(|&other| other != var)
                    .collect();
                let label = format!("needs:{}:{need}", self.label(var));
                self.push(label, implies_any_or_exclude(var, others));
            }
        }
    }

    fn requires(&mut self) {
        for var in self.var_ids().collect::<Vec<_>>() {
            let dependencies = self.candidate(var).requires.as_slice().to_vec();
            for (position, dependency) in dependencies.iter().enumerate() {
                let met = self.vars_where(Some(self.slot_of(var)), |candidate| {
                    candidate.component_id == dependency.component_id
                        && candidate.kind == dependency.kind
                        && candidate.definition_digest == dependency.definition_digest
                });
                let label = format!("requires:{}:{position}", self.label(var));
                self.push(label, implies_any_or_exclude(var, met));
            }
        }
    }

    fn tools_need_a_tool_model(&mut self) {
        let tools = self.vars_where(None, |candidate| candidate.kind == AgentComponentKind::Tool);
        for tool in tools {
            let tool_models = self.vars_where(Some(self.slot_of(tool)), |candidate| {
                facts::model_facts(candidate).is_some_and(|model| model.supports_tools)
            });
            let label = format!("tool-model:{}", self.label(tool));
            self.push(label, implies_any_or_exclude(tool, tool_models));
        }
    }

    fn prompt_fits_model(&mut self) {
        let prompts = self.vars_where(None, |candidate| facts::prompt_tokens(candidate).is_some());
        for &prompt in &prompts {
            let tokens = facts::prompt_tokens(self.candidate(prompt)).unwrap_or(0);
            let models = self.vars_where(Some(self.slot_of(prompt)), |candidate| {
                facts::model_facts(candidate).is_some()
            });
            for &model in &models {
                let window = facts::model_facts(self.candidate(model))
                    .map_or(0, |model| model.context_window_tokens);
                if tokens > window {
                    let label = format!("fits:{}:{}", self.label(prompt), self.label(model));
                    let vars = vec![prompt, model];
                    self.push(label, ConstraintBody::AtMost { vars, k: 1 });
                }
            }
        }
    }

    fn budgets(&mut self, inputs: &DecisionInputs, currency: Option<&str>) {
        let constraints = &inputs.request.requirements.constraints;
        if let Some(budget) = &constraints.cost_budget {
            let cost = |candidate: &CandidateFacts| facts::per_call_cost(candidate, currency);
            self.knapsack("budget:declared_cost", cost, budget.max_micros);
        }
        if let Some(ceiling) = constraints.max_p95_latency_ms {
            self.knapsack(
                "budget:declared_p95_latency",
                facts::p95_latency,
                u64::from(ceiling),
            );
        }
    }

    /// `Σ known coefficient · x ≤ limit`, omitted when every selection fits.
    /// An unknown value is not in the row: under a non-strict budget it is
    /// admitted and ranked in the objective's unknown tier instead (§7.3).
    fn knapsack(&mut self, label: &str, value: impl Fn(&CandidateFacts) -> Declared, limit: u64) {
        let terms: Vec<Term> = self
            .var_ids()
            .filter_map(|var| match value(self.candidate(var)) {
                Declared::Known(coefficient) if coefficient > 0 => Some(Term { var, coefficient }),
                Declared::Known(_) | Declared::None | Declared::Unknown => None,
            })
            .collect();
        let total: u64 = terms
            .iter()
            .map(|term| term.coefficient.unsigned_abs())
            .sum();
        if terms.is_empty() || total <= limit {
            return;
        }
        let rhs = i64::try_from(limit).unwrap_or(i64::MAX);
        let relation = Relation::LessEqual;
        self.push(
            label.to_string(),
            ConstraintBody::Linear {
                terms,
                relation,
                rhs,
            },
        );
    }

    fn max_components(&mut self, max_components: Option<u32>) {
        let Some(k) = max_components else { return };
        if k as usize >= self.vars.len() {
            return;
        }
        let vars: Vec<VarId> = self.var_ids().collect();
        self.push(
            "max_components".to_string(),
            ConstraintBody::AtMost { vars, k },
        );
    }

    fn abstention(&self) -> Option<Vec<AbstainReason>> {
        let mut reasons: Vec<AbstainReason> = self
            .uncovered
            .iter()
            .map(|iri| AbstainReason::UncoveredCapability { iri: iri.clone() })
            .collect();
        if !self.infeasible.is_empty() {
            let constraints = self.infeasible.iter().take(64).cloned().collect();
            reasons.push(AbstainReason::Infeasible {
                constraints: eg_types::contract::BoundedVec::new(constraints)
                    .expect("taken to the bound"),
            });
        }
        (!reasons.is_empty()).then_some(reasons)
    }
}

/// `var ⇒ any(consequents)`, or `var` excluded outright when nothing could
/// satisfy it.
fn implies_any_or_exclude(var: VarId, consequents: Vec<VarId>) -> ConstraintBody {
    if consequents.is_empty() {
        return ConstraintBody::Fix { var, value: false };
    }
    ConstraintBody::ImpliesAny {
        antecedent: var,
        consequents,
    }
}
