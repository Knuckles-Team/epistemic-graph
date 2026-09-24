//! The topology rows (SWARM-TOPOLOGY-DECIDE-DESIGN §3), appended to one
//! template's assembly programme.
//!
//! Each slot gets a one-hot over its (width, rounds) choices, variables
//! `t<slot>.w<width>.r<rounds>` after the candidate variables. Every
//! quantity is a precomputed coefficient ([`super::tables`]), so each row is
//! linear:
//!
//! * `shape:<node>` -- exactly one choice per slot;
//! * `demand:<node>` -- a fan-out (`Child`) slot takes no width below
//!   `⌈subtasks / per_agent_subtasks⌉`;
//! * `lease:<cell>:<class>` -- `Σ width · per-agent amount ≤ headroom`;
//! * `deadline` -- `Σ makespan ≤ deadline_ms`;
//! * `cap:max_tokens` -- `Σ tokens ≤ max_tokens`.
//!
//! A cap that leaves a slot no choice is `cap:max_width`; a demanded resource
//! class with no observed cell is `lease:*:<class>`; an undeclared p95 under a
//! deadline (or tokens under a token cap) abstains `UnknownFact`, never zero.
//! Two objective levels go FIRST: declared makespan, then total lease amount.

use eg_types::capacity_lease::CapacityResourceClass;
use eg_types::decision::{AbstainReason, DecisionErrorCode, SlotRole, SlotTopology, TopologyFacts};
use eg_types::solve::{
    Coefficient, ConstraintBody, ConstraintSpec, ObjectiveLevelSpec, ObjectiveTerm, Relation, Term,
    VarId,
};

use super::super::model::Built;
use super::super::AssembleError;
use super::tables::{demanded_width, slot_choices, Choice};
use super::Question;
use crate::solve::model::MAX_ABS_COEFFICIENT;

/// The serde name of a resource class, as constraint ids spell it.
pub(super) fn class_name(class: CapacityResourceClass) -> &'static str {
    match class {
        CapacityResourceClass::LlmGenerator => "llm_generator",
        CapacityResourceClass::LlmEmbedding => "llm_embedding",
        CapacityResourceClass::Gpu => "gpu",
        CapacityResourceClass::Worker => "worker",
        CapacityResourceClass::Cpu => "cpu",
        CapacityResourceClass::Broker => "broker",
    }
}

fn coefficient(value: u64) -> Result<i64, AssembleError> {
    i64::try_from(value)
        .ok()
        .filter(|value| *value <= MAX_ABS_COEFFICIENT)
        .ok_or_else(|| {
            AssembleError::new(
                DecisionErrorCode::AssemblyModelInvalid,
                "a topology coefficient exceeds the solver's coefficient range",
            )
        })
}

/// The rows under construction, over the choices appended after `offset`.
struct ShapeRows<'q> {
    question: &'q Question<'q>,
    offset: u32,
    choices: Vec<Choice>,
    /// Per choice: cut off by demand.
    short: Vec<bool>,
    constraints: Vec<ConstraintSpec>,
    infeasible: Vec<String>,
    unknown: Vec<AbstainReason>,
}

impl ShapeRows<'_> {
    fn var(&self, position: usize) -> VarId {
        VarId(self.offset + position as u32)
    }

    fn node(&self, slot: usize) -> &str {
        &self.question.facts.slots.as_slice()[slot].node_id
    }

    fn one_hot(&mut self) {
        let slots = self.question.facts.slots.len();
        for slot in 0..slots {
            let caps = self.question.caps;
            let subtasks = self.question.requirements.subtasks;
            let choices = slot_choices(self.question.facts, slot, &caps, subtasks);
            if choices.is_empty() {
                self.infeasible.push("cap:max_width".to_string());
                continue;
            }
            let first = self.choices.len();
            self.choices.extend(choices);
            let vars = (first..self.choices.len()).map(|p| self.var(p)).collect();
            let label = format!("shape:{}", self.node(slot));
            self.push(label, ConstraintBody::ExactlyOne { vars });
        }
    }

    fn push(&mut self, label: String, body: ConstraintBody) {
        self.constraints.push(ConstraintSpec { label, body });
    }

    /// A fan-out slot takes no width below `⌈subtasks / per_agent⌉`: those
    /// choices are marked short and cut off by one row per slot.
    fn demand(&mut self) {
        let requirements = self.question.requirements;
        let need = demanded_width(requirements.subtasks, requirements.per_agent_subtasks);
        let facts = self.question.facts;
        self.short = self
            .choices
            .iter()
            .map(|c| slot_of(facts, c).role == SlotRole::Child && u64::from(c.width) < need)
            .collect();
        for slot in 0..facts.slots.len() {
            let positions: Vec<usize> = (0..self.choices.len())
                .filter(|&p| self.choices[p].slot == slot && self.short[p])
                .collect();
            if positions.is_empty() {
                continue;
            }
            let label = format!("demand:{}", self.node(slot));
            if positions.len() == self.slot_len(slot) {
                self.infeasible.push(label.clone());
            }
            let vars = positions.into_iter().map(|p| self.var(p)).collect();
            self.push(label, ConstraintBody::AtMost { vars, k: 0 });
        }
    }

    fn slot_len(&self, slot: usize) -> usize {
        self.choices.iter().filter(|c| c.slot == slot).count()
    }

    /// The smallest `Σ values` any selection can reach: per slot, the least
    /// value among its choices demand leaves, summed (the slots are
    /// independent one-hots).
    fn floor(&self, values: &[u64]) -> u64 {
        (0..self.question.facts.slots.len())
            .filter_map(|slot| {
                (0..self.choices.len())
                    .filter(|&p| self.choices[p].slot == slot && !self.short[p])
                    .map(|p| values[p])
                    .min()
            })
            .fold(0, u64::saturating_add)
    }

    /// `Σ values[position] · t ≤ rhs`, zero coefficients omitted. A row no
    /// selection can meet is named at once instead of being left to the
    /// solver's infeasibility core.
    fn linear(&mut self, label: String, values: &[u64], rhs: u64) -> Result<(), AssembleError> {
        if self.floor(values) > rhs {
            self.infeasible.push(label);
            return Ok(());
        }
        let mut terms = Vec::new();
        for (position, value) in values.iter().enumerate().filter(|(_, v)| **v > 0) {
            terms.push(Term {
                var: self.var(position),
                coefficient: coefficient(*value)?,
            });
        }
        if terms.is_empty() {
            return Ok(());
        }
        let rhs = coefficient(rhs.min(MAX_ABS_COEFFICIENT as u64))?;
        let relation = Relation::LessEqual;
        self.push(
            label,
            ConstraintBody::Linear {
                terms,
                relation,
                rhs,
            },
        );
        Ok(())
    }

    /// Concurrent amount of `class` each choice leases: `width · per-agent`.
    fn lease_values(&self, class: CapacityResourceClass) -> Vec<u64> {
        self.choices
            .iter()
            .map(|choice| concurrent_amount(self.question.facts, choice, class))
            .collect()
    }

    fn leases(&mut self) -> Result<(), AssembleError> {
        for class in demanded_classes(self.question.facts) {
            let cells: Vec<(String, u64)> = self
                .question
                .read
                .headroom
                .iter()
                .filter(|headroom| headroom.class == class)
                .map(|headroom| (headroom.cell_id.clone(), headroom.available))
                .collect();
            if cells.is_empty() {
                self.infeasible
                    .push(format!("lease:*:{}", class_name(class)));
            }
            let values = self.lease_values(class);
            for (cell, available) in cells {
                self.linear(
                    format!("lease:{cell}:{}", class_name(class)),
                    &values,
                    available,
                )?;
            }
        }
        Ok(())
    }

    fn limited(
        &mut self,
        (label, field): (&str, &str),
        value: fn(&Choice) -> Option<u64>,
        limit: Option<u64>,
    ) -> Result<(), AssembleError> {
        let Some(limit) = limit else {
            return Ok(());
        };
        match declared(&self.choices, value) {
            Some(values) => self.linear(label.to_string(), &values, limit),
            None => {
                self.unknown_fields(value, field);
                Ok(())
            }
        }
    }

    fn unknown_fields(&mut self, value: fn(&Choice) -> Option<u64>, field: &str) {
        let mut slots: Vec<usize> = self
            .choices
            .iter()
            .filter(|choice| value(choice).is_none())
            .map(|choice| choice.slot)
            .collect();
        slots.dedup();
        for slot in slots {
            let reason = AbstainReason::UnknownFact {
                component_id: self.question.template.graph_id.clone(),
                field: format!("topology.{}.{field}", self.node(slot)),
            };
            self.unknown.push(reason);
        }
    }

    fn abstention(&self) -> Option<Vec<AbstainReason>> {
        let mut reasons = self.unknown.clone();
        if !self.infeasible.is_empty() {
            reasons.extend(super::infeasible(self.infeasible.clone()));
        }
        (!reasons.is_empty()).then_some(reasons)
    }
}

/// The slot facts a choice belongs to.
fn slot_of<'f>(facts: &'f TopologyFacts, choice: &Choice) -> &'f SlotTopology {
    &facts.slots.as_slice()[choice.slot]
}

/// Per-agent amount of `class` one slot leases.
fn slot_amount(slot: &SlotTopology, class: CapacityResourceClass) -> u64 {
    slot.lease
        .iter()
        .filter(|demand| demand.class == class)
        .map(|demand| demand.amount)
        .sum()
}

/// What `choice` leases of `class` at once: `width · per-agent amount`.
pub(super) fn concurrent_amount(
    facts: &TopologyFacts,
    choice: &Choice,
    class: CapacityResourceClass,
) -> u64 {
    u64::from(choice.width).saturating_mul(slot_amount(slot_of(facts, choice), class))
}

/// Every resource class some slot leases a non-zero amount of.
fn demanded_classes(facts: &TopologyFacts) -> Vec<CapacityResourceClass> {
    let mut classes: Vec<CapacityResourceClass> = facts
        .slots
        .iter()
        .flat_map(|slot| slot.lease.iter())
        .filter(|demand| demand.amount > 0)
        .map(|demand| demand.class)
        .collect();
    classes.sort();
    classes.dedup();
    classes
}

/// The per-choice value of an optional declared quantity, when every choice
/// declares it.
fn declared(choices: &[Choice], value: fn(&Choice) -> Option<u64>) -> Option<Vec<u64>> {
    choices.iter().map(value).collect()
}

/// The two topology levels, most significant first: declared makespan (an
/// undeclared p95 is the unknown tier), then total lease amount.
fn levels(
    facts: &TopologyFacts,
    choices: &[Choice],
    offset: u32,
) -> Result<[ObjectiveLevelSpec; 2], AssembleError> {
    let makespan = level_terms(choices, offset, |choice| match choice.makespan_ms {
        Some(ms) => coefficient(ms).map(Coefficient::Known),
        None => Ok(Coefficient::Unknown),
    })?;
    let lease = level_terms(choices, offset, |choice| {
        let per_agent: u64 = slot_of(facts, choice)
            .lease
            .iter()
            .map(|demand| demand.amount)
            .fold(0, u64::saturating_add);
        coefficient(per_agent.saturating_mul(u64::from(choice.width))).map(Coefficient::Known)
    })?;
    Ok([
        ObjectiveLevelSpec {
            label: "topology:makespan".to_string(),
            terms: makespan,
        },
        ObjectiveLevelSpec {
            label: "topology:lease".to_string(),
            terms: lease,
        },
    ])
}

fn level_terms(
    choices: &[Choice],
    offset: u32,
    value: impl Fn(&Choice) -> Result<Coefficient, AssembleError>,
) -> Result<Vec<ObjectiveTerm>, AssembleError> {
    let mut terms = Vec::new();
    for (position, choice) in choices.iter().enumerate() {
        let coefficient = value(choice)?;
        if coefficient != Coefficient::Known(0) {
            terms.push(ObjectiveTerm {
                var: VarId(offset + position as u32),
                coefficient,
            });
        }
    }
    Ok(terms)
}

fn makespan_of(choice: &Choice) -> Option<u64> {
    choice.makespan_ms
}

fn tokens_of(choice: &Choice) -> Option<u64> {
    choice.tokens
}

impl Question<'_> {
    /// Append this question's variables, rows and objective levels to
    /// `built`, or name why the template cannot answer it.
    pub(in super::super) fn extend(
        &self,
        built: &mut Built,
    ) -> Result<Option<Vec<AbstainReason>>, AssembleError> {
        let mut rows = ShapeRows {
            question: self,
            offset: built.spec.variables.len() as u32,
            choices: Vec::new(),
            short: Vec::new(),
            constraints: Vec::new(),
            infeasible: Vec::new(),
            unknown: Vec::new(),
        };
        rows.one_hot();
        rows.demand();
        rows.leases()?;
        let deadline = self.requirements.deadline_ms.map(u64::from);
        rows.limited(("deadline", "p95_ms"), makespan_of, deadline)?;
        rows.limited(
            ("cap:max_tokens", "tokens"),
            tokens_of,
            self.caps.max_tokens,
        )?;
        if let Some(reasons) = rows.abstention() {
            return Ok(Some(reasons));
        }
        let levels = levels(self.facts, &rows.choices, rows.offset)?;
        built
            .spec
            .variables
            .extend(rows.choices.iter().map(var_name));
        built.spec.constraints.extend(rows.constraints);
        built.spec.objective.splice(0..0, levels);
        built.shape = rows.choices;
        Ok(None)
    }
}

/// `t<slot>.w<width>.r<rounds>`.
pub(super) fn var_name(choice: &Choice) -> String {
    format!("t{}.w{}.r{}", choice.slot, choice.width, choice.rounds)
}
