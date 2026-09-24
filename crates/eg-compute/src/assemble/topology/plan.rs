//! Reading the decided plan back from the certificate's selection, and the
//! premises a topology answer rests on.

use eg_types::contract::BoundedVec;
use eg_types::decision::{
    CellLease, LeasePlan, PremiseClass, PremiseProvenance, PremiseRef, SlotPlan, SubagentAllowance,
    SubagentFallback, TopologyPlan,
};

use super::model::concurrent_amount;
use super::tables::Choice;
use super::Question;

/// The choices the selection took, one per slot, in slot order.
fn taken(shape: &[Choice], selected: &[bool], offset: usize) -> Vec<Choice> {
    shape
        .iter()
        .enumerate()
        .filter(|(position, _)| selected.get(offset + position).copied().unwrap_or(false))
        .map(|(_, choice)| *choice)
        .collect()
}

impl Question<'_> {
    /// The plan a solved selection proves. `offset` is the index of the first
    /// topology variable in the programme.
    pub(in super::super) fn plan(
        &self,
        shape: &[Choice],
        selected: &[bool],
        offset: usize,
    ) -> TopologyPlan {
        let taken = taken(shape, selected, offset);
        let slots = taken
            .iter()
            .map(|choice| SlotPlan {
                node_id: self.facts.slots.as_slice()[choice.slot].node_id.clone(),
                width: choice.width,
                rounds: choice.rounds,
            })
            .collect();
        let makespan_ms = taken
            .iter()
            .map(|choice| choice.makespan_ms)
            .sum::<Option<u64>>();
        TopologyPlan {
            class_iri: self.facts.class_iri.clone(),
            slots: BoundedVec::new(slots).expect("one plan entry per slot"),
            stop: self.facts.stop,
            lease: self.lease_plan(&taken),
            allowances: BoundedVec::new(self.allowances(&taken)).expect("one per slot at most"),
            makespan_ms,
        }
    }

    /// Per requested cell, the concurrent amount of its class the plan needs.
    fn lease_plan(&self, taken: &[Choice]) -> LeasePlan {
        let amount_of = |class| {
            taken
                .iter()
                .map(|choice| concurrent_amount(self.facts, choice, class))
                .fold(0, u64::saturating_add)
        };
        let per_cell = self
            .read
            .headroom
            .iter()
            .map(|headroom| CellLease {
                cell_id: headroom.cell_id.clone(),
                class: headroom.class,
                amount: amount_of(headroom.class),
            })
            .filter(|lease| lease.amount > 0)
            .collect();
        LeasePlan {
            per_cell: BoundedVec::new(per_cell).expect("at most one lease per requested cell"),
            priority: self.requirements.capacity.priority,
        }
    }

    /// A slot on a harness realises its width as one harness process plus
    /// `width − 1` native sub-agents, nested no deeper than the caps leave.
    /// A harness that cannot enforce that count falls back to DISABLED unless
    /// the policy opts it in to a token budget (ruling 2026-09-24, Q1).
    fn allowances(&self, taken: &[Choice]) -> Vec<SubagentAllowance> {
        taken
            .iter()
            .filter_map(|choice| {
                let slot = &self.facts.slots.as_slice()[choice.slot];
                let harness = slot.harness.as_ref()?;
                let opted_in = self
                    .policy
                    .token_budget_harnesses
                    .iter()
                    .any(|h| h == harness);
                let fallback = match (opted_in, choice.tokens) {
                    (true, Some(max_tokens)) => SubagentFallback::TokenBudget { max_tokens },
                    (true, None) | (false, _) => SubagentFallback::Disabled,
                };
                Some(SubagentAllowance {
                    node_id: slot.node_id.clone(),
                    harness: harness.clone(),
                    max_children: choice.width.saturating_sub(1),
                    max_depth: self.caps.max_depth.saturating_sub(self.facts.depth),
                    max_tokens: choice.tokens,
                    fallback,
                })
            })
            .collect()
    }

    /// The premises a topology answer adds: the template's topology facts (a
    /// publisher claim), each admissibility fact, and each cell observation.
    pub(in super::super) fn premises(&self) -> Vec<PremiseRef> {
        let mut out = vec![PremiseRef {
            subject: self.template.graph_id.clone(),
            fact: "topology".to_string(),
            class: PremiseClass::Claim,
            provenance: PremiseProvenance::Publisher {
                component_id: self.template.graph_id.clone(),
                definition_digest: self.template.definition_digest.clone(),
            },
        }];
        out.extend(super::admissible::premises(self));
        out.extend(self.read.headroom.iter().map(|headroom| PremiseRef {
            subject: headroom.cell_id.clone(),
            fact: format!("headroom:{}", super::model::class_name(headroom.class)),
            class: PremiseClass::Observation,
            provenance: PremiseProvenance::CapacityCell {
                cell_id: headroom.cell_id.clone(),
                epoch: headroom.epoch,
            },
        }));
        out
    }
}
