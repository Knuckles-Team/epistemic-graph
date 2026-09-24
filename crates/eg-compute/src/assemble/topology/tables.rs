//! Precomputed integer tables: every (width, rounds) choice of every slot and
//! the quantities it implies, so each topology row stays LINEAR in its
//! one-hot variables (SWARM-TOPOLOGY-DECIDE-DESIGN §3).
//!
//! A slot's load `n` is the request's subtasks for a `Child` (fan-out) slot
//! and one turn for every other role. With `w` agents and `k` rounds:
//!
//! * turns per agent `⌈n / w⌉`;
//! * makespan `k · ⌈n / w⌉ · p95` -- slots summed as if sequential, an upper
//!   bound for any parallel branch;
//! * concurrent agents `w` (what a lease is sized by);
//! * tokens `w · k · ⌈n / w⌉ · tokens-per-turn`.
//!
//! An undeclared p95 or token count is `None`, never zero.

use eg_types::decision::{SlotRole, SlotTopology, TopologyCaps, TopologyFacts};

/// One (width, rounds) choice of one slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Choice {
    pub slot: usize,
    pub width: u8,
    pub rounds: u8,
    pub makespan_ms: Option<u64>,
    pub tokens: Option<u64>,
}

/// The rounds a slot may run: its own ceiling, the caps' and the stop rule's.
fn round_ceiling(facts: &TopologyFacts, slot: &SlotTopology, caps: &TopologyCaps) -> u8 {
    let rule = facts.stop.round_ceiling().unwrap_or(u8::MAX);
    slot.max_rounds.min(caps.max_rounds).min(rule)
}

fn load(slot: &SlotTopology, subtasks: u32) -> u64 {
    match slot.role {
        SlotRole::Child => u64::from(subtasks.max(1)),
        SlotRole::Parent | SlotRole::Peer | SlotRole::Aggregator | SlotRole::Verifier => 1,
    }
}

fn choice(index: usize, slot: &SlotTopology, subtasks: u32, width: u8, rounds: u8) -> Choice {
    let turns = load(slot, subtasks).div_ceil(u64::from(width));
    let rounds_turns = u64::from(rounds).saturating_mul(turns);
    Choice {
        slot: index,
        width,
        rounds,
        makespan_ms: slot
            .p95_ms
            .map(|p95| rounds_turns.saturating_mul(u64::from(p95))),
        tokens: slot.tokens.map(|tokens| {
            u64::from(width)
                .saturating_mul(rounds_turns)
                .saturating_mul(tokens)
        }),
    }
}

/// Every choice of slot `index`, width ascending then rounds ascending; empty
/// when the caps leave the slot no width or no round.
pub(crate) fn slot_choices(
    facts: &TopologyFacts,
    index: usize,
    caps: &TopologyCaps,
    subtasks: u32,
) -> Vec<Choice> {
    let slot = &facts.slots.as_slice()[index];
    let max_width = slot.max_width.min(caps.max_width);
    let max_rounds = round_ceiling(facts, slot, caps);
    (slot.min_width..=max_width)
        .flat_map(|width| (1..=max_rounds).map(move |rounds| (width, rounds)))
        .map(|(width, rounds)| choice(index, slot, subtasks, width, rounds))
        .collect()
}

/// The fewest agents the fan-out needs: `⌈subtasks / per_agent_subtasks⌉`.
pub(crate) fn demanded_width(subtasks: u32, per_agent_subtasks: u32) -> u64 {
    u64::from(subtasks).div_ceil(u64::from(per_agent_subtasks.max(1)))
}
