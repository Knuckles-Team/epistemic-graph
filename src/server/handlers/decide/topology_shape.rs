//! `TemplateTopologyShape` at publish time (SWARM-TOPOLOGY-DECIDE-DESIGN §4,
//! ST-4): a graph template's topology facts are checked against its own shape,
//! programmatically and fail-closed, whatever the environment's SHACL gate
//! says -- the DECIDE §4.8 `DecisionShape` pattern. AU publishes the same rules
//! as SHACL (`swarm-topology` shapes) for its own data; this is the authority.
//!
//! The facts' own bounds (`minWidth ≤ maxWidth ≤ MAX`, `1 ≤ k ≤ n`, distinct
//! slots, one lease per class) are `TopologyFacts::validate`, run by the draft
//! itself. The rules here relate the facts to the shape they describe; each is
//! one row of [`RULES`], and the first one violated names the refusal.

use std::collections::BTreeSet;

use eg_types::agent_graph::{AgentGraphDraft, AgentGraphNodeKind};
use eg_types::decision::{SlotRole, StopRule, TopologyFacts};

/// The refusal prefix a caller branches on.
pub(crate) const TOPOLOGY_SHAPE_INVALID: &str = "TOPOLOGY_SHAPE_INVALID";

type Rule = fn(&AgentGraphDraft, &TopologyFacts) -> Option<String>;

/// Every shape rule, in the order they are reported.
const RULES: &[Rule] = &[
    slots_are_the_agent_nodes,
    fan_out_has_fanout_then_join,
    rounds_within_the_stop_rule,
    verifier_pass_needs_a_verifier,
    quorum_fits_the_peers,
    nesting_is_declared,
];

fn agent_nodes(draft: &AgentGraphDraft) -> BTreeSet<&str> {
    draft
        .shape
        .nodes
        .iter()
        .filter(|node| node.kind.agent_pin().is_some())
        .map(|node| node.node_id.as_str())
        .collect()
}

/// Every slot fact names an `Agent` node, and every `Agent` node has one.
fn slots_are_the_agent_nodes(draft: &AgentGraphDraft, facts: &TopologyFacts) -> Option<String> {
    let declared: BTreeSet<&str> = facts.slots.iter().map(|s| s.node_id.as_str()).collect();
    (declared != agent_nodes(draft))
        .then(|| "topology slots must be exactly the template's agent nodes".to_string())
}

fn has_kind(draft: &AgentGraphDraft, kind: &AgentGraphNodeKind) -> bool {
    draft.shape.nodes.iter().any(|node| &node.kind == kind)
}

/// A template with a fan-out (`Child`) slot runs it between a `Fanout` and a
/// `Join` node.
fn fan_out_has_fanout_then_join(draft: &AgentGraphDraft, facts: &TopologyFacts) -> Option<String> {
    let fans_out = facts.slots.iter().any(|slot| slot.role == SlotRole::Child);
    let bracketed =
        has_kind(draft, &AgentGraphNodeKind::Fanout) && has_kind(draft, &AgentGraphNodeKind::Join);
    (fans_out && !bracketed)
        .then(|| "a fan-out slot needs a Fanout node followed by a Join node".to_string())
}

/// No slot declares more rounds than the stop rule lets a run take.
fn rounds_within_the_stop_rule(_: &AgentGraphDraft, facts: &TopologyFacts) -> Option<String> {
    let ceiling = facts.stop.round_ceiling()?;
    facts
        .slots
        .iter()
        .find(|slot| slot.max_rounds > ceiling)
        .map(|slot| {
            format!(
                "slot '{}' declares more rounds than its stop rule",
                slot.node_id
            )
        })
}

/// A verifier-pass stop needs a verifier to pass.
fn verifier_pass_needs_a_verifier(_: &AgentGraphDraft, facts: &TopologyFacts) -> Option<String> {
    let has_verifier = facts
        .slots
        .iter()
        .any(|slot| slot.role == SlotRole::Verifier);
    (matches!(facts.stop, StopRule::VerifierPass { .. }) && !has_verifier)
        .then(|| "a verifier-pass stop rule needs a verifier slot".to_string())
}

/// A quorum of `n` needs peer slots able to field `n` voters.
fn quorum_fits_the_peers(_: &AgentGraphDraft, facts: &TopologyFacts) -> Option<String> {
    let StopRule::Quorum { n, .. } = facts.stop else {
        return None;
    };
    let voters: u32 = facts
        .slots
        .iter()
        .filter(|slot| slot.role == SlotRole::Peer)
        .map(|slot| u32::from(slot.max_width))
        .sum();
    (voters < u32::from(n)).then(|| format!("a quorum of {n} needs at least {n} peer voters"))
}

/// A template that nests another graph declares a depth above one.
fn nesting_is_declared(draft: &AgentGraphDraft, facts: &TopologyFacts) -> Option<String> {
    let nests = draft
        .shape
        .nodes
        .iter()
        .any(|node| matches!(node.kind, AgentGraphNodeKind::Graph { .. }));
    (nests && facts.depth < 2)
        .then(|| "a template that nests a graph declares depth of at least two".to_string())
}

/// The refusal a draft's topology facts earn, if any. A draft without
/// topology facts has nothing to check.
pub(crate) fn refusal(draft: &AgentGraphDraft) -> Option<String> {
    let facts = draft.topology.as_ref()?;
    if let Err(error) = facts.validate() {
        return Some(format!("{TOPOLOGY_SHAPE_INVALID}: {error}"));
    }
    RULES
        .iter()
        .find_map(|rule| rule(draft, facts))
        .map(|detail| format!("{TOPOLOGY_SHAPE_INVALID}: {detail}"))
}

#[cfg(test)]
mod tests;
