//! SCXML entry semantics (CONCEPT:INT-P2-2): what it means to ENTER a state.
//!
//! Entry is outer-in and recursive. Activating a state runs its `onentry` actions the
//! first time it joins the configuration; a parallel state then enters every orthogonal
//! region, and a compound state descends into exactly one child — the one its history
//! remembers if it has any, otherwise its default. Deep history restores the remembered
//! descendant subtree and falls back to defaults wherever the memory does not reach.
//! Entering along a transition path is the same walk with the pass-through ancestors
//! getting their entry actions only, since the path continues past them.

use std::collections::{BTreeMap, BTreeSet};

use crate::action::Action;
use crate::model::{HistoryKind, State, StateId, StatechartDef};

/// Activate `id`, running its entry actions the first time it enters the configuration,
/// and answer with its definition only when there is something to descend into — `None`
/// for an undeclared or atomic state. The shared prologue of every entry walk.
fn activate<'a>(
    def: &'a StatechartDef,
    id: &str,
    active: &mut BTreeSet<StateId>,
    actions: &mut Vec<Action>,
) -> Option<&'a State> {
    if active.insert(id.to_string()) {
        if let Some(s) = def.state(id) {
            actions.extend(s.entry.iter().cloned());
        }
    }
    let s = def.state(id)?;
    if s.children.is_empty() {
        None
    } else {
        Some(s)
    }
}

/// The child a compound state descends into when nothing is remembered: its declared
/// `initial_child`, else its first child.
fn default_child(s: &State) -> Option<StateId> {
    s.initial_child
        .clone()
        .or_else(|| s.children.first().cloned())
}

/// The remembered child to resume into and how deeply to restore it, or `None` when this
/// state carries no history marker or has nothing remembered inside it.
fn remembered_child<'a>(
    s: &'a State,
    history: &'a BTreeMap<StateId, BTreeSet<StateId>>,
) -> Option<(HistoryKind, &'a StateId, &'a BTreeSet<StateId>)> {
    let kind = s.history?;
    let remembered = history.get(&s.id)?;
    let child = s.children.iter().find(|c| remembered.contains(*c))?;
    Some((kind, child, remembered))
}

/// Enter `id` (running its entry actions if newly activated), then descend into its
/// default / history child(ren). The recursive OUTER-in entry primitive.
pub(super) fn enter_full(
    def: &StatechartDef,
    id: &str,
    history: &BTreeMap<StateId, BTreeSet<StateId>>,
    active: &mut BTreeSet<StateId>,
    actions: &mut Vec<Action>,
) {
    let Some(s) = activate(def, id, active, actions) else {
        return;
    };
    if s.parallel {
        // Every orthogonal region is entered.
        for child in &s.children {
            enter_full(def, child, history, active, actions);
        }
        return;
    }
    // Compound: pick the child to descend into — resume from history if remembered.
    match remembered_child(s, history) {
        // Shallow history restores only the immediate child; descend by default below it.
        Some((HistoryKind::Shallow, child, _)) => enter_full(def, child, history, active, actions),
        Some((HistoryKind::Deep, child, remembered)) => {
            enter_restore(def, child, remembered, history, active, actions)
        }
        None => {
            if let Some(child) = default_child(s) {
                enter_full(def, &child, history, active, actions);
            }
        }
    }
}

/// Deep-history restore: enter `id` and re-activate exactly the `remembered` descendant
/// subtree (falling back to defaults where the memory does not reach).
fn enter_restore(
    def: &StatechartDef,
    id: &str,
    remembered: &BTreeSet<StateId>,
    history: &BTreeMap<StateId, BTreeSet<StateId>>,
    active: &mut BTreeSet<StateId>,
    actions: &mut Vec<Action>,
) {
    let Some(s) = activate(def, id, active, actions) else {
        return;
    };
    if s.parallel {
        for child in &s.children {
            if remembered.contains(child) {
                enter_restore(def, child, remembered, history, active, actions);
            } else {
                enter_full(def, child, history, active, actions);
            }
        }
        return;
    }
    match s.children.iter().find(|c| remembered.contains(*c)) {
        Some(child) => enter_restore(def, child, remembered, history, active, actions),
        None => {
            if let Some(child) = default_child(s) {
                enter_full(def, &child, history, active, actions);
            }
        }
    }
}

/// Enter the path from `domain` (exclusive) down to `target`, then fill `target`'s
/// default/history subtree. Pass-through ancestors get only their entry actions; a
/// pass-through PARALLEL ancestor also default-enters its non-path regions.
pub(super) fn enter_to_target(
    def: &StatechartDef,
    parent: &BTreeMap<&str, &str>,
    domain: Option<&str>,
    target: &str,
    history: &BTreeMap<StateId, BTreeSet<StateId>>,
    active: &mut BTreeSet<StateId>,
    actions: &mut Vec<Action>,
) {
    // Chain from target up to (but excluding) the domain, then reversed to outer-in.
    let mut chain: Vec<&str> = Vec::new();
    let mut cur: Option<&str> = Some(target);
    while let Some(c) = cur {
        if Some(c) == domain {
            break;
        }
        chain.push(c);
        cur = parent.get(c).copied();
    }
    chain.reverse();
    let Some((&deepest, ancestors)) = chain.split_last() else {
        // target IS the domain (rare self-loop on a composite) — descend it fully.
        enter_full(def, target, history, active, actions);
        return;
    };
    for (i, node) in ancestors.iter().enumerate() {
        enter_pass_through(def, node, chain[i + 1], history, active, actions);
    }
    enter_full(def, deepest, history, active, actions);
}

/// Enter a pass-through ancestor on an entry path: its entry actions only — it must NOT
/// take its default child, because the path continues to `next`. A parallel pass-through
/// still default-enters every region the path does not run through.
fn enter_pass_through(
    def: &StatechartDef,
    node: &str,
    next: &str,
    history: &BTreeMap<StateId, BTreeSet<StateId>>,
    active: &mut BTreeSet<StateId>,
    actions: &mut Vec<Action>,
) {
    if active.insert(node.to_string()) {
        if let Some(s) = def.state(node) {
            actions.extend(s.entry.iter().cloned());
        }
    }
    let Some(s) = def.state(node) else { return };
    if !s.parallel {
        return;
    }
    for c in &s.children {
        if c.as_str() != next {
            enter_full(def, c, history, active, actions);
        }
    }
}
