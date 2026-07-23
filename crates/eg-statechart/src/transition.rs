//! The **deterministic transition function** δ with extended state
//! (CONCEPT:INT-P2-2).
//!
//! ```text
//! transition(def, state, context, event) -> TransitionOutcome
//! ```
//!
//! This is the ONE place the machine's semantics live, and it is deliberately
//! **pure**: it reads `(def, state, context, event)`, DECIDES the next state, the next
//! context, and the ordered list of actions to perform — and returns them. It never
//! performs I/O, never mutates its inputs, and never executes an emitted effect. An
//! interpreter (the dispatch handler, an agent executor) is the only impure part; it
//! executes the returned effects. That split is what makes the whole engine testable
//! WITHOUT mocks — you call `transition` and assert on the returned value.
//!
//! ## No-op semantics — why illegal transitions are unrepresentable
//!
//! If no transition is defined from `state` on `event` (or every candidate's guard is
//! false), the result is a well-defined **no-op**: stay in `state`, context unchanged,
//! no actions, `fired = false`. There is no "illegal transition" error to handle,
//! because there is no way to *represent* an illegal transition — an undefined edge is
//! simply inert. (A genuinely malformed request — an event that is not in Σ at all, or
//! a state that is not in S — is a different thing: a [`TransitionError`], because it
//! signals a caller bug, not a legitimate "nothing happens here".)
//!
//! ## Determinism
//!
//! When several transitions share a `(from, event)`, their guards are evaluated in
//! DECLARATION ORDER and the FIRST enabled one fires. Given the same
//! `(def, state, context, event)` the outcome is therefore always identical.
//!
//! ## Action order (SCXML-style)
//!
//! A firing transition performs, in order: the source state's `exit` actions, then the
//! transition's own `actions`, then the target state's `entry` actions. The context
//! mutations among them are folded (in that order) into `next_context`; the full
//! ordered list (including `Emit`/`Log`/`Custom` effects) is returned for the
//! interpreter.

use std::collections::{BTreeMap, BTreeSet};

use crate::action::{apply_all, Action};
use crate::context::{Context, EventInput};
use crate::instance::Configuration;
use crate::model::{HistoryKind, StateId, StatechartDef};

/// Why a `(state, event)` produced no state change.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NoOpReason {
    /// No transition is defined from this state on this event.
    NoTransitionDefined,
    /// Transitions exist for this `(state, event)` but every guard evaluated false.
    AllGuardsFalse,
}

/// The result of applying the transition function. Total: `transition` returns this
/// (or a [`TransitionError`] for a malformed request), never panicking.
#[derive(Clone, Debug, PartialEq)]
pub struct TransitionOutcome {
    /// The state after the event (equal to the input state on a no-op).
    pub next_state: StateId,
    /// The context after the event (equal to the input context on a no-op).
    pub next_context: Context,
    /// The ordered actions the interpreter should perform (empty on a no-op). Context
    /// mutations among these are ALREADY reflected in `next_context`; the interpreter
    /// runs only the non-context effects (`Emit`/`Log`/`Custom`).
    pub actions: Vec<Action>,
    /// Whether a transition actually fired (false ⇒ no-op).
    pub fired: bool,
    /// On a no-op, why; `None` when a transition fired.
    pub no_op_reason: Option<NoOpReason>,
    /// The firing transition's label (or `from-event->to`), for tracing; `None` on a
    /// no-op.
    pub fired_label: Option<String>,
}

impl TransitionOutcome {
    fn no_op(state: &str, context: &Context, reason: NoOpReason) -> Self {
        Self {
            next_state: state.to_string(),
            next_context: context.clone(),
            actions: Vec::new(),
            fired: false,
            no_op_reason: Some(reason),
            fired_label: None,
        }
    }

    /// The non-context-mutating effects the interpreter must actually execute
    /// (`Emit`/`Log`/`Custom`), in order — context mutations are already applied.
    pub fn effects(&self) -> impl Iterator<Item = &Action> {
        self.actions.iter().filter(|a| !a.is_context_mutation())
    }
}

/// A request that is not a legitimate transition attempt at all.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransitionError {
    /// The current state is not a declared state of the chart.
    UnknownState(String),
    /// The event symbol is not a member of the alphabet Σ.
    UnknownEvent(String),
    /// A matched transition targets a state the chart does not declare. (A validated
    /// definition can never hit this — [`crate::check`] rejects it at define-time —
    /// but the transition function still fails closed rather than inventing a state.)
    UndeclaredTarget { transition: String, to: String },
    /// The current or target state is composite/parallel/history — beyond the phase-1
    /// flat interpreter. (Again, a validated definition never hits this.)
    CompositeUnsupported(String),
}

impl std::fmt::Display for TransitionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransitionError::UnknownState(s) => write!(f, "unknown state '{s}'"),
            TransitionError::UnknownEvent(e) => write!(f, "event '{e}' is not in the alphabet"),
            TransitionError::UndeclaredTarget { transition, to } => {
                write!(f, "transition {transition} targets undeclared state '{to}'")
            }
            TransitionError::CompositeUnsupported(s) => {
                write!(f, "state '{s}' is composite/parallel/history (unsupported in phase-1)")
            }
        }
    }
}

impl std::error::Error for TransitionError {}

/// Apply the transition function (CONCEPT:INT-P2-2). Pure and side-effect-free.
///
/// * `Ok(outcome)` with `fired = true` — a transition fired; `next_state`/
///   `next_context`/`actions` describe the result.
/// * `Ok(outcome)` with `fired = false` — a well-defined no-op (undefined edge or all
///   guards false); the machine stays put.
/// * `Err(_)` — the REQUEST was malformed (unknown state/event, or — impossible for a
///   validated def — an undeclared target / composite state).
pub fn transition(
    def: &StatechartDef,
    state: &str,
    context: &Context,
    event: &EventInput,
) -> Result<TransitionOutcome, TransitionError> {
    let Some(source) = def.state(state) else {
        return Err(TransitionError::UnknownState(state.to_string()));
    };
    if source.is_composite() {
        return Err(TransitionError::CompositeUnsupported(state.to_string()));
    }
    if !def.in_alphabet(&event.name) {
        return Err(TransitionError::UnknownEvent(event.name.clone()));
    }

    // Determinism: first candidate (declaration order) whose guard holds.
    let mut had_candidate = false;
    for candidate in def.transitions_from(state, &event.name) {
        had_candidate = true;
        if !candidate.guard_holds(context, event) {
            continue;
        }
        // A firing edge must target a declared, non-composite state.
        let label = candidate.label.clone().unwrap_or_else(|| {
            format!("{}-{}->{}", candidate.from, candidate.event, candidate.to)
        });
        let Some(target) = def.state(&candidate.to) else {
            return Err(TransitionError::UndeclaredTarget {
                transition: label,
                to: candidate.to.clone(),
            });
        };
        if target.is_composite() {
            return Err(TransitionError::CompositeUnsupported(candidate.to.clone()));
        }

        // SCXML action order: exit(source) ++ transition ++ entry(target).
        let mut actions =
            Vec::with_capacity(source.exit.len() + candidate.actions.len() + target.entry.len());
        actions.extend(source.exit.iter().cloned());
        actions.extend(candidate.actions.iter().cloned());
        actions.extend(target.entry.iter().cloned());

        let next_context = apply_all(context.clone(), &actions, event);
        return Ok(TransitionOutcome {
            next_state: candidate.to.clone(),
            next_context,
            actions,
            fired: true,
            no_op_reason: None,
            fired_label: Some(label),
        });
    }

    let reason = if had_candidate {
        NoOpReason::AllGuardsFalse
    } else {
        NoOpReason::NoTransitionDefined
    };
    Ok(TransitionOutcome::no_op(state, context, reason))
}

// ── Hierarchical / parallel / history execution (CONCEPT:INT-P2-2, hierarchy phase) ──
//
// The flat `transition` above stays intact for single-state charts (and its tests). The
// GENERAL interpreter below operates on a whole [`Configuration`] and implements
// SCXML-style compound/parallel/history semantics:
//
//   * HIERARCHY  — a composite state holds child states; a transition declared on a
//     parent applies to any active descendant (an atomic state that has no edge for the
//     event delegates upward to its ancestors); entry actions run OUTER-in on entry and
//     exit actions run INNER-out on exit.
//   * PARALLEL   — a parallel composite holds N orthogonal regions, each with its own
//     active child; an event is offered to EVERY region, so several non-conflicting
//     transitions can fire in one step.
//   * HISTORY    — a composite marked shallow/deep remembers its active descendants at
//     exit and RESUMES them on re-entry instead of restarting from its default child.

/// The result of a general (configuration-aware) [`step`]. Mirrors [`TransitionOutcome`]
/// but carries the whole next [`Configuration`] rather than a single next state, and a
/// joined `fired_label` (a parallel step can fire more than one edge at once).
#[derive(Clone, Debug, PartialEq)]
pub struct StepOutcome {
    /// The configuration after the event (equal to the input configuration on a no-op).
    pub next: Configuration,
    /// The context after the event (equal to the input context on a no-op).
    pub next_context: Context,
    /// The ordered actions the interpreter should perform: all exits (inner-out), then
    /// all firing transitions' own actions (document order), then all entries (outer-in).
    /// Context mutations among these are ALREADY reflected in `next_context`.
    pub actions: Vec<Action>,
    /// Whether at least one transition fired (false ⇒ no-op).
    pub fired: bool,
    /// On a no-op, why; `None` when a transition fired.
    pub no_op_reason: Option<NoOpReason>,
    /// The fired edges' labels, joined with `+` (a parallel step may fire several);
    /// `None` on a no-op.
    pub fired_label: Option<String>,
}

impl StepOutcome {
    fn no_op(config: &Configuration, context: &Context, reason: NoOpReason) -> Self {
        Self {
            next: config.clone(),
            next_context: context.clone(),
            actions: Vec::new(),
            fired: false,
            no_op_reason: Some(reason),
            fired_label: None,
        }
    }

    /// The non-context-mutating effects the interpreter must actually execute
    /// (`Emit`/`Log`/`Custom`), in order.
    pub fn effects(&self) -> impl Iterator<Item = &Action> {
        self.actions.iter().filter(|a| !a.is_context_mutation())
    }
}

/// Build the `child id -> parent id` map from the definition's containment tree.
fn parent_map(def: &StatechartDef) -> BTreeMap<&str, &str> {
    let mut map = BTreeMap::new();
    for state in &def.states {
        for child in &state.children {
            map.insert(child.as_str(), state.id.as_str());
        }
    }
    map
}

/// Whether `id` is a proper descendant of `ancestor` in the containment tree.
fn is_proper_descendant(parent: &BTreeMap<&str, &str>, id: &str, ancestor: &str) -> bool {
    let mut cur = parent.get(id).copied();
    while let Some(a) = cur {
        if a == ancestor {
            return true;
        }
        cur = parent.get(a).copied();
    }
    false
}

/// Containment depth (number of ancestors) of `id` — a top-level state is depth 0.
fn depth(parent: &BTreeMap<&str, &str>, id: &str) -> usize {
    let mut d = 0;
    let mut cur = parent.get(id).copied();
    while let Some(a) = cur {
        d += 1;
        cur = parent.get(a).copied();
    }
    d
}

/// Whether a state is composite (has children).
fn is_compound(def: &StatechartDef, id: &str) -> bool {
    def.state(id).map_or(false, |s| !s.children.is_empty())
}

/// The transition domain: the least common compound ancestor of `source` and `target`
/// whose subtree the transition exits and re-enters. `None` denotes the implicit ROOT
/// (a top-level transition, exiting the whole active configuration under the root — the
/// flat case). Mirrors SCXML's `getTransitionDomain`.
fn transition_domain<'a>(
    parent: &BTreeMap<&'a str, &'a str>,
    def: &'a StatechartDef,
    source: &'a str,
    target: &'a str,
) -> Option<&'a str> {
    // Internal-style: target strictly inside a compound source ⇒ source is the domain.
    if is_compound(def, source) && is_proper_descendant(parent, target, source) {
        return Some(source);
    }
    // Otherwise walk up from source to the least compound ancestor that also contains
    // the target (inclusive).
    let mut cur = parent.get(source).copied();
    while let Some(a) = cur {
        if is_compound(def, a) && (a == target || is_proper_descendant(parent, target, a)) {
            return Some(a);
        }
        cur = parent.get(a).copied();
    }
    None
}

/// The active states a transition with the given `domain` exits: every active state that
/// is a proper descendant of the domain (or, for the ROOT domain, the whole active set).
fn exit_set(
    parent: &BTreeMap<&str, &str>,
    active: &BTreeSet<StateId>,
    domain: Option<&str>,
) -> BTreeSet<StateId> {
    active
        .iter()
        .filter(|s| match domain {
            None => true,
            Some(d) => is_proper_descendant(parent, s.as_str(), d),
        })
        .cloned()
        .collect()
}

/// Enter `id` (running its entry actions if newly activated), then descend into its
/// default / history child(ren). The recursive OUTER-in entry primitive.
fn enter_full(
    def: &StatechartDef,
    parent: &BTreeMap<&str, &str>,
    id: &str,
    history: &BTreeMap<StateId, BTreeSet<StateId>>,
    active: &mut BTreeSet<StateId>,
    actions: &mut Vec<Action>,
) {
    if active.insert(id.to_string()) {
        if let Some(s) = def.state(id) {
            actions.extend(s.entry.iter().cloned());
        }
    }
    let Some(s) = def.state(id) else { return };
    if s.children.is_empty() {
        return;
    }
    if s.parallel {
        // Every orthogonal region is entered.
        for child in &s.children {
            enter_full(def, parent, child, history, active, actions);
        }
        return;
    }
    // Compound: pick the child to descend into — resume from history if remembered.
    if let Some(kind) = s.history {
        if let Some(remembered) = history.get(&s.id) {
            if let Some(child) = s.children.iter().find(|c| remembered.contains(*c)) {
                match kind {
                    HistoryKind::Shallow => {
                        // Restore only the immediate child; descend by default below it.
                        enter_full(def, parent, child, history, active, actions);
                        return;
                    }
                    HistoryKind::Deep => {
                        enter_restore(def, parent, child, remembered, history, active, actions);
                        return;
                    }
                }
            }
        }
    }
    let child = s
        .initial_child
        .clone()
        .or_else(|| s.children.first().cloned());
    if let Some(child) = child {
        enter_full(def, parent, &child, history, active, actions);
    }
}

/// Deep-history restore: enter `id` and re-activate exactly the `remembered` descendant
/// subtree (falling back to defaults where the memory does not reach).
fn enter_restore(
    def: &StatechartDef,
    parent: &BTreeMap<&str, &str>,
    id: &str,
    remembered: &BTreeSet<StateId>,
    history: &BTreeMap<StateId, BTreeSet<StateId>>,
    active: &mut BTreeSet<StateId>,
    actions: &mut Vec<Action>,
) {
    if active.insert(id.to_string()) {
        if let Some(s) = def.state(id) {
            actions.extend(s.entry.iter().cloned());
        }
    }
    let Some(s) = def.state(id) else { return };
    if s.children.is_empty() {
        return;
    }
    if s.parallel {
        for child in &s.children {
            if remembered.contains(child) {
                enter_restore(def, parent, child, remembered, history, active, actions);
            } else {
                enter_full(def, parent, child, history, active, actions);
            }
        }
        return;
    }
    if let Some(child) = s.children.iter().find(|c| remembered.contains(*c)) {
        enter_restore(def, parent, child, remembered, history, active, actions);
    } else {
        let child = s
            .initial_child
            .clone()
            .or_else(|| s.children.first().cloned());
        if let Some(child) = child {
            enter_full(def, parent, &child, history, active, actions);
        }
    }
}

/// Enter the path from `domain` (exclusive) down to `target`, then fill `target`'s
/// default/history subtree. Pass-through ancestors get only their entry actions; a
/// pass-through PARALLEL ancestor also default-enters its non-path regions.
fn enter_to_target(
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
    if chain.is_empty() {
        // target IS the domain (rare self-loop on a composite) — descend it fully.
        enter_full(def, parent, target, history, active, actions);
        return;
    }
    let last = chain.len() - 1;
    for (i, node) in chain.iter().enumerate() {
        if i == last {
            enter_full(def, parent, node, history, active, actions);
        } else {
            // Pass-through ancestor: entry actions only, do NOT take its default child
            // (the path continues to the next chain element).
            if active.insert(node.to_string()) {
                if let Some(s) = def.state(node) {
                    actions.extend(s.entry.iter().cloned());
                }
            }
            // If this pass-through node is parallel, its OTHER regions must still be
            // entered by default.
            if let Some(s) = def.state(node) {
                if s.parallel {
                    let next = chain[i + 1];
                    for c in &s.children {
                        if c.as_str() != next {
                            enter_full(def, parent, c, history, active, actions);
                        }
                    }
                }
            }
        }
    }
}

/// The initial configuration of a fresh instance: enter the definition's `initial` state
/// (descending into its default children), collecting the ordered entry actions.
pub fn initial_configuration(
    def: &StatechartDef,
) -> Result<(Configuration, Vec<Action>), TransitionError> {
    if def.state(&def.initial).is_none() {
        return Err(TransitionError::UnknownState(def.initial.clone()));
    }
    let parent = parent_map(def);
    let mut active = BTreeSet::new();
    let mut actions = Vec::new();
    let history = BTreeMap::new();
    enter_full(def, &parent, &def.initial, &history, &mut active, &mut actions);
    Ok((Configuration { active, history }, actions))
}

/// Apply an event to a whole [`Configuration`] (CONCEPT:INT-P2-2). Pure and total,
/// exactly like [`transition`], but hierarchy/parallel/history-aware:
///
/// * `Ok(outcome)` with `fired = true` — at least one edge fired; `next`/`next_context`/
///   `actions` describe the result.
/// * `Ok(outcome)` with `fired = false` — a well-defined no-op.
/// * `Err(_)` — a malformed request (event not in Σ, or an active state not in S).
pub fn step(
    def: &StatechartDef,
    config: &Configuration,
    context: &Context,
    event: &EventInput,
) -> Result<StepOutcome, TransitionError> {
    if !def.in_alphabet(&event.name) {
        return Err(TransitionError::UnknownEvent(event.name.clone()));
    }
    for id in &config.active {
        if def.state(id).is_none() {
            return Err(TransitionError::UnknownState(id.clone()));
        }
    }
    let parent = parent_map(def);
    let active = &config.active;

    // ── Selection: for each active leaf, walk up to the first ancestor with an ENABLED
    //    transition on the event (guards evaluated in declaration order). Dedup by
    //    transition index, keep document order. ──────────────────────────────────────
    let leaves: Vec<&str> = config.leaves(def);
    let mut candidate_idx: BTreeSet<usize> = BTreeSet::new();
    for leaf in &leaves {
        let mut cur: Option<&str> = Some(*leaf);
        'up: while let Some(s) = cur {
            for (idx, t) in def.transitions.iter().enumerate() {
                if t.from == s && t.event == event.name && t.guard_holds(context, event) {
                    candidate_idx.insert(idx);
                    break 'up;
                }
            }
            cur = parent.get(s).copied();
        }
    }

    // ── Conflict resolution: in document order keep transitions whose exit sets are
    //    pairwise disjoint (so orthogonal regions fire together, but two edges in the
    //    same region cannot). ──────────────────────────────────────────────────────
    let mut selected: Vec<usize> = Vec::new();
    for &i in &candidate_idx {
        let ti = &def.transitions[i];
        let di = transition_domain(&parent, def, &ti.from, &ti.to);
        let ei = exit_set(&parent, active, di);
        let conflict = selected.iter().any(|&j| {
            let tj = &def.transitions[j];
            let dj = transition_domain(&parent, def, &tj.from, &tj.to);
            let ej = exit_set(&parent, active, dj);
            !ei.is_disjoint(&ej)
        });
        if !conflict {
            selected.push(i);
        }
    }

    if selected.is_empty() {
        // Distinguish "no edge for this event anywhere in the active nest" from "edges
        // exist but every guard was false".
        let had_candidate = def
            .transitions
            .iter()
            .any(|t| t.event == event.name && active.contains(&t.from));
        let reason = if had_candidate {
            NoOpReason::AllGuardsFalse
        } else {
            NoOpReason::NoTransitionDefined
        };
        return Ok(StepOutcome::no_op(config, context, reason));
    }

    let mut new_active = active.clone();
    let mut new_history = config.history.clone();
    let mut actions: Vec<Action> = Vec::new();

    // Combined exit set across all firing transitions.
    let mut exit_all: BTreeSet<StateId> = BTreeSet::new();
    let mut domains: Vec<Option<String>> = Vec::with_capacity(selected.len());
    for &i in &selected {
        let t = &def.transitions[i];
        let dom = transition_domain(&parent, def, &t.from, &t.to).map(|s| s.to_string());
        for s in exit_set(&parent, &new_active, dom.as_deref()) {
            exit_all.insert(s);
        }
        domains.push(dom);
    }

    // Record history for every exited composite that carries a history marker BEFORE we
    // tear the active set down.
    for h in &exit_all {
        if let Some(st) = def.state(h) {
            if st.history.is_some() {
                let snapshot: BTreeSet<StateId> = new_active
                    .iter()
                    .filter(|s| is_proper_descendant(&parent, s.as_str(), h))
                    .cloned()
                    .collect();
                new_history.insert(h.clone(), snapshot);
            }
        }
    }

    // Exit inner-out (deepest first): run exit actions, drop from active.
    let mut exit_ordered: Vec<&StateId> = exit_all.iter().collect();
    exit_ordered
        .sort_by(|a, b| depth(&parent, b.as_str()).cmp(&depth(&parent, a.as_str())).then(b.cmp(a)));
    for s in exit_ordered {
        if let Some(st) = def.state(s) {
            actions.extend(st.exit.iter().cloned());
        }
        new_active.remove(s);
    }

    // Transition executable content, in document order.
    for &i in &selected {
        actions.extend(def.transitions[i].actions.iter().cloned());
    }

    // Entries, outer-in, per firing transition.
    let mut labels: Vec<String> = Vec::with_capacity(selected.len());
    for (k, &i) in selected.iter().enumerate() {
        let t = &def.transitions[i];
        enter_to_target(
            def,
            &parent,
            domains[k].as_deref(),
            &t.to,
            &new_history,
            &mut new_active,
            &mut actions,
        );
        labels.push(
            t.label
                .clone()
                .unwrap_or_else(|| format!("{}-{}->{}", t.from, t.event, t.to)),
        );
    }

    let next_context = apply_all(context.clone(), &actions, event);
    Ok(StepOutcome {
        next: Configuration {
            active: new_active,
            history: new_history,
        },
        next_context,
        actions,
        fired: true,
        no_op_reason: None,
        fired_label: Some(labels.join("+")),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::action::{Action, ActionValue};
    use crate::guard::Guard;
    use crate::model::{State, Transition};

    /// A turnstile: Locked --coin--> Unlocked --push--> Locked, plus a guarded
    /// counter and entry/exit actions to exercise Moore+Mealy data flow.
    fn turnstile() -> StatechartDef {
        StatechartDef {
            name: "turnstile".into(),
            schema_version: 1,
            states: vec![
                State::new("locked")
                    .with_entry(vec![Action::Assign {
                        key: "at".into(),
                        value: ActionValue::Const { value: serde_json::json!("locked") },
                    }]),
                State::new("unlocked")
                    .with_entry(vec![Action::Assign {
                        key: "at".into(),
                        value: ActionValue::Const { value: serde_json::json!("unlocked") },
                    }]),
            ],
            alphabet: vec!["coin".into(), "push".into()],
            transitions: vec![
                Transition::new("locked", "coin", "unlocked").with_actions(vec![Action::Assign {
                    key: "coins".into(),
                    value: ActionValue::Const { value: serde_json::json!(1) },
                }]),
                Transition::new("unlocked", "push", "locked"),
            ],
            initial: "locked".into(),
            finals: vec![],
            meta: Default::default(),
        }
    }

    #[test]
    fn fires_defined_edge_and_applies_action_order() {
        let def = turnstile();
        let ctx = Context::new();
        let out = transition(&def, "locked", &ctx, &EventInput::new("coin")).unwrap();
        assert!(out.fired);
        assert_eq!(out.next_state, "unlocked");
        // transition action set coins=1, then entry(unlocked) set at="unlocked".
        assert_eq!(out.next_context.get("coins"), Some(&serde_json::json!(1)));
        assert_eq!(out.next_context.get("at"), Some(&serde_json::json!("unlocked")));
    }

    #[test]
    fn undefined_edge_is_a_noop_not_an_error() {
        let def = turnstile();
        let ctx = Context::new();
        // 'push' from 'locked' is undefined ⇒ stay put, nothing happens.
        let out = transition(&def, "locked", &ctx, &EventInput::new("push")).unwrap();
        assert!(!out.fired);
        assert_eq!(out.next_state, "locked");
        assert_eq!(out.no_op_reason, Some(NoOpReason::NoTransitionDefined));
        assert!(out.next_context.is_empty());
    }

    #[test]
    fn event_outside_alphabet_is_a_request_error() {
        let def = turnstile();
        let err = transition(&def, "locked", &Context::new(), &EventInput::new("teleport"))
            .unwrap_err();
        assert_eq!(err, TransitionError::UnknownEvent("teleport".into()));
    }

    #[test]
    fn first_matching_guard_wins_deterministically() {
        let def = StatechartDef {
            name: "guarded".into(),
            schema_version: 0,
            states: vec![State::new("a"), State::new("hi"), State::new("lo")],
            alphabet: vec!["go".into()],
            transitions: vec![
                Transition::new("a", "go", "hi")
                    .with_guard(Guard::Gt { key: "n".into(), value: 10.0 }),
                Transition::new("a", "go", "lo"),
            ],
            initial: "a".into(),
            finals: vec![],
            meta: Default::default(),
        };
        let mut hi = Context::new();
        hi.set("n", serde_json::json!(20));
        assert_eq!(transition(&def, "a", &hi, &EventInput::new("go")).unwrap().next_state, "hi");
        // n<=10 ⇒ first guard false ⇒ fall through to the unguarded 'lo' edge.
        let lo = Context::new();
        assert_eq!(transition(&def, "a", &lo, &EventInput::new("go")).unwrap().next_state, "lo");
    }

    #[test]
    fn all_guards_false_is_a_distinct_noop_reason() {
        let def = StatechartDef {
            name: "veto".into(),
            schema_version: 0,
            states: vec![State::new("a"), State::new("b")],
            alphabet: vec!["go".into()],
            transitions: vec![
                Transition::new("a", "go", "b").with_guard(Guard::Never),
            ],
            initial: "a".into(),
            finals: vec![],
            meta: Default::default(),
        };
        let out = transition(&def, "a", &Context::new(), &EventInput::new("go")).unwrap();
        assert!(!out.fired);
        assert_eq!(out.no_op_reason, Some(NoOpReason::AllGuardsFalse));
    }

    #[test]
    fn purity_input_context_is_never_mutated() {
        let def = turnstile();
        let ctx = Context::new();
        let _ = transition(&def, "locked", &ctx, &EventInput::new("coin")).unwrap();
        // The caller's context is untouched — the function returns a NEW one.
        assert!(ctx.is_empty());
    }

    // ── Hierarchy / parallel / history execution ─────────────────────────────────────

    fn log_messages(actions: &[Action]) -> Vec<String> {
        actions
            .iter()
            .filter_map(|a| match a {
                Action::Log { message } => Some(message.clone()),
                _ => None,
            })
            .collect()
    }

    /// A composite `active` state (idle|running) plus a top-level `off`. A `kill` edge is
    /// declared on the PARENT, so it applies whichever inner state is active. Entry/exit
    /// Log markers let the test observe outer-in / inner-out ordering.
    fn nested_chart() -> StatechartDef {
        let mut active = State::new("active")
            .with_entry(vec![Action::Log { message: "enter-active".into() }])
            .with_exit(vec![Action::Log { message: "exit-active".into() }]);
        active.children = vec!["idle".into(), "running".into()];
        active.initial_child = Some("idle".into());
        let running = State::new("running")
            .with_entry(vec![Action::Log { message: "enter-running".into() }])
            .with_exit(vec![Action::Log { message: "exit-running".into() }]);
        StatechartDef {
            name: "nested".into(),
            schema_version: 1,
            states: vec![active, State::new("idle"), running, State::new("off")],
            alphabet: vec!["start".into(), "stop".into(), "kill".into()],
            transitions: vec![
                Transition::new("idle", "start", "running"),
                Transition::new("running", "stop", "idle"),
                Transition::new("active", "kill", "off"),
            ],
            initial: "active".into(),
            finals: vec![],
            meta: Default::default(),
        }
    }

    #[test]
    fn hierarchy_initial_descends_and_parent_edge_applies_to_descendants() {
        let def = nested_chart();
        let (config, _entry) = initial_configuration(&def).unwrap();
        // Entering `active` descends into its default child `idle`.
        assert!(config.contains("active") && config.contains("idle"));
        assert_eq!(config.active.len(), 2);

        // start: the inner region moves idle -> running; `active` stays active.
        let out = step(&def, &config, &Context::new(), &EventInput::new("start")).unwrap();
        assert!(out.fired);
        assert!(out.next.contains("active") && out.next.contains("running"));
        assert!(!out.next.contains("idle"));

        // kill: `running` has no such edge, so it delegates UP to `active`'s edge.
        let out2 = step(&def, &out.next, &Context::new(), &EventInput::new("kill")).unwrap();
        assert!(out2.fired);
        assert_eq!(
            out2.next.active.iter().cloned().collect::<Vec<_>>(),
            vec!["off".to_string()]
        );
        // exit runs INNER-out: running before active.
        assert_eq!(
            log_messages(&out2.actions),
            vec!["exit-running".to_string(), "exit-active".to_string()]
        );
    }

    /// A parallel state `p` with two orthogonal regions r1 (a1|a2) and r2 (b1|b2).
    fn parallel_chart() -> StatechartDef {
        let mut p = State::new("p");
        p.children = vec!["r1".into(), "r2".into()];
        p.parallel = true;
        let mut r1 = State::new("r1");
        r1.children = vec!["a1".into(), "a2".into()];
        r1.initial_child = Some("a1".into());
        let mut r2 = State::new("r2");
        r2.children = vec!["b1".into(), "b2".into()];
        r2.initial_child = Some("b1".into());
        StatechartDef {
            name: "parallel".into(),
            schema_version: 1,
            states: vec![
                p,
                r1,
                State::new("a1"),
                State::new("a2"),
                r2,
                State::new("b1"),
                State::new("b2"),
            ],
            alphabet: vec!["e1".into(), "e2".into(), "sync".into()],
            transitions: vec![
                Transition::new("a1", "e1", "a2"),
                Transition::new("b1", "e2", "b2"),
                Transition::new("a1", "sync", "a2"),
                Transition::new("b1", "sync", "b2"),
            ],
            initial: "p".into(),
            finals: vec![],
            meta: Default::default(),
        }
    }

    #[test]
    fn parallel_regions_are_independent_and_sync_fires_every_region() {
        let def = parallel_chart();
        let (config, _entry) = initial_configuration(&def).unwrap();
        // Entering `p` enters BOTH regions and each region's initial child.
        for id in ["p", "r1", "a1", "r2", "b1"] {
            assert!(config.contains(id), "expected {id} active");
        }
        assert_eq!(config.active.len(), 5);

        // e1 touches only region 1.
        let out = step(&def, &config, &Context::new(), &EventInput::new("e1")).unwrap();
        assert!(out.next.contains("a2"));
        assert!(out.next.contains("b1")); // region 2 untouched

        // sync fires an edge in BOTH regions in a single step.
        let out2 = step(&def, &config, &Context::new(), &EventInput::new("sync")).unwrap();
        assert!(out2.fired);
        assert!(out2.next.contains("a2") && out2.next.contains("b2"));
        assert!(!out2.next.contains("a1") && !out2.next.contains("b1"));
    }

    /// Shallow-history `menu` (m1|m2|m3) with a `screensaver` sibling. Re-entering `menu`
    /// after `wake` must resume the remembered child, not restart at m1.
    fn shallow_history_chart() -> StatechartDef {
        let mut menu = State::new("menu");
        menu.children = vec!["m1".into(), "m2".into(), "m3".into()];
        menu.initial_child = Some("m1".into());
        menu.history = Some(HistoryKind::Shallow);
        StatechartDef {
            name: "history".into(),
            schema_version: 1,
            states: vec![
                menu,
                State::new("m1"),
                State::new("m2"),
                State::new("m3"),
                State::new("screensaver"),
            ],
            alphabet: vec!["next".into(), "sleep".into(), "wake".into()],
            transitions: vec![
                Transition::new("m1", "next", "m2"),
                Transition::new("m2", "next", "m3"),
                Transition::new("menu", "sleep", "screensaver"),
                Transition::new("screensaver", "wake", "menu"),
            ],
            initial: "menu".into(),
            finals: vec![],
            meta: Default::default(),
        }
    }

    #[test]
    fn shallow_history_resumes_last_active_child() {
        let def = shallow_history_chart();
        let (config, _entry) = initial_configuration(&def).unwrap();
        assert!(config.contains("m1"));

        // Advance the menu to m2.
        let out = step(&def, &config, &Context::new(), &EventInput::new("next")).unwrap();
        assert!(out.next.contains("m2"));

        // Sleep out of the menu — history records menu = {m2}.
        let out2 = step(&def, &out.next, &Context::new(), &EventInput::new("sleep")).unwrap();
        assert!(out2.next.contains("screensaver") && !out2.next.contains("menu"));

        // Wake back into the menu — shallow history RESUMES m2 (not the default m1).
        let out3 = step(&def, &out2.next, &Context::new(), &EventInput::new("wake")).unwrap();
        assert!(out3.next.contains("menu"));
        assert!(
            out3.next.contains("m2"),
            "shallow history should resume m2, got {:?}",
            out3.next.active
        );
        assert!(!out3.next.contains("m1") && !out3.next.contains("m3"));
    }

    /// Deep-history `outer` wrapping a compound `inner` (x1|x2). Deep history must resume
    /// the FULL nested subtree (down to x2), where shallow would only restore `inner` and
    /// then default back to x1.
    fn deep_history_chart() -> StatechartDef {
        let mut outer = State::new("outer");
        outer.children = vec!["inner".into(), "other".into()];
        outer.initial_child = Some("inner".into());
        outer.history = Some(HistoryKind::Deep);
        let mut inner = State::new("inner");
        inner.children = vec!["x1".into(), "x2".into()];
        inner.initial_child = Some("x1".into());
        StatechartDef {
            name: "deep".into(),
            schema_version: 1,
            states: vec![
                outer,
                inner,
                State::new("x1"),
                State::new("x2"),
                State::new("other"),
                State::new("parked"),
            ],
            alphabet: vec!["go".into(), "pause".into(), "resume".into()],
            transitions: vec![
                Transition::new("x1", "go", "x2"),
                Transition::new("outer", "pause", "parked"),
                Transition::new("parked", "resume", "outer"),
            ],
            initial: "outer".into(),
            finals: vec![],
            meta: Default::default(),
        }
    }

    #[test]
    fn deep_history_resumes_full_nested_subtree() {
        let def = deep_history_chart();
        let (config, _entry) = initial_configuration(&def).unwrap();
        for id in ["outer", "inner", "x1"] {
            assert!(config.contains(id), "expected {id} active initially");
        }

        // Drive the deep leaf to x2.
        let out = step(&def, &config, &Context::new(), &EventInput::new("go")).unwrap();
        assert!(out.next.contains("x2") && !out.next.contains("x1"));

        // Pause out of `outer` — deep history records {inner, x2}.
        let out2 = step(&def, &out.next, &Context::new(), &EventInput::new("pause")).unwrap();
        assert!(out2.next.contains("parked") && !out2.next.contains("outer"));

        // Resume — deep history restores the ENTIRE subtree, back to x2.
        let out3 = step(&def, &out2.next, &Context::new(), &EventInput::new("resume")).unwrap();
        for id in ["outer", "inner", "x2"] {
            assert!(out3.next.contains(id), "deep resume should restore {id}: {:?}", out3.next.active);
        }
        assert!(!out3.next.contains("x1"));
    }

    #[test]
    fn step_on_flat_chart_matches_flat_transition() {
        // The general interpreter must reproduce flat behaviour exactly.
        let def = turnstile();
        let config = Configuration::atomic("locked");
        let out = step(&def, &config, &Context::new(), &EventInput::new("coin")).unwrap();
        assert!(out.fired);
        assert_eq!(
            out.next.active.iter().cloned().collect::<Vec<_>>(),
            vec!["unlocked".to_string()]
        );
        // undefined edge from locked is a no-op.
        let noop = step(&def, &config, &Context::new(), &EventInput::new("push")).unwrap();
        assert!(!noop.fired);
        assert_eq!(noop.no_op_reason, Some(NoOpReason::NoTransitionDefined));
        // event outside Σ is a hard request error.
        assert!(step(&def, &config, &Context::new(), &EventInput::new("teleport")).is_err());
    }
}
