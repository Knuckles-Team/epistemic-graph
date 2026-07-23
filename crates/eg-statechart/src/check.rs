//! Definition-time **completeness / reachability checks** and the exhaustive
//! **table-walk** utility (CONCEPT:INT-P2-2, requirement 5).
//!
//! A statechart is only trustworthy if it is well-formed BEFORE any instance runs.
//! [`validate`] proves, at define-time, a set of structural invariants and refuses to
//! store a chart that violates any of them:
//!
//! * s₀ ∈ S, and F ⊆ S.
//! * No duplicate state ids; S is non-empty.
//! * Every transition's `from`/`to` is a declared state, and its `event` ∈ Σ.
//! * Every state is REACHABLE from s₀ (no dead states).
//! * Final states have NO outgoing transitions (a final is terminal).
//! * (Phase-1) no state is composite/parallel/history — the flat interpreter's
//!   contract (see [`crate::model`]).
//!
//! Softer observations that do not block storage are surfaced as [`DefWarning`]s:
//! nondeterministic `(state, event)` pairs (two unguarded edges), shadowed edges, and
//! unused alphabet symbols.
//!
//! [`coverage_matrix`] enumerates the FULL S × Σ grid and classifies every cell as a
//! defined transition or a documented no-op — the exhaustive table-walk requirement 5
//! asks for. Because every cell is provably one or the other by construction,
//! [`assert_total`] can never fail on a validated chart; its VALUE is that tests use
//! the matrix to assert a chart's EXACT shape, so an accidentally-deleted edge shows
//! up as a changed cell.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::model::{EventName, StateId, StatechartDef};

/// A hard structural defect that blocks storage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DefError {
    /// S is empty.
    NoStates,
    /// Two states share an id.
    DuplicateState(String),
    /// s₀ is not a declared state.
    InitialNotDeclared(String),
    /// A final id is not a declared state.
    FinalNotDeclared(String),
    /// A transition's source is not a declared state.
    TransitionFromUndeclared { transition: String, from: String },
    /// A transition's target is not a declared state.
    TransitionToUndeclared { transition: String, to: String },
    /// A transition's event is not in Σ.
    EventNotInAlphabet { transition: String, event: String },
    /// A state cannot be reached from s₀.
    UnreachableState(String),
    /// A final state has an outgoing transition.
    FinalHasOutgoing { state: String, transition: String },
    /// A state uses composite/parallel/history features (unsupported in phase-1).
    /// Retained for wire/back-compat; the hierarchy phase no longer emits it — composite
    /// charts are accepted and checked structurally instead.
    CompositeUnsupported(String),
    /// The alphabet contains a duplicate symbol.
    DuplicateAlphabetSymbol(String),
    /// A composite state lists a child that is not a declared state.
    ChildNotDeclared { parent: String, child: String },
    /// A composite state's `initial_child` is not among its declared children.
    InitialChildNotInChildren { parent: String, child: String },
    /// A state is listed as a child of more than one composite (the containment tree
    /// must be a forest — every state has at most one parent).
    MultipleParents(String),
    /// The containment tree has a cycle reachable from this state (a state cannot be its
    /// own ancestor).
    CompositeCycle(String),
    /// An ATOMIC state (no children) carries a history marker — history only means
    /// something on a composite state that has children to remember.
    HistoryOnAtomic(String),
}

impl std::fmt::Display for DefError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DefError::NoStates => write!(f, "chart has no states"),
            DefError::DuplicateState(s) => write!(f, "duplicate state id '{s}'"),
            DefError::InitialNotDeclared(s) => write!(f, "initial state '{s}' is not declared"),
            DefError::FinalNotDeclared(s) => write!(f, "final state '{s}' is not declared"),
            DefError::TransitionFromUndeclared { transition, from } => {
                write!(
                    f,
                    "transition {transition} starts from undeclared state '{from}'"
                )
            }
            DefError::TransitionToUndeclared { transition, to } => {
                write!(f, "transition {transition} targets undeclared state '{to}'")
            }
            DefError::EventNotInAlphabet { transition, event } => {
                write!(
                    f,
                    "transition {transition} uses event '{event}' not in the alphabet"
                )
            }
            DefError::UnreachableState(s) => {
                write!(f, "state '{s}' is unreachable from the initial state")
            }
            DefError::FinalHasOutgoing { state, transition } => {
                write!(
                    f,
                    "final state '{state}' has an outgoing transition {transition}"
                )
            }
            DefError::CompositeUnsupported(s) => {
                write!(
                    f,
                    "state '{s}' is composite/parallel/history (unsupported in phase-1)"
                )
            }
            DefError::DuplicateAlphabetSymbol(s) => write!(f, "duplicate alphabet symbol '{s}'"),
            DefError::ChildNotDeclared { parent, child } => {
                write!(
                    f,
                    "composite state '{parent}' lists undeclared child '{child}'"
                )
            }
            DefError::InitialChildNotInChildren { parent, child } => {
                write!(
                    f,
                    "state '{parent}' initial_child '{child}' is not one of its children"
                )
            }
            DefError::MultipleParents(s) => {
                write!(f, "state '{s}' is a child of more than one composite state")
            }
            DefError::CompositeCycle(s) => {
                write!(f, "state '{s}' is part of a containment cycle")
            }
            DefError::HistoryOnAtomic(s) => {
                write!(
                    f,
                    "atomic state '{s}' carries a history marker but has no children"
                )
            }
        }
    }
}

/// A soft observation that does not block storage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DefWarning {
    /// Two or more unguarded transitions share a `(state, event)`; only the first can
    /// ever fire (the rest are dead). Deterministic, but almost certainly a mistake.
    NondeterministicPair {
        state: StateId,
        event: EventName,
        count: usize,
    },
    /// An alphabet symbol never appears on any transition.
    UnusedAlphabetSymbol(EventName),
}

/// The full result of validating a definition.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CompletenessReport {
    /// Hard defects (any ⇒ the chart is invalid and must not be stored).
    pub errors: Vec<DefError>,
    /// Soft observations.
    pub warnings: Vec<DefWarning>,
}

impl CompletenessReport {
    /// Whether the definition is free of hard defects.
    pub fn is_valid(&self) -> bool {
        self.errors.is_empty()
    }
}

/// Validate a definition (CONCEPT:INT-P2-2). `Ok(report)` when there are no hard
/// [`DefError`]s (the report may still carry warnings); `Err(report)` when at least
/// one hard error is present.
pub fn validate(def: &StatechartDef) -> Result<CompletenessReport, CompletenessReport> {
    let mut report = CompletenessReport::default();

    // ── S well-formed ────────────────────────────────────────────────────────────
    if def.states.is_empty() {
        report.errors.push(DefError::NoStates);
    }
    let mut seen_states = BTreeSet::new();
    for state in &def.states {
        if !seen_states.insert(state.id.as_str()) {
            report
                .errors
                .push(DefError::DuplicateState(state.id.clone()));
        }
    }

    // ── containment tree well-formed (hierarchy / parallel / history) ────────────────
    // A composite state's children must be declared, form a forest (single parent, no
    // cycle), and any initial_child must be a real child; history only means something on
    // a composite. This replaces the phase-1 blanket rejection of composite charts.
    let declared: BTreeSet<&str> = def.state_ids();
    let mut parents_of: BTreeMap<&str, usize> = BTreeMap::new();
    for state in &def.states {
        for child in &state.children {
            if !declared.contains(child.as_str()) {
                report.errors.push(DefError::ChildNotDeclared {
                    parent: state.id.clone(),
                    child: child.clone(),
                });
            }
            *parents_of.entry(child.as_str()).or_default() += 1;
        }
        if let Some(initial_child) = &state.initial_child {
            if !state.children.iter().any(|c| c == initial_child) {
                report.errors.push(DefError::InitialChildNotInChildren {
                    parent: state.id.clone(),
                    child: initial_child.clone(),
                });
            }
        }
        if state.history.is_some() && state.children.is_empty() {
            report
                .errors
                .push(DefError::HistoryOnAtomic(state.id.clone()));
        }
    }
    for (child, count) in &parents_of {
        if *count > 1 {
            report
                .errors
                .push(DefError::MultipleParents(child.to_string()));
        }
    }
    // Cycle guard: walk each state up its (single-parent) containment chain; a revisit of
    // the start, or an over-long walk, is a cycle.
    let single_parent: BTreeMap<&str, &str> = def
        .states
        .iter()
        .flat_map(|s| s.children.iter().map(move |c| (c.as_str(), s.id.as_str())))
        .filter(|(child, _)| parents_of.get(*child).copied() == Some(1))
        .collect();
    for state in &def.states {
        let mut cur = single_parent.get(state.id.as_str()).copied();
        let mut steps = 0usize;
        while let Some(ancestor) = cur {
            if ancestor == state.id.as_str() || steps > def.states.len() {
                report
                    .errors
                    .push(DefError::CompositeCycle(state.id.clone()));
                break;
            }
            steps += 1;
            cur = single_parent.get(ancestor).copied();
        }
    }

    // ── Σ well-formed ────────────────────────────────────────────────────────────
    let mut seen_symbols = BTreeSet::new();
    for symbol in &def.alphabet {
        if !seen_symbols.insert(symbol.as_str()) {
            report
                .errors
                .push(DefError::DuplicateAlphabetSymbol(symbol.clone()));
        }
    }

    // ── s₀ ∈ S, F ⊆ S ────────────────────────────────────────────────────────────
    if !def.has_state(&def.initial) {
        report
            .errors
            .push(DefError::InitialNotDeclared(def.initial.clone()));
    }
    for final_id in &def.finals {
        if !def.has_state(final_id) {
            report
                .errors
                .push(DefError::FinalNotDeclared(final_id.clone()));
        }
    }

    // ── δ well-formed + finals are terminal + alphabet usage ─────────────────────
    let mut used_symbols = BTreeSet::new();
    for t in &def.transitions {
        let label = t
            .label
            .clone()
            .unwrap_or_else(|| format!("{}-{}->{}", t.from, t.event, t.to));
        used_symbols.insert(t.event.as_str());
        if !def.has_state(&t.from) {
            report.errors.push(DefError::TransitionFromUndeclared {
                transition: label.clone(),
                from: t.from.clone(),
            });
        }
        if !def.has_state(&t.to) {
            report.errors.push(DefError::TransitionToUndeclared {
                transition: label.clone(),
                to: t.to.clone(),
            });
        }
        if !def.in_alphabet(&t.event) {
            report.errors.push(DefError::EventNotInAlphabet {
                transition: label.clone(),
                event: t.event.clone(),
            });
        }
        if def.is_final(&t.from) {
            report.errors.push(DefError::FinalHasOutgoing {
                state: t.from.clone(),
                transition: label,
            });
        }
    }
    for symbol in &def.alphabet {
        if !used_symbols.contains(symbol.as_str()) {
            report
                .warnings
                .push(DefWarning::UnusedAlphabetSymbol(symbol.clone()));
        }
    }

    // ── reachability from s₀ ─────────────────────────────────────────────────────
    // Only meaningful when s₀ is real; otherwise the initial-not-declared error stands
    // on its own and a reachability sweep would be noise.
    if def.has_state(&def.initial) {
        let reachable = reachable_from(def, &def.initial);
        for state in &def.states {
            if !reachable.contains(state.id.as_str()) {
                report
                    .errors
                    .push(DefError::UnreachableState(state.id.clone()));
            }
        }
    }

    // ── nondeterminism warnings (unguarded duplicates on one (state,event)) ──────
    let mut unguarded: BTreeMap<(&str, &str), usize> = BTreeMap::new();
    for t in &def.transitions {
        if t.guard.is_none() {
            *unguarded
                .entry((t.from.as_str(), t.event.as_str()))
                .or_default() += 1;
        }
    }
    for ((state, event), count) in unguarded {
        if count > 1 {
            report.warnings.push(DefWarning::NondeterministicPair {
                state: state.to_string(),
                event: event.to_string(),
                count,
            });
        }
    }

    if report.is_valid() {
        Ok(report)
    } else {
        Err(report)
    }
}

/// The set of state ids reachable from `start` (BFS). Two kinds of edge make a state
/// reachable: a TRANSITION targeting it, and CONTAINMENT — entering a composite state
/// enters its children, and a transition declared on a parent can target any of them, so
/// every child of a reachable composite is itself reachable. This is what lets a valid
/// hierarchical chart pass the no-dead-state check.
pub fn reachable_from<'a>(def: &'a StatechartDef, start: &str) -> BTreeSet<&'a str> {
    let mut reachable = BTreeSet::new();
    if let Some(state) = def.state(start) {
        let mut queue = VecDeque::new();
        reachable.insert(state.id.as_str());
        queue.push_back(state.id.as_str());
        while let Some(current) = queue.pop_front() {
            // transition targets
            for t in def.transitions.iter().filter(|t| t.from == current) {
                if let Some(target) = def.state(&t.to) {
                    if reachable.insert(target.id.as_str()) {
                        queue.push_back(target.id.as_str());
                    }
                }
            }
            // containment: children of a reachable composite are reachable
            if let Some(state) = def.state(current) {
                for child in &state.children {
                    if let Some(child_state) = def.state(child) {
                        if reachable.insert(child_state.id.as_str()) {
                            queue.push_back(child_state.id.as_str());
                        }
                    }
                }
            }
        }
    }
    reachable
}

// ── Exhaustive table-walk (requirement 5) ───────────────────────────────────────

/// How a single `(state, event)` cell of the S × Σ grid behaves.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Coverage {
    /// Exactly one transition is defined (its target + whether it is guarded).
    Defined { to: StateId, guarded: bool },
    /// Several transitions are defined for this cell (target list, in order). Legal and
    /// deterministic (first enabled fires); listed so a coverage test can see them all.
    DefinedMany { to: Vec<StateId> },
    /// No transition is defined ON this state, but an ANCESTOR (composite parent) has one
    /// for this event — so an atomic state in this cell INHERITS the parent's edge
    /// (hierarchy semantics). Records the ancestor it inherits from and that edge's
    /// target.
    Inherited { from: StateId, to: StateId },
    /// No transition is defined — a well-defined no-op (the machine stays put).
    NoOp,
}

/// The composite state that directly contains `id`, if any (its parent in the
/// containment tree).
fn container_of<'a>(def: &'a StatechartDef, id: &str) -> Option<&'a str> {
    def.states
        .iter()
        .find(|s| s.children.iter().any(|c| c == id))
        .map(|s| s.id.as_str())
}

/// Classify one `(state, event)` cell.
pub fn coverage(def: &StatechartDef, state: &str, event: &str) -> Coverage {
    let targets: Vec<&crate::model::Transition> = def.transitions_from(state, event).collect();
    match targets.as_slice() {
        [] => {
            // Hierarchy: an atomic state with no direct edge delegates to its ancestors.
            let mut cur = container_of(def, state);
            while let Some(ancestor) = cur {
                if let Some(t) = def.transitions_from(ancestor, event).next() {
                    return Coverage::Inherited {
                        from: ancestor.to_string(),
                        to: t.to.clone(),
                    };
                }
                cur = container_of(def, ancestor);
            }
            Coverage::NoOp
        }
        [only] => Coverage::Defined {
            to: only.to.clone(),
            guarded: only.guard.is_some(),
        },
        many => Coverage::DefinedMany {
            to: many.iter().map(|t| t.to.clone()).collect(),
        },
    }
}

/// Enumerate the ENTIRE S × Σ grid, classifying every cell (CONCEPT:INT-P2-2,
/// requirement 5). The returned rows are ordered by declared state then declared
/// alphabet symbol, so the walk is stable across runs.
pub fn coverage_matrix(def: &StatechartDef) -> Vec<(StateId, EventName, Coverage)> {
    let mut rows = Vec::with_capacity(def.states.len() * def.alphabet.len());
    for state in &def.states {
        for event in &def.alphabet {
            rows.push((
                state.id.clone(),
                event.clone(),
                coverage(def, &state.id, event),
            ));
        }
    }
    rows
}

/// Assert the S × Σ grid is TOTAL — every cell is either a defined transition or a
/// documented no-op. Always `Ok` for any chart by construction (there is no third
/// possibility), so this is a self-documenting totality proof rather than a check that
/// can fail; callers use it to assert the invariant explicitly in tests.
pub fn assert_total(def: &StatechartDef) -> Result<(), String> {
    for (state, event, cover) in coverage_matrix(def) {
        match cover {
            Coverage::Defined { .. }
            | Coverage::DefinedMany { .. }
            | Coverage::Inherited { .. }
            | Coverage::NoOp => {}
            #[allow(unreachable_patterns)]
            _ => return Err(format!("({state}, {event}) is neither defined nor a no-op")),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{State, Transition};

    fn base() -> StatechartDef {
        StatechartDef {
            name: "t".into(),
            schema_version: 0,
            states: vec![State::new("a"), State::new("b")],
            alphabet: vec!["go".into(), "back".into()],
            transitions: vec![
                Transition::new("a", "go", "b"),
                Transition::new("b", "back", "a"),
            ],
            initial: "a".into(),
            finals: vec![],
            meta: Default::default(),
        }
    }

    #[test]
    fn a_well_formed_chart_validates() {
        let report = validate(&base()).expect("valid");
        assert!(report.warnings.is_empty());
    }

    #[test]
    fn unreachable_state_is_rejected() {
        let mut def = base();
        def.states.push(State::new("island"));
        let err = validate(&def).unwrap_err();
        assert!(err
            .errors
            .contains(&DefError::UnreachableState("island".into())));
    }

    #[test]
    fn final_with_outgoing_edge_is_rejected() {
        let mut def = base();
        def.finals = vec!["b".into()];
        def.transitions.push(Transition::new("b", "go", "a"));
        // 'b' is final but has an outgoing 'go' edge.
        let err = validate(&def).unwrap_err();
        assert!(err
            .errors
            .iter()
            .any(|e| matches!(e, DefError::FinalHasOutgoing { state, .. } if state == "b")));
    }

    #[test]
    fn transition_to_undeclared_state_is_rejected() {
        let mut def = base();
        def.transitions.push(Transition::new("a", "back", "ghost"));
        let err = validate(&def).unwrap_err();
        assert!(err
            .errors
            .iter()
            .any(|e| matches!(e, DefError::TransitionToUndeclared { to, .. } if to == "ghost")));
    }

    /// A well-formed composite chart: `parent`(a|b, initial a) with a parent-level `fin`
    /// edge to a final `done`.
    fn composite() -> StatechartDef {
        let mut parent = State::new("parent");
        parent.children = vec!["a".into(), "b".into()];
        parent.initial_child = Some("a".into());
        StatechartDef {
            name: "composite".into(),
            schema_version: 0,
            states: vec![parent, State::new("a"), State::new("b"), State::new("done")],
            alphabet: vec!["go".into(), "fin".into()],
            transitions: vec![
                Transition::new("a", "go", "b"),
                Transition::new("parent", "fin", "done"),
            ],
            initial: "parent".into(),
            finals: vec!["done".into()],
            meta: Default::default(),
        }
    }

    #[test]
    fn well_formed_composite_chart_is_accepted() {
        // The hierarchy phase ACCEPTS composite/parallel/history charts.
        let report = validate(&composite()).expect("composite chart is valid");
        assert!(report.errors.is_empty());
    }

    #[test]
    fn malformed_composite_is_rejected() {
        // initial_child not among children.
        let mut def = composite();
        if let Some(parent) = def.states.iter_mut().find(|s| s.id == "parent") {
            parent.initial_child = Some("ghost".into());
        }
        let err = validate(&def).unwrap_err();
        assert!(err.errors.iter().any(|e| matches!(
            e,
            DefError::InitialChildNotInChildren { child, .. } if child == "ghost"
        )));

        // child referencing an undeclared state.
        let mut def2 = composite();
        if let Some(parent) = def2.states.iter_mut().find(|s| s.id == "parent") {
            parent.children.push("nope".into());
        }
        let err2 = validate(&def2).unwrap_err();
        assert!(err2.errors.iter().any(|e| matches!(
            e,
            DefError::ChildNotDeclared { child, .. } if child == "nope"
        )));

        // a state claimed as a child of two composites.
        let mut def3 = composite();
        let mut poacher = State::new("poacher");
        poacher.children = vec!["a".into()];
        def3.states.push(poacher);
        let err3 = validate(&def3).unwrap_err();
        assert!(err3.errors.contains(&DefError::MultipleParents("a".into())));
    }

    #[test]
    fn coverage_reflects_inherited_parent_transitions() {
        let def = composite();
        // `a` has a direct `go` edge...
        assert_eq!(
            coverage(&def, "a", "go"),
            Coverage::Defined {
                to: "b".into(),
                guarded: false
            }
        );
        // ...but no direct `fin` edge — it INHERITS the parent's `fin` edge to `done`.
        assert_eq!(
            coverage(&def, "a", "fin"),
            Coverage::Inherited {
                from: "parent".into(),
                to: "done".into()
            }
        );
        // and `b` inherits it too.
        assert_eq!(
            coverage(&def, "b", "fin"),
            Coverage::Inherited {
                from: "parent".into(),
                to: "done".into()
            }
        );
        assert!(assert_total(&def).is_ok());
    }

    #[test]
    fn coverage_matrix_enumerates_the_full_grid() {
        let def = base();
        let matrix = coverage_matrix(&def);
        // 2 states x 2 symbols = 4 cells, every one classified.
        assert_eq!(matrix.len(), 4);
        assert!(assert_total(&def).is_ok());
        // Exact shape: (a,go)=Defined->b, (a,back)=NoOp, (b,go)=NoOp, (b,back)=Defined->a.
        assert_eq!(
            coverage(&def, "a", "go"),
            Coverage::Defined {
                to: "b".into(),
                guarded: false
            }
        );
        assert_eq!(coverage(&def, "a", "back"), Coverage::NoOp);
        assert_eq!(coverage(&def, "b", "go"), Coverage::NoOp);
    }

    #[test]
    fn nondeterministic_unguarded_pair_warns_but_still_valid() {
        let mut def = base();
        // second unguarded edge on (a, go)
        def.transitions.push(Transition::new("a", "go", "a"));
        let report = validate(&def).expect("still structurally valid");
        assert!(report.warnings.iter().any(|w| matches!(
            w,
            DefWarning::NondeterministicPair { state, event, count }
                if state == "a" && event == "go" && *count == 2
        )));
    }
}
