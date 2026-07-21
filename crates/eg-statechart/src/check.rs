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
    CompositeUnsupported(String),
    /// The alphabet contains a duplicate symbol.
    DuplicateAlphabetSymbol(String),
}

impl std::fmt::Display for DefError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DefError::NoStates => write!(f, "chart has no states"),
            DefError::DuplicateState(s) => write!(f, "duplicate state id '{s}'"),
            DefError::InitialNotDeclared(s) => write!(f, "initial state '{s}' is not declared"),
            DefError::FinalNotDeclared(s) => write!(f, "final state '{s}' is not declared"),
            DefError::TransitionFromUndeclared { transition, from } => {
                write!(f, "transition {transition} starts from undeclared state '{from}'")
            }
            DefError::TransitionToUndeclared { transition, to } => {
                write!(f, "transition {transition} targets undeclared state '{to}'")
            }
            DefError::EventNotInAlphabet { transition, event } => {
                write!(f, "transition {transition} uses event '{event}' not in the alphabet")
            }
            DefError::UnreachableState(s) => write!(f, "state '{s}' is unreachable from the initial state"),
            DefError::FinalHasOutgoing { state, transition } => {
                write!(f, "final state '{state}' has an outgoing transition {transition}")
            }
            DefError::CompositeUnsupported(s) => {
                write!(f, "state '{s}' is composite/parallel/history (unsupported in phase-1)")
            }
            DefError::DuplicateAlphabetSymbol(s) => write!(f, "duplicate alphabet symbol '{s}'"),
        }
    }
}

/// A soft observation that does not block storage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DefWarning {
    /// Two or more unguarded transitions share a `(state, event)`; only the first can
    /// ever fire (the rest are dead). Deterministic, but almost certainly a mistake.
    NondeterministicPair { state: StateId, event: EventName, count: usize },
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
            report.errors.push(DefError::DuplicateState(state.id.clone()));
        }
        if state.is_composite() {
            report.errors.push(DefError::CompositeUnsupported(state.id.clone()));
        }
    }

    // ── Σ well-formed ────────────────────────────────────────────────────────────
    let mut seen_symbols = BTreeSet::new();
    for symbol in &def.alphabet {
        if !seen_symbols.insert(symbol.as_str()) {
            report.errors.push(DefError::DuplicateAlphabetSymbol(symbol.clone()));
        }
    }

    // ── s₀ ∈ S, F ⊆ S ────────────────────────────────────────────────────────────
    if !def.has_state(&def.initial) {
        report.errors.push(DefError::InitialNotDeclared(def.initial.clone()));
    }
    for final_id in &def.finals {
        if !def.has_state(final_id) {
            report.errors.push(DefError::FinalNotDeclared(final_id.clone()));
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
            report.warnings.push(DefWarning::UnusedAlphabetSymbol(symbol.clone()));
        }
    }

    // ── reachability from s₀ ─────────────────────────────────────────────────────
    // Only meaningful when s₀ is real; otherwise the initial-not-declared error stands
    // on its own and a reachability sweep would be noise.
    if def.has_state(&def.initial) {
        let reachable = reachable_from(def, &def.initial);
        for state in &def.states {
            if !reachable.contains(state.id.as_str()) {
                report.errors.push(DefError::UnreachableState(state.id.clone()));
            }
        }
    }

    // ── nondeterminism warnings (unguarded duplicates on one (state,event)) ──────
    let mut unguarded: BTreeMap<(&str, &str), usize> = BTreeMap::new();
    for t in &def.transitions {
        if t.guard.is_none() {
            *unguarded.entry((t.from.as_str(), t.event.as_str())).or_default() += 1;
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

/// The set of state ids reachable from `start` by following transitions (BFS).
pub fn reachable_from<'a>(def: &'a StatechartDef, start: &str) -> BTreeSet<&'a str> {
    let mut reachable = BTreeSet::new();
    if let Some(state) = def.state(start) {
        let mut queue = VecDeque::new();
        reachable.insert(state.id.as_str());
        queue.push_back(state.id.as_str());
        while let Some(current) = queue.pop_front() {
            for t in def.transitions.iter().filter(|t| t.from == current) {
                if let Some(target) = def.state(&t.to) {
                    if reachable.insert(target.id.as_str()) {
                        queue.push_back(target.id.as_str());
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
    /// No transition is defined — a well-defined no-op (the machine stays put).
    NoOp,
}

/// Classify one `(state, event)` cell.
pub fn coverage(def: &StatechartDef, state: &str, event: &str) -> Coverage {
    let targets: Vec<&crate::model::Transition> = def.transitions_from(state, event).collect();
    match targets.as_slice() {
        [] => Coverage::NoOp,
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
            Coverage::Defined { .. } | Coverage::DefinedMany { .. } | Coverage::NoOp => {}
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
        assert!(err.errors.contains(&DefError::UnreachableState("island".into())));
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

    #[test]
    fn composite_state_is_rejected_in_phase_1() {
        let mut def = base();
        let mut composite = State::new("parent");
        composite.children = vec!["a".into()];
        def.states.push(composite);
        let err = validate(&def).unwrap_err();
        assert!(err.errors.contains(&DefError::CompositeUnsupported("parent".into())));
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
            Coverage::Defined { to: "b".into(), guarded: false }
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
