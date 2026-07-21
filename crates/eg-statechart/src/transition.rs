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

use crate::action::{apply_all, Action};
use crate::context::{Context, EventInput};
use crate::model::{StateId, StatechartDef};

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
}
