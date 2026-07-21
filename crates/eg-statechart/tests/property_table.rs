//! Property + table tests for the statechart engine (CONCEPT:INT-P2-2, requirement 5).
//!
//! These go beyond example-based unit tests: they enumerate the ENTIRE S × Σ grid and
//! fuzz long random event sequences (with a deterministic in-test LCG — no external
//! `proptest` dependency, keeping the crate's dep set lean) to assert the invariants
//! that must hold for EVERY chart and EVERY sequence:
//!
//! * the machine never leaves S (its state is always a declared state);
//! * `transition` is total — it never panics, and every S × Σ cell is Defined-or-NoOp;
//! * a no-op truly leaves `(state, context)` unchanged;
//! * δ is deterministic — the same seed replays to the same final `(state, context)`;
//! * finals are terminal — once in F, every event is a no-op.

use std::collections::BTreeSet;

use eg_statechart::action::{Action, ActionValue};
use eg_statechart::check::{assert_total, coverage, coverage_matrix, Coverage};
use eg_statechart::guard::Guard;
use eg_statechart::model::{State, StatechartDef, Transition};
use eg_statechart::{transition, Context, EventInput};

/// A small deterministic linear-congruential generator (glibc constants) so the
/// "property" walks are reproducible and dependency-free.
struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0 >> 33
    }
    fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[(self.next() as usize) % items.len()]
    }
}

/// A richer chart than the turnstile: a guarded counter that accumulates in context
/// and latches into a final `done` state once `n >= 3`.
fn counter_chart() -> StatechartDef {
    StatechartDef {
        name: "counter".into(),
        schema_version: 1,
        states: vec![
            State::new("idle"),
            State::new("counting"),
            State::new("done"),
        ],
        alphabet: vec!["start".into(), "inc".into(), "finish".into(), "reset".into()],
        transitions: vec![
            Transition::new("idle", "start", "counting").with_actions(vec![Action::Assign {
                key: "n".into(),
                value: ActionValue::Const { value: serde_json::json!(0) },
            }]),
            // inc while below threshold: self-transition bumping n (guard reads context)
            Transition::new("counting", "inc", "counting")
                .with_guard(Guard::Lt { key: "n".into(), value: 3.0 })
                .with_actions(vec![Action::Assign {
                    key: "n".into(),
                    value: ActionValue::Event { key: "n_next".into() },
                }]),
            // finish only when n has reached the threshold
            Transition::new("counting", "finish", "done")
                .with_guard(Guard::Ge { key: "n".into(), value: 3.0 }),
            Transition::new("counting", "reset", "idle"),
        ],
        initial: "idle".into(),
        finals: vec!["done".into()],
        meta: Default::default(),
    }
}

fn state_set(def: &StatechartDef) -> BTreeSet<String> {
    def.states.iter().map(|s| s.id.clone()).collect()
}

/// Drive a random sequence, returning the visited `(state, context)` trace.
fn walk(def: &StatechartDef, seed: u64, steps: usize) -> Vec<(String, Context)> {
    let states = state_set(def);
    let mut lcg = Lcg(seed);
    let mut state = def.initial.clone();
    let mut context = Context::new();
    let mut trace = vec![(state.clone(), context.clone())];

    for _ in 0..steps {
        let event_name = lcg.pick(&def.alphabet).clone();
        // supply an incrementing candidate so the counter chart can make progress
        let n_now = context.get("n").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let event = EventInput::with_payload(
            event_name,
            serde_json::json!({"n_next": n_now + 1.0}),
        );

        let before = (state.clone(), context.clone());
        let outcome = transition(def, &state, &context, &event).expect("event is in Σ");

        // INVARIANT: next_state is always a declared state.
        assert!(states.contains(&outcome.next_state), "escaped S: {}", outcome.next_state);

        if !outcome.fired {
            // INVARIANT: a no-op leaves (state, context) exactly as they were.
            assert_eq!(outcome.next_state, before.0);
            assert_eq!(outcome.next_context, before.1);
        }

        state = outcome.next_state;
        context = outcome.next_context;
        trace.push((state.clone(), context.clone()));
    }
    trace
}

#[test]
fn transition_is_total_over_the_whole_grid() {
    let def = counter_chart();
    assert_total(&def).expect("every (state,event) is Defined or NoOp");
    // 3 states x 4 symbols = 12 cells, all classified.
    assert_eq!(coverage_matrix(&def).len(), 12);
}

#[test]
fn coverage_matrix_pins_the_exact_shape() {
    let def = counter_chart();
    // A representative slice of the grid — an accidentally-deleted edge changes a cell.
    assert_eq!(
        coverage(&def, "idle", "start"),
        Coverage::Defined { to: "counting".into(), guarded: false }
    );
    assert_eq!(coverage(&def, "idle", "inc"), Coverage::NoOp);
    assert_eq!(
        coverage(&def, "counting", "inc"),
        Coverage::Defined { to: "counting".into(), guarded: true }
    );
    // 'done' is final: nothing is defined out of it — every symbol is a no-op.
    for symbol in &def.alphabet {
        assert_eq!(coverage(&def, "done", symbol), Coverage::NoOp);
    }
}

#[test]
fn random_walks_never_escape_s_and_never_panic() {
    let def = counter_chart();
    for seed in 0..200u64 {
        // The per-step invariants are asserted inside `walk`.
        let trace = walk(&def, seed.wrapping_mul(2654435761).wrapping_add(1), 40);
        assert_eq!(trace.len(), 41);
    }
}

#[test]
fn transition_function_is_deterministic() {
    let def = counter_chart();
    for seed in 0..50u64 {
        let a = walk(&def, seed + 7, 30);
        let b = walk(&def, seed + 7, 30);
        assert_eq!(a, b, "same seed must replay identically");
    }
}

#[test]
fn finals_are_terminal_once_reached() {
    let def = counter_chart();
    // Drive deterministically into 'done': start, inc x3, finish.
    let mut state = def.initial.clone();
    let mut ctx = Context::new();
    let script = [
        ("start", 0.0),
        ("inc", 1.0),
        ("inc", 2.0),
        ("inc", 3.0),
        ("finish", 0.0),
    ];
    for (ev, n_next) in script {
        let event = EventInput::with_payload(ev, serde_json::json!({"n_next": n_next}));
        let out = transition(&def, &state, &ctx, &event).unwrap();
        state = out.next_state;
        ctx = out.next_context;
    }
    assert_eq!(state, "done");
    assert!(def.is_final(&state));
    // Every subsequent event is a no-op — a final state has no outgoing edges.
    for symbol in &def.alphabet {
        let out = transition(&def, &state, &ctx, &EventInput::new(symbol)).unwrap();
        assert!(!out.fired, "event {symbol} fired out of a final state");
        assert_eq!(out.next_state, "done");
    }
}
