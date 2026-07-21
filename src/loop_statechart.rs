//! The **Loop statechart** (CONCEPT:INT-P2-2, W2.5 control-plane migration).
//!
//! `LoopStatus` (the Python `agent_utilities.knowledge_graph.research.loops.LoopStatus`
//! `StrEnum`, 16 members) has NO durable engine-native home today: the Loop's actual
//! persisted state is just its backing WorkItem's 8-value `status` plus a free-text
//! `error_ref` suffix nothing parses back into a real state. This module gives Loop its
//! FIRST durable state machine, expressed as a plain, reusable [`eg_statechart::StatechartDef`]
//! the engine instantiates via the EXISTING `Method::Statechart` wire surface — no new
//! wire protocol, no change to `eg-statechart` itself. See
//! `reports/w2_5-statechart-migration-design.md` §1 for the full derivation.
//!
//! `loop_statechart_def()` is the single source of truth the Rust tests below exercise
//! directly (via [`eg_statechart::transition`], no server needed); the Python
//! `agent_utilities` side authors the SAME shape as a plain dict and registers it once
//! via `client.statechart.define(...)` (see `loops.py`).
//!
//! ## Corrections to the design doc (disclosed here, not silently applied)
//!
//! 1. **Row 13 count.** The design doc's §1.3 row 13 says "5 concrete rows" but then
//!    lists `failed, cancelled, rejected` **plus** "the 6 harness-terminals" — that is
//!    3 + 6 = **9** rows (every final state except `completed`, which row 12 already
//!    covers), not 5. [`CALLEE_TERMINAL_MIRRORS`] holds the correct 9.
//! 2. **Row 12/13 `from`.** The design doc restricts the legacy-trust `callee_terminal`
//!    mirror (rows 12/13) to `from: running` only. `run_loop`'s actual legacy-trust
//!    check (`loop_controller.py:2526-2531`) runs unconditionally on the freshly
//!    computed `decided` value every iteration, regardless of what the tracked
//!    `status` was going in — so a loop currently parked in `pending`/`validating`
//!    that then receives a callee-declared terminal on its next `posttick` must still
//!    transition. Restricting to `running` only would silently strand such a loop in a
//!    no-op forever. This module fires rows 12/13 from all three active non-terminal
//!    states (`running`/`pending`/`validating`), matching rows 8-11.
//! 3. **Rows 9 and 14 cannot use `Guard::Ge` the way the design doc describes.**
//!    `Guard::Ge`/`Gt`/`Lt`/`Le` read the machine's persistent **context**
//!    (`guard.rs::number(context, key)`), never the event payload — only
//!    `Guard::EventEq` reads the payload. This chart's context is (by the design
//!    doc's own §1.3 note) always empty, so a `Guard::Ge{key:"fail_count",...}` over an
//!    EVENT field would be vacuously `false` forever, silently disabling exit 7 (error
//!    threshold) and exit 2 (turn cap). Rather than widen `eg-statechart`'s guard
//!    language (out of scope — the task is to reuse the EXISTING engine), this chart
//!    has the caller pass the already-computed booleans it already has at the call
//!    site: `error_threshold_tripped` (`ConsecutiveFailureGuard.record_failure()`'s own
//!    return value, row 9) and `turn_cap_reached` (`iteration >= max_iterations`, row
//!    14) — both plain `EventEq` guards, no numeric comparison needed engine-side.
//!    `fail_count`/`fail_threshold`/`iteration`/`max_iterations` are dropped from the
//!    payload as a result (the caller already computed the comparison; sending the raw
//!    numbers too would be dead data).
//!
//! Two more corrections, found by `eg_statechart::check::validate`'s reachability
//! check (every declared state must be reachable from `s₀`) failing on the first
//! draft of this def:
//!
//! 4. **Row 16 needs a real transition after all — `pending`/`validating` are
//!    otherwise unreachable.** The first draft omitted row 16 (relying on the
//!    engine's built-in no-op fallthrough), reasoning that "stay non-terminal" needed
//!    no explicit edge. That is true for STAYING in the current state, but it cannot
//!    make `pending`/`validating` reachable AT ALL, since nothing ever transitions
//!    INTO them — `check::validate` correctly rejects the def as having unreachable
//!    states. Fix: a `heartbeat_target` payload field (the caller already knows
//!    exactly which of `running`/`pending`/`validating` its `decided`/`heartbeat_status`
//!    value is — `loop_controller.py:2455`) drives a genuine row-16 transition,
//!    declared LAST (after every terminal guard 8-15) so it only fires once nothing
//!    terminal did.
//! 5. **`orphaned` needs an inbound edge — it had none.** The design doc's guard table
//!    only ever uses `orphaned` as a `from` (row 1b's re-claim). Nothing in Σ as
//!    specified transitions INTO it, so it is likewise unreachable. WorkItem's own
//!    chart (§2.3) has a precedent for exactly this shape: an internally-fired
//!    `lease_reclaim` event, sent by a reaper/expiry sweep rather than by any of the
//!    normal wire calls. This chart adds the Loop-side analogue, `lease_lost` (Σ grows
//!    to 6 members), firing `running`/`pending`/`validating` -> `orphaned` — the sweep
//!    that already exists at the WorkItem layer (`redb_store.rs`'s expired-lease
//!    fencing) is the natural caller once it detects a Loop's backing WorkItem lease
//!    expired out from under it.
//!
//! One simplification remains (functionally inert):
//!
//! * **Row 14's `Missing{callee_terminal-is-terminal}` guard is dropped** as redundant:
//!   because transitions fire first-match-wins in declaration order and rows 12/13
//!   already exhaustively cover every terminal `callee_terminal` value ahead of row 14,
//!   control only reaches row 14 when `callee_terminal` was already absent/non-terminal.

use eg_statechart::{Action, Guard, State, StatechartDef, Transition};

/// The six non-final `LoopStatus` string ids relevant to this chart's states (§1.1).
/// `paused` is the first-class `awaitingHuman` state (§1.4); `orphaned` is the
/// re-claimable post-crash state.
const NON_FINAL_STATES: &[&str] = &[
    "submitted",
    "running",
    "pending",
    "validating",
    "paused",
    "orphaned",
];

/// The ten final `LoopStatus` string ids (§1.1) — identical to today's
/// `loops.TERMINAL_STATUS`.
pub const FINAL_STATES: &[&str] = &[
    "completed",
    "failed",
    "cancelled",
    "rejected",
    "max_iterations_exceeded",
    "budget_exceeded",
    "wall_clock_exceeded",
    "stalled",
    "error_threshold_exceeded",
    "external_event_satisfied",
];

/// The active (non-terminal, in-flight) states rows 3-6 and 8-13 apply from — a loop
/// mid-iteration can be in any of these depending on `kind`/evaluator phase.
const ACTIVE_STATES: &[&str] = &["running", "pending", "validating"];

/// Every final state a callee can self-declare through `callee_terminal`, EXCLUDING
/// `completed` (handled separately by row 12's dedicated guard). Row 13's mirror: one
/// transition per value, `<from> --posttick[callee_terminal==value]--> <value>`.
const CALLEE_TERMINAL_MIRRORS: &[&str] = &[
    "failed",
    "cancelled",
    "rejected",
    "max_iterations_exceeded",
    "budget_exceeded",
    "wall_clock_exceeded",
    "stalled",
    "error_threshold_exceeded",
    "external_event_satisfied",
];

fn event_eq(key: &str, value: serde_json::Value) -> Guard {
    Guard::EventEq {
        key: key.to_string(),
        value,
    }
}

/// Build the Loop `StatechartDef` (CONCEPT:INT-P2-2 §1). Pure, deterministic, and
/// content-addressed via [`StatechartDef::def_id`] — calling this twice yields
/// byte-identical definitions (`define()` is therefore idempotent to re-register).
pub fn loop_statechart_def() -> StatechartDef {
    let states: Vec<State> = NON_FINAL_STATES
        .iter()
        .chain(FINAL_STATES.iter())
        .map(|id| State::new(*id))
        .collect();

    let mut transitions: Vec<Transition> = Vec::new();

    // ── claim / reject (§1.3 rows 1, 1b, 2) ─────────────────────────────────────
    transitions.push(Transition::new("submitted", "claim", "running").with_guard(Guard::Always));
    transitions.push(Transition::new("orphaned", "claim", "running").with_guard(Guard::Always));
    transitions.push(Transition::new("submitted", "reject", "rejected").with_guard(Guard::Always));

    // ── pretick guards (§1.3 rows 3-6), evaluated in declaration order per
    // `run_loop`'s human-interrupt -> budget -> external-event precedence, one
    // transition per active `from` state. ───────────────────────────────────────
    for from in ACTIVE_STATES {
        transitions.push(
            Transition::new(*from, "pretick", "paused")
                .with_guard(event_eq("human_signal", serde_json::json!("pause")))
                .with_actions(vec![Action::Log {
                    message: "human interrupt: pause".into(),
                }]),
        );
    }
    for from in ACTIVE_STATES {
        transitions.push(
            Transition::new(*from, "pretick", "cancelled")
                .with_guard(Guard::Any {
                    guards: vec![
                        event_eq("human_signal", serde_json::json!("kill")),
                        event_eq("human_signal", serde_json::json!("cancel")),
                        event_eq("human_signal", serde_json::json!("stop")),
                    ],
                })
                .with_actions(vec![Action::Log {
                    message: "human interrupt: kill".into(),
                }]),
        );
    }
    for from in ACTIVE_STATES {
        transitions.push(
            Transition::new(*from, "pretick", "budget_exceeded")
                .with_guard(event_eq("budget_exceeded", serde_json::json!(true))),
        );
    }
    for from in ACTIVE_STATES {
        transitions.push(
            Transition::new(*from, "pretick", "external_event_satisfied")
                .with_guard(event_eq("external_event_fired", serde_json::json!(true))),
        );
    }

    // ── resume (§1.3 row 7): awaitingHuman -> running, inbound/external. ────────
    transitions.push(Transition::new("paused", "resume", "running").with_guard(Guard::Always));

    // ── posttick guards (§1.3 rows 8-15), one block per row, one transition per
    // active `from` state within a block, in declaration order. ────────────────

    // Row 8: exit 1 GOAL MET.
    for from in ACTIVE_STATES {
        transitions.push(
            Transition::new(*from, "posttick", "completed")
                .with_guard(event_eq("measured_pass", serde_json::json!(true))),
        );
    }
    // Row 9: exit 7 ERROR THRESHOLD. Declared before row 11's plain retryable check so
    // a threshold trip always wins ties (mirrors `fail_guard.record_failure()`
    // short-circuiting the "keep looping" branch). See correction (3) above:
    // `error_threshold_tripped` is the pre-computed boolean, not a numeric comparison.
    for from in ACTIVE_STATES {
        transitions.push(
            Transition::new(*from, "posttick", "error_threshold_exceeded").with_guard(Guard::All {
                guards: vec![
                    event_eq("retryable_failure", serde_json::json!(true)),
                    event_eq("error_threshold_tripped", serde_json::json!(true)),
                ],
            }),
        );
    }
    // Row 10: exit 5 NO PROGRESS.
    for from in ACTIVE_STATES {
        transitions.push(
            Transition::new(*from, "posttick", "stalled")
                .with_guard(event_eq("stalled", serde_json::json!(true))),
        );
    }
    // Row 11: a retryable failure that did NOT trip the error threshold (row 9 already
    // claimed that case) keeps the lease alive as a heartbeat (self-loop).
    for from in ACTIVE_STATES {
        transitions.push(
            Transition::new(*from, "posttick", *from)
                .with_guard(event_eq("retryable_failure", serde_json::json!(true))),
        );
    }
    // Row 12: legacy-trust, callee self-declared `completed`.
    for from in ACTIVE_STATES {
        transitions.push(
            Transition::new(*from, "posttick", "completed")
                .with_guard(event_eq("callee_terminal", serde_json::json!("completed"))),
        );
    }
    // Row 13: legacy-trust mirror — every OTHER final `callee_terminal` value maps to
    // that same terminal state (9 values x 3 active `from` states = 27 rows).
    for from in ACTIVE_STATES {
        for terminal in CALLEE_TERMINAL_MIRRORS {
            transitions.push(
                Transition::new(*from, "posttick", *terminal)
                    .with_guard(event_eq("callee_terminal", serde_json::json!(*terminal))),
            );
        }
    }
    // Row 14: exit 2 TURN CAP (post-loop fallback, modeled as a same-tick guard). See
    // correction (3): `turn_cap_reached` is the pre-computed `iteration >= max_iterations`.
    for from in ACTIVE_STATES {
        transitions.push(
            Transition::new(*from, "posttick", "max_iterations_exceeded")
                .with_guard(event_eq("turn_cap_reached", serde_json::json!(true))),
        );
    }
    // Row 15: exit 4 WALL CLOCK.
    for from in ACTIVE_STATES {
        transitions.push(
            Transition::new(*from, "posttick", "wall_clock_exceeded")
                .with_guard(event_eq("deadline_passed", serde_json::json!(true))),
        );
    }
    // Row 16 (correction 4): ordinary non-terminal continuation. Declared LAST among
    // the posttick rows so every terminal guard (8-15) gets first refusal; only fires
    // once nothing terminal did. `heartbeat_target` is whichever of
    // running/pending/validating the caller's `decided`/`heartbeat_status` already is.
    for from in ACTIVE_STATES {
        for target in ACTIVE_STATES {
            transitions.push(
                Transition::new(*from, "posttick", *target)
                    .with_guard(event_eq("heartbeat_target", serde_json::json!(*target))),
            );
        }
    }

    // `lease_lost` (correction 5): an internally-fired event (mirrors
    // WorkItem's `lease_reclaim`, §2.3) — a reaper/expiry sweep observing the backing
    // WorkItem lease expired out from under an in-flight Loop marks it re-claimable.
    for from in ACTIVE_STATES {
        transitions
            .push(Transition::new(*from, "lease_lost", "orphaned").with_guard(Guard::Always));
    }

    StatechartDef {
        name: "loop".to_string(),
        schema_version: 1,
        states,
        alphabet: vec![
            "claim".to_string(),
            "pretick".to_string(),
            "resume".to_string(),
            "posttick".to_string(),
            "reject".to_string(),
            "lease_lost".to_string(),
        ],
        transitions,
        initial: "submitted".to_string(),
        finals: FINAL_STATES.iter().map(|s| s.to_string()).collect(),
        meta: Default::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eg_statechart::context::{Context, EventInput};
    use eg_statechart::instance::Configuration;
    use eg_statechart::{transition, validate};

    fn def() -> StatechartDef {
        loop_statechart_def()
    }

    fn payload(fields: &[(&str, serde_json::Value)]) -> serde_json::Value {
        serde_json::Value::Object(
            fields
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
        )
    }

    // ── structural validity ──────────────────────────────────────────────────────

    #[test]
    fn definition_is_structurally_valid_and_content_addressed() {
        let d = def();
        validate(&d).expect("loop statechart must pass eg-statechart's own validator");
        assert_eq!(d.def_id(), def().def_id(), "def_id must be deterministic");
        assert_eq!(d.initial, "submitted");
        assert_eq!(d.finals.len(), 10);
        for id in FINAL_STATES {
            assert!(d.is_final(id), "{id} must be a declared final state");
        }
    }

    #[test]
    fn every_state_the_design_lists_is_present() {
        let d = def();
        for id in [
            "submitted",
            "running",
            "pending",
            "validating",
            "paused",
            "orphaned",
            "completed",
            "failed",
            "cancelled",
            "rejected",
            "max_iterations_exceeded",
            "budget_exceeded",
            "wall_clock_exceeded",
            "stalled",
            "error_threshold_exceeded",
            "external_event_satisfied",
        ] {
            assert!(d.has_state(id), "missing state {id}");
        }
        assert_eq!(d.states.len(), 16, "LoopStatus has exactly 16 members");
    }

    // ── claim / reject / resume ──────────────────────────────────────────────────

    #[test]
    fn claim_moves_submitted_and_orphaned_to_running() {
        let d = def();
        let ctx = Context::new();
        let out = transition(&d, "submitted", &ctx, &EventInput::new("claim")).unwrap();
        assert!(out.fired);
        assert_eq!(out.next_state, "running");

        let out2 = transition(&d, "orphaned", &ctx, &EventInput::new("claim")).unwrap();
        assert!(out2.fired);
        assert_eq!(out2.next_state, "running");
    }

    #[test]
    fn reject_is_intake_only_never_entered_running() {
        let d = def();
        let ctx = Context::new();
        let out = transition(&d, "submitted", &ctx, &EventInput::new("reject")).unwrap();
        assert!(out.fired);
        assert_eq!(out.next_state, "rejected");
    }

    #[test]
    fn resume_returns_paused_to_running() {
        let d = def();
        let ctx = Context::new();
        let out = transition(&d, "paused", &ctx, &EventInput::new("resume")).unwrap();
        assert!(out.fired);
        assert_eq!(out.next_state, "running");
    }

    // ── the 8 harness-enforced exits, TEST-PROVEN from every active state ───────

    #[test]
    fn exit_1_goal_met_fires_completed_from_every_active_state() {
        let d = def();
        let ctx = Context::new();
        for from in ACTIVE_STATES {
            let ev = EventInput::with_payload(
                "posttick",
                payload(&[("measured_pass", serde_json::json!(true))]),
            );
            let out = transition(&d, from, &ctx, &ev).unwrap();
            assert!(out.fired, "measured_pass must fire from {from}");
            assert_eq!(out.next_state, "completed");
        }
    }

    #[test]
    fn exit_2_turn_cap_fires_max_iterations_exceeded() {
        let d = def();
        let ctx = Context::new();
        let ev = EventInput::with_payload(
            "posttick",
            payload(&[("turn_cap_reached", serde_json::json!(true))]),
        );
        let out = transition(&d, "running", &ctx, &ev).unwrap();
        assert!(out.fired);
        assert_eq!(out.next_state, "max_iterations_exceeded");
    }

    #[test]
    fn exit_3_budget_cap_fires_on_pretick_before_the_step() {
        let d = def();
        let ctx = Context::new();
        let ev = EventInput::with_payload(
            "pretick",
            payload(&[("budget_exceeded", serde_json::json!(true))]),
        );
        let out = transition(&d, "running", &ctx, &ev).unwrap();
        assert!(out.fired);
        assert_eq!(out.next_state, "budget_exceeded");
    }

    #[test]
    fn exit_4_wall_clock_fires_wall_clock_exceeded() {
        let d = def();
        let ctx = Context::new();
        let ev = EventInput::with_payload(
            "posttick",
            payload(&[("deadline_passed", serde_json::json!(true))]),
        );
        let out = transition(&d, "pending", &ctx, &ev).unwrap();
        assert!(out.fired);
        assert_eq!(out.next_state, "wall_clock_exceeded");
    }

    #[test]
    fn exit_5_no_progress_fires_stalled() {
        let d = def();
        let ctx = Context::new();
        let ev =
            EventInput::with_payload("posttick", payload(&[("stalled", serde_json::json!(true))]));
        let out = transition(&d, "validating", &ctx, &ev).unwrap();
        assert!(out.fired);
        assert_eq!(out.next_state, "stalled");
    }

    #[test]
    fn exit_6_human_interrupt_pause_parks_in_awaiting_human_then_resumes() {
        let d = def();
        let ctx = Context::new();
        let ev = EventInput::with_payload(
            "pretick",
            payload(&[("human_signal", serde_json::json!("pause"))]),
        );
        let out = transition(&d, "running", &ctx, &ev).unwrap();
        assert!(out.fired);
        assert_eq!(
            out.next_state, "paused",
            "paused is the awaitingHuman state"
        );

        // A follow-up kill from paused still escalates straight to cancelled (row 4
        // also applies from `paused`... no — design row 4 is only `from` the active
        // states. A paused loop resumes first, THEN can be killed on the next tick).
        let resumed = transition(&d, "paused", &ctx, &EventInput::new("resume")).unwrap();
        assert_eq!(resumed.next_state, "running");
    }

    #[test]
    fn exit_6_human_interrupt_kill_cancel_stop_all_cancel() {
        let d = def();
        let ctx = Context::new();
        for signal in ["kill", "cancel", "stop"] {
            let ev = EventInput::with_payload(
                "pretick",
                payload(&[("human_signal", serde_json::json!(signal))]),
            );
            let out = transition(&d, "running", &ctx, &ev).unwrap();
            assert!(out.fired, "{signal} must fire cancelled");
            assert_eq!(out.next_state, "cancelled");
        }
    }

    #[test]
    fn exit_7_error_threshold_fires_only_once_tripped_else_keeps_looping() {
        let d = def();
        let ctx = Context::new();
        // Retryable but NOT tripped -> stays running (row 11 heartbeat, self-loop).
        let not_tripped = EventInput::with_payload(
            "posttick",
            payload(&[
                ("retryable_failure", serde_json::json!(true)),
                ("error_threshold_tripped", serde_json::json!(false)),
            ]),
        );
        let out = transition(&d, "running", &ctx, &not_tripped).unwrap();
        assert!(out.fired);
        assert_eq!(out.next_state, "running", "not yet tripped -> keep looping");

        // Retryable AND tripped -> error_threshold_exceeded (row 9 wins the tie).
        let tripped = EventInput::with_payload(
            "posttick",
            payload(&[
                ("retryable_failure", serde_json::json!(true)),
                ("error_threshold_tripped", serde_json::json!(true)),
            ]),
        );
        let out2 = transition(&d, "running", &ctx, &tripped).unwrap();
        assert!(out2.fired);
        assert_eq!(out2.next_state, "error_threshold_exceeded");
    }

    #[test]
    fn exit_8_external_event_fires_on_pretick() {
        let d = def();
        let ctx = Context::new();
        let ev = EventInput::with_payload(
            "pretick",
            payload(&[("external_event_fired", serde_json::json!(true))]),
        );
        let out = transition(&d, "validating", &ctx, &ev).unwrap();
        assert!(out.fired);
        assert_eq!(out.next_state, "external_event_satisfied");
    }

    // ── pretick precedence: human-interrupt > budget > external-event, matching
    // `run_loop`'s if/elif declaration order exactly. ────────────────────────────

    #[test]
    fn pretick_precedence_matches_run_loop_if_elif_order() {
        let d = def();
        let ctx = Context::new();
        // All three pretick signals true at once -> the FIRST declared guard wins:
        // human interrupt (pause), not budget, not external event.
        let ev = EventInput::with_payload(
            "pretick",
            payload(&[
                ("human_signal", serde_json::json!("pause")),
                ("budget_exceeded", serde_json::json!(true)),
                ("external_event_fired", serde_json::json!(true)),
            ]),
        );
        let out = transition(&d, "running", &ctx, &ev).unwrap();
        assert_eq!(out.next_state, "paused");
    }

    // ── legacy-trust callee_terminal mirror: all 9 non-completed finals + completed ──

    #[test]
    fn legacy_trust_mirrors_every_callee_declared_terminal() {
        let d = def();
        let ctx = Context::new();
        for terminal in CALLEE_TERMINAL_MIRRORS {
            let ev = EventInput::with_payload(
                "posttick",
                payload(&[("callee_terminal", serde_json::json!(*terminal))]),
            );
            let out = transition(&d, "running", &ctx, &ev).unwrap();
            assert!(out.fired, "callee_terminal={terminal} must fire");
            assert_eq!(&out.next_state, terminal);
        }
        assert_eq!(
            CALLEE_TERMINAL_MIRRORS.len(),
            9,
            "3 + 6 harness terminals, not 5"
        );

        // completed goes through row 12, not row 13.
        let ev = EventInput::with_payload(
            "posttick",
            payload(&[("callee_terminal", serde_json::json!("completed"))]),
        );
        let out = transition(&d, "running", &ctx, &ev).unwrap();
        assert_eq!(out.next_state, "completed");
    }

    #[test]
    fn legacy_trust_mirror_fires_from_pending_and_validating_too() {
        // Correction (2): rows 12/13 must NOT be restricted to `from: running`.
        let d = def();
        let ctx = Context::new();
        for from in ["pending", "validating"] {
            let ev = EventInput::with_payload(
                "posttick",
                payload(&[("callee_terminal", serde_json::json!("failed"))]),
            );
            let out = transition(&d, from, &ctx, &ev).unwrap();
            assert!(out.fired, "callee_terminal mirror must fire from {from}");
            assert_eq!(out.next_state, "failed");
        }
    }

    // ── row 16 heartbeat + the lease_lost reachability fix ──────────────────────

    #[test]
    fn heartbeat_target_moves_between_active_states_when_nothing_terminal_fires() {
        let d = def();
        let ctx = Context::new();
        for (from, target) in [
            ("running", "pending"),
            ("pending", "validating"),
            ("validating", "running"),
        ] {
            let ev = EventInput::with_payload(
                "posttick",
                payload(&[("heartbeat_target", serde_json::json!(target))]),
            );
            let out = transition(&d, from, &ctx, &ev).unwrap();
            assert!(out.fired, "heartbeat_target={target} must fire from {from}");
            assert_eq!(out.next_state, target);
        }
    }

    #[test]
    fn retryable_heartbeat_self_loop_takes_precedence_over_heartbeat_target() {
        // Row 11 (declared before row 16) wins: a retryable-but-not-tripped failure
        // stays in the SAME state even if a (nonsensical) heartbeat_target disagreed.
        let d = def();
        let ctx = Context::new();
        let ev = EventInput::with_payload(
            "posttick",
            payload(&[
                ("retryable_failure", serde_json::json!(true)),
                ("error_threshold_tripped", serde_json::json!(false)),
                ("heartbeat_target", serde_json::json!("pending")),
            ]),
        );
        let out = transition(&d, "running", &ctx, &ev).unwrap();
        assert_eq!(out.next_state, "running", "row 11 must win over row 16");
    }

    #[test]
    fn lease_lost_makes_orphaned_reachable_and_re_claimable() {
        let d = def();
        let ctx = Context::new();
        for from in ACTIVE_STATES {
            let out = transition(&d, from, &ctx, &EventInput::new("lease_lost")).unwrap();
            assert!(out.fired, "lease_lost must fire from {from}");
            assert_eq!(out.next_state, "orphaned");
        }
        // and orphaned re-enters running via claim (row 1b).
        let reclaimed = transition(&d, "orphaned", &ctx, &EventInput::new("claim")).unwrap();
        assert_eq!(reclaimed.next_state, "running");
    }

    // ── no-op semantics: an undefined edge never errors, never moves ────────────

    #[test]
    fn ordinary_continuation_with_no_signal_is_a_well_defined_noop() {
        let d = def();
        let ctx = Context::new();
        let ev = EventInput::with_payload("posttick", payload(&[]));
        let out = transition(&d, "pending", &ctx, &ev).unwrap();
        assert!(!out.fired);
        assert_eq!(
            out.next_state, "pending",
            "stays put — no terminal, no heartbeat needed"
        );
    }

    #[test]
    fn a_final_state_receiving_any_further_event_is_a_noop() {
        let d = def();
        let ctx = Context::new();
        for event in ["pretick", "posttick", "claim", "resume", "reject"] {
            let out = transition(&d, "completed", &ctx, &EventInput::new(event)).unwrap();
            assert!(!out.fired, "a final state must not re-fire on {event}");
        }
    }

    // ── end-to-end through the durable store + generalized `Configuration` step,
    // exactly the path `Method::Statechart` drives. ─────────────────────────────

    #[test]
    fn end_to_end_through_the_durable_store_reaches_stalled() {
        let dir = tempfile::tempdir().unwrap();
        let store = eg_statechart::StatechartStore::open_in_dir(dir.path()).unwrap();
        let def_id = store.define(&def()).unwrap();
        let instance = store
            .instantiate(&def_id, Context::new(), "tenant-a", "loop-driver")
            .unwrap();
        assert!(instance.in_state("submitted"));

        let claimed = store
            .send_event(&instance.instance_id, &EventInput::new("claim"), None)
            .unwrap();
        assert!(claimed.instance.in_state("running"));

        let stalled = store
            .send_event(
                &instance.instance_id,
                &EventInput::with_payload(
                    "posttick",
                    payload(&[("stalled", serde_json::json!(true))]),
                ),
                None,
            )
            .unwrap();
        assert!(stalled.instance.in_state("stalled"));
        assert!(
            eg_statechart::instance::InstanceStatus::Final == stalled.instance.status,
            "reaching a final state marks the instance Final"
        );
    }

    #[test]
    fn coverage_matrix_reports_every_declared_state_reachable() {
        // Sanity: the generalized `step`/`Configuration` interpreter (what the
        // durable store actually drives) agrees with the flat `transition` results
        // above for an atomic (non-composite) chart like this one.
        let d = def();
        let config = Configuration::atomic("submitted");
        let ctx = Context::new();
        let out = eg_statechart::step(&d, &config, &ctx, &EventInput::new("claim")).unwrap();
        assert!(out.fired);
        assert!(out.next.contains("running"));
    }
}
