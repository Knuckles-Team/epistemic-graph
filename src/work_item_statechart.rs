//! The **WorkItem statechart** (CONCEPT:INT-P2-2, ADR-5 / W2.2 — the statechart
//! substrate for the durable unit-of-work lifecycle).
//!
//! `agent_utilities.orchestration.work_item`'s eight-value lifecycle
//! (`submitted → ready → leased → running → {succeeded|failed|cancelled|dead_letter}`)
//! is enforced today by ~20 hand-rolled `matches!`/`if` state-literal sites inside
//! `redb_store.rs`'s five native WorkItem handlers. This module expresses that lifecycle
//! ONCE as a reusable, content-addressed [`eg_statechart::StatechartDef`] — a closed
//! state vocabulary + guarded δ — so the lifecycle has a single, formally-checkable
//! source of truth.
//!
//! ## The dual-authority split (ADR-5, the rule that prevents the classic bug)
//!
//! * The redb **CAS/fencing row stays the sole CLAIM/LEASE authority** — who may work:
//!   `lease_owner`, `lease_epoch`, `fencing_token`, `lease_expires_at`. Untouched here.
//! * The **statechart becomes the sole LIFECYCLE authority** — which transitions are
//!   legal: `submitted → ready → …`. This chart IS that authority's definition.
//!
//! They never answer the same question. This chart never decides who holds a lease; the
//! CAS row never decides whether `leased → succeeded` is a legal edge.
//!
//! ## Phase 1 (this wave): dual-write mirror, NOT a flip
//!
//! In phase 1 the redb row's hand-computed `status` remains the AUTHORITY. Each of the
//! five native handlers additionally drives a co-located [`eg_statechart::MachineInstance`]
//! mirror (stored on the SAME WorkItem node, in the SAME redb write transaction — so a
//! `kill -9` mid-transition can never split the row from its mirror) via
//! [`mirror_outcome`], and compares the chart's independently-computed next state against
//! the authority's. A disagreement raises the [`emit_divergence`] alarm (a structured
//! `tracing::warn!` + a Prometheus counter). Phase 2 flips authority to the chart; phase 3
//! deletes the Python enum vocabulary. See `reports/wave2/w2_2-statechart-phase-plan.md`.
//!
//! ## Fencing / retry / DLQ policy is DATA on the event, not code here (ADR-5 §1)
//!
//! `eg_statechart::Guard` is a closed language that can compare a context/event field to a
//! **constant**, but NOT one field to another (there is no `context.x == event.y` guard).
//! So — exactly as [`crate::loop_statechart`]'s correction (3) does — the caller passes the
//! booleans it has ALREADY computed at the call site as event-payload fields the chart
//! reads with a plain `EventEq`:
//!
//! * `fence_valid` — the redb handler's own lease/epoch/fencing-token/expiry validity
//!   check (`lease_owner == worker_ref && lease_epoch == expected && … && not expired`).
//!   This is precisely "transition-to-`leased`/`running` carries the CAS fencing token as a
//!   GUARD INPUT" (ADR-5 §2): the fencing token gates the lifecycle edge through this
//!   boolean, so a fenced-out worker's event is a well-defined no-op on both sides.
//! * `retryable` — the caller's `retryable` flag (retry-curve POLICY, owned by Python).
//! * `retry_eligible` — `retryable && attempt < max_attempts` (DLQ-threshold POLICY).
//! * `cancellable` — not terminal and not a live in-flight lease.
//!
//! The chart therefore keeps an empty `Context`; every guard is an `EventEq` on a
//! pre-computed decision boolean — the lifecycle STRUCTURE (which state a given event +
//! condition lands in) is what the chart owns and what the mirror independently verifies.

use std::sync::LazyLock;

use eg_statechart::{transition, Context, EventInput, Guard, State, StatechartDef, Transition};

/// The four non-final states.
const NON_FINAL_STATES: &[&str] = &["submitted", "ready", "leased", "running"];

/// The four final states — identical to `work_item.TERMINAL_WORK_ITEM_STATUSES`.
pub const FINAL_STATES: &[&str] = &["succeeded", "failed", "cancelled", "dead_letter"];

/// The two active (leased-or-running) states a fenced mutation may transition from.
const LEASED_OR_RUNNING: &[&str] = &["leased", "running"];

// ── Σ event names (the five native WorkItem methods + the internal expiry sweep) ──────
pub const EV_SUBMIT: &str = "submit";
pub const EV_DEPEND_RELEASE: &str = "depend_release";
pub const EV_CLAIM: &str = "claim";
pub const EV_RENEW: &str = "renew";
pub const EV_COMMIT_SUCCEEDED: &str = "commit_succeeded";
pub const EV_COMMIT_FAILED: &str = "commit_failed";
pub const EV_COMMIT_CANCELLED: &str = "commit_cancelled";
pub const EV_CANCEL: &str = "cancel";
pub const EV_DEFER: &str = "defer";
pub const EV_LEASE_RECLAIM: &str = "lease_reclaim";
pub const EV_LEASE_EXHAUSTED: &str = "lease_exhausted";

fn event_true(key: &str) -> Guard {
    Guard::EventEq {
        key: key.to_string(),
        value: serde_json::Value::Bool(true),
    }
}

/// Build the WorkItem `StatechartDef` (ADR-5 §1). Pure, deterministic, content-addressed
/// via [`StatechartDef::def_id`] — calling it twice yields byte-identical bytes.
///
/// The transition list order is the evaluation order: `eg-statechart` fires the FIRST
/// guard that holds, so the three `commit_failed` rows are declared retry → DLQ → fail to
/// mirror `redb_store.rs`'s `if retryable && attempt < max { ready } else if retryable
/// { dead_letter } else { failed }` branch order exactly.
pub fn work_item_statechart_def() -> StatechartDef {
    let states: Vec<State> = NON_FINAL_STATES
        .iter()
        .chain(FINAL_STATES.iter())
        .map(|id| State::new(*id))
        .collect();

    let mut transitions: Vec<Transition> = Vec::new();

    // ── submit / dependency-release (intake; the mirror seeds from the row status, so
    // these exist for chart completeness + phase-2 self-driving, not the phase-1 driver) ──
    transitions
        .push(Transition::new("submitted", EV_SUBMIT, "ready").with_guard(event_true("dep_zero")));
    transitions.push(
        Transition::new("submitted", EV_DEPEND_RELEASE, "ready")
            .with_guard(event_true("dep_becomes_zero")),
    );

    // ── claim: the authority already ran multi-row candidate selection, so the picked
    // item's `ready → leased` edge is unconditional here (ADR-5 §2 / design §3.1). ──
    transitions.push(Transition::new("ready", EV_CLAIM, "leased").with_guard(Guard::Always));

    // ── expiry sweep: an expired lease is fenced back to `ready` during selection,
    // unless its claim already consumed the retry budget.  The latter transition
    // is deliberately terminal: ClaimWorkItem must never create attempt N+1.
    for from in LEASED_OR_RUNNING {
        transitions
            .push(Transition::new(*from, EV_LEASE_RECLAIM, "ready").with_guard(Guard::Always));
        transitions.push(
            Transition::new(*from, EV_LEASE_EXHAUSTED, "dead_letter").with_guard(Guard::Always),
        );
    }

    // ── renew / checkpoint heartbeat: leased|running → running under a valid fence. ──
    for from in LEASED_OR_RUNNING {
        transitions.push(
            Transition::new(*from, EV_RENEW, "running").with_guard(event_true("fence_valid")),
        );
    }

    // ── commit succeeded / failed / cancelled under a valid fence. ──
    for from in LEASED_OR_RUNNING {
        transitions.push(
            Transition::new(*from, EV_COMMIT_SUCCEEDED, "succeeded")
                .with_guard(event_true("fence_valid")),
        );
    }
    // commit_failed: retry (→ready) THEN exhausted-retry DLQ (→dead_letter) THEN
    // non-retryable (→failed) — declaration order IS the branch order of the handler.
    for from in LEASED_OR_RUNNING {
        transitions.push(
            Transition::new(*from, EV_COMMIT_FAILED, "ready").with_guard(Guard::All {
                guards: vec![
                    event_true("fence_valid"),
                    event_true("retryable"),
                    event_true("retry_eligible"),
                ],
            }),
        );
    }
    for from in LEASED_OR_RUNNING {
        transitions.push(
            Transition::new(*from, EV_COMMIT_FAILED, "dead_letter").with_guard(Guard::All {
                guards: vec![event_true("fence_valid"), event_true("retryable")],
            }),
        );
    }
    for from in LEASED_OR_RUNNING {
        transitions.push(
            Transition::new(*from, EV_COMMIT_FAILED, "failed")
                .with_guard(event_true("fence_valid")),
        );
    }
    for from in LEASED_OR_RUNNING {
        transitions.push(
            Transition::new(*from, EV_COMMIT_CANCELLED, "cancelled")
                .with_guard(event_true("fence_valid")),
        );
    }

    // ── cancel: any non-terminal state that is not a live in-flight lease → cancelled. ──
    for from in NON_FINAL_STATES {
        transitions.push(
            Transition::new(*from, EV_CANCEL, "cancelled").with_guard(event_true("cancellable")),
        );
    }

    // ── defer: release a fenced lease back to `ready` for a later retry. ──
    for from in LEASED_OR_RUNNING {
        transitions
            .push(Transition::new(*from, EV_DEFER, "ready").with_guard(event_true("fence_valid")));
    }

    StatechartDef {
        name: "work_item".to_string(),
        schema_version: 1,
        states,
        alphabet: vec![
            EV_SUBMIT.to_string(),
            EV_DEPEND_RELEASE.to_string(),
            EV_CLAIM.to_string(),
            EV_RENEW.to_string(),
            EV_COMMIT_SUCCEEDED.to_string(),
            EV_COMMIT_FAILED.to_string(),
            EV_COMMIT_CANCELLED.to_string(),
            EV_CANCEL.to_string(),
            EV_DEFER.to_string(),
            EV_LEASE_RECLAIM.to_string(),
            EV_LEASE_EXHAUSTED.to_string(),
        ],
        transitions,
        initial: "submitted".to_string(),
        finals: FINAL_STATES.iter().map(|s| s.to_string()).collect(),
        meta: Default::default(),
    }
}

/// The embedded WorkItem chart, built once. The pure decision function used by every
/// native handler's phase-1 mirror.
pub static WORK_ITEM_DEF: LazyLock<StatechartDef> = LazyLock::new(work_item_statechart_def);

/// The content-addressed id of [`WORK_ITEM_DEF`] — stamped onto each mirror so a
/// projected state row is traceable to the exact chart that produced it.
pub static WORK_ITEM_DEF_ID: LazyLock<String> = LazyLock::new(|| WORK_ITEM_DEF.def_id());

/// The result of driving the phase-1 mirror one step: the chart's independently-computed
/// next state, whether it fired, and whether it DIVERGED from the authority's decision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MirrorOutcome {
    /// The state the chart lands in (`pre_status` unchanged on a no-op).
    pub next_state: String,
    /// Whether the chart fired a real transition (vs a well-defined no-op).
    pub fired: bool,
    /// True iff the chart disagrees with the authority: either the authority transitioned
    /// and the chart did not (or landed elsewhere), or vice versa.
    pub diverged: bool,
}

/// Drive the WorkItem mirror one step and compare it to the authoritative outcome
/// (ADR-5 phase 1). Pure and total — no I/O, so it is directly unit-testable and the
/// induced-divergence test drives it with a deliberately-mismatched `authoritative_next`.
///
/// * `pre_status` — the row's status BEFORE the authority mutated it (the mirror's
///   pre-state; the mirror is re-seeded from the authority each step to keep phase 1 a
///   per-transition oracle with no divergence cascade).
/// * `event` / `payload` — the mapped Σ event + the pre-computed decision booleans.
/// * `authoritative_next` — `Some(status)` the authority persisted, or `None` if the
///   authority did NOT transition (a fenced/no-op result).
pub fn mirror_outcome(
    pre_status: &str,
    event: &str,
    payload: serde_json::Value,
    authoritative_next: Option<&str>,
) -> MirrorOutcome {
    let ctx = Context::new();
    let ev = EventInput::with_payload(event, payload);
    match transition(&WORK_ITEM_DEF, pre_status, &ctx, &ev) {
        Ok(outcome) => {
            let next_state = outcome.next_state;
            let authoritative_fired = authoritative_next.is_some();
            let diverged = outcome.fired != authoritative_fired
                || authoritative_next.is_some_and(|auth| auth != next_state);
            MirrorOutcome {
                next_state,
                fired: outcome.fired,
                diverged,
            }
        }
        Err(error) => {
            // An unknown state/event is itself a divergence: the chart cannot represent a
            // transition the authority took. Report the authority's own next state (or the
            // pre-state) so the mirror row never carries an un-decodable value, and flag it.
            tracing::warn!(
                target: "epistemic_graph::statechart_divergence",
                pre_status,
                event,
                error = %error,
                "WorkItem statechart mirror could not decode a transition",
            );
            MirrorOutcome {
                next_state: authoritative_next.unwrap_or(pre_status).to_string(),
                fired: authoritative_next.is_some(),
                diverged: true,
            }
        }
    }
}

/// Raise the phase-1 divergence ALARM: a structured, targeted `tracing::warn!` carrying
/// the exact fields an operator needs to triage a chart bug, plus the
/// `epistemic_graph_statechart_divergence_total{machine="work_item"}` counter. A
/// first-class deliverable of the dual-write migration (ADR-5 §4) — a silent mirror would
/// defeat the whole point of running one before the authority flip.
pub fn emit_divergence(
    work_item_id: &str,
    pre_status: &str,
    event: &str,
    authoritative_next: Option<&str>,
    mirror_next: &str,
) {
    crate::metrics::statechart_divergence("work_item");
    tracing::warn!(
        target: "epistemic_graph::statechart_divergence",
        work_item_id,
        machine = "work_item",
        pre_status,
        event,
        authoritative = authoritative_next.unwrap_or("<no-transition>"),
        mirror = mirror_next,
        "WorkItem statechart mirror diverged from the authoritative lifecycle status",
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use eg_statechart::validate;

    fn def() -> StatechartDef {
        work_item_statechart_def()
    }

    fn payload(fields: &[(&str, bool)]) -> serde_json::Value {
        serde_json::Value::Object(
            fields
                .iter()
                .map(|(k, v)| (k.to_string(), serde_json::Value::Bool(*v)))
                .collect(),
        )
    }

    // ── structural validity ──────────────────────────────────────────────────────────
    #[test]
    fn definition_is_structurally_valid_and_content_addressed() {
        let d = def();
        validate(&d).expect("work_item chart must pass eg-statechart's own validator");
        assert_eq!(d.def_id(), def().def_id(), "def_id must be deterministic");
        assert_eq!(d.initial, "submitted");
        assert_eq!(d.states.len(), 8, "the ONE 8-value lifecycle vocabulary");
        assert_eq!(d.finals.len(), 4);
        for id in FINAL_STATES {
            assert!(d.is_final(id), "{id} must be a declared final state");
        }
    }

    #[test]
    fn every_lifecycle_state_is_present() {
        let d = def();
        for id in [
            "submitted",
            "ready",
            "leased",
            "running",
            "succeeded",
            "failed",
            "cancelled",
            "dead_letter",
        ] {
            assert!(d.has_state(id), "missing state {id}");
        }
    }

    // ── the happy path: submit → ready → leased → running → succeeded ──────────────────
    #[test]
    fn happy_path_reaches_succeeded() {
        let d = def();
        let ctx = Context::new();
        let ready = transition(
            &d,
            "submitted",
            &ctx,
            &EventInput::with_payload(EV_SUBMIT, payload(&[("dep_zero", true)])),
        )
        .unwrap();
        assert_eq!(ready.next_state, "ready");
        let leased = transition(&d, "ready", &ctx, &EventInput::new(EV_CLAIM)).unwrap();
        assert_eq!(leased.next_state, "leased");
        let running = transition(
            &d,
            "leased",
            &ctx,
            &EventInput::with_payload(EV_RENEW, payload(&[("fence_valid", true)])),
        )
        .unwrap();
        assert_eq!(running.next_state, "running");
        let done = transition(
            &d,
            "running",
            &ctx,
            &EventInput::with_payload(EV_COMMIT_SUCCEEDED, payload(&[("fence_valid", true)])),
        )
        .unwrap();
        assert_eq!(done.next_state, "succeeded");
    }

    // ── the three commit_failed branches (retry / DLQ / fail) match the handler order ──
    #[test]
    fn commit_failed_retry_eligible_returns_to_ready() {
        let d = def();
        for from in ["leased", "running"] {
            let out = transition(
                &d,
                from,
                &Context::new(),
                &EventInput::with_payload(
                    EV_COMMIT_FAILED,
                    payload(&[
                        ("fence_valid", true),
                        ("retryable", true),
                        ("retry_eligible", true),
                    ]),
                ),
            )
            .unwrap();
            assert_eq!(
                out.next_state, "ready",
                "retryable + eligible → ready, from {from}"
            );
        }
    }

    #[test]
    fn commit_failed_retryable_but_exhausted_dead_letters() {
        let d = def();
        let out = transition(
            &d,
            "running",
            &Context::new(),
            &EventInput::with_payload(
                EV_COMMIT_FAILED,
                payload(&[
                    ("fence_valid", true),
                    ("retryable", true),
                    ("retry_eligible", false),
                ]),
            ),
        )
        .unwrap();
        assert_eq!(out.next_state, "dead_letter");
    }

    #[test]
    fn commit_failed_non_retryable_fails() {
        let d = def();
        let out = transition(
            &d,
            "leased",
            &Context::new(),
            &EventInput::with_payload(
                EV_COMMIT_FAILED,
                payload(&[("fence_valid", true), ("retryable", false)]),
            ),
        )
        .unwrap();
        assert_eq!(out.next_state, "failed");
    }

    // ── the fencing-token guard input: an invalid fence is a well-defined no-op ────────
    #[test]
    fn a_fenced_out_mutation_is_a_noop_on_the_chart() {
        let d = def();
        for event in [EV_RENEW, EV_COMMIT_SUCCEEDED, EV_COMMIT_FAILED, EV_DEFER] {
            for from in ["leased", "running"] {
                let out = transition(
                    &d,
                    from,
                    &Context::new(),
                    &EventInput::with_payload(event, payload(&[("fence_valid", false)])),
                )
                .unwrap();
                assert!(
                    !out.fired,
                    "fence_valid=false must be a no-op for {event} from {from}"
                );
                assert_eq!(out.next_state, from);
            }
        }
    }

    #[test]
    fn cancel_fires_from_every_non_terminal_state_when_cancellable() {
        let d = def();
        for from in ["submitted", "ready", "leased", "running"] {
            let out = transition(
                &d,
                from,
                &Context::new(),
                &EventInput::with_payload(EV_CANCEL, payload(&[("cancellable", true)])),
            )
            .unwrap();
            assert_eq!(out.next_state, "cancelled", "cancellable from {from}");
        }
    }

    #[test]
    fn cancel_of_a_live_lease_is_a_noop() {
        let d = def();
        let out = transition(
            &d,
            "running",
            &Context::new(),
            &EventInput::with_payload(EV_CANCEL, payload(&[("cancellable", false)])),
        )
        .unwrap();
        assert!(!out.fired, "an in-flight lease is not cancellable → no-op");
    }

    #[test]
    fn defer_reclaim_and_exhausted_lease_have_distinct_terminal_outcomes() {
        let d = def();
        let deferred = transition(
            &d,
            "running",
            &Context::new(),
            &EventInput::with_payload(EV_DEFER, payload(&[("fence_valid", true)])),
        )
        .unwrap();
        assert_eq!(deferred.next_state, "ready");
        let reclaimed = transition(
            &d,
            "leased",
            &Context::new(),
            &EventInput::new(EV_LEASE_RECLAIM),
        )
        .unwrap();
        assert_eq!(reclaimed.next_state, "ready");
        let exhausted = transition(
            &d,
            "running",
            &Context::new(),
            &EventInput::new(EV_LEASE_EXHAUSTED),
        )
        .unwrap();
        assert_eq!(exhausted.next_state, "dead_letter");
    }

    #[test]
    fn terminal_states_absorb_every_event() {
        let d = def();
        for terminal in FINAL_STATES {
            for event in [EV_CLAIM, EV_RENEW, EV_COMMIT_SUCCEEDED, EV_CANCEL, EV_DEFER] {
                let out = transition(
                    &d,
                    terminal,
                    &Context::new(),
                    &EventInput::with_payload(event, payload(&[("fence_valid", true)])),
                )
                .unwrap();
                assert!(!out.fired, "terminal {terminal} must absorb {event}");
            }
        }
    }

    // ── mirror_outcome: agreement + the induced-divergence path ───────────────────────
    #[test]
    fn mirror_agrees_with_the_authority_on_the_happy_path() {
        // ready --claim--> leased: authority says leased, chart says leased ⇒ no divergence.
        let out = mirror_outcome("ready", EV_CLAIM, serde_json::json!({}), Some("leased"));
        assert!(out.fired);
        assert_eq!(out.next_state, "leased");
        assert!(!out.diverged);
    }

    #[test]
    fn mirror_flags_divergence_when_authority_lands_elsewhere() {
        // Induce a divergence: the chart routes commit_failed+retry_eligible to `ready`,
        // but the authority (hypothetically buggy) claims it committed `succeeded`.
        let out = mirror_outcome(
            "running",
            EV_COMMIT_FAILED,
            serde_json::json!({"fence_valid": true, "retryable": true, "retry_eligible": true}),
            Some("succeeded"),
        );
        assert_eq!(out.next_state, "ready", "the chart's own decision");
        assert!(
            out.diverged,
            "chart=ready vs authority=succeeded must diverge"
        );
    }

    #[test]
    fn mirror_flags_divergence_when_authority_did_not_transition() {
        // The chart WOULD fire ready--claim-->leased, but the authority reported no
        // transition (None) — that mismatch is a divergence too.
        let out = mirror_outcome("ready", EV_CLAIM, serde_json::json!({}), None);
        assert!(out.fired);
        assert!(out.diverged);
    }

    #[test]
    fn mirror_agrees_when_both_no_op_on_a_fenced_event() {
        // fence_valid=false ⇒ chart no-op; authority also fenced (None) ⇒ agreement.
        let out = mirror_outcome(
            "running",
            EV_COMMIT_SUCCEEDED,
            serde_json::json!({"fence_valid": false}),
            None,
        );
        assert!(!out.fired);
        assert!(!out.diverged);
        assert_eq!(out.next_state, "running");
    }
}
