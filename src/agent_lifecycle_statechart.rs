//! The **agent-instance lifecycle statechart** (CONCEPT:INT-P2-2, ADR-6 / W2.3 — the
//! agents-as-data activation layer).
//!
//! ## The thesis (ADR-6): a million agents are ROWS, not processes
//!
//! An agent instance is a **statechart template + a per-instance
//! [`eg_statechart::MachineInstance`] + a mailbox**. While it is not doing work it is a
//! single durable `(state, context)` row in `statecharts.redb` — **no connection, no
//! thread, no heartbeat**. This module is that template: the ONE canonical, reusable,
//! content-addressed [`eg_statechart::StatechartDef`] for an agent instance's own
//! lifecycle, exactly as [`crate::loop_statechart`] is for a Loop and
//! [`crate::work_item_statechart`] is for a unit of work.
//!
//! ```text
//!   dormant ──activate──▶ active ──deactivate──▶ dormant        (the activation cycle)
//!      │                    │
//!      └──────terminate─────┴────────▶ terminated  (final)      (retirement)
//! ```
//!
//! ## Two lifecycles, never one (ADR-6 §2, the anti-sprawl rule)
//!
//! An agent instance's lifecycle (this chart: `dormant ⇄ active`) is DISTINCT from an
//! **activation's** lifecycle (the [`crate::work_item_statechart`]: `submitted → ready →
//! leased → running → …`). They compose, they do not duplicate:
//!
//! * An **event** (CDC match, timer, broker message, direct call) creates a **WorkItem**
//!   (the ADR-5 substrate) referencing this instance. The WorkItem's own chart owns
//!   "is this activation submitted / leased / running / done"; its redb CAS row owns
//!   "who may run it" (the lease). This chart never re-encodes any of that.
//! * A stateless worker **claims** the WorkItem via the existing CAS lease, then drives
//!   this instance `dormant → active`, runs the loop, and drives it `active → dormant` on
//!   release. So `active` is precisely "an activation currently holds this instance", and
//!   the statechart's OCC `version` is the instance-level concurrency guard: two racing
//!   activations of the SAME instance cannot both land `dormant → active` (the second is
//!   a well-defined no-op — an undefined `activate`-from-`active` edge — or an
//!   OCC-`version` conflict). A dead worker's lease simply expires and the WorkItem
//!   re-queues (bounded retries → dead_letter); this chart is untouched by that recovery,
//!   because liveness is the WorkItem LEASE, never a heartbeat on the instance row.
//!
//! ## Guards are trivial here — the authority lives in the WorkItem CAS lease (ADR-6 §2/3)
//!
//! Every transition is [`eg_statechart::Guard::Always`]: the worker has ALREADY passed
//! the real gate (it holds the WorkItem's fencing-token lease) before it drives this
//! chart, exactly as [`crate::work_item_statechart`]'s `ready --claim--> leased` edge is
//! unconditional because native candidate-selection already ran. This chart therefore
//! keeps an empty `Context`; it is a pure structural authority for "which lifecycle edge
//! is legal", independently validated by [`eg_statechart::validate`].
//!
//! ## Single source of truth + the Python mirror
//!
//! [`agent_lifecycle_statechart_def`] is the canonical definition the Rust tests below
//! exercise directly (via [`eg_statechart::transition`], no server needed). The Python
//! `agent_utilities` side authors the SAME shape as a plain dict and registers it once
//! via `client.statechart.define(...)` over the EXISTING `Method::Statechart` wire surface
//! — no new wire protocol, no change to `eg-statechart` itself (mirrors `loops.py`'s
//! `LOOP_STATECHART_DEF`). Because [`eg_statechart::StatechartDef::def_id`] is a content
//! address, the two sides agree on the id iff they are byte-identical — the parity check.

use std::sync::LazyLock;

use eg_statechart::{Guard, State, StatechartDef, Transition};

/// The single non-final "doing nothing" state — the dormant row.
pub const STATE_DORMANT: &str = "dormant";
/// The single non-final "an activation holds this instance" state.
pub const STATE_ACTIVE: &str = "active";
/// The one final state — a retired instance absorbs every further event.
pub const STATE_TERMINATED: &str = "terminated";

// ── Σ event names (the three lifecycle events the worker drives) ─────────────────────
/// A claimed activation begins running the instance: `dormant → active`.
pub const EV_ACTIVATE: &str = "activate";
/// The activation released the instance back to rest: `active → dormant`.
pub const EV_DEACTIVATE: &str = "deactivate";
/// The instance is retired for good: `{dormant|active} → terminated`.
pub const EV_TERMINATE: &str = "terminate";

/// Build the agent-instance lifecycle [`StatechartDef`] (ADR-6 §1). Pure, deterministic,
/// content-addressed via [`StatechartDef::def_id`] — calling it twice yields
/// byte-identical bytes, so the Python dict mirror can be checked for parity by def id.
pub fn agent_lifecycle_statechart_def() -> StatechartDef {
    let states = vec![
        State::new(STATE_DORMANT),
        State::new(STATE_ACTIVE),
        State::new(STATE_TERMINATED),
    ];

    // Every edge is unconditional: the worker already holds the WorkItem CAS lease that
    // authorizes the transition (see the module doc). Declaration order is evaluation
    // order, but no two rows share a `(from, event)` here, so order is not load-bearing.
    let transitions = vec![
        Transition::new(STATE_DORMANT, EV_ACTIVATE, STATE_ACTIVE).with_guard(Guard::Always),
        Transition::new(STATE_ACTIVE, EV_DEACTIVATE, STATE_DORMANT).with_guard(Guard::Always),
        Transition::new(STATE_DORMANT, EV_TERMINATE, STATE_TERMINATED).with_guard(Guard::Always),
        Transition::new(STATE_ACTIVE, EV_TERMINATE, STATE_TERMINATED).with_guard(Guard::Always),
    ];

    StatechartDef {
        name: "agent_lifecycle".to_string(),
        schema_version: 1,
        states,
        alphabet: vec![
            EV_ACTIVATE.to_string(),
            EV_DEACTIVATE.to_string(),
            EV_TERMINATE.to_string(),
        ],
        transitions,
        initial: STATE_DORMANT.to_string(),
        finals: vec![STATE_TERMINATED.to_string()],
        meta: Default::default(),
    }
}

/// The embedded agent-lifecycle chart, built once — the canonical template every dormant
/// agent instance is an instance OF.
pub static AGENT_LIFECYCLE_DEF: LazyLock<StatechartDef> =
    LazyLock::new(agent_lifecycle_statechart_def);

/// The content-addressed id of [`AGENT_LIFECYCLE_DEF`] — the same id the Python
/// `AGENT_LIFECYCLE_DEF` dict must produce when registered, proving byte-parity.
pub static AGENT_LIFECYCLE_DEF_ID: LazyLock<String> =
    LazyLock::new(|| AGENT_LIFECYCLE_DEF.def_id());

#[cfg(test)]
mod tests {
    use super::*;
    use eg_statechart::{transition, validate, Context, EventInput};

    fn def() -> StatechartDef {
        agent_lifecycle_statechart_def()
    }

    #[test]
    fn definition_is_structurally_valid_and_content_addressed() {
        let d = def();
        validate(&d).expect("agent_lifecycle chart must pass eg-statechart's own validator");
        assert_eq!(d.def_id(), def().def_id(), "def_id must be deterministic");
        assert_eq!(d.initial, STATE_DORMANT);
        assert_eq!(d.states.len(), 3, "dormant + active + terminated");
        assert_eq!(d.finals.len(), 1);
        assert!(d.is_final(STATE_TERMINATED));
        assert!(!d.is_final(STATE_DORMANT));
        assert!(!d.is_final(STATE_ACTIVE));
    }

    #[test]
    fn every_lifecycle_state_is_present() {
        let d = def();
        for id in [STATE_DORMANT, STATE_ACTIVE, STATE_TERMINATED] {
            assert!(d.has_state(id), "missing state {id}");
        }
    }

    // ── the activation cycle: dormant → active → dormant ─────────────────────────────
    #[test]
    fn activation_cycle_round_trips() {
        let d = def();
        let ctx = Context::new();
        let activated = transition(&d, STATE_DORMANT, &ctx, &EventInput::new(EV_ACTIVATE)).unwrap();
        assert!(activated.fired);
        assert_eq!(activated.next_state, STATE_ACTIVE);
        let released = transition(&d, STATE_ACTIVE, &ctx, &EventInput::new(EV_DEACTIVATE)).unwrap();
        assert!(released.fired);
        assert_eq!(released.next_state, STATE_DORMANT);
    }

    // ── the instance-level concurrency guard: a second activate is a no-op (ADR-6 §2) ──
    #[test]
    fn activate_of_an_already_active_instance_is_a_noop() {
        let d = def();
        let out = transition(
            &d,
            STATE_ACTIVE,
            &Context::new(),
            &EventInput::new(EV_ACTIVATE),
        )
        .unwrap();
        assert!(
            !out.fired,
            "a racing second activation cannot re-activate an already-active instance"
        );
        assert_eq!(out.next_state, STATE_ACTIVE);
    }

    #[test]
    fn deactivate_of_a_dormant_instance_is_a_noop() {
        let d = def();
        let out = transition(
            &d,
            STATE_DORMANT,
            &Context::new(),
            &EventInput::new(EV_DEACTIVATE),
        )
        .unwrap();
        assert!(!out.fired, "a dormant instance has nothing to deactivate");
        assert_eq!(out.next_state, STATE_DORMANT);
    }

    // ── retirement: terminate fires from both non-final states ────────────────────────
    #[test]
    fn terminate_fires_from_dormant_and_active() {
        let d = def();
        for from in [STATE_DORMANT, STATE_ACTIVE] {
            let out =
                transition(&d, from, &Context::new(), &EventInput::new(EV_TERMINATE)).unwrap();
            assert!(out.fired, "terminate must fire from {from}");
            assert_eq!(out.next_state, STATE_TERMINATED);
        }
    }

    #[test]
    fn terminated_absorbs_every_event() {
        let d = def();
        for event in [EV_ACTIVATE, EV_DEACTIVATE, EV_TERMINATE] {
            let out = transition(
                &d,
                STATE_TERMINATED,
                &Context::new(),
                &EventInput::new(event),
            )
            .unwrap();
            assert!(!out.fired, "terminal state must absorb {event}");
            assert_eq!(out.next_state, STATE_TERMINATED);
        }
    }

    #[test]
    fn def_id_is_stable_and_exposed() {
        // The Python `AGENT_LIFECYCLE_DEF` mirror must reproduce this exact id.
        assert_eq!(*AGENT_LIFECYCLE_DEF_ID, def().def_id());
        assert!(!AGENT_LIFECYCLE_DEF_ID.is_empty());
    }
}
