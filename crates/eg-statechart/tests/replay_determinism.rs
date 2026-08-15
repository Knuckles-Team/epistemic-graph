//! DE7 — replay-determinism property test for `eg-statechart::StatechartStore`.
//!
//! Same discipline as `eg-jobs`' sibling test (`crates/eg-jobs/tests/
//! replay_determinism.rs`): a deterministic script of events must reach the
//! IDENTICAL final durable state whether it runs straight through, or is
//! interrupted by a SIMULATED CRASH (the `StatechartStore` — and the
//! `redb::Database` it owns — dropped mid-script with no graceful shutdown, then
//! reopened from the same `statecharts.redb` file) at an arbitrary cut point.
//!
//! **D-DE7-2 is now CLOSED.** This test used to carve `created_at_ms`/
//! `updated_at_ms` out of its comparison because `StatechartStore::instantiate`/
//! `send_event` read `SystemTime::now()` and a local `AtomicU64` counter
//! internally, with no caller-supplied clock — unlike `eg-jobs`, whose fenced
//! transition methods (`claim_next`/`checkpoint_fenced`/…) all take an explicit
//! `now_ms: i64`. `crates/eg-statechart/src/store.rs` now carries the direct
//! mirror of that fix: [`StatechartStore::instantiate_batch`] derives the new
//! instance's id from a caller-supplied, pre-Raft-proposal `request_batch_id`
//! (mirrors `eg-jobs::job_id_for_batch`) instead of the local counter, and
//! [`StatechartStore::send_event`] takes an explicit `now_ms: i64` parameter
//! instead of reading the clock internally. Both are pure functions of their
//! caller-supplied arguments, so — exactly like `eg-jobs`' `submit_batch` script —
//! the SAME script (shared between the clean and crash-interrupted runs by one
//! `proptest!` closure) reaches byte-identical `MachineInstance` state, timestamps
//! included. This test now compares the FULL `MachineInstance`, not a
//! timestamp-excluding projection.

use eg_statechart::{
    Context, EventInput, MachineInstance, State, StatechartDef, StatechartStore, Transition,
};
use proptest::prelude::*;

/// A flat two-state machine (`locked` --coin--> `unlocked` --push--> `locked`),
/// the same shape `store.rs`'s own private `turnstile()` test fixture uses.
fn turnstile() -> StatechartDef {
    StatechartDef {
        name: "de7-turnstile".into(),
        schema_version: 1,
        states: vec![State::new("locked"), State::new("unlocked")],
        alphabet: vec!["coin".into(), "push".into()],
        transitions: vec![
            Transition::new("locked", "coin", "unlocked"),
            Transition::new("unlocked", "push", "locked"),
        ],
        initial: "locked".into(),
        finals: vec![],
        meta: Default::default(),
    }
}

/// The event that fires from whichever side of the turnstile step `i` starts on
/// — alternating `coin`/`push` keeps every step a FIRING transition (never a
/// well-defined no-op), so every step is guaranteed to bump `version`.
fn event_for_step(i: usize) -> EventInput {
    if i.is_multiple_of(2) {
        EventInput::new("coin")
    } else {
        EventInput::new("push")
    }
}

/// Deterministic creation: `request_batch_id` and `committed_at_ms` are pure
/// functions of the script's fixed inputs (never wall-clock, never a local
/// counter), so calling this against the clean store and, separately, against the
/// crash-interrupted store's first open reaches the SAME `instance_id`.
fn instantiate_deterministic(store: &StatechartStore, def_id: &str) -> MachineInstance {
    let (instance, _replayed) = store
        .instantiate_batch(
            def_id,
            Context::new(),
            "tenant-x",
            "actor-y",
            "de7-statechart-instantiate:0",
            1_000,
        )
        .expect("instantiate_batch");
    instance
}

/// Run script steps `range` (alternating coin/push) against a pre-instantiated
/// `instance_id`, each with a deterministic `now_ms` derived purely from the step
/// index — never the wall clock.
fn run_steps(store: &StatechartStore, instance_id: &str, range: std::ops::Range<usize>) {
    for i in range {
        let now_ms = 2_000 + i as i64 * 100;
        store
            .send_event(instance_id, &event_for_step(i), None, now_ms)
            .expect("send_event");
    }
}

fn snapshot(store: &StatechartStore, instance_id: &str) -> MachineInstance {
    store.get_instance(instance_id).expect("get_instance")
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    /// The DE7 headline property: for ANY number of events and ANY cut point in
    /// the resulting script, a crash-and-reopen mid-script reaches the SAME final
    /// durable state — every field, including the two timestamps — as an
    /// uninterrupted run of the identical script.
    #[test]
    fn crash_replay_reaches_identical_state(n in 1usize..=8, crash_frac in 0usize..=100usize) {
        let crash_at = (crash_frac * n) / 100;

        // Uninterrupted baseline.
        let clean_dir = tempfile::tempdir().unwrap();
        let clean_store = StatechartStore::open_in_dir(clean_dir.path()).unwrap();
        let def_id = clean_store.define(&turnstile()).unwrap();
        let clean_instance = instantiate_deterministic(&clean_store, &def_id);
        run_steps(&clean_store, &clean_instance.instance_id, 0..n);
        let clean_snapshot = snapshot(&clean_store, &clean_instance.instance_id);

        // The clean and crash runs execute back-to-back in the same process; a
        // short sleep guarantees they land in DIFFERENT wall-clock milliseconds.
        // This has NO effect on a correct, replay-safe store (every timestamp in
        // the script is caller-supplied, never read from the clock) — it exists
        // only so a real wall-clock leak (the exact defect class D-DE7-2 was) is
        // reliably observable instead of passing by coincidence when both runs
        // happen to land in the same millisecond.
        std::thread::sleep(std::time::Duration::from_millis(15));

        // Crash-interrupted: same script, same persist dir, a hard drop (no
        // graceful shutdown) at `crash_at`, then reopen and finish the script.
        let crash_dir = tempfile::tempdir().unwrap();
        let instance_id;
        {
            let store = StatechartStore::open_in_dir(crash_dir.path()).unwrap();
            let def_id = store.define(&turnstile()).unwrap();
            let instance = instantiate_deterministic(&store, &def_id);
            instance_id = instance.instance_id.clone();
            run_steps(&store, &instance_id, 0..crash_at);
            // `store` drops here — simulated crash: no close/shutdown call.
        }
        let reopened = StatechartStore::open_in_dir(crash_dir.path()).unwrap();
        run_steps(&reopened, &instance_id, crash_at..n);
        let crash_snapshot = snapshot(&reopened, &instance_id);

        prop_assert_eq!(clean_instance.instance_id, instance_id);
        prop_assert_eq!(clean_snapshot, crash_snapshot);
    }
}
