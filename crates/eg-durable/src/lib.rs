//! `eg-durable` — the durable-execution ROUTING CONTRACT (register `D-DE-0`, lane
//! `w6-de0-contracts`, parent program `D-DE-1` /
//! `plans/au-eg-program/program/durable-execution-native.md`).
//!
//! # What this crate is
//!
//! Restate gives an SDK author one `ctx.*` mental model regardless of what backs it
//! (Bifrost log, partition store, RocksDB). `durable-execution-native.md` Part 3
//! asks for the same thing here, over backends that already exist and already work:
//!
//! ```text
//!  agent / tool code
//!         │  durable_run() · durable_call() · durable_sleep() · durable_state_get/set()
//!         ▼
//!    eg-durable  (THIS CRATE — the routing layer, DE0 contracts / DE2 implementation)
//!         │
//!         ├─ keyed single-writer state  → eg-statechart (MachineInstance, OCC)
//!         ├─ async/checkpointed work    → eg-jobs (AnalyticsJob, fenced leases)
//!         ├─ cross-store atomic step    → eg-mutation-store (saga/2PC coordinator)
//!         └─ agent-loop continuations   → AU DurableRun (Python step checkpoints)
//! ```
//!
//! **DE0 ships only the routing decision** — [`CallShape`], [`WorkShape`],
//! [`DurableBackendKind`], the total [`route_call_shape`]/[`route_work_shape`]
//! functions, the [`DurableBackendRef`] marker trait + its four zero-sized marker
//! types, and the [`DurableExecutionUnitMirror`] DTO matching the KG schema reserved
//! in the sibling `agent-utilities` ontology change
//! (`AU-KG.storage.durable-execution-unit` and its four subclass concepts). It links
//! none of `eg-statechart`, `eg-jobs`, or `eg-mutation-store` — wiring an actual call
//! through to one of those crates' real, already-durable APIs is DE2 (the unified
//! `durable_run`/`durable_call`/`durable_sleep`/`durable_state_get`/
//! `durable_state_set` MCP tool surface), not this lane.
//!
//! # What this crate is emphatically NOT
//!
//! **No journal. No lease. No two-phase commit. No state store.** Those mechanisms
//! already exist, are already tested, and are already durable:
//!
//! - `eg-mutation-store` — commit-before-ack journal (`BATCHES`/`IDEMPOTENCY`/
//!   `VERSIONS`/`OUTBOX`) plus a real saga/2PC coordinator (`prepare_saga`/
//!   `commit_saga`) restate itself does not have.
//! - `eg-jobs` — durable async jobs, fenced-lease CAS (`claim_next`/`renew_lease`),
//!   cron/interval/manual triggers.
//! - `eg-statechart` — durable keyed state with OCC, routed through the same
//!   `MutationBatch`/`eg-mutation-store` gateway `eg-jobs` uses.
//! - `agent_utilities.orchestration.durable_execution` — `DurableExecutionManager`/
//!   `DurableRun`, SQLite-/Postgres-backed named-step checkpointing.
//!
//! If a future change to this crate starts implementing any of the above, that
//! change has drifted off DE0's scope — the gap this program closes is that these
//! four subsystems are not modeled as ONE KG-queryable concept family, not that any
//! of them is missing a durability mechanism.
//!
//! # Zero new dependencies
//!
//! `serde` is the only dependency, already resolved workspace-wide by every other
//! `eg-*` crate — this optional, default-off crate adds nothing new to `Cargo.lock`.

pub mod mirror;
pub mod route;

pub use mirror::DurableExecutionUnitMirror;
pub use route::{
    route_call_shape, route_work_shape, CallShape, DurableBackendKind, DurableBackendRef,
    JobsBackend, MutationStoreSagaBackend, PythonDurableRunBackend, StatechartBackend, WorkShape,
};
