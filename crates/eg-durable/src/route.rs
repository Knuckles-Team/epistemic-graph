//! The routing table itself (CONCEPT:AU-ORCH.execution.durable-routing-trait).
//!
//! Two levels, matching `durable-execution-native.md` Part 3 exactly:
//!
//! 1. [`CallShape`] — the five agent/tool-facing primitives DE2 will expose.
//! 2. [`WorkShape`] — the four backend-facing categories the architecture diagram
//!    names as what the existing subsystems were each independently built to serve.
//!
//! [`route_call_shape`] maps a `CallShape` to the `WorkShape` that serves it;
//! [`route_work_shape`] maps a `WorkShape` to the [`DurableBackendKind`] that owns
//! it. Both are `const fn`, total (an exhaustive `match`, no wildcard arm — adding a
//! new variant to either enum without extending the routing function is a compile
//! error, not a silent default), and free of I/O or backend dependencies.

use serde::{Deserialize, Serialize};

/// The five durable-execution call shapes `durable-execution-native.md` DE2 commits
/// to exposing as one small, documented agent-utilities tool surface: `durable_run`,
/// `durable_call`, `durable_sleep`, `durable_state_get`, `durable_state_set`.
///
/// This crate does not implement any of them — it only names them so the routing
/// table has a stable input type DE2 can build against.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CallShape {
    /// `durable_run()` — run an agent-loop/workflow durably, resuming from its last
    /// named checkpoint on restart.
    Run,
    /// `durable_call()` — a durable, idempotent call whose effect must commit
    /// atomically, potentially across more than one store.
    Call,
    /// `durable_sleep()` — wake a specific in-flight execution at (or after) a
    /// given time.
    Sleep,
    /// `durable_state_get()` — read the current value of one keyed, single-writer
    /// durable state slot.
    StateGet,
    /// `durable_state_set()` — write one keyed, single-writer durable state slot
    /// under optimistic concurrency control.
    StateSet,
}

/// The four work shapes `durable-execution-native.md` Part 3's architecture diagram
/// names as what `eg-statechart`/`eg-jobs`/`eg-mutation-store`/`DurableRun` were each
/// independently built to serve — the actual routing granularity. A `CallShape`
/// always resolves to exactly one of these before it resolves to a backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkShape {
    /// Keyed, single-writer, OCC-versioned state — `eg-statechart`'s shape.
    KeyedSingleWriterState,
    /// Durable async/checkpointed work, including timer-driven wakeups —
    /// `eg-jobs`'s shape.
    AsyncCheckpointedWork,
    /// A call whose effect must commit atomically, potentially across more than one
    /// store — `eg-mutation-store`'s saga/2PC coordinator shape.
    CrossStoreAtomicStep,
    /// A resumable agent-loop/workflow continuation, checkpointed at named steps —
    /// agent-utilities' `DurableRun` shape.
    AgentLoopContinuation,
}

/// The four existing durable backends this crate routes to. Every variant names an
/// ALREADY-REAL, independently-tested subsystem; this crate links none of them (see
/// the module doc's "zero new dependencies" section and the crate doc's "what this
/// crate is emphatically NOT").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DurableBackendKind {
    /// `eg-statechart::MachineInstance` — keyed, OCC-versioned durable state.
    Statechart,
    /// `eg-jobs::AnalyticsJob` — durable async job with fenced-lease checkpointing.
    Jobs,
    /// `eg-mutation-store`'s `prepare_saga`/`commit_saga` coordinator.
    MutationStoreSaga,
    /// `agent_utilities.orchestration.durable_execution.DurableExecutionManager` —
    /// cross-language; reached only over the engine RPC boundary DE2 wires, never a
    /// Rust dependency edge.
    PythonDurableRun,
}

/// Route a [`CallShape`] to the [`WorkShape`] that serves it.
///
/// Four of the five mappings are unambiguous restatements of the plan's own
/// architecture diagram. `Call → CrossStoreAtomicStep` is the one judgment call DE0
/// makes explicitly rather than leaving unstated: restate's `ctx.call()` is a
/// durable, idempotent RPC whose closest existing analog is a saga-coordinated
/// atomic step (real rollback semantics restate itself lacks — Part 1 row 6), not a
/// bare fire-and-forget job. DE2 owns the right to split `Call` further once real
/// call sites exist; this mapping is the DE0 default, not a closed decision.
#[must_use]
pub const fn route_call_shape(shape: CallShape) -> WorkShape {
    match shape {
        CallShape::StateGet | CallShape::StateSet => WorkShape::KeyedSingleWriterState,
        CallShape::Sleep => WorkShape::AsyncCheckpointedWork,
        CallShape::Call => WorkShape::CrossStoreAtomicStep,
        CallShape::Run => WorkShape::AgentLoopContinuation,
    }
}

/// Route a [`WorkShape`] to the [`DurableBackendKind`] that owns it — a direct,
/// unambiguous restatement of the architecture diagram's four rows.
#[must_use]
pub const fn route_work_shape(shape: WorkShape) -> DurableBackendKind {
    match shape {
        WorkShape::KeyedSingleWriterState => DurableBackendKind::Statechart,
        WorkShape::AsyncCheckpointedWork => DurableBackendKind::Jobs,
        WorkShape::CrossStoreAtomicStep => DurableBackendKind::MutationStoreSaga,
        WorkShape::AgentLoopContinuation => DurableBackendKind::PythonDurableRun,
    }
}

impl DurableBackendKind {
    /// The concrete `:DurableExecutionUnit` OWL subclass label this backend mirrors
    /// into (DE1, `agent_utilities/knowledge_graph/ontology_orchestration.ttl`) — the
    /// single source of truth for the label string, so [`crate::project::project`] and
    /// any future Python/Rust caller never re-spell it independently.
    #[must_use]
    pub const fn unit_label(self) -> &'static str {
        match self {
            DurableBackendKind::Statechart => "StatechartInstance",
            DurableBackendKind::Jobs => "AnalyticsJob",
            DurableBackendKind::MutationStoreSaga => "SagaCoordination",
            DurableBackendKind::PythonDurableRun => "DurableRun",
        }
    }
}

/// Convenience composition of [`route_call_shape`] then [`route_work_shape`] — the
/// end-to-end answer to "which backend serves this call shape".
#[must_use]
pub const fn route_call_shape_to_backend(shape: CallShape) -> DurableBackendKind {
    route_work_shape(route_call_shape(shape))
}

/// A compile-time marker trait binding a zero-sized Rust type to one
/// [`DurableBackendKind`] plus its stable string id — the "eg-durable routing-layer
/// trait" `durable-execution-native.md` DE0 asks for. Implementers carry NO
/// behavior (no method beyond identity) and NO dependency on the backend crate they
/// name; a real call-through implementation is DE2's job.
pub trait DurableBackendRef {
    /// The backend this marker identifies.
    const KIND: DurableBackendKind;

    /// The stable, human-readable id used in logs, KG `:backendRef` prefixes, and
    /// diagnostics — never parsed, only displayed.
    fn backend_id() -> &'static str;
}

/// Marker for [`DurableBackendKind::Statechart`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatechartBackend;

impl DurableBackendRef for StatechartBackend {
    const KIND: DurableBackendKind = DurableBackendKind::Statechart;

    fn backend_id() -> &'static str {
        "eg-statechart"
    }
}

/// Marker for [`DurableBackendKind::Jobs`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JobsBackend;

impl DurableBackendRef for JobsBackend {
    const KIND: DurableBackendKind = DurableBackendKind::Jobs;

    fn backend_id() -> &'static str {
        "eg-jobs"
    }
}

/// Marker for [`DurableBackendKind::MutationStoreSaga`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MutationStoreSagaBackend;

impl DurableBackendRef for MutationStoreSagaBackend {
    const KIND: DurableBackendKind = DurableBackendKind::MutationStoreSaga;

    fn backend_id() -> &'static str {
        "eg-mutation-store"
    }
}

/// Marker for [`DurableBackendKind::PythonDurableRun`] — the one backend this crate
/// names but can never link, since it is a Python module reached only over the
/// engine RPC boundary DE2 wires.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PythonDurableRunBackend;

impl DurableBackendRef for PythonDurableRunBackend {
    const KIND: DurableBackendKind = DurableBackendKind::PythonDurableRun;

    fn backend_id() -> &'static str {
        "agent_utilities.orchestration.durable_execution.DurableExecutionManager"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_call_shape_routes_to_the_diagram_work_shape() {
        assert_eq!(
            route_call_shape(CallShape::StateGet),
            WorkShape::KeyedSingleWriterState
        );
        assert_eq!(
            route_call_shape(CallShape::StateSet),
            WorkShape::KeyedSingleWriterState
        );
        assert_eq!(
            route_call_shape(CallShape::Sleep),
            WorkShape::AsyncCheckpointedWork
        );
        assert_eq!(
            route_call_shape(CallShape::Call),
            WorkShape::CrossStoreAtomicStep
        );
        assert_eq!(
            route_call_shape(CallShape::Run),
            WorkShape::AgentLoopContinuation
        );
    }

    #[test]
    fn every_work_shape_routes_to_the_diagram_backend() {
        assert_eq!(
            route_work_shape(WorkShape::KeyedSingleWriterState),
            DurableBackendKind::Statechart
        );
        assert_eq!(
            route_work_shape(WorkShape::AsyncCheckpointedWork),
            DurableBackendKind::Jobs
        );
        assert_eq!(
            route_work_shape(WorkShape::CrossStoreAtomicStep),
            DurableBackendKind::MutationStoreSaga
        );
        assert_eq!(
            route_work_shape(WorkShape::AgentLoopContinuation),
            DurableBackendKind::PythonDurableRun
        );
    }

    #[test]
    fn end_to_end_composition_matches_the_two_step_route() {
        for shape in [
            CallShape::Run,
            CallShape::Call,
            CallShape::Sleep,
            CallShape::StateGet,
            CallShape::StateSet,
        ] {
            assert_eq!(
                route_call_shape_to_backend(shape),
                route_work_shape(route_call_shape(shape))
            );
        }
    }

    #[test]
    fn backend_markers_agree_with_their_kind() {
        assert_eq!(StatechartBackend::KIND, DurableBackendKind::Statechart);
        assert_eq!(StatechartBackend::backend_id(), "eg-statechart");
        assert_eq!(JobsBackend::KIND, DurableBackendKind::Jobs);
        assert_eq!(JobsBackend::backend_id(), "eg-jobs");
        assert_eq!(
            MutationStoreSagaBackend::KIND,
            DurableBackendKind::MutationStoreSaga
        );
        assert_eq!(MutationStoreSagaBackend::backend_id(), "eg-mutation-store");
        assert_eq!(
            PythonDurableRunBackend::KIND,
            DurableBackendKind::PythonDurableRun
        );
        assert_eq!(
            PythonDurableRunBackend::backend_id(),
            "agent_utilities.orchestration.durable_execution.DurableExecutionManager"
        );
    }

    #[test]
    fn call_shape_serializes_to_the_de2_tool_names() {
        // Locks the wire vocabulary DE2's MCP tool surface will use, so a rename
        // there is a deliberate, visible change here too.
        assert_eq!(serde_json::to_string(&CallShape::Run).unwrap(), "\"run\"");
        assert_eq!(serde_json::to_string(&CallShape::Call).unwrap(), "\"call\"");
        assert_eq!(
            serde_json::to_string(&CallShape::Sleep).unwrap(),
            "\"sleep\""
        );
        assert_eq!(
            serde_json::to_string(&CallShape::StateGet).unwrap(),
            "\"state_get\""
        );
        assert_eq!(
            serde_json::to_string(&CallShape::StateSet).unwrap(),
            "\"state_set\""
        );
    }

    #[test]
    fn unit_label_matches_the_de0_ontology_subclass_names() {
        // Matches agent_utilities/knowledge_graph/ontology_orchestration.ttl's
        // :StatechartInstance / :AnalyticsJob / :SagaCoordination / :DurableRun
        // labels exactly -- a rename on either side without the other is a defect
        // this test is the tripwire for.
        assert_eq!(
            DurableBackendKind::Statechart.unit_label(),
            "StatechartInstance"
        );
        assert_eq!(DurableBackendKind::Jobs.unit_label(), "AnalyticsJob");
        assert_eq!(
            DurableBackendKind::MutationStoreSaga.unit_label(),
            "SagaCoordination"
        );
        assert_eq!(
            DurableBackendKind::PythonDurableRun.unit_label(),
            "DurableRun"
        );
    }
}
