//! Connector, modality, embedding, search, discovery, and native inference surfaces.

use crate::{DurabilityDomain, OpaqueKind, PayloadShape, SchemaRef, Stability, TxnParticipation};

use super::{make_policy, spec, PolicyFlags, PolicyRow, PYTHON};

pub(crate) const ROWS: &[PolicyRow] = &[
    #[cfg(feature = "modality-serving")]
    ("ServedModality", spec(make_policy(true, DurabilityDomain::GraphRedb, "modality:write", PolicyFlags { idempotent: false, audited: true, emits_cdc: true }, TxnParticipation::Atomic), SchemaRef::Opaque(OpaqueKind::Undeclared), PYTHON, Stability::Stable), "runtime-conditional: authority/query/events/capabilities are verified read snapshots; ingest/delete/cold/restore commit an encrypted state-backed MutationBatch"),
    ("ParseFile", spec(make_policy(false, DurabilityDomain::None, "compute:parse", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::None), SchemaRef::Opaque(OpaqueKind::Json), PYTHON, Stability::Stable), ""),
    ("ParseFiles", spec(make_policy(false, DurabilityDomain::None, "compute:parse", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::None), SchemaRef::Opaque(OpaqueKind::Json), PYTHON, Stability::Stable), ""),
    ("IndexRepository", spec(make_policy(false, DurabilityDomain::None, "compute:parse", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::None), SchemaRef::Opaque(OpaqueKind::Json), PYTHON, Stability::Stable), ""),
    ("ObserveScreen", spec(make_policy(false, DurabilityDomain::None, "compute:vision", PolicyFlags { idempotent: false, audited: false, emits_cdc: false }, TxnParticipation::None), SchemaRef::Opaque(OpaqueKind::Json), PYTHON, Stability::Stable), ""),
    ("AddEmbedding", spec(make_policy(true, DurabilityDomain::GraphRedb, "node:write", PolicyFlags { idempotent: false, audited: true, emits_cdc: false }, TxnParticipation::Atomic), SchemaRef::Payload(PayloadShape::Text), PYTHON, Stability::Stable), ""),
    ("SemanticSearch", spec(make_policy(false, DurabilityDomain::None, "compute:semantic", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), SchemaRef::Opaque(OpaqueKind::Undeclared), PYTHON, Stability::Stable), ""),
    ("Discover", spec(make_policy(false, DurabilityDomain::None, "compute:semantic", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), SchemaRef::Opaque(OpaqueKind::Undeclared), PYTHON, Stability::Stable), ""),
    #[cfg(feature = "quantum")]
    ("Quantum", spec(make_policy(false, DurabilityDomain::None, "quantum:run", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::None), SchemaRef::Opaque(OpaqueKind::Undeclared), PYTHON, Stability::Stable), "self-routes before dispatch_graph_op like AnalyticsJob/Statechart, never reaches the graph tamper-evident audit chain; R5 override audit instead rides the response's PlannerDecision.audit trail into the agent-utilities :ToolCall/:QuantumJob provenance"),
    #[cfg(feature = "asr-native")]
    ("Asr", spec(make_policy(false, DurabilityDomain::None, "asr:transcribe", PolicyFlags { idempotent: false, audited: false, emits_cdc: false }, TxnParticipation::None), SchemaRef::Opaque(OpaqueKind::Undeclared), PYTHON, Stability::Stable), "self-routes before dispatch_graph_op like Quantum/Viz; direct non-durable whisper-rs transcription, commits no asr.result.v1 (that governed commit is future worker/AU-orchestration work, W03/W06)"),
    #[cfg(feature = "viz")]
    ("Viz", spec(make_policy(false, DurabilityDomain::None, "viz:render", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::None), SchemaRef::Opaque(OpaqueKind::Undeclared), PYTHON, Stability::Stable), "pure compute: resolves a fresh per-request ColumnStore and returns rendered bytes, no durable write (D-VZ-1 lanes V4/V6)"),
];
