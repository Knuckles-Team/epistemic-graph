//! Connector, modality, embedding, search, discovery, and native inference surfaces.

use crate::{DurabilityDomain, TxnParticipation};

use super::{make_policy, PolicyFlags, PolicyRow};

pub(crate) const ROWS: &[PolicyRow] = &[
    #[cfg(feature = "modality-serving")]
    ("ServedModality", make_policy(true, DurabilityDomain::GraphRedb, "modality:write", PolicyFlags { idempotent: false, audited: true, emits_cdc: true }, TxnParticipation::Atomic), "runtime-conditional: authority/query/events/capabilities are verified read snapshots; ingest/delete/cold/restore commit an encrypted state-backed MutationBatch"),
    ("ParseFile", make_policy(false, DurabilityDomain::None, "compute:parse", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::None), ""),
    ("ParseFiles", make_policy(false, DurabilityDomain::None, "compute:parse", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::None), ""),
    ("IndexRepository", make_policy(false, DurabilityDomain::None, "compute:parse", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::None), ""),
    ("ObserveScreen", make_policy(false, DurabilityDomain::None, "compute:vision", PolicyFlags { idempotent: false, audited: false, emits_cdc: false }, TxnParticipation::None), ""),
    ("AddEmbedding", make_policy(true, DurabilityDomain::GraphRedb, "node:write", PolicyFlags { idempotent: false, audited: true, emits_cdc: false }, TxnParticipation::Atomic), ""),
    ("SemanticSearch", make_policy(false, DurabilityDomain::None, "compute:semantic", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), ""),
    ("Discover", make_policy(false, DurabilityDomain::None, "compute:semantic", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), ""),
    #[cfg(feature = "quantum")]
    ("Quantum", make_policy(false, DurabilityDomain::None, "quantum:run", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::None), "self-routes before dispatch_graph_op like AnalyticsJob/Statechart, never reaches the graph tamper-evident audit chain; R5 override audit instead rides the response's PlannerDecision.audit trail into the agent-utilities :ToolCall/:QuantumJob provenance"),
    #[cfg(feature = "asr-native")]
    ("Asr", make_policy(false, DurabilityDomain::None, "asr:transcribe", PolicyFlags { idempotent: false, audited: false, emits_cdc: false }, TxnParticipation::None), "self-routes before dispatch_graph_op like Quantum/Viz; direct non-durable whisper-rs transcription, commits no asr.result.v1 (that governed commit is future worker/AU-orchestration work, W03/W06)"),
    #[cfg(feature = "viz")]
    ("Viz", make_policy(false, DurabilityDomain::None, "viz:render", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::None), "pure compute: resolves a fresh per-request ColumnStore and returns rendered bytes, no durable write (D-VZ-1 lanes V4/V6)"),
    #[cfg(feature = "tts-piper")]
    ("TtsSynthesize", make_policy(false, DurabilityDomain::None, "tts:synthesize", PolicyFlags { idempotent: false, audited: true, emits_cdc: false }, TxnParticipation::None), "pure compute: native Piper-ONNX synthesis runs inline and returns audio, no durable graph write (GOC-34, no CAS/rendition publication exists yet)"),
];
