//! Connector, modality, embedding, search, discovery, and native inference surfaces.

use crate::{DurabilityDomain, Stability, TxnParticipation};

use super::{make_policy, spec, PolicyFlags, PolicyRow, PYTHON};

pub(crate) const ROWS: &[PolicyRow] = &[
    ("SourceIngest", spec(make_policy(true, DurabilityDomain::GraphRedb, "source:ingest", PolicyFlags { idempotent: true, audited: true, emits_cdc: true }, TxnParticipation::Saga), PYTHON, Stability::Stable), "RF-ADR-009 native ingestion authority: tenant-bound typed Connector Manifest mapping resolution and idempotent raw-CAS admission precede one atomic ChangeEnvelope commit for mapped graph material, provenance, cursor and receipt; unknown mapping, tenant or authority fails closed; exact MCP catalog generation and digest binding makes the generated consumer contract replay-safe"),
    ("SourceIngestStatus", spec(make_policy(false, DurabilityDomain::None, "source:ingest", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), PYTHON, Stability::Stable), "read-only authoritative source-partition checkpoint and receipt identity for restart/failover CAS recovery; callers must not substitute local checkpoint authority"),
    #[cfg(feature = "modality-serving")]
    ("ServedModality", spec(make_policy(true, DurabilityDomain::GraphRedb, "modality:write", PolicyFlags { idempotent: false, audited: true, emits_cdc: true }, TxnParticipation::Atomic), PYTHON, Stability::Stable), "runtime-conditional: authority/query/events/capabilities are verified read snapshots; ingest/delete/cold/restore commit an encrypted state-backed MutationBatch"),
    ("ParseFile", spec(make_policy(false, DurabilityDomain::None, "compute:parse", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::None), PYTHON, Stability::Stable), ""),
    ("ParseFiles", spec(make_policy(false, DurabilityDomain::None, "compute:parse", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::None), PYTHON, Stability::Stable), ""),
    ("IndexRepository", spec(make_policy(false, DurabilityDomain::None, "compute:parse", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::None), PYTHON, Stability::Stable), ""),
    ("ObserveScreen", spec(make_policy(false, DurabilityDomain::None, "compute:vision", PolicyFlags { idempotent: false, audited: false, emits_cdc: false }, TxnParticipation::None), PYTHON, Stability::Stable), ""),
    ("AddEmbedding", spec(make_policy(true, DurabilityDomain::GraphRedb, "node:write", PolicyFlags { idempotent: false, audited: true, emits_cdc: false }, TxnParticipation::Atomic), PYTHON, Stability::Stable), ""),
    ("SemanticIndex", spec(make_policy(true, DurabilityDomain::SemanticIndexRedb, "semantic:binding-write", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Atomic), PYTHON, Stability::Stable), "RF-019's S1-S6 tiered ingestion queue. Runtime-conditional like the four agent layers, but over SIX authz actions rather than two: binding lifecycle is semantic:binding-write, S1 admission semantic:source-admit, subscribe/claim/release semantic:stage-claim, stage completion semantic:stage-complete, and the reads semantic:binding-read / semantic:stage-read. The row names the binding-write leg; SemanticIndexOp::authz_action is the authority for each operation"),
    ("SemanticSearch", spec(make_policy(false, DurabilityDomain::None, "compute:semantic", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), PYTHON, Stability::Stable), ""),
    ("Discover", spec(make_policy(false, DurabilityDomain::None, "compute:semantic", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), PYTHON, Stability::Stable), ""),
    #[cfg(feature = "quantum")]
    ("Quantum", spec(make_policy(false, DurabilityDomain::None, "quantum:run", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::None), PYTHON, Stability::Stable), "self-routes before dispatch_graph_op like AnalyticsJob/Statechart, never reaches the graph tamper-evident audit chain; R5 override audit instead rides the response's PlannerDecision.audit trail into the agent-utilities :ToolCall/:QuantumJob provenance"),
    #[cfg(feature = "asr-native")]
    ("Asr", spec(make_policy(false, DurabilityDomain::None, "asr:transcribe", PolicyFlags { idempotent: false, audited: false, emits_cdc: false }, TxnParticipation::None), PYTHON, Stability::Stable), "self-routes before dispatch_graph_op like Quantum/Viz; direct non-durable whisper-rs transcription, commits no asr.result.v1 (that governed commit is future worker/AU-orchestration work, W03/W06)"),
    #[cfg(feature = "viz")]
    ("Viz", spec(make_policy(false, DurabilityDomain::None, "viz:render", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::None), PYTHON, Stability::Stable), "pure compute: resolves a fresh per-request ColumnStore and returns rendered bytes, no durable write (D-VZ-1 lanes V4/V6)"),
];
