//! eg-types — the wire protocol + graph data model. Bottom of the engine crate
//! DAG: it depends only on serde. Everything else (`eg-core`, `eg-compute`,
//! `eg-server`, the `epistemic-graph` facade) depends on it, never the reverse.
//!
//! It also owns the pure-data types that the protocol enum embeds but whose
//! behavior lives upstream: `wire` (finance/datascience DTOs) and `acl`
//! (`AgentRole`/`AgentIdentity`). The upstream modules re-export these
//! (`pub use eg_types::wire::Order;`) so their algorithm code is unchanged —
//! the data lives at the bottom of the DAG, the logic stays where it belongs.

pub mod acl;
pub mod change_envelope;
// CONCEPT:EG-KG.compute.uncertainty-values — probabilistic / uncertainty VALUE (distribution-valued
// properties). A stored value at the bottom of the DAG, NOT a wire `Op`.
pub mod distribution;
// CONCEPT:EG-KG.compute.epistemic-operations-protocol — strict shared DTOs for
// RequestContext, mutation/ingestion, work, artifact, query, job, and trace outcomes.
pub mod epistemic_operations;
pub mod epistemic_operations_manifest;
// CONCEPT:INT-P2-1 — the durable analytics-job plane's wire op (`JobOp`), gated
// `jobs`. Lives here (not in `eg-jobs`, which sits ABOVE eg-core in the DAG) for the
// SAME reason `acl::RbacAdminOp` does: `protocol::Method::AnalyticsJob` carries it
// over the wire, and `protocol` is bottom-of-DAG. Pure serde — no dep.
#[cfg(feature = "jobs")]
pub mod jobs;
// CONCEPT:INT-P2-2 — the native statechart engine's wire op (`StatechartOp`), gated
// `statechart`. Lives here (not in `eg-statechart`, which sits ABOVE this crate in the
// DAG) for the SAME reason `jobs` does: `protocol::Method::Statechart` carries it over
// the wire, and `protocol` is bottom-of-DAG. Pure serde — no dep.
#[cfg(feature = "knowledge-batch")]
pub mod knowledge_stream;
#[cfg(feature = "modality-serving")]
pub mod modality;
pub mod msgpack;
pub mod mutation_batch;
pub mod protocol;
pub mod row_predicate;
#[cfg(feature = "statechart")]
pub mod statechart;
pub mod types;
pub mod wire;

// CONCEPT:EG-KG.query.compound-predicate-decode — the serializable compound-WHERE predicate AST lives at the
// bottom of the DAG so `eg-core` can evaluate it; `eg-query` decodes SQL into it.
pub use row_predicate::{CmpOp, RowPredicate};

// CONCEPT:EG-KG.compute.uncertainty-values — surface the distribution VALUE at the crate root for callers.
pub use change_envelope::{
    BlobReference, ChangeCursor, ChangeEnvelope, ChangeEnvelopeCommit, ChangeEnvelopeRecord,
    ContentVersion, ContentVersionPosition, CursorPosition, EvidenceRecord, FeatureRecord,
    LineageRecord, MaterialOperation, PolicyRecord, PrivacyAttestation, CHANGE_ENVELOPE_VERSION,
};
pub use distribution::Distribution;
#[cfg(feature = "knowledge-batch")]
pub use knowledge_stream::{
    KnowledgeResultFamily, KnowledgeStreamBatchV1, KnowledgeStreamCursorV1,
    KnowledgeStreamProjection, KnowledgeStreamQuery, KnowledgeStreamRequestV1,
    KNOWLEDGE_STREAM_SCHEMA_VERSION,
};
#[cfg(feature = "modality-serving")]
pub use modality::{
    ServedModalityIngestItem, ServedModalityKind, ServedModalityOp, ServedNativePredicate,
    ServedSegmentKind,
};
pub use mutation_batch::{
    MutationBatch, MutationBatchCommit, MutationBatchRecord, MutationBatchStatus,
    MutationOperation, MutationOutboxIntent, MutationOutboxRecord, MutationRequestContext,
    MutationSurface, MutationVersionScope, MUTATION_BATCH_VERSION, NON_GRAPH_SOURCE_VERSION,
};
