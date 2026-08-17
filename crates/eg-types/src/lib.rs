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
// CONCEPT:EG-KG.compute.native-asr-whisper-provider — the native-ASR agent-facing wire op
// (`AsrOp`), gated `asr-native`. Lives here (not in `eg-asr-whisper`), for the
// SAME reason `quantum.rs` lives here rather than in `eg-quantum-core`: `eg-types`
// is the bottom-of-DAG crate every wire consumer already depends on, while the
// heavy provider crate sits above the facade. Pure serde — no dep on `eg-audio`/
// `eg-asr-whisper`.
#[cfg(feature = "asr-native")]
pub mod asr_wire;
// CONCEPT:AU-ORCH.scheduling — GOC-21 distributed CapacityCell/CapacityLease
// wire contract + pure fencing/admission algorithm (cross-host capacity
// authority for the AU fair scheduler). Unconditional, like `acl`/`jobs`.
pub mod capacity_lease;
pub mod change_envelope;
// GOC-03 — the cross-domain commit-descriptor/read-barrier currency shared by
// graph, modality, vector, blob/refcount, time-series, evidence, table/lake, and
// terminal-analytics-outcome participants. Deliberately a NEW module (not folded
// into `mutation_batch`): `MutationBatch`/`MutationProjectionCursor` remain the
// per-surface request/outbox envelope; `CommitDescriptorV1` is the one commit
// identity every domain's participant registers against. See
// `plans/graph-os-completion-program/lanes/GOC-03-cross-domain-commit-currency.md`.
pub mod commit_descriptor;
// CONCEPT:EG-KG.compute.uncertainty-values — probabilistic / uncertainty VALUE (distribution-valued
// properties). A stored value at the bottom of the DAG, NOT a wire `Op`.
pub mod distribution;
// CONCEPT:EG-KG.compute.epistemic-operations-protocol — strict shared DTOs for
// RequestContext, mutation/ingestion, work, artifact, query, job, and trace outcomes.
pub mod epistemic_operations;
// GOC-46 — hand-written wire DTOs (WorkItemClaimCapability*, CasWorkItemMetadata*)
// that the AU protocol catalog does not yet declare (needs generator support for a
// binary payload type). Kept OUT of `epistemic_operations` so that module can stay
// exactly generator-clean; see the module doc comment for the full finding.
pub mod epistemic_operations_ext;
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
// GOC-10 — canonical SQL table / lake catalog authority wire types
// (`CatalogEntryV1`/`TableSchemaVersionV1`/`PartitionManifestV1`/`LakeSnapshotV1`/
// `QualityReportRef`/`TableChangeV1`). Pure serde, no dep — unconditional like
// `acl`/`mutation_batch`, since it adds no `protocol::Method` variant (the durable
// store + REST projection that would carry these records over the wire is
// GOC-10-W03/W05, not yet implemented).
pub mod lake_catalog;
#[cfg(feature = "modality-serving")]
pub mod modality;
pub mod msgpack;
pub mod mutation_batch;
// GOC-20 — the atomic WorkItem outcome/provenance bundle and durable run-event
// wire contract (BUG-015). Deliberately a NEW module (not folded into
// `mutation_batch`), same rationale as `commit_descriptor` above: the native
// fusion of this bundle into `Method::CommitWorkItemResult`'s transaction is
// `mutation_batch.rs` work, currently blocked on the FO-001 five-lane
// (GOC-03/04/19/20/35) file-ownership collision. See
// `plans/graph-os-completion-program/decisions/GOC-20-atomic-outcome-provenance.md`.
pub mod outcome_bundle;
pub mod protocol;
// CONCEPT:EG-KG.compute.quantum-agent-api — the agent-facing quantum control-plane
// wire op (`QuantumOp`), gated `quantum`. Lives here (not in `eg-quantum-core`,
// which sits ABOVE this crate in the DAG) for the SAME reason `jobs`/`statechart`
// do: `protocol::Method::Quantum` carries it over the wire, and `protocol` is
// bottom-of-DAG. Pure serde — no dep on `eg-quantum-core`.
#[cfg(feature = "quantum")]
pub mod quantum;
pub mod row_predicate;
#[cfg(feature = "statechart")]
pub mod statechart;
pub mod types;
// GOC-19 — the WorkItem submission command-log admission core (tenant-scoped
// idempotency replay + per-authority fencing), built on GOC-03's
// `commit_descriptor::CommitDescriptorV1` currency. Unconditional, pure
// data/logic, NO `protocol::Method` variant yet — mirrors `lake_catalog`'s
// precedent of shipping ahead of its wire-protocol wiring. See
// `plans/graph-os-completion-program/lanes/GOC-19-atomic-workitem-command-log.md`.
pub mod work_item_command_log;
// D-VZ-1 (lanes V4 "engine integration" / V6 "graph-native marks") — the native
// visualization engine's wire op (`VizOp`), gated `viz`. Lives here (not in
// `eg-viz-core`, which sits in a separate small leaf DAG, not below eg-types) for
// the SAME reason `jobs`/`statechart` do: `protocol::Method::Viz` carries it over
// the wire, and `protocol` is bottom-of-DAG. Pure serde — no dep on
// eg-viz-core/eg-viz-columnstore/eg-viz-export; the handler
// (`src/server/handlers/viz.rs`, facade feature `viz-static-export`) is the one
// place that parses `VizRenderRequest::spec_json` into a real
// `eg_viz_core::ViewSpec` and resolves it.
#[cfg(feature = "viz")]
pub mod viz;
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
