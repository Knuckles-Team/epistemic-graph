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
// RF-020 — durable Agent Library identity and mutation-context contracts. The
// physical table and transaction adapter live above this pure-data crate.
pub mod agent_component;
pub mod agent_graph;
pub mod agent_library;
pub mod agent_ontology;
pub mod agent_template;
// RF-020 — typed Agent Library delegation admission/result currency.
pub mod delegation;
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
// RF-RULING-004 — canonical serializable authority/replay evidence for the future
// eg-transaction admission boundary. This module grants no executable capability.
pub mod authority;
pub mod change_envelope;
// GOC-03 — the cross-domain commit-descriptor/read-barrier currency shared by
// graph, modality, vector, blob/refcount, time-series, evidence, table/lake, and
// terminal-analytics-outcome participants. Deliberately a NEW module (not folded
// into `mutation_batch`): `MutationBatch`/`MutationProjectionCursor` remain the
// per-surface request/outbox envelope; `CommitDescriptor` is the one commit
// identity every domain's participant registers against. See
// `plans/graph-os-completion-program/lanes/GOC-03-cross-domain-commit-currency.md`.
pub mod commit_descriptor;
// RF-RULING-004 — current-only parent/participant transaction coordination DTOs.
pub mod consensus;
// RF-RULING-004 — bounded scalar/collection primitives shared by kernel DTOs.
pub mod contract;
// RF-ADR-009 — connector MCP pack import: index, entries, framed digests, ops,
// results and the durable import record. Pure serde; the importer and its
// validation rules live in the server.
pub mod connector_pack;
// RF-ADR-010 — the Decide layer's wire contract: assembly requests and records,
// the decision policy, the statistical surface and the two admin jobs. Pure
// data; every algorithm lives above this crate.
pub mod decision;
// CONCEPT:EG-KG.sharding.semantic-embedding-store-backed — the pinned embedding-space
// identity (`EmbeddingSpaceRef`) + stamped-vector (`StampedVector`) currency shared
// by BOTH `eg-core::compute::semantic` backends, plus their two dimensionality
// ceilings. Pure serde, no dep — unconditional like `commit_descriptor`/
// `row_predicate`, since `eg-core` needs it on both sides of its backend `#[cfg]`
// split.
pub mod embedding;
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
pub mod ingestion_wire;
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
// (`CatalogEntry`/`TableSchemaVersion`/`PartitionManifest`/`LakeSnapshot`/
// `QualityReportRef`/`TableChange`). Pure serde, no dep — unconditional like
// `acl`/`mutation_batch`, since it adds no `protocol::Method` variant (the durable
// store + REST projection that would carry these records over the wire is
// GOC-10-W03/W05, not yet implemented).
pub mod lake_catalog;
pub mod messaging_wire;
#[cfg(feature = "modality-serving")]
pub mod modality;
pub mod msgpack;
// X9 — keyed schema sources (shapes + ontology) on one request graph, and the
// composed digest that identifies the set in force.
pub mod graph_schema;
// X10 — the operator view of one owner's mutation outbox: standing, dead
// letters and bounded re-delivery.
pub mod mutation_outbox;
// RF-RULING-004 — untrusted serialized mutation request/evidence DTOs only.
// Executable admitted plans/tokens are private to the future eg-transaction kernel.
pub mod mutation;
pub mod mutation_batch;
// AU wire-first native control-plane operations: bounded capacity leases and
// WorkItem admission/submission.  This module is hand-written until the AU
// protocol catalog generator can express the bounded maps and native enums.
pub mod native_control;
// GOC-20 — the atomic WorkItem outcome/provenance bundle and durable run-event
// wire contract (BUG-015). Deliberately a NEW module (not folded into
// `mutation_batch`), same rationale as `commit_descriptor` above: the native
// fusion of this bundle into `Method::CommitWorkItemResult`'s transaction is
// `mutation_batch.rs` work, currently blocked on the FO-001 five-lane
// (GOC-03/04/19/20/35) file-ownership collision. See
// `plans/graph-os-completion-program/decisions/GOC-20-atomic-outcome-provenance.md`.
pub mod outcome_bundle;
// RF-RULING-004 — mutation-kernel-owned outbox intent, delivery, and cursor DTOs.
pub mod outbox;
pub mod protocol;
// The general bounded 0-1 integer programme (`Method::Solve`): model, config,
// certificate and exact scalars. The search and the verifier live in
// `eg-compute`, which re-exports these so there is one definition of each.
pub mod solve;
// CONCEPT:EG-KG.compute.quantum-agent-api — the agent-facing quantum control-plane
// wire op (`QuantumOp`), gated `quantum`. Lives here (not in `eg-quantum-core`,
// which sits ABOVE this crate in the DAG) for the SAME reason `jobs`/`statechart`
// do: `protocol::Method::Quantum` carries it over the wire, and `protocol` is
// bottom-of-DAG. Pure serde — no dep on `eg-quantum-core`.
#[cfg(feature = "quantum")]
pub mod quantum;
// F3: RDF load/update/rule/shape-validation report bodies (eg-rdf/eg-shacl/eg-shex results).
pub mod rdf_report;
pub mod row_predicate;
// F3: the typed result contract -- one marker per method result, compile-checked at every
// handler site and walked by `eg-capabilities` to publish the result schemas.
pub mod result_contract;
// RF-019 — the sole transport-neutral semantic-index contract.  Runtime
// storage, queues, handlers, and surface projections live in crates above this
// bottom-of-DAG owner and must consume these exact operation and identity DTOs.
pub mod semantic_index;
// RF-ADR-009 — raw connector records, exact manifest mapping reference, cursor
// CAS and the terminal native ingestion receipt. Runtime authority lives in the
// server; these are bounded pure wire types.
pub mod source_ingestion;
#[cfg(feature = "statechart")]
pub mod statechart;
pub mod storage_wire;
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;
pub mod types;
// Result bodies of the `compute` contract domain, declared by
// `result_contract::compute`; gated per compute family like `wire`.
pub mod compute_result;
// GOC-19 — the WorkItem submission command-log admission core (tenant-scoped
// idempotency replay + per-authority fencing), built on GOC-03's
// `commit_descriptor::CommitDescriptor` currency. Unconditional, pure
// data/logic; the native protocol/storage adapter now lives in
// `native_control`, `server::mutation_batch`, and `redb_store`. See
// `plans/graph-os-completion-program/lanes/GOC-19-atomic-workitem-command-log.md`.
pub mod work_item_command_log;
// EH-219 — typed, tenant-bound WorkItem reads (`GetWorkItem`/`ListWorkItems`):
// the caller's row view, its three-bound page scan, and its cursor family.
pub mod work_item_read;
// The opaque tenant-bound keyset cursor shared by every paged read
// (`AgentComponent.Search`, `ListWorkItems`).
pub mod tenant_cursor;
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
// RF-ADR-009 D18 — EG-owned versioned source change sets and append-only
// write-back/reconciliation receipts. Connector transports remain in the SDK.
pub mod write_back;

// CONCEPT:EG-KG.query.compound-predicate-decode — the serializable compound-WHERE predicate AST lives at the
// bottom of the DAG so `eg-core` can evaluate it; `eg-query` decodes SQL into it.
pub use row_predicate::{CmpOp, RowPredicate};

// CONCEPT:EG-KG.compute.uncertainty-values — surface the distribution VALUE at the crate root for callers.
pub use agent_library::{
    AgentLibraryCommittedResult, AgentLibraryEntry, AgentLibraryEntryDraft, AgentLibraryLifecycle,
    AgentLibraryMutationContext, AgentLibraryMutationKind, AgentLibraryOp, AgentLibraryOutboxEvent,
    AgentLibraryPublishRequest, AgentLibraryRetireRequest, AgentLibraryStatusRequest,
    AgentLibraryWriteResult, AGENT_LIBRARY_DEFINITION_DIGEST_DOMAIN,
    AGENT_LIBRARY_ENTRY_SCHEMA_VERSION, AGENT_LIBRARY_OUTBOX_SCHEMA_VERSION,
    AGENT_LIBRARY_RESULT_SCHEMA_ID, AGENT_LIBRARY_RESULT_SCHEMA_VERSION,
};
pub use change_envelope::{
    BlobReference, ChangeCursor, ChangeEnvelope, ChangeEnvelopeCommit, ChangeEnvelopeRecord,
    ContentVersion, ContentVersionPosition, CursorPosition, EvidenceRecord, FeatureRecord,
    LineageRecord, MaterialOperation, PolicyRecord, PrivacyAttestation, CHANGE_ENVELOPE_VERSION,
};
pub use delegation::{
    AgentLibraryEntryRef, KgDelegateDecision, KgDelegateRequest, KgDelegateResult,
    KgDelegateSchemaVersion,
};
pub use distribution::Distribution;
pub use embedding::{
    EmbeddingSpaceRef, StampedVector, MAX_EMBEDDING_DIMENSIONS, MAX_MAINTAINED_ANN_DIMENSIONS,
};
#[cfg(feature = "knowledge-batch")]
pub use knowledge_stream::{
    KnowledgeResultFamily, KnowledgeStreamBatch, KnowledgeStreamCursor, KnowledgeStreamProjection,
    KnowledgeStreamQuery, KnowledgeStreamRequest, KNOWLEDGE_STREAM_SCHEMA_VERSION,
};
#[cfg(feature = "modality-serving")]
pub use modality::{
    ServedModalityIngestItem, ServedModalityKind, ServedModalityOp, ServedNativePredicate,
    ServedSegmentKind,
};
// MutationBatch v1 replaces the (version_scope, source_graph_version) pair with the
// closed `CommittedVersion` enum, so a scope without its matching version is no longer
// representable. `MutationVersionScope` and `NON_GRAPH_SOURCE_VERSION` are therefore
// not re-exported: they have no v1 meaning, and keeping an alias would be exactly the
// compatibility seam RF-ADR-001 forbids.
pub use mutation_batch::{
    CommittedVersion, IncarnationId, LogicalName, MutationBatch, MutationBatchCommit,
    MutationBatchRecord, MutationBatchStatus, MutationEnvelope, MutationOperation,
    MutationOutboxIntent, MutationOutboxLease, MutationOutboxRecord, MutationProjectionCursor,
    MutationScope, MutationScopeIdentity, MutationStateDescriptor, MutationSurface, ScopeTenantId,
    VersionExpectation, MUTATION_BATCH_VERSION,
};
