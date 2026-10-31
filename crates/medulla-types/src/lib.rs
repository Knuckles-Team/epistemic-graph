//! medulla-types — the wire protocol + graph data model. Bottom of the engine crate
//! DAG: it depends only on serde. Everything else (`medulla-core`, `medulla-compute`,
//! `medulla-server`, the `medulla` facade) depends on it, never the reverse.
//!
//! It also owns the pure-data types that the protocol enum embeds but whose
//! behavior lives upstream: `wire` (finance/datascience DTOs) and `acl`
//! (`AgentRole`/`AgentIdentity`). The upstream modules re-export these
//! (`pub use medulla_types::wire::Order;`) so their algorithm code is unchanged —
//! the data lives at the bottom of the DAG, the logic stays where it belongs.

pub mod acl;
// RF-020 — durable Agent Library identity and mutation-context contracts. The
// physical table and transaction adapter live above this pure-data crate.
pub mod agent_component;
pub mod agent_graph;
pub mod agent_library;
pub mod agent_ontology;
pub mod agent_template;
// MEDULLA-CQ-ORCH-R010.1 — pure data model for graph-derived architecture views.
pub mod architecture_view;
// MEDULLA-UNIFIED-DATA-PLANE-R012.1 — typed model for governed attached-source write-back.
pub mod attached_source_governance;
// MEDULLA-UNIFIED-DATA-PLANE-R014.1 — typed model distinguishing the MySQL and
// MariaDB attached-source dialect entries. See module docs.
pub mod attached_source_dialect;
// MEDULLA-UNIFIED-DATA-PLANE-R014.2.1 — MySQL/MariaDB adapter skeleton: typed
// connection config → sqlx-shaped driver config, with a connect refusal
// path that needs no live database. Binlog/GTID capture and the per-engine
// conformance entries are R014.2.2+.
pub mod mysql_mariadb_connection;
// RF-020 — typed Agent Library delegation admission/result currency.
pub mod delegation;
// CONCEPT:MEDULLA-KG.compute.native-asr-whisper-provider — the native-ASR agent-facing wire op
// (`AsrOp`), gated `asr-native`. Lives here (not in `medulla-asr-whisper`), for the
// SAME reason `quantum.rs` lives here rather than in `medulla-quantum-core`: `medulla-types`
// is the bottom-of-DAG crate every wire consumer already depends on, while the
// heavy provider crate sits above the facade. Pure serde — no dep on `medulla-audio`/
// `medulla-asr-whisper`.
#[cfg(feature = "asr-native")]
pub mod asr_wire;
// CONCEPT:AU-ORCH.scheduling — GOC-21 distributed CapacityCell/CapacityLease
// wire contract + pure fencing/admission algorithm (cross-host capacity
// authority for the AU fair scheduler). Unconditional, like `acl`/`jobs`.
pub mod capacity_lease;
// MEDULLA-DURABLE-KERNEL-R076.1 — authoritative scheduler policy data and validation.
pub mod scheduler_config;
// MEDULLA-INGEST-WORKER-R001.1/.2/.3/.7/.8 — ingest job envelope, lanes, partition key, retry.
pub mod ingest_job;
// MEDULLA-INGEST-WORKER-R002.1/.2 — induced ontology proposals and admission scores.
pub mod ontology_proposal;
// MEDULLA-INGEST-WORKER-R003.1 — typed knowledge atoms with mandatory provenance.
pub mod knowledge_atom;
// MEDULLA-INGEST-WORKER-R701.1 — extracted facts, candidate claims and evidence spans.
pub mod extracted_fact;
// MEDULLA-INGEST-WORKER-R900.1/R902.1/R903.1/R904.1 — source governance records.
pub mod source_governance;
// MEDULLA-INGEST-WORKER-R900.4/R901.4/R902.4/R903.4/R904.4/R701.2/R701.4/R002.4/R003.4 — pure ingest refusal rules.
pub mod ingest_rules;
// MEDULLA-INGEST-WORKER-R003.7 — normalized documents and accepted/quarantined outcomes.
pub mod source_document;
// MEDULLA-INGEST-WORKER-R003.13 — source transcript threads and messages.
pub mod source_transcript;
// MEDULLA-TYPED-PACKS-R803.1 — typed sensitive-data detector and detection records.
pub mod sensitive_detect;
// MEDULLA-TYPED-PACKS-R804.1 — ontology revision difference and impact records.
pub mod ontology_diff;
// RF-RULING-004 — canonical serializable authority/replay evidence for the future
// medulla-transaction admission boundary. This module grants no executable capability.
pub mod authority;
// MEDULLA-SRC-AUTH-R007.1 — typed audit entries for source selection evidence.
pub mod authority_audit;
// MEDULLA-REPO-INGEST-R700 — code-health findings from a bounded reachability sweep.
pub mod change_envelope;
pub mod code_health;
// MEDULLA-DURABLE-KERNEL-R087.1 — typed semantic changeset-review model. The
// authenticated producer/consumer route is a later R087.2 concern.
pub mod changeset_review;
// MEDULLA-UNIFIED-DATA-PLANE-R019.1 — typed inferred-shape catalog for an attached
// MongoDB/DocumentDB source. Pure data; the native-driver connection and
// change-stream capture are later children.
pub mod document_source_catalog;
// MEDULLA-UNIFIED-DATA-PLANE-R019.3.1 — typed conformance-entry constructor for the
// MongoDB/DocumentDB adapter, over the generic R022.1 report entry. Pure
// data; actually running the comparison is a later child.
pub mod document_conformance;
// MEDULLA-UNIFIED-DATA-PLANE-R020.1 — typed federated warehouse/lake source-kind
// model (Snowflake, BigQuery, DuckDB, Iceberg) and its per-kind required-field
// refusal. Pure data; query pushdown and the conformance entry are later
// children.
pub mod warehouse_federation;
// MEDULLA-UNIFIED-DATA-PLANE-R020.3.1 — typed conformance-entry constructor for the
// Snowflake/BigQuery/DuckDB/Iceberg warehouse adapters, over the generic
// R022.1 report entry. Pure data; actually running the comparison is a later
// child.
pub mod warehouse_conformance;
// MEDULLA-UNIFIED-DATA-PLANE-R021.1 — typed Debezium Kafka change-event envelope
// shape and its op-code/before-after refusal rules. Pure data; the Kafka
// consumer and the real ChangeEnvelope conversion are later children.
pub mod debezium_bridge;
// MEDULLA-UNIFIED-DATA-PLANE-R022.1 — typed per-dialect conformance report entry
// and deviation shape, refusing an auto-suppressed (unreviewed) deviation.
// Pure data; running the containerized matrix is a later child.
pub mod dialect_conformance;
// MEDULLA-UNIFIED-DATA-PLANE-R022.3.1 — pure comparison of a change-capture
// replay's resulting row state against a source snapshot, building a
// `dialect_conformance::DialectConformanceEntry`. No live replay stream or
// source connection; running the comparison against a real replay is a
// later child.
pub mod replay_conformance;
// MEDULLA-UNIFIED-DATA-PLANE-R023.1 — typed shared CloudNativePG/MariaDB cluster
// group and per-application role-set shape, refusing a Postgres group
// without logical wal_level and an aliased application role. Pure data;
// operator wiring, PITR scheduling and the restore drill are later children.
pub mod shared_db_platform;
// MEDULLA-UNIFIED-DATA-PLANE-R023.2.1 — typed PITR schedule contract to object
// storage, refusing an archiving mode mismatched to its engine, zero
// retention, or a blank cron/destination. No operator wired; installing the
// real schedule is a later child.
pub mod pitr_schedule;
// MEDULLA-UNIFIED-DATA-PLANE-R023.3.1 — typed restore-drill result, refusing a
// `data_matched` flag that disagrees with the actual pre/post-restore
// digest comparison. No restore is run yet; automating the real drill is a
// later child.
pub mod restore_drill;
// MEDULLA-UNIFIED-DATA-PLANE-R024.1 — typed one-at-a-time application admission
// stage machine, refusing to retire an ingest connector before parity is
// confirmed. Pure data; the admission runner, rollback path, and the
// per-application admission test are later children.
pub mod platform_admission;
// MEDULLA-UNIFIED-DATA-PLANE-R025.1 — typed pgrx companion-extension spike
// decision record, refusing an outcome recorded without evidence for every
// declared scope area. Pure data; running the spike and the ADR review are
// later children.
pub mod pgrx_spike;
// MEDULLA-UNIFIED-DATA-PLANE-R025.3.1 — adapter skeleton reviewing a recorded
// pgrx spike decision into an approved ADR record, refusing an invalid
// decision, an empty reviewer, or an empty ADR title. Filing the ADR
// document itself is a later child.
pub mod pgrx_spike_review;
// MEDULLA-UNIFIED-DATA-PLANE-R026.1 — typed Gramps native-hosting pilot phase
// gate (P0-P5), refusing a wrong or unreviewed exit artifact and skipping
// ahead of an un-exited earlier phase. Pure data; running each phase is a
// later child.
pub mod gramps_pilot_phase;
// MEDULLA-UNIFIED-DATA-PLANE-R026.2.1 — adapter skeleton mapping a typed P0
// baseline-content-digest record to the P0 phase exit artifact, refusing a
// zero record count or an empty digest. The real baseline capture is a
// later child.
pub mod gramps_baseline_digest;
// MEDULLA-UNIFIED-DATA-PLANE-R026.3.1 — adapter skeleton mapping a typed P3
// proven-gap-fix record (failing test before, passing test after) to the P3
// phase exit artifact, refusing an identical before/after test name. The
// real restore verification (P4) and schema-understanding demonstration
// (P5) are later children.
pub mod gramps_gap_fix;
// MEDULLA-UNIFIED-DATA-PLANE-R033.1 — typed Gramps production-cutover go-ahead
// (pilot phase P6), reusing gramps_pilot_phase's P5 exit and refusing a
// go-ahead that cites the wrong phase, an invalid P5 exit, no approver, or
// a fallback window shorter than the required 30 days. Pure data; the real
// cutover (Postgres replica, nightly export, rehearsed failover) is a
// later child.
pub mod gramps_cutover_gate;
// MEDULLA-UNIFIED-DATA-PLANE-R038.1 — typed metadata contract and acceleration
// policy, refusing a hot-subset predicate declared without source copy
// capability and falling a missing freshness watermark back to a live
// read. Pure data; real discovery, the copy pipeline, and EXPLAIN's
// per-fragment reporting are later children.
pub mod metadata_acceleration;
// MEDULLA-UNIFIED-DATA-PLANE-R040.1/.2 — per-modality embedding promotion gate.
pub mod embedding_promotion;
// MEDULLA-UNIFIED-DATA-PLANE-R038.6.1 — read-only property-graph view over mappings.
pub mod virtual_graph_view;
// MEDULLA-UNIFIED-DATA-PLANE-R761.1 — column profile record.
pub mod profile;
// MEDULLA-UNIFIED-DATA-PLANE-R762.1 — quality rules and scores.
pub mod quality;
// MEDULLA-UNIFIED-DATA-PLANE-R702 and R763.1 — dataset and column lineage.
pub mod lineage;
// MEDULLA-UNIFIED-DATA-PLANE-R764.1/.2 — asset usage and catalog search hits.
pub mod asset_usage;
// MEDULLA-ENGINE-PARITY-R020.1 — relational engine capability report.
pub mod engine_capabilities;
// MEDULLA-UNIFIED-DATA-PLANE-R700 — governed action specs and runs.
pub mod action_spec;
// MEDULLA-UNIFIED-DATA-PLANE-R701 — standards, cohorts, drift and consolidation.
pub mod standards_drift;
// MEDULLA-UNIFIED-DATA-PLANE-R030.2/.3/.700 — blocking, Fellegi-Sunter scoring,
// approval and identity evidence over entity link proposals.
pub mod entity_resolution;
// MEDULLA-UNIFIED-DATA-PLANE-R750.750-752 — run audit rows, tool calls and the
// scoped conversation-history step store.
pub mod run_history;
// MEDULLA-UNIFIED-DATA-PLANE-R027.1 — typed Postgres compatibility-feature
// inventory and fix-proof shape, refusing a fix recorded without a complete
// captured-traffic/failing-test/passing-test proof. Pure data;
// implementing each proven feature is a later child.
pub mod pg_compat_feature;
// MEDULLA-UNIFIED-DATA-PLANE-R028.1 — typed Postgres-equivalence differential
// deviation baseline, refusing a baseline update that drops a deviation
// without an explicit resolution record (no self-updating baseline). Pure
// data; running the regression/SQLancer/captured-traffic corpora is a
// later child.
pub mod pg_diff_baseline;
// MEDULLA-UNIFIED-DATA-PLANE-R029.1 — typed schema_context answer shape and
// catalog, refusing to answer for a table absent from the catalog. Pure
// data; wiring the real query entry point is a later child.
pub mod schema_context;
// MEDULLA-FEDERATED-QUERY-R058.2.2.1 — the disclosed NL query result (executed UQL + rows).
pub mod nl_query;
// MEDULLA-UNIFIED-DATA-PLANE-R030.1 — typed cross-application entity-link
// proposal, refusing an owl:sameAs assertion before approval or for a
// non-Match classification. Pure data; blocking, field-similarity scoring
// and the real Fellegi-Sunter model are later children.
pub mod entity_link_proposal;
// MEDULLA-CQ-XSRC-R006.1/R006.4 — the typed supply-chain impact report (one
// report: impact per service, the items behind each, the sources used and
// why, and typed abstentions) and its pure, deterministic assembly from the
// planned query's rows and premises.
pub mod supply_chain_report;
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
// MEDULLA-TYPED-PACKS-R097.1 — one typed, versioned domain vocabulary (classes,
// enumerations, properties, alignment to an upper ontology), published through
// the pack import boundary above `connector_pack`. Pure data; validation lives
// here, import wiring lands in a later child requirement.
pub mod domain_pack;
// RF-ADR-010 — the Decide layer's wire contract: assembly requests and records,
// the decision policy, the statistical surface and the two admin jobs. Pure
// data; every algorithm lives above this crate.
pub mod decision;
// EH-345 — the fleet catalog: discovery observations and operator overrides
// beside the server registry, and the typed projection that joins them with
// connector-pack `AgentComponent`s. Pure serde; the handler lives in the server.
pub mod fleet_catalog;
// MEDULLA-FEDERATED-QUERY-R722.3 — host-driven bounded foreign-read lifecycle DTOs.
pub mod foreign_read;
// MEDULLA-DURABLE-KERNEL-R032 — the native primary-key fast-path routing decision
// (pgwire/MySQL/MSSQL point operations that bypass DataFusion planning).
// Pure serde, no dep.
pub mod fast_path;
// MEDULLA-DURABLE-KERNEL-R033 — the hot-store engine evaluation's typed model
// (redb/fjall/RocksDB candidates, workload profiles, validated benchmark
// results, winner selection). Pure serde, no dep.
pub mod hot_store_engine;
// MEDULLA-DURABLE-KERNEL-R034 — cross-store atomicity's typed transaction-scope
// model: a single-store scope structurally can never map to the cross-store
// commit path. Pure serde, no dep.
pub mod cross_store_scope;
// MEDULLA-DURABLE-KERNEL-R035 — the PostgreSQL wire protocol's required
// operational surfaces and a fail-closed compatibility matrix. Pure serde,
// no dep.
pub mod pg_operational_surface;
// MEDULLA-DURABLE-KERNEL-R039 — in-memory structure/atomic-primitive kinds and a
// durability declaration that always names redb as sole durability
// authority. Pure serde, no dep.
pub mod atomic_primitive;
// MEDULLA-DURABLE-KERNEL-R040 — pipelined RESP/pgwire-extended batch admission:
// one admission maps to exactly one commit with order-preserving replies.
// Pure serde, no dep.
pub mod pipeline_batch;
// EH-346/EH-347 — capture-first open-weight policy evolution: the capability,
// capture, model-policy version, training-run and evaluation records EG owns.
// Pure serde; EG records and relates, it never trains.
pub mod policy_evolution;
// MEDULLA-UNIFIED-DATA-PLANE-R013.1 — Postgres attached-source adapter config and
// type-mapping typed model (connect/catalog/capture wiring is a later slice).
pub mod postgres_adapter;
// CONCEPT:MEDULLA-KG.sharding.semantic-embedding-store-backed — the pinned embedding-space
// identity (`EmbeddingSpaceRef`) + stamped-vector (`StampedVector`) currency shared
// by BOTH `medulla-core::compute::semantic` backends, plus their two dimensionality
// ceilings. Pure serde, no dep — unconditional like `commit_descriptor`/
// `row_predicate`, since `medulla-core` needs it on both sides of its backend `#[cfg]`
// split.
pub mod embedding;
// MEDULLA-FEDERATED-QUERY-R083.1 — typed prior/retrieved candidates and the closed
// arbitration verdict value; support classification and policy live above.
pub mod retrieval_arbitration;
// CONCEPT:MEDULLA-KG.compute.uncertainty-values — probabilistic / uncertainty VALUE (distribution-valued
// properties). A stored value at the bottom of the DAG, NOT a wire `Op`.
pub mod distribution;
// MEDULLA-DURABLE-KERNEL-R038 — the declared durability class (ephemeral/async/sync)
// a KV namespace or table carries. Pure serde, no dep.
pub mod durability_class;
// MEDULLA-DURABLE-KERNEL-R084.1 — typed optimistic-write conflict and the closed
// set of scope-level merge strategies. Resolution and served wiring live above
// this pure-data crate.
pub mod write_conflict;
pub use write_conflict::{MergeStrategy, WriteConflict};
// MEDULLA-DURABLE-KERNEL-R037 — the workload classification (point/structure/sql)
// every wire listener routes an operation by. Pure, no dep.
pub mod workload_class;

// MEDULLA-DURABLE-KERNEL-R074.1 — pure worker registry, registration validation,
// and deterministic aggregate capabilities. No RPC or persistence ownership.
pub mod worker_registry;

// MEDULLA-DURABLE-KERNEL-R089.1 — per-step verification verdict. Pure, no dep.
pub mod step_verdict;
// MEDULLA-DURABLE-KERNEL-R088.1 — rule-output correction with two-principal approval.
pub mod rule_correction;
// MEDULLA-DURABLE-KERNEL-R851.1 — the A2A peer record. Pure serde.
pub mod agent_peer;
// MEDULLA-DURABLE-KERNEL-R852.1/.2/.4 — health anomalies and correlated incidents.
pub mod health_incident;
// MEDULLA-DURABLE-KERNEL-R700.1 — the retention-cleanup policy shape.
pub mod retention_policy;
// MEDULLA-DECISION-ENGINE-R617.1 — the engine-held memory records: the conversation
// record a chat log parses into, the layered project rules, the memory
// retention policy and the context budget. Pure data; parsing, pruning and
// the covering-set optimizer are later children.
pub mod memory;
// MEDULLA-DURABLE-KERNEL-R094.1 — hierarchical concept-ID grammar and path claims.
pub mod concept_id;

// MEDULLA-DURABLE-KERNEL-R041 — the SQL plan cache key (statement digest + schema
// version) and its lookup decision. Pure, no dep beyond `contract::Digest256`.
pub mod plan_cache_key;
// MEDULLA-DURABLE-KERNEL-R042 — a published benchmark report's typed superiority
// claim and the refusal for an unpublished-configuration or non-passing one.
pub mod benchmark_claim;
// MEDULLA-DURABLE-KERNEL-R011 — the required pre-merge test-module category
// (raft/backup/persistence) and a validated declaration within one.
pub mod pre_merge_test_module;
// MEDULLA-DURABLE-KERNEL-R036 — the mirror-sink side: sink kind (fan-out/postgres)
// and durable per-mirror cursor with a non-empty-position guard. Pure serde, no dep.
pub mod mirror_sink;
// CONCEPT:MEDULLA-KG.compute.epistemic-operations-protocol — strict shared DTOs for
// RequestContext, mutation/ingestion, work, artifact, query, job, and trace outcomes.
pub mod epistemic_operations;
// GOC-46 — hand-written wire DTOs (WorkItemClaimCapability*, CasWorkItemMetadata*)
// that the AU protocol catalog does not yet declare (needs generator support for a
// binary payload type). Kept OUT of `epistemic_operations` so that module can stay
// exactly generator-clean; see the module doc comment for the full finding.
pub mod epistemic_operations_ext;
pub mod epistemic_operations_manifest;
// EH-194 — typed `Claim`/`Evidence` graph nodes: the one owner of the shape every
// epistemic writer stores and every reader decodes. Pure serde, unconditional.
pub mod epistemic_node;
pub mod ingestion_wire;
// CONCEPT:INT-P2-1 — the durable analytics-job plane's wire op (`JobOp`), gated
// `jobs`. Lives here (not in `medulla-jobs`, which sits ABOVE medulla-core in the DAG) for the
// SAME reason `acl::RbacAdminOp` does: `protocol::Method::AnalyticsJob` carries it
// over the wire, and `protocol` is bottom-of-DAG. Pure serde — no dep.
#[cfg(feature = "jobs")]
pub mod jobs;
// CONCEPT:INT-P2-2 — the native statechart engine's wire op (`StatechartOp`), gated
// `statechart`. Lives here (not in `medulla-statechart`, which sits ABOVE this crate in the
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
/// User-managed indexes and edge-native search on the wire (EH-351 / EH-352).
pub mod managed_index;
// X10 — the operator view of one owner's mutation outbox: standing, dead
// letters and bounded re-delivery.
pub mod mutation_outbox;
// RF-RULING-004 — untrusted serialized mutation request/evidence DTOs only.
// Executable admitted plans/tokens are private to the future medulla-transaction kernel.
pub mod mutation;
pub mod mutation_batch;
// MEDULLA-ORCH-VOCAB-R801/R802 — the versioned harness-component vocabulary and the
// improvement library beside it: typed model + pure rules. The durable write
// (ChangeEnvelope/AddNode) and the served reads live upstream.
pub mod orchestration;
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
// MEDULLA-FINANCE-PRIMITIVES-R011.1 — the typed finance alert envelope staged for
// the finance outbox topic family; net-new slice (model + validation +
// refusal tests only, no outbox-append wiring yet).
#[cfg(feature = "finance")]
pub mod finance_event_envelope;
pub mod protocol;
// The general bounded 0-1 integer programme (`Method::Solve`): model, config,
// certificate and exact scalars. The search and the verifier live in
// `medulla-compute`, which re-exports these so there is one definition of each.
pub mod solve;
// CONCEPT:MEDULLA-KG.compute.quantum-agent-api — the agent-facing quantum control-plane
// wire op (`QuantumOp`), gated `quantum`. Lives here (not in `medulla-quantum-core`,
// which sits ABOVE this crate in the DAG) for the SAME reason `jobs`/`statechart`
// do: `protocol::Method::Quantum` carries it over the wire, and `protocol` is
// bottom-of-DAG. Pure serde — no dep on `medulla-quantum-core`.
#[cfg(feature = "quantum")]
pub mod quantum;
// Typed RDF input and the ontology inspection view, so orchestration callers
// never build or parse RDF text themselves.
pub mod ontology_inspection;
// F3: RDF load/update/rule/shape-validation report bodies (medulla-rdf/medulla-shacl/medulla-shex results).
pub mod rdf_report;
pub mod row_predicate;
// MEDULLA-FEDERATED-QUERY-R082.1 — typed calibration, bounded limits and closed
// stop-verdict vocabulary; decision precedence lives in medulla-plan.
pub mod retrieval_stop;
pub mod runtime_limit;
// EH-558 — sealed record classes: content-addressed rows generic writes may not change.
pub mod sealed_record;
// EH-522 — the series-expression algebra (`DERIVE` columns, derived series). Pure serde.
pub mod series_expr;
// F3: the typed result contract -- one marker per method result, compile-checked at every
// handler site and walked by `medulla-capabilities` to publish the result schemas.
pub mod result_contract;
// RF-019 — the sole transport-neutral semantic-index contract.  Runtime
// storage, queues, handlers, and surface projections live in crates above this
// bottom-of-DAG owner and must consume these exact operation and identity DTOs.
pub mod semantic_index;
// MEDULLA-SDLC — release, deployment, defect, derived status and lifecycle trace records.
pub mod lifecycle;
// MEDULLA-SCM-R002 — the source changeset record and its structured trailers.
pub mod source_changeset;
// MEDULLA-SCM-R013 — graph-native symbol edits.
pub mod source_symbol_edit;
// RF-ADR-009 — raw connector records, exact manifest mapping reference, cursor
// CAS and the terminal native ingestion receipt. Runtime authority lives in the
// server; these are bounded pure wire types.
pub mod source_ingestion;
#[cfg(feature = "statechart")]
pub mod statechart;
// EH-408 / EH-409 — the receipt of a served telemetry derivation.
pub mod storage_wire;
pub mod telemetry_derive;
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;
pub mod types;
// Result bodies of the `compute` contract domain, declared by
// `result_contract::compute`; gated per compute family like `wire`.
pub mod compute_result;
pub mod current_plan;
pub mod run_checkpoint;
// GOC-19 — the WorkItem submission command-log admission core (tenant-scoped
// idempotency replay + per-authority fencing), built on GOC-03's
// `commit_descriptor::CommitDescriptor` currency. Unconditional, pure
// data/logic; the native protocol/storage adapter now lives in
// `native_control`, `server::mutation_batch`, and `redb_store`. See
// `plans/graph-os-completion-program/lanes/GOC-19-atomic-workitem-command-log.md`.
pub mod work_item_command_log;
// EH-219 — typed, tenant-bound WorkItem reads (`GetWorkItem`/`ListWorkItems`):
// the caller's row view, its three-bound page scan, and its cursor family.
pub mod run_completion;
pub mod work_item_read;
// MEDULLA-IDENTITY-R610 — tenant parent chains, secured reads and ownership claims.
pub mod tenancy;
// The opaque tenant-bound keyset cursor shared by every paged read
// (`AgentComponent.Search`, `ListWorkItems`).
pub mod tenant_cursor;
// The three-bound keyset page shared by the tenant-bound native listings.
pub mod keyset_page;
// graph-os MEDULLA-2 — native control leases: a tenant-bound, time-boxed grant with
// a one-way active -> revoked|expired lifecycle (not a capacity admission).
pub mod control_lease;
pub mod host_start;
// The engine-owned identity store: principals, credentials,
// sessions, roles/groups/IdPs, the auth-mode singleton and scope classes.
pub mod identity;
// EH-400 — declared freshness: `eg:volatilityClass`, per-class invalidation events and
// foreign-source watermarks (the `FreshnessFeed` wire shapes).
pub mod freshness;
// MEDULLA-TYPED-PACKS-R802.1/R805.1 — governance record models: stewardship and
// data products. Pure data and field validation; served wiring lands in later children.
pub mod data_product;
pub mod stewardship;
// EH-348 — the graph-driven work market: the canonical `:Gap` paired with its
// one native WorkItem, the versioned derived `WorkOffer`, and the deterministic
// legal-offer ranking stage the `Decide` layer runs.
pub mod work_market;
// EH-404 -- just-in-time RBAC elevation: an `rbac.elevation` control lease the
// graph-access chokepoint consults (two-person, exact-scope, hard expiry).
pub mod rbac_elevation;
// EH-560 -- governed changes: reserved control-lease kinds whose approval EG
// proves two-person (approver != proposer, exact digest, single use).
pub mod governed_change;
// EH-406 -- error-budget adaptive (AIMD) throttling on a capacity cell:
// automatic steps only narrow or give back what they narrowed.
pub mod capacity_throttle;
// MEDULLA-UNIFIED-DATA-PLANE-R003 — the typed, hashed attached-source catalog graph
// (tables/columns) and its ambiguous-table refusal.
pub mod attached_catalog;
// MEDULLA-UNIFIED-DATA-PLANE-R004 — bounded, policy-gated column-sampling budget
// for deterministic schema inference.
pub mod schema_inference;
// MEDULLA-UNIFIED-DATA-PLANE-R015.1 — typed SQLite attached-source catalog model:
// WITHOUT ROWID handling and its WAL-tailing/watermark capture-mode
// refusal. Pure data; the live read-only attach and real capture are later
// children.
pub mod sqlite_attached_catalog;
// MEDULLA-UNIFIED-DATA-PLANE-R015.2.1 — SQLite live-attach adapter skeleton:
// typed attach config → read-only driver open config, refusing an
// immutable open mode paired with a change-capture mode. No file I/O; real
// WAL-frame tailing/watermark polling is R015.2.2+.
pub mod sqlite_live_attach;
// MEDULLA-UNIFIED-DATA-PLANE-R016.1 — typed SQL Server attached-source catalog
// model: two-part schema.table entries and the bracket-quoting identifier
// refusal. Pure data; the tiberius connection and T-SQL rendering are later
// children.
pub mod mssql_attached_catalog;
// MEDULLA-UNIFIED-DATA-PLANE-R016.2.1 — MSSQL adapter skeleton: typed connection
// config → tiberius-shaped driver config, with a connect refusal path that
// needs no live server. T-SQL rendering and LSN-polled CDC/Change Tracking
// capture are R016.2.2+.
pub mod mssql_driver_connection;
// MEDULLA-UNIFIED-DATA-PLANE-R017.1 — typed ClickHouse attached-source catalog
// model, with its change-capture capability always reported as the
// explicit Unsupported value. Pure data; the HTTP/native client connection
// is a later child.
pub mod clickhouse_attached_catalog;
// MEDULLA-UNIFIED-DATA-PLANE-R018.1 — typed Oracle/Db2 attached-source catalog
// model: double-quoted identifier refusal and the Debezium-captured-table
// primary-key refusal. Pure data; the driver/ODBC connection and Debezium
// bridge wiring are later children.
pub mod relational_attached_catalog;
// MEDULLA-UNIFIED-DATA-PLANE-R031.1 — typed consolidation-isolation admission:
// per-application quota and upgrade-coupling policy, with the empty-identity
// and all-zero-quota refusal. Pure data; real fault isolation and quota
// metering are later children.
pub mod consolidation_isolation;
// MEDULLA-UNIFIED-DATA-PLANE-R032.1 — typed Immich connector pilot phase gate
// (I0-I6), refusing a wrong or unreviewed exit artifact and skipping ahead
// of an un-exited earlier phase. Pure data; running each phase is a later
// child.
pub mod immich_pilot_phase;
// D-VZ-1 (lanes V4 "engine integration" / V6 "graph-native marks") — the native
// visualization engine's wire op (`VizOp`), gated `viz`. Lives here (not in
// `medulla-viz-core`, which sits in a separate small leaf DAG, not below medulla-types) for
// the SAME reason `jobs`/`statechart` do: `protocol::Method::Viz` carries it over
// the wire, and `protocol` is bottom-of-DAG. Pure serde — no dep on
// medulla-viz-core/medulla-viz-columnstore/medulla-viz-export; the handler
// (`src/server/handlers/viz.rs`, facade feature `viz-static-export`) is the one
// place that parses `VizRenderRequest::spec_json` into a real
// `medulla_viz_core::ViewSpec` and resolves it.
pub mod run_usage;
pub mod usage;
#[cfg(feature = "viz")]
pub mod viz;
pub mod wire;
// RF-ADR-009 D18 — EG-owned versioned source change sets and append-only
// write-back/reconciliation receipts. Connector transports remain in the SDK.
pub mod write_back;
// MEDULLA-FEDERATED-QUERY-R700 — AIF argument maps and their Dung projection.
pub mod argument_map;
// MEDULLA-FEDERATED-QUERY-R701 — live artifacts re-derived from a query source.
pub mod live_artifact;
// MEDULLA-FEDERATED-QUERY-R702 — search-task records and shortcut-risk reports.
pub mod search_task;
// MEDULLA-FEDERATED-QUERY-R081 — retrieval sufficiency, selection, feedback and
// hierarchy contracts (incl. the R081.9.7.1 section hierarchy policy); the deciding functions live in `medulla_plan::retrieval_verdict`.
pub mod retrieval_verdict;
// MEDULLA-FEDERATED-QUERY-R085.1 — approved, read-only source query records.
pub mod source_query;

// CONCEPT:MEDULLA-KG.query.compound-predicate-decode — the serializable compound-WHERE predicate AST lives at the
// bottom of the DAG so `medulla-core` can evaluate it; `medulla-query` decodes SQL into it.
pub use row_predicate::{CmpOp, RowPredicate};

// CONCEPT:MEDULLA-KG.compute.uncertainty-values — surface the distribution VALUE at the crate root for callers.
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

/// The six native capacity writes. Keep admission, identity normalization and
/// served dispatch on the same variant set.
#[macro_export]
macro_rules! capacity_lease_writes {
    () => {
        $crate::protocol::Method::AcquireCapacity { .. }
            | $crate::protocol::Method::RenewCapacity { .. }
            | $crate::protocol::Method::ReleaseCapacity { .. }
            | $crate::protocol::Method::ReclaimExpiredCapacity { .. }
            | $crate::protocol::Method::UpdateCapacityCell { .. }
            | $crate::protocol::Method::ThrottleCapacityCell { .. }
    };
}
