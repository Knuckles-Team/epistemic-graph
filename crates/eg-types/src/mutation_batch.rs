//! Canonical durable mutation-batch contract.
//!
//! A mutation is not complete merely because its serving projection changed in
//! memory.  This contract is the durable unit shared by transaction, query, RDF,
//! and lifecycle adapters: identity/context, placement and OCC fences, the ordered
//! operation list, durable result status, and projection/outbox intents travel
//! together.  Persistence implementations must commit all of it atomically before
//! a caller publishes the corresponding in-memory state.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::protocol::Method;

/// Current on-disk/wire schema version for mutation and projection records.
///
/// Version 2 is an intentional current-only cutover: operation domains and
/// projection version semantics are required fields. Readers do not synthesize
/// either value for records written under an older shape.
pub const MUTATION_BATCH_VERSION: u16 = 2;

/// Required `source_graph_version` value for a record whose authoritative state
/// lives outside the graph store. Native SQL/KV/blob/job counters remain in their
/// owner stores and must never be represented as graph versions.
pub const NON_GRAPH_SOURCE_VERSION: u64 = 0;

/// Authenticated request facts copied into a durable batch.
///
/// Verification is deliberately performed above this pure-data crate.  These are
/// the facts that were verified, not caller-controlled replacements for them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MutationRequestContext {
    pub request_id: u64,
    pub principal: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub purpose: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy_fingerprint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
}

/// Origin surface for an operation.  The durable semantics never depend on this
/// value; it exists for policy/audit/projection consumers and for architecture
/// tests proving that every public mutation surface compiled into this contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationSurface {
    Graph,
    Transaction,
    Query,
    Rdf,
    Lifecycle,
    Job,
    Broker,
    Other,
}

/// Authoritative durability domain for an operation.
///
/// This is intentionally independent from the public API surface: SQL may lower
/// to graph rows or to the SQL catalog, while a transaction may span graph rows,
/// vectors, blobs and time series.  Every mutating protocol method must resolve to
/// exactly one of these domains (or to a coordinator whose child batches do).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationDomain {
    GraphRows,
    GraphSnapshot,
    RdfDataset,
    SqlCatalog,
    BlobStore,
    KvStore,
    TimeSeries,
    AnalyticsJob,
    Broker,
    CrossModal,
    MultiGraph,
    Lifecycle,
    ControlPlane,
}

/// Exact commit boundaries used only by the black-box release certification
/// harness. The running process aborts without unwinding when a strictly parsed,
/// request-qualified certification fault matches one of these boundaries.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationCommitPhase {
    BeforeRows,
    AfterRowsBeforeMetadata,
    BeforeCommit,
    AfterCommitBeforeAck,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CertificationFaultSpec {
    schema_version: u8,
    nonce: String,
    request_id: u64,
    domain: MutationDomain,
    phase: MutationCommitPhase,
}

/// Abort at one exact MutationBatch boundary when the release-certification
/// process has been explicitly armed.
///
/// The seam is deliberately scoped by a 256-bit nonce, the authenticated request
/// id, and the canonical durability domain. Merely setting a phase cannot crash an
/// unrelated write. Environment ownership already permits terminating the engine;
/// this hook adds no authority beyond making that termination deterministic at the
/// commit boundary. Invalid configuration fails the mutation before any rows are
/// changed rather than being ignored.
#[doc(hidden)]
pub fn apply_certification_fault(
    batch: &MutationBatch,
    phase: MutationCommitPhase,
) -> Result<(), String> {
    const ENV: &str = "EPISTEMIC_GRAPH_CERTIFICATION_FAULT";
    const SCHEMA_VERSION: u8 = 1;

    let Some(raw) = std::env::var_os(ENV) else {
        return Ok(());
    };
    let raw = raw
        .into_string()
        .map_err(|_| "certification fault configuration is not UTF-8".to_string())?;
    let spec: CertificationFaultSpec = serde_json::from_str(&raw)
        .map_err(|_| "certification fault configuration is invalid".to_string())?;
    if spec.schema_version != SCHEMA_VERSION
        || spec.request_id == 0
        || spec.nonce.len() != 64
        || !spec
            .nonce
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err("certification fault configuration is invalid".to_string());
    }
    if spec.phase != phase
        || spec.request_id != batch.context.request_id
        || !batch
            .operations
            .iter()
            .any(|operation| operation.domain == spec.domain)
    {
        return Ok(());
    }

    eprintln!(
        "EG_CERTIFICATION_FAULT phase={:?} domain={:?} request_id={}",
        phase, spec.domain, spec.request_id
    );
    std::process::abort();
}

/// One ordered engine operation in a [`MutationBatch`].
///
/// `Method` is the engine's already-versioned canonical operation DTO.  Keeping it
/// here avoids an unsafe second mutation vocabulary and lets WAL/Raft/redb replay
/// use the same deterministic apply implementation as live traffic.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MutationOperation {
    pub ordinal: u32,
    pub surface: MutationSurface,
    /// Required authoritative domain. A missing domain is a corrupt or unsupported
    /// persisted record, never an implicit graph-row operation.
    pub domain: MutationDomain,
    pub method: Method,
}

/// Digest/version descriptor for authenticated authoritative graph material.
/// The algorithm identifies either a complete snapshot (`sha256`) or a bounded
/// affected-row delta (`sha256-row-delta-v2`).
///
/// Runtime-result mutations are executed against an isolated staging graph.  The
/// resulting authoritative material is supplied separately to the persistence
/// kernel, verified against this digest, and atomically updates graph rows before
/// RAM is published. The payload is deliberately not duplicated in the batch
/// record; the descriptor is sufficient for idempotency and reconciliation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MutationStateDescriptor {
    pub algorithm: String,
    pub digest: String,
    pub source_graph_version: u64,
    pub target_graph_version: u64,
}

/// A projection/index/CDC/audit/lineage notification to publish after commit.
/// The intent is stored in the same transaction as authoritative state; consumers
/// may retry delivery using `(batch_id, ordinal)` without duplicating the mutation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MutationOutboxIntent {
    pub topic: String,
    pub key: String,
    #[serde(with = "serde_bytes")]
    pub payload: Vec<u8>,
    pub headers: BTreeMap<String, String>,
}

/// Universal, deterministic durable mutation unit.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MutationBatch {
    pub schema_version: u16,
    /// Stable identity for status lookup and outbox correlation.
    pub batch_id: String,
    pub context: MutationRequestContext,
    /// Authoritative tenant scope, copied from verified context/graph routing.
    pub tenant: String,
    /// Logical graph name (not a sanitized filename).
    pub graph: String,
    /// Catalog epoch used to resolve the graph. Zero explicitly denotes a local,
    /// unplaced authority; readers never infer an epoch from missing data.
    pub placement_epoch: u64,
    /// Caller-stable retry key.  Uniqueness is scoped by `(tenant, graph)`.
    pub idempotency_key: String,
    /// OCC version observed before validation, when the surface has one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_graph_version: Option<u64>,
    /// Lease/worker fencing epoch for work-driven writes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fencing_token: Option<u64>,
    /// Present when the operation list is represented by a complete staged graph
    /// image rather than solely by row-local methods.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authoritative_state: Option<MutationStateDescriptor>,
    pub operations: Vec<MutationOperation>,
    pub outbox: Vec<MutationOutboxIntent>,
    /// Creation time supplied by the request boundary, never generated during
    /// replay.  This keeps replay and content digests deterministic.
    pub created_at_ms: u64,
}

impl MutationBatch {
    /// Validate invariants required by every persistence implementation.
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != MUTATION_BATCH_VERSION {
            return Err(format!(
                "unsupported mutation batch version {} (expected {})",
                self.schema_version, MUTATION_BATCH_VERSION
            ));
        }
        if self.batch_id.trim().is_empty() {
            return Err("mutation batch_id must not be empty".to_string());
        }
        if !self
            .context
            .principal
            .strip_prefix("principal:sha256:")
            .is_some_and(|digest| {
                digest.len() == 64
                    && digest
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
            })
        {
            return Err("mutation principal authority must be an opaque digest".to_string());
        }
        if self.tenant.trim().is_empty() {
            return Err("mutation tenant must not be empty".to_string());
        }
        if self.graph.trim().is_empty() {
            return Err("mutation graph must not be empty".to_string());
        }
        if self.idempotency_key.trim().is_empty() {
            return Err("mutation idempotency_key must not be empty".to_string());
        }
        if self.operations.is_empty() {
            return Err("mutation batch must contain at least one operation".to_string());
        }
        for (expected, op) in self.operations.iter().enumerate() {
            if op.ordinal as usize != expected {
                return Err(format!(
                    "mutation operation ordinal {} is not contiguous at index {}",
                    op.ordinal, expected
                ));
            }
        }
        if self.placement_epoch > 0 && self.fencing_token.is_none() {
            return Err("placed mutation requires a fencing token".to_string());
        }
        if let Some(state) = &self.authoritative_state {
            if !matches!(state.algorithm.as_str(), "sha256" | "sha256-row-delta-v2")
                || state.digest.len() != 64
                || !state
                    .digest
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            {
                return Err(
                    "authoritative state requires a supported lowercase sha256 descriptor"
                        .to_string(),
                );
            }
            let expected_target = state
                .source_graph_version
                .checked_add(1)
                .ok_or_else(|| "authoritative state source version overflow".to_string())?;
            if state.target_graph_version != expected_target {
                return Err(
                    "authoritative state target version must be exactly source version plus one"
                        .to_string(),
                );
            }
            if self.expected_graph_version != Some(state.source_graph_version) {
                return Err(
                    "authoritative state source version must equal expected_graph_version"
                        .to_string(),
                );
            }
        }
        for intent in &self.outbox {
            if intent.topic.trim().is_empty() || intent.key.trim().is_empty() {
                return Err("mutation outbox topic/key must not be empty".to_string());
            }
        }
        Ok(())
    }
}

/// Durable terminal state.  `Prepared` is reserved for Raft/2PC adapters; local
/// atomic commits persist `Committed` in the same transaction as state rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationBatchStatus {
    Prepared,
    Committed,
    Aborted,
}

/// Durable status/result record used for retry reconciliation after restart.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MutationBatchRecord {
    pub batch: MutationBatch,
    pub status: MutationBatchStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_msgpack: Option<Vec<u8>>,
    pub committed_at_ms: u64,
}

/// Meaning of the required `source_graph_version` field on a durable outbox row
/// or projection watermark.
///
/// `Graph` records carry a strictly positive committed graph version. `NonGraph`
/// records carry [`NON_GRAPH_SOURCE_VERSION`]; their native authority owns any
/// domain-specific counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationVersionScope {
    Graph,
    NonGraph,
}

/// One durable outbox row.  Delivery state is intentionally separate so marking a
/// row delivered never rewrites the authoritative batch or operation list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MutationOutboxRecord {
    pub schema_version: u16,
    pub batch_id: String,
    pub ordinal: u32,
    pub tenant: String,
    pub graph: String,
    pub version_scope: MutationVersionScope,
    /// Required committed graph version. It is strictly positive for `Graph`
    /// records and exactly [`NON_GRAPH_SOURCE_VERSION`] for `NonGraph` records.
    pub source_graph_version: u64,
    pub intent: MutationOutboxIntent,
    pub created_at_ms: u64,
}

impl MutationOutboxRecord {
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != MUTATION_BATCH_VERSION {
            return Err(format!(
                "unsupported mutation outbox version {} (expected {})",
                self.schema_version, MUTATION_BATCH_VERSION
            ));
        }
        if self.batch_id.trim().is_empty()
            || self.tenant.trim().is_empty()
            || self.graph.trim().is_empty()
            || self.intent.topic.trim().is_empty()
            || self.intent.key.trim().is_empty()
        {
            return Err("mutation outbox identity/topic/key must not be empty".to_string());
        }
        match self.version_scope {
            MutationVersionScope::Graph if self.source_graph_version == 0 => Err(
                "graph-authoritative outbox record requires a non-zero source_graph_version"
                    .to_string(),
            ),
            MutationVersionScope::NonGraph
                if self.source_graph_version != NON_GRAPH_SOURCE_VERSION =>
            {
                Err(
                    "non-graph outbox record must use the explicit non-graph source version"
                        .to_string(),
                )
            }
            _ => Ok(()),
        }
    }
}

/// Durable lease over an outbox row.  Consumers acknowledge the exact lease epoch;
/// a late worker can therefore never mark a row delivered after its lease expired
/// and was reassigned.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MutationOutboxLease {
    pub record: MutationOutboxRecord,
    pub consumer: String,
    pub lease_epoch: u64,
    pub lease_until_ms: u64,
    pub attempt: u32,
}

/// Monotonic projection watermark updated in the same transaction as outbox ack.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MutationProjectionCursor {
    pub schema_version: u16,
    pub projection: String,
    pub tenant: String,
    pub graph: String,
    pub batch_id: String,
    pub outbox_ordinal: u32,
    pub version_scope: MutationVersionScope,
    pub source_graph_version: u64,
    pub advanced_at_ms: u64,
}

impl MutationProjectionCursor {
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != MUTATION_BATCH_VERSION {
            return Err(format!(
                "unsupported mutation projection cursor version {} (expected {})",
                self.schema_version, MUTATION_BATCH_VERSION
            ));
        }
        if self.projection.trim().is_empty()
            || self.tenant.trim().is_empty()
            || self.graph.trim().is_empty()
            || self.batch_id.trim().is_empty()
        {
            return Err("mutation projection cursor identity must not be empty".to_string());
        }
        match self.version_scope {
            MutationVersionScope::Graph if self.source_graph_version == 0 => Err(
                "graph-authoritative projection cursor requires a non-zero source_graph_version"
                    .to_string(),
            ),
            MutationVersionScope::NonGraph
                if self.source_graph_version != NON_GRAPH_SOURCE_VERSION =>
            {
                Err(
                    "non-graph projection cursor must use the explicit non-graph source version"
                        .to_string(),
                )
            }
            _ => Ok(()),
        }
    }
}

/// Result returned by a persistence commit.  `replayed=true` means the durable
/// idempotency row already pointed at the returned committed record; no operation
/// was applied a second time and no duplicate outbox row was created.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MutationBatchCommit {
    pub record: MutationBatchRecord,
    pub replayed: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn batch() -> MutationBatch {
        MutationBatch {
            schema_version: MUTATION_BATCH_VERSION,
            batch_id: "batch-1".into(),
            context: MutationRequestContext {
                request_id: 7,
                principal: format!("principal:sha256:{}", "a".repeat(64)),
                purpose: Some("unit-test".into()),
                policy_fingerprint: None,
                trace_id: None,
            },
            tenant: "tenant-a".into(),
            graph: "graph-a".into(),
            placement_epoch: 3,
            idempotency_key: "idem-1".into(),
            expected_graph_version: Some(9),
            fencing_token: Some(4),
            authoritative_state: None,
            operations: vec![MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Transaction,
                domain: MutationDomain::GraphRows,
                method: Method::RemoveNode {
                    node_id: "n".into(),
                },
            }],
            outbox: Vec::new(),
            created_at_ms: 10,
        }
    }

    #[test]
    fn validates_contiguous_non_empty_batch() {
        batch().validate().unwrap();
        let mut invalid = batch();
        invalid.operations[0].ordinal = 1;
        assert!(invalid.validate().unwrap_err().contains("not contiguous"));
    }

    #[test]
    fn roundtrip_keeps_fences_and_operation() {
        let original = batch();
        let bytes = rmp_serde::to_vec_named(&original).unwrap();
        let decoded: MutationBatch = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(decoded.schema_version, 2);
        assert_eq!(decoded.placement_epoch, 3);
        assert_eq!(decoded.expected_graph_version, Some(9));
        assert_eq!(decoded.fencing_token, Some(4));
        assert_eq!(decoded.operations.len(), 1);
        assert_eq!(decoded.operations[0].domain, MutationDomain::GraphRows);
    }

    #[test]
    fn missing_operation_domain_and_old_batch_version_are_rejected() {
        let mut operation = serde_json::to_value(&batch().operations[0]).unwrap();
        operation.as_object_mut().unwrap().remove("domain");
        assert!(serde_json::from_value::<MutationOperation>(operation).is_err());

        let mut old = batch();
        old.schema_version = 1;
        assert!(old.validate().unwrap_err().contains("unsupported"));
    }

    #[test]
    fn authoritative_state_requires_one_checked_version_step() {
        let mut state_backed = batch();
        state_backed.authoritative_state = Some(MutationStateDescriptor {
            algorithm: "sha256".into(),
            digest: "0".repeat(64),
            source_graph_version: 9,
            target_graph_version: 11,
        });
        assert!(state_backed
            .validate()
            .unwrap_err()
            .contains("exactly source version plus one"));

        let state = state_backed.authoritative_state.as_mut().unwrap();
        state.source_graph_version = u64::MAX;
        state.target_graph_version = u64::MAX;
        state_backed.expected_graph_version = Some(u64::MAX);
        assert!(state_backed.validate().unwrap_err().contains("overflow"));
    }

    fn outbox(scope: MutationVersionScope, source_graph_version: u64) -> MutationOutboxRecord {
        MutationOutboxRecord {
            schema_version: MUTATION_BATCH_VERSION,
            batch_id: "batch-1".into(),
            ordinal: 0,
            tenant: "tenant-a".into(),
            graph: "graph-a".into(),
            version_scope: scope,
            source_graph_version,
            intent: MutationOutboxIntent {
                topic: "engine.projection.rebuild".into(),
                key: "batch-1".into(),
                payload: Vec::new(),
                headers: BTreeMap::new(),
            },
            created_at_ms: 10,
        }
    }

    #[test]
    fn outbox_roundtrip_requires_explicit_version_semantics() {
        let graph = outbox(MutationVersionScope::Graph, 10);
        graph.validate().unwrap();
        let bytes = rmp_serde::to_vec_named(&graph).unwrap();
        let decoded: MutationOutboxRecord = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(decoded, graph);

        let mut old = graph.clone();
        old.schema_version = 1;
        assert!(old.validate().unwrap_err().contains("unsupported"));

        assert!(outbox(MutationVersionScope::Graph, 0)
            .validate()
            .unwrap_err()
            .contains("non-zero"));
        assert!(outbox(MutationVersionScope::NonGraph, 1)
            .validate()
            .unwrap_err()
            .contains("non-graph"));
        outbox(MutationVersionScope::NonGraph, NON_GRAPH_SOURCE_VERSION)
            .validate()
            .unwrap();

        let mut missing = serde_json::to_value(&graph).unwrap();
        missing
            .as_object_mut()
            .unwrap()
            .remove("source_graph_version");
        assert!(serde_json::from_value::<MutationOutboxRecord>(missing).is_err());
    }

    #[test]
    fn projection_cursor_rejects_zero_graph_watermark() {
        let cursor = MutationProjectionCursor {
            schema_version: MUTATION_BATCH_VERSION,
            projection: "reasoning".into(),
            tenant: "tenant-a".into(),
            graph: "graph-a".into(),
            batch_id: "batch-1".into(),
            outbox_ordinal: 0,
            version_scope: MutationVersionScope::Graph,
            source_graph_version: 0,
            advanced_at_ms: 10,
        };
        assert!(cursor.validate().unwrap_err().contains("non-zero"));

        let mut encoded = serde_json::to_value(&cursor).unwrap();
        encoded
            .as_object_mut()
            .unwrap()
            .remove("source_graph_version");
        assert!(serde_json::from_value::<MutationProjectionCursor>(encoded).is_err());
    }
}
