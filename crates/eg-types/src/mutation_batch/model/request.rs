use serde::{Deserialize, Serialize};

use crate::protocol::Method;

/// Authenticated request facts copied into a durable batch.
///
/// Verification is deliberately performed above this pure-data crate. These are
/// the facts that were verified, not caller-controlled replacements for them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct MutationRequestContext {
    pub request_id: u64,
    pub principal: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub purpose: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy_fingerprint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
    /// Capabilities verified at the authenticated admission boundary.
    pub verified_capabilities: std::collections::BTreeSet<MutationCapability>,
}

/// Origin surface for an operation. The durable semantics never depend on this
/// value; it exists for policy/audit/projection consumers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DurabilityDomain {
    GraphRows = 0,
    GraphSnapshot = 1,
    RdfDataset = 2,
    SqlCatalog = 3,
    BlobStore = 4,
    KvStore = 5,
    TimeSeries = 6,
    AnalyticsJob = 7,
    SemanticIndex = 8,
    Broker = 9,
    CrossModal = 10,
    MultiGraph = 11,
    Lifecycle = 12,
    ControlPlane = 13,
}

const MUTATION_DOMAIN_NAMES: [&str; DurabilityDomain::ControlPlane as usize + 1] = [
    "graph_rows",
    "graph_snapshot",
    "rdf_dataset",
    "sql_catalog",
    "blob_store",
    "kv_store",
    "time_series",
    "analytics_job",
    "semantic_index",
    "broker",
    "cross_modal",
    "multi_graph",
    "lifecycle",
    "control_plane",
];

impl DurabilityDomain {
    pub const fn canonical_name(self) -> &'static str {
        MUTATION_DOMAIN_NAMES[self as usize]
    }

    /// True when this domain MAY own a `MutationScope::Native`.
    ///
    /// The scope and the domain are two independent axes, and three classes of
    /// domain fall out of them:
    ///
    /// * graph-only (`GraphRows`, `GraphSnapshot`, `RdfDataset`) -- the graph is
    ///   the authority; these may never own a native scope;
    /// * store-only (see [`Self::requires_native_scope`]) -- the domain's own
    ///   store is the authority; these may never ride a graph scope;
    /// * either (`Lifecycle`, `ControlPlane`, `CrossModal`, `MultiGraph`) -- the
    ///   family describes the operation, not the authority, so the same domain is
    ///   legal in a graph scope (versioned by `MUTATION_GRAPH_VERSION`) *and* in a
    ///   native scope (e.g. the RBAC `security-control` store, whose authority is
    ///   its own file).
    ///
    /// Answering "may own a native scope" and "is forbidden in a graph scope"
    /// with one predicate is what made every control-plane batch unrepresentable:
    /// graph scope was rejected by `validate_operations`, and native scope was
    /// rejected one call deeper by the graph commit route.
    pub const fn may_own_native_scope(self) -> bool {
        !matches!(
            self,
            Self::GraphRows | Self::GraphSnapshot | Self::RdfDataset
        )
    }

    /// True when this domain's authoritative state and version counter live in
    /// its OWN store, so it must travel in a `MutationScope::Native` whose
    /// declared domain equals it and may never ride a graph's OCC counter.
    /// True when this domain may NEVER ride a graph scope, because its state and
    /// its version counter live in its own store and it has no graph-committed
    /// route at all.
    ///
    /// This is the VALIDATOR's question, and it is not the same as
    /// [`Self::requires_native_scope`], which is the PRODUCER's default. The two
    /// differ for exactly `SqlCatalog` and `Broker`: both default to a native
    /// scope (eg-query's catalog path, and the broker's own compile sites), yet
    /// both also travel legitimately through the graph kernel --
    /// `compile_methods` commits every operation it is handed through
    /// `commit_mutation_batch_inner`, and a wire `INSERT INTO nodes` really is a
    /// graph-row mutation that happens to be written in SQL. `Broker` owns no
    /// mutation store whatsoever, so its state is versioned by the graph counter.
    ///
    /// Collapsing the two questions into one predicate was tried and measurably
    /// regressed the suite (15 -> 27 failures): answering the validator's
    /// question also changed the producer's default and broke the native catalog
    /// path. They are separate questions and need separate predicates.
    pub const fn forbidden_in_graph_scope(self) -> bool {
        matches!(
            self,
            Self::BlobStore
                | Self::KvStore
                | Self::TimeSeries
                | Self::AnalyticsJob
                | Self::SemanticIndex
        )
    }

    pub const fn requires_native_scope(self) -> bool {
        // NOTE: this predicate is doing DOUBLE DUTY and that is a known,
        // recorded defect (see the duplication/sprawl inventory). It answers
        // both "is this forbidden in a graph scope?" (`validate_operations`)
        // and "what scope should a compiled batch default to?"
        // (`mutation_batch.rs`'s `graph_scope = !domain.requires_native_scope()`).
        //
        // Those are different questions for `SqlCatalog` and `Broker`, which have
        // BOTH a store-authoritative route (eg-query's `sql_scope_key`, which
        // refuses a graph scope) and a graph-routed one (a wire `INSERT INTO
        // nodes` is a graph-row mutation written in SQL). Moving them out of this
        // set was tried and measurably regressed the suite 15 -> 27 failures,
        // because it flipped the PRODUCER default for every SQL/broker batch to
        // graph scope, breaking the native catalog path that was working.
        //
        // They therefore stay here, which keeps the producer default correct.
        // The graph-routed callers are the ones that must be fixed, by choosing
        // their scope explicitly rather than deriving it from the domain tag --
        // the scope is a property of the commit ROUTE, which only the caller
        // knows, not of the operation family.
        matches!(
            self,
            Self::SqlCatalog
                | Self::BlobStore
                | Self::KvStore
                | Self::TimeSeries
                | Self::AnalyticsJob
                | Self::SemanticIndex
                | Self::Broker
        )
    }
}

/// Admission capabilities that materially change persistence semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum MutationCapability {
    /// Reserved for authenticated first-boot/system-recovery operations.
    UnversionedSystemMutation,
}

/// Exact commit boundaries used by the black-box release certification harness.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationCommitPhase {
    BeforeRows,
    AfterRowsBeforeMetadata,
    BeforeCommit,
    AfterCommitBeforeAck,
}

/// One ordered engine operation in a [`MutationBatch`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct MutationOperation {
    pub ordinal: u32,
    pub surface: MutationSurface,
    /// Required authoritative domain.
    pub domain: DurabilityDomain,
    pub method: Method,
}
