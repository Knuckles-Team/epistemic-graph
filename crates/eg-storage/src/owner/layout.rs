use crate::owner::contract::expected_table_contracts;
use crate::physical::manifest::{hash_table_contract, TableContract};
use eg_types::mutation_batch::DurabilityDomain;
use eg_types::MutationScopeIdentity;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const OWNER_LAYOUT_DOMAIN: &[u8] = b"eg/mutation-owner-layout/v1\0";
const OWNER_LAYOUT_NAMES: [&str; 18] = [
    "ledger_only",
    "rbac",
    "jobs",
    "statechart",
    "time_series",
    "kv",
    "blob",
    "semantic_index",
    "sql",
    "path_index",
    "request_replay",
    "viz_provenance",
    "cold_tier",
    "tenant_catalog",
    "node_info",
    "cluster_hierarchy",
    "graph_shard",
    "agent_library",
];
pub(crate) const OWNER_LAYOUT_DOMAINS: [DurabilityDomain; 18] = [
    DurabilityDomain::ControlPlane,
    DurabilityDomain::ControlPlane,
    DurabilityDomain::AnalyticsJob,
    DurabilityDomain::Lifecycle,
    DurabilityDomain::TimeSeries,
    DurabilityDomain::KvStore,
    DurabilityDomain::BlobStore,
    DurabilityDomain::SemanticIndex,
    DurabilityDomain::SqlCatalog,
    DurabilityDomain::ControlPlane,
    DurabilityDomain::ControlPlane,
    DurabilityDomain::ControlPlane,
    DurabilityDomain::ControlPlane,
    DurabilityDomain::ControlPlane,
    DurabilityDomain::ControlPlane,
    DurabilityDomain::ControlPlane,
    DurabilityDomain::GraphRows,
    // RF-020 Agent Library: a ControlPlane owner file, reusing the existing
    // native mutation ledger rather than introducing another durability domain.
    DurabilityDomain::ControlPlane,
];

/// Closed registry of physical owner-table layouts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum OwnerLayout {
    LedgerOnly,
    Rbac,
    Jobs,
    Statechart,
    TimeSeries,
    Kv,
    Blob,
    SemanticIndex,
    /// The SQL catalog/row store (`__sql_*`) owned by `eg-query`.
    Sql,
    /// The durable logical-path index owned by `eg-core`'s path persistence.
    PathIndex,
    /// One node's durable signed-request replay ledger (`request-replay.redb`).
    RequestReplay,
    /// The durable render-provenance side store (`viz_provenance.redb`).
    VizProvenance,
    /// The cold-tier offloaded-graph cache (`cold.redb`).
    ColdTier,
    /// The durable tenant catalog (`catalog.redb`).
    TenantCatalog,
    /// The durable cluster node-info directory (`node_info.redb`).
    NodeInfo,
    /// The durable Leiden cluster-hierarchy cache (`cluster_hierarchy.redb`).
    ClusterHierarchy,
    /// The authoritative graph shard (`graph-N.redb`).
    ///
    /// The only layout whose declared domain is graph-authoritative, so it is
    /// the only one that serves a [`MutationScope::Graph`]: one shard file
    /// hosts many graphs, and a graph scope binds to the shard file of its
    /// graph. See [`crate::owner::graph_shard`] for the census and for the two
    /// tables classes it deliberately excludes.
    GraphShard,
    /// The durable RF-020 Agent Library revisions and current heads
    /// (`agent_library.redb`).
    AgentLibrary,
}

impl OwnerLayout {
    pub const fn canonical_name(self) -> &'static str {
        OWNER_LAYOUT_NAMES[self as usize]
    }

    /// Whether this layout's file may serve one exact logical scope.
    ///
    /// The two scope shapes bind by the same rule, read off the layout's own
    /// declared domain rather than off a hand-maintained pairing table:
    ///
    /// * a **native** scope binds to the layout that declares its exact
    ///   `DurabilityDomain` (`Rbac`/`PathIndex` and the five root sidecars all
    ///   declare `ControlPlane`; they are distinct files, and `create_owner` /
    ///   `open_owner` are layout-typed, so the non-injectivity is not reachable
    ///   as an ambiguity);
    /// * a **graph** scope has no native domain at all
    ///   (`MutationScope::graph(..)`), so it binds to the layout whose declared
    ///   domain is graph-authoritative — one that `may_own_native_scope`
    ///   refuses a native scope to. `GraphShard`/`GraphRows` is the only such
    ///   layout, which is what makes "a graph scope binds to the shard file of
    ///   its graph" a property of the registry rather than a special case.
    ///
    /// `LedgerOnly` declares no owner table and serves any scope.
    pub(crate) fn accepts(self, identity: &MutationScopeIdentity) -> bool {
        if self == Self::LedgerOnly {
            return true;
        }
        let declared = OWNER_LAYOUT_DOMAINS[self as usize];
        match identity.scope().native_domain() {
            Some(domain) => domain == declared,
            None => !declared.may_own_native_scope(),
        }
    }

    pub(crate) fn digest(self) -> [u8; 32] {
        layout_digest_over(self, &expected_table_contracts(self))
    }
}

/// The layout digest over an explicit contract list, ledger tables first.
///
/// [`OwnerLayout::digest`] is this over the compiled contracts; a persisted
/// manifest's own contract list gives the digest that file was written under,
/// which is how a predecessor file is told apart from a corrupt one.
pub(crate) fn layout_digest_over(layout: OwnerLayout, contracts: &[TableContract]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(OWNER_LAYOUT_DOMAIN);
    hasher.update(layout.canonical_name().as_bytes());
    for contract in contracts {
        hash_table_contract(&mut hasher, contract);
    }
    hasher.finalize().into()
}

pub(crate) fn layout_domain_tag() -> &'static [u8] {
    OWNER_LAYOUT_DOMAIN
}
