use crate::owner::contract::{expected_owner_table_contract, ledger_table_contract};
use crate::owner::registry::owner_table_names;
use crate::physical::manifest::hash_table_contract;
use eg_types::mutation_batch::MutationDomain;
use eg_types::MutationScopeIdentity;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const OWNER_LAYOUT_DOMAIN: &[u8] = b"eg/mutation-owner-layout/v1\0";
const OWNER_LAYOUT_NAMES: [&str; 10] = [
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
];
pub(crate) const OWNER_LAYOUT_DOMAINS: [MutationDomain; 10] = [
    MutationDomain::ControlPlane,
    MutationDomain::ControlPlane,
    MutationDomain::AnalyticsJob,
    MutationDomain::Lifecycle,
    MutationDomain::TimeSeries,
    MutationDomain::KvStore,
    MutationDomain::BlobStore,
    MutationDomain::SemanticIndex,
    MutationDomain::SqlCatalog,
    MutationDomain::ControlPlane,
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
}

impl OwnerLayout {
    pub const fn canonical_name(self) -> &'static str {
        OWNER_LAYOUT_NAMES[self as usize]
    }

    pub(crate) fn accepts(self, identity: &MutationScopeIdentity) -> bool {
        if self == Self::LedgerOnly {
            return true;
        }
        matches!(
            (self, identity.scope().native_domain()),
            (Self::Rbac, Some(MutationDomain::ControlPlane))
                | (Self::Jobs, Some(MutationDomain::AnalyticsJob))
                | (Self::Statechart, Some(MutationDomain::Lifecycle))
                | (Self::TimeSeries, Some(MutationDomain::TimeSeries))
                | (Self::Kv, Some(MutationDomain::KvStore))
                | (Self::Blob, Some(MutationDomain::BlobStore))
                | (Self::SemanticIndex, Some(MutationDomain::SemanticIndex))
                | (Self::Sql, Some(MutationDomain::SqlCatalog))
                | (Self::PathIndex, Some(MutationDomain::ControlPlane))
        )
    }

    pub(crate) fn digest(self) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(OWNER_LAYOUT_DOMAIN);
        hasher.update(self.canonical_name().as_bytes());
        for table in crate::tables::ledger_table_names() {
            hash_table_contract(&mut hasher, &ledger_table_contract(table));
        }
        for table in owner_table_names(self) {
            hash_table_contract(&mut hasher, &expected_owner_table_contract(table, self));
        }
        hasher.finalize().into()
    }
}

pub(crate) fn layout_domain_tag() -> &'static [u8] {
    OWNER_LAYOUT_DOMAIN
}
