use crate::owner::contract::{expected_owner_table_contract, ledger_table_contract};
use crate::owner::registry::owner_table_names;
use crate::physical::manifest::hash_table_contract;
use eg_types::mutation_batch::MutationDomain;
use eg_types::MutationScopeIdentity;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const OWNER_LAYOUT_DOMAIN: &[u8] = b"eg/mutation-owner-layout/v1\0";
const OWNER_LAYOUT_NAMES: [&str; 8] = [
    "ledger_only",
    "rbac",
    "jobs",
    "statechart",
    "time_series",
    "kv",
    "blob",
    "semantic_index",
];
pub(crate) const OWNER_LAYOUT_DOMAINS: [MutationDomain; 8] = [
    MutationDomain::ControlPlane,
    MutationDomain::ControlPlane,
    MutationDomain::AnalyticsJob,
    MutationDomain::Lifecycle,
    MutationDomain::TimeSeries,
    MutationDomain::KvStore,
    MutationDomain::BlobStore,
    MutationDomain::SemanticIndex,
];
pub(crate) const LEDGER_TABLE_NAMES: [&str; 17] = [
    "mutation_store_root_v1",
    "mutation_scope_bindings_v1",
    "mutation_owner_manifest_v1",
    "mutation_batches_v1",
    "mutation_idempotency_v1",
    "mutation_versions_v1",
    "mutation_fences_v1",
    "mutation_outbox_v1",
    "mutation_private_payloads_v1",
    "mutation_outbox_topic_index_v1",
    "mutation_outbox_consumers_v1",
    "mutation_outbox_deliveries_v1",
    "mutation_outbox_cursors_v1",
    "mutation_outbox_claim_cursors_v1",
    "mutation_outbox_fairness_v1",
    "mutation_replay_nonces_v1",
    "mutation_replay_operations_v1",
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
        )
    }

    pub(crate) fn digest(self) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(OWNER_LAYOUT_DOMAIN);
        hasher.update(self.canonical_name().as_bytes());
        for table in LEDGER_TABLE_NAMES {
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
