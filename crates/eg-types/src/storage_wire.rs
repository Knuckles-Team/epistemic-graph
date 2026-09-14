//! Pure-data result bodies of the `storage` contract domain: the backup / restore
//! receipts and the SQLite `.db` transfer reports.
//!
//! They live here, at the bottom of the crate DAG, so the result contract
//! (`result_contract::storage`) can name the exact body a handler encodes.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Integrity census of one physical recovery owner file: the row count of every
/// recovery-relevant table, as the storage kernel validated it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RecoveryStoreCounts {
    pub store_roots: u64,
    pub scope_bindings: u64,
    pub batches: u64,
    pub prepared: u64,
    pub committed: u64,
    pub aborted: u64,
    pub maintenance_claims: u64,
    pub versions: u64,
    pub fences: u64,
    pub outbox: u64,
    pub encrypted_private_payloads: u64,
    pub replay_nonces: u64,
    pub replay_operations: u64,
    pub maintenance: u64,
}

/// Result of `Method::Backup`: aggregate counts of the published bundle. It carries
/// no local path and no raw label.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct BackupReceipt {
    /// Shard files written into the bundle.
    pub shards: usize,
    /// Distinct graph scopes the bundle carries.
    pub graph_scopes: u64,
    /// Per-shard recovery census, in shard order.
    pub shard_counts: Vec<RecoveryStoreCounts>,
    /// In-doubt cross-shard participant prepare records carried.
    pub xshard_prepares: u64,
    /// Retained cross-shard coordinator decisions carried.
    pub xshard_decisions: u64,
    /// Non-shard durable stores copied into the bundle, as `file name -> rows copied`.
    pub bundled_stores: BTreeMap<String, u64>,
    /// Admin coordinator batches carried.
    pub admin_batches: u64,
    /// Admin coordinator parents still prepared.
    pub prepared_parents: u64,
    /// Encrypted staged recovery plans carried.
    pub encrypted_recovery_plans: u64,
}

/// Result of `Method::Restore`: the opaque stage reference an operator swaps in, plus
/// the import totals. Portable: no username or filesystem reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RestoreReceipt {
    /// Deterministic opaque token naming the staged restore.
    pub stage_ref: String,
    /// Shard count the persist dir was rebuilt at.
    pub restored_shards: usize,
    pub graphs: usize,
    pub nodes: u64,
    pub edges: u64,
    pub ledger: u64,
    pub semantic: u64,
    pub audit: u64,
    pub auxiliary: u64,
    pub global: u64,
    pub xshard_prepares: u64,
    pub xshard_decisions: u64,
    pub admin_batches: u64,
    pub prepared_parents: u64,
    pub encrypted_recovery_plans: u64,
}

/// Rows transferred for one SQLite user table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SqliteTableRows {
    pub table: String,
    pub rows: u64,
}

/// Where an imported table set came from. The only source is a SQLite `.db` file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum SqliteImportSource {
    #[serde(rename = "sqlite")]
    Sqlite,
}

/// Result of `Method::ImportSqliteFile`, also the durable batch result a replay
/// returns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SqliteImportReport {
    pub source: SqliteImportSource,
    pub imported_tables: Vec<SqliteTableRows>,
}

/// Where an exported table set was published: always the configured private transfer
/// root, never a host path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum SqliteExportDestination {
    #[serde(rename = "transfer-root")]
    TransferRoot,
}

/// Result of `Method::ExportSqliteFile`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SqliteExportReport {
    pub destination: SqliteExportDestination,
    pub exported_tables: Vec<SqliteTableRows>,
}
