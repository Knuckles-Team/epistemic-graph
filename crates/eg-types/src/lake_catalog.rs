//! GOC-10 — canonical SQL table / lake catalog authority wire types.
//!
//! Durable table/schema/partition/lake-catalog state today lives in two disjoint
//! places: the owner-scoped redb `TableStore` (`eg-query::tables::store`, physical
//! rows + a `MutationBatch`-shaped commit path) and a minimal in-memory
//! `IcebergRestCatalog` (`eg-lake::catalog`, `(namespace, table) -> metadata
//! location`, no governance fields, no restart durability). Neither carries the
//! governance/versioning vocabulary GOC-11 (analytics snapshots) and GOC-12
//! (cross-modal predicate pushdown) need to pin an immutable fence and reason about
//! schema/quality: tenant, owner, classification, purpose, retention/legal-hold,
//! commit fence, schema digest, quality status (GOC-10 invariant 2).
//!
//! This module is the CANONICAL value shape for that authority (GOC-10-W02): pure
//! serde, no I/O, no dependency beyond this crate. It intentionally does not touch
//! `eg-lake::catalog`/`server::lake::rest`/`server::lake::mod` — the REST/durable
//! projection of these records is GOC-75's file-owned surface (see the GOC-10 lane
//! doc's "conflicts/coordination" note); a durable authority STORE that persists and
//! commits these records through a `MutationBatch` participant is GOC-10-W03/W04,
//! not yet implemented. Callers construct these values today, `validate()` them, and
//! round-trip them over the wire (`msgpack::decode_bounded`) exactly like every other
//! type in this crate; a store that persists them can adopt the SAME shape without a
//! second migration.
//!
//! ## Invariants encoded here (GOC-10 "Authority and invariants")
//! 1. [`TableSchemaVersionV1`] links each version to its predecessor
//!    (`previous_version`) in a strict, gapless chain — "changes create a new
//!    version linked by derivation".
//! 2. [`GovernanceMeta`] is embedded in every top-level record: tenant, owner,
//!    classification, purpose, retention/legal-hold. [`CommitFenceRef`] and a
//!    schema/manifest/policy digest are carried alongside it on the record that
//!    needs them.
//! 3. [`CommitFenceRef`] mirrors `mutation_batch::MutationBatch`'s
//!    `(tenant, graph)` + `placement_epoch` + `fencing_token` + `batch_id`
//!    vocabulary verbatim, so a durable participant can bind a catalog/table/lake
//!    record to the SAME GOC-03 commit descriptor a graph/SQL mutation uses,
//!    without inventing a second fencing scheme.
//! 4. [`LakeSnapshotV1`] pins schema version + digest, partition-manifest
//!    references (by digest — file bytes are immutable CAS/object refs, never
//!    duplicated inline), commit sequence, quality, and a policy digest.
//! 5. Carrier/barrier verification is NOT modeled here — it is enforced above this
//!    pure-data layer (`server::lake::rest`, GOC-15/GOC-75 territory) before any of
//!    these records are read or written.
//! 6. [`TableChangeV1::validate`] enforces: the declared [`SchemaCompatibility`]
//!    must match [`classify_schema_change`]'s classification of the declared
//!    [`SchemaChangeKind`] (a caller cannot mislabel a destructive change as
//!    additive), a `Destructive` change requires `approved_by`, and a destructive
//!    change is rejected outright while [`RetentionPolicy::legal_hold`] is set —
//!    approval cannot override a legal hold, only lifting the hold can.

use serde::{Deserialize, Serialize};

/// Data-sensitivity classification a catalog/table/lake object carries
/// (GOC-10 invariant 2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataClassification {
    Public,
    Internal,
    Confidential,
    Restricted,
}

/// Retention / deletion / legal-hold state (GOC-10 invariant 2, migration/rollback
/// "no destructive schema change is applied without ... policy approval").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetentionPolicy {
    /// Epoch-ms after which the object may be reclaimed; `None` is indefinite.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retain_until_ms: Option<u64>,
    /// A legal hold freezes destructive schema/table/lake changes regardless of
    /// policy approval (see [`TableChangeV1::validate`]).
    pub legal_hold: bool,
    /// Epoch-ms a deletion was requested, if any. Distinct from `retain_until_ms`:
    /// this records an explicit request, not a schedule.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deletion_requested_at_ms: Option<u64>,
    /// Whether the object has actually been removed (a tombstone, not a physical
    /// erase of an immutable prior version — see the module doc's invariant 1).
    pub deleted: bool,
}

impl RetentionPolicy {
    pub fn active() -> Self {
        Self {
            retain_until_ms: None,
            legal_hold: false,
            deletion_requested_at_ms: None,
            deleted: false,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.deleted && self.legal_hold {
            return Err("a legal hold blocks deletion; lift the hold first".to_string());
        }
        if self.deleted && self.deletion_requested_at_ms.is_none() {
            return Err("a deleted object must record when deletion was requested".to_string());
        }
        Ok(())
    }
}

/// Shared governance fields carried by every top-level GOC-10 authority record
/// (GOC-10 invariant 2: "every table/lake object carries tenant, owner/principal,
/// classification, purpose, retention/deletion/legal hold").
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GovernanceMeta {
    /// Authoritative tenant scope (mirrors `mutation_batch::MutationBatch::tenant`).
    pub tenant: String,
    /// Opaque owner/principal scope. This crate does not mandate ONE digest shape
    /// (the SQL owner-scoped store and the Iceberg-REST bearer path derive their
    /// own opaque owner strings differently); it only requires non-empty — see the
    /// GOC-10 handoff note that `owner_scope()`, not the tenant claim, carries
    /// per-agent isolation within one deployment.
    pub owner: String,
    pub classification: DataClassification,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub purpose: Option<String>,
    pub retention: RetentionPolicy,
}

impl GovernanceMeta {
    pub fn validate(&self) -> Result<(), String> {
        if self.tenant.trim().is_empty() {
            return Err("governance tenant must not be empty".to_string());
        }
        if self.owner.trim().is_empty() {
            return Err("governance owner must not be empty".to_string());
        }
        self.retention.validate()
    }
}

/// A catalog/table/lake record's binding to a GOC-03 commit descriptor.
///
/// Deliberately mirrors `mutation_batch::MutationBatch`'s `(tenant, graph)` +
/// `placement_epoch` + `fencing_token` vocabulary (`crate::mutation_batch`) rather
/// than inventing a parallel fencing scheme, so a durable participant (GOC-10-W03)
/// can bind a catalog/table/lake row to the SAME batch a graph/SQL mutation commits
/// under (GOC-10 invariant 3). `batch_id` is that `MutationBatch::batch_id`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommitFenceRef {
    pub batch_id: String,
    pub tenant: String,
    pub placement_epoch: u64,
    pub fencing_token: u64,
}

impl CommitFenceRef {
    pub fn validate(&self) -> Result<(), String> {
        if self.batch_id.trim().is_empty() {
            return Err("commit fence batch_id must not be empty".to_string());
        }
        if self.tenant.trim().is_empty() {
            return Err("commit fence tenant must not be empty".to_string());
        }
        Ok(())
    }
}

/// A prior quality validation's outcome (GOC-10 invariant 4: `TableSnapshotV1`
/// pins "quality checks").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QualityStatus {
    /// No quality check has been run against this version/snapshot yet.
    Unvalidated,
    Passing,
    Failing,
    /// Passing with at least one non-blocking warning.
    Degraded,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QualityReportRef {
    pub report_id: String,
    pub status: QualityStatus,
    pub checked_at_ms: u64,
    /// Named failed checks. Required non-empty when `status == Failing` — a
    /// failing report with no recorded cause is not evidence of anything.
    pub failed_checks: Vec<String>,
}

impl QualityReportRef {
    pub fn validate(&self) -> Result<(), String> {
        if self.report_id.trim().is_empty() {
            return Err("quality report_id must not be empty".to_string());
        }
        if self.status == QualityStatus::Failing && self.failed_checks.is_empty() {
            return Err(
                "a failing quality report must record at least one failed check".to_string(),
            );
        }
        Ok(())
    }
}

/// Validates a value is a lowercase hex-encoded 256-bit digest, the same shape
/// `mutation_batch::MutationStateDescriptor::digest` already requires.
fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// Schema evolution shape (GOC-10 invariant 6: "explicit — additive/rename/drop/
/// type change with compatibility classification"). Named after the durable
/// `TxnOp` DDL variants (`eg-query::tables::store::TxnOp`) that a future
/// GOC-10-W03 commit participant would classify through this enum; this crate has
/// no dependency on `eg-query` (bottom-of-DAG), so the mapping is documented, not
/// coded, here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaChangeKind {
    AddColumn,
    DropColumn,
    RenameColumn,
    AlterColumnType,
    RenameTable,
    DropTable,
    AddConstraint,
    DropConstraint,
}

/// GOC-10 invariant 6's three-way compatibility classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaCompatibility {
    /// Existing readers of the prior version remain valid (e.g. a new nullable
    /// column, a new constraint that does not reject existing rows' current
    /// values at declaration time in practice).
    Additive,
    /// The prior version's queries keep working, but the SHAPE differs (a rename,
    /// or dropping a constraint that only narrowed writes) — a reader keyed by the
    /// old name/definition must be updated, but no data is at risk.
    Compatible,
    /// Data or reachability can be lost (a dropped column/table, a type change
    /// that a stored value may not coerce into). Requires policy approval and is
    /// rejected outright under a legal hold (see [`TableChangeV1::validate`]).
    Destructive,
}

/// Classify a [`SchemaChangeKind`] (GOC-10 invariant 6). Pure and total — every
/// variant maps to exactly one compatibility class, so [`TableChangeV1::validate`]
/// can always check a caller's claim against it.
pub fn classify_schema_change(kind: SchemaChangeKind) -> SchemaCompatibility {
    match kind {
        SchemaChangeKind::AddColumn => SchemaCompatibility::Additive,
        SchemaChangeKind::RenameColumn => SchemaCompatibility::Compatible,
        SchemaChangeKind::RenameTable => SchemaCompatibility::Compatible,
        // Dropping a CHECK/UNIQUE constraint only widens what future writes
        // accept; it cannot destroy already-durable data.
        SchemaChangeKind::DropConstraint => SchemaCompatibility::Compatible,
        // A new constraint can reject a caller's NEXT write against existing
        // data shape assumptions (e.g. a fresh UNIQUE index revealing a
        // pre-existing duplicate on the next insert attempt); treated as
        // Compatible, not Additive, because it is not purely non-breaking.
        SchemaChangeKind::AddConstraint => SchemaCompatibility::Compatible,
        SchemaChangeKind::DropColumn => SchemaCompatibility::Destructive,
        SchemaChangeKind::DropTable => SchemaCompatibility::Destructive,
        SchemaChangeKind::AlterColumnType => SchemaCompatibility::Destructive,
    }
}

/// One durable schema version of one table (GOC-10 invariant 1 + invariant 2).
/// Immutable once referenced by a [`LakeSnapshotV1`] — enforced by the future
/// durable store (GOC-10-W03), not by this value type; this struct only carries
/// the derivation link (`previous_version`) that makes such enforcement possible.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableSchemaVersionV1 {
    pub governance: GovernanceMeta,
    pub table: String,
    /// Monotonic, gapless per-table version counter starting at 0 (the `CREATE
    /// TABLE` version).
    pub version: u64,
    /// SHA-256 hex digest over the canonical column list (see
    /// `eg-query::tables::schema::TableSchema::schema_digest`).
    pub schema_digest: String,
    /// `None` only for `version == 0`; otherwise `Some(version - 1)` — a strict
    /// chain, never a gap or a fork.
    pub previous_version: Option<u64>,
    pub commit_fence: CommitFenceRef,
    pub created_at_ms: u64,
}

impl TableSchemaVersionV1 {
    pub fn validate(&self) -> Result<(), String> {
        if self.table.trim().is_empty() {
            return Err("table schema version requires a non-empty table name".to_string());
        }
        if !is_sha256_hex(&self.schema_digest) {
            return Err("table schema version digest must be a sha256 hex digest".to_string());
        }
        match (self.version, self.previous_version) {
            (0, None) => {}
            (0, Some(_)) => {
                return Err("version 0 must not declare a previous_version".to_string())
            }
            (v, Some(prev)) if prev == v - 1 => {}
            _ => {
                return Err(
                    "table schema version must chain to exactly version - 1".to_string(),
                )
            }
        }
        self.governance.validate()?;
        self.commit_fence.validate()
    }
}

/// One immutable manifest-referenced file (GOC-10 invariant 4: "file bytes are
/// immutable CAS/object refs").
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestFileRef {
    /// A CAS/object-store reference (never raw bytes, never a credential).
    pub object_ref: String,
    pub byte_len: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_count: Option<u64>,
    pub content_digest: String,
}

impl ManifestFileRef {
    pub fn validate(&self) -> Result<(), String> {
        if self.object_ref.trim().is_empty() {
            return Err("manifest file object_ref must not be empty".to_string());
        }
        if self.byte_len == 0 {
            return Err("manifest file byte_len must be nonzero".to_string());
        }
        if !is_sha256_hex(&self.content_digest) {
            return Err("manifest file content_digest must be a sha256 hex digest".to_string());
        }
        Ok(())
    }
}

/// One partition's durable file manifest (GOC-10 invariant 4).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PartitionManifestV1 {
    pub governance: GovernanceMeta,
    pub table: String,
    pub partition_key: String,
    pub files: Vec<ManifestFileRef>,
    /// SHA-256 hex digest over the canonical (sorted) file-ref list.
    pub manifest_digest: String,
    pub commit_fence: CommitFenceRef,
    pub created_at_ms: u64,
}

impl PartitionManifestV1 {
    pub fn validate(&self) -> Result<(), String> {
        if self.table.trim().is_empty() {
            return Err("partition manifest requires a non-empty table name".to_string());
        }
        if self.partition_key.trim().is_empty() {
            return Err("partition manifest requires a non-empty partition_key".to_string());
        }
        if self.files.is_empty() {
            return Err("partition manifest must reference at least one file".to_string());
        }
        for file in &self.files {
            file.validate()?;
        }
        if !is_sha256_hex(&self.manifest_digest) {
            return Err("partition manifest_digest must be a sha256 hex digest".to_string());
        }
        self.governance.validate()?;
        self.commit_fence.validate()
    }
}

/// A lightweight reference into a [`PartitionManifestV1`], used inside
/// [`LakeSnapshotV1`] so a snapshot never duplicates the full file list inline
/// (GOC-10 invariant 4).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PartitionManifestRef {
    pub manifest_digest: String,
    pub partition_key: String,
}

/// An immutable, pinned analytical snapshot of one table (GOC-10 invariant 4:
/// "`TableSnapshotV1` pins table/catalog versions, partition/file manifests,
/// commit sequence, quality checks, and policy digest"). Consumed by GOC-11
/// (analytics jobs) and GOC-12 (cross-modal predicate pushdown reads the schema
/// digest + quality status, never a raw partition scan).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LakeSnapshotV1 {
    pub governance: GovernanceMeta,
    pub table: String,
    pub snapshot_id: String,
    pub schema_version: u64,
    pub schema_digest: String,
    pub partitions: Vec<PartitionManifestRef>,
    /// Monotonic per-table commit sequence at pin time.
    pub commit_sequence: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quality: Option<QualityReportRef>,
    /// SHA-256 hex digest over classification + retention + purpose at pin time,
    /// so a later governance change is provably distinguishable from the exact
    /// policy state a consumer read the snapshot under.
    pub policy_digest: String,
    pub commit_fence: CommitFenceRef,
    pub pinned_at_ms: u64,
}

impl LakeSnapshotV1 {
    pub fn validate(&self) -> Result<(), String> {
        if self.table.trim().is_empty() {
            return Err("lake snapshot requires a non-empty table name".to_string());
        }
        if self.snapshot_id.trim().is_empty() {
            return Err("lake snapshot requires a non-empty snapshot_id".to_string());
        }
        if !is_sha256_hex(&self.schema_digest) {
            return Err("lake snapshot schema_digest must be a sha256 hex digest".to_string());
        }
        if !is_sha256_hex(&self.policy_digest) {
            return Err("lake snapshot policy_digest must be a sha256 hex digest".to_string());
        }
        for partition in &self.partitions {
            if !is_sha256_hex(&partition.manifest_digest) {
                return Err(
                    "lake snapshot partition manifest_digest must be a sha256 hex digest"
                        .to_string(),
                );
            }
        }
        if let Some(quality) = &self.quality {
            quality.validate()?;
        }
        self.governance.validate()?;
        self.commit_fence.validate()
    }
}

/// One durable table/catalog entry — the row a served SQL catalog and a durable
/// Iceberg-REST catalog projection both resolve to (GOC-10 invariant 2 + the
/// design note "`IcebergRestCatalog` becomes a durable projection of catalog
/// authority, not a standalone `BTreeMap`").
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogEntryV1 {
    pub governance: GovernanceMeta,
    pub namespace: String,
    pub table: String,
    pub current_schema_version: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_snapshot_id: Option<String>,
    pub commit_fence: CommitFenceRef,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quality: Option<QualityReportRef>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

impl CatalogEntryV1 {
    pub fn validate(&self) -> Result<(), String> {
        if self.namespace.trim().is_empty() {
            return Err("catalog entry requires a non-empty namespace".to_string());
        }
        if self.table.trim().is_empty() {
            return Err("catalog entry requires a non-empty table name".to_string());
        }
        if self.updated_at_ms < self.created_at_ms {
            return Err("catalog entry updated_at_ms must not precede created_at_ms".to_string());
        }
        if let Some(quality) = &self.quality {
            quality.validate()?;
        }
        self.governance.validate()?;
        self.commit_fence.validate()
    }
}

/// One durable schema-evolution audit record (GOC-10 invariant 6, migration
/// section: "no destructive schema change is applied without a new version and
/// policy approval").
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableChangeV1 {
    pub governance: GovernanceMeta,
    pub table: String,
    pub change: SchemaChangeKind,
    /// The change's compatibility classification. Must equal
    /// `classify_schema_change(change)` — [`Self::validate`] rejects a caller
    /// that mislabels a destructive change as additive/compatible.
    pub compatibility: SchemaCompatibility,
    pub from_version: u64,
    pub to_version: u64,
    /// Opaque principal reference recording who approved a destructive change.
    /// Required (and only meaningful) when `compatibility == Destructive`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approved_by: Option<String>,
    pub commit_fence: CommitFenceRef,
    pub recorded_at_ms: u64,
}

impl TableChangeV1 {
    pub fn validate(&self) -> Result<(), String> {
        if self.table.trim().is_empty() {
            return Err("table change requires a non-empty table name".to_string());
        }
        if self.to_version != self.from_version + 1 {
            return Err("table change to_version must be exactly from_version + 1".to_string());
        }
        let expected = classify_schema_change(self.change);
        if expected != self.compatibility {
            return Err(format!(
                "table change compatibility {:?} does not match the {:?} classification of {:?}",
                self.compatibility, expected, self.change
            ));
        }
        if self.compatibility == SchemaCompatibility::Destructive {
            if self.approved_by.is_none() {
                return Err(
                    "a destructive schema change requires policy approval (approved_by)"
                        .to_string(),
                );
            }
            if self.governance.retention.legal_hold {
                return Err(
                    "a destructive schema change is rejected while a legal hold is active"
                        .to_string(),
                );
            }
        }
        self.governance.validate()?;
        self.commit_fence.validate()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::msgpack::{decode_bounded, MsgpackLimits};

    fn limits() -> MsgpackLimits {
        MsgpackLimits::new(1024 * 1024, 10_000, 32)
    }

    fn round_trip<T>(value: &T) -> T
    where
        T: Serialize + serde::de::DeserializeOwned,
    {
        let bytes = rmp_serde::to_vec_named(value).expect("encode");
        decode_bounded::<T>(&bytes, limits()).expect("decode")
    }

    fn governance() -> GovernanceMeta {
        GovernanceMeta {
            tenant: "tenant-a".to_string(),
            owner: "owner-a".to_string(),
            classification: DataClassification::Internal,
            purpose: Some("unit-test".to_string()),
            retention: RetentionPolicy::active(),
        }
    }

    fn fence() -> CommitFenceRef {
        CommitFenceRef {
            batch_id: "batch-1".to_string(),
            tenant: "tenant-a".to_string(),
            placement_epoch: 1,
            fencing_token: 1,
        }
    }

    /// A 64-char lowercase hex digest fixture, distinguishable by `byte`.
    fn digest(byte: u8) -> String {
        [byte; 32].iter().map(|b| format!("{b:02x}")).collect()
    }

    // ── schema version ────────────────────────────────────────────────────

    #[test]
    fn schema_version_round_trips_over_the_wire() {
        let version = TableSchemaVersionV1 {
            governance: governance(),
            table: "metrics".to_string(),
            version: 1,
            schema_digest: digest(0xaa),
            previous_version: Some(0),
            commit_fence: fence(),
            created_at_ms: 1000,
        };
        version.validate().unwrap();
        assert_eq!(round_trip(&version), version);
    }

    #[test]
    fn schema_version_zero_must_not_declare_a_predecessor() {
        let mut version = TableSchemaVersionV1 {
            governance: governance(),
            table: "metrics".to_string(),
            version: 0,
            schema_digest: digest(0xaa),
            previous_version: Some(0),
            commit_fence: fence(),
            created_at_ms: 1000,
        };
        assert!(version.validate().is_err());
        version.previous_version = None;
        assert!(version.validate().is_ok());
    }

    #[test]
    fn schema_version_rejects_a_broken_chain() {
        // Known-bad input: version 3 claiming predecessor 0 skips 1 and 2.
        let version = TableSchemaVersionV1 {
            governance: governance(),
            table: "metrics".to_string(),
            version: 3,
            schema_digest: digest(0xaa),
            previous_version: Some(0),
            commit_fence: fence(),
            created_at_ms: 1000,
        };
        let error = version.validate().unwrap_err();
        assert!(error.contains("chain"));
    }

    #[test]
    fn schema_version_rejects_a_malformed_digest() {
        let version = TableSchemaVersionV1 {
            governance: governance(),
            table: "metrics".to_string(),
            version: 0,
            schema_digest: "not-a-digest".to_string(),
            previous_version: None,
            commit_fence: fence(),
            created_at_ms: 1000,
        };
        assert!(version.validate().is_err());
    }

    // ── schema change / compatibility classification ────────────────────────

    #[test]
    fn classify_schema_change_covers_every_variant() {
        assert_eq!(
            classify_schema_change(SchemaChangeKind::AddColumn),
            SchemaCompatibility::Additive
        );
        assert_eq!(
            classify_schema_change(SchemaChangeKind::RenameColumn),
            SchemaCompatibility::Compatible
        );
        assert_eq!(
            classify_schema_change(SchemaChangeKind::RenameTable),
            SchemaCompatibility::Compatible
        );
        assert_eq!(
            classify_schema_change(SchemaChangeKind::AddConstraint),
            SchemaCompatibility::Compatible
        );
        assert_eq!(
            classify_schema_change(SchemaChangeKind::DropConstraint),
            SchemaCompatibility::Compatible
        );
        assert_eq!(
            classify_schema_change(SchemaChangeKind::DropColumn),
            SchemaCompatibility::Destructive
        );
        assert_eq!(
            classify_schema_change(SchemaChangeKind::DropTable),
            SchemaCompatibility::Destructive
        );
        assert_eq!(
            classify_schema_change(SchemaChangeKind::AlterColumnType),
            SchemaCompatibility::Destructive
        );
    }

    fn change(kind: SchemaChangeKind, compatibility: SchemaCompatibility) -> TableChangeV1 {
        TableChangeV1 {
            governance: governance(),
            table: "metrics".to_string(),
            change: kind,
            compatibility,
            from_version: 0,
            to_version: 1,
            approved_by: None,
            commit_fence: fence(),
            recorded_at_ms: 1000,
        }
    }

    #[test]
    fn additive_change_round_trips_without_approval() {
        let record = change(SchemaChangeKind::AddColumn, SchemaCompatibility::Additive);
        record.validate().unwrap();
        assert_eq!(round_trip(&record), record);
    }

    #[test]
    fn destructive_change_without_approval_is_rejected() {
        // Known-bad input: DROP COLUMN with no approver recorded.
        let record = change(
            SchemaChangeKind::DropColumn,
            SchemaCompatibility::Destructive,
        );
        let error = record.validate().unwrap_err();
        assert!(error.contains("policy approval"));
    }

    #[test]
    fn destructive_change_with_approval_passes() {
        let mut record = change(
            SchemaChangeKind::DropColumn,
            SchemaCompatibility::Destructive,
        );
        record.approved_by = Some("principal:sha256:approver".to_string());
        record.validate().unwrap();
    }

    #[test]
    fn destructive_change_is_rejected_under_legal_hold_even_with_approval() {
        // Known-bad input: an approved DROP TABLE while the table is under legal hold.
        let mut record = change(SchemaChangeKind::DropTable, SchemaCompatibility::Destructive);
        record.approved_by = Some("principal:sha256:approver".to_string());
        record.governance.retention.legal_hold = true;
        let error = record.validate().unwrap_err();
        assert!(error.contains("legal hold"));
    }

    #[test]
    fn mislabeled_compatibility_is_rejected() {
        // Known-bad input: caller claims a DROP COLUMN is merely Additive.
        let record = change(SchemaChangeKind::DropColumn, SchemaCompatibility::Additive);
        let error = record.validate().unwrap_err();
        assert!(error.contains("does not match"));
    }

    #[test]
    fn non_contiguous_versions_are_rejected() {
        let mut record = change(SchemaChangeKind::AddColumn, SchemaCompatibility::Additive);
        record.to_version = 5;
        assert!(record.validate().is_err());
    }

    // ── manifest / partition ─────────────────────────────────────────────

    fn file(byte: u8) -> ManifestFileRef {
        ManifestFileRef {
            object_ref: "s3://lakehouse/metrics/part-0.parquet".to_string(),
            byte_len: 128,
            row_count: Some(4),
            content_digest: digest(byte),
        }
    }

    #[test]
    fn partition_manifest_round_trips_over_the_wire() {
        let manifest = PartitionManifestV1 {
            governance: governance(),
            table: "metrics".to_string(),
            partition_key: "dt=2026-08-16".to_string(),
            files: vec![file(0x01), file(0x02)],
            manifest_digest: digest(0xbb),
            commit_fence: fence(),
            created_at_ms: 1000,
        };
        manifest.validate().unwrap();
        assert_eq!(round_trip(&manifest), manifest);
    }

    #[test]
    fn partition_manifest_rejects_an_empty_file_list() {
        // Known-bad input: a manifest that references zero files.
        let manifest = PartitionManifestV1 {
            governance: governance(),
            table: "metrics".to_string(),
            partition_key: "dt=2026-08-16".to_string(),
            files: Vec::new(),
            manifest_digest: digest(0xbb),
            commit_fence: fence(),
            created_at_ms: 1000,
        };
        let error = manifest.validate().unwrap_err();
        assert!(error.contains("at least one file"));
    }

    #[test]
    fn manifest_file_rejects_a_zero_byte_length() {
        // Known-bad input: a manifest file claiming zero bytes.
        let mut bad = file(0x01);
        bad.byte_len = 0;
        assert!(bad.validate().is_err());
    }

    // ── snapshot / catalog entry ──────────────────────────────────────────

    fn snapshot() -> LakeSnapshotV1 {
        LakeSnapshotV1 {
            governance: governance(),
            table: "metrics".to_string(),
            snapshot_id: "snap-1".to_string(),
            schema_version: 1,
            schema_digest: digest(0xaa),
            partitions: vec![PartitionManifestRef {
                manifest_digest: digest(0xbb),
                partition_key: "dt=2026-08-16".to_string(),
            }],
            commit_sequence: 7,
            quality: Some(QualityReportRef {
                report_id: "qr-1".to_string(),
                status: QualityStatus::Passing,
                checked_at_ms: 999,
                failed_checks: Vec::new(),
            }),
            policy_digest: digest(0xcc),
            commit_fence: fence(),
            pinned_at_ms: 1000,
        }
    }

    #[test]
    fn lake_snapshot_round_trips_over_the_wire() {
        let snap = snapshot();
        snap.validate().unwrap();
        assert_eq!(round_trip(&snap), snap);
    }

    #[test]
    fn lake_snapshot_rejects_a_failing_report_with_no_recorded_failure() {
        // Known-bad input: status Failing but failed_checks is empty.
        let mut snap = snapshot();
        snap.quality = Some(QualityReportRef {
            report_id: "qr-2".to_string(),
            status: QualityStatus::Failing,
            checked_at_ms: 999,
            failed_checks: Vec::new(),
        });
        let error = snap.validate().unwrap_err();
        assert!(error.contains("failed check"));
    }

    #[test]
    fn lake_snapshot_rejects_a_malformed_policy_digest() {
        let mut snap = snapshot();
        snap.policy_digest = "short".to_string();
        assert!(snap.validate().is_err());
    }

    #[test]
    fn catalog_entry_round_trips_over_the_wire() {
        let entry = CatalogEntryV1 {
            governance: governance(),
            namespace: "analytics".to_string(),
            table: "metrics".to_string(),
            current_schema_version: 1,
            current_snapshot_id: Some("snap-1".to_string()),
            commit_fence: fence(),
            quality: None,
            created_at_ms: 1000,
            updated_at_ms: 2000,
        };
        entry.validate().unwrap();
        assert_eq!(round_trip(&entry), entry);
    }

    #[test]
    fn catalog_entry_rejects_updated_before_created() {
        // Known-bad input: updated_at_ms precedes created_at_ms.
        let entry = CatalogEntryV1 {
            governance: governance(),
            namespace: "analytics".to_string(),
            table: "metrics".to_string(),
            current_schema_version: 1,
            current_snapshot_id: None,
            commit_fence: fence(),
            quality: None,
            created_at_ms: 2000,
            updated_at_ms: 1000,
        };
        assert!(entry.validate().is_err());
    }

    // ── governance / retention ────────────────────────────────────────────

    #[test]
    fn retention_rejects_deletion_under_legal_hold() {
        // Known-bad input: deleted=true and legal_hold=true simultaneously.
        let retention = RetentionPolicy {
            retain_until_ms: None,
            legal_hold: true,
            deletion_requested_at_ms: Some(1),
            deleted: true,
        };
        let error = retention.validate().unwrap_err();
        assert!(error.contains("legal hold"));
    }

    #[test]
    fn governance_rejects_an_empty_tenant() {
        let mut meta = governance();
        meta.tenant = String::new();
        assert!(meta.validate().is_err());
    }

    #[test]
    fn wire_encoding_is_deterministic() {
        let snap = snapshot();
        let first = rmp_serde::to_vec_named(&snap).unwrap();
        let second = rmp_serde::to_vec_named(&snap).unwrap();
        assert_eq!(first, second);
    }
}
