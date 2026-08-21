//! Durable, forward-only schema migration contracts for user tables.
//!
//! A migration is a signed (by digest, not by a secret key) description of one
//! ordered schema transition.  The [`TableStore`] applies the description and
//! the schema change in one redb write transaction; this module owns the pure
//! plan, checksum, and compatibility contract.  Keeping the contract here
//! avoids making a SQL parser or a server handler the authority for schema
//! history.
//!
//! The important invariants are deliberately strict:
//!
//! * a migration has one immutable identity and one deterministic checksum;
//! * versions advance by exactly one, with an explicit compare-and-swap
//!   precondition on the previous version and schema digest;
//! * replaying an already applied identity is a durable no-op, while an identity
//!   with different bytes is rejected;
//! * destructive operations and lossy coercions require an explicit policy;
//! * rollback metadata is descriptive and forward-only.  There is no unsafe
//!   inverse/down-migration operation;
//! * RLS and secondary-index boundaries are explicit.  The store fails closed
//!   when an affected local index or an unacknowledged RLS revalidation is
//!   encountered.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::schema::{Column, ColumnType, TableConstraint, TableSchema};

/// Version of the serialized migration contract.
pub const SCHEMA_MIGRATION_FORMAT_VERSION: u16 = 1;
/// Bound the amount of work one migration can request from the write path.
pub const MAX_SCHEMA_MIGRATION_OPERATIONS: usize = 128;
/// Bound identity and dependency strings persisted in the catalog.
pub const MAX_SCHEMA_MIGRATION_TEXT: usize = 4096;

/// A point-in-time schema reader token.  It is intentionally small enough to
/// pass through a planner/request and is checked again by the migration CAS.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaSnapshot {
    pub tenant_scope: String,
    pub table: String,
    pub version: u64,
    pub schema_digest: String,
}

/// How a migration may interact with dependent secondary indexes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum SecondaryIndexPolicy {
    /// Reject a plan that affects a column carrying a registered index.  The
    /// caller must explicitly drop/rebuild that index in a separate governed
    /// operation before retrying the migration.
    #[default]
    RejectAffected,
    /// An explicit acknowledgement that an index rebuild is coordinated by the
    /// caller.  The store still verifies the local catalog and never silently
    /// drops an index; the rebuild must be visible in the surrounding plan.
    RebuildByCaller,
}

/// Policy switches that make destructive or cross-authority changes explicit.
/// The policy is part of the checksummed migration identity, so retrying with a
/// weaker or stronger policy cannot silently change what an existing ID means.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationPolicy {
    /// Permit DROP COLUMN and DROP CONSTRAINT operations.
    pub allow_destructive: bool,
    /// Permit an ALTER COLUMN TYPE operation whose caller has marked the
    /// conversion as potentially lossy.
    pub allow_lossy_coercion: bool,
    /// Compatibility action for an affected secondary index.
    #[serde(default)]
    pub secondary_indexes: SecondaryIndexPolicy,
    /// Require an external RLS authority to revalidate the resulting schema.
    /// The engine has no second RLS policy editor, so the binding digest must be
    /// supplied as evidence and is retained in the migration record.
    #[serde(default)]
    pub require_rls_revalidation: bool,
    /// Opaque digest of the RLS policy binding observed by the caller.  Raw
    /// policy text and credentials never enter the migration catalog.
    #[serde(default)]
    pub rls_binding_digest: Option<String>,
}

impl Default for MigrationPolicy {
    fn default() -> Self {
        Self {
            allow_destructive: false,
            allow_lossy_coercion: false,
            secondary_indexes: SecondaryIndexPolicy::RejectAffected,
            require_rls_revalidation: false,
            rls_binding_digest: None,
        }
    }
}

/// Forward-only recovery metadata.  A failed migration leaves no record; an
/// applied migration records the previous point so an operator can restore from
/// a governed snapshot.  The contract intentionally does not contain inverse
/// operations or a down-migration flag.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RollbackMetadata {
    /// Must remain true.  It is checked on every decode/restart verification.
    pub forward_only: bool,
    /// Version that was current before this migration.
    pub prior_schema_version: u64,
    /// Digest of the schema that was current before this migration.
    pub prior_schema_digest: String,
    /// Optional operator-owned snapshot/checkpoint reference; never raw data.
    pub restore_checkpoint: Option<String>,
    /// Human/audit explanation for why recovery is snapshot-based.
    pub reason: String,
}

impl RollbackMetadata {
    fn for_prior(version: u64, digest: &str) -> Self {
        Self {
            forward_only: true,
            prior_schema_version: version,
            prior_schema_digest: digest.to_string(),
            restore_checkpoint: None,
            reason: "forward-only migration; recover with a governed snapshot".to_string(),
        }
    }
}

/// One schema operation.  Operations are intentionally table-local and map to
/// the existing transactional store helpers; no parser or DataFusion plan is
/// embedded in durable migration state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SchemaMigrationOperation {
    AddColumn {
        column: Column,
    },
    DropColumn {
        column: String,
    },
    RenameColumn {
        from: String,
        to: String,
    },
    AlterColumnType {
        column: String,
        new_type: ColumnType,
        /// True when the conversion may lose information.  It is rejected
        /// unless [`MigrationPolicy::allow_lossy_coercion`] is true.
        lossy: bool,
    },
    AddConstraint {
        constraint: TableConstraint,
    },
    DropConstraint {
        constraint: String,
    },
}

impl SchemaMigrationOperation {
    /// Columns whose physical or semantic representation can invalidate a
    /// dependent index or relationship.
    pub fn affected_columns(&self) -> Vec<&str> {
        match self {
            Self::AddColumn { .. } => Vec::new(),
            Self::DropColumn { column } | Self::AlterColumnType { column, .. } => {
                vec![column.as_str()]
            }
            Self::RenameColumn { from, to } => vec![from.as_str(), to.as_str()],
            Self::AddConstraint { constraint } => match constraint {
                TableConstraint::PrimaryKey { columns, .. }
                | TableConstraint::Unique { columns, .. }
                | TableConstraint::ForeignKey { columns, .. } => {
                    columns.iter().map(String::as_str).collect()
                }
                TableConstraint::Check { .. } => Vec::new(),
            },
            Self::DropConstraint { .. } => Vec::new(),
        }
    }

    fn is_destructive(&self) -> bool {
        matches!(self, Self::DropColumn { .. } | Self::DropConstraint { .. })
    }

    fn is_lossy(&self) -> bool {
        matches!(self, Self::AlterColumnType { lossy: true, .. })
    }
}

/// Conservative type-conversion classification used in addition to the
/// caller-supplied `lossy` marker.  A plan cannot label a narrowing conversion
/// lossless merely to bypass the explicit policy gate.
pub fn conversion_may_be_lossy(from: ColumnType, to: ColumnType) -> bool {
    if from == to {
        return false;
    }
    !matches!(
        (from, to),
        (ColumnType::Int, ColumnType::BigInt) | (ColumnType::Float, ColumnType::Double)
    )
}

/// Immutable migration description.  Construct it with [`Self::for_schema`],
/// which computes the target digest and checksum from a current schema, or use
/// [`Self::draft`] followed by [`Self::seal_for`] when a caller needs to stage a
/// plan before it is submitted.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SchemaMigration {
    pub format_version: u16,
    pub migration_id: String,
    pub tenant_scope: String,
    pub table: String,
    pub expected_schema_version: u64,
    pub expected_schema_digest: String,
    pub target_schema_version: u64,
    pub target_schema_digest: String,
    pub operations: Vec<SchemaMigrationOperation>,
    pub policy: MigrationPolicy,
    pub rollback: RollbackMetadata,
    /// SHA-256 over the canonical migration payload excluding this field.
    pub checksum: String,
}

/// Durable applied migration record.  Keeping a wrapper leaves room for
/// terminal state metadata without changing the immutable migration bytes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SchemaMigrationRecord {
    pub migration: SchemaMigration,
    pub state: MigrationState,
    pub applied_schema_version: u64,
    /// Monotonic catalog-wide version assigned at the same commit as this
    /// table transition.  It invalidates query/catalog readers even when no
    /// row-domain mutation counter changes.
    pub catalog_version: u64,
}

/// Only forward application is supported today.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationState {
    Applied,
}

/// Result of applying a migration.  `replayed` is true when the durable record
/// already matched exactly and no schema operation was executed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaMigrationApply {
    pub migration_id: String,
    pub schema_version: u64,
    pub catalog_version: u64,
    pub schema_digest: String,
    pub replayed: bool,
}

impl SchemaMigration {
    /// Build an unsealed plan.  Call [`Self::seal_for`] before submission.
    pub fn draft(
        migration_id: impl Into<String>,
        tenant_scope: impl Into<String>,
        table: impl Into<String>,
        expected_schema_version: u64,
        expected_schema_digest: impl Into<String>,
        operations: Vec<SchemaMigrationOperation>,
        policy: MigrationPolicy,
    ) -> Result<Self, String> {
        let migration = Self {
            format_version: SCHEMA_MIGRATION_FORMAT_VERSION,
            migration_id: migration_id.into(),
            tenant_scope: tenant_scope.into(),
            table: table.into(),
            expected_schema_version,
            expected_schema_digest: expected_schema_digest.into(),
            target_schema_version: expected_schema_version
                .checked_add(1)
                .ok_or_else(|| "schema migration version overflow".to_string())?,
            target_schema_digest: String::new(),
            operations,
            policy,
            rollback: RollbackMetadata::for_prior(expected_schema_version, "unsealed"),
            checksum: String::new(),
        };
        migration.validate_identity(false)?;
        Ok(migration)
    }

    /// Build and seal one migration against `current_schema`.
    pub fn for_schema(
        migration_id: impl Into<String>,
        tenant_scope: impl Into<String>,
        expected_schema_version: u64,
        current_schema: &TableSchema,
        operations: Vec<SchemaMigrationOperation>,
        policy: MigrationPolicy,
    ) -> Result<Self, String> {
        let mut migration = Self::draft(
            migration_id,
            tenant_scope,
            current_schema.name.clone(),
            expected_schema_version,
            current_schema.schema_digest()?,
            operations,
            policy,
        )?;
        migration.seal_for(current_schema)?;
        Ok(migration)
    }

    /// Compute the target shape, rollback metadata, and immutable checksum.
    pub fn seal_for(&mut self, current_schema: &TableSchema) -> Result<(), String> {
        self.validate_identity(false)?;
        if current_schema.name != self.table {
            return Err(format!(
                "schema migration table binding mismatch: plan targets `{}` but current schema is `{}`",
                self.table, current_schema.name
            ));
        }
        let current_digest = current_schema.schema_digest()?;
        if current_digest != self.expected_schema_digest {
            return Err("schema migration cannot seal against a stale schema digest".to_string());
        }
        self.validate_type_policies(current_schema)?;
        let projected = self.projected_schema(current_schema)?;
        self.target_schema_digest = projected.schema_digest()?;
        self.rollback =
            RollbackMetadata::for_prior(self.expected_schema_version, &self.expected_schema_digest);
        self.checksum = self.compute_checksum()?;
        self.validate_identity(true)
    }

    /// Return the schema resulting from applying the pure operation list.  The
    /// store separately applies row coercions and relationship checks inside the
    /// same write transaction.
    pub fn projected_schema(&self, current_schema: &TableSchema) -> Result<TableSchema, String> {
        if current_schema.name != self.table {
            return Err("schema migration table binding mismatch".to_string());
        }
        let mut projected = current_schema.clone();
        for operation in &self.operations {
            match operation {
                SchemaMigrationOperation::AddColumn { column } => {
                    if projected.column(&column.name).is_some() {
                        return Err(format!(
                            "column `{}` already exists in table `{}`",
                            column.name, self.table
                        ));
                    }
                    projected.columns_mut().push(column.clone());
                    if column.primary_key {
                        projected.columns_mut().last_mut().unwrap().nullable = false;
                    }
                }
                SchemaMigrationOperation::DropColumn { column } => {
                    let index = projected.column_index(column).ok_or_else(|| {
                        format!("column `{column}` does not exist in table `{}`", self.table)
                    })?;
                    if projected.columns().len() == 1 {
                        return Err(format!(
                            "cannot drop the only column `{column}` of table `{}`",
                            self.table
                        ));
                    }
                    projected.columns_mut().remove(index);
                }
                SchemaMigrationOperation::RenameColumn { from, to } => {
                    if from != to && projected.column(to).is_some() {
                        return Err(format!(
                            "column `{to}` already exists in table `{}`",
                            self.table
                        ));
                    }
                    let index = projected.column_index(from).ok_or_else(|| {
                        format!("column `{from}` does not exist in table `{}`", self.table)
                    })?;
                    projected.columns_mut()[index].name = to.clone();
                }
                SchemaMigrationOperation::AlterColumnType {
                    column, new_type, ..
                } => {
                    let index = projected.column_index(column).ok_or_else(|| {
                        format!("column `{column}` does not exist in table `{}`", self.table)
                    })?;
                    projected.columns_mut()[index].ty = *new_type;
                }
                SchemaMigrationOperation::AddConstraint { constraint } => {
                    projected.push_constraint(constraint.clone());
                    force_primary_key_not_null(&mut projected);
                }
                SchemaMigrationOperation::DropConstraint { constraint } => {
                    if !remove_constraint_like_store(&mut projected, constraint) {
                        return Err(format!(
                            "constraint `{constraint}` does not exist on table `{}`",
                            self.table
                        ));
                    }
                }
            }
            projected.validate()?;
        }
        Ok(projected)
    }

    /// Verify all static invariants and the checksum.  `sealed=false` is used
    /// only while constructing a draft.
    pub fn validate(&self) -> Result<(), String> {
        self.validate_identity(true)
    }

    /// Return the canonical checksum expected for the current fields.
    pub fn expected_checksum(&self) -> Result<String, String> {
        self.compute_checksum()
    }

    /// Return true only when the supplied checksum matches the canonical bytes.
    pub fn verify_checksum(&self) -> Result<(), String> {
        let expected = self.compute_checksum()?;
        if self.checksum != expected {
            return Err(format!(
                "schema migration `{}` checksum drift: expected {}, got {}",
                self.migration_id, expected, self.checksum
            ));
        }
        Ok(())
    }

    /// Check conversions against the schema shape at each ordered step.  This
    /// catches a caller that marks a narrowing conversion as lossless and also
    /// handles an add-then-alter sequence in one migration.
    pub fn validate_type_policies(&self, current_schema: &TableSchema) -> Result<(), String> {
        let mut shape = current_schema.clone();
        for operation in &self.operations {
            match operation {
                SchemaMigrationOperation::AddColumn { column } => {
                    shape.columns_mut().push(column.clone());
                }
                SchemaMigrationOperation::DropColumn { column } => {
                    if let Some(index) = shape.column_index(column) {
                        shape.columns_mut().remove(index);
                    }
                }
                SchemaMigrationOperation::RenameColumn { from, to } => {
                    if let Some(index) = shape.column_index(from) {
                        shape.columns_mut()[index].name = to.clone();
                    }
                }
                SchemaMigrationOperation::AlterColumnType {
                    column,
                    new_type,
                    lossy,
                } => {
                    let old_type = shape
                        .column(column)
                        .ok_or_else(|| format!("column `{column}` does not exist"))?
                        .ty;
                    let inferred_loss = conversion_may_be_lossy(old_type, *new_type);
                    if inferred_loss && !lossy {
                        return Err(format!(
                            "ALTER COLUMN `{column}` conversion from {:?} to {:?} must be marked lossy",
                            old_type, new_type
                        ));
                    }
                    if inferred_loss && !self.policy.allow_lossy_coercion {
                        return Err(format!(
                            "ALTER COLUMN `{column}` conversion from {:?} to {:?} requires allow_lossy_coercion=true",
                            old_type, new_type
                        ));
                    }
                    let index = shape
                        .column_index(column)
                        .ok_or_else(|| format!("column `{column}` does not exist"))?;
                    shape.columns_mut()[index].ty = *new_type;
                }
                SchemaMigrationOperation::AddConstraint { .. }
                | SchemaMigrationOperation::DropConstraint { .. } => {}
            }
        }
        Ok(())
    }

    fn validate_identity(&self, sealed: bool) -> Result<(), String> {
        if self.format_version != SCHEMA_MIGRATION_FORMAT_VERSION {
            return Err(format!(
                "unsupported schema migration format version {}",
                self.format_version
            ));
        }
        for (kind, value) in [
            ("migration id", self.migration_id.as_str()),
            ("tenant scope", self.tenant_scope.as_str()),
            ("table", self.table.as_str()),
        ] {
            validate_text(kind, value)?;
        }
        if self.expected_schema_digest.len() != 64
            || !self
                .expected_schema_digest
                .bytes()
                .all(|c| c.is_ascii_hexdigit())
        {
            return Err("schema migration expected digest is not a SHA-256 hex value".to_string());
        }
        let expected_target = self
            .expected_schema_version
            .checked_add(1)
            .ok_or_else(|| "schema migration version overflow".to_string())?;
        if self.target_schema_version != expected_target {
            return Err(format!(
                "schema migration versions must advance by one (expected {}, got {})",
                expected_target, self.target_schema_version
            ));
        }
        if self.operations.is_empty() {
            return Err("schema migration must contain at least one operation".to_string());
        }
        if self.operations.len() > MAX_SCHEMA_MIGRATION_OPERATIONS {
            return Err("schema migration operation bound exceeded".to_string());
        }
        if self.policy.require_rls_revalidation {
            let digest = self.policy.rls_binding_digest.as_deref().ok_or_else(|| {
                "schema migration requires RLS revalidation but carries no binding digest"
                    .to_string()
            })?;
            validate_digest("RLS binding", digest)?;
        } else if self.policy.rls_binding_digest.is_some() {
            return Err("an RLS binding digest requires require_rls_revalidation=true".to_string());
        }
        for operation in &self.operations {
            validate_operation(operation)?;
            if operation.is_destructive() && !self.policy.allow_destructive {
                return Err(
                    "destructive schema migration operation requires allow_destructive=true"
                        .to_string(),
                );
            }
            if operation.is_lossy() && !self.policy.allow_lossy_coercion {
                return Err(
                    "lossy schema migration coercion requires allow_lossy_coercion=true"
                        .to_string(),
                );
            }
        }
        if sealed {
            validate_digest("target schema", &self.target_schema_digest)?;
            validate_digest("migration checksum", &self.checksum)?;
            if !self.rollback.forward_only {
                return Err("schema migration rollback metadata must be forward-only".to_string());
            }
            if self.rollback.prior_schema_version != self.expected_schema_version
                || self.rollback.prior_schema_digest != self.expected_schema_digest
            {
                return Err(
                    "schema migration rollback metadata does not match its CAS precondition"
                        .to_string(),
                );
            }
            validate_text("rollback reason", &self.rollback.reason)?;
            if let Some(checkpoint) = &self.rollback.restore_checkpoint {
                validate_text("restore checkpoint", checkpoint)?;
            }
            self.verify_checksum()?;
        }
        Ok(())
    }

    fn compute_checksum(&self) -> Result<String, String> {
        #[derive(Serialize)]
        struct UnsignedMigration<'a> {
            format_version: u16,
            migration_id: &'a str,
            tenant_scope: &'a str,
            table: &'a str,
            expected_schema_version: u64,
            expected_schema_digest: &'a str,
            target_schema_version: u64,
            target_schema_digest: &'a str,
            operations: &'a [SchemaMigrationOperation],
            policy: &'a MigrationPolicy,
            rollback: &'a RollbackMetadata,
        }
        let payload = UnsignedMigration {
            format_version: self.format_version,
            migration_id: &self.migration_id,
            tenant_scope: &self.tenant_scope,
            table: &self.table,
            expected_schema_version: self.expected_schema_version,
            expected_schema_digest: &self.expected_schema_digest,
            target_schema_version: self.target_schema_version,
            target_schema_digest: &self.target_schema_digest,
            operations: &self.operations,
            policy: &self.policy,
            rollback: &self.rollback,
        };
        let encoded = rmp_serde::to_vec_named(&payload)
            .map_err(|error| format!("encode schema migration checksum payload: {error}"))?;
        let mut hasher = Sha256::new();
        hasher.update(b"epistemic-graph/schema-migration-v1\0");
        hasher.update(encoded);
        Ok(hex::encode(hasher.finalize()))
    }
}

fn validate_text(kind: &str, value: &str) -> Result<(), String> {
    if value.is_empty() || value.len() > MAX_SCHEMA_MIGRATION_TEXT || value.contains('\0') {
        return Err(format!(
            "schema migration {kind} is empty, oversized, or contains NUL"
        ));
    }
    Ok(())
}

fn validate_digest(kind: &str, value: &str) -> Result<(), String> {
    if value.len() != 64 || !value.bytes().all(|c| c.is_ascii_hexdigit()) {
        return Err(format!(
            "schema migration {kind} is not a SHA-256 hex value"
        ));
    }
    Ok(())
}

fn validate_operation(operation: &SchemaMigrationOperation) -> Result<(), String> {
    let text_fields: Vec<(&str, &str)> = match operation {
        SchemaMigrationOperation::AddColumn { column } => vec![("column", column.name.as_str())],
        SchemaMigrationOperation::DropColumn { column }
        | SchemaMigrationOperation::AlterColumnType { column, .. } => {
            vec![("column", column.as_str())]
        }
        SchemaMigrationOperation::RenameColumn { from, to } => {
            vec![
                ("source column", from.as_str()),
                ("target column", to.as_str()),
            ]
        }
        SchemaMigrationOperation::AddConstraint { .. } => Vec::new(),
        SchemaMigrationOperation::DropConstraint { constraint } => {
            vec![("constraint", constraint.as_str())]
        }
    };
    for (kind, value) in text_fields {
        validate_text(kind, value)?;
    }
    if let SchemaMigrationOperation::AddColumn { column } = operation {
        if column.name.is_empty() {
            return Err("schema migration cannot add an unnamed column".to_string());
        }
    }
    Ok(())
}

fn force_primary_key_not_null(schema: &mut TableSchema) {
    let primary_key_columns: HashSet<String> = schema
        .constraints()
        .iter()
        .filter_map(|constraint| match constraint {
            TableConstraint::PrimaryKey { columns, .. } => Some(columns.iter().cloned()),
            _ => None,
        })
        .flatten()
        .collect();
    if primary_key_columns.is_empty() {
        return;
    }
    for column in schema.columns_mut() {
        if primary_key_columns.contains(&column.name) {
            column.nullable = false;
        }
    }
}

fn remove_constraint_like_store(schema: &mut TableSchema, name: &str) -> bool {
    if schema.remove_constraint_named(name) {
        return true;
    }
    if name == format!("{}_pkey", schema.name) {
        let mut removed = false;
        for column in schema.columns_mut() {
            if column.primary_key {
                column.primary_key = false;
                column.unique = false;
                removed = true;
            }
        }
        return removed;
    }
    let table = schema.name.clone();
    let mut removed = false;
    for column in schema.columns_mut() {
        if name == format!("{}_{}_key", table, column.name) && column.is_unique() {
            column.unique = false;
            column.primary_key = false;
            removed = true;
        } else if name == format!("{}_{}_check", table, column.name) && column.check.is_some() {
            column.check = None;
            removed = true;
        }
    }
    removed
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema() -> TableSchema {
        TableSchema::new(
            "events",
            vec![Column::new("id", ColumnType::BigInt, false, true)],
        )
    }

    #[test]
    fn sealed_identity_is_deterministic_and_replay_safe() {
        let first = SchemaMigration::for_schema(
            "events-add-source",
            "tenant-a",
            0,
            &schema(),
            vec![SchemaMigrationOperation::AddColumn {
                column: Column::new("source", ColumnType::Text, true, false),
            }],
            MigrationPolicy::default(),
        )
        .unwrap();
        let second = SchemaMigration::for_schema(
            "events-add-source",
            "tenant-a",
            0,
            &schema(),
            vec![SchemaMigrationOperation::AddColumn {
                column: Column::new("source", ColumnType::Text, true, false),
            }],
            MigrationPolicy::default(),
        )
        .unwrap();
        assert_eq!(first, second);
        assert_eq!(first.expected_checksum().unwrap(), first.checksum);
    }

    #[test]
    fn destructive_and_lossy_operations_require_explicit_policy() {
        let source = schema();
        let drop = SchemaMigration::draft(
            "drop-id",
            "tenant-a",
            "events",
            0,
            source.schema_digest().unwrap(),
            vec![SchemaMigrationOperation::DropColumn {
                column: "id".to_string(),
            }],
            MigrationPolicy::default(),
        );
        assert!(drop.is_err());

        let lossy = SchemaMigration::draft(
            "narrow-id",
            "tenant-a",
            "events",
            0,
            source.schema_digest().unwrap(),
            vec![SchemaMigrationOperation::AlterColumnType {
                column: "id".to_string(),
                new_type: ColumnType::Int,
                lossy: true,
            }],
            MigrationPolicy::default(),
        );
        assert!(lossy.is_err());
    }

    #[test]
    fn stale_digest_and_version_gaps_are_rejected() {
        let mut migration = SchemaMigration::for_schema(
            "events-add-source",
            "tenant-a",
            0,
            &schema(),
            vec![SchemaMigrationOperation::AddColumn {
                column: Column::new("source", ColumnType::Text, true, false),
            }],
            MigrationPolicy::default(),
        )
        .unwrap();
        migration.expected_schema_version = 3;
        assert!(migration.validate().is_err());

        let mut stale = migration.clone();
        stale.expected_schema_version = 0;
        stale.rollback.prior_schema_version = 0;
        stale.checksum = stale.expected_checksum().unwrap();
        assert!(stale
            .seal_for(&TableSchema::new(
                "events",
                vec![Column::new("id", ColumnType::BigInt, false, true)],
            ))
            .is_err());
    }
}
