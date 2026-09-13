//! SQL source reconciliation: the durable continuation checkpoint, the
//! bounded source-entity pages and the canonical source revision.

use super::batch::{semantic_digest, MetadataMutation};
use super::reconciliation_codec::{
    decode_reconciliation_checkpoint, encode_reconciliation_checkpoint,
};
use super::record::{decode_valid, row_bytes};
use super::{
    corrupt, ensure, kernel_error, refused, semantic_contract_error, SemanticCodeError,
    SemanticCodeStore,
};
use eg_storage::{SemanticIndexOwner, SEMANTIC_SOURCE_PROGRESS, SEMANTIC_SQL_SOURCES};
use eg_transaction::AdmittedMutation;
use eg_types::semantic_index::{
    SemanticBinding, SemanticDigest, SemanticSourceProgress, SemanticSqlSourceManifest,
};

/// One durable continuation for a source reconciliation generation.  The
/// continuation lives in the existing source-progress owner table under this
/// reserved entity key; it is deliberately not a second registry/table
/// authority.  Source entity ids are `semantic-sql-source:<digest>`, so this
/// key cannot collide with a source row accepted by the read port.
pub(super) const SEMANTIC_RECONCILIATION_CHECKPOINT_ENTITY: &str =
    "__semantic_source_reconciliation_checkpoint_v1__";

pub(super) const SEMANTIC_RECONCILIATION_CHECKPOINT_MAGIC: &[u8] =
    b"semantic-source-reconciliation/v1\0";

pub(super) const SEMANTIC_RECONCILIATION_MAX_CURSOR_BYTES: usize = 4096;

pub(super) const SEMANTIC_RECONCILIATION_MAX_REVISION_BYTES: usize = 256;

pub(super) const SEMANTIC_RECONCILIATION_MAX_ENTITY_BYTES: usize = 512;

/// The durable phase of a source reconciliation cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SemanticSourceReconciliationPhase {
    /// The authoritative SQL source is still being paged.  `source_cursor`
    /// is required and no tombstone proof may be used yet.
    Scanning,
    /// The complete source snapshot has been admitted and the prior durable
    /// source identity set is being paged for deletion tombstones.
    FinalizingTombstones,
}

/// A bounded, CAS-protected native continuation for SQL source reconciliation.
/// The source revision and complete snapshot receipt are persisted alongside
/// the cursors, so a crash resumes from the exact authority/epoch proof rather
/// than trusting a caller's in-memory page position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SemanticSourceReconciliationCheckpoint {
    pub(crate) source_wakeup_digest: SemanticDigest,
    pub(crate) source_revision: String,
    pub(crate) phase: SemanticSourceReconciliationPhase,
    pub(crate) source_cursor: Option<Vec<u8>>,
    pub(crate) prior_cursor: Option<String>,
    pub(crate) rows_seen: u64,
    pub(crate) source_bytes_seen: u64,
    pub(crate) pages_seen: u64,
    pub(crate) complete_snapshot_receipt_digest: Option<SemanticDigest>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct SqlSourceRevision<'a> {
    pub(super) authority: &'a str,
    pub(super) epoch: u64,
}

impl SemanticCodeStore {
    /// Read the one native reconciliation continuation for the current
    /// binding generation.  The generation/head check is part of the same
    /// serving snapshot as the checkpoint row, so a worker that resumes after
    /// a refresh cannot accidentally continue an obsolete source stream.
    pub(crate) fn read_source_reconciliation_checkpoint(
        &self,
        generation: u64,
    ) -> Result<Option<SemanticSourceReconciliationCheckpoint>, SemanticCodeError> {
        ensure(
            generation != 0,
            "source reconciliation generation must be nonzero",
        )?;
        let read = self.door.serving_read()?;
        let current = self
            .read_binding_in(&read)?
            .ok_or_else(|| refused("source reconciliation requires a durable binding head"))?;
        ensure(
            current.generation == generation,
            "source reconciliation checkpoint is for a stale binding generation",
        )?;
        let progress_rows = read
            .open_owner_table(SEMANTIC_SOURCE_PROGRESS)
            .map_err(kernel_error)?;
        row_bytes(progress_rows.get(self.reconciliation_key(generation)))?
            .map(|bytes| decode_reconciliation_checkpoint(&bytes))
            .transpose()
    }

    /// Write a reconciliation continuation using an admitted owner mutation.
    /// The `expected` row is compared inside that write, which makes a stale
    /// worker fail before changing source progress or publishing another
    /// continuation.  Repeating an already committed `next` is idempotent.
    pub(crate) fn write_source_reconciliation_checkpoint(
        &self,
        generation: u64,
        expected: Option<&SemanticSourceReconciliationCheckpoint>,
        next: &SemanticSourceReconciliationCheckpoint,
    ) -> Result<(), SemanticCodeError> {
        ensure(
            generation != 0,
            "source reconciliation generation must be nonzero",
        )?;
        let next_bytes = encode_reconciliation_checkpoint(next)?;
        let expected_bytes = expected.map(encode_reconciliation_checkpoint).transpose()?;
        let batch_id = format!(
            "semantic-index:reconciliation-checkpoint:{}",
            self.reconciliation_batch_digest(b"write\0", generation, &next_bytes)
        );
        let subject = format!("{}:{generation}", self.binding);
        let mutation = MetadataMutation {
            batch_id: &batch_id,
            event_type: "semantic_source_reconciliation_checkpoint",
            subject: &subject,
            mutation_digest: semantic_digest(&next_bytes),
        };
        self.commit_maintenance(mutation, Vec::new(), 0, None, |write, rows| {
            let current = self.reconciliation_row_in_write(
                write,
                generation,
                "source reconciliation checkpoint targets a stale generation",
            )?;
            if let Some(current) = current.as_deref() {
                decode_reconciliation_checkpoint(current)?;
            }
            let is_next = current.as_deref() == Some(next_bytes.as_slice());
            ensure(
                is_next || current.as_deref() == expected_bytes.as_deref(),
                "source reconciliation checkpoint CAS predecessor is stale",
            )?;
            if is_next {
                return Ok(());
            }
            rows.open_table(SEMANTIC_SOURCE_PROGRESS)
                .map_err(kernel_error)?
                .insert(self.reconciliation_key(generation), next_bytes.as_slice())
                .map_err(kernel_error)?;
            Ok(())
        })
        .map(|_| ())
    }

    /// Remove a completed reconciliation continuation with an in-write CAS.
    /// A replay of the same clear batch is safe because the first commit has
    /// already consumed the exact expected row.
    pub(crate) fn clear_source_reconciliation_checkpoint(
        &self,
        generation: u64,
        expected: &SemanticSourceReconciliationCheckpoint,
    ) -> Result<(), SemanticCodeError> {
        ensure(
            generation != 0,
            "source reconciliation generation must be nonzero",
        )?;
        let expected_bytes = encode_reconciliation_checkpoint(expected)?;
        let batch_id = format!(
            "semantic-index:reconciliation-clear:{}",
            self.reconciliation_batch_digest(b"clear\0", generation, &expected_bytes)
        );
        let subject = format!("{}:{generation}", self.binding);
        let mutation = MetadataMutation {
            batch_id: &batch_id,
            event_type: "semantic_source_reconciliation_checkpoint_clear",
            subject: &subject,
            mutation_digest: semantic_digest(&expected_bytes),
        };
        self.commit_maintenance(mutation, Vec::new(), 0, None, |write, rows| {
            let current = self.reconciliation_row_in_write(
                write,
                generation,
                "source reconciliation clear targets a stale generation",
            )?;
            ensure(
                current.as_deref() == Some(expected_bytes.as_slice()),
                "source reconciliation checkpoint clear predecessor is stale",
            )?;
            rows.open_table(SEMANTIC_SOURCE_PROGRESS)
                .map_err(kernel_error)?
                .remove(self.reconciliation_key(generation))
                .map_err(kernel_error)?;
            Ok(())
        })
        .map(|_| ())
    }

    /// Return a bounded lexicographic page of the generation's durable source
    /// identities.  The returned cursor is the last returned identity, so a
    /// retry after a crash is deterministic and cannot skip the row following
    /// the page boundary.
    pub(crate) fn list_source_entities_page(
        &self,
        generation: u64,
        after: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<String>, Option<String>), SemanticCodeError> {
        ensure(
            generation != 0 && limit != 0 && limit <= 256,
            "source entity page has an invalid generation or bounded limit",
        )?;
        ensure(
            after.is_none_or(valid_source_entity_id_for_reconciliation),
            "source entity page cursor is not a canonical source identity",
        )?;
        let read = self.door.serving_read()?;
        let binding = self
            .read_binding_in(&read)?
            .ok_or_else(|| refused("source entity paging requires a durable binding head"))?;
        ensure(
            binding.generation == generation,
            "source entity page targets a stale generation",
        )?;
        let table = read
            .open_owner_table(SEMANTIC_SOURCE_PROGRESS)
            .map_err(kernel_error)?;
        let owner = (self.tenant.as_str(), self.binding.as_str());
        let rows = table
            .range((owner.0, owner.1, generation, after.unwrap_or(""))..)
            .map_err(kernel_error)?;
        let mut entities = Vec::with_capacity(limit);
        for row in rows {
            let (key, value) = row.map_err(kernel_error)?;
            let key = key.value();
            if (key.0, key.1, key.2) != (owner.0, owner.1, generation) {
                break;
            }
            if key.3 == SEMANTIC_RECONCILIATION_CHECKPOINT_ENTITY
                || after.is_some_and(|cursor| key.3 <= cursor)
            {
                continue;
            }
            if !valid_source_entity_id_for_reconciliation(key.3) {
                return Err(corrupt(
                    "source-progress page contains a non-canonical source identity",
                ));
            }
            self.generation_progress(
                &binding,
                key.3,
                value.value(),
                "source-progress page row is outside its durable binding generation",
            )?;
            if entities.len() == limit {
                let next_cursor = entities.last().cloned();
                return Ok((entities, next_cursor));
            }
            entities.push(key.3.to_string());
        }
        Ok((entities, None))
    }

    /// Test whether a source identity has a durable progress row in this
    /// generation.  This is an existence proof for tombstones; it does not
    /// expose arbitrary owner rows to the caller.
    pub(crate) fn source_entity_exists(
        &self,
        generation: u64,
        source_entity_id: &str,
    ) -> Result<bool, SemanticCodeError> {
        ensure(
            generation != 0 && valid_source_entity_id_for_reconciliation(source_entity_id),
            "source entity existence query has an invalid identity",
        )?;
        let progress = self.current_progress(
            generation,
            source_entity_id,
            "source entity existence requires a durable binding head",
            "source-progress existence row is outside its durable binding generation",
        )?;
        Ok(progress.is_some())
    }

    /// Read the retained canonical SQL manifest for one historical source
    /// entity.  Reconciliation uses this after a source row has disappeared:
    /// the complete snapshot supplies the deletion proof, while this row is
    /// the only durable source identity that may be carried into the
    /// tombstone transition.  The lookup is bounded to one owner-table key;
    /// it never reconstructs a deleted row or scans unrelated history.
    pub(crate) fn sql_source_manifest(
        &self,
        generation: u64,
        source_entity_id: &str,
    ) -> Result<Option<SemanticSqlSourceManifest>, SemanticCodeError> {
        ensure(
            generation != 0 && valid_source_entity_id_for_reconciliation(source_entity_id),
            "SQL source manifest lookup has an invalid generation or source identity",
        )?;
        let read = self.door.serving_read()?;
        let manifests = read
            .open_owner_table(SEMANTIC_SQL_SOURCES)
            .map_err(kernel_error)?;
        let key = (
            self.tenant.as_str(),
            self.binding.as_str(),
            generation,
            source_entity_id,
        );
        let Some(raw) = row_bytes(manifests.get(key))? else {
            return Ok(None);
        };
        let manifest: SemanticSqlSourceManifest = decode_valid(&raw)?;
        validate_sql_source_revision(&manifest.source_revision)?;
        if (
            manifest.binding_id.as_str(),
            manifest.generation,
            manifest.source_entity_id.as_str(),
        ) != (self.binding.as_str(), generation, source_entity_id)
        {
            return Err(corrupt(
                "SQL source manifest row does not match its canonical key",
            ));
        }
        let binding = self
            .read_binding_generation_in(&read, generation)?
            .ok_or_else(|| corrupt("SQL source manifest names a missing binding generation"))?;
        ensure_manifest_within_binding(&manifest, &binding)?;
        Ok(Some(manifest))
    }

    /// Return whether the current, non-superseded progress row was observed at
    /// the supplied source revision.  A row at an older revision remains
    /// useful for history but cannot satisfy a newer reconciliation tombstone.
    pub(crate) fn source_entity_seen_at_revision(
        &self,
        generation: u64,
        source_entity_id: &str,
        source_revision: &str,
    ) -> Result<bool, SemanticCodeError> {
        validate_sql_source_revision(source_revision)?;
        ensure(
            generation != 0 && valid_source_entity_id_for_reconciliation(source_entity_id),
            "source revision query has an invalid identity",
        )?;
        let progress = self.current_progress(
            generation,
            source_entity_id,
            "source revision query requires a durable binding head",
            "source revision row is outside its durable binding generation",
        )?;
        Ok(progress.is_some_and(|progress| {
            progress.source_revision == source_revision && progress.superseded_by_revision.is_none()
        }))
    }

    /// The entity's progress row when `generation` is the current binding
    /// head, validated against that head; `None` for another generation or
    /// an absent row.
    fn current_progress(
        &self,
        generation: u64,
        source_entity_id: &str,
        no_head: &str,
        outside: &str,
    ) -> Result<Option<SemanticSourceProgress>, SemanticCodeError> {
        let read = self.door.serving_read()?;
        let binding = self
            .read_binding_in(&read)?
            .ok_or_else(|| refused(no_head))?;
        if binding.generation != generation {
            return Ok(None);
        }
        let progress_rows = read
            .open_owner_table(SEMANTIC_SOURCE_PROGRESS)
            .map_err(kernel_error)?;
        let key = (
            self.tenant.as_str(),
            self.binding.as_str(),
            generation,
            source_entity_id,
        );
        row_bytes(progress_rows.get(key))?
            .map(|raw| self.generation_progress(&binding, source_entity_id, &raw, outside))
            .transpose()
    }

    /// A progress row of `binding`'s generation for exactly `source_entity_id`.
    fn generation_progress(
        &self,
        binding: &SemanticBinding,
        source_entity_id: &str,
        bytes: &[u8],
        outside: &str,
    ) -> Result<SemanticSourceProgress, SemanticCodeError> {
        let progress: SemanticSourceProgress = decode_valid(bytes)?;
        if (
            progress.binding_id.as_str(),
            progress.binding_digest,
            progress.generation,
            progress.source_entity_id.as_str(),
        ) != (
            self.binding.as_str(),
            binding.binding_digest,
            binding.generation,
            source_entity_id,
        ) {
            return Err(corrupt(outside));
        }
        Ok(progress)
    }

    /// The generation's reconciliation checkpoint row, read inside the write
    /// after proving `generation` is still the binding head.
    fn reconciliation_row_in_write(
        &self,
        write: &AdmittedMutation<'_, SemanticIndexOwner>,
        generation: u64,
        stale: &str,
    ) -> Result<Option<Vec<u8>>, SemanticCodeError> {
        let binding = self
            .read_binding_in_write(write)?
            .ok_or_else(|| refused("source reconciliation requires a durable binding head"))?;
        ensure(binding.generation == generation, stale)?;
        let progress_rows = write
            .open_read_table(SEMANTIC_SOURCE_PROGRESS)
            .map_err(kernel_error)?;
        row_bytes(progress_rows.get(self.reconciliation_key(generation)))
    }

    fn reconciliation_key(&self, generation: u64) -> (&str, &str, u64, &'static str) {
        (
            self.tenant.as_str(),
            self.binding.as_str(),
            generation,
            SEMANTIC_RECONCILIATION_CHECKPOINT_ENTITY,
        )
    }

    /// The batch identity of one checkpoint write or clear: the operation, the
    /// owner, the generation and the exact checkpoint bytes.
    fn reconciliation_batch_digest(
        &self,
        operation: &[u8],
        generation: u64,
        checkpoint_bytes: &[u8],
    ) -> SemanticDigest {
        semantic_digest(
            &[
                operation,
                self.tenant.as_bytes(),
                b"\0",
                self.binding.as_bytes(),
                b"\0",
                &generation.to_be_bytes(),
                b"\0",
                checkpoint_bytes,
            ]
            .concat(),
        )
    }
}

/// A retained SQL manifest belongs to its binding generation's source
/// identity, schema, field set and ACL, under the binding's SQL authority.
fn ensure_manifest_within_binding(
    manifest: &SemanticSqlSourceManifest,
    binding: &SemanticBinding,
) -> Result<(), SemanticCodeError> {
    manifest
        .source_identity
        .validate_against_binding(binding)
        .map_err(semantic_contract_error)?;
    let components = &binding.policy_identity.components;
    let manifest_authority = (
        (
            manifest.binding_id.as_str(),
            manifest.binding_digest,
            manifest.generation,
        ),
        (
            &manifest.source_schema_digest,
            &manifest.source_field_set_digest,
            manifest.source_acl_revision,
            &manifest.source_acl_digest,
        ),
    );
    let binding_authority = (
        (
            binding.binding_id.as_str(),
            binding.binding_digest,
            binding.generation,
        ),
        (
            &binding.source_schema_digest,
            &binding.source_field_set_digest,
            components.source_acl_revision,
            &components.source_acl_digest,
        ),
    );
    if manifest_authority != binding_authority {
        return Err(corrupt(
            "SQL source manifest is outside its durable binding authority",
        ));
    }
    let manifest_revision = sql_source_revision_parts(&manifest.source_revision)
        .ok_or_else(|| corrupt("SQL source manifest has no canonical source authority"))?;
    let binding_revision = sql_source_revision_parts(&binding.source_revision)
        .ok_or_else(|| corrupt("durable binding has no canonical SQL source authority"))?;
    if manifest_revision.authority != binding_revision.authority {
        return Err(corrupt(
            "SQL source manifest authority differs from its durable binding authority",
        ));
    }
    Ok(())
}

pub(super) fn valid_source_entity_id_for_reconciliation(entity: &str) -> bool {
    entity.len() <= SEMANTIC_RECONCILIATION_MAX_ENTITY_BYTES
        && entity
            .strip_prefix("semantic-sql-source:")
            .and_then(|digest| SemanticDigest::parse(digest).ok())
            .is_some()
}

/// Parse the tenant-wide SQL source revision emitted by the authoritative SQL
/// snapshot port.  The authority digest is part of the lineage, so equal
/// numeric epochs from two SQL owners cannot be compared as one stream.
pub(super) fn sql_source_revision_parts(revision: &str) -> Option<SqlSourceRevision<'_>> {
    let rest = revision.strip_prefix("sql-source:")?;
    let (authority, epoch) = rest.rsplit_once(":epoch:")?;
    let digest = authority.strip_prefix("sha256:")?;
    if digest.len() != 64
        || digest
            .bytes()
            .any(|byte| !matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        return None;
    }
    let epoch = epoch.parse().ok()?;
    if epoch == 0 {
        return None;
    }
    Some(SqlSourceRevision { authority, epoch })
}

pub(super) fn compare_source_revision(left: &str, right: &str) -> std::cmp::Ordering {
    match (
        sql_source_revision_parts(left),
        sql_source_revision_parts(right),
    ) {
        (Some(left_parts), Some(right_parts)) if left_parts.authority == right_parts.authority => {
            left_parts
                .epoch
                .cmp(&right_parts.epoch)
                .then_with(|| left.cmp(right))
        }
        _ => left.cmp(right),
    }
}

/// SQL refresh revisions are the tenant-wide source authority and epoch
/// returned by the atomic SQL snapshot read.  A bare or event-local counter
/// is insufficient because two SQL resources can commit the same table.
pub(super) fn validate_sql_source_revision(revision: &str) -> Result<(), SemanticCodeError> {
    ensure(
        sql_source_revision_parts(revision).is_some(),
        "semantic SQL refresh revision must bind a canonical source authority and nonzero epoch",
    )
}
