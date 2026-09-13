//! SQL source reconciliation: the durable continuation checkpoint, the
//! bounded source-entity pages and the canonical source revision.

use super::batch::{semantic_digest, MetadataMutation};
use super::reconciliation_codec::{
    decode_reconciliation_checkpoint, encode_reconciliation_checkpoint,
};
use super::{kernel_error, semantic_contract_error, SemanticCodeError, SemanticCodeStore};
use eg_storage::{SEMANTIC_SOURCE_PROGRESS, SEMANTIC_SQL_SOURCES};
use eg_types::semantic_index::{SemanticDigest, SemanticSourceProgress, SemanticSqlSourceManifest};

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
        if generation == 0 {
            return Err(SemanticCodeError::Refused(
                "source reconciliation generation must be nonzero".to_string(),
            ));
        }
        let read = self.door.serving_read()?;
        let current = self.read_binding_in(&read)?.ok_or_else(|| {
            SemanticCodeError::Refused(
                "source reconciliation requires a durable binding head".to_string(),
            )
        })?;
        if current.generation != generation {
            return Err(SemanticCodeError::Refused(
                "source reconciliation checkpoint is for a stale binding generation".to_string(),
            ));
        }
        let raw = read
            .open_owner_table(SEMANTIC_SOURCE_PROGRESS)
            .map_err(kernel_error)?
            .get((
                self.tenant.as_str(),
                self.binding.as_str(),
                generation,
                SEMANTIC_RECONCILIATION_CHECKPOINT_ENTITY,
            ))
            .map_err(kernel_error)?
            .map(|value| value.value().to_vec());
        raw.map(|bytes| decode_reconciliation_checkpoint(&bytes))
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
        if generation == 0 {
            return Err(SemanticCodeError::Refused(
                "source reconciliation generation must be nonzero".to_string(),
            ));
        }
        let next_bytes = encode_reconciliation_checkpoint(next)?;
        let expected_bytes = expected.map(encode_reconciliation_checkpoint).transpose()?;
        let digest = semantic_digest(&next_bytes);
        let batch_id = format!(
            "semantic-index:reconciliation-checkpoint:{}",
            semantic_digest(
                &[
                    b"write\0".as_slice(),
                    self.tenant.as_bytes(),
                    b"\0",
                    self.binding.as_bytes(),
                    b"\0",
                    &generation.to_be_bytes(),
                    b"\0",
                    next_bytes.as_slice(),
                ]
                .concat()
            )
        );
        let owner = self.door.owner();
        self.door
            .commit_metadata(
                |version| {
                    self.metadata_batch(
                        owner,
                        version,
                        MetadataMutation {
                            batch_id: &batch_id,
                            event_type: "semantic_source_reconciliation_checkpoint",
                            subject: &format!("{}:{generation}", self.binding),
                            mutation_digest: digest,
                        },
                        Vec::new(),
                        0,
                    )
                },
                digest,
                0,
                |write, rows| {
                    let current_binding = self.read_binding_in_write(write)?.ok_or_else(|| {
                        SemanticCodeError::Refused(
                            "source reconciliation requires a durable binding head".to_string(),
                        )
                    })?;
                    if current_binding.generation != generation {
                        return Err(SemanticCodeError::Refused(
                            "source reconciliation checkpoint targets a stale generation"
                                .to_string(),
                        ));
                    }
                    let current_raw = write
                        .open_read_table(SEMANTIC_SOURCE_PROGRESS)
                        .map_err(kernel_error)?
                        .get((
                            self.tenant.as_str(),
                            self.binding.as_str(),
                            generation,
                            SEMANTIC_RECONCILIATION_CHECKPOINT_ENTITY,
                        ))
                        .map_err(kernel_error)?
                        .map(|value| value.value().to_vec());
                    let current = current_raw
                        .as_deref()
                        .map(decode_reconciliation_checkpoint)
                        .transpose()?;
                    let is_next = current_raw.as_deref() == Some(next_bytes.as_slice());
                    let matches_expected = match (expected_bytes.as_deref(), current_raw.as_deref())
                    {
                        (None, None) => true,
                        (Some(expected), Some(actual)) => actual == expected,
                        _ => false,
                    };
                    if !matches_expected && !is_next {
                        return Err(SemanticCodeError::Refused(
                            "source reconciliation checkpoint CAS predecessor is stale".to_string(),
                        ));
                    }
                    if current.is_some() && is_next {
                        return Ok(());
                    }
                    let mut table = rows
                        .open_table(SEMANTIC_SOURCE_PROGRESS)
                        .map_err(kernel_error)?;
                    table
                        .insert(
                            (
                                self.tenant.as_str(),
                                self.binding.as_str(),
                                generation,
                                SEMANTIC_RECONCILIATION_CHECKPOINT_ENTITY,
                            ),
                            next_bytes.as_slice(),
                        )
                        .map_err(kernel_error)?;
                    Ok(())
                },
            )
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
        if generation == 0 {
            return Err(SemanticCodeError::Refused(
                "source reconciliation generation must be nonzero".to_string(),
            ));
        }
        let expected_bytes = encode_reconciliation_checkpoint(expected)?;
        let digest = semantic_digest(&expected_bytes);
        let batch_id = format!(
            "semantic-index:reconciliation-clear:{}",
            semantic_digest(
                &[
                    b"clear\0".as_slice(),
                    self.tenant.as_bytes(),
                    b"\0",
                    self.binding.as_bytes(),
                    b"\0",
                    &generation.to_be_bytes(),
                    b"\0",
                    expected_bytes.as_slice(),
                ]
                .concat()
            )
        );
        let owner = self.door.owner();
        self.door
            .commit_metadata(
                |version| {
                    self.metadata_batch(
                        owner,
                        version,
                        MetadataMutation {
                            batch_id: &batch_id,
                            event_type: "semantic_source_reconciliation_checkpoint_clear",
                            subject: &format!("{}:{generation}", self.binding),
                            mutation_digest: digest,
                        },
                        Vec::new(),
                        0,
                    )
                },
                digest,
                0,
                |write, rows| {
                    let current_binding = self.read_binding_in_write(write)?.ok_or_else(|| {
                        SemanticCodeError::Refused(
                            "source reconciliation requires a durable binding head".to_string(),
                        )
                    })?;
                    if current_binding.generation != generation {
                        return Err(SemanticCodeError::Refused(
                            "source reconciliation clear targets a stale generation".to_string(),
                        ));
                    }
                    let current = write
                        .open_read_table(SEMANTIC_SOURCE_PROGRESS)
                        .map_err(kernel_error)?
                        .get((
                            self.tenant.as_str(),
                            self.binding.as_str(),
                            generation,
                            SEMANTIC_RECONCILIATION_CHECKPOINT_ENTITY,
                        ))
                        .map_err(kernel_error)?
                        .map(|value| value.value().to_vec());
                    if current.as_deref() != Some(expected_bytes.as_slice()) {
                        return Err(SemanticCodeError::Refused(
                            "source reconciliation checkpoint clear predecessor is stale"
                                .to_string(),
                        ));
                    }
                    rows.open_table(SEMANTIC_SOURCE_PROGRESS)
                        .map_err(kernel_error)?
                        .remove((
                            self.tenant.as_str(),
                            self.binding.as_str(),
                            generation,
                            SEMANTIC_RECONCILIATION_CHECKPOINT_ENTITY,
                        ))
                        .map_err(kernel_error)?;
                    Ok(())
                },
            )
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
        if generation == 0 || limit == 0 || limit > 256 {
            return Err(SemanticCodeError::Refused(
                "source entity page has an invalid generation or bounded limit".to_string(),
            ));
        }
        if let Some(after) = after {
            if !valid_source_entity_id_for_reconciliation(after) {
                return Err(SemanticCodeError::Refused(
                    "source entity page cursor is not a canonical source identity".to_string(),
                ));
            }
        }
        let read = self.door.serving_read()?;
        let binding = self.read_binding_in(&read)?.ok_or_else(|| {
            SemanticCodeError::Refused(
                "source entity paging requires a durable binding head".to_string(),
            )
        })?;
        if binding.generation != generation {
            return Err(SemanticCodeError::Refused(
                "source entity page targets a stale generation".to_string(),
            ));
        }
        let table = read
            .open_owner_table(SEMANTIC_SOURCE_PROGRESS)
            .map_err(kernel_error)?;
        let mut entities = Vec::with_capacity(limit);
        let mut next_cursor = None;
        let range_start = after.unwrap_or("");
        let rows = table
            .range(
                (
                    self.tenant.as_str(),
                    self.binding.as_str(),
                    generation,
                    range_start,
                )..,
            )
            .map_err(kernel_error)?;
        for row in rows {
            let (key, value) = row.map_err(kernel_error)?;
            let key = key.value();
            if key.0 != self.tenant || key.1 != self.binding || key.2 != generation {
                break;
            }
            if key.3 == SEMANTIC_RECONCILIATION_CHECKPOINT_ENTITY
                || after.is_some_and(|cursor| key.3 <= cursor)
            {
                continue;
            }
            if !valid_source_entity_id_for_reconciliation(key.3)
                || key.3.len() > SEMANTIC_RECONCILIATION_MAX_ENTITY_BYTES
            {
                return Err(SemanticCodeError::Corrupt(
                    "source-progress page contains a non-canonical source identity".to_string(),
                ));
            }
            let progress = SemanticSourceProgress::from_canonical_cbor(value.value())
                .map_err(semantic_contract_error)?;
            progress.validate().map_err(semantic_contract_error)?;
            if progress.binding_id != self.binding
                || progress.binding_digest != binding.binding_digest
                || progress.generation != generation
                || progress.source_entity_id != key.3
            {
                return Err(SemanticCodeError::Corrupt(
                    "source-progress page row is outside its durable binding generation"
                        .to_string(),
                ));
            }
            if entities.len() < limit {
                entities.push(key.3.to_string());
            } else {
                next_cursor = entities.last().cloned();
                break;
            }
        }
        Ok((entities, next_cursor))
    }

    /// Test whether a source identity has a durable progress row in this
    /// generation.  This is an existence proof for tombstones; it does not
    /// expose arbitrary owner rows to the caller.
    pub(crate) fn source_entity_exists(
        &self,
        generation: u64,
        source_entity_id: &str,
    ) -> Result<bool, SemanticCodeError> {
        if generation == 0 || !valid_source_entity_id_for_reconciliation(source_entity_id) {
            return Err(SemanticCodeError::Refused(
                "source entity existence query has an invalid identity".to_string(),
            ));
        }
        let read = self.door.serving_read()?;
        let binding = self.read_binding_in(&read)?.ok_or_else(|| {
            SemanticCodeError::Refused(
                "source entity existence requires a durable binding head".to_string(),
            )
        })?;
        if binding.generation != generation {
            return Ok(false);
        }
        let raw = read
            .open_owner_table(SEMANTIC_SOURCE_PROGRESS)
            .map_err(kernel_error)?
            .get((
                self.tenant.as_str(),
                self.binding.as_str(),
                generation,
                source_entity_id,
            ))
            .map_err(kernel_error)?
            .map(|value| value.value().to_vec());
        let Some(raw) = raw else {
            return Ok(false);
        };
        let progress =
            SemanticSourceProgress::from_canonical_cbor(&raw).map_err(semantic_contract_error)?;
        progress.validate().map_err(semantic_contract_error)?;
        if progress.binding_id != self.binding
            || progress.binding_digest != binding.binding_digest
            || progress.generation != generation
            || progress.source_entity_id != source_entity_id
        {
            return Err(SemanticCodeError::Corrupt(
                "source-progress existence row is outside its durable binding generation"
                    .to_string(),
            ));
        }
        Ok(true)
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
        if generation == 0 || !valid_source_entity_id_for_reconciliation(source_entity_id) {
            return Err(SemanticCodeError::Refused(
                "SQL source manifest lookup has an invalid generation or source identity"
                    .to_string(),
            ));
        }
        let read = self.door.serving_read()?;
        let raw = read
            .open_owner_table(SEMANTIC_SQL_SOURCES)
            .map_err(kernel_error)?
            .get((
                self.tenant.as_str(),
                self.binding.as_str(),
                generation,
                source_entity_id,
            ))
            .map_err(kernel_error)?
            .map(|value| value.value().to_vec());
        let Some(raw) = raw else {
            return Ok(None);
        };
        let manifest = SemanticSqlSourceManifest::from_canonical_cbor(&raw)
            .map_err(semantic_contract_error)?;
        manifest.validate().map_err(semantic_contract_error)?;
        validate_sql_source_revision(&manifest.source_revision)?;
        if manifest.binding_id != self.binding
            || manifest.generation != generation
            || manifest.source_entity_id != source_entity_id
        {
            return Err(SemanticCodeError::Corrupt(
                "SQL source manifest row does not match its canonical key".to_string(),
            ));
        }
        let binding = self
            .read_binding_generation_in(&read, generation)?
            .ok_or_else(|| {
                SemanticCodeError::Corrupt(
                    "SQL source manifest names a missing binding generation".to_string(),
                )
            })?;
        manifest
            .source_identity
            .validate_against_binding(&binding)
            .map_err(semantic_contract_error)?;
        if manifest.binding_id != binding.binding_id
            || manifest.binding_digest != binding.binding_digest
            || manifest.generation != binding.generation
            || manifest.source_schema_digest != binding.source_schema_digest
            || manifest.source_field_set_digest != binding.source_field_set_digest
            || manifest.source_acl_revision
                != binding.policy_identity.components.source_acl_revision
            || manifest.source_acl_digest != binding.policy_identity.components.source_acl_digest
        {
            return Err(SemanticCodeError::Corrupt(
                "SQL source manifest is outside its durable binding authority".to_string(),
            ));
        }
        let manifest_revision =
            sql_source_revision_parts(&manifest.source_revision).ok_or_else(|| {
                SemanticCodeError::Corrupt(
                    "SQL source manifest has no canonical source authority".to_string(),
                )
            })?;
        let binding_revision =
            sql_source_revision_parts(&binding.source_revision).ok_or_else(|| {
                SemanticCodeError::Corrupt(
                    "durable binding has no canonical SQL source authority".to_string(),
                )
            })?;
        if manifest_revision.authority != binding_revision.authority {
            return Err(SemanticCodeError::Corrupt(
                "SQL source manifest authority differs from its durable binding authority"
                    .to_string(),
            ));
        }
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
        if generation == 0 || !valid_source_entity_id_for_reconciliation(source_entity_id) {
            return Err(SemanticCodeError::Refused(
                "source revision query has an invalid identity".to_string(),
            ));
        }
        let read = self.door.serving_read()?;
        let binding = self.read_binding_in(&read)?.ok_or_else(|| {
            SemanticCodeError::Refused(
                "source revision query requires a durable binding head".to_string(),
            )
        })?;
        if binding.generation != generation {
            return Ok(false);
        }
        let raw = read
            .open_owner_table(SEMANTIC_SOURCE_PROGRESS)
            .map_err(kernel_error)?
            .get((
                self.tenant.as_str(),
                self.binding.as_str(),
                generation,
                source_entity_id,
            ))
            .map_err(kernel_error)?
            .map(|value| value.value().to_vec());
        let Some(raw) = raw else {
            return Ok(false);
        };
        let progress =
            SemanticSourceProgress::from_canonical_cbor(&raw).map_err(semantic_contract_error)?;
        progress.validate().map_err(semantic_contract_error)?;
        if progress.binding_id != self.binding
            || progress.binding_digest != binding.binding_digest
            || progress.generation != generation
            || progress.source_entity_id != source_entity_id
        {
            return Err(SemanticCodeError::Corrupt(
                "source revision row is outside its durable binding generation".to_string(),
            ));
        }
        Ok(
            progress.source_revision == source_revision
                && progress.superseded_by_revision.is_none(),
        )
    }
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
    if sql_source_revision_parts(revision).is_none() {
        return Err(SemanticCodeError::Refused(
            "semantic SQL refresh revision must bind a canonical source authority and nonzero epoch"
                .to_string(),
        ));
    }
    Ok(())
}
