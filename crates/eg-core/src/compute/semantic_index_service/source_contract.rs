use super::*;

pub(super) fn validate_source_dirty_record(
    binding: &SemanticBinding,
    resolved_source_scope_digest: SemanticDigest,
    record: &MutationOutboxRecord,
) -> Result<SemanticSourceDirtyIntent, SemanticIndexError> {
    binding.validate()?;
    // This is the SQL catalog scope digest carried by the committed outbox
    // identity. It is deliberately distinct from the semantic binding digest:
    // a selector may have several governed semantic bindings over one source.
    let expected_scope_digest =
        SemanticDigest::from_bytes(*record.identity.binding_digest().as_bytes());
    if resolved_source_scope_digest != expected_scope_digest {
        return Err(SemanticIndexError::SourceManifestMismatch);
    }
    record
        .validate()
        .map_err(|reason| SemanticIndexError::InvalidField {
            field: "outbox_record".to_string(),
            reason,
        })?;
    if record.intent.topic != eg_types::semantic_index::SEMANTIC_SOURCE_DIRTY_TOPIC {
        return Err(SemanticIndexError::InvalidField {
            field: "outbox_topic".to_string(),
            reason: "semantic source dirty consumer received another topic".to_string(),
        });
    }
    if record.intent.key != record.batch_id || !record.intent.headers.is_empty() {
        return Err(SemanticIndexError::InvalidField {
            field: "outbox_identity".to_string(),
            reason: "source dirty wakeup key/headers are not the canonical producer shape"
                .to_string(),
        });
    }
    if record.identity.tenant().as_str() != binding.tenant_id
        || !matches!(
            record.identity.scope(),
            eg_types::mutation_batch::MutationScope::Native {
                domain: eg_types::mutation_batch::DurabilityDomain::SqlCatalog,
                ..
            }
        )
    {
        return Err(SemanticIndexError::SourceManifestMismatch);
    }
    let wakeup = SemanticSourceDirtyIntent::from_canonical_cbor(&record.intent.payload)?;
    wakeup.validate()?;
    if wakeup.source_scope_digest != resolved_source_scope_digest {
        return Err(SemanticIndexError::SourceManifestMismatch);
    }
    Ok(wakeup)
}

/// Expand the coarse SQL source fact into the binding-specific S1 intent.
/// SQL owns neither binding identity nor semantic generation, so this is the
/// only function that may join the source selector to a durable binding before
/// `SemanticCodeStore::enqueue_stage_intent` admits it. The wakeup record is
/// authenticated here, while E3 supplies the tenant-wide source revision from
/// its atomic authoritative read; no request field can select the revision.
pub(super) fn source_dirty_to_s1(
    binding: &SemanticBinding,
    source_entity_id: &str,
    resolved_source_scope_digest: SemanticDigest,
    record: &MutationOutboxRecord,
    source_revision: &str,
    input_digest: SemanticDigest,
) -> Result<SemanticStageIntent, SemanticIndexError> {
    validate_source_dirty_record(binding, resolved_source_scope_digest, record)?;
    if source_entity_id.trim().is_empty() {
        return Err(SemanticIndexError::SourceManifestMismatch);
    }
    coalesce_authoritative_revision_to_s1(binding, source_entity_id, source_revision, input_digest)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct SqlSourceRevision<'a> {
    authority: &'a str,
    epoch: u64,
}

/// Parse the canonical tenant-wide SQL source revision.  The authority is
/// deliberately parsed before the epoch so a numerically newer foreign
/// source cannot supersede the tenant's current source lineage.
pub(super) fn sql_source_revision_parts(revision: &str) -> Option<SqlSourceRevision<'_>> {
    let rest = revision.strip_prefix("sql-source:")?;
    let (authority, epoch) = rest.rsplit_once(":epoch:")?;
    SemanticDigest::parse(authority).ok()?;
    let epoch = epoch.parse().ok()?;
    if epoch == 0 {
        return None;
    }
    Some(SqlSourceRevision { authority, epoch })
}

pub(super) fn validate_manifest_against_binding(
    binding: &SemanticBinding,
    source_entity_id: &str,
    snapshot: &SemanticSqlSourceManifest,
) -> Result<(), SemanticIndexError> {
    snapshot.validate()?;
    snapshot.source_identity.validate_against_binding(binding)?;
    if snapshot.binding_id != binding.binding_id
        || snapshot.binding_digest != binding.binding_digest
        || snapshot.generation != binding.generation
        || snapshot.source_entity_id != source_entity_id
        || snapshot.source_schema_digest != binding.source_schema_digest
        || snapshot.source_field_set_digest != binding.source_field_set_digest
        || snapshot.source_acl_revision != binding.policy_identity.components.source_acl_revision
        || snapshot.source_acl_digest != binding.policy_identity.components.source_acl_digest
    {
        return Err(SemanticIndexError::SourceManifestMismatch);
    }
    Ok(())
}

pub(super) fn validate_source_record_against_binding(
    binding: &SemanticBinding,
    source: &SemanticSqlSourceRecord,
) -> Result<(), SemanticIndexError> {
    source.validate()?;
    source.source_identity.validate_against_binding(binding)?;
    if source.source_schema_digest != binding.source_schema_digest
        || source.source_field_set_digest != binding.source_field_set_digest
        || source.source_acl_revision != binding.policy_identity.components.source_acl_revision
        || source.source_acl_digest != binding.policy_identity.components.source_acl_digest
    {
        return Err(SemanticIndexError::SourceManifestMismatch);
    }
    Ok(())
}

/// Compare a source observation to the durable binding head.  The wakeup is a
/// notification only; E3's atomically read tenant authority/epoch is the
/// source revision, so an event-local native scope never enters this proof.
pub(super) fn coalesce_authoritative_revision_to_s1(
    binding: &SemanticBinding,
    source_entity_id: &str,
    source_revision: &str,
    source_content_digest: SemanticDigest,
) -> Result<SemanticStageIntent, SemanticIndexError> {
    let source = sql_source_revision_parts(source_revision)
        .ok_or(SemanticIndexError::SourceManifestMismatch)?;
    let binding_revision = sql_source_revision_parts(&binding.source_revision)
        .ok_or(SemanticIndexError::SourceManifestMismatch)?;
    if binding_revision.authority != source.authority {
        return Err(SemanticIndexError::SourceManifestMismatch);
    }
    if source.epoch < binding_revision.epoch {
        return Err(SemanticIndexError::SourceRevisionStale);
    }
    source_dirty_intent(
        binding,
        source_entity_id,
        source_revision,
        source_content_digest,
    )
}

pub(super) fn source_dirty_intent(
    binding: &SemanticBinding,
    source_entity_id: &str,
    source_revision: &str,
    input_digest: SemanticDigest,
) -> Result<SemanticStageIntent, SemanticIndexError> {
    SemanticStageIntent::create(SemanticStageIntentDraft {
        binding_id: binding.binding_id.clone(),
        binding_digest: binding.binding_digest,
        generation: binding.generation,
        scope: SemanticStageScope::Entity {
            source_entity_id: source_entity_id.to_string(),
        },
        source_revision: source_revision.to_string(),
        stage: SemanticStage::SourceCommit,
        predecessor: eg_types::semantic_index::SemanticStagePredecessor::None,
        input_digest,
    })
}

pub(super) fn validate_sql_revision_lineage(
    current_revision: &str,
    replacement_revision: &str,
) -> Result<(), SemanticIndexError> {
    let current = sql_source_revision_parts(current_revision)
        .ok_or(SemanticIndexError::SourceManifestMismatch)?;
    let replacement = sql_source_revision_parts(replacement_revision)
        .ok_or(SemanticIndexError::SourceManifestMismatch)?;
    if current.authority != replacement.authority {
        return Err(SemanticIndexError::SourceManifestMismatch);
    }
    if replacement.epoch <= current.epoch {
        return Err(SemanticIndexError::SourceRevisionStale);
    }
    Ok(())
}

/// Reconcile a coarse source wakeup with an authorized raw SQL observation.
/// The observation owns the exact source bytes or deletion proof; only its
/// digest enters the durable S1 intent.
pub fn coalesce_sql_source_record_to_s1(
    binding: &SemanticBinding,
    resolved_source_scope_digest: SemanticDigest,
    record: &MutationOutboxRecord,
    source: &SemanticSqlSourceRecord,
) -> Result<SemanticStageIntent, SemanticIndexError> {
    binding.validate()?;
    validate_source_record_against_binding(binding, source)?;
    let source_entity_id = source.source_entity_id();
    source_dirty_to_s1(
        binding,
        &source_entity_id,
        resolved_source_scope_digest,
        record,
        &source.source_revision,
        source.source_content_digest(),
    )
}

/// Build the completed S1 artifact from the same raw source claim that
/// produced its intent.  Keeping this construction beside coalescing prevents
/// the SQL ACL decision digest from being mistaken for the full authorization
/// receipt digest when a consumer persists the manifest.
pub fn sql_source_stage_artifact(
    binding: &SemanticBinding,
    transition: &SemanticStageTransition,
    source: &SemanticSqlSourceRecord,
    authorized_at: &str,
) -> Result<SemanticStageArtifact, SemanticIndexError> {
    transition.validate()?;
    if transition.intent.stage != SemanticStage::SourceCommit {
        return Err(SemanticIndexError::StageArtifactMismatch);
    }
    let source_entity_id = source.source_entity_id();
    let source_content_digest = source.source_content_digest();
    if transition.intent.scope
        != (SemanticStageScope::Entity {
            source_entity_id: source_entity_id.clone(),
        })
        || transition.intent.source_revision != source.source_revision
        || transition.intent.input_digest != source_content_digest
        || transition.receipt.output_digest != source_content_digest
    {
        return Err(SemanticIndexError::StageArtifactMismatch);
    }
    let authorization = source.authorization_receipt(binding, authorized_at)?;
    let manifest = SemanticSqlSourceManifest::create(SemanticSqlSourceManifestDraft {
        binding_id: binding.binding_id.clone(),
        binding_digest: binding.binding_digest,
        generation: binding.generation,
        source_identity: source.source_identity.clone(),
        source_revision: source.source_revision.clone(),
        source_content_digest,
        source_schema_revision: source.source_schema_revision,
        source_schema_digest: source.source_schema_digest.clone(),
        source_field_set_digest: source.source_field_set_digest.clone(),
        source_acl_revision: source.source_acl_revision,
        source_acl_digest: source.source_acl_digest.clone(),
        authorization_receipt_digest: authorization.authorization_receipt_digest,
        completed_receipt_digest: transition.receipt.receipt_digest(),
        completed_at: transition.receipt.completed_at.clone(),
    })?;
    let artifact = SemanticStageArtifact::SqlSourceManifest {
        manifest: Box::new(manifest),
        authorization: Box::new(authorization),
    };
    artifact.validate_against(transition)?;
    Ok(artifact)
}

/// Build the S1 intent for a row that was absent from an authenticated,
/// complete E3 snapshot.  A tombstone carries only the durable entity key:
/// reconstructing a deleted row's primary-key bytes in core would invent
/// source data.  The complete-page receipt and tenant revision make the
/// deletion proof specific to this authoritative scan.
pub(super) fn coalesce_sql_source_tombstone_to_s1(
    binding: &SemanticBinding,
    resolved_source_scope_digest: SemanticDigest,
    record: &MutationOutboxRecord,
    source_entity_id: &str,
    source_revision: &str,
    deletion_proof_digest: SemanticDigest,
) -> Result<SemanticStageIntent, SemanticIndexError> {
    binding.validate()?;
    validate_source_dirty_record(binding, resolved_source_scope_digest, record)?;
    if !valid_source_entity_id(source_entity_id)
        || deletion_proof_digest == SemanticDigest::from_bytes([0; 32])
    {
        return Err(SemanticIndexError::SourceManifestMismatch);
    }
    coalesce_authoritative_revision_to_s1(
        binding,
        source_entity_id,
        source_revision,
        deletion_proof_digest,
    )
}

/// Compute the canonical proof that one source entity was absent from a
/// complete authoritative snapshot.  Server adapters use this helper when
/// constructing a tombstone transition so the deletion digest cannot drift
/// from the core reconciliation path.
pub fn sql_source_deletion_proof(
    source_entity_id: &str,
    source_revision: &str,
    complete_snapshot_receipt_digest: SemanticDigest,
) -> SemanticDigest {
    let mut hasher = Sha256::new();
    hasher.update(b"eg/semantic-sql-source-deletion/v1\0");
    hasher.update(source_entity_id.as_bytes());
    hasher.update((source_entity_id.len() as u64).to_be_bytes());
    hasher.update(source_revision.as_bytes());
    hasher.update((source_revision.len() as u64).to_be_bytes());
    hasher.update(complete_snapshot_receipt_digest.as_bytes());
    SemanticDigest::from_bytes(hasher.finalize().into())
}

pub(super) fn read_reconciliation_page(
    read_port: &dyn SemanticSqlSourceReadPort,
    binding: &SemanticBinding,
    wakeup: &SemanticSourceDirtyIntent,
    record: &MutationOutboxRecord,
    cursor: Option<&[u8]>,
) -> Result<SemanticSqlSourceReadPage, SemanticCodeError> {
    read_port
        .read_current_sql_source_page(binding, wakeup, record, cursor)
        .map_err(|error| {
            SemanticCodeError::Refused(format!("authoritative SQL source read rejected: {error:?}"))
        })
}

pub(super) fn source_reconciliation_wakeup_digest(
    record: &MutationOutboxRecord,
    wakeup: &SemanticSourceDirtyIntent,
) -> SemanticDigest {
    let mut hasher = Sha256::new();
    hasher.update(b"eg/semantic-source-reconciliation-wakeup/v1\0");
    hasher.update((record.batch_id.len() as u64).to_be_bytes());
    hasher.update(record.batch_id.as_bytes());
    hasher.update(record.ordinal.to_be_bytes());
    match record.committed_version {
        eg_types::mutation_batch::CommittedVersion::Graph { source, target } => {
            hasher.update([b'g']);
            hasher.update(source.to_be_bytes());
            hasher.update(target.to_be_bytes());
        }
        eg_types::mutation_batch::CommittedVersion::Native { source, target } => {
            hasher.update([b'n']);
            hasher.update(source.to_be_bytes());
            hasher.update(target.to_be_bytes());
        }
        eg_types::mutation_batch::CommittedVersion::None => hasher.update([b'0']),
    }
    hasher.update(wakeup.source_scope_digest.as_bytes());
    hasher.update(wakeup.input_digest.as_bytes());
    SemanticDigest::from_bytes(hasher.finalize().into())
}

pub(super) fn checkpoint_matches_wakeup(
    checkpoint: &SemanticSourceReconciliationCheckpoint,
    wakeup_digest: SemanticDigest,
) -> bool {
    checkpoint.source_wakeup_digest == wakeup_digest
}

pub(super) fn validate_page_revision_against_binding(
    binding: &SemanticBinding,
    source_revision: &str,
) -> Result<(), SemanticCodeError> {
    let page = sql_source_revision_parts(source_revision).ok_or_else(|| {
        SemanticCodeError::Refused(
            "authoritative SQL source page has a non-canonical source revision".to_string(),
        )
    })?;
    let binding_revision =
        sql_source_revision_parts(&binding.source_revision).ok_or_else(|| {
            SemanticCodeError::Corrupt(
                "durable semantic binding has a non-canonical SQL source revision".to_string(),
            )
        })?;
    if page.authority != binding_revision.authority {
        return Err(SemanticCodeError::Refused(
            "authoritative SQL source page belongs to another source authority".to_string(),
        ));
    }
    if page.epoch < binding_revision.epoch {
        return Err(SemanticCodeError::Refused(
            "authoritative SQL source page is stale relative to the binding head".to_string(),
        ));
    }
    Ok(())
}
