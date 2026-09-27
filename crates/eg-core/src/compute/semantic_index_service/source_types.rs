use super::*;

impl SemanticSqlSourceRecord {
    pub fn source_entity_id(&self) -> String {
        self.source_identity.source_entity_id()
    }

    pub fn is_tombstone(&self) -> bool {
        matches!(self.value, SemanticSqlSourceValue::Tombstone { .. })
    }

    pub(super) fn source_bytes_len(&self) -> usize {
        match &self.value {
            SemanticSqlSourceValue::Present { source_bytes } => source_bytes.len(),
            SemanticSqlSourceValue::Tombstone { .. } => 0,
        }
    }

    /// Return the canonical digest bound to the admitted source value.
    ///
    /// Present values hash their exact bytes; tombstones carry their
    /// authenticated deletion proof. Server adapters use this method when
    /// binding a leased intent to a typed source record.
    pub fn source_content_digest(&self) -> SemanticDigest {
        match &self.value {
            SemanticSqlSourceValue::Present { source_bytes } => {
                SemanticDigest::from_bytes(Sha256::digest(source_bytes).into())
            }
            SemanticSqlSourceValue::Tombstone {
                deletion_proof_digest,
            } => *deletion_proof_digest,
        }
    }

    pub(super) fn validate(&self) -> Result<(), SemanticIndexError> {
        self.source_identity.validate()?;
        if self.source_revision.trim().is_empty()
            || self.source_schema_digest.trim().is_empty()
            || self.source_field_set_digest.trim().is_empty()
            || self.source_acl_digest.trim().is_empty()
            || self.source_acl_revision == 0
            || self.authorization_receipt_digest == SemanticDigest::from_bytes([0; 32])
        {
            return Err(SemanticIndexError::SourceManifestMismatch);
        }
        Ok(())
    }

    /// Turn the SQL owner's raw policy decision digest into the complete
    /// semantic authorization receipt used by S1 artifacts.  The adapter
    /// intentionally supplies only the decision digest; the binding supplies
    /// the governed actor/purpose/policy coordinates, and the S1 consumer
    /// supplies the authoritative authorization time when it completes work.
    pub fn authorization_receipt(
        &self,
        binding: &SemanticBinding,
        authorized_at: &str,
    ) -> Result<SemanticAuthorizationReceipt, SemanticIndexError> {
        binding.validate()?;
        validate_source_record_against_binding(binding, self)?;
        SemanticAuthorizationReceipt::create(SemanticAuthorizationReceiptDraft {
            tenant_id: binding.tenant_id.clone(),
            actor_scope: binding.actor_scope.clone(),
            effective_actor_scope: binding.effective_actor_scope.clone(),
            purpose_id: binding.purpose_id.clone(),
            policy_identity: binding.policy_identity.clone(),
            policy_decision_digest: self.authorization_receipt_digest.to_string(),
            binding_id: binding.binding_id.clone(),
            binding_digest: binding.binding_digest,
            generation: binding.generation,
            scope: SemanticStageScope::Entity {
                source_entity_id: self.source_entity_id(),
            },
            source_revision: self.source_revision.clone(),
            authorized_at: authorized_at.to_string(),
        })
    }
}
impl SemanticSqlSourceReadPage {
    pub(super) const MAX_SOURCES: usize = 256;
    pub(super) const MAX_CURSOR_BYTES: usize = 4096;
    /// Keep this aligned with `eg_query::tables::ROW_SNAPSHOT_MAX_BYTES`.
    pub(super) const MAX_SOURCE_BYTES: usize = 8 * 1024 * 1024;

    pub fn validate(&self) -> Result<(), SemanticIndexError> {
        if sql_source_revision_parts(&self.source_revision).is_none() {
            return Err(SemanticIndexError::SourceManifestMismatch);
        }
        if self.sources.len() > Self::MAX_SOURCES {
            return Err(SemanticIndexError::InvalidField {
                field: "sql_source_page.sources".to_string(),
                reason: format!("page exceeds {} source rows", Self::MAX_SOURCES),
            });
        }
        self.validate_cursor_state()?;
        self.validate_snapshot_proof()?;
        self.validate_sources()
    }

    pub(super) fn validate_cursor_state(&self) -> Result<(), SemanticIndexError> {
        match (&self.complete, &self.next_cursor) {
            (true, Some(_)) => Err(SemanticIndexError::InvalidField {
                field: "sql_source_page.next_cursor".to_string(),
                reason: "a complete page cannot carry a continuation cursor".to_string(),
            }),
            (false, None) => Err(SemanticIndexError::InvalidField {
                field: "sql_source_page.next_cursor".to_string(),
                reason: "a partial page requires a continuation cursor".to_string(),
            }),
            (_, Some(cursor)) if cursor.is_empty() || cursor.len() > Self::MAX_CURSOR_BYTES => {
                Err(SemanticIndexError::InvalidField {
                    field: "sql_source_page.next_cursor".to_string(),
                    reason: "continuation cursor is empty or exceeds the bounded size".to_string(),
                })
            }
            _ => Ok(()),
        }
    }

    pub(super) fn validate_snapshot_proof(&self) -> Result<(), SemanticIndexError> {
        match (self.complete, self.complete_snapshot_receipt_digest) {
            (false, None) => Ok(()),
            (true, Some(digest)) if digest != SemanticDigest::from_bytes([0; 32]) => Ok(()),
            _ => Err(SemanticIndexError::SourceManifestMismatch),
        }
    }

    pub(super) fn validate_sources(&self) -> Result<(), SemanticIndexError> {
        let mut entities = BTreeSet::new();
        let mut source_bytes = 0usize;
        for source in &self.sources {
            source.validate()?;
            if source.source_revision != self.source_revision
                || (!self.complete && source.is_tombstone())
                || !entities.insert(source.source_entity_id())
            {
                return Err(SemanticIndexError::SourceManifestMismatch);
            }
            source_bytes = source_bytes
                .checked_add(source.source_bytes_len())
                .filter(|bytes| *bytes <= Self::MAX_SOURCE_BYTES)
                .ok_or_else(|| SemanticIndexError::InvalidField {
                    field: "sql_source_page.source_bytes".to_string(),
                    reason: format!("page source bytes exceed {} bytes", Self::MAX_SOURCE_BYTES),
                })?;
        }
        Ok(())
    }
}
