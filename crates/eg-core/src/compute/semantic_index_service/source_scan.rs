use super::*;

impl SemanticIndexService {
    pub(super) fn admit_sql_source_page(
        &self,
        binding: &SemanticBinding,
        resolved_source_scope_digest: SemanticDigest,
        record: &MutationOutboxRecord,
        page: &SemanticSqlSourceReadPage,
        now_ms: u64,
    ) -> Result<SemanticSqlSourcePageAdmission, SemanticCodeError> {
        validate_page_revision_against_binding(binding, &page.source_revision)?;
        let mut receipts = Vec::with_capacity(page.sources.len());
        for source in &page.sources {
            let source_entity_id = source.source_entity_id();
            if source.is_tombstone()
                && !self
                    .store
                    .source_entity_exists(binding.generation, &source_entity_id)?
            {
                return Err(SemanticCodeError::Refused(
                    "authoritative SQL source tombstone names no durable prior entity".to_string(),
                ));
            }
            receipts.push(self.admit_sql_source_dirty(
                binding,
                resolved_source_scope_digest,
                record,
                source,
                now_ms,
            )?);
        }
        Ok(SemanticSqlSourcePageAdmission {
            receipts,
            next_cursor: page.next_cursor.clone(),
            complete: page.complete,
        })
    }

    pub(super) fn resume_tombstone_reconciliation(
        &self,
        binding: &SemanticBinding,
        scope_digest: SemanticDigest,
        record: &MutationOutboxRecord,
        checkpoint: Option<&SemanticSourceReconciliationCheckpoint>,
        now_ms: u64,
    ) -> Result<Option<SemanticSqlSourceReconciliationAdmission>, SemanticCodeError> {
        let Some(existing) = checkpoint else {
            return Ok(None);
        };
        validate_checkpoint_against_binding(binding, existing)?;
        if !matches!(
            &existing.phase,
            SemanticSourceReconciliationPhase::FinalizingTombstones
        ) {
            return Ok(None);
        }
        self.finalize_source_reconciliation(
            binding,
            scope_digest,
            record,
            existing.clone(),
            Vec::new(),
            now_ms,
        )
        .map(Some)
    }

    pub(super) fn load_checkpoint_for_wakeup(
        &self,
        binding: &SemanticBinding,
        wakeup_digest: SemanticDigest,
    ) -> Result<ReconciliationCheckpointState, SemanticCodeError> {
        let checkpoint = self
            .store
            .read_source_reconciliation_checkpoint(binding.generation)?;
        let Some(existing) = checkpoint else {
            return Ok(ReconciliationCheckpointState::Owned(None));
        };
        validate_checkpoint_against_binding(binding, &existing)?;
        if checkpoint_matches_wakeup(&existing, wakeup_digest) {
            return Ok(ReconciliationCheckpointState::Owned(Some(existing)));
        }
        // Keep the older checkpoint owned by its durable outbox record.  A
        // mismatching wakeup is explicitly pending, so alternating retries
        // cannot clear/restart one another's bounded continuation.
        Ok(ReconciliationCheckpointState::Pending(admission_result(
            Vec::new(),
            &existing,
            false,
        )?))
    }

    pub(super) fn persist_scan_budget_checkpoint(
        &self,
        binding: &SemanticBinding,
        previous_revision: Option<&str>,
        cursor: Option<Vec<u8>>,
        checkpoint: Option<&SemanticSourceReconciliationCheckpoint>,
        receipts: Vec<SemanticMutationReceipt>,
    ) -> Result<SemanticSqlSourceReconciliationAdmission, SemanticCodeError> {
        let state = checkpoint_for_scan(
            previous_revision.ok_or_else(|| {
                SemanticCodeError::Corrupt(
                    "source page had no canonical revision after validation".to_string(),
                )
            })?,
            cursor,
            checkpoint,
        )?;
        // All pages before this cursor were admitted before it advanced. If
        // the process dies between those native commits, retry replays the
        // deterministic S1 rows instead of skipping them.
        self.store.write_source_reconciliation_checkpoint(
            binding.generation,
            checkpoint,
            &state,
        )?;
        admission_result(receipts, &state, false)
    }

    pub(super) fn persist_partial_reconciliation_page(
        &self,
        binding: &SemanticBinding,
        checkpoint: Option<&SemanticSourceReconciliationCheckpoint>,
        scanned: ScannedPage<'_>,
        progress: SourceScanProgress<'_>,
        budget: &ReconciliationTurnBudget,
        receipts: Vec<SemanticMutationReceipt>,
    ) -> Result<PartialReconciliationProgress, SemanticCodeError> {
        let ScannedPage { page, cursor } = scanned;
        let SourceScanProgress {
            source_wakeup_digest,
            source_revision,
            rows_seen,
            source_bytes_seen,
            pages_seen,
        } = progress;
        let next_cursor = page.next_cursor.clone().ok_or_else(|| {
            SemanticCodeError::Corrupt(
                "validated partial source page lost its continuation cursor".to_string(),
            )
        })?;
        if cursor == Some(next_cursor.as_slice()) {
            return Err(SemanticCodeError::Refused(
                "authoritative SQL source cursor did not advance".to_string(),
            ));
        }
        let state = SemanticSourceReconciliationCheckpoint {
            source_wakeup_digest,
            source_revision: source_revision.to_string(),
            phase: SemanticSourceReconciliationPhase::Scanning,
            source_cursor: Some(next_cursor.clone()),
            prior_cursor: None,
            rows_seen,
            source_bytes_seen,
            pages_seen,
            complete_snapshot_receipt_digest: None,
        };
        self.store.write_source_reconciliation_checkpoint(
            binding.generation,
            checkpoint,
            &state,
        )?;
        if budget.exhausted() {
            return admission_result(receipts, &state, false)
                .map(PartialReconciliationProgress::Yield);
        }
        Ok(PartialReconciliationProgress::Continue {
            checkpoint: state,
            cursor: next_cursor,
            receipts,
        })
    }

    pub(super) fn admit_missing_tombstones(
        &self,
        wakeup: AdmittedSourceWakeup<'_>,
        state: &SemanticSourceReconciliationCheckpoint,
        source_entities: &[String],
        mut receipts: Vec<SemanticMutationReceipt>,
        now_ms: u64,
    ) -> Result<Vec<SemanticMutationReceipt>, SemanticCodeError> {
        let AdmittedSourceWakeup {
            binding,
            scope_digest,
            record,
        } = wakeup;
        // The complete-read proof is READ OFF the checkpoint rather than passed
        // beside it. It used to be both, and two authorities for one fact can
        // disagree: a caller could hand a proof from one snapshot next to a
        // checkpoint recording another.
        let complete_receipt = state.complete_snapshot_receipt_digest.ok_or_else(|| {
            SemanticCodeError::Corrupt(
                "tombstone reconciliation checkpoint has no complete read proof".to_string(),
            )
        })?;
        for source_entity_id in source_entities {
            if self.store.source_entity_seen_at_revision(
                binding.generation,
                source_entity_id,
                &state.source_revision,
            )? {
                continue;
            }
            let proof = sql_source_deletion_proof(
                source_entity_id,
                &state.source_revision,
                complete_receipt,
            );
            let intent = coalesce_sql_source_tombstone_to_s1(
                binding,
                scope_digest,
                record,
                source_entity_id,
                &state.source_revision,
                proof,
            )
            .map_err(|error| {
                SemanticCodeError::Refused(format!(
                    "authoritative SQL deletion proof rejected: {error:?}"
                ))
            })?;
            receipts.push(
                self.store
                    .enqueue_reconciliation_tombstone(&intent, state, now_ms)?,
            );
        }
        Ok(receipts)
    }

    /// Reuse the durable binding and native-scope checks across page, snapshot,
    /// and single-row SQL wakeups. No source read runs before these checks.
    pub(super) fn validated_source_wakeup(
        &self,
        record: &MutationOutboxRecord,
    ) -> Result<(SemanticBinding, SemanticDigest, SemanticSourceDirtyIntent), SemanticCodeError>
    {
        let binding = self.store.read_binding()?.ok_or_else(|| {
            SemanticCodeError::Refused("source wakeup has no durable semantic binding".to_string())
        })?;
        let scope_digest = SemanticDigest::from_bytes(*record.identity.binding_digest().as_bytes());
        let wakeup =
            validate_source_dirty_record(&binding, scope_digest, record).map_err(|error| {
                SemanticCodeError::Refused(format!("source wakeup rejected: {error:?}"))
            })?;
        Ok((binding, scope_digest, wakeup))
    }

    /// Read and validate an authoritative page only after wakeup admission.
    pub(super) fn validated_source_page(
        &self,
        binding: &SemanticBinding,
        wakeup: &SemanticSourceDirtyIntent,
        record: &MutationOutboxRecord,
        read_port: &dyn SemanticSqlSourceReadPort,
        cursor: Option<&[u8]>,
    ) -> Result<SemanticSqlSourceReadPage, SemanticCodeError> {
        let page = read_port
            .read_current_sql_source_page(binding, wakeup, record, cursor)
            .map_err(|error| {
                SemanticCodeError::Refused(format!(
                    "authoritative SQL source read rejected: {error:?}"
                ))
            })?;
        page.validate().map_err(|error| {
            SemanticCodeError::Refused(format!("authoritative SQL source page rejected: {error:?}"))
        })?;
        validate_page_revision_against_binding(binding, &page.source_revision)?;
        Ok(page)
    }

    /// Resolve one bounded SQL wakeup page against the binding already
    /// admitted in this owner. Each row is admitted through the same durable
    /// S1 path; retrying a partially committed page replays already admitted
    /// row intents before continuing with the opaque cursor.
    pub fn admit_sql_source_dirty_page(
        &self,
        record: &MutationOutboxRecord,
        read_port: &dyn SemanticSqlSourceReadPort,
        cursor: Option<&[u8]>,
        now_ms: u64,
    ) -> Result<SemanticSqlSourcePageAdmission, SemanticCodeError> {
        let (binding, scope_digest, wakeup) = self.validated_source_wakeup(record)?;
        let page = self.validated_source_page(&binding, &wakeup, record, read_port, cursor)?;
        self.admit_sql_source_page(&binding, scope_digest, record, &page, now_ms)
    }
}
