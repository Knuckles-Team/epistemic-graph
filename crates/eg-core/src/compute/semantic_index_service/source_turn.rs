use super::*;

struct ScanCounters {
    rows_seen: u64,
    source_bytes_seen: u64,
    pages_seen: u64,
}

struct CompletedScan<'a> {
    binding: &'a SemanticBinding,
    scope_digest: SemanticDigest,
    record: &'a MutationOutboxRecord,
    wakeup_digest: SemanticDigest,
    page: &'a SemanticSqlSourceReadPage,
    revision: &'a str,
    checkpoint: Option<&'a SemanticSourceReconciliationCheckpoint>,
    counters: ScanCounters,
    receipts: Vec<SemanticMutationReceipt>,
    now_ms: u64,
}

fn scan_counters(
    checkpoint: Option<&SemanticSourceReconciliationCheckpoint>,
    page_rows: u64,
    page_source_bytes: u64,
) -> Result<ScanCounters, SemanticCodeError> {
    let rows_seen = checkpoint
        .map_or(0, |state| state.rows_seen)
        .checked_add(page_rows)
        .ok_or_else(|| {
            SemanticCodeError::Refused("source reconciliation row counter overflowed".to_string())
        })?;
    let source_bytes_seen = checkpoint
        .map_or(0, |state| state.source_bytes_seen)
        .checked_add(page_source_bytes)
        .ok_or_else(|| {
            SemanticCodeError::Refused("source reconciliation byte counter overflowed".to_string())
        })?;
    let pages_seen = checkpoint
        .map_or(0, |state| state.pages_seen)
        .checked_add(1)
        .ok_or_else(|| {
            SemanticCodeError::Refused("source reconciliation page counter overflowed".to_string())
        })?;
    Ok(ScanCounters {
        rows_seen,
        source_bytes_seen,
        pages_seen,
    })
}

impl SemanticIndexService {
    /// Read and admit one bounded prefix of an authoritative SQL snapshot.
    /// The owner checkpoint is advanced only after the page's S1 intents are
    /// durable, so a crash can replay a prefix but can never skip it. A
    /// complete page enters a second bounded phase that pages durable prior
    /// identities and emits absence tombstones only after the complete proof.
    /// The source-progress rows written by each S1 admission are the durable
    /// seen set; the checkpoint stores only cursors and counters, avoiding an
    /// unbounded serialized identity vector.
    pub fn admit_sql_source_dirty_reconcile(
        &self,
        record: &MutationOutboxRecord,
        read_port: &dyn SemanticSqlSourceReadPort,
        now_ms: u64,
    ) -> Result<SemanticSqlSourceReconciliationAdmission, SemanticCodeError> {
        let (binding, scope_digest, wakeup) = self.validated_source_wakeup(record)?;
        let wakeup_digest = source_reconciliation_wakeup_digest(record, &wakeup);
        let mut checkpoint = match self.load_checkpoint_for_wakeup(&binding, wakeup_digest)? {
            ReconciliationCheckpointState::Owned(checkpoint) => checkpoint,
            ReconciliationCheckpointState::Pending(admission) => return Ok(admission),
        };
        if let Some(admission) = self.resume_tombstone_reconciliation(
            &binding,
            scope_digest,
            record,
            checkpoint.as_ref(),
            now_ms,
        )? {
            return Ok(admission);
        }
        let mut cursor = checkpoint
            .as_ref()
            .and_then(|state| state.source_cursor.clone());
        let mut previous_revision = checkpoint
            .as_ref()
            .map(|state| state.source_revision.clone());
        let mut seen_entities = BTreeSet::new();
        let mut seen_cursors = BTreeSet::new();
        let mut receipts = Vec::new();
        let mut budget = ReconciliationTurnBudget::default();

        loop {
            let page =
                read_reconciliation_page(read_port, &binding, &wakeup, record, cursor.as_deref())?;
            let page_source_bytes = validate_reconciliation_page(
                self,
                &binding,
                &page,
                cursor.as_deref(),
                &mut seen_cursors,
                &mut previous_revision,
                &mut seen_entities,
            )?;
            let page_rows = page.sources.len() as u64;
            if budget.would_exceed(page_rows, page_source_bytes) {
                return self.persist_scan_budget_checkpoint(
                    &binding,
                    previous_revision.as_deref(),
                    cursor.clone(),
                    checkpoint.as_ref(),
                    receipts,
                );
            }

            let page_admission =
                self.admit_sql_source_page(&binding, scope_digest, record, &page, now_ms)?;
            receipts.extend(page_admission.receipts);
            budget.record(page_rows, page_source_bytes);

            let revision = previous_revision.as_deref().ok_or_else(|| {
                SemanticCodeError::Corrupt(
                    "source page had no canonical revision after admission".to_string(),
                )
            })?;
            let counters = scan_counters(checkpoint.as_ref(), page_rows, page_source_bytes)?;

            if !page.complete {
                match self.persist_partial_reconciliation_page(
                    &binding,
                    checkpoint.as_ref(),
                    ScannedPage {
                        page: &page,
                        cursor: cursor.as_deref(),
                    },
                    SourceScanProgress {
                        source_wakeup_digest: wakeup_digest,
                        source_revision: revision,
                        rows_seen: counters.rows_seen,
                        source_bytes_seen: counters.source_bytes_seen,
                        pages_seen: counters.pages_seen,
                    },
                    &budget,
                    receipts,
                )? {
                    PartialReconciliationProgress::Yield(admission) => return Ok(admission),
                    PartialReconciliationProgress::Continue {
                        checkpoint: next_checkpoint,
                        cursor: next_cursor,
                        receipts: next_receipts,
                    } => {
                        checkpoint = Some(next_checkpoint);
                        cursor = Some(next_cursor);
                        receipts = next_receipts;
                    }
                }
                continue;
            }

            return self.complete_reconciliation_scan(CompletedScan {
                binding: &binding,
                scope_digest,
                record,
                wakeup_digest,
                page: &page,
                revision,
                checkpoint: checkpoint.as_ref(),
                counters,
                receipts,
                now_ms,
            });
        }
    }

    fn complete_reconciliation_scan(
        &self,
        completed: CompletedScan<'_>,
    ) -> Result<SemanticSqlSourceReconciliationAdmission, SemanticCodeError> {
        let complete_receipt =
            completed
                .page
                .complete_snapshot_receipt_digest
                .ok_or_else(|| {
                    SemanticCodeError::Corrupt(
                        "validated complete source page lost its read proof".to_string(),
                    )
                })?;
        let state = SemanticSourceReconciliationCheckpoint {
            source_wakeup_digest: completed.wakeup_digest,
            source_revision: completed.revision.to_string(),
            phase: SemanticSourceReconciliationPhase::FinalizingTombstones,
            source_cursor: None,
            prior_cursor: None,
            rows_seen: completed.counters.rows_seen,
            source_bytes_seen: completed.counters.source_bytes_seen,
            pages_seen: completed.counters.pages_seen,
            complete_snapshot_receipt_digest: Some(complete_receipt),
        };
        self.store.write_source_reconciliation_checkpoint(
            completed.binding.generation,
            completed.checkpoint,
            &state,
        )?;
        self.finalize_source_reconciliation(
            completed.binding,
            completed.scope_digest,
            completed.record,
            state,
            completed.receipts,
            completed.now_ms,
        )
    }

    pub(super) fn finalize_source_reconciliation(
        &self,
        binding: &SemanticBinding,
        scope_digest: SemanticDigest,
        record: &MutationOutboxRecord,
        state: SemanticSourceReconciliationCheckpoint,
        mut receipts: Vec<SemanticMutationReceipt>,
        now_ms: u64,
    ) -> Result<SemanticSqlSourceReconciliationAdmission, SemanticCodeError> {
        validate_checkpoint_against_binding(binding, &state)?;
        let (prior_entities, next_prior_cursor) = self.store.list_source_entities_page(
            binding.generation,
            state.prior_cursor.as_deref(),
            Self::MAX_SOURCE_RECONCILIATION_TOMBSTONES_PER_TURN,
        )?;
        let prior_entities = validate_prior_entity_page(
            prior_entities,
            next_prior_cursor.as_deref(),
            state.prior_cursor.as_deref(),
            Self::MAX_SOURCE_RECONCILIATION_TOMBSTONES_PER_TURN,
        )?;
        receipts = self.admit_missing_tombstones(
            AdmittedSourceWakeup {
                binding,
                scope_digest,
                record,
            },
            &state,
            &prior_entities,
            receipts,
            now_ms,
        )?;

        if let Some(prior_cursor) = next_prior_cursor {
            let next_state = SemanticSourceReconciliationCheckpoint {
                prior_cursor: Some(prior_cursor),
                ..state.clone()
            };
            self.store.write_source_reconciliation_checkpoint(
                binding.generation,
                Some(&state),
                &next_state,
            )?;
            return admission_result(receipts, &next_state, false);
        }

        // Clearing follows every final tombstone intent. A crash before this
        // point rechecks the bounded prior page and replays already-durable
        // intents; a crash after it cannot leave a false complete marker.
        self.store
            .clear_source_reconciliation_checkpoint(binding.generation, &state)?;
        admission_result(receipts, &state, true)
    }

    /// Resolve one coarse SQL wakeup when the source owner guarantees one
    /// complete row. Multi-row providers must use
    /// [`Self::admit_sql_source_dirty_page`].
    pub fn admit_sql_source_dirty_record(
        &self,
        record: &MutationOutboxRecord,
        read_port: &dyn SemanticSqlSourceReadPort,
        now_ms: u64,
    ) -> Result<SemanticMutationReceipt, SemanticCodeError> {
        let (binding, scope_digest, wakeup) = self.validated_source_wakeup(record)?;
        let page = self.validated_source_page(&binding, &wakeup, record, read_port, None)?;
        if !page.complete || page.sources.len() != 1 {
            return Err(SemanticCodeError::Refused(
                "single-row SQL admission requires one complete source page".to_string(),
            ));
        }
        let admission =
            self.admit_sql_source_page(&binding, scope_digest, record, &page, now_ms)?;
        admission.receipts.into_iter().next().ok_or_else(|| {
            SemanticCodeError::Refused("complete single-row SQL page was empty".to_string())
        })
    }

    /// Admit S1 after a source refresh has advanced the durable binding head.
    /// The current and replacement rows are both checked before the intent is
    /// built, so a caller cannot route a snapshot into an unrelated binding or
    /// generation.
    pub fn admit_sql_source_dirty_replacement(
        &self,
        replacement: &SemanticBinding,
        source_manifest: &SemanticSqlSourceManifest,
        record: &MutationOutboxRecord,
        now_ms: u64,
    ) -> Result<SemanticMutationReceipt, SemanticCodeError> {
        let current = self.store.read_binding()?.ok_or_else(|| {
            SemanticCodeError::Refused(
                "replacement source wakeup has no current semantic binding".to_string(),
            )
        })?;
        if current.binding_id != replacement.binding_id
            || current.tenant_id != replacement.tenant_id
            || current.generation != replacement.generation
            || current.binding_digest != replacement.binding_digest
            || current.durable_state != SemanticBindingState::Pending
        {
            return Err(SemanticCodeError::Refused(
                "replacement source wakeup does not name the durable binding head".to_string(),
            ));
        }
        let scope_digest = SemanticDigest::from_bytes(*record.identity.binding_digest().as_bytes());
        validate_source_dirty_record(&current, scope_digest, record).map_err(|error| {
            SemanticCodeError::Refused(format!("source wakeup rejected: {error:?}"))
        })?;
        let source_entity_id = source_manifest.source_entity_id.as_str();
        let intent = coalesce_sql_snapshot_to_s1(
            &current,
            source_entity_id,
            scope_digest,
            record,
            source_manifest,
        )
        .map_err(|error| {
            SemanticCodeError::Refused(format!("semantic replacement rejected: {error:?}"))
        })?;
        self.store.enqueue_stage_intent(&intent, now_ms)
    }
}
