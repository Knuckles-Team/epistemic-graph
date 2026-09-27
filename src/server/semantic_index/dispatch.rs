use super::*;

impl SemanticIndexServerAdapter {
    /// Resolve a committed S1 from its durable lease and expected intent. The
    /// core store reads the server-generated transition and receipt; this path
    /// therefore needs no pre-crash raw claim, cursor or completion timestamp
    /// and performs no SQL source or ACL read.
    pub(crate) async fn replay_sql_source_stage(
        &self,
        req_id: u64,
        authority: CarrierAuthority,
        lease: MutationOutboxLease,
        expected_intent: SemanticStageIntent,
        now_ms: u64,
    ) -> Result<Option<eg_core::compute::semantic_ann_codes::SemanticMutationReceipt>, Response>
    {
        let binding_service = Arc::clone(&self.service);
        let binding = compute_off_lock(req_id, move || binding_service.binding())
            .await?
            .map_err(|error| {
                crate::server::handlers::semantic_index::semantic_failure_response(req_id, error)
            })?
            .ok_or_else(|| Response::err(req_id, "CONFLICT: semantic binding is unavailable"))?;
        self.authorize_binding_worker(&binding, &authority)
            .map_err(|error| Response::err(req_id, error))?;
        crate::server::handlers::semantic_index::own_lease(&lease, &authority)
            .map_err(|error| Response::err(req_id, error))?;
        let service = Arc::clone(&self.service);
        compute_off_lock(req_id, move || {
            service.replay_completed_sql_source_stage(&lease, &expected_intent, now_ms)
        })
        .await?
        .map(|replay| replay.map(|(_transition, receipt)| receipt))
        .map_err(|error| {
            crate::server::handlers::semantic_index::semantic_failure_response(req_id, error)
        })
    }

    /// Capture one raw row from a bounded authorized page. The opaque input
    /// cursor is retained so fresh completion can re-read the same page; an
    /// already committed retry resolves from the durable stage record first.
    pub(crate) async fn claim_sql_source(
        &self,
        req_id: u64,
        port: AuthorizedSqlSourceReadPort,
        binding: SemanticBinding,
        intent: SemanticStageIntent,
        page_cursor: Option<Vec<u8>>,
    ) -> Result<AuthorizedSqlSourceClaim, Response> {
        let read_cursor = page_cursor.clone();
        let result = compute_off_lock(req_id, move || {
            let source_entity_id = intent
                .scope
                .source_entity_id()
                .ok_or(SemanticIndexError::SourceManifestMismatch)?;
            let snapshot = port.read_snapshot_with_decision(&binding, read_cursor.as_deref())?;
            let source = snapshot
                .page
                .sources
                .into_iter()
                .find(|source| source.source_entity_id() == source_entity_id)
                .ok_or(SemanticIndexError::SourceManifestMismatch)?;
            source_claim_matches_intent(&source, &intent)?;
            Ok::<_, SemanticIndexError>(AuthorizedSqlSourceClaim {
                source,
                page_cursor,
                decision_at_ms: snapshot.decision_at_ms,
                intent_digest: intent.intent_digest,
            })
        })
        .await?;
        result.map_err(|_| Response::err(req_id, "CONFLICT: semantic SQL source claim was refused"))
    }

    /// Reconstitute one deletion claim from the retained prior source manifest
    /// and an authenticated current complete snapshot. The entity id is a
    /// one-way digest, so the prior identity is read from the semantic owner;
    /// it is never reconstructed from caller text.
    pub(crate) async fn claim_sql_tombstone(
        &self,
        req_id: u64,
        port: AuthorizedSqlSourceReadPort,
        binding: SemanticBinding,
        intent: SemanticStageIntent,
        page_cursor: Option<Vec<u8>>,
    ) -> Result<AuthorizedSqlSourceClaim, Response> {
        let service = Arc::clone(&self.service);
        // Keep the owner's detailed mismatch reason inside the server. It can
        // contain backend text and must not become a client response or log.
        let result = compute_off_lock(req_id, move || {
            let source_entity_id = intent
                .scope
                .source_entity_id()
                .ok_or_else(|| source_mismatch("stage scope names no source entity"))?;
            let prior = service
                .sql_source_manifest(binding.generation, source_entity_id)
                .map_err(|error| {
                    source_mismatch(format!(
                        "prior SQL source manifest is unreadable: {error:?}"
                    ))
                })?
                .ok_or_else(|| {
                    source_mismatch("no retained prior SQL source manifest for this source entity")
                })?;
            let snapshot = port
                .read_snapshot_with_decision(&binding, page_cursor.as_deref())
                .map_err(refused_by("authorized complete-snapshot read refused"))?;
            tombstone_claim_from_complete_page(&binding, &intent, &prior, page_cursor, snapshot)
        })
        .await?;
        result.map_err(|_| {
            Response::err(req_id, "CONFLICT: semantic SQL tombstone claim was refused")
        })
    }

    /// Complete a fresh leased S1 transition from a server-minted raw claim.
    /// Durable replay is resolved and acknowledged before current SQL authority
    /// is consulted, so ACL changes or a later clock sample cannot invalidate a
    /// transition that already committed. A fresh transition re-reads the exact
    /// bounded page and preserves the original decision instant in its receipt.
    pub(crate) async fn complete_sql_source_stage(
        &self,
        req_id: u64,
        port: AuthorizedSqlSourceReadPort,
        binding: SemanticBinding,
        completion: SqlSourceStageCompletion,
        now_ms: u64,
    ) -> Result<eg_core::compute::semantic_ann_codes::SemanticMutationReceipt, Response> {
        let SqlSourceStageCompletion {
            lease,
            transition,
            claim,
            successor,
        } = completion;
        let replay = self
            .replay_sql_source_stage(
                req_id,
                port.authority().clone(),
                lease.clone(),
                transition.intent.clone(),
                now_ms,
            )
            .await?;
        if let Some(receipt) = replay {
            return Ok(receipt);
        }
        if claim.intent_digest != transition.intent.intent_digest {
            return Err(Response::err(
                req_id,
                "CONFLICT: semantic SQL source claim does not match the leased stage intent",
            ));
        }

        let page_cursor = claim.page_cursor.clone();
        let claimed_source = claim.source.clone();
        let source_transition = transition.clone();
        let source_service = Arc::clone(&self.service);
        let result = compute_off_lock(req_id, move || {
            let snapshot = port.read_snapshot_with_decision(&binding, page_cursor.as_deref())?;
            let current = match &claimed_source.value {
                SemanticSqlSourceValue::Present { .. } => snapshot
                    .page
                    .sources
                    .into_iter()
                    .find(|source| source.source_identity == claimed_source.source_identity)
                    .ok_or(SemanticIndexError::SourceManifestMismatch)?,
                SemanticSqlSourceValue::Tombstone { .. } => {
                    let source_entity_id = source_transition
                        .intent
                        .scope
                        .source_entity_id()
                        .ok_or(SemanticIndexError::SourceManifestMismatch)?;
                    let prior = source_service
                        .sql_source_manifest(binding.generation, source_entity_id)
                        .map_err(|_| SemanticIndexError::SourceManifestMismatch)?
                        .ok_or(SemanticIndexError::SourceManifestMismatch)?;
                    tombstone_claim_from_complete_page(
                        &binding,
                        &source_transition.intent,
                        &prior,
                        page_cursor,
                        snapshot,
                    )
                    .map_err(|(error, _reason)| error)?
                    .source
                }
            };
            if current != claimed_source {
                return Err(SemanticIndexError::SourceManifestMismatch);
            }
            Ok::<_, SemanticIndexError>(current)
        })
        .await?;
        let source = result.map_err(|_| {
            Response::err(
                req_id,
                "CONFLICT: semantic SQL source changed before completion",
            )
        })?;
        let authorized_at = format!("unix-ms:{}", claim.decision_at_ms);
        let completion_service = Arc::clone(&self.service);
        compute_off_lock(req_id, move || {
            completion_service.complete_sql_source_stage(
                &lease,
                &transition,
                &source,
                &authorized_at,
                successor.as_ref(),
                now_ms,
            )
        })
        .await?
        .map_err(|error| {
            crate::server::handlers::semantic_index::semantic_failure_response(req_id, error)
        })
    }
}
