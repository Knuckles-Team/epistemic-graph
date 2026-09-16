use super::*;

/// Inputs used by the replay-authority comparison. Keeping this as one
/// request boundary makes it harder to accidentally compare fields from a
/// different authority while retaining the individual diagnostics below.
pub(super) struct ReplayRecordCheck<'a> {
    pub(super) record: &'a crate::mutation_batch::MutationBatchRecord,
    pub(super) req: &'a RaftRequest,
    pub(super) batch_id: &'a str,
    pub(super) expected_principal: &'a str,
    pub(super) operation_matches: bool,
    pub(super) expected_result: Option<&'a [u8]>,
}

impl EgStore {
    /// The mutation-authority equality check `try_apply_change_envelope` guards
    /// its commit on (CX WB1-EG-01 CCN reduction) — a long `||` chain contributes
    /// one branch per term regardless of which function it lives in, so lifting
    /// it into its own boolean-returning helper is the cheapest way to keep the
    /// caller's own CCN low. Pure extract-method, byte-identical behaviour.
    pub(super) fn change_envelope_mismatches_authority(
        req: &RaftRequest,
        envelope: &crate::change_envelope::ChangeEnvelope,
        expected_tenant_scope: &str,
    ) -> bool {
        req.mutation.batch_id != envelope.mutation.batch_id
            || eg_types::mutation_batch::batch_request_number(&envelope.mutation)
                != Some(req.mutation.request_id)
            || req.mutation.tenant_scope != expected_tenant_scope
            || Some(req.mutation.principal_fingerprint.as_str())
                != crate::server::mutation_batch::batch_actor(&envelope.mutation)
            || req.mutation.placement_epoch != envelope.mutation.placement_epoch
            || req.mutation.fencing_token != envelope.mutation.fencing_token
            || req.mutation.created_at_ms != envelope.mutation.created_at_ms
    }

    /// Idempotent-replay fast path within the ordinary-mutation staging pipeline.
    /// Phase-2 recovery intentionally submits the same deterministic child
    /// authority again; resolving that receipt BEFORE staging from today's graph
    /// image is required, or its authoritative-state digest would differ and turn
    /// a valid replay into an idempotency conflict. The probe still enters the
    /// universal commit kernel: that is where a caller-supplied attempt nonce is
    /// finalized, so this read cannot become an unaccounted replay. `Ok(Some(..))`
    /// / `Ok(None)` follow the same early-exit-as-value convention as
    /// `try_apply_native_command`.
    pub(super) async fn try_apply_idempotent_replay(
        &self,
        req: &RaftRequest,
        core: &Arc<crate::graph::GraphCore>,
        persistence: &Arc<dyn PersistenceBackend>,
        durable_method: &Method,
        #[cfg(feature = "modality-serving")] modality_command: Option<
            &super::super::SanitizedModalityRaftCommand,
        >,
    ) -> Result<Option<RaftResponse>, String> {
        let authority = &req.mutation;
        let batch_id = authority.batch_id.as_str();
        let expected_principal = authority.principal_fingerprint.as_str();

        let Some(record) = persistence
            .read_mutation_batch(&req.graph_fname, batch_id)
            .await?
        else {
            return Ok(None);
        };
        let encoded_method =
            rmp_serde::to_vec_named(durable_method).map_err(|error| error.to_string())?;
        let expected_digest = format!("sha256:{}", hex::encode(Sha256::digest(&encoded_method)));
        let operation_matches =
            Self::idempotent_replay_operation_matches(&record, &expected_digest);
        #[cfg(feature = "modality-serving")]
        let expected_result = modality_command
            .map(|_| Self::modality_or_default_bool_result(modality_command))
            .transpose()?;
        #[cfg(not(feature = "modality-serving"))]
        let expected_result: Option<Vec<u8>> = None;
        if let Some(mismatch) = Self::idempotent_replay_record_mismatches(ReplayRecordCheck {
            record: &record,
            req,
            batch_id,
            expected_principal,
            operation_matches,
            expected_result: expected_result.as_deref(),
        }) {
            // Name the term that fired. A bare "conflicts with replay authority"
            // says a seven-way disjunction was true and nothing about WHICH,
            // which is the difference between a one-run diagnosis and a guess.
            return Err(format!(
                "replicated child receipt conflicts with replay authority: {mismatch}"
            ));
        }
        #[cfg(feature = "modality-serving")]
        let modality_replay = modality_command.is_some();
        #[cfg(not(feature = "modality-serving"))]
        let modality_replay = false;
        // Validate the stored terminal payload before admitting the probe. A
        // normal graph mutation has one terminal shape (`Bool(true)`), while
        // Create/CAS expose their exact boolean outcome to the caller. Keeping
        // these checks separate prevents a corrupt/non-boolean receipt from
        // being reported as ordinary CAS contention.
        let _validated_result = Self::decode_replayed_graph_result(
            record.result_msgpack.as_deref(),
            durable_method,
            modality_replay,
        )?;

        let mut descriptor =
            record.batch.authoritative_state.clone().ok_or_else(|| {
                "committed replicated graph has no authoritative state".to_string()
            })?;
        let source_version = persistence
            .read_mutation_graph_version(&req.graph_fname)
            .await?
            .ok_or_else(|| "committed replicated graph has no authoritative version".to_string())?;
        descriptor.source_graph_version = source_version;
        descriptor.target_graph_version = source_version
            .checked_add(1)
            .ok_or_else(|| "authoritative graph version overflow".to_string())?;
        let audited = eg_capabilities::policy(durable_method).audited;
        let probe = crate::server::mutation_batch::compile_methods(
            crate::server::mutation_batch::CompileBatch {
                batch_id,
                request_id: authority.request_id,
                attempt_nonce: authority.attempt_nonce,
                principal: Some(expected_principal),
                tenant: &authority.tenant_scope,
                graph: &req.graph_name,
                placement_epoch: authority.placement_epoch,
                idempotency_key: batch_id,
                expected_graph_version: Some(source_version),
                fencing_token: authority.fencing_token,
                created_at_ms: authority.created_at_ms,
                default_surface: crate::mutation_batch::MutationSurface::Graph,
                authoritative_state: Some(descriptor),
            },
            vec![durable_method.clone()],
        )?;
        probe.validate()?;
        let committed = persistence
            .commit_mutation_batch_state(
                &req.graph_fname,
                &probe,
                Vec::new(),
                None,
                authority.created_at_ms,
                audited,
            )
            .await?;
        if !committed.replayed {
            return Err("replicated replay probe unexpectedly committed fresh work".to_string());
        }
        let native_result = Self::decode_replayed_graph_result(
            committed.record.result_msgpack.as_deref(),
            durable_method,
            modality_replay,
        )?;
        self.install_authoritative_graph_snapshot(req, core, persistence)
            .await?;
        Ok(Some(RaftResponse {
            applied: true,
            native_result,
            native_commit: Some(committed),
            ..Default::default()
        }))
    }

    /// Decode and validate the terminal result carried by an ordinary replicated
    /// graph receipt. The result is part of the durable idempotency contract, so
    /// a missing, malformed, or wrong-shape value must fail closed. Modality
    /// receipts retain their command-specific payload and are validated by the
    /// exact-byte comparison above.
    pub(super) fn decode_replayed_graph_result(
        result_msgpack: Option<&[u8]>,
        durable_method: &Method,
        modality_replay: bool,
    ) -> Result<Option<ResultPayload>, String> {
        let result = result_msgpack.ok_or_else(|| {
            if matches!(
                durable_method,
                Method::CreateNodeIfAbsent { .. } | Method::CompareAndSetNodeFields { .. }
            ) {
                "replicated CAS receipt is missing its exact apply result".to_string()
            } else {
                "replicated graph receipt is missing its terminal result".to_string()
            }
        })?;
        let decoded: ResultPayload = rmp_serde::from_slice(result).map_err(|_| {
            if matches!(
                durable_method,
                Method::CreateNodeIfAbsent { .. } | Method::CompareAndSetNodeFields { .. }
            ) {
                "replicated CAS receipt result is corrupt".to_string()
            } else {
                "replicated graph receipt result is corrupt".to_string()
            }
        })?;
        if modality_replay {
            return Ok(None);
        }
        match durable_method {
            Method::CreateNodeIfAbsent { .. } | Method::CompareAndSetNodeFields { .. } => {
                match decoded {
                    ResultPayload::Bool(value) => Ok(Some(ResultPayload::Bool(value))),
                    _ => Err("replicated CAS receipt result must be Bool".to_string()),
                }
            }
            _ => match decoded {
                ResultPayload::Bool(true) => Ok(None),
                ResultPayload::Bool(false) => {
                    Err("replicated graph receipt result must be Bool(true)".to_string())
                }
                _ => Err("replicated graph receipt result must be Bool(true)".to_string()),
            },
        }
    }

    /// Whether the durable record's single operation matches the expected
    /// authoritative-state digest (CX WB1-EG-01 CCN reduction). Pure
    /// extract-method, byte-identical behaviour.
    pub(super) fn idempotent_replay_operation_matches(
        record: &crate::mutation_batch::MutationBatchRecord,
        expected_digest: &str,
    ) -> bool {
        record.batch.operations.len() == 1
            && matches!(
                &record.batch.operations[0].method,
                crate::protocol::Method::ApplyMutation { event_type, query }
                    if event_type == "authoritative_state_operation"
                        && query == expected_digest
            )
    }

    /// Install the graph's current committed authoritative snapshot onto
    /// `core` (CX WB1-EG-01 CCN reduction). Shared by
    /// `try_apply_idempotent_replay` (a phase-2-recovery replay) and
    /// `finalize_ordinary_commit` (a redundant commit that turned out to
    /// already be durably committed) — both did this identical
    /// read-then-install sequence inline before this extraction. Pure
    /// extract-method, byte-identical behaviour.
    pub(super) async fn install_authoritative_graph_snapshot(
        &self,
        req: &RaftRequest,
        core: &Arc<crate::graph::GraphCore>,
        persistence: &Arc<dyn PersistenceBackend>,
    ) -> Result<(), String> {
        let (snapshot, version) = persistence
            .read_authoritative_graph_snapshot(&req.graph_fname)
            .await?
            .ok_or_else(|| "committed replicated graph image is missing".to_string())?;
        core.install_committed_snapshot(snapshot, version)
    }

    /// The idempotent-replay record's mismatch-against-authority check
    /// (CX WB1-EG-01 CCN reduction) — same rationale as
    /// `change_envelope_mismatches_authority`: a long `||` chain contributes one
    /// branch per term regardless of which function it lives in.
    ///
    /// Returns the NAME of the first term that fired rather than a bare `bool`.
    /// The caller's error used to say only that "the replicated child receipt
    /// conflicts with replay authority", which is a seven-way disjunction
    /// reported as one sentence: every failure of this guard cost a bisect to
    /// learn which field disagreed. The names are static strings, carry no
    /// tenant/principal/graph value, and so keep the diagnostic privacy-safe.
    pub(super) fn idempotent_replay_record_mismatches(
        check: ReplayRecordCheck<'_>,
    ) -> Option<&'static str> {
        let ReplayRecordCheck {
            record,
            req,
            batch_id,
            expected_principal,
            operation_matches,
            expected_result,
        } = check;
        // `bind_caller_batch` persists the physical, sanitized file name in the
        // graph scope. Compare that durable key with `graph_fname`, not the
        // request's logical display name. `graph_name()` is `None` for a native
        // scope; fail closed instead of silently accepting one (no
        // sentinel/empty-string substitution — see MIGRATION-CONTRACT.md).
        let graph_matches = match record.batch.identity.scope().graph_name() {
            Some(graph_name) => graph_name.as_str() == req.graph_fname.as_str(),
            None => false,
        };
        if record.status != crate::mutation_batch::MutationBatchStatus::Committed {
            return Some("durable record is not Committed");
        }
        if record.batch.batch_id != batch_id {
            return Some("durable batch id differs from the authority's");
        }
        // The caller's tenant, read from the envelope's preserved authority --
        // NOT from `record.batch.identity`, which the durable bind path rewrote
        // to the shard's own reserved scope before storing (see
        // `MutationBatchRecord::committing_tenant`). Comparing the rebound
        // identity meant comparing `__shard__` against a caller tenant that is
        // forbidden to BE `__shard__`, so this term fired on EVERY replicated
        // replay: the idempotent-replay fast path this guard protects -- the one
        // Phase-2 recovery depends on to return a cached receipt instead of
        // re-applying -- was unreachable, for every caller, on every graph.
        if !matches!(record.committing_tenant(), Ok(tenant) if tenant == req.mutation.tenant_scope.as_str())
        {
            return Some("durable tenant scope differs from the authority's");
        }
        if !graph_matches {
            return Some("durable scope names a different graph (or no graph at all)");
        }
        if !matches!(record.committing_actor(), Ok(actor) if actor == expected_principal) {
            return Some("durable committing actor differs from the authority's principal");
        }
        if !operation_matches {
            return Some("durable operation digest differs from the proposed method");
        }
        if expected_result
            .is_some_and(|expected| record.result_msgpack.as_deref() != Some(expected))
        {
            return Some("durable result differs from the expected replay result");
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn physical_graph_replay_fixture(
        logical_graph: &str,
    ) -> (
        crate::mutation_batch::MutationBatchRecord,
        RaftRequest,
        String,
    ) {
        let graph_fname = crate::persist::sanitize(logical_graph);
        let method = Method::AddNode {
            node_id: "replay-scope-proof".to_string(),
            properties_msgpack: Vec::new(),
        };
        let batch_id = "replay-scope-proof";
        let batch = crate::server::mutation_batch::compile_methods(
            crate::server::mutation_batch::CompileBatch {
                batch_id,
                request_id: 9,
                attempt_nonce: None,
                principal: Some("replay-scope-proof-principal"),
                tenant: "replay-scope-proof-tenant",
                graph: &graph_fname,
                placement_epoch: 0,
                idempotency_key: batch_id,
                expected_graph_version: Some(0),
                fencing_token: None,
                created_at_ms: 11,
                default_surface: crate::mutation_batch::MutationSurface::Graph,
                authoritative_state: None,
            },
            vec![method.clone()],
        )
        .expect("replay fixture batch must compile");
        let identity = batch.identity.clone();
        let record = crate::mutation_batch::MutationBatchRecord {
            batch,
            identity,
            status: crate::mutation_batch::MutationBatchStatus::Committed,
            committed_version: crate::mutation_batch::CommittedVersion::Graph {
                source: 0,
                target: 1,
            },
            result_msgpack: Some(
                rmp_serde::to_vec_named(&ResultPayload::Bool(true))
                    .expect("replay fixture result must encode"),
            ),
            committed_at_ms: 11,
        };
        let principal = record
            .committing_actor()
            .expect("replay fixture must preserve the caller actor")
            .to_string();
        let request = RaftRequest {
            graph_fname,
            graph_name: logical_graph.to_string(),
            graph_type: GraphType::Global,
            command: ReplicatedMutation::graph(method, "cluster-test-key")
                .expect("replay fixture command must seal"),
            committed_at_ms: 11,
            mutation: crate::raft::RaftMutationContext {
                batch_id: batch_id.to_string(),
                request_id: 9,
                attempt_nonce: None,
                tenant_scope: "replay-scope-proof-tenant".to_string(),
                principal_fingerprint: principal.clone(),
                identity_bootstrap: false,
                placement_epoch: 0,
                fencing_token: None,
                created_at_ms: 11,
            },
        };
        (record, request, principal)
    }

    #[test]
    fn replay_scope_uses_physical_name_for_escaped_and_hashed_graphs() {
        let long_graph = "x".repeat(300);
        for logical_graph in ["a:b".to_string(), long_graph] {
            let (record, request, principal) = physical_graph_replay_fixture(&logical_graph);
            assert_ne!(
                request.graph_name, request.graph_fname,
                "fixture must exercise a sanitized graph key"
            );
            let matching = EgStore::idempotent_replay_record_mismatches(ReplayRecordCheck {
                record: &record,
                req: &request,
                batch_id: request.mutation.batch_id.as_str(),
                expected_principal: &principal,
                operation_matches: true,
                expected_result: None,
            });
            assert_eq!(
                matching, None,
                "physical graph key must replay: {logical_graph:?}"
            );

            let mut logical_key_request = request.clone();
            logical_key_request.graph_fname = logical_graph.clone();
            let mismatch = EgStore::idempotent_replay_record_mismatches(ReplayRecordCheck {
                record: &record,
                req: &logical_key_request,
                batch_id: logical_key_request.mutation.batch_id.as_str(),
                expected_principal: &principal,
                operation_matches: true,
                expected_result: None,
            });
            assert_eq!(
                mismatch,
                Some("durable scope names a different graph (or no graph at all)"),
                "logical graph name must not match the persisted physical key: {logical_graph:?}"
            );
        }
    }
}
