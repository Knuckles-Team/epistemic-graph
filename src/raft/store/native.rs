use super::*;

impl EgStore {
    /// Native-mutation-command fast path of `apply_request` (CX WB1-EG-01 CCN
    /// reduction). `Ok(Some(response))` means "return this from `apply_request`
    /// immediately" (mirrors every `return Ok(RaftResponse{..})` this block used
    /// to perform directly); `Ok(None)` means "fall through to the ordinary
    /// graph-mutation path" (mirrors the block's implicit fallthrough when
    /// `req.command` isn't `Native`, or is `Native` but matches neither an
    /// explicit `NativeMutationCommand` arm nor `open_public_method`). Pure
    /// extract-method: every `return Ok(RaftResponse{..})` below is verbatim
    /// from the original inline block, just wrapped in `Some(..)`.
    pub(super) async fn try_apply_native_command(
        &self,
        req: &RaftRequest,
        server_secret: &str,
    ) -> Result<Option<RaftResponse>, String> {
        req.validate_graph_command(server_secret, self.group_id)?;
        if let ReplicatedMutation::Native { command } = &req.command {
            match command {
                NativeMutationCommand::TransactionParticipant {
                    phase,
                    coordinator_id,
                    participant_id,
                    ..
                } => {
                    let plan = command.open_transaction_plan(server_secret)?;
                    let outcome = crate::server::apply_replicated_transaction_participant(
                        &self.ctx.state,
                        req.mutation.request_id,
                        req.committed_at_ms,
                        &req.mutation,
                        self.group_id,
                        *phase,
                        crate::server::ReplicatedParticipantRef {
                            coordinator_id,
                            participant_id: *participant_id,
                            plan: plan.as_deref(),
                        },
                    )
                    .await;
                    return Ok(Some(Self::native_bool_outcome_to_response(outcome)));
                }
                NativeMutationCommand::TransactionDecision {
                    coordinator_id,
                    commit,
                } => {
                    let outcome = crate::server::apply_replicated_transaction_decision(
                        &self.ctx.state,
                        req.committed_at_ms,
                        &req.mutation,
                        coordinator_id,
                        *commit,
                    )
                    .await;
                    return Ok(Some(Self::native_bool_outcome_to_response(outcome)));
                }
                NativeMutationCommand::TransactionFinalize {
                    coordinator_id,
                    commit,
                } => {
                    let outcome = crate::server::apply_replicated_transaction_finalize(
                        &self.ctx.state,
                        req.committed_at_ms,
                        &req.mutation,
                        coordinator_id,
                        *commit,
                    )
                    .await;
                    return Ok(Some(Self::native_bool_outcome_to_response(outcome)));
                }
                #[cfg(feature = "jobs")]
                NativeMutationCommand::JobPublicationCommit { coordinator_id, .. } => {
                    let plan = command.open_job_publication_payload(server_secret)?;
                    let outcome = crate::server::apply_replicated_job_publication_commit(
                        &self.ctx.state,
                        req.mutation.request_id,
                        req.committed_at_ms,
                        &req.mutation,
                        self.group_id,
                        coordinator_id,
                        &plan,
                    )
                    .await;
                    return Ok(Some(Self::native_commit_outcome_to_response(outcome)));
                }
                #[cfg(feature = "jobs")]
                NativeMutationCommand::JobPublicationFinalize { coordinator_id, .. } => {
                    let receipt = command.open_job_publication_payload(server_secret)?;
                    let outcome = crate::server::apply_replicated_job_publication_finalize(
                        &self.ctx.state,
                        req.committed_at_ms,
                        &req.mutation,
                        coordinator_id,
                        &receipt,
                    )
                    .await;
                    return Ok(Some(Self::native_result_outcome_to_response(outcome)));
                }
                NativeMutationCommand::NodeInfo { .. } => {
                    let info = command.open_node_info(server_secret)?;
                    let outcome = crate::server::apply_replicated_node_info(
                        &self.ctx.state,
                        req.mutation.request_id,
                        info,
                    )
                    .await;
                    return Ok(Some(Self::native_bool_outcome_to_response(outcome)));
                }
                _ => {}
            }
            if let Some(method) = command.open_public_method(server_secret)? {
                return Ok(Some(
                    self.try_apply_public_native_method(req, method).await?,
                ));
            }
        }
        Ok(None)
    }

    /// `Ok(value) => RaftResponse{native_result: Some(Bool(value))}` /
    /// `Err(error) => RaftResponse{native_error: Some(error)}`, shared by every
    /// `NativeMutationCommand` arm whose outcome is a bare `Result<bool, String>`
    /// (CX WB1-EG-01 CCN reduction). Pure extract-method, byte-identical
    /// behaviour.
    pub(super) fn native_bool_outcome_to_response(outcome: Result<bool, String>) -> RaftResponse {
        match outcome {
            Ok(value) => RaftResponse {
                applied: true,
                native_result: Some(crate::protocol::ResultPayload::Bool(value)),
                ..Default::default()
            },
            Err(error) => RaftResponse {
                applied: true,
                native_error: Some(error),
                ..Default::default()
            },
        }
    }

    /// Same shape as [`native_bool_outcome_to_response`], for the one
    /// `NativeMutationCommand` arm (`JobPublicationFinalize`) whose outcome is
    /// already a typed `ResultPayload` rather than a bare `bool`.
    pub(super) fn native_result_outcome_to_response(
        outcome: Result<crate::protocol::ResultPayload, String>,
    ) -> RaftResponse {
        match outcome {
            Ok(result) => RaftResponse {
                applied: true,
                native_result: Some(result),
                ..Default::default()
            },
            Err(error) => RaftResponse {
                applied: true,
                native_error: Some(error),
                ..Default::default()
            },
        }
    }

    /// Preserve the exact durable MutationBatch receipt produced by a native
    /// job-publication commit.  The receipt carries identity and replay state
    /// that cannot be recovered from `applied` or a boolean result.  Ordinary
    /// native commands continue to use the two helpers above unchanged.
    #[cfg(feature = "jobs")]
    pub(super) fn native_commit_outcome_to_response(
        outcome: Result<crate::mutation_batch::MutationBatchCommit, String>,
    ) -> RaftResponse {
        match outcome {
            Ok(commit) => match commit.validate() {
                Ok(()) => RaftResponse {
                    applied: true,
                    native_commit: Some(commit),
                    ..Default::default()
                },
                Err(error) => RaftResponse {
                    applied: true,
                    native_error: Some(format!("native commit receipt is invalid: {error}")),
                    ..Default::default()
                },
            },
            Err(error) => RaftResponse {
                applied: true,
                native_error: Some(error),
                ..Default::default()
            },
        }
    }

    /// The `open_public_method` branch of `try_apply_native_command` (CX
    /// WB1-EG-01 CCN reduction): a replicated `Method` opaque-scoped as a native
    /// command (the `Commit`/transaction-prepare special case, or the ordinary
    /// native-apply path). Pure extract-method, byte-identical behaviour, no
    /// signature change.
    pub(super) async fn try_apply_public_native_method(
        &self,
        req: &RaftRequest,
        method: Method,
    ) -> Result<RaftResponse, String> {
        if crate::server::txn::consensus_graph_is_prepared(&req.graph_name)
            || crate::server::txn::consensus_control_conflicts(&method)
        {
            return Ok(RaftResponse {
                applied: true,
                native_error: Some(
                    "graph is reserved by a prepared consensus transaction".to_string(),
                ),
                ..Default::default()
            });
        }
        let response = if matches!(method, Method::Commit { .. }) {
            let Method::Commit {
                txn_id,
                idempotency_key,
            } = method
            else {
                unreachable!();
            };
            crate::server::apply_replicated_transaction_prepare(
                &self.ctx.state,
                req.mutation.request_id,
                req.committed_at_ms,
                &req.mutation,
                &txn_id,
                idempotency_key.as_deref(),
                Some(&req.mutation.tenant_scope),
            )
            .await
        } else {
            crate::server::apply_replicated_native(
                &self.ctx.state,
                req.graph_name.clone(),
                req.mutation.request_id,
                req.committed_at_ms,
                &req.mutation,
                method,
            )
            .await
        };
        Ok(RaftResponse {
            applied: true,
            native_result: response.result,
            native_error: response.error,
            ..Default::default()
        })
    }

    /// Resolve the target graph's RESIDENT core (CX WB1-EG-01 CCN reduction).
    /// Pure extract-method from `apply_request`'s preamble, byte-identical
    /// behaviour, no signature change. See the call site's doc comment for the
    /// lazy-open / fast-read-lock-then-escalate rationale.
    /// Validate the complete sanitized modality envelope BEFORE lazy graph
    /// materialization or graph creation (CX WB1-EG-01 CCN reduction). A
    /// malformed/forged replicated command must be a pure rejected log entry,
    /// never an opportunity to create a catalog row or touch a serving
    /// projection. This is the single replica-side gate for the versioned typed
    /// result codec, sealed runtime state, bounded lengths, and HMAC/digest
    /// bindings. Pure extract-method, byte-identical behaviour.
    #[cfg(feature = "modality-serving")]
    pub(super) fn validate_modality_command<'a>(
        req: &'a RaftRequest,
        server_secret: &str,
    ) -> Result<Option<&'a super::super::SanitizedModalityRaftCommand>, String> {
        match &req.command {
            ReplicatedMutation::Native {
                command: NativeMutationCommand::ServedModality { command },
            } => {
                command.validate_for_request(
                    server_secret,
                    &req.mutation.tenant_scope,
                    &req.graph_name,
                    &req.graph_fname,
                )?;
                Ok(Some(command.as_ref()))
            }
            _ => Ok(None),
        }
    }

    /// A replicated graph command must be a deterministic, non-work-item
    /// durable mutation (CX WB1-EG-01 CCN reduction). Pure extract-method,
    /// byte-identical behaviour.
    pub(super) fn require_durable_graph_method(
        graph_method: Option<&Method>,
    ) -> Result<(), String> {
        let Some(method) = graph_method else {
            return Ok(());
        };
        if !crate::mutation_apply::is_durable_mutation(method)
            || crate::server::mutation_batch::is_work_item_method(method)
        {
            return Err(
                "Raft graph command is not a deterministic replicated mutation".to_string(),
            );
        }
        Ok(())
    }

    pub(super) async fn resolve_replicated_graph(
        &self,
        req: &RaftRequest,
    ) -> Result<
        (
            Arc<crate::graph::GraphCore>,
            Option<Arc<dyn PersistenceBackend>>,
        ),
        String,
    > {
        #[cfg(feature = "redb")]
        {
            let miss = {
                let s = self.ctx.state.read().await;
                s.registry.get(&req.graph_name).is_none()
            };
            if miss {
                let cap = crate::server::persistence::cold_offload::max_resident_graphs();
                let page_size = crate::server::persistence::cold_offload::lazy_open_page_size();
                crate::server::persistence::cold_offload::lazy_open(
                    &self.ctx.state,
                    &req.graph_name,
                    cap,
                    page_size,
                )
                .await;
            }
        }
        // Steady-state fast path: the overwhelming majority of applies land on a
        // graph that already exists (created by an earlier entry, possibly in a
        // DIFFERENT Raft group — `ctx.state` is the ONE `ServerState` shared by
        // every group's `EgStore` on this node, see `MultiRaft::create_group`).
        // Resolving `core`/`persistence` under a WRITE lock unconditionally would
        // force every group's apply loop through one exclusive lock for every
        // entry, serializing N groups down to the throughput of one regardless of
        // how many independent redb shard writer threads back them. Take a READ
        // lock first (readers run concurrently across groups); only escalate to a
        // WRITE lock, with a re-check, when the graph is genuinely missing. This
        // is a pure lock-scope narrowing — `create_graph` still runs at most once
        // per graph, under exclusive access, identically to before.
        let fast = {
            let s = self.ctx.state.read().await;
            s.registry
                .get(&req.graph_name)
                .map(|e| (e.core.clone(), s.persistence.clone()))
        };
        match fast {
            Some(pair) => Ok(pair),
            None => {
                let mut s = self.ctx.state.write().await;
                if !s.registry.exists(&req.graph_name) {
                    s.registry
                        .create_graph(&req.graph_name, req.graph_type, None)
                        .map_err(|e| {
                            format!("graph '{}' create failed on replay: {e}", req.graph_name)
                        })?;
                }
                let core = match s.registry.get(&req.graph_name).map(|e| e.core.clone()) {
                    Some(c) => c,
                    None => {
                        return Err(format!("graph '{}' missing after create", req.graph_name));
                    }
                };
                Ok((core, s.persistence.clone()))
            }
        }
    }
}
