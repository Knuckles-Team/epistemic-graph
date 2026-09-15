use super::*;

pub(super) struct MutationBatchCommitArgs<'a> {
    pub(super) req: &'a RaftRequest,
    pub(super) persistence: &'a Arc<dyn PersistenceBackend>,
    pub(super) durable_method: Method,
    pub(super) descriptor: crate::mutation_batch::MutationStateDescriptor,
    pub(super) batch_id: &'a str,
    pub(super) expected_principal: &'a str,
    pub(super) source_version: u64,
    pub(super) created_at_ms: u64,
    pub(super) state_msgpack: Vec<u8>,
    pub(super) result: Vec<u8>,
}

impl EgStore {
    /// Ordinary (non-native, non-ChangeEnvelope) replicated graph mutation:
    /// idempotent-replay check, then stage from the durable pre-image, apply,
    /// compile into a MutationBatch, and commit (CX WB1-EG-01 CCN reduction).
    /// Pure extract-method from `apply_request`'s tail, byte-identical
    /// behaviour, no signature change.
    /// The durable receipt method for this replicated command: the modality
    /// command's own receipt method when one is present (a modality-serving
    /// build), else the plain graph method (CX WB1-EG-01 CCN reduction). Pure
    /// extract-method, byte-identical behaviour.
    pub(super) fn compute_durable_method(
        graph_method: &Option<Method>,
        #[cfg(feature = "modality-serving")] modality_command: Option<
            &super::super::SanitizedModalityRaftCommand,
        >,
    ) -> Result<Method, String> {
        #[cfg(feature = "modality-serving")]
        {
            modality_command
                .map(super::super::SanitizedModalityRaftCommand::receipt_method)
                .or_else(|| graph_method.clone())
                .ok_or_else(|| "replicated native command has no receipt method".to_string())
        }
        #[cfg(not(feature = "modality-serving"))]
        graph_method
            .clone()
            .ok_or_else(|| "replicated native command has no receipt method".to_string())
    }

    /// Compile the durable method into a `MutationBatch`, validate it, and
    /// commit it through the universal MutationBatch kernel (CX WB1-EG-01 CCN
    /// reduction). Pure extract-method from `stage_and_commit_ordinary_mutation`,
    /// byte-identical behaviour, no signature change.
    pub(super) async fn compile_and_commit_mutation_batch(
        &self,
        args: MutationBatchCommitArgs<'_>,
    ) -> Result<crate::mutation_batch::MutationBatchCommit, String> {
        let MutationBatchCommitArgs {
            req,
            persistence,
            durable_method,
            descriptor,
            batch_id,
            expected_principal,
            source_version,
            created_at_ms,
            state_msgpack,
            result,
        } = args;
        let authority = &req.mutation;
        // Resolve BEFORE `durable_method` is moved into `compile_methods` below --
        // `compile_methods` erases it into an opaque state receipt (see
        // `redb_store::commit_mutation_batch_inner`'s doc comment), so the
        // policy-audited answer for the REAL replicated method must be captured
        // here or it becomes unrecoverable.
        let audited = eg_capabilities::policy(&durable_method).audited;
        let batch = crate::server::mutation_batch::compile_methods(
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
                created_at_ms,
                default_surface: crate::mutation_batch::MutationSurface::Graph,
                authoritative_state: Some(descriptor),
            },
            vec![durable_method],
        )?;
        batch.validate()?;
        persistence
            .commit_mutation_batch_state(
                &req.graph_fname,
                &batch,
                state_msgpack,
                Some(&result),
                created_at_ms,
                audited,
            )
            .await
    }

    pub(super) async fn stage_and_commit_ordinary_mutation(
        &self,
        req: &RaftRequest,
        core: &Arc<crate::graph::GraphCore>,
        persistence: &Arc<dyn PersistenceBackend>,
        graph_method: Option<Method>,
        #[cfg(feature = "modality-serving")] modality_command: Option<
            &super::super::SanitizedModalityRaftCommand,
        >,
    ) -> Result<RaftResponse, String> {
        let durable_method = Self::compute_durable_method(
            &graph_method,
            #[cfg(feature = "modality-serving")]
            modality_command,
        )?;

        if let Some(early) = self
            .try_apply_idempotent_replay(
                req,
                core,
                persistence,
                &durable_method,
                #[cfg(feature = "modality-serving")]
                modality_command,
            )
            .await?
        {
            return Ok(early);
        }

        let authority = &req.mutation;
        let batch_id = authority.batch_id.as_str();
        let expected_principal = authority.principal_fingerprint.as_str();

        let (staged_snapshot, source_version, graph_result) = self
            .stage_ordinary_mutation_snapshot(
                req,
                core,
                persistence,
                &graph_method,
                #[cfg(feature = "modality-serving")]
                modality_command,
            )
            .await?;
        let state_msgpack = staged_snapshot.to_msgpack()?;
        let result = match graph_result.as_ref() {
            Some(result) => rmp_serde::to_vec_named(result).map_err(|error| {
                format!("replicated graph result serialization failed: {error}")
            })?,
            None => Self::modality_or_default_bool_result(
                #[cfg(feature = "modality-serving")]
                modality_command,
            )?,
        };
        let descriptor = crate::mutation_batch::MutationStateDescriptor {
            algorithm: "sha256".to_string(),
            digest: hex::encode(Sha256::digest(&state_msgpack)),
            source_graph_version: source_version,
            target_graph_version: source_version.saturating_add(1),
        };
        let created_at_ms = authority.created_at_ms;
        let committed = self
            .compile_and_commit_mutation_batch(MutationBatchCommitArgs {
                req,
                persistence,
                durable_method: durable_method.clone(),
                descriptor,
                batch_id,
                expected_principal,
                source_version,
                created_at_ms,
                state_msgpack,
                result,
            })
            .await?;
        self.finalize_ordinary_commit(
            req,
            core,
            persistence,
            &committed,
            staged_snapshot,
            source_version,
        )
        .await?;
        #[cfg(all(feature = "modality-serving", feature = "streaming"))]
        self.emit_modality_cdc_if_needed(req, &committed, modality_command)
            .await;
        #[cfg(feature = "modality-serving")]
        let modality_replay = modality_command.is_some();
        #[cfg(not(feature = "modality-serving"))]
        let modality_replay = false;
        let native_result = Self::decode_replayed_graph_result(
            committed.record.result_msgpack.as_deref(),
            &durable_method,
            modality_replay,
        )?;
        Ok(RaftResponse {
            applied: true,
            native_result,
            native_commit: Some(committed),
            ..Default::default()
        })
    }

    /// Stage the ordinary mutation from the durable pre-image and apply it to
    /// the staged (never-yet-published) `GraphCore` (CX WB1-EG-01 CCN
    /// reduction). This handles the complete mutation vocabulary (including
    /// runtime-result/multi-row methods) without applying a speculative write
    /// to the serving projection. Pure extract-method, byte-identical
    /// behaviour; returns the staged snapshot + the source graph version
    /// instead of the whole `GraphCore` since nothing after the call site in
    /// the caller needs the live staged core, only its snapshot.
    pub(super) async fn stage_ordinary_mutation_snapshot(
        &self,
        req: &RaftRequest,
        core: &Arc<crate::graph::GraphCore>,
        persistence: &Arc<dyn PersistenceBackend>,
        graph_method: &Option<Method>,
        #[cfg(feature = "modality-serving")] modality_command: Option<
            &super::super::SanitizedModalityRaftCommand,
        >,
    ) -> Result<(crate::graph::GraphSnapshot, u64, Option<ResultPayload>), String> {
        let (base_snapshot, source_version) = self
            .resolve_mutation_base_snapshot(req, core, persistence)
            .await?;
        let staged = crate::graph::GraphCore::from_snapshot(base_snapshot, source_version)?;
        let result = Self::apply_staged_ordinary_mutation(
            &staged,
            graph_method,
            #[cfg(feature = "modality-serving")]
            modality_command,
        )?;
        Ok((staged.snapshot(), source_version, result))
    }

    /// Resolve the durable pre-image to stage the ordinary mutation from: the
    /// authoritative committed snapshot if one exists, else the resident core's
    /// own current snapshot paired with its durable mutation-graph version (CX
    /// WB1-EG-01 CCN reduction). Pure extract-method, byte-identical behaviour.
    pub(super) async fn resolve_mutation_base_snapshot(
        &self,
        req: &RaftRequest,
        core: &Arc<crate::graph::GraphCore>,
        persistence: &Arc<dyn PersistenceBackend>,
    ) -> Result<(crate::graph::GraphSnapshot, u64), String> {
        match persistence
            .read_authoritative_graph_snapshot(&req.graph_fname)
            .await?
        {
            Some(value) => Ok(value),
            None => {
                let version = persistence
                    .read_mutation_graph_version(&req.graph_fname)
                    .await?
                    .unwrap_or_else(|| core.version());
                Ok((core.snapshot(), version))
            }
        }
    }

    /// Apply the replicated mutation to the staged (not-yet-published)
    /// `GraphCore` (CX WB1-EG-01 CCN reduction): a modality-serving build routes
    /// a `ServedModality` command through `add_node` directly; every other
    /// build (and every non-modality command in a modality-serving build)
    /// re-runs the ordinary typed `Method` through `mutation_apply::apply`.
    /// Pure extract-method, byte-identical behaviour.
    pub(super) fn apply_staged_ordinary_mutation(
        staged: &crate::graph::GraphCore,
        graph_method: &Option<Method>,
        #[cfg(feature = "modality-serving")] modality_command: Option<
            &super::super::SanitizedModalityRaftCommand,
        >,
    ) -> Result<Option<ResultPayload>, String> {
        #[cfg(feature = "modality-serving")]
        if let Some(command) = modality_command {
            staged.add_node(
                command.node_id.clone(),
                command.sealed_runtime_state.clone(),
            );
            return Ok(None);
        }
        let method = graph_method
            .as_ref()
            .ok_or_else(|| "replicated graph mutation is missing its typed method".to_string())?;
        let result = match method {
            Method::CreateNodeIfAbsent {
                node_id,
                properties_msgpack,
            } => Some(ResultPayload::Bool(staged.create_node_if_absent(
                node_id.clone(),
                properties_msgpack.clone(),
            ))),
            Method::CompareAndSetNodeFields {
                node_id,
                conditions_msgpack,
                updates_msgpack,
            } => {
                let applied = match (
                    eg_types::msgpack::decode_property_object(conditions_msgpack),
                    eg_types::msgpack::decode_property_object(updates_msgpack),
                ) {
                    (Ok(conditions), Ok(updates)) => {
                        staged.compare_and_set_fields(node_id, &conditions, &updates)
                    }
                    _ => false,
                };
                Some(ResultPayload::Bool(applied))
            }
            _ => {
                crate::mutation_apply::apply(staged, method);
                None
            }
        };
        Ok(result)
    }

    /// `Some(command) => command.result_msgpack.clone()`, else the default
    /// encoded `ResultPayload::Bool(true)` — shared by
    /// `try_apply_idempotent_replay`'s `expected_result` and
    /// `stage_and_commit_ordinary_mutation`'s `result` (CX WB1-EG-01 CCN
    /// reduction; both computed the identical expression inline before this
    /// extraction). Pure extract-method, byte-identical behaviour.
    pub(super) fn modality_or_default_bool_result(
        #[cfg(feature = "modality-serving")] modality_command: Option<
            &super::super::SanitizedModalityRaftCommand,
        >,
    ) -> Result<Vec<u8>, String> {
        #[cfg(feature = "modality-serving")]
        {
            if let Some(command) = modality_command {
                return Ok(command.result_msgpack.clone());
            }
        }
        rmp_serde::to_vec_named(&crate::protocol::ResultPayload::Bool(true))
            .map_err(|error| error.to_string())
    }

    /// Install the committed replicated graph image (a phase-2-recovery replay)
    /// or publish the freshly-staged snapshot (CX WB1-EG-01 CCN reduction). Pure
    /// extract-method from `stage_and_commit_ordinary_mutation`'s tail,
    /// byte-identical behaviour.
    pub(super) async fn finalize_ordinary_commit(
        &self,
        req: &RaftRequest,
        core: &Arc<crate::graph::GraphCore>,
        persistence: &Arc<dyn PersistenceBackend>,
        committed: &crate::mutation_batch::MutationBatchCommit,
        staged_snapshot: crate::graph::GraphSnapshot,
        source_version: u64,
    ) -> Result<(), String> {
        if committed.replayed {
            self.install_authoritative_graph_snapshot(req, core, persistence)
                .await?;
        } else {
            core.prepare_snapshot_publish(staged_snapshot, source_version)?;
            core.mark_dirty();
        }
        Ok(())
    }

    /// Emit the served-modality CDC event for a freshly-committed (non-replayed)
    /// modality write (CX WB1-EG-01 CCN reduction). Pure extract-method from
    /// `stage_and_commit_ordinary_mutation`'s tail, byte-identical behaviour.
    #[cfg(all(feature = "modality-serving", feature = "streaming"))]
    pub(super) async fn emit_modality_cdc_if_needed(
        &self,
        req: &RaftRequest,
        committed: &crate::mutation_batch::MutationBatchCommit,
        modality_command: Option<&super::super::SanitizedModalityRaftCommand>,
    ) {
        if committed.replayed {
            return;
        }
        let Some(command) = modality_command else {
            return;
        };
        let hub = self.ctx.state.read().await.cdc.clone();
        if let Some(hub) = hub.as_ref() {
            crate::server::cdc::emit_served_modality(hub, &req.graph_name, command.modality);
        }
    }
}
