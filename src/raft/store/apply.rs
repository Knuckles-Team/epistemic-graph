use super::*;

impl EgStore {
    /// Apply ONE committed request to the engine. Every ordinary method is staged
    /// against the authoritative image and committed as one state-backed
    /// MutationBatch before RAM publication. A ChangeEnvelope invokes its richer
    /// native atomic kernel as one state-machine command.
    ///
    /// Explicitly boxed (rather than a bare `async fn`) so its return type is a
    /// concrete `Pin<Box<dyn Future>>` instead of an inferred opaque type: this
    /// function is the shared, ~500-line body BOTH `apply` and `install_snapshot`
    /// (the two huge `RaftStateMachine` trait-method coroutines above) call, and an
    /// opaque return type here entangles their two independent `Send`-bound
    /// computations into a single rustc query cycle (`error[E0391]: cycle detected
    /// ... coroutine witness ... Send`) once enough unrelated type-checking work
    /// exists elsewhere in the crate to shift query evaluation order — the standard,
    /// behavior-preserving fix for this class of async-fn-shared-by-two-trait-impls
    /// cycle is to make the shared callee's future type explicit.
    pub(super) fn apply_request<'a>(
        &'a self,
        req: &'a RaftRequest,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<RaftResponse, String>> + Send + 'a>,
    > {
        Box::pin(async move {
            req.validate()?;
            let server_secret = self.ctx.state.read().await.auth_secret.clone();

            // Phase 1: every bounded native family executes only after its command is
            // committed. Re-entering the domain dispatcher under the replicated-apply
            // scope reuses the existing MutationBatch/saga kernels while suppressing any
            // nested Raft proposal and replacing local wall-clock reads with the
            // leader-selected commit time. The reconstructed authority contains only
            // opaque scopes.
            if let Some(early) = self.try_apply_native_command(req, &server_secret).await? {
                return Ok(early);
            }
            if crate::server::txn::consensus_graph_is_prepared(&req.graph_name) {
                return Ok(RaftResponse {
                    applied: true,
                    native_error: Some(
                        "graph is reserved by a prepared consensus transaction".to_string(),
                    ),
                    ..Default::default()
                });
            }

            // Phase 2: an ordinary (non-native) replicated graph mutation --
            // ChangeEnvelope or a plain graph method.
            self.apply_ordinary_graph_mutation(req, &server_secret)
                .await
        })
    }

    /// Phase 2 of `apply_request` (CX WB1-EG-01 CCN reduction): resolve the
    /// target graph, validate the graph method is a deterministic durable
    /// mutation, then try the `ChangeEnvelope` fast path before falling through
    /// to the ordinary stage-and-commit pipeline. Pure extract-method from
    /// `apply_request`'s tail, byte-identical behaviour, no signature change.
    pub(super) async fn apply_ordinary_graph_mutation(
        &self,
        req: &RaftRequest,
        server_secret: &str,
    ) -> Result<RaftResponse, String> {
        // Validate the complete sanitized modality envelope BEFORE lazy graph
        // materialization or graph creation. A malformed/forged replicated
        // command must be a pure rejected log entry, never an opportunity to
        // create a catalog row or touch a serving projection. This is the
        // single replica-side gate for the versioned typed result codec,
        // sealed runtime state, bounded lengths, and HMAC/digest bindings.
        #[cfg(feature = "modality-serving")]
        let modality_command = Self::validate_modality_command(req, server_secret)?;

        // Resolve the target graph's RESIDENT core. Mirrors the live dispatch
        // cold-path: a follower replaying a CreateGraph->write sequence may find the
        // graph catalog-known but not yet materialized (evicted mid-replay, or
        // catalog-only after a restart) -- `exists()` true but `get()` None. Lazy-open
        // it FIRST (a no-op for a genuinely-unknown name, so a follower's first sight
        // of a brand-new graph still falls through to create), then create only if
        // genuinely absent. Fixes the follower catch-up "missing after create" apply
        // failure that stalls a lagging node from ever finishing catch-up.
        let (core, persistence) = self.resolve_replicated_graph(req).await?;

        let graph_method = req.command.open_graph(server_secret)?;
        Self::require_durable_graph_method(graph_method.as_ref())?;
        let change_envelope = req.command.open_change_envelope(server_secret)?;

        // ChangeEnvelope is one atomic state-machine command. Decomposing it into
        // graph operations would lose its content-version, cursor, governance,
        // lineage, evidence, and outbox authority.
        if let Some(early) = self
            .try_apply_change_envelope(req, &core, persistence.as_ref(), change_envelope.as_ref())
            .await?
        {
            return Ok(early);
        }

        let persistence = persistence.ok_or_else(|| {
            "ordinary replicated mutation requires a configured persistence backend".to_string()
        })?;

        self.stage_and_commit_ordinary_mutation(
            req,
            &core,
            &persistence,
            graph_method,
            #[cfg(feature = "modality-serving")]
            modality_command,
        )
        .await
    }
    /// `ChangeEnvelope` fast path of `apply_request` (CX WB1-EG-01 CCN
    /// reduction). `Ok(Some(response))` / `Ok(None)` follow the same
    /// early-exit-as-value convention as `try_apply_native_command`. Pure
    /// extract-method, byte-identical behaviour: ChangeEnvelope is one atomic
    /// state-machine command, decomposing it into graph operations would lose
    /// its content-version, cursor, governance, lineage, evidence, and outbox
    /// authority.
    pub(super) async fn try_apply_change_envelope(
        &self,
        req: &RaftRequest,
        core: &Arc<crate::graph::GraphCore>,
        persistence: Option<&Arc<dyn PersistenceBackend>>,
        change_envelope: Option<&crate::change_envelope::ChangeEnvelope>,
    ) -> Result<Option<RaftResponse>, String> {
        let Some(envelope) = change_envelope else {
            return Ok(None);
        };
        // `identity.scope().graph_name()` is `None` for a native (non-graph)
        // scope; fail closed instead of letting a native-scope envelope
        // silently compare equal to a graph name (no sentinel/empty-string
        // substitution — see MIGRATION-CONTRACT.md).
        let envelope_graph_matches = match envelope.mutation.identity.scope().graph_name() {
            Some(graph_name) => graph_name.as_str() == req.graph_name.as_str(),
            None => false,
        };
        if !envelope_graph_matches {
            return Err(
                "replicated ChangeEnvelope graph does not match request authority".to_string(),
            );
        }
        let expected_tenant_scope = crate::server::mutation_batch::opaque_coordinator_key(
            "carrier-tenant",
            "verified",
            envelope.mutation.identity.tenant().as_str(),
        );
        if Self::change_envelope_mismatches_authority(req, envelope, &expected_tenant_scope) {
            return Err(
                "replicated ChangeEnvelope does not match its mutation authority".to_string(),
            );
        }
        let committed_at_ms = req.committed_at_ms;
        let backend = persistence.ok_or_else(|| {
            "replicated ChangeEnvelope requires a configured persistence backend".to_string()
        })?;
        let committed = backend
            .commit_change_envelope(&req.graph_fname, envelope, committed_at_ms)
            .await?;
        let projection_pending = if committed.replayed {
            false
        } else {
            match crate::server::mutation_batch::publish_change_envelope_projection(core, envelope)
            {
                Ok(()) => false,
                Err(error) => {
                    tracing::warn!(
                        graph = %req.graph_fname,
                        error = %error,
                        "replicated ChangeEnvelope projection queued for repair"
                    );
                    true
                }
            }
        };
        Ok(Some(RaftResponse {
            applied: true,
            change_envelope_commit: Some(committed),
            projection_pending,
            ..Default::default()
        }))
    }
}
