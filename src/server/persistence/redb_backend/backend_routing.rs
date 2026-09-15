use super::*;

impl RedbBackend {
    /// Attach a tenant catalog to OVERRIDE graph→shard routing (CONCEPT:EG-KG.sharding.empty-catalog-routing, M3).
    /// Builder-style so the open path stays untouched; default (no call) = pure EG-026.
    pub fn with_catalog(
        mut self,
        catalog: Arc<crate::server::persistence::tenant_catalog::TenantCatalog>,
    ) -> Self {
        self.catalog = Some(catalog);
        self
    }

    /// The attached tenant catalog, if any (CONCEPT:EG-KG.sharding.r5-feature). `None` ⇒ pure EG-026. The
    /// admin/API surface uses this to populate/persist placements (`assign`/`reassign`/
    /// `remove`); a placement change that must also MOVE the graph's rows goes through
    /// [`Self::reshard_graph`] instead (which flips the route AND migrates the data).
    pub fn catalog(
        &self,
    ) -> Option<Arc<crate::server::persistence::tenant_catalog::TenantCatalog>> {
        self.catalog.clone()
    }

    /// One scoped read over the admin-mutations ledger — the capability every
    /// `eg_transaction::read_*` / `version` call over this store is a function of.
    pub(crate) fn admin_mutations_read(&self) -> Result<AdminScopedRead<'_>, String> {
        self.admin_mutations.read()
    }

    /// Prepare one admin saga, with its sealed private recovery payload when it has one:
    /// a durable `Prepared` receipt, no owner rows yet.
    pub(crate) fn admin_saga_step(
        &self,
        batch: &eg_types::MutationBatch,
        prepared_at_ms: u64,
        private_payload: Option<&[u8]>,
    ) -> Result<eg_transaction::SagaBegin, String> {
        let store = &self.admin_mutations;
        store
            .mutations()
            .saga_step(store.owner(), batch, prepared_at_ms, private_payload)
    }

    /// Terminalize one prepared admin saga. The `bool` is `true` when the saga had
    /// already committed and this call only replayed its receipt.
    pub(crate) fn admin_saga_end(
        &self,
        batch: &eg_types::MutationBatch,
        result_msgpack: Vec<u8>,
        committed_at_ms: u64,
    ) -> Result<(eg_types::MutationBatchRecord, bool), String> {
        let store = &self.admin_mutations;
        store
            .mutations()
            .saga_end(store.owner(), batch, result_msgpack, committed_at_ms)
    }

    /// The durable cluster-topology self-report store (CONCEPT:EG-KG.sharding.cluster-topology, ADR-1 / W1.1).
    /// Always present (see the field doc); empty on a single-node deployment that
    /// never ran a clustered startup.
    pub(crate) fn node_info(&self) -> Arc<super::super::node_info_store::NodeInfoStore> {
        self.node_info.clone()
    }

    /// Stable transaction-recovery-plan cipher handle resolved when the durable backend
    /// opened (D-ORC-50). Private transaction staging (the parent plan, cross-shard
    /// prepares, and coordinator recovery plans in `server::dispatch`) uses this exact
    /// cipher, not a second environment read, so every private-payload site in one
    /// process shares one configured recovery authority for its lifetime.
    ///
    /// DELIBERATELY NOT `self.shard0().cipher` (the data-at-rest cipher): that field
    /// controls the on-disk format of every ordinary node/edge/property value blob,
    /// including ones already durably written before any key existed. Returning it here
    /// would mean "configure durability for a multi-op transaction" and "expect every
    /// existing value in the store to already be sealed" are the same switch — which is
    /// exactly the destructive-read failure mode D-ORC-50 found (enabling the shared key
    /// on a populated plaintext store made every plaintext read fail with "encrypted
    /// durable value is missing sealed framing"). `shard0().txn_recovery_cipher` is
    /// resolved from its own env var (falling back to the shared key only when the
    /// dedicated one is absent), so it can be turned on independently.
    #[cfg(feature = "security")]
    pub(crate) fn transaction_recovery_cipher(&self) -> Option<crate::crypto::ValueCipher> {
        self.shard0().txn_recovery_cipher.clone()
    }

    /// Move ONE graph's rows from its current shard to `dst_shard` while the engine RUNS,
    /// then flip the catalog route (CONCEPT:EG-KG.backend.catalog-shard-resolve — the M3 keystone). Requires an attached
    /// tenant catalog (CONCEPT:EG-KG.sharding.r5-feature / R5). No data loss, single-writer-per-shard
    /// correctness, and audit-chain validity all hold across the move; other graphs are
    /// never touched. See [`super::super::online_reshard`] for the verbatim copy + crash-ordering.
    ///
    /// `graph_fname` must already be `sanitize`d (the durable key). `dst_shard` is clamped
    /// into `0..K`. A graph already on the target shard is a no-op.
    pub async fn reshard_graph(
        &self,
        graph_fname: &str,
        dst_shard: u32,
    ) -> Result<super::super::online_reshard::ReshardReport, String> {
        let catalog = self.catalog.clone().ok_or_else(|| {
            "online reshard requires an attached tenant catalog \
             (set EPISTEMIC_GRAPH_TENANT_CATALOG=1)"
                .to_string()
        })?;
        let k = self.shards.len().max(1);
        let dst_idx = (dst_shard as usize) % k;
        let src_idx = catalog.resolve_shard(graph_fname, k);
        if src_idx == dst_idx {
            return Ok(super::super::online_reshard::ReshardReport::no_op(
                graph_fname,
                src_idx,
            ));
        }
        let src_tx = self.shards[src_idx].tx.clone();
        let dst_tx = self.shards[dst_idx].tx.clone();
        let graph = graph_fname.to_string();

        // CONCEPT:EG-KG.backend.flush-pending-first (R1 delta-copy) — SNAPSHOT + DELTA to shrink the moved graph's
        // write-pause. PHASE 1 copies the BULK verbatim off a src read snapshot WITHOUT
        // the exclusive routing quiesce, so writes keep flowing to `src` while the (large)
        // copy runs — the graph is NOT paused. PHASE 2 takes the exclusive `routing_epoch`
        // WRITE guard (quiescing only THIS catalog's durable writes) and copies just the
        // small DELTA accumulated during phase 1, flips the route, and GCs the source. The
        // pause is therefore O(delta), not O(graph). Crash-consistency is preserved:
        // import(bulk) committed -> import(delta) committed -> catalog flip durable ->
        // purge(src) (a crash before the flip leaves the data on `src` where the route
        // still points; after the flip on `dst` where both bulk+delta already landed).
        let source_shard = self.shards[src_idx]
            .shard
            .upgrade()
            .ok_or_else(|| "redb source writer thread is gone".to_string())?;
        let destination_shard = self.shards[dst_idx]
            .shard
            .upgrade()
            .ok_or_else(|| "redb destination writer thread is gone".to_string())?;
        let s1 = src_tx.clone();
        let d1 = dst_tx.clone();
        let g1 = graph.clone();
        let bulk_source = Arc::clone(&source_shard);
        let bulk_destination = Arc::clone(&destination_shard);
        let bulk = tokio::task::spawn_blocking(move || {
            let endpoints = super::super::online_reshard::ReshardEndpoints {
                source: bulk_source.as_ref(),
                source_tx: &s1,
                source_index: src_idx,
                destination: bulk_destination.as_ref(),
                destination_tx: &d1,
                destination_index: dst_idx,
            };
            super::super::online_reshard::bulk_copy(&endpoints, &g1)
        })
        .await
        .map_err(|e| format!("reshard bulk join error: {e}"))??;

        // Exclusive routing quiesce held ONLY across the delta + flip (the small window):
        // no catalog-attached write can resolve/enqueue while the route flips, so the flip
        // never loses or misroutes a write; once released, every write resolves the catalog
        // AFTER the flip and follows the graph to `dst`.
        let quiesce = self.routing_epoch.clone().write_owned().await;
        tokio::task::spawn_blocking(move || {
            let _held = quiesce;
            let endpoints = super::super::online_reshard::ReshardEndpoints {
                source: source_shard.as_ref(),
                source_tx: &src_tx,
                source_index: src_idx,
                destination: destination_shard.as_ref(),
                destination_tx: &dst_tx,
                destination_index: dst_idx,
            };
            super::super::online_reshard::delta_flip_purge(
                &endpoints,
                catalog.as_ref(),
                &graph,
                bulk,
            )
        })
        .await
        .map_err(|e| format!("reshard delta join error: {e}"))?
    }

    /// Execute a rebalance PLAN move-by-move via online resharding (CONCEPT:EG-KG.backend.r3-plan-execution, R3
    /// plan execution). Each move is one [`Self::reshard_graph`] — online, ONE graph at a
    /// time, every other graph unaffected. The plan's `from_shard` is informational: each
    /// move resolves its source from the catalog's CURRENT state, so applying the moves in
    /// order is robust even as earlier moves shift placements. Returns the per-move reports.
    /// Requires an attached tenant catalog (every `reshard_graph` does).
    pub async fn rebalance_execute(
        &self,
        plan: &super::super::rebalance::RebalancePlan,
    ) -> Result<Vec<super::super::online_reshard::ReshardReport>, String> {
        let mut reports = Vec::with_capacity(plan.moves.len());
        for mv in &plan.moves {
            reports.push(self.reshard_graph(&mv.graph, mv.to_shard).await?);
        }
        Ok(reports)
    }
}
