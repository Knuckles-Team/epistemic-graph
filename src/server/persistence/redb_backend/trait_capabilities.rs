macro_rules! persistence_capabilities {
    () => {
    fn supports_native_resource_reservations(&self) -> bool {
        true
    }

    fn supports_native_capacity_leases(&self) -> bool {
        true
    }

    fn supports_native_work_item_submission(&self) -> bool {
        true
    }

    fn supports_cluster_hierarchy_cache(&self) -> bool {
        true
    }

    async fn save_cluster_hierarchy(&self, graph_fname: &str, blob: Vec<u8>) -> Result<(), String> {
        self.cluster_hierarchy.put(graph_fname, blob)
    }

    async fn load_cluster_hierarchy(&self, graph_fname: &str) -> Result<Option<Vec<u8>>, String> {
        Ok(self.cluster_hierarchy.get(graph_fname))
    }

    async fn load_all(&self, state: &Arc<RwLock<ServerState>>) -> Result<usize, String> {
        let n = self.load_into(state).await?;
        tracing::info!(
            "redb: loaded {} graph(s) from {} shard(s) under the persist dir",
            n,
            self.shards.len()
        );
        Ok(n)
    }

    /// Populate the registry's CATALOG ONLY (CONCEPT:EG-KG.sharding.lazy-graph-catalog, DIST-P2-3) — every
    /// graph's `{name, graph_type}` identity row, with NO node/edge/ledger/semantic
    /// data read. Each graph's `GraphCore` then materializes lazily on first access
    /// (`server::persistence::cold_offload::lazy_open`), via
    /// `read_through::BackendGraphMaterializer` calling
    /// [`Self::read_graph_material_blocking`] below. Served startup selects this
    /// catalog-first path unconditionally.
    async fn load_catalog(&self, state: &Arc<RwLock<ServerState>>) -> Result<usize, String> {
        let n = self.load_catalog_into(state).await?;
        tracing::info!(
            "redb: catalog-loaded {} graph(s) from {} shard(s) — lazy startup, no node/edge \
             data read (CONCEPT:EG-KG.sharding.lazy-graph-catalog)",
            n,
            self.shards.len()
        );
        Ok(n)
    }

    /// SYNC durable-material fetch for a lazy first-open (CONCEPT:EG-KG.sharding.lazy-graph-catalog,
    /// DIST-P2-3) — reuses [`Self::read_graph_dump_blocking`], the SAME per-graph
    /// rehydrate path `shard_migrate`/`backup` already use, so a lazily-opened
    /// graph replays byte-identically to an eagerly-loaded one.
    };
}

pub(crate) use persistence_capabilities;
