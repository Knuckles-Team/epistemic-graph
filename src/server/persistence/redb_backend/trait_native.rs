// These control reads intentionally use the shared shard directly. Keep their
// crypto selection and writer-liveness check in one place while preserving the
// synchronous read posture (the ordinary `read_snapshot` routes off-thread).
macro_rules! read_enrichment_shard {
    ($backend:expr, $graph:expr, |$shard:ident, $crypto:ident| $read:expr) => {{
        let writer = $backend.shard_for($graph);
        let $shard = writer
            .shard
            .upgrade()
            .ok_or_else(|| "redb writer thread is gone".to_string())?;
        #[cfg(feature = "security")]
        let $crypto = crate::redb_store::DurableCrypto::new(writer.cipher.as_ref());
        #[cfg(not(feature = "security"))]
        let $crypto = crate::redb_store::DurableCrypto::none();
        $read
    }};
}
pub(crate) use read_enrichment_shard;

macro_rules! persistence_native {
    ($next:ident { $($methods:tt)* }) => {
    $next! {
    $($methods)*
    async fn read_resource_reservation(
        &self,
        graph_fname: &str,
        request: &crate::epistemic_operations::ResourceReservationStatusRequest,
    ) -> Result<crate::epistemic_operations::ResourceReservationResult, String> {
        let graph_fname = graph_fname.to_owned();
        let routing_graph = graph_fname.clone();
        let request = request.clone();
        self.read_snapshot(&routing_graph, move |shard, crypto| {
            read_resource_reservation_record(shard, &graph_fname, &request, crypto)
        })
        .await
    }

    async fn read_resource_reservation_status(
        &self,
        graph_fname: &str,
        request: &crate::epistemic_operations::ResourceReservationStatusRequest,
    ) -> Result<crate::epistemic_operations::ResourceReservationStatusResult, String> {
        let graph_fname = graph_fname.to_owned();
        let routing_graph = graph_fname.clone();
        let request = request.clone();
        self.read_snapshot(&routing_graph, move |shard, crypto| {
            read_resource_reservation_status_record(shard, &graph_fname, &request, crypto)
        })
        .await
    }

    /// Execute the narrow native WorkItem claim-capability mint operation on
    /// the graph's writer shard.  This is crate-private: external callers can
    /// submit only the typed opaque request through dispatch after authz.
    async fn mint_work_item_claim_capability(
        &self,
        graph_fname: &str,
        request: crate::epistemic_operations_ext::WorkItemClaimCapabilityMintRequest,
        authority: crate::redb_store::work_item_capability::AuthenticatedAuthority,
    ) -> Result<crate::epistemic_operations_ext::WorkItemClaimCapabilityResult, String> {
        let (done, rx) = oneshot::channel();
        let cmd = Cmd::MintWorkItemClaimCapability {
            graph: graph_fname.to_string(),
            request,
            authority,
            done,
        };
        self.enqueue(graph_fname, cmd, "claim-capability mint").await?;
        rx.await
            .map_err(|_| "redb writer dropped claim-capability mint completion".to_string())?
    }

    /// Execute the linearizable native WorkItem claim-capability verification
    /// operation.  The writer command flushes earlier mutations before the
    /// control-row-first authorization/read sequence.
    async fn verify_work_item_claim_capability(
        &self,
        graph_fname: &str,
        request: crate::epistemic_operations_ext::WorkItemClaimCapabilityVerifyRequest,
        authority: crate::redb_store::work_item_capability::AuthenticatedAuthority,
    ) -> Result<crate::epistemic_operations_ext::WorkItemClaimCapabilityResult, String> {
        let (done, rx) = oneshot::channel();
        let cmd = Cmd::VerifyWorkItemClaimCapability {
            graph: graph_fname.to_string(),
            request,
            authority,
            done,
        };
        self.enqueue(graph_fname, cmd, "claim-capability verify").await?;
        rx.await
            .map_err(|_| "redb writer dropped claim-capability verify completion".to_string())?
    }

    /// Execute a native development-lane mutation (RMDD-28) on the graph's
    /// writer shard. `method` must be one of the six DevelopmentLane write
    /// variants; the kernel validates and rejects anything else.
    async fn commit_development_lane(
        &self,
        graph_fname: &str,
        method: Method,
        now_ms: u64,
    ) -> Result<Vec<u8>, String> {
        let (done, rx) = oneshot::channel();
        let cmd = Cmd::CommitDevelopmentLane {
            graph: graph_fname.to_string(),
            method: Box::new(method),
            now_ms,
            done,
        };
        self.enqueue(graph_fname, cmd, "development-lane commit").await?;
        rx.await
            .map_err(|_| "redb writer dropped development-lane commit completion".to_string())?
    }

    async fn commit_capacity_lease(
        &self,
        graph_fname: &str,
        method: Method,
    ) -> Result<Vec<u8>, String> {
        let (done, rx) = oneshot::channel();
        let cmd = Cmd::CommitCapacityLease {
            graph: graph_fname.to_string(),
            method: Box::new(method),
            done,
        };
        self.enqueue(graph_fname, cmd, "capacity lease commit").await?;
        rx.await
            .map_err(|_| "redb writer dropped capacity lease completion".to_string())?
    }

    async fn read_capacity_status(
        &self,
        graph_fname: &str,
        request: &eg_types::native_control::CapacityStatusRequest,
    ) -> Result<eg_types::native_control::CapacityStatusResult, String> {
        let graph = graph_fname.to_owned();
        let request = request.clone();
        self.read_snapshot(graph_fname, move |shard, crypto| {
            crate::redb_store::capacity_lease::read(shard, &graph, &request, crypto)
        })
        .await
    }

    /// Per-cell headroom at one priority (ST-8), same MVCC posture as
    /// `read_capacity_status` above.
    async fn read_capacity_headroom(
        &self,
        graph_fname: &str,
        cells: &[String],
        priority: eg_types::capacity_lease::LeasePriority,
    ) -> Result<Vec<eg_types::decision::CapacityHeadroom>, String> {
        let graph = graph_fname.to_owned();
        let cells = cells.to_vec();
        self.read_snapshot(graph_fname, move |shard, crypto| {
            crate::redb_store::capacity_lease::headroom(shard, &graph, &cells, priority, crypto)
        })
        .await
    }

    async fn read_enrichment_budget_checkpoint(
        &self,
        graph_fname: &str,
        source_envelope: &str,
    ) -> Result<Option<eg_types::native_control::EnrichmentBudgetCheckpoint>, String> {
        super::trait_native::read_enrichment_shard!(self, graph_fname, |shard, crypto| {
            crate::redb_store::enrichment_budget::read(&shard, graph_fname, source_envelope, crypto)
        })
    }

    async fn read_enrichment_policy_revision(
        &self,
        graph_fname: &str,
        source_envelope: &str,
    ) -> Result<Option<crate::redb_store::enrichment_budget::RepositoryEnrichmentPolicyRevision>, String> {
        super::trait_native::read_enrichment_shard!(self, graph_fname, |shard, crypto| {
            crate::redb_store::enrichment_budget::read_policy_revision(
                &shard,
                graph_fname,
                source_envelope,
                crypto,
            )
        })
    }

    async fn read_enrichment_budget_park(
        &self,
        graph_fname: &str,
    ) -> Result<Option<eg_types::native_control::EnrichmentBudgetPark>, String> {
        super::trait_native::read_enrichment_shard!(self, graph_fname, |shard, crypto| {
            crate::redb_store::enrichment_budget::read_park(&shard, graph_fname, crypto)
        })
    }

    async fn read_enrichment_supersession(
        &self,
        graph_fname: &str,
        source_envelope: &str,
        snapshot_digest: &str,
    ) -> Result<bool, String> {
        super::trait_native::read_enrichment_shard!(self, graph_fname, |shard, crypto| {
            crate::redb_store::enrichment_budget::is_superseded(
                &shard,
                graph_fname,
                source_envelope,
                snapshot_digest,
                crypto,
            )
        })
    }

    async fn park_enrichment_budget(
        &self,
        graph_fname: &str,
        park: eg_types::native_control::EnrichmentBudgetPark,
    ) -> Result<(), String> {
        let (done, rx) = oneshot::channel();
        let cmd = Cmd::ParkEnrichmentBudget {
            graph: graph_fname.to_string(),
            park: Box::new(park),
            done,
        };
        self.enqueue(graph_fname, cmd, "park_enrichment_budget")
            .await?;
        rx.await
            .map_err(|_| "redb writer dropped enrichment budget park completion".to_string())?
    }

    /// Exact authenticated native development-lane hold/tombstone read (RMDD-28).
    /// An MVCC snapshot read off the writer shard's shared `Shard`, same
    /// posture as `read_resource_reservation` above -- never routed through the
    /// writer thread channel.
    async fn read_development_lane(
        &self,
        graph_fname: &str,
        request: &crate::epistemic_operations::DevelopmentLaneQueryRequest,
        now_ms: u64,
    ) -> Result<crate::epistemic_operations::DevelopmentLaneQueryResult, String> {
        let graph = graph_fname.to_owned();
        let request = request.clone();
        self.read_snapshot(graph_fname, move |shard, crypto| {
            crate::redb_store::development_lane::read_development_lane(
                shard, &graph, &request, now_ms, crypto,
            )
        })
        .await
    }

    /// Bounded native development-lane tenant status page (RMDD-28). An MVCC
    /// snapshot read, same posture as `read_resource_reservation_status` above.
    async fn read_development_lane_status(
        &self,
        graph_fname: &str,
        request: &crate::epistemic_operations::DevelopmentLaneStatusRequest,
        now_ms: u64,
    ) -> Result<crate::epistemic_operations::DevelopmentLaneStatusResult, String> {
        let graph = graph_fname.to_owned();
        let request = request.clone();
        self.read_snapshot(graph_fname, move |shard, crypto| {
            crate::redb_store::development_lane::read_development_lane_status(
                shard, &graph, &request, now_ms, crypto,
            )
        })
        .await
    }

    /// **Cross-modal ACID (CONCEPT:EG-KG.txn.reader-never-sees-node).** Land graph + vectors + blob-refs for ONE
    /// graph in ONE redb `WriteTransaction`, awaiting its durable fsync. On any error
    /// the transaction is dropped without commit, so NONE of the modalities land — a
    /// true rollback (no partial cross-modal commit). Routed through the owner thread
    /// (exclusive file lock) via a blocking send off the reactor.
    }
    };
}

pub(crate) use persistence_native;
