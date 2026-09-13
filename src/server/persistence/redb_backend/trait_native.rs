macro_rules! persistence_native {
    () => {
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
        let send = if self.catalog.is_some() {
            let guard = self.routing_epoch.clone().read_owned().await;
            let tx = self.shard_for(graph_fname).tx.clone();
            tokio::task::spawn_blocking(move || {
                let _routing = guard;
                tx.send(cmd).map_err(|_| ())
            })
            .await
        } else {
            let tx = self.shard_for(graph_fname).tx.clone();
            tokio::task::spawn_blocking(move || tx.send(cmd).map_err(|_| ())).await
        };
        send.map_err(|error| format!("claim-capability mint join error: {error}"))?
            .map_err(|_| "redb writer thread is gone".to_string())?;
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
        let send = if self.catalog.is_some() {
            let guard = self.routing_epoch.clone().read_owned().await;
            let tx = self.shard_for(graph_fname).tx.clone();
            tokio::task::spawn_blocking(move || {
                let _routing = guard;
                tx.send(cmd).map_err(|_| ())
            })
            .await
        } else {
            let tx = self.shard_for(graph_fname).tx.clone();
            tokio::task::spawn_blocking(move || tx.send(cmd).map_err(|_| ())).await
        };
        send.map_err(|error| format!("claim-capability verify join error: {error}"))?
            .map_err(|_| "redb writer thread is gone".to_string())?;
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
        let send = if self.catalog.is_some() {
            let guard = self.routing_epoch.clone().read_owned().await;
            let tx = self.shard_for(graph_fname).tx.clone();
            tokio::task::spawn_blocking(move || {
                let _routing = guard;
                tx.send(cmd).map_err(|_| ())
            })
            .await
        } else {
            let tx = self.shard_for(graph_fname).tx.clone();
            tokio::task::spawn_blocking(move || tx.send(cmd).map_err(|_| ())).await
        };
        send.map_err(|error| format!("development-lane commit join error: {error}"))?
            .map_err(|_| "redb writer thread is gone".to_string())?;
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
        let send = if self.catalog.is_some() {
            let guard = self.routing_epoch.clone().read_owned().await;
            let tx = self.shard_for(graph_fname).tx.clone();
            tokio::task::spawn_blocking(move || {
                let _routing = guard;
                tx.send(cmd).map_err(|_| ())
            })
            .await
        } else {
            let tx = self.shard_for(graph_fname).tx.clone();
            tokio::task::spawn_blocking(move || tx.send(cmd).map_err(|_| ())).await
        };
        send.map_err(|error| format!("capacity lease commit join error: {error}"))?
            .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.await
            .map_err(|_| "redb writer dropped capacity lease completion".to_string())?
    }

    async fn read_capacity_status(
        &self,
        graph_fname: &str,
        request: &eg_types::native_control::CapacityStatusRequest,
    ) -> Result<eg_types::native_control::CapacityStatusResult, String> {
        let writer = self.shard_for(graph_fname);
        let shard = writer
            .shard
            .upgrade()
            .ok_or_else(|| "redb writer thread is gone".to_string())?;
        #[cfg(feature = "security")]
        let crypto = crate::redb_store::DurableCrypto::new(writer.cipher.as_ref());
        #[cfg(not(feature = "security"))]
        let crypto = crate::redb_store::DurableCrypto::none();
        crate::redb_store::capacity_lease::read(&shard, graph_fname, request, crypto)
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
        let writer = self.shard_for(graph_fname);
        let shard = writer
            .shard
            .upgrade()
            .ok_or_else(|| "redb writer thread is gone".to_string())?;
        #[cfg(feature = "security")]
        let crypto = crate::redb_store::DurableCrypto::new(writer.cipher.as_ref());
        #[cfg(not(feature = "security"))]
        let crypto = crate::redb_store::DurableCrypto::none();
        crate::redb_store::development_lane::read_development_lane(
            &shard,
            graph_fname,
            request,
            now_ms,
            crypto,
        )
    }

    /// Bounded native development-lane tenant status page (RMDD-28). An MVCC
    /// snapshot read, same posture as `read_resource_reservation_status` above.
    async fn read_development_lane_status(
        &self,
        graph_fname: &str,
        request: &crate::epistemic_operations::DevelopmentLaneStatusRequest,
        now_ms: u64,
    ) -> Result<crate::epistemic_operations::DevelopmentLaneStatusResult, String> {
        let writer = self.shard_for(graph_fname);
        let shard = writer
            .shard
            .upgrade()
            .ok_or_else(|| "redb writer thread is gone".to_string())?;
        #[cfg(feature = "security")]
        let crypto = crate::redb_store::DurableCrypto::new(writer.cipher.as_ref());
        #[cfg(not(feature = "security"))]
        let crypto = crate::redb_store::DurableCrypto::none();
        crate::redb_store::development_lane::read_development_lane_status(
            &shard,
            graph_fname,
            request,
            now_ms,
            crypto,
        )
    }

    /// **Cross-modal ACID (CONCEPT:EG-KG.txn.reader-never-sees-node).** Land graph + vectors + blob-refs for ONE
    /// graph in ONE redb `WriteTransaction`, awaiting its durable fsync. On any error
    /// the transaction is dropped without commit, so NONE of the modalities land — a
    /// true rollback (no partial cross-modal commit). Routed through the owner thread
    /// (exclusive file lock) via a blocking send off the reactor.
    };
}

pub(crate) use persistence_native;
