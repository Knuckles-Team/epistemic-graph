use super::*;
use crate::epistemic_operations::{
    DevelopmentLaneCleanupIntent, DevelopmentLaneCleanupIntentSchemaVersion,
};

pub(super) struct NativeLaneFixture {
    path: PathBuf,
    pub(super) shard: Shard,
    pub(super) reserve: DevelopmentLaneReserveRequest,
    remove_file_on_drop: bool,
}

impl Drop for NativeLaneFixture {
    fn drop(&mut self) {
        if self.remove_file_on_drop {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

impl NativeLaneFixture {
    pub(super) fn new(policy: DevelopmentLaneQuotaPolicy) -> Self {
        let path = test_path("fixture");
        // `Shard::open` materializes the whole declared shard census, so
        // the hand-written table bootstrap the raw path needed is gone.
        let shard = Shard::open(&path).expect("open lane shard");
        let reserve = test_reserve_request(test_intent(
            "tenant:a",
            "initial",
            "branch:initial",
            "lanes/initial",
        ));
        seed_lane_work_item(&shard, &reserve).expect("seed linked resource reservation");
        let fixture = Self {
            path,
            shard,
            reserve,
            remove_file_on_drop: true,
        };
        let result: DevelopmentLaneQuotaUpdateResult = fixture.decode(
            &fixture.commit(
                Method::UpdateDevelopmentLaneQuota {
                    request: DevelopmentLaneQuotaUpdateRequest {
                        schema_version:
                            crate::epistemic_operations::DevelopmentLaneQuotaUpdateRequestSchemaVersion::V1,
                        tenant_ref: "tenant:a".into(),
                        policy,
                        expected_policy_revision: 0,
                        expected_policy_version: None,
                        idempotency_key: "policy:initial".into(),
                        now_ms: TEST_NOW,
                    },
                },
                TEST_NOW,
            ),
        );
        assert_eq!(
            result.decision,
            crate::epistemic_operations::DevelopmentLaneQuotaUpdateResultDecision::Accepted
        );
        fixture
    }

    pub(super) fn close_for_reopen(mut self) -> PathBuf {
        self.remove_file_on_drop = false;
        let path = self.path.clone();
        drop(self);
        path
    }

    pub(super) fn commit(&self, method: Method, now_ms: u64) -> Vec<u8> {
        commit_development_lane(
            &self.shard,
            TEST_GRAPH,
            &method,
            now_ms,
            DurableCrypto::none(),
        )
        .expect("lane transaction")
    }

    /// One kernel read bounded to the fixture's graph.
    ///
    /// A closure rather than a returned `ScopedRead` because the read
    /// borrows the bound handle this call owns.
    pub(super) fn with_read<R>(
        &self,
        read: impl FnOnce(&ScopedRead<'_, GraphShardOwner>) -> R,
    ) -> R {
        let handle = self.shard.graph(TEST_GRAPH).expect("bind lane graph scope");
        let scoped = self.shard.read(&handle).expect("lane scoped read");
        read(&scoped)
    }

    /// One admitted maintenance write over the fixture's graph.
    pub(super) fn with_write<R>(
        &self,
        tag: &str,
        write: impl FnOnce(&ShardWrite<'_>) -> Result<R, String>,
    ) -> Result<R, String> {
        shard_write(&self.shard, tag, write)
    }

    pub(super) fn decode<T: serde::de::DeserializeOwned>(&self, bytes: &[u8]) -> T {
        rmp_serde::from_slice(bytes).expect("typed lane result")
    }

    pub(super) fn commit_work_item(
        &self,
        method: Method,
        batch_suffix: &str,
        committed_at_ms: u64,
    ) -> Result<MutationBatchCommit, String> {
        self.commit_work_item_with_crash(method, batch_suffix, committed_at_ms, None)
    }

    pub(super) fn commit_work_item_with_crash(
        &self,
        method: Method,
        batch_suffix: &str,
        committed_at_ms: u64,
        crashpoint: Option<super::super::super::MutationBatchCrashpoint>,
    ) -> Result<MutationBatchCommit, String> {
        // Graph-scoped: this batch is committed via `commit_mutation_batch_inner`
        // below, which every path in this module routes by `graph_fname` and
        // requires `MutationScope::Graph` for (`mutation_batch_graph_name` fails
        // closed on a native scope) -- `DurabilityDomain::ControlPlane` here is
        // just the operation's own domain tag (WorkItem rows physically live in
        // the same graph redb file), matching `compute_native_terminal_work_item_cas`
        // in the parent module. v1's `VersionExpectation` has no "unversioned" arm
        // for an ordinary tenant, so unlike v2 this can no longer pass `None` for
        // "don't care": read the real current version instead (this fixture is the
        // sole writer at this point, so it observes the exact same state a `None`
        // OCC skip effectively would have).
        let batch_id = format!("native-work-item:{batch_suffix}");
        // A REPLAY must recompile to the byte-identical batch that was
        // stored, because `verify_replay_identity` compares the whole
        // struct. Re-reading the live version here would observe the value
        // the FIRST attempt already advanced, so the recompiled batch would
        // differ on request metadata alone and the replay would fail closed
        // with IDEMPOTENCY_CONFLICT instead of replaying. Reuse the stored
        // expectation when a record for this batch already exists; only a
        // genuinely first attempt reads live state.
        //
        // This mirrors the production fix in `handlers/admin.rs`. That the
        // same correction is needed independently here is the evidence that
        // the underlying validator is comparing more than identity.
        let expected_graph_version = match super::super::super::read_mutation_batch_for_graph(
            &self.shard,
            TEST_GRAPH,
            &batch_id,
        )? {
            Some(stored) => match stored.batch.version_expectation {
                VersionExpectation::Graph(version) => version,
                other => {
                    return Err(format!(
                        "stored development-lane batch has a non-graph expectation: {other:?}"
                    ))
                }
            },
            None => super::super::super::read_mutation_graph_version(&self.shard, TEST_GRAPH)?,
        };
        let identity = MutationScopeIdentity::graph(
            ScopeTenantId::new(self.reserve.tenant_ref.clone())?,
            LogicalName::new(TEST_GRAPH)?,
            IncarnationId::new("incarnation:test:development-lane")?,
        );
        let mut batch = MutationBatch {
            schema_version: MUTATION_BATCH_VERSION,
            batch_id: batch_id.clone(),
            envelope: super::super::super::fixture_operation_envelope(
                &identity,
                &format!("principal:sha256:{}", "a".repeat(64)),
                committed_at_ms,
                &format!("native-work-item-idem:{batch_suffix}"),
            ),
            identity,
            placement_epoch: 0,
            version_expectation: VersionExpectation::Graph(expected_graph_version),
            fencing_token: None,
            authoritative_state: None,
            operations: vec![MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Job,
                domain: DurabilityDomain::ControlPlane,
                method,
            }],
            outbox: vec![MutationOutboxIntent {
                topic: "native-lane.test".into(),
                key: format!("native-work-item:{batch_suffix}"),
                payload: Vec::new(),
                headers: Default::default(),
            }],
            created_at_ms: committed_at_ms,
        };
        batch
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("a fixture batch reseals its envelope over its final body");
        #[cfg(feature = "security")]
        let mut audit = super::super::super::AuditTailCache::new();
        super::super::super::commit_mutation_batch_inner(
            &self.shard,
            super::super::super::BatchCommitInput {
                graph_fname: TEST_GRAPH,
                batch: &batch,
                change: None,
                authoritative_state_msgpack: None,
                crossmodal: None,
                result_msgpack: None,
                committed_at_ms,
                audited: true,
                crashpoint,
            },
            DurableCrypto::none(),
            #[cfg(feature = "security")]
            &mut audit,
        )
    }

    pub(super) fn compare_and_set_work_item_owner(
        &self,
        expected_owner: &str,
        updated_owner: &str,
        batch_suffix: &str,
        committed_at_ms: u64,
    ) -> Result<MutationBatchCommit, String> {
        let expected_graph_version =
            super::super::super::read_mutation_graph_version(&self.shard, TEST_GRAPH)?;
        let conditions_msgpack =
            rmp_serde::to_vec_named(&serde_json::json!({"lease_owner": expected_owner}))
                .map_err(|error| error.to_string())?;
        let updates_msgpack =
            rmp_serde::to_vec_named(&serde_json::json!({"lease_owner": updated_owner}))
                .map_err(|error| error.to_string())?;
        let identity = MutationScopeIdentity::graph(
            ScopeTenantId::new(self.reserve.tenant_ref.clone())?,
            LogicalName::new(TEST_GRAPH)?,
            IncarnationId::new("incarnation:test:development-lane")?,
        );
        let mut batch = MutationBatch {
            schema_version: MUTATION_BATCH_VERSION,
            batch_id: format!("native-owner-cas:{batch_suffix}"),
            envelope: super::super::super::fixture_operation_envelope(
                &identity,
                &format!("principal:sha256:{}", "a".repeat(64)),
                committed_at_ms,
                &format!("native-owner-cas-idem:{batch_suffix}"),
            ),
            identity,
            placement_epoch: 0,
            version_expectation: VersionExpectation::Graph(expected_graph_version),
            fencing_token: None,
            authoritative_state: None,
            operations: vec![MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Graph,
                domain: DurabilityDomain::GraphRows,
                method: Method::CompareAndSetNodeFields {
                    node_id: self.reserve.work_item_id.clone(),
                    conditions_msgpack,
                    updates_msgpack,
                },
            }],
            outbox: vec![MutationOutboxIntent {
                topic: "native-lane.test".into(),
                key: format!("native-owner-cas:{batch_suffix}"),
                payload: Vec::new(),
                headers: Default::default(),
            }],
            created_at_ms: committed_at_ms,
        };
        batch
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("a fixture batch reseals its envelope over its final body");
        #[cfg(feature = "security")]
        let mut audit = super::super::super::AuditTailCache::new();
        super::super::super::commit_mutation_batch_inner(
            &self.shard,
            super::super::super::BatchCommitInput {
                graph_fname: TEST_GRAPH,
                batch: &batch,
                change: None,
                authoritative_state_msgpack: None,
                crossmodal: None,
                result_msgpack: None,
                committed_at_ms,
                audited: true,
                crashpoint: None,
            },
            DurableCrypto::none(),
            #[cfg(feature = "security")]
            &mut audit,
        )
    }

    pub(super) fn commit_work_item_result(
        &self,
        outcome: &str,
        retryable: bool,
        batch_suffix: &str,
        committed_at_ms: u64,
    ) -> Result<MutationBatchCommit, String> {
        self.commit_work_item_result_with_tuple(
            self.reserve.lease_epoch,
            self.reserve.fencing_token,
            outcome,
            retryable,
            batch_suffix,
            committed_at_ms,
        )
    }

    pub(super) fn commit_work_item_result_with_tuple(
        &self,
        lease_epoch: u64,
        fencing_token: u64,
        outcome: &str,
        retryable: bool,
        batch_suffix: &str,
        committed_at_ms: u64,
    ) -> Result<MutationBatchCommit, String> {
        self.commit_work_item(
            Method::CommitWorkItemResult {
                tenant: self.reserve.tenant_ref.clone(),
                work_item_id: self.reserve.work_item_id.clone(),
                worker_id: self.reserve.owner_id.clone(),
                lease_epoch,
                fencing_token,
                idempotency_key: format!("work-item:{batch_suffix}"),
                outcome: outcome.into(),
                result_ref: None,
                outcome_extension: None,
                error_ref: None,
                retryable,
                now_ms: committed_at_ms,
            },
            batch_suffix,
            committed_at_ms,
        )
    }

    pub(super) fn cancel_work_item(
        &self,
        batch_suffix: &str,
        committed_at_ms: u64,
    ) -> Result<MutationBatchCommit, String> {
        self.cancel_work_item_with_crash(batch_suffix, committed_at_ms, None)
    }

    pub(super) fn cancel_work_item_with_crash(
        &self,
        batch_suffix: &str,
        committed_at_ms: u64,
        crashpoint: Option<super::super::super::MutationBatchCrashpoint>,
    ) -> Result<MutationBatchCommit, String> {
        self.commit_work_item_with_crash(
            Method::CancelWorkItem {
                tenant: self.reserve.tenant_ref.clone(),
                work_item_id: self.reserve.work_item_id.clone(),
                idempotency_key: format!("cancel-item:{batch_suffix}"),
                reason_ref: Some("reason:test".into()),
                now_ms: committed_at_ms,
            },
            batch_suffix,
            committed_at_ms,
            crashpoint,
        )
    }

    pub(super) fn reserve_method(&self, key: &str) -> Method {
        let mut request = self.reserve.clone();
        request.idempotency_key = key.into();
        Method::ReserveDevelopmentLane { request }
    }

    pub(super) fn candidate(
        &self,
        suffix: &str,
        branch: &str,
        worktree_locator: &str,
    ) -> DevelopmentLaneReserveRequest {
        self.candidate_for_tenant("tenant:a", suffix, branch, worktree_locator)
    }

    pub(super) fn candidate_for_tenant(
        &self,
        tenant: &str,
        suffix: &str,
        branch: &str,
        worktree_locator: &str,
    ) -> DevelopmentLaneReserveRequest {
        let request = test_reserve_request(test_intent(tenant, suffix, branch, worktree_locator));
        seed_lane_work_item(&self.shard, &request).expect("seed lane candidate");
        request
    }

    pub(super) fn update_policy(
        &self,
        tenant: &str,
        policy: DevelopmentLaneQuotaPolicy,
        expected_policy_revision: u64,
        idempotency_key: &str,
        now_ms: u64,
    ) -> DevelopmentLaneQuotaUpdateResult {
        self.decode(&self.commit(
            Method::UpdateDevelopmentLaneQuota {
                request: DevelopmentLaneQuotaUpdateRequest {
                    schema_version:
                        crate::epistemic_operations::DevelopmentLaneQuotaUpdateRequestSchemaVersion::V1,
                    tenant_ref: tenant.into(),
                    policy,
                    expected_policy_revision,
                    expected_policy_version: None,
                    idempotency_key: idempotency_key.into(),
                    now_ms,
                },
            },
            now_ms,
        ))
    }

    pub(super) fn mutate_work_item<F>(&self, work_item_id: &str, mutate: F)
    where
        F: FnOnce(&mut serde_json::Map<String, serde_json::Value>),
    {
        self.with_write("mutate-work-item", |write| {
            let mut nodes = write.graph(TEST_GRAPH)?.open_scoped_table(NODES)?;
            let mut props: serde_json::Map<String, serde_json::Value> = {
                let bytes = nodes
                    .get((TEST_GRAPH, work_item_id))?
                    .expect("WorkItem exists");
                decode_durable(bytes.value()).expect("decode WorkItem")
            };
            mutate(&mut props);
            let encoded = rmp_serde::to_vec_named(&props).expect("encode WorkItem");
            nodes.insert((TEST_GRAPH, work_item_id), encoded.as_slice())
        })
        .expect("commit WorkItem mutation");
    }

    pub(super) fn seed_cleanup_work_item(
        &self,
        hold: &DevelopmentLaneHold,
        cleanup_work_item_id: &str,
        cleanup_work_item_fence: &str,
    ) {
        let correlation = DevelopmentLaneCleanupIntent {
            schema_version: DevelopmentLaneCleanupIntentSchemaVersion::V1,
            hold_id: hold.hold_id.clone(),
            lane_id: hold.lane_id.clone(),
            expected_hold_revision: hold.hold_revision,
        };
        let props = serde_json::json!({
            "node_type": "WorkItem",
            "kind": "lane.cleanup",
            "tenant": hold.tenant_ref,
            "status": "running",
            "lease_owner": "cleanup-controller",
            "last_lease_owner": "cleanup-controller",
            "attempt": 1,
            "lease_epoch": 1,
            "fencing_token": 1,
            "work_item_fence": cleanup_work_item_fence,
            "lease_expires_at": 1000.0,
            "metadata": {
                "repository_work_item": {
                    "development_lane_cleanup": serde_json::to_value(correlation)
                        .expect("encode cleanup correlation")
                }
            }
        });
        let encoded = rmp_serde::to_vec_named(&props).expect("encode cleanup WorkItem");
        self.with_write("seed-cleanup-work-item", |write| {
            write
                .graph(TEST_GRAPH)?
                .open_scoped_table(NODES)?
                .insert((TEST_GRAPH, cleanup_work_item_id), encoded.as_slice())
        })
        .expect("commit cleanup WorkItem");
    }
}
