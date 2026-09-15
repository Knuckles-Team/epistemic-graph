//! Private implementation module for the analytics job handler.

#[cfg(feature = "raft")]
use super::core::job_result_payload;
use super::prelude_jobs::*;
use super::prelude_server::*;
use super::prelude_std::*;
use super::*;

/// Transient scheduler-group PREPARE result. It is returned only to the trusted
/// coordinator; every subsequent Raft command carries it AEAD-sealed.
#[cfg(feature = "raft")]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PreparedJobPublication {
    schema_version: u16,
    pub(crate) coordinator_id: String,
    pub(crate) target_graph: String,
    pub(crate) target_graph_type: crate::protocol::GraphType,
    job_id: String,
    worker_ref: String,
    lease_epoch: u64,
    result_ref: String,
    principal_ref: String,
    batch_id: String,
    claim_id: String,
    dataset_ref: String,
    methods: Vec<Method>,
    #[cfg(feature = "program-optimization")]
    promotion: Option<ProgramRevisionIdentity>,
}

/// Target-group COMMIT plan. Placement authority is frozen before it is sealed,
/// preventing a coordinator retry from silently changing participants.
#[cfg(feature = "raft")]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RoutedJobPublication {
    schema_version: u16,
    prepared: PreparedJobPublication,
    group_id: crate::raft::GroupId,
    placement_epoch: u64,
    fencing_token: Option<u64>,
}

/// Scheduler-group FINALIZE receipt. It contains no claim payload; the target
/// commit's success is represented by the fact this sealed command was proposed.
#[cfg(feature = "raft")]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FinalizeJobPublication {
    schema_version: u16,
    coordinator_id: String,
    job_id: String,
    worker_ref: String,
    lease_epoch: u64,
    result_ref: String,
}

#[cfg(feature = "raft")]
pub(super) fn valid_publication_scope(value: &str) -> bool {
    value.rsplit_once(':').is_some_and(|(namespace, digest)| {
        !namespace.is_empty()
            && digest.len() == 64
            && digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    })
}

#[cfg(feature = "raft")]
impl PreparedJobPublication {
    fn validate(&self) -> Result<(), String> {
        let coordinator_material = format!(
            "{}\0{}\0{}\0{}",
            self.job_id, self.worker_ref, self.lease_epoch, self.result_ref
        );
        let expected_coordinator = crate::server::mutation_batch::opaque_coordinator_key(
            "job-publication",
            &self.target_graph,
            &coordinator_material,
        );
        let batch_material = format!("{}\0{}", self.result_ref, self.job_id);
        let expected_batch = crate::server::mutation_batch::opaque_coordinator_key(
            "job-result",
            &self.target_graph,
            &batch_material,
        );
        if self.schema_version != JOB_PUBLICATION_PLAN_VERSION {
            return Err("unsupported job publication plan version".to_string());
        }
        if self.target_and_job_id_invalid()
            || self.lease_and_scope_invalid()
            || self.references_and_methods_invalid(&expected_coordinator, &expected_batch)
        {
            return Err("job publication plan is invalid".to_string());
        }
        let bytes = rmp_serde::to_vec_named(self).map_err(|error| error.to_string())?;
        if bytes.len() > MAX_JOB_PUBLICATION_PLAN_BYTES {
            return Err("job publication plan exceeds resource limits".to_string());
        }
        Ok(())
    }

    fn target_and_job_id_invalid(&self) -> bool {
        self.target_graph.is_empty()
            || self.target_graph.len() > 4_096
            || self.target_graph.chars().any(char::is_control)
            || self.job_id.is_empty()
            || self.job_id.len() > 256
            || self.job_id.chars().any(char::is_control)
    }

    fn lease_and_scope_invalid(&self) -> bool {
        self.lease_epoch == 0
            || !valid_publication_scope(&self.coordinator_id)
            || !valid_publication_scope(&self.worker_ref)
            || !valid_publication_scope(&self.principal_ref)
            || !valid_publication_scope(&self.batch_id)
    }

    fn references_and_methods_invalid(
        &self,
        expected_coordinator: &str,
        expected_batch: &str,
    ) -> bool {
        self.coordinator_id != expected_coordinator
            || self.batch_id != expected_batch
            || !is_opaque_result_ref(&self.result_ref)
            || !is_opaque_result_ref(&self.dataset_ref)
            || self.claim_id != eg_jobs::claim::claim_node_id(&self.result_ref)
            || self.methods.is_empty()
            || self.methods.len() > 64
            || self
                .methods
                .iter()
                .any(|method| !matches!(method, Method::AddNode { .. } | Method::AddEdge { .. }))
            || self.promotion_invalid()
    }

    #[cfg(feature = "program-optimization")]
    fn promotion_invalid(&self) -> bool {
        self.promotion
            .as_ref()
            .is_some_and(|identity| identity.validate().is_err())
    }

    #[cfg(not(feature = "program-optimization"))]
    fn promotion_invalid(&self) -> bool {
        false
    }
}

#[cfg(feature = "raft")]
impl RoutedJobPublication {
    fn validate(&self) -> Result<(), String> {
        if self.schema_version != JOB_PUBLICATION_PLAN_VERSION {
            return Err("unsupported routed job publication plan version".to_string());
        }
        self.prepared.validate()?;
        if self
            .fencing_token
            .is_some_and(|token| token != self.group_id)
        {
            return Err("job publication placement fence is invalid".to_string());
        }
        Ok(())
    }
}

#[cfg(feature = "raft")]
impl FinalizeJobPublication {
    fn validate(&self) -> Result<(), String> {
        if self.schema_version != JOB_PUBLICATION_PLAN_VERSION
            || !valid_publication_scope(&self.coordinator_id)
            || self.job_id.is_empty()
            || self.job_id.len() > 256
            || !valid_publication_scope(&self.worker_ref)
            || self.lease_epoch == 0
            || !is_opaque_result_ref(&self.result_ref)
        {
            return Err("job publication finalize receipt is invalid".to_string());
        }
        Ok(())
    }
}

pub(super) fn publication_result(
    claim_id: &str,
    dataset_ref: &str,
    promotion_identity: Option<serde_json::Value>,
) -> ResultPayload {
    let mut result = serde_json::Map::new();
    result.insert(
        "claim_id".to_string(),
        serde_json::Value::String(claim_id.to_string()),
    );
    result.insert(
        "dataset_ref".to_string(),
        serde_json::Value::String(dataset_ref.to_string()),
    );
    if let Some(identity) = promotion_identity {
        result.insert("promotion_identity".to_string(), identity);
    }
    ResultPayload::Json(serde_json::Value::Object(result))
}

/// Validate the target group's exact durable receipt against the publication
/// plan that requested it.  Transport validation proves that the response is a
/// committed mutation; this domain check proves that it is THIS job's graph
/// batch and terminal publication result.
#[cfg(feature = "raft")]
pub(crate) fn validate_job_publication_commit(
    prepared: &PreparedJobPublication,
    committed: &crate::mutation_batch::MutationBatchCommit,
) -> Result<(), String> {
    prepared.validate()?;
    committed.validate()?;
    let batch = &committed.record.batch;
    if batch.batch_id != prepared.batch_id || batch.idempotency_key() != prepared.batch_id {
        return Err("job publication receipt has the wrong batch identity".to_string());
    }
    let expected_scope = eg_types::MutationScopeIdentity::fixed_graph(
        &prepared.target_graph,
        &prepared.target_graph,
        crate::server::mutation_batch::COMPILED_BATCH_INCARNATION,
    )?;
    if batch.identity != expected_scope
        || committed.record.identity != expected_scope
        || committed.identity != expected_scope
    {
        return Err("job publication receipt has the wrong mutation scope identity".to_string());
    }
    #[cfg(feature = "program-optimization")]
    let promotion_value = prepared
        .promotion
        .as_ref()
        .map(serde_json::to_value)
        .transpose()
        .map_err(|error| error.to_string())?;
    let expected = publication_result(
        &prepared.claim_id,
        &prepared.dataset_ref,
        #[cfg(feature = "program-optimization")]
        promotion_value,
        #[cfg(not(feature = "program-optimization"))]
        None,
    );
    let expected = rmp_serde::to_vec_named(&expected).map_err(|error| error.to_string())?;
    if committed.record.result_msgpack.as_deref() != Some(expected.as_slice()) {
        return Err("job publication receipt has the wrong terminal result".to_string());
    }
    Ok(())
}

#[cfg(feature = "raft")]
pub(super) async fn prepare_consensus_job_publication(
    state: &Arc<RwLock<ServerState>>,
    job: &eg_jobs::AnalyticsJob,
    worker_ref: &str,
    lease_epoch: u64,
) -> Result<Vec<u8>, String> {
    let (target_graph, target_graph_type, _core) =
        resolve_core_ref(state, &job.input_snapshot.graph)
            .await
            .ok_or_else(|| "analytics target graph is unavailable".to_string())?;
    let (confidence, calibration) = result_quality(job);
    let plan = eg_jobs::plan_result_claim(job, confidence, calibration)?;
    let result_ref = job.result_ref();
    let coordinator_material = format!(
        "{}\0{}\0{}\0{}",
        job.job_id, worker_ref, lease_epoch, result_ref
    );
    let coordinator_id = crate::server::mutation_batch::opaque_coordinator_key(
        "job-publication",
        &target_graph,
        &coordinator_material,
    );
    let batch_material = format!("{}\0{}", result_ref, job.job_id);
    let batch_id = crate::server::mutation_batch::opaque_coordinator_key(
        "job-result",
        &target_graph,
        &batch_material,
    );
    let dataset_ref = job
        .output
        .as_ref()
        .map(|output| output.dataset_ref.clone())
        .ok_or_else(|| "staged analytics result is missing".to_string())?;
    #[cfg(feature = "program-optimization")]
    let promotion = staged_program_promotion(job)?;
    let prepared = PreparedJobPublication {
        schema_version: JOB_PUBLICATION_PLAN_VERSION,
        coordinator_id,
        target_graph,
        target_graph_type,
        job_id: job.job_id.clone(),
        worker_ref: worker_ref.to_string(),
        lease_epoch,
        result_ref,
        principal_ref: job.policy.actor.clone(),
        batch_id,
        claim_id: plan.claim_id,
        dataset_ref,
        methods: plan.methods,
        #[cfg(feature = "program-optimization")]
        promotion,
    };
    prepared.validate()?;
    rmp_serde::to_vec_named(&prepared).map_err(|error| error.to_string())
}

#[cfg(feature = "raft")]
pub(crate) fn decode_prepared_job_publication(
    bytes: &[u8],
) -> Result<PreparedJobPublication, String> {
    if bytes.is_empty() || bytes.len() > MAX_JOB_PUBLICATION_PLAN_BYTES {
        return Err("job publication prepare result exceeds resource limits".to_string());
    }
    let prepared: PreparedJobPublication = eg_types::msgpack::decode_bounded(
        bytes,
        eg_types::msgpack::MsgpackLimits::new(
            MAX_JOB_PUBLICATION_PLAN_BYTES,
            1_000_000,
            eg_types::msgpack::DEFAULT_MAX_DEPTH,
        ),
    )
    .map_err(|_| "job publication prepare result is invalid".to_string())?;
    prepared.validate()?;
    Ok(prepared)
}

#[cfg(feature = "raft")]
pub(crate) fn build_job_publication_commands(
    prepared: PreparedJobPublication,
    group_id: crate::raft::GroupId,
    placement_epoch: u64,
    fencing_token: Option<u64>,
) -> Result<(Vec<u8>, Vec<u8>), String> {
    prepared.validate()?;
    let finalize = FinalizeJobPublication {
        schema_version: JOB_PUBLICATION_PLAN_VERSION,
        coordinator_id: prepared.coordinator_id.clone(),
        job_id: prepared.job_id.clone(),
        worker_ref: prepared.worker_ref.clone(),
        lease_epoch: prepared.lease_epoch,
        result_ref: prepared.result_ref.clone(),
    };
    finalize.validate()?;
    let routed = RoutedJobPublication {
        schema_version: JOB_PUBLICATION_PLAN_VERSION,
        prepared,
        group_id,
        placement_epoch,
        fencing_token,
    };
    routed.validate()?;
    let commit = rmp_serde::to_vec_named(&routed).map_err(|error| error.to_string())?;
    let finalize = rmp_serde::to_vec_named(&finalize).map_err(|error| error.to_string())?;
    if commit.len() > MAX_JOB_PUBLICATION_PLAN_BYTES
        || finalize.len() > MAX_JOB_PUBLICATION_PLAN_BYTES
    {
        return Err("job publication command exceeds resource limits".to_string());
    }
    Ok((commit, finalize))
}

#[cfg(feature = "raft")]
pub(super) fn decode_routed_job_publication(bytes: &[u8]) -> Result<RoutedJobPublication, String> {
    if bytes.is_empty() || bytes.len() > MAX_JOB_PUBLICATION_PLAN_BYTES {
        return Err("job publication commit plan exceeds resource limits".to_string());
    }
    let plan: RoutedJobPublication = eg_types::msgpack::decode_bounded(
        bytes,
        eg_types::msgpack::MsgpackLimits::new(
            MAX_JOB_PUBLICATION_PLAN_BYTES,
            1_000_000,
            eg_types::msgpack::DEFAULT_MAX_DEPTH,
        ),
    )
    .map_err(|_| "job publication commit plan is invalid".to_string())?;
    plan.validate()?;
    Ok(plan)
}

#[cfg(feature = "raft")]
pub(super) fn decode_finalize_job_publication(
    bytes: &[u8],
) -> Result<FinalizeJobPublication, String> {
    if bytes.is_empty() || bytes.len() > MAX_JOB_PUBLICATION_PLAN_BYTES {
        return Err("job publication finalize receipt exceeds resource limits".to_string());
    }
    let receipt: FinalizeJobPublication = eg_types::msgpack::decode_bounded(
        bytes,
        eg_types::msgpack::MsgpackLimits::new(
            MAX_JOB_PUBLICATION_PLAN_BYTES,
            64,
            eg_types::msgpack::DEFAULT_MAX_DEPTH,
        ),
    )
    .map_err(|_| "job publication finalize receipt is invalid".to_string())?;
    receipt.validate()?;
    Ok(receipt)
}

#[cfg(feature = "raft")]
pub(crate) async fn apply_consensus_job_publication_commit(
    state: &Arc<RwLock<ServerState>>,
    request_id: u64,
    authority: &crate::raft::RaftMutationContext,
    applying_group: crate::raft::GroupId,
    expected_coordinator_id: &str,
    plan_bytes: &[u8],
) -> Result<crate::mutation_batch::MutationBatchCommit, String> {
    let plan = decode_routed_job_publication(plan_bytes)?;
    if plan.prepared.coordinator_id != expected_coordinator_id
        || plan.group_id != applying_group
        || plan.placement_epoch != authority.placement_epoch
        || plan.fencing_token != authority.fencing_token
    {
        return Err("job publication commit reached the wrong authority".to_string());
    }
    let (multi, core, persistence) = {
        let current = state.read().await;
        let multi = current
            .multi_raft
            .clone()
            .ok_or_else(|| "job publication lost placement authority".to_string())?;
        let entry = current
            .registry
            .get(&plan.prepared.target_graph)
            .ok_or_else(|| "analytics target graph is unavailable".to_string())?;
        if entry.graph_type != plan.prepared.target_graph_type {
            return Err("analytics target graph type changed".to_string());
        }
        (multi, entry.core.clone(), current.persistence.clone())
    };
    let route = multi.route_graph(&plan.prepared.target_graph).await;
    if route.group != plan.group_id
        || route.epoch != plan.placement_epoch
        || route.placed.then_some(route.fencing_token()) != plan.fencing_token
    {
        return Err("job publication target placement changed".to_string());
    }
    #[cfg(feature = "program-optimization")]
    let promotion_identity = plan.prepared.promotion.as_ref();
    #[cfg(feature = "program-optimization")]
    let promotion_value = promotion_identity
        .map(serde_json::to_value)
        .transpose()
        .map_err(|error| error.to_string())?;
    let result = publication_result(
        &plan.prepared.claim_id,
        &plan.prepared.dataset_ref,
        #[cfg(feature = "program-optimization")]
        promotion_value,
        #[cfg(not(feature = "program-optimization"))]
        None,
    );
    #[cfg(feature = "program-optimization")]
    let committed = if let Some(identity) = promotion_identity {
        crate::server::mutation_batch::commit_program_promotion(
            crate::server::mutation_batch::ProgramPromotionRequest::new(
                persistence.as_ref(),
                &core,
                request_id,
                Some(&plan.prepared.principal_ref),
                &plan.prepared.target_graph,
                &plan.prepared.batch_id,
                plan.prepared.methods,
                &result,
            )
            .with_identity(identity)
            .with_attempt_nonce(authority.attempt_nonce),
        )
        .await?
    } else {
        crate::server::mutation_batch::commit_internal_graph_methods_with_nonce(
            crate::server::mutation_batch::InternalGraphCommitRequest::new(
                persistence.as_ref(),
                &core,
                request_id,
                Some(&plan.prepared.principal_ref),
                &plan.prepared.target_graph,
                &plan.prepared.batch_id,
                plan.prepared.methods,
                &result,
            )
            .with_attempt_nonce(authority.attempt_nonce),
        )
        .await?
    };
    #[cfg(not(feature = "program-optimization"))]
    let committed = crate::server::mutation_batch::commit_internal_graph_methods_with_nonce(
        crate::server::mutation_batch::InternalGraphCommitRequest::new(
            persistence.as_ref(),
            &core,
            request_id,
            Some(&plan.prepared.principal_ref),
            &plan.prepared.target_graph,
            &plan.prepared.batch_id,
            plan.prepared.methods,
            &result,
        )
        .with_attempt_nonce(authority.attempt_nonce.clone()),
    )
    .await?;
    Ok(committed)
}

#[cfg(feature = "raft")]
pub(crate) async fn apply_consensus_job_publication_finalize(
    state: &Arc<RwLock<ServerState>>,
    committed_at_ms: u64,
    expected_coordinator_id: &str,
    receipt_bytes: &[u8],
) -> Result<ResultPayload, String> {
    let receipt = decode_finalize_job_publication(receipt_bytes)?;
    if receipt.coordinator_id != expected_coordinator_id {
        return Err("job publication finalize coordinator changed".to_string());
    }
    let persist_dir = state.read().await.persist_dir.clone();
    let store = job_store(persist_dir.as_deref())?;
    let committed_at_ms = i64::try_from(committed_at_ms)
        .map_err(|_| "job publication timestamp exceeds resource limits".to_string())?;
    let job = store
        .complete_publication_prepared(
            &receipt.job_id,
            &receipt.worker_ref,
            receipt.lease_epoch,
            &receipt.result_ref,
            committed_at_ms,
        )
        .map_err(|error| error.to_string())?;
    job_result_payload::<eg_types::result_contract::coordination::JobWorkerPublish>(&job)
}

pub(super) async fn publish_staged_result(
    state: &Arc<RwLock<ServerState>>,
    store: &JobStore,
    job: eg_jobs::AnalyticsJob,
    worker_ref: &str,
    epoch: u64,
) -> Result<(), String> {
    let (target_graph, _target_graph_type, core) =
        resolve_core_ref(state, &job.input_snapshot.graph)
            .await
            .ok_or_else(|| "analytics target graph is unavailable".to_string())?;
    let persistence = state.read().await.persistence.clone();
    let (confidence, calibration) = result_quality(&job);
    let plan = eg_jobs::plan_result_claim(&job, confidence, calibration)?;
    let coordinator = format!("{}\0{}", job.result_ref(), job.job_id);
    let batch_id = crate::server::mutation_batch::opaque_coordinator_key(
        "job-result",
        &target_graph,
        &coordinator,
    );
    let dataset_ref = job
        .output
        .as_ref()
        .map(|output| output.dataset_ref.clone())
        .unwrap_or_default();
    #[cfg(feature = "program-optimization")]
    let promotion_identity = staged_program_promotion(&job)?;
    #[cfg(feature = "program-optimization")]
    let promotion_value = promotion_identity
        .as_ref()
        .map(serde_json::to_value)
        .transpose()
        .map_err(|error| error.to_string())?;
    let result = publication_result(
        &plan.claim_id,
        &dataset_ref,
        #[cfg(feature = "program-optimization")]
        promotion_value,
        #[cfg(not(feature = "program-optimization"))]
        None,
    );
    #[cfg(feature = "program-optimization")]
    if let Some(identity) = promotion_identity.as_ref() {
        crate::server::mutation_batch::commit_program_promotion(
            crate::server::mutation_batch::ProgramPromotionRequest::new(
                persistence.as_ref(),
                &core,
                0,
                Some(&job.policy.actor),
                &target_graph,
                &batch_id,
                plan.methods,
                &result,
            )
            .with_identity(identity)
            .with_attempt_nonce(None),
        )
        .await?;
    } else {
        crate::server::mutation_batch::commit_internal_graph_methods(
            crate::server::mutation_batch::InternalGraphCommitRequest::new(
                persistence.as_ref(),
                &core,
                0,
                Some(&job.policy.actor),
                &target_graph,
                &batch_id,
                plan.methods,
                &result,
            ),
        )
        .await?;
    }
    #[cfg(not(feature = "program-optimization"))]
    crate::server::mutation_batch::commit_internal_graph_methods(
        crate::server::mutation_batch::InternalGraphCommitRequest::new(
            persistence.as_ref(),
            &core,
            0,
            Some(&job.policy.actor),
            &target_graph,
            &batch_id,
            plan.methods,
            &result,
        ),
    )
    .await?;
    let completed = store
        .complete_publication_fenced(&job.job_id, worker_ref, epoch, unix_ms())
        .map_err(|error| error.to_string())?;
    let _ = completed;
    Ok(())
}

#[cfg(all(test, feature = "raft"))]
mod publication_receipt_tests {
    use super::*;

    fn prepared() -> PreparedJobPublication {
        let target_graph = "publication-receipt-graph";
        let result_ref = native_opaque_ref("job_result", "publication-receipt-result");
        let job_id = "publication-receipt-job";
        let worker_ref = native_opaque_ref("worker", "publication-receipt-worker");
        let lease_epoch = 1;
        let coordinator_id = crate::server::mutation_batch::opaque_coordinator_key(
            "job-publication",
            target_graph,
            &format!("{job_id}\0{worker_ref}\0{lease_epoch}\0{result_ref}"),
        );
        let batch_id = crate::server::mutation_batch::opaque_coordinator_key(
            "job-result",
            target_graph,
            &format!("{result_ref}\0{job_id}"),
        );
        PreparedJobPublication {
            schema_version: JOB_PUBLICATION_PLAN_VERSION,
            coordinator_id,
            target_graph: target_graph.to_string(),
            target_graph_type: crate::protocol::GraphType::Agent,
            job_id: job_id.to_string(),
            worker_ref,
            lease_epoch,
            result_ref: result_ref.clone(),
            principal_ref: native_opaque_ref("principal", "publication-receipt-principal"),
            batch_id,
            claim_id: eg_jobs::claim::claim_node_id(&result_ref),
            dataset_ref: native_opaque_ref("dataset", "publication-receipt-dataset"),
            methods: vec![Method::AddNode {
                node_id: "publication-receipt-claim".to_string(),
                properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({
                    "type": "Claim",
                    "family": "job"
                }))
                .unwrap(),
            }],
            #[cfg(feature = "program-optimization")]
            promotion: None,
        }
    }

    fn receipt_for(
        prepared: &PreparedJobPublication,
    ) -> crate::mutation_batch::MutationBatchCommit {
        let batch = crate::server::mutation_batch::compile_methods(
            crate::server::mutation_batch::CompileBatch {
                batch_id: &prepared.batch_id,
                request_id: 11,
                attempt_nonce: None,
                principal: Some(&prepared.principal_ref),
                tenant: &prepared.target_graph,
                graph: &prepared.target_graph,
                placement_epoch: 0,
                idempotency_key: &prepared.batch_id,
                expected_graph_version: Some(0),
                fencing_token: None,
                created_at_ms: 17,
                default_surface: MutationSurface::Job,
                authoritative_state: None,
            },
            prepared.methods.clone(),
        )
        .unwrap();
        let identity = batch.identity.clone();
        let result = publication_result(
            &prepared.claim_id,
            &prepared.dataset_ref,
            #[cfg(feature = "program-optimization")]
            None,
            #[cfg(not(feature = "program-optimization"))]
            None,
        );
        crate::mutation_batch::MutationBatchCommit {
            record: crate::mutation_batch::MutationBatchRecord {
                batch,
                identity: identity.clone(),
                status: crate::mutation_batch::MutationBatchStatus::Committed,
                committed_version: crate::mutation_batch::CommittedVersion::Graph {
                    source: 0,
                    target: 1,
                },
                result_msgpack: Some(rmp_serde::to_vec_named(&result).unwrap()),
                committed_at_ms: 17,
            },
            identity,
            replayed: false,
        }
    }

    #[test]
    fn publication_receipt_is_bound_to_plan_batch_graph_and_result() {
        let prepared = prepared();
        let receipt = receipt_for(&prepared);
        validate_job_publication_commit(&prepared, &receipt).unwrap();

        let mut wrong_batch = receipt.clone();
        wrong_batch.record.batch.batch_id.push_str("-other");
        assert!(validate_job_publication_commit(&prepared, &wrong_batch).is_err());

        let mut wrong_result = receipt;
        wrong_result.record.result_msgpack = Some(
            rmp_serde::to_vec_named(&publication_result(
                "jobclaim:other",
                &prepared.dataset_ref,
                #[cfg(feature = "program-optimization")]
                None,
                #[cfg(not(feature = "program-optimization"))]
                None,
            ))
            .unwrap(),
        );
        assert!(validate_job_publication_commit(&prepared, &wrong_result).is_err());

        let wrong_tenant = eg_types::MutationScopeIdentity::fixed_graph(
            "other-publication-tenant",
            &prepared.target_graph,
            crate::server::mutation_batch::COMPILED_BATCH_INCARNATION,
        )
        .unwrap();
        let mut wrong_tenant_receipt = receipt_for(&prepared);
        wrong_tenant_receipt.record.batch.identity = wrong_tenant.clone();
        wrong_tenant_receipt.record.identity = wrong_tenant.clone();
        wrong_tenant_receipt.identity = wrong_tenant;
        assert!(validate_job_publication_commit(&prepared, &wrong_tenant_receipt).is_err());

        let wrong_incarnation = eg_types::MutationScopeIdentity::fixed_graph(
            &prepared.target_graph,
            &prepared.target_graph,
            "epistemic-graph:mutation-batch-compiler:wrong-incarnation",
        )
        .unwrap();
        let mut wrong_incarnation_receipt = receipt_for(&prepared);
        wrong_incarnation_receipt.record.batch.identity = wrong_incarnation.clone();
        wrong_incarnation_receipt.record.identity = wrong_incarnation.clone();
        wrong_incarnation_receipt.identity = wrong_incarnation;
        assert!(validate_job_publication_commit(&prepared, &wrong_incarnation_receipt).is_err());
    }
}
