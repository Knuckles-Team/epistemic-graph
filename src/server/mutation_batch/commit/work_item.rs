use std::sync::Arc;

use sha2::{Digest, Sha256};

use crate::graph::GraphCore;
use crate::mutation_batch::MutationSurface;
use crate::protocol::{Method, ResultPayload};
use crate::server::persistence::PersistenceBackend;
use eg_types::contract::Nonce;

use super::super::compile::{authoritative_graph_version, compile_methods, CompileBatch};
use super::internal::lock_graph;

fn derive_work_item_identity(
    graph: &str,
    tenant: &str,
    request_id: u64,
    stable_idempotency_key: Option<&str>,
    method: &Method,
) -> Result<(super::super::digest::WorkItemBatchIdentity, String), String> {
    // Claim, renew, and metadata-CAS methods have no method-body idempotency key;
    // their old identity therefore depended on the transport request number.
    // Once authenticated, the envelope key is the retry identity and the
    // request number used by the existing privacy-safe digest must be stable
    // across a retry. Native callers keep the transport-derived identity below.
    let identity_request_id = if let Some(key) = stable_idempotency_key {
        if matches!(
            method,
            Method::ClaimWorkItem { .. }
                | Method::RenewWorkItemLease { .. }
                | Method::CasWorkItemMetadata { .. }
        ) {
            let mut digest = Sha256::new();
            digest.update(b"epistemic-graph.authenticated-work-item.v1");
            for field in [graph.as_bytes(), tenant.as_bytes(), key.as_bytes()] {
                digest.update((field.len() as u64).to_be_bytes());
                digest.update(field);
            }
            let mut request_bytes = [0u8; 8];
            request_bytes.copy_from_slice(&digest.finalize()[..8]);
            u64::from_be_bytes(request_bytes).max(1)
        } else {
            request_id
        }
    } else {
        request_id
    };
    let identity =
        super::super::digest::work_item_batch_identity(graph, tenant, identity_request_id, method)?;
    // The batch id and the idempotency key must be functions of the SAME inputs,
    // or a legitimate retry is refused as a conflict. Terminal WorkItem methods
    // derive both values from their body key; row-CAS methods keep the authenticated
    // envelope key as their retry identity.
    let batch_idempotency_key = if identity.uses_native_row_cas {
        identity.idempotency_key.clone()
    } else {
        stable_idempotency_key
            .unwrap_or(identity.idempotency_key.as_str())
            .to_string()
    };
    Ok((identity, batch_idempotency_key))
}

fn work_item_tenant(method: &Method) -> Result<String, String> {
    let tenant = match method {
        Method::SubmitWorkItem { request } | Method::SubmitWorkItems { request } => {
            request.context.tenant_id.clone()
        }
        Method::ClaimWorkItem { request } | Method::CasWorkItemMetadata { request } => {
            request.tenant_ref.clone()
        }
        Method::RenewWorkItemLease { tenant, .. }
        | Method::CommitWorkItemResult { tenant, .. }
        | Method::CancelWorkItem { tenant, .. }
        | Method::DeferWorkItem { tenant, .. } => tenant.clone(),
        Method::ReserveWorkItemResources { request }
        | Method::ReleaseWorkItemResources { request }
        | Method::ReclaimWorkItemResources { request }
        | Method::UpdateResourceHost { request } => request.tenant_ref.clone(),
        _ => return Err("commit_work_item received a non-WorkItem operation".to_string()),
    };
    if tenant.trim().is_empty() {
        return Err("WorkItem mutation requires a non-empty tenant".to_string());
    }
    Ok(tenant)
}

pub(crate) struct WorkItemCommitRequest<'a> {
    pub(crate) persistence: Option<&'a Arc<dyn PersistenceBackend>>,
    pub(crate) core: &'a Arc<GraphCore>,
    pub(crate) request_id: u64,
    attempt_nonce: Option<Nonce>,
    pub(crate) stable_idempotency_key: Option<&'a str>,
    pub(crate) principal: Option<&'a str>,
    pub(crate) graph: &'a str,
    pub(crate) placement_epoch: u64,
    placement_fencing_token: Option<u64>,
    pub(crate) method: Method,
}

impl<'a> WorkItemCommitRequest<'a> {
    pub(crate) fn new(
        persistence: Option<&'a Arc<dyn PersistenceBackend>>,
        core: &'a Arc<GraphCore>,
        request_id: u64,
        stable_idempotency_key: Option<&'a str>,
        principal: Option<&'a str>,
        graph: &'a str,
        placement_epoch: u64,
        method: Method,
    ) -> Self {
        Self {
            persistence,
            core,
            request_id,
            attempt_nonce: None,
            stable_idempotency_key,
            principal,
            graph,
            placement_epoch,
            placement_fencing_token: None,
            method,
        }
    }

    pub(crate) fn with_attempt_nonce(mut self, attempt_nonce: Option<Nonce>) -> Self {
        self.attempt_nonce = attempt_nonce;
        self
    }

    pub(crate) fn with_placement_fencing_token(mut self, token: Option<u64>) -> Self {
        self.placement_fencing_token = token;
        self
    }
}

struct WorkItemCommitPlan<'a> {
    persistence: &'a Arc<dyn PersistenceBackend>,
    core: &'a Arc<GraphCore>,
    attempt_nonce: Option<Nonce>,
    principal: Option<&'a str>,
    graph: &'a str,
    placement_epoch: u64,
    placement_fencing_token: Option<u64>,
    method: Method,
    tenant: String,
    identity: super::super::digest::WorkItemBatchIdentity,
    batch_idempotency_key: String,
}

struct WorkItemCommitOutcome<'a> {
    persistence: &'a Arc<dyn PersistenceBackend>,
    core: &'a Arc<GraphCore>,
    graph_fname: String,
    committed: crate::mutation_batch::MutationBatchCommit,
    publishes_work_item_rows: bool,
    submit: bool,
    submit_batch: bool,
}

#[derive(Clone, Copy)]
struct WorkItemOutputFlags {
    submit: bool,
    submit_batch: bool,
    publishes_work_item_rows: bool,
}

fn work_item_output_flags(method: &Method) -> WorkItemOutputFlags {
    let submit_batch = matches!(method, Method::SubmitWorkItems { .. });
    let submit = submit_batch || matches!(method, Method::SubmitWorkItem { .. });
    WorkItemOutputFlags {
        submit,
        submit_batch,
        publishes_work_item_rows: !matches!(method, Method::UpdateResourceHost { .. }),
    }
}

async fn commit_work_item_inner(
    request: WorkItemCommitRequest<'_>,
) -> Result<ResultPayload, String> {
    let WorkItemCommitRequest {
        persistence,
        core,
        request_id,
        attempt_nonce,
        stable_idempotency_key,
        principal,
        graph,
        placement_epoch,
        placement_fencing_token,
        method,
    } = request;
    let persistence = persistence.ok_or_else(|| {
        "WorkItem mutation requires an authoritative persistence backend".to_string()
    })?;
    let tenant = work_item_tenant(&method)?;
    if stable_idempotency_key.is_some_and(|key| key.trim().is_empty()) {
        return Err(
            "authenticated WorkItem mutation requires a non-empty idempotency key".to_string(),
        );
    }
    let (identity, batch_idempotency_key) =
        derive_work_item_identity(graph, &tenant, request_id, stable_idempotency_key, &method)?;
    commit_work_item_plan(WorkItemCommitPlan {
        persistence,
        core,
        attempt_nonce,
        principal,
        graph,
        placement_epoch,
        placement_fencing_token,
        method,
        tenant,
        identity,
        batch_idempotency_key,
    })
    .await
}

async fn commit_work_item_plan(plan: WorkItemCommitPlan<'_>) -> Result<ResultPayload, String> {
    let WorkItemCommitPlan {
        persistence,
        core,
        attempt_nonce,
        principal,
        graph,
        placement_epoch,
        placement_fencing_token,
        method,
        tenant,
        identity,
        batch_idempotency_key,
    } = plan;
    // Keep version discovery, durable commit, and RAM publication inside the
    // shared per-graph lane so a ChangeEnvelope cannot pass its version fence.
    let _mutation_guard = lock_graph(graph).await;
    let flags = work_item_output_flags(&method);
    let created_at_ms = crate::server::dispatch::authoritative_now_ms();
    let fname = crate::persist::sanitize(graph);
    let expected_graph_version = authoritative_graph_version(persistence, &fname, core).await?;
    let batch = compile_methods(
        CompileBatch {
            batch_id: &identity.batch_id,
            request_id: identity.durable_request_id,
            attempt_nonce,
            principal,
            tenant: &tenant,
            graph,
            placement_epoch,
            idempotency_key: &batch_idempotency_key,
            expected_graph_version: Some(expected_graph_version),
            fencing_token: placement_fencing_token,
            created_at_ms,
            default_surface: MutationSurface::Job,
            authoritative_state: None,
        },
        vec![method],
    )?;
    let committed = persistence
        .commit_mutation_batch(&fname, &batch, None, created_at_ms)
        .await?;
    let outcome = WorkItemCommitOutcome {
        persistence,
        core,
        graph_fname: fname,
        committed,
        publishes_work_item_rows: flags.publishes_work_item_rows,
        submit: flags.submit,
        submit_batch: flags.submit_batch,
    };
    publish_work_item_outcome(outcome).await
}

async fn publish_work_item_outcome(
    outcome: WorkItemCommitOutcome<'_>,
) -> Result<ResultPayload, String> {
    let WorkItemCommitOutcome {
        persistence,
        core,
        graph_fname,
        committed,
        publishes_work_item_rows,
        submit,
        submit_batch,
    } = outcome;
    let result = publish_committed_work_item(
        persistence,
        &graph_fname,
        core,
        &committed,
        publishes_work_item_rows,
    )
    .await;
    match result {
        Ok(result) if committed.replayed && submit => mark_submit_replayed(result, submit_batch),
        Ok(result) => Ok(result),
        Err(error) => {
            match reconcile_projection_from_authority(persistence, &graph_fname, core).await {
                Ok(()) => Err(error),
                Err(repair) => Err(format!(
                    "{error}; serving projection repair from authority also failed: {repair}"
                )),
            }
        }
    }
}

/// Execute a WorkItem claim/renew/result transition inside the redb
/// MutationBatch transaction and then refresh every affected in-memory node from
/// the authoritative store. No selection or transition runs in RAM first.
pub(crate) async fn commit_work_item(
    request: WorkItemCommitRequest<'_>,
) -> Result<ResultPayload, String> {
    commit_work_item_inner(request).await
}
/// The durably committed submit result a replay re-answers, as its declared body.
fn decode_submit_receipt<T: serde::de::DeserializeOwned>(
    result: ResultPayload,
) -> Result<T, String> {
    match result {
        ResultPayload::Raw(bytes) => eg_types::msgpack::decode_bounded(
            &bytes,
            eg_types::msgpack::MsgpackLimits::new(4 * 1024 * 1024, 100_000, 64),
        )
        .map_err(|_| "replayed SubmitWorkItem result is corrupt".to_string()),
        ResultPayload::Json(value) => serde_json::from_value(value)
            .map_err(|_| "replayed SubmitWorkItem result is corrupt".to_string()),
        _ => Err("replayed SubmitWorkItem result has an invalid payload shape".to_string()),
    }
}

fn mark_submit_replayed(result: ResultPayload, batch: bool) -> Result<ResultPayload, String> {
    match batch {
        true => replayed_submit_batch(result),
        false => replayed_submit(result),
    }
}

fn replayed_submit_batch(result: ResultPayload) -> Result<ResultPayload, String> {
    let mut body: eg_types::native_control::SubmitWorkItemsResult = decode_submit_receipt(result)?;
    body.replayed = true;
    for child in &mut body.results {
        child.created = false;
        child.replayed = true;
    }
    ResultPayload::of::<eg_types::result_contract::coordination::SubmitWorkItems>(body)
}

fn replayed_submit(result: ResultPayload) -> Result<ResultPayload, String> {
    let mut body: eg_types::native_control::SubmitWorkItemResult = decode_submit_receipt(result)?;
    body.created = false;
    body.replayed = true;
    ResultPayload::of::<eg_types::result_contract::coordination::SubmitWorkItem>(body)
}

/// Decode a durably committed WorkItem batch's terminal result and publish its
/// changed rows into the serving projection, advancing the serving version exactly
/// once. Fallible only in the RAM-publication sense — the caller owns repairing the
/// projection from authority when this fails.
async fn publish_committed_work_item(
    persistence: &Arc<dyn PersistenceBackend>,
    graph_fname: &str,
    core: &Arc<GraphCore>,
    committed: &crate::mutation_batch::MutationBatchCommit,
    publishes_work_item_rows: bool,
) -> Result<ResultPayload, String> {
    let bytes = committed
        .record
        .result_msgpack
        .as_deref()
        .ok_or_else(|| "committed WorkItem batch has no durable result".to_string())?;
    let result: ResultPayload = eg_types::msgpack::decode_bounded(
        bytes,
        eg_types::msgpack::MsgpackLimits::new(64 * 1024 * 1024, 1_000_000, 64),
    )
    .map_err(|_| "committed WorkItem result is corrupt".to_string())?;

    if !committed.replayed {
        for node_id in changed_work_item_ids(&result, publishes_work_item_rows)? {
            let props = persistence
                .read_node(graph_fname, &node_id)
                .await?
                .ok_or_else(|| format!("committed WorkItem projection '{}' is missing", node_id))?;
            core.add_node(node_id, props);
        }
        core.mark_dirty();
    }
    Ok(result)
}

/// Re-materialize the serving projection from the authoritative durable image at the
/// authority's own version. This is the SAME primitive every idempotent-replay path
/// uses (`read_authoritative_graph_snapshot` -> `install_committed_snapshot`): it
/// installs the committed image and its committed version together, so it can never
/// silence the authority check by writing a version the durable rows do not back.
async fn reconcile_projection_from_authority(
    persistence: &Arc<dyn PersistenceBackend>,
    graph_fname: &str,
    core: &Arc<GraphCore>,
) -> Result<(), String> {
    let (snapshot, version) = persistence
        .read_authoritative_graph_snapshot(graph_fname)
        .await?
        .ok_or_else(|| "committed graph image is missing".to_string())?;
    core.install_committed_snapshot(snapshot, version)
}

pub(crate) fn changed_work_item_ids(
    result: &ResultPayload,
    publishes_work_item_rows: bool,
) -> Result<Vec<String>, String> {
    fn from_json(
        value: &serde_json::Value,
        publishes_work_item_rows: bool,
    ) -> Result<Vec<String>, String> {
        if !publishes_work_item_rows {
            return if value.get("changed_work_item_ids").is_none() {
                Ok(Vec::new())
            } else {
                Err(
                    "committed resource-host result unexpectedly has changed_work_item_ids"
                        .to_string(),
                )
            };
        }
        let values = value
            .get("changed_work_item_ids")
            .ok_or_else(|| "committed WorkItem result has no changed_work_item_ids".to_string())?
            .as_array()
            .ok_or_else(|| {
                "committed WorkItem result has non-array changed_work_item_ids".to_string()
            })?;
        values
            .iter()
            .map(|value| {
                value.as_str().map(str::to_string).ok_or_else(|| {
                    "committed WorkItem result has a non-string changed id".to_string()
                })
            })
            .collect()
    }

    match result {
        ResultPayload::Json(value) => from_json(value, publishes_work_item_rows),
        // ``ResultPayload::raw`` is the one canonical binary result representation.
        // The durable outer payload decodes to it and carries the typed WorkItem
        // result that must refresh the resident graph projection.
        ResultPayload::Raw(bytes) => {
            let value: serde_json::Value = eg_types::msgpack::decode_bounded(
                bytes,
                eg_types::msgpack::MsgpackLimits::new(1024 * 1024, 10_000, 32),
            )
            .map_err(|_| "committed WorkItem inner result is corrupt".to_string())?;
            from_json(&value, publishes_work_item_rows)
        }
        _ => Err("committed WorkItem result has an invalid payload shape".to_string()),
    }
}
