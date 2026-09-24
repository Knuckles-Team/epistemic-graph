//! `Method::PolicyEvolution` (EH-346/EH-347): capture-first open-weight
//! policy evolution's five EG-owned records.
//!
//! EG records and relates; it never trains, holds no checkpoint bytes and runs
//! no model. Each write is admitted against what the request graph already
//! holds and then lands as exactly ONE `CreateNodeIfAbsent` through the
//! ordinary graph gateway -- durable, audited and CDC-emitted like any node
//! write. Records are content-addressed, so a repeat is a replay and nothing
//! is ever overwritten.
//!
//! * `gate` -- cross-record admission (capability controls, sampler binding,
//!   trajectory, run outcome), pure over a lookup.
//! * `store` -- node encoding, and decoding that re-verifies each record's id.
//! * `blobs` -- held Blob-CAS references.

use eg_types::policy_evolution::{
    PolicyEvolutionRecord, PolicyRecordGetRequest, PolicyRecordKind, PolicyRecordReceipt,
    PolicyRecordStored, PolicyWriteDisposition, StoredPolicyRecord,
};
use eg_types::result_contract::graph::{
    ModelPolicyVersionRegister, PolicyCapabilityPut, PolicyCaptureCommit, PolicyEvaluationCommit,
    PolicyRecordGet, TrainingRunCommit,
};

use super::graph_pipeline::dispatch_graph_op;
use super::*;

mod blobs;
mod gate;
mod store;

use store::GraphRecords;

/// Where one policy-evolution request lands: the request graph, and who asked.
pub(super) struct PolicyEvolutionTarget<'a> {
    pub(super) req_id: u64,
    pub(super) graph: &'a str,
    pub(super) caller: Option<&'a str>,
}

/// Route one operation. Scope and admin authority were checked against the
/// op's own `authz_action` before dispatch; the tenant and writer come only
/// from `verified`.
pub(super) async fn handle_policy_evolution(
    state: &Arc<RwLock<ServerState>>,
    target: PolicyEvolutionTarget<'_>,
    verified: &VerifiedRequestContext,
    op: eg_types::policy_evolution::PolicyEvolutionOp,
) -> Response {
    match op.into_record() {
        Ok(record) => {
            let kind = record.kind();
            let receipt = commit(state, &target, verified, record).await;
            receipt_response(target.req_id, kind, receipt)
        }
        Err(request) => get(state, &target, verified, request).await,
    }
}

/// Encode a receipt as the kind's declared result, or answer the refusal.
fn receipt_response(
    req_id: u64,
    kind: PolicyRecordKind,
    receipt: Result<PolicyRecordReceipt, String>,
) -> Response {
    let payload = receipt.and_then(|receipt| match kind {
        PolicyRecordKind::Capability => ResultPayload::of::<PolicyCapabilityPut>(receipt),
        PolicyRecordKind::Capture => ResultPayload::of::<PolicyCaptureCommit>(receipt),
        PolicyRecordKind::ModelPolicyVersion => {
            ResultPayload::of::<ModelPolicyVersionRegister>(receipt)
        }
        PolicyRecordKind::TrainingRun => ResultPayload::of::<TrainingRunCommit>(receipt),
        PolicyRecordKind::PolicyEvaluation => ResultPayload::of::<PolicyEvaluationCommit>(receipt),
    });
    match payload {
        Ok(payload) => Response::ok(req_id, payload),
        Err(error) => Response::err(req_id, error),
    }
}

/// What the committed image says before the write: already recorded, or
/// admitted. Reads only; the lock is released before anything is written.
async fn admit_against_graph(
    state: &Arc<RwLock<ServerState>>,
    graph: &str,
    tenant_id: &str,
    record_id: &str,
    record: &PolicyEvolutionRecord,
) -> Result<bool, String> {
    let current = timed_read(state).await;
    let records = GraphRecords {
        core: current.registry.get(graph).map(|entry| entry.core.as_ref()),
        tenant_id,
    };
    if records
        .view(record_id)
        .map_err(|refusal| refusal.to_string())?
        .is_some()
    {
        return Ok(true);
    }
    gate::admit(record, &records).map_err(|refusal| refusal.to_string())?;
    Ok(false)
}

async fn commit(
    state: &Arc<RwLock<ServerState>>,
    target: &PolicyEvolutionTarget<'_>,
    verified: &VerifiedRequestContext,
    record: PolicyEvolutionRecord,
) -> Result<PolicyRecordReceipt, String> {
    record.validate().map_err(|refusal| refusal.to_string())?;
    let tenant_id = verified.tenant();
    let record_id = record.record_id(tenant_id)?;
    let recorded = admit_against_graph(state, target.graph, tenant_id, &record_id, &record).await?;
    let disposition = if recorded {
        PolicyWriteDisposition::Replayed
    } else {
        blobs::verify_held_blobs(state, verified, &record)
            .await
            .map_err(|refusal| refusal.to_string())?;
        write_node(state, target, verified, &record_id, &record).await?
    };
    Ok(PolicyRecordReceipt {
        kind: record.kind(),
        eligibility: match &record {
            PolicyEvolutionRecord::Capture { record } => Some(record.eligibility()),
            _ => None,
        },
        record_id,
        disposition,
        observed_at_ms: authoritative_now_ms(),
    })
}

/// The single durable write: the WorkItem kernel's internal
/// `PolicyEvolutionStore`, the only writer of a policy-evolution row. A
/// `created: false` answer means a concurrent writer committed the same
/// content-addressed record first.
async fn write_node(
    state: &Arc<RwLock<ServerState>>,
    target: &PolicyEvolutionTarget<'_>,
    verified: &VerifiedRequestContext,
    record_id: &str,
    record: &PolicyEvolutionRecord,
) -> Result<PolicyWriteDisposition, String> {
    let request = StoredPolicyRecord {
        record_id: record_id.to_string(),
        tenant_id: verified.tenant().to_string(),
        recorded_by: verified.principal_persistence_id(),
        recorded_at_ms: authoritative_now_ms(),
        record: record.clone(),
    };
    let method = Method::PolicyEvolutionStore {
        request: Box::new(request),
    };
    let response = dispatch_graph_op(
        state,
        target.graph,
        target.req_id,
        target.caller,
        verified,
        method,
    )
    .await;
    let stored: PolicyRecordStored = match response {
        Response {
            error: Some(error), ..
        } => return Err(error),
        Response {
            result: Some(payload),
            ..
        } => decode_stored(payload)?,
        Response { .. } => return Err("policy record store answered no result".into()),
    };
    Ok(if stored.created {
        PolicyWriteDisposition::Written
    } else {
        PolicyWriteDisposition::Replayed
    })
}

/// The kernel's typed answer, in either result encoding.
fn decode_stored(payload: ResultPayload) -> Result<PolicyRecordStored, String> {
    let limits = eg_types::msgpack::MsgpackLimits::new(64 * 1024, 1_024, 8);
    match payload {
        ResultPayload::Raw(bytes) => eg_types::msgpack::decode_bounded(&bytes, limits)
            .map_err(|_| "policy record store answered a corrupt result".to_string()),
        ResultPayload::Json(value) => serde_json::from_value(value)
            .map_err(|_| "policy record store answered a corrupt result".to_string()),
        _ => Err("policy record store answered an unexpected result".to_string()),
    }
}

async fn get(
    state: &Arc<RwLock<ServerState>>,
    target: &PolicyEvolutionTarget<'_>,
    verified: &VerifiedRequestContext,
    request: PolicyRecordGetRequest,
) -> Response {
    let current = timed_read(state).await;
    let records = GraphRecords {
        core: current
            .registry
            .get(target.graph)
            .map(|entry| entry.core.as_ref()),
        tenant_id: verified.tenant(),
    };
    let view = records
        .view(request.record_id.as_str())
        .map_err(|refusal| refusal.to_string())
        .and_then(ResultPayload::of::<PolicyRecordGet>);
    match view {
        Ok(payload) => Response::ok(target.req_id, payload),
        Err(error) => Response::err(target.req_id, error),
    }
}
