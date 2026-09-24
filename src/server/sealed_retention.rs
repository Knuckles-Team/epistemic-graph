//! Policy-driven expiry of sealed records (EH-558).
//!
//! A sealed record is removed only by its owning op, `RetireSealedRecord`. Expiry is
//! that same op, issued by the engine: this sweep finds every sealed record whose
//! class has a configured retention and whose `sealed_at_property` is older than it,
//! and commits a `RetireSealedRecord` for it through the native WorkItem kernel. The
//! tombstone is audited like any other retirement; it names the engine as the actor
//! and the retention as the reason.
//!
//! Every field of the request is derived from the record and the policy — the
//! retirement instant is the record's expiry instant, not the sweep's clock — so a
//! sweep that fails half way and runs again replays the same request under the same
//! idempotency key.
//!
//! The policy is `EPISTEMIC_GRAPH_SEALED_RETENTION`, a comma-separated list of
//! `Class=milliseconds` (for example `AnalysisSnapshot=2592000000` for 30 days). It is
//! empty by default: no sealed record expires unless a retention is configured. The
//! sweep runs every `EPISTEMIC_GRAPH_SEALED_RETENTION_SECS` seconds (default 3600)
//! when a policy is configured.

use std::sync::Arc;

use eg_types::sealed_record::{
    expires_at_ms, sealed_class, sealed_record_class, RetireSealedRecordRequest, SealedClass,
    SEALED_TOMBSTONE_TYPE,
};
use tokio::sync::RwLock;

use crate::graph::GraphCore;
use crate::protocol::Method;
use crate::server::ServerState;

/// The actor every policy retirement is attributed to.
pub const RETENTION_ACTOR: &str = "engine:sealed-retention";
/// Default sweep interval, seconds.
const DEFAULT_SWEEP_SECS: u64 = 3_600;

/// One class's configured retention.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Retention {
    pub class: &'static SealedClass,
    pub retention_ms: u64,
}

/// Parse a retention policy. An unknown class, a tombstone class or a malformed
/// entry is an error naming the entry: a mistyped policy must not silently retain
/// forever.
pub fn parse_policy(spec: &str) -> Result<Vec<Retention>, String> {
    spec.split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(parse_entry)
        .collect()
}

fn parse_entry(entry: &str) -> Result<Retention, String> {
    let malformed = || format!("sealed retention entry '{entry}' is not Class=milliseconds");
    let (class, retention) = entry.split_once('=').ok_or_else(malformed)?;
    let retention_ms: u64 = retention.trim().parse().map_err(|_| malformed())?;
    let class = sealed_class(class.trim())
        .filter(|class| class.name != SEALED_TOMBSTONE_TYPE)
        .ok_or_else(|| format!("sealed retention entry '{entry}' names no retirable class"))?;
    if retention_ms == 0 {
        return Err(malformed());
    }
    Ok(Retention {
        class,
        retention_ms,
    })
}

/// The configured policy; an invalid policy is logged and treated as empty.
pub fn configured_policy() -> Vec<Retention> {
    let spec = std::env::var("EPISTEMIC_GRAPH_SEALED_RETENTION").unwrap_or_default();
    parse_policy(&spec).unwrap_or_else(|error| {
        tracing::error!(%error, "sealed record retention policy ignored");
        Vec::new()
    })
}

/// The configured positive sweep interval.
pub fn sweep_interval_secs() -> u64 {
    crate::server::state::positive_runtime_limit_from_env(
        "EPISTEMIC_GRAPH_SEALED_RETENTION_SECS",
        DEFAULT_SWEEP_SECS,
    )
}

/// The retirement a policy owes one stored row at `now_ms`, if any.
pub fn due_retirement(
    tenant: &str,
    graph: &str,
    node_id: &str,
    row: &serde_json::Map<String, serde_json::Value>,
    policy: &[Retention],
    now_ms: u64,
) -> Option<RetireSealedRecordRequest> {
    let class = sealed_record_class(row)?;
    let retention = policy.iter().find(|entry| entry.class.name == class.name)?;
    let expires = expires_at_ms(row, retention.retention_ms).filter(|at| *at <= now_ms)?;
    let digest = row.get(class.digest_property)?.as_str()?.to_string();
    Some(RetireSealedRecordRequest {
        tenant: tenant.to_string(),
        node_id: node_id.to_string(),
        idempotency_key: format!("sealed-retention:{graph}:{node_id}:{digest}"),
        digest,
        reason: format!(
            "expired: {} retention of {} ms",
            class.name, retention.retention_ms
        ),
        retired_at_ms: expires.max(1),
        retired_by: RETENTION_ACTOR.to_string(),
    })
}

/// Every retirement a policy owes one graph at `now_ms`.
pub fn due_in_graph(
    tenant: &str,
    graph: &str,
    core: &GraphCore,
    policy: &[Retention],
    now_ms: u64,
) -> Vec<RetireSealedRecordRequest> {
    core.get_nodes()
        .into_iter()
        .filter_map(|(node_id, blob)| {
            let row = eg_types::msgpack::decode_property_object(&blob).ok()?;
            due_retirement(tenant, graph, &node_id, &row, policy, now_ms)
        })
        .collect()
}

/// Retire every expired sealed record in every resident graph, through the owning
/// op. Returns how many were retired; a failed retirement is logged and retried on
/// the next sweep.
pub async fn expire_sealed_records(
    state: &Arc<RwLock<ServerState>>,
    policy: &[Retention],
    now_ms: u64,
) -> u64 {
    if policy.is_empty() {
        return 0;
    }
    let (graphs, persistence) = {
        let state = state.read().await;
        let graphs: Vec<(String, Arc<GraphCore>)> = state
            .registry
            .all_entries()
            .iter()
            .map(|entry| (entry.name.clone(), entry.core.clone()))
            .collect();
        (graphs, state.persistence.clone())
    };
    let tenant = std::env::var("EPISTEMIC_GRAPH_TENANT").ok();
    let mut retired = 0;
    for (graph, core) in graphs {
        let tenant = tenant.as_deref().unwrap_or(&graph);
        for request in due_in_graph(tenant, &graph, &core, policy, now_ms) {
            retired += u64::from(retire(persistence.as_ref(), &core, &graph, request).await);
        }
    }
    retired
}

/// Commit one policy retirement through the native WorkItem kernel.
async fn retire(
    persistence: Option<&Arc<dyn crate::server::persistence::PersistenceBackend>>,
    core: &Arc<GraphCore>,
    graph: &str,
    request: RetireSealedRecordRequest,
) -> bool {
    let key = request.idempotency_key.clone();
    let node_id = request.node_id.clone();
    let commit = crate::server::mutation_batch::commit_work_item(
        crate::server::mutation_batch::WorkItemCommitRequest::new(
            persistence,
            core,
            crate::server::mutation_batch::CommitOrigin {
                request_id: 0,
                principal: Some(RETENTION_ACTOR),
            },
            Some(&key),
            graph,
            0,
            Method::RetireSealedRecord { request },
        ),
    )
    .await;
    match commit {
        Ok(_) => true,
        Err(error) => {
            tracing::warn!(%graph, %node_id, %error, "sealed record expiry failed; retrying next sweep");
            false
        }
    }
}

#[cfg(test)]
mod tests;
