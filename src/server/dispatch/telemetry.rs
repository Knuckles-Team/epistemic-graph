//! `Method::TelemetryDerive` (EH-408 / EH-409): stored telemetry becomes
//! ontology-bound facts in the request graph.
//!
//! One served derivation, end to end:
//!
//! 1. [`collect`] reads the caller's telemetry from the in-process
//!    observability store (`ServerState::obs`) over `[from_ms, to_ms)`:
//!    log records from the named streams, each of which must lie in the
//!    caller's tenant namespace (`<tenant>` or `<tenant>/…`; any other stream
//!    refuses the whole request), plus the spans and metric samples that carry
//!    the caller's tenant under [`collect::TENANT_ATTRIBUTE`]. Telemetry with
//!    no tenant marker belongs to no caller and is never read.
//! 2. [`declarations`] reads the request graph's individuals whose class the
//!    ontology subsumes under Server / Service / Host / Workload / Agent
//!    ([`classes`]) through the verified caller's row-level read authority:
//!    their declared resolution keys and declared health.
//! 3. The pure engine (`eg_stream::telemetry`) binds, rolls up, runs the
//!    declared CEP patterns and checks conformance.
//! 4. [`materialize`] turns every fact into one upsert of a `BatchUpdate`
//!    that is dispatched through the ordinary graph gateway
//!    (`dispatch_graph_op`) under the same verified context -- so the write
//!    is ACL-checked, durable, audited and CDC-emitted like any other graph
//!    write, and a re-derivation of the same telemetry is an idempotent upsert
//!    of the same fact ids.

mod classes;
mod collect;
mod declarations;
mod materialize;
#[cfg(test)]
mod tests;

use std::sync::Arc;

use eg_stream::telemetry::{derive_facts, fact_graph, TelemetryFacts, TelemetryPolicy};
use eg_types::result_contract::ingestion::TelemetryDerive as TelemetryDeriveResult;
use eg_types::telemetry_derive::TelemetryDeriveReceipt;
use tokio::sync::RwLock;

use super::graph_pipeline::dispatch_graph_op;
use super::timed_read;
use crate::protocol::{Method, Response};
use crate::server::auth::VerifiedRequestContext;
use crate::server::obs::ObsState;
use crate::server::state::ServerState;

use collect::Window;
use materialize::{FactBatch, FactOwner};

/// Bounds on the MessagePack policy a caller sends.
const MAX_POLICY_BYTES: usize = 256 * 1024;
const MAX_POLICY_ITEMS: usize = 16_384;
const MAX_POLICY_DEPTH: usize = 32;

/// The fields of one `Method::TelemetryDerive`.
pub(super) struct DeriveRequest {
    pub(super) from_ms: u64,
    pub(super) to_ms: u64,
    pub(super) streams: Vec<String>,
    pub(super) policy_msgpack: Vec<u8>,
}

/// Who asked, and which graph the facts are written into.
pub(super) struct DeriveTarget<'a> {
    pub(super) graph: &'a str,
    pub(super) req_id: u64,
    pub(super) caller: Option<&'a str>,
    pub(super) verified: &'a VerifiedRequestContext,
}

/// Serve one derivation.
pub(super) async fn handle_telemetry_derive(
    state: &Arc<RwLock<ServerState>>,
    target: DeriveTarget<'_>,
    request: DeriveRequest,
) -> Response {
    let req_id = target.req_id;
    crate::server::dispatch::typed_response::<TelemetryDeriveResult>(
        req_id,
        derive(state, &target, request).await,
    )
}

async fn derive(
    state: &Arc<RwLock<ServerState>>,
    target: &DeriveTarget<'_>,
    request: DeriveRequest,
) -> Result<TelemetryDeriveReceipt, String> {
    let policy = decode_policy(&request.policy_msgpack)?;
    let window = Window::new(request.from_ms, request.to_ms)?;
    let tenant = target.verified.tenant().to_string();
    collect::require_tenant_streams(&tenant, &request.streams)?;
    let obs = observability_store(state).await?;
    let streams = request.streams;
    let signals = tokio::task::spawn_blocking(move || {
        collect::tenant_signals(&obs, &tenant, &window, &streams)
    })
    .await
    .map_err(|_| "telemetry read worker failed".to_string())??;
    let declared = declarations::read(state, target.graph, target.verified).await?;
    let facts = derive_facts(&policy, &declared.declarations, &signals);
    let graph = fact_graph(&facts).map_err(|_| "telemetry fact encoding failed".to_string())?;
    let owner = FactOwner {
        tenant_id: target.verified.tenant(),
        agent_id: target.verified.agent_id(),
    };
    let batch = materialize::fact_batch(&graph, &owner)?;
    write_facts(state, target, &batch).await?;
    Ok(receipt(signals.len(), declared.invalid, &facts, batch))
}

fn decode_policy(policy_msgpack: &[u8]) -> Result<TelemetryPolicy, String> {
    let limits =
        eg_types::msgpack::MsgpackLimits::new(MAX_POLICY_BYTES, MAX_POLICY_ITEMS, MAX_POLICY_DEPTH);
    eg_types::msgpack::decode_bounded(policy_msgpack, limits).map_err(|_| {
        "INVALID_ARGUMENT: policy_msgpack is not a bounded TelemetryPolicy".to_string()
    })
}

/// The in-process observability store, or a typed refusal when this engine
/// was started without one.
async fn observability_store(state: &Arc<RwLock<ServerState>>) -> Result<Arc<ObsState>, String> {
    timed_read(state).await.obs.clone().ok_or_else(|| {
        "TELEMETRY_UNAVAILABLE: this engine has no observability store configured".to_string()
    })
}

/// Commit the batch as ONE graph operation (the gateway's replay protection
/// is keyed by the request, so a derivation never issues a second write).
async fn write_facts(
    state: &Arc<RwLock<ServerState>>,
    target: &DeriveTarget<'_>,
    batch: &FactBatch,
) -> Result<(), String> {
    if batch.is_empty() {
        return Ok(());
    }
    let method = Method::BatchUpdate {
        operations_msgpack: batch.operations_msgpack.clone(),
    };
    let response = dispatch_graph_op(
        state,
        target.graph,
        target.req_id,
        target.caller,
        target.verified,
        method,
    )
    .await;
    match response.error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn receipt(
    signals_read: usize,
    invalid_declarations: usize,
    facts: &TelemetryFacts,
    batch: FactBatch,
) -> TelemetryDeriveReceipt {
    let count = |n: usize| n as u64;
    TelemetryDeriveReceipt {
        signals_read: count(signals_read),
        observations: count(facts.observations.len()),
        anomalies: count(facts.anomalies.len()),
        incidents: count(facts.incidents.len()),
        violations: count(facts.violations.len()),
        unresolved: count(facts.unresolved.len()),
        ignored_metric_samples: facts.ignored_metric_samples,
        invalid_declarations: count(invalid_declarations),
        nodes_written: count(batch.fact_ids.len()),
        edges_written: count(batch.edges),
        fact_ids: batch.fact_ids,
    }
}
