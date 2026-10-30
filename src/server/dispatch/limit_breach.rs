//! Limit-breach facts on the decision commit path (EG-DECISION-ENGINE-R131.5).
//!
//! A committed decision whose sealed action breached a limit is a fact: one
//! `LimitBreachObservation` node per breach, and an `observed_on` edge to the
//! signal node, written as ONE `BatchUpdate` into the graph the action names.
//! Node ids are functions of the record and the rule, so a replayed commit
//! upserts the same rows.

use super::graph_pipeline::dispatch_graph_op;
use super::*;
use eg_types::decision::limit_guard::LimitBreach;
use serde_json::{json, Map, Value};

/// Node id of one breach observation.
pub(crate) fn observation_id(record_id: &str, rule_id: &str) -> String {
    format!("limit_breach:{record_id}:{rule_id}")
}

/// The `BatchUpdate` operations for `breaches`: nodes first, then edges.
pub(crate) fn operations(
    record_id: &str,
    breaches: &[LimitBreach],
    tenant_id: &str,
    agent_id: &str,
) -> Vec<Value> {
    let stamp = |properties: &mut Map<String, Value>| {
        properties.insert("tenant_id".into(), tenant_id.into());
        properties.insert("_owner".into(), agent_id.into());
        properties.insert("_visibility".into(), "private".into());
    };
    let mut ops = Vec::with_capacity(breaches.len() * 2);
    for breach in breaches {
        let mut properties = Map::new();
        properties.insert("node_type".into(), "LimitBreachObservation".into());
        properties.insert("record_id".into(), record_id.into());
        properties.insert("rule_id".into(), breach.rule_id.clone().into());
        properties.insert("observed".into(), breach.observed.into());
        properties.insert("bound".into(), breach.bound.into());
        stamp(&mut properties);
        ops.push(json!({
            "op": "upsert_node",
            "id": observation_id(record_id, &breach.rule_id),
            "properties": properties,
        }));
    }
    for breach in breaches {
        let mut properties = Map::from_iter([("relationship".to_string(), "observed_on".into())]);
        stamp(&mut properties);
        ops.push(json!({
            "op": "upsert_edge",
            "source": observation_id(record_id, &breach.rule_id),
            "target": breach.signal_iri,
            "properties": properties,
        }));
    }
    ops
}

/// After a successful `DecisionLog` commit, write the breach facts of its
/// record. Returns the commit response, or the refusal of the graph write.
pub(super) async fn emit_after_commit(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    verified: &VerifiedRequestContext,
    record: &eg_types::decision::statistical::StatisticalDecisionRecord,
    committed: Response,
) -> Response {
    if committed.refusal_text().is_some() {
        return committed;
    }
    let Some(action) = &record.inputs.guarded_action else {
        return committed;
    };
    let breaches = match handlers::decide::stat_limit_guard::breaches_of_commit(
        state,
        verified,
        record,
    )
    .await
    {
        Ok(breaches) => breaches,
        Err(error) => return Response::err(req_id, error),
    };
    if breaches.is_empty() {
        return committed;
    }
    let ops = operations(
        &record.record_id,
        &breaches,
        verified.tenant(),
        verified.agent_id(),
    );
    let operations_msgpack = match rmp_serde::to_vec_named(&ops) {
        Ok(bytes) => bytes,
        Err(_) => return Response::err(req_id, "limit breach fact encoding failed".to_string()),
    };
    let written = dispatch_graph_op(
        state,
        &action.graph,
        req_id,
        caller,
        verified,
        Method::BatchUpdate { operations_msgpack },
    )
    .await;
    match written.refusal_text() {
        Some(refusal) => Response::err(req_id, refusal),
        None => committed,
    }
}
