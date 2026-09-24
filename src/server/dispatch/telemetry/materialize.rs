//! Derived facts as ONE `BatchUpdate` (EH-408 / EH-409).
//!
//! Every fact node becomes an `upsert_node` and every fact edge an
//! `upsert_edge`, nodes first so each edge's endpoints exist when it is
//! applied. Fact ids are functions of the telemetry and the policy, so a
//! re-derivation upserts the same rows. Each row is stamped with the deriving
//! tenant and is private to the deriving agent (`_owner` / `_visibility`), the
//! same row-level contract every other engine-written record carries.

use eg_stream::telemetry::FactGraph;
use serde_json::{json, Map, Value};

/// `BatchUpdate`'s own operation bound: a larger derivation is refused, not split.
const MAX_BATCH_OPERATIONS: usize = 50_000;

/// Who the written rows belong to.
pub(super) struct FactOwner<'a> {
    pub(super) tenant_id: &'a str,
    pub(super) agent_id: &'a str,
}

impl FactOwner<'_> {
    fn stamp(&self, properties: &mut Map<String, Value>) {
        properties.insert("tenant_id".into(), self.tenant_id.into());
        properties.insert("_owner".into(), self.agent_id.into());
        properties.insert("_visibility".into(), "private".into());
    }
}

/// The encoded batch and what it writes.
#[derive(Debug)]
pub(super) struct FactBatch {
    pub(super) operations_msgpack: Vec<u8>,
    /// Fact node ids, in write order.
    pub(super) fact_ids: Vec<String>,
    pub(super) edges: usize,
}

impl FactBatch {
    pub(super) fn is_empty(&self) -> bool {
        self.fact_ids.is_empty() && self.edges == 0
    }
}

/// Encode `graph` as one `BatchUpdate` operation list owned by `owner`.
pub(super) fn fact_batch(graph: &FactGraph, owner: &FactOwner<'_>) -> Result<FactBatch, String> {
    if graph.nodes.len() + graph.edges.len() > MAX_BATCH_OPERATIONS {
        return Err(
            "TELEMETRY_WINDOW_TOO_LARGE: the derived facts exceed one BatchUpdate".to_string(),
        );
    }
    let mut operations = Vec::with_capacity(graph.nodes.len() + graph.edges.len());
    for node in &graph.nodes {
        let mut properties = node.properties.clone();
        owner.stamp(&mut properties);
        operations.push(json!({"op": "upsert_node", "id": node.id, "properties": properties}));
    }
    for edge in &graph.edges {
        let mut properties =
            Map::from_iter([("relationship".to_string(), edge.relationship.clone().into())]);
        owner.stamp(&mut properties);
        operations.push(json!({
            "op": "upsert_edge",
            "source": edge.source,
            "target": edge.target,
            "properties": properties,
        }));
    }
    let operations_msgpack = rmp_serde::to_vec_named(&operations)
        .map_err(|_| "telemetry fact batch encoding failed".to_string())?;
    Ok(FactBatch {
        operations_msgpack,
        fact_ids: graph.nodes.iter().map(|node| node.id.clone()).collect(),
        edges: graph.edges.len(),
    })
}
