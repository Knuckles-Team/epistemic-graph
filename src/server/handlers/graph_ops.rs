//! Graph-targeted operation handlers (node/edge CRUD, embeddings + semantic
//! search, topology/centrality/community algorithms, lifecycle/decay, ledger,
//! reasoning, and cross-graph fork/diff/subgraph-match). These borrow the graph
//! `core` (and, for cross-graph ops, the registry via `state`); heavy reads run
//! off-lock. The dispatch shell owns the cross-cutting write side-effects
//! (dirty/WAL/gauge) — handlers here only produce the `Response`.

use std::ops::ControlFlow;
use std::sync::Arc;
use tokio::sync::RwLock;

use super::super::access::{check_graph_access, requires_write, GraphReadAuthority};
use super::super::compute::{compute_off_lock, weight_semantic_results};
use super::super::mutation::{self, GatewayAuthzCtx, MutationCtx, MutationPlan};
use super::super::persistence::PersistenceBackend;
use super::super::state::{max_response_edges, max_response_nodes, ServerState, MAX_BATCH_IDS};
use crate::graph::GraphCore;
use crate::isolation::AccessLevel;
use crate::protocol::{LedgerReadResult, Method, Response, ResultPayload};

mod algorithms;
mod broker;
mod edges;
mod gateway;
#[cfg(feature = "broker")]
mod gateway_broker;
mod gateway_graph;
#[cfg(feature = "mining")]
mod gateway_mining;
#[cfg(feature = "mining")]
mod gateway_mining_derived;
#[cfg(any(feature = "graphlearn", feature = "ml-pipeline"))]
mod gateway_mining_ml;
mod hierarchy;
mod memory;
mod nodes;
mod semantic;
mod subgraph;
mod terminal;
mod union;

pub(crate) use gateway::try_handle_gateway;
pub(crate) use terminal::try_handle;

/// Every edge's endpoints paired with its decoded property blob (`Null` for
/// an undecodable one). The subgraph and cluster-hierarchy wire projections
/// both start by walking `GraphView::edge_properties` and decoding each blob
/// exactly this way — one walk so a fix to how a bad blob decodes can't land
/// on one wire shape and not the other.
fn decoded_edge_properties(
    sub: &crate::graph::GraphView,
) -> Vec<(String, String, serde_json::Value)> {
    let mut decoded = Vec::new();
    for ((src, tgt), blobs) in &sub.edge_properties {
        for blob in blobs {
            let props =
                eg_types::msgpack::decode_property_value(blob).unwrap_or(serde_json::Value::Null);
            decoded.push((src.clone(), tgt.clone(), props));
        }
    }
    decoded
}
