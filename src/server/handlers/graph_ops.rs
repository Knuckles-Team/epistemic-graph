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
pub(crate) use gateway::commit_gateway;
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
