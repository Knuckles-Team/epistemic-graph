//! X4 — the distributed cross-shard DAG EXCHANGE operator (CONCEPT:EG-KG.query.dag-distributed-exchange).
//!
//! `eg_plan::dag::PlanDag`'s multi-input node is the seam a cross-shard exchange hangs
//! off (see `crates/eg-plan/src/dag.rs`'s "X4" doc section) — a multi-branch plan whose
//! branches execute on DIFFERENT Raft groups, each producing a partial `RowSet`, merged
//! at the join node. This module is that operator, entirely OUTSIDE the dep-free
//! `eg-plan` crate (which gains only a generic, cluster-agnostic hook —
//! [`eg_plan::execute_dag_with`] — no new `Op`/`PlanNode` variant, no raft/openraft
//! dependency).
//!
//! ## The shape
//!
//!  1. **The marker.** [`ExchangeMap`] is an "exchange marker on the DAG" kept OUTSIDE
//!     `PlanDag`/`PlanNode`: it maps a branch's ROOT [`eg_plan::NodeId`] to the graph
//!     name it reads and the [`GroupId`] that owns it (resolved via
//!     [`super::multi::GroupRouter::group_of`] through [`ExchangeMap::route_via_router`]).
//!     A node with no entry — or one that maps to the LOCAL group — runs exactly as
//!     `execute_dag` already does.
//!  2. **The extraction.** [`extract_branch`] pulls a routed root's full ancestor
//!     closure out of the whole dag into a standalone, renumbered `PlanDag` (its
//!     node-id order is [`PlanDag::topo_order`]'s own — deterministic, ties broken
//!     ascending id), so the remote side gets a self-contained sub-plan whose SINK is
//!     exactly the branch root.
//!  3. **The transport.** [`call_remote_branch`] ships that sub-plan to the routed
//!     group's endpoint as ONE length-prefixed-MessagePack request/response — the SAME
//!     framing convention `federation`'s `RemoteEngineSource` and this crate's own
//!     `network::{write_frame, read_frame}` already use — and gets the partial `RowSet`
//!     back (as plain `(id, score)` rows, exactly the shape `RemoteEngineSource::fetch_uql`
//!     projects). [`serve_exchange_conn`] is the responder half: it decodes the
//!     sub-plan, runs it through the UNMODIFIED [`eg_plan::execute_dag`] against
//!     whatever local graph the [`ExchangeGraphResolver`] resolves, and replies with the
//!     rows.
//!  4. **The merge.** [`execute_dag_distributed`] runs the WHOLE original dag through
//!     [`eg_plan::execute_dag_with`], overriding exactly the routed branch roots (and
//!     skipping — as cheap no-ops — their local ancestors, since the whole subtree
//!     shipped as one remote unit) with the fetched `RowSet`. The join node's own
//!     `join_inputs` call is the UNTOUCHED `RowSet::intersect_keep_order` chain E5
//!     already proved deterministic and order-preserving for an all-local dag — reused,
//!     not reimplemented, so a plan whose branches all happen to route to the local
//!     group is byte-identical to plain `execute_dag`.
//!
//! ## What is NOT built here (honest scope, matching `cross_shard_txn`'s own posture)
//!
//! Like the cross-shard 2PC coordinator's documented "cross-NODE participant apply path"
//! remainder, [`ExchangeGraphResolver`] is a SEAM, not a production wiring: implementing
//! it against the real `ServerState`/`MultiRaft` (so a live cluster actually serves
//! `serve_exchange_conn` on a listener alongside the raft/2PC ports) is deployment work
//! left as a follow-up. What IS proven here — by `tests` below, over REAL loopback TCP
//! sockets, exactly the `xshard_modality_harness` in-process-multi-group convention —
//! is that the routing, extraction, wire round-trip and merge are correct and
//! deterministic end-to-end.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use eg_plan::dag::NodeId as DagNodeId;
use eg_plan::{execute_dag, execute_dag_with, PlanCtx, PlanDag, RowSet};

use super::multi::GroupRouter;
use super::GroupId;

// ── 1. the exchange marker (kept OUTSIDE PlanDag/PlanNode) ─────────────────────────

/// Routes SOME branch-root node ids of a [`PlanDag`] to the [`GroupId`] (+ graph name)
/// that should execute them (CONCEPT:EG-KG.query.dag-distributed-exchange). A node absent from
/// the map runs LOCALLY, exactly like plain `execute_dag` — so an empty `ExchangeMap`
/// makes [`execute_dag_distributed`] behave byte-for-byte like `execute_dag`.
#[derive(Default, Clone, Debug)]
pub struct ExchangeMap {
    targets: BTreeMap<DagNodeId, (String, GroupId)>,
}

impl ExchangeMap {
    pub fn new() -> Self {
        Self::default()
    }

    /// Route the branch rooted at `node` to `group`, reading graph `graph` there.
    pub fn route(&mut self, node: DagNodeId, graph: impl Into<String>, group: GroupId) -> &mut Self {
        self.targets.insert(node, (graph.into(), group));
        self
    }

    /// Route the branch rooted at `node` (which reads `graph`) to whatever
    /// [`GroupRouter::group_of`] resolves `graph` to — the intended entry point: the
    /// caller supplies the branch's source graph name (e.g. from its leaf `Op::Scan`'s
    /// label / a UQL `GRAPH` clause), the router does the shard resolution.
    pub fn route_via_router(
        &mut self,
        node: DagNodeId,
        graph: impl Into<String>,
        router: &GroupRouter,
    ) -> &mut Self {
        let graph = graph.into();
        let group = router.group_of(&graph);
        self.targets.insert(node, (graph, group));
        self
    }

    pub fn target_of(&self, node: DagNodeId) -> Option<&(String, GroupId)> {
        self.targets.get(&node)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&DagNodeId, &(String, GroupId))> {
        self.targets.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.targets.is_empty()
    }

    pub fn len(&self) -> usize {
        self.targets.len()
    }
}

// ── 2. branch subtree extraction ────────────────────────────────────────────────────

/// Every node reachable from `root` via `.inputs`, transitively, EXCLUDING `root`
/// itself — `root`'s ancestor closure.
fn ancestor_closure(dag: &PlanDag, root: DagNodeId) -> BTreeSet<DagNodeId> {
    let mut seen = BTreeSet::new();
    let mut stack = vec![root];
    while let Some(id) = stack.pop() {
        for &input in &dag.nodes[id].inputs {
            if seen.insert(input) {
                stack.push(input);
            }
        }
    }
    seen
}

/// Extract the subtree rooted at `root` (root + its full ancestor closure) into a
/// standalone, renumbered [`PlanDag`] whose SINK is `root` — the self-contained
/// sub-plan shipped to a remote group (CONCEPT:EG-KG.query.dag-distributed-exchange). Node ids are
/// reassigned along [`PlanDag::topo_order`]'s own deterministic order (ties ascending
/// by original id) restricted to the closure, so extraction is REPRODUCIBLE — the same
/// branch always serializes to the same bytes, which is what keeps the distributed
/// execution's output order-preserving/deterministic (mirroring the ANN
/// `merge_topk_stable` discipline: a stable, order-preserving merge over independently
/// computed shards).
pub fn extract_branch(dag: &PlanDag, root: DagNodeId) -> Result<PlanDag, String> {
    if root >= dag.nodes.len() {
        return Err(format!(
            "exchange: extract_branch: node {root} is out of range (dag has {} nodes)",
            dag.nodes.len()
        ));
    }
    let mut closure = ancestor_closure(dag, root);
    closure.insert(root);

    let full_order = dag.topo_order()?;
    let order: Vec<DagNodeId> = full_order
        .into_iter()
        .filter(|id| closure.contains(id))
        .collect();
    let remap: BTreeMap<DagNodeId, DagNodeId> = order
        .iter()
        .enumerate()
        .map(|(new_id, &old_id)| (old_id, new_id))
        .collect();

    let nodes = order
        .iter()
        .map(|&old_id| {
            let mut node = dag.nodes[old_id].clone();
            node.inputs = node.inputs.iter().map(|i| remap[i]).collect();
            node
        })
        .collect();
    Ok(PlanDag::new(nodes))
}

// ── 3. wire transport (length-prefixed MessagePack, `network`'s framing convention) ──

/// One exchange request: run `dag` (a branch extracted by [`extract_branch`]) against
/// the local state for `graph` on the receiving node.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct ExchangeRequest {
    graph: String,
    dag: PlanDag,
}

/// The reply: the branch's rows (id + optional score, in order) — exactly the
/// `(String, Option<f32>)` shape `federation::RemoteEngineSource::fetch_uql` projects
/// remote rows into, and what [`RowSet::from_rows`] rebuilds locally — or the remote's
/// error message.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct ExchangeReply(Result<Vec<(String, Option<f32>)>, String>);

/// 4-byte big-endian length prefix + a MessagePack body — IDENTICAL framing to
/// `super::network::{write_frame, read_frame}` and `eg_plan::federation`'s
/// `RemoteEngineSource::round_trip`; reimplemented at this call site only because it
/// runs over a BLOCKING `std::net::TcpStream` (the caller is
/// [`execute_dag_distributed`]'s synchronous per-node override closure, matching how
/// `RemoteEngineSource::fetch` is itself sync and run on the executor's blocking pool)
/// rather than `network`'s tokio one.
fn blocking_round_trip(addr: &str, body: &[u8]) -> Result<Vec<u8>, String> {
    let mut stream = TcpStream::connect(addr)
        .map_err(|e| format!("exchange: connect {addr} failed: {e}"))?;
    let len = (body.len() as u32).to_be_bytes();
    stream
        .write_all(&len)
        .and_then(|()| stream.write_all(body))
        .map_err(|e| format!("exchange: write to {addr}: {e}"))?;

    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .map_err(|e| format!("exchange: read len from {addr}: {e}"))?;
    let resp_len = u32::from_be_bytes(len_buf) as usize;
    let mut resp_buf = vec![0u8; resp_len];
    stream
        .read_exact(&mut resp_buf)
        .map_err(|e| format!("exchange: read body from {addr}: {e}"))?;
    Ok(resp_buf)
}

/// Ship `dag` (already extracted via [`extract_branch`]) to `addr` and return the
/// partial `RowSet` it computed over `graph`. Blocking — call it from the executor's
/// blocking pool exactly like a federation `ForeignSource::fetch`.
pub fn call_remote_branch(addr: &str, graph: &str, dag: &PlanDag) -> Result<RowSet, String> {
    let req = ExchangeRequest {
        graph: graph.to_string(),
        dag: dag.clone(),
    };
    let body =
        rmp_serde::to_vec_named(&req).map_err(|e| format!("exchange: encode request: {e}"))?;
    let resp_bytes = blocking_round_trip(addr, &body)?;
    let ExchangeReply(result) = rmp_serde::from_slice(&resp_bytes)
        .map_err(|e| format!("exchange: decode reply: {e}"))?;
    let rows = result.map_err(|e| format!("exchange: remote error: {e}"))?;
    Ok(RowSet::from_rows(rows))
}

/// Resolves a graph name to the ingredients an exchange RESPONDER needs to run a
/// received branch locally (CONCEPT:EG-KG.query.dag-distributed-exchange) — mirrors the federation
/// `ForeignSource` seam: one trait, implemented ONCE against the real deployment
/// (`ServerState`/`MultiRaft` — the documented follow-up, see module docs) and by a
/// fixture in tests.
pub trait ExchangeGraphResolver: Send + Sync {
    /// Run `dag` (a self-contained branch — its own sink is the value to return) over
    /// the CURRENT local snapshot for `graph`, returning its rows. Uses the UNMODIFIED
    /// [`eg_plan::execute_dag`] — the responder does not reinterpret the branch, it
    /// just runs it exactly like any other dag.
    fn execute_branch(&self, graph: &str, dag: &PlanDag) -> Result<RowSet, String>;
}

/// An [`ExchangeGraphResolver`] over one fixed `(GraphView, SemanticStore)` pair —
/// realistic when a node hosts exactly one graph per group (today's default `raft`
/// routing: [`GroupRouter`] maps one graph per group), and the shape the tests below
/// use to stand in for a real per-group `ServerState` lookup.
pub struct FixedGraphResolver<'a> {
    pub graph: String,
    pub ctx: PlanCtx<'a>,
}

impl ExchangeGraphResolver for FixedGraphResolver<'_> {
    fn execute_branch(&self, graph: &str, dag: &PlanDag) -> Result<RowSet, String> {
        if graph != self.graph {
            return Err(format!(
                "exchange: this responder only serves graph '{}', not '{graph}'",
                self.graph
            ));
        }
        execute_dag(dag, &self.ctx)
    }
}

/// Serve ONE exchange round-trip on `stream`: read a length-prefixed request, decode
/// it, run it through `resolver`, write back a length-prefixed reply. Reuses
/// [`super::network::write_frame`]/[`super::network::read_frame`] verbatim — the exact
/// async tokio framing helpers the Raft RPC listener already uses — rather than
/// reimplementing the framing on the async side (the blocking client above is the one
/// side that genuinely needs its own small blocking version, see
/// [`blocking_round_trip`]'s doc comment).
pub async fn serve_exchange_conn(
    stream: &mut tokio::net::TcpStream,
    resolver: &(dyn ExchangeGraphResolver + Sync),
) -> std::io::Result<()> {
    let body = super::network::read_frame(stream).await?;
    let reply = match rmp_serde::from_slice::<ExchangeRequest>(&body) {
        Ok(req) => match resolver.execute_branch(&req.graph, &req.dag) {
            Ok(rows) => ExchangeReply(Ok(rows
                .rows()
                .iter()
                .map(|r| (r.id.clone(), r.score))
                .collect())),
            Err(e) => ExchangeReply(Err(e)),
        },
        Err(e) => ExchangeReply(Err(format!("exchange: decode request: {e}"))),
    };
    let out = rmp_serde::to_vec_named(&reply)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    super::network::write_frame(stream, &out).await
}

/// Loop [`serve_exchange_conn`] over `stream` until the peer closes it — one connection,
/// many sequential round-trips (matching the Raft multi-listener's `serve_conn` shape).
pub async fn serve_exchange_loop(
    mut stream: tokio::net::TcpStream,
    resolver: Arc<dyn ExchangeGraphResolver + Send + Sync>,
) {
    loop {
        match serve_exchange_conn(&mut stream, resolver.as_ref()).await {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return,
            Err(e) => {
                tracing::debug!("exchange: connection ended: {e}");
                return;
            }
        }
    }
}

// ── 4. the distributed executor: same dag_exec join, remote-fed branches ───────────

/// Execute `dag` DISTRIBUTED across Raft groups (CONCEPT:EG-KG.query.dag-distributed-exchange, X4): every
/// branch root `routes` maps to a group OTHER than `local_group` is shipped to that
/// group's `endpoints` address (via [`extract_branch`] + [`call_remote_branch`]) instead
/// of running through `ctx` locally; its ancestors (already folded into the shipped
/// subtree) are skipped locally as cheap no-ops. Every other node — including the join
/// node(s) that consume the routed branches — runs through the UNMODIFIED
/// [`eg_plan::execute_dag_with`]/`join_inputs` path, so the merge is EXACTLY the
/// `RowSet::intersect_keep_order` chain a single-node multi-branch dag already uses.
///
/// An empty `routes` (or one that only maps to `local_group`) makes this byte-identical
/// to plain `execute_dag(dag, ctx)` — the zero-distribution case.
pub fn execute_dag_distributed(
    dag: &PlanDag,
    ctx: &PlanCtx<'_>,
    routes: &ExchangeMap,
    endpoints: &BTreeMap<GroupId, String>,
    local_group: GroupId,
) -> Result<RowSet, String> {
    // Precompute which nodes are (a) a REMOTE branch root — override with the fetched
    // RowSet — or (b) folded into one of those branches' ancestor closures — override
    // with an empty placeholder, since the whole subtree already shipped as one unit
    // and nothing downstream reads its output directly (only the root is listed in the
    // join node's `inputs`).
    let mut remote_root: BTreeMap<DagNodeId, (String, GroupId)> = BTreeMap::new();
    let mut skip_local: BTreeSet<DagNodeId> = BTreeSet::new();
    for (&root, (graph, group)) in routes.iter() {
        if *group == local_group {
            continue;
        }
        remote_root.insert(root, (graph.clone(), *group));
        skip_local.extend(ancestor_closure(dag, root));
    }
    // A node cannot be BOTH a remote root and a skipped ancestor of another remote
    // root — that would mean two routed branches share ancestry, which breaks the
    // "ship the whole private subtree" assumption. Caught here as a clear config
    // error rather than a silently wrong merge.
    for root in remote_root.keys() {
        if skip_local.contains(root) {
            return Err(format!(
                "exchange: node {root} is routed to a remote group but is ALSO an \
                 ancestor of another routed branch — routed branches must not share \
                 ancestry (CONCEPT:EG-KG.query.dag-distributed-exchange)"
            ));
        }
    }

    execute_dag_with(dag, ctx, |id, _node, _joined_input| {
        if let Some((graph, group)) = remote_root.get(&id) {
            let addr = endpoints.get(group).ok_or_else(|| {
                format!("exchange: no endpoint configured for group {group}")
            })?;
            let branch = extract_branch(dag, id)?;
            let rows = call_remote_branch(addr, graph, &branch)?;
            return Ok(Some(rows));
        }
        if skip_local.contains(&id) {
            return Ok(Some(RowSet::new()));
        }
        Ok(None)
    })
}

#[cfg(test)]
mod tests {
    //! Proves the X4 shape end-to-end over REAL loopback TCP sockets — the same
    //! in-process-multi-"node" convention `xshard_modality_harness` uses for its
    //! cross-group proofs (a real socket round-trip, one process, standing in for a
    //! separate physical node per CONCEPT:EG-KG.txn.harness-crash's documented
    //! cross-NODE-participant remainder).
    use super::*;
    use eg_core::compute::semantic::SemanticStore;
    use eg_core::graph::GraphCore;
    use eg_plan::{Op, PlanNode};
    use serde_json::json;

    fn blob(v: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&v).unwrap()
    }

    /// Group A's graph: two `Doc` nodes `a`,`b` plus a `Tool` node `c` — the SAME
    /// fixture shape `eg_plan::dag_exec`'s own tests use, so results are directly
    /// comparable to a plain local `execute_dag` baseline.
    fn fixture_a() -> (GraphCore, SemanticStore) {
        let core = GraphCore::new();
        for (id, ty) in [("a", "Doc"), ("b", "Doc"), ("c", "Tool")] {
            core.add_node(id.into(), blob(json!({ "type": ty })));
        }
        (core, SemanticStore::new())
    }

    /// Group B's graph: a DIFFERENT dataset (`Tool` nodes `b`,`d`) on its OWN shard —
    /// proving the remote branch genuinely reads DIFFERENT data than the coordinator's
    /// local graph, not an accidental local fallback.
    fn fixture_b() -> (GraphCore, SemanticStore) {
        let core = GraphCore::new();
        for (id, ty) in [("b", "Tool"), ("d", "Tool")] {
            core.add_node(id.into(), blob(json!({ "type": ty })));
        }
        (core, SemanticStore::new())
    }

    /// Stand a real tokio TCP listener up serving `resolver` over `serve_exchange_loop`,
    /// return its `127.0.0.1:port` address. Dropped (and the task aborted) at the end
    /// of the test's runtime.
    async fn spawn_responder(
        graph: &str,
        core: &GraphCore,
        semantic: SemanticStore,
    ) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let view = core.analysis_snapshot();
        // `PlanCtx` borrows the view/semantic; leak them for the life of the test
        // process so the spawned task can hold a `'static` resolver — acceptable in a
        // short-lived test binary, mirroring how other in-process harnesses here keep
        // fixtures alive for the test's duration.
        let view: &'static eg_core::graph::GraphView = Box::leak(Box::new(view));
        let semantic: &'static SemanticStore = Box::leak(Box::new(semantic));
        let resolver: Arc<dyn ExchangeGraphResolver + Send + Sync> = Arc::new(FixedGraphResolver {
            graph: graph.to_string(),
            ctx: PlanCtx::new(view, semantic),
        });
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let resolver = resolver.clone();
                tokio::spawn(serve_exchange_loop(stream, resolver));
            }
        });
        addr
    }

    /// A two-branch dag — `Scan("Doc")` (routed to a REMOTE group serving `fixture_a`)
    /// joined with `Scan("Tool")` (routed to a SECOND remote group serving
    /// `fixture_b`) — proves genuinely DISTRIBUTED execution: neither branch's data
    /// lives on the coordinator's own (empty) local graph, so a correct non-empty
    /// merged result can ONLY come from both remote round-trips actually running.
    #[tokio::test]
    async fn multi_branch_plan_executes_each_branch_on_its_owning_group_and_merges() {
        let (core_a, sem_a) = fixture_a();
        let (core_b, sem_b) = fixture_b();
        let addr_a = spawn_responder("shardA", &core_a, sem_a).await;
        let addr_b = spawn_responder("shardB", &core_b, sem_b).await;

        // The coordinator's OWN local graph is intentionally EMPTY — if the merge
        // silently fell back to local execution instead of the remote branches, the
        // result would be empty too, so a non-empty `{b}` is proof the round trips ran.
        let empty_core = GraphCore::new();
        let empty_view = empty_core.analysis_snapshot();
        let empty_semantic = SemanticStore::new();
        let ctx = PlanCtx::new(&empty_view, &empty_semantic);

        let dag = PlanDag::new(vec![
            PlanNode::new(
                Op::Scan {
                    label: "Doc".into(),
                },
                vec![],
            ),
            PlanNode::new(
                Op::Scan {
                    label: "Tool".into(),
                },
                vec![],
            ),
            // The join: Doc{a,b} (group A) ∩ Tool{b,d} (group B) = {b}.
            PlanNode::new(Op::Limit { k: 10 }, vec![0, 1]),
        ]);

        let mut routes = ExchangeMap::new();
        routes.route(0, "shardA", 1);
        routes.route(1, "shardB", 2);
        let endpoints: BTreeMap<GroupId, String> =
            [(1, addr_a.clone()), (2, addr_b.clone())].into();

        let out = execute_dag_distributed(&dag, &ctx, &routes, &endpoints, 0 /* local_group */)
            .unwrap();
        assert_eq!(out.ids(), vec!["b".to_string()]);
    }

    /// Determinism: running the SAME distributed plan twice yields the SAME rows in
    /// the SAME order — the scatter-gather determinism X4 must preserve (mirroring the
    /// ANN `merge_topk_stable` discipline for a ranked shard-merge; here it is the
    /// `RowSet::intersect_keep_order` chain's own order-preservation, unmodified by
    /// distribution).
    #[tokio::test]
    async fn distributed_execution_is_deterministic_across_repeated_runs() {
        let (core_a, sem_a) = fixture_a();
        let addr_a = spawn_responder("shardA", &core_a, sem_a).await;

        let empty_core = GraphCore::new();
        let empty_view = empty_core.analysis_snapshot();
        let empty_semantic = SemanticStore::new();
        let ctx = PlanCtx::new(&empty_view, &empty_semantic);

        // A single remote branch feeding straight into Limit — its own sink.
        let dag = PlanDag::new(vec![
            PlanNode::new(
                Op::Scan {
                    label: "Doc".into(),
                },
                vec![],
            ),
            PlanNode::new(Op::Limit { k: 10 }, vec![0]),
        ]);
        let mut routes = ExchangeMap::new();
        routes.route(0, "shardA", 1);
        let endpoints: BTreeMap<GroupId, String> = [(1, addr_a.clone())].into();

        let first = execute_dag_distributed(&dag, &ctx, &routes, &endpoints, 0).unwrap();
        let second = execute_dag_distributed(&dag, &ctx, &routes, &endpoints, 0).unwrap();
        assert_eq!(first.ids(), second.ids());
        assert_eq!(first.ids(), vec!["a".to_string(), "b".to_string()]);
    }

    /// A route that targets `local_group` is a no-op distribution: the branch runs
    /// LOCALLY (no network hop, no endpoint needed) and the result is byte-identical
    /// to plain `execute_dag` — the zero-behavior-change baseline X4 must preserve
    /// when nothing actually spans shards.
    #[tokio::test]
    async fn route_to_local_group_matches_plain_execute_dag() {
        let (core, semantic) = fixture_a();
        let view = core.analysis_snapshot();
        let ctx = PlanCtx::new(&view, &semantic);

        let dag = PlanDag::new(vec![
            PlanNode::new(
                Op::Scan {
                    label: "Doc".into(),
                },
                vec![],
            ),
            PlanNode::new(Op::Limit { k: 10 }, vec![0]),
        ]);

        let mut routes = ExchangeMap::new();
        routes.route(0, "local-graph", 0 /* == local_group below */);
        let endpoints: BTreeMap<GroupId, String> = BTreeMap::new();

        let via_distributed =
            execute_dag_distributed(&dag, &ctx, &routes, &endpoints, 0).unwrap();
        let via_plain = execute_dag(&dag, &ctx).unwrap();
        assert_eq!(via_distributed.ids(), via_plain.ids());
    }

    /// [`extract_branch`] pulls out exactly `root` + its ancestors, renumbered with
    /// `root` as the new sink — proved directly (not just through the end-to-end
    /// round trip above).
    #[test]
    fn extract_branch_yields_a_self_contained_renumbered_subplan() {
        let dag = PlanDag::new(vec![
            PlanNode::new(
                Op::Scan {
                    label: "Doc".into(),
                },
                vec![],
            ),
            PlanNode::new(Op::Limit { k: 5 }, vec![0]),
            // An unrelated sibling branch — must NOT be pulled in.
            PlanNode::new(
                Op::Scan {
                    label: "Tool".into(),
                },
                vec![],
            ),
            PlanNode::new(Op::Limit { k: 10 }, vec![1, 2]),
        ]);
        let branch = extract_branch(&dag, 1).unwrap();
        assert_eq!(branch.len(), 2, "only node 1 + its ancestor (node 0)");
        assert_eq!(branch.sink(), Ok(1));
        assert_eq!(branch.nodes[0].inputs, Vec::<DagNodeId>::new());
        assert_eq!(branch.nodes[1].inputs, vec![0]);
    }

    /// Two routed branches that share an ancestor is a config error the coordinator
    /// must refuse loudly rather than silently merge a wrong result.
    #[tokio::test]
    async fn overlapping_routed_branches_is_a_clean_error() {
        let dag = PlanDag::new(vec![
            PlanNode::new(
                Op::Scan {
                    label: "Doc".into(),
                },
                vec![],
            ),
            // node 1 depends on node 0 too — routing BOTH 0 and 1 remotely means node
            // 0 is simultaneously "a remote root" and "an ancestor of remote root 1".
            PlanNode::new(Op::Limit { k: 5 }, vec![0]),
            PlanNode::new(Op::Limit { k: 10 }, vec![0, 1]),
        ]);
        let empty_core = GraphCore::new();
        let empty_view = empty_core.analysis_snapshot();
        let empty_semantic = SemanticStore::new();
        let ctx = PlanCtx::new(&empty_view, &empty_semantic);

        let mut routes = ExchangeMap::new();
        routes.route(0, "shardA", 1);
        routes.route(1, "shardA", 1);
        let endpoints: BTreeMap<GroupId, String> = BTreeMap::new();

        let err = execute_dag_distributed(&dag, &ctx, &routes, &endpoints, 0).unwrap_err();
        assert!(err.contains("share ancestry"), "got: {err}");
    }
}
