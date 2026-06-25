//! Read-only query handler. Owns BOTH query methods (one module per domain, per
//! the dispatch conventions — `Sql` + `CypherQuery` are the one `// ── Query ──`
//! protocol section):
//!   * `Method::Sql` (CONCEPT:KG-2.178, feature `query`) — `SELECT … FROM nodes …`
//!     over ONE graph via DataFusion (eg-query::exec_sql).
//!   * `Method::CypherQuery` (CONCEPT:KG-2.179, feature `cypher`) — `MATCH … RETURN
//!     …` over ONE graph, DEP-FREE (eg-query::exec_cypher; label index / VF2 / BFS,
//!     no DataFusion). This is the lean-Pi query path.
//!
//! Both are read-only — they cannot mutate — so the centralized write side-effects
//! (dirty/WAL/gauge) in the dispatch shell never fire for them.
//!
//! Off-lock execution: take the owned `analysis_snapshot()` (a GraphView that
//! shares property bytes by Arc) under a brief read lock, then run on the blocking
//! pool via `compute_off_lock` — the VF2/algorithm idiom, so no query work runs on
//! a reactor worker or under the graph lock.
//!
//! Each arm is gated on ITS feature and returns `Err(method)` when its feature is
//! off, so a method whose feature is absent falls through to the graph_ops
//! "not available in this build" catch-all (never a panic, never a mis-route).

#![allow(clippy::result_large_err)]

use std::sync::Arc;

use super::super::compute::compute_off_lock;
use crate::graph::GraphCore;
use crate::protocol::Method;
#[cfg(any(feature = "query", feature = "cypher"))]
use crate::protocol::{Response, ResultPayload};

/// Handle `Method::Sql` / `Method::CypherQuery`. `Err(method)` hands a non-query
/// method (or a query method whose feature is off) back to the dispatcher
/// (routing fall-through). (CONCEPT:KG-2.19 — server dispatch convention)
pub(crate) async fn try_handle(
    req_id: u64,
    core: Arc<GraphCore>,
    method: Method,
    #[cfg(feature = "security")] caller: Option<&str>,
    #[cfg(feature = "security")] rls: &Arc<crate::isolation::IsolationLayer>,
) -> Result<Response, Method> {
    match method {
        #[cfg(feature = "query")]
        Method::Sql { query, .. } => {
            // Owned, off-lock snapshot (shares property bytes by Arc).
            let snap = rls_snapshot(
                &core,
                #[cfg(feature = "security")]
                caller,
                #[cfg(feature = "security")]
                rls,
            );
            let resp =
                match compute_off_lock(req_id, move || eg_query::exec_sql(&snap, &query)).await {
                    Ok(Ok(result)) => Response::ok(req_id, ResultPayload::raw(&result)),
                    Ok(Err(msg)) => Response::err(req_id, format!("SQL error: {msg}")),
                    Err(resp) => resp,
                };
            Ok(resp)
        }
        #[cfg(feature = "query")]
        Method::UnifiedQuery {
            plan,
            reorder_filter_selectivity,
        } => {
            // ONE cross-modal plan (CONCEPT:KG-2.208/209): filter (DataFusion) →
            // traverse (BFS) → rank (kNN) over ONE consistent off-lock snapshot. Take
            // BOTH the GraphView (topology + property blobs) and a SemanticStore clone
            // under a brief read each — same point-in-time, so the cross-modal read is
            // snapshot-isolated — then run the whole pipeline on the blocking pool.
            let snap = rls_snapshot(
                &core,
                #[cfg(feature = "security")]
                caller,
                #[cfg(feature = "security")]
                rls,
            );
            let semantic = core.semantic_store.read().clone();
            let resp = match compute_off_lock(req_id, move || {
                run_unified(plan, reorder_filter_selectivity, &snap, &semantic)
            })
            .await
            {
                Ok(Ok(rows)) => Response::ok(req_id, ResultPayload::raw(&rows)),
                Ok(Err(msg)) => Response::err(req_id, format!("UnifiedQuery error: {msg}")),
                Err(resp) => resp,
            };
            Ok(resp)
        }
        #[cfg(feature = "query")]
        Method::UnifiedQueryText {
            text,
            reorder_filter_selectivity,
        } => {
            // UQL (CONCEPT:KG-2.214): parse the TEXT query into the SAME `wire::Plan`
            // `UnifiedQuery` carries, then run the IDENTICAL `run_unified` executor —
            // a pure front-end, no new execution path. A parse error is a clear,
            // caret-annotated error Response (never a panic).
            let plan = match eg_plan::uql::parse(&text) {
                Ok(plan) => plan,
                Err(e) => return Ok(Response::err(req_id, e.render(&text))),
            };
            let snap = rls_snapshot(
                &core,
                #[cfg(feature = "security")]
                caller,
                #[cfg(feature = "security")]
                rls,
            );
            let semantic = core.semantic_store.read().clone();
            let resp = match compute_off_lock(req_id, move || {
                run_unified(plan, reorder_filter_selectivity, &snap, &semantic)
            })
            .await
            {
                Ok(Ok(rows)) => Response::ok(req_id, ResultPayload::raw(&rows)),
                Ok(Err(msg)) => Response::err(req_id, format!("UnifiedQuery error: {msg}")),
                Err(resp) => resp,
            };
            Ok(resp)
        }
        #[cfg(feature = "cypher")]
        Method::CypherQuery { query } => {
            // Same off-lock snapshot + blocking-pool idiom as SQL — but DEP-FREE
            // (label index / VF2 / BFS), so it runs in a no-DataFusion Pi build.
            let snap = rls_snapshot(
                &core,
                #[cfg(feature = "security")]
                caller,
                #[cfg(feature = "security")]
                rls,
            );
            let resp = match compute_off_lock(req_id, move || eg_query::exec_cypher(&snap, &query))
                .await
            {
                Ok(Ok(result)) => Response::ok(req_id, ResultPayload::raw(&result)),
                Ok(Err(msg)) => Response::err(req_id, format!("Cypher error: {msg}")),
                Err(resp) => resp,
            };
            Ok(resp)
        }
        other => Err(other),
    }
}

/// Execute a unified cross-modal plan (CONCEPT:KG-2.208/209) over one off-lock
/// snapshot and return the result rows as `[id, score|nil]`. When
/// `reorder_filter_selectivity` is set, the cost model reorders an adjacent
/// (Filter, Rank) pair before execution (CONCEPT:KG-2.209). Synchronous — runs on
/// the blocking pool via `compute_off_lock`, like the SQL/Cypher legs.
#[cfg(feature = "query")]
fn run_unified(
    plan: eg_plan::Plan,
    reorder_filter_selectivity: Option<f64>,
    view: &crate::graph::GraphView,
    semantic: &eg_core::compute::semantic::SemanticStore,
) -> Result<Vec<(String, Option<f32>)>, String> {
    use eg_plan::{CostModel, Op, PlanCtx, Stats};

    // Optional cost-based reorder of the adjacent (Filter, Rank) pair. The final
    // top-k requested by a trailing Limit drives the cost asymmetry; default to the
    // seed size if there is no Limit. Seed/embedding counts come straight from the
    // snapshot, so the decision is fed by derivable stats (CONCEPT:KG-2.209).
    let ops = match reorder_filter_selectivity {
        Some(sel) => {
            let seed_rows = view.node_properties.len();
            let top_k = plan
                .ops
                .iter()
                .rev()
                .find_map(|o| match o {
                    Op::Limit { k } => Some(*k),
                    _ => None,
                })
                .unwrap_or(seed_rows.max(1));
            let stats = Stats::estimate(seed_rows, sel, top_k, semantic.len());
            CostModel::reorder_filter_rank(plan.ops, &stats)
        }
        None => plan.ops,
    };

    // `PlanCtx::new` defaults the (feature-gated) text index to `None`, so a
    // `RankText`/`FuseRrf` op served today degrades to no lexical hits rather than
    // erroring. Threading a live BM25 `TextIndex` into `ServerState` (index-on-write +
    // an `AddText`/`IndexText` Method + a persist dir beside graph.redb) is the
    // explicit follow-up integration (CONCEPT:KG-2.215 increment 2); the algebra +
    // index crate land + are proven here.
    let ctx = PlanCtx::new(view, semantic);
    let result = eg_plan::execute(&eg_plan::Plan::new(ops), &ctx)?;
    Ok(result
        .rows()
        .iter()
        .map(|r| (r.id.clone(), r.score))
        .collect())
}

/// Produce the off-lock `GraphView` the query planner consumes, with per-agent
/// Row-Level Security applied IN the read/plan path (CONCEPT:KG-2.231). Under the
/// `security` feature the owned snapshot is filtered down to the rows `caller` may
/// see BEFORE it reaches any query surface (SQL/Cypher/unified), so no surface can
/// exfiltrate a forbidden row. Without the feature this is exactly
/// `core.analysis_snapshot()` (zero overhead, behavior unchanged).
#[cfg(any(feature = "query", feature = "cypher"))]
fn rls_snapshot(
    core: &Arc<GraphCore>,
    #[cfg(feature = "security")] caller: Option<&str>,
    #[cfg(feature = "security")] rls: &Arc<crate::isolation::IsolationLayer>,
) -> crate::graph::GraphView {
    #[cfg_attr(not(feature = "security"), allow(unused_mut))]
    let mut snap = core.analysis_snapshot();
    #[cfg(feature = "security")]
    rls.filter_view(caller.unwrap_or(""), &mut snap);
    snap
}

#[cfg(all(test, feature = "security", feature = "query", feature = "cypher"))]
mod rls_no_exfiltrate_tests {
    //! Proof (CONCEPT:KG-2.231): RLS filters the read/plan-path snapshot so neither
    //! SQL nor Cypher can exfiltrate a forbidden row. Agent A's query MUST exclude
    //! agent B's private node; a public node is visible to both.
    use crate::graph::GraphView;
    use crate::isolation::{AgentIdentity, AgentRole, IsolationLayer};
    use std::sync::Arc;

    fn node_blob(pairs: &[(&str, &str)]) -> Arc<Vec<u8>> {
        let m: std::collections::BTreeMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Arc::new(rmp_serde::to_vec_named(&m).unwrap())
    }

    /// Three nodes: B's private, a public one, an unowned legacy one.
    fn seeded_view() -> GraphView {
        let mut v = GraphView::default();
        for id in ["secret_b", "public_x", "legacy_z"] {
            let idx = v.graph.add_node(id.to_string());
            v.node_map.insert(id.to_string(), idx);
        }
        v.node_properties.insert(
            "secret_b".to_string(),
            node_blob(&[
                ("type", "Secret"),
                ("_owner", "bob"),
                ("_visibility", "private"),
            ]),
        );
        v.node_properties.insert(
            "public_x".to_string(),
            node_blob(&[
                ("type", "Public"),
                ("_owner", "bob"),
                ("_visibility", "public"),
            ]),
        );
        v.node_properties
            .insert("legacy_z".to_string(), node_blob(&[("type", "Legacy")]));
        v
    }

    fn isolation() -> IsolationLayer {
        let mut layer = IsolationLayer::new();
        layer.register_agent(AgentIdentity {
            agent_id: "alice".to_string(),
            role: AgentRole::Agent,
            teams: vec![],
        });
        layer.register_agent(AgentIdentity {
            agent_id: "bob".to_string(),
            role: AgentRole::Agent,
            teams: vec![],
        });
        layer
    }

    fn sql_ids(view: &GraphView) -> Vec<String> {
        let r = eg_query::exec_sql(view, "SELECT id FROM nodes").expect("sql");
        r.rows
            .iter()
            .filter_map(|blob| {
                let cells: Vec<serde_json::Value> = rmp_serde::from_slice(blob).ok()?;
                cells
                    .first()
                    .and_then(|c| c.as_str())
                    .map(|s| s.to_string())
            })
            .collect()
    }

    #[test]
    fn sql_excludes_other_agents_private_node() {
        let layer = isolation();
        // Alice's filtered view: NO secret_b.
        let mut va = seeded_view();
        layer.filter_view("alice", &mut va);
        let ids = sql_ids(&va);
        assert!(
            !ids.contains(&"secret_b".to_string()),
            "exfiltration: {ids:?}"
        );
        assert!(
            ids.contains(&"public_x".to_string()),
            "public hidden: {ids:?}"
        );
        assert!(ids.contains(&"legacy_z".to_string()));

        // Bob (owner) sees his own private node.
        let mut vb = seeded_view();
        layer.filter_view("bob", &mut vb);
        let ids_b = sql_ids(&vb);
        assert!(
            ids_b.contains(&"secret_b".to_string()),
            "owner blocked: {ids_b:?}"
        );
    }

    #[test]
    fn cypher_excludes_other_agents_private_node() {
        let layer = isolation();
        let mut va = seeded_view();
        layer.filter_view("alice", &mut va);
        let r = eg_query::exec_cypher(&va, "MATCH (n) RETURN n").expect("cypher");
        // The cypher result must not reference the hidden node id anywhere.
        let any_secret = r
            .rows
            .iter()
            .any(|blob| String::from_utf8_lossy(blob).contains("secret_b"));
        assert!(!any_secret, "cypher exfiltrated secret_b");
    }
}
