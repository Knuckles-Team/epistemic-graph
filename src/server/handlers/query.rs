//! Governed query handler. Owns BOTH query methods (one module per domain, per
//! the dispatch conventions — `Sql` + `CypherQuery` are the one `// ── Query ──`
//! protocol section):
//!   * `Method::Sql` (CONCEPT:EG-KG.query.read-only-sql-query, feature `query`) — `SELECT … FROM nodes …`
//!     over ONE graph via DataFusion (eg-query::exec_sql).
//!   * `Method::CypherQuery` (CONCEPT:EG-KG.query.dep-free-behind, feature `cypher`) — `MATCH … RETURN
//!     …` over ONE graph, DEP-FREE (eg-query::exec_cypher; label index / VF2 / BFS,
//!     no DataFusion). This is the lean-Pi query path.
//!
//! SQL and Cypher reads use detached, policy-filtered snapshots. Query-language
//! writes are staged and committed by the centralized MutationBatch boundary.
//! Cypher's explicit wire mode is verified against the native parser before
//! admission, authorization, or persistence selection.
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

use tokio::sync::RwLock;

use super::super::compute::compute_off_lock;
use super::super::state::ServerState;
use crate::graph::GraphCore;
use crate::protocol::Method;
#[cfg(any(feature = "query", feature = "cypher", feature = "graphql"))]
use crate::protocol::{Response, ResultPayload};
use crate::server::access::GraphReadAuthority;
#[cfg(feature = "result-cache")]
use eg_core::result_cache::ResultCache;
#[cfg(feature = "graphql")]
use eg_graphql::parser::{Field, GqlValue};

#[cfg(feature = "query")]
use eg_types::result_contract::EncodeRef;
#[cfg(any(feature = "query", feature = "cypher", feature = "graphql"))]
use eg_types::result_contract::{encoding, query as query_results, Dynamic, MethodResult};

/// MessagePack bytes of a value that is not itself a result: a result-cache key, or one
/// row of a SQL result.
#[cfg(feature = "query")]
fn msgpack_bytes<T: serde::Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, String> {
    rmp_serde::to_vec_named(value).map_err(|error| format!("result serialization failed: {error}"))
}

/// Answer with `body` encoded as `M`'s declared result.
#[cfg(feature = "query")]
fn result_response<M>(req_id: u64, body: &M::Body) -> Response
where
    M: MethodResult,
    M::Encoding: EncodeRef<M::Body>,
{
    Response::ok(req_id, ResultPayload::of_ref::<M>(body))
}

/// Answer with a caller-shaped `body` encoded as `M`'s declared `Raw` result.
#[cfg(any(feature = "query", feature = "cypher", feature = "graphql"))]
fn dynamic_response<M, T>(req_id: u64, body: &T) -> Response
where
    M: MethodResult<Body = Dynamic, Encoding = encoding::Raw>,
    T: serde::Serialize + ?Sized,
{
    Response::ok(req_id, ResultPayload::of_dynamic::<M, T>(body))
}

mod base;
pub(crate) use base::*;
mod dispatch;
pub(crate) use dispatch::*;
mod sql_read;
pub(crate) use sql_read::*;
mod explain_handlers;
pub(crate) use explain_handlers::*;
mod nl;
pub(crate) use nl::*;
mod graphql;
pub(crate) use graphql::*;
mod cypher;
pub(crate) use cypher::*;
mod planning;
pub(crate) use planning::*;
mod explain_wire_a;
pub(crate) use explain_wire_a::*;
mod explain_wire_b;
pub(crate) use explain_wire_b::*;
mod overlay;
pub(crate) use overlay::*;
mod sql_write;
pub(crate) use sql_write::*;
mod sql_dispatch;
pub(crate) use sql_dispatch::*;
mod sql_catalog;
pub(crate) use sql_catalog::*;

#[cfg(test)]
pub(crate) mod current_auth_test_support {
    use crate::acl::{AgentIdentity, AgentRole};
    use crate::isolation::IsolationLayer;
    use crate::protocol::{Method, Request};
    use crate::server::auth::build_shared_test_request;
    use crate::server::persistence::PersistenceBackend;
    use crate::server::state::ServerState;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    const TEST_AGENT: &str = "unit-test-agent";

    /// Add the Commons RBAC grant shared by authenticated protocol fixtures.
    /// Keeping this in the query test-support seam lets wire tests reuse the
    /// exact policy shape without copying an agent-registration block.
    pub(crate) fn grant_commons_user(isolation: &mut IsolationLayer) {
        #[cfg(feature = "security")]
        {
            use crate::acl::{Grant, GrantEffect, RbacAction, ResourceSelector, Role};
            isolation.add_role(Role::new("commons-user"));
            let commons_graph = ResourceSelector::Graph("__commons__".to_string());
            for action in [RbacAction::Read, RbacAction::Write] {
                isolation.add_grant(Grant {
                    role: "commons-user".to_string(),
                    resource: commons_graph.clone(),
                    action,
                    effect: GrantEffect::Allow,
                });
            }
        }
    }

    /// Register a non-System fixture principal with the shared Commons role.
    /// Both query and protocol round-trip tests use this helper; the caller
    /// remains responsible for choosing the principal's identity and any
    /// additional roles required by its scenario.
    pub(crate) fn register_commons_agent(isolation: &mut IsolationLayer, agent_id: &str) {
        isolation.register_agent(AgentIdentity {
            agent_id: agent_id.to_string(),
            role: AgentRole::Agent,
            teams: Vec::new(),
            #[cfg(feature = "security")]
            roles: vec!["commons-user".to_string()],
            #[cfg(not(feature = "security"))]
            roles: Vec::new(),
        });
    }

    pub(super) fn current_isolation() -> IsolationLayer {
        current_isolation_with_agents(&[])
    }

    pub(super) fn current_isolation_with_agents(agent_ids: &[&str]) -> IsolationLayer {
        let mut isolation = ServerState::test_isolation(TEST_AGENT);
        // RBAC (CONCEPT:EG-KG.compute.feature) is the mandatory current access
        // decision under `feature = "security"` (`isolation.rs::check_access`) —
        // there is no pre-RBAC "Commons is public" fall-through any more for a
        // non-`System` identity (see `server::mod::tests::multi_tenant_state`'s doc
        // comment for the same migration). Give every non-System test agent the
        // SAME "commons-user" R/W grant that fixture already establishes as the
        // replacement for the retired open-bus ACL semantics.
        grant_commons_user(&mut isolation);
        for agent_id in agent_ids {
            register_commons_agent(&mut isolation, agent_id);
        }
        isolation
    }

    pub(super) fn current_request(secret: &str, id: u64, graph: &str, method: Method) -> Request {
        current_request_as(secret, id, graph, TEST_AGENT, method)
    }

    pub(super) fn current_request_as(
        secret: &str,
        id: u64,
        graph: &str,
        agent_id: &str,
        method: Method,
    ) -> Request {
        build_shared_test_request(secret, id, graph, agent_id, method)
    }

    #[cfg(feature = "redb")]
    pub(super) fn open_test_backend(dir: String) -> Arc<dyn PersistenceBackend> {
        Arc::new(
            crate::server::persistence::redb_backend::RedbBackend::open(dir, 4096)
                .expect("open test redb backend"),
        )
    }

    pub(super) fn state_with_backend(
        secret: &str,
        isolation: IsolationLayer,
        dir: String,
        backend: Arc<dyn PersistenceBackend>,
    ) -> Arc<RwLock<ServerState>> {
        let mut state = ServerState::new_for_test(secret, isolation);
        state.persist_dir = Some(dir);
        state.persistence = Some(backend);
        Arc::new(RwLock::new(state))
    }

    #[cfg(feature = "redb")]
    pub(super) fn persisted_state(
        secret: &str,
        isolation: IsolationLayer,
    ) -> Arc<RwLock<ServerState>> {
        let dir = crate::server::sql_tables::test_persist_dir()
            .to_string_lossy()
            .into_owned();
        std::fs::create_dir_all(&dir).expect("create test persist dir");
        let backend = open_test_backend(dir.clone());
        state_with_backend(secret, isolation, dir, backend)
    }

    pub(super) mod prelude {
        #[cfg(feature = "redb")]
        pub(in super::super) use super::persisted_state;
        pub(in super::super) use super::{
            current_isolation, current_isolation_with_agents, current_request, current_request_as,
        };
        pub(in super::super) use crate::acl::{AgentIdentity, AgentRole};
        pub(in super::super) use crate::protocol::{Method, Request, Response, ResultPayload};
        pub(in super::super) use crate::server::auth::dispatch_test_on_heap as dispatch_on_heap;
        pub(in super::super) use crate::server::state::ServerState;
        pub(in super::super) use std::sync::Arc;
        pub(in super::super) use tokio::sync::RwLock;
    }
}

#[cfg(all(test, feature = "security", feature = "query", feature = "cypher"))]
mod rls_no_exfiltrate_tests {
    //! Proof (CONCEPT:EG-KG.sharding.row-level-security): RLS filters the read/plan-path snapshot so neither
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

    /// Three nodes: B's private, an explicitly public one, and an untagged one.
    fn seeded_view() -> GraphView {
        let mut v = GraphView::default();
        for id in ["secret_b", "public_x", "untagged_z"] {
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
            .insert("untagged_z".to_string(), node_blob(&[("type", "Untagged")]));
        v
    }

    fn isolation() -> IsolationLayer {
        let mut layer = IsolationLayer::new();
        layer.register_agent(AgentIdentity {
            agent_id: "alice".to_string(),
            role: AgentRole::Agent,
            teams: vec![],
            roles: vec![],
        });
        layer.register_agent(AgentIdentity {
            agent_id: "bob".to_string(),
            role: AgentRole::Agent,
            teams: vec![],
            roles: vec![],
        });
        layer
    }

    fn sql_ids(view: &GraphView) -> Vec<String> {
        let r = eg_query::exec_sql(
            view,
            "SELECT id FROM nodes",
            &eg_query::CancellationToken::new(),
        )
        .expect("sql");
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
        assert!(!ids.contains(&"untagged_z".to_string()));

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

// ── Version-keyed result cache, end-to-end through dispatch (CONCEPT:EG-KG.coordination.distributed-cache-coherence) ──
//
// Proves the cache over the REAL `dispatch` entrypoint (auth → routing → handler →
// cache → Cypher), on the lean Pi path (cypher, NO DataFusion):
//   1. the SAME query twice on an UNCHANGED graph HITS (didn't recompute, proven by
//      the cache hit counter) and returns identical bytes;
//   2. a WRITE bumps `version()` → the next identical query MISSES and recomputes a
//      CORRECT (changed) result;
//   3. the CDC feed invalidates a SECOND instance's cache for that graph
//      (cross-replica coherence): a write on A, replayed as a CDC event to B,
//      retires B's cached result so B recomputes.
#[cfg(all(
    test,
    feature = "result-cache",
    feature = "cypher",
    feature = "streaming",
    feature = "redb"
))]
mod result_cache_dispatch_tests {
    use super::current_auth_test_support::prelude::*;

    const SECRET: &str = "result-cache-test-secret";

    fn state() -> Arc<RwLock<ServerState>> {
        // Post-FLIP every dispatch-served mutation is authoritative
        // (commit-before-ack), so `AddNode` through `dispatch` REQUIRES a
        // persistence backend — a backendless fixture rejects the write
        // ("authoritative MutationBatch commit requires a persistence
        // backend") before the cache paths under test are ever reached.
        persisted_state(SECRET, current_isolation())
    }

    fn req(id: u64, method: Method) -> Request {
        current_request(SECRET, id, "__commons__", method)
    }

    async fn add_node(state: &Arc<RwLock<ServerState>>, id: u64, node: &str, label: &str) {
        let props = serde_json::json!({ "node_type": label });
        let bytes = rmp_serde::to_vec_named(&props).unwrap();
        let r = dispatch_on_heap(
            state,
            req(
                id,
                Method::AddNode {
                    node_id: node.into(),
                    properties_msgpack: bytes,
                },
            ),
        )
        .await;
        assert!(r.error.is_none(), "AddNode failed: {:?}", r.error);
    }

    fn raw(resp: &Response) -> Vec<u8> {
        match &resp.result {
            Some(ResultPayload::Raw(b)) => b.clone(),
            other => panic!("expected Raw result, got {other:?}"),
        }
    }

    const Q: &str = "MATCH (n:Person) RETURN n";

    fn cache_stats(core: &Arc<crate::graph::GraphCore>) -> (u64, u64) {
        core.result_cache().stats()
    }

    async fn core_of(state: &Arc<RwLock<ServerState>>) -> Arc<crate::graph::GraphCore> {
        state
            .read()
            .await
            .registry
            .get("__commons__")
            .unwrap()
            .core
            .clone()
    }

    #[tokio::test]
    async fn hit_on_unchanged_then_write_invalidates() {
        // Reads the ambient encryption env at its durable open, so the env must hold
        // still for this whole body. READ guard: it excludes only a key MUTATOR, never
        // another opener. See `crate::crypto::acquire_test_env_read_lock`'s doc.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let state = state();
        add_node(&state, 1, "p1", "Person").await;
        add_node(&state, 2, "p2", "Person").await;

        let core = core_of(&state).await;
        let (h0, m0) = cache_stats(&core);

        // First query: cold MISS, computes + caches.
        let r1 = dispatch_on_heap(
            &state,
            req(
                10,
                Method::CypherQuery {
                    query: Q.into(),
                    mode: crate::protocol::CypherMode::Read,
                },
            ),
        )
        .await;
        assert!(r1.error.is_none());
        let bytes1 = raw(&r1);
        let (h1, m1) = cache_stats(&core);
        assert_eq!((h1 - h0, m1 - m0), (0, 1), "first query is a miss");

        // Second identical query on the UNCHANGED graph: HIT, identical bytes, no
        // recompute (the hit counter moved, the miss counter did not).
        let r2 = dispatch_on_heap(
            &state,
            req(
                11,
                Method::CypherQuery {
                    query: Q.into(),
                    mode: crate::protocol::CypherMode::Read,
                },
            ),
        )
        .await;
        assert_eq!(raw(&r2), bytes1, "cached bytes identical to computed");
        let (h2, m2) = cache_stats(&core);
        assert_eq!((h2 - h1, m2 - m1), (1, 0), "second query hit the cache");

        // A WRITE bumps version → the cached entry is now unreachable.
        let v_before = core.version();
        add_node(&state, 3, "p3", "Person").await;
        assert_ne!(core.version(), v_before, "write must bump version");

        // Same query again: MISS (recompute), and the result is CORRECT (now 3 rows).
        let r3 = dispatch_on_heap(
            &state,
            req(
                12,
                Method::CypherQuery {
                    query: Q.into(),
                    mode: crate::protocol::CypherMode::Read,
                },
            ),
        )
        .await;
        assert!(r3.error.is_none());
        let bytes3 = raw(&r3);
        assert_ne!(
            bytes3, bytes1,
            "result changed after the write (recomputed)"
        );
        let (h3, m3) = cache_stats(&core);
        assert_eq!(
            (h3 - h2, m3 - m2),
            (0, 1),
            "post-write query missed + recomputed"
        );

        // And it is cached again at the new version: the next identical query HITS.
        let r4 = dispatch_on_heap(
            &state,
            req(
                13,
                Method::CypherQuery {
                    query: Q.into(),
                    mode: crate::protocol::CypherMode::Read,
                },
            ),
        )
        .await;
        assert_eq!(raw(&r4), bytes3);
        let (h4, _m4) = cache_stats(&core);
        assert_eq!(h4 - h3, 1, "post-write result is itself cached + re-hit");
    }

    #[tokio::test]
    async fn cdc_drives_cross_instance_invalidation() {
        // Reads the ambient encryption env at its durable open, so the env must hold
        // still for this whole body. READ guard: it excludes only a key MUTATOR, never
        // another opener. See `crate::crypto::acquire_test_env_read_lock`'s doc.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        // Two independent in-process instances A and B (separate registries/caches),
        // each holding the SAME logical graph + the SAME data.
        let a = state();
        let b = state();
        for (id, n) in [(1u64, "p1"), (2, "p2")] {
            add_node(&a, id, n, "Person").await;
            add_node(&b, id, n, "Person").await;
        }
        let core_b = core_of(&b).await;

        // Warm B's cache: query B once (miss) then again (hit) — B is now serving a
        // cached result for the graph.
        let _ = dispatch_on_heap(
            &b,
            req(
                20,
                Method::CypherQuery {
                    query: Q.into(),
                    mode: crate::protocol::CypherMode::Read,
                },
            ),
        )
        .await;
        let r_hit = dispatch_on_heap(
            &b,
            req(
                21,
                Method::CypherQuery {
                    query: Q.into(),
                    mode: crate::protocol::CypherMode::Read,
                },
            ),
        )
        .await;
        let (h_before, _m) = cache_stats(&core_b);
        assert!(h_before >= 1, "B should have a warm cache hit");
        let bytes_b_old = raw(&r_hit);

        // A WRITE lands on A. A emits a CDC event into its feed (the dispatch shell
        // does this for every durable mutation). Grab A's CDC feed.
        add_node(&a, 3, "p3", "Person").await;
        let hub_a = {
            let s = a.read().await;
            s.cdc.clone().unwrap()
        };
        // A produced at least one CDC event for the AddNode.
        let events = hub_a.read("__commons__", 0, 0).events;
        assert!(!events.is_empty(), "A's write must emit a CDC event");

        // COHERENCE: drain A's feed into B → B invalidates its local cache for the
        // graph (bumps B's version). This is the cross-replica invalidation signal.
        let v_b_before = core_b.version();
        let next =
            crate::server::cache_coherence::drain_and_invalidate(&b, &hub_a, "__commons__", 0, 0)
                .await
                .unwrap();
        assert!(next > 0, "drained at least one event");
        assert_ne!(
            core_b.version(),
            v_b_before,
            "B's version bumped on the remote change"
        );

        // B's previously-cached result is now unreachable: the SAME query MISSES.
        let (h2, m2) = cache_stats(&core_b);
        let r_after = dispatch_on_heap(
            &b,
            req(
                22,
                Method::CypherQuery {
                    query: Q.into(),
                    mode: crate::protocol::CypherMode::Read,
                },
            ),
        )
        .await;
        let (h3, m3) = cache_stats(&core_b);
        assert_eq!(
            (h3 - h2, m3 - m2),
            (0, 1),
            "after CDC invalidation B recomputes (miss), not a stale hit"
        );
        // The recompute SUCCEEDS over B's own (unreplicated) data and is non-empty —
        // proving the post-invalidation read recomputes a valid result rather than
        // erroring or serving a stale hit. (Cypher row ORDER isn't stable, so we don't
        // byte-compare to the pre-invalidation bytes; the load-bearing proof is the
        // miss counter above — invalidation fired.)
        assert!(r_after.error.is_none());
        assert!(!raw(&r_after).is_empty());
        assert!(!bytes_b_old.is_empty());
    }
}

// ── ⚠ RLS × result-cache: NO cross-agent leak (CONCEPT:EG-KG.coordination.distributed-cache-coherence × KG-2.231) ──
//
// THE headline reconciliation proof. With RLS ACTIVE, agent A and agent B run the
// SAME query text against the SAME graph and the SAME version, yet:
//   * A's cached (A-filtered) result is NEVER served to B — B's identical query is a
//     cache MISS (the rls-aware key differs by agent_id) and recomputes B's OWN view;
//   * the bytes B receives do NOT contain A's private node — no exfiltration through
//     the cache;
//   * a SECOND A query IS a hit (A's key is stable), proving caching still works
//     per-agent (we didn't just disable the cache under RLS).
#[cfg(all(
    test,
    feature = "result-cache",
    feature = "cypher",
    feature = "security",
    feature = "redb"
))]
mod rls_aware_cache_no_cross_agent_leak {
    use super::current_auth_test_support::prelude::*;

    const SECRET: &str = "rls-cache-test-secret";

    fn state() -> Arc<RwLock<ServerState>> {
        // Post-FLIP every dispatch-served mutation is authoritative
        // (commit-before-ack), so `AddNode` through `dispatch` REQUIRES a
        // persistence backend — a backendless fixture rejects the write
        // ("authoritative MutationBatch commit requires a persistence
        // backend") before the RLS-aware cache paths under test are ever
        // reached. Mirrors `result_cache_dispatch_tests::state`'s already-fixed
        // fixture.
        let mut isolation = current_isolation_with_agents(&["alice", "bob"]);
        // Under `security`, `check_access` defers entirely to RBAC -- the old
        // "`__commons__` is open to all authenticated agents" graph-type rule is
        // ignored for a non-System identity. Grant alice/bob the SAME shape
        // explicitly so their `AddNode`/`CypherQuery`/`GraphQl` calls below reach
        // the RLS cache logic this module actually tests, instead of failing
        // closed on an empty RBAC policy before RLS is ever exercised.
        #[cfg(feature = "security")]
        {
            use crate::acl::{Grant, GrantEffect, RbacAction, ResourceSelector, Role};
            isolation.add_role(Role::new("commons-rw"));
            for action in [RbacAction::Read, RbacAction::Write] {
                isolation.add_grant(Grant {
                    role: "commons-rw".into(),
                    resource: ResourceSelector::Graph("__commons__".into()),
                    action,
                    effect: GrantEffect::Allow,
                });
            }
            for agent in ["alice", "bob"] {
                isolation.register_agent(AgentIdentity {
                    agent_id: agent.into(),
                    role: AgentRole::Agent,
                    teams: Vec::new(),
                    roles: vec!["commons-rw".into()],
                });
            }
        }
        persisted_state(SECRET, isolation)
    }

    /// A request as `agent_id`.
    fn req_as(id: u64, agent_id: &str, method: Method) -> Request {
        current_request_as(SECRET, id, "__commons__", agent_id, method)
    }

    fn raw(resp: &Response) -> Vec<u8> {
        match &resp.result {
            Some(ResultPayload::Raw(b)) => b.clone(),
            other => panic!("expected Raw result, got {other:?}"),
        }
    }

    /// Add a node with RLS owner/visibility props (the `_owner`/`_visibility`
    /// convention `IsolationLayer::filter_view` reads).
    async fn add_rls_node(
        state: &Arc<RwLock<ServerState>>,
        id: u64,
        node: &str,
        label: &str,
        owner: &str,
        visibility: &str,
    ) {
        let props = serde_json::json!({
            "node_type": label,
            "_owner": owner,
            "_visibility": visibility,
        });
        let bytes = rmp_serde::to_vec_named(&props).unwrap();
        let r = dispatch_on_heap(
            state,
            req_as(
                id,
                owner,
                Method::AddNode {
                    node_id: node.into(),
                    properties_msgpack: bytes,
                },
            ),
        )
        .await;
        assert!(r.error.is_none(), "AddNode failed: {:?}", r.error);
    }

    async fn core_of(state: &Arc<RwLock<ServerState>>) -> Arc<crate::graph::GraphCore> {
        state
            .read()
            .await
            .registry
            .get("__commons__")
            .unwrap()
            .core
            .clone()
    }

    // The SAME query text both agents run. It matches BOTH nodes by label, so RLS is
    // the only thing that differentiates their result sets.
    const Q: &str = "MATCH (n:Secret) RETURN n";

    #[tokio::test]
    async fn agent_a_cached_result_is_not_served_to_agent_b() {
        let state = state();
        // The fixture provisions two peer agents with no manager/grant between them.
        // A private :Secret node owned by alice (bob must NEVER see it), plus a public
        // :Secret node both can see — so neither agent's result is empty.
        add_rls_node(&state, 10, "alice_secret", "Secret", "alice", "private").await;
        add_rls_node(&state, 11, "shared", "Secret", "alice", "public").await;

        let core = core_of(&state).await;

        // ── Alice queries Q: cold MISS, computes + caches under alice's rls-key. Her
        //    filtered view sees BOTH nodes (she owns the private one + the public one).
        let (h0, m0) = core.result_cache().stats();
        let ra1 = dispatch_on_heap(
            &state,
            req_as(
                20,
                "alice",
                Method::CypherQuery {
                    query: Q.into(),
                    mode: crate::protocol::CypherMode::Read,
                },
            ),
        )
        .await;
        assert!(ra1.error.is_none());
        let alice_bytes = raw(&ra1);
        let (h1, m1) = core.result_cache().stats();
        assert_eq!(
            (h1 - h0, m1 - m0),
            (0, 1),
            "alice's first query is a cold miss"
        );
        let alice_str = String::from_utf8_lossy(&alice_bytes);
        assert!(
            alice_str.contains("alice_secret"),
            "alice must see her own private node: {alice_str}"
        );

        // ── Bob runs the IDENTICAL query text on the UNCHANGED graph (same version).
        //    If the cache were NOT rls-aware, bob would HIT alice's entry and receive
        //    `alice_secret` — a cross-agent leak. With the rls-aware key it MISSES
        //    (different agent_id ⇒ different key) and recomputes BOB's filtered view.
        let rb = dispatch_on_heap(
            &state,
            req_as(
                21,
                "bob",
                Method::CypherQuery {
                    query: Q.into(),
                    mode: crate::protocol::CypherMode::Read,
                },
            ),
        )
        .await;
        assert!(rb.error.is_none());
        let bob_bytes = raw(&rb);
        let (h2, m2) = core.result_cache().stats();
        assert_eq!(
            (h2 - h1, m2 - m1),
            (0, 1),
            "bob's identical query MISSES the rls-aware cache (no cross-agent hit)"
        );
        // THE leak assertions: bob's bytes must NOT differ-by-leak — no alice_secret.
        let bob_str = String::from_utf8_lossy(&bob_bytes);
        assert!(
            !bob_str.contains("alice_secret"),
            "EXFILTRATION via cache: bob received alice's private node: {bob_str}"
        );
        assert!(
            bob_str.contains("shared"),
            "bob must still see the public node: {bob_str}"
        );
        assert_ne!(
            bob_bytes, alice_bytes,
            "bob's RLS-filtered result must differ from alice's (no shared cache slot)"
        );

        // ── Alice repeats Q: now it HITS her own entry (caching still works per-agent;
        //    we didn't simply disable the cache under RLS), and serves her bytes back.
        let ra2 = dispatch_on_heap(
            &state,
            req_as(
                22,
                "alice",
                Method::CypherQuery {
                    query: Q.into(),
                    mode: crate::protocol::CypherMode::Read,
                },
            ),
        )
        .await;
        let (h3, m3) = core.result_cache().stats();
        assert_eq!(
            (h3 - h2, m3 - m2),
            (1, 0),
            "alice's repeat query HITS her own rls-aware cache slot"
        );
        assert_eq!(raw(&ra2), alice_bytes, "alice's cached bytes are her own");
    }

    // The SAME GraphQL query both agents run. It selects every `:Secret` node's id, so
    // — exactly like the Cypher case — RLS is the ONLY thing differentiating the two
    // agents' result sets. Locks in reconciliation #1: a GraphQL read must go through
    // the SAME RLS-aware result-cache compose and never leak across agents.
    #[cfg(feature = "graphql")]
    const GQL: &str = "{ Secret { id } }";

    #[cfg(feature = "graphql")]
    #[tokio::test]
    async fn agent_a_graphql_cached_result_is_not_served_to_agent_b() {
        // Reads the ambient encryption env at its durable open, so the env must hold
        // still for this whole body. READ guard: it excludes only a key MUTATOR, never
        // another opener. See `crate::crypto::acquire_test_env_read_lock`'s doc.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let state = state();
        add_rls_node(&state, 10, "alice_secret", "Secret", "alice", "private").await;
        add_rls_node(&state, 11, "shared", "Secret", "alice", "public").await;

        let core = core_of(&state).await;

        // Alice: cold MISS, cached under alice's rls-key; her view sees both nodes.
        let (h0, m0) = core.result_cache().stats();
        let ra1 = dispatch_on_heap(
            &state,
            req_as(
                20,
                "alice",
                Method::GraphQl {
                    query: GQL.into(),
                    variables: None,
                },
            ),
        )
        .await;
        assert!(ra1.error.is_none(), "alice GraphQL failed: {:?}", ra1.error);
        let alice_bytes = raw(&ra1);
        let (h1, m1) = core.result_cache().stats();
        assert_eq!(
            (h1 - h0, m1 - m0),
            (0, 1),
            "alice's first GraphQL query is a cold miss"
        );
        let alice_str = String::from_utf8_lossy(&alice_bytes);
        assert!(
            alice_str.contains("alice_secret"),
            "alice must see her own private node via GraphQL: {alice_str}"
        );

        // Bob: IDENTICAL GraphQL text on the UNCHANGED graph. An rls-UNAWARE cache would
        // serve him alice's entry (leak). The rls-aware key MISSES (different agent_id)
        // and recomputes BOB's filtered view.
        let rb = dispatch_on_heap(
            &state,
            req_as(
                21,
                "bob",
                Method::GraphQl {
                    query: GQL.into(),
                    variables: None,
                },
            ),
        )
        .await;
        assert!(rb.error.is_none(), "bob GraphQL failed: {:?}", rb.error);
        let bob_bytes = raw(&rb);
        let (h2, m2) = core.result_cache().stats();
        assert_eq!(
            (h2 - h1, m2 - m1),
            (0, 1),
            "bob's identical GraphQL query MISSES the rls-aware cache (no cross-agent hit)"
        );
        let bob_str = String::from_utf8_lossy(&bob_bytes);
        assert!(
            !bob_str.contains("alice_secret"),
            "EXFILTRATION via GraphQL cache: bob received alice's private node: {bob_str}"
        );
        assert!(
            bob_str.contains("shared"),
            "bob must still see the public node via GraphQL: {bob_str}"
        );
        assert_ne!(
            bob_bytes, alice_bytes,
            "bob's RLS-filtered GraphQL result must differ from alice's (no shared slot)"
        );

        // Alice repeats: HITS her own per-agent slot (caching still works under RLS).
        let ra2 = dispatch_on_heap(
            &state,
            req_as(
                22,
                "alice",
                Method::GraphQl {
                    query: GQL.into(),
                    variables: None,
                },
            ),
        )
        .await;
        let (h3, m3) = core.result_cache().stats();
        assert_eq!(
            (h3 - h2, m3 - m2),
            (1, 0),
            "alice's repeat GraphQL query HITS her own rls-aware cache slot"
        );
        assert_eq!(
            raw(&ra2),
            alice_bytes,
            "alice's cached GraphQL bytes are her own"
        );
    }
}

// ── Server-dispatch WRITE wiring (CONCEPT:EG-KG.query.mirrors-pgwire) ─────────────────────────────────
//
// Proves the five EG-023 wirings land THROUGH the real `dispatch_on_heap()` entrypoint a wire
// request hits (auth → routing → handler → write): a GraphQL mutation creates a node a
// later query sees; a Cypher CREATE is then visible to a MATCH; a wire `Sql`
// CREATE TABLE + INSERT + SELECT round-trips; an `INSERT INTO nodes` is visible to a
// SELECT; and the read paths still work.
#[cfg(all(
    test,
    feature = "query",
    feature = "cypher",
    feature = "graphql",
    feature = "redb"
))]
mod dispatch_write_tests {
    use super::current_auth_test_support::prelude::*;

    const SECRET: &str = "dispatch-write-test-secret";

    fn state() -> Arc<RwLock<ServerState>> {
        // Post-FLIP every dispatch-served mutation is authoritative
        // (commit-before-ack), so `AddNode`/`CREATE`/`INSERT` through `dispatch`
        // REQUIRE a persistence backend — a backendless fixture rejects the write
        // ("authoritative MutationBatch commit requires a persistence backend")
        // before the write path under test is ever reached. Mirrors
        // `result_cache_dispatch_tests::state`'s already-fixed fixture.
        persisted_state(SECRET, current_isolation())
    }

    fn req(id: u64, method: Method) -> Request {
        current_request(SECRET, id, "__commons__", method)
    }

    /// Sign one direct-dispatch request with an explicit stable operation key.
    /// Tests use this to model the protocol contract: retries mint a fresh
    /// transport nonce while retaining the caller's idempotency key.
    fn retry_req(
        id: u64,
        agent_id: &str,
        method: Method,
        nonce: &str,
        idempotency_key: &str,
    ) -> Request {
        let context = crate::acl::RequestContextClaims {
            principal: agent_id.to_string(),
            tenant: "tenant-shared".to_string(),
            audience: "epistemic-graph-test".to_string(),
            agent_id: agent_id.to_string(),
            roles: vec!["test".to_string()],
            scopes: vec!["kg:read".to_string(), "kg:write".to_string()],
            policy_version: "policy-test".to_string(),
            delegation: Vec::new(),
            node: None,
            priority: None,
        };
        let mut request = Request {
            id,
            graph: "__commons__".to_string(),
            auth_token: String::new(),
            agent_id: Some(agent_id.to_string()),
            method,
        };
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("test clock after epoch")
            .as_secs();
        request.auth_token = crate::server::auth::compute_verified_envelope_token(
            SECRET,
            &request,
            &crate::server::auth::VerifiedEnvelopeParams {
                context: &context,
                timestamp,
                nonce,
                idempotency_key,
            },
        );
        request
    }

    fn raw(resp: &Response) -> Vec<u8> {
        match &resp.result {
            Some(ResultPayload::Raw(b)) => b.clone(),
            other => panic!("expected Raw result, got {other:?} / err {:?}", resp.error),
        }
    }

    /// Decode a `Raw(QueryResult)` write/read response into `(columns, rows)` where each
    /// row is the decoded `Vec<serde_json::Value>` cell list.
    fn query_result(resp: &Response) -> (Vec<String>, Vec<Vec<serde_json::Value>>) {
        let qr: crate::protocol::QueryResult =
            rmp_serde::from_slice(&raw(resp)).expect("QueryResult");
        let rows = qr
            .rows
            .iter()
            .map(|b| rmp_serde::from_slice::<Vec<serde_json::Value>>(b).expect("row cells"))
            .collect();
        (qr.columns, rows)
    }

    /// THE GraphQL write→read proof (CONCEPT:EG-KG.query.mutation/EG-023): a `mutation { createNode … }`
    /// dispatched over the wire creates a node a subsequent GraphQL query SEES.
    #[tokio::test]
    async fn graphql_mutation_creates_node_via_dispatch() {
        // Reads the ambient encryption env at its durable open, so the env must hold
        // still for this whole body. READ guard: it excludes only a key MUTATOR, never
        // another opener. See `crate::crypto::acquire_test_env_read_lock`'s doc.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let state = state();
        let m = dispatch_on_heap(
            &state,
            req(
                1,
                Method::GraphQl {
                    query: r#"mutation { createNode(label: "Person", id: "dave", props: {name: "Dave", age: 50}) { id name } }"#.into(),
                    variables: None,
                },
            ),
        )
        .await;
        assert!(m.error.is_none(), "mutation failed: {:?}", m.error);
        let v: serde_json::Value = rmp_serde::from_slice(&raw(&m)).unwrap();
        assert_eq!(v["data"]["createNode"]["id"], serde_json::json!("dave"));

        // A fresh GraphQL query over the post-write graph sees Dave.
        let q = dispatch_on_heap(
            &state,
            req(
                2,
                Method::GraphQl {
                    query: r#"{ Person(name: "Dave") { name age } }"#.into(),
                    variables: None,
                },
            ),
        )
        .await;
        assert!(q.error.is_none(), "query failed: {:?}", q.error);
        let qv: serde_json::Value = rmp_serde::from_slice(&raw(&q)).unwrap();
        let people = qv["data"]["Person"].as_array().unwrap();
        assert_eq!(people.len(), 1);
        assert_eq!(people[0]["name"], serde_json::json!("Dave"));
        assert_eq!(people[0]["age"], serde_json::json!(50));
    }

    /// THE Cypher write→read proof (CONCEPT:EG-KG.query.register-each-user-table/EG-023): a `CREATE` dispatched over the
    /// wire is then visible to a `MATCH` (which still runs the read path).
    #[tokio::test]
    async fn cypher_create_then_match_via_dispatch() {
        // Reads the ambient encryption env at its durable open, so the env must hold
        // still for this whole body. READ guard: it excludes only a key MUTATOR, never
        // another opener. See `crate::crypto::acquire_test_env_read_lock`'s doc.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let state = state();
        let c = dispatch_on_heap(
            &state,
            req(
                1,
                Method::CypherQuery {
                    query: "CREATE (n:Widget {name: 'gizmo', qty: 7})".into(),
                    mode: crate::protocol::CypherMode::Write,
                },
            ),
        )
        .await;
        assert!(c.error.is_none(), "cypher CREATE failed: {:?}", c.error);

        let r = dispatch_on_heap(
            &state,
            req(
                2,
                Method::CypherQuery {
                    query: "MATCH (n:Widget) RETURN n.name".into(),
                    mode: crate::protocol::CypherMode::Read,
                },
            ),
        )
        .await;
        assert!(r.error.is_none(), "cypher MATCH failed: {:?}", r.error);
        let (_cols, rows) = query_result(&r);
        let names: Vec<String> = rows
            .iter()
            .map(|cells| cells[0].as_str().unwrap().to_string())
            .collect();
        assert_eq!(names, vec!["gizmo"], "MATCH must see the CREATEd node");
    }

    #[tokio::test]
    async fn cypher_declared_mode_must_match_native_parser() {
        // Reads the ambient encryption env at its durable open, so the env must hold
        // still for this whole body. READ guard: it excludes only a key MUTATOR, never
        // another opener. See `crate::crypto::acquire_test_env_read_lock`'s doc.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let state = state();
        let disguised_write = dispatch_on_heap(
            &state,
            req(
                1,
                Method::CypherQuery {
                    query: "CREATE (n:Forbidden {name: 'write-through-read'})".into(),
                    mode: crate::protocol::CypherMode::Read,
                },
            ),
        )
        .await;
        assert!(
            disguised_write
                .error
                .as_deref()
                .is_some_and(|message| message.contains("declared mode")),
            "write declared as read must fail closed: {:?}",
            disguised_write.error
        );

        let mislabeled_read = dispatch_on_heap(
            &state,
            req(
                2,
                Method::CypherQuery {
                    query: "MATCH (n) RETURN n".into(),
                    mode: crate::protocol::CypherMode::Write,
                },
            ),
        )
        .await;
        assert!(
            mislabeled_read
                .error
                .as_deref()
                .is_some_and(|message| message.contains("declared mode")),
            "read declared as write must fail closed: {:?}",
            mislabeled_read.error
        );
    }

    /// THE wire-SQL DDL/DML round-trip (CONCEPT:EG-KG.query.mirrors-pgwire): `CREATE TABLE` + `INSERT` + a
    /// `SELECT` that reads the user table back, all over `Method::Sql` through dispatch.
    #[tokio::test]
    async fn wire_sql_create_insert_select_round_trips() {
        // Reads the ambient encryption env at its durable open, so the env must hold
        // still for this whole body. READ guard: it excludes only a key MUTATOR, never
        // another opener. See `crate::crypto::acquire_test_env_read_lock`'s doc.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let state = state();
        // Unique table name + DROP-IF-EXISTS so the process-global store is clean even on
        // a re-run against a persisted temp store.
        let table = format!("eg023_kv_{}", std::process::id());

        let sql = |q: String| Method::Sql {
            query: q,
            params_msgpack: Vec::new(),
        };

        let d =
            dispatch_on_heap(&state, req(1, sql(format!("DROP TABLE IF EXISTS {table}")))).await;
        assert!(d.error.is_none(), "DROP failed: {:?}", d.error);

        let c = dispatch_on_heap(
            &state,
            req(2, sql(format!("CREATE TABLE {table} (k TEXT, v BIGINT)"))),
        )
        .await;
        assert!(c.error.is_none(), "CREATE TABLE failed: {:?}", c.error);

        let i = dispatch_on_heap(
            &state,
            req(
                3,
                sql(format!(
                    "INSERT INTO {table} (k, v) VALUES ('a', 1), ('b', 2)"
                )),
            ),
        )
        .await;
        assert!(i.error.is_none(), "INSERT failed: {:?}", i.error);

        let s = dispatch_on_heap(
            &state,
            req(4, sql(format!("SELECT k, v FROM {table} ORDER BY k"))),
        )
        .await;
        assert!(s.error.is_none(), "SELECT failed: {:?}", s.error);
        let (cols, rows) = query_result(&s);
        assert_eq!(cols, vec!["k", "v"]);
        assert_eq!(
            rows.len(),
            2,
            "two rows round-tripped through the table store"
        );
        assert_eq!(rows[0][0], serde_json::json!("a"));
        assert_eq!(rows[0][1], serde_json::json!(1));
        assert_eq!(rows[1][0], serde_json::json!("b"));
        assert_eq!(rows[1][1], serde_json::json!(2));

        // cleanup
        let _ =
            dispatch_on_heap(&state, req(5, sql(format!("DROP TABLE IF EXISTS {table}")))).await;
    }

    /// Method::Sql `GRAPH_TABLE` must resolve the graph re-admitted into the
    /// caller's ephemeral ACL projection under that projection's private scope.
    /// A same-tenant actor with no graph/table grant receives the same absence
    /// result as an unknown graph and cannot read the owner's row.
    #[tokio::test]
    async fn wire_sql_graph_table_uses_authorized_projection_scope() {
        // Reads the ambient encryption env at its durable open, so the env must hold
        // still for this whole body. READ guard: it excludes only a key MUTATOR, never
        // another opener. See `crate::crypto::acquire_test_env_read_lock`'s doc.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let guest = "sql-pgq-guest";
        let state = persisted_state(SECRET, current_isolation_with_agents(&[guest]));
        let table = format!("eg_pgq_people_{}", std::process::id());
        let graph = format!("eg_pgq_social_{}", std::process::id());
        let sql = |query: String| Method::Sql {
            query,
            params_msgpack: Vec::new(),
        };

        for (id, query) in [
            (
                1,
                format!("CREATE TABLE {table} (person_id TEXT PRIMARY KEY, name TEXT)"),
            ),
            (
                2,
                format!("INSERT INTO {table} (person_id, name) VALUES ('1', 'Alice')"),
            ),
            (
                3,
                format!(
                    "CREATE PROPERTY GRAPH {graph} VERTEX TABLES (\
                     {table} KEY (person_id) LABEL person PROPERTIES (name))"
                ),
            ),
        ] {
            let response = dispatch_on_heap(&state, req(id, sql(query))).await;
            assert!(
                response.error.is_none(),
                "setup failed: {:?}",
                response.error
            );
        }

        let graph_table = format!(
            "SELECT * FROM GRAPH_TABLE ({graph} MATCH (p:person) \
             COLUMNS (p.name AS name))"
        );
        let owner = dispatch_on_heap(&state, req(4, sql(graph_table.clone()))).await;
        assert!(
            owner.error.is_none(),
            "owner GRAPH_TABLE failed: {:?}",
            owner.error
        );
        let (columns, rows) = query_result(&owner);
        assert_eq!(columns, vec!["name"]);
        assert_eq!(rows, vec![vec![serde_json::json!("Alice")]]);

        let denied = dispatch_on_heap(
            &state,
            retry_req(
                5,
                guest,
                sql(graph_table),
                "direct-sql-graph-read-guest",
                "direct-sql-graph-read-guest",
            ),
        )
        .await;
        assert!(
            denied
                .error
                .as_deref()
                .is_some_and(|error| error.contains("does not exist")),
            "ungranted actor must not resolve the property graph: {:?}",
            denied.error
        );
    }

    /// The opaque catalog scope a verified carrier for `tenant` actually
    /// serves under.
    ///
    /// `tenant_acl_table_store`/`tenant_table_store` are keyed by THIS, never
    /// by the raw tenant name: `CarrierAuthority::from_verified` wraps every
    /// non-opaque tenant in `carrier-tenant:<digest>` before anything reaches
    /// the catalog. A fixture that passes the raw name therefore opens a
    /// DIFFERENT, empty catalog -- so a storage fault installed there is never
    /// on the path the dispatch takes, and an assertion about that fault can
    /// never fire. The passing pgwire sibling
    /// (`server::wire`'s `ordinary_create_retains_intent_until_owner_repair_
    /// completes`) gets this right by going through a real `CarrierAuthority`;
    /// this derives the same scope the same way production does.
    fn carrier_tenant_scope(tenant: &str) -> String {
        crate::server::mutation_batch::opaque_coordinator_key("carrier-tenant", "verified", tenant)
    }

    /// A direct signed CREATE that committed its table before owner
    /// registration failed must recover through the exact stable operation
    /// receipt. The retry uses a fresh nonce and the same idempotency key,
    /// returns the recorded result without executing CREATE twice, and repairs
    /// ownership before later DDL is admitted.
    #[tokio::test]
    async fn wire_sql_create_fresh_nonce_retry_repairs_owner() {
        // Reads the ambient encryption env at its durable open, so the env must hold
        // still for this whole body. READ guard: it excludes only a key MUTATOR, never
        // another opener. See `crate::crypto::acquire_test_env_read_lock`'s doc.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let guest = "sql-owner-repair-guest";
        let state = persisted_state(SECRET, current_isolation_with_agents(&[guest]));
        let table = format!("eg_direct_owner_repair_{}", std::process::id());
        let persist_dir = state
            .read()
            .await
            .persist_dir
            .clone()
            .expect("persisted state path");

        // Install the same deterministic cross-redb fault used by pgwire's
        // recovery proof: physical CREATE can commit, while owner registration
        // fails against the malformed source-authority catalog.
        let acl = crate::server::sql_tables::tenant_acl_table_store(
            &carrier_tenant_scope("tenant-shared"),
            std::path::Path::new(&persist_dir),
        )
        .expect("open tenant ACL store");
        acl.create_table(
            &eg_query::TableSchema::new(
                "__eg_sql_owners__",
                vec![eg_query::Column::new(
                    "table_name",
                    eg_query::ColumnType::Text,
                    false,
                    true,
                )],
            ),
            false,
        )
        .expect("install malformed owner catalog");

        let create = Method::Sql {
            query: format!("CREATE TABLE {table} (id TEXT PRIMARY KEY)"),
            params_msgpack: Vec::new(),
        };
        let operation_key = format!("direct-sql-owner-repair-{}", std::process::id());
        let first = dispatch_on_heap(
            &state,
            retry_req(
                50,
                "unit-test-agent",
                create.clone(),
                "direct-sql-create-attempt-1",
                &operation_key,
            ),
        )
        .await;
        assert!(
            first
                .error
                .as_deref()
                .is_some_and(|error| error.contains("SQL_OWNER_REPAIR_PENDING")),
            "owner catalog fault must surface after physical commit: {:?}",
            first.error
        );
        let store = crate::server::sql_tables::tenant_table_store(
            &carrier_tenant_scope("tenant-shared"),
            std::path::Path::new(&persist_dir),
        )
        .expect("open tenant table store");
        assert!(
            store
                .get_schema(&table)
                .expect("read physical CREATE")
                .is_some(),
            "the SQL table commit precedes owner registration"
        );

        acl.drop_table("__eg_sql_owners__", false)
            .expect("remove malformed owner catalog");
        let retried = dispatch_on_heap(
            &state,
            retry_req(
                51,
                "unit-test-agent",
                create,
                "direct-sql-create-attempt-2",
                &operation_key,
            ),
        )
        .await;
        assert!(
            retried.error.is_none(),
            "fresh-nonce retry must replay and repair ownership: {:?}",
            retried.error
        );

        let owner_alter = dispatch_on_heap(
            &state,
            retry_req(
                52,
                "unit-test-agent",
                Method::Sql {
                    query: format!("ALTER TABLE {table} ADD COLUMN note TEXT"),
                    params_msgpack: Vec::new(),
                },
                "direct-sql-owner-alter",
                "direct-sql-owner-alter",
            ),
        )
        .await;
        assert!(
            owner_alter.error.is_none(),
            "repaired owner must retain ALTER authority: {:?}",
            owner_alter.error
        );

        let guest_alter = dispatch_on_heap(
            &state,
            retry_req(
                53,
                guest,
                Method::Sql {
                    query: format!("ALTER TABLE {table} ADD COLUMN denied TEXT"),
                    params_msgpack: Vec::new(),
                },
                "direct-sql-guest-alter",
                "direct-sql-guest-alter",
            ),
        )
        .await;
        assert!(
            guest_alter
                .error
                .as_deref()
                .is_some_and(|error| error.contains(crate::server::sql_catalog_acl::ACCESS_DENIED)),
            "same-tenant non-owner must remain denied after repair: {:?}",
            guest_alter.error
        );
    }

    /// `INSERT INTO nodes` over the wire lands in the graph core and a `SELECT` sees it —
    /// the agent-utilities `graph_table`/`sql_exec` node-write path (CONCEPT:EG-KG.query.mirrors-pgwire).
    #[tokio::test]
    async fn wire_sql_insert_node_then_select_via_dispatch() {
        // Reads the ambient encryption env at its durable open, so the env must hold
        // still for this whole body. READ guard: it excludes only a key MUTATOR, never
        // another opener. See `crate::crypto::acquire_test_env_read_lock`'s doc.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let state = state();
        let sql = |q: &str| Method::Sql {
            query: q.into(),
            params_msgpack: Vec::new(),
        };

        // `node_type` alongside `type`: `eg_query::cypher::exec::node_has_label`
        // deliberately treats a bare `type`/`label` property as a legacy payload
        // that "must never satisfy a Cypher node label" (its own doc comment,
        // matching Cypher's `CREATE`/`MERGE` canonicalization onto `node_type`
        // only) — a documented, intentional divergence from the broader `type`/
        // `node_type`/`label` convention `GraphCore::labels_of` (and this SQL
        // surface) use. This test's whole point is cross-surface visibility (SQL
        // write → Cypher read), so the inserted row carries both: `type` for the
        // native/SQL convention, `node_type` for Cypher's label index.
        let i = dispatch_on_heap(
            &state,
            req(
                1,
                sql("INSERT INTO nodes (id, type, node_type, name) VALUES \
                     ('sqlnode', 'Gadget', 'Gadget', 'Zed')"),
            ),
        )
        .await;
        assert!(i.error.is_none(), "INSERT INTO nodes failed: {:?}", i.error);

        // SELECT over the graph projection sees the new node.
        let s = dispatch_on_heap(
            &state,
            req(2, sql("SELECT id FROM nodes WHERE id = 'sqlnode'")),
        )
        .await;
        assert!(s.error.is_none(), "SELECT failed: {:?}", s.error);
        let (_c, rows) = query_result(&s);
        assert_eq!(
            rows.len(),
            1,
            "the SQL-inserted node is visible to a SELECT"
        );
        assert_eq!(rows[0][0], serde_json::json!("sqlnode"));

        // And a Cypher read sees it too (cross-surface).
        let cy = dispatch_on_heap(
            &state,
            req(
                3,
                Method::CypherQuery {
                    query: "MATCH (n:Gadget) RETURN n.name".into(),
                    mode: crate::protocol::CypherMode::Read,
                },
            ),
        )
        .await;
        assert!(cy.error.is_none(), "cypher read failed: {:?}", cy.error);
        let (_c2, rows2) = query_result(&cy);
        assert_eq!(rows2.len(), 1);
        assert_eq!(rows2[0][0], serde_json::json!("Zed"));
    }

    /// CX WB1-EG-01 characterization: `UPDATE nodes SET …` and `DELETE FROM
    /// nodes …` over `Method::Sql` had no dedicated dispatch-level test before
    /// this refactor extracted `exec_sql_write_update_nodes`/
    /// `exec_sql_write_delete_nodes` out of `exec_sql_write`'s
    /// `K::UpdateNodes`/`K::DeleteNodes` arms — added per the Phase B
    /// discipline ("write one for a branch nothing covers before moving it").
    #[tokio::test]
    async fn wire_sql_update_then_delete_node_via_dispatch() {
        // Reads the ambient encryption env at its durable open, so the env must hold
        // still for this whole body. READ guard: it excludes only a key MUTATOR, never
        // another opener. See `crate::crypto::acquire_test_env_read_lock`'s doc.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let state = state();
        let sql = |q: String| Method::Sql {
            query: q,
            params_msgpack: Vec::new(),
        };
        let id = format!("sqlupd_{}", std::process::id());

        let i = dispatch_on_heap(
            &state,
            req(
                1,
                sql(format!(
                    "INSERT INTO nodes (id, type, rank) VALUES ('{id}', 'Agent', 1)"
                )),
            ),
        )
        .await;
        assert!(i.error.is_none(), "INSERT failed: {:?}", i.error);

        // `UPDATE nodes SET …` (`exec_sql_write_update_nodes`).
        let u = dispatch_on_heap(
            &state,
            req(
                2,
                sql(format!("UPDATE nodes SET rank = 42 WHERE id = '{id}'")),
            ),
        )
        .await;
        assert!(u.error.is_none(), "UPDATE failed: {:?}", u.error);

        let s = dispatch_on_heap(
            &state,
            req(3, sql(format!("SELECT rank FROM nodes WHERE id = '{id}'"))),
        )
        .await;
        assert!(
            s.error.is_none(),
            "SELECT after UPDATE failed: {:?}",
            s.error
        );
        let (_c, rows) = query_result(&s);
        assert_eq!(rows.len(), 1, "the updated node is still present");
        assert_eq!(rows[0][0], serde_json::json!(42));

        // `DELETE FROM nodes …` (`exec_sql_write_delete_nodes`).
        let d = dispatch_on_heap(
            &state,
            req(4, sql(format!("DELETE FROM nodes WHERE id = '{id}'"))),
        )
        .await;
        assert!(d.error.is_none(), "DELETE failed: {:?}", d.error);

        let s2 = dispatch_on_heap(
            &state,
            req(5, sql(format!("SELECT id FROM nodes WHERE id = '{id}'"))),
        )
        .await;
        assert!(
            s2.error.is_none(),
            "SELECT after DELETE failed: {:?}",
            s2.error
        );
        let (_c2, rows2) = query_result(&s2);
        assert_eq!(rows2.len(), 0, "the deleted node is gone");
    }
}

// ── In-transaction cross-modal read-your-own-writes (CONCEPT:EG-KG.query.txn-cross-modal-ryow) ──────────
// End-to-end dispatch tests for the `TxnUnifiedQuery{,Text}` overlay path: a txn's
// STAGED (uncommitted) node + embedding + edge are visible to a unified cross-modal
// query issued INSIDE that txn (RYOW), while an identical OFF-txn query sees nothing
// until COMMIT. Drives the real `dispatch` shell, so it exercises begin → stage →
// overlaid query → commit exactly as a client would.
#[cfg(all(test, feature = "query", feature = "redb", feature = "security"))]
mod txn_ryow_dispatch_tests {
    use super::current_auth_test_support::prelude::*;
    use serde_json::json;

    const SECRET: &str = "txn-ryow-test-secret";

    fn state() -> Arc<RwLock<ServerState>> {
        // Post-FLIP every dispatch-served mutation (including a transaction
        // commit) is authoritative (commit-before-ack), so it REQUIRES a
        // persistence backend — a backendless fixture rejects the commit
        // ("transaction commit requires an authoritative MutationBatch backend")
        // before the read-your-own-writes path under test is ever reached.
        // Mirrors `result_cache_dispatch_tests::state`'s already-fixed fixture.
        //
        // A second, layered requirement: the cross-modal handler-commit path seals
        // its transaction recovery plan (`server::handlers::txn::seal_txn_recovery_plan`),
        // which fail-closed REQUIRES `EPISTEMIC_GRAPH_ENCRYPTION_KEY` to be configured —
        // the SAME seal requirement `redb_backend::tests::cm_dir` already documents and
        // provisions for its own cross-modal ACID tests. Mirror that exact pattern (a
        // `std::sync::Once`-guarded env var set, since it is process-global).
        // The `Once` below sets a process-global env var, and `EPISTEMIC_GRAPH_
        // ENCRYPTION_KEY` must then stay stable for the rest of this test (a
        // `RedbBackend` resolves+caches its cipher ONCE at `open()`, called right
        // below). Both callers of `state()` (`in_txn_cross_modal_ryow`,
        // `commit_makes_txn_writes_visible_off_txn`) hold `crate::crypto::
        // acquire_test_env_lock()` for their ENTIRE body — see that lock's doc — so
        // this `Once` always fires (or, after the first caller, is a no-op check)
        // inside that held lock. Do NOT also acquire the lock here: `std::sync::
        // Mutex` is not reentrant, and the caller already holds it.
        crate::crypto::provision_test_at_rest_key_under_write_guard();
        persisted_state(SECRET, current_isolation())
    }

    fn req(id: u64, method: Method) -> Request {
        current_request(SECRET, id, "__commons__", method)
    }

    fn pack(v: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&v).unwrap()
    }

    /// Decode a unified-query response into its result node ids.
    fn unified_ids(resp: &Response) -> Vec<String> {
        crate::server::decode_unified_ids(resp)
    }

    async fn begin(state: &Arc<RwLock<ServerState>>, id: u64) -> String {
        let r = dispatch_on_heap(
            state,
            req(
                id,
                Method::BeginTxn {
                    graph: None,
                    isolation: None,
                },
            ),
        )
        .await;
        match r.result {
            Some(ResultPayload::String(s)) => s,
            other => panic!("BeginTxn failed: {:?} / {other:?}", r.error),
        }
    }

    async fn ok(state: &Arc<RwLock<ServerState>>, id: u64, method: Method) {
        let r = dispatch_on_heap(state, req(id, method)).await;
        assert!(r.error.is_none(), "stage op {id} failed: {:?}", r.error);
    }

    // Cross-modal RYOW: staged node + embedding rank in-txn; staged edge is BFS-
    // reachable in-txn; an identical OFF-txn query sees none of it.
    #[tokio::test]
    async fn in_txn_cross_modal_ryow() {
        // Held for the whole test: `state()` provisions `EPISTEMIC_GRAPH_ENCRYPTION_KEY`
        // once (process-global) and this test's transaction-commit path depends on it
        // staying set throughout — see `crate::crypto::acquire_test_env_lock`'s doc.
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let state = state();
        let txn = begin(&state, 1).await;
        // Stage: node `sn` (Widget) + its embedding, node `tn` (Gadget), edge sn→tn.
        ok(
            &state,
            2,
            Method::TxnAddNode {
                txn_id: txn.clone(),
                node_id: "sn".into(),
                properties_msgpack: pack(json!({"type": "Widget"})),
                graph: None,
            },
        )
        .await;
        ok(
            &state,
            3,
            Method::TxnAddEmbedding {
                txn_id: txn.clone(),
                node_id: "sn".into(),
                embedding: vec![1.0, 0.0],
                graph: None,
            },
        )
        .await;
        ok(
            &state,
            4,
            Method::TxnAddNode {
                txn_id: txn.clone(),
                node_id: "tn".into(),
                properties_msgpack: pack(json!({"type": "Gadget"})),
                graph: None,
            },
        )
        .await;
        ok(
            &state,
            5,
            Method::TxnAddEdge {
                txn_id: txn.clone(),
                source_id: "sn".into(),
                target_id: "tn".into(),
                properties_msgpack: pack(json!({"relationship": "LINKS"})),
                graph: None,
            },
        )
        .await;

        // IN-TXN cross-modal (graph label Scan fused with staged-vector Rank): sees sn.
        let vec_q = "MATCH (:Widget) |> RANK BY ~[1.0,0.0] |> LIMIT 5";
        let in_txn = dispatch_on_heap(
            &state,
            req(
                6,
                Method::TxnUnifiedQueryText {
                    txn_id: txn.clone(),
                    text: vec_q.into(),
                },
            ),
        )
        .await;
        assert_eq!(
            unified_ids(&in_txn),
            vec!["sn".to_string()],
            "staged node + embedding must be visible to the in-txn cross-modal query"
        );

        // IN-TXN traverse over the STAGED edge: reaches tn.
        let trav_q = "MATCH (:Widget) |> TRAVERSE -[:LINKS]->{1,1} |> LIMIT 5";
        let trav = dispatch_on_heap(
            &state,
            req(
                7,
                Method::TxnUnifiedQueryText {
                    txn_id: txn.clone(),
                    text: trav_q.into(),
                },
            ),
        )
        .await;
        assert!(
            unified_ids(&trav).contains(&"tn".to_string()),
            "staged edge must make tn BFS-reachable in-txn"
        );

        // OFF-TXN identical query: empty — staged writes are invisible before commit.
        let off = dispatch_on_heap(
            &state,
            req(8, Method::UnifiedQueryText { text: vec_q.into() }),
        )
        .await;
        assert!(
            unified_ids(&off).is_empty(),
            "off-txn query must see none of the txn's uncommitted writes"
        );
    }

    // The "until COMMIT" half: a graph-only txn (no vectors → commits in-memory with
    // no persistence backend) is invisible off-txn before commit, visible after.
    #[tokio::test]
    async fn commit_makes_txn_writes_visible_off_txn() {
        // See `in_txn_cross_modal_ryow` above: held for the whole test.
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let state = state();
        let txn = begin(&state, 1).await;
        ok(
            &state,
            2,
            Method::TxnAddNode {
                txn_id: txn.clone(),
                node_id: "cn".into(),
                properties_msgpack: pack(json!({"type": "Committed"})),
                graph: None,
            },
        )
        .await;
        let q = "MATCH (:Committed) |> LIMIT 5";

        // Before commit: off-txn empty, in-txn sees it (RYOW).
        let before =
            dispatch_on_heap(&state, req(3, Method::UnifiedQueryText { text: q.into() })).await;
        assert!(
            unified_ids(&before).is_empty(),
            "off-txn empty before commit"
        );
        let in_txn = dispatch_on_heap(
            &state,
            req(
                4,
                Method::TxnUnifiedQueryText {
                    txn_id: txn.clone(),
                    text: q.into(),
                },
            ),
        )
        .await;
        assert_eq!(unified_ids(&in_txn), vec!["cn".to_string()], "RYOW in-txn");

        // Commit, then the same OFF-txn query now sees the committed node.
        let c = dispatch_on_heap(
            &state,
            req(
                5,
                Method::Commit {
                    txn_id: txn.clone(),
                    idempotency_key: None,
                },
            ),
        )
        .await;
        assert!(
            matches!(
                c.result,
                Some(ResultPayload::Json(serde_json::Value::Bool(true)))
            ),
            "commit must succeed: {:?}",
            c.error
        );
        let after =
            dispatch_on_heap(&state, req(6, Method::UnifiedQueryText { text: q.into() })).await;
        assert_eq!(
            unified_ids(&after),
            vec!["cn".to_string()],
            "committed node must be visible off-txn after commit"
        );
    }
}

// ── SURPASS gap-closure: "unify the two evidence resolvers" ──────────────────────
// `explain_evidence_wire`'s `alignment`-gated variant must ACTUALLY call
// `CasEvidenceResolver` and attach the resolved content to each citation — before
// this, `CasEvidenceResolver` had zero served-RPC call sites (only its own unit
// tests in `src/server/blob/cas_resolver.rs` exercised it). These tests drive
// `explain_evidence_wire` directly (the same function `Method::ExplainEvidence`'s
// handler arm calls), over a real `RedbChunkStore`, so a regression that silently
// drops `resolved` back to always-`None` fails here, not just in the resolver's own
// isolated unit tests.
#[cfg(all(test, feature = "evidence-graph", feature = "alignment"))]
mod evidence_resolver_wiring_tests {
    use super::explain_evidence_wire;
    use crate::graph::GraphCore;
    use crate::server::blob::stream::stream_blob_put;
    use crate::server::blob::{ChunkStore, RedbChunkStore};
    use serde_json::json;
    use std::sync::Arc;

    fn node_blob(v: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&v).unwrap()
    }

    fn edge_blob(relationship: &str) -> Vec<u8> {
        rmp_serde::to_vec_named(&json!({ "relationship": relationship })).unwrap()
    }

    const SUBJECT: &str = "eg:artifact:0000000000000002";

    fn locus(address: serde_json::Value) -> serde_json::Value {
        json!({
            "id": "eg:locus:0000000000000001",
            "subject": { "kind": "artifact", "id": SUBJECT },
            "address": address,
            "policy_ref": "eg:policy:0000000000000003",
            "derivation_ref": "eg:derivation:0000000000000004"
        })
    }

    /// A `CharacterRange` citation resolves to the REAL text excerpt read back out of
    /// the blob CAS -- `explain_evidence_wire` must thread the configured blob store
    /// all the way through to `EvidenceCitationWire::resolved`, not just return the
    /// locus metadata `evidence_citation_wire`'s `alignment`-less twin would.
    #[tokio::test]
    async fn explain_evidence_wire_attaches_a_real_resolved_excerpt() {
        let cas: Arc<dyn ChunkStore> = Arc::new(RedbChunkStore::open_temp().unwrap());
        let committed = stream_blob_put(cas.as_ref(), "hello world".as_bytes(), 0).unwrap();

        let core = GraphCore::new();
        core.add_node(
            SUBJECT.into(),
            node_blob(json!({ "node_type": "Document", "blob_ref": committed.digest })),
        );
        core.add_node(
            "claim1".into(),
            node_blob(json!({ "type": "Claim", "confidence": 0.5 })),
        );
        core.add_node(
            "evidence1".into(),
            node_blob(json!({
                "type": "Evidence",
                "confidence": 0.9,
                "evidence_locus": locus(json!({
                    "kind": "character_range", "start": 0, "end": 5
                })),
            })),
        );
        core.add_edge("evidence1".into(), "claim1".into(), edge_blob("SUPPORTS"))
            .unwrap();

        let view = core.analysis_snapshot();
        let result = explain_evidence_wire("claim1", &view, Some(cas));

        assert_eq!(result.citations.len(), 1);
        let citation = &result.citations[0];
        assert_eq!(citation.evidence_id, "evidence1");
        let resolved = citation
            .resolved
            .as_ref()
            .expect("character-range citation must resolve through the configured blob store");
        assert_eq!(resolved.kind, "text");
        assert_eq!(resolved.subject_ref, SUBJECT);
        assert_eq!(resolved.excerpt.as_deref(), Some("hello"));
    }

    /// No blob store configured (`None`, e.g. an in-memory/no-persist-dir deployment)
    /// -- `resolved` stays `None` on every citation, exactly the pre-existing
    /// locus-only behavior. Never an error, never a fabricated resolution.
    #[tokio::test]
    async fn explain_evidence_wire_leaves_resolved_none_without_a_blob_store() {
        let core = GraphCore::new();
        core.add_node(
            "claim1".into(),
            node_blob(json!({ "type": "Claim", "confidence": 0.5 })),
        );
        core.add_node(
            "evidence1".into(),
            node_blob(json!({
                "type": "Evidence",
                "confidence": 0.9,
                "evidence_locus": locus(json!({
                    "kind": "character_range", "start": 0, "end": 5
                })),
            })),
        );
        core.add_edge("evidence1".into(), "claim1".into(), edge_blob("SUPPORTS"))
            .unwrap();

        let view = core.analysis_snapshot();
        let result = explain_evidence_wire("claim1", &view, None);

        assert_eq!(result.citations.len(), 1);
        assert_eq!(result.citations[0].resolved, None);
    }

    /// A `CodeSymbol` citation resolves to the REAL line-range excerpt -- proving
    /// the wiring covers the newly-added `CodeSymbol` codec path too, not just
    /// `CharacterRange`.
    #[tokio::test]
    async fn explain_evidence_wire_attaches_a_real_code_symbol_excerpt() {
        let cas: Arc<dyn ChunkStore> = Arc::new(RedbChunkStore::open_temp().unwrap());
        let source = "fn a() {}\nfn b() {\n    1 + 1\n}\n";
        let committed = stream_blob_put(cas.as_ref(), source.as_bytes(), 0).unwrap();

        let core = GraphCore::new();
        core.add_node(
            SUBJECT.into(),
            node_blob(json!({ "node_type": "Code", "blob_ref": committed.digest })),
        );
        core.add_node(
            "claim1".into(),
            node_blob(json!({ "type": "Claim", "confidence": 0.5 })),
        );
        core.add_node(
            "evidence1".into(),
            node_blob(json!({
                "type": "Evidence",
                "confidence": 0.9,
                "evidence_locus": locus(json!({
                        "kind": "code_symbol",
                        "revision_ref": "eg:revision:0000000000000005",
                        "symbol_ref": "eg:symbol:0000000000000006",
                        "start_line": 1,
                        "end_line": 4
                })),
            })),
        );
        core.add_edge("evidence1".into(), "claim1".into(), edge_blob("SUPPORTS"))
            .unwrap();

        let view = core.analysis_snapshot();
        let result = explain_evidence_wire("claim1", &view, Some(cas));

        let resolved = result.citations[0]
            .resolved
            .as_ref()
            .expect("CodeSymbol citation must resolve");
        assert_eq!(resolved.kind, "text");
        assert_eq!(resolved.excerpt.as_deref(), Some("fn b() {\n    1 + 1\n}"));
    }
}
