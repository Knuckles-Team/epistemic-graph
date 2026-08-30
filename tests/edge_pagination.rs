//! CONCEPT:EG-KG.ingest.resets-socket-so-assimilation — `GetEdgesPage` keyset pagination over the REAL
//! served `dispatch` surface (the edge-count sibling of `GetNodesByLabel`'s
//! coverage). Proves a full page walk (small `limit`, advancing the cursor)
//! recovers EXACTLY the same edges — including every parallel edge under one
//! `(source, target)` pair — as the unbounded `GetEdges` dump, in strictly
//! increasing `(source, target, ordinal)` order with no duplicates or gaps.
// Every test in this file dispatches through the REAL secure-envelope auth
// path (`common::signed_request*` -> `dispatch`), which links against the
// library WITHOUT `cfg(test)` (integration-test crates are separate compilation
// units), so it always hits `durable_replay_ledger`'s production fail-closed
// branch. That branch requires the `security` feature (which also pulls in
// `redb`, satisfying `common::tempdir_persistence`'s durable-backend need).
// Without `security`, every test here fails immediately with "secure request
// context requires the security feature" before it ever reaches pagination
// logic -- this is a genuine capability requirement, not a mis-asserted slim
// test (mirrors the `redb`+`security` precedent in
// `tests/txn_recovery_key_decoupled_d_orc_50.rs`).
#![cfg(all(feature = "server", feature = "security"))]

mod common;
#[path = "common/test_support.rs"]
mod test_support;

use epistemic_graph::protocol::{GraphType, Method, Response, ResultPayload};

const SECRET: &str = "edge-pagination-secret";

fn state() -> test_support::SharedState {
    test_support::durable_state(SECRET, common::current_isolation())
}

async fn add_node(state: &test_support::SharedState, id: u64, node_id: &str) {
    let resp = test_support::dispatch(
        state,
        test_support::request(
            SECRET,
            id,
            "g",
            Method::AddNode {
                node_id: node_id.to_string(),
                properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({"type": "Doc"}))
                    .unwrap(),
            },
        ),
    )
    .await;
    assert!(resp.error.is_none(), "add_node {node_id}: {:?}", resp.error);
}

async fn add_edge(state: &test_support::SharedState, id: u64, src: &str, tgt: &str, tag: &str) {
    let resp = test_support::dispatch(
        state,
        test_support::request(
            SECRET,
            id,
            "g",
            Method::AddEdge {
                source_id: src.to_string(),
                target_id: tgt.to_string(),
                properties_msgpack: test_support::edge_properties(tag),
            },
        ),
    )
    .await;
    assert!(
        resp.error.is_none(),
        "add_edge {src}->{tgt} ({tag}): {:?}",
        resp.error
    );
}

fn edge_list_rows(resp: &Response) -> Vec<(String, String, Vec<u8>)> {
    assert!(resp.error.is_none(), "GetEdges: {:?}", resp.error);
    match &resp.result {
        Some(ResultPayload::EdgeList(rows)) => rows.clone(),
        other => panic!("expected EdgeList, got {other:?}"),
    }
}

fn page_rows(resp: &Response) -> Vec<(String, String, u32, Vec<u8>)> {
    assert!(resp.error.is_none(), "GetEdgesPage: {:?}", resp.error);
    match &resp.result {
        Some(ResultPayload::Raw(bytes)) => rmp_serde::from_slice(bytes).unwrap(),
        other => panic!("expected Raw, got {other:?}"),
    }
}

/// K=1 pages (`limit=1`) walked with the cursor threaded through recover EXACTLY
/// the same edges as the unbounded `GetEdges` dump — including both rows of a
/// parallel a->b edge pair — in strictly increasing `(source, target, ordinal)`
/// order, with no duplicate or skipped row.
#[tokio::test]
async fn edges_page_recovers_every_edge_including_parallel_edges_in_order() {
    let state = state();
    {
        let s = &mut *state.write().await;
        s.registry
            .create_graph("g", GraphType::Commons, None)
            .unwrap();
    }
    for (i, n) in ["a", "b", "c", "d"].into_iter().enumerate() {
        add_node(&state, i as u64, n).await;
    }
    // a->b TWICE (parallel edges under one (source, target) pair), plus a->d,
    // plus b->c.
    add_edge(&state, 10, "a", "b", "first").await;
    add_edge(&state, 11, "a", "b", "second").await;
    add_edge(&state, 12, "a", "d", "only").await;
    add_edge(&state, 13, "b", "c", "only").await;

    // Reference: the unbounded dump (well under the cap, so this is unaffected
    // by the new oversize guard).
    let full = test_support::dispatch(
        &state,
        test_support::request(SECRET, 20, "g", Method::GetEdges),
    )
    .await;
    let mut full_rows = edge_list_rows(&full);
    full_rows.sort();
    assert_eq!(
        full_rows.len(),
        4,
        "sanity: 4 edges total (incl. 1 parallel pair)"
    );

    // Page through with limit=1, threading the (source, target, ordinal) cursor.
    let mut after: Option<(String, String, u32)> = None;
    let mut paged: Vec<(String, String, u32, Vec<u8>)> = Vec::new();
    let mut next_id = 100u64;
    loop {
        let resp = test_support::dispatch(
            &state,
            test_support::request(
                SECRET,
                next_id,
                "g",
                Method::GetEdgesPage {
                    after: after.clone(),
                    limit: 1,
                },
            ),
        )
        .await;
        next_id += 1;
        let rows = page_rows(&resp);
        if rows.is_empty() {
            break;
        }
        assert_eq!(
            rows.len(),
            1,
            "limit=1 must return at most one row per page"
        );
        let (s, t, ord, _) = rows[0].clone();
        after = Some((s, t, ord));
        paged.extend(rows);
        assert!(
            paged.len() <= full_rows.len(),
            "pagination did not terminate at the true edge count"
        );
    }

    assert_eq!(paged.len(), 4, "must recover exactly 4 edges over 4 pages");
    // Strictly increasing (source, target, ordinal) across the whole walk — no
    // duplicate row and no page-boundary skip, even across the parallel pair.
    for w in paged.windows(2) {
        let a = (w[0].0.as_str(), w[0].1.as_str(), w[0].2);
        let b = (w[1].0.as_str(), w[1].1.as_str(), w[1].2);
        assert!(
            a < b,
            "page rows must be strictly increasing: {a:?} then {b:?}"
        );
    }
    // Dropping the ordinal, the paged (source, target, properties) triples equal
    // the full unbounded dump as a multiset.
    let mut paged_triples: Vec<(String, String, Vec<u8>)> =
        paged.into_iter().map(|(s, t, _, p)| (s, t, p)).collect();
    paged_triples.sort();
    assert_eq!(paged_triples, full_rows);
}

/// `limit == 0` is uncapped — one call returns every edge, matching `GetEdges`.
#[tokio::test]
async fn edges_page_limit_zero_returns_everything_in_one_call() {
    let state = state();
    {
        let s = &mut *state.write().await;
        s.registry
            .create_graph("g", GraphType::Commons, None)
            .unwrap();
    }
    for n in ["a", "b", "c"] {
        add_node(&state, 0, n).await;
    }
    add_edge(&state, 1, "a", "b", "only").await;
    add_edge(&state, 2, "a", "c", "only").await;

    let resp = test_support::dispatch(
        &state,
        test_support::request(
            SECRET,
            3,
            "g",
            Method::GetEdgesPage {
                after: None,
                limit: 0,
            },
        ),
    )
    .await;
    let rows = page_rows(&resp);
    assert_eq!(rows.len(), 2, "limit=0 must return every edge uncapped");
}

/// An empty graph pages cleanly to an empty first page (no panic, no phantom
/// rows) — the boundary the loop-termination logic above depends on.
#[tokio::test]
async fn edges_page_on_empty_graph_returns_empty_first_page() {
    let state = state();
    {
        let s = &mut *state.write().await;
        s.registry
            .create_graph("g", GraphType::Commons, None)
            .unwrap();
    }
    let resp = test_support::dispatch(
        &state,
        test_support::request(
            SECRET,
            1,
            "g",
            Method::GetEdgesPage {
                after: None,
                limit: 10,
            },
        ),
    )
    .await;
    assert!(page_rows(&resp).is_empty());
}
