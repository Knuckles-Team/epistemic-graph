//! MEDULLA-FEDERATED-QUERY-R065: prove the DAG EXPLAIN/PROFILE contract through
//! the served `Method::Uql` path, rather than only through `medulla-plan`.
// spec: MEDULLA-FEDERATED-QUERY-R065
#![cfg(all(feature = "query", feature = "redb", feature = "security"))]

mod common;
#[path = "common/test_support.rs"]
mod test_support;

use medulla_types::wire::UqlResult;
use medulla::protocol::{Method, Response, ResultPayload};

const SECRET: &str = "uql-dag-served-test-secret";

fn state() -> test_support::SharedState {
    let (persist_dir, persistence) = common::tempdir_persistence();
    test_support::state_with(
        SECRET,
        common::current_isolation(),
        persist_dir,
        persistence,
    )
}

async fn add_doc(state: &test_support::SharedState, id: u64, node: &str, year: i64) {
    let response = test_support::dispatch(
        state,
        test_support::commons_request(
            SECRET,
            id,
            Method::AddNode {
                node_id: node.to_string(),
                properties_msgpack: test_support::json_bytes(
                    serde_json::json!({"type": "Doc", "year": year}),
                ),
            },
        ),
    )
    .await;
    assert!(
        response.error.is_none(),
        "AddNode {node} failed: {:?}",
        response.error
    );
}

async fn add_cites(state: &test_support::SharedState, id: u64, source: &str, target: &str) {
    let response = test_support::dispatch(
        state,
        test_support::commons_request(
            SECRET,
            id,
            Method::AddEdge {
                source_id: source.to_string(),
                target_id: target.to_string(),
                properties_msgpack: test_support::json_bytes(
                    serde_json::json!({"relationship": "CITES"}),
                ),
            },
        ),
    )
    .await;
    assert!(
        response.error.is_none(),
        "AddEdge {source}->{target} failed: {:?}",
        response.error
    );
}

async fn uql(state: &test_support::SharedState, id: u64, text: &str) -> Response {
    test_support::dispatch(
        state,
        test_support::commons_request(
            SECRET,
            id,
            Method::Uql {
                text: text.to_string(),
                params: Default::default(),
                prepare: false,
                foreign_execution: None,
            },
        ),
    )
    .await
}

fn decode(response: Response) -> UqlResult {
    assert!(response.error.is_none(), "UQL failed: {:?}", response.error);
    match response.result {
        Some(ResultPayload::Raw(bytes)) => {
            rmp_serde::from_slice(&bytes).expect("decode served UQL result")
        }
        other => panic!("expected Raw UQL result, got {other:?}"),
    }
}

/// The source-level DAG checks already cover materialization and execution. This test
/// closes the served seam by sending the same named-subplan program through `Method::Uql`.
#[tokio::test]
async fn served_dag_explain_and_profile_report_each_node() {
    let state = state();
    for (id, node, year) in [
        (1, "d1", 2020),
        (2, "d2", 2021),
        (3, "d3", 2022),
        (4, "d4", 2023),
    ] {
        add_doc(&state, id, node, year).await;
    }
    add_cites(&state, 5, "d1", "d2").await;
    add_cites(&state, 6, "d2", "d3").await;

    let program = "LET docs = MATCH (:Doc); LET cited = FROM docs |> TRAVERSE CITES; \
                   JOIN docs, cited |> RERANK MENTIONS |> RETURN mentions";

    let explain = decode(uql(&state, 7, &format!("EXPLAIN {program}")).await);
    let UqlResult::Explain {
        canonical,
        optimized,
        stages,
        incremental,
        incremental_note,
        ..
    } = explain
    else {
        panic!("expected served EXPLAIN result")
    };
    assert_eq!(canonical, optimized, "served DAG runs as written");
    assert_eq!(stages.len(), 4, "one served EXPLAIN stage per DAG node");
    assert!(stages[0].stage.starts_with("#0 MATCH (:Doc)"));
    assert!(stages[2].stage.starts_with("#2 <- #0,#1 RERANK MENTIONS"));
    assert!(stages
        .iter()
        .all(|stage| stage.rows.is_none() && stage.micros.is_none()));
    assert!(!incremental);
    assert!(incremental_note.contains("no cost"));
    assert!(incremental_note.contains("not incrementally maintained"));

    let profile = decode(uql(&state, 8, &format!("PROFILE {program}")).await);
    let UqlResult::Profile {
        rows,
        columns,
        stages,
        ..
    } = profile
    else {
        panic!("expected served PROFILE result")
    };
    assert_eq!(columns, vec!["mentions"]);
    assert_eq!(stages.len(), 4, "one served PROFILE stage per DAG node");
    assert_eq!(
        stages.iter().map(|stage| stage.rows).collect::<Vec<_>>(),
        vec![Some(4), Some(2), Some(2), Some(2)]
    );
    assert!(stages.iter().all(|stage| stage.micros.is_some()));
    assert_eq!(rows.len(), 2, "served DAG result keeps the joined rows");
    assert!(rows.iter().all(|row| row.channels == vec![Some(1.0)]));
}
