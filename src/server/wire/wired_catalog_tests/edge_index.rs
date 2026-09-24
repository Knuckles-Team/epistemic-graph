//! EH-352 over the served surfaces: `DROP INDEX [IF EXISTS]` never discloses an
//! index across an authorization boundary, and the request graph's edge indexes
//! reach `information_schema.eg_index_status` on the pgwire and Method::Sql
//! reads (the KnowledgeStream read is proven in `handlers::knowledge_stream`'s
//! tests) through the one shared projection builder.

use super::*;
use crate::server::handlers::query::current_auth_test_support::{
    edge_status_row, register_detached_edge_index, sql_rows, EDGE_STATUS_SQL,
};

const TENANT: &str = "tenant-shared";

async fn outcome(session: &WireSession, sql: &str) -> Result<String, String> {
    session
        .execute(sql)
        .await
        .map(|outcome| format!("{outcome:?}"))
        .map_err(|error| error.message)
}

async fn registered_store(state: &Arc<RwLock<ServerState>>, agent: &str) -> (String, TableStore) {
    let tenant = authority(agent, TENANT).tenant_scope().to_string();
    let persist_dir = test_persist_dir_of(state).await;
    let store = crate::server::sql_tables::tenant_table_store(&tenant, &persist_dir).unwrap();
    (tenant, store)
}

#[tokio::test]
async fn drop_index_never_discloses_an_index_across_authorization() {
    let _env_read_lock = crate::crypto::provisioned_test_env_read_lock().await;
    let state = test_state(&[CREATOR, "idx-owner", "idx-stranger"]);
    let graph = "eh352-drop-index-graph";
    let owner = session_for(state.clone(), graph, "idx-owner", TENANT);
    let stranger = session_for(state.clone(), graph, "idx-stranger", TENANT);
    owner
        .execute("CREATE TABLE docs (id TEXT PRIMARY KEY, emb vector(4))")
        .await
        .unwrap();
    owner
        .execute("CREATE INDEX docs_emb ON docs USING hnsw (emb vector_l2_ops)")
        .await
        .unwrap();

    assert_eq!(
        outcome(&stranger, "DROP INDEX IF EXISTS docs_emb").await,
        outcome(&stranger, "DROP INDEX IF EXISTS no_such_index").await,
        "IF EXISTS answers an index the caller may not alter exactly like a missing one"
    );
    let hidden = outcome(&stranger, "DROP INDEX docs_emb").await.unwrap_err();
    let missing = outcome(&stranger, "DROP INDEX no_such_index")
        .await
        .unwrap_err();
    assert_eq!(
        hidden.replace("docs_emb", "<name>"),
        missing.replace("no_such_index", "<name>"),
        "without IF EXISTS it is the same not-found"
    );
    let (_, store) = registered_store(&state, "idx-owner").await;
    assert!(
        store.ann_index_table("docs_emb").unwrap().is_some(),
        "the stranger dropped nothing"
    );

    owner.execute("DROP INDEX docs_emb").await.unwrap();
    assert!(store.ann_index_table("docs_emb").unwrap().is_none());
}

#[tokio::test]
async fn edge_index_statuses_reach_the_pgwire_read() {
    let _env_read_lock = crate::crypto::provisioned_test_env_read_lock().await;
    let state = test_state(&[CREATOR, "edge-reader"]);
    let graph = "eh352-edge-status-pgwire";
    create_test_graph(&state, graph, 1).await;
    let (tenant, store) = registered_store(&state, "edge-reader").await;
    register_detached_edge_index(&store, graph, &tenant);
    let reader = session_for(state.clone(), graph, "edge-reader", TENANT);

    let rows = read_rows(&reader, EDGE_STATUS_SQL).await.unwrap().rows;

    assert_eq!(rows, vec![edge_status_row()]);
}

#[tokio::test]
async fn edge_index_statuses_reach_the_served_sql_read() {
    let _env_read_lock = crate::crypto::provisioned_test_env_read_lock().await;
    let state = test_state(&[CREATOR]);
    let graph = "eh352-edge-status-sql";
    create_test_graph(&state, graph, 1).await;
    let (tenant, store) = registered_store(&state, CREATOR).await;
    register_detached_edge_index(&store, graph, &tenant);

    let response = crate::server::dispatch(
        &state,
        request(
            2,
            graph,
            Method::Sql {
                query: EDGE_STATUS_SQL.to_string(),
                params_msgpack: Vec::new(),
            },
        ),
    )
    .await;

    assert!(response.error.is_none(), "{:?}", response.error);
    assert_eq!(sql_rows(&response), vec![edge_status_row()]);
}
