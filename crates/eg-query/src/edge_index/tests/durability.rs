//! EH-351 / EH-352: an edge index registered in the tenant's catalog survives
//! a restart without a rebuild — its persisted generation is restored and
//! reconciled against the graph as it is now — and a drop removes the
//! registration and every generation.

use super::*;
use crate::tables::TableStore;

// spec: EG-FEDERATED-QUERY-R009
#[test]
fn a_durable_edge_index_is_restored_after_a_restart_without_a_rebuild() {
    let (store, _path) = TableStore::open_temp().unwrap();
    let core = parallel_graph();
    create_durable_edge_index(&store, "g", &core, vector_spec("rel_emb")).unwrap();
    refresh_durable_edge_index(&store, "g", &core, "rel_emb").unwrap();

    // The restarted process: the same graph, with one pair replaced meanwhile.
    let reloaded = parallel_graph();
    commit(
        &reloaded,
        &[("a", "c")],
        &[("a", "c", json!({"emb": [6.0, 6.0, 6.0, 6.0]}))],
    );
    assert_eq!(install_edge_indexes(&store, "g", &reloaded).unwrap(), 1);

    let status = &reloaded.indexes().managed_statuses()[0];
    assert_eq!(
        (status.state, status.generation),
        (ManagedIndexState::Active, Some(1)),
        "the persisted generation serves at once"
    );
    let answer = search(
        &reloaded,
        "rel_emb",
        EdgeQuery::Vector(&[6.0; DIM]),
        1,
        None,
    );
    assert_eq!(answer.path, Ok(1), "no rebuild");
    assert_eq!(
        keys(&answer),
        vec![key("a", "c", 0)],
        "a pair changed before the restore is reconciled and scored exactly"
    );
    assert_eq!(install_edge_indexes(&store, "g", &reloaded).unwrap(), 0);
}

// spec: EG-FEDERATED-QUERY-R009
#[test]
fn a_durable_drop_removes_the_registration_and_every_generation() {
    let (store, _path) = TableStore::open_temp().unwrap();
    let core = parallel_graph();
    create_durable_edge_index(&store, "g", &core, vector_spec("rel_emb")).unwrap();
    let again = create_durable_edge_index(&store, "g", &core, vector_spec("rel_emb")).unwrap_err();
    assert_eq!(again.reason, IndexBlockReason::NotIndexable);
    refresh_durable_edge_index(&store, "g", &core, "rel_emb").unwrap();
    assert!(store.edge_generation("g", "rel_emb").unwrap().is_some());

    assert!(drop_durable_edge_index(&store, "g", &core, "rel_emb").unwrap());

    assert!(edge_index(&core, "rel_emb").is_none());
    assert!(store.edge_generation("g", "rel_emb").unwrap().is_none());
    assert_eq!(
        install_edge_indexes(&store, "g", &parallel_graph()).unwrap(),
        0
    );
    assert!(!drop_durable_edge_index(&store, "g", &core, "rel_emb").unwrap());
}
