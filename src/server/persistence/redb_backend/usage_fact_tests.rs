use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn usage_fact_page_survives_eviction_and_restart() {
    let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
    let dir = std::env::temp_dir().join(format!(
        "eg-usage-facts-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let dir_s = dir.to_string_lossy().to_string();
    let graph = "usage-fact-test";
    let tenant = "carrier-tenant:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let prefix = format!("usage:event:{tenant}:");
    let ids = [
        format!("{prefix}pref_usage_dedup_{}", "1".repeat(64)),
        format!("{prefix}pref_usage_dedup_{}", "2".repeat(64)),
    ];
    let backend = RedbBackend::open(dir_s.clone(), 64).expect("open");
    backend
        .register_graph(graph, graph, GraphType::Global)
        .await
        .expect("register");
    let core = GraphCore::new();
    for id in &ids {
        let properties = rmp_serde::to_vec_named(&serde_json::json!({
            "type": "UsageEvent",
            "schema": "usage-event-fact-v1",
            "tenant_ref": tenant,
            "input_tokens": 1,
        }))
        .unwrap();
        backend
            .record_durable(
                graph,
                &Method::AddNode {
                    node_id: id.clone(),
                    properties_msgpack: properties.clone(),
                },
            )
            .await
            .expect("durable insert");
        core.add_node(id.clone(), properties);
    }
    assert_eq!(core.evict_resident_nodes(&ids), 2);
    assert!(core
        .get_nodes_by_label_page("UsageEvent", Some(&prefix), 2)
        .is_empty());
    let page = backend
        .read_usage_fact_nodes(graph, &prefix, &prefix, 3)
        .expect("durable page")
        .expect("redb index");
    assert_eq!(page.len(), 2);
    assert_eq!(page[0].0, ids[0]);
    assert_eq!(page[1].0, ids[1]);
    backend.shutdown();
    drop(backend);

    let reopened = RedbBackend::open(dir_s.clone(), 64).expect("reopen");
    let page = reopened
        .read_usage_fact_nodes(graph, &prefix, &prefix, 3)
        .expect("restarted durable page")
        .expect("redb index");
    assert_eq!(page.len(), 2);
    let after_first = reopened
        .read_usage_fact_nodes(graph, &prefix, &ids[0], 1)
        .expect("keyset page")
        .expect("redb index");
    assert_eq!(after_first[0].0, ids[1]);
    reopened.shutdown();
    drop(reopened);
    std::fs::remove_dir_all(dir).expect("cleanup");
}
