//! Uniform-K layout discovery and round-trip migration proofs.

use super::*;

/// CONCEPT:EG-KG.sharding.atomic-shard-swap — migrate a K=1 store with G graphs to K=4, reopen at K=4, and
/// confirm every graph + its nodes/edges survive AND route to the shard the engine
/// looks for them in. The round-trip proof.
#[tokio::test(flavor = "multi_thread")]
async fn roundtrip_k1_to_k4_preserves_all_graphs() {
    // Held for the whole test: it seeds a K=1 backend, migrates it to K=4, then
    // reopens the K=4 layout — every open must resolve the same
    // `EPISTEMIC_GRAPH_ENCRYPTION_KEY` cipher, or the reopen panics with
    // "decryption failed (wrong key or tampered ciphertext)". See
    // `crate::crypto::acquire_test_env_lock`'s doc for the full mechanism.
    #[cfg(feature = "security")]
    let _env_lock = crate::crypto::acquire_test_env_lock().await;
    let root = temp_root("rt");
    let _ = std::fs::remove_dir_all(&root);
    let src = root.join("k1");
    let dst = root.join("k4");
    std::fs::create_dir_all(&src).unwrap();
    let src_s = src.to_string_lossy().to_string();

    let graphs = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta"];
    seed_k1(&src_s, &graphs).await;

    // K=1 uses the canonical indexed layout.
    assert!(src.join("graph-0.redb").exists(), "K=1 shard written");

    // ── migrate K=1 -> K=4 ──
    let report = migrate_shards(&src, &dst, 4).expect("migrate");
    assert_eq!(report.source_shards, 1);
    assert_eq!(report.dest_shards, 4);
    assert_eq!(report.graphs, graphs.len());
    assert_eq!(report.nodes, (graphs.len() * 2) as u64);
    assert_eq!(report.edges, graphs.len() as u64);

    for i in 0..4 {
        assert!(
            dst.join(format!("graph-{i}.redb")).exists(),
            "graph-{i}.redb"
        );
    }

    // ── reopen at K=4 and verify each graph routes + reads back intact ──
    let dst_s = dst.to_string_lossy().to_string();
    let backend = RedbBackend::open(dst_s.clone(), 256).expect("reopen K=4");
    assert_eq!(backend.shard_count(), 4, "on-disk layout honored as K=4");

    for g in &graphs {
        let dump = backend
            .read_graph_dump_blocking(g)
            .expect("read")
            .unwrap_or_else(|| panic!("graph {g} missing after migration"));
        assert_eq!(dump.name, *g);
        assert_eq!(dump.nodes.len(), 2, "graph {g} nodes");
        assert_eq!(dump.edges.len(), 1, "graph {g} edges");
        // The node 'a' carries the graph tag — proves no cross-graph mixing.
        let a = dump
            .nodes
            .iter()
            .find(|(id, _)| id == "a")
            .map(|(_, blob)| blob.clone())
            .expect("node a present");
        let val: serde_json::Value = rmp_serde::from_slice(&a).unwrap();
        assert_eq!(val.get("g").and_then(|x| x.as_str()), Some(*g));
    }
    backend.shutdown();
    let _ = std::fs::remove_dir_all(&root);
}

/// The sole reader for the retired unindexed K=1 FILENAME layout is this explicit
/// offline migration path. Even a K=1 target is rewritten to canonical
/// `graph-0.redb`.
#[test]
fn retired_k1_layout_migrates_to_canonical_k1() {
    // Every store this test builds or reopens resolves the value cipher from
    // the process-global encryption env vars, and `cargo test` runs the whole
    // crate concurrently. Without this lock an unrelated test's transient
    // set_var/remove_var lands between two of those resolutions and flips the
    // cipher -- see `crate::crypto::acquire_test_env_lock`'s doc. Its five
    // sibling in-place migration tests already hold it; these did not.
    let _env_lock = crate::crypto::acquire_test_env_lock_blocking();
    let dir = temp_root("retired-k1");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    empty_shard(&dir.join("graph.redb"));

    let report = migrate_in_place(&dir.to_string_lossy(), 1).unwrap();
    assert_eq!(report.source_shards, 1);
    assert_eq!(report.dest_shards, 1);
    assert!(!dir.join("graph.redb").exists());
    assert!(dir.join("graph-0.redb").exists());

    let backend = RedbBackend::open_with_shards(dir.to_string_lossy().to_string(), 64, 1)
        .expect("canonical migrated layout reopens");
    backend.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn migration_discovery_rejects_mixed_and_sparse_layouts() {
    // Every store this test builds or reopens resolves the value cipher from
    // the process-global encryption env vars, and `cargo test` runs the whole
    // crate concurrently. Without this lock an unrelated test's transient
    // set_var/remove_var lands between two of those resolutions and flips the
    // cipher -- see `crate::crypto::acquire_test_env_lock`'s doc. Its five
    // sibling in-place migration tests already hold it; these did not.
    let _env_lock = crate::crypto::acquire_test_env_lock_blocking();
    let root = temp_root("invalid-layout");
    let mixed = root.join("mixed");
    let sparse = root.join("sparse");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&mixed).unwrap();
    std::fs::create_dir_all(&sparse).unwrap();
    empty_shard(&mixed.join("graph.redb"));
    empty_shard(&mixed.join("graph-0.redb"));
    empty_shard(&sparse.join("graph-0.redb"));
    empty_shard(&sparse.join("graph-2.redb"));

    let mixed_err = discover_source_shards(&mixed).unwrap_err();
    assert!(
        mixed_err.contains("mixed retired and current"),
        "{mixed_err}"
    );
    let sparse_err = discover_source_shards(&sparse).unwrap_err();
    assert!(sparse_err.contains("non-contiguous"), "{sparse_err}");
    let _ = std::fs::remove_dir_all(&root);
}

/// Refuses to clobber an existing destination shard file.
#[test]
fn refuses_existing_destination() {
    // Every store this test builds or reopens resolves the value cipher from
    // the process-global encryption env vars, and `cargo test` runs the whole
    // crate concurrently. Without this lock an unrelated test's transient
    // set_var/remove_var lands between two of those resolutions and flips the
    // cipher -- see `crate::crypto::acquire_test_env_lock`'s doc. Its five
    // sibling in-place migration tests already hold it; these did not.
    let _env_lock = crate::crypto::acquire_test_env_lock_blocking();
    let dir = temp_root("clobber");
    let _ = std::fs::remove_dir_all(&dir);
    let src = dir.join("src");
    let dst = dir.join("dst");
    std::fs::create_dir_all(&src).unwrap();
    std::fs::create_dir_all(&dst).unwrap();
    // a canonical source shard, and a pre-existing destination graph-0.redb
    empty_shard(&src.join("graph-0.redb"));
    empty_shard(&dst.join("graph-0.redb"));
    let err = migrate_shards(&src, &dst, 4).unwrap_err();
    assert!(err.contains("already exists"), "got: {err}");
    let _ = std::fs::remove_dir_all(&dir);
}

/// A GENUINE multi-source-shard layout (K=2, not K=1) round-tripped through
/// `migrate_shards`. Every other backend-driven test starts from K=1 (one source
/// file); this is the only proof that the loop over MULTIPLE source shards is
/// correct.
#[tokio::test(flavor = "multi_thread")]
async fn multi_source_migration_preserves_graphs() {
    #[cfg(feature = "security")]
    let _env_lock = crate::crypto::acquire_test_env_lock().await;
    let root = temp_root("multisrc");
    let _ = std::fs::remove_dir_all(&root);
    let src = root.join("k2");
    let dst = root.join("k3");
    std::fs::create_dir_all(&src).unwrap();
    let src_s = src.to_string_lossy().to_string();

    let graphs = [
        "one", "two", "three", "four", "five", "six", "seven", "eight",
    ];
    seed_at_k(&src_s, 2, &graphs).await;

    assert!(src.join("graph-0.redb").exists(), "K=2 shard 0 written");
    assert!(src.join("graph-1.redb").exists(), "K=2 shard 1 written");

    let report = migrate_shards(&src, &dst, 3).expect("migrate K=2 -> K=3");
    assert_eq!(report.source_shards, 2);
    assert_eq!(report.dest_shards, 3);
    assert_eq!(report.dest_raft_groups, 3);
    assert_eq!(report.graphs, graphs.len());
    assert_eq!(report.nodes, (graphs.len() * 2) as u64);
    assert_eq!(report.edges, graphs.len() as u64);

    for i in 0..3 {
        assert!(
            dst.join(format!("graph-{i}.redb")).exists(),
            "graph-{i}.redb"
        );
    }

    let dst_s = dst.to_string_lossy().to_string();
    let backend = RedbBackend::open(dst_s.clone(), 256).expect("reopen K=3");
    assert_eq!(backend.shard_count(), 3, "on-disk layout honored as K=3");
    for g in &graphs {
        let dump = backend
            .read_graph_dump_blocking(g)
            .expect("read")
            .unwrap_or_else(|| panic!("graph {g} missing after migration"));
        assert_eq!(dump.name, *g);
        assert_eq!(dump.nodes.len(), 2, "graph {g} nodes");
        assert_eq!(dump.edges.len(), 1, "graph {g} edges");
        let a = dump
            .nodes
            .iter()
            .find(|(id, _)| id == "a")
            .map(|(_, blob)| blob.clone())
            .expect("node a present");
        let val: serde_json::Value = rmp_serde::from_slice(&a).unwrap();
        assert_eq!(val.get("g").and_then(|x| x.as_str()), Some(*g));
    }
    backend.shutdown();
    let _ = std::fs::remove_dir_all(&root);
}
