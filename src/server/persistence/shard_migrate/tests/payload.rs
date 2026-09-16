//! Graph authority and auxiliary payload preservation proofs.

use super::*;
#[cfg(feature = "security")]
use crate::redb_store::PROVENANCE_ANCHOR_MEMBERS;
use crate::redb_store::{capacity_lease, development_lane, work_item_capability};
#[cfg(feature = "matview")]
use crate::redb_store::{MATVIEW_OPERATOR_STATE, PLAN_MATVIEWS};
use crate::redb_store::{NODES, RESOURCE_RESERVATIONS};

/// Re-pinned from `migration_routes_mutation_and_change_authority_with_its_graph`:
/// the eight private `mutation_*` tables it planted rows in are retired, and the
/// replay/outbox/version state they held is now the KERNEL ledger, moved by the
/// graft. The property is unchanged and the assertions are stronger — instead of
/// counting copied rows, this reads the moved authority back:
///
/// * the governed ChangeEnvelope rows arrive with their graph, and
/// * the graph's own receipt is readable at the destination at the SOURCE's
///   marker-inclusive version, not a fresh re-admission, and
/// * the source no longer serves the graph — a migration is a MOVE.
#[test]
fn migration_moves_change_authority_and_the_kernel_ledger_with_its_graph() {
    let root = temp_root("aux");
    let _ = std::fs::remove_dir_all(&root);
    let src = root.join("src");
    let dst = root.join("dst");
    std::fs::create_dir_all(&src).unwrap();
    let source_path = src.join("graph-0.redb");
    let (batch_id, source_version) = seed_graph(&source_path, "aux-graph", "aux");

    let report = migrate_shards(&src, &dst, 4).unwrap();
    assert_eq!(report.graphs, 1);

    let home = dst.join(format!("graph-{}.redb", shard_index("aux-graph", 4)));
    assert_eq!(
        graph_row_count(&home, "aux-graph", CHANGE_ENVELOPES),
        1,
        "change_envelopes moved with its graph"
    );
    assert_eq!(graph_row_count(&home, "aux-graph", CONTENT_VERSIONS), 1);
    assert_eq!(graph_row_count(&home, "aux-graph", CHANGE_CURSORS), 1);

    let (version, receipt) = graph_ledger(&home, "aux-graph", &batch_id);
    assert!(receipt, "the graph's receipt moved to the destination");
    assert_eq!(
        version,
        source_version + 1,
        "the destination serves the SOURCE's marker-inclusive version"
    );

    // The move consumed the source: a fresh binding of the same name finds nothing.
    assert_eq!(
        graph_row_count(&source_path, "aux-graph", CHANGE_ENVELOPES),
        0
    );
    let (source_after, receipt_after) = graph_ledger(&source_path, "aux-graph", &batch_id);
    assert_eq!(source_after, 0, "the source scope was retired");
    assert!(!receipt_after, "the source no longer holds the receipt");

    let _ = std::fs::remove_dir_all(&root);
}

/// Re-pinned from `mutation_batch_chain_routes_independently_per_source_shard`:
/// the `routed_batch_ids` set that test guarded is gone with the private
/// batch-addressed tables, but the property it protected is not. TWO source shard
/// files each carry their own graph and their own ledger, both graphs route to the
/// SAME destination (K=1), and each must arrive with ITS OWN receipt and version —
/// never the other's, and never one of them dropped.
#[test]
fn two_source_shards_move_their_own_ledgers_independently() {
    let root = temp_root("multisrc-ledger");
    let _ = std::fs::remove_dir_all(&root);
    let src = root.join("src");
    let dst = root.join("dst");
    std::fs::create_dir_all(&src).unwrap();

    let (alpha_batch, alpha_version) = seed_graph(&src.join("graph-0.redb"), "alpha", "alpha");
    let (beta_batch, beta_version) = seed_graph(&src.join("graph-1.redb"), "beta", "beta");

    let report = migrate_shards(&src, &dst, 1).expect("migrate K=2 -> K=1");
    assert_eq!(report.source_shards, 2);
    assert_eq!(report.graphs, 2);

    let home = dst.join("graph-0.redb");
    let (alpha_at_dest, alpha_receipt) = graph_ledger(&home, "alpha", &alpha_batch);
    assert!(alpha_receipt, "alpha's own receipt arrived");
    assert_eq!(
        alpha_at_dest,
        alpha_version + 1,
        "alpha's ledger version plus the graft marker survived the move"
    );
    let (beta_at_dest, beta_receipt) = graph_ledger(&home, "beta", &beta_batch);
    assert!(beta_receipt, "beta's own receipt arrived");
    assert_eq!(
        beta_at_dest,
        beta_version + 1,
        "beta's ledger version plus the graft marker survived the move"
    );

    // Neither source's ledger leaked into the other's scope.
    assert!(
        !graph_ledger(&home, "alpha", &beta_batch).1,
        "beta's receipt is readable inside alpha's scope"
    );
    assert!(
        !graph_ledger(&home, "beta", &alpha_batch).1,
        "alpha's receipt is readable inside beta's scope"
    );

    // Each graph's owner rows came with it, from its own source file.
    assert_eq!(graph_row_count(&home, "alpha", CHANGE_ENVELOPES), 1);
    assert_eq!(graph_row_count(&home, "beta", CHANGE_ENVELOPES), 1);

    let _ = std::fs::remove_dir_all(&root);
}

/// BUG-CX-016, FIXED: `provenance_anchor_members` (scope-prefixed, feature
/// `security`) and `plan_matviews` / `matview_operator_state` (file-wide,
/// `shard0()`-homed, feature `matview`) live in the SAME `graph-<n>.redb` shard
/// files as `nodes`/`audit_chain`. `migrate_shards` routes all three, so a K-shard
/// migration carries them across exactly like `nodes`/`audit_chain` do. `NODES`
/// stays asserted as the differential control. Confirmed FAILING before the fix
/// (`anchors`/`plan_matviews`/`matview_state` were all `0`).
#[tokio::test(flavor = "multi_thread")]
async fn migration_preserves_provenance_anchor_and_matview_state() {
    #[cfg(feature = "security")]
    let _env_lock = crate::crypto::acquire_test_env_lock().await;
    let root = temp_root("dropped-tables");
    let _ = std::fs::remove_dir_all(&root);
    let src = root.join("src");
    let dst = root.join("dst");
    std::fs::create_dir_all(&src).unwrap();
    let src_s = src.to_string_lossy().to_string();

    let backend = RedbBackend::open(src_s.clone(), 256).expect("open K=1 backend");
    backend
        .register_graph("g", "g", GraphType::Global)
        .await
        .expect("register");
    backend
        .record_durable(
            "g",
            &Method::AddNode {
                node_id: "a".into(),
                properties_msgpack: props(serde_json::json!({"type": "Task"})),
            },
        )
        .await
        .expect("node a");

    #[cfg(feature = "security")]
    {
        backend
            .provenance_anchor_commit_blocking("g", [9u8; 32], vec![("a".to_string(), [7u8; 32])])
            .expect("provenance anchor commit")
            .expect("anchor actually wrote a row (root differs from none)");
    }
    #[cfg(feature = "matview")]
    {
        backend
            .plan_matview_put("mv-1", b"plan-matview-definition".to_vec())
            .await
            .expect("plan matview put");
        backend
            .matview_operator_state_put("mv-1", b"operator-state".to_vec())
            .await
            .expect("matview operator state put");
    }
    backend.shutdown();

    // Sanity: the rows are actually on disk in the SOURCE before migration --
    // otherwise their absence downstream would prove nothing about migrate_shards.
    let source_path = src.join("graph-0.redb");
    #[cfg(feature = "security")]
    assert_eq!(
        graph_row_count(&source_path, "g", PROVENANCE_ANCHOR_MEMBERS),
        1,
        "source has the provenance anchor row pre-migration"
    );
    #[cfg(feature = "matview")]
    {
        assert_eq!(
            control_row_count(&source_path, PLAN_MATVIEWS),
            1,
            "source has the plan matview row pre-migration"
        );
        assert_eq!(
            control_row_count(&source_path, MATVIEW_OPERATOR_STATE),
            1,
            "source has the matview operator-state row pre-migration"
        );
    }
    assert_eq!(
        graph_row_count(&source_path, "g", NODES),
        1,
        "source has the node pre-migration"
    );

    let report = migrate_shards(&src, &dst, 2).expect("migrate K=1 -> K=2");
    assert_eq!(report.graphs, 1);
    assert_eq!(report.nodes, 1);

    let home = dst.join(format!("graph-{}.redb", shard_index("g", 2)));
    assert_eq!(
        graph_row_count(&home, "g", NODES),
        1,
        "node survives the migration (known-good table)"
    );
    #[cfg(feature = "security")]
    assert_eq!(
        graph_row_count(&home, "g", PROVENANCE_ANCHOR_MEMBERS),
        1,
        "FIXED (BUG-CX-016): provenance_anchor_members survives migrate_shards"
    );
    #[cfg(feature = "matview")]
    {
        // File-wide and shard0()-homed, so they land on destination shard 0
        // regardless of where the graph routed.
        assert_eq!(
            control_row_count(&dst.join("graph-0.redb"), PLAN_MATVIEWS),
            1,
            "FIXED (BUG-CX-016): plan_matviews survives migrate_shards"
        );
        assert_eq!(
            control_row_count(&dst.join("graph-0.redb"), MATVIEW_OPERATOR_STATE),
            1,
            "FIXED (BUG-CX-016): matview_operator_state survives migrate_shards"
        );
    }

    let _ = std::fs::remove_dir_all(&root);
}

/// BUG-CX-054 (RESOURCE_*/development_lane/NATIVE_WORK_ITEMS) plus the two
/// undocumented gaps in the SAME class (capacity_lease, the remaining
/// work_item_capability tables): one representative table per subsystem, seeded
/// directly through the admitted write path (bypassing the request APIs those
/// subsystems would otherwise require), migrated K=1 -> K=2, and confirmed present
/// afterward. Each of these tables is exercised individually by the type checker (a
/// key-shape or table-name transcription error fails to compile), but a wrong FIELD
/// ORDER within a correctly-typed tuple would still compile — this test is the
/// semantic check compilation cannot provide, across all four subsystems in one
/// migration run.
#[tokio::test(flavor = "multi_thread")]
async fn migration_preserves_resource_lane_capacity_and_capability_tables() {
    #[cfg(feature = "security")]
    let _env_lock = crate::crypto::acquire_test_env_lock().await;
    let root = temp_root("cx054-tables");
    let _ = std::fs::remove_dir_all(&root);
    let src = root.join("src");
    let dst = root.join("dst");
    std::fs::create_dir_all(&src).unwrap();
    let src_s = src.to_string_lossy().to_string();

    let backend = RedbBackend::open(src_s.clone(), 256).expect("open K=1 backend");
    backend
        .register_graph("g", "g", GraphType::Global)
        .await
        .expect("register");
    backend.shutdown();

    let shard0 = src.join("graph-0.redb");
    seed_owner_row(&shard0, RESOURCE_RESERVATIONS, "g", "r1", b"reservation");
    seed_owner_row(&shard0, development_lane::HOLDS, "g", "h1", b"hold");
    seed_owner_row(&shard0, capacity_lease::CELLS, "g", "c1", b"cell");
    seed_owner_row(
        &shard0,
        work_item_capability::CAPABILITIES,
        "g",
        "digest1",
        b"capability",
    );
    seed_owner_row(
        &shard0,
        work_item_capability::NATIVE_WORK_ITEMS,
        "g",
        "wi1",
        b"native-work-item",
    );

    // Sanity: every seeded row landed in the source before migration.
    assert_eq!(graph_row_count(&shard0, "g", RESOURCE_RESERVATIONS), 1);
    assert_eq!(graph_row_count(&shard0, "g", development_lane::HOLDS), 1);
    assert_eq!(graph_row_count(&shard0, "g", capacity_lease::CELLS), 1);
    assert_eq!(
        graph_row_count(&shard0, "g", work_item_capability::CAPABILITIES),
        1
    );
    assert_eq!(
        graph_row_count(&shard0, "g", work_item_capability::NATIVE_WORK_ITEMS),
        1
    );

    let report = migrate_shards(&src, &dst, 2).expect("migrate K=1 -> K=2");
    assert_eq!(report.graphs, 1);
    assert_eq!(
        report.capability_and_resource, 5,
        "all 5 seeded rows counted under the coverage bucket"
    );

    let home = dst.join(format!("graph-{}.redb", shard_index("g", 2)));
    assert_eq!(
        graph_row_count(&home, "g", RESOURCE_RESERVATIONS),
        1,
        "resource_reservations survives migrate_shards (BUG-CX-054)"
    );
    assert_eq!(
        graph_row_count(&home, "g", development_lane::HOLDS),
        1,
        "development_lane_holds survives migrate_shards (BUG-CX-054)"
    );
    assert_eq!(
        graph_row_count(&home, "g", capacity_lease::CELLS),
        1,
        "capacity_cells survives migrate_shards (undocumented gap, same class as BUG-CX-054)"
    );
    assert_eq!(
        graph_row_count(&home, "g", work_item_capability::CAPABILITIES),
        1,
        "work_item_claim_capabilities survives migrate_shards (undocumented gap, same class as BUG-CX-054)"
    );
    assert_eq!(
        graph_row_count(&home, "g", work_item_capability::NATIVE_WORK_ITEMS),
        1,
        "native_work_item_authority survives migrate_shards (BUG-CX-054)"
    );

    let _ = std::fs::remove_dir_all(&root);
}
