//! Readiness admission and interrupted in-place swap recovery proofs.

use super::super::inplace::{
    migrate_in_place_after_graph_for_test, migrate_in_place_after_install_for_test,
    migrate_in_place_after_old_move_for_test, migrate_in_place_before_swap_for_test,
};
use super::super::readiness::{
    file_sha256, parse_ready_marker, write_ready_marker, IN_PLACE_READY_MARKER,
    READY_MARKER_VERSION,
};
use super::super::swap::swap_ready_shards;
use super::*;

/// CONCEPT:EG-KG.sharding.atomic-shard-swap — the in-place migration swaps shard files atomically and leaves the
/// old files aside; reopening picks up the new K.
#[tokio::test(flavor = "multi_thread")]
async fn in_place_migration_swaps_and_backs_up() {
    // See `roundtrip_k1_to_k4_preserves_all_graphs` above: held for the whole
    // test (this one also reopens after an in-place migration + backup).
    #[cfg(feature = "security")]
    let _env_lock = crate::crypto::acquire_test_env_lock().await;
    let dir = temp_root("inplace");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let dir_s = dir.to_string_lossy().to_string();

    let graphs = ["one", "two", "three", "four", "five"];
    seed_k1(&dir_s, &graphs).await;

    let report = migrate_in_place(&dir_s, 4).expect("in-place migrate");
    assert_eq!(report.dest_shards, 4);
    assert_eq!(report.graphs, graphs.len());

    // New shard files are in place and the old shard set is in the backup dir.
    for i in 0..4 {
        assert!(dir.join(format!("graph-{i}.redb")).exists());
    }
    assert!(
        !dir.join("graph.redb").exists(),
        "retired layout was not created"
    );
    let has_backup = std::fs::read_dir(&dir).unwrap().flatten().any(|e| {
        e.file_name()
            .to_string_lossy()
            .starts_with(".shard-migrate-backup-")
    });
    assert!(has_backup, "the old shard files were moved aside");

    // Reopen in place at K=4 and confirm all graphs are reachable.
    let backend = RedbBackend::open(dir_s.clone(), 256).expect("reopen");
    assert_eq!(backend.shard_count(), 4);
    for g in &graphs {
        assert!(
            backend.read_graph_dump_blocking(g).unwrap().is_some(),
            "graph {g}"
        );
    }
    backend.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn ready_marker_requires_unique_basenames_and_contiguous_digest_bound_targets() {
    let directory = tempfile::tempdir().unwrap();
    let marker = directory.path().join(IN_PLACE_READY_MARKER);
    let digest = "A".repeat(64);
    let header = format!(
        "{READY_MARKER_VERSION}\nbackup={}\nnew_k=1\n",
        directory.path().join("backup").display()
    );
    let valid = format!("source=graph-0.redb\ntarget=graph-0.redb\t{digest}\n");
    std::fs::write(&marker, format!("{header}{valid}")).unwrap();
    let ready = parse_ready_marker(&marker, 1).unwrap();
    assert_eq!(ready.source_names, vec!["graph-0.redb".to_string()]);
    assert_eq!(ready.targets[0].sha256, digest.to_ascii_lowercase());

    for (manifest, error) in [
        (
            format!("source=../graph-0.redb\ntarget=graph-0.redb\t{digest}\n"),
            "invalid source manifest",
        ),
        (
            format!("source=graph-0.redb\nsource=graph-0.redb\ntarget=graph-0.redb\t{digest}\n"),
            "invalid source manifest",
        ),
        (
            format!("{valid}target=graph-0.redb\t{digest}\n"),
            "invalid target manifest",
        ),
        (
            format!("source=graph-0.redb\ntarget=graph-1.redb\t{digest}\n"),
            "non-contiguous target manifest",
        ),
        (
            "source=graph-0.redb\ntarget=graph-0.redb\t1234\n".to_string(),
            "invalid target manifest",
        ),
        (
            format!(
                "source=graph-0.redb\ntarget=graph-0.redb\t{}\n",
                "g".repeat(64)
            ),
            "invalid target manifest",
        ),
        (format!("{valid}foreign=graph-0.redb\n"), "unknown entry"),
    ] {
        std::fs::write(&marker, format!("{header}{manifest}")).unwrap();
        let refused = parse_ready_marker(&marker, 1).unwrap_err();
        assert!(refused.contains(error), "{refused}");
    }
}

/// A corrupt later target must refuse the whole swap before any source rename.
#[test]
fn corrupt_later_ready_target_preserves_all_live_sources_and_readiness() {
    let directory = tempfile::tempdir().unwrap();
    let base = directory.path();
    let tmp = base.join(".shard-migrate-tmp");
    let backup = base.join(".shard-migrate-backup-test");
    let snapshot = backup.join("source");
    std::fs::create_dir_all(&tmp).unwrap();
    std::fs::create_dir_all(&snapshot).unwrap();
    let sources: [(&str, &[u8]); 2] = [
        ("graph-0.redb", b"source zero"),
        ("graph-1.redb", b"source one!"),
    ];
    for (name, bytes) in sources {
        std::fs::write(base.join(name), bytes).unwrap();
        std::fs::write(snapshot.join(name), bytes).unwrap();
    }
    std::fs::write(tmp.join("graph-0.redb"), b"target zero").unwrap();
    std::fs::write(tmp.join("graph-1.redb"), b"target one").unwrap();
    let ready = write_ready_marker(
        &tmp,
        &backup,
        &[base.join("graph-0.redb"), base.join("graph-1.redb")],
        2,
    )
    .unwrap();
    let marker = tmp.join(IN_PLACE_READY_MARKER);
    let marker_before = std::fs::read(&marker).unwrap();
    std::fs::write(tmp.join("graph-1.redb"), b"foreign later target").unwrap();

    let error = swap_ready_shards(base, &tmp, &ready, None).unwrap_err();
    assert!(error.contains("graph-1.redb"), "{error}");
    assert!(error.contains("does not match its ready digest"), "{error}");
    for (name, bytes) in sources {
        assert_eq!(std::fs::read(base.join(name)).unwrap(), bytes);
        assert_eq!(std::fs::read(snapshot.join(name)).unwrap(), bytes);
    }
    assert_eq!(std::fs::read(&marker).unwrap(), marker_before);
    assert_eq!(
        std::fs::read(tmp.join("graph-0.redb")).unwrap(),
        b"target zero"
    );
    assert_eq!(std::fs::read_dir(backup.join("old")).unwrap().count(), 0);
}

/// A partial destination is an accounted recovery artifact.  A retry must
/// refuse it rather than deleting the only remaining copy of any graph
/// whose source graft may already have retired its snapshot scope.
#[test]
fn interrupted_in_place_build_is_preserved_for_recovery() {
    // Every store this test builds or reopens resolves the value cipher from
    // the process-global encryption env vars, and `cargo test` runs the whole
    // crate concurrently. Without this lock an unrelated test's transient
    // set_var/remove_var lands between two of those resolutions and flips the
    // cipher -- see `crate::crypto::acquire_test_env_lock`'s doc. Its five
    // sibling in-place migration tests already hold it; these did not.
    let _env_lock = crate::crypto::acquire_test_env_lock_blocking();
    let dir = temp_root("inplace-preserve");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    empty_shard(&dir.join("graph-0.redb"));
    let tmp = dir.join(".shard-migrate-tmp");
    std::fs::create_dir_all(&tmp).unwrap();
    std::fs::write(tmp.join("partial-copy"), b"accounted partial destination").unwrap();

    let error = migrate_in_place(&dir.to_string_lossy(), 2).unwrap_err();
    assert!(error.contains("preserving"), "{error}");
    assert!(tmp.join("partial-copy").exists());
    assert!(dir.join("graph-0.redb").exists());
    let _ = std::fs::remove_dir_all(&dir);
}

/// A failure after one graph has completed Phase C must not strand the live
/// source in a partially retired state. In-place migration builds from the
/// copied working snapshot, so the original K=1 file and immutable backup
/// remain complete while the partial destination is retained for recovery.
#[tokio::test(flavor = "multi_thread")]
async fn in_place_fault_after_first_graft_keeps_live_source_and_backup() {
    #[cfg(feature = "security")]
    let _env_lock = crate::crypto::acquire_test_env_lock().await;
    let dir = temp_root("inplace-fault-after-graft");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let dir_s = dir.to_string_lossy().to_string();
    let graphs = ["fault-one", "fault-two"];
    seed_k1(&dir_s, &graphs).await;

    let error = migrate_in_place_after_graph_for_test(&dir_s, 2, 1).unwrap_err();
    assert!(
        error.contains("injected migration fault after 1 graph graft"),
        "{error}"
    );
    assert!(error.contains("source snapshot"), "{error}");
    assert!(dir.join("graph-0.redb").exists(), "live source was removed");
    assert!(dir.join(".shard-migrate-tmp").exists());
    assert!(!dir
        .join(".shard-migrate-tmp")
        .join(IN_PLACE_READY_MARKER)
        .exists());

    // The original source remains a complete, usable store after the first
    // graft has retired only the working copy.
    let backend = RedbBackend::open(dir_s.clone(), 256).expect("live source remains usable");
    for graph in &graphs {
        assert!(
            backend.read_graph_dump_blocking(graph).unwrap().is_some(),
            "live source lost graph {graph}"
        );
    }
    backend.shutdown();

    let backup = std::fs::read_dir(&dir)
        .unwrap()
        .flatten()
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(".shard-migrate-backup-"))
        })
        .expect("immutable migration backup");
    assert!(backup.join("source").join("graph-0.redb").exists());

    // "Usable" is proved the way an operator actually recovers from this
    // tree, not by opening it where it lies (eg-f3 burndown, 2026-09-11).
    //
    // A store's physical root is `(dev, ino)`-derived and checked on every
    // open, so a byte copy cannot be served at a new path until it is
    // rebound -- and `rebind_copied_store` REWRITES the file it is given.
    // Opening `.shard-migrate-backup-*/source` in place therefore had only
    // two outcomes: refuse with "mutation store root incarnation mismatch"
    // (what this test hit), or mutate the evidence. `migrate_in_place_inner`
    // records that rebinding the snapshot in place was tried and regressed
    // four tests, because something downstream depends on the snapshot still
    // carrying the ORIGINAL root -- so the evidence stays pristine, by
    // design, and the recovery step is: copy it out, rebind the COPY against
    // the original it was taken from, open the copy.
    //
    // That is what is exercised here. The assertion is not weakened: it still
    // proves both graphs are fully readable out of the preserved backup, and
    // it additionally proves the evidence survives the read unmodified.
    let evidence = backup.join("source");
    let evidence_before: Vec<(std::path::PathBuf, Vec<u8>)> = std::fs::read_dir(&evidence)
        .unwrap()
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "redb"))
        .map(|path| {
            let bytes = std::fs::read(&path).unwrap();
            (path, bytes)
        })
        .collect();
    assert!(
        !evidence_before.is_empty(),
        "the preserved backup must contain at least one store file"
    );
    let recovery = dir.join(".recovery-open");
    let _ = std::fs::remove_dir_all(&recovery);
    std::fs::create_dir_all(&recovery).unwrap();
    for (path, _) in &evidence_before {
        let name = path.file_name().unwrap();
        std::fs::copy(path, recovery.join(name)).unwrap();
        // The snapshot is a plain byte copy of the live source, so it still
        // carries that store's root -- which is what `copied_from` must name.
        eg_storage::rebind_copied_store(&recovery.join(name), &dir.join(name))
            .expect("rebind the recovery copy of the immutable backup");
    }
    let backup_backend = RedbBackend::open(recovery.to_string_lossy().to_string(), 256)
        .expect("immutable source backup remains usable");
    for graph in &graphs {
        assert!(
            backup_backend
                .read_graph_dump_blocking(graph)
                .unwrap()
                .is_some(),
            "immutable backup lost graph {graph}"
        );
    }
    backup_backend.shutdown();
    drop(backup_backend);
    for (path, bytes) in &evidence_before {
        assert_eq!(
            &std::fs::read(path).unwrap(),
            bytes,
            "reading the backup must leave the evidence byte-identical: {}",
            path.display()
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// A fault after the first live source file has moved must be resumable:
/// the immutable target manifest and the old-file backup identify exactly
/// which side of the rename already completed.
#[tokio::test(flavor = "multi_thread")]
async fn in_place_fault_after_old_move_resumes_without_losing_graphs() {
    #[cfg(feature = "security")]
    let _env_lock = crate::crypto::acquire_test_env_lock().await;
    let dir = temp_root("inplace-fault-after-old-move");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let dir_s = dir.to_string_lossy().to_string();
    let graphs = ["old-move-one", "old-move-two"];
    seed_k1(&dir_s, &graphs).await;

    let error = migrate_in_place_after_old_move_for_test(&dir_s, 2, 1).unwrap_err();
    assert!(
        error.contains("injected migration fault after 1 old shard move"),
        "{error}"
    );
    assert!(
        !dir.join("graph-0.redb").exists(),
        "old source was not moved"
    );
    assert!(
        dir.join(".shard-migrate-tmp")
            .join(IN_PLACE_READY_MARKER)
            .exists(),
        "ready marker must survive the fault"
    );

    migrate_in_place(&dir_s, 2).expect("retry after old-file move");
    let backend = RedbBackend::open(dir_s.clone(), 256).expect("reopen resumed migration");
    for graph in &graphs {
        assert!(
            backend.read_graph_dump_blocking(graph).unwrap().is_some(),
            "graph {graph} lost after old-file retry"
        );
    }
    backend.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A fault after installing the first destination target must not move that
/// authenticated target back into the old-file backup on retry.
#[tokio::test(flavor = "multi_thread")]
async fn in_place_fault_after_first_install_resumes_idempotently() {
    #[cfg(feature = "security")]
    let _env_lock = crate::crypto::acquire_test_env_lock().await;
    let dir = temp_root("inplace-fault-after-install");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let dir_s = dir.to_string_lossy().to_string();
    let graphs = ["install-one", "install-two"];
    seed_k1(&dir_s, &graphs).await;

    let error = migrate_in_place_after_install_for_test(&dir_s, 2, 1).unwrap_err();
    assert!(
        error.contains("injected migration fault after 1 target install"),
        "{error}"
    );
    assert!(
        dir.join("graph-0.redb").exists(),
        "first target was not installed"
    );
    assert!(
        dir.join(".shard-migrate-tmp")
            .join(IN_PLACE_READY_MARKER)
            .exists(),
        "ready marker must survive the fault"
    );

    migrate_in_place(&dir_s, 2).expect("retry after target install");
    let backend = RedbBackend::open(dir_s.clone(), 256).expect("reopen resumed migration");
    for graph in &graphs {
        assert!(
            backend.read_graph_dump_blocking(graph).unwrap().is_some(),
            "graph {graph} lost after target-install retry"
        );
    }
    backend.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// The readiness marker is bound to the target K.  A retry with a changed
/// target set must refuse before renaming either the live source or a
/// staged target.
#[tokio::test(flavor = "multi_thread")]
async fn in_place_changed_target_k_refuses_before_swap() {
    #[cfg(feature = "security")]
    let _env_lock = crate::crypto::acquire_test_env_lock().await;
    let dir = temp_root("inplace-changed-target-k");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let dir_s = dir.to_string_lossy().to_string();
    let graphs = ["changed-k-one", "changed-k-two"];
    seed_k1(&dir_s, &graphs).await;

    let source = dir.join("graph-0.redb");
    let marker = dir.join(".shard-migrate-tmp").join(IN_PLACE_READY_MARKER);
    let source_before = file_sha256(&source).unwrap();
    assert!(migrate_in_place_before_swap_for_test(&dir_s, 2)
        .unwrap_err()
        .contains("injected migration fault before swap"));
    let marker_before = file_sha256(&marker).unwrap();

    let error = migrate_in_place(&dir_s, 1).unwrap_err();
    assert!(
        error.contains("target K 2 does not match requested K 1"),
        "{error}"
    );
    assert_eq!(file_sha256(&source).unwrap(), source_before);
    assert_eq!(file_sha256(&marker).unwrap(), marker_before);
    assert!(source.exists(), "changed-K refusal moved the live source");
    assert!(
        marker.exists(),
        "changed-K refusal removed the ready marker"
    );

    let backend = RedbBackend::open_with_shards(dir_s.clone(), 256, 1)
        .expect("live source remains readable after changed-K refusal");
    for graph in &graphs {
        assert!(
            backend.read_graph_dump_blocking(graph).unwrap().is_some(),
            "graph {graph} lost during changed-K refusal"
        );
    }
    backend.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}
