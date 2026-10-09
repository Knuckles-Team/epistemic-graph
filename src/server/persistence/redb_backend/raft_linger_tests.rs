use super::*;

/// EH-290: a shallow Raft-only append batch uses the same bounded micro-linger
/// as graph mutations. The old `can_linger` predicate required
/// `raft_log_ops.is_empty()`, so this exact workload bypassed the group-commit
/// window and approached one Immediate fsync per append.
///
/// The injected gate makes coalescing deterministic on one CPU: the first
/// append holds the writer at the receive window until the second is queued.
/// Both acknowledgements still happen only after their shared commit, and
/// reopening proves both entries crossed that durable barrier.
#[tokio::test(flavor = "multi_thread")]
async fn micro_linger_coalesces_shallow_raft_appends() {
    let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
    let dir = std::env::temp_dir().join(format!("eg-redb-raft-linger-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let dir_s = dir.to_string_lossy().to_string();
    let (control, entered_rx, release_tx) = RedbGroupCommitTestControl::new();
    let backend = RedbBackend::open_with_group_commit_config(
        dir_s.clone(),
        64,
        RedbGroupCommitConfig {
            linger: Duration::from_millis(2),
            shallow_threshold: 32,
            test_control: Some(control),
        },
    )
    .expect("open");
    let mut release = ReleaseOnDrop::new(release_tx);
    let shard = backend.shard_for_group(0);
    let stats = shard.stats.clone();

    let enqueue = |index: u64, byte: u8| {
        let (done, rx) = oneshot::channel();
        shard
            .tx
            .send(Cmd::RaftLogAppend {
                group_id: 0,
                entries: vec![(index, vec![byte; 32])],
                done,
            })
            .expect("redb writer thread alive");
        rx
    };
    let first = enqueue(1, 1);
    tokio::task::spawn_blocking(move || {
        crate::test_rendezvous::recv_within(
            &entered_rx,
            "the raft append entering the injected linger gate",
        );
    })
    .await
    .expect("linger gate waiter completed");
    let second = enqueue(2, 2);
    release
        .release()
        .expect("redb writer still held at linger gate");

    first
        .await
        .expect("writer retained first completion")
        .expect("first raft append durable");
    second
        .await
        .expect("writer retained second completion")
        .expect("second raft append durable");
    assert_eq!(stats.commits(), 1, "two shallow appends share one fsync");
    assert_eq!(stats.lingered(), 1, "raft append exercised micro-linger");
    // `shutdown` joins the writer but the backend still owns every shard's
    // strong `Database` handle. Release the stats probe and the backend itself
    // before reopening the same files in-process.
    drop(stats);
    backend.shutdown();
    drop(backend);

    let reopened = RedbBackend::open(dir_s.clone(), 64).expect("reopen");
    assert_eq!(
        reopened.raft_log_read(0, 1, 2).expect("read durable log"),
        vec![vec![1; 32], vec![2; 32]],
    );
    reopened.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}
