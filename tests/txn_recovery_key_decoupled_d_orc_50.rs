//! D-ORC-50 encryption-bootstrap proof (CONCEPT:EG-KG.txn.multi-op-occ-acid,
//! CONCEPT:EG-KG.sharding.row-level-security).
//!
//! Root cause: a multi-op OCC `Commit` (e.g. a compare-and-set that stages a node
//! property + an ANN embedding together via `BeginTxn`/`TxnAddNode`/`TxnAddEmbedding`/
//! `Commit`) durably logs a private recovery plan so a crash mid-commit can resume. That
//! sealing step (`RedbBackend::transaction_recovery_cipher`) used to read the SAME
//! cipher as the data-at-rest value-blob format (`EPISTEMIC_GRAPH_ENCRYPTION_KEY`). So
//! configuring a key to unblock multi-op transaction durability ALSO flipped every
//! ordinary node/edge/property read over to "expect sealed framing" — on a store that
//! already holds plaintext values from before the key existed, every one of those reads
//! then fails closed with `"encrypted durable value is missing sealed framing"`. That is
//! a destructive-read operation on a populated plaintext store, not a config toggle, and
//! is exactly what happened when a prior lane tried "just add the key" against the live
//! ~31K-node production graph (reverted within ~90s).
//!
//! The fix (`crypto::TXN_RECOVERY_KEY_ENV` / `resolve_txn_recovery_key`,
//! `Shard::txn_recovery_cipher`) decouples the two ciphers. This test proves BOTH
//! halves, entirely against throwaway on-disk copies under `std::env::temp_dir()` —
//! never against a live/production store:
//!
//!   1. `reproduces_old_destructive_behavior_as_a_regression_guard` — using the OLD
//!      coupled behavior (data cipher doubling as the recovery cipher) on a store that
//!      already holds plaintext values, enabling the shared key breaks the pre-existing
//!      plaintext read. This is the failure this whole item exists to avoid; keeping it
//!      as a live regression guard proves the new code path is not just "different" but
//!      actually escapes the specific failure mode observed in production.
//!
//!   2. `dedicated_recovery_key_unblocks_txn_commit_without_touching_existing_plaintext`
//!      — the actual fix: reopening a COPY of the same plaintext store with ONLY
//!      `EPISTEMIC_GRAPH_TXN_RECOVERY_KEY` set (a) still reads the pre-existing plaintext
//!      node correctly, (b) commits a multi-op `BeginTxn`/`TxnAddNode`/`TxnAddEmbedding`/
//!      `Commit` transaction (the exact shape `compare_and_set_node_embedding` drives)
//!      that previously required the shared key, and (c) the newly written node's raw
//!      redb bytes are still PLAINTEXT — proving data-at-rest encryption never turned on.

#![cfg(all(feature = "redb", feature = "security"))]

mod common;
#[path = "common/test_support.rs"]
mod test_support;

use std::sync::Arc;

use tokio::sync::RwLock;

use epistemic_graph::crypto::{ENCRYPTION_KEY_ENV, TXN_RECOVERY_KEY_ENV};
use epistemic_graph::protocol::{GraphType, Method, Request, Response, ResultPayload};
use epistemic_graph::server::persistence::redb_backend::RedbBackend;
use epistemic_graph::server::persistence::PersistenceBackend;
use epistemic_graph::server::ServerState;

const SECRET: &str = "d-orc-50-decoupled-recovery-key-test";
const PLAINTEXT_SECRET_PROP: &str = "pre-existing-plaintext-node-payload-marker";
const PRE_EXISTING_GRAPH: &str = "g";
const PRE_EXISTING_NODE: &str = "pre-existing";

/// Serializes every test in this binary: both key env vars are process-global and
/// `cargo test` runs this file's tests concurrently by default.
static KEY_ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn dispatch(state: &test_support::SharedState, request: Request) -> Response {
    test_support::dispatch(state, request).await
}

fn pack(value: serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(&value).unwrap()
}

fn fresh_dir(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "eg-dorc50-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

fn copy_dir(src: &std::path::Path, dst: &std::path::Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let target = dst.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).unwrap();
        }
    }
}

/// Rebind every kernel-owned store file in `copy` against its original in `src`.
///
/// A store's physical root is derived from its `(dev, ino)` and re-checked on
/// every open -- that is what stops a byte copy being served as the original --
/// so a copy is unopenable at its new path until its root is rebound. Without
/// this, `RedbBackend::open` on the copy fails closed with "mutation store root
/// incarnation mismatch" before reading a row. `shard_migrate::migrate_in_place`
/// does exactly this for its own build source, for exactly this reason.
fn rebind_copied_tree(copy: &std::path::Path, src: &std::path::Path) {
    for entry in std::fs::read_dir(copy).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        let original = src.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            rebind_copied_tree(&path, &original);
        } else if path.extension().is_some_and(|ext| ext == "redb") {
            eg_storage::rebind_copied_store(&path, &original)
                .unwrap_or_else(|error| panic!("rebind {}: {error}", path.display()));
        }
    }
}

fn clear_key_envs() {
    std::env::remove_var(ENCRYPTION_KEY_ENV);
    std::env::remove_var(TXN_RECOVERY_KEY_ENV);
}

/// Seed a throwaway plaintext store: no key configured, one node with a marker
/// property, via the same `commit_crossmodal` write path production data went through.
async fn seed_plaintext_store(dir_s: &str) {
    let backend = RedbBackend::open(dir_s.to_string(), 64).expect("open");
    backend
        .commit_crossmodal(
            PRE_EXISTING_GRAPH,
            &[Method::AddNode {
                node_id: PRE_EXISTING_NODE.to_string(),
                properties_msgpack: pack(serde_json::json!({ "secret": PLAINTEXT_SECRET_PROP })),
            }],
            &[],
            &[],
            &[],
        )
        .await
        .expect("seed a pre-existing plaintext node");
    backend.shutdown();
}

/// Stand-in for `compare_and_set_node_embedding`: begin a txn, stage a node property
/// write AND an embedding into the SAME multi-op transaction, then commit. This is the
/// exact op shape that requires `RedbBackend::transaction_recovery_cipher()` to resolve
/// (single-op writes like `seed_plaintext_store` above never touch the recovery-plan
/// path at all).
async fn compare_and_set_node_embedding(
    state: &Arc<RwLock<ServerState>>,
    graph: &str,
    node_id: &str,
    property_value: &str,
    embedding: Vec<f32>,
) -> Response {
    let begun: Response = Box::pin(dispatch(
        state,
        test_support::request(
            SECRET,
            100,
            graph,
            Method::BeginTxn {
                graph: Some(graph.to_string()),
                isolation: None,
            },
        ),
    ))
    .await;
    assert!(begun.error.is_none(), "BeginTxn failed: {:?}", begun.error);
    let txn_id = match begun.result {
        Some(ResultPayload::String(value)) => value,
        other => panic!("unexpected BeginTxn result shape: {other:?}"),
    };

    let staged_node: Response = Box::pin(dispatch(
        state,
        test_support::request(
            SECRET,
            101,
            graph,
            Method::TxnAddNode {
                txn_id: txn_id.clone(),
                node_id: node_id.to_string(),
                properties_msgpack: pack(serde_json::json!({ "value": property_value })),
                graph: None,
            },
        ),
    ))
    .await;
    assert!(
        staged_node.error.is_none(),
        "TxnAddNode failed: {:?}",
        staged_node.error
    );

    let staged_embedding: Response = Box::pin(dispatch(
        state,
        test_support::request(
            SECRET,
            102,
            graph,
            Method::TxnAddEmbedding {
                txn_id: txn_id.clone(),
                node_id: node_id.to_string(),
                embedding,
                graph: None,
            },
        ),
    ))
    .await;
    assert!(
        staged_embedding.error.is_none(),
        "TxnAddEmbedding failed: {:?}",
        staged_embedding.error
    );

    Box::pin(dispatch(
        state,
        test_support::request(
            SECRET,
            103,
            graph,
            Method::Commit {
                txn_id,
                idempotency_key: None,
            },
        ),
    ))
    .await
}

/// (1) REGRESSION GUARD — configuring the data-at-rest key over a POPULATED PLAINTEXT
/// store is destructive, and the engine must never do it silently. Kept as a live test
/// (not just a comment) so a future refactor that re-couples the two ciphers, or drops
/// the guard, gets caught here instead of in production again.
///
/// WHERE it is caught moved, and this test moved with it (eg-f3 burndown, 2026-09-11).
/// When the production incident happened, the coupling was caught late: the open
/// succeeded and the first READ of a pre-existing value failed with "encrypted durable
/// value is missing sealed framing". `RedbBackend::open` now carries an encryption
/// canary and fails CLOSED at open instead, naming the condition and the documented
/// offline re-encryption procedure. That is strictly stronger — nothing is served at
/// all, rather than served until the first read — so this guard pins the open refusal
/// and additionally proves the store is untouched: with the key removed again, the
/// pre-existing plaintext value still reads back byte-for-byte. A build that re-coupled
/// the ciphers and dropped the canary would open here and fail the first assertion.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reproduces_old_destructive_behavior_as_a_regression_guard() {
    let _guard = KEY_ENV_LOCK.lock().await;
    clear_key_envs();

    let dir = fresh_dir("bug-repro");
    let dir_s = dir.to_string_lossy().to_string();
    seed_plaintext_store(&dir_s).await;

    // Simulate "just add the key" — the exact remediation a prior lane tried against
    // production, reproduced safely against a throwaway directory.
    std::env::set_var(ENCRYPTION_KEY_ENV, "just-add-the-key");
    let refusal = match RedbBackend::open(dir_s.clone(), 64) {
        Ok(backend) => {
            // `RedbBackend` deliberately does not derive `Debug` (it owns live
            // handles), so destructure rather than widen a trait bound for a test.
            backend.shutdown();
            clear_key_envs();
            panic!(
                "enabling at-rest encryption over a POPULATED PLAINTEXT store must fail \
                 closed at open; it opened instead, which is the destructive-read \
                 condition this guard exists to catch"
            );
        }
        Err(message) => message,
    };
    clear_key_envs();
    assert!(
        refusal.contains("PLAINTEXT") && refusal.contains("destructive-read"),
        "the refusal must name the condition and the documented remediation, got: {refusal}"
    );

    // NON-DESTRUCTIVE: the refusal changed nothing. With the key removed the
    // pre-existing plaintext value still reads back exactly as it was written.
    let reopened = RedbBackend::open(dir_s.clone(), 64).expect("reopen with no key configured");
    let node_bytes = reopened
        .read_node_blocking(PRE_EXISTING_GRAPH, PRE_EXISTING_NODE)
        .expect("pre-existing plaintext node must still be readable")
        .expect("pre-existing node must be present");
    let decoded: serde_json::Value = rmp_serde::from_slice(&node_bytes).unwrap();
    assert_eq!(
        decoded["secret"], PLAINTEXT_SECRET_PROP,
        "a refused at-rest-encryption open must leave the plaintext store untouched"
    );
    reopened.shutdown();

    let _ = std::fs::remove_dir_all(&dir);
}

/// (2) THE FIX — a dedicated `EPISTEMIC_GRAPH_TXN_RECOVERY_KEY` unblocks multi-op OCC
/// transaction durability WITHOUT requiring (or silently enabling) at-rest encryption of
/// existing plaintext values.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dedicated_recovery_key_unblocks_txn_commit_without_touching_existing_plaintext() {
    let _guard = KEY_ENV_LOCK.lock().await;
    clear_key_envs();

    // Seed a plaintext store exactly like the live production graph today: no key
    // configured, ordinary node writes, byte-for-byte plaintext on disk.
    let baseline_dir = fresh_dir("baseline");
    let baseline_s = baseline_dir.to_string_lossy().to_string();
    seed_plaintext_store(&baseline_s).await;

    // Copy the baseline so this test is provably working against "a copy of the store",
    // never mutating the seeded directory in place.
    let work_dir = fresh_dir("decoupled-fix");
    copy_dir(&baseline_dir, &work_dir);
    // A copy is not openable at its new path until its physical root is rebound
    // -- see `rebind_copied_tree`. This step used to be missing, so the reopen
    // below failed closed with "mutation store root incarnation mismatch" before
    // the recovery-key behaviour under test was ever reached.
    rebind_copied_tree(&work_dir, &baseline_dir);
    let work_s = work_dir.to_string_lossy().to_string();

    // THE FIX under test: set ONLY the dedicated recovery key. The data-at-rest key
    // (EPISTEMIC_GRAPH_ENCRYPTION_KEY) is deliberately left unset.
    std::env::set_var(TXN_RECOVERY_KEY_ENV, "dedicated-recovery-key-only");
    assert!(
        std::env::var(ENCRYPTION_KEY_ENV).is_err(),
        "test precondition: the data-at-rest key must be unset"
    );

    let reopened = RedbBackend::open(work_s.clone(), 64).expect("reopen with recovery key only");

    // (a) Non-destructive: the pre-existing plaintext node is still readable, byte for
    // byte, exactly as it would be with no key at all.
    let node_bytes = reopened
        .read_node_blocking(PRE_EXISTING_GRAPH, PRE_EXISTING_NODE)
        .expect(
            "pre-existing plaintext node must still be readable with only the \
                 recovery key configured",
        )
        .expect("pre-existing node must be present");
    let decoded: serde_json::Value = rmp_serde::from_slice(&node_bytes).unwrap();
    assert_eq!(
        decoded["secret"], PLAINTEXT_SECRET_PROP,
        "pre-existing plaintext value must be unchanged"
    );

    // (b) The actual unblock: a multi-op OCC transaction shaped exactly like
    // `compare_and_set_node_embedding` (node property + embedding staged together, one
    // Commit) now succeeds — this is the call that raised "transaction durability
    // requires EPISTEMIC_GRAPH_ENCRYPTION_KEY to be configured" before this fix. It runs
    // on a brand-new graph name so it never has to touch the pre-existing durable graph
    // through the higher-level dispatch/registry machinery.
    let backend: Arc<dyn PersistenceBackend> = Arc::new(reopened);
    let state = test_support::state_with(
        SECRET,
        common::current_isolation(),
        Some(work_s.clone()),
        Some(backend.clone()),
    );
    let new_graph = "embedding-backfill-target";
    let create: Response = Box::pin(dispatch(
        &state,
        test_support::request(
            SECRET,
            1,
            new_graph,
            Method::CreateGraph {
                graph_name: new_graph.to_string(),
                graph_type: GraphType::Global,
            },
        ),
    ))
    .await;
    assert!(
        create.error.is_none(),
        "CreateGraph failed: {:?}",
        create.error
    );

    let commit = compare_and_set_node_embedding(
        &state,
        new_graph,
        "embedded-node",
        "backfilled-value",
        vec![0.1, 0.2, 0.3, 0.4],
    )
    .await;
    assert!(
        matches!(commit.result, Some(ResultPayload::Bool(true))),
        "compare-and-set-shaped multi-op commit must succeed with only the dedicated \
         recovery key configured: {:?}",
        commit.error
    );

    // (c) Data-at-rest encryption genuinely never turned on: the NEW node's raw redb
    // bytes must still be plaintext (contain the literal property value), not sealed
    // ChaCha20-Poly1305 framing.
    let mut found_plaintext = false;
    if let Ok(entries) = std::fs::read_dir(&work_dir) {
        for e in entries.flatten() {
            if let Ok(bytes) = std::fs::read(e.path()) {
                if bytes
                    .windows(b"backfilled-value".len())
                    .any(|w| w == b"backfilled-value")
                {
                    found_plaintext = true;
                }
            }
        }
    }
    assert!(
        found_plaintext,
        "the new node's property value must be stored PLAINTEXT on disk — finding it \
         sealed would mean the recovery key silently turned on data-at-rest encryption, \
         re-coupling the two ciphers"
    );

    // Belt-and-suspenders: the pre-existing node is STILL readable after the multi-op
    // commit landed (proves the commit didn't retroactively disturb it either).
    let backend_ref = backend.as_redb().expect("redb backend");
    assert!(
        backend_ref
            .read_node_blocking(PRE_EXISTING_GRAPH, PRE_EXISTING_NODE)
            .expect("read after commit")
            .is_some(),
        "pre-existing plaintext node must remain readable after the multi-op commit"
    );

    backend.shutdown();
    clear_key_envs();
    let _ = std::fs::remove_dir_all(&baseline_dir);
    let _ = std::fs::remove_dir_all(&work_dir);
}
