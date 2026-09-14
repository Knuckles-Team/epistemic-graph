//! Pure redb durable-row machinery (CONCEPT:EG-KG.storage.kg-kg / KG-2.195 / KG-2.216).
//!
//! This is the SERVER-INDEPENDENT half of the redb durable tier: the on-disk
//! table layout, the `Method → redb rows` apply, the group-commit, and the
//! full checkpoint/load read-back. It has NO Tokio and NO `ServerState`
//! dependency, so it compiles under `--features redb` ALONE (no `server`).
//!
//! Two callers share it — ONE durable format, never duplicated:
//!   * the out-of-process server's `server::persistence::redb_backend::RedbBackend`
//!     (gated on `server`), which wraps these in its off-reactor group-commit
//!     writer thread + the `PersistenceBackend` async trait; and
//!   * the in-process [`crate::embedded::EmbeddedEngine`] (gated on `embedded`),
//!     which commits through them DIRECTLY (the caller is the writer — durable,
//!     commit-before-return, no Tokio runtime).
//!
//! The redb `Database` and every table key/value shape here are byte-identical to
//! what the server writes, so a graph written by the embedded API reopens in the
//! server and vice-versa.
//!
//! ## Tables (all keyed by graph prefix)
//!   * `nodes`          `(graph, id)            -> node properties msgpack`
//!   * `edges`          `(graph, src, tgt, ord) -> edge properties msgpack`
//!   * `ledger`         `(graph, seq)           -> ledger line`
//!   * `semantic_store` `graph                  -> semantic store blob (msgpack)`
//!   * `graph_meta`     `graph                  -> identity + integrity-policy blob`

mod store_prelude;
use store_prelude::*;

/// Durable rows are outside the native RPC frame validator and may be supplied
/// by a corrupted, restored, or otherwise untrusted database file. Keep one
/// format-wide ceiling aligned with the native protocol's hard request budget,
/// then structurally preflight every MessagePack row before serde can honor an
/// attacker-controlled collection size hint.
const MAX_DURABLE_MSGPACK_BYTES: usize = 384 * 1024 * 1024;
const MAX_DURABLE_STORED_BYTES: usize = MAX_DURABLE_MSGPACK_BYTES + 1024;
const MAX_DURABLE_MSGPACK_ITEMS: usize = 4_000_000;
const INITIAL_GRAPH_VERSION: u64 = 0;

/// Return whether any optional exact-text filter rejects its corresponding
/// stored value.  The resource and development-lane status projections use
/// the same predicate over different request/record DTOs; keeping the
/// comparison here removes that duplicated policy without coupling domains.
pub(crate) fn any_optional_text_filter_mismatch<'a, I>(filters: I) -> bool
where
    I: IntoIterator<Item = (Option<&'a str>, &'a str)>,
{
    filters
        .into_iter()
        .any(|(expected, actual)| expected.is_some_and(|expected| expected != actual))
}

mod store_batch;
mod store_cleanup;
mod store_control;
mod store_mutation;
mod store_native;
mod store_read;
mod store_retry;
mod store_rows;
mod store_state;
mod store_state_tables;
mod store_types;

pub(crate) use store_batch::*;
pub(crate) use store_cleanup::*;
pub(crate) use store_control::*;
pub(crate) use store_mutation::*;
pub(crate) use store_native::*;
pub(crate) use store_read::*;
pub(crate) use store_retry::*;
pub(crate) use store_rows::*;
pub(crate) use store_state::*;
pub(crate) use store_state_tables::*;
pub(crate) use store_types::*;

#[cfg(test)]
mod resource_reservation_tests;
#[cfg(test)]
mod shard_control_tests;

pub(crate) mod capacity_lease;
#[cfg(feature = "redb")]
pub(crate) mod development_lane;
/// The graph shard as a kernel-owned store (RF-RULING-004 step 8).
#[cfg(feature = "redb")]
pub(crate) mod shard;
pub(crate) mod work_item_capability;

pub(crate) mod audit;
pub(crate) mod checkpoint;
pub(crate) mod control;
pub(crate) mod crossmodal;
pub(crate) mod dump;
pub(crate) mod resource;
pub(crate) mod work_item;

// The decomposition keeps the physical file as one `redb_store` API surface.
// Keep the root's imports and its server/embedded callers on that surface
// explicitly: child modules use `super::*`, while persistence code reaches the
// durable machinery through `crate::redb_store`, never through a second store.
#[cfg(feature = "security")]
pub(crate) use audit::{
    append_audit_entry, prove_inclusion, provenance_anchor_commit, provenance_leaf_hashes,
    verify_audit, AuditTailCache, ProvenanceAnchorCache,
};
pub(crate) use checkpoint::apply_checkpoint;
pub(crate) use control::{
    clear_xshard_decision, clear_xshard_prepare, get_xshard_decision, get_xshard_decision_retain,
    get_xshard_prepare, put_xshard_decision, put_xshard_prepare, put_xshard_recoverable_pending,
    scan_xshard_decisions, scan_xshard_prepares,
};
#[cfg(feature = "matview")]
pub(crate) use control::{
    delete_matview_operator_state, delete_plan_matview, put_matview_operator_state,
    put_plan_matview, scan_matview_operator_state, scan_plan_matviews,
};
#[cfg(feature = "compute-dist")]
pub(crate) use control::{put_matview, scan_matviews};
pub(crate) use crossmodal::apply_crossmodal_projection_rows;
pub(crate) use crossmodal::{commit_crossmodal, CrossModalStaged};
pub(crate) use crossmodal::{BlobRefRow, VectorUpsert};
pub(crate) use dump::{
    decode_graph_meta_identity, decode_meta_record, encode_meta_record,
    encode_meta_with_incarnation, graph_meta_schema_version, new_incarnation_id, read_all_dumps,
    read_all_graph_meta, read_graph_dump, read_graph_dump_page, upgrade_legacy_graph_meta,
    GraphDumpPage, PageCursorRef,
};
pub(crate) use resource::{
    read_resource_reservation, read_resource_reservation_status, resource_decode, resource_encode,
    resource_host_update_snapshot_kind, resource_load_host, resource_metadata_maps,
    resource_record_target_kind, resource_record_work_item_live, resource_request_from_record,
    resource_reservation_snapshot_kind, resource_target_selection_matches, resource_text,
    resource_validate_work_item, MAX_RESOURCE_CLEAR_SCAN, MAX_RESOURCE_HOST_DISK_POLICIES,
};
pub(crate) use shard::{Shard, ShardWrite};
pub(crate) use work_item::{
    apply_submit_work_item_rows, apply_submit_work_items_rows, apply_work_item_rows,
    WorkItemCommitScope,
};

// ── Cross-shard 2PC durable rows (CONCEPT:EG-KG.storage.lane-n-increment) — pure, server-INDEPENDENT ──
// Shared store helpers (mirroring NODES/EDGES/purge_graph_rows): the `Cmd` arms in
// `redb_backend`'s off-reactor writer thread call straight into these.

#[cfg(all(test, feature = "security"))]
mod security_tests {
    //! Encryption-at-rest + tamper-evident audit proofs over the durable store
    //! (CONCEPT:EG-KG.sharding.row-level-security), exercised through the SAME `commit_ops`/read/`verify_audit`
    //! the server + embedded engine use.
    use super::*;
    use crate::crypto::ValueCipher;
    use crate::test_rendezvous::join_bounded;

    fn open_db(dir: &std::path::Path) -> Shard {
        let path = dir.join("graph-0.redb");
        Shard::open(&path).unwrap()
    }

    fn add_node_method(node_id: &str, props: serde_json::Value) -> Method {
        Method::AddNode {
            node_id: node_id.to_string(),
            properties_msgpack: rmp_serde::to_vec_named(&props).unwrap(),
        }
    }

    fn add_edge_method(src: &str, tgt: &str) -> Method {
        Method::AddEdge {
            source_id: src.to_string(),
            target_id: tgt.to_string(),
            properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({})).unwrap(),
        }
    }

    /// Read back the stored ordinals for one (graph,src,tgt) in ascending order.
    fn edge_ords(shard: &Shard, graph: &str, src: &str, tgt: &str) -> Vec<u32> {
        let handle = shard.graph(graph).unwrap();
        let read = shard.read(&handle).unwrap();
        let edges = read.scoped_owner_table(EDGES).unwrap();
        edges
            .scope_rows()
            .unwrap()
            .map(|row| row.expect("read durable edge row"))
            .filter(|(k, _)| {
                let (g, s, t, _) = k.value();
                g == graph && s == src && t == tgt
            })
            .map(|(k, _)| k.value().3)
            .collect()
    }

    fn next_drain_id(tag: &str) -> String {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        format!(
            "{tag}-{}",
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        )
    }

    fn tamper_audit_row(shard: &Shard, graph: &str, sequence: u64) {
        let members = shard.graph_members(&[graph]).unwrap();
        let op_id = next_drain_id("tamper-audit");
        let (group, batches) = shard.admit_maintenance(&members, &op_id).unwrap();
        let write = ShardWrite::open(shard, &group, &members, &batches).unwrap();
        {
            let mut audit = write
                .graph(graph)
                .unwrap()
                .open_scoped_table(AUDIT)
                .unwrap();
            let original = audit
                .get((graph, sequence))
                .unwrap()
                .unwrap()
                .value()
                .to_vec();
            let mut mutated = original;
            let last = mutated.len() - 1;
            mutated[last] ^= 0xFF;
            audit.insert((graph, sequence), mutated.as_slice()).unwrap();
        }
        write.finish().unwrap();
        shard.commit_drain(group, &batches, 0).unwrap();
    }

    #[test]
    fn hierarchical_edge_ordinal_cache_invalidates_only_the_requested_scope() {
        EDGE_ORD_CACHE.with(|cache| {
            let mut cache = cache.borrow_mut();
            cache.clear();
            cache
                .entry("g1".into())
                .or_default()
                .entry("a".into())
                .or_default()
                .extend([("b".into(), 2), ("c".into(), 3)]);
            cache
                .entry("g1".into())
                .or_default()
                .entry("x".into())
                .or_default()
                .insert("y".into(), 4);
            cache
                .entry("g2".into())
                .or_default()
                .entry("a".into())
                .or_default()
                .insert("b".into(), 5);
        });

        invalidate_edge_ord("g1", "a", "b");
        EDGE_ORD_CACHE.with(|cache| {
            let cache = cache.borrow();
            assert!(!cache["g1"]["a"].contains_key("b"));
            assert_eq!(cache["g1"]["a"]["c"], 3);
            assert_eq!(cache["g2"]["a"]["b"], 5);
        });

        invalidate_node_edge_ords("g1", "a");
        EDGE_ORD_CACHE.with(|cache| {
            let cache = cache.borrow();
            assert!(!cache["g1"].contains_key("a"));
            assert_eq!(cache["g1"]["x"]["y"], 4);
        });

        invalidate_graph_edge_ords("g1");
        EDGE_ORD_CACHE.with(|cache| {
            let mut cache = cache.borrow_mut();
            assert!(!cache.contains_key("g1"));
            assert_eq!(cache["g2"]["a"]["b"], 5);
            cache.clear();
        });
    }

    #[test]
    fn edge_ordinal_cache_assigns_u32_max_once_then_fails_closed() {
        let dir = tempdir();
        let writer = std::thread::Builder::new()
            .name("eg-redb-writer-exhaustion-test".to_string())
            .spawn(move || {
                let db = open_db(&dir);
                let members = db.graph_members(&["g"]).unwrap();
                let op_id = next_drain_id("edge-space");
                let (group, batches) = db.admit_maintenance(&members, &op_id).unwrap();
                let write = ShardWrite::open(&db, &group, &members, &batches).unwrap();
                let edges = write.graph("g").unwrap().open_scoped_table(EDGES).unwrap();
                EDGE_ORD_CACHE.with(|cache| {
                    cache
                        .borrow_mut()
                        .entry("g".into())
                        .or_default()
                        .entry("a".into())
                        .or_default()
                        .insert("b".into(), u64::from(u32::MAX));
                });

                assert_eq!(next_edge_ordinal(&edges, "g", "a", "b").unwrap(), u32::MAX);
                assert_eq!(
                    next_edge_ordinal(&edges, "g", "a", "b").unwrap_err(),
                    "edge ordinal space exhausted"
                );
                drop(edges);
                write.finish().unwrap();
                db.commit_drain(group, &batches, 0).unwrap();
                EDGE_ORD_CACHE.with(|cache| cache.borrow_mut().clear());
            })
            .unwrap();
        join_bounded(writer, "the edge-ordinal writer thread");
    }

    /// CONCEPT:EG-KG.storage.redb-store #3 — the O(1) edge-ordinal counter assigns CORRECT, strictly
    /// monotonic ordinals across many `AddEdge` to one node (per-op across SEPARATE commit
    /// batches on the dedicated writer thread — the hot path that used to range-scan every
    /// time), and a FRESH writer thread (the restart case) RE-SEEDS each (src,tgt) from one
    /// bounded tail seek and continues with no gap, reset, or collision. `RemoveEdge` invalidates the
    /// counter so a re-add resets to 0, matching the old scan behavior exactly.
    #[test]
    fn edge_ordinals_monotonic_o1_counter_and_reseed_after_restart() {
        let dir = tempdir();

        // PHASE 1 — on a dedicated `eg-redb-writer*` thread so the EG-029 counter is active.
        let d1 = dir.clone();
        let writer = std::thread::Builder::new()
            .name("eg-redb-writer-egtest".to_string())
            .spawn(move || {
                let crypto = DurableCrypto::none();
                let db = open_db(&d1);
                let mut tail = AuditTailCache::new();
                let mut commit = |m: Method| {
                    let mut ops = vec![("g".to_string(), m)];
                    let mut log = Vec::new();
                    commit_ops(
                        &db,
                        &mut ops,
                        &mut log,
                        &next_drain_id("security-edge"),
                        0,
                        crypto,
                        &mut tail,
                    )
                    .unwrap();
                };
                // AddEdge now requires both endpoints to already be durable nodes
                // (redb_store.rs's "AddEdge requires durable endpoints" guard) --
                // seed the three nodes this test's edges reference before adding
                // any edge between them.
                commit(add_node_method("a", serde_json::json!({})));
                commit(add_node_method("b", serde_json::json!({})));
                commit(add_node_method("c", serde_json::json!({})));

                // 6 multi-edges a->b across SEPARATE batches (cross-batch in-RAM counter),
                // interleaved with 2 a->c.
                for _ in 0..6 {
                    commit(add_edge_method("a", "b"));
                }
                commit(add_edge_method("a", "c"));
                commit(add_edge_method("a", "c"));
                assert_eq!(edge_ords(&db, "g", "a", "b"), vec![0, 1, 2, 3, 4, 5]);
                assert_eq!(edge_ords(&db, "g", "a", "c"), vec![0, 1]);

                // RemoveEdge invalidates the counter → re-add resets to 0 (old behavior).
                commit(Method::RemoveEdge {
                    source_id: "a".into(),
                    target_id: "c".into(),
                });
                assert_eq!(edge_ords(&db, "g", "a", "c"), Vec::<u32>::new());
                commit(add_edge_method("a", "c"));
                assert_eq!(edge_ords(&db, "g", "a", "c"), vec![0]);
            })
            .unwrap();
        join_bounded(writer, "the edge-ordinal writer thread");

        // PHASE 2 — RESTART: reopen the SAME file on a NEW writer thread (fresh thread-local
        // counter). Adding 3 more a->b must RE-SEED from one scan (max was 5) and continue
        // 6,7,8 — monotonic, no reset, no collision.
        let d2 = dir.clone();
        let writer = std::thread::Builder::new()
            .name("eg-redb-writer-egtest".to_string())
            .spawn(move || {
                let crypto = DurableCrypto::none();
                let db = open_db(&d2);
                let mut tail = AuditTailCache::new();
                for _ in 0..3 {
                    let mut ops = vec![("g".to_string(), add_edge_method("a", "b"))];
                    let mut log = Vec::new();
                    commit_ops(
                        &db,
                        &mut ops,
                        &mut log,
                        &next_drain_id("security-edge-restart"),
                        0,
                        crypto,
                        &mut tail,
                    )
                    .unwrap();
                }
                assert_eq!(
                    edge_ords(&db, "g", "a", "b"),
                    vec![0, 1, 2, 3, 4, 5, 6, 7, 8],
                    "re-seeded counter must continue monotonically after restart"
                );
            })
            .unwrap();
        join_bounded(writer, "the edge-ordinal writer thread");
    }

    /// CONCEPT:EG-KG.storage.redb-store #4 — with encryption OFF, `seal` returns `Cow::Borrowed` and the
    /// stored value blob is BYTE-FOR-BYTE the caller's plaintext (zero clone, no format
    /// change). Proven by reading the stored bytes back and comparing to the input.
    #[test]
    fn seal_off_stores_plaintext_bytes_byte_identical() {
        let dir = tempdir();
        let crypto = DurableCrypto::none();
        let db = open_db(&dir);
        let pbytes = rmp_serde::to_vec_named(&serde_json::json!({"k": "v-plain-123"})).unwrap();
        let mut ops = vec![(
            "g".to_string(),
            Method::AddNode {
                node_id: "n".to_string(),
                properties_msgpack: pbytes.clone(),
            },
        )];
        let mut log = Vec::new();
        let mut tail = AuditTailCache::new();
        commit_ops(
            &db,
            &mut ops,
            &mut log,
            &next_drain_id("security-plaintext"),
            0,
            crypto,
            &mut tail,
        )
        .unwrap();

        let handle = db.graph("g").unwrap();
        let read = db.read(&handle).unwrap();
        let nodes = read.scoped_owner_table(NODES).unwrap();
        let stored = nodes.get(("g", "n")).unwrap().unwrap().value().to_vec();
        assert_eq!(
            stored, pbytes,
            "encryption-off stored bytes must equal the input plaintext (seal = identity)"
        );
    }

    #[test]
    fn encryption_no_plaintext_on_disk_round_trips_and_wrong_key_fails() {
        let dir = tempdir();
        let db_path = dir.join("graph-0.redb");
        let cipher = ValueCipher::from_key_material(b"correct-horse-battery-staple");
        let crypto = DurableCrypto::new(Some(&cipher));

        // Write a node carrying a recognizable SECRET via the durable write path.
        {
            let db = open_db(&dir);
            let mut ops = vec![(
                "g".to_string(),
                add_node_method("n1", serde_json::json!({"ssn": "SECRET-123-45-6789"})),
            )];
            let mut log = Vec::new();
            let mut audit_tail = AuditTailCache::new();
            commit_ops(
                &db,
                &mut ops,
                &mut log,
                &next_drain_id("security-encrypted"),
                0,
                crypto,
                &mut audit_tail,
            )
            .unwrap();
        }

        // The raw on-disk redb bytes must NOT contain the plaintext secret.
        let raw = std::fs::read(&db_path).unwrap();
        let needle = b"SECRET-123-45-6789";
        assert!(
            !raw.windows(needle.len()).any(|w| w == needle),
            "plaintext node property leaked into raw redb file"
        );

        // It round-trips with the right key.
        {
            let db = open_db(&dir);
            let dumps = read_all_dumps(&db, crypto).unwrap();
            let g = dumps.iter().find(|d| d.graph == "g").expect("graph g");
            let (_, props) = &g.nodes[0];
            let m: serde_json::Value = rmp_serde::from_slice(props).unwrap();
            assert_eq!(m["ssn"], "SECRET-123-45-6789");
        }

        // A WRONG key fails to decrypt (never silent plaintext).
        {
            let db = open_db(&dir);
            let wrong = ValueCipher::from_key_material(b"totally-different-key");
            let res = read_all_dumps(&db, DurableCrypto::new(Some(&wrong)));
            assert!(res.is_err(), "wrong key must not decrypt");
        }
    }

    #[test]
    fn audit_chain_verifies_clean_and_detects_tampering() {
        let dir = tempdir();
        let crypto = DurableCrypto::none();
        let db = open_db(&dir);

        // Three durable mutations → three chained audit entries.
        let mut audit_tail = AuditTailCache::new();
        for (i, m) in [
            add_node_method("a", serde_json::json!({"v": 1})),
            add_node_method("b", serde_json::json!({"v": 2})),
            Method::AddEdge {
                source_id: "a".into(),
                target_id: "b".into(),
                properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({})).unwrap(),
            },
        ]
        .into_iter()
        .enumerate()
        {
            let _ = i;
            let mut ops = vec![("g".to_string(), m)];
            let mut log = Vec::new();
            commit_ops(
                &db,
                &mut ops,
                &mut log,
                &next_drain_id("security-audit"),
                0,
                crypto,
                &mut audit_tail,
            )
            .unwrap();
        }

        // A clean chain verifies.
        let report = verify_audit(&db, "g").unwrap();
        assert!(report.ok, "{report:?}");
        assert_eq!(report.entries, 3);

        // Tamper entry seq=1: flip its stored line/hash bytes directly in the table.
        tamper_audit_row(&db, "g", 1);

        let broken = verify_audit(&db, "g").unwrap();
        assert!(!broken.ok, "tamper undetected");
        assert_eq!(broken.first_broken_seq, Some(1), "wrong break position");
    }

    /// CONCEPT:EG-KG.storage.embedded-store — the O(1) tail-cache append produces an IDENTICAL, verifiable
    /// chain to the old per-op scan across: (1) many ops in ONE commit batch
    /// (intra-batch chaining off RAM), (2) several commit batches reusing the cache
    /// (inter-batch), and (3) a fresh cache that must RE-SEED the tail from one scan
    /// (the restart case) and continue the chain without a gap. Two interleaved graphs
    /// prove per-graph isolation of the cache.
    #[test]
    fn audit_tail_cache_o1_append_builds_verifiable_chain_across_batches_and_restart() {
        let dir = tempdir();
        let crypto = DurableCrypto::none();
        let db = open_db(&dir);

        // Helper: commit a batch of (graph, node) AddNode ops through commit_ops with a
        // caller-owned cache (mirrors the writer thread's persistent cache).
        let commit_batch = |shard: &Shard, cache: &mut AuditTailCache, batch: &[(&str, &str)]| {
            let mut ops: Vec<(String, Method)> = batch
                .iter()
                .map(|(g, n)| {
                    (
                        g.to_string(),
                        add_node_method(n, serde_json::json!({"n": n})),
                    )
                })
                .collect();
            let mut log = Vec::new();
            commit_ops(
                shard,
                &mut ops,
                &mut log,
                &next_drain_id("security-cache"),
                0,
                crypto,
                cache,
            )
            .unwrap();
        };

        // Batch 1: 5 ops for "g1" + 3 ops for "g2" in ONE commit (intra-batch chaining,
        // interleaved graphs). The cache seeds each graph once (genesis) then chains in RAM.
        let mut cache = AuditTailCache::new();
        commit_batch(
            &db,
            &mut cache,
            &[
                ("g1", "a"),
                ("g2", "x"),
                ("g1", "b"),
                ("g1", "c"),
                ("g2", "y"),
                ("g1", "d"),
                ("g2", "z"),
                ("g1", "e"),
            ],
        );
        // Cache must reflect the in-RAM tails: g1 saw 5 ops (seq 0..4), g2 saw 3 (seq 0..2).
        assert_eq!(cache.get("g1").unwrap().0, 4, "g1 tail seq");
        assert_eq!(cache.get("g2").unwrap().0, 2, "g2 tail seq");

        // Batch 2: REUSE the same cache (inter-batch). No scan should be needed; the
        // chain must continue seamlessly.
        commit_batch(&db, &mut cache, &[("g1", "f"), ("g2", "w"), ("g1", "g")]);

        // Batch 3: simulate a WRITER RESTART — a brand-new empty cache. The first touch
        // of each graph must RE-SEED the tail from one range-scan and continue with NO gap.
        let mut cache_after_restart = AuditTailCache::new();
        commit_batch(&db, &mut cache_after_restart, &[("g1", "h"), ("g2", "v")]);
        assert_eq!(
            cache_after_restart.get("g1").unwrap().0,
            7,
            "g1 re-seeded tail continues (5+2 prior ⇒ next seq 7)"
        );
        assert_eq!(
            cache_after_restart.get("g2").unwrap().0,
            4,
            "g2 re-seeded tail continues (3+1+1 prior ⇒ seq 4)"
        );

        // The FULL chains must verify clean (tamper-evidence intact, no gaps/breaks).
        let r1 = verify_audit(&db, "g1").unwrap();
        assert!(r1.ok, "g1 chain broken: {r1:?}");
        assert_eq!(r1.entries, 8, "g1 entry count (5+2+1)");
        let r2 = verify_audit(&db, "g2").unwrap();
        assert!(r2.ok, "g2 chain broken: {r2:?}");
        assert_eq!(r2.entries, 5, "g2 entry count (3+1+1)");

        // And tamper-evidence still fires on the cache-built chain.
        tamper_audit_row(&db, "g1", 3);
        let broken = verify_audit(&db, "g1").unwrap();
        assert!(!broken.ok, "tamper on cache-built chain undetected");
        assert_eq!(broken.first_broken_seq, Some(3));
    }

    /// CONCEPT:EG-KG.storage.embedded-store — the cold-seed tail lookup is a BOUNDED reverse seek
    /// (`(graph, 0)..=(graph, u64::MAX)` + `next_back`), not a forward walk to the end
    /// of the chain. Proven by comparing the wall-clock cost of re-seeding (fresh
    /// cache, i.e. after a simulated writer restart) a chain of 200,000 prior entries
    /// against re-seeding a chain of 5: the chains differ 40,000x in length, but a
    /// single bounded reverse seek's cost is independent of that (only the B-tree's
    /// O(log n) depth differs — a small constant next to the `*50 + 20ms` slack
    /// below). The old `.range((graph, 0u64)..)` + `.last()` forward walk (O(chain
    /// length)) would blow well past this bound on the long chain.
    #[test]
    fn audit_tail_cold_seed_is_a_bounded_seek_not_a_forward_scan() {
        let crypto = DurableCrypto::none();

        // Build a chain of `len` AddNode entries for `graph` in ONE commit batch (the
        // cache stays warm for the whole build, so construction cost is irrelevant —
        // only the POST-RESTART re-seed below is timed), then re-seed from a FRESH
        // cache (simulating a writer restart) and time just that one call.
        let reseed_cost = |graph: &str, len: usize| -> u64 {
            let dir = tempdir();
            let db = open_db(&dir);
            let mut ops: Vec<(String, Method)> = (0..len)
                .map(|i| {
                    (
                        graph.to_string(),
                        add_node_method(&format!("n{i}"), serde_json::json!({"i": i})),
                    )
                })
                .collect();
            let mut log = Vec::new();
            let mut warm_cache = AuditTailCache::new();
            commit_ops(
                &db,
                &mut ops,
                &mut log,
                &next_drain_id("security-cold-build"),
                0,
                crypto,
                &mut warm_cache,
            )
            .unwrap();

            // Simulated restart: a brand-new empty cache forces the NEXT append to
            // cold-seed the tail from durable state instead of chaining off RAM.
            let mut cold_cache = AuditTailCache::new();
            let mut restart_ops = vec![(
                graph.to_string(),
                add_node_method(&format!("n{len}"), serde_json::json!({"i": len})),
            )];
            let mut restart_log = Vec::new();
            let _ = super::audit::cold_seed_rows_touched_take(); // discard anything the build left
            commit_ops(
                &db,
                &mut restart_ops,
                &mut restart_log,
                &next_drain_id("security-cold-restart"),
                0,
                crypto,
                &mut cold_cache,
            )
            .unwrap();
            let rows_touched = super::audit::cold_seed_rows_touched_take();

            // Correctness: the re-seeded tail must continue the chain with no gap, and
            // the full (len + 1)-entry chain must still verify clean.
            assert_eq!(
                cold_cache.get(graph).unwrap().0,
                len as u64,
                "re-seeded tail must continue at seq {len}"
            );
            let report = verify_audit(&db, graph).unwrap();
            assert!(report.ok, "{report:?}");
            assert_eq!(report.entries, (len + 1) as u64);

            rows_touched
        };

        let short = reseed_cost("g-short", 5);
        let long = reseed_cost("g-long", 200_000);

        // The property under test is STRUCTURAL — "a bounded seek, not a forward
        // scan" — so assert it structurally: the number of audit rows the cold seed
        // pulls must not depend on how long the chain is. A bounded reverse seek
        // pulls exactly one row from a 5-entry chain and exactly one from a
        // 200,000-entry chain; a forward walk pulls 5 and 200,000.
        //
        // This replaces a wall-clock ratio (`long <= short*50 + 20ms`). That budget
        // was BOTH flaky and weak: it failed reproducibly on a shared build host
        // purely from a concurrent job's disk contention (measured 5/5 at 1.6-2.0s
        // against a 0.5-1.1s budget with the mechanism provably unchanged), and it
        // would equally have PASSED a genuine forward-scan regression on a fast
        // enough machine. Counting the rows is deterministic, machine-independent,
        // and strictly stronger.
        assert_eq!(
            short, 1,
            "cold seed of a 5-entry chain should pull exactly one audit row"
        );
        assert_eq!(
            long, short,
            "cold-seed cost must be INDEPENDENT of chain length: a 200,000-entry \
             chain pulled {long} audit row(s) vs {short} for a 5-entry chain — the \
             O(1) bounded reverse seek has regressed to an O(chain length) forward scan"
        );
    }

    /// A throwaway temp dir under the scratch space.
    fn tempdir() -> std::path::PathBuf {
        let base = crate::test_support::temp_dir("eg-sec", "test");
        std::fs::create_dir_all(&base).unwrap();
        base
    }
}

/// Build a unique process-local `.redb` fixture path for a kernel-owned-store
/// test: `<prefix>-<tag>-<pid>-<nanos>.redb` under the OS temp directory.
/// Unlike [`test_support::temp_dir`], this hands back a *file* path meant to
/// go straight to `redb::Database::create`/`Shard::open`, not a directory the
/// fixture owns end-to-end — so there is no matching stale-path cleanup here;
/// each caller is responsible for its own fixture's lifecycle, exactly as it
/// was before this helper had a single home. The four kernel-owned-store test
/// modules that build these paths (mutation-batch replay here, the keyset-page
/// dump tests, the shard bootstrap/graft tests, and the online-reshard tests)
/// used to hand-roll this same construction independently, varying only the
/// prefix; they now call through this one definition instead.
#[cfg(test)]
pub(crate) fn temp_path(prefix: &str, tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "{prefix}-{tag}-{}-{}.redb",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

#[cfg(test)]
mod mutation_batch_tests {
    use super::*;
    use crate::change_envelope::{
        ChangeCursor, ChangeEnvelope, ContentVersion, ContentVersionPosition, CursorPosition,
        PolicyRecord, PrivacyAttestation, CHANGE_ENVELOPE_VERSION,
    };
    use crate::mutation_batch::{
        DurabilityDomain, IncarnationId, LogicalName, MutationOperation, MutationOutboxIntent,
        MutationScopeIdentity, MutationSurface, ScopeTenantId, MUTATION_BATCH_VERSION,
    };
    use crate::test_rendezvous::{join_bounded, meet};
    use eg_transaction::OutboxClaimBudget;
    use eg_types::outcome_bundle::{
        CommitOutcomeBundle, OutcomeCompleteness, ReceiptNode, ReceiptNodeKind, RunEvent,
        TerminalOutcomeExtension, OUTCOME_BUNDLE_VERSION, RUN_EVENT_OUTBOX_TOPIC,
    };
    use sha2::{Digest, Sha256};

    fn temp_path(tag: &str) -> std::path::PathBuf {
        super::temp_path("eg-mutation-batch", tag)
    }

    fn open(path: &std::path::Path) -> Shard {
        let shard = Shard::open(path).unwrap();
        // The old fixture seeded the retired graph-version table at 3.  Advance
        // the kernel-owned ledger through three real maintenance admissions so
        // every test keeps the same OCC starting point without recreating a
        // second version authority.
        let members = shard.graph_members(&["graph-a"]).unwrap();
        // Reopening an existing fixture must not re-admit the same maintenance
        // seed keys: the kernel correctly resolves those members as Replay, and
        // replayed members are forbidden from opening owner rows. Seed only a
        // graph whose authoritative ledger version row is absent.
        // Freshness cannot be probed by "is the version row absent?", because this
        // read is what CREATES that row: `read_mutation_graph_version` opens the
        // graph, and `Shard::graph` binds a cold scope (`bind_scope`) which inserts
        // `INITIAL_GRAPH_VERSION`. The absent-row arm below is therefore
        // unreachable through this path, and reading it as "already seeded" left
        // every fixture at version 0 while 47 of them expect the seeded base of 3
        // -- 42 tests failing closed with `STALE_VERSION: expected version 3 but
        // authoritative version is 0`.
        //
        // An unadvanced ledger is the real freshness signal: only seeding moves
        // graph-a off `INITIAL_GRAPH_VERSION`, so a reopened fixture is at 3 or
        // more and is still correctly left alone.
        let seed_required = match read_mutation_graph_version(&shard, "graph-a") {
            Ok(version) => version == INITIAL_GRAPH_VERSION,
            Err(error) if error == "mutation scope binding is missing its version row" => true,
            Err(error) => panic!("unexpected graph version read failure: {error}"),
        };
        if seed_required {
            for index in 0..3 {
                let op_id = format!("mutation-test-seed/{index}");
                let (group, batches) = shard.admit_maintenance(&members, &op_id).unwrap();
                let write = ShardWrite::open(&shard, &group, &members, &batches).unwrap();
                write.finish().unwrap();
                shard.commit_drain(group, &batches, 0).unwrap();
            }
        }
        shard
    }

    /// Reopen an EXISTING fixture database without seeding anything.
    ///
    /// [`open`] deliberately seeds `MUTATION_GRAPH_VERSION["graph-a"] = 3` when
    /// that row is absent, which 47 fixtures depend on. That makes it the wrong
    /// tool for asserting a row was durably DELETED: it re-inserts the very row
    /// under test between the deletion and the assertion, so such a test can only
    /// ever fail — it reports as coverage of durability while being incapable of
    /// observing it.
    fn reopen(path: &std::path::Path) -> Shard {
        Shard::open(path).unwrap()
    }

    fn node(id: &str, value: i64) -> Method {
        Method::AddNode {
            node_id: id.to_string(),
            properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({"value": value}))
                .unwrap(),
        }
    }

    fn batch(batch_id: &str, key: &str) -> MutationBatch {
        let identity = MutationScopeIdentity::graph(
            ScopeTenantId::new("tenant-a").unwrap(),
            LogicalName::new("graph-a").unwrap(),
            IncarnationId::new("incarnation:test:redb-store").unwrap(),
        );
        let mut batch = MutationBatch {
            schema_version: MUTATION_BATCH_VERSION,
            batch_id: batch_id.to_string(),
            envelope: super::fixture_operation_envelope(
                &identity,
                &format!("principal:sha256:{}", "a".repeat(64)),
                42,
                key,
            ),
            identity,
            placement_epoch: 7,
            version_expectation: VersionExpectation::Graph(3),
            fencing_token: Some(9),
            authoritative_state: None,
            operations: vec![
                MutationOperation {
                    ordinal: 0,
                    surface: MutationSurface::Transaction,
                    domain: DurabilityDomain::GraphRows,
                    method: node("a", 1),
                },
                MutationOperation {
                    ordinal: 1,
                    surface: MutationSurface::Transaction,
                    domain: DurabilityDomain::GraphRows,
                    method: node("b", 2),
                },
            ],
            outbox: vec![MutationOutboxIntent {
                topic: "projection.test".to_string(),
                key: batch_id.to_string(),
                payload: vec![1, 2, 3],
                headers: Default::default(),
            }],
            created_at_ms: 100,
        };
        batch
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("a fixture batch reseals its envelope over its final body");
        batch
    }

    #[test]
    fn graph_record_decoder_validates_the_complete_receipt() {
        let batch = batch("decode-record", "decode-record-key");
        let mut record = MutationBatchRecord {
            identity: batch.identity.clone(),
            batch,
            status: MutationBatchStatus::Committed,
            committed_version: CommittedVersion::Graph {
                source: 3,
                target: 4,
            },
            result_msgpack: None,
            committed_at_ms: 101,
        };
        let encoded = rmp_serde::to_vec_named(&record).unwrap();
        decode_mutation_batch_record(&encoded, "graph-a", "decode-record").unwrap();
        assert!(decode_mutation_batch_record(&encoded, "graph-b", "decode-record").is_err());
        assert!(decode_mutation_batch_record(&encoded, "graph-a", "moved-record").is_err());

        for status in [MutationBatchStatus::Prepared, MutationBatchStatus::Aborted] {
            record.status = status;
            record.committed_version = CommittedVersion::None;
            record.validate().unwrap();
            let non_terminal = rmp_serde::to_vec_named(&record).unwrap();
            assert!(
                decode_mutation_batch_record(&non_terminal, "graph-a", "decode-record").is_err()
            );
        }

        let native_identity = MutationScopeIdentity::native(
            ScopeTenantId::new("tenant-a").unwrap(),
            DurabilityDomain::SqlCatalog,
            LogicalName::new("graph-a").unwrap(),
            IncarnationId::new("incarnation:test:redb-store").unwrap(),
        )
        .unwrap();
        record.batch.identity = native_identity.clone();
        record.identity = native_identity;
        record.batch.version_expectation = VersionExpectation::Native(3);
        for operation in &mut record.batch.operations {
            operation.domain = DurabilityDomain::SqlCatalog;
        }
        record.batch.envelope = super::fixture_operation_envelope(
            &record.batch.identity,
            &format!("principal:sha256:{}", "a".repeat(64)),
            42,
            "decode-record-key",
        );
        record
            .batch
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("native receipt fixture reseals its final body");
        record.status = MutationBatchStatus::Committed;
        record.committed_version = CommittedVersion::Native {
            source: 3,
            target: 4,
        };
        record.validate().unwrap();
        let wrong_store = rmp_serde::to_vec_named(&record).unwrap();
        assert!(decode_mutation_batch_record(&wrong_store, "graph-a", "decode-record").is_err());
    }

    fn ready_work_item_method(work_item_id: &str, max_attempts: u64) -> Method {
        Method::AddNode {
            node_id: work_item_id.to_string(),
            properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({
                "node_type": "WorkItem",
                "tenant": "tenant-a",
                "status": "ready",
                "max_attempts": max_attempts,
            }))
            .unwrap(),
        }
    }

    fn delegated_work_item_method(work_item_id: &str, max_attempts: u64) -> Method {
        delegated_work_item_method_with_status(work_item_id, max_attempts, "ready")
    }

    fn delegated_work_item_method_with_status(
        work_item_id: &str,
        max_attempts: u64,
        status: &str,
    ) -> Method {
        Method::AddNode {
            node_id: work_item_id.to_string(),
            properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({
                "node_type": "WorkItem",
                "tenant": "tenant-a",
                "status": status,
                "state": "ready",
                "kind": "agent.execute",
                "queue": "agent.execute",
                "prio_bucket": 0,
                "created_at": 1.0,
                "next_retry_at": 0.0,
                "resource_class": "",
                "fairness_group": "",
                "lease_owner": null,
                "last_lease_owner": null,
                "lease_epoch": 0,
                "fencing_token": 0,
                "lease_expires_at": null,
                "work_item_fence": "",
                "attempt": 0,
                "max_attempts": max_attempts,
                "backoff_base_s": 1.0,
                "downstream_ids": [],
                "dep_count": 0,
                "metadata": {
                    "delegation_id": "delegation:terminal",
                    "run_id": "run:terminal",
                    "agent_id": "agent:selected-b",
                    "capability_digest": digest_for('b')
                },
                "context": {"agent_id": "agent:delegator-a"},
                "catalog_digest": digest_for('c'),
                "policy_digest": digest_for('d'),
                "model_digest": digest_for('e')
            }))
            .unwrap(),
        }
    }

    fn digest_for(byte: char) -> String {
        byte.to_string().repeat(64)
    }

    fn terminal_receipt_node(
        bundle: &CommitOutcomeBundle,
        kind: ReceiptNodeKind,
        node_id: &str,
    ) -> ReceiptNode {
        let kind_name = match kind {
            ReceiptNodeKind::RunTrace => "run_trace",
            ReceiptNodeKind::ToolCall => "tool_call",
            ReceiptNodeKind::OutcomeEvaluation => "outcome_evaluation",
        };
        let payload_ref = format!("cas:receipt:{node_id}");
        let properties = serde_json::json!({
            "node_id": node_id,
            "kind": kind_name,
            "delegation_id": bundle.delegation_id,
            "delegator_id": bundle.delegator_id,
            "selected_agent_id": bundle.selected_agent_id,
            "executor_lease_actor": bundle.executor_lease_actor,
            "outcome": bundle.outcome,
            "work_item_id": bundle.work_item_id,
            "run_id": bundle.run_id,
            "fence_token": bundle.fence_token,
            "result_ref": bundle.result_ref,
            "result_digest": bundle.result_digest,
            "event_sequence": bundle.event_sequence,
            "completeness": bundle.completeness,
            "missing_refs": bundle.missing_refs,
            "outbox_id": bundle.outbox_id,
            "payload_ref": payload_ref,
            "capability_digest": bundle.capability_digest,
            "catalog_digest": bundle.catalog_digest,
            "policy_digest": bundle.policy_digest,
            "model_digest": bundle.model_digest,
            "payload": {"fixture": true}
        });
        let properties_msgpack = rmp_serde::to_vec_named(&properties).unwrap();
        ReceiptNode {
            node_id: node_id.to_string(),
            kind,
            delegation_id: bundle.delegation_id.clone(),
            work_item_id: bundle.work_item_id.clone(),
            run_id: bundle.run_id.clone(),
            fence_token: bundle.fence_token,
            result_ref: bundle.result_ref.clone(),
            outbox_id: bundle.outbox_id.clone(),
            payload_ref,
            payload_digest: hex::encode(Sha256::digest(&properties_msgpack)),
            properties_msgpack,
        }
    }

    fn terminal_extension(
        batch_id: &str,
        work_item_id: &str,
        fencing_token: u64,
        outcome: &str,
        worker_id: &str,
    ) -> TerminalOutcomeExtension {
        let bundle = CommitOutcomeBundle {
            schema_version: OUTCOME_BUNDLE_VERSION,
            delegation_id: "delegation:terminal".into(),
            delegator_id: "agent:delegator-a".into(),
            selected_agent_id: "agent:selected-b".into(),
            executor_lease_actor: worker_id.into(),
            outcome: outcome.into(),
            work_item_id: work_item_id.into(),
            fence_token: fencing_token,
            run_id: "run:terminal".into(),
            result_ref: Some("cas:result:terminal".into()),
            result_digest: Some(digest_for('a')),
            artifacts: Vec::new(),
            trace_ref: "trace:terminal".into(),
            tool_call_refs: vec!["toolcall:terminal:0".into()],
            outcome_ref: "outcome:terminal".into(),
            capability_digest: digest_for('b'),
            catalog_digest: digest_for('c'),
            policy_digest: digest_for('d'),
            model_digest: digest_for('e'),
            event_sequence: 1,
            completeness: OutcomeCompleteness::Complete,
            missing_refs: Vec::new(),
            outbox_id: batch_id.into(),
            langfuse_observation_refs: Vec::new(),
        };
        let receipt_nodes = vec![
            terminal_receipt_node(&bundle, ReceiptNodeKind::RunTrace, &bundle.trace_ref),
            terminal_receipt_node(
                &bundle,
                ReceiptNodeKind::ToolCall,
                &bundle.tool_call_refs[0],
            ),
            terminal_receipt_node(
                &bundle,
                ReceiptNodeKind::OutcomeEvaluation,
                &bundle.outcome_ref,
            ),
        ];
        TerminalOutcomeExtension {
            outcome_bundle: bundle,
            receipt_nodes,
            run_event: RunEvent {
                schema_version: OUTCOME_BUNDLE_VERSION,
                delegation_id: "delegation:terminal".into(),
                delegator_id: "agent:delegator-a".into(),
                selected_agent_id: "agent:selected-b".into(),
                executor_lease_actor: worker_id.into(),
                outcome: outcome.into(),
                work_item_id: work_item_id.into(),
                run_id: "run:terminal".into(),
                fence_token: fencing_token,
                outbox_id: batch_id.into(),
                result_ref: Some("cas:result:terminal".into()),
                capability_digest: digest_for('b'),
                catalog_digest: digest_for('c'),
                policy_digest: digest_for('d'),
                model_digest: digest_for('e'),
                event_sequence: 1,
                completeness: OutcomeCompleteness::Complete,
                missing_refs: Vec::new(),
                kind: "outcome".into(),
                tool_call_ref: None,
                outcome_ref: Some("outcome:terminal".into()),
                payload_digest: digest_for('f'),
                timestamp_ms: 10,
                cursor_token: "cursor:terminal:1".into(),
                carrier_digest: digest_for('0'),
            },
        }
    }

    /// The worker's claim on the work item a terminal fixture commits against.
    ///
    /// Every call site plucks `lease_epoch` and `fencing_token` off the same
    /// `ClaimWorkItemResult` that already named the work item and the worker,
    /// and the terminal commit is only admitted when all four still agree with
    /// that claim -- so the fixture takes the hold as one value instead of four
    /// positional halves two of which a caller can silently cross-wire.
    struct TerminalLeaseHold<'a> {
        work_item_id: &'a str,
        worker_id: &'a str,
        lease_epoch: u64,
        fencing_token: u64,
    }

    fn terminal_extension_batch(
        batch_id: &str,
        idempotency_key: &str,
        expected_graph_version: u64,
        hold: TerminalLeaseHold<'_>,
        outcome: &str,
        retryable: bool,
    ) -> MutationBatch {
        let TerminalLeaseHold {
            work_item_id,
            worker_id,
            lease_epoch,
            fencing_token,
        } = hold;
        let mut terminal = batch(batch_id, idempotency_key);
        terminal.version_expectation = VersionExpectation::Graph(expected_graph_version);
        let extension =
            terminal_extension(batch_id, work_item_id, fencing_token, outcome, worker_id);
        terminal.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Job,
            domain: DurabilityDomain::ControlPlane,
            method: Method::CommitWorkItemResult {
                tenant: "tenant-a".into(),
                work_item_id: work_item_id.into(),
                worker_id: worker_id.into(),
                lease_epoch,
                fencing_token,
                idempotency_key: idempotency_key.into(),
                outcome: outcome.into(),
                result_ref: Some("cas:result:terminal".into()),
                outcome_extension: Some(Box::new(extension.clone())),
                error_ref: None,
                retryable,
                now_ms: 1_000,
            },
        }];
        let bundle = &extension.outcome_bundle;
        let completeness = serde_json::to_value(bundle.completeness)
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        let missing_refs = serde_json::to_string(&bundle.missing_refs).unwrap();
        let actor = terminal
            .envelope
            .operation()
            .expect("terminal fixture has an operation envelope")
            .authority
            .actor
            .clone();
        let mut scope_digest = Sha256::new();
        scope_digest.update(terminal.identity.tenant().as_str().as_bytes());
        scope_digest.update([0]);
        scope_digest.update(
            terminal
                .identity
                .scope()
                .graph_name()
                .expect("terminal fixture is graph-scoped")
                .as_str()
                .as_bytes(),
        );
        let scope_digest = hex::encode(scope_digest.finalize());
        let mut headers = BTreeMap::from([
            ("batch_id".to_string(), batch_id.to_string()),
            ("delegation_id".to_string(), bundle.delegation_id.clone()),
            ("delegator_id".to_string(), bundle.delegator_id.clone()),
            (
                "selected_agent_id".to_string(),
                bundle.selected_agent_id.clone(),
            ),
            (
                "executor_lease_actor".to_string(),
                bundle.executor_lease_actor.clone(),
            ),
            ("outcome".to_string(), bundle.outcome.clone()),
            ("work_item_id".to_string(), bundle.work_item_id.clone()),
            ("run_id".to_string(), bundle.run_id.clone()),
            ("fence_token".to_string(), bundle.fence_token.to_string()),
            (
                "capability_digest".to_string(),
                bundle.capability_digest.clone(),
            ),
            ("catalog_digest".to_string(), bundle.catalog_digest.clone()),
            ("policy_digest".to_string(), bundle.policy_digest.clone()),
            ("model_digest".to_string(), bundle.model_digest.clone()),
            ("completeness".to_string(), completeness),
            ("missing_refs".to_string(), missing_refs),
            ("actor".to_string(), actor.as_str().to_string()),
            ("scope_sha256".to_string(), scope_digest),
        ]);
        if let Some(result_ref) = &bundle.result_ref {
            headers.insert("result_ref".to_string(), result_ref.clone());
        }
        terminal.outbox.push(MutationOutboxIntent {
            topic: RUN_EVENT_OUTBOX_TOPIC.into(),
            key: batch_id.into(),
            payload: rmp_serde::to_vec_named(&extension.run_event).unwrap(),
            headers,
        });
        terminal
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .unwrap();
        terminal
    }

    fn assert_persisted_terminal_currency(
        shard: &Shard,
        batch_id: &str,
        outcome: &str,
        completeness: OutcomeCompleteness,
        missing_refs: &[&str],
    ) {
        let outbox = read_mutation_outbox(shard, "graph-a", batch_id).unwrap();
        assert_eq!(outbox.len(), 1);
        let event: RunEvent = rmp_serde::from_slice(&outbox[0].intent.payload).unwrap();
        assert_eq!(event.outcome, outcome);
        assert_eq!(event.completeness, completeness);
        assert_eq!(
            event.missing_refs,
            missing_refs
                .iter()
                .map(|reference| (*reference).to_string())
                .collect::<Vec<_>>()
        );
        let expected_actor = format!("principal:sha256:{}", "a".repeat(64));
        assert_eq!(
            outbox[0].intent.headers.get("actor").map(String::as_str),
            Some(expected_actor.as_str())
        );
        let admitted_scope = shard::graph_scope_identity("graph-a")
            .expect("terminal assertions use the canonical admitted graph scope");
        let mut expected_scope_digest = Sha256::new();
        expected_scope_digest.update(admitted_scope.tenant().as_str().as_bytes());
        expected_scope_digest.update([0]);
        expected_scope_digest.update(
            admitted_scope
                .scope()
                .graph_name()
                .expect("canonical terminal assertion scope is graph-scoped")
                .as_str()
                .as_bytes(),
        );
        let expected_scope = hex::encode(expected_scope_digest.finalize());
        assert_eq!(
            outbox[0]
                .intent
                .headers
                .get("scope_sha256")
                .map(String::as_str),
            Some(expected_scope.as_str())
        );
        for node_id in ["trace:terminal", "toolcall:terminal:0", "outcome:terminal"] {
            let receipt = read_one_node(shard, "graph-a", node_id, DurableCrypto::none()).unwrap();
            if missing_refs.contains(&node_id) {
                assert!(receipt.is_none(), "missing receipt {node_id} was persisted");
                continue;
            }
            let receipt = receipt.expect("terminal receipt should be persisted");
            let receipt: serde_json::Value = decode_durable(&receipt).unwrap();
            assert_eq!(receipt["outcome"], outcome);
            assert_eq!(
                receipt["completeness"],
                serde_json::to_value(completeness).unwrap()
            );
            assert_eq!(receipt["missing_refs"], serde_json::json!(missing_refs));
        }
    }

    fn seed_and_claim_terminal_work_item(
        shard: &Shard,
        tag: &str,
        work_item_id: &str,
    ) -> ClaimWorkItemResult {
        let mut seed = batch(&format!("{tag}-seed"), &format!("{tag}-seed-key"));
        seed.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Transaction,
            domain: DurabilityDomain::GraphRows,
            method: delegated_work_item_method(work_item_id, 3),
        }];
        seed.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .unwrap();
        commit_at(shard, &seed, None).unwrap();
        commit_native_claim(
            shard,
            &format!("{tag}-claim"),
            &format!("{tag}-claim-key"),
            4,
            Some(work_item_id),
            "worker-a",
            0,
            60_000,
            64,
        )
    }

    // Test-only fixture builder: every parameter is an independent field of
    // the `ClaimWorkItemRequest` under construction; no natural grouping.
    #[allow(clippy::too_many_arguments)]
    fn native_claim_batch(
        batch_id: &str,
        idempotency_key: &str,
        expected_graph_version: u64,
        work_item_id: Option<&str>,
        worker_id: &str,
        now_ms: u64,
        lease_ms: u64,
        max_tenant_in_flight: u64,
    ) -> MutationBatch {
        let mut claim = batch(batch_id, idempotency_key);
        claim.version_expectation = VersionExpectation::Graph(expected_graph_version);
        claim.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Transaction,
            domain: DurabilityDomain::GraphRows,
            method: Method::ClaimWorkItem {
                request: crate::epistemic_operations::ClaimWorkItemRequest {
                    schema_version:
                        crate::epistemic_operations::ClaimWorkItemRequestSchemaVersion::V1,
                    tenant_ref: "tenant-a".into(),
                    work_item_id: work_item_id.map(str::to_string),
                    queue_ref: None,
                    resource_class: None,
                    fairness_group: None,
                    worker_ref: worker_id.into(),
                    now_ms,
                    lease_ms,
                    max_tenant_in_flight,
                },
            },
        }];
        claim
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("native claim fixture reseals its final body");
        claim
    }

    // Test-only fixture builder; mirrors `native_claim_batch`'s justification
    // above plus the `db` handle it commits the built batch against.
    #[allow(clippy::too_many_arguments)]
    fn commit_native_claim(
        shard: &Shard,
        batch_id: &str,
        idempotency_key: &str,
        expected_graph_version: u64,
        work_item_id: Option<&str>,
        worker_id: &str,
        now_ms: u64,
        lease_ms: u64,
        max_tenant_in_flight: u64,
    ) -> ClaimWorkItemResult {
        let committed = commit_at(
            shard,
            &native_claim_batch(
                batch_id,
                idempotency_key,
                expected_graph_version,
                work_item_id,
                worker_id,
                now_ms,
                lease_ms,
                max_tenant_in_flight,
            ),
            None,
        )
        .unwrap();
        let payload: crate::protocol::ResultPayload = decode_durable(
            committed
                .record
                .result_msgpack
                .as_deref()
                .expect("claim result"),
        )
        .unwrap();
        let bytes = match payload {
            crate::protocol::ResultPayload::Raw(inner) => inner,
            other => panic!("ClaimWorkItem must return a bin-encoded typed result, got {other:?}"),
        };
        decode_durable(&bytes).unwrap()
    }

    fn public_batch_method(operations: serde_json::Value) -> Method {
        Method::BatchUpdate {
            operations_msgpack: rmp_serde::to_vec_named(&operations).unwrap(),
        }
    }

    fn commit_at_graph(
        shard: &Shard,
        graph_fname: &str,
        batch: &MutationBatch,
        point: Option<MutationBatchCrashpoint>,
    ) -> Result<MutationBatchCommit, String> {
        #[cfg(feature = "security")]
        let mut audit = AuditTailCache::new();
        commit_mutation_batch_inner(
            shard,
            BatchCommitInput {
                graph_fname,
                batch,
                change: None,
                authoritative_state_msgpack: None,
                crossmodal: None,
                result_msgpack: Some(&[0x81, 0xa2, b'o', b'k']),
                committed_at_ms: 101,
                audited: true,
                crashpoint: point,
            },
            DurableCrypto::none(),
            #[cfg(feature = "security")]
            &mut audit,
        )
    }

    fn commit_at(
        shard: &Shard,
        batch: &MutationBatch,
        point: Option<MutationBatchCrashpoint>,
    ) -> Result<MutationBatchCommit, String> {
        commit_at_graph(shard, "graph-a", batch, point)
    }

    fn commit_with_result(
        shard: &Shard,
        batch: &MutationBatch,
        result: &[u8],
    ) -> Result<MutationBatchCommit, String> {
        #[cfg(feature = "security")]
        let mut audit = AuditTailCache::new();
        commit_mutation_batch(
            shard,
            "graph-a",
            batch,
            Some(result),
            101,
            DurableCrypto::none(),
            #[cfg(feature = "security")]
            &mut audit,
        )
    }

    fn commit_crossmodal_at(
        shard: &Shard,
        batch: &MutationBatch,
        methods: &[Method],
        vectors: &[VectorUpsert],
        point: Option<MutationBatchCrashpoint>,
    ) -> Result<MutationBatchCommit, String> {
        #[cfg(feature = "security")]
        let mut audit = AuditTailCache::new();
        commit_mutation_batch_inner(
            shard,
            BatchCommitInput {
                graph_fname: "graph-a",
                batch,
                change: None,
                authoritative_state_msgpack: None,
                crossmodal: Some(CrossModalBatchRows {
                    methods,
                    vectors,
                    blob_refs: &[],
                    measurements: &[],
                }),
                result_msgpack: Some(&[0x81, 0xa2, b'o', b'k']),
                committed_at_ms: 101,
                audited: true,
                crashpoint: point,
            },
            DurableCrypto::none(),
            #[cfg(feature = "security")]
            &mut audit,
        )
    }

    fn assert_absent_after_reopen(path: &std::path::Path, batch_id: &str) {
        let reopened = open(path);
        assert!(
            read_one_node(&reopened, "graph-a", "a", DurableCrypto::none())
                .unwrap()
                .is_none()
        );
        assert!(
            read_one_node(&reopened, "graph-a", "b", DurableCrypto::none())
                .unwrap()
                .is_none()
        );
        assert!(
            read_mutation_batch_for_graph(&reopened, "graph-a", batch_id)
                .unwrap()
                .is_none()
        );
        assert!(read_mutation_outbox(&reopened, "graph-a", batch_id)
            .unwrap()
            .is_empty());
    }

    /// Deterministic kill points before the redb commit all reopen as NO mutation:
    /// never one node, status without rows, or an orphan outbox record.
    #[test]
    fn precommit_crashpoints_reopen_with_no_partial_batch() {
        for point in [
            MutationBatchCrashpoint::BeforeRows,
            MutationBatchCrashpoint::AfterRowsBeforeMetadata,
            MutationBatchCrashpoint::BeforeCommit,
        ] {
            let path = temp_path(&format!("pre-{point:?}"));
            {
                let db = open(&path);
                let b = batch("batch-pre", "idem-pre");
                assert!(commit_at(&db, &b, Some(point)).is_err());
            }
            assert_absent_after_reopen(&path, "batch-pre");
            let _ = std::fs::remove_file(path);
        }
    }

    #[test]
    fn mutation_batch_write_budgets_leave_no_graph_or_receipt_effects() {
        let oversized_path = temp_path("oversized-result");
        {
            let db = open(&oversized_path);
            let mutation = batch("batch-oversized-result", "idem-oversized-result");
            let result = vec![0; (64 * 1024 * 1024) + 1];
            assert!(commit_with_result(&db, &mutation, &result).is_err());
            assert_eq!(read_mutation_graph_version(&db, "graph-a").unwrap(), 3);
        }
        assert_absent_after_reopen(&oversized_path, "batch-oversized-result");
        let _ = std::fs::remove_file(oversized_path);

        let collection_path = temp_path("excessive-collection");
        {
            let db = open(&collection_path);
            let mut mutation = batch("batch-excessive-collection", "idem-excessive-collection");
            mutation.outbox = vec![mutation.outbox[0].clone(); 100_001];
            mutation
                .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
                .expect("oversized collection fixture reseals its final body");
            assert!(commit_at(&db, &mutation, None).is_err());
            assert_eq!(read_mutation_graph_version(&db, "graph-a").unwrap(), 3);
        }
        assert_absent_after_reopen(&collection_path, "batch-excessive-collection");
        let _ = std::fs::remove_file(collection_path);
    }

    #[test]
    fn missing_graph_version_is_initial_zero_not_caller_seeded() {
        let path = temp_path("missing-version");
        let db = reopen(&path);
        let error = commit_at(&db, &batch("batch-version", "idem-version"), None).unwrap_err();
        assert!(error.contains("authoritative version is 0"));
        drop(db);
        let _ = std::fs::remove_file(path);
    }

    /// A crash after commit but before acknowledgement reopens as one COMPLETE
    /// committed mutation.  Retrying returns its stored result and does not append
    /// duplicate rows or outbox events.
    #[test]
    fn postcommit_crash_restarts_and_replays_idempotently() {
        let path = temp_path("postcommit");
        let b = batch("batch-post", "idem-post");
        {
            let db = open(&path);
            assert!(
                commit_at(&db, &b, Some(MutationBatchCrashpoint::AfterCommitBeforeAck)).is_err()
            );
        }
        {
            let db = open(&path);
            assert!(read_one_node(&db, "graph-a", "a", DurableCrypto::none())
                .unwrap()
                .is_some());
            assert!(read_one_node(&db, "graph-a", "b", DurableCrypto::none())
                .unwrap()
                .is_some());
            let record = read_mutation_batch_for_graph(&db, "graph-a", "batch-post")
                .unwrap()
                .unwrap();
            assert_eq!(record.status, MutationBatchStatus::Committed);
            let outbox = read_mutation_outbox(&db, "graph-a", "batch-post").unwrap();
            // One physical row per EXPLICIT logical intent, and this batch
            // carries exactly one. The old expectation of three counted "two
            // canonical events" -- one auto-generated per operation -- and no
            // such row exists: `eg_transaction::commit::write_outbox` iterates
            // `batch.outbox` and nothing else, and the sibling
            // `outbox_claim_ack_is_ordered_fenced_and_reconcilable` asserts the
            // same rule verbatim ("one explicit logical intent writes one
            // physical outbox row") while passing. The subject of THIS test --
            // that an idempotent replay adds no outbox row -- is unchanged and
            // is still asserted against the same number below. Corrected
            // 2026-09-11 by the redb_store absolute-green lane (F1).
            assert_eq!(outbox.len(), 1, "one explicit intent, one physical row");

            let mut retry = b.clone();
            retry.envelope = fixture_operation_envelope(
                &retry.identity,
                "principal:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                42,
                "idem-post",
            );
            retry
                .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
                .unwrap();
            assert_ne!(
                retry
                    .envelope
                    .operation()
                    .expect("retry carries an operation envelope")
                    .authority
                    .nonce,
                b.envelope
                    .operation()
                    .expect("original carries an operation envelope")
                    .authority
                    .nonce,
                "the lost-ack retry must use a fresh attempt nonce"
            );
            let replay = commit_at(&db, &retry, None).unwrap();
            assert!(replay.replayed);
            assert_eq!(
                read_mutation_outbox(&db, "graph-a", "batch-post")
                    .unwrap()
                    .len(),
                1
            );
        }
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn caller_scoped_ingress_binds_shard_identity_and_replays_once() {
        let path = temp_path("caller-ingress-binding");
        let mut first = batch("batch-caller-ingress", "idem-caller-ingress");
        let caller_authority = first
            .envelope
            .operation()
            .expect("fixture carries an operation envelope")
            .authority
            .clone();
        let caller_actor = caller_authority.actor.as_str().to_string();
        let schema_digest = first
            .envelope
            .operation()
            .expect("fixture carries an operation envelope")
            .method_schema_digest;
        for intent in &mut first.outbox {
            intent.headers.insert(
                crate::mutation_batch::MUTATION_ACTOR_HEADER.to_string(),
                caller_actor.clone(),
            );
        }
        first.reseal_envelope(schema_digest).unwrap();

        let expected_identity = shard::graph_scope_identity("graph-a").unwrap();
        let mut retry = batch("batch-caller-ingress", "idem-caller-ingress");
        let retry_schema_digest = retry
            .envelope
            .operation()
            .expect("fixture carries an operation envelope")
            .method_schema_digest;
        for intent in &mut retry.outbox {
            intent.headers.insert(
                crate::mutation_batch::MUTATION_ACTOR_HEADER.to_string(),
                caller_actor.clone(),
            );
        }
        retry.reseal_envelope(retry_schema_digest).unwrap();
        assert_ne!(
            caller_authority.nonce,
            retry
                .envelope
                .operation()
                .expect("retry carries an operation envelope")
                .authority
                .nonce,
            "the replay regression must use a fresh attempt nonce"
        );

        {
            let db = open(&path);
            let committed = commit_at(&db, &first, None).unwrap();
            assert!(!committed.replayed);
            assert_eq!(committed.identity, expected_identity);

            let record = read_mutation_batch_for_graph(&db, "graph-a", first.batch_id.as_str())
                .unwrap()
                .unwrap();
            assert_eq!(record.identity, expected_identity);
            assert_eq!(record.batch.identity, expected_identity);
            let operation = record
                .batch
                .envelope
                .operation()
                .expect("durable record retains operation authority");
            assert_eq!(
                operation.serving_principal,
                crate::mutation_apply::ENGINE_LEDGER_PRINCIPAL
            );
            assert_eq!(operation.authority, caller_authority);
            assert_eq!(record.committing_actor().unwrap(), caller_actor);

            let outbox = read_mutation_outbox(&db, "graph-a", first.batch_id.as_str()).unwrap();
            assert!(!outbox.is_empty());
            for row in &outbox {
                assert_eq!(row.identity, expected_identity);
                assert_eq!(
                    row.intent
                        .headers
                        .get(crate::mutation_batch::MUTATION_ACTOR_HEADER),
                    Some(&caller_actor)
                );
            }
            let version = read_mutation_graph_version(&db, "graph-a").unwrap();
            let outbox_count = outbox.len();

            let replay = commit_at(&db, &retry, None).unwrap();
            assert!(replay.replayed);
            assert_eq!(replay.identity, expected_identity);
            assert_eq!(
                read_mutation_graph_version(&db, "graph-a").unwrap(),
                version
            );
            assert_eq!(
                read_mutation_outbox(&db, "graph-a", first.batch_id.as_str())
                    .unwrap()
                    .len(),
                outbox_count
            );
        }

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn caller_route_and_authority_proofs_precede_shard_rebind() {
        let path = temp_path("caller-route-authority-proof");
        let db = open(&path);

        // A batch authorized and compiled for graph-a must not become a graph-b
        // write merely because the persistence caller supplied graph-b as its
        // routing key. The graph-b scope may be lazily bound by the read/write
        // path, but no mutation receipt, row, outbox entry, or version advance
        // may be produced.
        let wrong_route = batch("batch-wrong-route", "idem-wrong-route");
        let error = commit_at_graph(&db, "graph-b", &wrong_route, None).unwrap_err();
        assert!(
            error.contains("caller mutation scope graph"),
            "got: {error}"
        );
        assert_eq!(read_mutation_graph_version(&db, "graph-a").unwrap(), 3);
        assert_eq!(read_mutation_graph_version(&db, "graph-b").unwrap(), 0);
        for graph in ["graph-a", "graph-b"] {
            assert!(read_one_node(&db, graph, "a", DurableCrypto::none())
                .unwrap()
                .is_none());
            assert!(
                read_mutation_batch_for_graph(&db, graph, "batch-wrong-route")
                    .unwrap()
                    .is_none()
            );
            assert!(read_mutation_outbox(&db, graph, "batch-wrong-route")
                .unwrap()
                .is_empty());
        }

        // Re-sealing after changing the caller identity makes the batch body
        // self-consistent, but its authority still names the original tenant
        // and authority scope. That mismatch must be rejected before the
        // identity is replaced by the reserved shard identity.
        let mut wrong_authority = batch("batch-wrong-authority", "idem-wrong-authority");
        wrong_authority.identity = MutationScopeIdentity::graph(
            ScopeTenantId::new("tenant-b").unwrap(),
            LogicalName::new("graph-a").unwrap(),
            IncarnationId::new("incarnation:test:redb-store").unwrap(),
        );
        wrong_authority
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .unwrap();
        let error = commit_at_graph(&db, "graph-a", &wrong_authority, None).unwrap_err();
        assert!(
            error.contains("caller mutation authority scope"),
            "got: {error}"
        );
        assert_eq!(read_mutation_graph_version(&db, "graph-a").unwrap(), 3);
        assert!(read_one_node(&db, "graph-a", "a", DurableCrypto::none())
            .unwrap()
            .is_none());
        assert!(
            read_mutation_batch_for_graph(&db, "graph-a", "batch-wrong-authority")
                .unwrap()
                .is_none()
        );
        assert!(
            read_mutation_outbox(&db, "graph-a", "batch-wrong-authority")
                .unwrap()
                .is_empty()
        );

        // Logical graph names are routed through the same escaping used by
        // the persistence boundary. The pre-bind proof compares the sanitized
        // route key, while the caller's logical identity remains the authority
        // evidence used to derive that key.
        let logical_graph = "graph:a";
        let physical_graph = crate::redb_store::sanitize(logical_graph);
        let logical_identity = MutationScopeIdentity::graph(
            ScopeTenantId::new("tenant-a").unwrap(),
            LogicalName::new(logical_graph).unwrap(),
            IncarnationId::new("incarnation:test:redb-store").unwrap(),
        );
        let mut logical_batch = batch("batch-sanitized-route", "idem-sanitized-route");
        logical_batch.identity = logical_identity.clone();
        logical_batch.envelope = fixture_operation_envelope(
            &logical_identity,
            "principal:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            42,
            "idem-sanitized-route",
        );
        // This is a new physical graph member on the fixture shard, so its
        // in-lock OCC version starts at zero; the route assertion is the
        // behavior under test rather than a carry-over from graph-a.
        logical_batch.version_expectation = VersionExpectation::Graph(0);
        logical_batch
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .unwrap();
        let committed = commit_at_graph(&db, &physical_graph, &logical_batch, None).unwrap();
        assert!(!committed.replayed);
        let mut retry = logical_batch.clone();
        retry.envelope = fixture_operation_envelope(
            &logical_identity,
            "principal:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            42,
            "idem-sanitized-route",
        );
        retry
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .unwrap();
        assert!(
            commit_at_graph(&db, &physical_graph, &retry, None)
                .unwrap()
                .replayed
        );

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn public_batch_reopens_and_replays_edges_vectors_and_tombstones_once() {
        let path = temp_path("public-batch-replay");
        let operations = serde_json::json!([
            {"op": "add_node", "id": "a", "properties": {"text": "alpha"}},
            {"op": "add_node", "id": "b", "properties": {"text": "beta"}},
            {"op": "add_node", "id": "c", "properties": {"text": "gamma"}},
            {"op": "add_edge", "source": "a", "target": "b", "properties": {"kind": "old"}},
            {"op": "add_edge", "source": "a", "target": "b", "properties": {"kind": "also old"}},
            {"op": "upsert_edge", "source": "a", "target": "b", "properties": {"kind": "new"}},
            {"op": "add_edge", "source": "c", "target": "a", "properties": {"kind": "incoming"}},
            {"op": "add_embedding", "id": "a", "embedding": [0.25, 0.75]}
        ]);
        let mut initial = batch("batch-public", "idem-public");
        initial.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Graph,
            domain: DurabilityDomain::GraphRows,
            method: public_batch_method(operations),
        }];
        initial
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .unwrap();
        {
            let db = open(&path);
            let committed = commit_at(&db, &initial, None).unwrap();
            assert!(!committed.replayed);
        }
        {
            let db = open(&path);
            let mut retry = initial.clone();
            retry.envelope = fixture_operation_envelope(
                &retry.identity,
                "principal:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                42,
                "idem-public",
            );
            retry
                .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
                .unwrap();
            assert_ne!(
                retry
                    .envelope
                    .operation()
                    .expect("retry carries an operation envelope")
                    .authority
                    .nonce,
                initial
                    .envelope
                    .operation()
                    .expect("original carries an operation envelope")
                    .authority
                    .nonce,
                "the reopen retry must use a fresh attempt nonce"
            );
            let replay = commit_at(&db, &retry, None).unwrap();
            assert!(replay.replayed, "retry must use the stored batch result");
            let dump = read_graph_dump(&db, "graph-a", DurableCrypto::none())
                .unwrap()
                .unwrap();
            assert_eq!(dump.nodes.len(), 3);
            assert_eq!(dump.edges.len(), 2, "upsert must not duplicate the pair");
            let (_, _, properties) = dump
                .edges
                .iter()
                .find(|(source, target, _)| source == "a" && target == "b")
                .expect("upserted edge");
            let properties: serde_json::Value = rmp_serde::from_slice(properties).unwrap();
            assert_eq!(properties["kind"], "new");
            let semantic: crate::compute::semantic::SemanticStore =
                rmp_serde::from_slice(&dump.semantic).unwrap();
            assert_eq!(semantic.get_embedding("a"), Some(vec![0.25, 0.75]));
        }

        let mut removal = batch("batch-remove-a", "idem-remove-a");
        removal.version_expectation = VersionExpectation::Graph(4);
        removal.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Graph,
            domain: DurabilityDomain::GraphRows,
            method: public_batch_method(serde_json::json!([
                {"op": "remove_node", "id": "a"}
            ])),
        }];
        removal
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("public removal fixture reseals its final body");
        {
            let db = open(&path);
            commit_at(&db, &removal, None).unwrap();
        }
        {
            let db = open(&path);
            let dump = read_graph_dump(&db, "graph-a", DurableCrypto::none())
                .unwrap()
                .unwrap();
            assert_eq!(dump.nodes.len(), 2);
            assert!(
                dump.edges.is_empty(),
                "outgoing and incoming edges must tombstone"
            );
            let semantic: crate::compute::semantic::SemanticStore =
                rmp_serde::from_slice(&dump.semantic).unwrap();
            assert_eq!(semantic.get_embedding("a"), None);
        }
        let _ = std::fs::remove_file(path);
    }

    /// BUG-CX-096: `read_graph_dump` used to expose ONLY `GRAPH_META` /
    /// `MUTATION_GRAPH_VERSION` / `NODES` / `EDGES` / `LEDGER` / `SEMANTIC`, making
    /// the 10 `development_lane_*` and 9 `resource_*` tables invisible to dump
    /// diagnostics. They are now observable but explicitly read-only: `GraphDump`
    /// remains incomplete for transfer, and `apply_checkpoint` rejects this origin.
    /// Seed one row directly into EVERY one of those 19 tables (raw redb inserts,
    /// bypassing every native-operation precondition — the same technique
    /// `redb_backend::tests::seed_raw_two_str_row` uses for the sibling BUG-CX-016/054
    /// reshard/backup coverage tests), then prove every row is present on
    /// `dump.native`. `read_graph_dump`'s scans copy these blobs through unsealed but
    /// otherwise UNDECODED (no typed `DurableLaneHold`/`DurableResourceReservation`
    /// deserialization — see `NativeOperationDumpRows`'s doc), so an arbitrary
    /// byte/text/int payload is a legitimate, minimal seed here.
    ///
    /// Confirmed FAILING before this fix: `GraphDump` had no `native` field at all
    /// (`cargo check` errors `no field \`native\` on type \`GraphDump\``) — the
    /// absence of the field IS the observability defect this test closes.
    #[test]
    fn read_graph_dump_carries_every_native_lane_and_resource_table() {
        let path = temp_path("native-dump-coverage");
        let db = open(&path);
        let seed = batch("batch-native-dump-seed", "idem-native-dump-seed");
        commit_at(&db, &seed, None).unwrap();

        {
            let members = db.graph_members(&["graph-a"]).unwrap();
            let (group, batches) = db.admit_maintenance(&members, "native-dump-seed").unwrap();
            let write = ShardWrite::open(&db, &group, &members, &batches).unwrap();
            {
                let mut t = write
                    .graph("graph-a")
                    .unwrap()
                    .open_scoped_table(development_lane::HOLDS)
                    .unwrap();
                t.insert(("graph-a", "hold-1"), b"hold-bytes".as_slice())
                    .unwrap();
            }
            {
                let mut t = write
                    .graph("graph-a")
                    .unwrap()
                    .open_scoped_table(development_lane::TENANT_INDEX)
                    .unwrap();
                t.insert(("graph-a", "tenant-1", "hold-1"), "hold-1")
                    .unwrap();
            }
            {
                let mut t = write
                    .graph("graph-a")
                    .unwrap()
                    .open_scoped_table(development_lane::LANE_INDEX)
                    .unwrap();
                t.insert(("graph-a", "tenant-1", "lane-1"), "hold-1")
                    .unwrap();
            }
            {
                let mut t = write
                    .graph("graph-a")
                    .unwrap()
                    .open_scoped_table(development_lane::REPOSITORY_BRANCH_INDEX)
                    .unwrap();
                t.insert(("graph-a", "tenant-1", "branch-1"), "hold-1")
                    .unwrap();
            }
            {
                let mut t = write
                    .graph("graph-a")
                    .unwrap()
                    .open_scoped_table(development_lane::WORKTREE_INDEX)
                    .unwrap();
                t.insert(("graph-a", "worktree-1"), "hold-1").unwrap();
            }
            {
                let mut t = write
                    .graph("graph-a")
                    .unwrap()
                    .open_scoped_table(development_lane::WORK_ITEM_INDEX)
                    .unwrap();
                t.insert(("graph-a", "tenant-1", 1u64), "hold-1").unwrap();
            }
            {
                let mut t = write
                    .graph("graph-a")
                    .unwrap()
                    .open_scoped_table(development_lane::COUNTERS)
                    .unwrap();
                t.insert(("graph-a", "scope-1"), b"counter-bytes".as_slice())
                    .unwrap();
            }
            {
                let mut t = write
                    .graph("graph-a")
                    .unwrap()
                    .open_scoped_table(development_lane::PRESSURE_INDEX)
                    .unwrap();
                t.insert(
                    (
                        "graph-a",
                        "tenant-1",
                        "scope-1",
                        "metric-1",
                        5u64,
                        "counter-1",
                    ),
                    1u8,
                )
                .unwrap();
            }
            {
                let mut t = write
                    .graph("graph-a")
                    .unwrap()
                    .open_scoped_table(development_lane::POLICIES)
                    .unwrap();
                t.insert(("graph-a", "tenant-1"), b"policy-bytes".as_slice())
                    .unwrap();
            }
            {
                let mut t = write
                    .graph("graph-a")
                    .unwrap()
                    .open_scoped_table(development_lane::INVOCATIONS)
                    .unwrap();
                t.insert(
                    ("graph-a", "tenant-1", "invocation-1"),
                    b"invocation-bytes".as_slice(),
                )
                .unwrap();
            }
            {
                let mut t = write
                    .graph("graph-a")
                    .unwrap()
                    .open_scoped_table(RESOURCE_RESERVATIONS)
                    .unwrap();
                t.insert(
                    ("graph-a", "reservation-1"),
                    b"reservation-bytes".as_slice(),
                )
                .unwrap();
            }
            {
                let mut t = write
                    .graph("graph-a")
                    .unwrap()
                    .open_scoped_table(RESOURCE_RESERVATION_TENANT_INDEX)
                    .unwrap();
                t.insert(("graph-a", "tenant-1", "reservation-1"), "reservation-1")
                    .unwrap();
            }
            {
                let mut t = write
                    .graph("graph-a")
                    .unwrap()
                    .open_scoped_table(RESOURCE_RESERVATION_ATTEMPTS)
                    .unwrap();
                t.insert(("graph-a", "work-item-1", 1u64), "reservation-1")
                    .unwrap();
            }
            {
                let mut t = write
                    .graph("graph-a")
                    .unwrap()
                    .open_scoped_table(RESOURCE_HOSTS)
                    .unwrap();
                t.insert(("graph-a", "host-1"), b"host-bytes".as_slice())
                    .unwrap();
            }
            {
                let mut t = write
                    .graph("graph-a")
                    .unwrap()
                    .open_scoped_table(RESOURCE_EXCLUSIVITY)
                    .unwrap();
                t.insert(("graph-a", "exclusivity-1"), "reservation-1")
                    .unwrap();
            }
            {
                let mut t = write
                    .graph("graph-a")
                    .unwrap()
                    .open_scoped_table(RESOURCE_FAIRNESS)
                    .unwrap();
                t.insert(("graph-a", "group-1"), b"fairness-bytes".as_slice())
                    .unwrap();
            }
            {
                let mut t = write
                    .graph("graph-a")
                    .unwrap()
                    .open_scoped_table(RESOURCE_CONCURRENCY)
                    .unwrap();
                t.insert(("graph-a", "key-1"), 3u64).unwrap();
            }
            {
                let mut t = write
                    .graph("graph-a")
                    .unwrap()
                    .open_scoped_table(RESOURCE_ANTI_AFFINITY)
                    .unwrap();
                t.insert(("graph-a", "host-1", "tag-1"), 2u64).unwrap();
            }
            {
                let mut t = write
                    .graph("graph-a")
                    .unwrap()
                    .open_scoped_table(RESOURCE_DISK_POLICIES)
                    .unwrap();
                t.insert(("graph-a", "policy-1"), b"disk-policy-bytes".as_slice())
                    .unwrap();
            }
            write.finish().unwrap();
            db.commit_drain(group, &batches, 0).unwrap();
        }

        let dump = read_graph_dump(&db, "graph-a", DurableCrypto::none())
            .unwrap()
            .unwrap();
        assert_eq!(dump.kind, GraphDumpKind::DurableReadOnlyMaterialization);

        assert_eq!(
            dump.native.development_lane_holds,
            vec![("hold-1".to_string(), b"hold-bytes".to_vec())]
        );
        assert_eq!(
            dump.native.development_lane_tenant_index,
            vec![(
                ("tenant-1".to_string(), "hold-1".to_string()),
                "hold-1".to_string()
            )]
        );
        assert_eq!(
            dump.native.development_lane_lane_index,
            vec![(
                ("tenant-1".to_string(), "lane-1".to_string()),
                "hold-1".to_string()
            )]
        );
        assert_eq!(
            dump.native.development_lane_repository_branch_index,
            vec![(
                ("tenant-1".to_string(), "branch-1".to_string()),
                "hold-1".to_string()
            )]
        );
        assert_eq!(
            dump.native.development_lane_worktree_index,
            vec![("worktree-1".to_string(), "hold-1".to_string())]
        );
        assert_eq!(
            dump.native.development_lane_work_item_index,
            vec![(("tenant-1".to_string(), 1u64), "hold-1".to_string())]
        );
        assert_eq!(
            dump.native.development_lane_counters,
            vec![("scope-1".to_string(), b"counter-bytes".to_vec())]
        );
        assert_eq!(
            dump.native.development_lane_pressure_index,
            vec![(
                (
                    "tenant-1".to_string(),
                    "scope-1".to_string(),
                    "metric-1".to_string(),
                    5u64,
                    "counter-1".to_string()
                ),
                1u8
            )]
        );
        assert_eq!(
            dump.native.development_lane_policies,
            vec![("tenant-1".to_string(), b"policy-bytes".to_vec())]
        );
        assert_eq!(
            dump.native.development_lane_invocations,
            vec![(
                ("tenant-1".to_string(), "invocation-1".to_string()),
                b"invocation-bytes".to_vec()
            )]
        );
        assert_eq!(
            dump.native.resource_reservations,
            vec![("reservation-1".to_string(), b"reservation-bytes".to_vec())]
        );
        assert_eq!(
            dump.native.resource_reservation_tenant_index,
            vec![(
                ("tenant-1".to_string(), "reservation-1".to_string()),
                "reservation-1".to_string()
            )]
        );
        assert_eq!(
            dump.native.resource_reservation_attempts,
            vec![(
                ("work-item-1".to_string(), 1u64),
                "reservation-1".to_string()
            )]
        );
        assert_eq!(
            dump.native.resource_hosts,
            vec![("host-1".to_string(), b"host-bytes".to_vec())]
        );
        assert_eq!(
            dump.native.resource_exclusivity,
            vec![("exclusivity-1".to_string(), "reservation-1".to_string())]
        );
        assert_eq!(
            dump.native.resource_fairness,
            vec![("group-1".to_string(), b"fairness-bytes".to_vec())]
        );
        assert_eq!(
            dump.native.resource_concurrency,
            vec![("key-1".to_string(), 3u64)]
        );
        assert_eq!(
            dump.native.resource_anti_affinity,
            vec![(("host-1".to_string(), "tag-1".to_string()), 2u64)]
        );
        assert_eq!(
            dump.native.resource_disk_policies,
            vec![("policy-1".to_string(), b"disk-policy-bytes".to_vec())]
        );

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn durable_read_dump_is_rejected_before_checkpoint_mutation() {
        let path = temp_path("read-dump-checkpoint-refusal");
        let db = open(&path);
        let seed = batch("batch-read-dump-refusal", "idem-read-dump-refusal");
        commit_at(&db, &seed, None).unwrap();

        let dump = read_graph_dump(&db, "graph-a", DurableCrypto::none())
            .unwrap()
            .expect("seed graph identity");
        assert_eq!(dump.kind, GraphDumpKind::DurableReadOnlyMaterialization);
        assert!(
            dump.native.is_empty(),
            "the origin marker must reject even a read with no native rows"
        );
        let incarnation_id = dump.incarnation_id.clone();
        let source_snapshot_version = dump.source_snapshot_version;
        let mut pending = vec![("graph-a".to_string(), Method::ClearGraph)];

        let error = apply_checkpoint(&db, &mut pending, vec![dump], DurableCrypto::none())
            .expect_err("a durable read is not a complete transfer image");
        assert!(error.contains("RedbBackend::reshard_graph"));
        assert_eq!(pending.len(), 1, "rejection must not consume pending work");
        assert!(matches!(pending[0].1, Method::ClearGraph));

        let after = read_graph_dump(&db, "graph-a", DurableCrypto::none())
            .unwrap()
            .expect("rejection must preserve destination rows");
        assert_eq!(after.incarnation_id, incarnation_id);
        assert_eq!(after.source_snapshot_version, source_snapshot_version);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn duplicate_checkpoint_graph_ids_are_rejected_before_pending_mutation() {
        let path = temp_path("duplicate-checkpoint-graph-id");
        let db = open(&path);
        let dump = || {
            GraphDump::in_place_core_checkpoint(InPlaceCoreCheckpoint {
                graph: "graph-a".to_string(),
                name: "graph-a".to_string(),
                graph_type: GraphType::Global,
                incarnation_id: "incarnation:test:duplicate-checkpoint".to_string(),
                source_snapshot_version: 1,
                integrity_policy: None,
                nodes: Vec::new(),
                edges: Vec::new(),
                ledger: Vec::new(),
                semantic: Vec::new(),
            })
        };
        let mut pending = vec![("graph-a".to_string(), Method::ClearGraph)];

        let error = apply_checkpoint(
            &db,
            &mut pending,
            vec![dump(), dump()],
            DurableCrypto::none(),
        )
        .expect_err("duplicate graph images are ambiguous");
        assert_eq!(error, "checkpoint contains duplicate graph id");
        assert_eq!(pending.len(), 1, "rejection must not consume pending work");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn in_place_checkpoint_rejects_attached_native_authority_before_mutation() {
        let path = temp_path("native-authority-checkpoint-refusal");
        let db = open(&path);
        let mut dump = GraphDump::in_place_core_checkpoint(InPlaceCoreCheckpoint {
            graph: "graph-a".to_string(),
            name: "graph-a".to_string(),
            graph_type: GraphType::Global,
            incarnation_id: "incarnation:test:native-authority-refusal".to_string(),
            source_snapshot_version: 1,
            integrity_policy: None,
            nodes: Vec::new(),
            edges: Vec::new(),
            ledger: Vec::new(),
            semantic: Vec::new(),
        });
        dump.native
            .resource_hosts
            .push(("host-a".to_string(), Vec::new()));
        let mut pending = vec![("graph-a".to_string(), Method::ClearGraph)];

        let error = apply_checkpoint(&db, &mut pending, vec![dump], DurableCrypto::none())
            .expect_err("ordinary checkpoints cannot carry native authority rows");
        assert!(error.contains("RedbBackend::reshard_graph"));
        assert_eq!(pending.len(), 1, "rejection must not consume pending work");
        assert!(matches!(pending[0].1, Method::ClearGraph));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn public_batch_upsert_node_merges_durable_fields_across_reopen() {
        let path = temp_path("public-batch-node-upsert");
        let mut seed = batch("batch-upsert-seed", "idem-upsert-seed");
        seed.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Graph,
            domain: DurabilityDomain::GraphRows,
            method: public_batch_method(serde_json::json!([{
                "op": "add_node",
                "id": "existing",
                "properties": {
                    "retained": "yes",
                    "overwritten": "old",
                    "nested": {"left": 1, "right": 2}
                }
            }])),
        }];
        seed.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("public seed fixture reseals its final body");
        let mut upsert = batch("batch-upsert-merge", "idem-upsert-merge");
        upsert.version_expectation = VersionExpectation::Graph(4);
        upsert.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Graph,
            domain: DurabilityDomain::GraphRows,
            method: public_batch_method(serde_json::json!([
                {
                    "op": "upsert_node",
                    "id": "existing",
                    "properties": {
                        "overwritten": "new",
                        "added": true,
                        "nested": {"left": 9}
                    }
                },
                {"op": "upsert_node", "id": "created", "properties": {"created": true}}
            ])),
        }];
        upsert
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("public upsert fixture reseals its final body");
        {
            let db = open(&path);
            commit_at(&db, &seed, None).unwrap();
            commit_at(&db, &upsert, None).unwrap();
        }
        {
            let db = open(&path);
            let dump = read_graph_dump(&db, "graph-a", DurableCrypto::none())
                .unwrap()
                .unwrap();
            let existing = dump
                .nodes
                .iter()
                .find(|(id, _)| id == "existing")
                .expect("existing node");
            let existing: serde_json::Value = rmp_serde::from_slice(&existing.1).unwrap();
            assert_eq!(existing["retained"], "yes");
            assert_eq!(existing["overwritten"], "new");
            assert_eq!(existing["added"], true);
            assert_eq!(existing["nested"], serde_json::json!({"left": 9}));
            let created = dump
                .nodes
                .iter()
                .find(|(id, _)| id == "created")
                .expect("created node");
            let created: serde_json::Value = rmp_serde::from_slice(&created.1).unwrap();
            assert_eq!(created, serde_json::json!({"created": true}));
        }
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn malformed_or_state_invalid_public_batch_rolls_back_all_rows() {
        for (tag, method) in [
            (
                "opaque",
                Method::BatchUpdate {
                    operations_msgpack: vec![0xc1],
                },
            ),
            (
                "missing-endpoint",
                public_batch_method(serde_json::json!([
                    {"op": "add_node", "id": "partial", "properties": {}},
                    {"op": "add_edge", "source": "partial", "target": "missing", "properties": {}}
                ])),
            ),
        ] {
            let path = temp_path(tag);
            let mut mutation = batch("batch-invalid", "idem-invalid");
            mutation.operations = vec![MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Graph,
                domain: DurabilityDomain::GraphRows,
                method,
            }];
            mutation
                .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
                .expect("malformed public fixture reseals its final body");
            {
                let db = open(&path);
                assert!(commit_at(&db, &mutation, None).is_err());
            }
            let db = open(&path);
            assert!(
                read_one_node(&db, "graph-a", "partial", DurableCrypto::none())
                    .unwrap()
                    .is_none(),
                "redb must discard earlier rows when a later operation fails"
            );
            assert!(
                read_mutation_batch_for_graph(&db, "graph-a", "batch-invalid")
                    .unwrap()
                    .is_none()
            );
            drop(db);
            let _ = std::fs::remove_file(path);
        }
    }

    #[test]
    fn terminal_work_item_retry_replays_and_conflicting_payload_fails_closed() {
        let path = temp_path("work-item-terminal-replay");
        let db = open(&path);

        let mut seed = batch("work-item-terminal-seed", "work-item-terminal-seed-key");
        seed.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Graph,
            domain: DurabilityDomain::GraphRows,
            method: ready_work_item_method("work-1", 3),
        }];
        seed.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("terminal seed fixture reseals its final body");
        commit_at(&db, &seed, None).unwrap();
        let claimed = commit_native_claim(
            &db,
            "work-item-terminal-claim",
            "work-item-terminal-claim-key",
            4,
            Some("work-1"),
            "worker-a",
            0,
            60_000,
            64,
        );
        assert!(claimed.claimed);
        assert_eq!(claimed.lease_epoch, Some(1));
        assert_eq!(claimed.fencing_token, Some(1));

        let terminal_method = Method::CommitWorkItemResult {
            tenant: "tenant-a".into(),
            work_item_id: "work-1".into(),
            worker_id: "worker-a".into(),
            lease_epoch: 1,
            fencing_token: 1,
            idempotency_key: "terminal-key".into(),
            outcome: "succeeded".into(),
            result_ref: Some("result:sha256:one".into()),
            outcome_extension: None,
            error_ref: None,
            retryable: false,
            now_ms: 1_000,
        };
        let mut terminal = batch(
            "work:terminal-stable-batch",
            "work-idem:terminal-stable-key",
        );
        // The attempt metadata this used to re-stamp -- request id, purpose,
        // policy fingerprint, trace id -- is either gone or structurally outside
        // the stable replay identity now, so a fixture that wants a distinct
        // request simply re-mints the envelope for it.
        terminal.envelope = fixture_operation_envelope(
            &terminal.identity,
            &format!("principal:sha256:{}", "a".repeat(64)),
            777,
            "work-idem:terminal-stable-key",
        );
        let remint_attempt = |batch: &mut MutationBatch, nonce_byte: u8, created_at_ms: u64| {
            let eg_types::mutation_batch::MutationEnvelope::Operation(operation) =
                &mut batch.envelope
            else {
                panic!("authenticated replay fixture must carry an operation envelope");
            };
            operation.authority.nonce = eg_types::contract::Nonce::from_bytes([nonce_byte; 32]);
            operation.authority.context_digest = operation
                .authority
                .recompute_context_digest()
                .expect("fixture authority context remains valid after nonce rotation");
            batch.created_at_ms = created_at_ms;
        };
        // `CommitWorkItemResult` is a `native_terminal_work_item_cas` batch:
        // `check_occ_version_and_fence` never checks its expectation against
        // the authoritative version (the WorkItem lease/fencing token is its
        // real CAS guard), but `VersionExpectation` no longer has a "none"
        // arm to encode that -- 5 is simply the actual current graph version
        // at this point (seed 3->4, claim 4->5), matching real state rather
        // than an invented placeholder.
        terminal.version_expectation = VersionExpectation::Graph(5);
        terminal.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Job,
            domain: DurabilityDomain::ControlPlane,
            method: terminal_method,
        }];
        terminal.outbox[0].key = terminal.batch_id.clone();
        terminal
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("terminal fixture reseals its final body");

        let first = commit_at(&db, &terminal, None).unwrap();
        assert!(!first.replayed);
        assert_eq!(read_mutation_graph_version(&db, "graph-a").unwrap(), 6);

        let consumed_nonce = commit_at(&db, &terminal, None).unwrap_err();
        assert!(
            consumed_nonce.contains("REPLAY_NONCE_CONSUMED"),
            "{consumed_nonce}"
        );

        // A fresh transport request is normalized to the same durable request id
        // before this kernel sees it. Re-mint its attempt nonce while preserving
        // the stable operation key and body, then replay the stored result.
        let mut retry = terminal.clone();
        retry.envelope = fixture_operation_envelope(
            &retry.identity,
            &format!("principal:sha256:{}", "a".repeat(64)),
            778,
            "work-idem:terminal-stable-key",
        );
        retry
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("terminal retry fixture reseals its final body");
        assert_ne!(
            retry
                .envelope
                .operation()
                .expect("retry carries an operation envelope")
                .authority
                .nonce,
            terminal
                .envelope
                .operation()
                .expect("original carries an operation envelope")
                .authority
                .nonce,
            "a retry must use a fresh attempt nonce"
        );
        retry.created_at_ms = 200;
        let replay = commit_at(&db, &retry, None).unwrap();
        assert!(replay.replayed);
        assert_eq!(read_mutation_graph_version(&db, "graph-a").unwrap(), 6);

        let mut conflicting_payload = retry.clone();
        let Method::CommitWorkItemResult { result_ref, .. } =
            &mut conflicting_payload.operations[0].method
        else {
            unreachable!();
        };
        *result_ref = Some("result:sha256:different".into());
        remint_attempt(&mut conflicting_payload, 0x43, 300);
        conflicting_payload
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .unwrap();
        let error = commit_at(&db, &conflicting_payload, None).unwrap_err();
        assert!(error.contains("IDEMPOTENCY_CONFLICT"));

        // Planted known-bad input: the same key under a DIFFERENT actor. The
        // actor is inside the stable operation identity, so this is a named
        // conflict rather than a silent replay -- the cross-actor
        // replay-ownership property the M1 review raised as a P1.
        let mut conflicting_authority = retry;
        let key = conflicting_authority.idempotency_key().to_string();
        let identity = conflicting_authority.identity.clone();
        conflicting_authority.envelope = fixture_operation_envelope(
            &identity,
            &format!("principal:sha256:{}", "b".repeat(64)),
            42,
            &key,
        );
        conflicting_authority
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("conflicting authority fixture reseals its final body");
        let error = commit_at(&db, &conflicting_authority, None).unwrap_err();
        assert!(error.contains("IDEMPOTENCY_CONFLICT"));
        assert_eq!(read_mutation_graph_version(&db, "graph-a").unwrap(), 6);

        let stored = read_one_node(&db, "graph-a", "work-1", DurableCrypto::none())
            .unwrap()
            .unwrap();
        let stored: serde_json::Value = decode_durable(&stored).unwrap();
        assert_eq!(stored["status"], "succeeded");
        assert_eq!(stored["result_ref"], "result:sha256:one");
        drop(db);
        let _ = std::fs::remove_file(path);
    }

    // GOC-19/GOC-20 (BUG-015 "B9"): a `CommitWorkItemResult` batch may
    // co-commit provenance `AddNode` operations (RunTrace/ToolCall/
    // OutcomeEvaluation) in the SAME redb write transaction as the WorkItem's
    // terminal status -- proven here against a real redb-backed database, not
    // just the pure-Rust admission logic in `eg-types::work_item_command_log`.
    #[test]
    fn commit_work_item_result_co_commits_provenance_add_node_operations() {
        let path = temp_path("work-item-outcome-bundle-fusion");
        let db = open(&path);

        let mut seed = batch("work-item-bundle-seed", "work-item-bundle-seed-key");
        seed.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Transaction,
            domain: DurabilityDomain::GraphRows,
            method: ready_work_item_method("work-bundle-1", 3),
        }];
        seed.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("bundle seed fixture reseals its final body");
        commit_at(&db, &seed, None).unwrap();
        let claimed = commit_native_claim(
            &db,
            "work-item-bundle-claim",
            "work-item-bundle-claim-key",
            4,
            Some("work-bundle-1"),
            "worker-a",
            0,
            60_000,
            64,
        );
        assert!(claimed.claimed);

        let terminal_method = Method::CommitWorkItemResult {
            tenant: "tenant-a".into(),
            work_item_id: "work-bundle-1".into(),
            worker_id: "worker-a".into(),
            lease_epoch: claimed.lease_epoch.unwrap(),
            fencing_token: claimed.fencing_token.unwrap(),
            idempotency_key: "bundle-terminal-key".into(),
            outcome: "succeeded".into(),
            result_ref: Some("result:sha256:bundled".into()),
            outcome_extension: None,
            error_ref: None,
            retryable: false,
            now_ms: 1_000,
        };
        let mut terminal = batch("work:bundle-batch", "work-idem:bundle-key");
        // See the identical comment in
        // `terminal_work_item_retry_replays_and_conflicting_payload_fails_closed`:
        // this is a `native_terminal_work_item_cas` batch whose expectation is
        // never checked; 5 is the real current graph version here too (seed
        // 3->4, claim 4->5).
        terminal.version_expectation = VersionExpectation::Graph(5);
        terminal.operations = vec![
            MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Job,
                domain: DurabilityDomain::ControlPlane,
                method: terminal_method,
            },
            MutationOperation {
                ordinal: 1,
                surface: MutationSurface::Job,
                domain: DurabilityDomain::GraphSnapshot,
                method: node("trace:bundle-1", 11),
            },
            MutationOperation {
                ordinal: 2,
                surface: MutationSurface::Job,
                domain: DurabilityDomain::GraphSnapshot,
                method: node("outcome:bundle-1", 22),
            },
        ];
        terminal.outbox[0].key = terminal.batch_id.clone();
        terminal
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("bundle terminal fixture reseals its final body");

        commit_at(&db, &terminal, None).unwrap();

        let work_item = read_one_node(&db, "graph-a", "work-bundle-1", DurableCrypto::none())
            .unwrap()
            .unwrap();
        let work_item: serde_json::Value = decode_durable(&work_item).unwrap();
        assert_eq!(work_item["status"], "succeeded");
        assert_eq!(work_item["result_ref"], "result:sha256:bundled");

        // The KNOWN-BAD half of this proof lives in the two tests below: this
        // establishes the PASS-on-good baseline -- both provenance nodes are
        // durable, in the SAME commit that landed the terminal status.
        let trace = read_one_node(&db, "graph-a", "trace:bundle-1", DurableCrypto::none())
            .unwrap()
            .unwrap();
        let trace: serde_json::Value = decode_durable(&trace).unwrap();
        assert_eq!(trace["value"], 11);

        let outcome = read_one_node(&db, "graph-a", "outcome:bundle-1", DurableCrypto::none())
            .unwrap()
            .unwrap();
        let outcome: serde_json::Value = decode_durable(&outcome).unwrap();
        assert_eq!(outcome["value"], 22);

        drop(db);
        let _ = std::fs::remove_file(path);
    }

    // KNOWN-BAD: a CommitWorkItemResult batch may NOT carry an arbitrary
    // accompanying method (only AddNode provenance operations are allowed) --
    // and rejecting it must leave NEITHER the WorkItem status NOR the
    // disallowed operation's row durable (no partial commit).
    #[test]
    fn commit_work_item_result_batch_rejects_a_disallowed_accompanying_method() {
        let path = temp_path("work-item-outcome-bundle-disallowed");
        let db = open(&path);

        let mut seed = batch("work-item-disallowed-seed", "work-item-disallowed-seed-key");
        seed.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Transaction,
            domain: DurabilityDomain::GraphRows,
            method: ready_work_item_method("work-disallowed-1", 3),
        }];
        seed.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("disallowed seed fixture reseals its final body");
        commit_at(&db, &seed, None).unwrap();
        let claimed = commit_native_claim(
            &db,
            "work-item-disallowed-claim",
            "work-item-disallowed-claim-key",
            4,
            Some("work-disallowed-1"),
            "worker-a",
            0,
            60_000,
            64,
        );
        assert!(claimed.claimed);

        let terminal_method = Method::CommitWorkItemResult {
            tenant: "tenant-a".into(),
            work_item_id: "work-disallowed-1".into(),
            worker_id: "worker-a".into(),
            lease_epoch: claimed.lease_epoch.unwrap(),
            fencing_token: claimed.fencing_token.unwrap(),
            idempotency_key: "disallowed-terminal-key".into(),
            outcome: "succeeded".into(),
            result_ref: Some("result:sha256:disallowed".into()),
            outcome_extension: None,
            error_ref: None,
            retryable: false,
            now_ms: 1_000,
        };
        let mut terminal = batch("work:disallowed-batch", "work-idem:disallowed-key");
        // A disallowed accompanying method makes `native_terminal_work_item_cas`
        // false (it re-validates every operation, not just `len()`), so this
        // batch is no longer exempt from graph-wide OCC -- supply the real
        // current version (seed 3->4, claim 4->5) so the batch reaches the
        // per-operation shape guard this test targets, instead of failing
        // earlier on a mismatched/missing `expected_graph_version`.
        terminal.version_expectation = VersionExpectation::Graph(5);
        terminal.operations = vec![
            MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Job,
                domain: DurabilityDomain::ControlPlane,
                method: terminal_method,
            },
            // Disallowed: only AddNode may ride alongside CommitWorkItemResult.
            MutationOperation {
                ordinal: 1,
                surface: MutationSurface::Job,
                domain: DurabilityDomain::GraphSnapshot,
                method: Method::RemoveNode {
                    node_id: "work-disallowed-1".into(),
                },
            },
        ];
        terminal.outbox[0].key = terminal.batch_id.clone();
        terminal
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("disallowed terminal fixture reseals its final body");

        let error = commit_at(&db, &terminal, None).unwrap_err();
        assert!(
            error.contains("may only carry additional AddNode"),
            "got: {error}"
        );

        // No partial effect: the WorkItem is still `leased`, not `succeeded`.
        let work_item = read_one_node(&db, "graph-a", "work-disallowed-1", DurableCrypto::none())
            .unwrap()
            .unwrap();
        let work_item: serde_json::Value = decode_durable(&work_item).unwrap();
        assert_ne!(work_item["status"], "succeeded");

        drop(db);
        let _ = std::fs::remove_file(path);
    }

    // KNOWN-BAD: a batch carrying TWO CommitWorkItemResult operations must be
    // rejected outright, never applying either.
    #[test]
    fn commit_work_item_result_batch_rejects_more_than_one_terminal_operation() {
        let path = temp_path("work-item-outcome-bundle-double-terminal");
        let db = open(&path);

        let mut seed = batch("work-item-double-seed", "work-item-double-seed-key");
        seed.operations = vec![
            MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Transaction,
                domain: DurabilityDomain::GraphRows,
                method: ready_work_item_method("work-double-1", 3),
            },
            MutationOperation {
                ordinal: 1,
                surface: MutationSurface::Transaction,
                domain: DurabilityDomain::GraphRows,
                method: ready_work_item_method("work-double-2", 3),
            },
        ];
        seed.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("double terminal seed fixture reseals its final body");
        commit_at(&db, &seed, None).unwrap();
        // Seed is ONE batch creating two ready WorkItems (3->4); each claim is
        // its own batch and bumps the version once more (4->5, then 5->6).
        let claimed_1 = commit_native_claim(
            &db,
            "work-item-double-claim-1",
            "work-item-double-claim-1-key",
            4,
            Some("work-double-1"),
            "worker-a",
            0,
            60_000,
            64,
        );
        assert!(claimed_1.claimed);
        let claimed_2 = commit_native_claim(
            &db,
            "work-item-double-claim-2",
            "work-item-double-claim-2-key",
            5,
            Some("work-double-2"),
            "worker-b",
            0,
            60_000,
            64,
        );
        assert!(claimed_2.claimed);

        let mut terminal = batch("work:double-batch", "work-idem:double-key");
        // Two CommitWorkItemResult operations also make
        // `native_terminal_work_item_cas` false (not all-AddNode after the
        // first), so -- same reasoning as the disallowed-method test above --
        // supply the real current version (6) to reach the per-operation shape
        // guard rather than failing earlier on OCC.
        terminal.version_expectation = VersionExpectation::Graph(6);
        terminal.operations = vec![
            MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Job,
                domain: DurabilityDomain::ControlPlane,
                method: Method::CommitWorkItemResult {
                    tenant: "tenant-a".into(),
                    work_item_id: "work-double-1".into(),
                    worker_id: "worker-a".into(),
                    lease_epoch: claimed_1.lease_epoch.unwrap(),
                    fencing_token: claimed_1.fencing_token.unwrap(),
                    idempotency_key: "double-terminal-key-1".into(),
                    outcome: "succeeded".into(),
                    result_ref: Some("result:sha256:double-one".into()),
                    outcome_extension: None,
                    error_ref: None,
                    retryable: false,
                    now_ms: 1_000,
                },
            },
            MutationOperation {
                ordinal: 1,
                surface: MutationSurface::Job,
                domain: DurabilityDomain::ControlPlane,
                method: Method::CommitWorkItemResult {
                    tenant: "tenant-a".into(),
                    work_item_id: "work-double-2".into(),
                    worker_id: "worker-b".into(),
                    lease_epoch: claimed_2.lease_epoch.unwrap(),
                    fencing_token: claimed_2.fencing_token.unwrap(),
                    idempotency_key: "double-terminal-key-2".into(),
                    outcome: "succeeded".into(),
                    result_ref: Some("result:sha256:double-two".into()),
                    outcome_extension: None,
                    error_ref: None,
                    retryable: false,
                    now_ms: 1_000,
                },
            },
        ];
        terminal.outbox[0].key = terminal.batch_id.clone();
        terminal
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("double terminal fixture reseals its final body");

        let error = commit_at(&db, &terminal, None).unwrap_err();
        assert!(
            error.contains("at most one CommitWorkItemResult"),
            "got: {error}"
        );

        for work_item_id in ["work-double-1", "work-double-2"] {
            let work_item = read_one_node(&db, "graph-a", work_item_id, DurableCrypto::none())
                .unwrap()
                .unwrap();
            let work_item: serde_json::Value = decode_durable(&work_item).unwrap();
            assert_ne!(work_item["status"], "succeeded");
        }

        drop(db);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn generic_work_item_claim_enforces_tenant_in_flight_limit() {
        let path = temp_path("work-item-quota");
        let db = open(&path);
        let mut seed = batch("work-item-seed", "work-item-seed-key");
        seed.operations = vec![
            MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Transaction,
                domain: DurabilityDomain::GraphRows,
                method: ready_work_item_method("leased", 3),
            },
            MutationOperation {
                ordinal: 1,
                surface: MutationSurface::Transaction,
                domain: DurabilityDomain::GraphRows,
                method: ready_work_item_method("ready", 3),
            },
        ];
        seed.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("quota seed fixture reseals its final body");
        commit_at(&db, &seed, None).unwrap();
        let claimed = commit_native_claim(
            &db,
            "work-item-native-lease",
            "work-item-native-lease-key",
            4,
            Some("leased"),
            "worker-a",
            1_000,
            10_000,
            64,
        );
        assert!(claimed.claimed);

        let mut claim = batch("work-item-claim", "work-item-claim-key");
        claim.version_expectation = VersionExpectation::Graph(5);
        claim.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Transaction,
            domain: DurabilityDomain::GraphRows,
            method: Method::ClaimWorkItem {
                request: crate::epistemic_operations::ClaimWorkItemRequest {
                    schema_version:
                        crate::epistemic_operations::ClaimWorkItemRequestSchemaVersion::V1,
                    tenant_ref: "tenant-a".into(),
                    work_item_id: None,
                    queue_ref: None,
                    resource_class: None,
                    fairness_group: None,
                    worker_ref: "worker-a".into(),
                    now_ms: 1_000,
                    lease_ms: 10_000,
                    max_tenant_in_flight: 1,
                },
            },
        }];
        claim
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("quota claim fixture reseals its final body");
        let committed = commit_at(&db, &claim, None).unwrap();
        let payload: crate::protocol::ResultPayload = decode_durable(
            committed
                .record
                .result_msgpack
                .as_deref()
                .expect("claim result"),
        )
        .unwrap();
        // `Raw` is the one canonical MessagePack-bin result representation.
        let bytes = match payload {
            crate::protocol::ResultPayload::Raw(inner) => inner,
            other => panic!("ClaimWorkItem must return a bin-encoded typed result, got {other:?}"),
        };
        let result: ClaimWorkItemResult = decode_durable(&bytes).unwrap();
        assert!(!result.claimed);
        assert_eq!(result.reason, ClaimWorkItemResultReason::TenantQuota);
        assert_eq!(result.tenant_in_flight, Some(1));

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn exact_work_item_claim_cannot_bypass_tenant_in_flight_limit() {
        use crate::epistemic_operations::{
            ClaimWorkItemRequest, ClaimWorkItemRequestSchemaVersion,
        };

        let path = temp_path("work-item-exact-quota");
        let db = open(&path);
        let mut seed = batch(
            "work-item-exact-quota-seed",
            "work-item-exact-quota-seed-key",
        );
        seed.operations = vec![
            MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Transaction,
                domain: DurabilityDomain::GraphRows,
                method: ready_work_item_method("live", 3),
            },
            MutationOperation {
                ordinal: 1,
                surface: MutationSurface::Transaction,
                domain: DurabilityDomain::GraphRows,
                method: ready_work_item_method("ready", 3),
            },
        ];
        seed.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("exact quota seed fixture reseals its final body");
        commit_at(&db, &seed, None).unwrap();
        let claimed = commit_native_claim(
            &db,
            "work-item-exact-native-lease",
            "work-item-exact-native-lease-key",
            4,
            Some("live"),
            "worker-a",
            1_000,
            10_000,
            64,
        );
        assert!(claimed.claimed);

        let mut claim = batch(
            "work-item-exact-quota-claim",
            "work-item-exact-quota-claim-key",
        );
        claim.version_expectation = VersionExpectation::Graph(5);
        claim.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Transaction,
            domain: DurabilityDomain::GraphRows,
            method: Method::ClaimWorkItem {
                request: ClaimWorkItemRequest {
                    schema_version: ClaimWorkItemRequestSchemaVersion::V1,
                    tenant_ref: "tenant-a".into(),
                    work_item_id: Some("ready".into()),
                    queue_ref: None,
                    resource_class: None,
                    fairness_group: None,
                    worker_ref: "worker-a".into(),
                    now_ms: 1_000,
                    lease_ms: 10_000,
                    max_tenant_in_flight: 1,
                },
            },
        }];
        claim
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("exact quota claim fixture reseals its final body");
        let committed = commit_at(&db, &claim, None).unwrap();
        let payload: crate::protocol::ResultPayload = decode_durable(
            committed
                .record
                .result_msgpack
                .as_deref()
                .expect("claim result"),
        )
        .unwrap();
        let bytes = match payload {
            crate::protocol::ResultPayload::Raw(inner) => inner,
            other => panic!("ClaimWorkItem must return a bin-encoded typed result, got {other:?}"),
        };
        let result: ClaimWorkItemResult = decode_durable(&bytes).unwrap();
        assert!(!result.claimed);
        assert_eq!(result.reason, ClaimWorkItemResultReason::TenantQuota);
        assert_eq!(result.tenant_in_flight, Some(1));

        let ready = read_one_node(&db, "graph-a", "ready", DurableCrypto::none())
            .unwrap()
            .expect("exact candidate remains inspectable");
        let ready: serde_json::Value = decode_durable(&ready).unwrap();
        assert_eq!(ready["status"], "ready");
        drop(db);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn expired_exhausted_work_item_is_terminalized_without_an_over_ceiling_claim() {
        let path = temp_path("work-item-expired-attempt-ceiling");
        let db = open(&path);
        let mut seed = batch("work-item-expired-seed", "work-item-expired-seed-key");
        seed.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Transaction,
            domain: DurabilityDomain::GraphRows,
            method: ready_work_item_method("exhausted", 3),
        }];
        seed.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("expired seed fixture reseals its final body");
        commit_at(&db, &seed, None).unwrap();
        let first = commit_native_claim(
            &db,
            "work-item-expired-first",
            "work-item-expired-first-key",
            4,
            Some("exhausted"),
            "dead-worker",
            0,
            10_000,
            64,
        );
        assert!(first.claimed);
        let second = commit_native_claim(
            &db,
            "work-item-expired-second",
            "work-item-expired-second-key",
            5,
            Some("exhausted"),
            "dead-worker",
            100_000,
            10_000,
            64,
        );
        assert!(second.claimed);
        assert_eq!(second.attempt, Some(2));
        let third = commit_native_claim(
            &db,
            "work-item-expired-third",
            "work-item-expired-third-key",
            6,
            Some("exhausted"),
            "dead-worker",
            200_000,
            10_000,
            64,
        );
        assert!(third.claimed);
        assert_eq!(third.attempt, Some(3));
        let result = commit_native_claim(
            &db,
            "work-item-expired-claim",
            "work-item-expired-claim-key",
            7,
            Some("exhausted"),
            "replacement-worker",
            300_000,
            10_000,
            64,
        );
        assert!(!result.claimed);
        assert_eq!(result.reason, ClaimWorkItemResultReason::Empty);
        assert_eq!(result.changed_work_item_ids, vec!["exhausted"]);

        let stored = read_one_node(&db, "graph-a", "exhausted", DurableCrypto::none())
            .unwrap()
            .expect("expired work item remains inspectable");
        let stored: serde_json::Value = decode_durable(&stored).unwrap();
        assert_eq!(stored["status"], "dead_letter");
        assert_eq!(
            stored["attempt"], 3,
            "the exhausted attempt is never incremented"
        );
        assert_eq!(stored["max_attempts"], 3);
        assert_eq!(stored["error_ref"], "lease_exhausted");
        assert!(stored["lease_owner"].is_null());
        assert!(stored["lease_expires_at"].is_null());
        assert_eq!(stored["lease_epoch"], 6, "the dead holder is fenced out");
        assert_eq!(stored["fencing_token"], 6);

        drop(db);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn generic_claim_reaps_exhausted_lease_then_claims_a_different_ready_item() {
        let path = temp_path("work-item-generic-expired-attempt-ceiling");
        let db = open(&path);
        let mut seed = batch(
            "work-item-generic-expired-seed",
            "work-item-generic-expired-seed-key",
        );
        seed.operations = vec![
            MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Transaction,
                domain: DurabilityDomain::GraphRows,
                method: ready_work_item_method("exhausted", 3),
            },
            MutationOperation {
                ordinal: 1,
                surface: MutationSurface::Transaction,
                domain: DurabilityDomain::GraphRows,
                method: ready_work_item_method("runnable", 3),
            },
        ];
        seed.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("generic expiry seed fixture reseals its final body");
        commit_at(&db, &seed, None).unwrap();
        assert!(
            commit_native_claim(
                &db,
                "work-item-generic-expired-first",
                "work-item-generic-expired-first-key",
                4,
                Some("exhausted"),
                "dead-worker",
                0,
                10_000,
                64,
            )
            .claimed
        );
        assert!(
            commit_native_claim(
                &db,
                "work-item-generic-expired-second",
                "work-item-generic-expired-second-key",
                5,
                Some("exhausted"),
                "dead-worker",
                100_000,
                10_000,
                64,
            )
            .claimed
        );
        assert!(
            commit_native_claim(
                &db,
                "work-item-generic-expired-third",
                "work-item-generic-expired-third-key",
                6,
                Some("exhausted"),
                "dead-worker",
                200_000,
                10_000,
                64,
            )
            .claimed
        );
        let result = commit_native_claim(
            &db,
            "work-item-generic-expired-claim",
            "work-item-generic-expired-claim-key",
            7,
            None,
            "worker-b",
            300_000,
            10_000,
            64,
        );
        assert!(result.claimed);
        assert_eq!(result.work_item_id.as_deref(), Some("runnable"));
        assert_eq!(result.attempt, Some(1));
        assert!(result
            .changed_work_item_ids
            .iter()
            .any(|id| id == "exhausted"));
        assert!(result
            .changed_work_item_ids
            .iter()
            .any(|id| id == "runnable"));

        let exhausted = read_one_node(&db, "graph-a", "exhausted", DurableCrypto::none())
            .unwrap()
            .expect("expired work item remains inspectable");
        let exhausted: serde_json::Value = decode_durable(&exhausted).unwrap();
        assert_eq!(exhausted["status"], "dead_letter");
        assert_eq!(exhausted["attempt"], 3);
        assert_eq!(exhausted["error_ref"], "lease_exhausted");

        drop(db);
        let _ = std::fs::remove_file(path);
    }

    /// Regression for INCIDENT-kg-readonly-2026-07-31 / D-INC-1 / D-SH-5: a
    /// `RenewWorkItemLease` against a work item that no longer exists MUST still
    /// carry `changed_work_item_ids` (even if empty) in its committed result. If it
    /// doesn't, `commit_work_item` (`src/server/mutation_batch.rs`) can no longer
    /// read that field after the durable commit has already advanced the
    /// authoritative graph version — the serving projection is stranded one
    /// version behind for good, and `authoritative_graph_version` then fails
    /// closed on every later write, taking the whole graph read-only. This test
    /// exercises the REAL redb dispatch path, not a hand-built JSON fixture, so it
    /// fails on the pre-fix shape (`{"renewed": false, "reason": "missing"}`) and
    /// passes once the field is always present.
    #[test]
    fn renew_lease_on_a_missing_work_item_still_carries_changed_work_item_ids() {
        let path = temp_path("work-item-renew-missing");
        let db = open(&path);

        let mut renew = batch("work-item-renew-missing", "work-item-renew-missing-key");
        renew.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Transaction,
            domain: DurabilityDomain::GraphRows,
            method: Method::RenewWorkItemLease {
                tenant: "tenant-a".into(),
                work_item_id: "does-not-exist".into(),
                worker_id: "worker-a".into(),
                lease_epoch: 1,
                fencing_token: 1,
                now_ms: 1_000,
                lease_ms: 10_000,
            },
        }];
        renew
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("missing renewal fixture reseals its final body");
        let committed = commit_at(&db, &renew, None).unwrap();
        let payload: crate::protocol::ResultPayload = decode_durable(
            committed
                .record
                .result_msgpack
                .as_deref()
                .expect("renew result"),
        )
        .unwrap();
        let value = match payload {
            crate::protocol::ResultPayload::Json(value) => value,
            other => panic!("RenewWorkItemLease must return a JSON result, got {other:?}"),
        };
        assert_eq!(value["renewed"], false);
        assert_eq!(value["reason"], "missing");
        assert_eq!(
            value.get("changed_work_item_ids"),
            Some(&serde_json::json!([])),
            "a missing-work-item renewal must still carry changed_work_item_ids so \
             commit_work_item can call core.mark_dirty() and keep the serving \
             projection from stranding behind the authoritative graph version; \
             full result was: {value}"
        );

        drop(db);
        let _ = std::fs::remove_file(path);
    }

    /// Same incident, the other bricking shape: a lease renewal that is FENCED
    /// (wrong fencing token/epoch/owner/status) must also carry
    /// `changed_work_item_ids` in its result.
    #[test]
    fn renew_lease_that_is_fenced_still_carries_changed_work_item_ids() {
        let path = temp_path("work-item-renew-fenced");
        let db = open(&path);

        let mut seed = batch(
            "work-item-renew-fenced-seed",
            "work-item-renew-fenced-seed-key",
        );
        seed.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Transaction,
            domain: DurabilityDomain::GraphRows,
            method: ready_work_item_method("leased", 3),
        }];
        seed.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("fenced renewal seed fixture reseals its final body");
        commit_at(&db, &seed, None).unwrap();
        let claimed = commit_native_claim(
            &db,
            "work-item-renew-fenced-claim",
            "work-item-renew-fenced-claim-key",
            4,
            Some("leased"),
            "worker-a",
            0,
            60_000,
            64,
        );
        assert!(claimed.claimed);

        // Same work item, but the caller's fencing token is stale (2 vs the
        // durable row's 1) — this must be rejected as "fenced", not applied.
        let mut renew = batch("work-item-renew-fenced", "work-item-renew-fenced-key");
        renew.version_expectation = VersionExpectation::Graph(5);
        renew.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Transaction,
            domain: DurabilityDomain::GraphRows,
            method: Method::RenewWorkItemLease {
                tenant: "tenant-a".into(),
                work_item_id: "leased".into(),
                worker_id: "worker-a".into(),
                lease_epoch: 1,
                fencing_token: 2,
                now_ms: 1_000,
                lease_ms: 10_000,
            },
        }];
        renew
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("fenced renewal fixture reseals its final body");
        let committed = commit_at(&db, &renew, None).unwrap();
        let payload: crate::protocol::ResultPayload = decode_durable(
            committed
                .record
                .result_msgpack
                .as_deref()
                .expect("renew result"),
        )
        .unwrap();
        let value = match payload {
            crate::protocol::ResultPayload::Json(value) => value,
            other => panic!("RenewWorkItemLease must return a JSON result, got {other:?}"),
        };
        assert_eq!(value["renewed"], false);
        assert_eq!(value["reason"], "fenced");
        assert_eq!(
            value.get("changed_work_item_ids"),
            Some(&serde_json::json!([])),
            "a fenced renewal must still carry changed_work_item_ids so \
             commit_work_item can call core.mark_dirty() and keep the serving \
             projection from stranding behind the authoritative graph version; \
             full result was: {value}"
        );

        drop(db);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn last_permitted_reclaim_survives_restart_but_the_next_reclaim_dead_letters() {
        let path = temp_path("work-item-attempt-boundary-restart");
        {
            let db = open(&path);
            let mut seed = batch("work-item-boundary-seed", "work-item-boundary-seed-key");
            seed.operations = vec![MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Transaction,
                domain: DurabilityDomain::GraphRows,
                method: ready_work_item_method("boundary", 3),
            }];
            seed.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
                .expect("attempt boundary seed fixture reseals its final body");
            commit_at(&db, &seed, None).unwrap();
            assert!(
                commit_native_claim(
                    &db,
                    "work-item-boundary-first",
                    "work-item-boundary-first-key",
                    4,
                    Some("boundary"),
                    "dead-worker",
                    0,
                    10_000,
                    64,
                )
                .claimed
            );
            let second = commit_native_claim(
                &db,
                "work-item-boundary-second",
                "work-item-boundary-second-key",
                5,
                Some("boundary"),
                "dead-worker",
                100_000,
                10_000,
                64,
            );
            assert!(second.claimed);
            assert_eq!(second.attempt, Some(2));
            let third = commit_native_claim(
                &db,
                "work-item-boundary-last",
                "work-item-boundary-last-key",
                6,
                Some("boundary"),
                "last-permitted-worker",
                200_000,
                10_000,
                64,
            );
            assert!(third.claimed);
            assert_eq!(third.attempt, Some(3));
        }

        let db = open(&path);
        let result = commit_native_claim(
            &db,
            "work-item-boundary-over",
            "work-item-boundary-over-key",
            7,
            Some("boundary"),
            "would-be-fourth-worker",
            300_000,
            10_000,
            64,
        );
        assert!(!result.claimed);
        let stored = read_one_node(&db, "graph-a", "boundary", DurableCrypto::none())
            .unwrap()
            .expect("boundary work item remains durable");
        let stored: serde_json::Value = decode_durable(&stored).unwrap();
        assert_eq!(stored["status"], "dead_letter");
        assert_eq!(stored["attempt"], 3);
        assert_eq!(stored["error_ref"], "lease_exhausted");

        drop(db);
        let _ = std::fs::remove_file(path);
    }

    /// BUG-111: deterministic proof that a `CasWorkItemMetadata` CONFLICT is a
    /// real, distinct outcome, not a silent overwrite. Two "contenders" race
    /// for the same field the same way ANY real race would -- both derive
    /// their request from the SAME pre-claim read (`expected_checkpoint_id:
    /// None`) -- but the race is constructed DETERMINISTICALLY (two
    /// sequential `commit_at` calls against one synchronous db, never a
    /// spawned/sleeping thread; GOC-70) rather than hoped into existence.
    /// The winner's `commit_at` call happens-before the loser's by
    /// construction, so this is not a flaky "usually the first one wins" --
    /// it is the exact same interleaving on every run.
    #[test]
    fn cas_work_item_metadata_deterministic_conflict_never_silently_overwrites() {
        use crate::epistemic_operations_ext::{
            CasWorkItemMetadataLeaseFence, CasWorkItemMetadataOutcome, CasWorkItemMetadataRequest,
            CasWorkItemMetadataRequestSchemaVersion, CasWorkItemMetadataResult,
        };

        let path = temp_path("cas-metadata-conflict");
        let db = open(&path);

        let mut seed = batch("cas-metadata-seed", "cas-metadata-seed-key");
        seed.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Transaction,
            domain: DurabilityDomain::GraphRows,
            method: ready_work_item_method("cas-a", 3),
        }];
        seed.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("CAS seed fixture reseals its final body");
        commit_at(&db, &seed, None).unwrap();

        let claim = commit_native_claim(
            &db,
            "cas-metadata-claim",
            "cas-metadata-claim-key",
            4,
            Some("cas-a"),
            "worker-a",
            0,
            60_000,
            64,
        );
        assert!(claim.claimed);
        let lease = CasWorkItemMetadataLeaseFence {
            worker_ref: claim.lease_holder_ref.clone().unwrap(),
            lease_epoch: claim.lease_epoch.unwrap(),
            fencing_token: claim.fencing_token.unwrap(),
        };

        let cas_request = |expected_checkpoint_id: Option<&str>,
                           set_checkpoint_id: &str,
                           expected_graph_version: u64| {
            let mut op = batch(
                &format!("cas-metadata-{set_checkpoint_id}"),
                &format!("cas-metadata-{set_checkpoint_id}-key"),
            );
            op.version_expectation = VersionExpectation::Graph(expected_graph_version);
            op.operations = vec![MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Transaction,
                domain: DurabilityDomain::GraphRows,
                method: Method::CasWorkItemMetadata {
                    request: CasWorkItemMetadataRequest {
                        schema_version: CasWorkItemMetadataRequestSchemaVersion::V1,
                        tenant_ref: "tenant-a".into(),
                        work_item_id: "cas-a".into(),
                        expected_lease: Some(lease.clone()),
                        expected_status: vec!["leased".into(), "running".into()],
                        expected_checkpoint_id: expected_checkpoint_id.map(str::to_string),
                        set_checkpoint_id: Some(set_checkpoint_id.to_string()),
                        expected_metadata_msgpack: None,
                        set_metadata_msgpack: None,
                        expected_prio_bucket: None,
                        set_prio_bucket: None,
                        now_ms: 1_000,
                    },
                },
            }];
            op.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
                .expect("CAS request fixture reseals its final body");
            op
        };

        let decode_result = |committed: &MutationBatchCommit| -> CasWorkItemMetadataResult {
            let payload: crate::protocol::ResultPayload = decode_durable(
                committed
                    .record
                    .result_msgpack
                    .as_deref()
                    .expect("cas result"),
            )
            .unwrap();
            let bytes = match payload {
                crate::protocol::ResultPayload::Raw(inner) => inner,
                other => {
                    panic!(
                        "CasWorkItemMetadata must return a bin-encoded typed result, got {other:?}"
                    )
                }
            };
            decode_durable(&bytes).unwrap()
        };

        // Contender A: reads checkpoint_id == None, wins.
        let winner = commit_at(&db, &cas_request(None, "checkpoint:1", 5), None).unwrap();
        let winner_result = decode_result(&winner);
        assert_eq!(winner_result.outcome, CasWorkItemMetadataOutcome::Applied);
        assert_eq!(
            winner_result.changed_work_item_ids,
            vec!["cas-a".to_string()]
        );

        // Contender B: derived its request from the SAME pre-claim read
        // (checkpoint_id == None) -- now stale, because A already committed.
        // It must be told CONFLICT, distinctly from both Applied and NotFound.
        let loser = commit_at(&db, &cas_request(None, "checkpoint:2", 6), None).unwrap();
        let loser_result = decode_result(&loser);
        assert_eq!(loser_result.outcome, CasWorkItemMetadataOutcome::Conflict);
        assert_eq!(loser_result.changed_work_item_ids, Vec::<String>::new());

        // The loser's write never landed: the durable row still carries the
        // WINNER's value, not the loser's, and not some third corrupted value.
        let stored = read_one_node(&db, "graph-a", "cas-a", DurableCrypto::none())
            .unwrap()
            .expect("cas-a remains durable");
        let stored: serde_json::Value = decode_durable(&stored).unwrap();
        assert_eq!(stored["checkpoint_id"], "checkpoint:1");

        drop(db);
        let _ = std::fs::remove_file(path);
    }

    /// GOC-19/BUG-111: the SAME no-silent-overwrite property proven
    /// deterministically above, but under REAL concurrency -- two genuine OS
    /// threads, synchronized to start racing at the same instant via a
    /// `Barrier` (GOC-70 rule 3: construct the pile-up deterministically
    /// through a barrier, never a sleep-based hope), both submitting a
    /// `CasWorkItemMetadata` commit derived from the identical pre-race
    /// state, exactly like `mutation_batch_same_attempt_race_has_one_
    /// durable_winner_and_replay` in `resource_reservation_tests.rs`. This
    /// exercises the ACTUAL storage-layer mutual exclusion -- redb's
    /// exclusive write transaction plus the in-transaction
    /// `expected_graph_version`/lease/status checks -- rather than a
    /// hand-simulated interleaving, proving the guard is atomic at the
    /// storage layer and not a read-then-write race.
    #[test]
    fn cas_work_item_metadata_real_concurrent_race_has_exactly_one_winner() {
        use crate::epistemic_operations_ext::{
            CasWorkItemMetadataLeaseFence, CasWorkItemMetadataOutcome, CasWorkItemMetadataRequest,
            CasWorkItemMetadataRequestSchemaVersion, CasWorkItemMetadataResult,
        };

        let path = temp_path("cas-metadata-concurrent");
        let db = std::sync::Arc::new(open(&path));

        let mut seed = batch(
            "cas-metadata-concurrent-seed",
            "cas-metadata-concurrent-seed-key",
        );
        seed.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Transaction,
            domain: DurabilityDomain::GraphRows,
            method: ready_work_item_method("cas-race", 3),
        }];
        seed.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("concurrent CAS seed fixture reseals its final body");
        commit_at(&db, &seed, None).unwrap();

        let claim = commit_native_claim(
            &db,
            "cas-metadata-concurrent-claim",
            "cas-metadata-concurrent-claim-key",
            4,
            Some("cas-race"),
            "worker-race",
            0,
            60_000,
            64,
        );
        assert!(
            claim.claimed,
            "setup: claim must win to reach the claimed state"
        );
        let lease = CasWorkItemMetadataLeaseFence {
            worker_ref: claim.lease_holder_ref.clone().unwrap(),
            lease_epoch: claim.lease_epoch.unwrap(),
            fencing_token: claim.fencing_token.unwrap(),
        };

        let make_request = |label: &str, set_checkpoint_id: &str| {
            let mut op = batch(
                &format!("cas-metadata-concurrent-{label}"),
                &format!("cas-metadata-concurrent-{label}-key"),
            );
            // Both racers derive from the SAME pre-race graph version (5,
            // the version immediately after the claim above) -- exactly
            // what two real callers who both read state before either wrote
            // would carry. Only the transaction that actually lands first
            // can have this match; the other's `expected_graph_version`
            // is stale by construction, not by chance.
            op.version_expectation = VersionExpectation::Graph(5);
            op.operations = vec![MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Transaction,
                domain: DurabilityDomain::GraphRows,
                method: Method::CasWorkItemMetadata {
                    request: CasWorkItemMetadataRequest {
                        schema_version: CasWorkItemMetadataRequestSchemaVersion::V1,
                        tenant_ref: "tenant-a".into(),
                        work_item_id: "cas-race".into(),
                        expected_lease: Some(lease.clone()),
                        expected_status: vec!["leased".into(), "running".into()],
                        expected_checkpoint_id: None,
                        set_checkpoint_id: Some(set_checkpoint_id.to_string()),
                        expected_metadata_msgpack: None,
                        set_metadata_msgpack: None,
                        expected_prio_bucket: None,
                        set_prio_bucket: None,
                        now_ms: 1_000,
                    },
                },
            }];
            op.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
                .expect("concurrent CAS request fixture reseals its final body");
            op
        };

        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let mut handles = Vec::new();
        for (label, checkpoint) in [("a", "race:A"), ("b", "race:B")] {
            let db = db.clone();
            let barrier = barrier.clone();
            let request = make_request(label, checkpoint);
            handles.push(std::thread::spawn(move || {
                meet(&barrier, "concurrent-CAS race: worker at the start line");
                commit_at(&db, &request, None)
            }));
        }
        let results: Vec<Result<MutationBatchCommit, String>> = handles
            .into_iter()
            .map(|handle| join_bounded(handle, "a concurrent-CAS race worker"))
            .collect();

        let decode_result = |committed: &MutationBatchCommit| -> CasWorkItemMetadataResult {
            let payload: crate::protocol::ResultPayload = decode_durable(
                committed
                    .record
                    .result_msgpack
                    .as_deref()
                    .expect("cas result"),
            )
            .unwrap();
            let bytes = match payload {
                crate::protocol::ResultPayload::Raw(inner) => inner,
                other => {
                    panic!(
                        "CasWorkItemMetadata must return a bin-encoded typed result, got {other:?}"
                    )
                }
            };
            decode_durable(&bytes).unwrap()
        };

        let applied_count = results
            .iter()
            .filter(|result| {
                result
                    .as_ref()
                    .map(|committed| {
                        decode_result(committed).outcome == CasWorkItemMetadataOutcome::Applied
                    })
                    .unwrap_or(false)
            })
            .count();
        assert_eq!(
            applied_count, 1,
            "exactly one racing thread's CAS must apply under real concurrency, never zero \
             (lost write) and never two (silent double-apply): {results:?}"
        );

        let loser_explicitly_rejected = results.iter().any(|result| match result {
            Err(message) => message.contains("STALE_VERSION"),
            Ok(committed) => {
                decode_result(committed).outcome == CasWorkItemMetadataOutcome::Conflict
            }
        });
        assert!(
            loser_explicitly_rejected,
            "the losing thread must receive an explicit, distinct rejection (STALE_VERSION at \
             the batch envelope or Conflict from the CAS handler itself) -- never silently \
             dropped, never silently merged with the winner: {results:?}"
        );

        // The durable row reflects EXACTLY the winner's write -- never both,
        // never neither, never a corrupted mix of the two.
        let stored = read_one_node(&db, "graph-a", "cas-race", DurableCrypto::none())
            .unwrap()
            .expect("cas-race remains durable");
        let stored: serde_json::Value = decode_durable(&stored).unwrap();
        let checkpoint = stored["checkpoint_id"].as_str().unwrap();
        assert!(
            checkpoint == "race:A" || checkpoint == "race:B",
            "stored checkpoint must be exactly one racer's value, got {checkpoint:?}"
        );

        drop(db);
        let _ = std::fs::remove_file(path);
    }

    /// BUG-111: a WorkItem row that does not exist is `not_found`, a THIRD
    /// distinct outcome from `applied`/`conflict` -- never collapsed into
    /// either.
    #[test]
    fn cas_work_item_metadata_missing_row_is_not_found() {
        use crate::epistemic_operations_ext::{
            CasWorkItemMetadataOutcome, CasWorkItemMetadataRequest,
            CasWorkItemMetadataRequestSchemaVersion, CasWorkItemMetadataResult,
        };

        let path = temp_path("cas-metadata-missing");
        let db = open(&path);

        let mut op = batch("cas-metadata-missing", "cas-metadata-missing-key");
        op.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Transaction,
            domain: DurabilityDomain::GraphRows,
            method: Method::CasWorkItemMetadata {
                request: CasWorkItemMetadataRequest {
                    schema_version: CasWorkItemMetadataRequestSchemaVersion::V1,
                    tenant_ref: "tenant-a".into(),
                    work_item_id: "does-not-exist".into(),
                    expected_lease: None,
                    expected_status: vec!["leased".into(), "running".into()],
                    expected_checkpoint_id: None,
                    set_checkpoint_id: Some("checkpoint:1".into()),
                    expected_metadata_msgpack: None,
                    set_metadata_msgpack: None,
                    expected_prio_bucket: None,
                    set_prio_bucket: None,
                    now_ms: 1_000,
                },
            },
        }];
        op.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("missing CAS fixture reseals its final body");
        let committed = commit_at(&db, &op, None).unwrap();
        let payload: crate::protocol::ResultPayload = decode_durable(
            committed
                .record
                .result_msgpack
                .as_deref()
                .expect("cas result"),
        )
        .unwrap();
        let bytes = match payload {
            crate::protocol::ResultPayload::Raw(inner) => inner,
            other => {
                panic!("CasWorkItemMetadata must return a bin-encoded typed result, got {other:?}")
            }
        };
        let result: CasWorkItemMetadataResult = decode_durable(&bytes).unwrap();
        assert_eq!(result.outcome, CasWorkItemMetadataOutcome::NotFound);
        assert_eq!(result.changed_work_item_ids, Vec::<String>::new());

        drop(db);
        let _ = std::fs::remove_file(path);
    }

    /// BUG-111: an ACKED CAS survives a restart. The db handle standing in
    /// for the engine process is dropped and the SAME on-disk file reopened
    /// (exactly `last_permitted_reclaim_survives_restart_but_the_next_reclaim_
    /// dead_letters`'s established restart idiom) -- the durable redb commit
    /// already fsync'd before this test ever saw the "applied" result, so if
    /// the RPC used a side path instead of the same durable WorkItem
    /// transaction, this is where it would show up as a lost write.
    #[test]
    fn cas_work_item_metadata_applied_write_survives_restart() {
        use crate::epistemic_operations_ext::{
            CasWorkItemMetadataLeaseFence, CasWorkItemMetadataRequest,
            CasWorkItemMetadataRequestSchemaVersion,
        };

        let path = temp_path("cas-metadata-restart");
        {
            let db = open(&path);
            let mut seed = batch("cas-metadata-restart-seed", "cas-metadata-restart-seed-key");
            seed.operations = vec![MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Transaction,
                domain: DurabilityDomain::GraphRows,
                method: ready_work_item_method("cas-restart", 3),
            }];
            seed.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
                .expect("restart CAS seed fixture reseals its final body");
            commit_at(&db, &seed, None).unwrap();

            let claim = commit_native_claim(
                &db,
                "cas-metadata-restart-claim",
                "cas-metadata-restart-claim-key",
                4,
                Some("cas-restart"),
                "worker-a",
                0,
                60_000,
                64,
            );
            assert!(claim.claimed);

            let mut apply = batch(
                "cas-metadata-restart-apply",
                "cas-metadata-restart-apply-key",
            );
            apply.version_expectation = VersionExpectation::Graph(5);
            apply.operations = vec![MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Transaction,
                domain: DurabilityDomain::GraphRows,
                method: Method::CasWorkItemMetadata {
                    request: CasWorkItemMetadataRequest {
                        schema_version: CasWorkItemMetadataRequestSchemaVersion::V1,
                        tenant_ref: "tenant-a".into(),
                        work_item_id: "cas-restart".into(),
                        expected_lease: Some(CasWorkItemMetadataLeaseFence {
                            worker_ref: claim.lease_holder_ref.clone().unwrap(),
                            lease_epoch: claim.lease_epoch.unwrap(),
                            fencing_token: claim.fencing_token.unwrap(),
                        }),
                        expected_status: vec!["leased".into(), "running".into()],
                        expected_checkpoint_id: None,
                        set_checkpoint_id: Some("checkpoint:durable".into()),
                        expected_metadata_msgpack: None,
                        set_metadata_msgpack: None,
                        expected_prio_bucket: None,
                        set_prio_bucket: None,
                        now_ms: 1_000,
                    },
                },
            }];
            apply
                .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
                .expect("restart CAS fixture reseals its final body");
            commit_at(&db, &apply, None).unwrap();
            drop(db);
        }

        // Reopen the SAME on-disk file as a fresh handle -- standing in for
        // a process restart. Nothing from the dropped `db`'s in-memory state
        // can leak forward; only what actually committed to disk is here.
        let db = open(&path);
        let stored = read_one_node(&db, "graph-a", "cas-restart", DurableCrypto::none())
            .unwrap()
            .expect("cas-restart remains durable across restart");
        let stored: serde_json::Value = decode_durable(&stored).unwrap();
        assert_eq!(stored["checkpoint_id"], "checkpoint:durable");
        assert_eq!(stored["status"], "leased");

        drop(db);
        let _ = std::fs::remove_file(path);
    }

    /// ADR-5 / W2.2 acceptance: a `kill -9` mid-transition resumes correctly. The
    /// WorkItem lifecycle `status` and its co-located statechart MIRROR (`machine_state`)
    /// are one row written in ONE redb write transaction, so a crash BEFORE the commit
    /// rolls both back and a crash AFTER the commit lands both — they can never split.
    #[cfg(feature = "statechart")]
    #[test]
    fn work_item_status_and_statechart_mirror_commit_atomically_across_kill9() {
        use crate::epistemic_operations::{
            ClaimWorkItemRequest, ClaimWorkItemRequestSchemaVersion,
        };

        let seed_ready = |shard: &Shard| {
            let mut seed = batch("wi-seed", "wi-seed-key");
            seed.operations = vec![MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Transaction,
                domain: DurabilityDomain::GraphRows,
                method: Method::AddNode {
                    node_id: "wi".into(),
                    properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({
                        "node_type": "WorkItem",
                        "tenant": "tenant-a",
                        "status": "ready",
                    }))
                    .unwrap(),
                },
            }];
            seed.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
                .expect("work-item seed fixture reseals its final body");
            commit_at(shard, &seed, None).unwrap();
        };

        let claim_batch = || {
            let mut claim = batch("wi-claim", "wi-claim-key");
            claim.version_expectation = VersionExpectation::Graph(4);
            claim.operations = vec![MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Transaction,
                domain: DurabilityDomain::GraphRows,
                method: Method::ClaimWorkItem {
                    request: ClaimWorkItemRequest {
                        schema_version: ClaimWorkItemRequestSchemaVersion::V1,
                        tenant_ref: "tenant-a".into(),
                        work_item_id: Some("wi".into()),
                        queue_ref: None,
                        resource_class: None,
                        fairness_group: None,
                        worker_ref: "worker-a".into(),
                        now_ms: 1_000,
                        lease_ms: 10_000,
                        max_tenant_in_flight: 64,
                    },
                },
            }];
            claim
                .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
                .expect("work-item claim fixture reseals its final body");
            claim
        };

        let read_pair = |shard: &Shard| -> (Option<String>, Option<String>) {
            match read_one_node(shard, "graph-a", "wi", DurableCrypto::none()).unwrap() {
                None => (None, None),
                Some(b) => {
                    let props: serde_json::Map<String, serde_json::Value> =
                        decode_durable(&b).unwrap();
                    let get = |k: &str| props.get(k).and_then(|v| v.as_str()).map(str::to_string);
                    (get("status"), get("machine_state"))
                }
            }
        };

        // Scenario A — crash BEFORE the redb commit: NEITHER status nor its mirror persist.
        {
            let path = temp_path("wi-kill9-precommit");
            {
                let db = open(&path);
                seed_ready(&db);
                assert!(commit_at(
                    &db,
                    &claim_batch(),
                    Some(MutationBatchCrashpoint::BeforeCommit)
                )
                .is_err());
            }
            let db = open(&path);
            let (status, machine) = read_pair(&db);
            assert_eq!(status.as_deref(), Some("ready"), "status must not advance");
            assert_eq!(machine, None, "the mirror must not advance either");
            let _ = std::fs::remove_file(path);
        }

        // Scenario B — crash AFTER the redb commit (before ack): BOTH status AND its
        // mirror are already durably on disk, together.
        {
            let path = temp_path("wi-kill9-postcommit");
            {
                let db = open(&path);
                seed_ready(&db);
                assert!(commit_at(
                    &db,
                    &claim_batch(),
                    Some(MutationBatchCrashpoint::AfterCommitBeforeAck)
                )
                .is_err());
            }
            let db = open(&path);
            let (status, machine) = read_pair(&db);
            assert_eq!(
                status.as_deref(),
                Some("leased"),
                "status committed durably"
            );
            assert_eq!(
                machine.as_deref(),
                Some("leased"),
                "mirror committed durably and atomically with status"
            );
            let _ = std::fs::remove_file(path);
        }
    }

    /// ADR-5 / W2.2 acceptance: the dual-write divergence alarm fires on an induced
    /// divergence. Drives the redb integration point (`apply_work_item_mirror`) with an
    /// authoritative next state the chart would never compute, and asserts the
    /// `epistemic_graph_statechart_divergence_total` counter increments while the agreeing
    /// case does not.
    #[cfg(all(feature = "statechart", feature = "metrics"))]
    #[test]
    fn work_item_mirror_divergence_raises_the_alarm() {
        fn divergence_count() -> u64 {
            for line in crate::metrics::render().lines() {
                if line.starts_with(
                    "epistemic_graph_statechart_divergence_total{machine=\"work_item\"}",
                ) {
                    return line
                        .rsplit(' ')
                        .next()
                        .and_then(|v| v.parse::<f64>().ok())
                        .map(|f| f as u64)
                        .unwrap_or(0);
                }
            }
            0
        }

        // Induced divergence: `ready --claim-->` the chart decides `leased`, but the
        // (hypothetically buggy) authority claims it landed `succeeded`.
        let before = divergence_count();
        let mut props = serde_json::Map::new();
        apply_work_item_mirror(
            &mut props,
            "wi",
            "ready",
            crate::work_item_statechart::EV_CLAIM,
            serde_json::json!({}),
            Some("succeeded"),
        );
        assert_eq!(
            divergence_count(),
            before + 1,
            "an induced divergence must increment the alarm counter"
        );
        // The mirror still records ITS OWN decision, so the divergence is queryable at rest.
        assert_eq!(
            props.get("machine_state").and_then(|v| v.as_str()),
            Some("leased")
        );

        // The agreeing case does NOT alarm.
        let steady = divergence_count();
        let mut props2 = serde_json::Map::new();
        apply_work_item_mirror(
            &mut props2,
            "wi2",
            "ready",
            crate::work_item_statechart::EV_CLAIM,
            serde_json::json!({}),
            Some("leased"),
        );
        assert_eq!(divergence_count(), steady, "agreement must not alarm");
        assert_eq!(
            props2.get("machine_state").and_then(|v| v.as_str()),
            Some("leased")
        );
    }

    #[test]
    fn crossmodal_batch_recovers_rows_status_vector_and_outbox_together() {
        let path = temp_path("crossmodal-postcommit");
        let mut mutation = batch("batch-crossmodal", "idem-crossmodal");
        let methods = mutation
            .operations
            .iter()
            .map(|operation| operation.method.clone())
            .collect::<Vec<_>>();
        mutation.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Transaction,
            domain: DurabilityDomain::CrossModal,
            method: Method::ApplyMutation {
                event_type: "crossmodal_operation".to_string(),
                query: "sha256:0000000000000000000000000000000000000000000000000000000000000000"
                    .to_string(),
            },
        }];
        mutation
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("the crossmodal fixture reseals its envelope over its final body");
        let vectors = vec![("a".to_string(), vec![0.25, 0.75])];
        {
            let db = open(&path);
            let crash = commit_crossmodal_at(
                &db,
                &mutation,
                &methods,
                &vectors,
                Some(MutationBatchCrashpoint::AfterCommitBeforeAck),
            )
            .unwrap_err();
            // Named, not just `is_err()`: the recovery this test asserts below
            // only exists if the commit actually reached its post-commit
            // crashpoint. Any EARLIER refusal (an admission failure, say) makes
            // the whole test vacuous -- nothing is durable, `read_graph_dump`
            // returns `None`, and the failure reads as a recovery bug instead of
            // a fixture bug. Named 2026-09-11 by the redb_store absolute-green
            // lane (F1) after exactly that happened.
            assert!(
                crash.contains("injected crash"),
                "the crossmodal batch must reach its AfterCommitBeforeAck crashpoint, got: {crash}"
            );
        }
        {
            let db = open(&path);
            let dump = read_graph_dump(&db, "graph-a", DurableCrypto::none())
                .unwrap()
                .unwrap();
            let semantic: crate::compute::semantic::SemanticStore =
                rmp_serde::from_slice(&dump.semantic).unwrap();
            assert_eq!(semantic.get_embedding("a"), Some(vec![0.25, 0.75]));
            assert!(
                read_mutation_batch_for_graph(&db, "graph-a", "batch-crossmodal")
                    .unwrap()
                    .is_some()
            );
            // One physical row per EXPLICIT logical intent, and this fixture
            // carries exactly one. Same correction as
            // `postcommit_crash_restarts_and_replays_idempotently`: the old
            // expectation counted an auto-generated "canonical event" row per
            // operation, and no such row exists --
            // `eg_transaction::commit::write_outbox` iterates `batch.outbox`
            // and nothing else. The subject here is that a post-commit crash
            // leaves the outbox recovered ALONGSIDE rows and the status vector,
            // which one row proves as well as two. Corrected 2026-09-11 by the
            // redb_store absolute-green lane (F1).
            assert_eq!(
                read_mutation_outbox(&db, "graph-a", "batch-crossmodal")
                    .unwrap()
                    .len(),
                1,
            );
            // Every retry below mints a FRESH attempt nonce under the SAME
            // idempotency key, which is what a real lost-ack retry does (see the
            // sibling `postcommit_crash_restarts_and_replays_idempotently`,
            // which asserts `assert_ne!` on exactly this). Re-presenting the
            // committed batch verbatim re-presents its CONSUMED nonce and is
            // refused with `REPLAY_NONCE_CONSUMED` before any replay question is
            // asked -- a caller bug, not a replay. Fixed 2026-09-11 by the
            // redb_store absolute-green lane (F1).
            let retry_of = |source: &MutationBatch| {
                let mut retry = source.clone();
                retry.envelope = fixture_operation_envelope(
                    &retry.identity,
                    &format!("principal:sha256:{}", "a".repeat(64)),
                    42,
                    "idem-crossmodal",
                );
                retry
                    .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
                    .expect("a crossmodal retry reseals its envelope over its final body");
                retry
            };

            let retry = retry_of(&mutation);
            let replay = commit_crossmodal_at(&db, &retry, &methods, &vectors, None).unwrap();
            assert!(replay.replayed);
            assert_eq!(
                replay.record.batch.version_expectation,
                VersionExpectation::Graph(3),
                "replay must retain the original OCC observation in the durable identity"
            );

            // A retry reconstructed after the acknowledgement-lost crash may
            // carry the now-current graph version.  It is still the same
            // cross-modal request and must replay without applying rows again.
            let mut rederived = retry_of(&mutation);
            rederived.version_expectation = VersionExpectation::Graph(4);
            let replay = commit_crossmodal_at(&db, &rederived, &methods, &vectors, None).unwrap();
            assert!(replay.replayed);
            assert_eq!(
                replay.record.batch.version_expectation,
                VersionExpectation::Graph(3),
                "a derived retry version must never overwrite the original durable version"
            );

            // The expected version is the only re-derived field permitted for
            // this replay shape.  Changing the operation under the same key is
            // a genuine idempotency conflict.
            let mut conflict = retry_of(&rederived);
            conflict.operations[0].method = Method::ApplyMutation {
                event_type: "crossmodal_operation".to_string(),
                query: "sha256:1111111111111111111111111111111111111111111111111111111111111111"
                    .to_string(),
            };
            conflict
                .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
                .expect("the conflicting fixture reseals its envelope over its final body");
            let error = commit_crossmodal_at(&db, &conflict, &methods, &vectors, None).unwrap_err();
            assert!(error.contains("IDEMPOTENCY_CONFLICT"));
        }
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn outbox_claim_ack_is_ordered_fenced_and_reconcilable() {
        let path = temp_path("outbox-lease");
        let db = open(&path);
        let mutation = batch("batch-outbox", "idem-outbox");
        commit_at(&db, &mutation, None).unwrap();
        let mut middle = batch("batch-outbox-middle", "idem-outbox-middle");
        middle.version_expectation = VersionExpectation::Graph(4);
        commit_at(&db, &middle, None).unwrap();
        let mut tail = batch("batch-outbox-tail", "idem-outbox-tail");
        tail.version_expectation = VersionExpectation::Graph(5);
        commit_at(&db, &tail, None).unwrap();
        for batch_id in ["batch-outbox", "batch-outbox-middle", "batch-outbox-tail"] {
            assert_eq!(
                read_mutation_outbox(&db, "graph-a", batch_id)
                    .unwrap()
                    .len(),
                1,
                "one explicit logical intent writes one physical outbox row"
            );
        }
        db.outbox_subscribe("graph-a", "projection-worker", "projection.test")
            .unwrap();

        // The sweep limit is 12, not 10, so ONE claim call can take all three
        // rows. `OutboxClaimBudget` applies a per-call run cap of `limit / 4`
        // (min 1) to every claim, contended or not -- 10 caps a call at 2 rows
        // and this test would observe an arbitrary 2-of-3 page. That cap is the
        // kernel's deliberate fairness contract, asserted directly by
        // `eg_transaction::tests::outbox::
        // a_contended_budget_caps_a_tenant_run_after_another_tenant_claims`
        // ("Budget 16 -> a 4-row per-call cap"), so the budget is what moves
        // here; the ordering/fencing/reconciliation assertions this test owns
        // are unchanged. Raised 2026-09-11 by the redb_store absolute-green
        // lane (F1).
        let mut budget = OutboxClaimBudget::new(12, 100, 1_000).unwrap();
        let outcome = db
            .outbox_claim("graph-a", "projection-worker", &mut budget)
            .unwrap();
        assert_eq!(outcome.deferred, None);
        let leases = outcome.claims;
        assert_eq!(leases.len(), 3);
        let gap = db.outbox_ack("graph-a", &leases[1], 1_001).unwrap_err();
        assert!(gap.contains("OUTBOX_ORDER_GAP"));

        for lease in &leases {
            db.outbox_ack("graph-a", lease, 1_001).unwrap();
        }
        let mut budget = OutboxClaimBudget::new(10, 100, 2_000).unwrap();
        let outcome = db
            .outbox_claim("graph-a", "projection-worker", &mut budget)
            .unwrap();
        assert_eq!(outcome.deferred, None);
        assert!(outcome.claims.is_empty());
        let cursor = db
            .outbox_cursor("graph-a", "projection-worker")
            .unwrap()
            .unwrap();
        assert_eq!(cursor.batch_id, "batch-outbox-tail");
        assert_eq!(cursor.outbox_ordinal, 0);
        assert_eq!(cursor.schema_version, MUTATION_BATCH_VERSION);
        assert_eq!(
            cursor.committed_version,
            CommittedVersion::Graph {
                source: 5,
                target: 6
            }
        );

        let mut next = batch("batch-outbox-next", "idem-outbox-next");
        next.version_expectation = VersionExpectation::Graph(6);
        commit_at(&db, &next, None).unwrap();
        let mut budget = OutboxClaimBudget::new(10, 100, 2_100).unwrap();
        let mut outcome = db
            .outbox_claim("graph-a", "projection-worker", &mut budget)
            .unwrap();
        assert_eq!(outcome.deferred, None);
        let next_lease = outcome.claims.remove(0);
        let advanced = db.outbox_ack("graph-a", &next_lease, 2_101).unwrap();
        assert_eq!(
            advanced.committed_version,
            CommittedVersion::Graph {
                source: 6,
                target: 7
            }
        );
        assert!(db
            .outbox_ack("graph-a", &leases[2], 2_102)
            .unwrap_err()
            .contains("STALE_OUTBOX_LEASE"));

        db.outbox_subscribe("graph-a", "lease-fence-worker", "projection.test")
            .unwrap();
        let mut budget = OutboxClaimBudget::new(1, 10, 3_000).unwrap();
        let mut outcome = db
            .outbox_claim("graph-a", "lease-fence-worker", &mut budget)
            .unwrap();
        assert_eq!(outcome.deferred, None);
        let first = outcome.claims.remove(0);
        let mut budget = OutboxClaimBudget::new(1, 10, 3_011).unwrap();
        let mut outcome = db
            .outbox_claim("graph-a", "lease-fence-worker", &mut budget)
            .unwrap();
        assert_eq!(outcome.deferred, None);
        let replacement = outcome.claims.remove(0);
        assert!(replacement.lease_epoch > first.lease_epoch);
        assert!(db
            .outbox_ack("graph-a", &first, 3_012)
            .unwrap_err()
            .contains("STALE_OUTBOX_LEASE"));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn staged_state_commit_replaces_rows_and_replays_without_reexecution() {
        use sha2::{Digest, Sha256};

        let path = temp_path("authoritative-state");
        let db = open(&path);
        let staged = crate::graph::GraphCore::new();
        staged.add_node(
            "replacement".to_string(),
            rmp_serde::to_vec_named(&serde_json::json!({"value": 7})).unwrap(),
        );
        let state = staged.snapshot().to_msgpack().unwrap();
        let mut mutation = batch("batch-state", "idem-state");
        mutation.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Query,
            domain: DurabilityDomain::GraphSnapshot,
            method: Method::ApplyMutation {
                event_type: "authoritative_state_operation".to_string(),
                query: "sha256:opaque".to_string(),
            },
        }];
        mutation.authoritative_state = Some(crate::mutation_batch::MutationStateDescriptor {
            algorithm: "sha256".to_string(),
            digest: hex::encode(Sha256::digest(&state)),
            source_graph_version: 3,
            target_graph_version: 4,
        });
        mutation
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .unwrap();
        #[cfg(feature = "security")]
        let mut audit = AuditTailCache::new();
        let committed = commit_mutation_batch_state(
            &db,
            StateCommitInput {
                graph_fname: "graph-a",
                batch: &mutation,
                authoritative_state_msgpack: &state,
                result_msgpack: Some(&[0x81, 0xa2, b'o', b'k']),
                committed_at_ms: 101,
                audited: true,
            },
            DurableCrypto::none(),
            #[cfg(feature = "security")]
            &mut audit,
        )
        .unwrap();
        assert!(!committed.replayed);
        assert!(
            read_one_node(&db, "graph-a", "replacement", DurableCrypto::none())
                .unwrap()
                .is_some()
        );
        assert!(read_one_node(&db, "graph-a", "a", DurableCrypto::none())
            .unwrap()
            .is_none());
        assert_eq!(read_mutation_graph_version(&db, "graph-a").unwrap(), 4);

        let consumed_nonce = commit_mutation_batch_state(
            &db,
            StateCommitInput {
                graph_fname: "graph-a",
                batch: &mutation,
                authoritative_state_msgpack: &state,
                result_msgpack: Some(&[0x81, 0xa2, b'o', b'k']),
                committed_at_ms: 101,
                audited: true,
            },
            DurableCrypto::none(),
            #[cfg(feature = "security")]
            &mut audit,
        )
        .unwrap_err();
        assert!(
            consumed_nonce.contains("REPLAY_NONCE_CONSUMED"),
            "{consumed_nonce}"
        );

        let mut retry = batch("batch-state", "idem-state");
        retry.operations = mutation.operations.clone();
        retry.authoritative_state = mutation.authoritative_state.clone();
        retry
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .unwrap();
        assert_ne!(
            retry
                .envelope
                .operation()
                .expect("retry operation envelope")
                .authority
                .nonce,
            mutation
                .envelope
                .operation()
                .expect("original operation envelope")
                .authority
                .nonce,
            "a retry must use a fresh attempt nonce"
        );

        let replay = commit_mutation_batch_state(
            &db,
            StateCommitInput {
                graph_fname: "graph-a",
                batch: &retry,
                authoritative_state_msgpack: &state,
                result_msgpack: Some(&[0x81, 0xa2, b'o', b'k']),
                committed_at_ms: 101,
                audited: true,
            },
            DurableCrypto::none(),
            #[cfg(feature = "security")]
            &mut audit,
        )
        .unwrap();
        assert!(replay.replayed);
        assert_eq!(read_mutation_graph_version(&db, "graph-a").unwrap(), 4);
        assert!(
            read_one_node(&db, "graph-a", "replacement", DurableCrypto::none())
                .unwrap()
                .is_some()
        );
        assert!(read_one_node(&db, "graph-a", "a", DurableCrypto::none())
            .unwrap()
            .is_none());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn staged_row_delta_updates_only_affected_durable_rows() {
        use sha2::{Digest, Sha256};

        let path = temp_path("authoritative-row-delta");
        let db = open(&path);
        let initial = batch("batch-row-delta-base", "idem-row-delta-base");
        commit_at(&db, &initial, None).unwrap();

        let before = crate::graph::GraphCore::new();
        before.add_node(
            "a".to_string(),
            rmp_serde::to_vec_named(&serde_json::json!({"value": 1})).unwrap(),
        );
        before.add_node(
            "b".to_string(),
            rmp_serde::to_vec_named(&serde_json::json!({"value": 2})).unwrap(),
        );
        before.clear_ledger();
        let before_snapshot = before.snapshot();
        let after = crate::graph::GraphCore::from_snapshot(before_snapshot.clone(), 0).unwrap();
        after.add_node(
            "a".to_string(),
            rmp_serde::to_vec_named(&serde_json::json!({"value": 9})).unwrap(),
        );
        after
            .add_edge(
                "a".to_string(),
                "b".to_string(),
                rmp_serde::to_vec_named(&serde_json::json!({"kind": "new"})).unwrap(),
            )
            .unwrap();
        after
            .semantic_store
            .write()
            .add_embedding("a".to_string(), vec![0.25, 0.75])
            .unwrap();
        after.set_integrity_policy(crate::graph::IntegrityPolicy {
            shapes_ttl: "@prefix sh: <http://www.w3.org/ns/shacl#> .".to_string(),
        });
        let delta = crate::graph_delta::GraphRowDelta::between(&before_snapshot, &after.snapshot())
            .unwrap();
        let state = delta.to_msgpack().unwrap();

        let mut mutation = batch("batch-row-delta", "idem-row-delta");
        mutation.version_expectation = VersionExpectation::Graph(4);
        mutation.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Query,
            domain: DurabilityDomain::GraphSnapshot,
            method: Method::ApplyMutation {
                event_type: "authoritative_state_operation".to_string(),
                query: "sha256-row-delta-v2:opaque".to_string(),
            },
        }];
        mutation.authoritative_state = Some(crate::mutation_batch::MutationStateDescriptor {
            algorithm: crate::graph_delta::ROW_DELTA_ALGORITHM.to_string(),
            digest: hex::encode(Sha256::digest(&state)),
            source_graph_version: 4,
            target_graph_version: 5,
        });
        mutation
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("row delta fixture reseals its final body");
        #[cfg(feature = "security")]
        let mut audit = AuditTailCache::new();
        commit_mutation_batch_state(
            &db,
            StateCommitInput {
                graph_fname: "graph-a",
                batch: &mutation,
                authoritative_state_msgpack: &state,
                result_msgpack: None,
                committed_at_ms: 102,
                audited: true,
            },
            DurableCrypto::none(),
            #[cfg(feature = "security")]
            &mut audit,
        )
        .unwrap();

        let a: serde_json::Value = decode_durable(
            &read_one_node(&db, "graph-a", "a", DurableCrypto::none())
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(a["value"], 9);
        let b: serde_json::Value = decode_durable(
            &read_one_node(&db, "graph-a", "b", DurableCrypto::none())
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(b["value"], 2, "the untouched row must survive");
        let dump = read_graph_dump(&db, "graph-a", DurableCrypto::none())
            .unwrap()
            .unwrap();
        assert_eq!(dump.edges.len(), 1);
        assert_eq!(dump.ledger, after.snapshot().ledger);
        let semantic: crate::compute::semantic::SemanticStore =
            decode_durable(&dump.semantic).unwrap();
        assert_eq!(
            semantic.embeddings_snapshot(),
            vec![("a".to_string(), vec![0.25, 0.75])]
        );
        assert_eq!(dump.source_snapshot_version, 5);
        assert_eq!(dump.integrity_policy, after.integrity_policy());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn failed_row_delta_commit_does_not_publish_integrity_policy() {
        use sha2::{Digest, Sha256};

        let path = temp_path("integrity-policy-rollback");
        let db = open(&path);
        commit_at(&db, &batch("batch-policy-base", "idem-policy-base"), None).unwrap();

        let before = crate::graph::GraphCore::new();
        let before_snapshot = before.snapshot();
        let after = crate::graph::GraphCore::from_snapshot(before_snapshot.clone(), 0).unwrap();
        after.set_integrity_policy(crate::graph::IntegrityPolicy {
            shapes_ttl: "@prefix sh: <http://www.w3.org/ns/shacl#> .".to_string(),
        });
        let delta = crate::graph_delta::GraphRowDelta::between(&before_snapshot, &after.snapshot())
            .unwrap();
        let state = delta.to_msgpack().unwrap();
        let mut mutation = batch("batch-policy-fail", "idem-policy-fail");
        mutation.version_expectation = VersionExpectation::Graph(4);
        mutation.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Graph,
            domain: DurabilityDomain::GraphSnapshot,
            method: Method::IcvConfigure {
                graph: Some("graph-a".to_string()),
                mode: "enforce".to_string(),
                shapes: "sha256:policy-receipt".to_string(),
            },
        }];
        mutation.authoritative_state = Some(crate::mutation_batch::MutationStateDescriptor {
            algorithm: crate::graph_delta::ROW_DELTA_ALGORITHM.to_string(),
            digest: hex::encode(Sha256::digest(&state)),
            source_graph_version: 4,
            target_graph_version: 5,
        });
        // Without this the batch is refused at ADMISSION for an envelope that
        // no longer covers its body ("mutation batch content does not match its
        // envelope's canonical payload digest"), so the commit below errors
        // before it ever reaches the `BeforeCommit` crashpoint this test is
        // about -- and the "policy was not published" assertion holds vacuously,
        // because no row phase ran at all. Resealing puts the injected-crash
        // rollback back under test. Added 2026-09-11 by the redb_store
        // absolute-green lane (F1).
        mutation
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("the row-delta fixture reseals its envelope over its final body");
        #[cfg(feature = "security")]
        let mut audit = AuditTailCache::new();
        assert!(commit_mutation_batch_inner(
            &db,
            BatchCommitInput {
                graph_fname: "graph-a",
                batch: &mutation,
                change: None,
                authoritative_state_msgpack: Some(&state),
                crossmodal: None,
                result_msgpack: None,
                committed_at_ms: 103,
                audited: true,
                crashpoint: Some(MutationBatchCrashpoint::BeforeCommit),
            },
            DurableCrypto::none(),
            #[cfg(feature = "security")]
            &mut audit,
        )
        .is_err());

        let dump = read_graph_dump(&db, "graph-a", DurableCrypto::none())
            .unwrap()
            .unwrap();
        assert!(dump.integrity_policy.is_none());
        assert_eq!(read_mutation_graph_version(&db, "graph-a").unwrap(), 4);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn idempotency_key_reuse_for_different_work_fails_closed() {
        let path = temp_path("conflict");
        let db = open(&path);
        let first = batch("batch-one", "same-key");
        commit_at(&db, &first, None).unwrap();
        let mut conflicting = batch("batch-two", "same-key");
        conflicting.operations[0].method = node("different", 99);
        conflicting
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .unwrap();
        let err = commit_at(&db, &conflicting, None).unwrap_err();
        assert!(err.contains("IDEMPOTENCY_CONFLICT"));
        assert!(
            read_one_node(&db, "graph-a", "different", DurableCrypto::none())
                .unwrap()
                .is_none()
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn batch_id_reuse_with_a_fresh_key_fails_closed() {
        let path = temp_path("batch-id-conflict");
        let db = open(&path);
        let first = batch("same-batch", "first-key");
        commit_at(&db, &first, None).unwrap();
        let mut conflicting = batch("same-batch", "fresh-key");
        conflicting.operations[0].method = node("different", 99);
        conflicting
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .unwrap();
        let err = commit_at(&db, &conflicting, None).unwrap_err();
        assert!(err.contains("IDEMPOTENCY_CONFLICT"));
        assert!(
            read_one_node(&db, "graph-a", "different", DurableCrypto::none())
                .unwrap()
                .is_none()
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn lifecycle_adapter_commits_meta_and_delete_before_registry_publication() {
        let path = temp_path("lifecycle");
        let db = open(&path);
        let mut create = batch("create-graph-a", "create-key");
        create.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Lifecycle,
            domain: DurabilityDomain::Lifecycle,
            method: Method::CreateGraph {
                graph_name: "graph-a".to_string(),
                graph_type: GraphType::Agent,
            },
        }];
        create
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("lifecycle create fixture reseals its final body");
        commit_at(&db, &create, None).unwrap();
        let meta = read_all_graph_meta(&db).unwrap();
        assert!(meta
            .iter()
            .any(|(fname, name, graph_type, incarnation_id)| {
                fname == "graph-a"
                    && name == "graph-a"
                    && *graph_type == GraphType::Agent
                    && incarnation_id == "create-graph-a"
            }));

        let mut delete = batch("delete-graph-a", "delete-key");
        delete.version_expectation = VersionExpectation::Graph(4);
        delete.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Lifecycle,
            domain: DurabilityDomain::Lifecycle,
            method: Method::DeleteGraph {
                graph_name: "graph-a".to_string(),
            },
        }];
        delete
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("lifecycle delete fixture reseals its final body");
        commit_at(&db, &delete, None).unwrap();
        assert!(read_all_graph_meta(&db).unwrap().is_empty());
        assert_eq!(
            read_mutation_batch_for_graph(&db, "graph-a", "delete-graph-a")
                .unwrap()
                .unwrap()
                .status,
            MutationBatchStatus::Committed
        );
        drop(db);
        let db = reopen(&path);
        // Reopening and binding the same name yields a fresh scope after the
        // delete retired its prior identity; the stale batch below must fail
        // against that new authoritative version before metadata can return.
        // The old incarnation's scope identity cannot be re-admitted after the
        // delete, so this retry must fail closed before metadata is recreated.
        let stale = commit_at(&db, &create, None).unwrap_err();
        assert!(stale.contains("STALE_VERSION"), "got: {stale}");
        assert!(
            read_all_graph_meta(&db).unwrap().is_empty(),
            "retrying the old Create must not resurrect graph metadata after Delete"
        );
        let _ = std::fs::remove_file(path);
    }

    /// D-P0-U04 regression: `Method::DeleteGraph` must atomically remove the
    /// PRIOR incarnation's mutation-authority rows (idempotency replay keys,
    /// `MUTATION_BATCHES`/`MUTATION_OUTBOX` records) -- not only graph/change/
    /// resource/lane rows -- so a same-name recreate never collides with or
    /// attempts to decrypt an old-incarnation mutation record. Fails before
    /// the `clear_mutation_authority_rows` call was wired into `DeleteGraph`'s
    /// commit path (the idempotency/outbox rows below survived the delete);
    /// passes after.
    #[test]
    fn delete_graph_purges_prior_incarnation_mutation_authority() {
        let path = temp_path("mutation-authority-purge");
        let db = open(&path);

        let mut create = batch("create-graph-a", "create-key");
        create.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Lifecycle,
            domain: DurabilityDomain::Lifecycle,
            method: Method::CreateGraph {
                graph_name: "graph-a".to_string(),
                graph_type: GraphType::Agent,
            },
        }];
        create
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("authority purge create fixture reseals its final body");
        commit_at(&db, &create, None).unwrap();

        // An ORDINARY (non-lifecycle) content mutation against the live graph --
        // this is what stamps MUTATION_IDEMPOTENCY/MUTATION_BATCHES/MUTATION_OUTBOX
        // for the incarnation being deleted below.
        let mut content = batch("content-batch-1", "content-key-1");
        content.version_expectation = VersionExpectation::Graph(4);
        content
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("authority purge content fixture reseals its final body");
        commit_at(&db, &content, None).unwrap();

        // Prove the prior incarnation's kernel ledger and outbox state is
        // actually present before delete, so the purge assertion is not
        // vacuous.
        assert!(
            read_mutation_batch_for_graph(&db, "graph-a", "content-batch-1")
                .unwrap()
                .is_some()
        );
        assert!(!read_mutation_outbox(&db, "graph-a", "content-batch-1")
            .unwrap()
            .is_empty());

        let mut delete = batch("delete-graph-a", "delete-key");
        delete.version_expectation = VersionExpectation::Graph(5);
        delete.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Lifecycle,
            domain: DurabilityDomain::Lifecycle,
            method: Method::DeleteGraph {
                graph_name: "graph-a".to_string(),
            },
        }];
        delete
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("authority purge delete fixture reseals its final body");
        commit_at(&db, &delete, None).unwrap();

        // The PRIOR incarnation's kernel ledger and outbox rows must be gone.
        assert!(
            read_mutation_batch_for_graph(&db, "graph-a", "content-batch-1")
                .unwrap()
                .is_none(),
            "DeleteGraph must retire the prior incarnation's receipt"
        );
        assert!(
            read_mutation_outbox(&db, "graph-a", "content-batch-1")
                .unwrap()
                .is_empty(),
            "DeleteGraph must retire the prior incarnation's outbox"
        );

        // A recreate under the SAME name reusing the SAME idempotency key must
        // be treated as fresh work, not resolved as a replay of the deleted
        // incarnation's stale batch_id.
        // The scope version is a MONOTONIC per-graph-name authority and
        // `DeleteGraph` does not reset it: the delete's own commit advances it
        // by one like any other batch (3 seeded -> create 4 -> content 5 ->
        // delete 6), and the recreate must therefore supply the NEXT real
        // version, not zero. That monotonicity is deliberate -- it is what makes
        // a stale pre-delete OCC expectation fail closed with `STALE_VERSION`
        // (asserted by the sibling
        // `lifecycle_adapter_commits_meta_and_delete_before_registry_publication`)
        // rather than match a counter that silently restarted. Only a full
        // binding retirement (`purge_graph_rows`) drops the version row, and
        // that is a different path with its own test. Corrected 2026-09-11 by
        // the redb_store absolute-green lane (F1) alongside restoring the
        // in-transaction authority purge this test was written for (D-P0-U04).
        let mut recreate = batch("create-graph-a-v2", "create-key-v2");
        recreate.version_expectation = VersionExpectation::Graph(6);
        recreate.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Lifecycle,
            domain: DurabilityDomain::Lifecycle,
            method: Method::CreateGraph {
                graph_name: "graph-a".to_string(),
                graph_type: GraphType::Agent,
            },
        }];
        recreate
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("authority recreate fixture reseals its final body");
        commit_at(&db, &recreate, None).unwrap();
        let mut content_v2 = batch("content-batch-1-v2", "content-key-1");
        content_v2.version_expectation = VersionExpectation::Graph(7);
        content_v2
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("authority recreate content fixture reseals its final body");
        commit_at(&db, &content_v2, None).unwrap();
        assert!(
            read_mutation_batch_for_graph(&db, "graph-a", "content-batch-1-v2")
                .unwrap()
                .is_some(),
            "the recreated scope must accept fresh work after prior retirement"
        );

        let _ = std::fs::remove_file(path);
    }

    /// The embedded/legacy whole-graph purge seam must remove the COMPLETE
    /// lifecycle-owned authority surface, not only graph rows and graph_meta.
    /// This is deliberately separate from
    /// `delete_graph_purges_prior_incarnation_mutation_authority`: the
    /// canonical MutationBatch DeleteGraph path already exercises its own
    /// in-transaction cleanup, while `purge_graph_rows` is the path used by
    /// `EmbeddedEngine::delete_graph` and the persistence `PurgeGraph` command.
    #[test]
    fn purge_graph_rows_removes_all_lifecycle_owned_mutation_state() {
        let path = temp_path("whole-graph-authority-purge");
        let db = open(&path);

        let mut create = batch("create-graph-a", "create-key");
        create.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Lifecycle,
            domain: DurabilityDomain::Lifecycle,
            method: Method::CreateGraph {
                graph_name: "graph-a".to_string(),
                graph_type: GraphType::Agent,
            },
        }];
        create
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("whole purge create fixture reseals its final body");
        commit_at(&db, &create, None).unwrap();

        // The ordinary mutation seeds an independent idempotency/batch/outbox
        // row for the incarnation being purged. The lifecycle batch above also
        // seeds the graph version, fence, and lifecycle-head rows.
        let mut content = batch("content-batch-1", "content-key-1");
        content.version_expectation = VersionExpectation::Graph(4);
        content
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("whole purge content fixture reseals its final body");
        commit_at(&db, &content, None).unwrap();

        // The helper is the shared durable whole-graph purge used by both the
        // embedded engine and the persistence writer's PurgeGraph command.
        purge_graph_rows(&db, "graph-a").unwrap();

        assert!(read_all_graph_meta(&db).unwrap().is_empty());
        assert!(
            read_mutation_batch_for_graph(&db, "graph-a", "content-batch-1")
                .unwrap()
                .is_none()
        );
        assert!(read_mutation_outbox(&db, "graph-a", "content-batch-1")
            .unwrap()
            .is_empty());
        for retired in [
            "mutation_batches",
            "mutation_idempotency",
            "mutation_outbox",
            "mutation_lifecycle_head",
            "mutation_graph_version",
            "mutation_fence",
            "mutation_outbox_delivery",
            "mutation_projection_cursor",
        ] {
            assert!(
                !eg_storage::owner_table_names(eg_storage::OwnerLayout::GraphShard)
                    .contains(&retired),
                "retired private table remains declared: {retired}"
            );
        }

        // The deletion is durable, not merely visible in the write
        // transaction that performed it.
        drop(db);
        // Reopening must not resurrect the retired receipt or catalog row.
        let reopened = reopen(&path);
        assert!(read_all_graph_meta(&reopened).unwrap().is_empty());
        assert!(
            read_mutation_batch_for_graph(&reopened, "graph-a", "content-batch-1")
                .unwrap()
                .is_none()
        );
        assert!(
            read_mutation_outbox(&reopened, "graph-a", "content-batch-1")
                .unwrap()
                .is_empty()
        );
        drop(reopened);
        let _ = std::fs::remove_file(path);
    }

    fn governed_envelope_for_tenant(
        tenant: &str,
        batch_id: &str,
        key: &str,
        sequence: u64,
        expected_graph_version: u64,
        envelope_id: &str,
    ) -> ChangeEnvelope {
        let mut mutation = batch(batch_id, key);
        let identity = MutationScopeIdentity::graph(
            ScopeTenantId::new(tenant).unwrap(),
            LogicalName::new("graph-a").unwrap(),
            IncarnationId::new("incarnation:test:redb-store").unwrap(),
        );
        let actor = format!("principal:sha256:{}", "a".repeat(64));
        mutation.identity = identity.clone();
        mutation.envelope = super::fixture_operation_envelope(&identity, &actor, 42, key);
        mutation.version_expectation = VersionExpectation::Graph(expected_graph_version);
        mutation.operations.truncate(1);
        mutation.outbox[0].payload = rmp_serde::to_vec_named(&serde_json::json!({
            "event": "projection.test"
        }))
        .unwrap();
        mutation
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .unwrap();
        let digest = if sequence == 1 { "a" } else { "b" }.repeat(64);
        ChangeEnvelope {
            schema_version: CHANGE_ENVELOPE_VERSION,
            envelope_id: envelope_id.to_string(),
            mutation,
            content_version: ContentVersion {
                object_id: "object-1".to_string(),
                digest_algorithm: "sha256".to_string(),
                digest,
                previous_digest: (sequence > 1).then(|| "a".repeat(64)),
                source_version: ContentVersionPosition::Sequence(sequence),
            },
            cursor: Some(ChangeCursor {
                source: "fixture-source".to_string(),
                partition: "partition-1".to_string(),
                position: CursorPosition::Sequence(sequence),
                expected_previous: (sequence > 1).then_some(CursorPosition::Sequence(sequence - 1)),
            }),
            blobs: Vec::new(),
            features: Vec::new(),
            evidence: Vec::new(),
            policies: vec![PolicyRecord {
                policy_id: "policy-object-1".to_string(),
                operation: MaterialOperation::Upsert,
                object_id: "object-1".to_string(),
                tenant: tenant.to_string(),
                classification: "internal".to_string(),
                policy_version: "policy-v1".to_string(),
                subject_set_digest: "c".repeat(64),
                retention_policy: "standard".to_string(),
                legal_hold: false,
            }],
            lineage: Vec::new(),
            privacy: PrivacyAttestation {
                policy_version: "privacy-v1".to_string(),
                sanitizer_version: "sanitizer-v1".to_string(),
                sanitized_payload_digest: "d".repeat(64),
            },
            commit_seq: None,
            commit_descriptor_ref: None,
        }
    }

    fn governed_envelope(batch_id: &str, key: &str, sequence: u64) -> ChangeEnvelope {
        governed_envelope_for_tenant(
            "tenant-a",
            batch_id,
            key,
            sequence,
            2 + sequence,
            &format!("envelope-{sequence}"),
        )
    }

    fn commit_envelope_at(
        shard: &Shard,
        envelope: &ChangeEnvelope,
    ) -> Result<ChangeEnvelopeCommit, String> {
        #[cfg(feature = "security")]
        let mut audit = AuditTailCache::new();
        commit_change_envelope(
            shard,
            "graph-a",
            envelope,
            123,
            DurableCrypto::none(),
            #[cfg(feature = "security")]
            &mut audit,
        )
    }

    #[test]
    fn change_envelope_commits_rows_governance_version_cursor_and_outbox_once() {
        let path = temp_path("change-envelope");
        let db = open(&path);
        assert_eq!(
            read_mutation_graph_version(&db, "graph-a").unwrap(),
            3,
            "open seeds graph-a through three committed maintenance admissions"
        );
        let first = governed_envelope("change-batch-1", "change-key-1", 1);
        let committed = commit_envelope_at(&db, &first).unwrap();
        assert!(!committed.replayed);
        assert_eq!(committed.outbox_count, 3);
        assert_eq!(
            read_mutation_graph_version(&db, "graph-a").unwrap(),
            4,
            "the first envelope advances the three-maintenance seed from 3 to 4"
        );
        let baseline_outbox =
            assert_single_outbox_effect(&db, "change-batch-1", "a", "projection.test");
        assert!(read_one_node(&db, "graph-a", "a", DurableCrypto::none())
            .unwrap()
            .is_some());
        assert_eq!(
            read_change_envelope(&db, "graph-a", "envelope-1", DurableCrypto::none())
                .unwrap()
                .unwrap()
                .envelope
                .mutation
                .batch_id,
            "change-batch-1"
        );
        assert_eq!(
            read_content_version(
                &db,
                "tenant-a",
                "graph-a",
                "object-1",
                DurableCrypto::none(),
            )
            .unwrap()
            .unwrap()
            .source_version,
            ContentVersionPosition::Sequence(1)
        );
        let mut first_retry = governed_envelope("change-batch-1", "change-key-1", 1);
        // Admission rebinds the caller's OCC expectation before replay
        // resolution; a fresh retry therefore carries the version now visible
        // at the graph while retaining the same stable operation identity.
        first_retry.mutation.version_expectation = VersionExpectation::Graph(4);
        assert!(commit_envelope_at(&db, &first_retry).unwrap().replayed);
        assert_eq!(
            read_mutation_graph_version(&db, "graph-a").unwrap(),
            4,
            "the replay returns the first receipt without advancing the version"
        );
        assert_eq!(
            assert_single_outbox_effect(&db, "change-batch-1", "a", "projection.test"),
            baseline_outbox,
            "replay must not duplicate or rewrite an outbox row"
        );

        let second = governed_envelope("change-batch-2", "change-key-2", 2);
        let second_commit = commit_envelope_at(&db, &second).unwrap();
        assert_eq!(second_commit.outbox_count, 3);
        assert_eq!(
            read_mutation_graph_version(&db, "graph-a").unwrap(),
            5,
            "three maintenance admissions plus two fresh envelopes account for version 5"
        );
        assert_single_outbox_effect(&db, "change-batch-2", "a", "projection.test");
        assert_eq!(
            read_change_cursor(
                &db,
                "tenant-a",
                "graph-a",
                "fixture-source",
                "partition-1",
                DurableCrypto::none(),
            )
            .unwrap()
            .unwrap()
            .position,
            CursorPosition::Sequence(2)
        );
        let mut stale = governed_envelope("change-batch-3", "change-key-3", 2);
        stale.envelope_id = "envelope-stale".to_string();
        // The stale content-version assertion must run after the OCC check: the
        // three maintenance seed admissions plus the two fresh envelopes leave
        // the authoritative graph at version 5, while sequence 2 is already
        // present and must be rejected as stale content.
        stale.mutation.version_expectation = VersionExpectation::Graph(5);
        let error = commit_envelope_at(&db, &stale).unwrap_err();
        assert!(
            error.contains("STALE_CONTENT_VERSION"),
            "unexpected stale-envelope rejection: {error}"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn change_envelope_rows_remain_scoped_to_each_caller_tenant() {
        let path = temp_path("change-envelope-caller-tenants");
        let db = open(&path);
        let tenant_a = governed_envelope_for_tenant(
            "tenant-a",
            "caller-tenant-a-batch",
            "caller-tenant-a-key",
            1,
            3,
            "caller-tenant-a-envelope",
        );
        let tenant_b = governed_envelope_for_tenant(
            "tenant-b",
            "caller-tenant-b-batch",
            "caller-tenant-b-key",
            1,
            4,
            "caller-tenant-b-envelope",
        );

        commit_envelope_at(&db, &tenant_a).unwrap();
        commit_envelope_at(&db, &tenant_b).unwrap();

        for (tenant, envelope_id) in [
            ("tenant-a", "caller-tenant-a-envelope"),
            ("tenant-b", "caller-tenant-b-envelope"),
        ] {
            let retained = read_change_envelope(&db, "graph-a", envelope_id, DurableCrypto::none())
                .unwrap()
                .expect("the caller envelope remains durably readable")
                .envelope;
            assert_eq!(retained.mutation.identity.tenant().as_str(), tenant);
            assert_eq!(retained.policies[0].tenant, tenant);

            assert_eq!(
                read_content_version(&db, tenant, "graph-a", "object-1", DurableCrypto::none(),)
                    .unwrap()
                    .unwrap()
                    .source_version,
                ContentVersionPosition::Sequence(1)
            );
            assert_eq!(
                read_change_cursor(
                    &db,
                    tenant,
                    "graph-a",
                    "fixture-source",
                    "partition-1",
                    DurableCrypto::none(),
                )
                .unwrap()
                .unwrap()
                .position,
                CursorPosition::Sequence(1)
            );
        }

        let _ = std::fs::remove_file(path);
    }

    // ── Batched ChangeEnvelope commit (W1.4) ──────────────────────────────────

    /// One first-write envelope on a DISTINCT object, chained onto the graph's seeded
    /// version 3: envelope `index` expects graph version `3 + index` and advances the
    /// shared source cursor to `index + 1`. A whole page of these commits in ONE
    /// transaction (read-your-writes chains version + cursor across the envelopes).
    fn governed_envelope_seq(index: u64) -> ChangeEnvelope {
        let object = format!("object-{index}");
        let mut mutation = batch(&format!("batch-{index}"), &format!("key-{index}"));
        mutation.version_expectation = VersionExpectation::Graph(3 + index);
        mutation.operations.truncate(1);
        mutation.operations[0].method = node(&format!("n{index}"), index as i64);
        mutation.outbox[0].payload =
            rmp_serde::to_vec_named(&serde_json::json!({ "event": "batch" })).unwrap();
        mutation
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .unwrap();
        ChangeEnvelope {
            schema_version: CHANGE_ENVELOPE_VERSION,
            envelope_id: format!("env-{index}"),
            mutation,
            content_version: ContentVersion {
                object_id: object.clone(),
                digest_algorithm: "sha256".to_string(),
                digest: format!("{:064x}", index + 1),
                previous_digest: None,
                source_version: ContentVersionPosition::Sequence(1),
            },
            cursor: Some(ChangeCursor {
                source: "batch-source".to_string(),
                partition: "p1".to_string(),
                position: CursorPosition::Sequence(index + 1),
                expected_previous: (index > 0).then_some(CursorPosition::Sequence(index)),
            }),
            blobs: Vec::new(),
            features: Vec::new(),
            evidence: Vec::new(),
            policies: vec![PolicyRecord {
                policy_id: format!("policy-{index}"),
                operation: MaterialOperation::Upsert,
                object_id: object,
                tenant: "tenant-a".to_string(),
                classification: "internal".to_string(),
                policy_version: "policy-v1".to_string(),
                subject_set_digest: "c".repeat(64),
                retention_policy: "standard".to_string(),
                legal_hold: false,
            }],
            lineage: Vec::new(),
            privacy: PrivacyAttestation {
                policy_version: "privacy-v1".to_string(),
                sanitizer_version: "sanitizer-v1".to_string(),
                sanitized_payload_digest: "d".repeat(64),
            },
            commit_seq: None,
            commit_descriptor_ref: None,
        }
    }

    fn commit_envelopes_at(
        shard: &Shard,
        envelopes: &[ChangeEnvelope],
    ) -> Result<Vec<ChangeEnvelopeCommit>, ChangeEnvelopesError> {
        #[cfg(feature = "security")]
        let mut audit = AuditTailCache::new();
        commit_change_envelopes(
            shard,
            "graph-a",
            envelopes,
            123,
            DurableCrypto::none(),
            #[cfg(feature = "security")]
            &mut audit,
        )
    }

    fn assert_single_outbox_effect(
        shard: &Shard,
        batch_id: &str,
        node_id: &str,
        event: &str,
    ) -> Vec<eg_types::mutation_batch::MutationOutboxRecord> {
        let receipt = read_mutation_batch_for_graph(shard, "graph-a", batch_id)
            .unwrap()
            .expect("the committed envelope must leave one durable kernel receipt");
        assert_eq!(receipt.batch.operations.len(), 1);
        match &receipt.batch.operations[0].method {
            Method::AddNode {
                node_id: recorded_node,
                ..
            } => assert_eq!(recorded_node, node_id),
            other => panic!("unexpected page operation in receipt: {other:?}"),
        }
        assert_eq!(receipt.batch.outbox.len(), 1);
        assert_eq!(receipt.batch.outbox[0].topic, "projection.test");
        assert_eq!(receipt.batch.outbox[0].key, batch_id);
        let expected_payload =
            rmp_serde::to_vec_named(&serde_json::json!({ "event": event })).unwrap();
        assert_eq!(receipt.batch.outbox[0].payload, expected_payload);

        let outbox = read_mutation_outbox(shard, "graph-a", batch_id).unwrap();
        assert_eq!(
            outbox.len(),
            1,
            "one batch outbox intent must write one row"
        );
        assert_eq!(outbox[0].ordinal, 0);
        assert_eq!(outbox[0].batch_id, batch_id);
        assert_eq!(outbox[0].intent.topic, receipt.batch.outbox[0].topic);
        assert_eq!(outbox[0].intent.key, receipt.batch.outbox[0].key);
        assert_eq!(outbox[0].intent.payload, expected_payload);
        outbox
    }

    #[test]
    fn change_envelopes_commit_whole_page_in_one_transaction() {
        let path = temp_path("change-envelopes-page");
        let db = open(&path);
        let page: Vec<ChangeEnvelope> = (0..3).map(governed_envelope_seq).collect();

        let commits = commit_envelopes_at(&db, &page).unwrap();

        // Per-envelope result vocabulary: every envelope is `applied` (not replayed).
        assert_eq!(commits.len(), 3);
        assert!(commits.iter().all(|commit| !commit.replayed));
        assert_eq!(commits[0].envelope_id, "env-0");
        // Every object landed and the graph version advanced by exactly N (3 -> 6),
        // proving all three applied inside the one shared transaction.
        for index in 0..3 {
            assert!(
                read_one_node(&db, "graph-a", &format!("n{index}"), DurableCrypto::none())
                    .unwrap()
                    .is_some()
            );
            assert!(
                read_mutation_batch_for_graph(&db, "graph-a", &format!("batch-{index}"))
                    .unwrap()
                    .is_some(),
                "envelope {index} must leave one durable kernel receipt"
            );
            assert_single_outbox_effect(
                &db,
                &format!("batch-{index}"),
                &format!("n{index}"),
                "batch",
            );
        }
        assert!(commits.iter().all(|commit| commit.outbox_count == 3));
        assert_eq!(
            read_mutation_graph_version(&db, "graph-a").unwrap(),
            6,
            "the shared page must advance the graph version once per envelope"
        );
        // The chained cursor advanced to the last envelope's position — proof that the
        // final envelope (and therefore every earlier one) committed atomically.
        assert_eq!(
            read_change_cursor(
                &db,
                "tenant-a",
                "graph-a",
                "batch-source",
                "p1",
                DurableCrypto::none()
            )
            .unwrap()
            .unwrap()
            .position,
            CursorPosition::Sequence(3)
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn change_envelopes_idempotent_replay_skips_without_duplicating_outbox() {
        let path = temp_path("change-envelopes-replay");
        let db = open(&path);
        let page: Vec<ChangeEnvelope> = (0..2).map(governed_envelope_seq).collect();

        commit_envelopes_at(&db, &page).unwrap();
        let baseline_batch_0 = assert_single_outbox_effect(&db, "batch-0", "n0", "batch");
        let baseline_batch_1 = assert_single_outbox_effect(&db, "batch-1", "n1", "batch");
        // A fresh attempt over the same stable operations: every envelope
        // idempotent-skips without reusing a consumed nonce.
        let mut retry_page: Vec<ChangeEnvelope> = (0..2).map(governed_envelope_seq).collect();
        for retry in &mut retry_page {
            retry.mutation.version_expectation = VersionExpectation::Graph(5);
        }
        let replay = commit_envelopes_at(&db, &retry_page).unwrap();
        assert!(replay.iter().all(|commit| commit.replayed));
        assert!(replay.iter().all(|commit| commit.outbox_count == 3));
        assert_eq!(
            assert_single_outbox_effect(&db, "batch-0", "n0", "batch"),
            baseline_batch_0,
            "idempotent replay must not duplicate or rewrite batch-0's outbox"
        );
        assert_eq!(
            assert_single_outbox_effect(&db, "batch-1", "n1", "batch"),
            baseline_batch_1,
            "idempotent replay must not duplicate or rewrite batch-1's outbox"
        );
        assert_eq!(
            read_mutation_graph_version(&db, "graph-a").unwrap(),
            5,
            "an all-replay page must not advance the graph version"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn change_envelopes_mixed_fresh_replay_and_fresh_share_one_transaction() {
        let path = temp_path("change-envelopes-mixed-success");
        let db = open(&path);
        let replay = governed_envelope_seq(0);
        commit_envelopes_at(&db, std::slice::from_ref(&replay)).unwrap();
        let replay_outbox = assert_single_outbox_effect(&db, "batch-0", "n0", "batch");
        let mut replay_retry = governed_envelope_seq(0);
        replay_retry.mutation.version_expectation = VersionExpectation::Graph(4);
        let fresh_first = governed_envelope_seq(1);
        let fresh_last = governed_envelope_seq(2);
        // A trailing replay must leave the last fresh batch as the group's
        // terminal reference; replacing it with this replay would make the
        // shared commit reject an otherwise valid page.
        let mut trailing_replay = governed_envelope_seq(0);
        trailing_replay.mutation.version_expectation = VersionExpectation::Graph(6);

        let commits = commit_envelopes_at(
            &db,
            &[fresh_first, replay_retry, fresh_last, trailing_replay],
        )
        .unwrap();

        assert_eq!(commits.len(), 4);
        assert!(!commits[0].replayed);
        assert!(commits[1].replayed);
        assert!(!commits[2].replayed);
        assert!(commits[3].replayed);
        assert!(read_one_node(&db, "graph-a", "n0", DurableCrypto::none())
            .unwrap()
            .is_some());
        assert!(read_one_node(&db, "graph-a", "n1", DurableCrypto::none())
            .unwrap()
            .is_some());
        assert!(read_one_node(&db, "graph-a", "n2", DurableCrypto::none())
            .unwrap()
            .is_some());
        assert_single_outbox_effect(&db, "batch-1", "n1", "batch");
        assert_single_outbox_effect(&db, "batch-2", "n2", "batch");
        assert_eq!(
            assert_single_outbox_effect(&db, "batch-0", "n0", "batch"),
            replay_outbox,
            "the replay must not duplicate or rewrite its prior outbox"
        );
        assert!(commits.iter().all(|commit| commit.outbox_count == 3));
        assert_eq!(
            read_mutation_graph_version(&db, "graph-a").unwrap(),
            6,
            "only the two fresh members advance the graph version"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn change_envelopes_mixed_replay_and_late_failure_roll_back_fresh_suffix() {
        let path = temp_path("change-envelopes-mixed-abort");
        let db = open(&path);
        let replay = governed_envelope_seq(0);
        commit_envelopes_at(&db, std::slice::from_ref(&replay)).unwrap();
        let replay_outbox = assert_single_outbox_effect(&db, "batch-0", "n0", "batch");
        let mut replay_retry = governed_envelope_seq(0);
        replay_retry.mutation.version_expectation = VersionExpectation::Graph(4);
        let mut bad = governed_envelope_seq(2);
        bad.content_version.previous_digest = Some("a".repeat(64));

        let error =
            commit_envelopes_at(&db, &[replay_retry, governed_envelope_seq(1), bad]).unwrap_err();

        assert_eq!(error.index, 2);
        assert!(error.error.contains("STALE_CONTENT_VERSION"));
        assert!(read_one_node(&db, "graph-a", "n0", DurableCrypto::none())
            .unwrap()
            .is_some());
        assert!(read_one_node(&db, "graph-a", "n1", DurableCrypto::none())
            .unwrap()
            .is_none());
        assert!(
            read_mutation_batch_for_graph(&db, "graph-a", "batch-1")
                .unwrap()
                .is_none(),
            "fresh work after a replay must roll back with the late failure"
        );
        assert!(read_mutation_outbox(&db, "graph-a", "batch-1")
            .unwrap()
            .is_empty());
        assert_eq!(
            assert_single_outbox_effect(&db, "batch-0", "n0", "batch"),
            replay_outbox,
            "the already durable replay remains unchanged"
        );
        assert_eq!(read_mutation_graph_version(&db, "graph-a").unwrap(), 4);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn change_envelopes_consumed_nonce_rejects_page_without_fresh_effects() {
        let path = temp_path("change-envelopes-nonce");
        let db = open(&path);
        let first = governed_envelope_seq(0);
        let mut duplicate_nonce = governed_envelope_seq(1);
        let nonce = first
            .mutation
            .envelope
            .operation()
            .expect("fixture operation envelope")
            .authority
            .nonce;
        let eg_types::mutation_batch::MutationEnvelope::Operation(operation) =
            &mut duplicate_nonce.mutation.envelope
        else {
            panic!("fixture operation envelope");
        };
        operation.authority.nonce = nonce;
        operation.authority.idempotency_key =
            Some(eg_types::contract::IdempotencyKey::new("different-page-key").unwrap());
        operation.authority.context_digest =
            operation.authority.recompute_context_digest().unwrap();
        duplicate_nonce
            .mutation
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .unwrap();

        let error = commit_envelopes_at(&db, &[first, duplicate_nonce]).unwrap_err();

        assert_eq!(error.index, 1);
        assert!(error.error.contains("REPLAY_NONCE_CONSUMED"), "{error:?}");
        assert!(read_one_node(&db, "graph-a", "n0", DurableCrypto::none())
            .unwrap()
            .is_none());
        assert!(read_mutation_batch_for_graph(&db, "graph-a", "batch-0")
            .unwrap()
            .is_none());
        assert!(read_mutation_outbox(&db, "graph-a", "batch-0")
            .unwrap()
            .is_empty());
        assert_eq!(read_mutation_graph_version(&db, "graph-a").unwrap(), 3);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn change_envelopes_abort_rolls_back_the_whole_graph_batch() {
        let path = temp_path("change-envelopes-abort");
        let db = open(&path);
        // The third envelope fails its content-version check (previous digest on a
        // fresh object) — both earlier envelopes must roll back with the page.
        let mut bad = governed_envelope_seq(2);
        bad.content_version.previous_digest = Some("a".repeat(64));
        let page = vec![governed_envelope_seq(0), governed_envelope_seq(1), bad];

        let error = commit_envelopes_at(&db, &page).unwrap_err();
        assert_eq!(error.index, 2);
        assert!(
            error.error.contains("STALE_CONTENT_VERSION"),
            "{}",
            error.error
        );
        // NOTHING committed: the first (valid) envelope rolled back with the batch.
        for index in 0..2 {
            assert!(
                read_one_node(&db, "graph-a", &format!("n{index}"), DurableCrypto::none())
                    .unwrap()
                    .is_none()
            );
            assert!(read_change_envelope(
                &db,
                "graph-a",
                &format!("env-{index}"),
                DurableCrypto::none()
            )
            .unwrap()
            .is_none());
            assert!(
                read_mutation_batch_for_graph(&db, "graph-a", &format!("batch-{index}"))
                    .unwrap()
                    .is_none(),
                "the kernel receipt for earlier envelope {index} must roll back"
            );
            assert!(
                read_mutation_outbox(&db, "graph-a", &format!("batch-{index}"))
                    .unwrap()
                    .is_empty(),
                "earlier envelope {index} must not leave an outbox row"
            );
        }
        assert_eq!(
            read_mutation_graph_version(&db, "graph-a").unwrap(),
            3,
            "the graph version must roll back with the envelope rows"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn change_envelopes_oversized_batch_is_a_typed_error() {
        let path = temp_path("change-envelopes-oversized");
        let db = open(&path);
        let too_many: Vec<ChangeEnvelope> =
            (0..(crate::change_envelope::MAX_ENVELOPES_PER_BATCH as u64 + 1))
                .map(governed_envelope_seq)
                .collect();

        let error = commit_envelopes_at(&db, &too_many).unwrap_err();
        assert!(
            error.error.contains("CHANGE_BATCH_TOO_LARGE"),
            "{}",
            error.error
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn change_envelope_batch_of_one_matches_the_single_commit_receipt() {
        let batch_path = temp_path("change-envelopes-parity-batch");
        let single_path = temp_path("change-envelopes-parity-single");
        let batch_db = open(&batch_path);
        let single_db = open(&single_path);
        let envelope = governed_envelope_seq(0);

        let batched = commit_envelopes_at(&batch_db, std::slice::from_ref(&envelope)).unwrap();
        let single = commit_envelope_at(&single_db, &envelope).unwrap();

        // A one-envelope batch yields the exact same receipt the single method does.
        assert_eq!(batched.len(), 1);
        assert_eq!(batched[0], single);
        let _ = std::fs::remove_file(batch_path);
        let _ = std::fs::remove_file(single_path);
    }

    // CXA-EG-02 characterization: `DeferWorkItem` and `CancelWorkItem` had NO
    // coverage anywhere in this module (or in `mutation_batch_tests` more broadly)
    // before this lane -- every other `apply_work_item_rows` arm (ClaimWorkItem,
    // RenewWorkItemLease, CasWorkItemMetadata, CommitWorkItemResult) is exercised
    // above, but these two were not. Added as part of decomposing
    // `apply_work_item_rows` (CCN 117 -> 2) so the extraction of these two arms has
    // a real black-box regression net, not just a clean `cargo check`. Ran GREEN
    // against the UNMODIFIED function before the refactor landed (see the lane
    // report for the exact `cargo test` transcript); unchanged by the refactor
    // commit.
    #[test]
    fn defer_work_item_returns_leased_item_to_ready_with_bumped_epoch() {
        let path = temp_path("work-item-defer");
        let db = open(&path);

        let mut seed = batch("work-item-defer-seed", "work-item-defer-seed-key");
        seed.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Graph,
            domain: DurabilityDomain::GraphRows,
            method: ready_work_item_method("work-defer", 3),
        }];
        seed.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("defer seed fixture reseals its final body");
        commit_at(&db, &seed, None).unwrap();
        let claimed = commit_native_claim(
            &db,
            "work-item-defer-claim",
            "work-item-defer-claim-key",
            4,
            Some("work-defer"),
            "worker-a",
            0,
            60_000,
            64,
        );
        assert!(claimed.claimed);
        assert_eq!(claimed.lease_epoch, Some(1));
        assert_eq!(claimed.fencing_token, Some(1));

        let mut defer = batch("work-item-defer-op", "work-item-defer-op-key");
        // `DeferWorkItem` is a `native_terminal_work_item_cas` batch (its own
        // lease/fencing token is the real CAS guard), but `version_expectation`
        // is still checked like any other graph-scoped batch by
        // `check_occ_version_and_fence` -- 5 is the actual current graph
        // version here (seed 3->4, claim 4->5) and must match exactly.
        defer.version_expectation = VersionExpectation::Graph(5);
        defer.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Job,
            domain: DurabilityDomain::ControlPlane,
            method: Method::DeferWorkItem {
                tenant: "tenant-a".into(),
                work_item_id: "work-defer".into(),
                worker_id: "worker-a".into(),
                lease_epoch: 1,
                fencing_token: 1,
                idempotency_key: "defer-key".into(),
                next_retry_at_ms: 5_000,
                reason_ref: Some("reason:sha256:one".into()),
                now_ms: 1_000,
            },
        }];
        defer
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("defer fixture reseals its final body");
        let committed = commit_at(&db, &defer, None).unwrap();
        let payload: crate::protocol::ResultPayload = decode_durable(
            committed
                .record
                .result_msgpack
                .as_deref()
                .expect("defer result"),
        )
        .unwrap();
        let value = match payload {
            crate::protocol::ResultPayload::Json(value) => value,
            other => panic!("DeferWorkItem must return a JSON result, got {other:?}"),
        };
        assert_eq!(value["status"], "deferred");
        assert_eq!(value["lease_epoch"], 2);
        assert_eq!(value["fencing_token"], 2);
        assert_eq!(value["next_retry_at_ms"], 5_000);
        assert_eq!(value["attempt"], 0);
        assert_eq!(value["defer_count"], 1);

        let row = read_one_node(&db, "graph-a", "work-defer", DurableCrypto::none())
            .unwrap()
            .expect("deferred item remains inspectable");
        let props: serde_json::Value = decode_durable(&row).unwrap();
        assert_eq!(props["status"], "ready");
        assert_eq!(props["lease_owner"], serde_json::Value::Null);
        assert_eq!(props["defer_reason_ref"], "reason:sha256:one");
        drop(db);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn cancel_work_item_marks_ready_item_cancelled_and_replay_is_a_noop() {
        let path = temp_path("work-item-cancel");
        let db = open(&path);

        let mut seed = batch("work-item-cancel-seed", "work-item-cancel-seed-key");
        seed.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Graph,
            domain: DurabilityDomain::GraphRows,
            method: ready_work_item_method("work-cancel", 3),
        }];
        seed.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("cancel seed fixture reseals its final body");
        commit_at(&db, &seed, None).unwrap();

        let mut cancel = batch("work-item-cancel-op", "work-item-cancel-op-key");
        // `CancelWorkItem` is a `native_terminal_work_item_cas` batch, but
        // v1 removed the "supply no expectation to skip the OCC check"
        // escape hatch structurally (a graph-scoped batch must always carry
        // `VersionExpectation::Graph(_)`; see `check_occ_version_and_fence`'s
        // doc). `check_occ_version_and_fence` DOES check this value now, for
        // every graph-scoped batch uniformly -- the same real-OCC upgrade
        // `resource_reservation_tests.rs` deliberately opted into. 4 is the
        // actual current graph version here (only `seed` has committed:
        // 3->4).
        cancel.version_expectation = VersionExpectation::Graph(4);
        cancel.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Job,
            domain: DurabilityDomain::ControlPlane,
            method: Method::CancelWorkItem {
                tenant: "tenant-a".into(),
                work_item_id: "work-cancel".into(),
                idempotency_key: "cancel-key".into(),
                reason_ref: Some("reason:sha256:two".into()),
                now_ms: 1_000,
            },
        }];
        cancel
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("cancel fixture reseals its final body");
        let committed = commit_at(&db, &cancel, None).unwrap();
        let payload: crate::protocol::ResultPayload = decode_durable(
            committed
                .record
                .result_msgpack
                .as_deref()
                .expect("cancel result"),
        )
        .unwrap();
        let value = match payload {
            crate::protocol::ResultPayload::Json(value) => value,
            other => panic!("CancelWorkItem must return a JSON result, got {other:?}"),
        };
        assert_eq!(value["status"], "cancelled");

        let row = read_one_node(&db, "graph-a", "work-cancel", DurableCrypto::none())
            .unwrap()
            .expect("cancelled item remains inspectable");
        let props: serde_json::Value = decode_durable(&row).unwrap();
        assert_eq!(props["status"], "cancelled");
        assert_eq!(props["cancel_reason_ref"], "reason:sha256:two");

        // A fresh (distinct idempotency key) CancelWorkItem operation against an
        // ALREADY-cancelled item exercises the handler's own internal
        // `matches!(status, "succeeded"|"failed"|"cancelled"|"dead_letter") ->
        // noop` guard -- distinct from MutationBatch-level idempotency replay,
        // which a different idempotency_key deliberately bypasses. Because it
        // is a genuinely NEW batch (not a replay), it is subject to
        // `check_occ_version_and_fence` like any other graph-scoped commit --
        // `cancel`'s own commit above already advanced `graph-a` 4->5, so this
        // second, independent commit must expect the CURRENT version (5), not
        // a stale clone of `cancel`'s now-superseded `Graph(4)`.
        let mut cancel_again = cancel.clone();
        cancel_again.batch_id = "work-item-cancel-op-2".into();
        cancel_again.envelope = fixture_operation_envelope(
            &cancel_again.identity,
            &format!("principal:sha256:{}", "a".repeat(64)),
            42,
            "work-item-cancel-op-2-key",
        );
        cancel_again.outbox[0].key = cancel_again.batch_id.clone();
        cancel_again.version_expectation = VersionExpectation::Graph(5);
        cancel_again
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("cancel replay fixture reseals its final body");
        let replay = commit_at(&db, &cancel_again, None).unwrap();
        let payload: crate::protocol::ResultPayload = decode_durable(
            replay
                .record
                .result_msgpack
                .as_deref()
                .expect("cancel replay result"),
        )
        .unwrap();
        let value = match payload {
            crate::protocol::ResultPayload::Json(value) => value,
            other => panic!("CancelWorkItem must return a JSON result, got {other:?}"),
        };
        assert_eq!(value["status"], "noop");

        drop(db);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn terminal_extension_commits_rows_outbox_and_replays_after_reopen() {
        let path = temp_path("terminal-extension-success-reopen");
        let db = open(&path);
        let mut seed = batch("terminal-extension-seed", "terminal-extension-seed-key");
        seed.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Transaction,
            domain: DurabilityDomain::GraphRows,
            method: delegated_work_item_method("work-extension-success", 3),
        }];
        seed.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .unwrap();
        commit_at(&db, &seed, None).unwrap();
        let claimed = commit_native_claim(
            &db,
            "terminal-extension-claim",
            "terminal-extension-claim-key",
            4,
            Some("work-extension-success"),
            "worker-a",
            0,
            60_000,
            64,
        );
        let terminal = terminal_extension_batch(
            "terminal-extension-success",
            "terminal-extension-success-key",
            5,
            TerminalLeaseHold {
                work_item_id: "work-extension-success",
                worker_id: "worker-a",
                lease_epoch: claimed.lease_epoch.unwrap(),
                fencing_token: claimed.fencing_token.unwrap(),
            },
            "succeeded",
            false,
        );
        let committed = commit_at(&db, &terminal, None).unwrap();
        assert!(!committed.replayed);
        let work_item = read_one_node(
            &db,
            "graph-a",
            "work-extension-success",
            DurableCrypto::none(),
        )
        .unwrap()
        .unwrap();
        let work_item: serde_json::Value = decode_durable(&work_item).unwrap();
        assert_eq!(work_item["status"], "succeeded");
        for node_id in ["trace:terminal", "toolcall:terminal:0", "outcome:terminal"] {
            let receipt = read_one_node(&db, "graph-a", node_id, DurableCrypto::none())
                .unwrap()
                .unwrap();
            let receipt: serde_json::Value = decode_durable(&receipt).unwrap();
            assert_eq!(receipt["work_item_id"], "work-extension-success");
            assert_eq!(receipt["delegator_id"], "agent:delegator-a");
            assert_eq!(receipt["selected_agent_id"], "agent:selected-b");
            assert_eq!(receipt["executor_lease_actor"], "worker-a");
            assert_eq!(receipt["outcome"], "succeeded");
            assert_eq!(receipt["completeness"], "complete");
            assert_eq!(receipt["missing_refs"], serde_json::json!([]));
            assert_eq!(receipt["model_digest"], digest_for('e'));
            assert_eq!(receipt["policy_digest"], digest_for('d'));
        }
        let outbox = read_mutation_outbox(&db, "graph-a", terminal.batch_id.as_str()).unwrap();
        assert_eq!(outbox.len(), 1);
        assert_eq!(
            outbox
                .iter()
                .filter(|row| row.intent.topic == RUN_EVENT_OUTBOX_TOPIC)
                .count(),
            1
        );
        drop(db);

        let reopened = reopen(&path);
        let durable_receipt = read_one_node(
            &reopened,
            "graph-a",
            "trace:terminal",
            DurableCrypto::none(),
        )
        .unwrap();
        assert!(durable_receipt.is_some());
        let mut replay = terminal.clone();
        let eg_types::mutation_batch::MutationEnvelope::Operation(operation) = &mut replay.envelope
        else {
            panic!("terminal replay fixture needs an operation envelope");
        };
        operation.authority.nonce = eg_types::contract::Nonce::from_bytes([0x43; 32]);
        operation.authority.context_digest =
            operation.authority.recompute_context_digest().unwrap();
        replay.created_at_ms = 200;
        replay
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .unwrap();
        let replayed = commit_at(&reopened, &replay, None).unwrap();
        assert!(replayed.replayed);
        let replay_outbox =
            read_mutation_outbox(&reopened, "graph-a", terminal.batch_id.as_str()).unwrap();
        assert_eq!(replay_outbox.len(), 1);
        assert_eq!(
            replay_outbox
                .iter()
                .filter(|row| row.intent.topic == RUN_EVENT_OUTBOX_TOPIC)
                .count(),
            1
        );
        drop(reopened);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn terminal_extension_rejects_resealed_foreign_scope_before_durable_advance() {
        let path = temp_path("terminal-extension-foreign-scope");
        let db = open(&path);
        let work_item_id = "work-terminal-extension-foreign-scope";
        let claimed = seed_and_claim_terminal_work_item(
            &db,
            "terminal-extension-foreign-scope",
            work_item_id,
        );
        let batch_id = "terminal-extension-foreign-scope-batch";
        let mut terminal = terminal_extension_batch(
            batch_id,
            "terminal-extension-foreign-scope-key",
            5,
            TerminalLeaseHold {
                work_item_id,
                worker_id: "worker-a",
                lease_epoch: claimed.lease_epoch.unwrap(),
                fencing_token: claimed.fencing_token.unwrap(),
            },
            "succeeded",
            false,
        );
        let version_before = read_mutation_graph_version(&db, "graph-a").unwrap();
        let caller_scope = terminal
            .outbox
            .iter()
            .find(|intent| intent.topic == RUN_EVENT_OUTBOX_TOPIC)
            .and_then(|intent| intent.headers.get("scope_sha256"))
            .cloned()
            .expect("terminal fixture carries the caller scope digest");

        let mut foreign_scope_digest = Sha256::new();
        foreign_scope_digest.update(b"tenant-b");
        foreign_scope_digest.update([0]);
        foreign_scope_digest.update(b"graph-b");
        let foreign_scope = hex::encode(foreign_scope_digest.finalize());
        terminal
            .outbox
            .iter_mut()
            .find(|intent| intent.topic == RUN_EVENT_OUTBOX_TOPIC)
            .expect("terminal fixture carries a run event")
            .headers
            .insert("scope_sha256".into(), foreign_scope);
        terminal
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .unwrap();

        let error = commit_at(&db, &terminal, None).unwrap_err();
        assert!(
            error.contains("scope_sha256") && error.contains("caller mutation scope"),
            "got: {error}"
        );
        assert_eq!(
            read_mutation_graph_version(&db, "graph-a").unwrap(),
            version_before,
            "caller-scope rejection must not advance the graph version"
        );
        assert!(
            read_mutation_batch_for_graph(&db, "graph-a", batch_id)
                .unwrap()
                .is_none(),
            "caller-scope rejection must not persist a batch receipt"
        );
        assert!(
            read_mutation_outbox(&db, "graph-a", batch_id)
                .unwrap()
                .is_empty(),
            "caller-scope rejection must not persist an outbox row"
        );

        // Restore the authenticated caller header and retry with the same
        // attempt nonce. A successful commit proves the rejected attempt did
        // not consume the durable replay nonce before scope authentication.
        terminal
            .outbox
            .iter_mut()
            .find(|intent| intent.topic == RUN_EVENT_OUTBOX_TOPIC)
            .expect("terminal fixture carries a run event")
            .headers
            .insert("scope_sha256".into(), caller_scope);
        terminal
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .unwrap();
        let committed = commit_at(&db, &terminal, None).unwrap();
        assert!(!committed.replayed);
        assert!(read_mutation_batch_for_graph(&db, "graph-a", batch_id)
            .unwrap()
            .is_some());
        assert_eq!(
            read_mutation_outbox(&db, "graph-a", batch_id)
                .unwrap()
                .len(),
            1
        );
        drop(db);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn terminal_extension_persists_terminal_currency_across_reopen() {
        for (outcome, tag) in [
            ("failed", "terminal-extension-failed-reopen"),
            ("cancelled", "terminal-extension-cancelled-reopen"),
        ] {
            let path = temp_path(tag);
            let db = open(&path);
            let work_item_id = format!("work-{tag}");
            let claimed = seed_and_claim_terminal_work_item(&db, tag, &work_item_id);
            let batch_id = format!("{tag}-batch");
            let terminal = terminal_extension_batch(
                &batch_id,
                &format!("{tag}-key"),
                5,
                TerminalLeaseHold {
                    work_item_id: &work_item_id,
                    worker_id: "worker-a",
                    lease_epoch: claimed.lease_epoch.unwrap(),
                    fencing_token: claimed.fencing_token.unwrap(),
                },
                outcome,
                false,
            );
            commit_at(&db, &terminal, None).unwrap();
            assert_persisted_terminal_currency(
                &db,
                &batch_id,
                outcome,
                OutcomeCompleteness::Complete,
                &[],
            );
            drop(db);

            let reopened = reopen(&path);
            assert_persisted_terminal_currency(
                &reopened,
                &batch_id,
                outcome,
                OutcomeCompleteness::Complete,
                &[],
            );
            drop(reopened);
            let _ = std::fs::remove_file(path);
        }

        let path = temp_path("terminal-extension-degraded-reopen");
        let db = open(&path);
        let work_item_id = "work-terminal-extension-degraded";
        let claimed =
            seed_and_claim_terminal_work_item(&db, "terminal-extension-degraded", work_item_id);
        let batch_id = "terminal-extension-degraded-batch";
        let mut terminal = terminal_extension_batch(
            batch_id,
            "terminal-extension-degraded-key",
            5,
            TerminalLeaseHold {
                work_item_id,
                worker_id: "worker-a",
                lease_epoch: claimed.lease_epoch.unwrap(),
                fencing_token: claimed.fencing_token.unwrap(),
            },
            "succeeded",
            false,
        );
        let missing_refs = ["toolcall:terminal:0"];
        let degraded_event = {
            let Method::CommitWorkItemResult {
                outcome_extension: Some(extension),
                ..
            } = &mut terminal.operations[0].method
            else {
                panic!("degraded fixture must carry a terminal extension");
            };
            extension.outcome_bundle.completeness = OutcomeCompleteness::Degraded;
            extension.outcome_bundle.missing_refs = missing_refs
                .iter()
                .map(|reference| (*reference).to_string())
                .collect();
            extension.run_event.completeness = OutcomeCompleteness::Degraded;
            extension.run_event.missing_refs = extension.outcome_bundle.missing_refs.clone();
            extension.run_event.kind = "degraded".into();
            let bundle = extension.outcome_bundle.clone();
            extension.receipt_nodes = vec![
                terminal_receipt_node(&bundle, ReceiptNodeKind::RunTrace, &bundle.trace_ref),
                terminal_receipt_node(
                    &bundle,
                    ReceiptNodeKind::OutcomeEvaluation,
                    &bundle.outcome_ref,
                ),
            ];
            extension.run_event.clone()
        };
        let event_intent = terminal
            .outbox
            .iter_mut()
            .find(|intent| intent.topic == RUN_EVENT_OUTBOX_TOPIC)
            .expect("degraded fixture must carry a run event intent");
        event_intent.payload = rmp_serde::to_vec_named(&degraded_event).unwrap();
        event_intent
            .headers
            .insert("completeness".into(), "degraded".into());
        event_intent.headers.insert(
            "missing_refs".into(),
            serde_json::to_string(&missing_refs).unwrap(),
        );
        terminal
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .unwrap();
        commit_at(&db, &terminal, None).unwrap();
        assert_persisted_terminal_currency(
            &db,
            batch_id,
            "succeeded",
            OutcomeCompleteness::Degraded,
            &missing_refs,
        );
        drop(db);

        let reopened = reopen(&path);
        assert_persisted_terminal_currency(
            &reopened,
            batch_id,
            "succeeded",
            OutcomeCompleteness::Degraded,
            &missing_refs,
        );
        drop(reopened);
        let _ = std::fs::remove_file(path);
    }

    struct NegativeTerminalCase {
        expected_version: u64,
        hold: TerminalLeaseHold<'static>,
        outcome: &'static str,
        retryable: bool,
    }

    fn prepare_negative_terminal_case(
        shard: &Shard,
        outcome_case: &str,
        tag: &str,
    ) -> NegativeTerminalCase {
        match outcome_case {
            "missing" => NegativeTerminalCase {
                expected_version: 3,
                hold: TerminalLeaseHold {
                    work_item_id: "work-extension-missing",
                    worker_id: "worker-a",
                    lease_epoch: 1,
                    fencing_token: 1,
                },
                outcome: "succeeded",
                retryable: false,
            },
            "fenced" => {
                let mut seed = batch(&format!("{tag}-seed"), &format!("{tag}-seed-key"));
                seed.operations = vec![MutationOperation {
                    ordinal: 0,
                    surface: MutationSurface::Transaction,
                    domain: DurabilityDomain::GraphRows,
                    method: delegated_work_item_method("work-extension-fenced", 3),
                }];
                seed.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
                    .unwrap();
                commit_at(shard, &seed, None).unwrap();
                commit_native_claim(
                    shard,
                    &format!("{tag}-claim"),
                    &format!("{tag}-claim-key"),
                    4,
                    Some("work-extension-fenced"),
                    "worker-a",
                    0,
                    60_000,
                    64,
                );
                NegativeTerminalCase {
                    expected_version: 5,
                    hold: TerminalLeaseHold {
                        work_item_id: "work-extension-fenced",
                        worker_id: "worker-b",
                        lease_epoch: 1,
                        fencing_token: 999,
                    },
                    outcome: "succeeded",
                    retryable: false,
                }
            }
            "noop" => {
                // The already-terminal item is driven terminal through the
                // NATIVE authority, not planted terminal by an AddNode. A public
                // submission may only introduce a WorkItem in `submitted`/`ready`
                // (`work_item_capability::validate_submission_properties`
                // -- "native WorkItem authority required for active
                // lease fields"), so seeding `status: "succeeded"` directly is
                // refused at admission and this case never reached the noop
                // precheck it exists to cover. `CancelWorkItem` is the terminal
                // transition that leaves NO receipt extension behind, which is
                // what keeps the `trace:terminal` assertion below meaningful.
                let mut seed = batch(&format!("{tag}-seed"), &format!("{tag}-seed-key"));
                seed.operations = vec![MutationOperation {
                    ordinal: 0,
                    surface: MutationSurface::Transaction,
                    domain: DurabilityDomain::GraphRows,
                    method: delegated_work_item_method("work-extension-noop", 3),
                }];
                seed.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
                    .unwrap();
                commit_at(shard, &seed, None).unwrap();
                let mut cancel = batch(&format!("{tag}-cancel"), &format!("{tag}-cancel-key"));
                cancel.version_expectation = VersionExpectation::Graph(4);
                cancel.operations = vec![MutationOperation {
                    ordinal: 0,
                    surface: MutationSurface::Job,
                    domain: DurabilityDomain::ControlPlane,
                    method: Method::CancelWorkItem {
                        tenant: "tenant-a".into(),
                        work_item_id: "work-extension-noop".into(),
                        idempotency_key: format!("{tag}-cancel-op-key"),
                        reason_ref: Some("reason:sha256:noop".into()),
                        now_ms: 1_000,
                    },
                }];
                cancel
                    .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
                    .unwrap();
                commit_at(shard, &cancel, None).unwrap();
                NegativeTerminalCase {
                    expected_version: 5,
                    hold: TerminalLeaseHold {
                        work_item_id: "work-extension-noop",
                        worker_id: "worker-a",
                        lease_epoch: 1,
                        fencing_token: 1,
                    },
                    outcome: "succeeded",
                    retryable: false,
                }
            }
            "retry_scheduled" => {
                let mut seed = batch(&format!("{tag}-seed"), &format!("{tag}-seed-key"));
                seed.operations = vec![MutationOperation {
                    ordinal: 0,
                    surface: MutationSurface::Transaction,
                    domain: DurabilityDomain::GraphRows,
                    method: delegated_work_item_method("work-extension-retry", 3),
                }];
                seed.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
                    .unwrap();
                commit_at(shard, &seed, None).unwrap();
                let claimed = commit_native_claim(
                    shard,
                    &format!("{tag}-claim"),
                    &format!("{tag}-claim-key"),
                    4,
                    Some("work-extension-retry"),
                    "worker-a",
                    0,
                    60_000,
                    64,
                );
                NegativeTerminalCase {
                    expected_version: 5,
                    hold: TerminalLeaseHold {
                        work_item_id: "work-extension-retry",
                        worker_id: "worker-a",
                        lease_epoch: claimed.lease_epoch.unwrap(),
                        fencing_token: claimed.fencing_token.unwrap(),
                    },
                    outcome: "failed",
                    retryable: true,
                }
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn terminal_extension_negative_results_have_no_receipt_rows_or_run_event() {
        let cases = [
            ("missing", "terminal-extension-missing"),
            ("fenced", "terminal-extension-fenced"),
            ("noop", "terminal-extension-noop"),
            ("retry_scheduled", "terminal-extension-retry"),
        ];
        for (outcome_case, tag) in cases {
            let path = temp_path(tag);
            let db = open(&path);
            let fixture = prepare_negative_terminal_case(&db, outcome_case, tag);
            let work_item_id = fixture.hold.work_item_id;
            let terminal = terminal_extension_batch(
                &format!("{tag}-batch"),
                &format!("{tag}-key"),
                fixture.expected_version,
                fixture.hold,
                fixture.outcome,
                fixture.retryable,
            );
            let committed = commit_at(&db, &terminal, None).unwrap();
            let result: crate::protocol::ResultPayload =
                decode_durable(committed.record.result_msgpack.as_deref().unwrap()).unwrap();
            let result = match result {
                crate::protocol::ResultPayload::Json(value) => value,
                other => panic!("terminal result must be JSON, got {other:?}"),
            };
            assert_eq!(result["status"], outcome_case);
            assert!(
                read_one_node(&db, "graph-a", "trace:terminal", DurableCrypto::none())
                    .unwrap()
                    .is_none()
            );
            assert!(
                read_mutation_outbox(&db, "graph-a", terminal.batch_id.as_str())
                    .unwrap()
                    .is_empty()
            );
            if fixture.retryable {
                let work_item = read_one_node(&db, "graph-a", work_item_id, DurableCrypto::none())
                    .unwrap()
                    .unwrap();
                let work_item: serde_json::Value = decode_durable(&work_item).unwrap();
                assert_eq!(work_item["status"], "ready");
            }
            drop(db);
            let _ = std::fs::remove_file(path);
        }
    }

    #[test]
    fn terminal_extension_rejects_preexisting_receipt_ids_atomically() {
        let path = temp_path("terminal-extension-preexisting-id");
        let db = open(&path);
        let preexisting_extension = terminal_extension(
            "terminal-extension-preexisting-batch",
            "work-extension-preexisting",
            1,
            "succeeded",
            "worker-a",
        );
        let mut seed = batch(
            "terminal-extension-preexisting-seed",
            "terminal-extension-preexisting-seed-key",
        );
        seed.operations = vec![
            MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Transaction,
                domain: DurabilityDomain::GraphRows,
                method: delegated_work_item_method("work-extension-preexisting", 3),
            },
            MutationOperation {
                ordinal: 1,
                surface: MutationSurface::Transaction,
                domain: DurabilityDomain::GraphRows,
                method: Method::AddNode {
                    node_id: "trace:terminal".into(),
                    properties_msgpack: preexisting_extension.receipt_nodes[0]
                        .properties_msgpack
                        .clone(),
                },
            },
        ];
        seed.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .unwrap();
        commit_at(&db, &seed, None).unwrap();
        let claimed = commit_native_claim(
            &db,
            "terminal-extension-preexisting-claim",
            "terminal-extension-preexisting-claim-key",
            4,
            Some("work-extension-preexisting"),
            "worker-a",
            0,
            60_000,
            64,
        );
        let terminal = terminal_extension_batch(
            "terminal-extension-preexisting-batch",
            "terminal-extension-preexisting-key",
            5,
            TerminalLeaseHold {
                work_item_id: "work-extension-preexisting",
                worker_id: "worker-a",
                lease_epoch: claimed.lease_epoch.unwrap(),
                fencing_token: claimed.fencing_token.unwrap(),
            },
            "succeeded",
            false,
        );
        let error = commit_at(&db, &terminal, None).unwrap_err();
        assert!(error.contains("already exists"), "{error}");
        let work_item = read_one_node(
            &db,
            "graph-a",
            "work-extension-preexisting",
            DurableCrypto::none(),
        )
        .unwrap()
        .unwrap();
        let work_item: serde_json::Value = decode_durable(&work_item).unwrap();
        assert_eq!(work_item["status"], "leased");
        assert!(read_mutation_batch_for_graph(
            &db,
            "graph-a",
            "terminal-extension-preexisting-batch",
        )
        .unwrap()
        .is_none());
        drop(db);
        let _ = std::fs::remove_file(path);
    }
}
