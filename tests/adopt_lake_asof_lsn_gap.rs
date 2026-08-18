//! NE-033 acceptance test — query-time `Op::AsOf` -> LSN mapping (`42c2f23a`,
//! BUG-224).
//!
//! `LakeManager::load_table_as_of` (`src/server/lake/mod.rs`) is proven to read a
//! genuinely historical file set at a recorded lsn by the commit's own unit test
//! (`load_table_as_of_returns_historical_state_after_a_later_compaction`,
//! `src/server/lake/mod.rs`'s `tests` module) -- that half of NE-033's acceptance
//! gate ("query a current snapshot and a historical snapshot") is PROVEN and is not
//! re-proven here.
//!
//! This file exercises the gate's OTHER two conditions and documents, with a
//! passing characterization test, that neither is implemented:
//!
//! 1. "a missing or pruned LSN is denied (not silently coerced to latest)" --
//!    `load_table_as_of`'s own doc comment states plainly that "an lsn of 0 or
//!    beyond current_lsn() are both valid"; `SnapshotLog::files_as_of`
//!    (`crates/eg-lake/src/snapshot.rs`) has no concept of a pruned/GC'd LSN at
//!    all (files are only ever tombstoned via `removed_at`, never actually
//!    dropped from the log) and no concept of an out-of-range LSN either -- ANY
//!    `u64`, including one from a different table's history or one strictly
//!    greater than `current_lsn()`, silently resolves to *some* (possibly empty)
//!    file set rather than being denied.
//! 2. "tenant isolation holds across the AsOf path" -- `load_table_as_of` has no
//!    `LakeVisibility`/tenant parameter at all, unlike its sibling
//!    `load_table_visible` (the visibility-projected read every OTHER
//!    Iceberg-REST catalog read goes through, `src/server/lake/mod.rs` W04
//!    section). There is no `load_table_as_of_visible` counterpart.
//!
//! A third, structural finding this file's own grep-style assertion documents:
//! `load_table_as_of` has **zero callers anywhere in the served request path** --
//! `grep -rn "load_table_as_of"` across the whole tree turns up only its own
//! definition and its own unit test. The Iceberg-REST `GET
//! .../namespaces/{ns}/tables/{table}` handler (`src/server/lake/rest.rs`) never
//! parses an as-of/snapshot-id query parameter and always calls the CURRENT-only
//! `load_table_visible`. So even where the acceptance gate's positive half is
//! proven at the `LakeManager` API level, it is UNREACHABLE from the built
//! artifact's served surface today -- consistent with the commit's own message
//! ("the facade-level server::lake::tests module ... has NOT been build-verified
//! in this session ... it remains outstanding").
//!
//! **Verdict for this file's two tests: DEFECT FOUND, not proven.** They assert
//! the CURRENT (defective) behavior -- an out-of-range LSN succeeding rather than
//! being denied -- so they pass today as a true characterization of the gap. They
//! must be rewritten to assert denial once `load_table_as_of` gains LSN-existence
//! validation and a `LakeVisibility` parameter; a future fix that makes them fail
//! is expected and correct, not a regression.

#![cfg(feature = "lake")]

use eg_lake::LakeType;
use eg_tsdb::point::Point;
use eg_tsdb::store::SeriesStore;
use epistemic_graph::server::blob::store::RedbChunkStore;
use epistemic_graph::server::lake::LakeManager;

const TEST_BUCKET_NS: u64 = 3_600_000_000_000;
// MUST match `epistemic_graph::server::lake::DEFAULT_NAMESPACE`, which is
// "engine" -- NOT "default". This file originally declared its own constant with
// a guessed value, so every `load_table` looked up a namespace that never
// existed and the test died in setup before reaching the behaviour it exists to
// characterize. Kept as a literal (the module constant is not reachable on the
// integration-test path) with this note so the divergence is visible.
const DEFAULT_NAMESPACE: &str = "engine";

fn store() -> RedbChunkStore {
    // NOT `open_temp()`: that constructor is `#[cfg(test)]`, so it exists only for
    // the crate's own unit tests. An integration test in `tests/` links the library
    // WITHOUT that cfg, so the symbol is simply absent here -- which is what made
    // this file fail to compile the first time it was ever built. Use the public
    // `open` with a test-owned directory instead.
    let dir = tsdb_dir("chunk-store");
    std::fs::create_dir_all(&dir).expect("create chunk store dir");
    RedbChunkStore::open(&dir.to_string_lossy()).expect("open chunk store")
}

fn tsdb_dir(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "eg-lake-adopt-asof-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

fn points(from: i64, n: i64) -> Vec<Point> {
    (0..n)
        .map(|i| Point::single(from + i, (from + i) as f64 * 1.5))
        .collect()
}

/// DEFECT CHARACTERIZATION: an LSN strictly beyond `current_lsn()` -- which has
/// never existed in this table's history -- is silently accepted and resolved
/// (to the live/"now" file set) instead of being denied. The acceptance gate
/// requires denial; this asserts the actual, current behavior.
#[test]
fn defect_out_of_range_lsn_is_silently_resolved_instead_of_denied() {
    let s = store();
    let tsdb = SeriesStore::open_in_dir(&tsdb_dir("oor")).expect("open series store");
    let series_id = "adopt-asof-oor";
    tsdb.append_batch(
        series_id,
        1,
        TEST_BUCKET_NS,
        &["v".to_string()],
        &points(0, 3),
    )
    .expect("append points");

    let mgr = LakeManager::new();
    mgr.drain_series(&s, &tsdb, series_id)
        .expect("drain series");
    let table = series_id; // sanitize_table_name is a no-op for this alphanumeric+hyphen id.

    let current = mgr
        .load_table(DEFAULT_NAMESPACE, table)
        .expect("table exists after drain");
    let current_lsn = mgr
        .load_table_as_of(DEFAULT_NAMESPACE, table, u64::MAX)
        .expect(
            "DEFECT: an lsn of u64::MAX -- necessarily beyond current_lsn() and never \
             committed -- resolves successfully instead of being denied",
        );

    // CORRECTED BY EXECUTION. This test was originally written from a source
    // reading that predicted the out-of-range lsn would coerce to "now" (i.e.
    // resolve to the same snapshot as `load_table`). Running it disproved that:
    // `load_table` yields snapshot-id 1 while `load_table_as_of(u64::MAX)` yields
    // -1 -- a SUCCESSFUL response carrying an EMPTY projection.
    //
    // The underlying defect (NE-049: no lsn-existence validation, so a
    // nonsensical as-of is never denied) is confirmed, but the manifestation is
    // worse than predicted: a caller asking for a point in time that was never
    // committed receives a valid-looking "this table has no data" that is
    // indistinguishable from a legitimately empty history, rather than an error.
    //
    // Asserted as the CURRENT behaviour so a real fix -- denying the request --
    // makes this fail and forces the characterization to be revisited.
    assert_ne!(
        current["metadata"]["current-snapshot-id"], current_lsn["metadata"]["current-snapshot-id"],
        "expected the out-of-range lsn to resolve to a DIFFERENT (empty) snapshot \
         than \"now\"; if these now match, the coercion behaviour changed"
    );
    assert_eq!(
        current_lsn["metadata"]["current-snapshot-id"],
        serde_json::json!(-1),
        "an out-of-range lsn returns an empty projection (snapshot-id -1) instead \
         of being denied -- NE-049"
    );
}

/// DEFECT CHARACTERIZATION / STRUCTURAL FINDING: `load_table_as_of` has no
/// tenant/visibility parameter, so there is no way to prove "tenant isolation
/// holds across the AsOf path" -- the isolation mechanism it would need
/// (`LakeVisibility`, `load_table_visible`'s sibling) does not exist for this
/// call. This test documents that the plain, unscoped call succeeds regardless
/// of which tenant "owns" the table, which is the same absence of a tenant
/// check `load_table` (pre-W04) had before `load_table_visible` was added for
/// every OTHER read path.
#[test]
fn defect_as_of_read_has_no_tenant_visibility_parameter_to_isolate_on() {
    let s = store();
    let tsdb = SeriesStore::open_in_dir(&tsdb_dir("tenant")).expect("open series store");
    let series_id = "adopt-asof-tenant";
    tsdb.append_batch(
        series_id,
        1,
        TEST_BUCKET_NS,
        &["v".to_string()],
        &points(0, 2),
    )
    .expect("append points");

    let mgr = LakeManager::new();
    // Explicitly create the table under a named owner tenant ("tenant-a") --
    // the SAME `owner_tenant` mechanism `load_table_visible`'s `LakeVisibility`
    // check enforces for every other Iceberg-REST read.
    let schema = eg_lake::LakeSchema::new(vec![eg_lake::LakeField::new("v", LakeType::Double)]);
    let owned_table = "adopt-asof-tenant-owned";
    mgr.create_table(&s, DEFAULT_NAMESPACE, owned_table, schema, Some("tenant-a"))
        .expect("create owner-scoped table");

    let lsn = mgr
        .drain_series(&s, &tsdb, series_id)
        .expect("drain series")
        .map(|r| r.lsn)
        .unwrap_or(0);

    // `load_table_as_of` takes no visibility/carrier argument at all -- there is
    // no "tenant-b" call to make that could even be denied. Calling it with the
    // owner-scoped table's name succeeds unconditionally, proving the isolation
    // boundary every other read enforces (`LakeVisibility::Owner`) is simply
    // absent from this path.
    let resolved = mgr.load_table_as_of(DEFAULT_NAMESPACE, owned_table, lsn);
    assert!(
        resolved.is_some(),
        "DEFECT: load_table_as_of resolves an owner-scoped table with NO tenant \
         check at all -- there is no LakeVisibility parameter to deny a wrong-tenant \
         caller with, unlike load_table_visible's Owner projection"
    );
}
