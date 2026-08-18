//! NE-034 acceptance test — durable trace-span + segment-manifest restart state
//! (`3cf8263c`, BUG-016/BUG-210).
//!
//! The commit's own unit tests (`src/server/obs/mod.rs::tests::
//! bug_016_traces_survive_an_obsstate_restart_only_after_persist_traces` and
//! `bug_210_segment_manifests_survive_an_obsstate_restart`) already prove restart
//! continuity for a SINGLE stream, and `crates/eg-tsdb/src/traces.rs::tests::
//! bug_016_recover_rejects_corrupt_bytes_without_panicking` already proves bounded
//! (fail-closed, non-panicking) recovery from corrupt durable bytes. Neither proves
//! the acceptance ledger's remaining condition: **zero cross-tenant rows** after
//! recovery.
//!
//! `ObsState`'s own module doc is explicit that `stream` IS the tenancy key ("Org/
//! stream namespace (the tenancy key -> its own series + text namespace)",
//! `src/server/obs/mod.rs`'s `LogRecord::stream` doc) and BUG-037's
//! `observability_access_denied` note records that this tier carries no
//! per-caller `CarrierAuthority` yet -- so the ONLY tenant boundary this durable
//! substrate can be asked to prove today is stream-scoped segment isolation, not a
//! carrier-authority-scoped one. This file drives that exact boundary through the
//! REAL `ObsState::open`/`ingest`/`segments_for`/`read_segment` surface (never a
//! stub), the same "restart on the same durable store" pattern BUG-210's own test
//! uses (drop + reopen on an identical `persist_dir`), but with TWO streams so a
//! restart's manifest rebuild (`ObsState::open` rebuilding `segments` from every
//! `{persist_dir}/obs/segments/<sanitized-stream>.msgpack` side file) is proven to
//! keep them apart rather than merging or cross-leaking rows.

#![cfg(feature = "obs")]

use std::collections::BTreeMap;

use epistemic_graph::server::obs::{LogRecord, ObsState};

fn record(stream: &str, ts: i64, body: &str) -> LogRecord {
    LogRecord {
        ts,
        stream: stream.to_string(),
        severity: "INFO".to_string(),
        body: body.to_string(),
        attrs: BTreeMap::new(),
    }
}

/// Two tenants (`tenant-a`, `tenant-b`) ingest past the flush threshold on the SAME
/// persist dir, "crash" (drop the handle), and reopen. Each tenant's segment
/// manifest and row bytes must be recoverable and must contain EXACTLY that
/// tenant's rows -- zero cross-tenant rows in either direction.
#[test]
fn obs_restart_recovers_disjoint_per_stream_segments_with_zero_cross_tenant_rows() {
    let dir = tempfile::tempdir().expect("temp dir");
    let persist_dir = dir.path().to_str().expect("utf8 temp path");

    {
        let obs = ObsState::open(Some(persist_dir), 2).expect("open");
        let tenant_a_recs = vec![
            record("tenant-a", 10, "alpha-one"),
            record("tenant-a", 20, "alpha-two"),
        ];
        let tenant_b_recs = vec![
            record("tenant-b", 30, "bravo-one"),
            record("tenant-b", 40, "bravo-two"),
        ];
        let out_a = obs.ingest(tenant_a_recs).expect("ingest tenant-a");
        assert_eq!(out_a.segments_flushed, 1, "threshold=2 rolls one segment for tenant-a");
        let out_b = obs.ingest(tenant_b_recs).expect("ingest tenant-b");
        assert_eq!(out_b.segments_flushed, 1, "threshold=2 rolls one segment for tenant-b");

        assert_eq!(obs.segments_for("tenant-a").len(), 1, "tenant-a durably indexed pre-restart");
        assert_eq!(obs.segments_for("tenant-b").len(), 1, "tenant-b durably indexed pre-restart");
    }

    // "Restart on the same durable store": drop the handle entirely and reopen
    // against the identical persist_dir, exactly like BUG-210's own proof.
    let reopened = ObsState::open(Some(persist_dir), 2).expect("reopen");

    let segs_a = reopened.segments_for("tenant-a");
    let segs_b = reopened.segments_for("tenant-b");
    assert_eq!(segs_a.len(), 1, "tenant-a's manifest must survive the restart");
    assert_eq!(segs_b.len(), 1, "tenant-b's manifest must survive the restart");

    let rows_a = reopened.read_segment(&segs_a[0]).expect("read tenant-a segment");
    let rows_b = reopened.read_segment(&segs_b[0]).expect("read tenant-b segment");

    let bodies_a: Vec<&str> = rows_a.iter().map(|r| r.body.as_str()).collect();
    let bodies_b: Vec<&str> = rows_b.iter().map(|r| r.body.as_str()).collect();

    assert_eq!(rows_a.len(), 2, "tenant-a recovers exactly its own 2 rows");
    assert_eq!(rows_b.len(), 2, "tenant-b recovers exactly its own 2 rows");
    assert!(
        bodies_a.contains(&"alpha-one") && bodies_a.contains(&"alpha-two"),
        "tenant-a's recovered rows must be tenant-a's own bodies: {bodies_a:?}"
    );
    assert!(
        bodies_b.contains(&"bravo-one") && bodies_b.contains(&"bravo-two"),
        "tenant-b's recovered rows must be tenant-b's own bodies: {bodies_b:?}"
    );

    // The negative half, explicit: zero cross-tenant rows in EITHER direction.
    assert!(
        !bodies_a.iter().any(|b| b.starts_with("bravo")),
        "tenant-a's recovered segment must contain ZERO tenant-b rows, got {bodies_a:?}"
    );
    assert!(
        !bodies_b.iter().any(|b| b.starts_with("alpha")),
        "tenant-b's recovered segment must contain ZERO tenant-a rows, got {bodies_b:?}"
    );

    // Every row entry also self-reports the correct stream via its manifest
    // filter, not just via body content: `segments_for` itself is the isolation
    // boundary, so a segment recovered under the wrong stream key would already
    // have failed the length assertions above. Re-assert row_count agreement
    // between the manifest and the actually-read rows as a sanity check that
    // `read_segment` didn't silently truncate or over-read.
    assert_eq!(segs_a[0].row_count, rows_a.len());
    assert_eq!(segs_b[0].row_count, rows_b.len());
}
