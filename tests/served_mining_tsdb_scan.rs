//! PLAN-SOURCED MINING `TsScan` (CONCEPT:EG-KG.mining.tsdb-typed-absent, W0.6) — proof that
//! a `graph_mine` plan containing an `Op::TsScan` leg reads the REAL committed series store
//! against a LIVE in-process server, instead of silently degrading to an empty result.
//!
//! Before this fix, `handlers::mining::gather_plan_rows` ALWAYS bound `TsdbLegBind { tsdb:
//! None, .. }` regardless of what the server had configured — a `TsScan`-bearing mining plan
//! therefore always yielded zero rows, indistinguishable from "your query legitimately
//! matched nothing". Two behaviors are proven here, against the SAME served RPC surface
//! (`Box::pin(dispatch(state, Request{ Method::* }))`) `served_query_completeness.rs` /
//! `served_tensor_writeback.rs` use:
//!
//!  1. With a REAL tsdb store configured and series data seeded through the served
//!     `Method::TsAppend` write path, a `Method::MineAnomaly` whose `plan` is a bare
//!     `Op::TsScan` returns every seeded point as a real feature row (and flags the seeded
//!     outlier) — not the old silent empty.
//!  2. The IDENTICAL plan against a server with NO tsdb store configured is now a TYPED
//!     error, never a silent-empty success — the old failure mode this closes.
//!
//! Module-gated on `mining` + `query` + `tsdb`; runs under `--features full`.
#![cfg(all(feature = "mining", feature = "query", feature = "tsdb"))]

mod common;
#[path = "common/test_support.rs"]
mod test_support;

use std::sync::Arc;

use eg_plan::{Op, Plan};
use eg_tsdb::store::SeriesStore;
use epistemic_graph::protocol::{AnomalyAlgorithm, Method, Response, ResultPayload, SvmKernel};

const SECRET: &str = "served-mining-tsdb-scan-secret";

/// A fresh, temp-file-backed `SeriesStore` — real durable storage, not an in-memory stub.
fn tmp_series() -> Arc<SeriesStore> {
    test_support::temporary_series("mining-tsdb-scan")
}

fn state(tsdb_store: Option<Arc<SeriesStore>>) -> test_support::SharedState {
    let (persist_dir, persistence) = common::tempdir_persistence();
    test_support::state_with_tsdb(
        SECRET,
        common::current_isolation(),
        persist_dir,
        persistence,
        tsdb_store,
    )
}

fn pack_points(points: &[(i64, Vec<f64>)]) -> Vec<u8> {
    rmp_serde::to_vec_named(&points.to_vec()).unwrap()
}

fn req(id: u64, method: Method) -> epistemic_graph::protocol::Request {
    test_support::commons_request(SECRET, id, method)
}

async fn dispatch(
    state: &test_support::SharedState,
    request: epistemic_graph::protocol::Request,
) -> Response {
    test_support::dispatch(state, request).await
}

/// Seed one series (`cpu`) with 5 points — 4 unremarkable and one clear outlier — through
/// the SERVED `Method::TsAppend` write path (the same MutationBatch-compiled path a real
/// client uses), so the read side below exercises genuine end-to-end wiring rather than a
/// backdoor write straight into the store.
async fn seed_series(state: &test_support::SharedState) {
    let points: Vec<(i64, Vec<f64>)> = vec![
        (1_000_000_000, vec![10.0]),
        (2_000_000_000, vec![11.0]),
        (3_000_000_000, vec![9.0]),
        (4_000_000_000, vec![10.5]),
        (5_000_000_000, vec![100.0]), // the outlier
    ];
    let r = Box::pin(dispatch(
        state,
        req(
            1,
            Method::TsAppend {
                series_id: "cpu".into(),
                n_fields: 1,
                bucket_ns: 60_000_000_000,
                field_names: vec!["value".into()],
                points_msgpack: pack_points(&points),
            },
        ),
    ))
    .await;
    assert!(r.error.is_none(), "TsAppend seed failed: {:?}", r.error);
}

/// A bare `TsScan` over the `cpu` series covering all 5 seeded points (ts in `[0, 10)` s).
fn ts_scan_plan() -> Plan {
    Plan::new(vec![Op::TsScan {
        series: vec!["cpu".into()],
        from: 0.0,
        to: 10.0,
    }])
}

/// `MineAnomaly` sourced entirely from `plan` (no explicit `features`/`values`/`source`),
/// read-only (`writeback: false`) so the test needs no durable graph-mutation backend.
fn mine_anomaly_over(plan: Plan) -> Method {
    Method::MineAnomaly {
        features: vec![],
        values: vec![],
        source: None,
        plan: Some(plan),
        algorithm: AnomalyAlgorithm::Zscore,
        k: 0,
        n_trees: 0,
        sample_size: 0,
        seed: 0,
        nu: 0.5,
        gamma: 0.0,
        kernel: SvmKernel::Linear,
        threshold: None,
        writeback: false,
        #[cfg(feature = "epistemic")]
        as_claim: false,
    }
}

fn json_result(resp: &Response) -> serde_json::Value {
    assert!(resp.error.is_none(), "dispatch error: {:?}", resp.error);
    match &resp.result {
        Some(ResultPayload::Json(v)) => v.clone(),
        other => panic!("expected Json result, got {other:?}"),
    }
}

/// CONCEPT:EG-KG.mining.tsdb-typed-absent — a served, plan-sourced `MineAnomaly` whose plan
/// is `Op::TsScan` returns every seeded point as a real feature row, and correctly flags the
/// seeded outlier, once a live tsdb store is bound.
#[tokio::test]
async fn plan_sourced_mining_tsscan_returns_real_rows() {
    let state = state(Some(tmp_series()));
    seed_series(&state).await;

    let resp = Box::pin(dispatch(&state, req(2, mine_anomaly_over(ts_scan_plan())))).await;
    let payload = json_result(&resp);
    let n_rows = payload["n_rows"].as_u64().unwrap_or(0);
    assert_eq!(
        n_rows, 5,
        "all 5 seeded tsdb points must come back as real feature rows: {payload:?}"
    );
    let rows = payload["rows"].as_array().expect("rows array");
    assert!(
        rows.iter().any(|r| r["is_anomaly"].as_bool() == Some(true)),
        "the seeded 100.0 outlier must be flagged anomalous: {payload:?}"
    );
}

/// CONCEPT:EG-KG.mining.tsdb-typed-absent — running the IDENTICAL plan-sourced mining call
/// a second time (same server, same store) proves the result isn't a one-shot fluke: the
/// binding is a real, stable server-lifetime wire, not a fresh store that happens to work once.
#[tokio::test]
async fn plan_sourced_mining_tsscan_repeatable_across_requests() {
    let state = state(Some(tmp_series()));
    seed_series(&state).await;

    for req_id in [10, 11] {
        let resp = Box::pin(dispatch(
            &state,
            req(req_id, mine_anomaly_over(ts_scan_plan())),
        ))
        .await;
        let payload = json_result(&resp);
        assert_eq!(
            payload["n_rows"].as_u64().unwrap_or(0),
            5,
            "request {req_id} must see all 5 points: {payload:?}"
        );
    }
}

/// CONCEPT:EG-KG.mining.tsdb-typed-absent — the SAME `TsScan`-bearing plan against a server
/// with NO tsdb store configured is a TYPED ERROR, never the old silent-empty success. This
/// is the regression the fix specifically closes: before it, `gather_plan_rows` unconditionally
/// bound `TsdbLegBind { tsdb: None, .. }`, so this exact call would have returned `n_rows: 0`
/// with NO error — indistinguishable from "the series legitimately has no points in range".
#[tokio::test]
async fn plan_sourced_mining_tsscan_errors_typed_when_store_absent() {
    let state = state(None);

    let resp = Box::pin(dispatch(&state, req(1, mine_anomaly_over(ts_scan_plan())))).await;
    assert!(
        resp.error.is_some(),
        "a TsScan-bearing mining plan with no tsdb store configured must surface a typed \
         error, not a silent-empty success: result={:?}",
        resp.result
    );
    let msg = resp.error.unwrap();
    assert!(
        msg.contains("TsScan") || msg.contains("time-series") || msg.contains("tsdb"),
        "the error should name the actual gap (no tsdb store), not a generic failure: {msg}"
    );
}

/// A plan that does NOT touch tsdb at all is completely unaffected by any of this — the
/// `MiningTsdbBind` plumbing must be a no-op for the common, non-timeseries mining path.
#[tokio::test]
async fn plan_sourced_mining_without_tsscan_is_unaffected_by_absent_store() {
    let state = state(None);
    let r = Box::pin(dispatch(
        &state,
        req(
            1,
            Method::AddNode {
                node_id: "n1".into(),
                properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({
                    "type": "Metric",
                }))
                .unwrap(),
            },
        ),
    ))
    .await;
    assert!(r.error.is_none(), "AddNode: {:?}", r.error);
    let r = Box::pin(dispatch(
        &state,
        req(
            2,
            Method::AddEmbedding {
                node_id: "n1".into(),
                embedding: vec![1.0, 0.0],
            },
        ),
    ))
    .await;
    assert!(r.error.is_none(), "AddEmbedding: {:?}", r.error);

    let plan = Plan::new(vec![Op::Scan {
        label: "Metric".into(),
    }]);
    let resp = Box::pin(dispatch(&state, req(3, mine_anomaly_over(plan)))).await;
    let payload = json_result(&resp);
    assert_eq!(
        payload["n_rows"].as_u64().unwrap_or(999),
        1,
        "a non-tsdb plan must still resolve normally with no tsdb store configured: {payload:?}"
    );
}
