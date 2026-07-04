//! Native time-series handler (CONCEPT:AU-KG.retrieval.god-nodes-communities/211, feature `tsdb`).
//!
//! Owns the `Ts*` methods (`TsAppend`/`TsRange`/`TsAsofJoin`/`TsWindow`/`TsGapFill`)
//! — the one `// ── Time-series ──` protocol section. These are STATEFUL (they use
//! the `SeriesStore` on `ServerState`), so like the txn handler they take `state`.
//!
//! Series are keyed by `series_id` in their OWN `series.redb` file, independent of
//! the graph registry + the per-graph write coalescer (a Ts op targets a series, not
//! a graph). The redb append/scan is off-reactor work, so each op runs on the
//! blocking pool via `compute_off_lock` (the Arc<SeriesStore> is cloned in, the brief
//! registry read-lock dropped first) — never under a tokio worker.
//!
//! Wire shapes (the protocol enum stays free of eg-tsdb types — it's at the bottom of
//! the DAG): points cross as MessagePack `Vec<(i64, Vec<f64>)>`; query results return
//! via `ResultPayload::raw` (the client double-unpacks), matching `Sql`/`Cypher`.

use std::sync::Arc;

use tokio::sync::RwLock;

use super::super::compute::compute_off_lock;
use super::super::state::ServerState;
use crate::protocol::{Method, Response, ResultPayload};

use eg_tsdb::point::Point;
use eg_tsdb::query::{asof_join_backward, gap_fill_locf, time_bucket, Agg};
use eg_tsdb::store::SeriesStore;

/// Decode the wire point blob (`Vec<(i64, Vec<f64>)>`) into store points.
fn decode_points(blob: &[u8]) -> Result<Vec<Point>, String> {
    let raw: Vec<(i64, Vec<f64>)> =
        rmp_serde::from_slice(blob).map_err(|e| format!("invalid points_msgpack: {e}"))?;
    Ok(raw
        .into_iter()
        .map(|(ts, values)| Point { ts, values })
        .collect())
}

fn parse_agg(s: &str) -> Result<Agg, String> {
    Ok(match s.to_ascii_lowercase().as_str() {
        "first" => Agg::First,
        "last" => Agg::Last,
        "min" => Agg::Min,
        "max" => Agg::Max,
        "mean" | "avg" => Agg::Mean,
        "sum" => Agg::Sum,
        "count" => Agg::Count,
        other => return Err(format!("unknown aggregate '{other}'")),
    })
}

/// Pull the configured series store, or an ERROR response if the engine booted
/// without one (only happens if a future build path leaves it `None`).
async fn store_of(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
) -> Result<Arc<SeriesStore>, Response> {
    let s = state.read().await;
    match &s.tsdb_store {
        Some(store) => Ok(store.clone()),
        None => Err(Response::err(req_id, "time-series store not configured")),
    }
}

/// Handle the `Ts*` methods. Returns `Err(method)` for any non-ts method so the
/// dispatch chain falls through (routing convention) — though dispatch only ever
/// routes Ts* methods here.
pub(crate) async fn try_handle(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    method: Method,
) -> Result<Response, Method> {
    match method {
        Method::TsAppend {
            series_id,
            n_fields,
            bucket_ns,
            field_names,
            points_msgpack,
        } => {
            let store = match store_of(state, req_id).await {
                Ok(s) => s,
                Err(r) => return Ok(r),
            };
            let points = match decode_points(&points_msgpack) {
                Ok(p) => p,
                Err(e) => return Ok(Response::err(req_id, e)),
            };
            let n = points.len() as u64;
            let resp = match compute_off_lock(req_id, move || {
                store.append_batch(&series_id, n_fields, bucket_ns, &field_names, &points)
            })
            .await
            {
                Ok(Ok(())) => Response::ok(req_id, ResultPayload::Count(n)),
                Ok(Err(e)) => Response::err(req_id, e.to_string()),
                Err(resp) => resp,
            };
            Ok(resp)
        }

        Method::TsRange {
            series_id,
            from,
            to,
        } => {
            let store = match store_of(state, req_id).await {
                Ok(s) => s,
                Err(r) => return Ok(r),
            };
            let resp =
                match compute_off_lock(req_id, move || store.range(&series_id, from, to)).await {
                    Ok(Ok(points)) => {
                        let wire: Vec<(i64, Vec<f64>)> =
                            points.into_iter().map(|p| (p.ts, p.values)).collect();
                        Response::ok(req_id, ResultPayload::raw(&wire))
                    }
                    Ok(Err(e)) => Response::err(req_id, e.to_string()),
                    Err(resp) => resp,
                };
            Ok(resp)
        }

        Method::TsAsofJoin {
            series_id,
            left_ts_msgpack,
            tolerance,
        } => {
            let store = match store_of(state, req_id).await {
                Ok(s) => s,
                Err(r) => return Ok(r),
            };
            let left_ts: Vec<i64> = match rmp_serde::from_slice(&left_ts_msgpack) {
                Ok(v) => v,
                Err(e) => {
                    return Ok(Response::err(
                        req_id,
                        format!("invalid left_ts_msgpack: {e}"),
                    ))
                }
            };
            // `-1` over the wire encodes "no tolerance" (unbounded).
            let tol = if tolerance < 0 { None } else { Some(tolerance) };
            let resp = match compute_off_lock(req_id, move || {
                let right = store.scan_all(&series_id)?;
                // Sort left events ascending so the O(L+R) merge holds, but return
                // results in the CALLER's input order (a stable join surface).
                let mut order: Vec<usize> = (0..left_ts.len()).collect();
                order.sort_by_key(|&i| left_ts[i]);
                let left: Vec<Point> = order
                    .iter()
                    .map(|&i| Point::single(left_ts[i], 0.0))
                    .collect();
                let joined = asof_join_backward(&left, &right, tol);
                // Re-key by original index: out[orig_i] = matched value (or None).
                let mut out: Vec<Option<f64>> = vec![None; left_ts.len()];
                for (slot, &orig_i) in order.iter().enumerate() {
                    out[orig_i] = joined[slot].right;
                }
                Ok::<_, eg_tsdb::point::TsError>(out)
            })
            .await
            {
                Ok(Ok(out)) => Response::ok(req_id, ResultPayload::raw(&out)),
                Ok(Err(e)) => Response::err(req_id, e.to_string()),
                Err(resp) => resp,
            };
            Ok(resp)
        }

        Method::TsWindow {
            series_id,
            from,
            to,
            width,
            agg,
        } => {
            let store = match store_of(state, req_id).await {
                Ok(s) => s,
                Err(r) => return Ok(r),
            };
            let agg = match parse_agg(&agg) {
                Ok(a) => a,
                Err(e) => return Ok(Response::err(req_id, e)),
            };
            let resp = match compute_off_lock(req_id, move || {
                let pts = store.range(&series_id, from, to)?;
                let bars = time_bucket(&pts, width, agg);
                let wire: Vec<(i64, f64, usize)> = bars
                    .into_iter()
                    .map(|b| (b.bucket_start, b.value, b.count))
                    .collect();
                Ok::<_, eg_tsdb::point::TsError>(wire)
            })
            .await
            {
                Ok(Ok(wire)) => Response::ok(req_id, ResultPayload::raw(&wire)),
                Ok(Err(e)) => Response::err(req_id, e.to_string()),
                Err(resp) => resp,
            };
            Ok(resp)
        }

        Method::TsGapFill {
            series_id,
            from,
            to,
            step,
        } => {
            let store = match store_of(state, req_id).await {
                Ok(s) => s,
                Err(r) => return Ok(r),
            };
            let resp = match compute_off_lock(req_id, move || {
                let pts = store.range(&series_id, from, to)?;
                let grid = gap_fill_locf(&pts, from, to, step);
                // (ts, value-or-NaN, filled-flag) — None encodes as NaN over the raw wire.
                let wire: Vec<(i64, f64, bool)> = grid
                    .into_iter()
                    .map(|g| (g.ts, g.value.unwrap_or(f64::NAN), g.filled))
                    .collect();
                Ok::<_, eg_tsdb::point::TsError>(wire)
            })
            .await
            {
                Ok(Ok(wire)) => Response::ok(req_id, ResultPayload::raw(&wire)),
                Ok(Err(e)) => Response::err(req_id, e.to_string()),
                Err(resp) => resp,
            };
            Ok(resp)
        }

        other => Err(other),
    }
}
