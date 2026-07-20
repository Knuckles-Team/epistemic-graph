//! Native time-series handler (CONCEPT:AU-KG.retrieval.god-nodes-communities/211, feature `tsdb`).
//!
//! Owns the `Ts*` methods (`TsAppend`/`TsRange`/`TsAsofJoin`/`TsWindow`/`TsGapFill`)
//! — the one `// ── Time-series ──` protocol section. These are STATEFUL (they use
//! the `SeriesStore` on `ServerState`), so like the txn handler they take `state`.
//!
//! Series are keyed by the canonical `(tenant, graph, series_id)` identity in their
//! OWN `series.redb` file. Every call arrives through graph dispatch first, so the
//! caller's graph ACL is checked before any series bytes are touched. The redb
//! append/scan is off-reactor work, so each op runs on the
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
use crate::mutation_batch::{MutationDomain, MutationSurface};
use crate::protocol::{Method, Response, ResultPayload};
use crate::server::access::CarrierAuthority;

use eg_tsdb::point::Point;
use eg_tsdb::query::{asof_join_backward, gap_fill_locf, time_bucket, Agg};
use eg_tsdb::store::{SeriesKey, SeriesStore};

const MAX_POINTS_MSGPACK_BYTES: usize = 32 * 1024 * 1024;
const MAX_POINTS_MSGPACK_ITEMS: usize = 1_000_000;
const MAX_POINTS_PER_REQUEST: usize = 100_000;
const MAX_FIELDS_PER_POINT: usize = 4_096;
const MAX_VALUES_PER_REQUEST: usize = 1_000_000;

fn scoped_key(
    authority: &CarrierAuthority,
    graph: &str,
    series_id: &str,
) -> Result<SeriesKey, String> {
    // A series and every point in it are owned by the verified tenant+principal.
    // The caller-controlled `series_id` is only local inside that scope.
    Ok(SeriesKey::new(
        authority.tenant_scope(),
        authority.namespace("timeseries-graph", graph),
        series_id,
    ))
}

/// Decode and semantically bound the shared wire point blob. Transaction staging
/// reuses this exact validator so the direct and ACID paths cannot drift.
pub(crate) fn decode_wire_points(blob: &[u8]) -> Result<Vec<(i64, Vec<f64>)>, String> {
    let raw: Vec<(i64, Vec<f64>)> = eg_types::msgpack::decode_bounded(
        blob,
        eg_types::msgpack::MsgpackLimits::new(
            MAX_POINTS_MSGPACK_BYTES,
            MAX_POINTS_MSGPACK_ITEMS,
            64,
        ),
    )
    .map_err(|_| "invalid or over-complex points_msgpack".to_string())?;
    if raw.len() > MAX_POINTS_PER_REQUEST {
        return Err("time-series point count exceeds the resource limit".to_string());
    }
    let expected_width = raw.first().map(|(_, values)| values.len()).unwrap_or(0);
    if expected_width > MAX_FIELDS_PER_POINT {
        return Err("time-series field count exceeds the resource limit".to_string());
    }
    let mut total_values = 0usize;
    for (_, values) in &raw {
        total_values = total_values
            .checked_add(values.len())
            .ok_or_else(|| "time-series values exceed the resource limit".to_string())?;
        if values.len() != expected_width
            || total_values > MAX_VALUES_PER_REQUEST
            || values.iter().any(|value| !value.is_finite())
        {
            return Err("time-series points violate the input policy".to_string());
        }
    }
    Ok(raw)
}

/// Decode the wire point blob (`Vec<(i64, Vec<f64>)>`) into store points.
fn decode_points(blob: &[u8]) -> Result<Vec<Point>, String> {
    Ok(decode_wire_points(blob)?
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
    authority: &CarrierAuthority,
    graph: &str,
    placement_epoch: u64,
    fencing_token: Option<u64>,
    method: Method,
) -> Result<Response, Method> {
    let original_method = method.clone();
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
            let graph = graph.to_string();
            let scope = authority.namespace("ts-scope", &graph);
            let expected = match store.mutation_version(authority.tenant_scope(), &scope) {
                Ok(version) => version,
                Err(error) => {
                    return Ok(Response::err(
                        req_id,
                        format!("time-series MutationBatch version read failed: {error}"),
                    ))
                }
            };
            let batch_id = crate::server::mutation_batch::opaque_request_key(
                "timeseries",
                &scope,
                req_id,
                &original_method,
            );
            let now = crate::server::dispatch::authoritative_now_ms();
            let batch = match crate::server::mutation_batch::compile_opaque_method(
                crate::server::mutation_batch::CompileBatch {
                    batch_id: &batch_id,
                    request_id: req_id,
                    principal: Some(authority.actor_scope()),
                    tenant: authority.tenant_scope(),
                    graph: &scope,
                    placement_epoch,
                    idempotency_key: &batch_id,
                    expected_graph_version: Some(expected),
                    fencing_token,
                    created_at_ms: now,
                    default_surface: MutationSurface::Other,
                    authoritative_state: None,
                },
                &original_method,
                MutationSurface::Other,
                MutationDomain::TimeSeries,
                "timeseries_append",
            ) {
                Ok(batch) => batch,
                Err(error) => {
                    return Ok(Response::err(
                        req_id,
                        format!("time-series MutationBatch compile failed: {error}"),
                    ))
                }
            };
            let authority = authority.clone();
            let resp = match compute_off_lock(req_id, move || {
                let key = scoped_key(&authority, &graph, &series_id)?;
                store
                    .append_scoped_batch(
                        &key,
                        n_fields,
                        bucket_ns,
                        &field_names,
                        &points,
                        &batch,
                        now,
                    )
                    .map_err(|e| e.to_string())
            })
            .await
            {
                Ok(Ok(committed_n)) => Response::ok(req_id, ResultPayload::Count(committed_n)),
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
            let graph = graph.to_string();
            let authority = authority.clone();
            let resp = match compute_off_lock(req_id, move || {
                let key = scoped_key(&authority, &graph, &series_id)?;
                store
                    .range_scoped(&key, from, to)
                    .map_err(|e| e.to_string())
            })
            .await
            {
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
            let left_ts: Vec<i64> = match eg_types::msgpack::decode_bounded(
                &left_ts_msgpack,
                eg_types::msgpack::MsgpackLimits::new(
                    MAX_POINTS_MSGPACK_BYTES,
                    MAX_POINTS_MSGPACK_ITEMS,
                    64,
                ),
            ) {
                Ok(v) => v,
                Err(_) => {
                    return Ok(Response::err(
                        req_id,
                        "invalid or over-complex left timestamp payload",
                    ))
                }
            };
            if left_ts.len() > MAX_POINTS_PER_REQUEST {
                return Ok(Response::err(
                    req_id,
                    "left timestamp count exceeds the resource limit",
                ));
            }
            // `-1` over the wire encodes "no tolerance" (unbounded).
            let tol = if tolerance < 0 { None } else { Some(tolerance) };
            let graph = graph.to_string();
            let authority = authority.clone();
            let resp = match compute_off_lock(req_id, move || {
                let key = scoped_key(&authority, &graph, &series_id)?;
                let right = store.scan_all_scoped(&key).map_err(|e| e.to_string())?;
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
                Ok::<_, String>(out)
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
            let graph = graph.to_string();
            let authority = authority.clone();
            let resp = match compute_off_lock(req_id, move || {
                let key = scoped_key(&authority, &graph, &series_id)?;
                let pts = store
                    .range_scoped(&key, from, to)
                    .map_err(|e| e.to_string())?;
                let bars = time_bucket(&pts, width, agg);
                let wire: Vec<(i64, f64, usize)> = bars
                    .into_iter()
                    .map(|b| (b.bucket_start, b.value, b.count))
                    .collect();
                Ok::<_, String>(wire)
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
            let graph = graph.to_string();
            let authority = authority.clone();
            let resp = match compute_off_lock(req_id, move || {
                let key = scoped_key(&authority, &graph, &series_id)?;
                let pts = store
                    .range_scoped(&key, from, to)
                    .map_err(|e| e.to_string())?;
                let grid = gap_fill_locf(&pts, from, to, step);
                // (ts, value-or-NaN, filled-flag) — None encodes as NaN over the raw wire.
                let wire: Vec<(i64, f64, bool)> = grid
                    .into_iter()
                    .map(|g| (g.ts, g.value.unwrap_or(f64::NAN), g.filled))
                    .collect();
                Ok::<_, String>(wire)
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

#[cfg(test)]
mod nested_payload_tests {
    use super::*;

    #[test]
    fn point_decoder_rejects_bombs_ragged_rows_and_non_finite_values() {
        assert!(decode_wire_points(&[0xdd, 0xff, 0xff, 0xff, 0xff]).is_err());

        let ragged =
            rmp_serde::to_vec_named(&vec![(1_i64, vec![1.0_f64]), (2_i64, vec![1.0_f64, 2.0])])
                .unwrap();
        assert!(decode_wire_points(&ragged).is_err());

        let non_finite = rmp_serde::to_vec_named(&vec![(1_i64, vec![f64::INFINITY])]).unwrap();
        assert!(decode_wire_points(&non_finite).is_err());

        let valid = rmp_serde::to_vec_named(&vec![
            (1_i64, vec![1.0_f64, 2.0]),
            (2_i64, vec![3.0_f64, 4.0]),
        ])
        .unwrap();
        assert_eq!(decode_wire_points(&valid).unwrap().len(), 2);
    }

    #[cfg(feature = "security")]
    #[test]
    fn same_series_name_isolated_by_actor_and_tenant() {
        let alice = CarrierAuthority::from_verified(
            &crate::server::auth::VerifiedRequestContext::verified_for_test_in_tenant(
                "alice", "tenant-a",
            ),
        )
        .unwrap();
        let bob = CarrierAuthority::from_verified(
            &crate::server::auth::VerifiedRequestContext::verified_for_test_in_tenant(
                "bob", "tenant-a",
            ),
        )
        .unwrap();
        let cross_tenant = CarrierAuthority::from_verified(
            &crate::server::auth::VerifiedRequestContext::verified_for_test_in_tenant(
                "alice", "tenant-b",
            ),
        )
        .unwrap();
        let key = |authority: &CarrierAuthority| {
            SeriesKey::new(
                authority.tenant_scope(),
                authority.namespace("timeseries-graph", "shared"),
                "cpu",
            )
            .encode()
        };
        assert_ne!(key(&alice), key(&bob));
        assert_ne!(key(&alice), key(&cross_tenant));
    }
}
