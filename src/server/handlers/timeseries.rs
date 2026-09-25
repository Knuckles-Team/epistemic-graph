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
//!
//! ## Why this file never calls `GraphReadAuthority::filter_view`/`project_core`
//!
//! Unlike `graph_ops.rs`/`rdf.rs`/`query.rs`, no method here ever constructs a
//! `GraphView`/`GraphCore` at all, so the per-node `_owner`/`_visibility`/
//! `_grants` row-level primitive (`crates/eg-core/src/isolation.rs::can_see_row`)
//! has nothing to filter. Every `Ts*` method instead derives its `SeriesKey`
//! from [`scoped_key`], which namespaces it under the VERIFIED caller's own
//! `CarrierAuthority` (`tenant_scope` + an owner-scoped `namespace`) — never
//! from a caller-supplied tenant/owner. Two different actors (or the same
//! actor in two different tenants) addressing the "same" `graph`/`series_id`
//! therefore compute two DIFFERENT, non-colliding keys: cross-actor addressing
//! is structurally impossible, not merely hidden-unless-granted. This is
//! stronger than default-deny RLS in one respect (there is no way to guess or
//! collide into another actor's series at all) but does not support explicit
//! `_visibility:public`/`_grants` SHARING the way graph-row RLS does — series
//! data has no such use case today. See
//! `server::access::read_rls_coverage_tests::every_read_method_routes_through_rls_or_is_a_documented_exception`
//! for the machine-checked inventory this reasoning is pinned against (the
//! `TsRange`/`TsAsofJoin`/`TsWindow`/`TsGapFill` entries there cite this exact
//! comment) — a future method here that DOES read graph node/edge data must
//! bind a `GraphReadAuthority` and route through `filter_view`/`project_core`
//! like every other graph-row read, not extend `scoped_key`'s reasoning to
//! data it wasn't designed for.
//!
//! Two tests back this reasoning: `same_series_name_isolated_by_actor_and_tenant`
//! proves the derived `SeriesKey`s never collide across actor/tenant, and
//! `cross_actor_and_cross_tenant_reads_see_no_points_through_the_real_store`
//! goes further — it appends through the same `MutationBatch`-compiled path
//! `TsAppend` uses and reads back through the same `range_scoped`/
//! `scan_all_scoped` primitives every `Ts*` read method calls, proving the
//! actual leak-equivalence-to-RLS behavior end to end against a real store,
//! not just that the keys differ.

use std::sync::Arc;

use tokio::sync::RwLock;

use super::super::compute::compute_off_lock;
use super::super::state::ServerState;
use crate::mutation_batch::{DurabilityDomain, MutationBatch, MutationSurface};
use crate::protocol::{Method, Response, ResultPayload};
use crate::server::access::CarrierAuthority;

use eg_tsdb::point::Point;
use eg_tsdb::query::{asof_join_backward, gap_fill_locf, time_bucket, Agg};
use eg_tsdb::store::{ScopedAppendBatch, SeriesKey, SeriesStore};
use eg_types::contract::Nonce;
use eg_types::result_contract::storage as results;

// EH-524 — materialised derived series: `TsDefineSeries` and the maintenance a
// `TsAppend` runs on the series derived from the appended one.
mod derived;

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
    try_handle_with_nonce(
        state,
        req_id,
        authority,
        None,
        SeriesPlacement {
            graph,
            placement_epoch,
            fencing_token,
        },
        method,
    )
    .await
}

/// Where a time-series request lands, and the fencing it lands under.  The
/// router resolves all three as one routing decision and they are only valid
/// together: the graph name namespaces the series scope, while the placement
/// epoch and fencing token are what a stale route is rejected by -- committing
/// under the right graph but a stale epoch is exactly the split-brain write the
/// pair exists to stop, so no caller gets to supply one without the other two.
pub(crate) struct SeriesPlacement<'a> {
    pub(crate) graph: &'a str,
    pub(crate) placement_epoch: u64,
    /// `None` when the route carries no fencing token (single-node / non-raft).
    pub(crate) fencing_token: Option<u64>,
}

struct TsRequestContext<'a> {
    state: &'a Arc<RwLock<ServerState>>,
    req_id: u64,
    authority: &'a CarrierAuthority,
    attempt_nonce: Option<Nonce>,
    graph: &'a str,
    placement_epoch: u64,
    fencing_token: Option<u64>,
    original_method: &'a Method,
}

struct PreparedAppend {
    store: Arc<SeriesStore>,
    points: Vec<Point>,
    graph: String,
    batch: MutationBatch,
    committed_at_ms: u64,
}

async fn prepare_append(
    context: &TsRequestContext<'_>,
    points_msgpack: &[u8],
) -> Result<PreparedAppend, Response> {
    let store = match store_of(context.state, context.req_id).await {
        Ok(store) => store,
        Err(response) => return Err(response),
    };
    let points = match decode_points(points_msgpack) {
        Ok(points) => points,
        Err(error) => return Err(Response::err(context.req_id, error)),
    };
    let committed_at_ms = crate::server::dispatch::authoritative_now_ms();
    let batch = context
        .series_batch(&store, context.original_method, committed_at_ms)
        .map_err(|error| Response::err(context.req_id, error))?;
    Ok(PreparedAppend {
        store,
        points,
        graph: context.graph.to_string(),
        batch,
        committed_at_ms,
    })
}

/// What a series batch commits under: the scope, the fencing and the request identity.
/// Plain data, so the blocking maintenance work (EH-524) can carry it off the reactor.
#[derive(Clone)]
pub(super) struct BatchScope {
    authority: CarrierAuthority,
    graph: String,
    req_id: u64,
    attempt_nonce: Option<Nonce>,
    placement_epoch: u64,
    fencing_token: Option<u64>,
}

impl BatchScope {
    /// Compile the one native MutationBatch a series write commits under, keyed by the
    /// request and `method` (the method digest separates several writes of one request).
    pub(super) fn series_batch(
        &self,
        store: &SeriesStore,
        method: &Method,
        committed_at_ms: u64,
    ) -> Result<MutationBatch, String> {
        let scope = self.authority.namespace("ts-scope", &self.graph);
        let expected = store
            .mutation_version(self.authority.tenant_scope(), &scope)
            .map_err(|error| format!("time-series MutationBatch version read failed: {error}"))?;
        let batch_id = crate::server::mutation_batch::opaque_request_key(
            "timeseries",
            &scope,
            self.req_id,
            method,
        );
        crate::server::mutation_batch::compile_opaque_method(
            crate::server::mutation_batch::CompileBatch {
                batch_id: &batch_id,
                request_id: self.req_id,
                attempt_nonce: self.attempt_nonce,
                principal: Some(self.authority.actor_scope()),
                tenant: self.authority.tenant_scope(),
                graph: &scope,
                placement_epoch: self.placement_epoch,
                idempotency_key: &batch_id,
                expected_graph_version: Some(expected),
                fencing_token: self.fencing_token,
                created_at_ms: committed_at_ms,
                default_surface: MutationSurface::Other,
                authoritative_state: None,
            },
            method,
            MutationSurface::Other,
            DurabilityDomain::TimeSeries,
            "timeseries_append",
        )
        .map_err(|error| format!("time-series MutationBatch compile failed: {error}"))
    }

    /// The scope a follow-on write of this request commits under (EH-524: a derived
    /// series' maintenance after the request's own append). The request's attempt
    /// nonce is consumed by its own batch, so each follow-on write takes a nonce
    /// derived from it and from `event`, the follow-on batch's identity: a retry of
    /// the request derives the same nonce and replays, and two follow-on writes of
    /// one request never share a nonce.
    pub(super) fn follow_on(&self, event: &str) -> Result<BatchScope, String> {
        let attempt_nonce = self
            .attempt_nonce
            .map(|parent| {
                eg_types::contract::Digest256::framed(
                    b"eg/timeseries-follow-on-nonce/v1",
                    &[parent.as_bytes(), event.as_bytes()],
                )
                .map(|digest| Nonce::from_bytes(*digest.as_bytes()))
            })
            .transpose()?;
        Ok(BatchScope {
            attempt_nonce,
            ..self.clone()
        })
    }

    /// The canonical key of `series_id` in this scope.
    pub(super) fn key(&self, series_id: &str) -> Result<SeriesKey, String> {
        scoped_key(&self.authority, &self.graph, series_id)
    }

    /// Every series key of this scope.
    pub(super) fn keys(&self, store: &SeriesStore) -> Result<Vec<SeriesKey>, String> {
        let tenant = self.authority.tenant_scope();
        let graph = self.authority.namespace("timeseries-graph", &self.graph);
        let all = store.list_series().map_err(|error| error.to_string())?;
        let mut keys: Vec<SeriesKey> = all
            .into_iter()
            .filter_map(|encoded| SeriesKey::decode(&encoded))
            .filter(|key| key.tenant == tenant && key.graph == graph)
            .collect();
        keys.sort_unstable_by(|a, b| a.series.cmp(&b.series));
        Ok(keys)
    }
}

impl TsRequestContext<'_> {
    fn batch_scope(&self) -> BatchScope {
        BatchScope {
            authority: self.authority.clone(),
            graph: self.graph.to_string(),
            req_id: self.req_id,
            attempt_nonce: self.attempt_nonce,
            placement_epoch: self.placement_epoch,
            fencing_token: self.fencing_token,
        }
    }

    fn series_batch(
        &self,
        store: &SeriesStore,
        method: &Method,
        committed_at_ms: u64,
    ) -> Result<MutationBatch, String> {
        self.batch_scope()
            .series_batch(store, method, committed_at_ms)
    }
}

async fn handle_append(
    context: &TsRequestContext<'_>,
    series_id: String,
    n_fields: usize,
    bucket_ns: u64,
    field_names: Vec<String>,
    points_msgpack: Vec<u8>,
) -> Response {
    let prepared = match prepare_append(context, &points_msgpack).await {
        Ok(prepared) => prepared,
        Err(response) => return response,
    };
    let min_ts = prepared.points.iter().map(|p| p.ts).min();
    let source = series_id.clone();
    let authority = context.authority.clone();
    let appended = compute_off_lock(context.req_id, move || {
        let key = scoped_key(&authority, &prepared.graph, &series_id)?;
        prepared
            .store
            .append_scoped_batch(
                &key,
                ScopedAppendBatch {
                    n_fields,
                    bucket_ns,
                    field_names: &field_names,
                    points: &prepared.points,
                    batch: &prepared.batch,
                    committed_at_ms: prepared.committed_at_ms,
                    derived: None,
                },
            )
            .map_err(|error| error.to_string())
    })
    .await;
    if matches!(appended, Ok(Ok(_))) {
        // EH-524: the series derived from this one advance from their checkpoints.
        derived::maintain_after_append(context, source, min_ts).await;
    }
    match appended {
        Ok(Ok(committed_n)) => Response::ok(
            context.req_id,
            ResultPayload::scalar::<results::TsAppend>(committed_n),
        ),
        Ok(Err(error)) => Response::err(context.req_id, error.to_string()),
        Err(response) => response,
    }
}

async fn handle_range(
    context: &TsRequestContext<'_>,
    series_id: String,
    from: i64,
    to: i64,
) -> Response {
    let store = match store_of(context.state, context.req_id).await {
        Ok(store) => store,
        Err(response) => return response,
    };
    let graph = context.graph.to_string();
    let authority = context.authority.clone();
    match compute_off_lock(context.req_id, move || {
        let key = scoped_key(&authority, &graph, &series_id)?;
        store
            .range_scoped(&key, from, to)
            .map_err(|error| error.to_string())
    })
    .await
    {
        Ok(Ok(points)) => {
            let wire: Vec<(i64, Vec<f64>)> = points
                .into_iter()
                .map(|point| (point.ts, point.values))
                .collect();
            Response::ok(
                context.req_id,
                ResultPayload::of_ref::<results::TsRange>(&wire),
            )
        }
        Ok(Err(error)) => Response::err(context.req_id, error.to_string()),
        Err(response) => response,
    }
}

fn decode_left_timestamps(blob: &[u8]) -> Result<Vec<i64>, &'static str> {
    let values: Vec<i64> = eg_types::msgpack::decode_bounded(
        blob,
        eg_types::msgpack::MsgpackLimits::new(
            MAX_POINTS_MSGPACK_BYTES,
            MAX_POINTS_MSGPACK_ITEMS,
            64,
        ),
    )
    .map_err(|_| "invalid or over-complex left timestamp payload")?;
    if values.len() > MAX_POINTS_PER_REQUEST {
        return Err("left timestamp count exceeds the resource limit");
    }
    Ok(values)
}

async fn handle_asof_join(
    context: &TsRequestContext<'_>,
    series_id: String,
    left_ts_msgpack: Vec<u8>,
    tolerance: i64,
) -> Response {
    let store = match store_of(context.state, context.req_id).await {
        Ok(store) => store,
        Err(response) => return response,
    };
    let left_ts = match decode_left_timestamps(&left_ts_msgpack) {
        Ok(values) => values,
        Err(error) => return Response::err(context.req_id, error),
    };
    // `-1` over the wire encodes "no tolerance" (unbounded).
    let tolerance = if tolerance < 0 { None } else { Some(tolerance) };
    let graph = context.graph.to_string();
    let authority = context.authority.clone();
    match compute_off_lock(context.req_id, move || {
        let key = scoped_key(&authority, &graph, &series_id)?;
        let right = store
            .scan_all_scoped(&key)
            .map_err(|error| error.to_string())?;
        // Sort left events ascending so the O(L+R) merge holds, but return
        // results in the CALLER's input order (a stable join surface).
        let mut order: Vec<usize> = (0..left_ts.len()).collect();
        order.sort_by_key(|&index| left_ts[index]);
        let left: Vec<Point> = order
            .iter()
            .map(|&index| Point::single(left_ts[index], 0.0))
            .collect();
        let joined = asof_join_backward(&left, &right, tolerance);
        // Re-key by original index: out[orig_i] = matched value (or None).
        let mut out: Vec<Option<f64>> = vec![None; left_ts.len()];
        for (slot, &orig_i) in order.iter().enumerate() {
            out[orig_i] = joined[slot].right;
        }
        Ok::<_, String>(out)
    })
    .await
    {
        Ok(Ok(out)) => Response::ok(
            context.req_id,
            ResultPayload::of_ref::<results::TsAsofJoin>(&out),
        ),
        Ok(Err(error)) => Response::err(context.req_id, error.to_string()),
        Err(response) => response,
    }
}

async fn handle_window(
    context: &TsRequestContext<'_>,
    series_id: String,
    from: i64,
    to: i64,
    width: i64,
    agg: String,
) -> Response {
    let store = match store_of(context.state, context.req_id).await {
        Ok(store) => store,
        Err(response) => return response,
    };
    let agg = match parse_agg(&agg) {
        Ok(agg) => agg,
        Err(error) => return Response::err(context.req_id, error),
    };
    let graph = context.graph.to_string();
    let authority = context.authority.clone();
    match compute_off_lock(context.req_id, move || {
        let key = scoped_key(&authority, &graph, &series_id)?;
        let points = store
            .range_scoped(&key, from, to)
            .map_err(|error| error.to_string())?;
        let bars = time_bucket(&points, width, agg);
        let wire: Vec<(i64, f64, usize)> = bars
            .into_iter()
            .map(|bar| (bar.bucket_start, bar.value, bar.count))
            .collect();
        Ok::<_, String>(wire)
    })
    .await
    {
        Ok(Ok(wire)) => Response::ok(
            context.req_id,
            ResultPayload::of_ref::<results::TsWindow>(&wire),
        ),
        Ok(Err(error)) => Response::err(context.req_id, error.to_string()),
        Err(response) => response,
    }
}

async fn handle_gap_fill(
    context: &TsRequestContext<'_>,
    series_id: String,
    from: i64,
    to: i64,
    step: i64,
) -> Response {
    let store = match store_of(context.state, context.req_id).await {
        Ok(store) => store,
        Err(response) => return response,
    };
    let graph = context.graph.to_string();
    let authority = context.authority.clone();
    match compute_off_lock(context.req_id, move || {
        let key = scoped_key(&authority, &graph, &series_id)?;
        let points = store
            .range_scoped(&key, from, to)
            .map_err(|error| error.to_string())?;
        let grid = gap_fill_locf(&points, from, to, step);
        // (ts, value-or-NaN, filled-flag) — None encodes as NaN over the raw wire.
        let wire: Vec<(i64, f64, bool)> = grid
            .into_iter()
            .map(|point| (point.ts, point.value.unwrap_or(f64::NAN), point.filled))
            .collect();
        Ok::<_, String>(wire)
    })
    .await
    {
        Ok(Ok(wire)) => Response::ok(
            context.req_id,
            ResultPayload::of_ref::<results::TsGapFill>(&wire),
        ),
        Ok(Err(error)) => Response::err(context.req_id, error.to_string()),
        Err(response) => response,
    }
}

async fn handle_evict(context: &TsRequestContext<'_>, series_id: String, cutoff: i64) -> Response {
    let store = match store_of(context.state, context.req_id).await {
        Ok(store) => store,
        Err(response) => return response,
    };
    let graph = context.graph.to_string();
    let authority = context.authority.clone();
    match compute_off_lock(context.req_id, move || {
        let key = scoped_key(&authority, &graph, &series_id)?;
        store
            .evict_before_scoped(&key, cutoff)
            .map_err(|error| error.to_string())
    })
    .await
    {
        Ok(Ok(dropped)) => Response::ok(
            context.req_id,
            ResultPayload::scalar::<results::TsEvict>(dropped as u64),
        ),
        Ok(Err(error)) => Response::err(context.req_id, error.to_string()),
        Err(response) => response,
    }
}

async fn handle_delete_series(context: &TsRequestContext<'_>, series_id: String) -> Response {
    let store = match store_of(context.state, context.req_id).await {
        Ok(store) => store,
        Err(response) => return response,
    };
    let graph = context.graph.to_string();
    let authority = context.authority.clone();
    match compute_off_lock(context.req_id, move || {
        let key = scoped_key(&authority, &graph, &series_id)?;
        store.delete_scoped(&key).map_err(|error| error.to_string())
    })
    .await
    {
        Ok(Ok(dropped)) => Response::ok(
            context.req_id,
            ResultPayload::scalar::<results::TsDeleteSeries>(dropped as u64),
        ),
        Ok(Err(error)) => Response::err(context.req_id, error.to_string()),
        Err(response) => response,
    }
}

async fn handle_list_series(context: &TsRequestContext<'_>) -> Response {
    let store = match store_of(context.state, context.req_id).await {
        Ok(store) => store,
        Err(response) => return response,
    };
    let scope = context.batch_scope();
    match compute_off_lock(context.req_id, move || {
        let keys = scope.keys(&store)?;
        Ok::<_, String>(keys.into_iter().map(|key| key.series).collect::<Vec<_>>())
    })
    .await
    {
        Ok(Ok(series_ids)) => Response::ok(
            context.req_id,
            ResultPayload::of_ref::<results::TsListSeries>(&series_ids),
        ),
        Ok(Err(error)) => Response::err(context.req_id, error.to_string()),
        Err(response) => response,
    }
}

pub(crate) async fn try_handle_with_nonce(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    authority: &CarrierAuthority,
    attempt_nonce: Option<Nonce>,
    placement: SeriesPlacement<'_>,
    method: Method,
) -> Result<Response, Method> {
    let SeriesPlacement {
        graph,
        placement_epoch,
        fencing_token,
    } = placement;
    let original_method = method.clone();
    let context = TsRequestContext {
        state,
        req_id,
        authority,
        attempt_nonce,
        graph,
        placement_epoch,
        fencing_token,
        original_method: &original_method,
    };
    match method {
        Method::TsAppend {
            series_id,
            n_fields,
            bucket_ns,
            field_names,
            points_msgpack,
        } => Ok(handle_append(
            &context,
            series_id,
            n_fields,
            bucket_ns,
            field_names,
            points_msgpack,
        )
        .await),
        Method::TsRange {
            series_id,
            from,
            to,
        } => Ok(handle_range(&context, series_id, from, to).await),
        Method::TsAsofJoin {
            series_id,
            left_ts_msgpack,
            tolerance,
        } => Ok(handle_asof_join(&context, series_id, left_ts_msgpack, tolerance).await),
        Method::TsWindow {
            series_id,
            from,
            to,
            width,
            agg,
        } => Ok(handle_window(&context, series_id, from, to, width, agg).await),
        Method::TsGapFill {
            series_id,
            from,
            to,
            step,
        } => Ok(handle_gap_fill(&context, series_id, from, to, step).await),
        // Retention is content-idempotent, so these use the store's direct durable
        // operations rather than the MutationBatch append path.
        Method::TsEvict { series_id, cutoff } => {
            Ok(handle_evict(&context, series_id, cutoff).await)
        }
        Method::TsDeleteSeries { series_id } => Ok(handle_delete_series(&context, series_id).await),
        // Enumeration is scoped to the caller's verified tenant and graph.
        Method::TsListSeries => Ok(handle_list_series(&context).await),
        Method::TsDefineSeries {
            series_id,
            source,
            expr,
        } => Ok(derived::handle_define(&context, series_id, source, expr).await),
        other => Err(other),
    }
}

#[cfg(test)]
mod nested_payload_tests;

#[cfg(test)]
mod derived_tests;
