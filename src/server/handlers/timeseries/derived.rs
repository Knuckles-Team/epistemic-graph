//! EH-524 — materialised derived series: `TsDefineSeries`, and the maintenance every
//! `TsAppend` runs on the series derived from the one it appended to.
//!
//! The logic is `eg_tsdb::derive::maintain` (pure); this module does the IO. Each
//! derived series commits its new points AND its new state (definition, program,
//! checkpoints) in ONE native MutationBatch in `series.redb`, so its points and its
//! checkpoint can never disagree. A source append commits first; a crash between the
//! two leaves the derived series lagging, never wrong, and the next append (or a
//! re-definition) catches it up from its checkpoint. Work per call is bounded by
//! [`WORK_BUDGET`] source points.
//!
//! Scope: a derived series lives in, and may only read, the caller's own verified
//! `(tenant, actor)` series scope (`BatchScope::key`), so it can never be more visible
//! than its source — the "most restrictive source label" of the design holds by
//! construction. Lineage (`derived_from`) and the expression digest are in the receipt
//! and in the series metadata.

use eg_tsdb::derive::maintain::{latest_versions, DerivedState, Step, DERIVED_FIELDS};
use eg_tsdb::derive::KERNEL_VERSION;
use eg_tsdb::point::{Point, Ts};
use eg_tsdb::store::{DerivedWrite, ScopedAppendBatch, SeriesKey, SeriesMeta, SeriesStore};
use eg_types::series_expr::{DerivedSeriesReceipt, SeriesExpr};

use super::{compute_off_lock, results, store_of, BatchScope, TsRequestContext};
use crate::protocol::{Method, Response, ResultPayload};

/// Source points one maintenance step may consume: the per-call work budget.
const WORK_BUDGET: usize = 100_000;

/// How long a derived-from-derived chain may be (the cycle guard's bound).
const MAX_DERIVATION_DEPTH: usize = 16;

/// One `TsDefineSeries` request.
struct Definition {
    series_id: String,
    source: String,
    expr: String,
}

pub(super) async fn handle_define(
    context: &TsRequestContext<'_>,
    series_id: String,
    source: String,
    expr: String,
) -> Response {
    let store = match store_of(context.state, context.req_id).await {
        Ok(store) => store,
        Err(response) => return response,
    };
    let scope = context.batch_scope();
    let definition = Definition {
        series_id,
        source,
        expr,
    };
    match compute_off_lock(context.req_id, move || define(&store, &scope, &definition)).await {
        Ok(Ok(receipt)) => Response::ok(
            context.req_id,
            ResultPayload::of_ref::<results::TsDefineSeries>(&receipt),
        ),
        Ok(Err(error)) => Response::err(context.req_id, error),
        Err(response) => response,
    }
}

/// Maintain every series derived (transitively) from `source` after an append whose
/// earliest point was `min_ts`. Best effort: the source append is already committed, so
/// a failure here is logged and the derived series catch up on the next call.
pub(super) async fn maintain_after_append(
    context: &TsRequestContext<'_>,
    source: String,
    min_ts: Option<Ts>,
) {
    let Ok(store) = store_of(context.state, context.req_id).await else {
        return;
    };
    let scope = context.batch_scope();
    let outcome = compute_off_lock(context.req_id, move || {
        maintain_dependents(&store, &scope, &source, min_ts, 0)
    })
    .await;
    if let Ok(Err(error)) = outcome {
        tracing::warn!(%error, "derived-series maintenance left a series lagging");
    }
}

fn define(
    store: &SeriesStore,
    scope: &BatchScope,
    def: &Definition,
) -> Result<DerivedSeriesReceipt, String> {
    if def.series_id == def.source {
        return Err("a derived series cannot derive from itself".into());
    }
    let expr = eg_plan::uql::parse_series_expr(&def.expr).map_err(|e| e.render(&def.expr))?;
    let source_key = scope.key(&def.source)?;
    let source_meta = meta(store, &source_key)?
        .ok_or_else(|| format!("the source series `{}` does not exist", def.source))?;
    check_fields(&expr, &source_meta)?;
    check_no_cycle(store, scope, &def.source, &def.series_id)?;
    let key = scope.key(&def.series_id)?;
    let state = match meta(store, &key)? {
        None => DerivedState::define(&def.source, expr)?,
        Some(existing) => same_definition(&existing, &def.source, &expr)?,
    };
    let committed = maintain_one(store, scope, &key, state, None)?;
    Ok(DerivedSeriesReceipt {
        series_id: def.series_id.clone(),
        derived_from: committed.state.source.clone(),
        expr: committed.state.canonical.clone(),
        digest: committed.state.digest.clone(),
        kernel_version: KERNEL_VERSION.to_string(),
        appended: committed.step.points.len() as u64,
        caught_up: committed.step.caught_up,
        last_ts: committed.state.last_ts,
    })
}

/// A derived series reads only its source's fields `v0..v{n-1}`.
fn check_fields(expr: &SeriesExpr, source: &SeriesMeta) -> Result<(), String> {
    let in_range = |name: &str| {
        name.strip_prefix('v')
            .and_then(|i| i.parse::<usize>().ok())
            .is_some_and(|i| i < source.n_fields)
    };
    match expr.channels().into_iter().find(|name| !in_range(name)) {
        None => Ok(()),
        Some(name) => Err(format!(
            "`{name}` is not a field of the source (it has v0..v{})",
            source.n_fields.saturating_sub(1)
        )),
    }
}

/// Refuse a definition whose source chain reaches the series being defined.
fn check_no_cycle(
    store: &SeriesStore,
    scope: &BatchScope,
    source: &str,
    target: &str,
) -> Result<(), String> {
    let mut current = source.to_string();
    for _ in 0..MAX_DERIVATION_DEPTH {
        let Some(state) = derived_state(store, &scope.key(&current)?)? else {
            return Ok(());
        };
        if state.source == target {
            return Err(format!(
                "defining `{target}` over `{source}` would make a cycle"
            ));
        }
        current = state.source;
    }
    Err(format!(
        "the derivation chain under `{source}` is deeper than {MAX_DERIVATION_DEPTH}"
    ))
}

/// An existing series may only be re-defined identically (the call then catches up).
fn same_definition(
    existing: &SeriesMeta,
    source: &str,
    expr: &SeriesExpr,
) -> Result<DerivedState, String> {
    let bytes = existing
        .derived
        .as_deref()
        .ok_or("the series already exists and is not a derived series")?;
    let state = DerivedState::decode(bytes)?;
    if state.source == source && &state.expr == expr {
        return Ok(state);
    }
    Err("the series is already derived by a different definition".into())
}

/// Maintain the series derived from `source`, then theirs, to a bounded depth.
fn maintain_dependents(
    store: &SeriesStore,
    scope: &BatchScope,
    source: &str,
    min_ts: Option<Ts>,
    depth: usize,
) -> Result<(), String> {
    if depth >= MAX_DERIVATION_DEPTH {
        return Err(format!(
            "the derivation chain under `{source}` is deeper than {MAX_DERIVATION_DEPTH}"
        ));
    }
    let Some(source_meta) = meta(store, &scope.key(source)?)? else {
        return Ok(());
    };
    for dependent in &source_meta.dependents {
        let key = scope.key(dependent)?;
        let Some(state) = derived_state(store, &key)? else {
            continue;
        };
        let committed = maintain_one(store, scope, &key, state, min_ts)?;
        let earliest = committed.step.points.iter().map(|p| p.ts).min();
        if earliest.is_some() {
            maintain_dependents(store, scope, dependent, earliest, depth + 1)?;
        }
    }
    Ok(())
}

/// One maintenance step's committed outcome.
struct Committed {
    state: DerivedState,
    step: Step,
}

/// Advance (or, for a revision at or before the last derived timestamp, replay) one
/// derived series and commit its points with its new state.
fn maintain_one(
    store: &SeriesStore,
    scope: &BatchScope,
    key: &SeriesKey,
    mut state: DerivedState,
    min_ts: Option<Ts>,
) -> Result<Committed, String> {
    let now = crate::server::dispatch::authoritative_now_ms();
    let source_key = scope.key(&state.source)?;
    let revised_from = min_ts.filter(|&m| state.last_ts.is_some_and(|last| m <= last));
    let step = match revised_from {
        Some(from) => {
            let lo = state.replay_start(from).map_or(Ts::MIN, |s| s + 1);
            let source = latest_versions(&range(store, &source_key, lo)?);
            let current = range(store, key, lo)?;
            state.replay(from, &source, &current, now, WORK_BUDGET)?
        }
        None => {
            let lo = state.last_ts.map_or(Ts::MIN, |t| t + 1);
            let source = latest_versions(&range(store, &source_key, lo)?);
            state.advance(&source, now, WORK_BUDGET)
        }
    };
    commit(store, scope, key, &state, &step, now)?;
    Ok(Committed { state, step })
}

/// Commit a derived series' points and state as ONE MutationBatch.
fn commit(
    store: &SeriesStore,
    scope: &BatchScope,
    key: &SeriesKey,
    state: &DerivedState,
    step: &Step,
    now: u64,
) -> Result<(), String> {
    let source_key = scope.key(&state.source)?;
    let bucket_ns = meta(store, &source_key)?
        .ok_or("the source series disappeared")?
        .bucket_ns;
    let field_names: Vec<String> = DERIVED_FIELDS.iter().map(|f| (*f).to_string()).collect();
    let bytes = state.encode()?;
    let batch = scope.series_batch(store, &maintenance_event(key, &step.points, &bytes)?, now)?;
    let source_storage = source_key.encode();
    store
        .append_scoped_batch(
            key,
            ScopedAppendBatch {
                n_fields: DERIVED_FIELDS.len(),
                bucket_ns,
                field_names: &field_names,
                points: &step.points,
                batch: &batch,
                committed_at_ms: now,
                derived: Some(DerivedWrite {
                    state: &bytes,
                    source_key: &source_storage,
                    dependent: &key.series,
                }),
            },
        )
        .map(|_| ())
        .map_err(|error| error.to_string())
}

/// The batch identity of one maintenance commit: the derived series and the digest of
/// the points and state it commits, so two commits of one request never share a key.
fn maintenance_event(key: &SeriesKey, points: &[Point], state: &[u8]) -> Result<Method, String> {
    use sha2::{Digest, Sha256};
    let wire: Vec<(i64, &[f64])> = points.iter().map(|p| (p.ts, p.values.as_slice())).collect();
    let content = rmp_serde::to_vec(&(wire, state)).map_err(|e| e.to_string())?;
    Ok(Method::ApplyMutation {
        event_type: "timeseries_derive".to_string(),
        query: format!(
            "{}:sha256:{}",
            key.encode(),
            hex::encode(Sha256::digest(&content))
        ),
    })
}

fn meta(store: &SeriesStore, key: &SeriesKey) -> Result<Option<SeriesMeta>, String> {
    store.meta_scoped(key).map_err(|error| error.to_string())
}

fn derived_state(store: &SeriesStore, key: &SeriesKey) -> Result<Option<DerivedState>, String> {
    meta(store, key)?
        .and_then(|m| m.derived)
        .map(|bytes| DerivedState::decode(&bytes))
        .transpose()
}

fn range(store: &SeriesStore, key: &SeriesKey, from: Ts) -> Result<Vec<Point>, String> {
    store
        .range_scoped(key, from, Ts::MAX)
        .map_err(|error| error.to_string())
}
