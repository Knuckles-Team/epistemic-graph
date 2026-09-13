use super::*;

// ── Time-series STARTUP RECONCILIATION (CONCEPT:EG-KG.backend.ts-startup-reconcile, L16) ──────────────
// EG-P0-4 (see `handlers::txn::commit_cross_modal_txn`'s doc comment) replays a
// cross-modal-committed measurement into the served `series.redb` immediately after the
// the authoritative-shard commit succeeds, but documents one residual: a crash strictly
// BETWEEN those two commits leaves the measurement durable + authoritative in
// after its SERIES-table commit may not be reflected in the served store.
// The pass below closes that window: run ONCE at boot, after both stores are open and
// before the server accepts traffic, so a prior crash's residual never lingers.
#[cfg(feature = "tsdb")]
#[derive(Default)]
pub struct TsReconcileReport {
    /// Series whose durable projection cursor/meta did not already match the shard
    /// (the only ones actually inspected point-by-point).
    pub series_examined: usize,
    /// Of those, how many actually needed a replay (a meta mismatch can, in principle,
    /// self-resolve to "nothing missing" once the exact point sets are compared).
    pub series_reconciled: usize,
    /// Total individual points replayed into the served store across all series.
    pub points_replayed: usize,
    /// Durable high-water cursors created or advanced this pass.
    pub projection_cursors_written: usize,
}

#[cfg(feature = "tsdb")]
fn source_meta<R: eg_tsdb::store::SeriesTableReader>(
    rtx: &R,
    series_id: &str,
) -> Result<eg_tsdb::store::SeriesMeta, String> {
    if eg_tsdb::store::SeriesKey::decode(series_id).is_none() {
        return Err("durable time-series key is not canonically scoped".to_string());
    }
    eg_tsdb::store::meta_in_rtx(rtx, series_id)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| {
            format!("durable time-series '{series_id}' has no meta row in the scan that named it")
        })
}

#[cfg(feature = "tsdb")]
fn projection_is_current(
    tsdb_store: &eg_tsdb::store::SeriesStore,
    series_id: &str,
    source_cursor: &eg_tsdb::store::ProjectionCursor,
) -> Result<bool, String> {
    let projection = tsdb_store
        .projection_health_by_storage_key(series_id)
        .map_err(|e| e.to_string())?;
    Ok(projection.status == eg_tsdb::store::ProjectionStatus::Ready
        && projection.cursor.as_ref() == Some(source_cursor))
}

#[cfg(feature = "tsdb")]
fn served_meta_matches(
    tsdb_store: &eg_tsdb::store::SeriesStore,
    series_id: &str,
    source_meta: &eg_tsdb::store::SeriesMeta,
) -> Result<bool, String> {
    let served_meta = tsdb_store.meta(series_id).map_err(|e| e.to_string())?;
    Ok(served_meta.as_ref().is_some_and(|served| {
        served.count == source_meta.count
            && served.min_ts == source_meta.min_ts
            && served.max_ts == source_meta.max_ts
            && served.n_fields == source_meta.n_fields
            && served.bucket_ns == source_meta.bucket_ns
    }))
}

#[cfg(feature = "tsdb")]
fn mark_ready(
    tsdb_store: &eg_tsdb::store::SeriesStore,
    series_id: &str,
    source_meta: &eg_tsdb::store::SeriesMeta,
    report: &mut TsReconcileReport,
) -> Result<(), String> {
    tsdb_store
        .mark_projection_ready(series_id, source_meta)
        .map_err(|e| e.to_string())?;
    report.projection_cursors_written += 1;
    Ok(())
}

#[cfg(feature = "tsdb")]
fn reconcile_series<R: eg_tsdb::store::SeriesTableReader>(
    rtx: &R,
    tsdb_store: &eg_tsdb::store::SeriesStore,
    series_id: &str,
    report: &mut TsReconcileReport,
) -> Result<(), String> {
    let graph_meta = source_meta(rtx, series_id)?;
    let source_cursor = eg_tsdb::store::ProjectionCursor::from(&graph_meta);
    if projection_is_current(tsdb_store, series_id, &source_cursor)? {
        return Ok(());
    }
    if served_meta_matches(tsdb_store, series_id, &graph_meta)? {
        return mark_ready(tsdb_store, series_id, &graph_meta, report);
    }
    report.series_examined += 1;
    let graph_points = eg_tsdb::store::range_in_rtx(
        rtx,
        series_id,
        eg_tsdb::point::Ts::MIN,
        eg_tsdb::point::Ts::MAX,
    )
    .map_err(|e| e.to_string())?;
    let served_points = tsdb_store.scan_all(series_id).map_err(|e| e.to_string())?;
    let missing = missing_points(graph_points, served_points);
    if missing.is_empty() {
        return mark_ready(tsdb_store, series_id, &graph_meta, report);
    }
    if let Err(error) = tsdb_store.append_batch(
        series_id,
        graph_meta.n_fields,
        graph_meta.bucket_ns,
        &graph_meta.field_names,
        &missing,
    ) {
        let message = error.to_string();
        let _ = tsdb_store.mark_projection_degraded(series_id, &message);
        return Err(message);
    }
    mark_ready(tsdb_store, series_id, &graph_meta, report)?;
    report.series_reconciled += 1;
    report.points_replayed += missing.len();
    tracing::warn!(
        "startup reconciliation: a scoped series was durable in the authoritative shard \
         but {} point(s) had not reached the served time-series store (a crash \
         between the two EG-P0-4 commits) — replayed",
        missing.len()
    );
    Ok(())
}

#[cfg(feature = "tsdb")]
fn reconcile_shard(
    shard: &Shard,
    tsdb_store: &eg_tsdb::store::SeriesStore,
    report: &mut TsReconcileReport,
) -> Result<(), String> {
    let rtx = shard.control_read()?;
    let series_ids = eg_tsdb::store::list_series_in_rtx(&rtx).map_err(|e| e.to_string())?;
    for series_id in series_ids {
        reconcile_series(&rtx, tsdb_store, &series_id, report)?;
    }
    Ok(())
}

#[cfg(feature = "tsdb")]
impl RedbBackend {
    /// Startup reconciliation (CONCEPT:EG-KG.backend.ts-startup-reconcile, L16): scan every shard's
    /// authoritative shard's SERIES tables — the atomic copy a cross-modal commit
    /// writes (EG-P0-4) — and replay into `tsdb_store` (the served `series.redb`) any
    /// measurement durable there but not yet reflected in the served store.
    ///
    /// **Idempotent + duplicate-free.** For each series, a durable projection cursor
    /// `(count, min_ts, max_ts)` is compared first; a current cursor skips the series
    /// without a point scan. An older store with no cursor falls back to full schema/span
    /// metadata and writes the cursor when already converged. A mismatch triggers an
    /// EXACT multiset point-diff (not a
    /// naive "skip the first N" positional diff, which would be WRONG if two batches that
    /// share a time bucket land out of append order — see the point-diff comment below)
    /// between the two stores' full point sets for that series, and only the points
    /// present in the authoritative shard but absent from the served store are appended — so a
    /// partially-replayed crash window is closed exactly, never duplicated. Any
    /// non-canonical key fails startup rather than guessing an owner.
    ///
    /// Read-only against each authoritative shard: uses the SAME shared `Weak<Shard>` handle the
    /// snapshot-read path (`read_node_blocking`) upgrades, so this never opens the
    /// file a SECOND time (redb's exclusive per-process file lock would reject that) —
    /// see `eg_tsdb::store::{list_series_in_rtx, meta_in_rtx, range_in_rtx}`, the read-only
    /// counterparts of `append_batch_in_wtx` extracted for exactly this caller.
    pub async fn reconcile_time_series(
        &self,
        tsdb_store: &eg_tsdb::store::SeriesStore,
    ) -> Result<TsReconcileReport, String> {
        let mut report = TsReconcileReport::default();
        for writer in &self.shards {
            // A shard whose writer thread already exited (shutdown mid-boot-sequence,
            // never happens in the normal boot path but guarded like every other
            // snapshot-read consumer of this handle) has nothing left to reconcile.
            let Some(shard) = writer.shard.upgrade() else {
                continue;
            };
            reconcile_shard(&shard, tsdb_store, &mut report)?;
        }
        Ok(report)
    }
}

/// Exact multiset point-diff (CONCEPT:EG-KG.backend.ts-startup-reconcile): the points present in `authoritative`
/// but not already accounted for in `served`, respecting multiplicity (two points sharing
/// a timestamp are legitimate siblings, not duplicates of one another — see
/// `eg_tsdb::store`'s `Chunk::insert` doc comment). A naive "skip the first `served.len()`
/// points of a merged/sorted scan" is WRONG here: two measurement batches that touch the
/// SAME time bucket can interleave within that bucket's sorted point list regardless of
/// which batch replayed to the served store first, so the served store's points are not
/// guaranteed to be a positional PREFIX of the authoritative scan — only a SUBSET of it.
/// `f64` values are compared by exact bit pattern (`to_bits`): both stores hold the
/// IDENTICAL byte-for-byte values the client originally sent (no arithmetic is ever
/// performed on a stored point), so bitwise equality is the correct — and only
/// semantically meaningful — comparison here.
#[cfg(feature = "tsdb")]
fn missing_points(
    authoritative: Vec<eg_tsdb::point::Point>,
    served: Vec<eg_tsdb::point::Point>,
) -> Vec<eg_tsdb::point::Point> {
    use std::collections::HashMap;

    fn key(p: &eg_tsdb::point::Point) -> (i64, Vec<u64>) {
        (p.ts, p.values.iter().map(|v| v.to_bits()).collect())
    }

    let mut served_counts: HashMap<(i64, Vec<u64>), usize> = HashMap::new();
    for p in &served {
        *served_counts.entry(key(p)).or_insert(0) += 1;
    }
    let mut missing = Vec::new();
    for p in authoritative {
        let k = key(&p);
        match served_counts.get_mut(&k) {
            Some(c) if *c > 0 => *c -= 1,
            _ => missing.push(p),
        }
    }
    missing
}
