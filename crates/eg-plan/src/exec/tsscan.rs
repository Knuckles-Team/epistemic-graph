//! The native time-series SOURCE (`TSSCAN`, CONCEPT:EG-KG.query.native-time-series) and the
//! in-txn staged-series overlay it reads (CONCEPT:EG-KG.query.txn-tsdb-read-your).
//!
//! **Fidelity (EH-521).** One row per point per series: the row id is `series@ts`
//! ([`crate::rowset::series_row_id`]), so two series' points at one timestamp are two rows
//! (a bare-ts id silently dropped every series but the first). Every field of the point
//! is carried EXACTLY as an `f64` value channel `v0..vk` (the side table of
//! [`RowSet::with_values`]); the `f32` score is `v0` narrowed, for the rank currency only.

use std::collections::BTreeMap;

use crate::rowset::{series_row_id, RowSet, ValueChannels};

/// An in-memory overlay of a transaction's STAGED, uncommitted time-series points
/// (CONCEPT:EG-KG.query.txn-tsdb-read-your) — the in-txn tsdb read-your-own-writes source. The native
/// [`eg_tsdb::store::SeriesStore`] is redb-file-backed with no in-memory overlay, so an
/// in-txn `Op::TsScan` cannot see the txn's own staged `measurements` through it; this
/// dep-free map (series id → its staged `(ts_ns, field_values)` points) is consulted
/// alongside the committed store and MERGED so the txn reads its own writes while an
/// off-txn read (no overlay attached) sees committed only. Points are stored verbatim
/// as staged (`i64` nanoseconds, matching `GraphTxnState.measurements`); the scan trims
/// them to the requested window and carries every field value, exactly as the
/// committed-store path does.
#[derive(Debug, Default, Clone)]
pub struct StagedSeries {
    series: std::collections::HashMap<String, Vec<(i64, Vec<f64>)>>,
}

impl StagedSeries {
    /// An empty overlay.
    pub fn new() -> Self {
        Self::default()
    }

    /// Stage a batch of `(ts_ns, field_values)` points for `series` (appended — a series
    /// may be staged across several `INSERT INTO series …` in one txn).
    pub fn push_points(&mut self, series: &str, points: impl IntoIterator<Item = (i64, Vec<f64>)>) {
        self.series
            .entry(series.to_string())
            .or_default()
            .extend(points);
    }

    /// True when nothing is staged (the executor then behaves as if no overlay were
    /// attached).
    pub fn is_empty(&self) -> bool {
        self.series.is_empty()
    }

    /// The staged points of `series` within `[from_ns, to_ns)`, every field kept.
    fn range(&self, series: &str, from_ns: i64, to_ns: i64) -> Vec<(i64, Vec<f64>)> {
        self.series
            .get(series)
            .map(|pts| {
                pts.iter()
                    .filter(|(ts, vals)| *ts >= from_ns && *ts < to_ns && !vals.is_empty())
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }
}

/// Where a scan reads committed points: the store and its verified `(tenant, graph)` scope.
pub(crate) struct CommittedSeries<'a> {
    pub store: Option<&'a eg_tsdb::store::SeriesStore>,
    pub tenant: Option<&'a str>,
    pub graph: Option<&'a str>,
}

impl CommittedSeries<'_> {
    fn range(&self, series: &str, from_ns: i64, to_ns: i64) -> Vec<(i64, Vec<f64>)> {
        let (Some(store), Some(tenant), Some(graph)) = (self.store, self.tenant, self.graph) else {
            return Vec::new();
        };
        store
            .range_scoped(
                &eg_tsdb::store::SeriesKey::new(tenant, graph, series),
                from_ns,
                to_ns,
            )
            .unwrap_or_default()
            .into_iter()
            .filter(|p| !p.values.is_empty())
            .map(|p| (p.ts, p.values))
            .collect()
    }
}

/// SOURCE (time-series): read the points of each id in `series` within the `[from, to)`
/// window and emit one row per point per series — `id` = `series@ts`, `score` = field 0
/// narrowed to `f32`, value channels `v0..vk` = every field exactly — so a downstream
/// `Rank`/`Limit`/`Filter`/`DERIVE` fuses the time-series leg with the other legs in ONE
/// plan (tsdb-in-plan fusion, CONCEPT:EG-KG.query.native-time-series).
///
/// Bounds are `f64` SECONDS on the wire; the store's range is half-open `[from, to)` in
/// nanoseconds. No store, an unknown series or an empty point contribute nothing: degrade,
/// never err. Rows are ordered by series (in request order), then by timestamp.
///
/// Several committed versions of one point (a correction, a derived series' revision,
/// EH-524) read as the LATEST one. CONCEPT:EG-KG.query.txn-tsdb-read-your — when a
/// [`StagedSeries`] overlay is attached, the transaction's OWN staged point shadows the
/// committed one at that timestamp (RYOW precedence), and staged-only points are visible
/// before commit. With no overlay the committed store is read alone.
pub(crate) fn tsdb_scan_op(
    committed: &CommittedSeries<'_>,
    staged: Option<&StagedSeries>,
    series: &[String],
    from: f64,
    to: f64,
) -> RowSet {
    const NS_PER_S: f64 = 1e9;
    let from_ns = (from.max(0.0) * NS_PER_S) as i64;
    let to_ns = (to.max(0.0) * NS_PER_S) as i64;
    let mut scored: Vec<(String, f32)> = Vec::new();
    let mut values = ValueChannels::new();
    for sid in series {
        // The store keeps every version of a point (equal timestamps in arrival order):
        // the last one is the latest correction. Staged points then shadow committed.
        let mut merged: BTreeMap<i64, Vec<f64>> =
            committed.range(sid, from_ns, to_ns).into_iter().collect();
        merged.extend(
            staged
                .map(|s| s.range(sid, from_ns, to_ns))
                .unwrap_or_default(),
        );
        for (ts, vals) in merged {
            let id = series_row_id(sid, ts);
            scored.push((id.clone(), vals[0] as f32));
            values.entry(id).or_insert_with(|| value_channels(&vals));
        }
    }
    RowSet::from_scored(scored).with_values(values)
}

/// A point's fields as the `v0..vk` channels.
fn value_channels(vals: &[f64]) -> BTreeMap<String, f64> {
    vals.iter()
        .enumerate()
        .map(|(i, &v)| (format!("v{i}"), v))
        .collect()
}
