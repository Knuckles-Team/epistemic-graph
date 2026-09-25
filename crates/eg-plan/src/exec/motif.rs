//! `MOTIF` / `DISCORD` (EH-529, ANALYTICS-HARVEST AH-09): per-series query-by-example
//! (MASS) and matrix-profile (SCRIMP++) search over a value channel, in timestamp
//! order — "find past windows shaped like this one" / "the most anomalous
//! subsequence". Rows are grouped exactly as [`super::derive::named_series_groups`]
//! groups them; the input's non-series rows (graph nodes) are dropped, matching
//! `SKILL`'s behaviour.
//!
//! Every reported `distance` is re-derived from a direct dot product
//! (`eg_numeric::series::mass::Shape::distance_at` / the matrix profile's own exact
//! diagonal walk), so it is identical whether or not the anytime search was cut —
//! the FFT only ever orders candidates, per `eg_numeric::series::fft`'s own doc.

use eg_numeric::series::mass::Shape;
use eg_numeric::series::matrix_profile::{self, Extreme, Hit, MatrixProfile, ProfileOptions};
use eg_types::series_expr::{MotifOp, MotifSearch};

use super::derive::{named_series_groups, read_channel};
use crate::rowset::{parse_series_row_id, series_event_row_id, Row, RowSet, ValueChannels};

/// One series' worth of context a hit row is built from — everything [`emit_hit`]
/// needs that does NOT vary hit to hit, bundled so the function stays under the
/// argument-count cap rather than reaching for `#[allow]`.
struct Hits<'a> {
    series: &'a str,
    tss: Vec<i64>,
    m: usize,
    approximate: bool,
    /// The row-id / key a later `CEP` stage would match: `discord` for `DISCORD`,
    /// `motif` for either `MOTIF` form.
    key: &'static str,
}

/// Replace the input's series rows with their `MOTIF` / `DISCORD` hit rows.
pub(crate) fn motif_op(input: RowSet, spec: &MotifOp, max_work: u64) -> Result<RowSet, String> {
    let (rows, values) = input.into_parts();
    let mut scored = Vec::new();
    let mut channels = ValueChannels::new();
    let key = match &spec.search {
        MotifSearch::Discord { .. } => "discord",
        MotifSearch::Like { .. } | MotifSearch::Pairs { .. } => "motif",
    };
    for (series, indices) in named_series_groups(&rows) {
        let (tss, xs) = aligned(&rows, &indices, &values, &spec.channel);
        if xs.is_empty() {
            continue;
        }
        let (hits, m, approximate) = search(&xs, spec, max_work)?;
        let ctx = Hits { series, tss, m, approximate, key };
        for hit in hits {
            emit_hit(&ctx, hit, &mut scored, &mut channels);
        }
    }
    Ok(RowSet::from_scored(scored).with_values(channels))
}

/// One series' `(ts, value)` pairs of `channel`, in timestamp order, split into
/// parallel vectors (the shape the numeric kernels take).
fn aligned(rows: &[Row], indices: &[usize], values: &ValueChannels, channel: &str) -> (Vec<i64>, Vec<f64>) {
    indices
        .iter()
        .filter_map(|&i| {
            let row = &rows[i];
            let (_, ts) = parse_series_row_id(&row.id)?;
            Some((ts, read_channel(values, row, channel)?))
        })
        .unzip()
}

/// Run the query the spec asks for: `LIKE` is MASS over the query shape; `LENGTH` (on
/// either keyword) is the SCRIMP++ matrix profile, motifs on `MOTIF`, discords on
/// `DISCORD`. Returns the hits, the subsequence length and whether the search was cut.
fn search(xs: &[f64], spec: &MotifOp, max_work: u64) -> Result<(Vec<Hit>, usize, bool), String> {
    match &spec.search {
        MotifSearch::Like { shape } => {
            let s = Shape::new(shape, xs).map_err(|e| format!("MOTIF LIKE: {e}"))?;
            let profile = s.profile();
            let starts = matrix_profile::select(&profile, spec.top as usize, shape.len(), Extreme::Nearest, &|_| None);
            let hits = starts
                .into_iter()
                .map(|start| Hit { start, neighbor: None, distance: s.distance_at(start) })
                .collect();
            Ok((hits, shape.len(), false))
        }
        MotifSearch::Pairs { length } => {
            let profile = profile_of(xs, *length as usize, spec.seed, max_work)?;
            Ok((matrix_profile::motifs(&profile, spec.top as usize), *length as usize, profile.approximate))
        }
        MotifSearch::Discord { length } => {
            let profile = profile_of(xs, *length as usize, spec.seed, max_work)?;
            Ok((matrix_profile::discords(&profile, spec.top as usize), *length as usize, profile.approximate))
        }
    }
}

fn profile_of(xs: &[f64], m: usize, seed: u64, max_work: u64) -> Result<MatrixProfile, String> {
    matrix_profile::matrix_profile(xs, ProfileOptions { m, max_work, seed }).map_err(|e| format!("MOTIF/DISCORD: {e}"))
}

/// One hit as a result row: id `<series>#motif@<start ts>` / `<series>#discord@<start
/// ts>`, score the distance, channels `distance start end neighbor approximate`.
fn emit_hit(ctx: &Hits<'_>, hit: Hit, scored: &mut Vec<(String, f32)>, channels: &mut ValueChannels) {
    let tss = &ctx.tss;
    let id = series_event_row_id(ctx.series, ctx.key, tss[hit.start]);
    scored.push((id.clone(), hit.distance as f32));
    let row = [
        ("distance", hit.distance),
        ("start", tss[hit.start] as f64),
        ("end", tss[hit.start + ctx.m - 1] as f64),
        ("neighbor", hit.neighbor.map_or(-1.0, |n| tss[n] as f64)),
        ("approximate", f64::from(ctx.approximate as u8)),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect();
    channels.insert(id, row);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rowset::series_row_id;

    /// A period-4 zigzag repeated 8 times, one period (points 16..19) replaced by a
    /// shape nothing else matches — the same fixture the eg-compute discord test uses.
    fn series_with_anomaly() -> RowSet {
        let period = [0.0, 1.0, 0.0, -1.0];
        let mut xs = Vec::new();
        for i in 0..8 {
            if i == 4 {
                xs.extend_from_slice(&[5.0, -5.0, 5.0, -5.0]);
            } else {
                xs.extend_from_slice(&period);
            }
        }
        row_set("s", &xs)
    }

    fn row_set(series: &str, xs: &[f64]) -> RowSet {
        let mut values = ValueChannels::new();
        let ids: Vec<String> = xs
            .iter()
            .enumerate()
            .map(|(i, &x)| {
                let id = series_row_id(series, i as i64);
                values.insert(id.clone(), [("v0".to_string(), x)].into_iter().collect());
                id
            })
            .collect();
        RowSet::from_ids(ids).with_values(values)
    }

    fn spec(search: MotifSearch, top: u64) -> MotifOp {
        MotifOp { channel: "v0".into(), search, top, seed: 1 }
    }

    #[test]
    fn like_finds_an_exact_self_match() {
        // The query IS the series' own first period: its best (non-adjacent) match is
        // another occurrence of the same period, at distance ~0.
        let input = series_with_anomaly();
        let hit = spec(MotifSearch::Like { shape: vec![0.0, 1.0, 0.0, -1.0] }, 1);
        let out = motif_op(input, &hit, u64::MAX).unwrap();
        assert_eq!(out.len(), 1);
        let id = &out.rows()[0].id;
        assert!(id.starts_with("s#motif@"), "id={id}");
        // A bit-identical repeat: the z-normalised distance floor is a few ULPs of
        // floating-point residue from the division/sqrt in `znorm_distance` (observed
        // ~4.2e-8 on this shape) — negligible against any real discord's O(1) scale.
        let distance = out.value(id, "distance").unwrap();
        assert!(distance < 1e-6, "distance={distance}");
    }

    #[test]
    fn discord_length_finds_the_planted_anomaly() {
        let input = series_with_anomaly();
        let hit = spec(MotifSearch::Discord { length: 4 }, 1);
        let out = motif_op(input, &hit, u64::MAX).unwrap();
        assert_eq!(out.len(), 1);
        let id = &out.rows()[0].id;
        assert!(id.starts_with("s#discord@"), "id={id}");
        let start = out.value(id, "start").unwrap() as i64;
        assert!((13..=19).contains(&start), "start={start}");
        assert_eq!(out.value(id, "approximate"), Some(0.0));
    }

    #[test]
    fn a_tiny_budget_never_panics_and_marks_any_result_approximate() {
        // The anytime cut/approximate-flag behaviour itself is eg-numeric's own
        // (`series::motif_tests`); this only checks the plan-level plumbing passes
        // the budget through and stays well-formed at either extreme.
        let input = series_with_anomaly();
        let hit = spec(MotifSearch::Discord { length: 4 }, 1);
        let tiny = motif_op(input, &hit, 1).unwrap();
        assert!(tiny.len() <= 1);
        for row in tiny.rows() {
            assert_eq!(tiny.value(&row.id, "approximate"), Some(1.0));
        }
    }

    #[test]
    fn an_empty_series_yields_no_rows() {
        let out = motif_op(RowSet::from_ids(["node-1".into()]), &spec(MotifSearch::Discord { length: 4 }, 1), u64::MAX)
            .unwrap();
        assert_eq!(out.len(), 0);
    }
}
