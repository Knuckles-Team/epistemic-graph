//! `DERIVE` (EH-522): per-series incremental series operators over the RowSet.
//!
//! Rows are grouped by the series their `series@ts` id names (a bare-integer id — a
//! `WINDOW` bucket — is the unnamed series `""`) and each group is fed to a fresh
//! `eg_tsdb::derive::Program` in timestamp order. The value lands in the row's value
//! channel under the column name; rows, their order and their scores are untouched. A
//! row that is not a series row (a graph node) gets no derived value.

use std::collections::BTreeMap;

use eg_tsdb::derive::Program;
use eg_types::series_expr::DeriveColumn;

use crate::rowset::{parse_series_row_id, Row, RowSet, ValueChannels};

/// The `score` channel a column may read (the row's `f32` score, widened).
const SCORE: &str = "score";

/// Evaluate every `DERIVE` column over `input`, in column order (a later column may read
/// an earlier one).
pub(crate) fn derive_op(input: RowSet, columns: &[DeriveColumn]) -> Result<RowSet, String> {
    let programs = columns
        .iter()
        .map(|c| Program::compile(&c.expr).map_err(|e| format!("DERIVE {}: {e}", c.name)))
        .collect::<Result<Vec<_>, _>>()?;
    let (rows, mut values) = input.into_parts();
    let groups = series_groups(&rows);
    for (column, template) in columns.iter().zip(&programs) {
        for group in &groups {
            let mut program = template.clone();
            for &i in group {
                derive_row(&mut program, &rows[i], &column.name, &mut values);
            }
        }
    }
    let kept = rows.into_iter().map(|r| (r.id, r.score));
    Ok(RowSet::from_rows(kept).with_values(values))
}

/// Advance `program` on one row and record its value (if any) under `name`.
fn derive_row(program: &mut Program, row: &Row, name: &str, values: &mut ValueChannels) {
    let value = program.step(&|channel| read_channel(values, row, channel));
    if let Some(v) = value {
        values
            .entry(row.id.clone())
            .or_default()
            .insert(name.to_string(), v);
    }
}

pub(super) fn read_channel(values: &ValueChannels, row: &Row, channel: &str) -> Option<f64> {
    if channel == SCORE {
        return row.score.map(f64::from);
    }
    values.get(&row.id).and_then(|m| m.get(channel)).copied()
}

/// Row indices per series, each in timestamp order; series in name order.
fn series_groups(rows: &[Row]) -> Vec<Vec<usize>> {
    named_series_groups(rows).into_values().collect()
}

/// Series name → its row indices in timestamp order.
pub(super) fn named_series_groups(rows: &[Row]) -> BTreeMap<&str, Vec<usize>> {
    let mut by_series: BTreeMap<&str, Vec<(i64, usize)>> = BTreeMap::new();
    for (i, row) in rows.iter().enumerate() {
        if let Some((series, ts)) = parse_series_row_id(&row.id) {
            by_series.entry(series).or_default().push((ts, i));
        }
    }
    by_series
        .into_iter()
        .map(|(series, mut points)| {
            points.sort_unstable();
            (series, points.into_iter().map(|(_, i)| i).collect())
        })
        .collect()
}
