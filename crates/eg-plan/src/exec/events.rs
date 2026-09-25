//! `EVENTS` (EH-529, ANALYTICS-HARVEST AH-09): a declared `DERIVE` shape predicate
//! (`gt`/`lt`/`greatest`/`least` — indicator series, [`eg_numeric::series::Arith`])
//! becomes a row a later `CEP` stage can pattern-match, with NO new NFA — `CEP`'s
//! existing event-row reader (`eg-plan/src/exec.rs::row_event`) already falls back to
//! [`crate::rowset::parse_series_event_row_id`] for exactly this row shape.
//!
//! Every series row whose value channel `c` is non-zero becomes one event row per
//! channel in `channels`, id `<series>#<c>@<ts>` (the key a `CEP … KEY 'c'` matches),
//! carrying the source row's own value channels so a pattern's predicates can still
//! read them. A row that is not a series row, or where `c` is absent or zero, emits
//! nothing.

use crate::rowset::{parse_series_row_id, series_event_row_id, Row, RowSet, ValueChannels};

use super::derive::read_channel;

/// Replace the input's series rows with one event row per (row, non-zero channel).
pub(crate) fn events_op(input: RowSet, channels: &[String]) -> Result<RowSet, String> {
    let (rows, values) = input.into_parts();
    let mut scored = Vec::new();
    let mut out_values = ValueChannels::new();
    for row in &rows {
        emit_row_events(row, channels, &values, &mut scored, &mut out_values);
    }
    Ok(RowSet::from_scored(scored).with_values(out_values))
}

/// One row's event(s): a fresh row per channel in `channels` that is present and
/// non-zero on `row`, carrying `row`'s whole value-channel set forward.
fn emit_row_events(
    row: &Row,
    channels: &[String],
    values: &ValueChannels,
    scored: &mut Vec<(String, f32)>,
    out_values: &mut ValueChannels,
) {
    let Some((series, ts)) = parse_series_row_id(&row.id) else {
        return;
    };
    let carried = values.get(&row.id).cloned().unwrap_or_default();
    for c in channels {
        let Some(v) = read_channel(values, row, c) else {
            continue;
        };
        if v == 0.0 {
            continue;
        }
        let id = series_event_row_id(series, c, ts);
        scored.push((id.clone(), v as f32));
        out_values.insert(id, carried.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row_set() -> RowSet {
        let values: ValueChannels = [
            ("svc@10", vec![("spike", 1.0), ("v0", 42.0)]),
            ("svc@20", vec![("spike", 0.0), ("v0", 1.0)]),
        ]
        .into_iter()
        .map(|(id, m)| {
            (
                id.to_string(),
                m.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
            )
        })
        .collect();
        RowSet::from_ids(["svc@10".into(), "svc@20".into()]).with_values(values)
    }

    #[test]
    fn only_nonzero_channels_emit_events_carrying_the_row() {
        let out = events_op(row_set(), &["spike".to_string()]).unwrap();
        assert_eq!(out.ids(), vec!["svc#spike@10"]);
        assert_eq!(out.value("svc#spike@10", "v0"), Some(42.0));
        assert_eq!(out.value("svc#spike@10", "spike"), Some(1.0));
    }

    #[test]
    fn an_absent_channel_emits_nothing() {
        let out = events_op(row_set(), &["no_such_channel".to_string()]).unwrap();
        assert!(out.ids().is_empty());
    }

    #[test]
    fn a_row_with_no_series_id_is_dropped() {
        let rs = RowSet::from_ids(["node-1".into()]);
        let out = events_op(rs, &["spike".to_string()]).unwrap();
        assert!(out.ids().is_empty());
    }
}
