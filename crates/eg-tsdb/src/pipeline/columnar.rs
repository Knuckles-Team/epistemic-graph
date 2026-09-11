//! Conversion of routed pipeline records into the native columnar segment.

use std::collections::BTreeSet;

use crate::columnar::{CellValue, ColumnarSegment};

use super::Record;

pub(super) fn stream_to_columnar(records: &[Record]) -> Result<ColumnarSegment, String> {
    let names: Vec<String> = records
        .iter()
        .flat_map(|record| record.keys())
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let rows = records
        .iter()
        .map(|record| columnar_row(record, &names))
        .collect::<Vec<Vec<CellValue>>>();
    ColumnarSegment::from_rows_inferred(&names, &rows)
}

fn columnar_row(record: &Record, names: &[String]) -> Vec<CellValue> {
    let mut row = Vec::with_capacity(names.len());
    for name in names {
        let cell = match record.get(name) {
            Some(value) => value.to_cell(),
            None => CellValue::Null,
        };
        row.push(cell);
    }
    row
}
