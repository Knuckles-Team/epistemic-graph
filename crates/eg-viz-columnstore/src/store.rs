//! `ColumnStore` — the durable-shape, in-memory columnar store (D-VZ-1 lane V1).
//!
//! Implements [`eg_viz_core::ColumnStoreIngest`] (V0's trait boundary) and adds
//! the real values-in ingest surface that trait deliberately left abstract:
//! [`ColumnStore::ingest_columns`] (the base API, any [`ColumnInput`] batch),
//! [`ColumnStore::ingest_rowset`] (the `eg-plan::RowSet` bridge — id + optional
//! score, exactly that crate's `Row` shape), and
//! [`ColumnStore::ingest_f64_array`] (the `eg-numeric` bridge — a flat `&[f64]`,
//! exactly what `ArrayD<f64>::as_slice()`/`.into_raw_vec()` hands back). Arrow IPC
//! ingestion lives in [`crate::arrow_ipc`] behind the `arrow-ipc` feature.
//!
//! ★ Every mark's [`crate::store::ColumnStore::materialize_f64`] output is what
//! [`eg_viz_core::select_tier`]'s `row_count` is *validated against* by
//! [`ColumnStore::row_count`] — this store never re-derives its own row/primitive
//! limits; a caller (the export backend) is expected to call `select_tier` first
//! and hand the resulting tier back in, never infer one independently (see
//! `eg-viz-export`'s `backend.rs`).

use std::collections::HashMap;

use eg_viz_core::{ColumnDescriptor, ColumnStoreIngest, ColumnType};

use crate::chunk::{self, Chunk, CHUNK_ROWS};
use crate::dictionary::Dictionary;
use crate::error::ColumnStoreError;
use crate::types::{ColumnData, ColumnInput};

/// One ingested column: its chunks (in row order) and, for `Categorical`, the
/// dictionary every chunk's codes are interned against.
#[derive(Clone, Debug)]
pub struct Column {
    pub name: String,
    pub logical_type: ColumnType,
    pub nullable: bool,
    pub chunks: Vec<Chunk>,
    pub dictionary: Option<Dictionary>,
}

impl Column {
    /// Aggregate `(min, max)` across every chunk's [`chunk::ZoneMap`] — the whole-
    /// column range an autoranging [`eg_viz_core::ScaleSpec`] resolves against,
    /// computed WITHOUT decoding a single value (zone maps are exactly the
    /// summary that makes this cheap).
    pub fn range(&self) -> Option<(f64, f64)> {
        let mut min: Option<f64> = None;
        let mut max: Option<f64> = None;
        for chunk in &self.chunks {
            if let Some(chunk_min) = chunk.zone.min {
                min = Some(min.map_or(chunk_min, |m| m.min(chunk_min)));
            }
            if let Some(chunk_max) = chunk.zone.max {
                max = Some(max.map_or(chunk_max, |m| m.max(chunk_max)));
            }
        }
        min.zip(max)
    }

    pub fn row_count(&self) -> u64 {
        self.chunks.iter().map(|c| c.zone.row_count as u64).sum()
    }
}

/// One ingested dataset — the resolved target of a [`eg_viz_core::MarkSpec::data_ref`].
#[derive(Clone, Debug)]
pub struct Dataset {
    pub dataset_ref: String,
    pub row_count: u64,
    pub columns: Vec<Column>,
}

impl Dataset {
    pub fn column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == name)
    }
}

/// Store-wide content-addressing statistics — proof the chunk table dedupes
/// (see [`ColumnStore::stats`] and the module tests).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StoreStats {
    pub datasets: usize,
    /// Total `Chunk` references across every column of every dataset (a repeated
    /// `content_id` counts once per reference here).
    pub chunk_references: usize,
    /// Distinct `content_id`s actually stored — `< chunk_references` whenever any
    /// dedup occurred.
    pub unique_chunks: usize,
    pub stored_bytes: u64,
}

#[derive(Default)]
pub struct ColumnStore {
    datasets: HashMap<String, Dataset>,
    chunk_bytes: HashMap<String, Vec<u8>>,
}

impl ColumnStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// The real ingest entry point: chunk, zone-map, content-address, and
    /// (for `Categorical`) dictionary-encode every column in `columns`, storing
    /// them under `dataset_ref`. Every column must describe the same row count.
    /// Re-ingesting under an existing `dataset_ref` replaces it.
    pub fn ingest_columns(
        &mut self,
        dataset_ref: &str,
        columns: Vec<ColumnInput>,
    ) -> Result<String, ColumnStoreError> {
        if columns.is_empty() {
            return Err(ColumnStoreError::NoColumns);
        }
        let row_count = columns[0].data.len() as u64;
        for input in &columns {
            if input.data.len() as u64 != row_count {
                return Err(ColumnStoreError::RowCountMismatch {
                    column: input.name.clone(),
                    expected: row_count,
                    actual: input.data.len() as u64,
                });
            }
            if input.data.logical_type() != input.logical_type {
                return Err(ColumnStoreError::TypeMismatch {
                    column: input.name.clone(),
                    declared: input.logical_type,
                });
            }
            if !input.nullable {
                if let Some(mask) = &input.validity {
                    if mask.iter().any(|v| !v) {
                        return Err(ColumnStoreError::UnexpectedNull {
                            column: input.name.clone(),
                        });
                    }
                }
            }
        }

        let mut built_columns = Vec::with_capacity(columns.len());
        for input in columns {
            let column = self.build_column(input)?;
            built_columns.push(column);
        }

        self.datasets.insert(
            dataset_ref.to_string(),
            Dataset {
                dataset_ref: dataset_ref.to_string(),
                row_count,
                columns: built_columns,
            },
        );
        Ok(dataset_ref.to_string())
    }

    fn build_column(&mut self, input: ColumnInput) -> Result<Column, ColumnStoreError> {
        let row_count = input.data.len();
        let mut dictionary = if matches!(input.data, ColumnData::Categorical(_)) {
            Some(Dictionary::new())
        } else {
            None
        };

        let mut chunks = Vec::new();
        let mut start = 0;
        while start < row_count {
            let end = (start + CHUNK_ROWS).min(row_count);
            let validity_slice = input.validity.as_deref().map(|v| &v[start..end]);

            let dictionary_codes: Option<Vec<u32>> =
                if let ColumnData::Categorical(values) = &input.data {
                    let dict = dictionary.as_mut().expect("categorical dictionary");
                    Some(values[start..end].iter().map(|v| dict.intern(v)).collect())
                } else {
                    None
                };

            let (bytes, zone) = chunk::encode_range(
                &input.data,
                start,
                end,
                validity_slice,
                dictionary_codes.as_deref(),
            );
            let content_id = chunk::content_id(&bytes);
            self.chunk_bytes.entry(content_id.clone()).or_insert(bytes);
            chunks.push(Chunk { content_id, zone });

            start = end;
        }

        Ok(Column {
            name: input.name,
            logical_type: input.logical_type,
            nullable: input.nullable,
            chunks,
            dictionary,
        })
    }

    /// The `eg-plan::RowSet` bridge (V1's charter-named ingest source): a RowSet
    /// is a sequence of `(id, Option<score>)` pairs — exactly `RowSet::rows()`'s
    /// shape — materialized here into a two-column dataset: `id` (`Categorical`,
    /// dictionary-encoded — ids repeat far less than a generic string column, but
    /// the dictionary costs nothing when they don't) and `score` (`F64`,
    /// nullable — an unranked RowSet has `score: None` for every row).
    pub fn ingest_rowset(
        &mut self,
        dataset_ref: &str,
        rows: &[(String, Option<f32>)],
    ) -> Result<String, ColumnStoreError> {
        let ids: Vec<String> = rows.iter().map(|(id, _)| id.clone()).collect();
        let scores: Vec<f64> = rows
            .iter()
            .map(|(_, score)| score.map(|s| s as f64).unwrap_or(0.0))
            .collect();
        let validity: Vec<bool> = rows.iter().map(|(_, score)| score.is_some()).collect();
        let all_scored = validity.iter().all(|v| *v);

        let mut score_input = ColumnInput::new("score", ColumnData::F64(scores));
        if !all_scored {
            score_input = score_input.nullable(validity);
        }

        self.ingest_columns(
            dataset_ref,
            vec![
                ColumnInput::new("id", ColumnData::Categorical(ids)),
                score_input,
            ],
        )
    }

    /// The `eg-numeric` bridge (V1's charter-named ingest source): a single flat
    /// `f64` array under one named column — exactly the shape
    /// `ArrayD<f64>::as_slice().unwrap()`/`.into_raw_vec()` hands back for a
    /// resolved eg-numeric result, with no `ndarray`/`nalgebra` dependency pulled
    /// into this crate to bridge it.
    pub fn ingest_f64_array(
        &mut self,
        dataset_ref: &str,
        column_name: &str,
        values: &[f64],
    ) -> Result<String, ColumnStoreError> {
        self.ingest_columns(
            dataset_ref,
            vec![ColumnInput::new(
                column_name,
                ColumnData::F64(values.to_vec()),
            )],
        )
    }

    pub fn dataset(&self, dataset_ref: &str) -> Option<&Dataset> {
        self.datasets.get(dataset_ref)
    }

    pub fn column(&self, dataset_ref: &str, name: &str) -> Option<&Column> {
        self.dataset(dataset_ref).and_then(|d| d.column(name))
    }

    fn chunk_bytes(&self, content_id: &str) -> Result<&[u8], ColumnStoreError> {
        self.chunk_bytes
            .get(content_id)
            .map(Vec::as_slice)
            .ok_or_else(|| ColumnStoreError::MissingChunkBytes(content_id.to_string()))
    }

    /// Decode a numeric (`F64`/`F32`/`I64`/`U64`) column back to one `f64` per
    /// row, in row order, across every chunk. A null slot decodes to
    /// `f64::NAN` — a render/reduction consumer skips `NaN` rather than plotting
    /// a fabricated zero (see `eg-viz-export`'s point-collection helpers).
    pub fn materialize_f64(
        &self,
        dataset_ref: &str,
        column: &str,
    ) -> Result<Vec<f64>, ColumnStoreError> {
        let col = self.resolve_column(dataset_ref, column)?;
        let width = match col.logical_type {
            ColumnType::F64 | ColumnType::I64 | ColumnType::U64 => 8,
            ColumnType::F32 => 4,
            other => {
                return Err(ColumnStoreError::MaterializeTypeMismatch {
                    column: column.to_string(),
                    logical_type: other,
                })
            }
        };
        let logical_type = col.logical_type;
        let mut out = Vec::with_capacity(col.row_count() as usize);
        for chunk in &col.chunks {
            let bytes = self.chunk_bytes(&chunk.content_id)?;
            let decoded = chunk::decode_numeric(
                bytes,
                chunk.zone.row_count as usize,
                col.nullable,
                width,
                |b| match logical_type {
                    ColumnType::F64 => f64::from_le_bytes(b.try_into().unwrap()),
                    ColumnType::F32 => f32::from_le_bytes(b.try_into().unwrap()) as f64,
                    ColumnType::I64 => i64::from_le_bytes(b.try_into().unwrap()) as f64,
                    ColumnType::U64 => u64::from_le_bytes(b.try_into().unwrap()) as f64,
                    _ => unreachable!(),
                },
            );
            out.extend(decoded);
        }
        Ok(out)
    }

    pub fn materialize_utf8(
        &self,
        dataset_ref: &str,
        column: &str,
    ) -> Result<Vec<String>, ColumnStoreError> {
        let col = self.resolve_column(dataset_ref, column)?;
        if col.logical_type != ColumnType::Utf8 {
            return Err(ColumnStoreError::MaterializeTypeMismatch {
                column: column.to_string(),
                logical_type: col.logical_type,
            });
        }
        let mut out = Vec::with_capacity(col.row_count() as usize);
        for chunk in &col.chunks {
            let bytes = self.chunk_bytes(&chunk.content_id)?;
            out.extend(chunk::decode_utf8(
                bytes,
                chunk.zone.row_count as usize,
                col.nullable,
            ));
        }
        Ok(out)
    }

    /// Decode a `Categorical` column back to its string values (dictionary
    /// resolved), one per row. A null slot decodes to `None`.
    pub fn materialize_categorical(
        &self,
        dataset_ref: &str,
        column: &str,
    ) -> Result<Vec<Option<String>>, ColumnStoreError> {
        let col = self.resolve_column(dataset_ref, column)?;
        if col.logical_type != ColumnType::Categorical {
            return Err(ColumnStoreError::MaterializeTypeMismatch {
                column: column.to_string(),
                logical_type: col.logical_type,
            });
        }
        let dict = col.dictionary.as_ref().expect("categorical dictionary");
        let mut out = Vec::with_capacity(col.row_count() as usize);
        for chunk in &col.chunks {
            let bytes = self.chunk_bytes(&chunk.content_id)?;
            let codes =
                chunk::decode_categorical_codes(bytes, chunk.zone.row_count as usize, col.nullable);
            out.extend(
                codes
                    .into_iter()
                    .map(|code| dict.resolve(code).map(str::to_string)),
            );
        }
        Ok(out)
    }

    fn resolve_column(&self, dataset_ref: &str, column: &str) -> Result<&Column, ColumnStoreError> {
        let dataset = self
            .datasets
            .get(dataset_ref)
            .ok_or_else(|| ColumnStoreError::UnknownDataset(dataset_ref.to_string()))?;
        dataset
            .column(column)
            .ok_or_else(|| ColumnStoreError::UnknownColumn {
                dataset_ref: dataset_ref.to_string(),
                column: column.to_string(),
            })
    }

    /// Store-wide content-addressing statistics — see [`StoreStats`].
    pub fn stats(&self) -> StoreStats {
        let mut chunk_references = 0usize;
        for dataset in self.datasets.values() {
            for column in &dataset.columns {
                chunk_references += column.chunks.len();
            }
        }
        StoreStats {
            datasets: self.datasets.len(),
            chunk_references,
            unique_chunks: self.chunk_bytes.len(),
            stored_bytes: self.chunk_bytes.values().map(|b| b.len() as u64).sum(),
        }
    }
}

impl ColumnStoreIngest for ColumnStore {
    type Error = ColumnStoreError;

    /// V0's abstract shape-only registration: no values ride this call (see the
    /// trait's own doc — `eg-viz-core::traits`). This creates a dataset entry
    /// with the declared `row_count` and empty typed columns (zero chunks) so
    /// [`ColumnStore::row_count`] answers correctly for a caller that has only
    /// reserved a `dataset_ref`'s shape (e.g. a V4 planner) before real data
    /// streams in via [`ColumnStore::ingest_columns`]/[`ColumnStore::ingest_rowset`]/
    /// [`ColumnStore::ingest_f64_array`], which is what actually populates chunks.
    fn ingest_rows(
        &mut self,
        dataset_ref: &str,
        columns: &[ColumnDescriptor],
        row_count: u64,
    ) -> Result<String, Self::Error> {
        let placeholder_columns = columns
            .iter()
            .map(|d| Column {
                name: d.name.clone(),
                logical_type: d.logical_type,
                nullable: d.nullable,
                chunks: Vec::new(),
                dictionary: matches!(d.logical_type, ColumnType::Categorical).then(Dictionary::new),
            })
            .collect();
        self.datasets.insert(
            dataset_ref.to_string(),
            Dataset {
                dataset_ref: dataset_ref.to_string(),
                row_count,
                columns: placeholder_columns,
            },
        );
        Ok(dataset_ref.to_string())
    }

    fn row_count(&self, dataset_ref: &str) -> Result<u64, Self::Error> {
        self.datasets
            .get(dataset_ref)
            .map(|d| d.row_count)
            .ok_or_else(|| ColumnStoreError::UnknownDataset(dataset_ref.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ingest_and_materialize_f64_round_trips() {
        let mut store = ColumnStore::new();
        store
            .ingest_columns(
                "ds:1",
                vec![ColumnInput::new("x", ColumnData::F64(vec![1.0, 2.0, 3.0]))],
            )
            .unwrap();
        assert_eq!(store.row_count("ds:1").unwrap(), 3);
        assert_eq!(
            store.materialize_f64("ds:1", "x").unwrap(),
            vec![1.0, 2.0, 3.0]
        );
    }

    #[test]
    fn nullable_column_materializes_nan_for_null() {
        let mut store = ColumnStore::new();
        store
            .ingest_columns(
                "ds:1",
                vec![ColumnInput::new("x", ColumnData::F64(vec![1.0, 2.0, 3.0]))
                    .nullable(vec![true, false, true])],
            )
            .unwrap();
        let values = store.materialize_f64("ds:1", "x").unwrap();
        assert_eq!(values[0], 1.0);
        assert!(values[1].is_nan());
        assert_eq!(values[2], 3.0);
    }

    #[test]
    fn categorical_column_dictionary_encodes_and_round_trips() {
        let mut store = ColumnStore::new();
        store
            .ingest_columns(
                "ds:1",
                vec![ColumnInput::new(
                    "continent",
                    ColumnData::Categorical(vec![
                        "asia".into(),
                        "europe".into(),
                        "asia".into(),
                        "africa".into(),
                    ]),
                )],
            )
            .unwrap();
        let column = store.column("ds:1", "continent").unwrap();
        assert_eq!(column.dictionary.as_ref().unwrap().len(), 3); // asia/europe/africa, not 4
        let values = store.materialize_categorical("ds:1", "continent").unwrap();
        assert_eq!(
            values,
            vec![
                Some("asia".to_string()),
                Some("europe".to_string()),
                Some("asia".to_string()),
                Some("africa".to_string()),
            ]
        );
    }

    #[test]
    fn dataset_spanning_multiple_chunks_round_trips_every_row() {
        let n = CHUNK_ROWS * 3 + 17; // deliberately not a multiple of CHUNK_ROWS
        let values: Vec<f64> = (0..n).map(|i| i as f64).collect();
        let mut store = ColumnStore::new();
        store
            .ingest_columns(
                "ds:big",
                vec![ColumnInput::new("v", ColumnData::F64(values.clone()))],
            )
            .unwrap();
        let column = store.column("ds:big", "v").unwrap();
        assert_eq!(column.chunks.len(), 4); // 3 full + 1 partial
        assert_eq!(store.row_count("ds:big").unwrap(), n as u64);
        assert_eq!(store.materialize_f64("ds:big", "v").unwrap(), values);
    }

    #[test]
    fn content_addressed_chunks_dedupe_identical_data() {
        // Two constant-valued columns, each spanning exactly one full chunk of
        // the SAME value, produce byte-identical encoded chunks -> one stored
        // chunk backs both columns' Chunk references.
        let mut store = ColumnStore::new();
        store
            .ingest_columns(
                "ds:a",
                vec![ColumnInput::new(
                    "v",
                    ColumnData::F64(vec![7.0; CHUNK_ROWS]),
                )],
            )
            .unwrap();
        store
            .ingest_columns(
                "ds:b",
                vec![ColumnInput::new(
                    "v",
                    ColumnData::F64(vec![7.0; CHUNK_ROWS]),
                )],
            )
            .unwrap();
        let stats = store.stats();
        assert_eq!(stats.datasets, 2);
        assert_eq!(stats.chunk_references, 2);
        assert_eq!(
            stats.unique_chunks, 1,
            "identical chunks must dedupe to one stored copy"
        );
    }

    #[test]
    fn column_range_aggregates_zone_maps_across_chunks() {
        let n = CHUNK_ROWS * 2;
        let values: Vec<f64> = (0..n).map(|i| i as f64).collect();
        let mut store = ColumnStore::new();
        store
            .ingest_columns("ds:1", vec![ColumnInput::new("v", ColumnData::F64(values))])
            .unwrap();
        let column = store.column("ds:1", "v").unwrap();
        assert_eq!(column.range(), Some((0.0, (n - 1) as f64)));
    }

    #[test]
    fn rowset_bridge_ingests_ids_and_scores() {
        let mut store = ColumnStore::new();
        let rows = vec![
            ("node:1".to_string(), Some(0.9f32)),
            ("node:2".to_string(), Some(0.4f32)),
        ];
        store.ingest_rowset("ds:rowset", &rows).unwrap();
        assert_eq!(store.row_count("ds:rowset").unwrap(), 2);
        let ids = store.materialize_categorical("ds:rowset", "id").unwrap();
        assert_eq!(
            ids,
            vec![Some("node:1".to_string()), Some("node:2".to_string())]
        );
        let scores = store.materialize_f64("ds:rowset", "score").unwrap();
        assert_eq!(scores, vec![0.9f32 as f64, 0.4f32 as f64]);
    }

    #[test]
    fn numeric_array_bridge_ingests_a_flat_f64_slice() {
        let mut store = ColumnStore::new();
        let values = [1.5, 2.5, 3.5];
        store.ingest_f64_array("ds:arr", "y", &values).unwrap();
        assert_eq!(
            store.materialize_f64("ds:arr", "y").unwrap(),
            vec![1.5, 2.5, 3.5]
        );
    }

    #[test]
    fn ingest_rows_shape_only_registers_row_count_without_data() {
        let mut store = ColumnStore::new();
        let columns = [ColumnDescriptor::new("x", ColumnType::F64, false)];
        ColumnStoreIngest::ingest_rows(&mut store, "ds:shape", &columns, 42).unwrap();
        assert_eq!(
            ColumnStoreIngest::row_count(&store, "ds:shape").unwrap(),
            42
        );
    }

    #[test]
    fn row_count_mismatch_across_columns_is_rejected() {
        let mut store = ColumnStore::new();
        let err = store
            .ingest_columns(
                "ds:bad",
                vec![
                    ColumnInput::new("x", ColumnData::F64(vec![1.0, 2.0])),
                    ColumnInput::new("y", ColumnData::F64(vec![1.0])),
                ],
            )
            .unwrap_err();
        assert!(matches!(err, ColumnStoreError::RowCountMismatch { .. }));
    }
}
