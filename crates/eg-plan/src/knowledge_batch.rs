//! `KnowledgeBatch` — the Arrow-columnar projection of a [`KnowledgeSet`]
//! (CONCEPT:EG-P1-2, Codex P1 feedback).
//!
//! [`RowSet`](crate::RowSet) stays the minimal id+score closed-algebra currency
//! (see `rowset.rs`'s module docs) and [`KnowledgeSet`] stays the row-oriented
//! enriched shape a caller builds AFTER a plan runs (see `knowledge.rs`'s module
//! docs) — this module changes NEITHER. `KnowledgeBatch` is a THIRD, still
//! OPT-IN, shape: the same `KnowledgeSet` data laid out **columnar** and
//! convertible to a real `arrow::record_batch::RecordBatch`, for a caller that
//! wants to hand results to anything Arrow-speaking (DataFusion, Parquet/Arrow
//! IPC, a Python/Polars/pandas consumer over Arrow FFI, a vectorized downstream
//! aggregation) instead of iterating `Vec<KnowledgeRow>` row-by-row.
//!
//! ## Feature gate
//!
//! Everything here lives behind the `knowledge-batch` cargo feature (which
//! implies `query`, since it builds FROM a `KnowledgeSet`). It pulls `arrow`,
//! pinned to the SAME version (`53`) `eg-lake`'s `lake` feature already carries,
//! so this does not introduce a second Arrow version into the workspace lock. A
//! default/Pi build sets neither feature and links NO Arrow at all — the
//! small-footprint contract stays intact (verify with `cargo tree -p eg-plan`).
//!
//! ## Column layout
//!
//! | column | Arrow type | source |
//! |---|---|---|
//! | `id` | `Utf8` (non-null) | `KnowledgeRow::id` |
//! | `kind` | `Utf8` (non-null) | `KnowledgeRow::kind` |
//! | `score_<name>` (one per [`KnowledgeBatch::score_names`]) | `Float32` | `KnowledgeRow::score` (name `"score"` from `from_knowledge_set`) or [`KnowledgeBatch::with_named_score`] |
//! | `confidence` | `Float64` (non-null) | `KnowledgeRow::confidence` |
//! | `evidence_kind` | `Utf8` | the FIRST `EvidenceSpan`'s variant tag (`"document_span"` / `"table_cell_range"` / `"image_region"` / `"audio_segment"` / `"video_shot"` / `"code_symbol"` / `"trace_span"`), or null when the row has none — a typed, filterable summary column covering every granularity `EvidenceSpan` models (text span, table cell, image pixel region, audio/video time interval or frame-range, code symbol, trace span) |
//! | `evidence_refs_json` | `List<Utf8>` | every `EvidenceSpan` on the row, each one JSON-serialized (lossless — `EvidenceSpan` already derives `Serialize`/`Deserialize`); a real Arrow `List` column, not a single opaque blob |
//! | `valid_from` / `valid_until` | `Int64` | `KnowledgeRow::valid_time` (u64 -> i64) |
//! | `tx_from` / `tx_to` | `Int64` | `KnowledgeRow::tx_time` |
//! | `source_refs` | `List<Utf8>` | `KnowledgeRow::source_refs` (provenance ids) |
//! | `policy_labels` | `List<Utf8>` | `KnowledgeRow::policy_labels` (policy/classification labels) |
//! | `transformation_ids` | `List<Utf8>` | reserved — see "Known-lossy / reserved fields" below |
//! | `proof_ids` / `alternative_ids` / `contradiction_ids` | `List<Utf8>` | reserved — see below |
//! | `blob_handle` | `Utf8` | `KnowledgeRow::payload_ref.node_id` when `has_payload`, else null — a LAZY handle (the node id, re-resolvable via `GraphView::node_row_object`), never the payload bytes |
//! | `has_payload` | `Boolean` (non-null) | `KnowledgeRow::payload_ref.has_payload` |
//!
//! ## Transformation/proof/alternative/contradiction columns (L22/CONCEPT:EG-P3-1)
//!
//! `KnowledgeRow` now carries per-row transformation lineage, proof-chain,
//! alternative-hypothesis, and contradiction NODE ids (see `knowledge.rs`'s module
//! docs — `AuxEdgeIndex` + `resolve_proof_ids`) alongside the aggregate
//! `policy_labels` classification (e.g. `"epistemic:contested"`).
//! [`KnowledgeBatchRow::transformation_ids`], `proof_ids`, `alternative_ids` and
//! `contradiction_ids` are copied straight off the corresponding `KnowledgeRow`
//! fields in [`KnowledgeBatch::from_knowledge_set`] — populated under `epistemic`
//! whenever the underlying graph carries the relevant edges (a `GENERATED_BY`
//! outgoing edge, a transitive support/contradiction chain, an `ALTERNATIVE_TO`
//! edge, or a `Contradicts`/`Attacks` edge on EITHER endpoint respectively), still
//! `Vec::new()` — never fabricated — for a row with none, and unconditionally empty
//! without the `epistemic` feature (the feature gate, not the data, decides). See
//! `knowledge.rs`'s module docs for the full derivation and the `GENERATED_BY`/
//! `ALTERNATIVE_TO` edge conventions (written by
//! `src/server/handlers/mining.rs`'s `materialize_claim` and
//! `eg-jobs::claim::commit_result_claim` for `GENERATED_BY`; `ALTERNATIVE_TO` has no
//! writeback producer yet — an honest, documented follow-up).
//!
//! ## Streaming cursor (CONCEPT:EG-P1-4, closes the L23 stub)
//!
//! [`KnowledgeCursor`] started as a STUB — a resumable POSITION (offset/total/
//! exhausted) with no way to actually advance. [`ChunkedKnowledgeCursor`] is the
//! real driver: it owns a `KnowledgeBatch`'s rows and hands them out
//! chunk-by-chunk via [`ChunkedKnowledgeCursor::next_chunk`], updating a
//! [`KnowledgeCursor`] position as it goes — so a caller consuming a large result
//! never has to hold the WHOLE thing as one `KnowledgeBatch` in flight; it pulls
//! bounded slices instead, each one a fully-formed `KnowledgeBatch` sharing the
//! source's Arrow schema (`score_names` is constant across every chunk).
//!
//! This is the CONSUMER-side half of streaming — turning an already-computed row
//! sequence into a genuine chunk-at-a-time iterator with correct bookkeeping
//! (proven by the equivalence test: concatenating every yielded chunk reconstructs
//! the source exactly). A PRODUCER-side cursor — one that re-drives the underlying
//! `Op` chain per chunk so a huge result is never even row-materialized in one
//! place — needs a resumable [`crate::exec::Driver`], not a `KnowledgeBatch`-shape
//! change; that is listed in the rollout backlog (EG-P1-4), out of this
//! workstream's scope.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int64Array, ListArray,
    ListBuilder, StringArray, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use eg_modality::EvidenceSpan;

use crate::knowledge::KnowledgeSet;

/// The stable variant tag for one [`EvidenceSpan`] — the `evidence_kind` column's
/// value. Covers every granularity the type models: `DocumentSpan` (text span),
/// `TableCellRange` (tabular cell range), `ImageRegion` (pixel region),
/// `AudioSegment`/`VideoShot` (time interval / frame range), `CodeSymbol` (code
/// symbol by line range), `TraceSpan` (distributed-trace span).
fn evidence_kind_tag(span: &EvidenceSpan) -> &'static str {
    match span {
        EvidenceSpan::DocumentSpan { .. } => "document_span",
        EvidenceSpan::TableCellRange { .. } => "table_cell_range",
        EvidenceSpan::ImageRegion { .. } => "image_region",
        EvidenceSpan::AudioSegment { .. } => "audio_segment",
        EvidenceSpan::VideoShot { .. } => "video_shot",
        EvidenceSpan::CodeSymbol { .. } => "code_symbol",
        EvidenceSpan::TraceSpan { .. } => "trace_span",
    }
}

/// One row of a [`KnowledgeBatch`] prior to Arrow columnar layout — the same
/// fields [`crate::knowledge::KnowledgeRow`] carries, widened with a name-keyed
/// score list (so more than one named score can ride the same row, CONCEPT:EG-P1-2)
/// and a lazy blob handle. See the module docs for the
/// transformation/proof/alternative/contradiction id lists (L22/CONCEPT:EG-P3-1).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct KnowledgeBatchRow {
    pub id: String,
    pub kind: String,
    /// `(name, value)` pairs, in the SAME order as [`KnowledgeBatch::score_names`].
    /// `from_knowledge_set` always populates exactly one, named `"score"`, from
    /// `KnowledgeRow::score`; [`KnowledgeBatch::with_named_score`] appends more.
    pub scores: Vec<(String, Option<f32>)>,
    pub confidence: f64,
    pub evidence_refs: Vec<EvidenceSpan>,
    /// `(valid_from, valid_until)`.
    pub valid_time: (Option<u64>, Option<u64>),
    /// `(tx_from, tx_to)`.
    pub tx_time: (Option<u64>, Option<u64>),
    pub source_refs: Vec<String>,
    pub policy_labels: Vec<String>,
    /// This row's own generating Activity id(s) (L22/CONCEPT:EG-P3-1) — copied from
    /// `KnowledgeRow::transformation_ids`; empty when the row has no outgoing
    /// `GENERATED_BY` edge, or unconditionally under a plain (non-`epistemic`) build.
    pub transformation_ids: Vec<String>,
    /// The transitive justification/premise chain underneath this row's belief
    /// (L22/CONCEPT:EG-P3-1) — copied from `KnowledgeRow::proof_ids`.
    pub proof_ids: Vec<String>,
    /// Mutually-exclusive alternative-claim counterpart ids (L22/CONCEPT:EG-P3-1) —
    /// copied from `KnowledgeRow::alternative_ids`. No current writeback path emits
    /// `ALTERNATIVE_TO` edges (documented follow-up); the column is wired and tested
    /// on the read side regardless.
    pub alternative_ids: Vec<String>,
    /// Ids of nodes this row contradicts/attacks or is contradicted/attacked BY
    /// (L22/CONCEPT:EG-P3-1, SYMMETRIC) — copied from `KnowledgeRow::contradiction_ids`.
    pub contradiction_ids: Vec<String>,
    /// A lazy handle onto the row's stored payload (its node id, NOT the decoded
    /// bytes) — re-resolvable via `GraphView::node_row_object`. `None` when the row
    /// had no decodable payload at all.
    pub blob_handle: Option<String>,
    pub has_payload: bool,
}

/// A resumable position over a (potentially unbounded/live) sequence of
/// [`KnowledgeBatch`]es — the streaming-cursor STUB (CONCEPT:EG-P1-2). See the
/// module docs' "Streaming cursor" section: wiring this into a real chunked
/// executor is explicit follow-up, not done here.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct KnowledgeCursor {
    /// Row offset already consumed across every batch seen so far.
    pub offset: usize,
    /// Total row count, when known up front (`None` for a genuinely unbounded/live
    /// source that hasn't reported a total).
    pub total: Option<usize>,
    /// Whether the underlying source is known to be exhausted (no further batches).
    pub exhausted: bool,
}

/// A REAL chunked/streaming cursor over a [`KnowledgeBatch`]'s rows (CONCEPT:EG-P1-4 — closes
/// the L23 `KnowledgeCursor` stub). Owns the row sequence and hands it out
/// CHUNK-BY-CHUNK via [`Self::next_chunk`], so a caller consuming a large result
/// never has to hold the WHOLE thing as one `KnowledgeBatch` — it pulls bounded
/// slices, exactly the shape a paginated wire response or a backpressured
/// consumer needs. `score_names` stays constant across every yielded chunk (each
/// one carries the SAME schema as the source batch), so a caller can convert
/// every chunk to a `RecordBatch` via [`KnowledgeBatch::to_record_batch`] and they
/// all share one Arrow schema.
///
/// Why "materialize once, then chunk" rather than a lazy database-style cursor
/// that re-drives the underlying query per chunk: a `KnowledgeSet` is itself built
/// from ONE snapshot-isolated plan run (`KnowledgeSet::from_rowset` over an
/// off-lock `GraphView`) — there is no live, resumable executor state underneath
/// it to re-drive per chunk (the engine's plan model today is "run once, get a
/// RowSet", not a server-side open cursor). This type is the real, non-stub
/// CONSUMER-side half of streaming: it turns an already-computed row sequence
/// into a genuine chunk-at-a-time iterator with correct `exhausted`/`offset`/
/// `total` bookkeeping — proven by the equivalence test: concatenating every
/// yielded chunk reconstructs the source batch exactly. A follow-up PRODUCER-side
/// cursor that re-drives the underlying `Op` chain per chunk (so a huge result is
/// never even row-materialized in one place) needs a resumable
/// [`crate::exec::Driver`], not a `KnowledgeBatch`-shape change — listed in the
/// rollout backlog, out of this workstream's scope.
#[derive(Clone, Debug)]
pub struct ChunkedKnowledgeCursor {
    rows: Vec<KnowledgeBatchRow>,
    score_names: Vec<String>,
    chunk_size: usize,
    pos: KnowledgeCursor,
}

impl ChunkedKnowledgeCursor {
    /// A chunked cursor over `batch`, yielding up to `chunk_size` rows per
    /// [`Self::next_chunk`] call (clamped to at least 1, so a caller can never wedge
    /// it with `chunk_size: 0`).
    pub fn new(batch: KnowledgeBatch, chunk_size: usize) -> Self {
        let total = batch.len();
        Self {
            rows: batch.rows,
            score_names: batch.score_names,
            chunk_size: chunk_size.max(1),
            pos: KnowledgeCursor {
                offset: 0,
                total: Some(total),
                exhausted: total == 0,
            },
        }
    }

    /// The cursor's current position/exhaustion snapshot.
    pub fn position(&self) -> KnowledgeCursor {
        self.pos
    }

    /// True iff every row has already been yielded (no more chunks to pull).
    pub fn is_exhausted(&self) -> bool {
        self.pos.exhausted
    }

    /// Total row count across every chunk this cursor will ever yield (fixed at
    /// construction — the source `KnowledgeBatch` is already fully materialized).
    pub fn total(&self) -> usize {
        self.rows.len()
    }

    /// Pull the NEXT chunk (up to `chunk_size` rows) as its own `KnowledgeBatch`,
    /// advancing `offset` and flipping `exhausted` once the source is drained.
    /// Returns `None` — with [`Self::is_exhausted`] already `true` — once every row
    /// has been yielded; a caller drains it with
    /// `while let Some(chunk) = cursor.next_chunk() { … }`. Every yielded chunk is
    /// non-empty (an already-`exhausted` cursor short-circuits to `None` rather
    /// than yielding an empty tail).
    pub fn next_chunk(&mut self) -> Option<KnowledgeBatch> {
        if self.pos.exhausted {
            return None;
        }
        let start = self.pos.offset;
        let end = (start + self.chunk_size).min(self.rows.len());
        let slice = self.rows[start..end].to_vec();
        self.pos.offset = end;
        if end >= self.rows.len() {
            self.pos.exhausted = true;
        }
        Some(KnowledgeBatch {
            rows: slice,
            score_names: self.score_names.clone(),
        })
    }
}

/// The Arrow-columnar projection of a [`KnowledgeSet`] (CONCEPT:EG-P1-2). See the
/// module docs for the full column layout and the reserved/lossy fields.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct KnowledgeBatch {
    pub rows: Vec<KnowledgeBatchRow>,
    /// The score column names, in schema order (`score_<name>` per entry).
    /// `from_knowledge_set` seeds this with `["score"]`.
    pub score_names: Vec<String>,
}

impl KnowledgeBatch {
    /// Build a `KnowledgeBatch` from a finished [`KnowledgeSet`] — the columnar
    /// projection of the SAME rows, not a re-computation: every field is copied
    /// straight off each `KnowledgeRow`, one score column named `"score"` seeded
    /// from `KnowledgeRow::score`. `KnowledgeSet` itself is only read, never
    /// mutated — this is purely additive, exactly like `KnowledgeSet::from_rowset`
    /// is additive over `RowSet`.
    pub fn from_knowledge_set(ks: &KnowledgeSet) -> KnowledgeBatch {
        let rows = ks
            .rows
            .iter()
            .map(|r| KnowledgeBatchRow {
                id: r.id.clone(),
                kind: r.kind.clone(),
                scores: vec![("score".to_string(), r.score)],
                confidence: r.confidence,
                evidence_refs: r.evidence_refs.clone(),
                valid_time: r.valid_time,
                tx_time: r.tx_time,
                source_refs: r.source_refs.clone(),
                policy_labels: r.policy_labels.clone(),
                transformation_ids: r.transformation_ids.clone(),
                proof_ids: r.proof_ids.clone(),
                alternative_ids: r.alternative_ids.clone(),
                contradiction_ids: r.contradiction_ids.clone(),
                blob_handle: r
                    .payload_ref
                    .as_ref()
                    .filter(|p| p.has_payload)
                    .map(|p| p.node_id.clone()),
                has_payload: r.payload_ref.as_ref().map(|p| p.has_payload).unwrap_or(false),
            })
            .collect();

        KnowledgeBatch {
            rows,
            score_names: vec!["score".to_string()],
        }
    }

    /// Append another named `Float32` score column (e.g. a per-modality fusion
    /// sub-score alongside the fused `"score"` column), aligned to `self.rows` by
    /// index. Errors (rather than silently truncating/padding) when `values` isn't
    /// exactly one entry per row.
    pub fn with_named_score(
        mut self,
        name: impl Into<String>,
        values: Vec<Option<f32>>,
    ) -> Result<Self, String> {
        if values.len() != self.rows.len() {
            return Err(format!(
                "named score column has {} values, batch has {} rows",
                values.len(),
                self.rows.len()
            ));
        }
        let name = name.into();
        for (row, v) in self.rows.iter_mut().zip(values) {
            row.scores.push((name.clone(), v));
        }
        self.score_names.push(name);
        Ok(self)
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// A cursor positioned at the end of THIS batch, as if it were the only/last
    /// batch in the stream (`exhausted: true`) — a bare position snapshot with no
    /// way to advance; see [`Self::into_chunks`]/[`ChunkedKnowledgeCursor`] for the
    /// REAL chunked driver (CONCEPT:EG-P1-4).
    pub fn cursor(&self) -> KnowledgeCursor {
        KnowledgeCursor {
            offset: self.len(),
            total: Some(self.len()),
            exhausted: true,
        }
    }

    /// Turn this batch into a [`ChunkedKnowledgeCursor`] yielding up to `chunk_size`
    /// rows per [`ChunkedKnowledgeCursor::next_chunk`] call (CONCEPT:EG-P1-4, closes the L23
    /// stub) — the real streaming consumer a caller drains instead of holding the
    /// whole batch in flight at once.
    pub fn into_chunks(self, chunk_size: usize) -> ChunkedKnowledgeCursor {
        ChunkedKnowledgeCursor::new(self, chunk_size)
    }

    /// The Arrow [`Schema`] this batch converts to, given its current
    /// `score_names` (the score columns are the only schema-shape-varying part).
    pub fn arrow_schema(&self) -> Schema {
        arrow_schema(&self.score_names)
    }

    /// Convert to a real Arrow [`RecordBatch`] — the wire-compatible columnar form
    /// (CONCEPT:EG-P1-2). See the module docs' column-layout table.
    pub fn to_record_batch(&self) -> Result<RecordBatch, String> {
        let schema = Arc::new(self.arrow_schema());

        let mut columns: Vec<ArrayRef> = Vec::new();

        columns.push(Arc::new(StringArray::from(
            self.rows.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
        )));
        columns.push(Arc::new(StringArray::from(
            self.rows
                .iter()
                .map(|r| r.kind.as_str())
                .collect::<Vec<_>>(),
        )));

        for name in &self.score_names {
            let vals: Vec<Option<f32>> = self
                .rows
                .iter()
                .map(|r| {
                    r.scores
                        .iter()
                        .find(|(n, _)| n == name)
                        .and_then(|(_, v)| *v)
                })
                .collect();
            columns.push(Arc::new(Float32Array::from(vals)));
        }

        columns.push(Arc::new(Float64Array::from(
            self.rows.iter().map(|r| r.confidence).collect::<Vec<_>>(),
        )));

        columns.push(Arc::new(StringArray::from(
            self.rows
                .iter()
                .map(|r| r.evidence_refs.first().map(evidence_kind_tag))
                .collect::<Vec<_>>(),
        )));

        columns.push(build_string_list(self.rows.iter().map(|r| {
            r.evidence_refs
                .iter()
                .map(|e| serde_json::to_string(e).unwrap_or_default())
                .collect::<Vec<_>>()
        })));

        columns.push(Arc::new(Int64Array::from(
            self.rows
                .iter()
                .map(|r| r.valid_time.0.map(|v| v as i64))
                .collect::<Vec<_>>(),
        )));
        columns.push(Arc::new(Int64Array::from(
            self.rows
                .iter()
                .map(|r| r.valid_time.1.map(|v| v as i64))
                .collect::<Vec<_>>(),
        )));
        columns.push(Arc::new(Int64Array::from(
            self.rows
                .iter()
                .map(|r| r.tx_time.0.map(|v| v as i64))
                .collect::<Vec<_>>(),
        )));
        columns.push(Arc::new(Int64Array::from(
            self.rows
                .iter()
                .map(|r| r.tx_time.1.map(|v| v as i64))
                .collect::<Vec<_>>(),
        )));

        columns.push(build_string_list(
            self.rows.iter().map(|r| r.source_refs.clone()),
        ));
        columns.push(build_string_list(
            self.rows.iter().map(|r| r.policy_labels.clone()),
        ));
        columns.push(build_string_list(
            self.rows.iter().map(|r| r.transformation_ids.clone()),
        ));
        columns.push(build_string_list(
            self.rows.iter().map(|r| r.proof_ids.clone()),
        ));
        columns.push(build_string_list(
            self.rows.iter().map(|r| r.alternative_ids.clone()),
        ));
        columns.push(build_string_list(
            self.rows.iter().map(|r| r.contradiction_ids.clone()),
        ));

        columns.push(Arc::new(StringArray::from(
            self.rows
                .iter()
                .map(|r| r.blob_handle.as_deref())
                .collect::<Vec<_>>(),
        )));
        columns.push(Arc::new(BooleanArray::from(
            self.rows.iter().map(|r| r.has_payload).collect::<Vec<_>>(),
        )));

        RecordBatch::try_new(schema, columns).map_err(|e| format!("arrow record batch: {e}"))
    }

    /// Reconstruct a `KnowledgeBatch` from a [`RecordBatch`] previously produced by
    /// [`Self::to_record_batch`] — the inverse conversion, lossless for every
    /// column except `evidence_refs_json`'s malformed-JSON edge case (a value that
    /// fails to parse back as an `EvidenceSpan` is dropped from that row's
    /// `evidence_refs`, never fabricated) and `score_<name>` columns not shaped
    /// like `Float32` (treated as absent for that name). Column order does not
    /// matter — columns are looked up BY NAME, so a schema built by
    /// [`Self::arrow_schema`] with a different `score_names` order still round-trips.
    pub fn from_record_batch(batch: &RecordBatch) -> Result<KnowledgeBatch, String> {
        let schema = batch.schema();
        let nrows = batch.num_rows();

        let col = |name: &str| -> Result<&ArrayRef, String> {
            let idx = schema
                .index_of(name)
                .map_err(|e| format!("missing column {name}: {e}"))?;
            Ok(batch.column(idx))
        };
        let as_utf8 = |name: &str| -> Result<Vec<Option<String>>, String> {
            let arr = col(name)?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| format!("column {name} is not Utf8"))?;
            Ok((0..nrows)
                .map(|i| (!arr.is_null(i)).then(|| arr.value(i).to_string()))
                .collect())
        };
        let as_string_list = |name: &str| -> Result<Vec<Vec<String>>, String> {
            let arr = col(name)?
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| format!("column {name} is not a List"))?;
            (0..nrows)
                .map(|i| {
                    if arr.is_null(i) {
                        return Ok(Vec::new());
                    }
                    let inner = arr.value(i);
                    let strs = inner
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| format!("column {name} elements are not Utf8"))?;
                    Ok((0..strs.len())
                        .filter(|&j| !strs.is_null(j))
                        .map(|j| strs.value(j).to_string())
                        .collect())
                })
                .collect()
        };
        let as_i64 = |name: &str| -> Result<Vec<Option<i64>>, String> {
            let arr = col(name)?
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| format!("column {name} is not Int64"))?;
            Ok((0..nrows)
                .map(|i| (!arr.is_null(i)).then(|| arr.value(i)))
                .collect())
        };

        let ids = as_utf8("id")?;
        let kinds = as_utf8("kind")?;
        let confidence_arr = col("confidence")?
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or("column confidence is not Float64")?;
        let evidence_json = as_string_list("evidence_refs_json")?;
        let valid_from = as_i64("valid_from")?;
        let valid_until = as_i64("valid_until")?;
        let tx_from = as_i64("tx_from")?;
        let tx_to = as_i64("tx_to")?;
        let source_refs = as_string_list("source_refs")?;
        let policy_labels = as_string_list("policy_labels")?;
        let transformation_ids = as_string_list("transformation_ids")?;
        let proof_ids = as_string_list("proof_ids")?;
        let alternative_ids = as_string_list("alternative_ids")?;
        let contradiction_ids = as_string_list("contradiction_ids")?;
        let blob_handles = as_utf8("blob_handle")?;
        let has_payload_arr = col("has_payload")?
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or("column has_payload is not Boolean")?;

        // Score columns: every `score_*` field in the schema, in schema order.
        let score_names: Vec<String> = schema
            .fields()
            .iter()
            .filter_map(|f| f.name().strip_prefix("score_").map(|s| s.to_string()))
            .collect();
        let mut score_cols: Vec<(String, Vec<Option<f32>>)> = Vec::new();
        for name in &score_names {
            let arr = col(&format!("score_{name}"))?
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| format!("column score_{name} is not Float32"))?;
            let vals = (0..nrows)
                .map(|i| (!arr.is_null(i)).then(|| arr.value(i)))
                .collect();
            score_cols.push((name.clone(), vals));
        }

        let rows = (0..nrows)
            .map(|i| {
                let scores = score_cols
                    .iter()
                    .map(|(name, vals)| (name.clone(), vals[i]))
                    .collect();
                let evidence_refs = evidence_json[i]
                    .iter()
                    .filter_map(|s| serde_json::from_str::<EvidenceSpan>(s).ok())
                    .collect();
                KnowledgeBatchRow {
                    id: ids[i].clone().unwrap_or_default(),
                    kind: kinds[i].clone().unwrap_or_default(),
                    scores,
                    confidence: confidence_arr.value(i),
                    evidence_refs,
                    valid_time: (
                        valid_from[i].map(|v| v as u64),
                        valid_until[i].map(|v| v as u64),
                    ),
                    tx_time: (tx_from[i].map(|v| v as u64), tx_to[i].map(|v| v as u64)),
                    source_refs: source_refs[i].clone(),
                    policy_labels: policy_labels[i].clone(),
                    transformation_ids: transformation_ids[i].clone(),
                    proof_ids: proof_ids[i].clone(),
                    alternative_ids: alternative_ids[i].clone(),
                    contradiction_ids: contradiction_ids[i].clone(),
                    blob_handle: blob_handles[i].clone(),
                    has_payload: has_payload_arr.value(i),
                }
            })
            .collect();

        Ok(KnowledgeBatch { rows, score_names })
    }
}

/// Build a real Arrow `List<Utf8>` column from one `Vec<String>` per row.
fn build_string_list<I>(rows: I) -> ArrayRef
where
    I: IntoIterator<Item = Vec<String>>,
{
    let mut builder = ListBuilder::new(StringBuilder::new());
    for row in rows {
        builder.append_value(row.iter().map(|s| Some(s.as_str())));
    }
    Arc::new(builder.finish())
}

fn string_list_field(name: &str) -> Field {
    Field::new(
        name,
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        true,
    )
}

/// The Arrow [`Schema`] a [`KnowledgeBatch`] converts to, given its score column
/// names (see the module docs' column-layout table).
fn arrow_schema(score_names: &[String]) -> Schema {
    let mut fields = vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("kind", DataType::Utf8, true),
    ];
    for name in score_names {
        fields.push(Field::new(format!("score_{name}"), DataType::Float32, true));
    }
    fields.push(Field::new("confidence", DataType::Float64, false));
    fields.push(Field::new("evidence_kind", DataType::Utf8, true));
    fields.push(string_list_field("evidence_refs_json"));
    fields.push(Field::new("valid_from", DataType::Int64, true));
    fields.push(Field::new("valid_until", DataType::Int64, true));
    fields.push(Field::new("tx_from", DataType::Int64, true));
    fields.push(Field::new("tx_to", DataType::Int64, true));
    fields.push(string_list_field("source_refs"));
    fields.push(string_list_field("policy_labels"));
    fields.push(string_list_field("transformation_ids"));
    fields.push(string_list_field("proof_ids"));
    fields.push(string_list_field("alternative_ids"));
    fields.push(string_list_field("contradiction_ids"));
    fields.push(Field::new("blob_handle", DataType::Utf8, true));
    fields.push(Field::new("has_payload", DataType::Boolean, false));
    Schema::new(fields)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rowset::RowSet;
    use eg_core::graph::GraphCore;
    use serde_json::json;

    fn blob(v: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&v).unwrap()
    }

    /// Build a `KnowledgeSet` exercising confidence, evidence spans, the
    /// bitemporal window and (indirectly, via `epistemic`) policy labels — the
    /// fixture the workstream's test plan asked for.
    fn fixture_batch() -> KnowledgeBatch {
        let core = GraphCore::new();
        core.add_node(
            "sym1".into(),
            blob(json!({
                "node_type": "Symbol",
                "id": "sym:abc123",
                "name": "handle_request",
                "qualified_name": "crate::server::handle_request",
                "symbol_type": "Function",
                "file_path": "src/server.rs",
                "line_start": 42,
                "line_end": 88,
                "column": 0,
                "ast_hash": "deadbeef",
                "dependencies": [],
                "documentation": "",
                "language": "rust",
                "is_exported": true,
                "annotations": [],
                "byte_start": 900,
                "byte_end": 1500,
                "confidence": 0.82,
                "valid_from": 100,
                "valid_until": 200,
                "tx_from": 10,
                "tx_to": null,
            })),
        );
        core.add_node(
            "d2".into(),
            blob(json!({
                "node_type": "Doc",
                "confidence": 0.5,
            })),
        );
        let view = core.analysis_snapshot();
        let rs = RowSet::from_scored([("sym1".to_string(), 0.9_f32), ("d2".to_string(), 0.3_f32)]);
        let ks = KnowledgeSet::from_rowset(&rs, &view, &[]);
        assert_eq!(ks.len(), 2);
        KnowledgeBatch::from_knowledge_set(&ks)
    }

    #[test]
    fn schema_has_expected_typed_columns() {
        let kb = fixture_batch();
        let schema = kb.arrow_schema();

        let expect = [
            ("id", DataType::Utf8),
            ("kind", DataType::Utf8),
            ("score_score", DataType::Float32),
            ("confidence", DataType::Float64),
            ("evidence_kind", DataType::Utf8),
            (
                "evidence_refs_json",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            ),
            ("valid_from", DataType::Int64),
            ("valid_until", DataType::Int64),
            ("tx_from", DataType::Int64),
            ("tx_to", DataType::Int64),
            (
                "source_refs",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            ),
            (
                "policy_labels",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            ),
            ("blob_handle", DataType::Utf8),
            ("has_payload", DataType::Boolean),
        ];
        for (name, ty) in expect {
            let field = schema
                .field_with_name(name)
                .unwrap_or_else(|_| panic!("missing column {name}"));
            assert_eq!(field.data_type(), &ty, "column {name} has wrong type");
        }
    }

    #[test]
    fn to_record_batch_round_trips_values() {
        let kb = fixture_batch();
        let rb = kb.to_record_batch().expect("to_record_batch");
        assert_eq!(rb.num_rows(), 2);

        let back = KnowledgeBatch::from_record_batch(&rb).expect("from_record_batch");
        assert_eq!(back.len(), 2);

        let sym_row = back.rows.iter().find(|r| r.id == "sym1").unwrap();
        assert_eq!(sym_row.kind, "Symbol");
        assert_eq!(sym_row.confidence, 0.82);
        assert_eq!(sym_row.valid_time, (Some(100), Some(200)));
        assert_eq!(sym_row.tx_time, (Some(10), None));
        assert_eq!(
            sym_row.scores,
            vec![("score".to_string(), Some(0.9_f32))]
        );
        // `evidence_refs` is only resolved by `KnowledgeSet::from_rowset` under the
        // `epistemic` feature (see `knowledge.rs`'s module docs) — a plain
        // `knowledge-batch` build (epistemic off) faithfully carries the empty
        // `Vec` through the round trip; never fabricated either way.
        if cfg!(feature = "epistemic") {
            assert_eq!(
                sym_row.evidence_refs,
                vec![EvidenceSpan::CodeSymbol {
                    file_path: "src/server.rs".to_string(),
                    symbol: "handle_request".to_string(),
                    start_line: 42,
                    end_line: 88,
                }]
            );
        } else {
            assert!(sym_row.evidence_refs.is_empty());
        }
        assert!(sym_row.has_payload);
        assert_eq!(sym_row.blob_handle, Some("sym1".to_string()));

        let doc_row = back.rows.iter().find(|r| r.id == "d2").unwrap();
        assert_eq!(doc_row.confidence, 0.5);
        assert!(doc_row.evidence_refs.is_empty());
        assert_eq!(doc_row.scores, vec![("score".to_string(), Some(0.3_f32))]);
    }

    #[test]
    fn with_named_score_adds_a_second_typed_column() {
        let kb = fixture_batch()
            .with_named_score("bm25", vec![Some(3.2), Some(1.1)])
            .expect("with_named_score");
        assert_eq!(kb.score_names, vec!["score".to_string(), "bm25".to_string()]);

        let schema = kb.arrow_schema();
        assert_eq!(
            schema.field_with_name("score_bm25").unwrap().data_type(),
            &DataType::Float32
        );

        let rb = kb.to_record_batch().expect("to_record_batch");
        let back = KnowledgeBatch::from_record_batch(&rb).expect("from_record_batch");
        let sym_row = back.rows.iter().find(|r| r.id == "sym1").unwrap();
        assert!(sym_row
            .scores
            .contains(&("bm25".to_string(), Some(3.2_f32))));
    }

    #[test]
    fn with_named_score_rejects_length_mismatch() {
        let err = fixture_batch()
            .with_named_score("bad", vec![Some(1.0)])
            .unwrap_err();
        assert!(err.contains("2 rows"));
    }

    /// End-to-end with `epistemic` ALSO on (run with
    /// `--features knowledge-batch,epistemic`): a claim with confidence + a
    /// bitemporal window + an incoming `SUPPORTS` edge (-> `source_refs` +
    /// `"epistemic:asserted"` in `policy_labels`) round-trips its
    /// `source_refs`/`policy_labels` `List<Utf8>` columns losslessly through
    /// `to_record_batch`/`from_record_batch`.
    #[cfg(feature = "epistemic")]
    #[test]
    fn epistemic_policy_and_provenance_round_trip() {
        let core = GraphCore::new();
        core.add_node(
            "claim1".into(),
            blob(json!({
                "node_type": "Claim",
                "confidence": 0.66,
                "valid_from": 5,
                "valid_until": 50,
                "tx_from": 1,
                "tx_to": null,
            })),
        );
        core.add_node(
            "evidence1".into(),
            blob(json!({ "node_type": "Evidence", "confidence": 0.9 })),
        );
        core.add_edge(
            "evidence1".into(),
            "claim1".into(),
            blob(json!({ "relationship_type": "SUPPORTS" })),
        )
        .unwrap();
        let view = core.analysis_snapshot();
        let rs = RowSet::from_ids(["claim1".to_string()]);
        let ks = KnowledgeSet::from_rowset(&rs, &view, &[]);

        let kb = KnowledgeBatch::from_knowledge_set(&ks);
        let rb = kb.to_record_batch().expect("to_record_batch");
        let back = KnowledgeBatch::from_record_batch(&rb).expect("from_record_batch");

        let row = &back.rows[0];
        assert_eq!(row.confidence, 0.66);
        assert_eq!(row.valid_time, (Some(5), Some(50)));
        assert_eq!(row.tx_time, (Some(1), None));
        assert_eq!(row.source_refs, vec!["evidence1".to_string()]);
        assert_eq!(row.policy_labels, vec!["epistemic:asserted".to_string()]);
    }

    #[test]
    fn reserved_columns_are_empty_lists_not_fabricated() {
        let kb = fixture_batch();
        for row in &kb.rows {
            assert!(row.transformation_ids.is_empty());
            assert!(row.proof_ids.is_empty());
            assert!(row.alternative_ids.is_empty());
            assert!(row.contradiction_ids.is_empty());
        }
    }

    /// L22/CONCEPT:EG-P3-1: a claim with a `GENERATED_BY` edge (transformation), a
    /// transitive support chain (proof), and a `CONTRADICTS` edge (contradiction) —
    /// all four previously-reserved columns populate AND round-trip losslessly
    /// through `to_record_batch`/`from_record_batch`.
    #[cfg(feature = "epistemic")]
    #[test]
    fn transformation_proof_contradiction_columns_populate_and_round_trip() {
        let core = GraphCore::new();
        core.add_node(
            "claim1".into(),
            blob(json!({ "node_type": "Claim", "confidence": 0.6 })),
        );
        core.add_node(
            "mid".into(),
            blob(json!({ "node_type": "Claim", "confidence": 0.7 })),
        );
        core.add_node(
            "evidence1".into(),
            blob(json!({ "node_type": "Evidence", "confidence": 0.9 })),
        );
        core.add_node(
            "counter1".into(),
            blob(json!({ "node_type": "Evidence", "confidence": 0.8 })),
        );
        core.add_node(
            "activity1".into(),
            blob(json!({ "node_type": "Activity" })),
        );
        core.add_edge(
            "mid".into(),
            "claim1".into(),
            blob(json!({ "relationship_type": "SUPPORTS" })),
        )
        .unwrap();
        core.add_edge(
            "evidence1".into(),
            "mid".into(),
            blob(json!({ "relationship_type": "SUPPORTS" })),
        )
        .unwrap();
        core.add_edge(
            "counter1".into(),
            "claim1".into(),
            blob(json!({ "relationship_type": "CONTRADICTS" })),
        )
        .unwrap();
        core.add_edge(
            "claim1".into(),
            "activity1".into(),
            blob(json!({ "relationship_type": "GENERATED_BY" })),
        )
        .unwrap();
        let view = core.analysis_snapshot();
        let rs = RowSet::from_ids(["claim1".to_string()]);
        let ks = KnowledgeSet::from_rowset(&rs, &view, &[]);

        let kb = KnowledgeBatch::from_knowledge_set(&ks);
        let rb = kb.to_record_batch().expect("to_record_batch");
        let back = KnowledgeBatch::from_record_batch(&rb).expect("from_record_batch");

        let row = &back.rows[0];
        assert_eq!(row.transformation_ids, vec!["activity1".to_string()]);
        assert_eq!(row.contradiction_ids, vec!["counter1".to_string()]);
        // `proof_ids` is the FULL justification tree — every premise regardless of
        // polarity, so it includes `counter1` (a `DerivedContradiction` premise) too,
        // not just the supporting chain.
        let mut proof = row.proof_ids.clone();
        proof.sort();
        assert_eq!(
            proof,
            vec!["counter1".to_string(), "evidence1".to_string(), "mid".to_string()]
        );
    }

    #[test]
    fn cursor_reports_exhausted_stub() {
        let kb = fixture_batch();
        let cursor = kb.cursor();
        assert_eq!(cursor.offset, 2);
        assert_eq!(cursor.total, Some(2));
        assert!(cursor.exhausted);
    }

    #[test]
    fn empty_knowledge_set_converts_to_zero_row_batch() {
        let ks = KnowledgeSet::default();
        let kb = KnowledgeBatch::from_knowledge_set(&ks);
        assert!(kb.is_empty());
        let rb = kb.to_record_batch().expect("to_record_batch");
        assert_eq!(rb.num_rows(), 0);
        assert_eq!(rb.num_columns(), kb.arrow_schema().fields().len());
    }

    // ── ChunkedKnowledgeCursor (CONCEPT:EG-P1-4, closes the L23 stub) ────────────────

    /// A `KnowledgeBatch` of `n` rows (`n0`..`n{n-1}`), for exercising chunked streaming
    /// over a "large" result — big enough that a small `chunk_size` yields several
    /// chunks.
    fn large_batch(n: usize) -> KnowledgeBatch {
        let core = GraphCore::new();
        let mut ids = Vec::with_capacity(n);
        for i in 0..n {
            let id = format!("n{i}");
            core.add_node(id.clone(), blob(json!({"node_type": "Item"})));
            ids.push(id);
        }
        let view = core.analysis_snapshot();
        let rs = RowSet::from_ids(ids);
        let ks = KnowledgeSet::from_rowset(&rs, &view, &[]);
        assert_eq!(ks.len(), n);
        KnowledgeBatch::from_knowledge_set(&ks)
    }

    /// A large result streams in MULTIPLE non-empty chunks, then exhausts: 25 rows at
    /// chunk_size 10 yields chunks of 10, 10, 5, then `None` — the canonical shape the
    /// task's test plan asked for.
    #[test]
    fn chunked_cursor_streams_multiple_nonempty_chunks_then_exhausts() {
        let kb = large_batch(25);
        let mut cursor = kb.into_chunks(10);

        assert!(!cursor.is_exhausted());
        let c1 = cursor.next_chunk().expect("chunk 1");
        assert_eq!(c1.len(), 10);
        assert!(!cursor.is_exhausted());

        let c2 = cursor.next_chunk().expect("chunk 2");
        assert_eq!(c2.len(), 10);
        assert!(!cursor.is_exhausted());

        let c3 = cursor.next_chunk().expect("chunk 3");
        assert_eq!(c3.len(), 5);
        assert!(cursor.is_exhausted());

        assert!(
            cursor.next_chunk().is_none(),
            "an exhausted cursor yields no further chunks"
        );
        assert_eq!(cursor.position().offset, 25);
        assert_eq!(cursor.position().total, Some(25));
        assert!(cursor.position().exhausted);
    }

    /// EQUIVALENCE: concatenating every chunk a cursor yields reconstructs the exact
    /// same row sequence (ids, in order) as the source batch — chunking never drops,
    /// duplicates, or reorders a row.
    #[test]
    fn chunked_cursor_concatenation_equals_source_batch() {
        let kb = large_batch(23);
        let expected: Vec<String> = kb.rows.iter().map(|r| r.id.clone()).collect();
        let score_names = kb.score_names.clone();

        let mut cursor = kb.into_chunks(7);
        let mut got: Vec<String> = Vec::new();
        let mut chunk_count = 0;
        while let Some(chunk) = cursor.next_chunk() {
            assert!(!chunk.is_empty(), "next_chunk never yields an empty chunk");
            assert_eq!(
                chunk.score_names, score_names,
                "every chunk shares the source's Arrow schema (score_names)"
            );
            got.extend(chunk.rows.into_iter().map(|r| r.id));
            chunk_count += 1;
        }
        assert_eq!(got, expected);
        assert_eq!(chunk_count, 4, "23 rows at chunk_size 7 ⇒ 7,7,7,2 = 4 chunks");
    }

    /// An empty batch is immediately exhausted — no chunk to yield, ever.
    #[test]
    fn chunked_cursor_over_empty_batch_is_immediately_exhausted() {
        let kb = KnowledgeBatch::default();
        let mut cursor = kb.into_chunks(10);
        assert!(cursor.is_exhausted());
        assert_eq!(cursor.total(), 0);
        assert!(cursor.next_chunk().is_none());
    }

    /// `chunk_size: 0` is clamped to `1` rather than wedging the cursor (which would
    /// never advance `offset` and loop forever).
    #[test]
    fn chunked_cursor_clamps_zero_chunk_size_to_one() {
        let kb = large_batch(3);
        let mut cursor = kb.into_chunks(0);
        let mut n = 0;
        while let Some(chunk) = cursor.next_chunk() {
            assert_eq!(chunk.len(), 1, "clamped to chunk_size 1");
            n += 1;
            assert!(n <= 10, "cursor must terminate, not loop forever");
        }
        assert_eq!(n, 3);
    }
}
