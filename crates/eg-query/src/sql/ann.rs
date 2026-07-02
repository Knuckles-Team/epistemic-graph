//! Real pgvector ANN top-k pushdown execution (CONCEPT:EG-313).
//!
//! EG-116 recognized `CREATE INDEX … USING hnsw|ivfflat (col opclass)` (an
//! [`AnnIndexPlan`]) and a `SELECT … FROM t ORDER BY col <-> $q LIMIT k` nearest-
//! neighbour query ([`plan_ann_search`](super::pgfamily::plan_ann_search)), but the
//! actual top-k search still fell back to the EG-115 brute-force `vector_l2()`
//! full-scan+sort. THIS module is the deferred half: when a matching hnsw/ivfflat
//! index exists for the column, the query is executed by building/consulting a REAL
//! [`eg_ann`] index (HNSW for `hnsw`, IVF-PQ for `ivfflat`) over the column's
//! vectors and returning the true top-k — instead of brute force. When no index
//! covers the `(table, column, metric)`, the caller keeps the brute-force path.
//!
//! ## Correctness + determinism (CONCEPT:EG-313)
//! The graph ANN indices (HNSW graph traversal, IVF-PQ coarse+PQ codes) are
//! *approximate*. To return the TRUE nearest-k (matching a brute-force reference)
//! this module uses the standard "ANN recall + exact rerank" pipeline eg-ann was
//! built for (CONCEPT:EG-297): consult the ANN index for an over-fetched candidate
//! pool, then re-score those candidates on their FULL f32 vectors with the exact
//! [`FlatIndex`] and take the precise top-k. Every stage is deterministic —
//! eg-ann's level assignment is a clock-free hash, its k-means is `ChaCha8Rng`-
//! seeded, and every tie breaks by ascending id — so two runs over the same rows
//! yield bit-identical top-k. On a set small enough that the pool covers every
//! candidate (the unit-test regime), the result is EXACTLY the brute-force top-k;
//! at scale it is a high-recall ANN whose recall is tunable via the pool/probe
//! parameters (a documented perf follow-up).

use arrow::array::{Array, FixedSizeListArray, Float32Array, ListArray, UInt32Array};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;

use eg_ann::{FlatIndex, HnswIndex, IvfPq, IvfPqParams, Metric, SearchParams};

use super::pgfamily::{AnnMethod, VectorMetric};

/// Deterministic seed for every eg-ann index built for a pushdown (CONCEPT:EG-313) —
/// fixed so repeated runs of the same query are bit-identical.
const ANN_SEED: u64 = 0x_E6_0313;
/// Candidate over-fetch factor: pull `k * OVERFETCH` candidates from the ANN index
/// before the exact rerank, so the true top-k is (almost surely) inside the pool.
const OVERFETCH: usize = 16;
/// Minimum candidate-pool floor — guarantees the exact rerank sees enough rows even
/// for a tiny `k`, and makes small sets (the test regime) exhaustive ⇒ exact.
const POOL_FLOOR: usize = 64;
/// HNSW build fan-out (`M`) and construction beam — modest, high-recall defaults.
const HNSW_M: usize = 16;
const HNSW_EF_CONSTRUCTION: usize = 200;

/// Map a pgvector [`VectorMetric`] (from the opclass / distance operator) to the
/// eg-ann [`Metric`] (CONCEPT:EG-313). `<->`→L2, `<=>`→cosine, `<#>`→(neg)inner.
pub(crate) fn metric_to_ann(metric: VectorMetric) -> Metric {
    match metric {
        VectorMetric::L2 => Metric::L2,
        VectorMetric::Cosine => Metric::Cosine,
        VectorMetric::InnerProduct => Metric::InnerProduct,
    }
}

/// Decode the query-vector operand of an [`AnnSearchPlan`](super::pgfamily::AnnSearchPlan)
/// (its `query` SQL text) into a dense `Vec<f32>` (CONCEPT:EG-313). Accepts the
/// pgvector literal forms `'[1,2,3]'` / `[1,2,3]` (with or without the surrounding
/// single quotes). Returns `None` for an unresolved bind placeholder (`$1`) or any
/// non-literal the pushdown cannot resolve here — the caller then keeps the
/// brute-force path.
pub(crate) fn parse_query_vector(raw: &str) -> Option<Vec<f32>> {
    let s = raw.trim().trim_matches('\'').trim();
    if s.is_empty() || s.starts_with('$') {
        return None;
    }
    crate::tables::schema::parse_vector_text(s)
        .ok()
        .filter(|v| !v.is_empty())
}

/// Whether `sql` carries a `WHERE` clause (CONCEPT:EG-313). The ANN pushdown pre-
/// selects the top-k rows over the WHOLE column, so a `WHERE` would change pgvector's
/// filter-THEN-rank semantics; the caller declines the pushdown (keeps brute force)
/// when this is true. A lightweight, parse-free scan sufficient for the recognized
/// single-table top-k shape.
pub(crate) fn sql_has_where(sql: &str) -> bool {
    let lower = sql.to_ascii_lowercase();
    // Match a ` where ` token (whitespace-delimited) so we don't trip on identifiers.
    lower
        .match_indices("where")
        .any(|(i, _)| {
            let before = lower.as_bytes().get(i.wrapping_sub(1)).copied();
            let after = lower.as_bytes().get(i + 5).copied();
            let ok_before = i == 0 || before.map(|b| b.is_ascii_whitespace()).unwrap_or(false);
            let ok_after = after.map(|b| b.is_ascii_whitespace()).unwrap_or(true);
            ok_before && ok_after
        })
}

/// Integer square root (floor) — used to size the IVF `nlist` ≈ √N without pulling a
/// nightly `usize::isqrt`.
fn isqrt(n: usize) -> usize {
    if n == 0 {
        return 0;
    }
    let mut x = (n as f64).sqrt() as usize;
    while x.saturating_mul(x) > n {
        x -= 1;
    }
    while (x + 1).saturating_mul(x + 1) <= n {
        x += 1;
    }
    x
}

/// Consult a REAL eg-ann index over `vectors` and return the TRUE top-`k` row
/// positions (indices into `vectors`), nearest-first (CONCEPT:EG-313).
///
/// * `method` — `hnsw` ⇒ [`HnswIndex`], `ivfflat` ⇒ [`IvfPq`]; the index type the
///   `CREATE INDEX` registered.
/// * `metric` — the pgvector distance the `<->`/`<=>`/`<#>` operator selects.
///
/// The ANN index yields an over-fetched candidate pool; the exact [`FlatIndex`]
/// re-scores it on the full f32 vectors so the returned k is the precise nearest-k
/// (deterministic, ties broken by ascending row index).
pub(crate) fn ann_topk_rows(
    method: AnnMethod,
    metric: VectorMetric,
    vectors: &[Vec<f32>],
    query: &[f32],
    k: usize,
) -> Vec<usize> {
    let n = vectors.len();
    let dim = query.len();
    if k == 0 || n == 0 || dim == 0 {
        return Vec::new();
    }
    let ann_metric = metric_to_ann(metric);
    let pool = n.min(k.saturating_mul(OVERFETCH).max(POOL_FLOOR));

    // Row position IS the eg-ann id, so results map straight back to the batch row.
    let items: Vec<(u64, Vec<f32>)> = vectors
        .iter()
        .enumerate()
        .map(|(i, v)| (i as u64, v.clone()))
        .collect();

    // 1. Consult the registered index type for a candidate pool.
    let candidates: Vec<u64> = match method {
        AnnMethod::Hnsw => {
            let mut idx = HnswIndex::new(dim, ann_metric, HNSW_M, HNSW_EF_CONSTRUCTION, ANN_SEED);
            idx.insert_batch(&items);
            // ef ≥ pool so a small set is explored exhaustively (⇒ exact rerank).
            idx.search(query, pool, pool)
                .into_iter()
                .map(|r| r.id)
                .collect()
        }
        AnnMethod::IvfFlat => {
            // m = 1 (dsub = dim) divides EVERY dim, so any embedding width trains;
            // the PQ approximation is irrelevant because FlatIndex reranks exactly.
            let nlist = isqrt(n).max(1).min(n);
            let params = IvfPqParams {
                dim,
                nlist,
                m: 1,
                kmeans_iters: 15,
                opq_iters: 0,
                seed: ANN_SEED,
            };
            let mut idx = IvfPq::train(&params, vectors);
            idx.add(&items);
            let sp = SearchParams {
                nprobe: nlist, // probe every cell so the pool covers small sets ⇒ exact
                refine: true,
                refine_factor: OVERFETCH,
            };
            idx.search(query, pool, sp)
                .into_iter()
                .map(|r| r.id)
                .collect()
        }
    };

    // 2. Exact rerank the candidate pool on full f32 vectors → true top-k.
    let mut flat = FlatIndex::new(dim);
    flat.add(&items);
    flat.rerank_metric(query, &candidates, k, ann_metric)
        .into_iter()
        .map(|r| r.id as usize)
        .collect()
}

/// Extract the dense f32 vectors of a `List<Float32>` / `FixedSizeList<Float32>`
/// column at `col_idx` (CONCEPT:EG-313): returns `(orig_row_index, vector)` for every
/// row whose vector is non-null AND matches `dim`. Rows failing either are skipped
/// (they can't be a nearest-neighbour under a well-formed query anyway).
fn column_vectors(batch: &RecordBatch, col_idx: usize, dim: usize) -> Vec<(usize, Vec<f32>)> {
    let col = batch.column(col_idx);
    let mut out = Vec::with_capacity(batch.num_rows());
    let mut push_row = |row: usize, floats: &Float32Array| {
        if floats.len() == dim && (0..floats.len()).all(|i| !floats.is_null(i)) {
            out.push((row, (0..dim).map(|i| floats.value(i)).collect::<Vec<f32>>()));
        }
    };
    if let Some(la) = col.as_any().downcast_ref::<ListArray>() {
        for row in 0..la.len() {
            if la.is_null(row) {
                continue;
            }
            let child = la.value(row);
            if let Some(f) = child.as_any().downcast_ref::<Float32Array>() {
                push_row(row, f);
            }
        }
    } else if let Some(fsl) = col.as_any().downcast_ref::<FixedSizeListArray>() {
        for row in 0..fsl.len() {
            if fsl.is_null(row) {
                continue;
            }
            let child = fsl.value(row);
            if let Some(f) = child.as_any().downcast_ref::<Float32Array>() {
                push_row(row, f);
            }
        }
    }
    out
}

/// Whether an Arrow field is a pgvector `vector` column — a `List`/`FixedSizeList`
/// with a `Float32` element (CONCEPT:EG-115/EG-313).
fn is_vector_field(dt: &DataType) -> bool {
    matches!(dt, DataType::List(f) | DataType::FixedSizeList(f, _) if *f.data_type() == DataType::Float32)
}

/// Execute the ANN top-k pushdown over one materialized table batch (CONCEPT:EG-313):
/// build/consult the registered eg-ann index over `column`'s vectors, then return a
/// NEW batch narrowed to the true nearest-k rows, in nearest-first order. Returns
/// `None` (⇒ the caller keeps the brute-force full-scan) when the pushdown does not
/// apply: the column is absent / not a vector, the query vector's dimension does not
/// match the stored vectors, or fewer than `k` usable (non-null, right-dim) rows
/// exist (so brute force's null-last ordering is preserved).
///
/// Re-running the ORIGINAL SQL over this k-row batch reproduces the exact projection,
/// column types, and (trivially, over k rows) the same nearest-first ORDER BY — the
/// eg-ann index has already done the top-k selection the brute-force scan would.
pub(crate) fn topk_slice(
    schema: &SchemaRef,
    batch: &RecordBatch,
    column: &str,
    method: AnnMethod,
    metric: VectorMetric,
    query: &[f32],
    k: usize,
) -> Option<RecordBatch> {
    if k == 0 {
        return None;
    }
    let col_idx = schema
        .fields()
        .iter()
        .position(|f| f.name().eq_ignore_ascii_case(column))?;
    if !is_vector_field(schema.field(col_idx).data_type()) {
        return None;
    }
    let dim = query.len();
    let live = column_vectors(batch, col_idx, dim);
    // Preserve brute-force semantics (null/wrong-dim rows sort last) — only push down
    // when there are at least k fully-usable rows.
    if live.len() < k {
        return None;
    }
    let vectors: Vec<Vec<f32>> = live.iter().map(|(_, v)| v.clone()).collect();
    let top = ann_topk_rows(method, metric, &vectors, query, k);
    if top.is_empty() {
        return None;
    }
    // Map the top-k positions (into `live`) back to original batch row indices.
    let indices: UInt32Array = top.iter().map(|&p| live[p].0 as u32).collect();
    arrow::compute::take_record_batch(batch, &indices).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float32Builder, Int64Array, ListBuilder};
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    /// A brute-force exact top-k reference (CONCEPT:EG-313) over the SAME rows the
    /// pushdown sees — the ground truth every ANN-pushdown test is measured against.
    fn brute_topk(vectors: &[Vec<f32>], query: &[f32], k: usize, metric: Metric) -> Vec<usize> {
        let mut scored: Vec<(usize, f32)> = vectors
            .iter()
            .enumerate()
            .map(|(i, v)| (i, metric.distance(query, v)))
            .collect();
        scored.sort_by(|a, b| {
            a.1.partial_cmp(&b.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });
        scored.truncate(k);
        scored.into_iter().map(|(i, _)| i).collect()
    }

    /// Deterministic, well-separated clustered vectors (distinct distances ⇒ no ties).
    fn sample(n: usize, dim: usize) -> Vec<Vec<f32>> {
        (0..n)
            .map(|i| {
                (0..dim)
                    .map(|j| ((i * 7 + j * 3) % 19) as f32 + (i as f32) * 0.001)
                    .collect()
            })
            .collect()
    }

    #[test]
    fn eg313_hnsw_topk_matches_brute_force_l2() {
        let dim = 8;
        let vectors = sample(40, dim);
        let query: Vec<f32> = (0..dim).map(|j| (j as f32) * 1.5 + 2.0).collect();
        for k in [1usize, 3, 10] {
            let got = ann_topk_rows(AnnMethod::Hnsw, VectorMetric::L2, &vectors, &query, k);
            let want = brute_topk(&vectors, &query, k, Metric::L2);
            assert_eq!(got, want, "hnsw L2 top-{k} must equal brute force");
        }
    }

    #[test]
    fn eg313_ivfflat_topk_matches_brute_force_l2() {
        let dim = 6;
        let vectors = sample(50, dim);
        let query: Vec<f32> = (0..dim).map(|j| (j as f32) + 0.5).collect();
        for k in [1usize, 5] {
            let got = ann_topk_rows(AnnMethod::IvfFlat, VectorMetric::L2, &vectors, &query, k);
            let want = brute_topk(&vectors, &query, k, Metric::L2);
            assert_eq!(got, want, "ivfflat L2 top-{k} must equal brute force");
        }
    }

    #[test]
    fn eg313_topk_cosine_and_inner_product_ops() {
        let dim = 5;
        let vectors = sample(30, dim);
        let query: Vec<f32> = (0..dim).map(|j| (j as f32) * 0.7 + 1.0).collect();
        // cosine (`<=>`)
        let got_c = ann_topk_rows(AnnMethod::Hnsw, VectorMetric::Cosine, &vectors, &query, 4);
        let want_c = brute_topk(&vectors, &query, 4, Metric::Cosine);
        assert_eq!(got_c, want_c, "hnsw cosine top-4 must equal brute force");
        // negative inner product (`<#>`)
        let got_i =
            ann_topk_rows(AnnMethod::IvfFlat, VectorMetric::InnerProduct, &vectors, &query, 4);
        let want_i = brute_topk(&vectors, &query, 4, Metric::InnerProduct);
        assert_eq!(got_i, want_i, "ivfflat inner-product top-4 must equal brute force");
    }

    #[test]
    fn eg313_topk_is_deterministic() {
        let dim = 8;
        let vectors = sample(45, dim);
        let query: Vec<f32> = (0..dim).map(|j| (j as f32) - 1.0).collect();
        let a = ann_topk_rows(AnnMethod::Hnsw, VectorMetric::L2, &vectors, &query, 7);
        let b = ann_topk_rows(AnnMethod::Hnsw, VectorMetric::L2, &vectors, &query, 7);
        assert_eq!(a, b, "repeated pushdown top-k must be bit-identical");
    }

    #[test]
    fn eg313_parse_query_vector_forms() {
        assert_eq!(parse_query_vector("'[1,2,3]'"), Some(vec![1.0, 2.0, 3.0]));
        assert_eq!(parse_query_vector("[0.5, 0.25]"), Some(vec![0.5, 0.25]));
        assert_eq!(parse_query_vector("$1"), None, "unresolved bind param");
        assert_eq!(parse_query_vector("'[]'"), None, "empty vector");
    }

    #[test]
    fn eg313_sql_has_where_detects_filter() {
        assert!(sql_has_where(
            "SELECT id FROM t WHERE a = 1 ORDER BY emb <-> '[1,2]' LIMIT 3"
        ));
        assert!(!sql_has_where(
            "SELECT id FROM t ORDER BY emb <-> '[1,2]' LIMIT 3"
        ));
        // `whereabouts` is an identifier, not a WHERE clause.
        assert!(!sql_has_where("SELECT whereabouts FROM t LIMIT 1"));
    }

    /// Build a `(id Int64, emb List<Float32>)` batch for the slice-level test.
    fn vec_batch(vectors: &[Vec<f32>]) -> (SchemaRef, RecordBatch) {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "emb",
                DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
                true,
            ),
        ]));
        let ids = Int64Array::from((0..vectors.len() as i64).collect::<Vec<_>>());
        let mut lb = ListBuilder::new(Float32Builder::new());
        for v in vectors {
            for &x in v {
                lb.values().append_value(x);
            }
            lb.append(true);
        }
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(ids), Arc::new(lb.finish())],
        )
        .unwrap();
        (schema, batch)
    }

    #[test]
    fn eg313_topk_slice_returns_nearest_k_rows_in_order() {
        let dim = 4;
        let vectors = sample(25, dim);
        let (schema, batch) = vec_batch(&vectors);
        let query: Vec<f32> = (0..dim).map(|j| (j as f32) + 0.3).collect();
        let sliced =
            topk_slice(&schema, &batch, "emb", AnnMethod::Hnsw, VectorMetric::L2, &query, 5).unwrap();
        assert_eq!(sliced.num_rows(), 5, "slice must have exactly k rows");
        // The `id` column of the sliced batch (id == original row) must equal the
        // brute-force nearest-5, in nearest-first order.
        let want = brute_topk(&vectors, &query, 5, Metric::L2);
        let ids = sliced
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let got: Vec<usize> = (0..ids.len()).map(|i| ids.value(i) as usize).collect();
        assert_eq!(got, want, "sliced rows must be the true nearest-k in order");
    }

    #[test]
    fn eg313_topk_slice_declines_when_fewer_than_k_usable_rows() {
        let dim = 3;
        let vectors = sample(2, dim); // only 2 rows
        let (schema, batch) = vec_batch(&vectors);
        let query = vec![1.0, 2.0, 3.0];
        // k=5 > 2 usable rows ⇒ decline (caller keeps brute force).
        assert!(
            topk_slice(&schema, &batch, "emb", AnnMethod::Hnsw, VectorMetric::L2, &query, 5)
                .is_none()
        );
    }
}
