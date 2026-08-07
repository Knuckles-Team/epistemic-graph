//! Sketch-backed SQL aggregate UDFs (CONCEPT:EG-KG.query.approx-distinct-cardinality, W4.5/N5):
//!
//!   * `approx_distinct(expr) -> Float64` — HyperLogLog-estimated `COUNT(DISTINCT expr)` in
//!     O(1) memory regardless of cardinality (BigQuery/Presto/Spark SQL's own `APPROX_DISTINCT`
//!     naming convention).
//!   * `approx_frequency(expr, probe) -> Float64` — Count-Min-Sketch-estimated occurrence count
//!     of the literal `probe` within `expr`'s values (never an underestimate).
//!   * `minhash_signature(expr) -> List<UInt64>` — a MinHash signature summarizing the SET of
//!     `expr`'s distinct values, from which [`minhash_similarity_udf`] estimates Jaccard
//!     similarity between two groups' signatures without ever joining their rows.
//!
//! Always-on under the base `sql` feature (no extra feature gate): the underlying sketches
//! (`eg_compute::sketch`) are pure-Rust with zero heavy deps — `eg-query` already links
//! `eg-compute` unconditionally — so this adds nothing beyond what `sql` already pulls.
//!
//! Every aggregate's STATE (the `Accumulator::state`/`merge_batch` pair DataFusion uses to
//! combine partial results across partitions) is the sketch's own raw byte/list representation,
//! round-tripped through each sketch's `from_raw_*`/`from_signature` reconstructor — the SAME
//! sketches, not a parallel serialization scheme.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, Float64Array, ListArray, StringArray, UInt64Array,
};
use arrow::datatypes::{DataType, Field};
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::{
    create_udaf, create_udf, Accumulator, AggregateUDF, ColumnarValue, ScalarUDF, Volatility,
};
use datafusion::scalar::ScalarValue;

use eg_compute::sketch::{CountMinSketch, HyperLogLog, MinHashSketch};

/// The Arrow element type every `List<UInt64>` signature column uses (MinHash's raw hash
/// values) — shared by the aggregate's return/state type and the scalar similarity function's
/// input type, so the two compose without an intermediate cast.
fn uint64_list_type() -> DataType {
    DataType::List(Arc::new(Field::new("item", DataType::UInt64, true)))
}

fn exec_err(msg: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(msg.into())
}

// ── APPROX_DISTINCT (HyperLogLog) ───────────────────────────────────────────────

/// Serialize a [`HyperLogLog`]'s state to bytes: one precision byte followed by its raw
/// registers — the exact pair [`HyperLogLog::from_raw_registers`] reconstructs from.
fn encode_hll(h: &HyperLogLog) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + h.num_registers());
    out.push(h.precision());
    out.extend_from_slice(h.registers());
    out
}

/// The inverse of [`encode_hll`]. `None` on an empty/malformed blob (a defensive no-op the
/// caller treats as "nothing to merge" rather than a hard error — a corrupt partial-aggregate
/// state should degrade the estimate, not fail the whole query).
fn decode_hll(bytes: &[u8]) -> Option<HyperLogLog> {
    let (&precision, registers) = bytes.split_first()?;
    Some(HyperLogLog::from_raw_registers(
        precision,
        registers.to_vec(),
    ))
}

#[derive(Debug)]
struct ApproxDistinctAcc {
    hll: HyperLogLog,
}

impl ApproxDistinctAcc {
    fn new() -> Self {
        Self {
            hll: HyperLogLog::default(),
        }
    }
}

impl Accumulator for ApproxDistinctAcc {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DfResult<()> {
        let arr = values[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| exec_err("approx_distinct: argument must be Utf8"))?;
        for i in 0..arr.len() {
            if !arr.is_null(i) {
                self.hll.insert(arr.value(i));
            }
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DfResult<()> {
        let bin = states[0]
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| exec_err("approx_distinct: state must be Binary"))?;
        for i in 0..bin.len() {
            if bin.is_null(i) {
                continue;
            }
            if let Some(other) = decode_hll(bin.value(i)) {
                self.hll.merge(&other);
            }
        }
        Ok(())
    }

    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Binary(Some(encode_hll(&self.hll)))])
    }

    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        Ok(ScalarValue::Float64(Some(self.hll.estimate())))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.hll.num_registers()
    }
}

/// `approx_distinct(expr) -> Float64` (CONCEPT:EG-KG.query.approx-distinct-cardinality, W4.5/N5).
pub(crate) fn approx_distinct_udaf() -> AggregateUDF {
    create_udaf(
        "approx_distinct",
        vec![DataType::Utf8],
        Arc::new(DataType::Float64),
        Volatility::Immutable,
        Arc::new(|_| Ok(Box::new(ApproxDistinctAcc::new()) as Box<dyn Accumulator>)),
        Arc::new(vec![DataType::Binary]),
    )
}

// ── APPROX_FREQUENCY (Count-Min Sketch) ─────────────────────────────────────────

/// Target relative error for the engine-default `approx_frequency` sizing: with probability
/// `>= 0.99`, the estimate overshoots the true count by no more than 1% of the total rows
/// aggregated (`CountMinSketch::with_error_rate`'s `(epsilon, delta)` contract).
const CMS_EPSILON: f64 = 0.01;
const CMS_DELTA: f64 = 0.01;

/// Serialize a [`CountMinSketch`]'s state: `width:u32`, `depth:u32` (both little-endian),
/// followed by `depth * width` little-endian `u32` counters, row-major — the exact layout
/// [`decode_cms`] reconstructs from via [`CountMinSketch::from_raw`].
fn encode_cms(c: &CountMinSketch) -> Vec<u8> {
    let (width, depth) = c.dims();
    let mut out = Vec::with_capacity(8 + width * depth * 4);
    out.extend_from_slice(&(width as u32).to_le_bytes());
    out.extend_from_slice(&(depth as u32).to_le_bytes());
    for row in c.counters() {
        for &v in row {
            out.extend_from_slice(&v.to_le_bytes());
        }
    }
    out
}

/// The inverse of [`encode_cms`]. `None` on a truncated/malformed blob (same defensive posture
/// as [`decode_hll`]).
fn decode_cms(bytes: &[u8]) -> Option<CountMinSketch> {
    if bytes.len() < 8 {
        return None;
    }
    let width = u32::from_le_bytes(bytes[0..4].try_into().ok()?) as usize;
    let depth = u32::from_le_bytes(bytes[4..8].try_into().ok()?) as usize;
    let mut counters = Vec::with_capacity(depth);
    let mut off = 8;
    for _ in 0..depth {
        let mut row = Vec::with_capacity(width);
        for _ in 0..width {
            let end = off + 4;
            if end > bytes.len() {
                return None;
            }
            row.push(u32::from_le_bytes(bytes[off..end].try_into().ok()?));
            off = end;
        }
        counters.push(row);
    }
    Some(CountMinSketch::from_raw(width, depth, counters))
}

#[derive(Debug)]
struct ApproxFrequencyAcc {
    cms: CountMinSketch,
    /// The probe value every row is compared against — read once from whichever batch/partition
    /// sees it first (it is a literal broadcast to every row by DataFusion, so any row's value
    /// is authoritative; mirrors `json_get`'s `as_inputs` doc: "`key` is taken from the first
    /// row (a scalar literal in practice)").
    probe: Option<String>,
}

impl ApproxFrequencyAcc {
    fn new() -> Self {
        Self {
            cms: CountMinSketch::with_error_rate(CMS_EPSILON, CMS_DELTA),
            probe: None,
        }
    }

    fn capture_probe(&mut self, probes: &StringArray) {
        if self.probe.is_some() {
            return;
        }
        for i in 0..probes.len() {
            if !probes.is_null(i) {
                self.probe = Some(probes.value(i).to_string());
                return;
            }
        }
    }
}

impl Accumulator for ApproxFrequencyAcc {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DfResult<()> {
        let items = values[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| exec_err("approx_frequency: first argument must be Utf8"))?;
        let probes = values[1]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| exec_err("approx_frequency: second argument must be Utf8"))?;
        self.capture_probe(probes);
        for i in 0..items.len() {
            if !items.is_null(i) {
                self.cms.insert(items.value(i));
            }
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DfResult<()> {
        let bin = states[0]
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| exec_err("approx_frequency: state[0] must be Binary"))?;
        let probes = states[1]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| exec_err("approx_frequency: state[1] must be Utf8"))?;
        self.capture_probe(probes);
        for i in 0..bin.len() {
            if bin.is_null(i) {
                continue;
            }
            if let Some(other) = decode_cms(bin.value(i)) {
                self.cms.merge(&other);
            }
        }
        Ok(())
    }

    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::Binary(Some(encode_cms(&self.cms))),
            ScalarValue::Utf8(self.probe.clone()),
        ])
    }

    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        let est = match &self.probe {
            Some(p) => self.cms.estimate(p) as f64,
            None => 0.0,
        };
        Ok(ScalarValue::Float64(Some(est)))
    }

    fn size(&self) -> usize {
        let (width, depth) = self.cms.dims();
        std::mem::size_of_val(self) + width * depth * std::mem::size_of::<u32>()
    }
}

/// `approx_frequency(expr, probe) -> Float64` (CONCEPT:EG-KG.query.approx-distinct-cardinality,
/// W4.5/N5) — the estimated occurrence count of the literal `probe` among `expr`'s values.
/// NEVER an underestimate (the Count-Min Sketch's core guarantee — see
/// `eg_compute::sketch::CountMinSketch`'s doc).
pub(crate) fn approx_frequency_udaf() -> AggregateUDF {
    create_udaf(
        "approx_frequency",
        vec![DataType::Utf8, DataType::Utf8],
        Arc::new(DataType::Float64),
        Volatility::Immutable,
        Arc::new(|_| Ok(Box::new(ApproxFrequencyAcc::new()) as Box<dyn Accumulator>)),
        Arc::new(vec![DataType::Binary, DataType::Utf8]),
    )
}

// ── MINHASH_SIGNATURE + MINHASH_SIMILARITY ──────────────────────────────────────

/// Signature length (number of independent hash "permutations"). 128 gives a similarity-estimate
/// standard deviation of `sqrt(J(1-J)/128) <= ~4.4%` across the full `[0,1]` similarity range —
/// ample precision for a query-planning/analytics aggregate at a modest (1 KiB) signature size.
const MINHASH_K: usize = 128;

fn u64_array_to_vec(arr: &dyn Array) -> DfResult<Vec<u64>> {
    let u = arr
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| exec_err("minhash: signature element type must be UInt64"))?;
    Ok((0..u.len()).map(|i| u.value(i)).collect())
}

fn minhash_scalar_list(sig: &[u64]) -> DfResult<ScalarValue> {
    let scalars: Vec<ScalarValue> = sig.iter().map(|&v| ScalarValue::UInt64(Some(v))).collect();
    Ok(ScalarValue::List(ScalarValue::new_list_nullable(
        &scalars,
        &DataType::UInt64,
    )))
}

#[derive(Debug)]
struct MinHashAcc {
    mh: MinHashSketch,
}

impl MinHashAcc {
    fn new() -> Self {
        Self {
            mh: MinHashSketch::new(MINHASH_K),
        }
    }
}

impl Accumulator for MinHashAcc {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DfResult<()> {
        let arr = values[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| exec_err("minhash_signature: argument must be Utf8"))?;
        for i in 0..arr.len() {
            if !arr.is_null(i) {
                self.mh.insert(arr.value(i));
            }
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DfResult<()> {
        let lists = states[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| exec_err("minhash_signature: state must be List<UInt64>"))?;
        for i in 0..lists.len() {
            if lists.is_null(i) {
                continue;
            }
            let sig = u64_array_to_vec(lists.value(i).as_ref())?;
            if sig.len() == self.mh.signature().len() {
                self.mh.merge(&MinHashSketch::from_signature(sig));
            }
        }
        Ok(())
    }

    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        Ok(vec![minhash_scalar_list(self.mh.signature())?])
    }

    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        minhash_scalar_list(self.mh.signature())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + std::mem::size_of_val(self.mh.signature())
    }
}

/// `minhash_signature(expr) -> List<UInt64>` (CONCEPT:EG-KG.query.approx-distinct-cardinality,
/// W4.5/N5) — a fixed-size MinHash signature summarizing the SET of `expr`'s distinct values
/// aggregated over. Compare two groups' signatures with [`minhash_similarity_udf`] to estimate
/// Jaccard similarity without ever joining their underlying rows.
pub(crate) fn minhash_signature_udaf() -> AggregateUDF {
    create_udaf(
        "minhash_signature",
        vec![DataType::Utf8],
        Arc::new(uint64_list_type()),
        Volatility::Immutable,
        Arc::new(|_| Ok(Box::new(MinHashAcc::new()) as Box<dyn Accumulator>)),
        Arc::new(vec![uint64_list_type()]),
    )
}

/// `minhash_similarity(sig1, sig2) -> Float64` — the estimated Jaccard similarity between two
/// [`minhash_signature_udaf`] results (CONCEPT:EG-KG.query.approx-distinct-cardinality, W4.5/N5):
/// the fraction of signature positions that agree. `NULL` if either signature is `NULL`; `0.0`
/// (not an error) if the two signatures have mismatched lengths (a caller comparing
/// differently-sized `minhash_signature` calls — mirrors `MinHashSketch::jaccard`'s own
/// defensive posture).
pub(crate) fn minhash_similarity_udf() -> ScalarUDF {
    let fun = Arc::new(|args: &[ColumnarValue]| {
        let arrays = ColumnarValue::values_to_arrays(args)?;
        let a = arrays[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| exec_err("minhash_similarity: first argument must be List<UInt64>"))?;
        let b = arrays[1]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| exec_err("minhash_similarity: second argument must be List<UInt64>"))?;
        let n = a.len();
        let mut out: Vec<Option<f64>> = Vec::with_capacity(n);
        for i in 0..n {
            if a.is_null(i) || b.is_null(i) {
                out.push(None);
                continue;
            }
            let sig_a = u64_array_to_vec(a.value(i).as_ref())?;
            let sig_b = u64_array_to_vec(b.value(i).as_ref())?;
            let ha = MinHashSketch::from_signature(sig_a);
            let hb = MinHashSketch::from_signature(sig_b);
            out.push(Some(ha.jaccard(&hb)));
        }
        Ok(ColumnarValue::Array(
            Arc::new(Float64Array::from(out)) as ArrayRef
        ))
    });
    create_udf(
        "minhash_similarity",
        vec![uint64_list_type(), uint64_list_type()],
        DataType::Float64,
        Volatility::Immutable,
        fun,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strings(vals: &[&str]) -> ArrayRef {
        Arc::new(StringArray::from(vals.to_vec()))
    }

    #[test]
    fn approx_distinct_acc_estimates_known_cardinality() {
        let mut acc = ApproxDistinctAcc::new();
        let vals: Vec<String> = (0..5_000).map(|i| format!("item-{i}")).collect();
        let refs: Vec<&str> = vals.iter().map(String::as_str).collect();
        acc.update_batch(&[strings(&refs)]).unwrap();
        let ScalarValue::Float64(Some(est)) = acc.evaluate().unwrap() else {
            panic!("expected Float64");
        };
        let rel_err = (est - 5_000.0).abs() / 5_000.0;
        assert!(rel_err < 0.1, "est={est} rel_err={rel_err}");
    }

    #[test]
    fn approx_distinct_state_roundtrips_through_merge() {
        let mut a = ApproxDistinctAcc::new();
        a.update_batch(&[strings(&["x", "y", "z"])]).unwrap();
        let state = a.state().unwrap();
        let ScalarValue::Binary(Some(bytes)) = &state[0] else {
            panic!("expected Binary state");
        };
        let mut b = ApproxDistinctAcc::new();
        b.merge_batch(&[Arc::new(BinaryArray::from(vec![bytes.as_slice()]))])
            .unwrap();
        assert_eq!(a.evaluate().unwrap(), b.evaluate().unwrap());
    }

    #[test]
    fn approx_frequency_never_underestimates() {
        let mut acc = ApproxFrequencyAcc::new();
        let items: Vec<&str> = std::iter::repeat_n("heavy", 40).collect();
        let probes: Vec<&str> = std::iter::repeat_n("heavy", 40).collect();
        acc.update_batch(&[strings(&items), strings(&probes)])
            .unwrap();
        let ScalarValue::Float64(Some(est)) = acc.evaluate().unwrap() else {
            panic!("expected Float64");
        };
        assert!(est >= 40.0, "must never underestimate, got {est}");
    }

    #[test]
    fn minhash_signature_identical_groups_similarity_one() {
        let mut a = MinHashAcc::new();
        let mut b = MinHashAcc::new();
        let vals: Vec<String> = (0..200).map(|i| format!("v{i}")).collect();
        let refs: Vec<&str> = vals.iter().map(String::as_str).collect();
        a.update_batch(&[strings(&refs)]).unwrap();
        b.update_batch(&[strings(&refs)]).unwrap();
        let ScalarValue::List(sa) = a.evaluate().unwrap() else {
            panic!("expected List");
        };
        let ScalarValue::List(sb) = b.evaluate().unwrap() else {
            panic!("expected List");
        };
        let sig_a = u64_array_to_vec(sa.values().as_ref()).unwrap();
        let sig_b = u64_array_to_vec(sb.values().as_ref()).unwrap();
        let ha = MinHashSketch::from_signature(sig_a);
        let hb = MinHashSketch::from_signature(sig_b);
        assert_eq!(ha.jaccard(&hb), 1.0);
    }

    #[test]
    fn minhash_similarity_udf_scalar_matches_direct_jaccard() {
        let udf = minhash_similarity_udf();
        assert_eq!(udf.name(), "minhash_similarity");
    }

    #[test]
    fn encode_decode_hll_roundtrips() {
        let mut h = HyperLogLog::default();
        for i in 0..500 {
            h.insert(&i);
        }
        let bytes = encode_hll(&h);
        let back = decode_hll(&bytes).unwrap();
        assert_eq!(h.estimate(), back.estimate());
    }

    #[test]
    fn encode_decode_cms_roundtrips() {
        let mut c = CountMinSketch::new(128, 3);
        for _ in 0..9 {
            c.insert(&"z");
        }
        let bytes = encode_cms(&c);
        let back = decode_cms(&bytes).unwrap();
        assert_eq!(c.estimate(&"z"), back.estimate(&"z"));
    }

    #[test]
    fn decode_cms_rejects_truncated_bytes() {
        assert!(decode_cms(&[1, 2, 3]).is_none());
    }

    #[test]
    fn decode_hll_rejects_empty_bytes() {
        assert!(decode_hll(&[]).is_none());
    }
}
