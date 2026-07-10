//! `ModalityContract` PILOT retrofit for [`Tensor`] (CONCEPT:E4).
//!
//! Behind the crate's own opt-in `contract` feature (default OFF) — see
//! `eg-modality`'s crate docs for the retrofit-order rationale (`eg-tensor`/`eg-geo`
//! are the v1 pilots; everything else is future work).

use eg_modality::{
    encode_staged, ConformanceTestable, EvidenceSpan, ModalityContract, Provenance, RowSetShape,
    StagedWrite,
};

use crate::tensor::{Buffer, Tensor};

impl ModalityContract for Tensor {
    fn storage_kind(&self) -> &'static str {
        "tensor"
    }

    /// Score by the tensor's L2 norm for a numeric buffer (a natural, cheap
    /// similarity-adjacent scalar a RANK op could sort on); `None` for `U8` (treated
    /// as opaque bytes, not a numeric signal) — an unranked `TensorScan` candidate,
    /// exactly like a bare FILTER/TRAVERSE result until a RANK op imposes order.
    fn to_rowset(&self, id: &str) -> RowSetShape {
        let score = match &self.data {
            Buffer::F32(v) => Some(v.iter().map(|x| x * x).sum::<f32>().sqrt()),
            Buffer::F64(v) => Some((v.iter().map(|x| x * x).sum::<f64>()).sqrt() as f32),
            Buffer::I32(v) => Some((v.iter().map(|&x| (x as f64) * (x as f64)).sum::<f64>()).sqrt() as f32),
            Buffer::I64(v) => Some((v.iter().map(|&x| (x as f64) * (x as f64)).sum::<f64>()).sqrt() as f32),
            Buffer::U8(_) => None,
        };
        RowSetShape {
            id: id.to_string(),
            score,
        }
    }

    fn txn_stage(&self, id: &str) -> StagedWrite {
        StagedWrite::put(id, encode_staged(self))
    }

    /// Bare tensor values are not (yet) on the CDC/streaming surface — a future
    /// `Op::TensorScan`-fed materialized view could change that, but nothing wires
    /// it today.
    fn cdc_topic(&self) -> Option<&'static str> {
        None
    }

    /// A tensor has no derivation history of its own (it is either asserted or
    /// produced by a plan operator that does not record lineage) — default `None`
    /// is correct as-is; no override needed.
    fn provenance(&self, _id: &str) -> Option<Provenance> {
        None
    }

    /// No located-evidence concept applies to a bare N-D array — default `None`.
    fn evidence(&self, _id: &str) -> Option<EvidenceSpan> {
        None
    }

    fn analytics_ops(&self) -> Vec<&'static str> {
        vec!["slice", "reduce", "elementwise", "reshape"]
    }
}

impl ConformanceTestable for Tensor {
    fn conformance_sample() -> Self {
        Tensor::new(vec![2, 2], Buffer::F32(vec![1.0, 2.0, 3.0, 4.0]))
            .expect("2x2 shape matches a 4-element buffer")
    }
}

// Also exercise an integer dtype + a U8 (unscored) buffer through the SAME battery,
// beyond the one `ConformanceTestable::conformance_sample` the macro drives, so the
// `to_rowset` None-score branch (`Buffer::U8`) gets direct coverage too.
#[cfg(test)]
mod extra_dtype_coverage {
    use super::*;

    #[test]
    fn u8_buffer_is_unscored() {
        let t = Tensor::new(vec![3], Buffer::U8(vec![1, 2, 3])).unwrap();
        let row = t.to_rowset("bytes-1");
        assert_eq!(row.id, "bytes-1");
        assert_eq!(row.score, None, "a U8 buffer is opaque bytes, not a numeric signal");
    }

    #[test]
    fn i64_buffer_is_scored() {
        let t = Tensor::new(vec![2], Buffer::I64(vec![3, 4])).unwrap();
        let row = t.to_rowset("ints-1");
        assert_eq!(row.score, Some(5.0));
    }

    #[test]
    fn dtype_is_reported() {
        use crate::tensor::DType;
        assert_eq!(DType::F32.tag(), 0);
    }
}

eg_modality::modality_conformance_tests!(Tensor);
