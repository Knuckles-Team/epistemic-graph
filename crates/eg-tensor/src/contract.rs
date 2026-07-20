//! `ModalityContract` PILOT retrofit for [`Tensor`] (CONCEPT:E4).
//!
//! Behind the crate's own opt-in `contract` feature (default OFF) — see
//! `eg-modality`'s crate docs for the retrofit-order rationale (`eg-tensor`/`eg-geo`
//! are the v1 pilots; everything else is future work).

use eg_modality::{
    decode_staged, encode_staged, ConformanceTestable, EvidenceAddress, IngestReport,
    ModalityContract, ModalitySelfTest, Provenance, RowSetShape, StagedWrite, StorageStats,
    TckPoint,
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
            Buffer::I32(v) => {
                Some((v.iter().map(|&x| (x as f64) * (x as f64)).sum::<f64>()).sqrt() as f32)
            }
            Buffer::I64(v) => {
                Some((v.iter().map(|&x| (x as f64) * (x as f64)).sum::<f64>()).sqrt() as f32)
            }
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
    fn evidence_address(&self) -> Option<EvidenceAddress> {
        None
    }

    fn analytics_ops(&self) -> Vec<&'static str> {
        vec!["slice", "reduce", "elementwise", "reshape"]
    }

    // ── EG-P1-1 hooks — real, minimal implementations over the tensor's OWN durable
    // byte-blob codec (`to_blob`/`from_blob`, the content-addressed CAS persistence
    // form) and the txn staging path. Tensor remains first-class/non-production:
    // streaming ingest and three storage-layer dimensions are N/A. ──

    /// Batch ingest = parse a `Tensor` back from its compact durable byte-blob (the
    /// exact form the content-addressed CAS stores). Streaming is genuinely N/A: a
    /// dense N-D array is a whole value, not an append stream (a future
    /// `Op::TensorScan`-fed materialized view would be a DIFFERENT, streaming shape).
    fn ingest_report(&self, _id: &str) -> IngestReport {
        let blob = self.to_blob();
        let batch = match Tensor::from_blob(&blob) {
            Ok(rt) if &rt == self => ModalitySelfTest::Passed,
            _ => ModalitySelfTest::Failed,
        };
        IngestReport {
            batch,
            streaming: ModalitySelfTest::NotApplicable(
                "a dense N-D tensor is a whole value, not an append stream",
            ),
        }
    }

    /// Real storage stats from the value itself: element count from the buffer,
    /// logical size from the durable blob length. A dense tensor is NOT secondary-
    /// indexed (it is stored/retrieved by content-hash id), so `has_secondary_index`
    /// is honestly `false` — the point still passes on stats presence.
    fn storage_stats(&self, _id: &str) -> Option<StorageStats> {
        Some(StorageStats {
            logical_bytes: self.to_blob().len() as u64,
            element_count: self.data.len() as u64,
            has_secondary_index: false,
        })
    }

    /// Backup→restore round-trip through the DURABLE blob codec (the on-disk form),
    /// then confirm the restored tensor equals the original. "Migrate" is the identity
    /// at schema v1; "recover" is the same restore path.
    fn backup_selfcheck(&self, _id: &str) -> ModalitySelfTest {
        let backup = self.to_blob();
        match Tensor::from_blob(&backup) {
            Ok(restored) if &restored == self => ModalitySelfTest::Passed,
            _ => ModalitySelfTest::Failed,
        }
    }

    /// Simulated single-node crash-and-recover through the txn staging path (a
    /// DIFFERENT codec from `backup_selfcheck`'s compact blob — this exercises the
    /// serde-encoded WAL payload). Stage the tensor as an in-txn write; the staged
    /// payload IS the durable WAL record; on "restart" replay-decode it and confirm
    /// the recovered tensor is intact.
    fn recovery_selfcheck(&self, id: &str) -> ModalitySelfTest {
        let staged: StagedWrite = self.txn_stage(id);
        match decode_staged::<Tensor>(&staged) {
            Ok(recovered) if &recovered == self => ModalitySelfTest::Passed,
            _ => ModalitySelfTest::Failed,
        }
    }

    /// The three points a bare numeric N-D array genuinely does NOT own (a distinct,
    /// honest status from "not implemented yet"), each with a concrete reason:
    /// * CDC/delete/GC — a tensor lives in the immutable content-addressed blob CAS;
    ///   an immutable, content-hashed value has no in-place mutation to capture, and
    ///   GC is unreferenced-blob collection at the STORE layer, not a per-value trait.
    /// * tenant/row/region policy — enforced at the graph-node/`eg-core::isolation`
    ///   layer that OWNS the tensor, never embedded in the array bytes themselves.
    /// * provenance/evidence/lineage — a bare array has no derivation history and is
    ///   not extracted from any located artifact; lineage, where it exists, is
    ///   recorded by the producing plan operator, not the value (matches the existing
    ///   `provenance()`/`evidence_address()` returning `None`).
    fn tck_not_applicable(&self, point: TckPoint) -> Option<&'static str> {
        match point {
            TckPoint::CdcDeleteRetentionGc => Some(
                "content-addressed immutable CAS value — no in-place mutation to capture; delete/GC is a store-layer refcount concern, not a modality-value capability",
            ),
            TckPoint::TenantRowRegionPolicy => Some(
                "no modality-intrinsic policy surface — tenant/row/region policy is enforced at the graph-node/eg-core::isolation layer that owns the tensor",
            ),
            TckPoint::ProvenanceEvidenceLineage => Some(
                "a bare numeric N-D array has no derivation history and no located-evidence artifact; lineage, where it exists, is recorded by the producing plan operator",
            ),
            _ => None,
        }
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
        assert_eq!(
            row.score, None,
            "a U8 buffer is opaque bytes, not a numeric signal"
        );
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
