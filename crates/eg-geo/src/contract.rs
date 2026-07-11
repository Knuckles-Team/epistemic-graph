//! `ModalityContract` PILOT retrofit for [`Geometry`] (CONCEPT:E4).
//!
//! Behind the crate's own opt-in `contract` feature (default OFF) — see
//! `eg-modality`'s crate docs for the retrofit-order rationale (`eg-tensor`/`eg-geo`
//! are the v1 pilots; everything else is future work).

use eg_modality::{
    decode_staged, encode_staged, ConformanceTestable, EvidenceSpan, IngestReport,
    ModalityContract, ModalitySelfTest, Provenance, RowSetShape, StagedWrite, StorageStats,
    TckPoint,
};

use crate::geometry::{Geometry, Point};
use crate::wkb::{from_wkb, to_wkb};

impl ModalityContract for Geometry {
    fn storage_kind(&self) -> &'static str {
        "geo"
    }

    /// A geometry is a spatial FILTER/SOURCE candidate, not an intrinsically ranked
    /// value — `eg-plan`'s `Op::SpatialScan` is a SOURCE op and `SpatialWithin`/
    /// `SpatialDWithin` are FILTER predicates, not a RANK; the natural "distance to
    /// a query point" score only exists relative to a query argument this method
    /// doesn't have. So this stays unranked — `None`, exactly like an unranked
    /// FILTER/TRAVERSE `RowSet` awaiting a RANK op.
    fn to_rowset(&self, id: &str) -> RowSetShape {
        RowSetShape::unranked(id)
    }

    fn txn_stage(&self, id: &str) -> StagedWrite {
        StagedWrite::put(id, encode_staged(self))
    }

    /// Geometries are not (yet) on the CDC/streaming surface.
    fn cdc_topic(&self) -> Option<&'static str> {
        None
    }

    /// A geometry has no derivation history of its own — default `None` is correct
    /// as-is; no override needed (distinct from a future `eg-rdf` GeoSPARQL
    /// individual, which WOULD have OWL provenance — that lives in `eg-rdf`, not
    /// here).
    fn provenance(&self, _id: &str) -> Option<Provenance> {
        None
    }

    /// No located-evidence concept applies to a bare geometry value — default
    /// `None`.
    fn evidence(&self, _id: &str) -> Option<EvidenceSpan> {
        None
    }

    fn analytics_ops(&self) -> Vec<&'static str> {
        vec![
            "within",
            "dwithin",
            "distance",
            "intersects",
            "buffer",
            "convex_hull",
        ]
    }

    // ── EG-P1-1 hooks — real, minimal implementations over the geometry's OWN
    // lossless WKB codec (`to_wkb`/`from_wkb`) and the txn staging path. These take
    // geo from 5/12 to 12/12 (9 PASS + the 3 N/A declared in `tck_not_applicable`). ──

    /// Batch ingest = parse a `Geometry` back from its lossless WKB encoding (a real,
    /// standard geospatial ingest format this crate implements). Streaming is
    /// genuinely N/A: a geometry is a whole spatial literal, not an append stream.
    fn ingest_report(&self, _id: &str) -> IngestReport {
        let wkb = to_wkb(self);
        let batch = match from_wkb(&wkb) {
            Ok(rt) if &rt == self => ModalitySelfTest::Passed,
            _ => ModalitySelfTest::Failed,
        };
        IngestReport {
            batch,
            streaming: ModalitySelfTest::NotApplicable(
                "a geometry is a whole spatial literal, not an append stream",
            ),
        }
    }

    /// Real storage stats: logical size from the WKB length; the storage unit is the
    /// whole geometry (one indexed value), so `element_count` is `1`. Geo genuinely
    /// DOES participate in a secondary index — the crate maintains a Hilbert/STR
    /// R-tree over geometry bboxes (CONCEPT:EG-263) — so `has_secondary_index` is
    /// honestly `true` here (the one modality that can claim it among the pilots).
    fn storage_stats(&self, _id: &str) -> Option<StorageStats> {
        Some(StorageStats {
            logical_bytes: to_wkb(self).len() as u64,
            element_count: 1,
            has_secondary_index: true,
        })
    }

    /// Backup→restore round-trip through the lossless WKB codec (the durable spatial
    /// interchange form), confirming the restored geometry equals the original.
    fn backup_selfcheck(&self, _id: &str) -> ModalitySelfTest {
        let backup = to_wkb(self);
        match from_wkb(&backup) {
            Ok(restored) if &restored == self => ModalitySelfTest::Passed,
            _ => ModalitySelfTest::Failed,
        }
    }

    /// Simulated single-node crash-and-recover through the txn staging path (the
    /// serde-encoded WAL payload — a different codec from WKB backup). Stage the
    /// geometry; the staged payload IS the WAL record; replay-decode it on "restart".
    fn recovery_selfcheck(&self, id: &str) -> ModalitySelfTest {
        let staged: StagedWrite = self.txn_stage(id);
        match decode_staged::<Geometry>(&staged) {
            Ok(recovered) if &recovered == self => ModalitySelfTest::Passed,
            _ => ModalitySelfTest::Failed,
        }
    }

    /// The three points a bare geometry literal genuinely does NOT own (honest N/A,
    /// distinct from "not yet"), each with a concrete reason — parallel to
    /// `eg-tensor`'s: CDC/delete/GC is a store-layer concern for an immutable spatial
    /// literal; tenant/row/region policy is enforced at the graph-node/isolation layer
    /// that owns the geometry; a bare geometry has no derivation history or located-
    /// evidence artifact (its `provenance()`/`evidence()` are `None` by design).
    fn tck_not_applicable(&self, point: TckPoint) -> Option<&'static str> {
        match point {
            TckPoint::CdcDeleteRetentionGc => Some(
                "a geometry is an immutable spatial literal — change-capture/delete/GC is a store-layer concern, not a modality-value capability",
            ),
            TckPoint::TenantRowRegionPolicy => Some(
                "no modality-intrinsic policy surface — tenant/row/region policy is enforced at the graph-node/eg-core::isolation layer that owns the geometry",
            ),
            TckPoint::ProvenanceEvidenceLineage => Some(
                "a bare geometry literal has no derivation history and no located-evidence artifact; lineage, where it exists, is recorded by the producing plan operator",
            ),
            _ => None,
        }
    }
}

impl ConformanceTestable for Geometry {
    fn conformance_sample() -> Self {
        Geometry::Point(Point::new(1.0, 2.0))
    }
}

// Exercise a non-Point variant through the SAME battery too, beyond the one sample
// the macro drives, so the enum's other branches get direct `to_rowset`/`txn_stage`
// coverage.
#[cfg(test)]
mod extra_variant_coverage {
    use super::*;
    use crate::geometry::LineString;
    use eg_modality::{decode_staged, WriteKind};

    #[test]
    fn linestring_stages_and_round_trips() {
        let line = Geometry::LineString(LineString {
            points: vec![Point::new(0.0, 0.0), Point::new(1.0, 1.0)],
        });
        let staged = line.txn_stage("line-1");
        assert_eq!(staged.kind, WriteKind::Put);
        let restored: Geometry = decode_staged(&staged).unwrap();
        assert_eq!(line, restored);
    }

    #[test]
    fn to_rowset_stays_unranked() {
        let line = Geometry::LineString(LineString {
            points: vec![Point::new(0.0, 0.0), Point::new(1.0, 1.0)],
        });
        let row = line.to_rowset("line-1");
        assert_eq!(row.score, None);
    }
}

eg_modality::modality_conformance_tests!(Geometry);
