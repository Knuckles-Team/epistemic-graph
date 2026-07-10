//! `ModalityContract` PILOT retrofit for [`Geometry`] (CONCEPT:E4).
//!
//! Behind the crate's own opt-in `contract` feature (default OFF) — see
//! `eg-modality`'s crate docs for the retrofit-order rationale (`eg-tensor`/`eg-geo`
//! are the v1 pilots; everything else is future work).

use eg_modality::{
    encode_staged, ConformanceTestable, EvidenceSpan, ModalityContract, Provenance, RowSetShape,
    StagedWrite,
};

use crate::geometry::{Geometry, Point};

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
        vec!["within", "dwithin", "distance", "intersects", "buffer", "convex_hull"]
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
