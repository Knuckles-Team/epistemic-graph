//! `ModalityContract` retrofit for [`Event`] (CONCEPT:E4).
//!
//! Behind the crate's own opt-in `contract` feature (default OFF) — see
//! `eg-modality`'s crate docs / README for the retrofit-order rationale (`eg-tsdb`/
//! `eg-stream` are next after the `eg-tensor`/`eg-geo` pilots: both already have a
//! staging-shaped concept — `eg-tsdb`'s `SeriesMeta`/chunk overlay, `eg-stream`'s CEP
//! window buffering — that maps directly onto `txn_stage`).

use eg_modality::{
    decode_staged, encode_staged, ConformanceTestable, EvidenceAddress, IngestReport,
    ModalityContract, ModalitySelfTest, Provenance, RowSetShape, StagedWrite, StorageStats,
    TckPoint,
};
use serde_json::{Map, Value};

use crate::event::Event;

impl ModalityContract for Event {
    fn storage_kind(&self) -> &'static str {
        "stream"
    }

    /// A raw event is a windowed-CEP FILTER/SOURCE candidate, not intrinsically
    /// ranked — `run()`'s [`crate::Match`] output is what carries a result shape,
    /// not the bare event feeding it. Unranked, exactly like `eg-geo::Geometry`'s
    /// `to_rowset` (a SOURCE-shaped value awaiting a downstream op).
    fn to_rowset(&self, id: &str) -> RowSetShape {
        RowSetShape::unranked(id)
    }

    /// Staging one event under `id` is exactly the CEP window buffering shape this
    /// modality already has (an event is appended to the live window/NFA state
    /// before a pattern can fire on it) — `StagedWrite::put` names that append.
    fn txn_stage(&self, id: &str) -> StagedWrite {
        StagedWrite::put(id, encode_staged(self))
    }

    /// The most natural non-`None` `cdc_topic` in the whole retrofit set (per the
    /// crate README's retrofit-order rationale): `eg-stream`'s `live` feature already
    /// fans events out over the EG-064 CDC bus to standing queries, so a bare event's
    /// append IS a CDC-observable write today.
    fn cdc_topic(&self) -> Option<&'static str> {
        Some("stream.event.append")
    }

    /// A raw event has no derivation history of its own — it is either ingested
    /// verbatim or synthesized by an upstream producer that doesn't (yet) record
    /// lineage. Default `None` is correct as-is.
    fn provenance(&self, _id: &str) -> Option<Provenance> {
        None
    }

    /// No located-evidence concept applies to a bare stream event — default `None`.
    fn evidence_address(&self) -> Option<EvidenceAddress> {
        None
    }

    /// The pattern-algebra operators `eg-stream` exposes to a downstream planner
    /// beyond plain store/retrieve (mirrors `eg-tensor`/`eg-geo` listing their real
    /// ops here, not a placeholder list).
    fn analytics_ops(&self) -> Vec<&'static str> {
        vec![
            "sequence",
            "within",
            "absence",
            "sliding_window",
            "tumbling_window",
        ]
    }

    // ── EG-P1-1 hooks — real, minimal implementations over Event's serialization
    // and txn staging. Events are windowed into CEP patterns. ──

    /// Batch ingest = parse an `Event` back from its serialized form. Streaming is
    /// genuinely N/A: a single event is a whole unit; streaming is an aggregate of
    /// many events (CEP windowing happens at the pattern engine level, not the value).
    fn ingest_report(&self, id: &str) -> IngestReport {
        let staged = self.txn_stage(id);
        let batch = match decode_staged::<Event>(&staged) {
            Ok(rt) if rt == *self => ModalitySelfTest::Passed,
            _ => ModalitySelfTest::Failed,
        };
        IngestReport {
            batch,
            streaming: ModalitySelfTest::NotApplicable(
                "a single event is a whole unit; CEP streaming is reconstructed at the pattern engine level",
            ),
        }
    }

    /// Real storage stats from the serialized Event: logical size from encoded
    /// length; element count is the number of attributes in the event. Events are
    /// not secondary-indexed (windowing is temporal, not indexed), so `has_secondary_index`
    /// is `false`.
    fn storage_stats(&self, _id: &str) -> Option<StorageStats> {
        let logical_bytes = encode_staged(self).len() as u64;
        let element_count = self.attrs.len() as u64;
        Some(StorageStats {
            logical_bytes,
            element_count,
            has_secondary_index: false,
        })
    }

    /// N/A: Events are streamed into windows and patterns; they are not backed up
    /// or migrated as individual values. Durability is maintained at the CEP engine
    /// level (the live window state or any persisted pattern state).
    fn backup_selfcheck(&self, _id: &str) -> ModalitySelfTest {
        ModalitySelfTest::NotApplicable(
            "events are windowed into patterns; backup/restore/migrate is a CEP-engine-layer concern, not a per-event capability",
        )
    }

    /// Simulated single-node crash-and-recover through the txn staging path.
    /// Stage the event as an in-txn write; the staged payload IS the WAL record;
    /// on "restart" replay-decode it and confirm the recovered event is intact.
    fn recovery_selfcheck(&self, id: &str) -> ModalitySelfTest {
        let staged: StagedWrite = self.txn_stage(id);
        match decode_staged::<Event>(&staged) {
            Ok(recovered) if recovered == *self => ModalitySelfTest::Passed,
            _ => ModalitySelfTest::Failed,
        }
    }

    /// Events have policy and provenance concerns at the CEP/pattern layer, not at
    /// the individual event level: CDC is already wired (stream.event.append); policy
    /// is at the graph-node layer; provenance is implicit in the event's timestamp/
    /// source attributes.
    fn tck_not_applicable(&self, point: TckPoint) -> Option<&'static str> {
        match point {
            TckPoint::CdcDeleteRetentionGc => Some(
                "CDC is already wired via stream.event.append; event retention/GC is a CEP window/pattern-engine concern, not a per-event capability",
            ),
            TckPoint::TenantRowRegionPolicy => Some(
                "no modality-intrinsic policy surface — tenant/row/region policy is enforced at the graph-node/eg-core::isolation layer that owns the event stream",
            ),
            TckPoint::ProvenanceEvidenceLineage => Some(
                "provenance is implicit in the event's timestamp and source attributes; per-event lineage is a CEP pipeline concern, not a modality-value property",
            ),
            _ => None,
        }
    }
}

impl ConformanceTestable for Event {
    fn conformance_sample() -> Self {
        let mut attrs = Map::new();
        attrs.insert("price".to_string(), Value::from(101.5));
        Event {
            ts: 1_700_000_000,
            key: "trade".to_string(),
            attrs,
        }
    }
}

eg_modality::modality_conformance_tests!(Event);
