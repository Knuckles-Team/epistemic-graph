//! `ModalityContract` retrofit for [`Event`] (CONCEPT:E4).
//!
//! Behind the crate's own opt-in `contract` feature (default OFF) — see
//! `eg-modality`'s crate docs / README for the retrofit-order rationale (`eg-tsdb`/
//! `eg-stream` are next after the `eg-tensor`/`eg-geo` pilots: both already have a
//! staging-shaped concept — `eg-tsdb`'s `SeriesMeta`/chunk overlay, `eg-stream`'s CEP
//! window buffering — that maps directly onto `txn_stage`).

use eg_modality::{
    encode_staged, ConformanceTestable, EvidenceSpan, ModalityContract, Provenance, RowSetShape,
    StagedWrite,
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
    fn evidence(&self, _id: &str) -> Option<EvidenceSpan> {
        None
    }

    /// The pattern-algebra operators `eg-stream` exposes to a downstream planner
    /// beyond plain store/retrieve (mirrors `eg-tensor`/`eg-geo` listing their real
    /// ops here, not a placeholder list).
    fn analytics_ops(&self) -> Vec<&'static str> {
        vec!["sequence", "within", "absence", "sliding_window", "tumbling_window"]
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
