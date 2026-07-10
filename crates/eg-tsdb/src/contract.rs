//! `ModalityContract` retrofit for [`SeriesMeta`] and [`Span`] (CONCEPT:E4/X1).
//!
//! Behind the crate's own opt-in `contract` feature (default OFF, and implying both
//! `redb-store` since [`SeriesMeta`] lives in the `store` module gated on it, AND
//! `traces` since [`Span`] lives in the `traces` module gated on IT) — see
//! `eg-modality`'s crate docs / README for the retrofit-order rationale (`eg-tsdb`/
//! `eg-stream` are next after the `eg-tensor`/`eg-geo` pilots).
//!
//! [`SeriesMeta`] — not a single point — is the crate's primary STORED value: it is
//! the one thing keyed 1:1 by `series_id` in `SERIES_META` (a bare [`crate::point::Point`]
//! has no id of its own; a series' points are appended UNDER a `SeriesMeta`'s id). It
//! already derives `Clone + Debug + PartialEq + Serialize + Deserialize`, so it needs
//! no new bounds to satisfy `ConformanceTestable`.
//!
//! [`Span`] is the X1 (multimodal-evidence) reference case for `evidence()`: a
//! distributed-tracing span already carries its own real `trace_id`/`span_id` —
//! exactly the identity `EvidenceSpan::TraceSpan` wants — so this maps it losslessly
//! rather than leaving the default `None`. It already derives `PartialEq`, so it too
//! needs no new bounds for `ConformanceTestable`.

use eg_modality::{
    encode_staged, ConformanceTestable, EvidenceSpan, ModalityContract, Provenance, RowSetShape,
    StagedWrite,
};

use crate::store::SeriesMeta;
use crate::traces::Span;

impl ModalityContract for SeriesMeta {
    fn storage_kind(&self) -> &'static str {
        "tsdb"
    }

    /// A series' metadata is a SOURCE-shaped candidate (`SERIES_META` keyed by
    /// `series_id`), not intrinsically ranked — a rank only emerges from a query
    /// primitive over the series' actual points (ASOF/OHLC/downsample), which this
    /// value doesn't carry. Unranked, exactly like `eg-geo::Geometry`.
    fn to_rowset(&self, id: &str) -> RowSetShape {
        RowSetShape::unranked(id)
    }

    /// Staging a `SeriesMeta` write under `id` is the same shape as the store's own
    /// `SERIES_META` upsert (a msgpack-encoded value keyed by `series_id`) — here
    /// encoded via the shared `encode_staged` (serde_json) since `StagedWrite`'s
    /// payload is a transport-agnostic description, not the store's own on-disk
    /// codec.
    fn txn_stage(&self, id: &str) -> StagedWrite {
        StagedWrite::put(id, encode_staged(self))
    }

    /// The most natural non-`None` `cdc_topic` in the retrofit set (per the crate
    /// README): a series append is a legitimate CDC-observable event even though
    /// nothing wires it today (mirrors `eg-stream`'s `stream.event.append`).
    fn cdc_topic(&self) -> Option<&'static str> {
        Some("tsdb.series.append")
    }

    /// Series metadata has no derivation history of its own (it is either asserted
    /// by an ingest pipeline or accumulated by appends) — default `None` is correct
    /// as-is; no override needed.
    fn provenance(&self, _id: &str) -> Option<Provenance> {
        None
    }

    /// No located-evidence concept applies to bare series metadata — default `None`.
    fn evidence(&self, _id: &str) -> Option<EvidenceSpan> {
        None
    }

    /// The crate's real native TS query primitives (`crate::query`), listed here
    /// exactly like `eg-tensor`/`eg-geo` list their real ops — not a placeholder.
    fn analytics_ops(&self) -> Vec<&'static str> {
        vec![
            "time_bucket",
            "asof_join_backward",
            "gap_fill_locf",
            "ohlc_bars",
            "downsample",
            "decay_weighted_mean",
        ]
    }
}

impl ConformanceTestable for SeriesMeta {
    fn conformance_sample() -> Self {
        SeriesMeta {
            n_fields: 2,
            bucket_ns: 3_600_000_000_000, // 1h buckets
            field_names: vec!["price".to_string(), "volume".to_string()],
            count: 42,
            min_ts: 1_700_000_000_000_000_000,
            max_ts: 1_700_000_600_000_000_000,
        }
    }
}

eg_modality::modality_conformance_tests!(SeriesMeta);

impl ModalityContract for Span {
    fn storage_kind(&self) -> &'static str {
        "trace"
    }

    /// A span is a SOURCE-shaped candidate (keyed by `span_id` within a trace), not
    /// intrinsically ranked — unranked, exactly like `SeriesMeta`.
    fn to_rowset(&self, id: &str) -> RowSetShape {
        RowSetShape::unranked(id)
    }

    fn txn_stage(&self, id: &str) -> StagedWrite {
        StagedWrite::put(id, encode_staged(self))
    }

    /// A span arrives via OTLP-JSON ingest into the in-memory `SpanStore` (see
    /// `crate::traces` module docs), not the engine's own txn/write path — not (yet)
    /// on the CDC/streaming surface, mirroring `SeriesMeta`'s real `Some` being the
    /// exception rather than the rule for this crate's non-store values.
    fn cdc_topic(&self) -> Option<&'static str> {
        None
    }

    /// Series metadata (above) has no derivation history; a span doesn't either — an
    /// observed span is asserted-by-observation, not derived. Default `None` is
    /// correct as-is; no override needed.
    fn provenance(&self, _id: &str) -> Option<Provenance> {
        None
    }

    /// The X1 reference `evidence()`: a span's `trace_id`/`span_id` pair IS its own
    /// located identity — "observed during this trace span" — mapping losslessly
    /// onto `EvidenceSpan::TraceSpan`, ignoring the caller-supplied `id` (the span
    /// already carries its own two-part identity, exact and self-sufficient).
    fn evidence(&self, _id: &str) -> Option<EvidenceSpan> {
        Some(EvidenceSpan::TraceSpan {
            trace_id: self.trace_id.clone(),
            span_id: self.span_id.clone(),
        })
    }
}

impl ConformanceTestable for Span {
    fn conformance_sample() -> Self {
        Span {
            trace_id: "trace-1".to_string(),
            span_id: "span-1".to_string(),
            parent_span_id: String::new(),
            service: "gateway".to_string(),
            operation: "GET /".to_string(),
            start_time: 1_700_000_000_000_000_000,
            duration: 500_000,
            status: "OK".to_string(),
            attributes: Default::default(),
            events: Vec::new(),
        }
    }
}

// `modality_conformance_tests!` expands to a FIXED `mod eg_modality_conformance`
// name — nested here (rather than invoked a second time at this file's top level,
// where `SeriesMeta`'s invocation above already claims that name) so the two
// batteries don't collide (CONCEPT:E4/X1).
mod span_conformance {
    use super::Span;

    eg_modality::modality_conformance_tests!(Span);
}

// A direct test of the `evidence()` mapping itself, beyond the generic
// "never panics" conformance check — the load-bearing proof that a `Span`'s own
// trace_id/span_id maps losslessly onto `EvidenceSpan::TraceSpan` (X1).
#[cfg(test)]
mod span_evidence {
    use super::*;

    #[test]
    fn maps_trace_and_span_id_losslessly() {
        let span = Span::conformance_sample();
        let ev = span.evidence("ignored").expect("an observed Span always has evidence");
        assert_eq!(
            ev,
            EvidenceSpan::TraceSpan {
                trace_id: "trace-1".to_string(),
                span_id: "span-1".to_string(),
            }
        );
    }
}
