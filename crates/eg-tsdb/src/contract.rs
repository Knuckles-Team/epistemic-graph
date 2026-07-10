//! `ModalityContract` retrofit for [`SeriesMeta`] (CONCEPT:E4).
//!
//! Behind the crate's own opt-in `contract` feature (default OFF, and implying
//! `redb-store` since [`SeriesMeta`] lives in the `store` module gated on it) — see
//! `eg-modality`'s crate docs / README for the retrofit-order rationale (`eg-tsdb`/
//! `eg-stream` are next after the `eg-tensor`/`eg-geo` pilots).
//!
//! [`SeriesMeta`] — not a single point — is the crate's primary STORED value: it is
//! the one thing keyed 1:1 by `series_id` in `SERIES_META` (a bare [`crate::point::Point`]
//! has no id of its own; a series' points are appended UNDER a `SeriesMeta`'s id). It
//! already derives `Clone + Debug + PartialEq + Serialize + Deserialize`, so it needs
//! no new bounds to satisfy `ConformanceTestable`.

use eg_modality::{
    encode_staged, ConformanceTestable, EvidenceSpan, ModalityContract, Provenance, RowSetShape,
    StagedWrite,
};

use crate::store::SeriesMeta;

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
