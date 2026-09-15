//! Wire DTOs embedded in feature-gated `protocol::Method` variants. They live in
//! eg-types (the bottom of the DAG) so the protocol enum can name them without
//! depending on eg-compute. Domain-specific DTOs are kept in private child modules
//! and re-exported here, preserving the established `eg_types::wire::*` API.

#[cfg(feature = "datascience")]
#[path = "wire_datascience.rs"]
mod wire_datascience;
#[cfg(feature = "finance")]
#[path = "wire_finance.rs"]
mod wire_finance;
#[cfg(feature = "mining")]
#[path = "wire_mining.rs"]
mod wire_mining;
#[cfg(feature = "ml-pipeline")]
#[path = "wire_pipeline.rs"]
mod wire_pipeline;
#[cfg(feature = "query")]
#[path = "wire_query_core.rs"]
mod wire_query_core;
#[cfg(any(
    feature = "geo",
    feature = "tensor",
    feature = "timeseries",
    feature = "probabilistic",
    feature = "stream",
    feature = "federation"
))]
#[path = "wire_query_modalities.rs"]
mod wire_query_modalities;
#[cfg(feature = "streaming")]
#[path = "wire_streaming.rs"]
mod wire_streaming;

#[cfg(feature = "datascience")]
pub use wire_datascience::*;
#[cfg(feature = "finance")]
pub use wire_finance::*;
#[cfg(feature = "mining")]
pub use wire_mining::*;
#[cfg(feature = "ml-pipeline")]
pub use wire_pipeline::*;
#[cfg(feature = "query")]
pub use wire_query_core::*;
#[cfg(any(
    feature = "geo",
    feature = "tensor",
    feature = "timeseries",
    feature = "probabilistic",
    feature = "stream",
    feature = "federation"
))]
pub use wire_query_modalities::*;
#[cfg(feature = "streaming")]
pub use wire_streaming::*;

// ── spatial wire-variant round-trip (CONCEPT:EG-KG.ontology.singles-concept) ─────────────────────────
#[cfg(all(test, feature = "geo"))]
mod geo_tests {
    use super::*;

    /// The spatial Op/Pred variants are pure-serde and round-trip through MessagePack
    /// (the wire format) unchanged — the proof `Method::UnifiedQuery { plan }` can carry
    /// a spatial plan.
    #[test]
    fn spatial_variants_round_trip() {
        let plan = Plan::new(vec![
            Op::SpatialScan {
                layer: "City".into(),
                bbox: [0.0, 0.0, 10.0, 10.0],
            },
            Op::Filter {
                preds: vec![
                    Pred::SpatialWithin {
                        column: "geom".into(),
                        wkt: "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))".into(),
                    },
                    Pred::SpatialDWithin {
                        column: "geom".into(),
                        wkt: "POINT (5 5)".into(),
                        distance: 2.5,
                    },
                ],
            },
            Op::Limit { k: 5 },
        ]);
        let bytes = rmp_serde::to_vec_named(&plan).unwrap();
        let back: Plan = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(plan, back);
    }

    /// The GIS batch wire variants — CRS reprojection (EG-255), the DE-9IM relation preds
    /// (EG-258), and the constructive `SpatialOp` (EG-259) — all round-trip through
    /// MessagePack unchanged.
    #[test]
    fn gis_batch_variants_round_trip() {
        let plan = Plan::new(vec![
            Op::SpatialScan {
                layer: "Parcel".into(),
                bbox: [0.0, 0.0, 100.0, 100.0],
            },
            Op::Reproject {
                to_epsg: 3857,
                from_epsg: Some(4326),
            },
            Op::Filter {
                preds: vec![
                    Pred::SpatialContains {
                        column: "geom".into(),
                        wkt: "POINT (5 5)".into(),
                    },
                    Pred::SpatialTouches {
                        column: "geom".into(),
                        wkt: "LINESTRING (0 0, 1 1)".into(),
                    },
                    Pred::SpatialOverlaps {
                        column: "geom".into(),
                        wkt: "POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))".into(),
                    },
                    Pred::SpatialDisjoint {
                        column: "geom".into(),
                        wkt: "POINT (99 99)".into(),
                    },
                ],
            },
            Op::SpatialOp {
                kind: SpatialOpKind::Buffer { distance: 2.0 },
            },
            Op::SpatialOp {
                kind: SpatialOpKind::ConvexHull,
            },
            Op::SpatialOp {
                kind: SpatialOpKind::Intersection {
                    wkt: "POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))".into(),
                },
            },
            Op::Limit { k: 10 },
        ]);
        let bytes = rmp_serde::to_vec_named(&plan).unwrap();
        let back: Plan = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(plan, back);
    }
}

// ── tensor wire-variant round-trip (CONCEPT:EG-KG.storage.content-addressed-dedup) ──────────────────────────
#[cfg(all(test, feature = "tensor"))]
mod tensor_tests {
    use super::*;

    /// The tensor Op variants + `TensorOpKind` are pure-serde and round-trip through
    /// MessagePack (the wire format) unchanged — the proof `Method::UnifiedQuery { plan }`
    /// can carry a tensor plan.
    #[test]
    fn tensor_variants_round_trip() {
        let plan = Plan::new(vec![
            Op::TensorScan {
                layer: "Frame".into(),
            },
            Op::TensorOp {
                kind: TensorOpKind::Slice {
                    ranges: vec![(0, 2), (1, 3)],
                },
            },
            Op::TensorOp {
                kind: TensorOpKind::Reduce {
                    axis: 1,
                    kind: TensorReduceKind::Mean,
                },
            },
            Op::TensorOp {
                kind: TensorOpKind::Elementwise {
                    op: TensorElementwiseOp::Mul,
                    scalar: 2.0,
                },
            },
            Op::Limit { k: 5 },
        ]);
        let bytes = rmp_serde::to_vec_named(&plan).unwrap();
        let back: Plan = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(plan, back);
    }
}

// ── probabilistic wire-variant round-trip (CONCEPT:EG-KG.compute.uncertainty-values) ───────────────────
#[cfg(all(test, feature = "probabilistic"))]
mod probabilistic_tests {
    use super::*;

    /// The `Op::Probabilistic` variant + its `ProbQuery` / `ProbEvidenceSpec` DTOs are
    /// pure-serde and round-trip through MessagePack (the wire format) unchanged — the
    /// proof `Method::UnifiedQuery { plan }` can carry a probabilistic plan (CONCEPT:EG-KG.compute.uncertainty-values).
    #[test]
    fn probabilistic_variants_round_trip() {
        let plan = Plan::new(vec![
            Op::Scan {
                label: "Belief".into(),
            },
            Op::Probabilistic {
                query: ProbQuery::Expectation,
            },
            Op::Probabilistic {
                query: ProbQuery::Marginal {
                    at: 0.5,
                    label: None,
                },
            },
            Op::Probabilistic {
                query: ProbQuery::Marginal {
                    at: 0.0,
                    label: Some("b".into()),
                },
            },
            Op::Probabilistic {
                query: ProbQuery::Conditional {
                    evidence: ProbEvidenceSpec::Bernoulli {
                        successes: 3.0,
                        failures: 1.0,
                    },
                },
            },
            Op::Probabilistic {
                query: ProbQuery::Conditional {
                    evidence: ProbEvidenceSpec::Gaussian {
                        observations: vec![1.0, 3.0],
                        known_variance: 1.0,
                    },
                },
            },
            Op::Probabilistic {
                query: ProbQuery::Sample { seed: 42 },
            },
            Op::Limit { k: 5 },
        ]);
        let bytes = rmp_serde::to_vec_named(&plan).unwrap();
        let back: Plan = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(plan, back);
    }
}

// ── epistemic wire-variant round-trip (CONCEPT:EG-KG.epistemic.epistemic-substrate, E2) ─────────────────
#[cfg(all(test, feature = "epistemic"))]
mod epistemic_tests {
    use super::*;

    /// The seven `Op::{EvidenceFor,Contradicts,SupportedBy,BeliefAsOf,SourceReliability,
    /// ConfidenceOp,ExplainBelief}` variants are pure-serde and round-trip through
    /// MessagePack (the wire format) unchanged — the proof `Method::UnifiedQuery { plan }`
    /// can carry an epistemic plan (CONCEPT:EG-KG.epistemic.epistemic-substrate).
    #[test]
    fn epistemic_variants_round_trip() {
        let plan = Plan::new(vec![
            Op::Scan {
                label: "Claim".into(),
            },
            Op::EvidenceFor {
                claim_id: "c1".into(),
            },
            Op::Contradicts {
                node_id: "c1".into(),
            },
            Op::SupportedBy {
                node_id: "c1".into(),
            },
            Op::BeliefAsOf { ts: 100.0 },
            Op::SourceReliability {
                source_id: "src1".into(),
            },
            Op::ConfidenceOp {},
            Op::ExplainBelief {
                node_id: "c1".into(),
            },
            Op::Limit { k: 5 },
        ]);
        let bytes = rmp_serde::to_vec_named(&plan).unwrap();
        let back: Plan = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(plan, back);
    }
}

// ── stream / CEP wire-variant round-trip (CONCEPT:EG-KG.query.pipelined-execution) ─────────────────────
#[cfg(all(test, feature = "stream"))]
mod stream_tests {
    use super::*;

    /// The `Op::Cep` variant + its `CepPatternSpec` tree (sequence/within/absence,
    /// matchers, attribute predicates, window) are pure-serde and round-trip through
    /// MessagePack (the wire format) unchanged — the proof `Method::UnifiedQuery { plan }`
    /// can carry a CEP plan.
    #[test]
    fn cep_variant_round_trips() {
        let matcher = CepMatcherSpec {
            key: Some("trade".into()),
            preds: vec![
                CepAttrPredSpec::Gt {
                    field: "qty".into(),
                    value: 100.0,
                },
                CepAttrPredSpec::Eq {
                    field: "sym".into(),
                    value: serde_json::json!("ACME"),
                },
                CepAttrPredSpec::Exists {
                    field: "venue".into(),
                },
            ],
        };
        let plan = Plan::new(vec![
            Op::Scan {
                label: "Event".into(),
            },
            Op::Cep {
                pattern: CepPatternSpec {
                    pattern: CepNodeSpec::Within {
                        within: 30,
                        pattern: Box::new(CepNodeSpec::Sequence(vec![
                            matcher.clone(),
                            CepMatcherSpec {
                                key: Some("cancel".into()),
                                preds: vec![],
                            },
                        ])),
                    },
                    window: CepWindowSpec::Sliding { size: 60 },
                },
            },
            Op::Cep {
                pattern: CepPatternSpec {
                    pattern: CepNodeSpec::Absence {
                        a: matcher,
                        b: CepMatcherSpec {
                            key: Some("ack".into()),
                            preds: vec![CepAttrPredSpec::Lt {
                                field: "latency".into(),
                                value: 5.0,
                            }],
                        },
                        within: 10,
                    },
                    window: CepWindowSpec::Tumbling { size: 100 },
                },
            },
            Op::Limit { k: 5 },
        ]);
        let bytes = rmp_serde::to_vec_named(&plan).unwrap();
        let back: Plan = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(plan, back);
    }
}

// ── sensor-fusion wire-variant round-trip (CONCEPT:EG-KG.query.multi-rate-sensor-stream) ───────────────────
#[cfg(all(test, feature = "timeseries"))]
mod timeseries_tests {
    use super::*;

    /// The `Op::SensorFuse` variant is pure-serde and round-trips through MessagePack
    /// (the wire format) unchanged — the proof `Method::UnifiedQuery { plan }` can carry
    /// a sensor-fusion plan.
    #[test]
    fn sensor_fuse_variant_round_trips() {
        let plan = Plan::new(vec![
            Op::SensorFuse {
                streams: vec!["imu".into(), "gps".into(), "lidar".into()],
                tolerance_ns: 50_000_000,
            },
            Op::Limit { k: 10 },
        ]);
        let bytes = rmp_serde::to_vec_named(&plan).unwrap();
        let back: Plan = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(plan, back);
    }

    /// The `Op::SensorAlign` variant + its `FuseStream`/`FuseInterp`/`FuseClock` payload
    /// are pure serde and round-trip through MessagePack (the wire format) unchanged, for
    /// BOTH clocks — the proof `Method::UnifiedQuery { plan }` can carry a declared-clock
    /// fusion plan beside the union-clock `Op::SensorFuse`.
    #[test]
    fn sensor_align_variant_round_trips() {
        for clock in [
            FuseClock::Uniform {
                from_ns: 0,
                to_ns: 5_000_000_000,
                step_ns: 500_000_000,
            },
            FuseClock::Tumbling {
                width_ns: 4_000_000_000,
                step_ns: 1_000_000_000,
            },
        ] {
            let plan = Plan::new(vec![
                Op::SensorAlign {
                    streams: vec![
                        FuseStream {
                            layer: "imu".into(),
                            interp: FuseInterp::Linear,
                        },
                        FuseStream {
                            layer: "gps".into(),
                            interp: FuseInterp::Nearest,
                        },
                        FuseStream {
                            layer: "mode".into(),
                            interp: FuseInterp::AsofHold,
                        },
                    ],
                    clock,
                    tolerance_ns: Some(50_000_000),
                },
                Op::Limit { k: 10 },
            ]);
            let bytes = rmp_serde::to_vec_named(&plan).unwrap();
            let back: Plan = rmp_serde::from_slice(&bytes).unwrap();
            assert_eq!(plan, back);
        }
    }

    /// An unbounded tolerance (`None`) round-trips too — the "no staleness bound" case is
    /// on the wire, not implied by a sentinel.
    #[test]
    fn sensor_align_unbounded_tolerance_round_trips() {
        let plan = Plan::new(vec![Op::SensorAlign {
            streams: vec![FuseStream {
                layer: "imu".into(),
                interp: FuseInterp::Linear,
            }],
            clock: FuseClock::Uniform {
                from_ns: 0,
                to_ns: 3,
                step_ns: 1,
            },
            tolerance_ns: None,
        }]);
        let bytes = rmp_serde::to_vec_named(&plan).unwrap();
        let back: Plan = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(plan, back);
    }
}

// ── document/JSON wire-variant round-trip (CONCEPT:EG-KG.query.json-wire-roundtrip) ───────────────────
#[cfg(all(test, feature = "query"))]
mod docjson_tests {
    use super::*;

    /// CONCEPT:EG-KG.query.json-wire-roundtrip — the `Pred::JsonPath` variant + its `JsonPathOp` (existence /
    /// equality / `@>` containment) are pure serde and round-trip through MessagePack
    /// (the wire format) unchanged, so `Method::UnifiedQuery { plan }` can carry a deep
    /// JSON filter.
    #[test]
    fn eg084_jsonpath_pred_round_trips() {
        let plan = Plan::new(vec![
            Op::Filter {
                preds: vec![
                    Pred::JsonPath {
                        path: "$.meta.lang".into(),
                        op: JsonPathOp::Eq {
                            value: serde_json::json!("rust"),
                        },
                    },
                    Pred::JsonPath {
                        path: "$.tags[*]".into(),
                        op: JsonPathOp::Exists,
                    },
                    Pred::JsonPath {
                        path: "$".into(),
                        op: JsonPathOp::Contains {
                            value: serde_json::json!({"meta": {"year": 2024}}),
                        },
                    },
                ],
            },
            Op::Limit { k: 5 },
        ]);
        let bytes = rmp_serde::to_vec_named(&plan).unwrap();
        let back: Plan = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(plan, back);
    }

    /// CONCEPT:EG-KG.query.json-wire-roundtrip — JSON round-trip too (the REST/UQL surface serializes the plan as
    /// JSON), proving the enum tags are stable across both wire encodings.
    #[test]
    fn eg084_jsonpath_pred_json_round_trips() {
        let p = Pred::JsonPath {
            path: "$.a.b".into(),
            op: JsonPathOp::Contains {
                value: serde_json::json!([1, 2, 3]),
            },
        };
        let s = serde_json::to_string(&p).unwrap();
        let back: Pred = serde_json::from_str(&s).unwrap();
        assert_eq!(p, back);
    }
}
