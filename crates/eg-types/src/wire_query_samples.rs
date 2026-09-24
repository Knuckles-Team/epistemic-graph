//! One representative value per [`OpKind`]/[`PredKind`] (UQL-06, test support).
//!
//! Exhaustive over the kinds, so a new wire variant cannot compile without a sample —
//! and eg-plan's every-variant walk then proves the sample prints and re-parses. Values
//! deliberately exercise the lexer's hard cases: quotes, unicode, reserved words used as
//! names, negative and non-integral numbers.

use super::*;

/// A representative [`Op`] of `kind`.
pub fn uql_sample_op(kind: OpKind) -> Op {
    match kind {
        OpKind::Scan => Op::Scan {
            label: "Doc".into(),
        },
        OpKind::ScanAll => Op::ScanAll {},
        OpKind::Filter => Op::Filter {
            preds: vec![
                uql_sample_pred(PredKind::GtNum),
                uql_sample_pred(PredKind::Or),
            ],
        },
        OpKind::Traverse => Op::Traverse {
            rel: "CITES".into(),
            min: 1,
            max: 3,
        },
        OpKind::Expand => Op::Expand {
            rel: None,
            dir: EdgeDir::Both,
            min: 0,
            max: 2,
            edge_preds: vec![uql_sample_pred(PredKind::Cmp)],
        },
        OpKind::Propagate => Op::Propagate {
            model: PropagateModel::Cascade {
                samples: 500,
                seed: 7,
            },
            rel: Some("dependsOn".into()),
            dir: EdgeDir::In,
            edge_preds: vec![uql_sample_pred(PredKind::Cmp)],
            hops: 4,
            default_transmission: 0.25,
        },
        OpKind::Rank => Op::Rank {
            query: vec![0.1, -0.25, 3.0e-7],
        },
        OpKind::RankEmbed => Op::RankEmbed {
            text: "it's a café".into(),
        },
        OpKind::RankNodeDistance => Op::RankNodeDistance {
            center: "kg-2.0".into(),
        },
        OpKind::RankMentions => Op::RankMentions {},
        OpKind::RankMmr => Op::RankMmr { lambda: 0.5, k: 10 },
        #[cfg(feature = "text")]
        OpKind::RankText => Op::RankText {
            query: "graph databases".into(),
        },
        #[cfg(feature = "text")]
        OpKind::FuseRrf => Op::FuseRrf {
            branches: vec![
                vec![Op::Rank {
                    query: vec![1.0, 0.0],
                }],
                vec![Op::RankText { query: "q".into() }, Op::Limit { k: 3 }],
            ],
            k: 60.0,
        },
        #[cfg(feature = "owl-plan")]
        OpKind::Reason => Op::Reason {
            target_class: "<http://ex/Device>".into(),
            ontology: "@prefix : <http://ex/> .".into(),
        },
        #[cfg(feature = "owl-plan")]
        OpKind::SparqlBgp => Op::SparqlBgp {
            query: "SELECT ?x WHERE { ?x a <http://ex/T> }".into(),
            var: "x".into(),
        },
        #[cfg(feature = "owl-plan")]
        OpKind::ValidateShape => Op::ValidateShape {
            shape: "Person Shape".into(),
            shapes: "@prefix sh: <http://www.w3.org/ns/shacl#> .".into(),
            keep: ShapeKeep::Violating,
        },
        #[cfg(feature = "wasm-udf")]
        OpKind::Udf => Op::Udf {
            id: "score-v2".into(),
        },
        #[cfg(feature = "federation")]
        OpKind::ForeignScan => Op::ForeignScan {
            source: Box::new(ForeignSourceSpec::HttpJson {
                url: "https://api.example/papers".into(),
                json_path: "$.items".into(),
                field_map: HttpFieldMap {
                    id: "doi".into(),
                    score: Some("rank".into()),
                },
            }),
            join: true,
        },
        OpKind::AsOf => Op::AsOf {
            ts: -86_400.5,
            axis: TimeAxis::Transaction,
        },
        OpKind::Window => Op::Window { secs: 0.005 },
        OpKind::WindowAgg => Op::WindowAgg {
            secs: 60.0,
            agg: "max".into(),
        },
        OpKind::Foreign => Op::Foreign {
            name: "peer-east".into(),
        },
        #[cfg(feature = "geo")]
        OpKind::SpatialScan => Op::SpatialScan {
            layer: "roads".into(),
            bbox: [-1.5, 0.0, 10.0, 20.25],
        },
        #[cfg(feature = "geo")]
        OpKind::Reproject => Op::Reproject {
            to_epsg: 3857,
            from_epsg: Some(4326),
        },
        #[cfg(feature = "geo")]
        OpKind::SpatialOp => Op::SpatialOp {
            kind: SpatialOpKind::Intersection {
                wkt: "POLYGON((0 0,1 0,1 1,0 0))".into(),
            },
        },
        #[cfg(feature = "tensor")]
        OpKind::TensorScan => Op::TensorScan {
            layer: "frames".into(),
        },
        #[cfg(feature = "tensor")]
        OpKind::TensorOp => Op::TensorOp {
            kind: TensorOpKind::Slice {
                ranges: vec![(0, 2), (1, 4)],
            },
        },
        #[cfg(feature = "stream")]
        OpKind::Cep => Op::Cep {
            pattern: sample_cep(),
        },
        #[cfg(feature = "timeseries")]
        OpKind::SensorFuse => Op::SensorFuse {
            streams: vec!["imu".into(), "gps".into()],
            tolerance_ns: 5_000_000,
        },
        #[cfg(feature = "timeseries")]
        OpKind::SensorAlign => Op::SensorAlign {
            streams: vec![
                FuseStream {
                    layer: "imu".into(),
                    interp: FuseInterp::Linear,
                },
                FuseStream {
                    layer: "gps".into(),
                    interp: FuseInterp::AsofHold,
                },
            ],
            clock: FuseClock::Uniform {
                from_ns: -10,
                to_ns: 1_000,
                step_ns: 10,
            },
            tolerance_ns: Some(50),
        },
        #[cfg(feature = "timeseries")]
        OpKind::TsScan => Op::TsScan {
            series: vec!["cpu".into(), "mem".into()],
            from: 0.0,
            to: 3600.0,
        },
        #[cfg(feature = "timeseries")]
        OpKind::Derive => derive_sample(),
        #[cfg(feature = "timeseries")]
        OpKind::Skill => Op::Skill {
            spec: crate::series_expr::SkillOp {
                feature: "v0".into(),
                outcome: "score".into(),
                horizons: vec![1, 5, 20],
                window: 60,
                resamples: 500,
                seed: 7,
            },
        },
        #[cfg(feature = "probabilistic")]
        OpKind::Probabilistic => Op::Probabilistic {
            query: ProbQuery::Conditional {
                evidence: ProbEvidenceSpec::Gaussian {
                    observations: vec![1.0, 2.5, -0.5],
                    known_variance: 0.25,
                },
            },
        },
        #[cfg(feature = "epistemic")]
        OpKind::EvidenceFor => Op::EvidenceFor {
            claim_id: "c:1".into(),
        },
        #[cfg(feature = "epistemic")]
        OpKind::Contradicts => Op::Contradicts {
            node_id: "c1".into(),
        },
        #[cfg(feature = "epistemic")]
        OpKind::SupportedBy => Op::SupportedBy {
            node_id: "c1".into(),
        },
        #[cfg(feature = "epistemic")]
        OpKind::BeliefAsOf => Op::BeliefAsOf {
            ts: 1_700_000_000.0,
        },
        #[cfg(feature = "epistemic")]
        OpKind::SourceReliability => Op::SourceReliability {
            source_id: "s1".into(),
        },
        #[cfg(feature = "epistemic")]
        OpKind::ConfidenceOp => Op::ConfidenceOp {},
        #[cfg(feature = "epistemic")]
        OpKind::ExplainBelief => Op::ExplainBelief {
            node_id: "c1".into(),
        },
        OpKind::Attribute => Op::Attribute {
            input: AttributionInput::Property {
                name: "p95 ms".into(),
            },
            value: AttributionValue::Percentile { p: 95 },
            method: AttributionMethod::ShapleySampled {
                samples: 4000,
                seed: 7,
            },
        },
        OpKind::DecisionScan => Op::DecisionScan {
            preds: vec![uql_sample_pred(PredKind::In)],
        },
        OpKind::Limit => Op::Limit { k: 10 },
        OpKind::Project => Op::Project {
            channels: vec!["similarity".into(), "window".into()],
        },
    }
}

#[cfg(feature = "stream")]
fn sample_cep() -> CepPatternSpec {
    let trade = CepMatcherSpec {
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
        ],
    };
    let cancel = CepMatcherSpec {
        key: Some("cancel".into()),
        preds: vec![CepAttrPredSpec::Exists {
            field: "reason".into(),
        }],
    };
    CepPatternSpec {
        pattern: CepNodeSpec::Within {
            within: 30,
            pattern: Box::new(CepNodeSpec::Sequence(vec![trade, cancel])),
        },
        window: CepWindowSpec::Sliding { size: 60 },
    }
}

/// A representative [`Pred`] of `kind`.
pub fn uql_sample_pred(kind: PredKind) -> Pred {
    match kind {
        PredKind::Eq => Pred::Eq {
            prop: "lang".into(),
            value: "O'Brien".into(),
        },
        PredKind::GtNum => Pred::GtNum {
            prop: "year".into(),
            n: 2024.0,
        },
        PredKind::LtNum => Pred::LtNum {
            prop: "score".into(),
            n: -0.5,
        },
        PredKind::Cmp => Pred::Cmp {
            prop: "weight".into(),
            op: CmpOp::Ge,
            value: PredLiteral::Num(1.5e-3),
        },
        PredKind::In => Pred::In {
            prop: "status".into(),
            values: vec![
                PredLiteral::Str("open".into()),
                PredLiteral::Num(3.0),
                PredLiteral::Bool(true),
            ],
        },
        PredKind::Between => Pred::Between {
            prop: "year".into(),
            lo: PredLiteral::Num(2000.0),
            hi: PredLiteral::Num(2010.0),
        },
        PredKind::IsNull => Pred::IsNull {
            prop: "limit".into(),
        },
        PredKind::And => Pred::And {
            preds: vec![
                uql_sample_pred(PredKind::Eq),
                uql_sample_pred(PredKind::Not),
            ],
        },
        PredKind::Or => Pred::Or {
            preds: vec![
                uql_sample_pred(PredKind::And),
                uql_sample_pred(PredKind::Between),
            ],
        },
        PredKind::Not => Pred::Not {
            pred: Box::new(uql_sample_pred(PredKind::IsNull)),
        },
        PredKind::JsonPath => Pred::JsonPath {
            path: "$.meta.tags[0]".into(),
            op: JsonPathOp::Contains {
                value: serde_json::json!({"k": [1, 2.5, null]}),
            },
        },
        #[cfg(feature = "geo")]
        PredKind::SpatialWithin => spatial(kind),
        #[cfg(feature = "geo")]
        PredKind::SpatialDWithin => Pred::SpatialDWithin {
            column: "geom".into(),
            wkt: "POINT(0 0)".into(),
            distance: 2.5,
        },
        #[cfg(feature = "geo")]
        PredKind::SpatialContains
        | PredKind::SpatialCovers
        | PredKind::SpatialTouches
        | PredKind::SpatialCrosses
        | PredKind::SpatialOverlaps
        | PredKind::SpatialEquals
        | PredKind::SpatialDisjoint => spatial(kind),
    }
}

/// The two-string spatial preds, all over the same column and geometry.
#[cfg(feature = "geo")]
fn spatial(kind: PredKind) -> Pred {
    let column = "geom".to_string();
    let wkt = "POLYGON((0 0,4 0,4 4,0 0))".to_string();
    match kind {
        PredKind::SpatialContains => Pred::SpatialContains { column, wkt },
        PredKind::SpatialCovers => Pred::SpatialCovers { column, wkt },
        PredKind::SpatialTouches => Pred::SpatialTouches { column, wkt },
        PredKind::SpatialCrosses => Pred::SpatialCrosses { column, wkt },
        PredKind::SpatialOverlaps => Pred::SpatialOverlaps { column, wkt },
        PredKind::SpatialEquals => Pred::SpatialEquals { column, wkt },
        PredKind::SpatialDisjoint => Pred::SpatialDisjoint { column, wkt },
        PredKind::SpatialWithin
        | PredKind::SpatialDWithin
        | PredKind::Eq
        | PredKind::GtNum
        | PredKind::LtNum
        | PredKind::Cmp
        | PredKind::In
        | PredKind::Between
        | PredKind::IsNull
        | PredKind::And
        | PredKind::Or
        | PredKind::Not
        | PredKind::JsonPath => Pred::SpatialWithin { column, wkt },
    }
}

/// A `DERIVE` with nesting, a real and an integer parameter, a negative constant, a
/// two-series call and an alias that must be back-quoted.
#[cfg(feature = "timeseries")]
fn derive_sample() -> Op {
    use crate::series_expr::{DeriveColumn, SeriesExpr, SeriesFunc};
    let v0 = || SeriesExpr::channel("v0");
    let smooth = SeriesExpr::call(SeriesFunc::Ewma, vec![v0()], vec![12.5]);
    let spread = SeriesExpr::call(
        SeriesFunc::Sub,
        vec![v0(), SeriesExpr::Const { value: -0.25 }],
        vec![],
    );
    Op::Derive {
        columns: vec![
            DeriveColumn {
                expr: SeriesExpr::call(SeriesFunc::Zscore, vec![smooth], vec![60.0]),
                name: "lat_z".into(),
            },
            DeriveColumn {
                expr: SeriesExpr::call(
                    SeriesFunc::Ic,
                    vec![SeriesExpr::channel("lat_z"), spread],
                    vec![20.0],
                ),
                name: "match".into(),
            },
        ],
    }
}
