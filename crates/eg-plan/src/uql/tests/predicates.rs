//! The predicate algebra (UQL-01): operators, precedence, typed literals.

use crate::uql::{parse, UqlCode};
use eg_types::wire::{CmpOp, JsonPathOp, Op, Pred, PredLiteral};

fn preds(where_body: &str) -> Vec<Pred> {
    match parse(&format!("MATCH () |> WHERE {where_body}"))
        .unwrap()
        .ops
        .pop()
    {
        Some(Op::Filter { preds }) => preds,
        other => panic!("expected a Filter, got {other:?}"),
    }
}

fn cmp(prop: &str, op: CmpOp, value: PredLiteral) -> Pred {
    Pred::Cmp {
        prop: prop.into(),
        op,
        value,
    }
}

#[test]
fn every_comparison_operator_parses() {
    assert_eq!(
        preds("a != 1"),
        vec![cmp("a", CmpOp::Ne, PredLiteral::Num(1.0))]
    );
    assert_eq!(
        preds("a <> 'x'"),
        vec![cmp("a", CmpOp::Ne, PredLiteral::Str("x".into()))]
    );
    assert_eq!(
        preds("a >= 1"),
        vec![cmp("a", CmpOp::Ge, PredLiteral::Num(1.0))]
    );
    assert_eq!(
        preds("a <= -2.5"),
        vec![cmp("a", CmpOp::Le, PredLiteral::Num(-2.5))]
    );
    assert_eq!(
        preds("a > 1"),
        vec![Pred::GtNum {
            prop: "a".into(),
            n: 1.0
        }]
    );
    assert_eq!(
        preds("a < 1"),
        vec![Pred::LtNum {
            prop: "a".into(),
            n: 1.0
        }]
    );
}

#[test]
fn equality_is_typed_not_stringified() {
    assert_eq!(
        preds("rank = 1"),
        vec![cmp("rank", CmpOp::Eq, PredLiteral::Num(1.0))]
    );
    assert_eq!(
        preds("ok = TRUE"),
        vec![cmp("ok", CmpOp::Eq, PredLiteral::Bool(true))]
    );
    assert_eq!(
        preds("lang = 'en'"),
        vec![Pred::Eq {
            prop: "lang".into(),
            value: "en".into()
        }]
    );
    let e = parse("MATCH () |> WHERE x = NULL").unwrap_err();
    assert_eq!(e.code, UqlCode::NullLiteral);
    assert!(e.help.unwrap().contains("IS NULL"));
}

#[test]
fn and_binds_tighter_than_or_and_parens_group() {
    let a = || Pred::GtNum {
        prop: "a".into(),
        n: 1.0,
    };
    let b = || Pred::GtNum {
        prop: "b".into(),
        n: 1.0,
    };
    let c = || Pred::GtNum {
        prop: "c".into(),
        n: 1.0,
    };
    assert_eq!(
        preds("a > 1 OR b > 1 AND c > 1"),
        vec![Pred::Or {
            preds: vec![
                a(),
                Pred::And {
                    preds: vec![b(), c()]
                }
            ]
        }]
    );
    assert_eq!(
        preds("(a > 1 OR b > 1) AND c > 1"),
        vec![
            Pred::Or {
                preds: vec![a(), b()]
            },
            c()
        ]
    );
    assert_eq!(
        preds("NOT (a > 1 AND b > 1)"),
        vec![Pred::Not {
            pred: Box::new(Pred::And {
                preds: vec![a(), b()]
            })
        }]
    );
}

#[test]
fn in_between_and_null_tests() {
    assert_eq!(
        preds("s IN ('a', 2, FALSE)"),
        vec![Pred::In {
            prop: "s".into(),
            values: vec![
                PredLiteral::Str("a".into()),
                PredLiteral::Num(2.0),
                PredLiteral::Bool(false)
            ]
        }]
    );
    assert_eq!(
        preds("y NOT BETWEEN 1 AND 2"),
        vec![Pred::Not {
            pred: Box::new(Pred::Between {
                prop: "y".into(),
                lo: PredLiteral::Num(1.0),
                hi: PredLiteral::Num(2.0)
            })
        }]
    );
    assert_eq!(
        preds("x IS NOT NULL AND z IS NULL"),
        vec![
            Pred::Not {
                pred: Box::new(Pred::IsNull { prop: "x".into() })
            },
            Pred::IsNull { prop: "z".into() }
        ]
    );
}

#[test]
fn json_path_predicates() {
    assert_eq!(
        preds("$.meta.tags[0] = 'x' AND JSONPATH '$.a b' EXISTS"),
        vec![
            Pred::JsonPath {
                path: "$.meta.tags[0]".into(),
                op: JsonPathOp::Eq {
                    value: serde_json::json!("x")
                }
            },
            Pred::JsonPath {
                path: "$.a b".into(),
                op: JsonPathOp::Exists
            }
        ]
    );
    assert_eq!(
        preds("$.n @> JSON '[1, 2]'"),
        vec![Pred::JsonPath {
            path: "$.n".into(),
            op: JsonPathOp::Contains {
                value: serde_json::json!([1, 2])
            }
        }]
    );
    // An integer stays an integer JSON number, a float a float.
    assert_eq!(
        preds("$.n = 3"),
        vec![Pred::JsonPath {
            path: "$.n".into(),
            op: JsonPathOp::Eq {
                value: serde_json::json!(3)
            }
        }]
    );
}

#[test]
fn nesting_is_bounded() {
    let deep = format!(
        "MATCH () |> WHERE {}a > 1{}",
        "(".repeat(200),
        ")".repeat(200)
    );
    assert_eq!(parse(&deep).unwrap_err().code, UqlCode::NestingTooDeep);
    let nots = format!("MATCH () |> WHERE {}a > 1", "NOT ".repeat(200));
    assert_eq!(parse(&nots).unwrap_err().code, UqlCode::NestingTooDeep);
}

#[cfg(feature = "geo")]
#[test]
fn spatial_predicates() {
    assert_eq!(
        preds("SPATIAL DWITHIN(geom, 'POINT(0 0)', 2.5) AND SPATIAL COVERS(geom, 'POINT(1 1)')"),
        vec![
            Pred::SpatialDWithin {
                column: "geom".into(),
                wkt: "POINT(0 0)".into(),
                distance: 2.5
            },
            Pred::SpatialCovers {
                column: "geom".into(),
                wkt: "POINT(1 1)".into()
            }
        ]
    );
}
