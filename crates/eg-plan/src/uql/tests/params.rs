//! Typed parameters (EH-371): values are bound, never spliced.

use crate::uql::{parse_with, Params, UqlCode};
use eg_types::wire::{CmpOp, Op, Pred, Scalar, UqlParam};

fn params(items: &[(&str, UqlParam)]) -> Params {
    items
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

#[test]
fn parameters_bind_as_typed_values() {
    let p = params(&[
        ("min", UqlParam::Num(2024.0)),
        ("lang", UqlParam::Str("en".into())),
        ("k", UqlParam::Num(5.0)),
        ("v", UqlParam::Vector(vec![1.0, -0.5])),
    ]);
    let plan = parse_with(
        "MATCH (:Doc) WHERE year >= $min AND lang = $lang |> RANK BY ~$v |> LIMIT $k",
        &p,
    )
    .unwrap();
    assert_eq!(
        plan.ops,
        vec![
            Op::Scan {
                label: "Doc".into()
            },
            Op::Filter {
                preds: vec![
                    Pred::Cmp {
                        prop: "year".into(),
                        op: CmpOp::Ge,
                        value: Scalar::Num(2024.0)
                    },
                    Pred::Eq {
                        prop: "lang".into(),
                        value: "en".into()
                    },
                ]
            },
            Op::Rank {
                query: vec![1.0, -0.5]
            },
            Op::Limit { k: 5 },
        ]
    );
}

/// THE injection property: a parameter holding query syntax is a string VALUE — the
/// plan's structure is exactly the structure of the text, whatever the value says.
#[test]
fn a_parameter_can_never_change_the_query_structure() {
    let hostile = "en' OR 1=1 |> MATCH () |> LIMIT 100000 --";
    let p = params(&[("lang", UqlParam::Str(hostile.into()))]);
    let plan = parse_with("MATCH (:Doc) WHERE lang = $lang |> LIMIT 1", &p).unwrap();
    assert_eq!(plan.ops.len(), 3);
    assert_eq!(
        plan.ops[1],
        Op::Filter {
            preds: vec![Pred::Eq {
                prop: "lang".into(),
                value: hostile.into()
            }]
        }
    );
}

#[test]
fn string_parameter_in_rank_position_embeds() {
    let p = params(&[("q", UqlParam::Str("graph dbs".into()))]);
    let plan = parse_with("MATCH () |> RANK BY ~$q", &p).unwrap();
    assert_eq!(
        plan.ops[1],
        Op::RankEmbed {
            text: "graph dbs".into()
        }
    );
}

#[test]
fn list_parameter_feeds_in() {
    let p = params(&[(
        "ids",
        UqlParam::List(vec![Scalar::Str("a".into()), Scalar::Str("b".into())]),
    )]);
    let plan = parse_with("MATCH () WHERE id IN $ids", &p).unwrap();
    assert_eq!(
        plan.ops[1],
        Op::Filter {
            preds: vec![Pred::In {
                prop: "id".into(),
                values: vec![Scalar::Str("a".into()), Scalar::Str("b".into())]
            }]
        }
    );
}

#[test]
fn unbound_mistyped_and_unused_parameters_are_typed_errors() {
    let none = Params::new();
    let e = parse_with("MATCH () |> LIMIT $k", &none).unwrap_err();
    assert_eq!(e.code, UqlCode::UnboundParameter);
    let wrong = params(&[("k", UqlParam::Str("five".into()))]);
    assert_eq!(
        parse_with("MATCH () |> LIMIT $k", &wrong).unwrap_err().code,
        UqlCode::ParameterType
    );
    let fractional = params(&[("k", UqlParam::Num(2.5))]);
    assert_eq!(
        parse_with("MATCH () |> LIMIT $k", &fractional)
            .unwrap_err()
            .code,
        UqlCode::ParameterType
    );
    let extra = params(&[("k", UqlParam::Num(1.0)), ("typo", UqlParam::Num(1.0))]);
    assert_eq!(
        parse_with("MATCH () |> LIMIT $k", &extra).unwrap_err().code,
        UqlCode::UnusedParameter
    );
}
