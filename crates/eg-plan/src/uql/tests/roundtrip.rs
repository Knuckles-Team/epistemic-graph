//! The printer ⇄ parser property (UQL-06): for every generated plan the printer
//! accepts, `parse(print(p)) == canonicalize(p)`. Generated values deliberately include
//! quotes, back-quotes, unicode, reserved words, negative zero and extreme magnitudes.

use eg_types::wire::{CmpOp, EdgeDir, JsonPathOp, Op, Plan, Pred, Scalar, TimeAxis};
use proptest::prelude::*;

use crate::uql::{canonicalize, parse};

fn text() -> impl Strategy<Value = String> {
    prop_oneof![
        "\\PC{0,10}",
        Just("LIMIT".to_string()),
        Just("it's `quoted`".to_string()),
        "[a-z_][a-z0-9_]{0,6}",
    ]
}

fn finite() -> impl Strategy<Value = f64> {
    prop_oneof![
        any::<f64>().prop_filter("finite", |n| n.is_finite()),
        Just(-0.0),
        Just(1e300),
        Just(-1e-300),
        -1000.0..1000.0f64,
    ]
}

fn finite32() -> impl Strategy<Value = f32> {
    any::<f32>().prop_filter("finite", |n| n.is_finite())
}

fn scalar() -> impl Strategy<Value = Scalar> {
    prop_oneof![
        text().prop_map(Scalar::Str),
        finite().prop_map(Scalar::Num),
        any::<bool>().prop_map(Scalar::Bool),
    ]
}

fn cmp_op() -> impl Strategy<Value = CmpOp> {
    prop_oneof![
        Just(CmpOp::Eq),
        Just(CmpOp::Ne),
        Just(CmpOp::Gt),
        Just(CmpOp::Ge),
        Just(CmpOp::Lt),
        Just(CmpOp::Le),
    ]
}

fn json() -> impl Strategy<Value = serde_json::Value> {
    let leaf = prop_oneof![
        Just(serde_json::Value::Null),
        any::<bool>().prop_map(serde_json::Value::Bool),
        any::<i64>().prop_map(|n| serde_json::json!(n)),
        finite().prop_map(|n| serde_json::json!(n)),
        text().prop_map(serde_json::Value::String),
    ];
    leaf.prop_recursive(2, 8, 3, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..3).prop_map(serde_json::Value::Array),
            prop::collection::btree_map("[a-z]{1,3}", inner, 0..3)
                .prop_map(|m| serde_json::Value::Object(m.into_iter().collect())),
        ]
    })
}

fn atom() -> impl Strategy<Value = Pred> {
    prop_oneof![
        (text(), text()).prop_map(|(prop, value)| Pred::Eq { prop, value }),
        (text(), finite()).prop_map(|(prop, n)| Pred::GtNum { prop, n }),
        (text(), finite()).prop_map(|(prop, n)| Pred::LtNum { prop, n }),
        (text(), cmp_op(), scalar()).prop_map(|(prop, op, value)| Pred::Cmp { prop, op, value }),
        (text(), prop::collection::vec(scalar(), 1..4))
            .prop_map(|(prop, values)| Pred::In { prop, values }),
        (text(), scalar(), scalar()).prop_map(|(prop, lo, hi)| Pred::Between { prop, lo, hi }),
        text().prop_map(|prop| Pred::IsNull { prop }),
        (text(), json()).prop_map(|(path, value)| Pred::JsonPath {
            path: format!("$.{path}"),
            op: JsonPathOp::Contains { value }
        }),
    ]
}

fn pred() -> impl Strategy<Value = Pred> {
    atom().prop_recursive(3, 16, 3, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 2..4).prop_map(|preds| Pred::And { preds }),
            prop::collection::vec(inner.clone(), 2..4).prop_map(|preds| Pred::Or { preds }),
            inner.prop_map(|p| Pred::Not { pred: Box::new(p) }),
        ]
    })
}

fn dir() -> impl Strategy<Value = EdgeDir> {
    prop_oneof![Just(EdgeDir::Out), Just(EdgeDir::In), Just(EdgeDir::Both)]
}

fn hops() -> impl Strategy<Value = (usize, usize)> {
    (0usize..5, 0usize..5).prop_map(|(a, b)| (a.min(b), a.max(b)))
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        text().prop_map(|label| Op::Scan { label }),
        Just(Op::ScanAll {}),
        prop::collection::vec(pred(), 1..3).prop_map(|preds| Op::Filter { preds }),
        (text(), hops()).prop_map(|(rel, (min, max))| Op::Traverse { rel, min, max }),
        (
            proptest::option::of(text()),
            dir(),
            hops(),
            prop::collection::vec(pred(), 0..2)
        )
            .prop_map(|(rel, dir, (min, max), edge_preds)| Op::Expand {
                rel,
                dir,
                min,
                max,
                edge_preds
            }),
        prop::collection::vec(finite32(), 0..4).prop_map(|query| Op::Rank { query }),
        text().prop_map(|text| Op::RankEmbed { text }),
        text().prop_map(|center| Op::RankNodeDistance { center }),
        Just(Op::RankMentions {}),
        (finite32(), any::<usize>()).prop_map(|(lambda, k)| Op::RankMmr { lambda, k }),
        (finite(), any::<bool>()).prop_map(|(ts, tx)| Op::AsOf {
            ts,
            axis: if tx {
                TimeAxis::Transaction
            } else {
                TimeAxis::Valid
            }
        }),
        finite().prop_map(|secs| Op::Window { secs }),
        (
            finite(),
            prop::sample::select(vec!["mean", "sum", "min", "max", "count", "first", "last"])
        )
            .prop_map(|(secs, agg)| Op::WindowAgg {
                secs,
                agg: agg.into()
            }),
        text().prop_map(|name| Op::Foreign { name }),
        any::<usize>().prop_map(|k| Op::Limit { k }),
        prop::collection::vec(text(), 1..3).prop_map(|channels| Op::Project { channels }),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn print_then_parse_is_canonical_identity(ops in prop::collection::vec(op(), 1..6)) {
        let plan = Plan::new(ops);
        let text = plan.to_uql().expect("generated plans are printable");
        let back = parse(&text).map_err(|e| e.render(&text));
        prop_assert_eq!(back, Ok(canonicalize(&plan)), "text:\n{}", text);
    }
}
