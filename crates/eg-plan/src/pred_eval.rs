//! Per-row evaluation of the RELATIONAL predicate algebra (UQL-01) over a decoded JSON
//! property object — the Rust twin of `exec::pred_sql`'s SQL, for the paths that cannot
//! run DataFusion: incremental maintenance (`incremental::circuit`) and `Op::Expand`
//! edge predicates.
//!
//! It implements SQL's three-valued logic so the two paths agree row for row: a
//! comparison involving a missing/`null` property, or values of different types, is
//! UNKNOWN (`None`); `NOT UNKNOWN` is UNKNOWN; `AND`/`OR` follow Kleene's tables; a row
//! is kept only when the predicate is definitely `true`. JSONPath and spatial predicates
//! are not relational — [`is_relational`] says so and [`holds`] treats them as UNKNOWN.

use std::cmp::Ordering;

use eg_types::row_predicate::cmp_op_matches;
use eg_types::wire::{CmpOp, Pred, PredLiteral};
use serde_json::{Map, Value};

// Keep the spatial family exhaustive in one place. Both the relational classifier
// and row evaluator must refuse these variants, while the executor routes them to
// the geometry evaluator.
macro_rules! spatial_predicate {
    () => {
        Pred::SpatialWithin { .. }
            | Pred::SpatialDWithin { .. }
            | Pred::SpatialContains { .. }
            | Pred::SpatialCovers { .. }
            | Pred::SpatialTouches { .. }
            | Pred::SpatialCrosses { .. }
            | Pred::SpatialOverlaps { .. }
            | Pred::SpatialEquals { .. }
            | Pred::SpatialDisjoint { .. }
    };
}

/// Spatial predicates are handled by the geometry evaluator, never by SQL rows.
pub fn is_spatial(pred: &Pred) -> bool {
    matches!(pred, spatial_predicate!())
}

/// Three-valued truth: `Some(true)`, `Some(false)`, or UNKNOWN (`None`).
pub type Truth = Option<bool>;

/// Is `pred` (and everything under it) evaluable by [`holds`]?
pub fn is_relational(pred: &Pred) -> bool {
    match pred {
        Pred::Eq { .. }
        | Pred::GtNum { .. }
        | Pred::LtNum { .. }
        | Pred::Cmp { .. }
        | Pred::In { .. }
        | Pred::Between { .. }
        | Pred::IsNull { .. } => true,
        Pred::And { preds } | Pred::Or { preds } => preds.iter().all(is_relational),
        Pred::Not { pred } => is_relational(pred),
        Pred::JsonPath { .. } | spatial_predicate!() => false,
    }
}

/// Is the row kept by every predicate of an implicit conjunction?
pub fn all_hold(props: &Map<String, Value>, preds: &[Pred]) -> bool {
    preds.iter().all(|p| holds(props, p) == Some(true))
}

/// Evaluate `pred` against `props` under SQL three-valued logic.
pub fn holds(props: &Map<String, Value>, pred: &Pred) -> Truth {
    match pred {
        Pred::Eq { prop, value } => legacy_eq(props.get(prop), value),
        Pred::GtNum { prop, n } => {
            compare(props.get(prop), &PredLiteral::Num(*n)).map(|o| o == Ordering::Greater)
        }
        Pred::LtNum { prop, n } => {
            compare(props.get(prop), &PredLiteral::Num(*n)).map(|o| o == Ordering::Less)
        }
        Pred::Cmp { prop, op, value } => {
            compare(props.get(prop), value).map(|o| cmp_op_matches((*op).into(), o))
        }
        Pred::In { prop, values } => in_list(props.get(prop), values),
        Pred::Between { prop, lo, hi } => between(props.get(prop), lo, hi),
        Pred::IsNull { prop } => Some(matches!(props.get(prop), None | Some(Value::Null))),
        Pred::And { preds } => kleene_and(preds.iter().map(|p| holds(props, p))),
        Pred::Or { preds } => kleene_or(preds.iter().map(|p| holds(props, p))),
        Pred::Not { pred } => holds(props, pred).map(|b| !b),
        Pred::JsonPath { .. } | spatial_predicate!() => None,
    }
}

/// Order a stored value against a typed literal; UNKNOWN when either side is missing or
/// the types differ (SQL would not compare them).
fn compare(stored: Option<&Value>, lit: &PredLiteral) -> Option<Ordering> {
    match (stored?, lit) {
        (Value::Number(n), PredLiteral::Num(x)) => n.as_f64()?.partial_cmp(x),
        (Value::String(s), PredLiteral::Str(x)) => Some(s.as_str().cmp(x.as_str())),
        (Value::Bool(b), PredLiteral::Bool(x)) => Some(b.cmp(x)),
        _ => None,
    }
}

/// The legacy string `Eq`: the stored value's text equals `value` (mirrors DataFusion's
/// literal coercion against an Int/Float/Bool/Utf8 column).
fn legacy_eq(stored: Option<&Value>, value: &str) -> Truth {
    match stored? {
        Value::String(s) => Some(s == value),
        Value::Number(n) => Some(match value.parse::<f64>() {
            Ok(v) => n.as_f64() == Some(v),
            Err(_) => n.to_string() == value,
        }),
        Value::Bool(b) => Some(b.to_string() == value),
        Value::Null | Value::Array(_) | Value::Object(_) => None,
    }
}

fn in_list(stored: Option<&Value>, values: &[PredLiteral]) -> Truth {
    kleene_or(
        values
            .iter()
            .map(|v| compare(stored, v).map(|o| o == Ordering::Equal)),
    )
}

fn between(stored: Option<&Value>, lo: &PredLiteral, hi: &PredLiteral) -> Truth {
    let above = compare(stored, lo).map(|o| o != Ordering::Less);
    let below = compare(stored, hi).map(|o| o != Ordering::Greater);
    kleene_and([above, below].into_iter())
}

fn kleene_and(items: impl Iterator<Item = Truth>) -> Truth {
    let mut unknown = false;
    for t in items {
        match t {
            Some(false) => return Some(false),
            None => unknown = true,
            Some(true) => {}
        }
    }
    (!unknown).then_some(true)
}

fn kleene_or(items: impl Iterator<Item = Truth>) -> Truth {
    let mut unknown = false;
    for t in items {
        match t {
            Some(true) => return Some(true),
            None => unknown = true,
            Some(false) => {}
        }
    }
    (!unknown).then_some(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn row(v: Value) -> Map<String, Value> {
        v.as_object().cloned().unwrap_or_default()
    }

    fn cmp(prop: &str, op: CmpOp, value: PredLiteral) -> Pred {
        Pred::Cmp {
            prop: prop.into(),
            op,
            value,
        }
    }

    #[test]
    fn missing_property_is_unknown_and_not_does_not_rescue_it() {
        let r = row(json!({"a": 1}));
        let gt = cmp("b", CmpOp::Gt, PredLiteral::Num(0.0));
        assert_eq!(holds(&r, &gt), None);
        assert_eq!(
            holds(
                &r,
                &Pred::Not {
                    pred: Box::new(gt.clone())
                }
            ),
            None
        );
        assert!(!all_hold(&r, &[Pred::Not { pred: Box::new(gt) }]));
    }

    #[test]
    fn kleene_connectives_match_sql() {
        let r = row(json!({"a": 1, "s": "x"}));
        let unknown = cmp("zz", CmpOp::Eq, PredLiteral::Num(1.0));
        let yes = cmp("a", CmpOp::Eq, PredLiteral::Num(1.0));
        let no = cmp("s", CmpOp::Eq, PredLiteral::Str("y".into()));
        let or = Pred::Or {
            preds: vec![unknown.clone(), yes.clone()],
        };
        let and = Pred::And {
            preds: vec![unknown.clone(), no.clone()],
        };
        assert_eq!(holds(&r, &or), Some(true));
        assert_eq!(holds(&r, &and), Some(false));
        let and_unknown = Pred::And {
            preds: vec![unknown, yes],
        };
        assert_eq!(holds(&r, &and_unknown), None);
    }

    #[test]
    fn typed_literals_do_not_cross_types() {
        let r = row(json!({"n": 3, "s": "3", "b": true}));
        assert_eq!(
            holds(&r, &cmp("n", CmpOp::Eq, PredLiteral::Num(3.0))),
            Some(true)
        );
        assert_eq!(holds(&r, &cmp("s", CmpOp::Eq, PredLiteral::Num(3.0))), None);
        assert_eq!(
            holds(&r, &cmp("b", CmpOp::Ne, PredLiteral::Bool(false))),
            Some(true)
        );
        let between = Pred::Between {
            prop: "n".into(),
            lo: PredLiteral::Num(3.0),
            hi: PredLiteral::Num(3.0),
        };
        assert_eq!(holds(&r, &between), Some(true));
        let is_null = Pred::IsNull {
            prop: "gone".into(),
        };
        assert_eq!(holds(&r, &is_null), Some(true));
    }

    #[test]
    fn relational_comparison_uses_shared_ordering_semantics() {
        let props = row(json!({"n": 3}));
        for (op, literal, expected) in [
            (CmpOp::Eq, 3.0, true),
            (CmpOp::Ne, 3.0, false),
            (CmpOp::Lt, 4.0, true),
            (CmpOp::Le, 3.0, true),
            (CmpOp::Gt, 2.0, true),
            (CmpOp::Ge, 3.0, true),
        ] {
            assert_eq!(
                holds(&props, &cmp("n", op, PredLiteral::Num(literal))),
                Some(expected),
                "{op:?}"
            );
        }
    }
}
