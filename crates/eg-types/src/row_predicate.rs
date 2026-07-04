// CONCEPT:EG-KG.query.compound-predicate-decode — Serializable compound WHERE predicate for DML.
//
// A serde-only predicate AST that lives at the BOTTOM of the crate DAG (eg-types)
// so `eg-core` can evaluate it under a held write guard WITHOUT depending on the
// SQL layer (`eg-query`), which sits ABOVE it. `eg-query` decodes a sqlparser
// `Expr` tree into this AST (the decode lives there, where sqlparser lives); the
// engine then re-checks `eval()` against each candidate row's CURRENT value for
// serializable UPDATE/DELETE semantics.
//
// Comparison rules (`eval`):
//   * a column missing from the row reads as SQL `NULL`;
//   * two numbers compare NUMERICALLY, any other pair compares as LEXICAL strings;
//   * a comparison against `NULL` (either side) is SQL-unknown → `false`;
//   * `IN`/`BETWEEN` follow SQL set/range membership.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::cmp::Ordering;

/// The six scalar comparison operators a `Cmp` predicate can carry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// A serializable compound row predicate (CONCEPT:EG-KG.query.compound-predicate-decode). Decoded from a SQL
/// `WHERE` by `eg-query` and evaluated against a single decoded row (`col -> value`)
/// by `eg-core` under the write guard.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RowPredicate {
    And(Vec<RowPredicate>),
    Or(Vec<RowPredicate>),
    Not(Box<RowPredicate>),
    Cmp {
        col: String,
        op: CmpOp,
        value: Value,
    },
    In {
        col: String,
        values: Vec<Value>,
    },
    Between {
        col: String,
        low: Value,
        high: Value,
    },
    IsNull {
        col: String,
    },
    IsNotNull {
        col: String,
    },
}

impl RowPredicate {
    /// Evaluate this predicate against one decoded row. A column absent from `row`
    /// reads as `NULL`. See the module docs for the comparison rules.
    pub fn eval(&self, row: &serde_json::Map<String, Value>) -> bool {
        match self {
            RowPredicate::And(preds) => preds.iter().all(|p| p.eval(row)),
            RowPredicate::Or(preds) => preds.iter().any(|p| p.eval(row)),
            RowPredicate::Not(inner) => !inner.eval(row),
            RowPredicate::Cmp { col, op, value } => {
                let current = row.get(col).unwrap_or(&Value::Null);
                // SQL three-valued logic: any comparison touching NULL is unknown → false.
                if current.is_null() || value.is_null() {
                    return false;
                }
                match json_cmp(current, value) {
                    Some(ord) => match op {
                        CmpOp::Eq => ord == Ordering::Equal,
                        CmpOp::Ne => ord != Ordering::Equal,
                        CmpOp::Lt => ord == Ordering::Less,
                        CmpOp::Le => ord != Ordering::Greater,
                        CmpOp::Gt => ord == Ordering::Greater,
                        CmpOp::Ge => ord != Ordering::Less,
                    },
                    None => false,
                }
            }
            RowPredicate::In { col, values } => {
                let current = row.get(col).unwrap_or(&Value::Null);
                if current.is_null() {
                    return false;
                }
                values
                    .iter()
                    .any(|v| !v.is_null() && json_cmp(current, v) == Some(Ordering::Equal))
            }
            RowPredicate::Between { col, low, high } => {
                let current = row.get(col).unwrap_or(&Value::Null);
                if current.is_null() || low.is_null() || high.is_null() {
                    return false;
                }
                let ge_low = matches!(json_cmp(current, low), Some(o) if o != Ordering::Less);
                let le_high = matches!(json_cmp(current, high), Some(o) if o != Ordering::Greater);
                ge_low && le_high
            }
            RowPredicate::IsNull { col } => row.get(col).map(Value::is_null).unwrap_or(true),
            RowPredicate::IsNotNull { col } => row.get(col).map(|v| !v.is_null()).unwrap_or(false),
        }
    }
}

/// Order two JSON values: two numbers compare numerically; otherwise both are
/// coerced to their lexical string form and compared. `None` only when a numeric
/// value is non-comparable (e.g. NaN), which the callers treat as "no match".
fn json_cmp(a: &Value, b: &Value) -> Option<Ordering> {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => match (x.as_f64(), y.as_f64()) {
            (Some(xf), Some(yf)) => xf.partial_cmp(&yf),
            _ => Some(cmp_string(a).cmp(&cmp_string(b))),
        },
        _ => Some(cmp_string(a).cmp(&cmp_string(b))),
    }
}

/// Lexical string form of a value for non-numeric comparison.
fn cmp_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn row(pairs: &[(&str, Value)]) -> serde_json::Map<String, Value> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    fn cmp(col: &str, op: CmpOp, value: Value) -> RowPredicate {
        RowPredicate::Cmp {
            col: col.to_string(),
            op,
            value,
        }
    }

    #[test]
    fn numeric_comparisons_are_numeric() {
        let r = row(&[("rank", json!(7))]);
        assert!(cmp("rank", CmpOp::Gt, json!(2)).eval(&r));
        assert!(cmp("rank", CmpOp::Ge, json!(7)).eval(&r));
        assert!(!cmp("rank", CmpOp::Lt, json!(7)).eval(&r));
        assert!(cmp("rank", CmpOp::Le, json!(7)).eval(&r));
        assert!(cmp("rank", CmpOp::Ne, json!(8)).eval(&r));
        // 10 vs 9 numerically (NOT lexically where "10" < "9").
        let r2 = row(&[("rank", json!(10))]);
        assert!(cmp("rank", CmpOp::Gt, json!(9)).eval(&r2));
    }

    #[test]
    fn string_comparisons_are_lexical() {
        let r = row(&[("type", json!("Tool"))]);
        assert!(cmp("type", CmpOp::Eq, json!("Tool")).eval(&r));
        assert!(cmp("type", CmpOp::Lt, json!("Zebra")).eval(&r));
        assert!(!cmp("type", CmpOp::Gt, json!("Zebra")).eval(&r));
    }

    #[test]
    fn missing_column_is_null() {
        let r = row(&[("type", json!("Agent"))]);
        // missing `rank` reads as NULL → every scalar comparison is false.
        assert!(!cmp("rank", CmpOp::Eq, json!(1)).eval(&r));
        assert!(RowPredicate::IsNull {
            col: "rank".to_string()
        }
        .eval(&r));
        assert!(!RowPredicate::IsNotNull {
            col: "rank".to_string()
        }
        .eval(&r));
        // present, non-null column
        assert!(RowPredicate::IsNotNull {
            col: "type".to_string()
        }
        .eval(&r));
    }

    #[test]
    fn explicit_null_is_null() {
        let r = row(&[("rank", Value::Null)]);
        assert!(RowPredicate::IsNull {
            col: "rank".to_string()
        }
        .eval(&r));
        assert!(!cmp("rank", CmpOp::Eq, json!(0)).eval(&r));
    }

    #[test]
    fn in_list_membership() {
        let r = row(&[("type", json!("Tool"))]);
        let p = RowPredicate::In {
            col: "type".to_string(),
            values: vec![json!("Agent"), json!("Tool"), json!("Skill")],
        };
        assert!(p.eval(&r));
        let r2 = row(&[("type", json!("Other"))]);
        assert!(!p.eval(&r2));
    }

    #[test]
    fn between_range() {
        let p = RowPredicate::Between {
            col: "rank".to_string(),
            low: json!(2),
            high: json!(8),
        };
        assert!(p.eval(&row(&[("rank", json!(2))]))); // inclusive low
        assert!(p.eval(&row(&[("rank", json!(8))]))); // inclusive high
        assert!(p.eval(&row(&[("rank", json!(5))])));
        assert!(!p.eval(&row(&[("rank", json!(1))])));
        assert!(!p.eval(&row(&[("rank", json!(9))])));
    }

    #[test]
    fn and_or_not_compose() {
        let r = row(&[("rank", json!(5)), ("type", json!("Agent"))]);
        let and = RowPredicate::And(vec![
            cmp("rank", CmpOp::Gt, json!(2)),
            cmp("type", CmpOp::Eq, json!("Agent")),
        ]);
        assert!(and.eval(&r));
        let or = RowPredicate::Or(vec![
            cmp("rank", CmpOp::Gt, json!(100)),
            cmp("type", CmpOp::Eq, json!("Agent")),
        ]);
        assert!(or.eval(&r));
        let not = RowPredicate::Not(Box::new(cmp("type", CmpOp::Eq, json!("Tool"))));
        assert!(not.eval(&r));
        // De-composed false case.
        assert!(!RowPredicate::And(vec![
            cmp("rank", CmpOp::Gt, json!(2)),
            cmp("type", CmpOp::Eq, json!("Tool")),
        ])
        .eval(&r));
    }

    #[test]
    fn serde_roundtrip() {
        let p = RowPredicate::And(vec![
            cmp("rank", CmpOp::Ge, json!(2)),
            RowPredicate::In {
                col: "type".to_string(),
                values: vec![json!("Agent"), json!("Tool")],
            },
        ]);
        let s = serde_json::to_string(&p).unwrap();
        let back: RowPredicate = serde_json::from_str(&s).unwrap();
        assert_eq!(p, back);
    }
}
