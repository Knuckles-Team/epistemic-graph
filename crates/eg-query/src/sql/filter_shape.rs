//! Shared recognition of pushable filter shapes for table providers.
//!
//! Every provider in this crate that narrows a scan from a `WHERE` conjunct asks
//! the same two questions: "is this a `column = literal` equality (either side)?"
//! and "which of these conjuncts do I report as `Inexact`?". Both live here so the
//! shape rule is decided in one place.

use datafusion::common::{Column, ScalarValue};
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};

/// The `(column, literal)` of a `column = literal` / `literal = column` equality;
/// `None` for any other expression shape.
pub(crate) fn column_eq_literal(expr: &Expr) -> Option<(&Column, &ScalarValue)> {
    let Expr::BinaryExpr(be) = expr else {
        return None;
    };
    if be.op != Operator::Eq {
        return None;
    }
    match (be.left.as_ref(), be.right.as_ref()) {
        (Expr::Column(c), Expr::Literal(v, _)) | (Expr::Literal(v, _), Expr::Column(c)) => {
            Some((c, v))
        }
        _ => None,
    }
}

/// Classify each conjunct: `Inexact` when `pushed` recognizes it (the provider
/// narrows the scan and DataFusion re-applies the filter above it), otherwise
/// `Unsupported` (an ordinary post-scan filter).
pub(crate) fn classify_pushdown(
    filters: &[&Expr],
    pushed: impl Fn(&Expr) -> bool,
) -> Vec<TableProviderFilterPushDown> {
    filters
        .iter()
        .map(|f| {
            if pushed(f) {
                TableProviderFilterPushDown::Inexact
            } else {
                TableProviderFilterPushDown::Unsupported
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::{col, lit};

    #[test]
    fn equality_is_recognized_on_either_side() {
        let left = col("src").eq(lit("a"));
        let right = lit("a").eq(col("src"));
        for expr in [left, right] {
            let (column, value) = column_eq_literal(&expr).expect("equality shape");
            assert_eq!(column.name, "src");
            assert_eq!(value, &ScalarValue::Utf8(Some("a".to_string())));
        }
    }

    #[test]
    fn other_shapes_are_not_equalities() {
        assert!(column_eq_literal(&col("n").gt(lit(1_i64))).is_none());
        assert!(column_eq_literal(&col("a").eq(col("b"))).is_none());
        assert!(column_eq_literal(&col("flag")).is_none());
    }

    #[test]
    fn classification_follows_the_predicate() {
        let pushed = col("src").eq(lit("a"));
        let kept = col("n").gt(lit(1_i64));
        let got = classify_pushdown(&[&pushed, &kept], |f| column_eq_literal(f).is_some());
        assert_eq!(
            got,
            vec![
                TableProviderFilterPushDown::Inexact,
                TableProviderFilterPushDown::Unsupported
            ]
        );
    }
}
