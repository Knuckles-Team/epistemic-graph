//! The row-level shape of a nearest-neighbour query the maintained ANN path may
//! narrow (RF-019).
//!
//! The maintained pushdown replaces a table with its nearest `LIMIT + OFFSET`
//! admitted rows and then runs the ORIGINAL statement over them. That is exact
//! only when the statement ranks rows one by one — no aggregate, grouping,
//! de-duplication or window over the table — and when its `WHERE` is one the
//! probe can apply itself, BEFORE ranking, with exactly SQL's meaning. This
//! module decides both, and names the reason when it cannot.

use std::ops::ControlFlow;

use datafusion::sql::sqlparser::ast::{
    visit_expressions, Expr, GroupByExpr, LimitClause, Offset, Query, Select, SetExpr, Statement,
    Value as SqlValue,
};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::parser::Parser;
use eg_types::RowPredicate;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::tables::{ColumnType, TableSchema};

/// Why a recognised nearest-neighbour query cannot take the maintained path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AnnDeclineReason {
    /// The statement aggregates, groups, de-duplicates or windows before ranking.
    NotRowRanked,
    /// The `WHERE` clause is outside the closed prefilter vocabulary.
    UnsupportedFilter,
    /// `OFFSET` is not a non-negative integer literal.
    UnsupportedOffset,
}

/// What the maintained path must honour from the statement.
#[derive(Debug, Clone, PartialEq)]
pub struct AnnQueryShape {
    /// The decoded `WHERE`, applied inside the probe.
    pub filter: Option<RowPredicate>,
    /// `OFFSET`; the narrowed relation must hold `LIMIT + OFFSET` rows.
    pub offset: usize,
}

/// Decode the shape of the single-table `SELECT` in `sql`.
pub(crate) fn ann_query_shape(sql: &str) -> Result<AnnQueryShape, AnnDeclineReason> {
    let statements = Parser::parse_sql(&PostgreSqlDialect {}, sql)
        .map_err(|_| AnnDeclineReason::NotRowRanked)?;
    let [Statement::Query(query)] = statements.as_slice() else {
        return Err(AnnDeclineReason::NotRowRanked);
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(AnnDeclineReason::NotRowRanked);
    };
    if !ranks_rows(select) {
        return Err(AnnDeclineReason::NotRowRanked);
    }
    let filter = select
        .selection
        .as_ref()
        .map(super::classify::decode_predicate)
        .transpose()
        .map_err(|_| AnnDeclineReason::UnsupportedFilter)?;
    Ok(AnnQueryShape {
        filter,
        offset: offset_of(query)?,
    })
}

/// Whether the probe can apply `filter` with exactly SQL's meaning over
/// `schema`: every leaf names an existing scalar column (exact spelling) and
/// compares it with a literal of that column's own kind, and there is no `NOT`
/// (SQL's `NOT NULL` is unknown, the row predicate's is true).
pub(crate) fn admissible_prefilter(filter: &RowPredicate, schema: &TableSchema) -> bool {
    match filter {
        RowPredicate::And(parts) | RowPredicate::Or(parts) => {
            parts.iter().all(|part| admissible_prefilter(part, schema))
        }
        RowPredicate::Not(_) => false,
        RowPredicate::Cmp { col, value, .. } => {
            leaf_admissible(schema, col, std::slice::from_ref(value))
        }
        RowPredicate::In { col, values } => leaf_admissible(schema, col, values),
        RowPredicate::Between { col, low, high } => {
            leaf_admissible(schema, col, &[low.clone(), high.clone()])
        }
        RowPredicate::IsNull { col } | RowPredicate::IsNotNull { col } => {
            null_test_admissible(schema, col)
        }
    }
}

/// A `SELECT` that ranks individual rows of its one table.
fn ranks_rows(select: &Select) -> bool {
    let ungrouped =
        matches!(&select.group_by, GroupByExpr::Expressions(exprs, _) if exprs.is_empty());
    let modified = select.distinct.is_some()
        || select.top.is_some()
        || select.having.is_some()
        || select.qualify.is_some()
        || !select.named_window.is_empty();
    ungrouped && !modified && !calls_function(select)
}

/// Whether any projected expression calls a function (an aggregate or window
/// among them) — conservatively, every call declines.
fn calls_function(select: &Select) -> bool {
    visit_expressions(&select.projection, |expr| {
        if matches!(expr, Expr::Function(_)) {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    })
    .is_break()
}

fn offset_of(query: &Query) -> Result<usize, AnnDeclineReason> {
    let offset = match &query.limit_clause {
        Some(LimitClause::LimitOffset {
            offset: Some(Offset { value, .. }),
            ..
        }) => value,
        Some(LimitClause::OffsetCommaLimit { offset, .. }) => offset,
        Some(LimitClause::LimitOffset { offset: None, .. }) | None => return Ok(0),
    };
    literal_usize(offset).ok_or(AnnDeclineReason::UnsupportedOffset)
}

fn literal_usize(expr: &Expr) -> Option<usize> {
    let Expr::Value(value) = expr else {
        return None;
    };
    let SqlValue::Number(text, _) = &value.value else {
        return None;
    };
    text.parse().ok()
}

fn leaf_admissible(schema: &TableSchema, col: &str, values: &[Value]) -> bool {
    let Some(kind) = schema
        .column(col)
        .and_then(|column| ScalarKind::of(column.ty))
    else {
        return false;
    };
    values.iter().all(|value| kind.accepts(value))
}

/// `IS [NOT] NULL` agrees with SQL on every column whose cells never render a
/// non-NULL value as JSON `null` — i.e. not on floats (NaN) or JSON (`'null'`).
fn null_test_admissible(schema: &TableSchema, col: &str) -> bool {
    schema.column(col).is_some_and(|column| {
        !matches!(
            column.ty,
            ColumnType::Float | ColumnType::Double | ColumnType::Json
        )
    })
}

/// The literal kinds whose row-predicate comparison agrees with SQL's.
#[derive(Clone, Copy)]
enum ScalarKind {
    Number,
    Text,
    Bool,
}

impl ScalarKind {
    fn of(ty: ColumnType) -> Option<Self> {
        // RowPredicate compares JSON numbers through f64. BigInt can exceed
        // f64's exact integer range, while Float/Double cells can contain NaN
        // (rendered as JSON null). Either case can disagree with SQL WHERE,
        // so keep those predicates on the ordinary scan path.
        if matches!(ty, ColumnType::Int) {
            return Some(Self::Number);
        }
        if matches!(ty, ColumnType::Text) {
            return Some(Self::Text);
        }
        matches!(ty, ColumnType::Bool).then_some(Self::Bool)
    }

    fn accepts(self, value: &Value) -> bool {
        match self {
            Self::Number => value.is_number(),
            Self::Text => value.is_string(),
            Self::Bool => value.is_boolean(),
        }
    }
}

#[cfg(test)]
mod prefilter_precision_tests {
    use super::*;
    use crate::tables::Column;
    use eg_types::CmpOp;
    use serde_json::json;

    #[test]
    fn numeric_prefilter_requires_exact_sql_comparison_semantics() {
        let schema = TableSchema::new(
            "docs",
            vec![
                Column::new("small", ColumnType::Int, false, false),
                Column::new("wide", ColumnType::BigInt, false, false),
                Column::new("float", ColumnType::Float, false, false),
                Column::new("double", ColumnType::Double, false, false),
            ],
        );
        let predicate = |col: &str, value: Value| RowPredicate::Cmp {
            col: col.to_string(),
            op: CmpOp::Eq,
            value,
        };
        assert!(admissible_prefilter(&predicate("small", json!(7)), &schema));
        for col in ["wide", "float", "double"] {
            assert!(!admissible_prefilter(
                &predicate(col, json!(9007199254740993_i64)),
                &schema
            ));
        }
    }
}
