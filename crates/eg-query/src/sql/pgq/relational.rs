//! The relational intermediate form a fixed SQL/PGQ pattern lowers into, and
//! the ONLY place its SQL text is rendered.
//!
//! Every identifier reaching the rendered text goes through
//! `SqlIdentifier::quoted_sql`, and every literal through the escaping in this
//! module, so no catalog text is ever interpolated raw.

use super::expr::BinaryOp;
use super::lex::SqlNumber;
use crate::tables::property_graph::{SqlIdentifier, SqlName};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalGraphPlan {
    pub(super) branches: Vec<RelationalSelect>,
    pub(super) output_columns: Vec<SqlIdentifier>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RelationalSelect {
    pub(super) from: RelationRef,
    pub(super) joins: Vec<RelationalJoin>,
    pub(super) predicates: Vec<RelationalExpr>,
    pub(super) projections: Vec<(RelationalExpr, SqlIdentifier)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RelationRef {
    pub(super) relation: SqlName,
    pub(super) alias: SqlIdentifier,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RelationalJoin {
    pub(super) relation: RelationRef,
    pub(super) conditions: Vec<(ColumnRef, ColumnRef)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ColumnRef {
    pub(super) relation_alias: SqlIdentifier,
    pub(super) column: SqlIdentifier,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum RelationalExpr {
    Column(ColumnRef),
    /// `ELEMENT_ID(v)`: the element table's identity, joined to its key. Two
    /// element tables have independent key spaces, so a bare key would collide
    /// across the `UNION ALL` a label disjunction lowers to.
    ElementId {
        element: String,
        key: ColumnRef,
    },
    Literal(RelationalLiteral),
    Not(Box<RelationalExpr>),
    Binary(Box<RelationalExpr>, BinaryOp, Box<RelationalExpr>),
}

/// A value literal in the relational form. Split from [`RelationalExpr`] so
/// literal rendering -- the escaping surface -- is one total function over
/// exactly the literal cases.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum RelationalLiteral {
    String(String),
    Number(SqlNumber),
    Boolean(bool),
    Null,
    CurrentDate,
}

impl RelationalLiteral {
    fn to_sql(&self) -> String {
        match self {
            Self::String(value) => format!("'{}'", value.replace('\'', "''")),
            Self::Number(value) => value.sql().into(),
            Self::Boolean(true) => "TRUE".into(),
            Self::Boolean(false) => "FALSE".into(),
            Self::Null => "NULL".into(),
            Self::CurrentDate => "CURRENT_DATE".into(),
        }
    }
}

impl RelationalGraphPlan {
    pub fn branch_count(&self) -> usize {
        self.branches.len()
    }

    pub fn output_columns(&self) -> &[SqlIdentifier] {
        &self.output_columns
    }

    pub fn to_sql(&self) -> String {
        self.branches
            .iter()
            .map(RelationalSelect::to_sql)
            .collect::<Vec<_>>()
            .join(" UNION ALL ")
    }
}

impl RelationalSelect {
    pub(super) fn to_sql(&self) -> String {
        let projection = self
            .projections
            .iter()
            .map(|(expr, alias)| format!("{} AS {}", expr.to_sql(), alias.quoted_sql()))
            .collect::<Vec<_>>()
            .join(", ");
        let mut sql = format!(
            "SELECT {projection} FROM {} AS {}",
            self.from.relation.quoted_sql(),
            self.from.alias.quoted_sql()
        );
        for join in &self.joins {
            let on = join
                .conditions
                .iter()
                .map(|(a, b)| format!("{} = {}", a.to_sql(), b.to_sql()))
                .collect::<Vec<_>>()
                .join(" AND ");
            sql.push_str(&format!(
                " JOIN {} AS {} ON {on}",
                join.relation.relation.quoted_sql(),
                join.relation.alias.quoted_sql()
            ));
        }
        if !self.predicates.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(
                &self
                    .predicates
                    .iter()
                    .map(RelationalExpr::to_sql)
                    .collect::<Vec<_>>()
                    .join(" AND "),
            );
        }
        sql
    }
}

impl ColumnRef {
    fn to_sql(&self) -> String {
        format!(
            "{}.{}",
            self.relation_alias.quoted_sql(),
            self.column.quoted_sql()
        )
    }
}

impl RelationalExpr {
    fn to_sql(&self) -> String {
        match self {
            Self::Column(value) => value.to_sql(),
            Self::ElementId { element, key } => format!(
                "('{}:' || CAST({} AS VARCHAR))",
                element.replace('\'', "''"),
                key.to_sql()
            ),
            Self::Literal(literal) => literal.to_sql(),
            Self::Not(value) => format!("NOT ({})", value.to_sql()),
            Self::Binary(a, op, b) => format!("({} {} {})", a.to_sql(), op.sql(), b.to_sql()),
        }
    }
}

impl BinaryOp {
    fn sql(self) -> &'static str {
        const SPELLINGS: [&str; 8] = ["=", "<>", "<", "<=", ">", ">=", "AND", "OR"];
        SPELLINGS[self as usize]
    }
}
