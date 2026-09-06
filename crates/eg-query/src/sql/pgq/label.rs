//! SQL/PGQ label expressions: the `!`/`&`/`|` algebra a pattern element's
//! label test is written in, and its bounded parser.

use std::collections::BTreeSet;

use super::lex::{Cursor, MAX_LABEL_EXPR_DEPTH};
use crate::tables::property_graph::SqlIdentifier;

/// A SQL/PGQ label expression: a label name combined with `!` (negation),
/// `&` (conjunction), and `|` (disjunction), in that binding order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LabelExpr {
    Label(SqlIdentifier),
    Not(Box<LabelExpr>),
    And(Box<LabelExpr>, Box<LabelExpr>),
    Or(Box<LabelExpr>, Box<LabelExpr>),
}

impl LabelExpr {
    /// Whether an element table carrying exactly `present` label names satisfies
    /// this expression.
    pub fn matches(&self, present: &BTreeSet<&SqlIdentifier>) -> bool {
        match self {
            Self::Label(name) => present.contains(name),
            Self::Not(inner) => !inner.matches(present),
            Self::And(left, right) => left.matches(present) && right.matches(present),
            Self::Or(left, right) => left.matches(present) || right.matches(present),
        }
    }

    /// Every label name referenced by this expression, so a caller can reject a
    /// pattern naming a label the catalog does not declare.
    pub fn label_names<'a>(&'a self, out: &mut BTreeSet<&'a SqlIdentifier>) {
        match self {
            Self::Label(name) => {
                out.insert(name);
            }
            Self::Not(inner) => inner.label_names(out),
            Self::And(left, right) | Self::Or(left, right) => {
                left.label_names(out);
                right.label_names(out);
            }
        }
    }
}

/// `label_expr := label_term ('|' label_term)*` — the loosest binding operator.
pub(super) fn parse_label_expr(cursor: &mut Cursor, depth: usize) -> Result<LabelExpr, String> {
    let mut left = parse_label_term(cursor, depth)?;
    while cursor.symbol('|') {
        left = LabelExpr::Or(Box::new(left), Box::new(parse_label_term(cursor, depth)?));
    }
    Ok(left)
}

/// `label_term := label_factor ('&' label_factor)*`.
fn parse_label_term(cursor: &mut Cursor, depth: usize) -> Result<LabelExpr, String> {
    let mut left = parse_label_factor(cursor, depth)?;
    while cursor.symbol('&') {
        left = LabelExpr::And(Box::new(left), Box::new(parse_label_factor(cursor, depth)?));
    }
    Ok(left)
}

/// `label_factor := '!' label_factor | '(' label_expr ')' | label`.
fn parse_label_factor(cursor: &mut Cursor, depth: usize) -> Result<LabelExpr, String> {
    if depth > MAX_LABEL_EXPR_DEPTH {
        return Err("label expression too deep".into());
    }
    if cursor.symbol('!') {
        return Ok(LabelExpr::Not(Box::new(parse_label_factor(
            cursor,
            depth + 1,
        )?)));
    }
    if cursor.symbol('(') {
        let value = parse_label_expr(cursor, depth + 1)?;
        cursor.expect(')')?;
        return Ok(value);
    }
    Ok(LabelExpr::Label(cursor.identifier()?))
}

#[cfg(test)]
mod tests {
    use super::super::ast::parse_graph_table;
    use super::*;

    fn label(value: &str) -> LabelExpr {
        LabelExpr::Label(SqlIdentifier::unquoted(value).expect("label"))
    }

    fn first_label(sql: &str) -> Option<LabelExpr> {
        parse_graph_table(sql)
            .expect("pattern parses")
            .path
            .first
            .label_expr
    }

    #[test]
    fn label_operators_bind_negation_then_conjunction_then_disjunction() {
        let element_label = first_label("GRAPH_TABLE (g MATCH (a:!x & y | z) COLUMNS (a.name))");
        assert_eq!(
            element_label,
            Some(LabelExpr::Or(
                Box::new(LabelExpr::And(
                    Box::new(LabelExpr::Not(Box::new(label("x")))),
                    Box::new(label("y")),
                )),
                Box::new(label("z")),
            ))
        );
        let grouped_label = first_label("GRAPH_TABLE (g MATCH (a:!(x | y)) COLUMNS (a.name))");
        assert_eq!(
            grouped_label,
            Some(LabelExpr::Not(Box::new(LabelExpr::Or(
                Box::new(label("x")),
                Box::new(label("y")),
            ))))
        );
    }

    #[test]
    fn label_expression_matching_and_name_collection_are_exact() {
        let element_label = first_label("GRAPH_TABLE (g MATCH (a:x & !y) COLUMNS (a.name))");
        let expression = element_label.expect("label expression");
        let mut names = BTreeSet::new();
        expression.label_names(&mut names);
        assert_eq!(names.len(), 2);

        let x = SqlIdentifier::unquoted("x").unwrap();
        let y = SqlIdentifier::unquoted("y").unwrap();
        assert!(expression.matches(&BTreeSet::from([&x])));
        assert!(!expression.matches(&BTreeSet::from([&x, &y])));
        assert!(!expression.matches(&BTreeSet::from([&y])));
        assert!(!expression.matches(&BTreeSet::new()));
    }

    #[test]
    fn nested_label_negation_is_bounded() {
        let deep = format!(
            "GRAPH_TABLE (g MATCH (a:{}x) COLUMNS (a.name))",
            "!".repeat(super::MAX_LABEL_EXPR_DEPTH + 2)
        );
        assert!(parse_graph_table(&deep)
            .unwrap_err()
            .contains("label expression too deep"));
    }
}
