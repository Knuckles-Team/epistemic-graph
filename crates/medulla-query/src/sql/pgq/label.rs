//! SQL/PGQ label expressions: the `!`/`&`/`|` algebra a pattern element's
//! label test is written in, and its bounded parser.

use std::collections::BTreeSet;

use super::lex::{Cursor, MAX_LABEL_EXPR_DEPTH, MAX_LABEL_EXPR_NODES};
use crate::tables::property_graph::SqlIdentifier;

/// A SQL/PGQ label expression: a label name combined with `!` (negation),
/// `&` (conjunction), and `|` (disjunction), in that binding order.
///
/// Every value of this type is produced by [`parse_label_expr`], which caps its
/// total node count at `MAX_LABEL_EXPR_NODES`. That cap — not the traversal
/// style of any one method — is what bounds recursion here: the derived `Drop`,
/// `Clone` and `PartialEq` on the boxed operands walk the same spine, and no
/// hand-written iterative walk can bound those.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LabelExpr {
    Label(SqlIdentifier),
    Not(Box<LabelExpr>),
    And(Box<LabelExpr>, Box<LabelExpr>),
    Or(Box<LabelExpr>, Box<LabelExpr>),
}

impl LabelExpr {
    /// Whether an element table carrying exactly `present` label names satisfies
    /// this expression. Depth is bounded by `MAX_LABEL_EXPR_NODES`.
    pub fn matches(&self, present: &BTreeSet<&SqlIdentifier>) -> bool {
        match self {
            Self::Label(name) => present.contains(name),
            Self::Not(inner) => !inner.matches(present),
            Self::And(left, right) => left.matches(present) && right.matches(present),
            Self::Or(left, right) => left.matches(present) || right.matches(present),
        }
    }

    /// Every label name referenced by this expression, so a caller can reject a
    /// pattern naming a label the catalog does not declare. Walked with an
    /// explicit work stack, so this method adds no machine-stack depth of its
    /// own on top of the node bound.
    pub fn label_names<'a>(&'a self, out: &mut BTreeSet<&'a SqlIdentifier>) {
        let mut pending = vec![self];
        while let Some(expression) = pending.pop() {
            match expression {
                Self::Label(name) => {
                    out.insert(name);
                }
                Self::Not(inner) => pending.push(inner),
                Self::And(left, right) | Self::Or(left, right) => {
                    pending.push(left);
                    pending.push(right);
                }
            }
        }
    }
}

/// Parse one whole label expression under a SHARED node budget.
///
/// `MAX_LABEL_EXPR_DEPTH` bounds `!`/parenthesis nesting; the budget bounds
/// total width, which nothing else does — the input byte cap alone would admit
/// tens of thousands of `|` terms.
pub(super) fn parse_label_expr(cursor: &mut Cursor, depth: usize) -> Result<LabelExpr, String> {
    let mut budget = MAX_LABEL_EXPR_NODES;
    parse_label_or(cursor, depth, &mut budget)
}

fn spend(budget: &mut usize) -> Result<(), String> {
    *budget = budget
        .checked_sub(1)
        .ok_or_else(|| format!("label expression exceeds {MAX_LABEL_EXPR_NODES} terms"))?;
    Ok(())
}

/// `label_expr := label_term ('|' label_term)*` — the loosest binding operator.
fn parse_label_or(
    cursor: &mut Cursor,
    depth: usize,
    budget: &mut usize,
) -> Result<LabelExpr, String> {
    let mut left = parse_label_and(cursor, depth, budget)?;
    while cursor.symbol('|') {
        spend(budget)?;
        left = LabelExpr::Or(
            Box::new(left),
            Box::new(parse_label_and(cursor, depth, budget)?),
        );
    }
    Ok(left)
}

/// `label_term := label_factor ('&' label_factor)*`.
fn parse_label_and(
    cursor: &mut Cursor,
    depth: usize,
    budget: &mut usize,
) -> Result<LabelExpr, String> {
    let mut left = parse_label_factor(cursor, depth, budget)?;
    while cursor.symbol('&') {
        spend(budget)?;
        left = LabelExpr::And(
            Box::new(left),
            Box::new(parse_label_factor(cursor, depth, budget)?),
        );
    }
    Ok(left)
}

/// `label_factor := '!' label_factor | '(' label_expr ')' | label`.
fn parse_label_factor(
    cursor: &mut Cursor,
    depth: usize,
    budget: &mut usize,
) -> Result<LabelExpr, String> {
    if depth > MAX_LABEL_EXPR_DEPTH {
        return Err("label expression too deep".into());
    }
    spend(budget)?;
    if cursor.symbol('!') {
        return Ok(LabelExpr::Not(Box::new(parse_label_factor(
            cursor,
            depth + 1,
            budget,
        )?)));
    }
    if cursor.symbol('(') {
        let value = parse_label_or(cursor, depth + 1, budget)?;
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
    fn label_expression_width_is_bounded() {
        // A flat `a|a|a|…` chain nests no deeper than one factor, so the depth
        // bound never fires on it. Before the node budget this built a left
        // spine of tens of thousands of boxes, and simply DROPPING it — let
        // alone matching over it — overflowed the machine stack.
        let at_cap = std::iter::repeat_n("a", super::MAX_LABEL_EXPR_NODES / 2).collect::<Vec<_>>();
        let accepted = format!(
            "GRAPH_TABLE (g MATCH (v:{}) COLUMNS (v.name))",
            at_cap.join("|")
        );
        let expression = first_label(&accepted).expect("a chain at the cap still parses");
        let mut names = BTreeSet::new();
        expression.label_names(&mut names);
        assert_eq!(names.len(), 1);
        let a = SqlIdentifier::unquoted("a").unwrap();
        assert!(expression.matches(&BTreeSet::from([&a])));

        let terms = std::iter::repeat_n("a", super::MAX_LABEL_EXPR_NODES + 8).collect::<Vec<_>>();
        for separator in ["|", "&"] {
            let sql = format!(
                "GRAPH_TABLE (g MATCH (v:{}) COLUMNS (v.name))",
                terms.join(separator)
            );
            assert!(
                parse_graph_table(&sql).unwrap_err().contains("exceeds"),
                "an over-cap `{separator}` chain was accepted"
            );
        }
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
