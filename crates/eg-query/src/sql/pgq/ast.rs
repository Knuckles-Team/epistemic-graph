//! Parsed SQL/PGQ graph-pattern abstract syntax.

use std::collections::BTreeSet;

use super::expr::{parse_expr, GraphExpr};
use super::label::{parse_label_expr, LabelExpr};
use super::lex::{parse_name, Cursor, MAX_GRAPH_PATTERN_EDGES, MAX_GRAPH_TABLE_COLUMNS};
use crate::tables::property_graph::{SqlIdentifier, SqlName};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeDirection {
    Outgoing,
    Incoming,
    Either,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElementPattern {
    pub variable: Option<SqlIdentifier>,
    /// `None` means the pattern places no label restriction on the element.
    pub label_expr: Option<LabelExpr>,
    pub predicate: Option<GraphExpr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgePattern {
    pub element: ElementPattern,
    pub direction: EdgeDirection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathPattern {
    pub first: ElementPattern,
    pub steps: Vec<(EdgePattern, ElementPattern)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphTableColumn {
    pub expression: GraphExpr,
    pub alias: SqlIdentifier,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphTableQuery {
    pub graph: SqlName,
    pub path: PathPattern,
    pub columns: Vec<GraphTableColumn>,
}

pub(super) fn validate_variables(path: &PathPattern) -> Result<(), String> {
    let mut names = BTreeSet::new();
    let elements = std::iter::once(&path.first).chain(
        path.steps
            .iter()
            .flat_map(|(edge, vertex)| [&edge.element, vertex]),
    );
    for element in elements {
        if let Some(name) = &element.variable {
            if !names.insert(name) {
                return Err(format!("duplicate graph variable '{}'", name.value()));
            }
        }
    }
    Ok(())
}
pub(super) fn parse_path(cursor: &mut Cursor) -> Result<PathPattern, String> {
    let first = parse_element(cursor, '(', ')')?;
    let mut steps = Vec::new();
    while cursor.peek('-') || cursor.peek('<') {
        if steps.len() == MAX_GRAPH_PATTERN_EDGES {
            return Err("graph path too long".into());
        }
        steps.push((
            parse_edge_pattern(cursor)?,
            parse_element(cursor, '(', ')')?,
        ));
    }
    Ok(PathPattern { first, steps })
}

fn parse_edge_pattern(cursor: &mut Cursor) -> Result<EdgePattern, String> {
    let incoming = parse_edge_start(cursor)?;
    let bracketed = cursor.peek('[');
    let element = if bracketed {
        parse_element(cursor, '[', ']')?
    } else {
        empty_element_pattern()
    };
    let direction = parse_edge_direction(cursor, (incoming, bracketed))?;
    Ok(EdgePattern { element, direction })
}

fn parse_edge_start(cursor: &mut Cursor) -> Result<bool, String> {
    if cursor.symbol('<') {
        cursor.expect('-')?;
        Ok(true)
    } else {
        cursor.expect('-')?;
        Ok(false)
    }
}

fn parse_edge_direction(
    cursor: &mut Cursor,
    syntax: (bool, bool),
) -> Result<EdgeDirection, String> {
    let (incoming, bracketed) = syntax;
    if incoming {
        if bracketed {
            cursor.expect('-')?;
        }
        return Ok(EdgeDirection::Incoming);
    }
    if bracketed {
        cursor.expect('-')?;
    }
    Ok(if cursor.symbol('>') {
        EdgeDirection::Outgoing
    } else {
        EdgeDirection::Either
    })
}

fn parse_element(cursor: &mut Cursor, open: char, close: char) -> Result<ElementPattern, String> {
    cursor.expect(open)?;
    let variable = if cursor.is_identifier() && !cursor.any_keyword(&["IS", "WHERE"]) {
        Some(cursor.identifier()?)
    } else {
        None
    };
    // `:` is the standard label-test spelling; `IS` is the equivalent keyword
    // form. Both introduce exactly the same label expression.
    let label_expr = if cursor.keyword("IS") || cursor.symbol(':') {
        Some(parse_label_expr(cursor, 0)?)
    } else {
        None
    };
    let predicate = if cursor.keyword("WHERE") {
        Some(parse_expr(cursor, 0, 0)?)
    } else {
        None
    };
    cursor.expect(close)?;
    Ok(ElementPattern {
        variable,
        label_expr,
        predicate,
    })
}

fn empty_element_pattern() -> ElementPattern {
    ElementPattern {
        variable: None,
        label_expr: None,
        predicate: None,
    }
}
pub fn parse_graph_table(sql: &str) -> Result<GraphTableQuery, String> {
    let mut p = Cursor::new(sql)?;
    parse_graph_table_tokens(&mut p)
}

/// Parse the bounded executable SQL/PGQ read shape. The direct `GRAPH_TABLE`
/// form is retained for composition and tests; the SQL statement form is
/// exactly `SELECT * FROM GRAPH_TABLE (...)`. Outer projection, predicates,
/// joins, ordering, and limits remain ordinary relational operations and are
/// intentionally not accepted by this first lowering boundary.
pub fn parse_graph_table_sql(sql: &str) -> Result<GraphTableQuery, String> {
    let mut p = Cursor::new(sql)?;
    if p.keyword("SELECT") {
        p.expect('*')?;
        p.expect_keyword("FROM")?;
    }
    parse_graph_table_tokens(&mut p)
}

fn parse_graph_table_tokens(p: &mut Cursor) -> Result<GraphTableQuery, String> {
    p.expect_keyword("GRAPH_TABLE")?;
    p.expect('(')?;
    let graph = parse_name(p)?;
    p.expect_keyword("MATCH")?;
    let path = parse_path(p)?;
    p.expect_keyword("COLUMNS")?;
    p.expect('(')?;
    let mut columns = Vec::new();
    loop {
        if columns.len() == MAX_GRAPH_TABLE_COLUMNS {
            return Err("too many GRAPH_TABLE output columns".into());
        }
        let expression = parse_expr(p, 0, 0)?;
        let alias = if p.keyword("AS") {
            p.identifier()?
        } else if let GraphExpr::Property(_, property) = &expression {
            property.clone()
        } else {
            return Err("a computed GRAPH_TABLE column requires AS alias".into());
        };
        columns.push(GraphTableColumn { expression, alias });
        if !p.symbol(',') {
            break;
        }
    }
    p.expect(')')?;
    p.expect(')')?;
    p.finish()?;
    let mut names = BTreeSet::new();
    for column in &columns {
        if !names.insert(&column.alias) {
            return Err(format!(
                "duplicate output column '{}'",
                column.alias.value()
            ));
        }
    }
    validate_variables(&path)?;
    Ok(GraphTableQuery {
        graph,
        path,
        columns,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn label(value: &str) -> LabelExpr {
        LabelExpr::Label(SqlIdentifier::unquoted(value).expect("label"))
    }

    fn first_element(sql: &str) -> ElementPattern {
        parse_graph_table(sql).expect("pattern parses").path.first
    }

    #[test]
    fn colon_and_is_produce_the_same_label_test() {
        let colon = first_element("GRAPH_TABLE (g MATCH (a:person) COLUMNS (a.name))");
        let keyword = first_element("GRAPH_TABLE (g MATCH (a IS person) COLUMNS (a.name))");
        assert_eq!(colon, keyword);
        assert_eq!(colon.label_expr, Some(label("person")));
        assert_eq!(
            first_element("GRAPH_TABLE (g MATCH (:person) COLUMNS (a.name))").variable,
            None
        );
        assert_eq!(
            first_element("GRAPH_TABLE (g MATCH (a) COLUMNS (a.name))").label_expr,
            None
        );
    }
}
