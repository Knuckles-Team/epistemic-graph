//! Parsed SQL/PGQ graph-pattern abstract syntax.

use std::collections::BTreeSet;

use super::label::{parse_label_expr, LabelExpr};
use super::lex::{
    parse_name, Cursor, SqlNumber, Token, MAX_GRAPH_EXPR_DEPTH, MAX_GRAPH_PATTERN_EDGES,
    MAX_GRAPH_TABLE_COLUMNS,
};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Eq,
    NotEq,
    Less,
    LessEq,
    Greater,
    GreaterEq,
    And,
    Or,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphExpr {
    Property(SqlIdentifier, SqlIdentifier),
    /// `ELEMENT_ID(v)` — the element's catalog key, resolved at lowering time.
    ElementId(SqlIdentifier),
    String(String),
    Number(SqlNumber),
    Boolean(bool),
    Null,
    CurrentDate,
    Not(Box<GraphExpr>),
    Binary(Box<GraphExpr>, BinaryOp, Box<GraphExpr>),
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

pub(super) fn parse_expr(cursor: &mut Cursor, min: u8, depth: usize) -> Result<GraphExpr, String> {
    if depth > MAX_GRAPH_EXPR_DEPTH {
        return Err("graph expression too deep".into());
    }
    let mut left = if cursor.keyword("NOT") {
        GraphExpr::Not(Box::new(parse_expr(cursor, 3, depth + 1)?))
    } else if cursor.symbol('(') {
        let value = parse_expr(cursor, 0, depth + 1)?;
        cursor.expect(')')?;
        value
    } else {
        parse_atom(cursor)?
    };
    while let Some((precedence, op)) = parse_binary_op(cursor) {
        if precedence < min {
            break;
        }
        cursor.at += 1;
        let right = parse_expr(cursor, precedence + 1, depth + 1)?;
        left = GraphExpr::Binary(Box::new(left), op, Box::new(right));
    }
    Ok(left)
}

fn parse_atom(cursor: &mut Cursor) -> Result<GraphExpr, String> {
    match cursor.tokens.get(cursor.at).cloned() {
        Some(Token::StringLiteral(value)) => {
            cursor.at += 1;
            Ok(GraphExpr::String(value))
        }
        Some(Token::Number(value)) => {
            cursor.at += 1;
            Ok(GraphExpr::Number(SqlNumber::parse(&value)?))
        }
        Some(Token::Word(value)) => parse_word_atom(cursor, &value),
        Some(Token::QuotedIdentifier(_)) => parse_property_atom(cursor),
        value => Err(format!("expected graph expression, found {value:?}")),
    }
}

/// A bare word is a keyword literal, an `ELEMENT_ID` application, or the
/// variable half of a property reference — in that order.
fn parse_word_atom(cursor: &mut Cursor, word: &str) -> Result<GraphExpr, String> {
    if let Some(literal) = keyword_literal(word) {
        cursor.at += 1;
        return Ok(literal);
    }
    if is_element_id_call(cursor, word) {
        cursor.at += 1;
        cursor.expect('(')?;
        let variable = cursor.identifier()?;
        cursor.expect(')')?;
        return Ok(GraphExpr::ElementId(variable));
    }
    parse_property_atom(cursor)
}

fn parse_property_atom(cursor: &mut Cursor) -> Result<GraphExpr, String> {
    let variable = cursor.identifier()?;
    cursor.expect('.')?;
    Ok(GraphExpr::Property(variable, cursor.identifier()?))
}

/// The keyword-spelled literals this bounded expression grammar accepts.
fn keyword_literal(word: &str) -> Option<GraphExpr> {
    if word.eq_ignore_ascii_case("TRUE") {
        return Some(GraphExpr::Boolean(true));
    }
    if word.eq_ignore_ascii_case("FALSE") {
        return Some(GraphExpr::Boolean(false));
    }
    if word.eq_ignore_ascii_case("NULL") {
        return Some(GraphExpr::Null);
    }
    if word.eq_ignore_ascii_case("CURRENT_DATE") {
        return Some(GraphExpr::CurrentDate);
    }
    None
}

fn parse_binary_op(cursor: &Cursor) -> Option<(u8, BinaryOp)> {
    match cursor.tokens.get(cursor.at) {
        Some(Token::Word(v)) if v.eq_ignore_ascii_case("OR") => Some((1, BinaryOp::Or)),
        Some(Token::Word(v)) if v.eq_ignore_ascii_case("AND") => Some((2, BinaryOp::And)),
        Some(Token::Symbol('=')) => Some((3, BinaryOp::Eq)),
        Some(Token::NotEqual) => Some((3, BinaryOp::NotEq)),
        Some(Token::Symbol('<')) => Some((3, BinaryOp::Less)),
        Some(Token::LessEqual) => Some((3, BinaryOp::LessEq)),
        Some(Token::Symbol('>')) => Some((3, BinaryOp::Greater)),
        Some(Token::GreaterEqual) => Some((3, BinaryOp::GreaterEq)),
        _ => None,
    }
}

/// `ELEMENT_ID` is only the standard element-identity function when it is
/// immediately applied; a bare word of the same spelling stays an ordinary
/// variable reference.
fn is_element_id_call(cursor: &Cursor, word: &str) -> bool {
    word.eq_ignore_ascii_case("ELEMENT_ID")
        && matches!(cursor.tokens.get(cursor.at + 1), Some(Token::Symbol('(')))
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

    #[test]
    fn element_id_is_a_call_but_a_bare_word_stays_a_variable() {
        let query =
            parse_graph_table("GRAPH_TABLE (g MATCH (a:person) COLUMNS (ELEMENT_ID(a) AS eid))")
                .expect("element id parses");
        assert_eq!(
            query.columns[0].expression,
            GraphExpr::ElementId(SqlIdentifier::unquoted("a").unwrap())
        );
        let property = parse_graph_table(
            "GRAPH_TABLE (g MATCH (element_id:person) COLUMNS (element_id.name))",
        )
        .expect("bare word stays a variable");
        assert!(matches!(
            property.columns[0].expression,
            GraphExpr::Property(_, _)
        ));
        assert!(
            parse_graph_table("GRAPH_TABLE (g MATCH (a:person) COLUMNS (ELEMENT_ID(a)))")
                .unwrap_err()
                .contains("requires AS alias")
        );
    }
}
