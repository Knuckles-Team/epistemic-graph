//! SQL/PGQ value expressions: the bounded literal/property/comparison algebra a
//! pattern element's `WHERE` and a `COLUMNS` projection are written in.

use super::lex::{Cursor, SqlNumber, Token, MAX_GRAPH_EXPR_DEPTH};
use crate::tables::property_graph::SqlIdentifier;

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

/// A value literal. Kept apart from [`GraphExpr`] because a literal binds no
/// graph variable and consumes no expression-depth budget, so every consumer
/// that handles literals handles EXACTLY these and needs no impossible arm.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphLiteral {
    String(String),
    Number(SqlNumber),
    Boolean(bool),
    Null,
    CurrentDate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphExpr {
    Property(SqlIdentifier, SqlIdentifier),
    /// `ELEMENT_ID(v)` — the element's catalog key, resolved at lowering time.
    ElementId(SqlIdentifier),
    Literal(GraphLiteral),
    Not(Box<GraphExpr>),
    Binary(Box<GraphExpr>, BinaryOp, Box<GraphExpr>),
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
            Ok(GraphExpr::Literal(GraphLiteral::String(value)))
        }
        Some(Token::Number(value)) => {
            cursor.at += 1;
            Ok(GraphExpr::Literal(GraphLiteral::Number(SqlNumber::parse(
                &value,
            )?)))
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
        return Ok(GraphExpr::Literal(literal));
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
fn keyword_literal(word: &str) -> Option<GraphLiteral> {
    if word.eq_ignore_ascii_case("TRUE") {
        return Some(GraphLiteral::Boolean(true));
    }
    if word.eq_ignore_ascii_case("FALSE") {
        return Some(GraphLiteral::Boolean(false));
    }
    if word.eq_ignore_ascii_case("NULL") {
        return Some(GraphLiteral::Null);
    }
    if word.eq_ignore_ascii_case("CURRENT_DATE") {
        return Some(GraphLiteral::CurrentDate);
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

#[cfg(test)]
mod tests {
    use super::super::ast::parse_graph_table;
    use super::*;

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
