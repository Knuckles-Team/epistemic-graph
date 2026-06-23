//! Hand-written recursive-descent parser for the Cypher subset (CONCEPT:KG-2.179).
//! Dep-free — a tiny char tokenizer + a recursive-descent grammar, no nom/pest.
//!
//! Grammar (the implemented subset):
//! ```text
//! query      := MATCH pattern (WHERE preds)? RETURN items (LIMIT int)?
//! pattern    := node (edge node)*
//! node       := '(' var? (':' label)? ')'
//! edge       := '-' '[' (':' reltype)? ('*' range)? ']' '->'
//!             | '<-' '[' (':' reltype)? ('*' range)? ']' '-'
//! range      := int ('..' int)?
//! preds      := pred (AND pred)*
//! pred       := var '.' prop op literal
//! op         := '=' | '<>' | '!=' | '<' | '<=' | '>' | '>='
//! literal    := string | number | true | false
//! items      := item (',' item)*
//! item       := var ('.' prop)?
//! ```
//! Identifiers/keywords are case-insensitive for keywords (MATCH/WHERE/RETURN/
//! LIMIT/AND/true/false); variable/label/prop names keep their case.

use serde_json::Value;

use super::plan::{
    CompareOp, CypherQuery, Direction, EdgePat, NodePat, Pattern, Predicate, ReturnItem,
};

/// A flat token. The tokenizer is whitespace-insensitive; punctuation is matched
/// greedily for the multi-char operators (`->`, `<-`, `<=`, `>=`, `<>`, `!=`,
/// `..`, `*`).
#[derive(Debug, Clone, PartialEq)]
enum Tok {
    LParen,
    RParen,
    LBracket,
    RBracket,
    Colon,
    Dot,
    Comma,
    Star,
    DotDot,
    Dash,       // '-'
    ArrowRight, // '->'
    ArrowLeft,  // '<-'
    Eq,
    Ne, // '<>' or '!='
    Lt,
    Le,
    Gt,
    Ge,
    Ident(String),
    Str(String),
    Num(f64),
}

fn tokenize(input: &str) -> Result<Vec<Tok>, String> {
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;
    let mut out = Vec::new();
    while i < chars.len() {
        let c = chars[i];
        match c {
            ws if ws.is_whitespace() => {
                i += 1;
            }
            '(' => {
                out.push(Tok::LParen);
                i += 1;
            }
            ')' => {
                out.push(Tok::RParen);
                i += 1;
            }
            '[' => {
                out.push(Tok::LBracket);
                i += 1;
            }
            ']' => {
                out.push(Tok::RBracket);
                i += 1;
            }
            ':' => {
                out.push(Tok::Colon);
                i += 1;
            }
            ',' => {
                out.push(Tok::Comma);
                i += 1;
            }
            '*' => {
                out.push(Tok::Star);
                i += 1;
            }
            '=' => {
                out.push(Tok::Eq);
                i += 1;
            }
            '.' => {
                if i + 1 < chars.len() && chars[i + 1] == '.' {
                    out.push(Tok::DotDot);
                    i += 2;
                } else {
                    out.push(Tok::Dot);
                    i += 1;
                }
            }
            '-' => {
                if i + 1 < chars.len() && chars[i + 1] == '>' {
                    out.push(Tok::ArrowRight);
                    i += 2;
                } else {
                    out.push(Tok::Dash);
                    i += 1;
                }
            }
            '<' => {
                if i + 1 < chars.len() && chars[i + 1] == '-' {
                    out.push(Tok::ArrowLeft);
                    i += 2;
                } else if i + 1 < chars.len() && chars[i + 1] == '=' {
                    out.push(Tok::Le);
                    i += 2;
                } else if i + 1 < chars.len() && chars[i + 1] == '>' {
                    out.push(Tok::Ne);
                    i += 2;
                } else {
                    out.push(Tok::Lt);
                    i += 1;
                }
            }
            '>' => {
                if i + 1 < chars.len() && chars[i + 1] == '=' {
                    out.push(Tok::Ge);
                    i += 2;
                } else {
                    out.push(Tok::Gt);
                    i += 1;
                }
            }
            '!' => {
                if i + 1 < chars.len() && chars[i + 1] == '=' {
                    out.push(Tok::Ne);
                    i += 2;
                } else {
                    return Err("unexpected '!'".into());
                }
            }
            '\'' | '"' => {
                let quote = c;
                i += 1;
                let mut s = String::new();
                while i < chars.len() && chars[i] != quote {
                    // Minimal escape handling: \' \" \\ pass the next char through.
                    if chars[i] == '\\' && i + 1 < chars.len() {
                        s.push(chars[i + 1]);
                        i += 2;
                    } else {
                        s.push(chars[i]);
                        i += 1;
                    }
                }
                if i >= chars.len() {
                    return Err("unterminated string literal".into());
                }
                i += 1; // closing quote
                out.push(Tok::Str(s));
            }
            d if d.is_ascii_digit() => {
                // Unsigned numeric literal. `-` is always a Dash token (handled
                // above); the grammar has no negative literals in this subset.
                let start = i;
                while i < chars.len()
                    && (chars[i].is_ascii_digit() || chars[i] == '.')
                    // stop before a '..' range token
                    && !(chars[i] == '.' && i + 1 < chars.len() && chars[i + 1] == '.')
                {
                    i += 1;
                }
                let num_str: String = chars[start..i].iter().collect();
                let n: f64 = num_str
                    .parse()
                    .map_err(|_| format!("bad number: {num_str}"))?;
                out.push(Tok::Num(n));
            }
            a if a.is_alphabetic() || a == '_' => {
                let start = i;
                while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                    i += 1;
                }
                let ident: String = chars[start..i].iter().collect();
                out.push(Tok::Ident(ident));
            }
            other => return Err(format!("unexpected character: {other:?}")),
        }
    }
    Ok(out)
}

struct Parser {
    toks: Vec<Tok>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.pos)
    }
    fn next(&mut self) -> Option<Tok> {
        let t = self.toks.get(self.pos).cloned();
        if t.is_some() {
            self.pos += 1;
        }
        t
    }
    fn expect(&mut self, t: &Tok) -> Result<(), String> {
        match self.next() {
            Some(ref got) if got == t => Ok(()),
            other => Err(format!("expected {t:?}, found {other:?}")),
        }
    }
    /// Consume an identifier, comparing case-insensitively against `kw`. Used for
    /// keywords (MATCH/WHERE/RETURN/LIMIT/AND).
    fn eat_keyword(&mut self, kw: &str) -> Result<(), String> {
        match self.next() {
            Some(Tok::Ident(s)) if s.eq_ignore_ascii_case(kw) => Ok(()),
            other => Err(format!("expected keyword {kw}, found {other:?}")),
        }
    }
    fn peek_keyword(&self, kw: &str) -> bool {
        matches!(self.peek(), Some(Tok::Ident(s)) if s.eq_ignore_ascii_case(kw))
    }
    fn ident(&mut self) -> Result<String, String> {
        match self.next() {
            Some(Tok::Ident(s)) => Ok(s),
            other => Err(format!("expected identifier, found {other:?}")),
        }
    }

    fn parse_query(&mut self) -> Result<CypherQuery, String> {
        self.eat_keyword("MATCH")?;
        let pattern = self.parse_pattern()?;

        let where_clause = if self.peek_keyword("WHERE") {
            self.eat_keyword("WHERE")?;
            self.parse_predicates()?
        } else {
            Vec::new()
        };

        self.eat_keyword("RETURN")?;
        let returns = self.parse_return_items()?;

        let limit = if self.peek_keyword("LIMIT") {
            self.eat_keyword("LIMIT")?;
            match self.next() {
                Some(Tok::Num(n)) if n >= 0.0 && n.fract() == 0.0 => Some(n as usize),
                other => return Err(format!("expected integer after LIMIT, found {other:?}")),
            }
        } else {
            None
        };

        if self.pos != self.toks.len() {
            return Err(format!(
                "trailing tokens after query: {:?}",
                &self.toks[self.pos..]
            ));
        }
        Ok(CypherQuery {
            pattern,
            where_clause,
            returns,
            limit,
        })
    }

    fn parse_pattern(&mut self) -> Result<Pattern, String> {
        let start = self.parse_node()?;
        let mut hops = Vec::new();
        while matches!(self.peek(), Some(Tok::Dash) | Some(Tok::ArrowLeft)) {
            let edge = self.parse_edge()?;
            let node = self.parse_node()?;
            hops.push((edge, node));
        }
        Ok(Pattern { start, hops })
    }

    fn parse_node(&mut self) -> Result<NodePat, String> {
        self.expect(&Tok::LParen)?;
        let mut var = None;
        let mut label = None;
        // optional var
        if let Some(Tok::Ident(_)) = self.peek() {
            var = Some(self.ident()?);
        }
        // optional :Label
        if matches!(self.peek(), Some(Tok::Colon)) {
            self.next();
            label = Some(self.ident()?);
        }
        self.expect(&Tok::RParen)?;
        Ok(NodePat { var, label })
    }

    /// Edge forms: `-[:REL]->`, `-[:REL*1..3]->`, `<-[:REL]-`. Direction is set by
    /// whether the leading token is `-` (right) or `<-` (left), and the trailing
    /// token closes it (`->` for right, `-` for left).
    fn parse_edge(&mut self) -> Result<EdgePat, String> {
        let direction = match self.next() {
            Some(Tok::Dash) => Direction::Right,
            Some(Tok::ArrowLeft) => Direction::Left,
            other => return Err(format!("expected edge start, found {other:?}")),
        };
        self.expect(&Tok::LBracket)?;
        let mut rel_type = None;
        if matches!(self.peek(), Some(Tok::Colon)) {
            self.next();
            rel_type = Some(self.ident()?);
        }
        let mut var_len = None;
        if matches!(self.peek(), Some(Tok::Star)) {
            self.next();
            var_len = Some(self.parse_range()?);
        }
        self.expect(&Tok::RBracket)?;
        // closing arrow
        match direction {
            Direction::Right => self.expect(&Tok::ArrowRight)?,
            Direction::Left => self.expect(&Tok::Dash)?,
        }
        Ok(EdgePat {
            rel_type,
            direction,
            var_len,
        })
    }

    /// `*` already consumed. Forms: `1..3`, `1..`, `..3`, `2` (exact), or bare
    /// (`1..` open) — we model open as a sane bound.
    fn parse_range(&mut self) -> Result<(usize, usize), String> {
        const OPEN_MAX: usize = 16;
        let lo = if let Some(Tok::Num(n)) = self.peek() {
            let n = *n;
            self.next();
            n as usize
        } else {
            1
        };
        if matches!(self.peek(), Some(Tok::DotDot)) {
            self.next();
            let hi = if let Some(Tok::Num(n)) = self.peek() {
                let n = *n;
                self.next();
                n as usize
            } else {
                OPEN_MAX
            };
            Ok((lo, hi))
        } else {
            // `*2` ⇒ exactly 2 hops.
            Ok((lo, lo))
        }
    }

    fn parse_predicates(&mut self) -> Result<Vec<Predicate>, String> {
        let mut preds = vec![self.parse_predicate()?];
        while self.peek_keyword("AND") {
            self.eat_keyword("AND")?;
            preds.push(self.parse_predicate()?);
        }
        Ok(preds)
    }

    fn parse_predicate(&mut self) -> Result<Predicate, String> {
        let var = self.ident()?;
        self.expect(&Tok::Dot)?;
        let prop = self.ident()?;
        let op = match self.next() {
            Some(Tok::Eq) => CompareOp::Eq,
            Some(Tok::Ne) => CompareOp::Ne,
            Some(Tok::Lt) => CompareOp::Lt,
            Some(Tok::Le) => CompareOp::Le,
            Some(Tok::Gt) => CompareOp::Gt,
            Some(Tok::Ge) => CompareOp::Ge,
            other => return Err(format!("expected comparison operator, found {other:?}")),
        };
        let value = self.parse_literal()?;
        Ok(Predicate {
            var,
            prop,
            op,
            value,
        })
    }

    fn parse_literal(&mut self) -> Result<Value, String> {
        match self.next() {
            Some(Tok::Str(s)) => Ok(Value::String(s)),
            Some(Tok::Num(n)) => {
                if n.fract() == 0.0 && n.abs() < 9.007e15 {
                    Ok(Value::Number((n as i64).into()))
                } else {
                    Ok(serde_json::Number::from_f64(n)
                        .map(Value::Number)
                        .unwrap_or(Value::Null))
                }
            }
            Some(Tok::Ident(s)) if s.eq_ignore_ascii_case("true") => Ok(Value::Bool(true)),
            Some(Tok::Ident(s)) if s.eq_ignore_ascii_case("false") => Ok(Value::Bool(false)),
            other => Err(format!("expected literal, found {other:?}")),
        }
    }

    fn parse_return_items(&mut self) -> Result<Vec<ReturnItem>, String> {
        let mut items = vec![self.parse_return_item()?];
        while matches!(self.peek(), Some(Tok::Comma)) {
            self.next();
            items.push(self.parse_return_item()?);
        }
        Ok(items)
    }

    fn parse_return_item(&mut self) -> Result<ReturnItem, String> {
        let var = self.ident()?;
        let prop = if matches!(self.peek(), Some(Tok::Dot)) {
            self.next();
            Some(self.ident()?)
        } else {
            None
        };
        Ok(ReturnItem { var, prop })
    }
}

/// Parse a Cypher-subset query string into the [`CypherQuery`] AST.
pub fn parse(input: &str) -> Result<CypherQuery, String> {
    let toks = tokenize(input)?;
    if toks.is_empty() {
        return Err("empty query".into());
    }
    let mut p = Parser { toks, pos: 0 };
    p.parse_query()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_single_node_match() {
        let q = parse("MATCH (a:Person) RETURN a").unwrap();
        assert_eq!(q.pattern.start.var.as_deref(), Some("a"));
        assert_eq!(q.pattern.start.label.as_deref(), Some("Person"));
        assert!(q.pattern.hops.is_empty());
        assert_eq!(q.returns.len(), 1);
        assert_eq!(q.returns[0].column(), "a");
    }

    #[test]
    fn parses_two_hop_with_where_and_limit() {
        let q = parse(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.name = 'Alice' RETURN a, b LIMIT 5",
        )
        .unwrap();
        assert_eq!(q.pattern.hops.len(), 1);
        assert_eq!(q.pattern.hops[0].0.rel_type.as_deref(), Some("KNOWS"));
        assert_eq!(q.pattern.hops[0].0.direction, Direction::Right);
        assert_eq!(q.where_clause.len(), 1);
        assert_eq!(q.where_clause[0].op, CompareOp::Eq);
        assert_eq!(q.limit, Some(5));
        assert_eq!(q.returns.len(), 2);
    }

    #[test]
    fn parses_variable_length_path() {
        let q = parse("MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) RETURN b").unwrap();
        assert_eq!(q.pattern.hops[0].0.var_len, Some((1, 3)));
    }

    #[test]
    fn parses_property_return_and_comparison() {
        let q = parse("MATCH (a:Doc) WHERE a.size > 10 RETURN a.size").unwrap();
        assert_eq!(q.returns[0].column(), "a.size");
        assert_eq!(q.where_clause[0].op, CompareOp::Gt);
        assert_eq!(q.where_clause[0].value, Value::Number(10.into()));
    }

    #[test]
    fn parses_left_direction() {
        let q = parse("MATCH (a:Person)<-[:KNOWS]-(b:Person) RETURN a").unwrap();
        assert_eq!(q.pattern.hops[0].0.direction, Direction::Left);
    }
}
