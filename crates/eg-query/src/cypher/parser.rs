//! Hand-written recursive-descent parser for the Cypher subset (CONCEPT:EG-KG.query.dep-free-behind).
//! Dep-free — a tiny char tokenizer + a recursive-descent grammar, no nom/pest.
//!
//! Grammar (the implemented subset; CONCEPT:EG-KG.query.eg-extend-read-side/EG-063 extend the read side):
//! ```text
//! query      := stage+ RETURN retbody
//! stage      := ('OPTIONAL')? 'MATCH' (var '=')? pattern ('WHERE' expr)?
//!             | 'WITH' withitems ('WHERE' expr)?
//! retbody    := ('DISTINCT')? ('*' | items) ('ORDER' 'BY' keys)? ('SKIP' int)? ('LIMIT' int)?
//! pattern    := node (edge node | group)*
//! node       := '(' var? (':' label)? ('{' propmap '}')? ')'
//! edge       := '-' '[' (var)? (':' reltype)? ('*' range)? ']' '->'
//!             | '<-' '[' (var)? (':' reltype)? ('*' range)? ']' '-'
//! range      := int ('..' int)?
//! group      := '(' pattern ')' quantifier node?     -- Cypher 25 QPP, e.g.
//!                                                     -- `((a)-[:REL]->(b)){1,3}`
//!                                                     -- (CONCEPT:EG-KG.query.quantified-path-pattern)
//! quantifier := '{' int (',' int?)? '}'
//! expr       := or
//! or         := and ('OR' and)*
//! and        := primary ('AND' primary)*
//! primary    := '(' or ')' | cond
//! cond       := var '.' prop test
//! test       := op literal
//!             | 'IN' '[' literal (',' literal)* ']'
//!             | ('STARTS'|'ENDS') 'WITH' string
//!             | 'CONTAINS' string
//!             | 'IS' ('NOT')? 'NULL'
//! item       := expr ('AS' alias)?
//! expr(proj) := 'count' '(' '*' ')' | agg '(' (var ('.' prop)?) ')' | var ('.' prop)?
//! op         := '=' | '<>' | '!=' | '<' | '<=' | '>' | '>='
//! literal    := string | number | true | false
//! ```
//! Identifiers/keywords are case-insensitive for keywords; variable/label/prop
//! names keep their case.

use serde_json::Value;

use super::plan::{
    AggArg, AggFunc, CompareOp, Condition, CypherQuery, Direction, EdgePat, Expr, ListExpr,
    NodePat, OrderKey, Pattern, PropVal, QuantifiedGroup, ReadStage, RemoveItem, ReturnItem,
    ReturnSpec, SetItem, Statement, Test, WhereExpr, WithItem, WriteOp, WriteQuery, YieldItem,
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
    LBrace,
    RBrace,
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
    /// `$name` — a query parameter reference (CONCEPT:EG-KG.query.param-list-drives-unwind).
    Param(String),
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
            '{' => {
                out.push(Tok::LBrace);
                i += 1;
            }
            '}' => {
                out.push(Tok::RBrace);
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
            '$' => {
                // `$name` parameter reference (CONCEPT:EG-KG.query.param-list-drives-unwind).
                i += 1;
                let start = i;
                while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                    i += 1;
                }
                if i == start {
                    return Err("expected a parameter name after '$'".into());
                }
                let name: String = chars[start..i].iter().collect();
                out.push(Tok::Param(name));
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
    fn peek2(&self) -> Option<&Tok> {
        self.toks.get(self.pos + 1)
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
    /// Consume an identifier, comparing case-insensitively against `kw`.
    fn eat_keyword(&mut self, kw: &str) -> Result<(), String> {
        match self.next() {
            Some(Tok::Ident(s)) if s.eq_ignore_ascii_case(kw) => Ok(()),
            other => Err(format!("expected keyword {kw}, found {other:?}")),
        }
    }
    fn peek_keyword(&self, kw: &str) -> bool {
        matches!(self.peek(), Some(Tok::Ident(s)) if s.eq_ignore_ascii_case(kw))
    }
    fn peek2_keyword(&self, kw: &str) -> bool {
        matches!(self.peek2(), Some(Tok::Ident(s)) if s.eq_ignore_ascii_case(kw))
    }
    fn ident(&mut self) -> Result<String, String> {
        match self.next() {
            Some(Tok::Ident(s)) => Ok(s),
            other => Err(format!("expected identifier, found {other:?}")),
        }
    }

    fn parse_pattern(&mut self) -> Result<Pattern, String> {
        let start = self.parse_node()?;
        let mut hops = Vec::new();
        loop {
            if matches!(self.peek(), Some(Tok::Dash) | Some(Tok::ArrowLeft)) {
                let edge = self.parse_edge()?;
                let node = self.parse_node()?;
                hops.push((edge, node));
            } else if self.at_quantified_group_start() {
                hops.push(self.parse_quantified_group()?);
            } else {
                break;
            }
        }
        Ok(Pattern { start, hops })
    }

    /// A quantified-path-pattern GROUP is disambiguated from a plain node purely
    /// by lookahead: a plain node's `(` is followed by a var/`:`/`{`/`)`, never by
    /// another `(` — only a group's outer paren wraps an inner pattern that itself
    /// opens with the inner start node's `(` (CONCEPT:EG-KG.query.quantified-path-pattern).
    fn at_quantified_group_start(&self) -> bool {
        matches!(self.peek(), Some(Tok::LParen)) && matches!(self.peek2(), Some(Tok::LParen))
    }

    /// `((inner-pattern)){min,max} node?` — a Cypher 25 quantified path pattern
    /// (CONCEPT:EG-KG.query.quantified-path-pattern), e.g. `((a)-[:REL]->(b)){1,3}`. Compiles
    /// to a synthetic `(EdgePat, NodePat)` hop whose `EdgePat.group` carries the
    /// inner sub-pattern + quantifier; matched in `exec.rs` by repeated
    /// whole-subpattern expansion (`group_reachable`), a generalization of the
    /// single-relationship `*min..max` BFS. The optional trailing `node` (e.g.
    /// `(y:Person)`) constrains the LAST repetition's end position, exactly like
    /// an ordinary hop's end node.
    fn parse_quantified_group(&mut self) -> Result<(EdgePat, NodePat), String> {
        self.expect(&Tok::LParen)?; // the group's own outer paren
        let inner = self.parse_pattern()?; // e.g. (a)-[:REL]->(b)
        self.expect(&Tok::RParen)?; // closes the group
        if inner.hops.is_empty() {
            return Err(
                "a quantified path pattern group must contain at least one relationship hop"
                    .into(),
            );
        }
        let quantifier = self.parse_quantifier()?;
        let group = QuantifiedGroup {
            start: inner.start,
            hops: inner.hops,
            quantifier,
        };
        let edge = EdgePat {
            rel_type: None,
            direction: Direction::Right, // unused: `group` drives matching, not this field
            var_len: None,
            var: None,
            props: None,
            group: Some(Box::new(group)),
        };
        // An explicit trailing node constrains the final repetition's end
        // position (`(y:Label)`). A following group-start `((` is NOT consumed
        // here — it chains onto the NEXT hop in the outer loop instead.
        let node = if matches!(self.peek(), Some(Tok::LParen)) && !self.at_quantified_group_start()
        {
            self.parse_node()?
        } else {
            NodePat {
                var: None,
                label: None,
                props: None,
            }
        };
        Ok((edge, node))
    }

    /// `{min,max}` / `{min,}` / `{n}` — a QPP repetition quantifier
    /// (CONCEPT:EG-KG.query.quantified-path-pattern). Mirrors [`Self::parse_range`]'s bound
    /// handling (open-max defaults to the same `OPEN_MAX`).
    fn parse_quantifier(&mut self) -> Result<(usize, usize), String> {
        const OPEN_MAX: usize = 16;
        self.expect(&Tok::LBrace)?;
        let lo = match self.next() {
            Some(Tok::Num(n)) => n as usize,
            other => return Err(format!("expected quantifier lower bound, found {other:?}")),
        };
        let hi = if matches!(self.peek(), Some(Tok::Comma)) {
            self.next();
            match self.peek() {
                Some(Tok::Num(n)) => {
                    let n = *n;
                    self.next();
                    n as usize
                }
                _ => OPEN_MAX,
            }
        } else {
            lo
        };
        self.expect(&Tok::RBrace)?;
        Ok((lo, hi))
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
        // optional inline property map `{k: v, …}` (write path; CONCEPT:EG-KG.query.register-each-user-table).
        let props = if matches!(self.peek(), Some(Tok::LBrace)) {
            Some(self.parse_prop_map()?)
        } else {
            None
        };
        self.expect(&Tok::RParen)?;
        Ok(NodePat { var, label, props })
    }

    /// Parse an inline `{ key: value, … }` property map (CONCEPT:EG-KG.query.register-each-user-table/EG-141). Keys
    /// are identifiers; values are a literal, a `$param`, or a bound-variable
    /// reference (`{id: x}`). An empty `{}` is valid.
    fn parse_prop_map(&mut self) -> Result<Vec<(String, PropVal)>, String> {
        self.expect(&Tok::LBrace)?;
        let mut out = Vec::new();
        if matches!(self.peek(), Some(Tok::RBrace)) {
            self.next();
            return Ok(out);
        }
        loop {
            let key = self.ident()?;
            self.expect(&Tok::Colon)?;
            let val = self.parse_prop_val()?;
            out.push((key, val));
            match self.peek() {
                Some(Tok::Comma) => {
                    self.next();
                }
                _ => break,
            }
        }
        self.expect(&Tok::RBrace)?;
        Ok(out)
    }

    /// Parse a single [`PropVal`] operand (CONCEPT:EG-KG.query.param-list-drives-unwind): a `$param`, a bare
    /// identifier that is a bound-variable reference (except `true`/`false`, which are
    /// boolean literals), or any other literal.
    fn parse_prop_val(&mut self) -> Result<PropVal, String> {
        match self.peek() {
            Some(Tok::Param(_)) => {
                let Some(Tok::Param(name)) = self.next() else {
                    unreachable!()
                };
                Ok(PropVal::Param(name))
            }
            Some(Tok::Ident(s))
                if !s.eq_ignore_ascii_case("true") && !s.eq_ignore_ascii_case("false") =>
            {
                Ok(PropVal::Ref(self.ident()?))
            }
            _ => Ok(PropVal::Lit(self.parse_literal()?)),
        }
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
        // optional edge variable `[r:REL]` (used by DELETE r on the write path).
        let mut var = None;
        if let Some(Tok::Ident(_)) = self.peek() {
            var = Some(self.ident()?);
        }
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
        // optional inline edge property map (write path).
        let props = if matches!(self.peek(), Some(Tok::LBrace)) {
            Some(self.parse_prop_map()?)
        } else {
            None
        };
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
            var,
            props,
            group: None,
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

    // ── WHERE boolean expressions (CONCEPT:EG-KG.query.eg-extend-read-side) ────────────────────────────

    fn parse_where_expr(&mut self) -> Result<WhereExpr, String> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<WhereExpr, String> {
        let first = self.parse_and()?;
        if !self.peek_keyword("OR") {
            return Ok(first);
        }
        let mut alts = vec![first];
        while self.peek_keyword("OR") {
            self.eat_keyword("OR")?;
            alts.push(self.parse_and()?);
        }
        Ok(WhereExpr::Or(alts))
    }

    fn parse_and(&mut self) -> Result<WhereExpr, String> {
        let first = self.parse_where_primary()?;
        if !self.peek_keyword("AND") {
            return Ok(first);
        }
        let mut parts = vec![first];
        while self.peek_keyword("AND") {
            self.eat_keyword("AND")?;
            parts.push(self.parse_where_primary()?);
        }
        Ok(WhereExpr::And(parts))
    }

    fn parse_where_primary(&mut self) -> Result<WhereExpr, String> {
        if matches!(self.peek(), Some(Tok::LParen)) {
            self.next();
            let e = self.parse_or()?;
            self.expect(&Tok::RParen)?;
            return Ok(e);
        }
        Ok(WhereExpr::Cond(self.parse_condition()?))
    }

    fn parse_condition(&mut self) -> Result<Condition, String> {
        let var = self.ident()?;
        self.expect(&Tok::Dot)?;
        let prop = self.ident()?;
        let test = self.parse_test()?;
        Ok(Condition { var, prop, test })
    }

    fn parse_test(&mut self) -> Result<Test, String> {
        if self.peek_keyword("IN") {
            self.eat_keyword("IN")?;
            self.expect(&Tok::LBracket)?;
            let mut list = Vec::new();
            if !matches!(self.peek(), Some(Tok::RBracket)) {
                list.push(self.parse_literal()?);
                while matches!(self.peek(), Some(Tok::Comma)) {
                    self.next();
                    list.push(self.parse_literal()?);
                }
            }
            self.expect(&Tok::RBracket)?;
            Ok(Test::In(list))
        } else if self.peek_keyword("STARTS") {
            self.eat_keyword("STARTS")?;
            self.eat_keyword("WITH")?;
            Ok(Test::StartsWith(self.parse_string_operand("STARTS WITH")?))
        } else if self.peek_keyword("ENDS") {
            self.eat_keyword("ENDS")?;
            self.eat_keyword("WITH")?;
            Ok(Test::EndsWith(self.parse_string_operand("ENDS WITH")?))
        } else if self.peek_keyword("CONTAINS") {
            self.eat_keyword("CONTAINS")?;
            Ok(Test::Contains(self.parse_string_operand("CONTAINS")?))
        } else if self.peek_keyword("IS") {
            self.eat_keyword("IS")?;
            if self.peek_keyword("NOT") {
                self.eat_keyword("NOT")?;
                self.eat_keyword("NULL")?;
                Ok(Test::IsNotNull)
            } else {
                self.eat_keyword("NULL")?;
                Ok(Test::IsNull)
            }
        } else {
            let op = self.parse_compare_op()?;
            Ok(Test::Cmp(op, self.parse_literal()?))
        }
    }

    fn parse_compare_op(&mut self) -> Result<CompareOp, String> {
        match self.next() {
            Some(Tok::Eq) => Ok(CompareOp::Eq),
            Some(Tok::Ne) => Ok(CompareOp::Ne),
            Some(Tok::Lt) => Ok(CompareOp::Lt),
            Some(Tok::Le) => Ok(CompareOp::Le),
            Some(Tok::Gt) => Ok(CompareOp::Gt),
            Some(Tok::Ge) => Ok(CompareOp::Ge),
            other => Err(format!("expected comparison operator, found {other:?}")),
        }
    }

    fn parse_string_operand(&mut self, kw: &str) -> Result<String, String> {
        match self.parse_literal()? {
            Value::String(s) => Ok(s),
            other => Err(format!("{kw} expects a string literal, found {other:?}")),
        }
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

    // ── RETURN projection (CONCEPT:EG-KG.query.eg-extend-read-side) ────────────────────────────────────

    fn parse_return_spec(&mut self) -> Result<ReturnSpec, String> {
        let distinct = if self.peek_keyword("DISTINCT") {
            self.eat_keyword("DISTINCT")?;
            true
        } else {
            false
        };
        let mut star = false;
        let mut items = Vec::new();
        if matches!(self.peek(), Some(Tok::Star)) {
            self.next();
            star = true;
        } else {
            items.push(self.parse_return_item()?);
            while matches!(self.peek(), Some(Tok::Comma)) {
                self.next();
                items.push(self.parse_return_item()?);
            }
        }
        let order_by = self.parse_optional_order_by()?;
        let skip = self.parse_optional_int_kw("SKIP")?;
        let limit = self.parse_optional_int_kw("LIMIT")?;
        Ok(ReturnSpec {
            items,
            star,
            distinct,
            order_by,
            skip,
            limit,
        })
    }

    fn parse_return_item(&mut self) -> Result<ReturnItem, String> {
        let expr = self.parse_proj_expr()?;
        let alias = if self.peek_keyword("AS") {
            self.eat_keyword("AS")?;
            Some(self.ident()?)
        } else {
            None
        };
        Ok(ReturnItem { expr, alias })
    }

    /// A projection expression: an aggregate (`count(*)`, `sum(a.p)`, …) or a bare
    /// `var` / `var.prop` (CONCEPT:EG-KG.query.eg-extend-read-side).
    fn parse_proj_expr(&mut self) -> Result<Expr, String> {
        // Aggregate: an agg-func ident immediately followed by `(`.
        if let Some(Tok::Ident(name)) = self.peek() {
            if matches!(self.peek2(), Some(Tok::LParen)) {
                if let Some(func) = agg_func(name) {
                    self.next(); // func name
                    self.expect(&Tok::LParen)?;
                    // `count(*)`
                    if func == AggFunc::Count && matches!(self.peek(), Some(Tok::Star)) {
                        self.next();
                        self.expect(&Tok::RParen)?;
                        return Ok(Expr::CountStar);
                    }
                    let arg = self.parse_agg_arg()?;
                    self.expect(&Tok::RParen)?;
                    return Ok(Expr::Aggregate(func, arg));
                }
            }
        }
        // Bare var / var.prop.
        let var = self.ident()?;
        if matches!(self.peek(), Some(Tok::Dot)) {
            self.next();
            Ok(Expr::Prop(var, self.ident()?))
        } else {
            Ok(Expr::Var(var))
        }
    }

    fn parse_agg_arg(&mut self) -> Result<AggArg, String> {
        let var = self.ident()?;
        if matches!(self.peek(), Some(Tok::Dot)) {
            self.next();
            Ok(AggArg::Prop(var, self.ident()?))
        } else {
            Ok(AggArg::Var(var))
        }
    }

    fn parse_optional_order_by(&mut self) -> Result<Vec<OrderKey>, String> {
        if !(self.peek_keyword("ORDER") && self.peek2_keyword("BY")) {
            return Ok(Vec::new());
        }
        self.eat_keyword("ORDER")?;
        self.eat_keyword("BY")?;
        let mut keys = vec![self.parse_order_key()?];
        while matches!(self.peek(), Some(Tok::Comma)) {
            self.next();
            keys.push(self.parse_order_key()?);
        }
        Ok(keys)
    }

    fn parse_order_key(&mut self) -> Result<OrderKey, String> {
        let expr = self.parse_proj_expr()?;
        let desc = if self.peek_keyword("DESC") {
            self.eat_keyword("DESC")?;
            true
        } else {
            if self.peek_keyword("ASC") {
                self.eat_keyword("ASC")?;
            }
            false
        };
        Ok(OrderKey { expr, desc })
    }

    fn parse_optional_int_kw(&mut self, kw: &str) -> Result<Option<usize>, String> {
        if self.peek_keyword(kw) {
            self.eat_keyword(kw)?;
            match self.next() {
                Some(Tok::Num(n)) if n >= 0.0 && n.fract() == 0.0 => Ok(Some(n as usize)),
                other => Err(format!("expected integer after {kw}, found {other:?}")),
            }
        } else {
            Ok(None)
        }
    }

    // ── read stages (CONCEPT:EG-KG.query.eg-extend-read-side) ──────────────────────────────────────────

    /// Whether the next token begins a write clause.
    fn at_write_clause(&self) -> bool {
        self.peek_keyword("CREATE")
            || self.peek_keyword("MERGE")
            || self.peek_keyword("SET")
            || self.peek_keyword("DELETE")
            || self.peek_keyword("DETACH")
            || self.peek_keyword("REMOVE")
    }

    /// Parse a whole statement: a read (one-or-more reading stages + `RETURN`) or a
    /// write (`[MATCH …] CREATE/MERGE/SET/DELETE/REMOVE … [RETURN …]`).
    fn parse_statement(&mut self) -> Result<Statement, String> {
        // A statement that opens with a write clause has no MATCH.
        if self.at_write_clause() {
            let ops = self.parse_write_clauses()?;
            let returns = self.parse_optional_simple_return()?;
            self.finish()?;
            return Ok(Statement::Write(WriteQuery {
                match_pattern: None,
                where_clause: None,
                ops,
                returns,
            }));
        }

        // A statement opening with (OPTIONAL) MATCH may be a write-over-a-binding.
        let first = if self.peek_keyword("MATCH") || self.peek_keyword("OPTIONAL") {
            let optional = if self.peek_keyword("OPTIONAL") {
                self.eat_keyword("OPTIONAL")?;
                true
            } else {
                false
            };
            self.eat_keyword("MATCH")?;
            let path_var = self.parse_optional_path_var()?;
            let pattern = self.parse_pattern()?;
            let where_clause = self.parse_optional_where()?;

            // `MATCH … <write clause>+ [RETURN …]` ⇒ a write over the matched binding.
            if self.at_write_clause() {
                if optional {
                    return Err("OPTIONAL MATCH cannot precede a write clause".into());
                }
                if path_var.is_some() {
                    return Err("a path variable cannot bind on a write statement".into());
                }
                let ops = self.parse_write_clauses()?;
                let returns = self.parse_optional_simple_return()?;
                self.finish()?;
                return Ok(Statement::Write(WriteQuery {
                    match_pattern: Some(pattern),
                    where_clause,
                    ops,
                    returns,
                }));
            }
            ReadStage::Match {
                pattern,
                optional,
                where_clause,
                path_var,
            }
        } else {
            // A read opening with UNWIND / CALL / WITH (CONCEPT:EG-KG.query.param-list-drives-unwind/142).
            self.parse_read_stage()?
        };

        // Collect any further reading stages, then the terminal RETURN.
        let mut stages = vec![first];
        loop {
            if self.peek_keyword("RETURN") {
                break;
            }
            stages.push(self.parse_read_stage()?);
        }
        self.eat_keyword("RETURN")?;
        let ret = self.parse_return_spec()?;
        self.finish()?;
        Ok(Statement::Read(CypherQuery { stages, ret }))
    }

    /// Parse one reading stage: `(OPTIONAL) MATCH …`, `WITH …`, `UNWIND …` or
    /// `CALL …` (CONCEPT:EG-KG.query.eg-extend-read-side/EG-141/EG-142).
    fn parse_read_stage(&mut self) -> Result<ReadStage, String> {
        if self.peek_keyword("UNWIND") {
            return self.parse_unwind();
        }
        if self.peek_keyword("CALL") {
            return self.parse_call();
        }
        if self.peek_keyword("WITH") {
            self.eat_keyword("WITH")?;
            let mut items = vec![self.parse_with_item()?];
            while matches!(self.peek(), Some(Tok::Comma)) {
                self.next();
                items.push(self.parse_with_item()?);
            }
            let where_clause = self.parse_optional_where()?;
            return Ok(ReadStage::With {
                items,
                where_clause,
            });
        }
        let optional = if self.peek_keyword("OPTIONAL") {
            self.eat_keyword("OPTIONAL")?;
            true
        } else {
            false
        };
        self.eat_keyword("MATCH")?;
        let path_var = self.parse_optional_path_var()?;
        let pattern = self.parse_pattern()?;
        let where_clause = self.parse_optional_where()?;
        Ok(ReadStage::Match {
            pattern,
            optional,
            where_clause,
            path_var,
        })
    }

    fn parse_with_item(&mut self) -> Result<WithItem, String> {
        let var = self.ident()?;
        let alias = if self.peek_keyword("AS") {
            self.eat_keyword("AS")?;
            Some(self.ident()?)
        } else {
            None
        };
        Ok(WithItem { var, alias })
    }

    // ── UNWIND / CALL (CONCEPT:EG-KG.query.param-list-drives-unwind / EG-142) ───────────────────────────────

    /// `UNWIND <list> AS <var>` (CONCEPT:EG-KG.query.param-list-drives-unwind).
    fn parse_unwind(&mut self) -> Result<ReadStage, String> {
        self.eat_keyword("UNWIND")?;
        let list = self.parse_list_expr()?;
        self.eat_keyword("AS")?;
        let var = self.ident()?;
        Ok(ReadStage::Unwind { list, var })
    }

    /// The UNWIND operand: `[e, …]`, `$param`, or a bound-var reference (CONCEPT:EG-KG.query.param-list-drives-unwind).
    fn parse_list_expr(&mut self) -> Result<ListExpr, String> {
        match self.peek() {
            Some(Tok::LBracket) => {
                self.next();
                let mut items = Vec::new();
                if !matches!(self.peek(), Some(Tok::RBracket)) {
                    items.push(self.parse_prop_val()?);
                    while matches!(self.peek(), Some(Tok::Comma)) {
                        self.next();
                        items.push(self.parse_prop_val()?);
                    }
                }
                self.expect(&Tok::RBracket)?;
                Ok(ListExpr::List(items))
            }
            Some(Tok::Param(_)) => {
                let Some(Tok::Param(name)) = self.next() else {
                    unreachable!()
                };
                Ok(ListExpr::Param(name))
            }
            Some(Tok::Ident(_)) => Ok(ListExpr::Ref(self.ident()?)),
            other => Err(format!(
                "expected a list expression for UNWIND, found {other:?}"
            )),
        }
    }

    /// `CALL { subquery }` or `CALL proc.name(args) YIELD …` (CONCEPT:EG-KG.query.cypher-planning).
    fn parse_call(&mut self) -> Result<ReadStage, String> {
        self.eat_keyword("CALL")?;
        if matches!(self.peek(), Some(Tok::LBrace)) {
            let subquery = self.parse_subquery()?;
            return Ok(ReadStage::Call {
                subquery: Box::new(subquery),
            });
        }
        let name = self.parse_proc_name()?;
        let args = self.parse_arg_list()?;
        self.eat_keyword("YIELD")?;
        let yields = self.parse_yield_items()?;
        Ok(ReadStage::CallProc { name, args, yields })
    }

    /// A dotted procedure name (`gds.pageRank`, `apoc.coll.sum`) (CONCEPT:EG-KG.query.cypher-planning).
    fn parse_proc_name(&mut self) -> Result<String, String> {
        let mut name = self.ident()?;
        while matches!(self.peek(), Some(Tok::Dot)) {
            self.next();
            name.push('.');
            name.push_str(&self.ident()?);
        }
        Ok(name)
    }

    /// `( arg, … )` — the parenthesized argument list of a procedure call. Each arg is
    /// a literal, a `$param`, a bound-var reference, or an inline `[…]` list literal.
    fn parse_arg_list(&mut self) -> Result<Vec<PropVal>, String> {
        self.expect(&Tok::LParen)?;
        let mut args = Vec::new();
        if !matches!(self.peek(), Some(Tok::RParen)) {
            args.push(self.parse_arg()?);
            while matches!(self.peek(), Some(Tok::Comma)) {
                self.next();
                args.push(self.parse_arg()?);
            }
        }
        self.expect(&Tok::RParen)?;
        Ok(args)
    }

    /// One procedure argument (CONCEPT:EG-KG.query.cypher-planning): an inline `[…]` list literal (of
    /// literals) folds to a `PropVal::Lit(Value::Array)`; a `{…}` config-map literal
    /// (CONCEPT:EG-KG.query.gds-call-procedures, e.g. `gds.pageRank({dampingFactor: 0.85})`) folds to a
    /// `PropVal::Lit(Value::Object)`; otherwise a [`PropVal`].
    fn parse_arg(&mut self) -> Result<PropVal, String> {
        if matches!(self.peek(), Some(Tok::LBracket)) {
            self.next();
            let mut items: Vec<Value> = Vec::new();
            if !matches!(self.peek(), Some(Tok::RBracket)) {
                items.push(self.parse_literal()?);
                while matches!(self.peek(), Some(Tok::Comma)) {
                    self.next();
                    items.push(self.parse_literal()?);
                }
            }
            self.expect(&Tok::RBracket)?;
            return Ok(PropVal::Lit(Value::Array(items)));
        }
        if matches!(self.peek(), Some(Tok::LBrace)) {
            return Ok(PropVal::Lit(self.parse_map_literal()?));
        }
        self.parse_prop_val()
    }

    /// `{ key: literal, … }` — a config-map literal argument for a `CALL gds.*`
    /// procedure (CONCEPT:EG-KG.query.gds-call-procedures). Values are literals, nested `[…]` list literals,
    /// or nested `{…}` maps. Folds to a `serde_json::Value::Object` so the procedure
    /// receives a plain JSON config it can key `dampingFactor`/`maxIterations`/
    /// `relationshipWeightProperty`/`topK`/… out of.
    fn parse_map_literal(&mut self) -> Result<Value, String> {
        self.expect(&Tok::LBrace)?;
        let mut obj = serde_json::Map::new();
        if matches!(self.peek(), Some(Tok::RBrace)) {
            self.next();
            return Ok(Value::Object(obj));
        }
        loop {
            let key = self.ident()?;
            self.expect(&Tok::Colon)?;
            let val = self.parse_map_value()?;
            obj.insert(key, val);
            match self.peek() {
                Some(Tok::Comma) => {
                    self.next();
                }
                _ => break,
            }
        }
        self.expect(&Tok::RBrace)?;
        Ok(Value::Object(obj))
    }

    /// One value inside a config-map literal (CONCEPT:EG-KG.query.gds-call-procedures): a nested `[…]` list, a
    /// nested `{…}` map, or a scalar literal.
    fn parse_map_value(&mut self) -> Result<Value, String> {
        if matches!(self.peek(), Some(Tok::LBracket)) {
            self.next();
            let mut items: Vec<Value> = Vec::new();
            if !matches!(self.peek(), Some(Tok::RBracket)) {
                items.push(self.parse_literal()?);
                while matches!(self.peek(), Some(Tok::Comma)) {
                    self.next();
                    items.push(self.parse_literal()?);
                }
            }
            self.expect(&Tok::RBracket)?;
            return Ok(Value::Array(items));
        }
        if matches!(self.peek(), Some(Tok::LBrace)) {
            return self.parse_map_literal();
        }
        self.parse_literal()
    }

    /// `YIELD col [AS alias], …` (CONCEPT:EG-KG.query.cypher-planning).
    fn parse_yield_items(&mut self) -> Result<Vec<YieldItem>, String> {
        let mut items = vec![self.parse_yield_item()?];
        while matches!(self.peek(), Some(Tok::Comma)) {
            self.next();
            items.push(self.parse_yield_item()?);
        }
        Ok(items)
    }

    fn parse_yield_item(&mut self) -> Result<YieldItem, String> {
        let col = self.ident()?;
        let alias = if self.peek_keyword("AS") {
            self.eat_keyword("AS")?;
            Some(self.ident()?)
        } else {
            None
        };
        Ok(YieldItem { col, alias })
    }

    /// `{ <read stages> RETURN <spec> }` — the body of a `CALL { … }` subquery
    /// (CONCEPT:EG-KG.query.cypher-planning). Reads only; terminated by the matching `}`.
    fn parse_subquery(&mut self) -> Result<CypherQuery, String> {
        self.expect(&Tok::LBrace)?;
        let mut stages = vec![self.parse_read_stage()?];
        loop {
            if self.peek_keyword("RETURN") {
                break;
            }
            stages.push(self.parse_read_stage()?);
        }
        self.eat_keyword("RETURN")?;
        let ret = self.parse_return_spec()?;
        self.expect(&Tok::RBrace)?;
        Ok(CypherQuery { stages, ret })
    }

    /// A leading `p =` path-variable binding (CONCEPT:EG-KG.query.concept-2): an identifier directly
    /// followed by `=` (a pattern always opens with `(`, so there is no ambiguity).
    fn parse_optional_path_var(&mut self) -> Result<Option<String>, String> {
        if matches!(self.peek(), Some(Tok::Ident(_))) && matches!(self.peek2(), Some(Tok::Eq)) {
            let v = self.ident()?;
            self.expect(&Tok::Eq)?;
            Ok(Some(v))
        } else {
            Ok(None)
        }
    }

    fn parse_optional_where(&mut self) -> Result<Option<WhereExpr>, String> {
        if self.peek_keyword("WHERE") {
            self.eat_keyword("WHERE")?;
            Ok(Some(self.parse_where_expr()?))
        } else {
            Ok(None)
        }
    }

    // ── write statements (CONCEPT:EG-KG.query.register-each-user-table / EG-061) ────────────────────────────

    /// Parse one-or-more consecutive write clauses (CONCEPT:EG-KG.query.register-each-user-table).
    fn parse_write_clauses(&mut self) -> Result<Vec<WriteOp>, String> {
        let mut ops = Vec::new();
        while self.at_write_clause() {
            ops.push(self.parse_write_clause()?);
        }
        if ops.is_empty() {
            return Err("expected a write clause (CREATE/MERGE/SET/DELETE/REMOVE)".into());
        }
        Ok(ops)
    }

    fn parse_write_clause(&mut self) -> Result<WriteOp, String> {
        if self.peek_keyword("CREATE") {
            self.eat_keyword("CREATE")?;
            let pattern = self.parse_pattern()?;
            Ok(WriteOp::Create(pattern))
        } else if self.peek_keyword("MERGE") {
            self.eat_keyword("MERGE")?;
            let node = self.parse_node()?;
            if !self.peek_is_end_of_clause() {
                return Err(
                    "MERGE supports a single `(n:Label {props})` node in this subset".into(),
                );
            }
            Ok(WriteOp::Merge(node))
        } else if self.peek_keyword("SET") {
            self.eat_keyword("SET")?;
            let items = self.parse_set_items()?;
            Ok(WriteOp::Set(items))
        } else if self.peek_keyword("REMOVE") {
            self.eat_keyword("REMOVE")?;
            let items = self.parse_remove_items()?;
            Ok(WriteOp::Remove(items))
        } else {
            // DELETE or DETACH DELETE.
            let detach = if self.peek_keyword("DETACH") {
                self.eat_keyword("DETACH")?;
                true
            } else {
                false
            };
            self.eat_keyword("DELETE")?;
            let vars = self.parse_var_list()?;
            Ok(WriteOp::Delete { vars, detach })
        }
    }

    /// A write-clause sub-parse stops at end-of-input or the next clause keyword.
    fn peek_is_end_of_clause(&self) -> bool {
        self.peek().is_none() || self.at_write_clause() || self.peek_keyword("RETURN")
    }

    fn parse_set_items(&mut self) -> Result<Vec<SetItem>, String> {
        let mut items = vec![self.parse_set_item()?];
        while matches!(self.peek(), Some(Tok::Comma)) {
            self.next();
            items.push(self.parse_set_item()?);
        }
        Ok(items)
    }

    fn parse_set_item(&mut self) -> Result<SetItem, String> {
        let var = self.ident()?;
        self.expect(&Tok::Dot)?;
        let prop = self.ident()?;
        self.expect(&Tok::Eq)?;
        let value = self.parse_literal()?;
        Ok(SetItem { var, prop, value })
    }

    /// `REMOVE v.prop | v:Label [, …]` (CONCEPT:EG-KG.query.cypher-execution).
    fn parse_remove_items(&mut self) -> Result<Vec<RemoveItem>, String> {
        let mut items = vec![self.parse_remove_item()?];
        while matches!(self.peek(), Some(Tok::Comma)) {
            self.next();
            items.push(self.parse_remove_item()?);
        }
        Ok(items)
    }

    fn parse_remove_item(&mut self) -> Result<RemoveItem, String> {
        let var = self.ident()?;
        match self.next() {
            // `v.prop` → property delete.
            Some(Tok::Dot) => {
                let prop = self.ident()?;
                Ok(RemoveItem::Property { var, prop })
            }
            // `v:Label` → label removal.
            Some(Tok::Colon) => {
                let label = self.ident()?;
                Ok(RemoveItem::Label { var, label })
            }
            other => Err(format!(
                "REMOVE expects `v.prop` or `v:Label`, found {other:?}"
            )),
        }
    }

    fn parse_var_list(&mut self) -> Result<Vec<String>, String> {
        let mut vars = vec![self.ident()?];
        while matches!(self.peek(), Some(Tok::Comma)) {
            self.next();
            vars.push(self.ident()?);
        }
        Ok(vars)
    }

    /// A trailing `RETURN <items>` on a WRITE statement — simple projection only (no
    /// aggregation/ORDER BY/SKIP/LIMIT/DISTINCT).
    fn parse_optional_simple_return(&mut self) -> Result<Vec<ReturnItem>, String> {
        if !self.peek_keyword("RETURN") {
            return Ok(Vec::new());
        }
        self.eat_keyword("RETURN")?;
        let mut items = vec![self.parse_return_item()?];
        while matches!(self.peek(), Some(Tok::Comma)) {
            self.next();
            items.push(self.parse_return_item()?);
        }
        Ok(items)
    }

    fn finish(&mut self) -> Result<(), String> {
        if self.pos != self.toks.len() {
            return Err(format!(
                "trailing tokens after statement: {:?}",
                &self.toks[self.pos..]
            ));
        }
        Ok(())
    }
}

/// The agg-func keyword → [`AggFunc`], or `None` if `name` is not an aggregate.
fn agg_func(name: &str) -> Option<AggFunc> {
    if name.eq_ignore_ascii_case("count") {
        Some(AggFunc::Count)
    } else if name.eq_ignore_ascii_case("collect") {
        Some(AggFunc::Collect)
    } else if name.eq_ignore_ascii_case("sum") {
        Some(AggFunc::Sum)
    } else if name.eq_ignore_ascii_case("avg") {
        Some(AggFunc::Avg)
    } else if name.eq_ignore_ascii_case("min") {
        Some(AggFunc::Min)
    } else if name.eq_ignore_ascii_case("max") {
        Some(AggFunc::Max)
    } else {
        None
    }
}

/// Parse a Cypher-subset query string into the [`CypherQuery`] AST (READ path).
/// Errors on a write statement so the read executor never silently mishandles one.
pub fn parse(input: &str) -> Result<CypherQuery, String> {
    match parse_statement(input)? {
        Statement::Read(q) => Ok(q),
        Statement::Write(_) => {
            Err("this is a write statement; use the Cypher write path (exec_cypher_write)".into())
        }
    }
}

/// Parse a Cypher-subset statement — a read or a write (CONCEPT:EG-KG.query.register-each-user-table).
pub fn parse_statement(input: &str) -> Result<Statement, String> {
    let toks = tokenize(input)?;
    if toks.is_empty() {
        return Err("empty query".into());
    }
    let mut p = Parser { toks, pos: 0 };
    p.parse_statement()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The first MATCH stage's pattern/where, for terse assertions.
    fn first_match(q: &CypherQuery) -> (&Pattern, &Option<WhereExpr>) {
        match &q.stages[0] {
            ReadStage::Match {
                pattern,
                where_clause,
                ..
            } => (pattern, where_clause),
            _ => panic!("first stage is not a MATCH"),
        }
    }

    #[test]
    fn parses_single_node_match() {
        let q = parse("MATCH (a:Person) RETURN a").unwrap();
        let (pat, _) = first_match(&q);
        assert_eq!(pat.start.var.as_deref(), Some("a"));
        assert_eq!(pat.start.label.as_deref(), Some("Person"));
        assert!(pat.hops.is_empty());
        assert_eq!(q.ret.items.len(), 1);
        assert_eq!(q.ret.items[0].column(), "a");
    }

    #[test]
    fn parses_two_hop_with_where_and_limit() {
        let q = parse(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.name = 'Alice' RETURN a, b LIMIT 5",
        )
        .unwrap();
        let (pat, where_c) = first_match(&q);
        assert_eq!(pat.hops.len(), 1);
        assert_eq!(pat.hops[0].0.rel_type.as_deref(), Some("KNOWS"));
        assert_eq!(pat.hops[0].0.direction, Direction::Right);
        assert!(where_c.is_some());
        assert_eq!(q.ret.limit, Some(5));
        assert_eq!(q.ret.items.len(), 2);
    }

    #[test]
    fn parses_variable_length_path() {
        let q = parse("MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) RETURN b").unwrap();
        let (pat, _) = first_match(&q);
        assert_eq!(pat.hops[0].0.var_len, Some((1, 3)));
    }

    #[test]
    fn parses_property_return_and_comparison() {
        let q = parse("MATCH (a:Doc) WHERE a.size > 10 RETURN a.size").unwrap();
        assert_eq!(q.ret.items[0].column(), "a.size");
        let (_, where_c) = first_match(&q);
        match where_c.as_ref().unwrap() {
            WhereExpr::Cond(c) => assert!(matches!(c.test, Test::Cmp(CompareOp::Gt, _))),
            _ => panic!("expected single comparison"),
        }
    }

    #[test]
    fn parses_left_direction() {
        let q = parse("MATCH (a:Person)<-[:KNOWS]-(b:Person) RETURN a").unwrap();
        let (pat, _) = first_match(&q);
        assert_eq!(pat.hops[0].0.direction, Direction::Left);
    }

    #[test]
    fn parses_where_or_in_starts_with() {
        let q =
            parse("MATCH (a:Person) WHERE a.name STARTS WITH 'A' OR a.id IN [1, 2, 3] RETURN a")
                .unwrap();
        let (_, where_c) = first_match(&q);
        match where_c.as_ref().unwrap() {
            WhereExpr::Or(alts) => assert_eq!(alts.len(), 2),
            _ => panic!("expected OR"),
        }
    }

    #[test]
    fn parses_aggregation_and_distinct() {
        let q = parse("MATCH (a:Person) RETURN DISTINCT count(*), collect(a.name)").unwrap();
        assert!(q.ret.distinct);
        assert!(matches!(q.ret.items[0].expr, Expr::CountStar));
        assert!(matches!(
            q.ret.items[1].expr,
            Expr::Aggregate(AggFunc::Collect, _)
        ));
    }

    #[test]
    fn parses_order_by_skip_limit() {
        let q =
            parse("MATCH (a:Person) RETURN a.name ORDER BY a.name DESC SKIP 1 LIMIT 2").unwrap();
        assert_eq!(q.ret.order_by.len(), 1);
        assert!(q.ret.order_by[0].desc);
        assert_eq!(q.ret.skip, Some(1));
        assert_eq!(q.ret.limit, Some(2));
    }

    #[test]
    fn parses_with_pipeline_and_star() {
        let q = parse("MATCH (a:Person) WITH a WHERE a.name = 'Alice' RETURN *").unwrap();
        assert_eq!(q.stages.len(), 2);
        assert!(matches!(q.stages[1], ReadStage::With { .. }));
        assert!(q.ret.star);
    }

    #[test]
    fn parses_optional_match() {
        let q = parse("MATCH (a:Person) OPTIONAL MATCH (a)-[:KNOWS]->(b) RETURN a, b").unwrap();
        assert_eq!(q.stages.len(), 2);
        match &q.stages[1] {
            ReadStage::Match { optional, .. } => assert!(*optional),
            _ => panic!("expected OPTIONAL MATCH"),
        }
    }

    #[test]
    fn parses_path_variable() {
        let q = parse("MATCH p = (a)-[:KNOWS*1..3]->(b) RETURN p").unwrap();
        match &q.stages[0] {
            ReadStage::Match { path_var, .. } => assert_eq!(path_var.as_deref(), Some("p")),
            _ => panic!("expected MATCH"),
        }
    }

    #[test]
    fn parses_unwind_list_literal() {
        // CONCEPT:EG-KG.query.param-list-drives-unwind
        let q = parse("UNWIND [1, 2, 3] AS x RETURN x").unwrap();
        match &q.stages[0] {
            ReadStage::Unwind { list, var } => {
                assert_eq!(var, "x");
                match list {
                    ListExpr::List(items) => assert_eq!(items.len(), 3),
                    _ => panic!("expected list literal"),
                }
            }
            _ => panic!("expected UNWIND"),
        }
    }

    #[test]
    fn parses_unwind_param_then_match_with_inline_prop_ref() {
        // CONCEPT:EG-KG.query.param-list-drives-unwind — $param list + read-side inline prop referencing a var.
        let q = parse("UNWIND $ids AS x MATCH (n {id: x}) RETURN n").unwrap();
        assert!(
            matches!(&q.stages[0], ReadStage::Unwind { list: ListExpr::Param(p), .. } if p == "ids")
        );
        match &q.stages[1] {
            ReadStage::Match { pattern, .. } => {
                let props = pattern.start.props.as_ref().unwrap();
                assert_eq!(props[0].0, "id");
                assert!(matches!(&props[0].1, PropVal::Ref(r) if r == "x"));
            }
            _ => panic!("expected MATCH"),
        }
    }

    #[test]
    fn parses_call_subquery() {
        // CONCEPT:EG-KG.query.cypher-planning
        let q = parse("CALL { MATCH (a:Person) RETURN a } RETURN a").unwrap();
        assert!(matches!(&q.stages[0], ReadStage::Call { .. }));
    }

    #[test]
    fn parses_call_proc_yield() {
        // CONCEPT:EG-KG.query.cypher-planning/EG-143
        let q = parse("CALL gds.pageRank() YIELD node, score RETURN node, score").unwrap();
        match &q.stages[0] {
            ReadStage::CallProc { name, args, yields } => {
                assert_eq!(name, "gds.pageRank");
                assert!(args.is_empty());
                assert_eq!(yields.len(), 2);
                assert_eq!(yields[0].col, "node");
            }
            _ => panic!("expected CALL proc"),
        }
    }

    #[test]
    fn parses_call_proc_with_list_arg_and_alias() {
        // CONCEPT:EG-KG.query.cypher-planning/EG-143
        let q = parse("CALL apoc.coll.sum([1, 2, 3]) YIELD value AS s RETURN s").unwrap();
        match &q.stages[0] {
            ReadStage::CallProc { name, args, yields } => {
                assert_eq!(name, "apoc.coll.sum");
                assert!(matches!(&args[0], PropVal::Lit(Value::Array(a)) if a.len() == 3));
                assert_eq!(yields[0].alias.as_deref(), Some("s"));
            }
            _ => panic!("expected CALL proc"),
        }
    }

    #[test]
    fn parses_remove_property_and_label() {
        let st = parse_statement("MATCH (n:Person) REMOVE n.age, n:Admin").unwrap();
        match st {
            Statement::Write(w) => {
                assert_eq!(w.ops.len(), 1);
                match &w.ops[0] {
                    WriteOp::Remove(items) => {
                        assert_eq!(items.len(), 2);
                        assert!(matches!(items[0], RemoveItem::Property { .. }));
                        assert!(matches!(items[1], RemoveItem::Label { .. }));
                    }
                    _ => panic!("expected REMOVE"),
                }
            }
            _ => panic!("REMOVE must classify as a write"),
        }
    }
}
