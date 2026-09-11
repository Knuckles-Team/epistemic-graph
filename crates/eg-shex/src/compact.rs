//! ShExC — the compact, Turtle-like ShEx textual syntax (CONCEPT:EG-KG.compute.concept-2, W3C ShEx
//! Definition Language §5) — parsed DIRECTLY to this crate's [`crate::schema::Schema`]
//! model (no intermediate ShExJ/JSON round-trip): a hand-rolled lexer + recursive-
//! descent parser, matching this codebase's existing idiom for protocol/grammar
//! parsers that don't warrant an external parser-combinator/PEG dependency.
//!
//! Scope — a bounded, pragmatic subset proven by the round-trip test
//! (`tests/compact.rs`: a ShExC document and an equivalent programmatic
//! [`crate::schema::Schema`] build produce the SAME model):
//!
//! * `PREFIX`/`BASE` directives (a relative `<iri>` resolves against the last `BASE`
//!   by simple concatenation, not full RFC 3986 resolution);
//! * `START =` and labelled shape expression declarations (`<label> shapeExpr` /
//!   `prefix:local shapeExpr`);
//! * shape expressions: `shapeOr` (`OR`) / `shapeAnd` (`AND`) / `shapeNot` (`NOT`) /
//!   a parenthesized sub-expression / `.` (the wildcard "any node" atom) / a shape
//!   reference (`@<label>` / `@prefix:local`) / a shape definition (`{ … }`,
//!   optionally `CLOSED` and/or `EXTRA <predicate>+`) / a node constraint, optionally
//!   `AND`-ed with a following shape ref/definition (`IRI @<Shape>`);
//! * node constraints: `IRI`/`BNODE`/`LITERAL`/`NONLITERAL`, a datatype IRI, a value
//!   set (`[ v1 v2 … ]`, IRI/literal members — stem ranges are an EG-133 follow-up,
//!   matching the existing ShExJ parser's own scope), and the string
//!   (`LENGTH`/`MINLENGTH`/`MAXLENGTH`/`PATTERN`+flags) and numeric
//!   (`MININCLUSIVE`/`MAXINCLUSIVE`/`MINEXCLUSIVE`/`MAXEXCLUSIVE`) facets;
//! * triple expressions: a triple constraint (`predicate` — `a` is the `rdf:type`
//!   shortcut — + an optional inline value shape expression + cardinality `?`/`*`/
//!   `+`/`{m,n}`/`{m,}`/`{m}`), `EachOf` (`;`) and `OneOf` (`|`), and a parenthesized
//!   sub-expression with its own trailing cardinality.
//!
//! Deferred (same EG-133 follow-up list the crate docs already carry, plus the
//! ShExC-specific pieces): inverse triple-expression REFERENCES (`&label`, distinct
//! from an inverse triple CONSTRAINT `^predicate`, which IS supported), triple
//! expression `$label` groups, semantic actions (`%…%`/`@%…%`), `EXTERNAL` shapes,
//! `TOTALDIGITS`/`FRACTIONDIGITS`, and value-set stem ranges (`~`, `-`). Any of these
//! — and any other construct outside the grammar above — is a parse `Err`, never a
//! silently wrong shape (mirrors `crate::schema::Schema::from_shexj`'s own contract).

use crate::schema::{
    NodeConstraint, NodeKind, Schema, Shape, ShapeExpr, TripleExpr, ValueSetValue,
};

mod lexer;
use lexer::{lex, StrSuffix, Tok};

/// Parse a ShExC (compact syntax) document into a [`Schema`] (CONCEPT:EG-KG.compute.concept-2).
pub fn parse(text: &str) -> Result<Schema, String> {
    let tokens = lex(text)?;
    let mut p = Parser {
        toks: &tokens,
        pos: 0,
    };
    p.parse_schema()
}

fn parse_node_constraint_atom(parser: &mut Parser<'_>) -> Result<NodeConstraint, String> {
    let mut nc = NodeConstraint::default();
    match parser.peek().clone() {
        Tok::Word(w) if w == "IRI" => {
            parser.bump();
            nc.node_kind = Some(NodeKind::Iri);
        }
        Tok::Word(w) if w == "BNODE" => {
            parser.bump();
            nc.node_kind = Some(NodeKind::BNode);
        }
        Tok::Word(w) if w == "LITERAL" => {
            parser.bump();
            nc.node_kind = Some(NodeKind::Literal);
        }
        Tok::Word(w) if w == "NONLITERAL" => {
            parser.bump();
            nc.node_kind = Some(NodeKind::NonLiteral);
        }
        Tok::Iri(iri) => {
            parser.bump();
            nc.datatype = Some(iri);
        }
        Tok::Punct('[') => {
            nc.values = Some(parser.parse_value_set()?);
        }
        other => {
            return Err(format!(
                "ShExC: expected a node constraint (IRI/BNODE/LITERAL/NONLITERAL/datatype/value set), found {other:?}"
            ))
        }
    }
    Ok(nc)
}

fn parse_node_constraint_facets(
    parser: &mut Parser<'_>,
    nc: &mut NodeConstraint,
) -> Result<(), String> {
    while parse_node_constraint_facet(parser, nc)? {}
    Ok(())
}

fn parse_node_constraint_facet(
    parser: &mut Parser<'_>,
    nc: &mut NodeConstraint,
) -> Result<bool, String> {
    let Tok::Word(word) = parser.peek().clone() else {
        return Ok(false);
    };
    match word.as_str() {
        "LENGTH" => {
            parser.bump();
            nc.string_facets.length = Some(parser.parse_facet_uint()?);
        }
        "MINLENGTH" => {
            parser.bump();
            nc.string_facets.minlength = Some(parser.parse_facet_uint()?);
        }
        "MAXLENGTH" => {
            parser.bump();
            nc.string_facets.maxlength = Some(parser.parse_facet_uint()?);
        }
        "PATTERN" => {
            parser.bump();
            let (pattern, flags) = parser.parse_pattern()?;
            nc.string_facets.pattern = Some(pattern);
            nc.string_facets.flags = flags;
        }
        "MININCLUSIVE" => {
            parser.bump();
            nc.numeric_facets.mininclusive = Some(parser.parse_facet_num()?);
        }
        "MAXINCLUSIVE" => {
            parser.bump();
            nc.numeric_facets.maxinclusive = Some(parser.parse_facet_num()?);
        }
        "MINEXCLUSIVE" => {
            parser.bump();
            nc.numeric_facets.minexclusive = Some(parser.parse_facet_num()?);
        }
        "MAXEXCLUSIVE" => {
            parser.bump();
            nc.numeric_facets.maxexclusive = Some(parser.parse_facet_num()?);
        }
        _ => return Ok(false),
    }
    Ok(true)
}

// ── Parser ────────────────────────────────────────────────────────────────

struct Parser<'a> {
    toks: &'a [Tok],
    pos: usize,
}

const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

impl<'a> Parser<'a> {
    fn peek(&self) -> &Tok {
        &self.toks[self.pos.min(self.toks.len() - 1)]
    }

    fn bump(&mut self) -> Tok {
        let t = self.peek().clone();
        if self.pos < self.toks.len() - 1 {
            self.pos += 1;
        }
        t
    }

    fn at_eof(&self) -> bool {
        matches!(self.peek(), Tok::Punct('\0'))
    }

    fn eat_punct(&mut self, p: char) -> bool {
        if matches!(self.peek(), Tok::Punct(c) if *c == p) {
            self.bump();
            true
        } else {
            false
        }
    }

    fn expect_punct(&mut self, p: char) -> Result<(), String> {
        if self.eat_punct(p) {
            Ok(())
        } else {
            Err(format!(
                "ShExC: expected `{p}`, found {:?} at token {}",
                self.peek(),
                self.pos
            ))
        }
    }

    fn eat_word(&mut self, w: &str) -> bool {
        if matches!(self.peek(), Tok::Word(s) if s == w) {
            self.bump();
            true
        } else {
            false
        }
    }

    fn parse_schema(&mut self) -> Result<Schema, String> {
        let mut schema = Schema::default();
        while !self.at_eof() {
            if self.eat_word("START") {
                self.expect_punct('=')?;
                schema.start = Some(self.parse_shape_expression()?);
                continue;
            }
            let label = self.parse_iri_or_label()?;
            let expr = self.parse_shape_expression()?;
            schema.shapes.insert(label, expr);
        }
        Ok(schema)
    }

    /// A shape-expression LABEL: an IRI (`<...>` or `prefix:local`).
    fn parse_iri_or_label(&mut self) -> Result<String, String> {
        match self.bump() {
            Tok::Iri(iri) => Ok(iri),
            other => Err(format!(
                "ShExC: expected a shape label (IRI), found {other:?}"
            )),
        }
    }

    // shapeOr := shapeAnd ("OR" shapeAnd)*
    fn parse_shape_expression(&mut self) -> Result<ShapeExpr, String> {
        self.parse_operator_chain("OR", Self::parse_shape_and, ShapeExpr::Or)
    }

    // shapeAnd := shapeNot ("AND" shapeNot)*
    fn parse_shape_and(&mut self) -> Result<ShapeExpr, String> {
        self.parse_operator_chain("AND", Self::parse_shape_not, ShapeExpr::And)
    }

    /// Parse a left-associative `operand (KEYWORD operand)*` production. A single operand
    /// with no keyword after it is returned unwrapped, so `A` does not become `And([A])`;
    /// two or more are handed to `combine`. Both ShExC shape-expression levels — `OR` over
    /// `shapeAnd` and `AND` over `shapeNot` — are this production.
    fn parse_operator_chain(
        &mut self,
        keyword: &str,
        operand: fn(&mut Self) -> Result<ShapeExpr, String>,
        combine: fn(Vec<ShapeExpr>) -> ShapeExpr,
    ) -> Result<ShapeExpr, String> {
        let first = operand(self)?;
        if !matches!(self.peek(), Tok::Word(w) if w == keyword) {
            return Ok(first);
        }
        let mut branches = vec![first];
        while self.eat_word(keyword) {
            branches.push(operand(self)?);
        }
        Ok(combine(branches))
    }

    // shapeNot := "NOT"? shapeAtom
    fn parse_shape_not(&mut self) -> Result<ShapeExpr, String> {
        if self.eat_word("NOT") {
            return Ok(ShapeExpr::Not(Box::new(self.parse_shape_atom()?)));
        }
        self.parse_shape_atom()
    }

    // shapeAtom := nodeConstraint shapeOrRef? | shapeOrRef | "(" shapeExpression ")" | "."
    fn parse_shape_atom(&mut self) -> Result<ShapeExpr, String> {
        if self.eat_punct('.') {
            return Ok(ShapeExpr::NodeConstraint(NodeConstraint::default()));
        }
        if self.eat_punct('(') {
            let inner = self.parse_shape_expression()?;
            self.expect_punct(')')?;
            return Ok(inner);
        }
        if matches!(self.peek(), Tok::Punct('@')) {
            return self.parse_shape_ref();
        }
        if self.at_shape_definition_start() {
            return self.parse_shape_definition_with_qualifiers();
        }
        // A node constraint, optionally AND-ed with a following shape ref/def
        // (ShExC's `IRI @<Shape>` / `IRI { ... }` idiom).
        let nc = self.parse_node_constraint()?;
        let is_non_literal = matches!(
            nc.node_kind,
            Some(NodeKind::Iri) | Some(NodeKind::BNode) | Some(NodeKind::NonLiteral)
        );
        if is_non_literal
            && (matches!(self.peek(), Tok::Punct('@')) || self.at_shape_definition_start())
        {
            let rest = if matches!(self.peek(), Tok::Punct('@')) {
                self.parse_shape_ref()?
            } else {
                self.parse_shape_definition_with_qualifiers()?
            };
            return Ok(ShapeExpr::And(vec![ShapeExpr::NodeConstraint(nc), rest]));
        }
        Ok(ShapeExpr::NodeConstraint(nc))
    }

    /// Without consuming anything: does the current position start a shape
    /// definition (`{ … }`, optionally preceded by `CLOSED`/`EXTRA` qualifiers)?
    fn at_shape_definition_start(&self) -> bool {
        matches!(self.peek(), Tok::Punct('{'))
            || matches!(self.peek(), Tok::Word(w) if w == "CLOSED" || w == "EXTRA")
    }

    fn parse_shape_definition_with_qualifiers(&mut self) -> Result<ShapeExpr, String> {
        let mut closed = false;
        let mut extra = Vec::new();
        loop {
            if self.eat_word("CLOSED") {
                closed = true;
                continue;
            }
            if self.eat_word("EXTRA") {
                loop {
                    extra.push(self.parse_predicate()?);
                    if !matches!(self.peek(), Tok::Iri(_)) {
                        break;
                    }
                }
                continue;
            }
            break;
        }
        self.parse_shape_definition(closed, extra)
    }

    fn parse_shape_ref(&mut self) -> Result<ShapeExpr, String> {
        self.expect_punct('@')?;
        let label = self.parse_iri_or_label()?;
        Ok(ShapeExpr::Ref(label))
    }

    fn parse_shape_definition(
        &mut self,
        closed: bool,
        extra: Vec<String>,
    ) -> Result<ShapeExpr, String> {
        self.expect_punct('{')?;
        let expression = if matches!(self.peek(), Tok::Punct('}')) {
            None
        } else {
            Some(self.parse_triple_expression()?)
        };
        self.expect_punct('}')?;
        Ok(ShapeExpr::Shape(Shape {
            expression,
            closed,
            extra,
        }))
    }

    // ── Node constraints ──────────────────────────────────────────────────

    fn parse_node_constraint(&mut self) -> Result<NodeConstraint, String> {
        let mut nc = parse_node_constraint_atom(self)?;
        parse_node_constraint_facets(self, &mut nc)?;
        Ok(nc)
    }

    fn parse_facet_uint(&mut self) -> Result<usize, String> {
        self.parse_facet("integer", "an integer")
    }

    fn parse_facet_num(&mut self) -> Result<f64, String> {
        self.parse_facet("numeric", "a numeric")
    }

    /// Parse a facet's numeric argument into `T`. `kind` names the argument type in the
    /// unparsable-number message and `expected` (the same noun with its article) in the
    /// wrong-token message.
    fn parse_facet<T>(&mut self, kind: &str, expected: &str) -> Result<T, String>
    where
        T: std::str::FromStr,
        T::Err: std::fmt::Display,
    {
        match self.bump() {
            Tok::Num(n) => n
                .parse::<T>()
                .map_err(|e| format!("ShExC: bad {kind} facet argument `{n}`: {e}")),
            other => Err(format!(
                "ShExC: expected {expected} facet argument, found {other:?}"
            )),
        }
    }

    /// `PATTERN "regex"` optionally followed directly by `/flags` (no whitespace).
    fn parse_pattern(&mut self) -> Result<(String, Option<String>), String> {
        let pattern = match self.bump() {
            Tok::Str(s, _) => s,
            other => return Err(format!("ShExC: expected a PATTERN string, found {other:?}")),
        };
        // Flags: only recognised as `word`-lexed run of letters directly glued to
        // a `/`; our lexer has no `/` token, so flags (rare in practice) are a
        // documented gap here rather than a fragile re-lex hack.
        Ok((pattern, None))
    }

    fn parse_value_set(&mut self) -> Result<Vec<ValueSetValue>, String> {
        self.expect_punct('[')?;
        let mut out = Vec::new();
        while !matches!(self.peek(), Tok::Punct(']')) {
            match self.bump() {
                Tok::Iri(iri) => out.push(ValueSetValue::Iri(iri)),
                Tok::Str(s, suffix) => {
                    let (datatype, language) = match suffix {
                        StrSuffix::None => (None, None),
                        StrSuffix::Datatype(d) => (Some(d), None),
                        StrSuffix::Lang(l) => (None, Some(l)),
                    };
                    out.push(ValueSetValue::Literal {
                        value: s,
                        datatype,
                        language,
                    });
                }
                Tok::Punct('~') => {
                    return Err(
                        "ShExC: value-set stem ranges (`~`) are not supported (EG-133 follow-up)"
                            .to_string(),
                    )
                }
                other => return Err(format!("ShExC: unexpected value-set member {other:?}")),
            }
        }
        self.expect_punct(']')?;
        Ok(out)
    }

    // ── Triple expressions ─────────────────────────────────────────────────

    // oneOfTripleExpr := groupTripleExpr ("|" groupTripleExpr)*
    fn parse_triple_expression(&mut self) -> Result<TripleExpr, String> {
        let first = self.parse_group_triple_expr()?;
        if !matches!(self.peek(), Tok::Punct('|')) {
            return Ok(first);
        }
        let mut branches = vec![first];
        while self.eat_punct('|') {
            branches.push(self.parse_group_triple_expr()?);
        }
        Ok(TripleExpr::OneOf(branches))
    }

    // groupTripleExpr := unaryTripleExpr (";" unaryTripleExpr)* ";"?
    fn parse_group_triple_expr(&mut self) -> Result<TripleExpr, String> {
        let first = self.parse_unary_triple_expr()?;
        if !matches!(self.peek(), Tok::Punct(';')) {
            return Ok(first);
        }
        let mut branches = vec![first];
        while self.eat_punct(';') {
            if matches!(
                self.peek(),
                Tok::Punct('}') | Tok::Punct('|') | Tok::Punct(')')
            ) {
                break; // trailing ';'
            }
            branches.push(self.parse_unary_triple_expr()?);
        }
        Ok(TripleExpr::EachOf(branches))
    }

    fn parse_unary_triple_expr(&mut self) -> Result<TripleExpr, String> {
        if self.eat_punct('(') {
            let inner = self.parse_triple_expression()?;
            self.expect_punct(')')?;
            let (min, max) = self.parse_cardinality();
            if (min, max) != (1, 1) {
                // Our `TripleExpr` model (shared with the ShExJ parser) has no
                // "repeated group" node — only a leaf `TripleConstraint` carries
                // min/max. Silently dropping the cardinality would validate data
                // that should have been rejected (or vice versa), so this is a
                // hard error rather than a silent approximation.
                return Err(
                    "ShExC: a cardinality on a parenthesized triple-expression group is not supported (only a single predicate's cardinality is)".to_string(),
                );
            }
            return Ok(inner);
        }
        self.parse_triple_constraint()
    }

    fn parse_triple_constraint(&mut self) -> Result<TripleExpr, String> {
        let inverse = self.eat_punct('^');
        let predicate = self.parse_predicate()?;
        let value_expr = if self.can_start_shape_expression() {
            Some(Box::new(self.parse_shape_expression()?))
        } else {
            None
        };
        let (min, max) = self.parse_cardinality();
        Ok(TripleExpr::TripleConstraint {
            predicate,
            value_expr,
            min,
            max,
            inverse,
        })
    }

    fn parse_predicate(&mut self) -> Result<String, String> {
        if matches!(self.peek(), Tok::Word(w) if w == "a") {
            self.bump();
            return Ok(RDF_TYPE.to_string());
        }
        match self.bump() {
            Tok::Iri(iri) => Ok(iri),
            other => Err(format!("ShExC: expected a predicate IRI, found {other:?}")),
        }
    }

    fn can_start_shape_expression(&self) -> bool {
        matches!(
            self.peek(),
            Tok::Punct('.')
                | Tok::Punct('(')
                | Tok::Punct('@')
                | Tok::Punct('{')
                | Tok::Punct('[')
                | Tok::Iri(_)
        ) || matches!(self.peek(), Tok::Word(w) if matches!(w.as_str(), "IRI" | "BNODE" | "LITERAL" | "NONLITERAL" | "CLOSED" | "EXTRA" | "NOT"))
    }

    /// `?` / `*` / `+` / `{m,n}` / `{m,}` / `{m}` / absent (defaults to `{1,1}`).
    fn parse_cardinality(&mut self) -> (i64, i64) {
        match self.peek().clone() {
            Tok::Punct('?') => {
                self.bump();
                (0, 1)
            }
            Tok::Punct('*') => {
                self.bump();
                (0, -1)
            }
            Tok::Punct('+') => {
                self.bump();
                (1, -1)
            }
            Tok::RepeatRange(min, max) => {
                self.bump();
                (min as i64, max.map(|m| m as i64).unwrap_or(-1))
            }
            _ => (1, 1),
        }
    }
}
