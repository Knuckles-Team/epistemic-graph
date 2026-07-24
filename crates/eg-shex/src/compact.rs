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

use std::collections::HashMap;

use crate::schema::{
    NodeConstraint, NodeKind, Schema, Shape, ShapeExpr, TripleExpr, ValueSetValue,
};

/// Parse a ShExC (compact syntax) document into a [`Schema`] (CONCEPT:EG-KG.compute.concept-2).
pub fn parse(text: &str) -> Result<Schema, String> {
    let tokens = lex(text)?;
    let mut p = Parser {
        toks: &tokens,
        pos: 0,
    };
    p.parse_schema()
}

// ── Lexer ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    /// A fully-resolved absolute IRI (from `<iri>`, prefix-expansion, or `a`).
    Iri(String),
    /// A bare word with no `:` — a keyword candidate (`CLOSED`, `AND`, `IRI`, …).
    Word(String),
    Str(String, StrSuffix),
    /// A bare numeric literal's lexical form (facet arguments).
    Num(String),
    /// `{m,n}` / `{m,}` / `{m}` — lexed as one token because a bare `{` alone
    /// starts a shape definition (disambiguated by "digit right after `{`").
    RepeatRange(u64, Option<u64>),
    Punct(char),
}

#[derive(Debug, Clone, PartialEq)]
enum StrSuffix {
    None,
    Datatype(String),
    Lang(String),
}

fn lex(text: &str) -> Result<Vec<Tok>, String> {
    let chars: Vec<char> = text.chars().collect();
    let mut i = 0usize;
    let mut prefixes: HashMap<String, String> = HashMap::new();
    let mut base: Option<String> = None;
    let mut out = Vec::new();

    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
            continue;
        }
        if c == '#' {
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
            continue;
        }
        if c == '%' {
            return Err("ShExC: semantic actions (%...%) are not supported".to_string());
        }
        if c == '<' {
            let (iri, next) = read_iriref(&chars, i)?;
            i = next;
            out.push(Tok::Iri(resolve_base(&iri, base.as_deref())));
            continue;
        }
        if c == '"' || c == '\'' {
            let (s, next) = read_string(&chars, i, c)?;
            i = next;
            let (suffix, next2) = read_str_suffix(&chars, i, &prefixes)?;
            i = next2;
            out.push(Tok::Str(s, suffix));
            continue;
        }
        if c == '{' {
            if let Some((min, max, next)) = try_read_repeat_range(&chars, i) {
                out.push(Tok::RepeatRange(min, max));
                i = next;
                continue;
            }
            out.push(Tok::Punct('{'));
            i += 1;
            continue;
        }
        if "}()[]|;,.?*+^$~=".contains(c) {
            out.push(Tok::Punct(c));
            i += 1;
            continue;
        }
        if c == '@' {
            out.push(Tok::Punct('@'));
            i += 1;
            continue;
        }
        if c.is_ascii_digit() || ((c == '-' || c == '+') && peek_digit(&chars, i + 1)) {
            let (num, next) = read_number(&chars, i);
            i = next;
            out.push(Tok::Num(num));
            continue;
        }
        if is_pn_char_start(c) {
            let (word, next) = read_pn(&chars, i);
            i = next;
            if let Some(local) = word.strip_prefix(':') {
                // A prefixed name with the DEFAULT (empty) prefix.
                let ns = prefixes.get("").cloned().unwrap_or_default();
                out.push(Tok::Iri(format!("{ns}{local}")));
                continue;
            }
            if let Some((pfx, local)) = word.split_once(':') {
                let ns = prefixes.get(pfx).ok_or_else(|| {
                    format!("ShExC: undeclared prefix `{pfx}:` (missing a PREFIX directive)")
                })?;
                out.push(Tok::Iri(format!("{ns}{local}")));
                continue;
            }
            match word.as_str() {
                "PREFIX" => {
                    let (pfx, next) = read_pname_ns(&chars, next_non_ws(&chars, i))?;
                    let (iri, next2) = expect_iriref(&chars, next_non_ws(&chars, next))?;
                    prefixes.insert(pfx, resolve_base(&iri, base.as_deref()));
                    i = next2;
                }
                "BASE" => {
                    let (iri, next2) = expect_iriref(&chars, next_non_ws(&chars, i))?;
                    base = Some(resolve_base(&iri, base.as_deref()));
                    i = next2;
                }
                _ => out.push(Tok::Word(word)),
            }
            continue;
        }
        return Err(format!("ShExC: unexpected character `{c}` at offset {i}"));
    }
    out.push(Tok::Punct('\0')); // EOF sentinel
    Ok(out)
}

fn next_non_ws(chars: &[char], mut i: usize) -> usize {
    while i < chars.len() && chars[i].is_whitespace() {
        i += 1;
    }
    i
}

fn peek_digit(chars: &[char], i: usize) -> bool {
    chars.get(i).is_some_and(|c| c.is_ascii_digit())
}

fn resolve_base(iri: &str, base: Option<&str>) -> String {
    if iri.contains("://") || iri.starts_with("urn:") || base.is_none() || iri.is_empty() {
        return iri.to_string();
    }
    format!("{}{iri}", base.unwrap_or_default())
}

fn read_iriref(chars: &[char], start: usize) -> Result<(String, usize), String> {
    debug_assert_eq!(chars[start], '<');
    let mut i = start + 1;
    let mut s = String::new();
    while i < chars.len() {
        match chars[i] {
            '>' => return Ok((s, i + 1)),
            '\\' if i + 1 < chars.len() => {
                s.push(chars[i + 1]);
                i += 2;
            }
            c => {
                s.push(c);
                i += 1;
            }
        }
    }
    Err("ShExC: unterminated IRIREF (missing `>`)".to_string())
}

fn expect_iriref(chars: &[char], i: usize) -> Result<(String, usize), String> {
    if chars.get(i) != Some(&'<') {
        return Err("ShExC: expected an IRIREF (`<...>`)".to_string());
    }
    read_iriref(chars, i)
}

/// A `PNAME_NS` for a `PREFIX` directive: `word:` (the prefix name, possibly empty).
fn read_pname_ns(chars: &[char], start: usize) -> Result<(String, usize), String> {
    let mut i = start;
    let mut s = String::new();
    while i < chars.len() && chars[i] != ':' && !chars[i].is_whitespace() {
        s.push(chars[i]);
        i += 1;
    }
    if chars.get(i) != Some(&':') {
        return Err("ShExC: expected `prefix:` in a PREFIX directive".to_string());
    }
    Ok((s, i + 1))
}

fn read_string(chars: &[char], start: usize, quote: char) -> Result<(String, usize), String> {
    let triple = chars.get(start + 1) == Some(&quote) && chars.get(start + 2) == Some(&quote);
    let mut i = start + if triple { 3 } else { 1 };
    let mut s = String::new();
    loop {
        if i >= chars.len() {
            return Err("ShExC: unterminated string literal".to_string());
        }
        if chars[i] == '\\' && i + 1 < chars.len() {
            s.push(match chars[i + 1] {
                'n' => '\n',
                't' => '\t',
                'r' => '\r',
                other => other,
            });
            i += 2;
            continue;
        }
        if chars[i] == quote {
            if !triple {
                return Ok((s, i + 1));
            }
            if chars.get(i + 1) == Some(&quote) && chars.get(i + 2) == Some(&quote) {
                return Ok((s, i + 3));
            }
        }
        s.push(chars[i]);
        i += 1;
    }
}

fn read_str_suffix(
    chars: &[char],
    i: usize,
    prefixes: &HashMap<String, String>,
) -> Result<(StrSuffix, usize), String> {
    if chars.get(i) == Some(&'^') && chars.get(i + 1) == Some(&'^') {
        let mut j = i + 2;
        if chars.get(j) == Some(&'<') {
            let (iri, next) = read_iriref(chars, j)?;
            return Ok((StrSuffix::Datatype(iri), next));
        }
        let (word, next) = read_pn(chars, j);
        j = next;
        let (pfx, local) = word.split_once(':').ok_or_else(|| {
            "ShExC: expected a datatype IRI or prefixed name after `^^`".to_string()
        })?;
        let ns = prefixes
            .get(pfx)
            .ok_or_else(|| format!("ShExC: undeclared prefix `{pfx}:` in a datatype"))?;
        return Ok((StrSuffix::Datatype(format!("{ns}{local}")), j));
    }
    if chars.get(i) == Some(&'@') {
        let mut j = i + 1;
        let mut tag = String::new();
        while j < chars.len() && (chars[j].is_alphanumeric() || chars[j] == '-') {
            tag.push(chars[j]);
            j += 1;
        }
        if !tag.is_empty() {
            return Ok((StrSuffix::Lang(tag), j));
        }
    }
    Ok((StrSuffix::None, i))
}

fn read_number(chars: &[char], start: usize) -> (String, usize) {
    let mut i = start;
    let mut s = String::new();
    if chars[i] == '-' || chars[i] == '+' {
        s.push(chars[i]);
        i += 1;
    }
    while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
        s.push(chars[i]);
        i += 1;
    }
    if i < chars.len() && (chars[i] == 'e' || chars[i] == 'E') {
        let mut j = i + 1;
        let mut exp = String::from(chars[i]);
        if j < chars.len() && (chars[j] == '+' || chars[j] == '-') {
            exp.push(chars[j]);
            j += 1;
        }
        if peek_digit(chars, j) {
            while j < chars.len() && chars[j].is_ascii_digit() {
                exp.push(chars[j]);
                j += 1;
            }
            s.push_str(&exp);
            i = j;
        }
    }
    (s, i)
}

fn is_pn_char_start(c: char) -> bool {
    c.is_alphabetic() || c == '_' || c == ':'
}

fn is_pn_char(c: char) -> bool {
    c.is_alphanumeric() || matches!(c, '_' | '-' | '.' | ':')
}

/// A bare word or `prefix:local` / `prefix:` / `:local` token (a keyword, a
/// directive name, or a prefixed name — disambiguated by the caller).
fn read_pn(chars: &[char], start: usize) -> (String, usize) {
    let mut i = start;
    let mut s = String::new();
    while i < chars.len() && is_pn_char(chars[i]) {
        s.push(chars[i]);
        i += 1;
    }
    // A trailing `.` is very likely end-of-statement punctuation, not part of a
    // local name (ShExC local names don't typically end in `.`); back off ONE
    // trailing `.` so `ex:Foo .` and `ex:Foo.` (no space) both lex sanely.
    if s.ends_with('.') && !s.ends_with("..") {
        s.pop();
        i -= 1;
    }
    (s, i)
}

/// `{` immediately followed (no whitespace) by a digit or `,` is a repeat-range
/// cardinality, not a shape-definition's opening brace.
fn try_read_repeat_range(chars: &[char], start: usize) -> Option<(u64, Option<u64>, usize)> {
    let mut i = start + 1;
    if !chars.get(i).is_some_and(|c| c.is_ascii_digit()) {
        return None;
    }
    let mut min_s = String::new();
    while chars.get(i).is_some_and(|c| c.is_ascii_digit()) {
        min_s.push(chars[i]);
        i += 1;
    }
    let min: u64 = min_s.parse().ok()?;
    let max = if chars.get(i) == Some(&',') {
        i += 1;
        let mut max_s = String::new();
        while chars.get(i).is_some_and(|c| c.is_ascii_digit()) {
            max_s.push(chars[i]);
            i += 1;
        }
        if max_s.is_empty() {
            None
        } else {
            Some(max_s.parse().ok()?)
        }
    } else {
        Some(min)
    };
    if chars.get(i) != Some(&'}') {
        return None;
    }
    Some((min, max, i + 1))
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
        let first = self.parse_shape_and()?;
        if !matches!(self.peek(), Tok::Word(w) if w == "OR") {
            return Ok(first);
        }
        let mut branches = vec![first];
        while self.eat_word("OR") {
            branches.push(self.parse_shape_and()?);
        }
        Ok(ShapeExpr::Or(branches))
    }

    // shapeAnd := shapeNot ("AND" shapeNot)*
    fn parse_shape_and(&mut self) -> Result<ShapeExpr, String> {
        let first = self.parse_shape_not()?;
        if !matches!(self.peek(), Tok::Word(w) if w == "AND") {
            return Ok(first);
        }
        let mut branches = vec![first];
        while self.eat_word("AND") {
            branches.push(self.parse_shape_not()?);
        }
        Ok(ShapeExpr::And(branches))
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
        let mut nc = NodeConstraint::default();
        match self.peek().clone() {
            Tok::Word(w) if w == "IRI" => {
                self.bump();
                nc.node_kind = Some(NodeKind::Iri);
            }
            Tok::Word(w) if w == "BNODE" => {
                self.bump();
                nc.node_kind = Some(NodeKind::BNode);
            }
            Tok::Word(w) if w == "LITERAL" => {
                self.bump();
                nc.node_kind = Some(NodeKind::Literal);
            }
            Tok::Word(w) if w == "NONLITERAL" => {
                self.bump();
                nc.node_kind = Some(NodeKind::NonLiteral);
            }
            Tok::Iri(iri) => {
                self.bump();
                nc.datatype = Some(iri);
            }
            Tok::Punct('[') => {
                nc.values = Some(self.parse_value_set()?);
            }
            other => {
                return Err(format!(
                    "ShExC: expected a node constraint (IRI/BNODE/LITERAL/NONLITERAL/datatype/value set), found {other:?}"
                ))
            }
        }
        loop {
            match self.peek().clone() {
                Tok::Word(w) if w == "LENGTH" => {
                    self.bump();
                    nc.string_facets.length = Some(self.parse_facet_uint()?);
                }
                Tok::Word(w) if w == "MINLENGTH" => {
                    self.bump();
                    nc.string_facets.minlength = Some(self.parse_facet_uint()?);
                }
                Tok::Word(w) if w == "MAXLENGTH" => {
                    self.bump();
                    nc.string_facets.maxlength = Some(self.parse_facet_uint()?);
                }
                Tok::Word(w) if w == "PATTERN" => {
                    self.bump();
                    let (pat, flags) = self.parse_pattern()?;
                    nc.string_facets.pattern = Some(pat);
                    nc.string_facets.flags = flags;
                }
                Tok::Word(w) if w == "MININCLUSIVE" => {
                    self.bump();
                    nc.numeric_facets.mininclusive = Some(self.parse_facet_num()?);
                }
                Tok::Word(w) if w == "MAXINCLUSIVE" => {
                    self.bump();
                    nc.numeric_facets.maxinclusive = Some(self.parse_facet_num()?);
                }
                Tok::Word(w) if w == "MINEXCLUSIVE" => {
                    self.bump();
                    nc.numeric_facets.minexclusive = Some(self.parse_facet_num()?);
                }
                Tok::Word(w) if w == "MAXEXCLUSIVE" => {
                    self.bump();
                    nc.numeric_facets.maxexclusive = Some(self.parse_facet_num()?);
                }
                _ => break,
            }
        }
        Ok(nc)
    }

    fn parse_facet_uint(&mut self) -> Result<usize, String> {
        match self.bump() {
            Tok::Num(n) => n
                .parse::<usize>()
                .map_err(|e| format!("ShExC: bad integer facet argument `{n}`: {e}")),
            other => Err(format!(
                "ShExC: expected an integer facet argument, found {other:?}"
            )),
        }
    }

    fn parse_facet_num(&mut self) -> Result<f64, String> {
        match self.bump() {
            Tok::Num(n) => n
                .parse::<f64>()
                .map_err(|e| format!("ShExC: bad numeric facet argument `{n}`: {e}")),
            other => Err(format!(
                "ShExC: expected a numeric facet argument, found {other:?}"
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
