//! CONCEPT:EG-172 — a pure-Rust PromQL parser + evaluator over the eg-tsdb series
//! model.
//!
//! This is the dependency-free engine half of EG-172: a hand-rolled recursive-descent
//! PromQL parser and an evaluator that runs the parsed expression against a
//! [`SeriesSource`] (the read seam the facade binds to the durable `SeriesStore`).
//! NOTHING here pulls a new dependency — the parser, the evaluator, and the anchored
//! regex matcher backing `=~`/`!~` are all hand-rolled, so this compiles into the lean
//! / Pi build (the Pi contract). The facade (`src/server/promql`) wires the
//! Prometheus-compatible HTTP API (`/api/v1/query`, `/api/v1/query_range`, `/labels`,
//! `/label/<name>/values`) over this and adapts the durable store to [`SeriesSource`].
//!
//! ## Model
//!
//! A PromQL sample carries a set of [`Labels`] (a metric's identity, including the
//! reserved `__name__`) and a float value. A *series* is labels + a ts-sorted list of
//! `(ts, value)` points; the [`SeriesSource`] returns series matching a set of
//! [`LabelMatcher`]s over a time window. Timestamps are epoch NANOSECONDS (the eg-tsdb
//! `Ts`); PromQL's second-granular semantics (`rate` per-second, the 5m lookback) are
//! computed by converting ns→s at the point of use.
//!
//! ## Supported surface (CONCEPT:EG-172)
//!
//!  * Selectors with matchers `=`, `!=`, `=~`, `!~`; instant + range (`metric[5m]`);
//!    `offset <dur>`.
//!  * Functions `rate`, `irate`, `increase` (range vectors, counter-reset aware),
//!    `abs`, `ceil`, `floor`, `histogram_quantile`.
//!  * Aggregations `sum`, `avg`, `min`, `max`, `count` with `by(...)` / `without(...)`.
//!  * Binary arithmetic (`+ - * / % ^`) and comparison (`== != > < >= <=`, with `bool`)
//!    between scalars and vectors, and vector↔vector with default / `on` / `ignoring`
//!    one-to-one label matching, plus set ops `and` / `or` / `unless`.
//!
//! Deferred (documented follow-ups): `group_left`/`group_right` many-to-one matching,
//! subqueries, `@`-modifier, `count_values`/`topk`/`quantile`, `label_replace`.

use std::collections::BTreeMap;

use crate::point::Ts;

/// A metric's label set — the reserved [`METRIC_NAME`] plus user labels.
pub type Labels = BTreeMap<String, String>;

/// The reserved label holding the metric name (`__name__`), Prometheus convention.
pub const METRIC_NAME: &str = "__name__";

/// Default instant-vector lookback: a selector's sample at `t` is the most recent
/// point within this window ending at `t`. Prometheus' default is 5 minutes.
pub const DEFAULT_LOOKBACK_NS: i64 = 5 * 60 * 1_000_000_000;

/// Nanoseconds per second (ns→s conversions for rate/increase).
const NS_PER_SEC: f64 = 1_000_000_000.0;

// ───────────────────────────── error ─────────────────────────────

/// A parse or evaluation error (`errorType` is derived by the facade).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromqlError(pub String);

impl std::fmt::Display for PromqlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl std::error::Error for PromqlError {}

fn err<T>(msg: impl Into<String>) -> Result<T, PromqlError> {
    Err(PromqlError(msg.into()))
}

// ───────────────────────────── label matching ─────────────────────────────

/// The four PromQL label-match operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchOp {
    /// `=` — exact equality.
    Eq,
    /// `!=` — inequality.
    Ne,
    /// `=~` — anchored regex match.
    Re,
    /// `!~` — anchored regex non-match.
    Nre,
}

/// One label matcher (`name <op> "value"`). For `=~`/`!~` a compiled anchored
/// [`Regex`] is carried alongside the source text.
#[derive(Debug, Clone)]
pub struct LabelMatcher {
    pub name: String,
    pub op: MatchOp,
    pub value: String,
    regex: Option<Regex>,
}

impl LabelMatcher {
    /// Build a matcher, compiling the regex up-front for `=~`/`!~`.
    pub fn new(
        name: impl Into<String>,
        op: MatchOp,
        value: impl Into<String>,
    ) -> Result<Self, PromqlError> {
        let name = name.into();
        let value = value.into();
        let regex = match op {
            MatchOp::Re | MatchOp::Nre => Some(Regex::compile(&value)?),
            _ => None,
        };
        Ok(Self {
            name,
            op,
            value,
            regex,
        })
    }

    /// Whether `labels` satisfies this matcher. A missing label is treated as the
    /// empty string (Prometheus semantics), so `foo!=""` selects series that HAVE
    /// `foo`, and `foo=~".*"` matches even absent labels (empty matches `.*`).
    pub fn matches(&self, labels: &Labels) -> bool {
        let v = labels.get(&self.name).map(String::as_str).unwrap_or("");
        match self.op {
            MatchOp::Eq => v == self.value,
            MatchOp::Ne => v != self.value,
            MatchOp::Re => self
                .regex
                .as_ref()
                .map(|r| r.is_full_match(v))
                .unwrap_or(false),
            MatchOp::Nre => !self
                .regex
                .as_ref()
                .map(|r| r.is_full_match(v))
                .unwrap_or(false),
        }
    }
}

// ───────────────────────────── series source seam ─────────────────────────────

/// One labeled series returned by a [`SeriesSource`]: its labels (including
/// `__name__`) and ts-sorted `(ts, value)` points.
#[derive(Debug, Clone, PartialEq)]
pub struct LabeledSeries {
    pub labels: Labels,
    pub points: Vec<(Ts, f64)>,
}

/// The read seam the evaluator runs against. The facade implements this over the
/// durable `SeriesStore`; tests use [`MemSeriesSource`].
pub trait SeriesSource {
    /// Every series whose labels satisfy ALL `matchers`, each with its `(ts, value)`
    /// points within `[start, end]` (inclusive) in ts order.
    fn select(&self, matchers: &[LabelMatcher], start: Ts, end: Ts) -> Vec<LabeledSeries>;

    /// All distinct label names across the source (for `/api/v1/labels`).
    fn label_names(&self) -> Vec<String> {
        Vec::new()
    }

    /// All distinct values for `name` (for `/api/v1/label/<name>/values`).
    fn label_values(&self, _name: &str) -> Vec<String> {
        Vec::new()
    }
}

/// An in-memory [`SeriesSource`] — the test source and a usable adapter for callers
/// that already hold materialized series.
#[derive(Debug, Default, Clone)]
pub struct MemSeriesSource {
    series: Vec<LabeledSeries>,
}

impl MemSeriesSource {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a series from `(name, [(label,value)])` and ts-sorted points.
    pub fn push(&mut self, labels: Labels, mut points: Vec<(Ts, f64)>) {
        points.sort_by_key(|p| p.0);
        self.series.push(LabeledSeries { labels, points });
    }

    /// Convenience: build labels from name + pairs.
    pub fn labels(name: &str, pairs: &[(&str, &str)]) -> Labels {
        let mut l = Labels::new();
        l.insert(METRIC_NAME.to_string(), name.to_string());
        for (k, v) in pairs {
            l.insert((*k).to_string(), (*v).to_string());
        }
        l
    }
}

impl SeriesSource for MemSeriesSource {
    fn select(&self, matchers: &[LabelMatcher], start: Ts, end: Ts) -> Vec<LabeledSeries> {
        self.series
            .iter()
            .filter(|s| matchers.iter().all(|m| m.matches(&s.labels)))
            .map(|s| LabeledSeries {
                labels: s.labels.clone(),
                points: s
                    .points
                    .iter()
                    .copied()
                    .filter(|(t, _)| *t >= start && *t <= end)
                    .collect(),
            })
            .collect()
    }

    fn label_names(&self) -> Vec<String> {
        let mut set: BTreeMap<String, ()> = BTreeMap::new();
        for s in &self.series {
            for k in s.labels.keys() {
                set.insert(k.clone(), ());
            }
        }
        set.into_keys().collect()
    }

    fn label_values(&self, name: &str) -> Vec<String> {
        let mut set: BTreeMap<String, ()> = BTreeMap::new();
        for s in &self.series {
            if let Some(v) = s.labels.get(name) {
                set.insert(v.clone(), ());
            }
        }
        set.into_keys().collect()
    }
}

// ───────────────────────────── AST ─────────────────────────────

/// Binary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
    Eq,
    Ne,
    Gt,
    Lt,
    Ge,
    Le,
    And,
    Or,
    Unless,
}

impl BinOp {
    fn is_comparison(self) -> bool {
        matches!(
            self,
            BinOp::Eq | BinOp::Ne | BinOp::Gt | BinOp::Lt | BinOp::Ge | BinOp::Le
        )
    }
    fn is_setop(self) -> bool {
        matches!(self, BinOp::And | BinOp::Or | BinOp::Unless)
    }
}

/// Aggregation operators (`sum`/`avg`/`min`/`max`/`count`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggOp {
    Sum,
    Avg,
    Min,
    Max,
    Count,
}

/// The label-matching modifier on a vector↔vector binary op.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorMatching {
    /// `true` ⇒ `on(labels)`, `false` ⇒ `ignoring(labels)`.
    pub on: bool,
    pub labels: Vec<String>,
}

/// A PromQL expression node.
#[derive(Debug, Clone)]
pub enum Expr {
    /// A scalar literal.
    Number(f64),
    /// A string literal (only valid as certain function args; carried for completeness).
    Str(String),
    /// An instant-vector selector: matchers + optional `offset`.
    Selector {
        matchers: Vec<LabelMatcher>,
        offset_ns: i64,
    },
    /// A range-vector selector: `inner[range]` (+ offset on the inner selector).
    Matrix {
        matchers: Vec<LabelMatcher>,
        range_ns: i64,
        offset_ns: i64,
    },
    /// A function call.
    Call { func: String, args: Vec<Expr> },
    /// An aggregation with an optional by/without grouping.
    Aggregate {
        op: AggOp,
        expr: Box<Expr>,
        by: bool,
        labels: Vec<String>,
    },
    /// A binary operation (with optional `bool` modifier + vector matching).
    Binary {
        op: BinOp,
        lhs: Box<Expr>,
        rhs: Box<Expr>,
        bool_modifier: bool,
        matching: Option<VectorMatching>,
    },
    /// Unary negation.
    Neg(Box<Expr>),
}

// ───────────────────────────── lexer ─────────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Ident(String),
    Number(f64),
    Duration(i64), // ns
    Str(String),
    LBrace,
    RBrace,
    LParen,
    RParen,
    LBracket,
    RBracket,
    Comma,
    // matchers
    Eq,
    Ne,
    Re,
    Nre,
    // arithmetic / comparison
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
    Caret,
    EqEq,
    Gt,
    Lt,
    Ge,
    Le,
}

struct Lexer<'a> {
    src: &'a [u8],
    pos: usize,
}

impl<'a> Lexer<'a> {
    fn new(src: &'a str) -> Self {
        Self {
            src: src.as_bytes(),
            pos: 0,
        }
    }

    fn tokenize(mut self) -> Result<Vec<Tok>, PromqlError> {
        let mut out = Vec::new();
        while let Some(t) = self.next_tok()? {
            out.push(t);
        }
        Ok(out)
    }

    fn peek(&self) -> Option<u8> {
        self.src.get(self.pos).copied()
    }

    fn next_tok(&mut self) -> Result<Option<Tok>, PromqlError> {
        while let Some(c) = self.peek() {
            if c.is_ascii_whitespace() {
                self.pos += 1;
            } else {
                break;
            }
        }
        let Some(c) = self.peek() else {
            return Ok(None);
        };
        // punctuation / operators
        let two =
            |a: u8, b: u8, s: &[u8], p: usize| s.get(p) == Some(&a) && s.get(p + 1) == Some(&b);
        if two(b'=', b'~', self.src, self.pos) {
            self.pos += 2;
            return Ok(Some(Tok::Re));
        }
        if two(b'!', b'~', self.src, self.pos) {
            self.pos += 2;
            return Ok(Some(Tok::Nre));
        }
        if two(b'=', b'=', self.src, self.pos) {
            self.pos += 2;
            return Ok(Some(Tok::EqEq));
        }
        if two(b'!', b'=', self.src, self.pos) {
            self.pos += 2;
            return Ok(Some(Tok::Ne));
        }
        if two(b'>', b'=', self.src, self.pos) {
            self.pos += 2;
            return Ok(Some(Tok::Ge));
        }
        if two(b'<', b'=', self.src, self.pos) {
            self.pos += 2;
            return Ok(Some(Tok::Le));
        }
        let single = match c {
            b'{' => Some(Tok::LBrace),
            b'}' => Some(Tok::RBrace),
            b'(' => Some(Tok::LParen),
            b')' => Some(Tok::RParen),
            b'[' => Some(Tok::LBracket),
            b']' => Some(Tok::RBracket),
            b',' => Some(Tok::Comma),
            b'=' => Some(Tok::Eq),
            b'+' => Some(Tok::Plus),
            b'-' => Some(Tok::Minus),
            b'*' => Some(Tok::Star),
            b'/' => Some(Tok::Slash),
            b'%' => Some(Tok::Percent),
            b'^' => Some(Tok::Caret),
            b'>' => Some(Tok::Gt),
            b'<' => Some(Tok::Lt),
            _ => None,
        };
        if let Some(t) = single {
            self.pos += 1;
            return Ok(Some(t));
        }
        // string literal
        if c == b'"' || c == b'\'' {
            return Ok(Some(self.lex_string(c)?));
        }
        // number or duration
        if c.is_ascii_digit()
            || (c == b'.'
                && self
                    .src
                    .get(self.pos + 1)
                    .is_some_and(|d| d.is_ascii_digit()))
        {
            return Ok(Some(self.lex_number_or_duration()?));
        }
        // identifier
        if c == b'_' || c == b':' || c.is_ascii_alphabetic() {
            return Ok(Some(self.lex_ident()));
        }
        err(format!(
            "unexpected character '{}' at offset {}",
            c as char, self.pos
        ))
    }

    fn lex_string(&mut self, quote: u8) -> Result<Tok, PromqlError> {
        self.pos += 1; // opening quote
        let mut s = String::new();
        while let Some(c) = self.peek() {
            if c == b'\\' {
                self.pos += 1;
                match self.peek() {
                    Some(b'n') => s.push('\n'),
                    Some(b't') => s.push('\t'),
                    Some(other) => s.push(other as char),
                    None => return err("unterminated escape in string literal"),
                }
                self.pos += 1;
            } else if c == quote {
                self.pos += 1;
                return Ok(Tok::Str(s));
            } else {
                s.push(c as char);
                self.pos += 1;
            }
        }
        err("unterminated string literal")
    }

    fn lex_ident(&mut self) -> Tok {
        let start = self.pos;
        while let Some(c) = self.peek() {
            if c == b'_' || c == b':' || c.is_ascii_alphanumeric() {
                self.pos += 1;
            } else {
                break;
            }
        }
        Tok::Ident(String::from_utf8_lossy(&self.src[start..self.pos]).to_string())
    }

    fn lex_number_or_duration(&mut self) -> Result<Tok, PromqlError> {
        let start = self.pos;
        // A duration is one-or-more `<int><unit>` groups with unit in y/w/d/h/m/s/ms
        // and NO '.'/exponent. Otherwise it's a float. Scan the raw run first.
        let mut has_dot = false;
        let mut has_exp = false;
        let mut j = self.pos;
        while let Some(c) = self.src.get(j).copied() {
            if c.is_ascii_digit() {
                j += 1;
            } else if c == b'.' {
                has_dot = true;
                j += 1;
            } else if (c == b'e' || c == b'E')
                && self
                    .src
                    .get(j + 1)
                    .is_some_and(|n| n.is_ascii_digit() || *n == b'+' || *n == b'-')
            {
                has_exp = true;
                j += 2;
            } else {
                break;
            }
        }
        // If the next char is a duration unit and we saw no '.'/exp, lex a duration.
        let next = self.src.get(j).copied();
        let looks_duration = !has_dot
            && !has_exp
            && matches!(
                next,
                Some(b's') | Some(b'm') | Some(b'h') | Some(b'd') | Some(b'w') | Some(b'y')
            );
        if looks_duration {
            return self.lex_duration();
        }
        self.pos = j;
        let raw = std::str::from_utf8(&self.src[start..self.pos]).unwrap_or("");
        raw.parse::<f64>()
            .map(Tok::Number)
            .map_err(|_| PromqlError(format!("invalid number '{raw}'")))
    }

    fn lex_duration(&mut self) -> Result<Tok, PromqlError> {
        let mut total: i64 = 0;
        let mut any = false;
        while let Some(c) = self.peek() {
            if !c.is_ascii_digit() {
                break;
            }
            let ds = self.pos;
            while self.peek().is_some_and(|d| d.is_ascii_digit()) {
                self.pos += 1;
            }
            let n: i64 = std::str::from_utf8(&self.src[ds..self.pos])
                .unwrap_or("0")
                .parse()
                .map_err(|_| PromqlError("invalid duration magnitude".into()))?;
            // unit: check `ms` before `m`
            let unit_ns: i64 =
                if self.peek() == Some(b'm') && self.src.get(self.pos + 1) == Some(&b's') {
                    self.pos += 2;
                    1_000_000
                } else {
                    let u = self.peek();
                    self.pos += 1;
                    match u {
                        Some(b's') => 1_000_000_000,
                        Some(b'm') => 60 * 1_000_000_000,
                        Some(b'h') => 3_600 * 1_000_000_000,
                        Some(b'd') => 86_400 * 1_000_000_000,
                        Some(b'w') => 7 * 86_400 * 1_000_000_000,
                        Some(b'y') => 365 * 86_400 * 1_000_000_000,
                        other => {
                            return err(format!(
                                "invalid duration unit '{:?}'",
                                other.map(|b| b as char)
                            ))
                        }
                    }
                };
            total += n * unit_ns;
            any = true;
        }
        if !any {
            return err("empty duration");
        }
        Ok(Tok::Duration(total))
    }
}

// ───────────────────────────── parser ─────────────────────────────

struct Parser {
    toks: Vec<Tok>,
    pos: usize,
}

/// Parse a PromQL expression string into an [`Expr`].
pub fn parse(input: &str) -> Result<Expr, PromqlError> {
    let toks = Lexer::new(input).tokenize()?;
    let mut p = Parser { toks, pos: 0 };
    let e = p.parse_expr()?;
    if p.pos != p.toks.len() {
        return err(format!(
            "trailing tokens after expression (at token {})",
            p.pos
        ));
    }
    Ok(e)
}

impl Parser {
    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.pos)
    }
    fn bump(&mut self) -> Option<Tok> {
        let t = self.toks.get(self.pos).cloned();
        if t.is_some() {
            self.pos += 1;
        }
        t
    }
    fn eat(&mut self, t: &Tok) -> bool {
        if self.peek() == Some(t) {
            self.pos += 1;
            true
        } else {
            false
        }
    }
    fn expect(&mut self, t: &Tok) -> Result<(), PromqlError> {
        if self.eat(t) {
            Ok(())
        } else {
            err(format!("expected {:?} at token {}", t, self.pos))
        }
    }
    fn peek_ident(&self) -> Option<&str> {
        match self.peek() {
            Some(Tok::Ident(s)) => Some(s.as_str()),
            _ => None,
        }
    }

    // expr := or_expr
    fn parse_expr(&mut self) -> Result<Expr, PromqlError> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<Expr, PromqlError> {
        let mut lhs = self.parse_and()?;
        while self.peek_ident() == Some("or") {
            self.pos += 1;
            let matching = self.parse_matching()?;
            let rhs = self.parse_and()?;
            lhs = Expr::Binary {
                op: BinOp::Or,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
                bool_modifier: false,
                matching,
            };
        }
        Ok(lhs)
    }

    fn parse_and(&mut self) -> Result<Expr, PromqlError> {
        let mut lhs = self.parse_cmp()?;
        loop {
            let op = match self.peek_ident() {
                Some("and") => BinOp::And,
                Some("unless") => BinOp::Unless,
                _ => break,
            };
            self.pos += 1;
            let matching = self.parse_matching()?;
            let rhs = self.parse_cmp()?;
            lhs = Expr::Binary {
                op,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
                bool_modifier: false,
                matching,
            };
        }
        Ok(lhs)
    }

    fn parse_cmp(&mut self) -> Result<Expr, PromqlError> {
        let mut lhs = self.parse_add()?;
        loop {
            let op = match self.peek() {
                Some(Tok::EqEq) => BinOp::Eq,
                Some(Tok::Ne) => BinOp::Ne,
                Some(Tok::Gt) => BinOp::Gt,
                Some(Tok::Lt) => BinOp::Lt,
                Some(Tok::Ge) => BinOp::Ge,
                Some(Tok::Le) => BinOp::Le,
                _ => break,
            };
            self.pos += 1;
            let bool_modifier = self.peek_ident() == Some("bool");
            if bool_modifier {
                self.pos += 1;
            }
            let matching = self.parse_matching()?;
            let rhs = self.parse_add()?;
            lhs = Expr::Binary {
                op,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
                bool_modifier,
                matching,
            };
        }
        Ok(lhs)
    }

    fn parse_add(&mut self) -> Result<Expr, PromqlError> {
        let mut lhs = self.parse_mul()?;
        loop {
            let op = match self.peek() {
                Some(Tok::Plus) => BinOp::Add,
                Some(Tok::Minus) => BinOp::Sub,
                _ => break,
            };
            self.pos += 1;
            let matching = self.parse_matching()?;
            let rhs = self.parse_mul()?;
            lhs = Expr::Binary {
                op,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
                bool_modifier: false,
                matching,
            };
        }
        Ok(lhs)
    }

    fn parse_mul(&mut self) -> Result<Expr, PromqlError> {
        let mut lhs = self.parse_pow()?;
        loop {
            let op = match self.peek() {
                Some(Tok::Star) => BinOp::Mul,
                Some(Tok::Slash) => BinOp::Div,
                Some(Tok::Percent) => BinOp::Mod,
                _ => break,
            };
            self.pos += 1;
            let matching = self.parse_matching()?;
            let rhs = self.parse_pow()?;
            lhs = Expr::Binary {
                op,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
                bool_modifier: false,
                matching,
            };
        }
        Ok(lhs)
    }

    // `^` is right-associative and binds tighter than `*`.
    fn parse_pow(&mut self) -> Result<Expr, PromqlError> {
        let lhs = self.parse_unary()?;
        if self.eat(&Tok::Caret) {
            let matching = self.parse_matching()?;
            let rhs = self.parse_pow()?; // right-assoc
            Ok(Expr::Binary {
                op: BinOp::Pow,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
                bool_modifier: false,
                matching,
            })
        } else {
            Ok(lhs)
        }
    }

    fn parse_unary(&mut self) -> Result<Expr, PromqlError> {
        if self.eat(&Tok::Minus) {
            let e = self.parse_unary()?;
            return Ok(Expr::Neg(Box::new(e)));
        }
        if self.eat(&Tok::Plus) {
            return self.parse_unary();
        }
        self.parse_atom()
    }

    // Optional `on(labels)` / `ignoring(labels)` after a binary operator.
    fn parse_matching(&mut self) -> Result<Option<VectorMatching>, PromqlError> {
        let on = match self.peek_ident() {
            Some("on") => true,
            Some("ignoring") => false,
            _ => return Ok(None),
        };
        self.pos += 1;
        self.expect(&Tok::LParen)?;
        let labels = self.parse_label_list()?;
        self.expect(&Tok::RParen)?;
        // group_left/group_right are parsed-and-rejected (documented follow-up).
        if matches!(self.peek_ident(), Some("group_left") | Some("group_right")) {
            return err(
                "group_left/group_right (many-to-one) not yet supported (EG-172 follow-up)",
            );
        }
        Ok(Some(VectorMatching { on, labels }))
    }

    fn parse_label_list(&mut self) -> Result<Vec<String>, PromqlError> {
        let mut labels = Vec::new();
        if self.peek() == Some(&Tok::RParen) {
            return Ok(labels);
        }
        loop {
            match self.bump() {
                Some(Tok::Ident(s)) => labels.push(s),
                other => return err(format!("expected label name, got {other:?}")),
            }
            if !self.eat(&Tok::Comma) {
                break;
            }
        }
        Ok(labels)
    }

    fn parse_atom(&mut self) -> Result<Expr, PromqlError> {
        match self.peek().cloned() {
            Some(Tok::Number(n)) => {
                self.pos += 1;
                Ok(Expr::Number(n))
            }
            Some(Tok::Str(s)) => {
                self.pos += 1;
                Ok(Expr::Str(s))
            }
            Some(Tok::LParen) => {
                self.pos += 1;
                let e = self.parse_expr()?;
                self.expect(&Tok::RParen)?;
                Ok(e)
            }
            Some(Tok::LBrace) => {
                // Selector with no metric name: `{__name__="x", ...}`.
                let matchers = self.parse_matchers(None)?;
                self.finish_selector(matchers)
            }
            Some(Tok::Ident(name)) => {
                self.pos += 1;
                // Aggregation operator?
                if let Some(op) = agg_op(&name) {
                    return self.parse_aggregate(op);
                }
                // Function call?
                if self.peek() == Some(&Tok::LParen) {
                    self.pos += 1;
                    let mut args = Vec::new();
                    if self.peek() != Some(&Tok::RParen) {
                        loop {
                            args.push(self.parse_expr()?);
                            if !self.eat(&Tok::Comma) {
                                break;
                            }
                        }
                    }
                    self.expect(&Tok::RParen)?;
                    return Ok(Expr::Call { func: name, args });
                }
                // Bare metric selector, optionally with `{...}`.
                let matchers = self.parse_matchers(Some(name))?;
                self.finish_selector(matchers)
            }
            other => err(format!("unexpected token {other:?} at atom position")),
        }
    }

    // Aggregation: `OP` [by/without(labels)] `(` expr `)`  OR  `OP (` expr `)` [by/without(labels)]
    fn parse_aggregate(&mut self, op: AggOp) -> Result<Expr, PromqlError> {
        let mut grouping = self.parse_grouping()?;
        self.expect(&Tok::LParen)?;
        let expr = self.parse_expr()?;
        self.expect(&Tok::RParen)?;
        if grouping.is_none() {
            grouping = self.parse_grouping()?;
        }
        let (by, labels) = grouping.unwrap_or((true, Vec::new()));
        Ok(Expr::Aggregate {
            op,
            expr: Box::new(expr),
            by,
            labels,
        })
    }

    fn parse_grouping(&mut self) -> Result<Option<(bool, Vec<String>)>, PromqlError> {
        let by = match self.peek_ident() {
            Some("by") => true,
            Some("without") => false,
            _ => return Ok(None),
        };
        self.pos += 1;
        self.expect(&Tok::LParen)?;
        let labels = self.parse_label_list()?;
        self.expect(&Tok::RParen)?;
        Ok(Some((by, labels)))
    }

    // Parse the `{...}` matcher block (may be absent) plus fold in the metric name.
    fn parse_matchers(&mut self, metric: Option<String>) -> Result<Vec<LabelMatcher>, PromqlError> {
        let mut matchers = Vec::new();
        if let Some(name) = metric {
            matchers.push(LabelMatcher::new(METRIC_NAME, MatchOp::Eq, name)?);
        }
        if self.eat(&Tok::LBrace) {
            if self.peek() != Some(&Tok::RBrace) {
                loop {
                    let lname = match self.bump() {
                        Some(Tok::Ident(s)) => s,
                        other => {
                            return err(format!("expected label name in matcher, got {other:?}"))
                        }
                    };
                    let op = match self.bump() {
                        Some(Tok::Eq) => MatchOp::Eq,
                        Some(Tok::Ne) => MatchOp::Ne,
                        Some(Tok::Re) => MatchOp::Re,
                        Some(Tok::Nre) => MatchOp::Nre,
                        other => return err(format!("expected match operator, got {other:?}")),
                    };
                    let val = match self.bump() {
                        Some(Tok::Str(s)) => s,
                        other => return err(format!("expected string in matcher, got {other:?}")),
                    };
                    matchers.push(LabelMatcher::new(lname, op, val)?);
                    if !self.eat(&Tok::Comma) {
                        break;
                    }
                }
            }
            self.expect(&Tok::RBrace)?;
        }
        if matchers.is_empty() {
            return err("selector requires a metric name or at least one matcher");
        }
        Ok(matchers)
    }

    // After matchers, optionally `[range]` (→ Matrix) and `offset <dur>`.
    fn finish_selector(&mut self, matchers: Vec<LabelMatcher>) -> Result<Expr, PromqlError> {
        let mut range_ns: Option<i64> = None;
        if self.eat(&Tok::LBracket) {
            match self.bump() {
                Some(Tok::Duration(d)) => range_ns = Some(d),
                other => {
                    return err(format!(
                        "expected duration in range selector, got {other:?}"
                    ))
                }
            }
            self.expect(&Tok::RBracket)?;
        }
        let mut offset_ns = 0i64;
        if self.peek_ident() == Some("offset") {
            self.pos += 1;
            match self.bump() {
                Some(Tok::Duration(d)) => offset_ns = d,
                other => return err(format!("expected duration after offset, got {other:?}")),
            }
        }
        match range_ns {
            Some(range_ns) => Ok(Expr::Matrix {
                matchers,
                range_ns,
                offset_ns,
            }),
            None => Ok(Expr::Selector {
                matchers,
                offset_ns,
            }),
        }
    }
}

fn agg_op(name: &str) -> Option<AggOp> {
    match name {
        "sum" => Some(AggOp::Sum),
        "avg" => Some(AggOp::Avg),
        "min" => Some(AggOp::Min),
        "max" => Some(AggOp::Max),
        "count" => Some(AggOp::Count),
        _ => None,
    }
}

// ───────────────────────────── values ─────────────────────────────

/// One instant-vector sample: labels + a value (evaluated at the query instant).
#[derive(Debug, Clone, PartialEq)]
pub struct InstantSample {
    pub labels: Labels,
    pub value: f64,
}

/// One range-vector series: labels + `(ts, value)` points within the range window.
#[derive(Debug, Clone, PartialEq)]
pub struct RangeSeries {
    pub labels: Labels,
    pub points: Vec<(Ts, f64)>,
}

/// A PromQL value.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Scalar(f64),
    Instant(Vec<InstantSample>),
    Range(Vec<RangeSeries>),
}

impl Value {
    /// Prometheus `resultType` string.
    pub fn result_type(&self) -> &'static str {
        match self {
            Value::Scalar(_) => "scalar",
            Value::Instant(_) => "vector",
            Value::Range(_) => "matrix",
        }
    }
}

// ───────────────────────────── evaluator ─────────────────────────────

/// The evaluator, bound to a [`SeriesSource`] with a configurable lookback.
pub struct Evaluator<'a> {
    source: &'a dyn SeriesSource,
    lookback_ns: i64,
}

impl<'a> Evaluator<'a> {
    pub fn new(source: &'a dyn SeriesSource) -> Self {
        Self {
            source,
            lookback_ns: DEFAULT_LOOKBACK_NS,
        }
    }

    pub fn with_lookback(source: &'a dyn SeriesSource, lookback_ns: i64) -> Self {
        Self {
            source,
            lookback_ns,
        }
    }

    /// Evaluate `expr` at instant `t` (epoch-ns).
    pub fn eval_instant(&self, expr: &Expr, t: Ts) -> Result<Value, PromqlError> {
        match expr {
            Expr::Number(n) => Ok(Value::Scalar(*n)),
            Expr::Str(_) => err("a string literal is not a valid top-level expression"),
            Expr::Neg(inner) => match self.eval_instant(inner, t)? {
                Value::Scalar(s) => Ok(Value::Scalar(-s)),
                Value::Instant(v) => Ok(Value::Instant(map_values(v, |x| -x))),
                Value::Range(_) => err("unary '-' not defined on a range vector"),
            },
            Expr::Selector {
                matchers,
                offset_ns,
            } => {
                let at = t - offset_ns;
                let series = self.source.select(matchers, at - self.lookback_ns, at);
                let mut out = Vec::new();
                for s in series {
                    // The instant value = the most recent point at-or-before `at`
                    // within the lookback window.
                    if let Some(&(_, v)) = s.points.iter().rev().find(|(ts, _)| *ts <= at) {
                        out.push(InstantSample {
                            labels: s.labels,
                            value: v,
                        });
                    }
                }
                Ok(Value::Instant(out))
            }
            Expr::Matrix {
                matchers,
                range_ns,
                offset_ns,
            } => {
                let at = t - offset_ns;
                let lo = at - range_ns;
                let series = self.source.select(matchers, lo, at);
                let out = series
                    .into_iter()
                    .map(|s| RangeSeries {
                        labels: s.labels,
                        // Prometheus range window is (lo, at].
                        points: s
                            .points
                            .into_iter()
                            .filter(|(ts, _)| *ts > lo && *ts <= at)
                            .collect(),
                    })
                    .filter(|s| !s.points.is_empty())
                    .collect();
                Ok(Value::Range(out))
            }
            Expr::Call { func, args } => self.eval_call(func, args, t),
            Expr::Aggregate {
                op,
                expr,
                by,
                labels,
            } => {
                let v = self.eval_instant(expr, t)?;
                let samples = as_instant(v, "aggregation operand")?;
                Ok(Value::Instant(aggregate(*op, samples, *by, labels)))
            }
            Expr::Binary {
                op,
                lhs,
                rhs,
                bool_modifier,
                matching,
            } => {
                let l = self.eval_instant(lhs, t)?;
                let r = self.eval_instant(rhs, t)?;
                eval_binary(*op, l, r, *bool_modifier, matching.as_ref())
            }
        }
    }

    fn eval_call(&self, func: &str, args: &[Expr], t: Ts) -> Result<Value, PromqlError> {
        match func {
            "rate" | "irate" | "increase" => {
                let arg = one_arg(func, args)?;
                let rv = self.eval_instant(arg, t)?;
                let series = as_range(rv, func)?;
                Ok(Value::Instant(rate_family(func, series)))
            }
            "abs" | "ceil" | "floor" => {
                let arg = one_arg(func, args)?;
                let iv = self.eval_instant(arg, t)?;
                let samples = as_instant(iv, func)?;
                let f: fn(f64) -> f64 = match func {
                    "abs" => f64::abs,
                    "ceil" => f64::ceil,
                    _ => f64::floor,
                };
                // scalar functions drop the metric name
                Ok(Value::Instant(map_values(strip_name(samples), f)))
            }
            "histogram_quantile" => {
                if args.len() != 2 {
                    return err("histogram_quantile expects (phi, vector)");
                }
                let phi = match self.eval_instant(&args[0], t)? {
                    Value::Scalar(s) => s,
                    _ => return err("histogram_quantile: first arg must be a scalar"),
                };
                let iv = self.eval_instant(&args[1], t)?;
                let samples = as_instant(iv, "histogram_quantile")?;
                Ok(Value::Instant(histogram_quantile(phi, samples)))
            }
            "scalar" => {
                let arg = one_arg(func, args)?;
                let iv = self.eval_instant(arg, t)?;
                let samples = as_instant(iv, "scalar")?;
                Ok(Value::Scalar(if samples.len() == 1 {
                    samples[0].value
                } else {
                    f64::NAN
                }))
            }
            other => err(format!("unsupported function '{other}' (EG-172 follow-up)")),
        }
    }

    /// Evaluate `expr` over `[start, end]` at `step` (all ns) — the range query. Returns
    /// a matrix: one series per distinct label set, with a point at each step where the
    /// instant evaluation produced a value.
    pub fn eval_range(
        &self,
        expr: &Expr,
        start: Ts,
        end: Ts,
        step: Ts,
    ) -> Result<Vec<RangeSeries>, PromqlError> {
        if step <= 0 {
            return err("step must be positive");
        }
        if end < start {
            return err("end must be >= start");
        }
        // Preserve first-seen label order for stable output.
        let mut order: Vec<Labels> = Vec::new();
        let mut acc: BTreeMap<String, (usize, Vec<(Ts, f64)>)> = BTreeMap::new();
        let mut t = start;
        loop {
            match self.eval_instant(expr, t)? {
                Value::Scalar(s) => {
                    let key = String::new(); // scalar → one empty-label series
                    let entry = acc.entry(key).or_insert_with(|| {
                        order.push(Labels::new());
                        (order.len() - 1, Vec::new())
                    });
                    entry.1.push((t, s));
                }
                Value::Instant(samples) => {
                    for s in samples {
                        let key = label_key(&s.labels);
                        let labels = s.labels.clone();
                        let entry = acc.entry(key).or_insert_with(|| {
                            order.push(labels);
                            (order.len() - 1, Vec::new())
                        });
                        entry.1.push((t, s.value));
                    }
                }
                Value::Range(_) => return err("a range vector cannot be range-queried directly; wrap it in a function like rate()"),
            }
            if t == end {
                break;
            }
            t = (t + step).min(end);
        }
        // Reassemble in first-seen order.
        let mut by_idx: Vec<Option<Vec<(Ts, f64)>>> = vec![None; order.len()];
        for (_k, (idx, pts)) in acc {
            by_idx[idx] = Some(pts);
        }
        Ok(order
            .into_iter()
            .zip(by_idx)
            .filter_map(|(labels, pts)| pts.map(|points| RangeSeries { labels, points }))
            .collect())
    }
}

// ───────────────────────────── helpers: value coercion ─────────────────────────────

fn one_arg<'a>(func: &str, args: &'a [Expr]) -> Result<&'a Expr, PromqlError> {
    if args.len() != 1 {
        return err(format!("{func} expects exactly one argument"));
    }
    Ok(&args[0])
}

fn as_instant(v: Value, ctx: &str) -> Result<Vec<InstantSample>, PromqlError> {
    match v {
        Value::Instant(s) => Ok(s),
        Value::Scalar(_) => err(format!("{ctx}: expected an instant vector, got a scalar")),
        Value::Range(_) => err(format!(
            "{ctx}: expected an instant vector, got a range vector"
        )),
    }
}

fn as_range(v: Value, ctx: &str) -> Result<Vec<RangeSeries>, PromqlError> {
    match v {
        Value::Range(s) => Ok(s),
        _ => err(format!(
            "{ctx}: expected a range vector (use metric[<dur>])"
        )),
    }
}

fn map_values(mut samples: Vec<InstantSample>, f: impl Fn(f64) -> f64) -> Vec<InstantSample> {
    for s in &mut samples {
        s.value = f(s.value);
    }
    samples
}

fn strip_name(mut samples: Vec<InstantSample>) -> Vec<InstantSample> {
    for s in &mut samples {
        s.labels.remove(METRIC_NAME);
    }
    samples
}

/// A stable string key for a label set (used to group samples).
fn label_key(labels: &Labels) -> String {
    let mut s = String::new();
    for (k, v) in labels {
        s.push_str(k);
        s.push('\u{1}');
        s.push_str(v);
        s.push('\u{2}');
    }
    s
}

// ───────────────────────────── rate / irate / increase ─────────────────────────────

/// Counter-reset-aware total increase over ts-sorted points: whenever a value drops
/// below its predecessor the counter is assumed to have reset, so the new value is the
/// increment from zero.
fn counter_increase(points: &[(Ts, f64)]) -> f64 {
    let mut total = 0.0;
    for w in points.windows(2) {
        let (_, prev) = w[0];
        let (_, cur) = w[1];
        if cur >= prev {
            total += cur - prev;
        } else {
            total += cur; // reset
        }
    }
    total
}

fn rate_family(func: &str, series: Vec<RangeSeries>) -> Vec<InstantSample> {
    let mut out = Vec::new();
    for s in series {
        if s.points.len() < 2 {
            continue; // need two points to compute a rate
        }
        let mut labels = s.labels.clone();
        labels.remove(METRIC_NAME);
        let value = match func {
            "increase" => counter_increase(&s.points),
            "rate" => {
                let span_s =
                    (s.points.last().unwrap().0 - s.points.first().unwrap().0) as f64 / NS_PER_SEC;
                if span_s <= 0.0 {
                    continue;
                }
                counter_increase(&s.points) / span_s
            }
            // irate: instantaneous rate from the last two points.
            _ => {
                let n = s.points.len();
                let (t0, v0) = s.points[n - 2];
                let (t1, v1) = s.points[n - 1];
                let span_s = (t1 - t0) as f64 / NS_PER_SEC;
                if span_s <= 0.0 {
                    continue;
                }
                let delta = if v1 >= v0 { v1 - v0 } else { v1 };
                delta / span_s
            }
        };
        out.push(InstantSample { labels, value });
    }
    out
}

// ───────────────────────────── aggregation ─────────────────────────────

fn aggregate(
    op: AggOp,
    samples: Vec<InstantSample>,
    by: bool,
    labels: &[String],
) -> Vec<InstantSample> {
    // Group key: the retained labels (drop __name__ always).
    let mut order: Vec<Labels> = Vec::new();
    let mut groups: BTreeMap<String, (usize, Vec<f64>)> = BTreeMap::new();
    for s in samples {
        let mut key_labels = Labels::new();
        for (k, v) in &s.labels {
            if k == METRIC_NAME {
                continue;
            }
            let keep = if by {
                labels.iter().any(|l| l == k)
            } else {
                !labels.iter().any(|l| l == k)
            };
            if keep {
                key_labels.insert(k.clone(), v.clone());
            }
        }
        let key = label_key(&key_labels);
        let entry = groups.entry(key).or_insert_with(|| {
            order.push(key_labels);
            (order.len() - 1, Vec::new())
        });
        entry.1.push(s.value);
    }
    let mut reduced: Vec<Option<f64>> = vec![None; order.len()];
    for (_k, (idx, vals)) in groups {
        reduced[idx] = Some(reduce(op, &vals));
    }
    order
        .into_iter()
        .zip(reduced)
        .filter_map(|(labels, v)| v.map(|value| InstantSample { labels, value }))
        .collect()
}

fn reduce(op: AggOp, vals: &[f64]) -> f64 {
    match op {
        AggOp::Sum => vals.iter().sum(),
        AggOp::Count => vals.len() as f64,
        AggOp::Min => vals.iter().copied().fold(f64::INFINITY, f64::min),
        AggOp::Max => vals.iter().copied().fold(f64::NEG_INFINITY, f64::max),
        AggOp::Avg => {
            if vals.is_empty() {
                f64::NAN
            } else {
                vals.iter().sum::<f64>() / vals.len() as f64
            }
        }
    }
}

// ───────────────────────────── histogram_quantile ─────────────────────────────

/// `histogram_quantile(phi, buckets)` — classic Prometheus algorithm over cumulative
/// bucket counts. Buckets are grouped by all labels except `le`; within a group the
/// (le, count) pairs are sorted and the phi-quantile is linearly interpolated between
/// bucket boundaries. Requires a `+Inf` bucket; a group without one yields NaN.
fn histogram_quantile(phi: f64, samples: Vec<InstantSample>) -> Vec<InstantSample> {
    // Group by labels sans `le`.
    let mut order: Vec<Labels> = Vec::new();
    let mut groups: BTreeMap<String, (usize, Vec<(f64, f64)>)> = BTreeMap::new();
    for s in samples {
        let le = match s.labels.get("le").and_then(|v| parse_le(v)) {
            Some(le) => le,
            None => continue, // not a bucket sample
        };
        let mut key_labels = s.labels.clone();
        key_labels.remove("le");
        key_labels.remove(METRIC_NAME);
        let key = label_key(&key_labels);
        let entry = groups.entry(key).or_insert_with(|| {
            order.push(key_labels);
            (order.len() - 1, Vec::new())
        });
        entry.1.push((le, s.value));
    }
    let mut results: Vec<Option<f64>> = vec![None; order.len()];
    for (_k, (idx, mut buckets)) in groups {
        buckets.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        results[idx] = Some(quantile_from_buckets(phi, &buckets));
    }
    order
        .into_iter()
        .zip(results)
        .filter_map(|(labels, v)| v.map(|value| InstantSample { labels, value }))
        .collect()
}

fn parse_le(v: &str) -> Option<f64> {
    match v {
        "+Inf" | "Inf" | "inf" => Some(f64::INFINITY),
        other => other.parse::<f64>().ok(),
    }
}

fn quantile_from_buckets(phi: f64, buckets: &[(f64, f64)]) -> f64 {
    if buckets.is_empty() {
        return f64::NAN;
    }
    if phi < 0.0 {
        return f64::NEG_INFINITY;
    }
    if phi > 1.0 {
        return f64::INFINITY;
    }
    // The last bucket must be +Inf (total count).
    let last = buckets[buckets.len() - 1];
    if !last.0.is_infinite() {
        return f64::NAN;
    }
    let total = last.1;
    if total == 0.0 {
        return f64::NAN;
    }
    let rank = phi * total;
    // Find the first bucket whose cumulative count >= rank.
    let mut b = 0usize;
    while b < buckets.len() && buckets[b].1 < rank {
        b += 1;
    }
    if b == buckets.len() {
        return last.0;
    }
    if b == buckets.len() - 1 && buckets[b].0.is_infinite() {
        // rank falls in the +Inf bucket → return the highest finite bound.
        return if buckets.len() >= 2 {
            buckets[buckets.len() - 2].0
        } else {
            f64::INFINITY
        };
    }
    let bucket_end = buckets[b].0;
    let (bucket_start, count_before) = if b == 0 {
        (0.0, 0.0)
    } else {
        (buckets[b - 1].0, buckets[b - 1].1)
    };
    let count_in_bucket = buckets[b].1 - count_before;
    if count_in_bucket <= 0.0 {
        return bucket_end;
    }
    let start = if bucket_start.is_infinite() {
        0.0
    } else {
        bucket_start
    };
    start + (bucket_end - start) * ((rank - count_before) / count_in_bucket)
}

// ───────────────────────────── binary ops ─────────────────────────────

fn apply_arith(op: BinOp, l: f64, r: f64) -> f64 {
    match op {
        BinOp::Add => l + r,
        BinOp::Sub => l - r,
        BinOp::Mul => l * r,
        BinOp::Div => l / r,
        BinOp::Mod => l % r,
        BinOp::Pow => l.powf(r),
        _ => f64::NAN,
    }
}

fn apply_cmp(op: BinOp, l: f64, r: f64) -> bool {
    match op {
        BinOp::Eq => l == r,
        BinOp::Ne => l != r,
        BinOp::Gt => l > r,
        BinOp::Lt => l < r,
        BinOp::Ge => l >= r,
        BinOp::Le => l <= r,
        _ => false,
    }
}

fn eval_binary(
    op: BinOp,
    l: Value,
    r: Value,
    bool_modifier: bool,
    matching: Option<&VectorMatching>,
) -> Result<Value, PromqlError> {
    use Value::*;
    match (l, r) {
        (Scalar(a), Scalar(b)) => {
            if op.is_comparison() {
                let res = apply_cmp(op, a, b);
                // scalar-scalar comparison always yields a scalar (bool by default).
                Ok(Scalar(if res { 1.0 } else { 0.0 }))
            } else if op.is_setop() {
                err("set operators (and/or/unless) require vector operands")
            } else {
                Ok(Scalar(apply_arith(op, a, b)))
            }
        }
        (Instant(v), Scalar(s)) => Ok(Instant(vector_scalar(op, v, s, bool_modifier, false))),
        (Scalar(s), Instant(v)) => Ok(Instant(vector_scalar(op, v, s, bool_modifier, true))),
        (Instant(a), Instant(b)) => vector_vector(op, a, b, bool_modifier, matching),
        _ => err("binary operators are not defined on range vectors"),
    }
}

/// vector `op` scalar (or scalar `op` vector when `scalar_lhs`).
fn vector_scalar(
    op: BinOp,
    samples: Vec<InstantSample>,
    s: f64,
    bool_modifier: bool,
    scalar_lhs: bool,
) -> Vec<InstantSample> {
    let mut out = Vec::new();
    for mut sample in samples {
        let (l, r) = if scalar_lhs {
            (s, sample.value)
        } else {
            (sample.value, s)
        };
        if op.is_comparison() {
            let keep = apply_cmp(op, l, r);
            if bool_modifier {
                sample.value = if keep { 1.0 } else { 0.0 };
                sample.labels.remove(METRIC_NAME);
                out.push(sample);
            } else if keep {
                // filter: keep the original sample value + labels (metric name kept).
                out.push(sample);
            }
        } else {
            sample.value = apply_arith(op, l, r);
            sample.labels.remove(METRIC_NAME);
            out.push(sample);
        }
    }
    out
}

/// The match key for vector↔vector matching under an optional on/ignoring modifier.
fn match_labels(labels: &Labels, matching: Option<&VectorMatching>) -> Labels {
    let mut out = Labels::new();
    match matching {
        Some(m) if m.on => {
            for l in &m.labels {
                if let Some(v) = labels.get(l) {
                    out.insert(l.clone(), v.clone());
                }
            }
        }
        Some(m) => {
            // ignoring
            for (k, v) in labels {
                if k == METRIC_NAME || m.labels.iter().any(|l| l == k) {
                    continue;
                }
                out.insert(k.clone(), v.clone());
            }
        }
        None => {
            for (k, v) in labels {
                if k == METRIC_NAME {
                    continue;
                }
                out.insert(k.clone(), v.clone());
            }
        }
    }
    out
}

fn vector_vector(
    op: BinOp,
    lhs: Vec<InstantSample>,
    rhs: Vec<InstantSample>,
    bool_modifier: bool,
    matching: Option<&VectorMatching>,
) -> Result<Value, PromqlError> {
    // Set operators.
    match op {
        BinOp::And => {
            let keys: std::collections::HashSet<String> = rhs
                .iter()
                .map(|s| label_key(&match_labels(&s.labels, matching)))
                .collect();
            return Ok(Value::Instant(
                lhs.into_iter()
                    .filter(|s| keys.contains(&label_key(&match_labels(&s.labels, matching))))
                    .collect(),
            ));
        }
        BinOp::Unless => {
            let keys: std::collections::HashSet<String> = rhs
                .iter()
                .map(|s| label_key(&match_labels(&s.labels, matching)))
                .collect();
            return Ok(Value::Instant(
                lhs.into_iter()
                    .filter(|s| !keys.contains(&label_key(&match_labels(&s.labels, matching))))
                    .collect(),
            ));
        }
        BinOp::Or => {
            let lkeys: std::collections::HashSet<String> = lhs
                .iter()
                .map(|s| label_key(&match_labels(&s.labels, matching)))
                .collect();
            let mut out = lhs.clone();
            for s in rhs {
                if !lkeys.contains(&label_key(&match_labels(&s.labels, matching))) {
                    out.push(s);
                }
            }
            return Ok(Value::Instant(out));
        }
        _ => {}
    }

    // Arithmetic / comparison: one-to-one match on the (on/ignoring) key.
    let mut rhs_by: BTreeMap<String, &InstantSample> = BTreeMap::new();
    for s in &rhs {
        rhs_by.insert(label_key(&match_labels(&s.labels, matching)), s);
    }
    let mut out = Vec::new();
    for lsample in &lhs {
        let key = label_key(&match_labels(&lsample.labels, matching));
        let Some(rsample) = rhs_by.get(&key) else {
            continue;
        };
        let (l, r) = (lsample.value, rsample.value);
        // The result label set is the match key (Prometheus drops __name__ and, under
        // ignoring, only the shared/kept labels survive).
        let result_labels = match_labels(&lsample.labels, matching);
        if op.is_comparison() {
            let keep = apply_cmp(op, l, r);
            if bool_modifier {
                out.push(InstantSample {
                    labels: result_labels,
                    value: if keep { 1.0 } else { 0.0 },
                });
            } else if keep {
                out.push(InstantSample {
                    labels: result_labels,
                    value: l,
                });
            }
        } else {
            out.push(InstantSample {
                labels: result_labels,
                value: apply_arith(op, l, r),
            });
        }
    }
    Ok(Value::Instant(out))
}

// ───────────────────────────── anchored regex (=~ / !~) ─────────────────────────────

/// A tiny hand-rolled, fully-anchored regex matcher for label `=~`/`!~` (Prometheus
/// anchors label regexes at both ends). Supports literals, `.`, char classes `[...]`
/// (ranges + `^` negation), groups `(...)`, alternation `|`, greedy quantifiers
/// `* + ?`, and escapes incl. `\d \w \s` (and their negations). NO dependency.
#[derive(Debug, Clone)]
struct Regex {
    root: ReNode,
}

#[derive(Debug, Clone)]
enum ReNode {
    Empty,
    Char(char),
    AnyChar,
    Class {
        neg: bool,
        ranges: Vec<(char, char)>,
    },
    Concat(Vec<ReNode>),
    Alt(Vec<ReNode>),
    Star(Box<ReNode>),
    Plus(Box<ReNode>),
    Opt(Box<ReNode>),
}

impl Regex {
    fn compile(pat: &str) -> Result<Regex, PromqlError> {
        let chars: Vec<char> = pat.chars().collect();
        let mut rp = ReParser {
            chars: &chars,
            pos: 0,
        };
        let root = rp.parse_alt()?;
        if rp.pos != rp.chars.len() {
            return err(format!(
                "invalid regex '{pat}': unexpected char at {}",
                rp.pos
            ));
        }
        Ok(Regex { root })
    }

    fn is_full_match(&self, s: &str) -> bool {
        let input: Vec<char> = s.chars().collect();
        re_match(&self.root, &input, 0, &|p| p == input.len())
    }
}

struct ReParser<'a> {
    chars: &'a [char],
    pos: usize,
}

impl<'a> ReParser<'a> {
    fn peek(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }

    // alt := concat ('|' concat)*
    fn parse_alt(&mut self) -> Result<ReNode, PromqlError> {
        let mut branches = vec![self.parse_concat()?];
        while self.peek() == Some('|') {
            self.pos += 1;
            branches.push(self.parse_concat()?);
        }
        if branches.len() == 1 {
            Ok(branches.pop().unwrap())
        } else {
            Ok(ReNode::Alt(branches))
        }
    }

    // concat := quantified*
    fn parse_concat(&mut self) -> Result<ReNode, PromqlError> {
        let mut parts = Vec::new();
        while let Some(c) = self.peek() {
            if c == '|' || c == ')' {
                break;
            }
            parts.push(self.parse_quantified()?);
        }
        if parts.is_empty() {
            Ok(ReNode::Empty)
        } else if parts.len() == 1 {
            Ok(parts.pop().unwrap())
        } else {
            Ok(ReNode::Concat(parts))
        }
    }

    fn parse_quantified(&mut self) -> Result<ReNode, PromqlError> {
        let atom = self.parse_atom()?;
        match self.peek() {
            Some('*') => {
                self.pos += 1;
                Ok(ReNode::Star(Box::new(atom)))
            }
            Some('+') => {
                self.pos += 1;
                Ok(ReNode::Plus(Box::new(atom)))
            }
            Some('?') => {
                self.pos += 1;
                Ok(ReNode::Opt(Box::new(atom)))
            }
            _ => Ok(atom),
        }
    }

    fn parse_atom(&mut self) -> Result<ReNode, PromqlError> {
        match self.peek() {
            Some('(') => {
                self.pos += 1;
                let inner = self.parse_alt()?;
                if self.peek() != Some(')') {
                    return err("unbalanced '(' in regex");
                }
                self.pos += 1;
                Ok(inner)
            }
            Some('[') => self.parse_class(),
            Some('.') => {
                self.pos += 1;
                Ok(ReNode::AnyChar)
            }
            Some('\\') => {
                self.pos += 1;
                let c = self
                    .peek()
                    .ok_or_else(|| PromqlError("dangling escape in regex".into()))?;
                self.pos += 1;
                Ok(escape_node(c))
            }
            Some(c) => {
                self.pos += 1;
                Ok(ReNode::Char(c))
            }
            None => Ok(ReNode::Empty),
        }
    }

    fn parse_class(&mut self) -> Result<ReNode, PromqlError> {
        self.pos += 1; // '['
        let mut neg = false;
        if self.peek() == Some('^') {
            neg = true;
            self.pos += 1;
        }
        let mut ranges: Vec<(char, char)> = Vec::new();
        while let Some(c) = self.peek() {
            if c == ']' {
                self.pos += 1;
                return Ok(ReNode::Class { neg, ranges });
            }
            let start = if c == '\\' {
                self.pos += 1;
                let e = self
                    .peek()
                    .ok_or_else(|| PromqlError("dangling escape in class".into()))?;
                self.pos += 1;
                if let Some((lo, hi)) = escape_class(e) {
                    ranges.push((lo, hi));
                    continue;
                }
                e
            } else {
                self.pos += 1;
                c
            };
            // range?
            if self.peek() == Some('-') && self.chars.get(self.pos + 1) != Some(&']') {
                self.pos += 1; // '-'
                let end = self
                    .peek()
                    .ok_or_else(|| PromqlError("unterminated class range".into()))?;
                self.pos += 1;
                ranges.push((start, end));
            } else {
                ranges.push((start, start));
            }
        }
        err("unterminated character class")
    }
}

fn escape_node(c: char) -> ReNode {
    match c {
        'd' => ReNode::Class {
            neg: false,
            ranges: vec![('0', '9')],
        },
        'D' => ReNode::Class {
            neg: true,
            ranges: vec![('0', '9')],
        },
        'w' => ReNode::Class {
            neg: false,
            ranges: vec![('a', 'z'), ('A', 'Z'), ('0', '9'), ('_', '_')],
        },
        'W' => ReNode::Class {
            neg: true,
            ranges: vec![('a', 'z'), ('A', 'Z'), ('0', '9'), ('_', '_')],
        },
        's' => ReNode::Class {
            neg: false,
            ranges: vec![(' ', ' '), ('\t', '\t'), ('\n', '\n'), ('\r', '\r')],
        },
        'S' => ReNode::Class {
            neg: true,
            ranges: vec![(' ', ' '), ('\t', '\t'), ('\n', '\n'), ('\r', '\r')],
        },
        'n' => ReNode::Char('\n'),
        't' => ReNode::Char('\t'),
        other => ReNode::Char(other),
    }
}

fn escape_class(c: char) -> Option<(char, char)> {
    match c {
        'd' => Some(('0', '9')),
        _ => None,
    }
}

fn class_match(neg: bool, ranges: &[(char, char)], c: char) -> bool {
    let hit = ranges.iter().any(|(lo, hi)| c >= *lo && c <= *hi);
    hit != neg
}

/// CPS backtracking matcher: `k` is the continuation over the remaining input position.
fn re_match(node: &ReNode, input: &[char], pos: usize, k: &dyn Fn(usize) -> bool) -> bool {
    match node {
        ReNode::Empty => k(pos),
        ReNode::Char(c) => pos < input.len() && input[pos] == *c && k(pos + 1),
        ReNode::AnyChar => pos < input.len() && k(pos + 1),
        ReNode::Class { neg, ranges } => {
            pos < input.len() && class_match(*neg, ranges, input[pos]) && k(pos + 1)
        }
        ReNode::Concat(parts) => re_concat(parts, 0, input, pos, k),
        ReNode::Alt(branches) => branches.iter().any(|b| re_match(b, input, pos, k)),
        ReNode::Star(inner) => re_star(inner, input, pos, k),
        ReNode::Plus(inner) => re_match(inner, input, pos, &|p| re_star(inner, input, p, k)),
        ReNode::Opt(inner) => re_match(inner, input, pos, k) || k(pos),
    }
}

fn re_concat(
    parts: &[ReNode],
    i: usize,
    input: &[char],
    pos: usize,
    k: &dyn Fn(usize) -> bool,
) -> bool {
    if i == parts.len() {
        return k(pos);
    }
    re_match(&parts[i], input, pos, &|p| {
        re_concat(parts, i + 1, input, p, k)
    })
}

fn re_star(inner: &ReNode, input: &[char], pos: usize, k: &dyn Fn(usize) -> bool) -> bool {
    // Greedy: consume as many as possible (guarding against empty matches), then k.
    re_match(inner, input, pos, &|p| {
        p > pos && re_star(inner, input, p, k)
    }) || k(pos)
}

// ───────────────────────────── convenience ─────────────────────────────

/// Parse + evaluate `expr_str` at instant `t` against `source`.
pub fn query_instant(
    source: &dyn SeriesSource,
    expr_str: &str,
    t: Ts,
) -> Result<Value, PromqlError> {
    let ast = parse(expr_str)?;
    Evaluator::new(source).eval_instant(&ast, t)
}

/// Parse + evaluate `expr_str` over `[start, end]` at `step` against `source`.
pub fn query_range(
    source: &dyn SeriesSource,
    expr_str: &str,
    start: Ts,
    end: Ts,
    step: Ts,
) -> Result<Vec<RangeSeries>, PromqlError> {
    let ast = parse(expr_str)?;
    Evaluator::new(source).eval_range(&ast, start, end, step)
}

// ───────────────────────────── tests (CONCEPT:EG-172) ─────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    const S: i64 = 1_000_000_000; // 1 second in ns

    fn src() -> MemSeriesSource {
        let mut s = MemSeriesSource::new();
        // http_requests_total{job="api",method="get"} counter
        s.push(
            MemSeriesSource::labels("http_requests_total", &[("job", "api"), ("method", "get")]),
            vec![
                (0, 0.0),
                (15 * S, 10.0),
                (30 * S, 20.0),
                (45 * S, 30.0),
                (60 * S, 40.0),
            ],
        );
        // http_requests_total{job="api",method="post"} counter
        s.push(
            MemSeriesSource::labels("http_requests_total", &[("job", "api"), ("method", "post")]),
            vec![
                (0, 0.0),
                (15 * S, 5.0),
                (30 * S, 10.0),
                (45 * S, 15.0),
                (60 * S, 20.0),
            ],
        );
        // node_cpu{instance="a"} gauge
        s.push(
            MemSeriesSource::labels("node_cpu", &[("instance", "a")]),
            vec![(60 * S, 3.5)],
        );
        s
    }

    #[test]
    fn eg172_selector_eq_and_regex_matchers() {
        let s = src();
        // exact match on method="get" → one series
        let v = query_instant(&s, r#"http_requests_total{method="get"}"#, 60 * S).unwrap();
        let iv = match v {
            Value::Instant(iv) => iv,
            _ => panic!("expected vector"),
        };
        assert_eq!(iv.len(), 1);
        assert_eq!(iv[0].value, 40.0);

        // regex match on job=~"a.*" → both http_requests_total series
        let v = query_instant(&s, r#"http_requests_total{job=~"a.*"}"#, 60 * S).unwrap();
        assert_eq!(
            match v {
                Value::Instant(iv) => iv.len(),
                _ => 0,
            },
            2
        );

        // negative regex method!~"g.*" → only the post series
        let v = query_instant(&s, r#"http_requests_total{method!~"g.*"}"#, 60 * S).unwrap();
        let iv = match v {
            Value::Instant(iv) => iv,
            _ => panic!(),
        };
        assert_eq!(iv.len(), 1);
        assert_eq!(iv[0].labels.get("method").unwrap(), "post");
    }

    #[test]
    fn eg172_rate_over_range_vector() {
        let s = src();
        // rate over the full minute for the get series. Window (0,60s]: points at
        // 15,30,45,60 (=10,20,30,40); increase=30 over span 45s → 30/45.
        let v = query_instant(
            &s,
            r#"rate(http_requests_total{method="get"}[60s])"#,
            60 * S,
        )
        .unwrap();
        let iv = match v {
            Value::Instant(iv) => iv,
            _ => panic!(),
        };
        assert_eq!(iv.len(), 1);
        assert!(
            (iv[0].value - (30.0 / 45.0)).abs() < 1e-9,
            "got {}",
            iv[0].value
        );
        // rate() drops the metric name.
        assert!(!iv[0].labels.contains_key(METRIC_NAME));
    }

    #[test]
    fn eg172_increase_and_irate() {
        let s = src();
        let v = query_instant(
            &s,
            r#"increase(http_requests_total{method="get"}[60s])"#,
            60 * S,
        )
        .unwrap();
        let iv = match v {
            Value::Instant(iv) => iv,
            _ => panic!(),
        };
        assert_eq!(iv[0].value, 30.0); // (0,60s] → 40-10

        let v = query_instant(
            &s,
            r#"irate(http_requests_total{method="get"}[60s])"#,
            60 * S,
        )
        .unwrap();
        let iv = match v {
            Value::Instant(iv) => iv,
            _ => panic!(),
        };
        assert!(
            (iv[0].value - (10.0 / 15.0)).abs() < 1e-9,
            "got {}",
            iv[0].value
        );
    }

    #[test]
    fn eg172_sum_by_aggregation() {
        let s = src();
        // sum by (job) of the two api series' instant values (40 + 20 = 60).
        let v = query_instant(&s, r#"sum by (job) (http_requests_total)"#, 60 * S).unwrap();
        let iv = match v {
            Value::Instant(iv) => iv,
            _ => panic!(),
        };
        assert_eq!(iv.len(), 1);
        assert_eq!(iv[0].value, 60.0);
        assert_eq!(iv[0].labels.get("job").unwrap(), "api");
        assert!(!iv[0].labels.contains_key("method")); // collapsed away
        assert!(!iv[0].labels.contains_key(METRIC_NAME));
    }

    #[test]
    fn eg172_aggregation_without_and_count_avg_min_max() {
        let s = src();
        // count without(method) → groups by remaining {job} → 2 series counted.
        let v = query_instant(
            &s,
            r#"count without (method) (http_requests_total)"#,
            60 * S,
        )
        .unwrap();
        let iv = match v {
            Value::Instant(iv) => iv,
            _ => panic!(),
        };
        assert_eq!(iv.len(), 1);
        assert_eq!(iv[0].value, 2.0);

        assert_eq!(agg_scalar(&s, r#"max(http_requests_total)"#), 40.0);
        assert_eq!(agg_scalar(&s, r#"min(http_requests_total)"#), 20.0);
        assert_eq!(agg_scalar(&s, r#"avg(http_requests_total)"#), 30.0);
        assert_eq!(agg_scalar(&s, r#"sum(http_requests_total)"#), 60.0);
    }

    fn agg_scalar(s: &MemSeriesSource, q: &str) -> f64 {
        match query_instant(s, q, 60 * S).unwrap() {
            Value::Instant(iv) => {
                assert_eq!(iv.len(), 1);
                iv[0].value
            }
            _ => panic!("expected vector"),
        }
    }

    #[test]
    fn eg172_histogram_quantile() {
        let mut s = MemSeriesSource::new();
        // A classic histogram: le buckets 0.1,0.5,1,+Inf with cumulative counts.
        for (le, c) in [("0.1", 1.0), ("0.5", 2.0), ("1", 6.0), ("+Inf", 10.0)] {
            s.push(
                MemSeriesSource::labels("http_latency_bucket", &[("le", le)]),
                vec![(60 * S, c)],
            );
        }
        // p90 → rank 9 falls in the (1, +Inf] bucket; algorithm returns the highest
        // finite bound (1.0) since the top bucket is +Inf.
        let v = query_instant(
            &s,
            r#"histogram_quantile(0.9, http_latency_bucket)"#,
            60 * S,
        )
        .unwrap();
        let iv = match v {
            Value::Instant(iv) => iv,
            _ => panic!(),
        };
        assert_eq!(iv.len(), 1);
        assert_eq!(iv[0].value, 1.0);

        // p50 → rank 5 in the (0.5, 1] bucket: 0.5 + (1-0.5)*((5-2)/(6-2)) = 0.875.
        let v = query_instant(
            &s,
            r#"histogram_quantile(0.5, http_latency_bucket)"#,
            60 * S,
        )
        .unwrap();
        let iv = match v {
            Value::Instant(iv) => iv,
            _ => panic!(),
        };
        assert!((iv[0].value - 0.875).abs() < 1e-9, "got {}", iv[0].value);
    }

    #[test]
    fn eg172_binary_ops_vector_scalar_and_vector_vector() {
        let s = src();
        // vector * scalar
        let v = query_instant(&s, r#"http_requests_total{method="get"} * 2"#, 60 * S).unwrap();
        let iv = match v {
            Value::Instant(iv) => iv,
            _ => panic!(),
        };
        assert_eq!(iv[0].value, 80.0);
        assert!(!iv[0].labels.contains_key(METRIC_NAME)); // arith drops name

        // vector - vector, matched on identical labels (get - get = 0).
        let v = query_instant(
            &s,
            r#"http_requests_total{method="get"} - http_requests_total{method="get"}"#,
            60 * S,
        )
        .unwrap();
        let iv = match v {
            Value::Instant(iv) => iv,
            _ => panic!(),
        };
        assert_eq!(iv.len(), 1);
        assert_eq!(iv[0].value, 0.0);

        // comparison filter: keep samples > 25 → only the get series (40).
        let v = query_instant(&s, r#"http_requests_total > 25"#, 60 * S).unwrap();
        let iv = match v {
            Value::Instant(iv) => iv,
            _ => panic!(),
        };
        assert_eq!(iv.len(), 1);
        assert_eq!(iv[0].value, 40.0);

        // comparison with bool → 0/1 for every sample.
        let v = query_instant(&s, r#"http_requests_total > bool 25"#, 60 * S).unwrap();
        let iv = match v {
            Value::Instant(iv) => iv,
            _ => panic!(),
        };
        assert_eq!(iv.len(), 2);
        assert_eq!(iv.iter().map(|x| x.value).sum::<f64>(), 1.0); // one true (get), one false (post)
    }

    #[test]
    fn eg172_abs_ceil_floor_and_unary_neg() {
        let mut s = MemSeriesSource::new();
        s.push(MemSeriesSource::labels("g", &[("k", "v")]), vec![(0, -3.2)]);
        assert_eq!(scalar_of(&s, "abs(g)"), 3.2);
        assert_eq!(scalar_of(&s, "ceil(g)"), -3.0);
        assert_eq!(scalar_of(&s, "floor(g)"), -4.0);
        assert!((scalar_of(&s, "-g") - 3.2).abs() < 1e-9);
    }

    fn scalar_of(s: &MemSeriesSource, q: &str) -> f64 {
        match query_instant(s, q, 0).unwrap() {
            Value::Instant(iv) => iv[0].value,
            Value::Scalar(x) => x,
            _ => panic!(),
        }
    }

    #[test]
    fn eg172_range_query_produces_matrix() {
        let s = src();
        // instant selector over a range → a point at each step per series.
        let m = query_range(
            &s,
            r#"http_requests_total{method="get"}"#,
            0,
            60 * S,
            15 * S,
        )
        .unwrap();
        assert_eq!(m.len(), 1);
        // steps 0,15,30,45,60 → 5 points (each within the 5m lookback).
        assert_eq!(m[0].points.len(), 5);
        assert_eq!(m[0].points.last().unwrap().1, 40.0);
    }

    #[test]
    fn eg172_scalar_literal_and_arith() {
        let s = src();
        assert_eq!(scalar_of(&s, "2 + 3 * 4"), 14.0);
        assert_eq!(scalar_of(&s, "2 ^ 3 ^ 2"), 512.0); // right-assoc
        assert_eq!(scalar_of(&s, "(2 + 3) * 4"), 20.0);
    }

    #[test]
    fn eg172_regex_engine_anchoring_and_alternation() {
        // Anchored: "get" matches "get" but not "getx".
        let m = LabelMatcher::new("x", MatchOp::Re, "get").unwrap();
        assert!(m.matches(&MemSeriesSource::labels("_", &[("x", "get")])));
        assert!(!m.matches(&MemSeriesSource::labels("_", &[("x", "getx")])));
        // alternation + classes + quantifiers
        let m = LabelMatcher::new("x", MatchOp::Re, "(foo|ba[rz])+[0-9]*").unwrap();
        assert!(m.matches(&MemSeriesSource::labels("_", &[("x", "foobar42")])));
        assert!(m.matches(&MemSeriesSource::labels("_", &[("x", "baz")])));
        assert!(!m.matches(&MemSeriesSource::labels("_", &[("x", "qux")])));
        // \d and .*
        let m = LabelMatcher::new("x", MatchOp::Re, r"v\d+.*").unwrap();
        assert!(m.matches(&MemSeriesSource::labels("_", &[("x", "v12beta")])));
        assert!(!m.matches(&MemSeriesSource::labels("_", &[("x", "vbeta")])));
    }

    #[test]
    fn eg172_duration_and_offset_parse() {
        // 1h30m duration + offset parse without error and produce a Matrix node.
        let e = parse(r#"rate(m[1h30m] offset 5m)"#).unwrap();
        match e {
            Expr::Call { func, args } => {
                assert_eq!(func, "rate");
                match &args[0] {
                    Expr::Matrix {
                        range_ns,
                        offset_ns,
                        ..
                    } => {
                        assert_eq!(*range_ns, (90 * 60) as i64 * S);
                        assert_eq!(*offset_ns, (5 * 60) as i64 * S);
                    }
                    other => panic!("expected matrix, got {other:?}"),
                }
            }
            other => panic!("expected call, got {other:?}"),
        }
    }

    #[test]
    fn eg172_set_ops_and_or_unless() {
        let s = src();
        // and: keep get-series only if a matching-label post exists → no match (labels differ), empty.
        let v = query_instant(
            &s,
            r#"http_requests_total{method="get"} and http_requests_total{method="post"}"#,
            60 * S,
        )
        .unwrap();
        assert_eq!(
            match v {
                Value::Instant(iv) => iv.len(),
                _ => 99,
            },
            0
        );
        // or: union of both → 2.
        let v = query_instant(
            &s,
            r#"http_requests_total{method="get"} or http_requests_total{method="post"}"#,
            60 * S,
        )
        .unwrap();
        assert_eq!(
            match v {
                Value::Instant(iv) => iv.len(),
                _ => 0,
            },
            2
        );
    }

    #[test]
    fn eg172_parse_errors_are_typed() {
        assert!(parse("http_requests_total{method=}").is_err());
        assert!(parse("sum by (").is_err());
        assert!(parse(")(").is_err());
    }
}
