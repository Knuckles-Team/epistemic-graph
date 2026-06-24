//! UQL recursive-descent parser (CONCEPT:KG-2.214) → the existing [`Plan`] AST.
//!
//! This is a pure FRONT-END: it parses a human/agent-writable text query into the
//! SAME `wire::Plan` (`Vec<Op>`) that `Method::UnifiedQuery` already executes — it
//! adds NO new execution path. The proof of faithfulness is [`super`]'s tests:
//! a UQL string parses to the byte-identical Plan a hand-built test constructs, and
//! the served query returns the same result as the structured plan AND the
//! separate-surfaces oracle.
//!
//! Dependency-free (no DataFusion, no regex) so the whole front-end ships in a
//! default/Pi build — only EXECUTION is `query`-gated.
//!
//! ## Grammar (EBNF) — the bound-this-increment subset
//!
//! ```text
//! query      = scan { "|>" stage } ;
//! scan       = "MATCH" "(" [ ":" ] label ")" [ "WHERE" pred_list ] ;
//! stage      = traverse | rank | limit | filter ;
//! filter     = "WHERE" pred_list ;                         (* a later-stage filter *)
//! traverse   = "TRAVERSE" edge ;
//! edge       = "-" "[" ":" rel "]" "->" [ hop_range ]
//!            | rel [ hop_range ] ;                          (* bare-rel shorthand    *)
//! hop_range  = "{" int [ ( "," | ".." ) int ] "}" ;        (* {2} = {2,2}; {1,3}    *)
//! rank       = "RANK" "BY" "~" vector_ref ;
//! vector_ref = "[" num { "," num } "]"                     (* inline literal vector *)
//!            | string                                       (* named embedding ref  *)
//!            | ident ;
//! limit      = "LIMIT" int ;
//! pred_list  = pred { "AND" pred } ;
//! pred       = prop ( ">" | "<" | ( "=" | "==" ) ) value ;
//! value      = num | string | ident ;
//! ```
//!
//! ### Op mapping (each grammar production → one `wire::Op`)
//! | UQL                                  | `Op`                               |
//! |--------------------------------------|------------------------------------|
//! | `MATCH (:Doc)`                       | `Scan { label: "Doc" }`            |
//! | `WHERE year > 2024 AND lang = 'en'`  | `Filter { preds: [GtNum, Eq] }`    |
//! | `TRAVERSE -[:CITES]->{1,2}`          | `Traverse { rel:"CITES",min:1,max:2 }` |
//! | `RANK BY ~[1.0,0.0]`                 | `Rank { query: [1.0, 0.0] }`       |
//! | `LIMIT 10`                           | `Limit { k: 10 }`                  |
//!
//! ### Forward-compat seams (documented, NOT built this increment)
//! The grammar leaves clean extension points so the deferred Ops are a grammar+Op
//! ADDITION, never a redesign:
//!  * **time / asof (Lane F)** — the `@` sigil is already lexed; reserve
//!    `AS OF @<ts>` / `WINDOW <dur>` as a trailing `scan`/`stage` clause feeding a
//!    future `Op::AsOf`/`Op::Window`.
//!  * **text-rank (Lane G4)** — `RANK BY ~"text query"` already accepts a *string*
//!    vector_ref; a future `Op::TextRank { query: String }` is selected when the
//!    `vector_ref` is a string the planner cannot resolve to an embedding handle
//!    (this increment treats a string as a named-embedding placeholder error until
//!    that Op lands — see [`VectorRef`]).
//!  * **reason (datalog fixpoint)** — reserve a `REASON <rel> {n}` stage mapping to a
//!    future `Op::Reason`; it slots into `stage` with no other change.

use eg_types::wire::{Op, Plan, Pred};

use super::lexer::{lex, Tok, Token};

/// A UQL parse error: a human-readable message plus the byte offset in the source it
/// occurred at. [`UqlError::render`] turns it into a multi-line caret diagnostic.
#[derive(Clone, Debug, PartialEq)]
pub struct UqlError {
    pub msg: String,
    pub at: usize,
}

impl UqlError {
    /// Render a `source`-relative caret diagnostic, e.g.
    /// ```text
    /// UQL parse error: expected `LIMIT` count, found `)`
    ///   MATCH (:Doc) |> LIMIT )
    ///                          ^
    /// ```
    pub fn render(&self, src: &str) -> String {
        let at = self.at.min(src.len());
        let caret = " ".repeat(at) + "^";
        format!(
            "UQL parse error: {}\n  {src}\n  {caret}",
            self.msg,
            src = src,
            caret = caret
        )
    }
}

impl std::fmt::Display for UqlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "UQL parse error at byte {}: {}", self.at, self.msg)
    }
}

/// How a `RANK BY ~<ref>` vector reference resolved. This increment binds ONLY the
/// inline literal vector (`Op::Rank { query }`); a NAMED reference (`~"my query"` /
/// `~handle`) is a documented forward seam (text-rank, Lane G4) and is rejected with
/// a clear "named embedding refs not yet supported" error — until the resolver + the
/// future `Op::TextRank` land, the grammar accepts it but the parser cannot lower it.
#[derive(Clone, Debug, PartialEq)]
enum VectorRef {
    Inline(Vec<f32>),
    Named(String),
}

/// Parse a UQL string into the existing [`Plan`] AST. The returned `Plan` is exactly
/// what a hand-built `Method::UnifiedQuery { plan }` carries — the parser is a pure
/// front-end over the proven planner.
pub fn parse(src: &str) -> Result<Plan, UqlError> {
    let tokens = lex(src).map_err(|e| UqlError {
        msg: e.msg,
        at: e.at,
    })?;
    let mut p = Parser {
        toks: &tokens,
        pos: 0,
        end: src.len(),
    };
    let plan = p.parse_query()?;
    p.expect_eof()?;
    Ok(plan)
}

struct Parser<'a> {
    toks: &'a [Token],
    pos: usize,
    /// Source length, for the "end of input" caret position.
    end: usize,
}

impl<'a> Parser<'a> {
    fn parse_query(&mut self) -> Result<Plan, UqlError> {
        let mut ops = self.parse_scan()?;
        while self.eat(&Tok::Pipe) {
            self.parse_stage(&mut ops)?;
        }
        Ok(Plan::new(ops))
    }

    /// `scan = "MATCH" "(" [":"] label ")" [ "WHERE" pred_list ]`. The optional inline
    /// WHERE is sugar for a following `Filter` stage (so `MATCH (:Doc) WHERE y>1` and
    /// `MATCH (:Doc) |> WHERE y>1` produce the identical Plan).
    fn parse_scan(&mut self) -> Result<Vec<Op>, UqlError> {
        self.expect_kw("MATCH")?;
        self.expect(&Tok::LParen, "`(` after MATCH")?;
        let _ = self.eat(&Tok::Colon); // optional leading `:` on the label
        let label = self.expect_ident("a node label")?;
        self.expect(&Tok::RParen, "`)` to close the MATCH pattern")?;

        let mut ops = vec![Op::Scan { label }];
        // An inline WHERE attached to MATCH lowers to a Filter op, same as a |> WHERE.
        if self.peek_kw("WHERE") {
            self.bump();
            let preds = self.parse_pred_list()?;
            ops.push(Op::Filter { preds });
        }
        Ok(ops)
    }

    /// `stage = traverse | rank | limit | filter`. Dispatched on the leading keyword.
    fn parse_stage(&mut self, ops: &mut Vec<Op>) -> Result<(), UqlError> {
        if self.peek_kw("TRAVERSE") {
            self.bump();
            ops.push(self.parse_traverse()?);
        } else if self.peek_kw("RANK") {
            self.bump();
            ops.push(self.parse_rank()?);
        } else if self.peek_kw("LIMIT") {
            self.bump();
            ops.push(self.parse_limit()?);
        } else if self.peek_kw("WHERE") {
            self.bump();
            let preds = self.parse_pred_list()?;
            ops.push(Op::Filter { preds });
        } else {
            return Err(self
                .err_here("expected a pipeline stage (`TRAVERSE`, `RANK`, `LIMIT`, or `WHERE`)"));
        }
        Ok(())
    }

    /// `traverse = "-" "[" ":" rel "]" "->" [hop_range] | rel [hop_range]`.
    fn parse_traverse(&mut self) -> Result<Op, UqlError> {
        let rel = if self.eat(&Tok::Dash) {
            // Cypher-ish edge pattern: -[:REL]->
            self.expect(&Tok::LBracket, "`[` in the edge pattern `-[:REL]->`")?;
            self.expect(&Tok::Colon, "`:` before the relationship type")?;
            let rel = self.expect_ident("a relationship type")?;
            self.expect(&Tok::RBracket, "`]` to close the edge pattern")?;
            self.expect(&Tok::Arrow, "`->` after the edge pattern")?;
            rel
        } else {
            // Bare-rel shorthand: TRAVERSE CITES{1,2}
            self.expect_ident("a relationship type (or `-[:REL]->`)")?
        };
        let (min, max) = self.parse_hop_range()?;
        Ok(Op::Traverse { rel, min, max })
    }

    /// `hop_range = "{" int [ ("," | "..") int ] "}"`. Absent ⇒ exactly one hop
    /// `{1,1}`; `{n}` ⇒ `{n,n}`; `{a,b}` / `{a..b}` ⇒ `min=a,max=b`.
    fn parse_hop_range(&mut self) -> Result<(usize, usize), UqlError> {
        if !self.eat(&Tok::LBrace) {
            return Ok((1, 1));
        }
        let min = self.expect_usize("a minimum hop count")?;
        let max = if self.eat(&Tok::Comma) || self.eat(&Tok::DotDot) {
            self.expect_usize("a maximum hop count")?
        } else {
            min
        };
        self.expect(&Tok::RBrace, "`}` to close the hop range")?;
        if max < min {
            return Err(self.err_here("hop range max must be ≥ min"));
        }
        Ok((min, max))
    }

    /// `rank = "RANK" "BY" "~" vector_ref`. Only an inline literal vector lowers this
    /// increment; a named ref is the documented text-rank forward seam.
    fn parse_rank(&mut self) -> Result<Op, UqlError> {
        self.expect_kw("BY")?;
        self.expect(&Tok::Tilde, "`~` before the rank vector (`RANK BY ~[…]`)")?;
        match self.parse_vector_ref()? {
            VectorRef::Inline(query) => Ok(Op::Rank { query }),
            VectorRef::Named(_) => Err(self.err_at(
                self.prev_start(),
                "named embedding refs (`RANK BY ~\"text\"`) are a reserved forward \
                 seam (text-rank, Lane G4) and not yet executable — use an inline \
                 literal vector `RANK BY ~[…]`",
            )),
        }
    }

    /// `vector_ref = "[" num {"," num} "]" | string | ident`.
    fn parse_vector_ref(&mut self) -> Result<VectorRef, UqlError> {
        if self.eat(&Tok::LBracket) {
            let mut v = Vec::new();
            if !self.peek(&Tok::RBracket) {
                loop {
                    v.push(self.expect_num("a vector component")? as f32);
                    if !self.eat(&Tok::Comma) {
                        break;
                    }
                }
            }
            self.expect(&Tok::RBracket, "`]` to close the rank vector")?;
            Ok(VectorRef::Inline(v))
        } else if let Some(Tok::Str(s)) = self.peek_kind() {
            let s = s.clone();
            self.bump();
            Ok(VectorRef::Named(s))
        } else if let Some(Tok::Ident(s)) = self.peek_kind() {
            let s = s.clone();
            self.bump();
            Ok(VectorRef::Named(s))
        } else {
            Err(self.err_here("expected a rank vector: `[…]`, a quoted ref, or a name"))
        }
    }

    /// `limit = "LIMIT" int`.
    fn parse_limit(&mut self) -> Result<Op, UqlError> {
        let k = self.expect_usize("a `LIMIT` count")?;
        Ok(Op::Limit { k })
    }

    /// `pred_list = pred { "AND" pred }`.
    fn parse_pred_list(&mut self) -> Result<Vec<Pred>, UqlError> {
        let mut preds = vec![self.parse_pred()?];
        while self.peek_kw("AND") {
            self.bump();
            preds.push(self.parse_pred()?);
        }
        Ok(preds)
    }

    /// `pred = prop ( ">" | "<" | "=" ) value`. `>`/`<` require a numeric RHS
    /// (`GtNum`/`LtNum`); `=` is string equality (`Eq`, JSON-stringified value).
    fn parse_pred(&mut self) -> Result<Pred, UqlError> {
        let prop = self.expect_ident("a property name")?;
        if self.eat(&Tok::Gt) {
            let n = self.expect_num("a numeric value after `>`")?;
            Ok(Pred::GtNum { prop, n })
        } else if self.eat(&Tok::Lt) {
            let n = self.expect_num("a numeric value after `<`")?;
            Ok(Pred::LtNum { prop, n })
        } else if self.eat(&Tok::Eq) {
            let value = self.expect_value("a value after `=`")?;
            Ok(Pred::Eq { prop, value })
        } else {
            Err(self.err_here("expected a comparison `>`, `<`, or `=`"))
        }
    }

    // ── token helpers ───────────────────────────────────────────────────────

    fn peek_kind(&self) -> Option<&Tok> {
        self.toks.get(self.pos).map(|t| &t.kind)
    }

    fn peek(&self, k: &Tok) -> bool {
        self.peek_kind() == Some(k)
    }

    /// Case-insensitive keyword lookahead (the lexer makes no keyword distinction).
    fn peek_kw(&self, kw: &str) -> bool {
        matches!(self.peek_kind(), Some(Tok::Ident(s)) if s.eq_ignore_ascii_case(kw))
    }

    fn bump(&mut self) {
        self.pos += 1;
    }

    fn eat(&mut self, k: &Tok) -> bool {
        if self.peek(k) {
            self.bump();
            true
        } else {
            false
        }
    }

    fn expect(&mut self, k: &Tok, what: &str) -> Result<(), UqlError> {
        if self.eat(k) {
            Ok(())
        } else {
            Err(self.err_here(&format!("expected {what}")))
        }
    }

    fn expect_kw(&mut self, kw: &str) -> Result<(), UqlError> {
        if self.peek_kw(kw) {
            self.bump();
            Ok(())
        } else {
            Err(self.err_here(&format!("expected keyword `{kw}`")))
        }
    }

    fn expect_ident(&mut self, what: &str) -> Result<String, UqlError> {
        match self.peek_kind() {
            Some(Tok::Ident(s)) => {
                let s = s.clone();
                self.bump();
                Ok(s)
            }
            _ => Err(self.err_here(&format!("expected {what}"))),
        }
    }

    fn expect_num(&mut self, what: &str) -> Result<f64, UqlError> {
        match self.peek_kind() {
            Some(Tok::Num(n)) => {
                let n = *n;
                self.bump();
                Ok(n)
            }
            _ => Err(self.err_here(&format!("expected {what}"))),
        }
    }

    fn expect_usize(&mut self, what: &str) -> Result<usize, UqlError> {
        let at = self.cur_start();
        let n = self.expect_num(what)?;
        if n < 0.0 || n.fract() != 0.0 {
            return Err(self.err_at(at, &format!("{what} must be a non-negative integer")));
        }
        Ok(n as usize)
    }

    /// A predicate RHS value: a string, a number (stringified), or a bare word.
    /// All become the `String` an `Eq` predicate compares against (the executor
    /// JSON-stringifies the stored value the same way).
    fn expect_value(&mut self, what: &str) -> Result<String, UqlError> {
        match self.peek_kind() {
            Some(Tok::Str(s)) => {
                let s = s.clone();
                self.bump();
                Ok(s)
            }
            Some(Tok::Ident(s)) => {
                let s = s.clone();
                self.bump();
                Ok(s)
            }
            Some(Tok::Num(n)) => {
                let n = *n;
                self.bump();
                // Render an integral f64 without a trailing `.0` so `= 3` matches the
                // stored integer's JSON string form.
                Ok(if n.fract() == 0.0 {
                    format!("{}", n as i64)
                } else {
                    format!("{n}")
                })
            }
            _ => Err(self.err_here(&format!("expected {what}"))),
        }
    }

    fn expect_eof(&mut self) -> Result<(), UqlError> {
        if self.pos >= self.toks.len() {
            Ok(())
        } else {
            Err(self.err_here("unexpected trailing tokens after the query"))
        }
    }

    // ── error positioning ───────────────────────────────────────────────────

    /// Byte offset of the current token (or end-of-input).
    fn cur_start(&self) -> usize {
        self.toks.get(self.pos).map(|t| t.start).unwrap_or(self.end)
    }

    /// Byte offset of the previously consumed token (for "this token was wrong").
    fn prev_start(&self) -> usize {
        self.pos
            .checked_sub(1)
            .and_then(|i| self.toks.get(i))
            .map(|t| t.start)
            .unwrap_or(self.end)
    }

    /// Build an error anchored at the current token, naming what was actually found.
    fn err_here(&self, msg: &str) -> UqlError {
        let found = match self.peek_kind() {
            Some(t) => format!(", found {t}"),
            None => ", found end of input".to_string(),
        };
        UqlError {
            msg: format!("{msg}{found}"),
            at: self.cur_start(),
        }
    }

    fn err_at(&self, at: usize, msg: &str) -> UqlError {
        UqlError {
            msg: msg.to_string(),
            at,
        }
    }
}
