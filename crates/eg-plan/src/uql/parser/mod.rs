//! UQL recursive-descent parser (CONCEPT:AU-KG.query.top-nodes-by-degree) → the existing
//! [`Plan`] AST (or, for `LET` programs, a DAG of the same ops).
//!
//! A pure FRONT-END: it parses text into the SAME `wire::Plan` (`Vec<Op>`) that
//! `Method::UnifiedQuery` executes — no new execution path. The grammar is
//! [`super::grammar::PRODUCTIONS`]; this module's [`Parser::stage_table`] is checked
//! against it in both directions. Module layout (one owner per concern):
//!  * `literal` — names, strings, numbers (read from their exact source text), typed
//!    parameters, lists, vectors, JSON literals;
//!  * `pred` — the predicate algebra (`OR`/`AND`/`NOT`/parentheses and every atom);
//!  * `basic` — `MATCH`, `WHERE`, `TRAVERSE`, `RANK`, `RERANK`, `AS OF`, `WINDOW`, `LIMIT`,
//!    `RETURN`, `FOREIGN`;
//!  * `semantic` — `TEXT`/`FUSE` (text), `REASON`/`SPARQL` (owl), `UDF` (wasm-udf) and the
//!    epistemic stages;
//!  * `modality` — spatial, tensor, time-series/sensor, CEP and probabilistic stages;
//!  * `program` — statements: `UQL n;`, `EXPLAIN`/`PROFILE`, `LET` bindings, `FROM`/`JOIN`.
//!
//! Feature policy (EH-374, one rule everywhere): every clause is RECOGNIZED by its
//! leading keyword in every build; a clause whose executor needs a cargo feature this
//! build lacks is refused at parse time with `UQL_FEATURE_NOT_IN_BUILD` naming the
//! feature. Dependency-free (no DataFusion, no regex).

use std::collections::{BTreeMap, BTreeSet};

use eg_types::wire::Op;

use super::diag::{UqlCode, UqlError, UqlWarning};
use super::lexer::{Tok, Token};
use super::Params;

/// Recognize a feature-gated parse method: the real body under the feature, a
/// `UQL_FEATURE_NOT_IN_BUILD` refusal without it. One rule for every gated clause.
macro_rules! gated {
    ($feature:literal, $(#[$m:meta])* fn $name:ident(&mut $self:ident) -> $ret:ty $body:block) => {
        #[cfg(feature = $feature)]
        $(#[$m])*
        pub(super) fn $name(&mut $self) -> $ret $body
        #[cfg(not(feature = $feature))]
        $(#[$m])*
        pub(super) fn $name(&mut $self) -> $ret {
            Err($self.not_built($feature))
        }
    };
}

mod basic;
mod literal;
mod modality;
mod pred;
pub(super) mod program;
mod semantic;
mod shape;

/// How deep parentheses / `NOT` / `FUSE` branches may nest (stack-safety bound).
pub(super) const MAX_DEPTH: usize = 64;

/// DecideText's clauses. Ordinary UQL refuses them by name rather than with the
/// generic "expected a pipeline stage", so a caller who sent decision text to
/// the query parser learns where it belongs (DECIDE-LAYER-DESIGN §5).
const DECISION_CLAUSES: [&str; 4] = ["DECIDE", "ASSEMBLE", "COVERS", "VALIDATE"];

/// A stage parser: the leading keyword has been consumed.
type StageFn<'a> = fn(&mut Parser<'a>) -> Result<Op, UqlError>;

/// The parser state. Fields are private to the `parser` module tree.
pub(super) struct Parser<'a> {
    toks: &'a [Token],
    pos: usize,
    /// Source length, for the "end of input" caret position.
    end: usize,
    src: &'a str,
    params: &'a Params,
    used_params: BTreeSet<String>,
    depth: usize,
    warnings: Vec<UqlWarning>,
    /// `LET` bindings parsed so far (define-before-use).
    bindings: BTreeMap<String, program::Chain>,
    /// Bindings referenced by a `FROM`/`JOIN` head (they become DAG nodes).
    referenced: BTreeSet<String>,
    /// Bindings inlined as `FUSE (a, b)` branches.
    inlined: BTreeSet<String>,
}

impl<'a> Parser<'a> {
    pub(super) fn new(src: &'a str, toks: &'a [Token], params: &'a Params) -> Self {
        Self {
            toks,
            pos: 0,
            end: src.len(),
            src,
            params,
            used_params: BTreeSet::new(),
            depth: 0,
            warnings: Vec::new(),
            bindings: BTreeMap::new(),
            referenced: BTreeSet::new(),
            inlined: BTreeSet::new(),
        }
    }

    /// Every stage/source keyword → its parser. MATCH (it may lower to two ops) and
    /// `VALIDATE SHAPE` (the two-keyword lead `parser::shape` owns, so that every other
    /// `VALIDATE …` still reaches the DecideText refusal) are handled by [`Self::stage`].
    pub(super) fn stage_table() -> [(&'static str, StageFn<'a>); 29] {
        [
            ("WHERE", Self::where_stage),
            ("TRAVERSE", Self::traverse),
            ("RANK", Self::rank),
            ("RERANK", Self::rerank),
            ("AS", Self::as_of),
            ("VALID", Self::valid_as_of),
            ("WINDOW", Self::window),
            ("LIMIT", Self::limit),
            ("RETURN", Self::return_stage),
            ("FOREIGN", Self::foreign),
            ("TEXT", Self::text),
            ("FUSE", Self::fuse),
            ("REASON", Self::reason),
            ("SPARQL", Self::sparql),
            ("UDF", Self::udf),
            ("EVIDENCE", Self::evidence_for),
            ("CONTRADICTS", Self::contradicts),
            ("SUPPORTED", Self::supported_by),
            ("BELIEF", Self::belief_as_of),
            ("SOURCE", Self::source_reliability),
            ("CONFIDENCE", Self::confidence),
            ("EXPLAIN", Self::explain_belief),
            ("SPATIAL", Self::spatial),
            ("REPROJECT", Self::reproject),
            ("TENSOR", Self::tensor),
            ("CEP", Self::cep),
            ("SENSOR", Self::sensor),
            ("TSSCAN", Self::tsscan),
            ("PROB", Self::prob),
        ]
    }

    /// Parse ONE stage (or source) at the current position, appending its op(s).
    pub(super) fn stage(&mut self, ops: &mut Vec<Op>) -> Result<(), UqlError> {
        if self.peek_kw("MATCH") {
            self.bump();
            ops.extend(self.match_source()?);
            return Ok(());
        }
        if let Some(op) = self.parse_shape_stage()? {
            ops.push(op);
            return Ok(());
        }
        let table = Self::stage_table();
        if let Some((_, parse)) = table.iter().find(|(kw, _)| self.peek_kw(kw)) {
            self.bump();
            ops.push(parse(self)?);
            return Ok(());
        }
        Err(self.unknown_stage())
    }

    fn unknown_stage(&self) -> UqlError {
        if let Some(clause) = DECISION_CLAUSES.iter().find(|kw| self.peek_kw(kw)) {
            return self.error(
                UqlCode::DecisionClauseInUql,
                &format!(
                    "DECISION_CLAUSE_IN_UQL: `{clause}` is a DecideText clause; ordinary UQL \
                     never plans a decision (parse it with `eg_plan::decide_text::parse`)"
                ),
            );
        }
        let expected: Vec<String> = super::grammar::stage_keywords()
            .into_iter()
            .map(|k| format!("`{k}`"))
            .collect();
        self.error(UqlCode::UnknownStage, "expected a pipeline stage or source")
            .expecting(expected)
    }

    // ── token helpers (names kept stable for sibling stage modules) ──────────

    pub(super) fn peek_kind(&self) -> Option<&Tok> {
        self.toks.get(self.pos).map(|t| &t.kind)
    }

    pub(super) fn peek(&self, k: &Tok) -> bool {
        self.peek_kind() == Some(k)
    }

    /// Case-insensitive keyword lookahead (only a BARE word can be a keyword).
    pub(super) fn peek_kw(&self, kw: &str) -> bool {
        matches!(self.peek_kind(), Some(Tok::Ident(s)) if s.eq_ignore_ascii_case(kw))
    }

    /// Keyword lookahead one token further.
    pub(super) fn peek_kw_at(&self, offset: usize, kw: &str) -> bool {
        matches!(
            self.toks.get(self.pos + offset).map(|t| &t.kind),
            Some(Tok::Ident(s)) if s.eq_ignore_ascii_case(kw)
        )
    }

    pub(super) fn bump(&mut self) {
        self.pos += 1;
    }

    pub(super) fn eat(&mut self, k: &Tok) -> bool {
        let hit = self.peek(k);
        if hit {
            self.bump();
        }
        hit
    }

    pub(super) fn eat_kw(&mut self, kw: &str) -> bool {
        let hit = self.peek_kw(kw);
        if hit {
            self.bump();
        }
        hit
    }

    pub(super) fn expect(&mut self, k: &Tok, what: &str) -> Result<(), UqlError> {
        if self.eat(k) {
            return Ok(());
        }
        Err(self.err_here(&format!("expected {what}")))
    }

    pub(super) fn expect_kw(&mut self, kw: &str) -> Result<(), UqlError> {
        if self.eat_kw(kw) {
            return Ok(());
        }
        Err(self
            .err_here(&format!("expected keyword `{kw}`"))
            .expecting(vec![format!("`{kw}`")]))
    }

    /// Enter a nested construct (parentheses, `NOT`, a `FUSE` branch).
    pub(super) fn enter(&mut self) -> Result<(), UqlError> {
        self.depth += 1;
        if self.depth > MAX_DEPTH {
            return Err(self.error(
                UqlCode::NestingTooDeep,
                &format!("nesting deeper than {MAX_DEPTH} levels"),
            ));
        }
        Ok(())
    }

    pub(super) fn leave(&mut self) {
        self.depth -= 1;
    }

    pub(super) fn at_end(&self) -> bool {
        self.pos >= self.toks.len()
    }

    // ── error positioning ───────────────────────────────────────────────────

    /// Span of the current token (or end of input).
    pub(super) fn cur_span(&self) -> (usize, usize) {
        self.toks
            .get(self.pos)
            .map_or((self.end, self.end), |t| (t.start, t.end))
    }

    /// Byte offset of the current token (or end-of-input).
    pub(super) fn cur_start(&self) -> usize {
        self.cur_span().0
    }

    /// Byte offset of the previously consumed token.
    pub(super) fn prev_start(&self) -> usize {
        self.prev_span().0
    }

    pub(super) fn prev_span(&self) -> (usize, usize) {
        self.pos
            .checked_sub(1)
            .and_then(|i| self.toks.get(i))
            .map_or((self.end, self.end), |t| (t.start, t.end))
    }

    /// The source text of the token at `index`.
    pub(super) fn text_of(&self, index: usize) -> &'a str {
        self.toks
            .get(index)
            .map_or("", |t| &self.src[t.start..t.end])
    }

    /// An error of `code` anchored at the current token, naming what was found.
    pub(super) fn error(&self, code: UqlCode, msg: &str) -> UqlError {
        let found = match self.peek_kind() {
            Some(t) => format!(", found {t}"),
            None => ", found end of input".to_string(),
        };
        UqlError::new(code, format!("{msg}{found}"), self.cur_span())
    }

    /// An `UQL_UNEXPECTED_TOKEN` at the current token.
    pub(super) fn err_here(&self, msg: &str) -> UqlError {
        self.error(UqlCode::UnexpectedToken, msg)
    }

    /// An `UQL_UNEXPECTED_TOKEN` at an explicit offset.
    pub(super) fn err_at(&self, at: usize, msg: &str) -> UqlError {
        UqlError::new(UqlCode::UnexpectedToken, msg, (at, at + 1))
    }

    /// The refusal of a clause whose executor this build lacks.
    pub(super) fn not_built(&self, feature: &str) -> UqlError {
        let clause = self
            .text_of(self.pos.saturating_sub(1))
            .to_ascii_uppercase();
        UqlError::new(
            UqlCode::FeatureNotInBuild,
            format!("`{clause}` requires build feature `{feature}`; not available in this build"),
            self.prev_span(),
        )
        .with_help(format!("build epistemic-graph with `--features {feature}`"))
    }

    pub(super) fn warn(&mut self, warning: UqlWarning) {
        self.warnings.push(warning);
    }

    pub(super) fn take_warnings(&mut self) -> Vec<UqlWarning> {
        std::mem::take(&mut self.warnings)
    }
}
