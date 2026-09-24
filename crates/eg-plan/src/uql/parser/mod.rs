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
//!    `RETURN`, `FOREIGN`, `DECISIONS`;
//!  * `semantic` — `TEXT`/`FUSE` (text), `REASON`/`SPARQL` (owl), `UDF` (wasm-udf) and the
//!    epistemic stages;
//!  * `modality` — spatial, tensor, time-series/sensor, CEP and probabilistic stages;
//!  * `attribute` — `ATTRIBUTE` (numeric: contribution attribution, EH-523);
//!  * `program` — statements: `UQL n;`, `EXPLAIN`/`PROFILE`, `LET` bindings, `FROM`/`JOIN`,
//!    and the `WITH PROOF` / `WITH KNOWLEDGE` row annotations.
//!
//! Feature policy (UQL-10, one rule everywhere): every clause is RECOGNIZED by its
//! leading keyword in every build; a clause whose executor needs a cargo feature this
//! build lacks is refused at parse time with `UQL_FEATURE_NOT_IN_BUILD` naming the
//! feature. Dependency-free (no DataFusion, no regex).

use std::collections::{BTreeMap, BTreeSet};

use eg_types::wire::Op;

use super::diag::{UqlCode, UqlError, UqlWarning};
use super::lexer::Token;
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

mod attribute;
mod basic;
mod cursor;
mod lists;
mod literal;
mod modality;
mod pred;
pub(super) mod program;
mod rank;
mod report;
mod semantic;
mod series;
mod shape;
mod traverse;

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
    /// `DERIVE … AS name` aliases declared so far (a later `RETURN`/`DERIVE` may name them).
    derived: BTreeSet<String>,
    /// A derived-series definition is being parsed: its sources bind the channel names.
    pub(super) free_channels: bool,
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
            derived: BTreeSet::new(),
            free_channels: false,
        }
    }

    /// A series value channel: `v0..vk` (a `TSSCAN` field, EH-521) or a declared
    /// `DERIVE` alias.
    pub(super) fn is_value_channel(&self, name: &str) -> bool {
        let field = name
            .strip_prefix('v')
            .is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()));
        field || self.derived.contains(name)
    }

    /// Every stage/source keyword → its parser. MATCH (it may lower to two ops) and
    /// `VALIDATE SHAPE` (the two-keyword lead `parser::shape` owns, so that every other
    /// `VALIDATE …` still reaches the DecideText refusal) are handled by [`Self::stage`].
    pub(super) fn stage_table() -> [(&'static str, StageFn<'a>); 33] {
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
            ("DECISIONS", Self::decisions),
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
            ("DERIVE", Self::derive),
            ("SKILL", Self::skill),
            ("PROB", Self::prob),
            ("ATTRIBUTE", Self::attribute),
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
}
