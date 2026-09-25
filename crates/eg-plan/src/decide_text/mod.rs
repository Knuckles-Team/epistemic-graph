//! DecideText: a UQL-like text front end for decisions (EH-073,
//! DECIDE-LAYER-DESIGN §5).
//!
//! It is deliberately NOT part of UQL and never produces a `wire::Op`: adding
//! decision stages to the shared `wire::Plan` would move the request-schema
//! digest of every method that embeds a plan and let `DECIDE` appear inside a
//! clustering plan or a UQL query. Instead a DecideText source parses
//! to a typed [`DecideTextRequest`] -- a `DecideRequest` or an
//! `AssemblyRequest` -- and the ordinary UQL parser refuses the decision
//! clauses with a typed `DECISION_CLAUSE_IN_UQL` error.
//!
//! It shares UQL's lexical family (EH-452): the UQL lexer, `$name` parameters (typed,
//! bound by name, never substituted into text — a parameter can never become part of a
//! query string), structured errors with a span, an expected set and a fix hint (a
//! candidate query's own UQL diagnostic is kept, pointing into the DecideText source),
//! and a grammar table ([`grammar::PRODUCTIONS`], the same `Production` shape as UQL's)
//! from which the reference EBNF and the clause error lists are generated.
//!
//! ```text
//! decide_text = candidates { "|>" clause } ;
//! candidates  = "CANDIDATES" ( "AGENT" "LIBRARY" "KINDS" "[" kind { "," kind } "]"
//!                              [ "UNDER" ( iri | string ) ]
//!                            | "GRAPH" string "QUERY" "{" uql "}" ) ;
//! clause      = "COVERS" param
//!             | "VALIDATE" "POLICY" ( "DEFAULT" | pin )
//!             | "DECIDE" question_kind "QUESTION" string [ "SAFETY" safety ]
//!                        "FEATURES" pin [ "HEAD" pin ] [ "MAX" int ]
//!             | "ASSEMBLE" [ "MAX" "COMPONENTS" int ] ;
//! pin         = string "AT" string ;            (* component id, definition digest *)
//! param       = "$" name ;
//! ```
//!
//! Exactly one `DECIDE` or `ASSEMBLE` clause, and it comes last.

pub mod grammar;
mod lexer;
mod parser;
mod replay;
#[cfg(test)]
mod replay_tests;
#[cfg(test)]
mod tests;

pub use replay::{parse_replay, ReplayBindings};

use std::collections::BTreeMap;

use eg_types::decision::statistical::{DecideRequest, TypedValue};
use eg_types::decision::AssemblyRequest;

/// What went wrong, as a closed kind a caller can branch on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecideTextErrorKind {
    Syntax,
    /// `@name` is referenced but not bound.
    UnboundParameter,
    /// A bound parameter has the wrong type for where it is used.
    ParameterType,
    /// No terminal `DECIDE` or `ASSEMBLE` clause, or more than one.
    MissingDecision,
    /// The candidate query did not parse as UQL.
    CandidateQuery,
}

impl DecideTextErrorKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Syntax => "DECIDE_TEXT_SYNTAX",
            Self::UnboundParameter => "DECIDE_TEXT_UNBOUND_PARAMETER",
            Self::ParameterType => "DECIDE_TEXT_PARAMETER_TYPE",
            Self::MissingDecision => "DECIDE_TEXT_MISSING_DECISION",
            Self::CandidateQuery => "DECIDE_TEXT_CANDIDATE_QUERY",
        }
    }
}

/// A typed DecideText error: a closed kind, a span, what would have been accepted and
/// a fix hint — the shape of [`crate::uql::UqlError`] — plus, for a candidate query that
/// is not valid UQL, the UQL diagnostic itself (its span rebased into this source).
#[derive(Debug, Clone, PartialEq)]
pub struct DecideTextError {
    pub kind: DecideTextErrorKind,
    pub msg: String,
    /// Start byte offset of the offending text.
    pub at: usize,
    /// End byte offset (exclusive).
    pub end: usize,
    pub expected: Vec<String>,
    pub help: Option<String>,
    /// The embedded UQL error, for a candidate query or a lexical error.
    pub cause: Option<Box<crate::uql::UqlError>>,
}

impl DecideTextError {
    pub(crate) fn new(
        kind: DecideTextErrorKind,
        msg: impl Into<String>,
        span: (usize, usize),
    ) -> Self {
        Self {
            kind,
            msg: msg.into(),
            at: span.0,
            end: span.1,
            expected: Vec::new(),
            help: None,
            cause: None,
        }
    }

    pub(crate) fn expecting(mut self, expected: Vec<String>) -> Self {
        self.expected = expected;
        self
    }

    pub(crate) fn with_help(mut self, help: impl Into<String>) -> Self {
        self.help = Some(help.into());
        self
    }

    pub(crate) fn caused_by(mut self, cause: crate::uql::UqlError) -> Self {
        self.cause = Some(Box::new(cause));
        self
    }

    /// A `line:column` caret diagnostic against `src`, rendered exactly like a UQL one;
    /// a candidate query's error renders its own UQL code and caret.
    pub fn render(&self, src: &str) -> String {
        if let Some(cause) = &self.cause {
            return format!("{}: {}", self.kind.as_str(), cause.render(src));
        }
        crate::uql::render_caret(
            self.kind.as_str(),
            &self.msg,
            (self.at, self.end),
            &self.expected,
            self.help.as_deref(),
            src,
        )
    }
}

impl std::fmt::Display for DecideTextError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}: {} (at byte {})",
            self.kind.as_str(),
            self.msg,
            self.at
        )
    }
}

/// What a DecideText source asks for.
#[derive(Debug, Clone, PartialEq)]
pub enum DecideTextRequest {
    Decide(Box<DecideRequest>),
    Assemble(Box<AssemblyRequest>),
}

/// Parse `src` for `tenant_id`, binding `$name` parameters from `params`.
pub fn parse(
    src: &str,
    tenant_id: &str,
    params: &BTreeMap<String, TypedValue>,
) -> Result<DecideTextRequest, DecideTextError> {
    let tokens = lexer::lex(src)?;
    parser::Parser::new(&tokens, src.len(), tenant_id, params).parse()
}
