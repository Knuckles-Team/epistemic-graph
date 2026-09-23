//! DecideText: a UQL-like text front end for decisions (EH-073,
//! DECIDE-LAYER-DESIGN §5).
//!
//! It is deliberately NOT part of UQL and never produces a `wire::Op`: adding
//! decision stages to the shared `wire::Plan` would move the request-schema
//! digest of every method that embeds a plan and let `DECIDE` appear inside a
//! clustering plan or a `UnifiedQueryText`. Instead a DecideText source parses
//! to a typed [`DecideTextRequest`] -- a `DecideRequest` or an
//! `AssemblyRequest` -- and the ordinary UQL parser refuses the decision
//! clauses with a typed `DECISION_CLAUSE_IN_UQL` error.
//!
//! Parameters are typed and bound by name (`@name`), never substituted into
//! text: a parameter can never become part of a query string.
//!
//! ```text
//! decide_text = candidates { "|>" clause } ;
//! candidates  = "CANDIDATES" ( "AGENT" "LIBRARY" "KINDS" "[" kind { "," kind } "]" [ "UNDER" word ]
//!                            | "GRAPH" string "QUERY" "{" uql "}" ) ;
//! clause      = "COVERS" param
//!             | "VALIDATE" "POLICY" ( "DEFAULT" | pin )
//!             | "DECIDE" question_kind "QUESTION" string [ "SAFETY" safety ]
//!                        "FEATURES" pin [ "HEAD" pin ] [ "MAX" int ]
//!             | "ASSEMBLE" [ "MAX" "COMPONENTS" int ] ;
//! pin         = string "AT" string ;            (* component id, definition digest *)
//! param       = "@" ident ;
//! ```
//!
//! Exactly one `DECIDE` or `ASSEMBLE` clause, and it comes last.

mod lexer;
mod parser;
#[cfg(test)]
mod tests;

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

/// A typed DecideText error at a byte offset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecideTextError {
    pub kind: DecideTextErrorKind,
    pub msg: String,
    pub at: usize,
}

impl DecideTextError {
    pub(crate) fn new(kind: DecideTextErrorKind, msg: impl Into<String>, at: usize) -> Self {
        Self {
            kind,
            msg: msg.into(),
            at,
        }
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

/// Parse `src` for `tenant_id`, binding `@name` parameters from `params`.
pub fn parse(
    src: &str,
    tenant_id: &str,
    params: &BTreeMap<String, TypedValue>,
) -> Result<DecideTextRequest, DecideTextError> {
    let tokens = lexer::lex(src)?;
    parser::Parser::new(&tokens, src.len(), tenant_id, params).parse()
}
