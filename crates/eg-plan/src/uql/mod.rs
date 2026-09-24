//! UQL — the Unified Query Language front-end (CONCEPT:AU-KG.query.top-nodes-by-degree).
//!
//! A human/agent-writable TEXT surface that parses to the SAME [`eg_types::wire::Plan`]
//! AST `Method::UnifiedQuery` already executes — so one query expresses filter
//! (relational) + traverse (graph) + rank (vector) + every other modality without a
//! caller hand-building the Op list. A pure FRONT-END: it adds no executor.
//!
//! * [`grammar`] — THE grammar (one table; the docs, the NL prompt and error lists are
//!   generated from it);
//! * [`parse`] / [`parse_with`] / [`parse_statement`] — text → plan, with typed `$name`
//!   parameters bound as values (never spliced into text);
//! * [`eg_types::wire::Plan::to_uql`] / [`print`] — plan → canonical text; for every
//!   printable plan `parse(print(p)) == canonicalize(p)`;
//! * [`UqlError`] — structured diagnostics (stable code, span, expected set, fix hint).
//!
//! The front-end is DEPENDENCY-FREE (no DataFusion, no regex), so it ships in a
//! default/Pi build — only EXECUTION of the resulting Plan is `query`-gated.
//!
//! ```
//! use eg_plan::uql;
//! use eg_types::wire::{Op, Pred};
//!
//! let plan = uql::parse(
//!     "MATCH (:Doc) WHERE year > 2024 |> TRAVERSE -[:CITES]->{1,2} \
//!      |> RANK BY ~[1.0, 0.0] |> LIMIT 10",
//! )
//! .unwrap();
//! assert_eq!(plan.ops[0], Op::Scan { label: "Doc".into() });
//! assert_eq!(
//!     plan.ops[1],
//!     Op::Filter { preds: vec![Pred::GtNum { prop: "year".into(), n: 2024.0 }] }
//! );
//! ```

use std::collections::BTreeMap;

use eg_types::wire::{Plan, UqlParam};

mod diag;
pub mod grammar;
mod lexer;
mod parser;
pub mod print;
/// Running a parsed statement: rows with score channels, EXPLAIN, PROFILE (UQL-07/08/09).
#[cfg(feature = "query")]
pub mod serve;

pub use diag::{UqlCode, UqlError, UqlWarnCode, UqlWarning, ALL_CODES};
#[cfg(feature = "query")]
pub use parser::program::to_plan_dag;
pub use parser::program::{Annotations, Body, DagNode, Mode, Statement, UQL_VERSION};
pub use print::canonicalize;

/// Typed parameter bindings: `$name` → value.
pub type Params = BTreeMap<String, UqlParam>;

/// Parse a full statement (version pragma, `EXPLAIN`/`PROFILE`, `LET` bindings) with
/// typed parameters. Every bound parameter must be referenced.
pub fn parse_statement(src: &str, params: &Params) -> Result<Statement, UqlError> {
    let tokens = lexer::lex(src)?;
    parser::Parser::new(src, &tokens, params).statement()
}

/// Parse a plain pipeline with typed parameters into the [`Plan`] `UnifiedQuery` runs.
/// `EXPLAIN`/`PROFILE`, DAG programs and `WITH …` annotations are refused
/// (`UQL_STATEMENT_NOT_PIPELINE`) — use [`parse_statement`] for those.
pub fn parse_with(src: &str, params: &Params) -> Result<Plan, UqlError> {
    let stmt = parse_statement(src, params)?;
    match (stmt.mode, stmt.body, stmt.annotations.any()) {
        (Mode::Run, Body::Pipeline(plan), false) => Ok(plan),
        _ => Err(UqlError::new(
            UqlCode::StatementNotPipeline,
            "this is an EXPLAIN/PROFILE statement, a LET … FROM/JOIN program or an annotated \
             (`WITH …`) statement, not a plain pipeline",
            (0, src.len().min(1)),
        )
        .with_help("use `eg_plan::uql::parse_statement`")),
    }
}

/// Parse a plain pipeline (no parameters) into the [`Plan`] AST.
pub fn parse(src: &str) -> Result<Plan, UqlError> {
    parse_with(src, &Params::new())
}

#[cfg(test)]
mod tests;
