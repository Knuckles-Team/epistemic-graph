//! Cypher surface (CONCEPT:EG-KG.query.dep-free-behind): a read-only `MATCH … WHERE … RETURN …
//! LIMIT k` over ONE graph, compiled to the engine's OWN primitives — NO
//! DataFusion. This is the lean-Pi query path: the whole module pulls only the
//! deps a default eg-query build already has (serde / serde_json / eg-core).
//!
//! Pipeline (parser → plan → exec), mirroring the `sql` module's split:
//!   * [`parser`] — a hand-written recursive-descent parser for the Cypher subset
//!     into the [`plan::CypherQuery`] AST.
//!   * [`plan`] — the AST types (pattern nodes/edges, WHERE predicates, RETURN
//!     items, LIMIT).
//!   * [`exec`] — runs the plan over an off-lock `GraphView` as an incremental
//!     neighbour-walk (`resolve_match`/`walk_hops`): start from the label-index
//!     candidates (narrowed by any inline `{k: v}` property constraints —
//!     applied uniformly whether or not a node position is named,
//!     CONCEPT:EG-KG.query.anon-propmap-parity), then extend hop by hop —
//!       - a fixed hop → relationship-typed neighbours;
//!       - a variable-length hop `*a..b` → petgraph BFS over the `GraphView`;
//!       - a Cypher 25 quantified group `((…)){m,n}` → repeated whole-subpattern
//!         expansion.
//!     `eg_core::graph::vf2_match_views` is not on this path; it remains only a
//!     cross-check oracle in the test suite.
//!
//! The result is the SAME `QueryResult { columns, rows }` carrier the SQL surface
//! returns (rows are msgpack-encoded `Vec<serde_json::Value>` aligned to columns),
//! so the protocol embeds no new payload variant and the Python client decodes it
//! identically.

mod exec;
mod gds;
mod parser;
mod plan;
mod proc;

pub use exec::{
    exec_cypher, exec_cypher_params, exec_cypher_write, exec_cypher_write_params, Params,
};
pub use plan::{CypherQuery, Statement, WriteOp, WriteQuery};
pub use proc::{CypherProcedure, ProcRow, YieldValue};

/// Parser-derived statement classification used by served authorization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CypherStatementKind {
    Read,
    Write,
}

/// Parse and classify one complete statement without executing it.
pub fn classify_cypher(input: &str) -> Result<CypherStatementKind, String> {
    match parser::parse_statement(input)? {
        Statement::Read(_) => Ok(CypherStatementKind::Read),
        Statement::Write(_) => Ok(CypherStatementKind::Write),
    }
}
