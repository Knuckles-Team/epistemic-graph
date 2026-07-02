//! Cypher surface (CONCEPT:KG-2.179): a read-only `MATCH … WHERE … RETURN …
//! LIMIT k` over ONE graph, compiled to the engine's OWN primitives — NO
//! DataFusion. This is the lean-Pi query path: the whole module pulls only the
//! deps a default eg-query build already has (serde / serde_json / eg-core).
//!
//! Pipeline (parser → plan → exec), mirroring the `sql` module's split:
//!   * [`parser`] — a hand-written recursive-descent parser for the Cypher subset
//!     into the [`plan::CypherQuery`] AST.
//!   * [`plan`] — the AST types (pattern nodes/edges, WHERE predicates, RETURN
//!     items, LIMIT).
//!   * [`exec`] — runs the plan over an off-lock `GraphView`, dispatching each
//!     pattern shape to the cheapest primitive:
//!       - a label predicate → the eg-core label index;
//!       - a fixed-shape multi-hop → a synthesized pattern `GraphView` fed to
//!         `eg_core::graph::vf2_match_views`;
//!       - a variable-length path `*a..b` → petgraph BFS over the `GraphView`.
//!
//! The result is the SAME `QueryResult { columns, rows }` carrier the SQL surface
//! returns (rows are msgpack-encoded `Vec<serde_json::Value>` aligned to columns),
//! so the protocol embeds no new payload variant and the Python client decodes it
//! identically.

mod exec;
mod parser;
mod plan;
mod proc;

pub use exec::{
    exec_cypher, exec_cypher_params, exec_cypher_write, exec_cypher_write_params, Params,
};
pub use plan::{CypherQuery, Statement, WriteOp, WriteQuery};
pub use proc::{CypherProcedure, ProcRow, YieldValue};
