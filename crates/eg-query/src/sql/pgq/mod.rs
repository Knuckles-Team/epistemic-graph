//! Bounded PostgreSQL 19 / SQL:2023 SQL/PGQ parser and relational lowerer.
//!
//! Property-graph definitions remain catalog metadata over ordinary relational
//! tables. Fixed graph patterns lower into relational SQL and execute only on
//! the existing DataFusion path; this module owns no executor or compatibility
//! grammar.

mod ast;
mod label;
mod lex;
mod lower;
mod parse;

pub use ast::*;
pub use label::LabelExpr;
pub use lex::{SqlNumber, MAX_PGQ_SQL_BYTES};
pub use lower::{lower_graph_table, lower_graph_table_to_datafusion, RelationalGraphPlan};
pub use parse::parse_property_graph_ddl;

pub(crate) use lex::{is_graph_table_sql, is_property_graph_ddl};
