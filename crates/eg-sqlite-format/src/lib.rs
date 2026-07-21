//! Pure-Rust SQLite `.db` file-format reader + bulk-load writer — no C, no libsqlite3.
//!
//! This is a leaf crate at the bottom of the epistemic-graph DAG (positioned like
//! `eg-ann`): it knows nothing about `TableStore`/`Cell`/`ColumnType`, only SQLite bytes.
//! The facade `src/server/handlers/sqlite_file.rs` is the sole translator between the
//! engine's typed model and this crate's [`Value`]/[`Row`]/[`ColumnDef`] vocabulary.
//!
//! Scope: ordinary rowid table b-trees only. WITHOUT ROWID tables and index b-trees are
//! rejected as [`Error::Unsupported`], never mis-decoded. The [`Writer`] is a one-shot
//! bulk serializer (rows known in full, in advance, sorted by rowid) — deliberately NOT a
//! general mutable SQLite database. Format primitives reimplement the public SQLite file
//! format (the technique used by `open-source-libraries/turso/core/storage`).

mod error;
mod freelist;
mod header;
mod overflow;
mod page;
mod reader;
mod record;
mod schema;
mod value;
mod varint;
mod writer;

pub use error::{Error, Result};
pub use reader::Reader;
pub use value::{ColumnDef, Row, Value};
pub use writer::Writer;
