//! The SQLite storage-class vocabulary shared by the reader and writer.

/// A single SQLite value (one of the five storage classes).
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

/// A decoded/encodable table row (column values in column order).
pub type Row = Vec<Value>;

/// A column declaration parsed from / emitted into `CREATE TABLE` SQL.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    /// The declared type text (may be empty), used for SQLite affinity mapping.
    pub decl_type: String,
}
