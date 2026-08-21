//! Durable, bounded secondary-index contracts for user tables.
//!
//! The graph engine already has a separate pgvector/ANN catalog.  This module is
//! deliberately narrower: it describes ordinary B-tree-like indexes over scalar
//! user-table columns and supplies the deterministic key encoding used by the
//! redb catalog and entry directory.  The catalog is owner-scoped by the
//! [`TableStore`](super::store::TableStore) handle; the scope is still part of
//! every durable key so a shared physical store cannot accidentally resolve an
//! index belonging to another tenant.
//!
//! An index is an optimization only.  A schema digest/version is stored with
//! every definition and a reader ignores stale or malformed definitions.  A
//! caller therefore falls back to a normal table scan rather than trusting a
//! directory that no longer describes the current rows.

use serde::{Deserialize, Serialize};

use super::schema::{Cell, ColumnType, TableSchema};

/// Version of the durable secondary-index definition and key encoding.
pub const SECONDARY_INDEX_SCHEMA_VERSION: u16 = 1;
/// Keep index construction and lookup bounded even when a caller supplies a
/// pathological DDL request.
pub const MAX_SECONDARY_INDEX_COLUMNS: usize = 4;
pub const MAX_SECONDARY_INDEXES_PER_TABLE: usize = 64;
pub const MAX_SECONDARY_INDEX_BUILD_ROWS: usize = 100_000;
pub const MAX_SECONDARY_INDEX_CANDIDATES: usize = 100_000;
pub const MAX_SECONDARY_INDEX_KEY_BYTES: usize = 16 * 1024;

/// The supported ordinary index family.  Keeping the enum closed prevents a
/// vector/ANN request from silently entering this scalar directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SecondaryIndexKind {
    BTree,
}

/// Direction used by the optional ordered-read contract.  Entry keys are
/// stored in ascending canonical order; descending callers reverse the
/// deterministic row-id result rather than changing the durable key format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SecondaryIndexOrder {
    Asc,
    Desc,
}

/// One column in a secondary-index definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecondaryIndexColumn {
    pub name: String,
    pub order: SecondaryIndexOrder,
}

impl SecondaryIndexColumn {
    pub fn ascending(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            order: SecondaryIndexOrder::Asc,
        }
    }
}

/// Durable identity and schema binding for an ordinary index.
///
/// `tenant_scope` is not inferred from a SQL index name.  It is supplied by the
/// owner-scoped store and is included in the catalog and entry keys.  A service
/// that multiplexes tenants in one redb file must open a scoped
/// `TableStore`; a scope mismatch is rejected before any catalog mutation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecondaryIndexSpec {
    pub tenant_scope: String,
    pub table: String,
    pub name: String,
    pub kind: SecondaryIndexKind,
    pub columns: Vec<SecondaryIndexColumn>,
    pub schema_version: u16,
    pub schema_digest: String,
}

impl SecondaryIndexSpec {
    /// Build a schema-bound B-tree definition.  Callers should use the
    /// `TableStore::secondary_index_spec` helper so the store's owner scope is
    /// selected rather than copied from untrusted SQL text.
    pub fn btree(
        tenant_scope: impl Into<String>,
        table: impl Into<String>,
        name: impl Into<String>,
        columns: Vec<SecondaryIndexColumn>,
        schema: &TableSchema,
    ) -> Result<Self, String> {
        let spec = Self {
            tenant_scope: tenant_scope.into(),
            table: table.into(),
            name: name.into(),
            kind: SecondaryIndexKind::BTree,
            columns,
            schema_version: SECONDARY_INDEX_SCHEMA_VERSION,
            schema_digest: schema.schema_digest()?,
        };
        validate_spec(&spec, schema)?;
        Ok(spec)
    }
}

/// Predicate shape that can be narrowed by the first column of a B-tree
/// definition.  DataFusion still re-applies the original expression above the
/// provider, so returning a candidate superset remains safe.
#[derive(Debug, Clone, PartialEq)]
pub enum SecondaryIndexLookup {
    Eq { column: String, value: Cell },
    Lt { column: String, value: Cell },
    Le { column: String, value: Cell },
    Gt { column: String, value: Cell },
    Ge { column: String, value: Cell },
}

impl SecondaryIndexLookup {
    pub fn column(&self) -> &str {
        match self {
            Self::Eq { column, .. }
            | Self::Lt { column, .. }
            | Self::Le { column, .. }
            | Self::Gt { column, .. }
            | Self::Ge { column, .. } => column,
        }
    }

    fn value(&self) -> &Cell {
        match self {
            Self::Eq { value, .. }
            | Self::Lt { value, .. }
            | Self::Le { value, .. }
            | Self::Gt { value, .. }
            | Self::Ge { value, .. } => value,
        }
    }
}

/// Validate a durable definition against the current schema.  This is called
/// both on CREATE and on every read; a corrupt/stale persisted definition is
/// therefore fail-closed to a scan.
pub fn validate_spec(spec: &SecondaryIndexSpec, schema: &TableSchema) -> Result<(), String> {
    schema.validate()?;
    if spec.schema_version != SECONDARY_INDEX_SCHEMA_VERSION {
        return Err(format!(
            "secondary index `{}` uses unsupported schema version {}",
            spec.name, spec.schema_version
        ));
    }
    if spec.table != schema.name {
        return Err(format!(
            "secondary index `{}` belongs to table `{}`, not `{}`",
            spec.name, spec.table, schema.name
        ));
    }
    if spec.tenant_scope.is_empty()
        || spec.tenant_scope.contains('\0')
        || spec.table.is_empty()
        || spec.table.contains('\0')
        || spec.name.is_empty()
        || spec.name.contains('\0')
    {
        return Err("secondary index scope, table, and name must be non-empty and NUL-free".into());
    }
    if spec.columns.is_empty() || spec.columns.len() > MAX_SECONDARY_INDEX_COLUMNS {
        return Err(format!(
            "secondary index `{}` must contain 1..={} columns",
            spec.name, MAX_SECONDARY_INDEX_COLUMNS
        ));
    }
    let digest = schema.schema_digest()?;
    if spec.schema_digest != digest {
        return Err(format!(
            "secondary index `{}` is stale for table `{}` (schema digest mismatch)",
            spec.name, spec.table
        ));
    }
    let mut seen = std::collections::HashSet::new();
    for indexed in &spec.columns {
        if indexed.order != SecondaryIndexOrder::Asc {
            return Err(format!(
                "secondary index `{}` only supports ascending key columns; use ordered read DESC for reverse pagination",
                spec.name
            ));
        }
        if !seen.insert(indexed.name.as_str()) {
            return Err(format!(
                "secondary index `{}` repeats column `{}`",
                spec.name, indexed.name
            ));
        }
        let column = schema.column(&indexed.name).ok_or_else(|| {
            format!(
                "secondary index `{}` references unknown column `{}`",
                spec.name, indexed.name
            )
        })?;
        if !is_indexable(column.ty) {
            return Err(format!(
                "secondary index `{}` cannot index vector, JSON, or array column `{}`",
                spec.name, indexed.name
            ));
        }
    }
    Ok(())
}

/// Scalar types have a stable total ordering and a bounded key representation.
/// Vectors remain exclusively in the ANN catalog; JSON/arrays are intentionally
/// unsupported rather than inventing a partial ordering.
pub fn is_indexable(ty: ColumnType) -> bool {
    matches!(
        ty,
        ColumnType::Int
            | ColumnType::BigInt
            | ColumnType::Float
            | ColumnType::Double
            | ColumnType::Text
            | ColumnType::Bool
            | ColumnType::Timestamp
            | ColumnType::TimestampTz
            | ColumnType::Bytes
            | ColumnType::Uuid
            | ColumnType::Numeric(_)
    )
}

/// Stable catalog key: scope + table + index name.  Each component is validated
/// by `validate_spec` before it reaches redb.
pub fn catalog_key(spec: &SecondaryIndexSpec) -> String {
    format!("{}\0{}\0{}", spec.tenant_scope, spec.table, spec.name)
}

/// Prefix of all physical entries belonging to a definition.
pub fn entry_prefix(spec: &SecondaryIndexSpec) -> String {
    format!("{}\0", catalog_key(spec))
}

/// Read the physical row id encoded at the end of an entry key.
pub fn rowid_from_entry_key(key: &str) -> Option<u64> {
    key.rsplit('\0').next()?.parse().ok()
}

/// Encode a row's indexed values into an order-preserving byte string.  The
/// store converts this to hex before using it as part of a redb string key.
pub fn row_key(
    spec: &SecondaryIndexSpec,
    schema: &TableSchema,
    cells: &[Cell],
) -> Result<Vec<u8>, String> {
    validate_spec(spec, schema)?;
    let mut out = Vec::new();
    for indexed in &spec.columns {
        let index = schema
            .column_index(&indexed.name)
            .ok_or_else(|| format!("indexed column `{}` is absent", indexed.name))?;
        let cell = cells.get(index).unwrap_or(&Cell::Null);
        encode_cell(&mut out, cell, schema.column(&indexed.name).unwrap().ty)?;
    }
    if out.len() > MAX_SECONDARY_INDEX_KEY_BYTES {
        return Err(format!(
            "secondary index `{}` key exceeds {} bytes",
            spec.name, MAX_SECONDARY_INDEX_KEY_BYTES
        ));
    }
    Ok(out)
}

/// Construct a physical entry key.  A fixed-width row id makes iteration
/// deterministic for equal indexed values while the encoded value remains the
/// primary B-tree order key.
pub fn entry_key(
    spec: &SecondaryIndexSpec,
    schema: &TableSchema,
    cells: &[Cell],
    rowid: u64,
) -> Result<String, String> {
    let encoded = hex::encode(row_key(spec, schema, cells)?);
    Ok(format!("{}{}\0{rowid:020}", entry_prefix(spec), encoded))
}

/// Compute an inclusive/exclusive lexical range over entry keys for a lookup on
/// the first indexed column.  A high Unicode sentinel includes all composite
/// suffixes for an inclusive bound while remaining above the NUL row-id
/// separator.  Unsupported shapes return `None`, which is the explicit scan
/// fallback contract.
pub fn entry_range(
    spec: &SecondaryIndexSpec,
    schema: &TableSchema,
    lookup: &SecondaryIndexLookup,
) -> Result<Option<(String, String)>, String> {
    validate_spec(spec, schema)?;
    if spec.columns.first().map(|c| c.name.as_str()) != Some(lookup.column()) {
        return Ok(None);
    }
    let ty = schema
        .column(lookup.column())
        .ok_or_else(|| format!("indexed column `{}` is absent", lookup.column()))?
        .ty;
    let mut component = Vec::new();
    encode_cell(&mut component, lookup.value(), ty)?;
    let encoded = hex::encode(component);
    let prefix = entry_prefix(spec);
    let low = format!("{prefix}{encoded}");
    let high = format!("{prefix}{encoded}\u{10ffff}");
    let all_low = prefix.clone();
    let all_high = format!("{prefix}\u{10ffff}");
    let bounds = match lookup {
        SecondaryIndexLookup::Eq { .. } => (low, high),
        SecondaryIndexLookup::Lt { .. } => (all_low, low),
        SecondaryIndexLookup::Le { .. } => (all_low, high),
        SecondaryIndexLookup::Gt { .. } => (high, all_high),
        SecondaryIndexLookup::Ge { .. } => (low, all_high),
    };
    Ok(Some(bounds))
}

fn encode_cell(out: &mut Vec<u8>, cell: &Cell, ty: ColumnType) -> Result<(), String> {
    // A NUL-escaped, terminated component preserves lexical order for strings
    // and gives a prefix range for composite indexes.  Null is indexed but no
    // SQL NULL predicate is planned here; it remains available to future IS NULL
    // support without changing the durable encoding.
    let (tag, payload): (u8, Vec<u8>) = match (cell, ty) {
        (Cell::Null, _) => (0x00, Vec::new()),
        (Cell::Int(value), ColumnType::Int | ColumnType::BigInt) => (
            0x10,
            ((*value as u64) ^ (1u64 << 63)).to_be_bytes().to_vec(),
        ),
        (Cell::Float(value), ColumnType::Float | ColumnType::Double | ColumnType::Numeric(_)) => {
            if !value.is_finite() {
                return Err("non-finite floating values are not indexable".into());
            }
            // SQL equality treats -0.0 and +0.0 as the same value; normalize
            // the sign before constructing the sortable representation.
            let canonical = if *value == 0.0 { 0.0 } else { *value };
            let bits = canonical.to_bits();
            let sortable = if bits & (1u64 << 63) != 0 {
                !bits
            } else {
                bits ^ (1u64 << 63)
            };
            (0x11, sortable.to_be_bytes().to_vec())
        }
        (Cell::Timestamp(value), ColumnType::Timestamp | ColumnType::TimestampTz) => (
            0x12,
            ((*value as u64) ^ (1u64 << 63)).to_be_bytes().to_vec(),
        ),
        (Cell::Text(value), ColumnType::Text | ColumnType::Uuid) => {
            (0x20, value.as_bytes().to_vec())
        }
        (Cell::Bool(value), ColumnType::Bool) => (0x30, vec![u8::from(*value)]),
        (Cell::Bytes(value), ColumnType::Bytes) => (0x40, value.clone()),
        _ => {
            return Err(format!(
                "cell shape does not match indexable column type {ty:?}"
            ))
        }
    };
    out.push(tag);
    for byte in payload {
        if byte == 0 {
            out.extend_from_slice(&[0, 0xff]);
        } else {
            out.push(byte);
        }
    }
    out.extend_from_slice(&[0, 0]);
    Ok(())
}
