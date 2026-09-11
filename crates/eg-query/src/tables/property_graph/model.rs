//! SQL:2023 SQL/PGQ property-graph catalog model.
//!
//! A property graph is a read-only view over ordinary relational tables.  This
//! module therefore stores only the definition; it owns neither graph data nor
//! query execution.  The companion `sql::pgq` module lowers a bounded
//! `GRAPH_TABLE` pattern to ordinary relational SQL for the existing DataFusion
//! path.
//!
//! Catalog records are tenant-scoped, structurally bounded, canonically ordered,
//! and digest-stamped.  Identifiers are structured values, never SQL fragments.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::validate::{
    validate_definition_header, validate_edge_tables, validate_shared_labels,
    validate_vertex_tables, EdgeTableDefinition, VertexTableDefinition,
};

pub const PROPERTY_GRAPH_SCHEMA_VERSION: u16 = 1;
pub const MAX_PROPERTY_GRAPH_RECORD_BYTES: usize = 256 * 1024;
pub const MAX_PROPERTY_GRAPH_ELEMENTS: usize = 256;
pub const MAX_PROPERTY_GRAPH_LABELS_PER_ELEMENT: usize = 32;
pub const MAX_PROPERTY_GRAPH_PROPERTIES_PER_LABEL: usize = 256;
pub const MAX_PROPERTY_GRAPH_KEY_COLUMNS: usize = 32;
pub const MAX_SQL_IDENTIFIER_BYTES: usize = 63;
pub const MAX_QUALIFIED_NAME_PARTS: usize = 3;
pub const MAX_TENANT_SCOPE_BYTES: usize = 512;
pub const PROPERTY_GRAPH_CATALOG_SCHEMA_VERSION: u16 = 2;
pub const MAX_PROPERTY_GRAPH_CATALOG_RECORD_BYTES: usize = 512 * 1024;
pub const MAX_CATALOG_ID_BYTES: usize = 256;
pub const MAX_CATALOG_OWNER_BYTES: usize = 256;
/// Explicit `SELECT` principals carried by one property-graph catalog record.
/// The owner and an engine administrator are implicit and do not consume rows.
pub const MAX_PROPERTY_GRAPH_SELECT_GRANTEES: usize = 1024;

/// A tenant-scoped SQL name. Tenant is an opaque security scope, not an SQL
/// identifier. An unqualified name resolves to the tenant's `public` schema.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalCatalogName {
    pub tenant_scope: String,
    pub catalog: Option<SqlIdentifier>,
    pub schema: SqlIdentifier,
    pub object: SqlIdentifier,
}

impl CanonicalCatalogName {
    pub fn resolve(tenant_scope: &str, name: &SqlName) -> Result<Self, String> {
        validate_bounded_text(tenant_scope, "tenant scope", MAX_TENANT_SCOPE_BYTES)?;
        name.validate()?;
        let public = || SqlIdentifier::unquoted("public");
        match name.0.as_slice() {
            [object] => Ok(Self {
                tenant_scope: tenant_scope.into(),
                catalog: None,
                schema: public()?,
                object: object.clone(),
            }),
            [schema, object] => Ok(Self {
                tenant_scope: tenant_scope.into(),
                catalog: None,
                schema: schema.clone(),
                object: object.clone(),
            }),
            [catalog, schema, object] => Ok(Self {
                tenant_scope: tenant_scope.into(),
                catalog: Some(catalog.clone()),
                schema: schema.clone(),
                object: object.clone(),
            }),
            _ => Err("SQL catalog name must have one, two, or three parts".into()),
        }
    }

    pub fn sql_name(&self) -> Result<SqlName, String> {
        let mut parts = Vec::with_capacity(if self.catalog.is_some() { 3 } else { 2 });
        if let Some(catalog) = &self.catalog {
            parts.push(catalog.clone());
        }
        parts.push(self.schema.clone());
        parts.push(self.object.clone());
        SqlName::new(parts)
    }

    pub(super) fn validate(&self) -> Result<(), String> {
        validate_bounded_text(&self.tenant_scope, "tenant scope", MAX_TENANT_SCOPE_BYTES)?;
        if let Some(catalog) = &self.catalog {
            catalog.validate()?;
        }
        self.schema.validate()?;
        self.object.validate()
    }
}

/// One SQL identifier with PostgreSQL folding already applied.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqlIdentifier {
    value: String,
}

impl SqlIdentifier {
    pub fn unquoted(value: impl Into<String>) -> Result<Self, String> {
        let value = value.into().to_ascii_lowercase();
        validate_identifier_value(&value)?;
        if !is_unquoted_identifier(&value) {
            return Err(format!("invalid unquoted SQL identifier `{value}`"));
        }
        if is_reserved_keyword(&value) {
            return Err(format!("reserved word requires SQL quoting: {value}"));
        }
        Ok(Self { value })
    }

    pub fn quoted(value: impl Into<String>) -> Result<Self, String> {
        let value = value.into();
        validate_identifier_value(&value)?;
        Ok(Self { value })
    }

    pub fn value(&self) -> &str {
        &self.value
    }

    /// Render a safe SQL identifier.  Every caller that produces relational SQL
    /// must use this method; raw catalog text is never interpolated.
    pub fn quoted_sql(&self) -> String {
        format!("\"{}\"", self.value.replace('"', "\"\""))
    }

    pub fn validate(&self) -> Result<(), String> {
        validate_identifier_value(&self.value)?;
        Ok(())
    }
}

/// A one-, two-, or three-part SQL name (`relation`, `schema.relation`, or
/// `catalog.schema.relation`).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SqlName(pub Vec<SqlIdentifier>);

impl SqlName {
    pub fn new(parts: Vec<SqlIdentifier>) -> Result<Self, String> {
        let name = Self(parts);
        name.validate()?;
        Ok(name)
    }

    pub fn leaf(&self) -> &SqlIdentifier {
        self.0.last().expect("validated SQL name is non-empty")
    }

    pub fn quoted_sql(&self) -> String {
        self.0
            .iter()
            .map(SqlIdentifier::quoted_sql)
            .collect::<Vec<_>>()
            .join(".")
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.0.is_empty() || self.0.len() > MAX_QUALIFIED_NAME_PARTS {
            return Err(format!(
                "SQL name must contain 1..={MAX_QUALIFIED_NAME_PARTS} identifiers"
            ));
        }
        self.0.iter().try_for_each(SqlIdentifier::validate)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PropertySet {
    None,
    AllColumns,
    Explicit(Vec<PropertyDefinition>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PropertyDefinition {
    /// The source column.  PostgreSQL also permits general expressions; this
    /// bounded first slice intentionally accepts column references only, so a
    /// stored definition can never become an unchecked SQL fragment.
    pub source_column: SqlIdentifier,
    pub property_name: SqlIdentifier,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LabelDefinition {
    pub name: SqlIdentifier,
    pub properties: PropertySet,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PropertyGraphDefinition {
    pub schema_version: u16,
    pub tenant_scope: String,
    pub name: SqlName,
    pub temporary: bool,
    pub vertex_tables: Vec<VertexTableDefinition>,
    pub edge_tables: Vec<EdgeTableDefinition>,
    pub digest: String,
}

#[derive(Serialize)]
struct DefinitionPayload<'a> {
    schema_version: u16,
    tenant_scope: &'a str,
    name: &'a SqlName,
    temporary: bool,
    vertex_tables: &'a [VertexTableDefinition],
    edge_tables: &'a [EdgeTableDefinition],
}

impl PropertyGraphDefinition {
    pub fn new(
        tenant_scope: impl Into<String>,
        name: SqlName,
        temporary: bool,
        vertex_tables: Vec<VertexTableDefinition>,
        edge_tables: Vec<EdgeTableDefinition>,
    ) -> Result<Self, String> {
        let mut definition = Self {
            schema_version: PROPERTY_GRAPH_SCHEMA_VERSION,
            tenant_scope: tenant_scope.into(),
            name,
            temporary,
            vertex_tables,
            edge_tables,
            digest: String::new(),
        };
        definition.canonicalize();
        definition.validate_shape()?;
        definition.digest = definition.compute_digest()?;
        Ok(definition)
    }

    pub fn validate(&self) -> Result<(), String> {
        self.validate_shape()?;
        let expected = self.compute_digest()?;
        if self.digest != expected {
            return Err(format!(
                "property graph `{}` digest mismatch: stored `{}`, computed `{expected}`",
                self.name.quoted_sql(),
                self.digest
            ));
        }
        let mut canonical = self.clone();
        canonical.canonicalize();
        if canonical.vertex_tables != self.vertex_tables
            || canonical.edge_tables != self.edge_tables
        {
            return Err("property graph catalog record is not canonically ordered".into());
        }
        Ok(())
    }

    pub fn canonical_bytes(&self) -> Result<Vec<u8>, String> {
        self.validate()?;
        serde_json::to_vec(self).map_err(|error| format!("encode property graph: {error}"))
    }

    fn payload(&self) -> DefinitionPayload<'_> {
        DefinitionPayload {
            schema_version: self.schema_version,
            tenant_scope: &self.tenant_scope,
            name: &self.name,
            temporary: self.temporary,
            vertex_tables: &self.vertex_tables,
            edge_tables: &self.edge_tables,
        }
    }

    fn compute_digest(&self) -> Result<String, String> {
        let payload = serde_json::to_vec(&self.payload())
            .map_err(|error| format!("encode property graph digest payload: {error}"))?;
        let mut hasher = Sha256::new();
        hasher.update(b"epistemic-graph/sql-pgq/property-graph/v1\0");
        hasher.update(payload);
        Ok(hex::encode(hasher.finalize()))
    }

    fn canonicalize(&mut self) {
        for vertex in &mut self.vertex_tables {
            canonicalize_labels(&mut vertex.labels);
        }
        for edge in &mut self.edge_tables {
            canonicalize_labels(&mut edge.labels);
        }
        self.vertex_tables.sort_by(|a, b| a.alias.cmp(&b.alias));
        self.edge_tables.sort_by(|a, b| a.alias.cmp(&b.alias));
    }

    fn validate_shape(&self) -> Result<(), String> {
        validate_definition_header(self)?;
        let (mut aliases, vertices) = validate_vertex_tables(&self.vertex_tables)?;
        validate_edge_tables(&self.edge_tables, &vertices, &mut aliases)?;
        validate_shared_labels(self)
    }

    pub fn vertex_by_alias(&self, alias: &SqlIdentifier) -> Option<&VertexTableDefinition> {
        self.vertex_tables
            .iter()
            .find(|table| &table.alias == alias)
    }

    pub fn edge_by_alias(&self, alias: &SqlIdentifier) -> Option<&EdgeTableDefinition> {
        self.edge_tables.iter().find(|table| &table.alias == alias)
    }
}

pub fn decode_property_graph(bytes: &[u8]) -> Result<PropertyGraphDefinition, String> {
    if bytes.is_empty() || bytes.len() > MAX_PROPERTY_GRAPH_RECORD_BYTES {
        return Err(format!(
            "property graph record must contain 1..={MAX_PROPERTY_GRAPH_RECORD_BYTES} bytes"
        ));
    }
    let definition: PropertyGraphDefinition =
        serde_json::from_slice(bytes).map_err(|error| format!("decode property graph: {error}"))?;
    definition.validate()?;
    if definition.canonical_bytes()? != bytes {
        return Err("property graph record is not canonical JSON".into());
    }
    Ok(definition)
}

fn canonicalize_labels(labels: &mut [LabelDefinition]) {
    for label in labels.iter_mut() {
        if let PropertySet::Explicit(properties) = &mut label.properties {
            properties.sort_by(|a, b| a.property_name.cmp(&b.property_name));
        }
    }
    labels.sort_by(|a, b| a.name.cmp(&b.name));
}

fn validate_identifier_value(value: &str) -> Result<(), String> {
    validate_bounded_text(value, "SQL identifier", MAX_SQL_IDENTIFIER_BYTES)
}

pub(super) fn validate_bounded_text(value: &str, field: &str, max: usize) -> Result<(), String> {
    bounded_text_is_valid(value, max)
        .then_some(())
        .ok_or_else(|| format!("{field} must contain 1..={max} non-NUL bytes"))
}

pub(super) fn validate_revision(value: u64, field: &str) -> Result<(), String> {
    if value == 0 {
        return Err(format!("{field} must be positive"));
    }
    Ok(())
}

pub(super) fn validate_digest(value: &str, field: &str) -> Result<(), String> {
    require(
        value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()),
        &format!("{field} must be a 64-character hexadecimal SHA-256"),
    )
}

pub(super) fn require(condition: bool, message: &str) -> Result<(), String> {
    condition.then_some(()).ok_or_else(|| message.to_string())
}

pub(super) fn digest_value<T: Serialize>(
    domain: &[u8],
    value: &T,
    field: &str,
) -> Result<String, String> {
    let encoded = serde_json::to_vec(value).map_err(|error| format!("encode {field}: {error}"))?;
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(encoded);
    Ok(hex::encode(hasher.finalize()))
}

fn bounded_text_is_valid(value: &str, max: usize) -> bool {
    !value.is_empty() && value.len() <= max && !value.contains('\0')
}

fn is_unquoted_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    matches!(chars.next(), Some('a'..='z' | '_'))
        && chars.all(|character| matches!(character, 'a'..='z' | '0'..='9' | '_' | '$'))
}

fn is_reserved_keyword(value: &str) -> bool {
    const RESERVED_KEYWORDS: &[&str] = &[
        "all",
        "analyse",
        "analyze",
        "and",
        "any",
        "array",
        "as",
        "asc",
        "asymmetric",
        "authorization",
        "binary",
        "both",
        "case",
        "cast",
        "check",
        "collate",
        "collation",
        "column",
        "concurrently",
        "constraint",
        "create",
        "cross",
        "current_catalog",
        "current_date",
        "current_role",
        "current_schema",
        "current_time",
        "current_timestamp",
        "current_user",
        "default",
        "deferrable",
        "desc",
        "distinct",
        "do",
        "else",
        "end",
        "except",
        "false",
        "fetch",
        "for",
        "foreign",
        "freeze",
        "from",
        "full",
        "grant",
        "group",
        "having",
        "ilike",
        "in",
        "initially",
        "inner",
        "intersect",
        "into",
        "is",
        "isnull",
        "join",
        "lateral",
        "leading",
        "left",
        "like",
        "limit",
        "localtime",
        "localtimestamp",
        "natural",
        "not",
        "notnull",
        "null",
        "offset",
        "on",
        "only",
        "or",
        "order",
        "outer",
        "overlaps",
        "placing",
        "primary",
        "references",
        "returning",
        "right",
        "select",
        "session_user",
        "similar",
        "some",
        "symmetric",
        "system_user",
        "table",
        "tablesample",
        "then",
        "to",
        "trailing",
        "true",
        "union",
        "unique",
        "user",
        "using",
        "variadic",
        "verbose",
        "when",
        "where",
        "window",
        "with",
        "alter",
        "columns",
        "destination",
        "drop",
        "edge",
        "exists",
        "graph",
        "graph_table",
        "key",
        "label",
        "match",
        "node",
        "owner",
        "properties",
        "property",
        "relationship",
        "rename",
        "set",
        "source",
        "tables",
        "update",
        "vertex",
    ];
    RESERVED_KEYWORDS.iter().any(|keyword| keyword == &value)
}
