//! Connector data resources as virtual-graph sources (EG-FEDERATED-QUERY-R722).
//!
//! A connector pack declares each read-only data resource with the ontology class
//! it answers, its field-to-property bindings, an access method and a pagination
//! form (`SDK-CONNECTOR-CONTROL-R074`). The engine registers one
//! [`PackSourceRegistration`] per resource and reads the source at query time. It
//! never copies the rows.
//!
//! The engine does not call a connector itself. The one door (arbora) owns
//! admission, auth and the governed fleet call, so every read goes through a
//! source executor that arbora implements. [`SourceReadRequest`],
//! [`SourceReadPage`] and [`SourceReadError`] are that executor's wire contract.
//! [`VirtualClassAnswer`] is what a query over an ontology class returns: merged
//! rows with per-row provenance, plus one outcome per source so a failed source is
//! a named partial result, never a silent gap.
//!
//! Pure serde: the registry, the planner and the executor seam live in
//! `eg-plan::pack_source`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Most field bindings one registration may declare.
pub const MAX_PACK_SOURCE_FIELDS: usize = 128;
/// Most bytes in one registration identifier (connector, IRI, URI, operation).
pub const MAX_PACK_SOURCE_ID_BYTES: usize = 1024;
/// Hard ceiling on pages read from one source for one query.
pub const MAX_SOURCE_PAGES: u32 = 32;
/// Hard ceiling on records requested per page.
pub const MAX_SOURCE_PAGE_SIZE: u32 = 500;
/// Hard ceiling on rows kept from one source for one query.
pub const MAX_SOURCE_ROWS: u32 = 10_000;
/// Hard ceiling on the per-source wall-clock budget.
pub const MAX_SOURCE_TIMEOUT_MS: u64 = 60_000;
/// Most sources one class query fans out to.
pub const MAX_CLASS_SOURCES: usize = 32;

/// The access method a data resource declares. The first four are the SDK
/// `ACCESS_KINDS`; `virtual_graph` is a relational or document mapping
/// (`EG-UNIFIED-DATA-PLANE-R037`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum PackSourceKind {
    McpTool,
    HttpEndpoint,
    GraphqlQuery,
    A2aSkill,
    VirtualGraph,
}

impl PackSourceKind {
    /// Parse the access kind a pack contract names. Unknown kinds are refused.
    pub fn parse(access_kind: &str) -> Option<Self> {
        Some(match access_kind {
            "mcp_tool" => Self::McpTool,
            "http_endpoint" => Self::HttpEndpoint,
            "graphql_query" => Self::GraphqlQuery,
            "a2a_skill" => Self::A2aSkill,
            "virtual_graph" => Self::VirtualGraph,
            _ => return None,
        })
    }

    /// The contract spelling of this access kind.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::McpTool => "mcp_tool",
            Self::HttpEndpoint => "http_endpoint",
            Self::GraphqlQuery => "graphql_query",
            Self::A2aSkill => "a2a_skill",
            Self::VirtualGraph => "virtual_graph",
        }
    }

    /// The `ForeignSourceSpec` kind of `EG-FEDERATED-QUERY-R073.1` (and the
    /// virtual-graph kind of `EG-UNIFIED-DATA-PLANE-R037`) this access kind reads as.
    pub fn foreign_kind(self) -> &'static str {
        match self {
            Self::McpTool => "mcp",
            Self::HttpEndpoint => "api",
            Self::GraphqlQuery => "graphql",
            Self::A2aSkill => "a2a",
            Self::VirtualGraph => "virtual_graph",
        }
    }
}

/// Whether a mapping may serve reads. Only admission (`ARBORA-FLEET-R042`) moves a
/// mapping to `Approved`; registration and a changed contract always leave it `Held`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum PackSourceApproval {
    #[default]
    Held,
    Approved,
}

/// The paging form a list operation declares (`SDK-CONNECTOR-CONTROL-R074.1`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum PackPagination {
    #[default]
    None,
    Page,
    Cursor,
    Offset,
}

/// One source field bound to one ontology property.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PackFieldBinding {
    pub field: String,
    pub property_iri: String,
}

/// One connector data resource registered as a virtual-graph source
/// (EG-FEDERATED-QUERY-R722.1).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PackSourceRegistration {
    /// The connector that owns the source (`github-mcp`).
    pub connector: String,
    /// The access contract IRI from the pack.
    pub contract_iri: String,
    /// The `vg:VirtualMapping` IRI; the registry key.
    pub mapping_iri: String,
    /// The pack data resource URI (`data://github-mcp/issues`).
    pub resource_uri: String,
    pub source_kind: PackSourceKind,
    /// The operation the executor calls: an MCP tool, an HTTP operation id, a
    /// GraphQL operation, an A2A skill or a virtual-graph name.
    pub operation: String,
    /// The ontology class this resource answers.
    pub class_iri: String,
    /// The record field that identifies one instance.
    pub key_field: String,
    pub fields: Vec<PackFieldBinding>,
    #[serde(default)]
    pub pagination: PackPagination,
    /// True only when the contract declares the key filter as pushable.
    #[serde(default)]
    pub key_filter_pushdown: bool,
    /// A write operation never registers as a readable source.
    pub read_only: bool,
    /// A credential reference (`openbao://…`, `env:…`), never a secret.
    #[serde(default)]
    pub credential_ref: Option<String>,
    /// Digest of the contract this registration was built from.
    pub contract_digest: String,
    #[serde(default)]
    pub approval: PackSourceApproval,
}

/// Why a registration was refused. `code()` is the wire code.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PackSourceRefusal {
    /// The contract names a literal secret instead of a credential reference.
    ContractSecret(String),
    /// The operation is not read-only.
    WriteOperation(String),
    /// A field is empty, too long or over the bound.
    Invalid(String),
}

impl PackSourceRefusal {
    pub fn code(&self) -> &'static str {
        match self {
            Self::ContractSecret(_) => "CONTRACT_SECRET",
            Self::WriteOperation(_) => "SOURCE_WRITE_OPERATION",
            Self::Invalid(_) => "INVALID_ARGUMENT",
        }
    }
}

impl std::fmt::Display for PackSourceRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let detail = match self {
            Self::ContractSecret(d) | Self::WriteOperation(d) | Self::Invalid(d) => d,
        };
        write!(f, "{}: {detail}", self.code())
    }
}

/// Schemes a credential reference may use. Anything else is treated as a literal
/// secret and refused.
pub const CREDENTIAL_REF_SCHEMES: &[&str] =
    &["openbao://", "vault://", "secret://", "env:", "ref:"];

impl PackSourceRegistration {
    /// Refuse a registration that could leak a secret, write, or exceed bounds.
    pub fn validate(&self) -> Result<(), PackSourceRefusal> {
        for (name, value) in [
            ("connector", &self.connector),
            ("contract_iri", &self.contract_iri),
            ("mapping_iri", &self.mapping_iri),
            ("resource_uri", &self.resource_uri),
            ("operation", &self.operation),
            ("class_iri", &self.class_iri),
            ("key_field", &self.key_field),
            ("contract_digest", &self.contract_digest),
        ] {
            if value.is_empty() || value.len() > MAX_PACK_SOURCE_ID_BYTES {
                return Err(PackSourceRefusal::Invalid(format!(
                    "{} {name} is empty or longer than {MAX_PACK_SOURCE_ID_BYTES} bytes",
                    self.mapping_iri
                )));
            }
        }
        if let Some(reference) = &self.credential_ref {
            if !CREDENTIAL_REF_SCHEMES
                .iter()
                .any(|scheme| reference.starts_with(scheme) && reference.len() > scheme.len())
            {
                return Err(PackSourceRefusal::ContractSecret(format!(
                    "{} names a credential that is not a reference",
                    self.mapping_iri
                )));
            }
        }
        if !self.read_only {
            return Err(PackSourceRefusal::WriteOperation(format!(
                "{} operation {} is not read-only",
                self.mapping_iri, self.operation
            )));
        }
        if self.fields.is_empty() || self.fields.len() > MAX_PACK_SOURCE_FIELDS {
            return Err(PackSourceRefusal::Invalid(format!(
                "{} must bind 1..={MAX_PACK_SOURCE_FIELDS} fields",
                self.mapping_iri
            )));
        }
        if self
            .fields
            .iter()
            .any(|b| b.field.is_empty() || b.property_iri.is_empty())
        {
            return Err(PackSourceRefusal::Invalid(format!(
                "{} has an empty field binding",
                self.mapping_iri
            )));
        }
        Ok(())
    }
}

/// The local name of an IRI or CURIE: the part after the last `#`, `/` or `:`.
pub fn iri_local_name(iri: &str) -> &str {
    iri.rsplit(['#', '/', ':']).next().unwrap_or(iri)
}

/// Whether a registration's class answers `wanted`. A full IRI matches exactly; a
/// bare name (`Issue`) matches the class's local name.
pub fn class_matches(class_iri: &str, wanted: &str) -> bool {
    if class_iri == wanted {
        return true;
    }
    !wanted.contains(['#', '/', ':']) && iri_local_name(class_iri) == wanted
}

/// The verified caller a read runs for. The engine takes it only from the verified
/// request envelope; the executor enforces the caller's grant per source.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SourceCaller {
    pub tenant: String,
    pub principal: String,
}

/// One page read the engine asks the source executor to perform.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SourceReadRequest {
    pub connector: String,
    pub contract_iri: String,
    pub mapping_iri: String,
    pub source_kind: PackSourceKind,
    pub operation: String,
    pub key_field: String,
    /// Set only when the contract declares the key filter pushable. The engine
    /// re-applies it locally either way.
    #[serde(default)]
    pub key_filter: Option<String>,
    pub pagination: PackPagination,
    /// The token the previous page returned; `None` for the first page.
    #[serde(default)]
    pub page_token: Option<String>,
    pub page_size: u32,
    /// Wall-clock milliseconds left for this source. The executor must give up by then.
    pub deadline_ms: u64,
    pub caller: SourceCaller,
}

/// One page of source records, as the connector returned them.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SourceReadPage {
    pub records: Vec<BTreeMap<String, Value>>,
    /// `None` when the source has no further page.
    #[serde(default)]
    pub next_page_token: Option<String>,
}

/// Why the executor did not return a page.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum SourceReadErrorCode {
    /// The caller holds no current grant for this source.
    NotGranted,
    /// The connector is down or unreachable.
    Unavailable,
    /// The connector did not answer within `deadline_ms`.
    Timeout,
    /// Admission or the outbound guard refused the call.
    Refused,
    /// The connector answered with something that is not a page of records.
    BadResponse,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SourceReadError {
    pub code: SourceReadErrorCode,
    pub reason: String,
}

/// Per-row provenance (EG-FEDERATED-QUERY-R722.5).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct VirtualRowProvenance {
    pub connector: String,
    pub contract_iri: String,
    pub mapping_iri: String,
    pub resource_uri: String,
    pub source_kind: PackSourceKind,
    pub contract_digest: String,
    pub fetched_at_ms: u64,
}

/// One instance of the queried class, read live from one source.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct VirtualClassRow {
    pub class_iri: String,
    /// `<connector>/<key>`: unique across sources.
    pub id: String,
    /// The source key value.
    pub key: String,
    /// Ontology property IRI to value.
    pub properties: BTreeMap<String, Value>,
    pub provenance: VirtualRowProvenance,
}

/// How one source took part in a class query.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum SourceOutcomeStatus {
    /// Every page was read.
    Ok,
    /// The page or row bound stopped the read; rows so far are kept.
    Truncated,
    /// The mapping is not approved and served nothing.
    Held,
    /// The caller may not read this source.
    NotGranted,
    Unavailable,
    Timeout,
    Refused,
    BadResponse,
}

impl SourceOutcomeStatus {
    /// The wire code a non-`Ok` outcome reports.
    pub fn code(self) -> &'static str {
        match self {
            Self::Ok => "OK",
            Self::Truncated => "SOURCE_TRUNCATED",
            Self::Held => "SOURCE_HELD",
            Self::NotGranted => "SOURCE_NOT_GRANTED",
            Self::Unavailable => "SOURCE_UNAVAILABLE",
            Self::Timeout => "SOURCE_TIMEOUT",
            Self::Refused => "SOURCE_REFUSED",
            Self::BadResponse => "SOURCE_BAD_RESPONSE",
        }
    }
}

impl From<SourceReadErrorCode> for SourceOutcomeStatus {
    fn from(code: SourceReadErrorCode) -> Self {
        match code {
            SourceReadErrorCode::NotGranted => Self::NotGranted,
            SourceReadErrorCode::Unavailable => Self::Unavailable,
            SourceReadErrorCode::Timeout => Self::Timeout,
            SourceReadErrorCode::Refused => Self::Refused,
            SourceReadErrorCode::BadResponse => Self::BadResponse,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SourceOutcome {
    pub connector: String,
    pub mapping_iri: String,
    pub status: SourceOutcomeStatus,
    pub rows: u32,
    pub pages: u32,
    #[serde(default)]
    pub reason: Option<String>,
}

/// A query over one ontology class, answered from every mapped source.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct VirtualClassQuery {
    /// A class IRI, or a bare local name such as `Issue`.
    pub class_iri: String,
    /// Keep only the instance with this key.
    #[serde(default)]
    pub key: Option<String>,
    #[serde(default = "default_max_pages")]
    pub max_pages_per_source: u32,
    #[serde(default = "default_page_size")]
    pub page_size: u32,
    #[serde(default = "default_max_rows")]
    pub max_rows_per_source: u32,
    #[serde(default = "default_timeout_ms")]
    pub source_timeout_ms: u64,
}

fn default_max_pages() -> u32 {
    4
}
fn default_page_size() -> u32 {
    100
}
fn default_max_rows() -> u32 {
    1_000
}
fn default_timeout_ms() -> u64 {
    10_000
}

impl VirtualClassQuery {
    /// A query over `class_iri` with the default bounds.
    pub fn new(class_iri: impl Into<String>) -> Self {
        Self {
            class_iri: class_iri.into(),
            key: None,
            max_pages_per_source: default_max_pages(),
            page_size: default_page_size(),
            max_rows_per_source: default_max_rows(),
            source_timeout_ms: default_timeout_ms(),
        }
    }

    /// The same query with every bound clamped to its hard ceiling (and at least 1).
    pub fn bounded(&self) -> Self {
        Self {
            class_iri: self.class_iri.clone(),
            key: self.key.clone(),
            max_pages_per_source: self.max_pages_per_source.clamp(1, MAX_SOURCE_PAGES),
            page_size: self.page_size.clamp(1, MAX_SOURCE_PAGE_SIZE),
            max_rows_per_source: self.max_rows_per_source.clamp(1, MAX_SOURCE_ROWS),
            source_timeout_ms: self.source_timeout_ms.clamp(1, MAX_SOURCE_TIMEOUT_MS),
        }
    }
}

/// Merged rows from every mapped source plus one outcome per source.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct VirtualClassAnswer {
    pub class_iri: String,
    pub rows: Vec<VirtualClassRow>,
    pub sources: Vec<SourceOutcome>,
    /// True when any source is not `Ok`.
    pub partial: bool,
}
