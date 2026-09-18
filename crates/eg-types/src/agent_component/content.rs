//! The `content` read: a component's bytes, served back verbatim.
//!
//! The parent module's rule still holds -- a component RECORD stores no
//! content. What this read serves is the engine-owned body a connector pack
//! import or a decision commit deposited, addressed by the revision that pins
//! its digest. The engine re-hashes the bytes before answering, so a caller
//! never has to trust the store it came out of.

use serde::{Deserialize, Serialize};

/// Format identity (RF-ADR-006) of [`AgentComponentContentResult`].
pub const COMPONENT_CONTENT_SCHEMA_VERSION: u16 = 1;

/// Largest body this read will serve in one response.
pub const MAX_COMPONENT_BODY_BYTES: usize = 2 * 1024 * 1024;

/// The media type served when a component declares none.
pub const DEFAULT_COMPONENT_MEDIA_TYPE: &str = "application/octet-stream";

/// The component attribute a publisher declares its media type under.
pub const COMPONENT_MEDIA_TYPE_ATTRIBUTE: &str = "content.media_type";

/// Read one component revision's bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AgentComponentContentRequest {
    pub tenant_id: String,
    pub component_id: String,
    /// `None` reads the current published revision.
    #[serde(default)]
    pub entry_revision: Option<u64>,
}

/// One component revision's bytes and the identity they were served under.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AgentComponentContentResult {
    pub schema_version: u16,
    pub component_id: String,
    pub entry_revision: u64,
    pub definition_digest: String,
    /// `sha256:<hex>`, verified by the engine against the bytes it is sending.
    pub content_digest: String,
    pub media_type: String,
    /// At most [`MAX_COMPONENT_BODY_BYTES`].
    #[serde(with = "serde_bytes")]
    #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
    pub body: Vec<u8>,
}
