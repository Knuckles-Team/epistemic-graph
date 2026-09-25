//! Redacted control-plane views of registered foreign sources.

use serde::{Deserialize, Serialize};

/// Public source kind. Endpoint and credential material is never included.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ForeignSourceKind {
    RemoteEngine,
    HttpJson,
    Sql,
    Named,
}

/// The only source metadata returned to a caller.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ForeignSourceSummary {
    pub name: String,
    pub kind: ForeignSourceKind,
}

/// Name-keyset page of the verified caller's sources.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ForeignSourcePage {
    pub items: Vec<ForeignSourceSummary>,
    pub next_cursor: Option<String>,
}

/// A bounded probe result, without response rows or endpoint details.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ForeignSourceProbe {
    pub status: String,
    pub diagnostic_code: String,
}
