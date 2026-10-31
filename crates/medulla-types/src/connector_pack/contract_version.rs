//! A connector pack release as a versioned contract, and the time-boxed grant
//! that lets an ontology stand alone (MEDULLA-TYPED-PACKS-R107.1).
//!
//! Pure data and field validation. The import-time diff, the pending
//! breaking release and `OrphanReport` are later children of
//! `MEDULLA-TYPED-PACKS-R107`.

use serde::{Deserialize, Serialize};

/// Longest connector name, version, IRI, reason or owner a record may carry.
pub const MAX_CONTRACT_TEXT_BYTES: usize = 1024;
/// The stable refusal code for a grant without a reason or an expiry.
pub const GRANT_INCOMPLETE: &str = "GRANT_INCOMPLETE";

/// One released contract version of a connector pack.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PackContractVersion {
    pub connector: String,
    pub version: String,
    /// `sha256:<64 lowercase hex>` of the whole contract.
    pub contract_digest: String,
    /// `sha256:<64 lowercase hex>` of the pack ontology.
    pub ontology_digest: String,
    /// `sha256:<64 lowercase hex>` of the pack shapes.
    pub shapes_digest: String,
    /// The version this release replaces; absent for the first release.
    #[serde(default)]
    pub supersedes: Option<String>,
}

/// A recorded exception that lets an ontology live without a parent link.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct StandaloneGrant {
    pub connector: String,
    pub ontology_iri: String,
    pub reason: String,
    pub owner: String,
    /// Unix seconds after which the ontology is reported as an orphan.
    pub expires_at: u64,
}

/// What makes a release break its contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ContractBreakKind {
    RemovedClass,
    RetypedClass,
    RemovedProperty,
    RetypedProperty,
    RemovedField,
    RetypedField,
    NarrowedShape,
    RemovedMapping,
}

/// A record that failed validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackContractError {
    pub code: &'static str,
    pub field: &'static str,
}

impl std::fmt::Display for PackContractError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.field)
    }
}

impl std::error::Error for PackContractError {}

fn invalid(field: &'static str) -> PackContractError {
    PackContractError {
        code: "INVALID_FIELD",
        field,
    }
}

fn text(field: &'static str, value: &str) -> Result<(), PackContractError> {
    if value.trim().is_empty()
        || value.trim() != value
        || value.len() > MAX_CONTRACT_TEXT_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(invalid(field));
    }
    Ok(())
}

fn digest(field: &'static str, value: &str) -> Result<(), PackContractError> {
    let hex = value.strip_prefix("sha256:").unwrap_or_default();
    if hex.len() == 64 && hex.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
        Ok(())
    } else {
        Err(invalid(field))
    }
}

impl PackContractVersion {
    pub fn validate(&self) -> Result<(), PackContractError> {
        text("connector", &self.connector)?;
        text("version", &self.version)?;
        digest("contract_digest", &self.contract_digest)?;
        digest("ontology_digest", &self.ontology_digest)?;
        digest("shapes_digest", &self.shapes_digest)?;
        if let Some(previous) = &self.supersedes {
            text("supersedes", previous)?;
            if *previous == self.version {
                return Err(invalid("supersedes"));
            }
        }
        Ok(())
    }
}

impl StandaloneGrant {
    pub fn validate(&self) -> Result<(), PackContractError> {
        text("connector", &self.connector)?;
        text("ontology_iri", &self.ontology_iri)?;
        text("owner", &self.owner)?;
        if self.reason.trim().is_empty() {
            return Err(PackContractError {
                code: GRANT_INCOMPLETE,
                field: "reason",
            });
        }
        if self.expires_at == 0 {
            return Err(PackContractError {
                code: GRANT_INCOMPLETE,
                field: "expires_at",
            });
        }
        text("reason", &self.reason)
    }

    /// Whether the grant has lapsed at `now` (unix seconds).
    pub fn expired_at(&self, now: u64) -> bool {
        now >= self.expires_at
    }
}
