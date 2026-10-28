//! Stewardship roles, change requests and certification records
//! (EG-TYPED-PACKS-R802.1).
//!
//! Pure data and field validation. Scope resolution, the change-request flow
//! and served admission are later children of `EG-TYPED-PACKS-R802`.

use serde::{Deserialize, Serialize};

/// Longest identifier, principal or asset reference a record may carry.
pub const MAX_STEWARDSHIP_TEXT_BYTES: usize = 512;
/// Most reviewers one change request may name.
pub const MAX_CHANGE_REQUEST_REVIEWERS: usize = 32;
/// Default certification lifetime: 365 days, in seconds.
pub const DEFAULT_CERTIFICATION_TTL_SECS: u64 = 365 * 24 * 60 * 60;

/// What a stewardship assignment governs.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum StewardshipScope {
    Domain(String),
    ConceptScheme(String),
    Asset(String),
}

impl StewardshipScope {
    fn id(&self) -> &str {
        match self {
            Self::Domain(id) | Self::ConceptScheme(id) | Self::Asset(id) => id,
        }
    }
}

/// The closed set of stewardship roles.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum StewardRole {
    Owner,
    Steward,
    Custodian,
}

/// One principal holding one role over one scope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct StewardshipAssignment {
    pub scope: StewardshipScope,
    pub role: StewardRole,
    pub principal: String,
}

/// Where a change request is in its lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ChangeRequestState {
    Open,
    Approved,
    Rejected,
    Applied,
}

/// A proposed change to a governed asset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ChangeRequest {
    pub target: String,
    /// `sha256:<64 lowercase hex>` of the proposed asset content.
    pub proposed_digest: String,
    pub state: ChangeRequestState,
    pub reviewers: Vec<String>,
}

/// A steward's statement that an asset is trusted, until it expires.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct Certification {
    pub asset: String,
    pub level: String,
    pub by: String,
    /// Unix seconds after which the certification no longer holds.
    pub expires_at: u64,
}

/// A record field that failed validation, named so a caller can fix it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StewardshipFieldError {
    pub field: &'static str,
    pub reason: &'static str,
}

impl std::fmt::Display for StewardshipFieldError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.field, self.reason)
    }
}

impl std::error::Error for StewardshipFieldError {}

fn text(field: &'static str, value: &str) -> Result<(), StewardshipFieldError> {
    let reason = if value.trim().is_empty() {
        "must not be empty"
    } else if value.len() > MAX_STEWARDSHIP_TEXT_BYTES {
        "exceeds the text bound"
    } else if value.trim() != value || value.chars().any(char::is_control) {
        "must not carry padding or control characters"
    } else {
        return Ok(());
    };
    Err(StewardshipFieldError { field, reason })
}

fn sha256_digest(field: &'static str, value: &str) -> Result<(), StewardshipFieldError> {
    let hex = value.strip_prefix("sha256:").unwrap_or_default();
    if hex.len() == 64 && hex.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
        Ok(())
    } else {
        Err(StewardshipFieldError {
            field,
            reason: "must be sha256:<64 lowercase hex>",
        })
    }
}

impl StewardshipAssignment {
    pub fn validate(&self) -> Result<(), StewardshipFieldError> {
        text("scope", self.scope.id())?;
        text("principal", &self.principal)
    }
}

impl ChangeRequest {
    pub fn validate(&self) -> Result<(), StewardshipFieldError> {
        text("target", &self.target)?;
        sha256_digest("proposed_digest", &self.proposed_digest)?;
        if self.reviewers.len() > MAX_CHANGE_REQUEST_REVIEWERS {
            return Err(StewardshipFieldError {
                field: "reviewers",
                reason: "exceeds the reviewer bound",
            });
        }
        let mut seen = std::collections::BTreeSet::new();
        for reviewer in &self.reviewers {
            text("reviewers", reviewer)?;
            if !seen.insert(reviewer.as_str()) {
                return Err(StewardshipFieldError {
                    field: "reviewers",
                    reason: "names a reviewer twice",
                });
            }
        }
        Ok(())
    }
}

impl Certification {
    pub fn validate(&self) -> Result<(), StewardshipFieldError> {
        text("asset", &self.asset)?;
        text("level", &self.level)?;
        text("by", &self.by)?;
        if self.expires_at == 0 {
            return Err(StewardshipFieldError {
                field: "expires_at",
                reason: "must be a positive unix time",
            });
        }
        Ok(())
    }

    /// Whether the certification still holds at `now` (unix seconds).
    pub fn holds_at(&self, now: u64) -> bool {
        now < self.expires_at
    }
}
