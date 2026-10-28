//! Data products, data contracts and access requests (EG-TYPED-PACKS-R805.1).
//!
//! Pure data and field validation. Contract checks, grants and served
//! admission are later children of `EG-TYPED-PACKS-R805`.

use serde::{Deserialize, Serialize};

/// Longest identifier, principal or purpose a record may carry.
pub const MAX_PRODUCT_TEXT_BYTES: usize = 1024;
/// Most members one product list may name.
pub const MAX_PRODUCT_MEMBERS: usize = 256;
/// The stable refusal code for an access request without a purpose.
pub const ACCESS_PURPOSE_MISSING: &str = "ACCESS_PURPOSE_MISSING";

/// How a consumer reaches a product.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ProductPort {
    pub name: String,
    pub kind: ProductPortKind,
}

/// The closed set of access-port kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ProductPortKind {
    Query,
    Api,
    Stream,
    File,
}

/// An owned, versioned set of datasets, terms and access ports.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DataProduct {
    pub id: String,
    pub owner: String,
    /// Semantic version `MAJOR.MINOR.PATCH`.
    pub version: String,
    pub datasets: Vec<String>,
    pub terms: Vec<String>,
    pub ports: Vec<ProductPort>,
}

/// What a product promises its consumers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DataContract {
    pub product: String,
    /// `sha256:<64 lowercase hex>` of the promised schema.
    pub schema_digest: String,
    pub quality_rules: Vec<String>,
    /// Largest promised age of the data, in seconds.
    pub freshness_s: u64,
}

/// Where an access request is in its lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum AccessRequestState {
    Pending,
    Approved,
    Rejected,
    Revoked,
}

/// A consumer's governed request to use a product.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AccessRequest {
    pub product: String,
    pub principal: String,
    pub purpose: String,
    pub state: AccessRequestState,
}

/// A record that failed validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataProductError {
    pub code: &'static str,
    pub field: &'static str,
}

impl std::fmt::Display for DataProductError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.field)
    }
}

impl std::error::Error for DataProductError {}

fn invalid(field: &'static str) -> DataProductError {
    DataProductError {
        code: "INVALID_FIELD",
        field,
    }
}

fn text(field: &'static str, value: &str) -> Result<(), DataProductError> {
    if value.trim().is_empty()
        || value.trim() != value
        || value.len() > MAX_PRODUCT_TEXT_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(invalid(field));
    }
    Ok(())
}

fn members<'a>(
    field: &'static str,
    values: impl ExactSizeIterator<Item = &'a str>,
) -> Result<(), DataProductError> {
    if values.len() > MAX_PRODUCT_MEMBERS {
        return Err(invalid(field));
    }
    let mut seen = std::collections::BTreeSet::new();
    for value in values {
        text(field, value)?;
        if !seen.insert(value) {
            return Err(invalid(field));
        }
    }
    Ok(())
}

/// Parse `MAJOR.MINOR.PATCH` into its three numbers.
pub fn parse_product_version(version: &str) -> Option<(u64, u64, u64)> {
    let mut parts = version.split('.').map(|part| {
        (!part.is_empty() && part.bytes().all(|b| b.is_ascii_digit()))
            .then(|| part.parse::<u64>().ok())
            .flatten()
    });
    let parsed = (parts.next()??, parts.next()??, parts.next()??);
    parts.next().is_none().then_some(parsed)
}

impl DataProduct {
    pub fn validate(&self) -> Result<(), DataProductError> {
        text("id", &self.id)?;
        text("owner", &self.owner)?;
        parse_product_version(&self.version).ok_or_else(|| invalid("version"))?;
        if self.datasets.is_empty() {
            return Err(invalid("datasets"));
        }
        members("datasets", self.datasets.iter().map(String::as_str))?;
        members("terms", self.terms.iter().map(String::as_str))?;
        members("ports", self.ports.iter().map(|port| port.name.as_str()))
    }
}

impl DataContract {
    pub fn validate(&self) -> Result<(), DataProductError> {
        text("product", &self.product)?;
        let hex = self
            .schema_digest
            .strip_prefix("sha256:")
            .unwrap_or_default();
        if hex.len() != 64 || !hex.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
            return Err(invalid("schema_digest"));
        }
        members("quality_rules", self.quality_rules.iter().map(String::as_str))?;
        if self.freshness_s == 0 {
            return Err(invalid("freshness_s"));
        }
        Ok(())
    }
}

impl AccessRequest {
    pub fn validate(&self) -> Result<(), DataProductError> {
        text("product", &self.product)?;
        text("principal", &self.principal)?;
        if self.purpose.trim().is_empty() {
            return Err(DataProductError {
                code: ACCESS_PURPOSE_MISSING,
                field: "purpose",
            });
        }
        text("purpose", &self.purpose)
    }
}
