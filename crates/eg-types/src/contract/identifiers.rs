//! Canonical identifiers, closed domain tokens, and normalized timestamps.
//!
//! Printable ASCII is already NFC, so the intentionally small identifier
//! alphabet prevents multiple Unicode spellings without another normalizer.

use super::{
    MAX_IDEMPOTENCY_KEY_BYTES, MAX_METHOD_ID_BYTES, MAX_OPAQUE_ID_BYTES, MAX_RESOURCE_ID_BYTES,
    MAX_TENANT_ID_BYTES,
};
use serde::de::{Error as _, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use std::fmt;

fn validate_canonical_id(value: &str, maximum: usize, label: &str) -> Result<(), String> {
    if value.is_empty() || value.len() > maximum {
        return Err(format!("{label} must contain 1..={maximum} bytes"));
    }
    if !value.is_ascii()
        || !value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(byte, b'-' | b'.' | b'_' | b':' | b'/' | b'@' | b'#')
        })
    {
        return Err(format!(
            "{label} must use the canonical ASCII identifier alphabet"
        ));
    }
    Ok(())
}

struct CanonicalIdVisitor {
    maximum: usize,
    label: &'static str,
}
impl Visitor<'_> for CanonicalIdVisitor {
    type Value = String;
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "a canonical {} containing at most {} bytes",
            self.label, self.maximum
        )
    }
    fn visit_borrowed_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        validate_canonical_id(value, self.maximum, self.label).map_err(E::custom)?;
        Ok(value.to_owned())
    }
    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        validate_canonical_id(value, self.maximum, self.label).map_err(E::custom)?;
        Ok(value.to_owned())
    }
}

macro_rules! canonical_id_type {
    ($name:ident, $maximum:expr, $label:literal) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
        #[serde(transparent)]
        pub struct $name(String);
        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, String> {
                let value = value.into();
                validate_canonical_id(&value, $maximum, $label)?;
                Ok(Self(value))
            }
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                deserializer
                    .deserialize_str(CanonicalIdVisitor {
                        maximum: $maximum,
                        label: $label,
                    })
                    .map(Self)
            }
        }
    };
}

canonical_id_type!(TenantId, MAX_TENANT_ID_BYTES, "tenant id");
canonical_id_type!(ActorId, MAX_OPAQUE_ID_BYTES, "actor id");
canonical_id_type!(AudienceId, MAX_OPAQUE_ID_BYTES, "audience id");
canonical_id_type!(OpaqueId, MAX_OPAQUE_ID_BYTES, "opaque id");
canonical_id_type!(ResourceId, MAX_RESOURCE_ID_BYTES, "resource id");
canonical_id_type!(ProtocolId, MAX_RESOURCE_ID_BYTES, "protocol id");
canonical_id_type!(MethodId, MAX_METHOD_ID_BYTES, "method id");
canonical_id_type!(PolicyRevision, MAX_OPAQUE_ID_BYTES, "policy revision");
canonical_id_type!(
    IdempotencyKey,
    MAX_IDEMPOTENCY_KEY_BYTES,
    "idempotency key"
);
canonical_id_type!(SchemaId, MAX_RESOURCE_ID_BYTES, "schema id");

macro_rules! closed_token_type {
    ($name:ident, $label:literal, [$($value:literal),+ $(,)?]) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
        #[serde(transparent)]
        pub struct $name(String);
        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, String> {
                let value = value.into();
                if matches!(value.as_str(), $($value)|+) { Ok(Self(value)) }
                else { Err(format!("{} is not a recognized {}", value, $label)) }
            }
            pub fn as_str(&self) -> &str { &self.0 }
        }
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where D: Deserializer<'de> {
                deserializer.deserialize_str(CanonicalIdVisitor { maximum: MAX_OPAQUE_ID_BYTES, label: $label })
                    .and_then(|value| Self::new(value).map_err(D::Error::custom))
            }
        }
    };
}

closed_token_type!(
    IngressSurface,
    "ingress surface",
    [
        "au_rest",
        "au_mcp",
        "au_cli",
        "atlas",
        "webui",
        "workflow",
        "source_control"
    ]
);
closed_token_type!(
    Operation,
    "operation",
    [
        "query",
        "mutation",
        "extract",
        "ingest",
        "backfeed",
        "configure",
        "audit"
    ]
);
closed_token_type!(
    PurposeKind,
    "purpose",
    [
        "graph_read",
        "graph_write",
        "source_extract",
        "source_ingest",
        "source_backfeed",
        "config_admin",
        "audit"
    ]
);
closed_token_type!(
    ScopeKind,
    "scope kind",
    ["tenant", "graph", "incarnation", "native"]
);
closed_token_type!(
    ReplayStatus,
    "replay status",
    ["consumed", "duplicate", "rejected", "unavailable"]
);
closed_token_type!(
    EffectState,
    "effect state",
    [
        "none",
        "pending",
        "committed",
        "failed_retryable",
        "failed_terminal"
    ]
);
closed_token_type!(
    VerificationStatus,
    "verification status",
    ["verified", "rejected", "unavailable"]
);
closed_token_type!(
    AdmissionOutcome,
    "admission state",
    ["admitted", "pending", "denied", "unavailable"]
);
closed_token_type!(
    DecisionOutcome,
    "decision outcome",
    ["allow", "deny", "unavailable"]
);
closed_token_type!(
    MutationDomain,
    "mutation domain",
    [
        "graph",
        "rdf",
        "sql_catalog",
        "blob",
        "key_value",
        "time_series",
        "analytics_job",
        "statechart",
        "semantic_index",
        "security",
        "control"
    ]
);
closed_token_type!(
    RequestedMutationResult,
    "requested mutation result",
    ["receipt_only", "changed_records", "domain_result"]
);
closed_token_type!(
    MutationDisposition,
    "mutation disposition",
    [
        "committed",
        "replayed",
        "rejected",
        "conflict",
        "unavailable"
    ]
);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct UtcUnixNanos(i64);
impl UtcUnixNanos {
    pub fn new(value: i64) -> Self {
        Self(value)
    }
    pub fn get(self) -> i64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn identifiers_are_bounded_and_ascii_nfc() {
        assert!(TenantId::new("tenant-a").is_ok());
        assert!(TenantId::new("é").is_err());
        assert!(TenantId::new(" tenant-a").is_err());
        assert!(ResourceId::new("r".repeat(MAX_RESOURCE_ID_BYTES + 1)).is_err());
        let removed_scope_kind = ["glo", "bal"].concat();
        assert!(ScopeKind::new(removed_scope_kind).is_err());
    }
}
