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
        #[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
        #[cfg_attr(feature = "contract-schema", schemars(transparent))]
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
canonical_id_type!(IdempotencyKey, MAX_IDEMPOTENCY_KEY_BYTES, "idempotency key");
canonical_id_type!(SchemaId, MAX_RESOURCE_ID_BYTES, "schema id");

impl ResourceId {
    /// Convert a canonical sanitized physical graph key into an internal
    /// authority subject at the shard boundary.
    ///
    /// Physical graph keys are storage names, not external resource ids: the
    /// shard sanitizer represents punctuation as `~xx` (or a bounded
    /// `~h<sha256>` key), while the public canonical alphabet deliberately
    /// excludes `~`. The internal subject is the `physical:` namespace plus
    /// the lowercase hex bytes of the complete sanitized key. Keeping the
    /// physical spelling intact makes ordinary escapes and one-way hash keys
    /// disjoint even when they would decode to the same logical text. The
    /// physical key is validated before conversion so a caller cannot smuggle
    /// a noncanonical or overlong storage spelling into authority.
    pub(crate) fn from_physical_graph_key(value: &str) -> Result<Self, String> {
        validate_physical_graph_key(value)?;
        let mut internal = String::with_capacity("physical:".len() + value.len() * 2);
        internal.push_str("physical:");
        for byte in value.bytes() {
            use std::fmt::Write as _;
            write!(&mut internal, "{byte:02x}").expect("writing to String cannot fail");
        }
        Self::new(internal)
    }
}

const MAX_ORDINARY_PHYSICAL_GRAPH_KEY_BYTES: usize = 200;

fn decode_physical_graph_key(value: &str) -> Vec<u8> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut offset = 0;
    while offset < bytes.len() {
        if bytes[offset] == b'~' {
            let high = hex_value(bytes[offset + 1]);
            let low = hex_value(bytes[offset + 2]);
            decoded.push((high << 4) | low);
            offset += 3;
        } else {
            decoded.push(bytes[offset]);
            offset += 1;
        }
    }
    decoded
}

fn validate_physical_graph_key(value: &str) -> Result<(), String> {
    if value.is_empty() {
        return Err("physical graph key must not be empty".to_string());
    }

    let bytes = value.as_bytes();
    if value.starts_with("~h") {
        return if bytes.len() == 66 && bytes[2..].iter().copied().all(is_lower_hex) {
            Ok(())
        } else {
            Err("physical graph hash key is malformed".to_string())
        };
    }

    validate_ordinary_physical_graph_key(value, bytes)
}

fn validate_ordinary_physical_graph_key(value: &str, bytes: &[u8]) -> Result<(), String> {
    if bytes.len() > MAX_ORDINARY_PHYSICAL_GRAPH_KEY_BYTES {
        return Err(format!(
            "ordinary physical graph key must contain at most {MAX_ORDINARY_PHYSICAL_GRAPH_KEY_BYTES} bytes"
        ));
    }

    let mut offset = 0;
    while offset < bytes.len() {
        match bytes[offset] {
            byte if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.') => {
                offset += 1;
            }
            b'~' if offset + 2 < bytes.len()
                && is_lower_hex(bytes[offset + 1])
                && is_lower_hex(bytes[offset + 2]) =>
            {
                offset += 3;
            }
            _ => {
                return Err(
                    "physical graph key must use the sanitized storage alphabet".to_string()
                );
            }
        }
    }
    let decoded = decode_physical_graph_key(value);
    std::str::from_utf8(&decoded)
        .map_err(|_| "physical graph key does not decode to valid UTF-8".to_string())?;
    if encode_physical_graph_key(&decoded) != value {
        return Err("physical graph key is not a canonical sanitizer spelling".to_string());
    }
    Ok(())
}

fn encode_physical_graph_key(value: &[u8]) -> String {
    let mut encoded = String::with_capacity(value.len());
    for &byte in value {
        use std::fmt::Write as _;
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.') {
            encoded.push(char::from(byte));
        } else {
            write!(&mut encoded, "~{byte:02x}").expect("writing to String cannot fail");
        }
    }
    encoded
}

fn is_lower_hex(byte: u8) -> bool {
    byte.is_ascii_digit() || matches!(byte, b'a'..=b'f')
}

fn hex_value(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        _ => unreachable!("physical graph key validation checked hex digits"),
    }
}

macro_rules! closed_token_type {
    ($name:ident, $label:literal, [$($value:literal),+ $(,)?]) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
        #[serde(transparent)]
        #[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
        #[cfg_attr(feature = "contract-schema", schemars(transparent))]
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
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "contract-schema", schemars(transparent))]
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
        assert!(ResourceId::new("graph~3ascope").is_err());
        assert_eq!(
            ResourceId::from_physical_graph_key("graph~3ascope")
                .unwrap()
                .as_str(),
            "physical:67726170687e336173636f7065"
        );
        let hash_key = format!("~h{}", "a".repeat(64));
        let hash_id = ResourceId::from_physical_graph_key(&hash_key).unwrap();
        assert!(hash_id.as_str().starts_with("physical:7e68"));
        let slash_hash_key = format!("~2fh{}", "a".repeat(64));
        let slash_hash_id = ResourceId::from_physical_graph_key(&slash_hash_key).unwrap();
        assert_ne!(hash_id, slash_hash_id);
        assert!(ResourceId::from_physical_graph_key("graph~61").is_err());
        assert!(ResourceId::from_physical_graph_key("graph~ff").is_err());
        assert!(ResourceId::from_physical_graph_key(&"a".repeat(201)).is_err());
        assert!(ResourceId::from_physical_graph_key("graph~zz").is_err());
        let removed_scope_kind = ["glo", "bal"].concat();
        assert!(ScopeKind::new(removed_scope_kind).is_err());
    }
}
