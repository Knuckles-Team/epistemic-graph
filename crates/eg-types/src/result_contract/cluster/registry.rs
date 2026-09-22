//! Typed request and result bodies for the fleet server registry.

use serde::{Deserialize, Serialize};

use crate::contract::{BoundedVec, Digest256};

/// Current schema of a [`RegisteredServerListPage`].
pub const REGISTERED_SERVER_LIST_SCHEMA_VERSION: u16 = 1;

/// Maximum and default number of server entries returned by one page.
pub const MAX_REGISTERED_SERVER_PAGE_ENTRIES: usize = 256;

/// Stable continuation over one complete live registry snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RegisteredServerCursor {
    /// Exclusive server-name lower bound for the next page.
    pub after_name: String,
    /// `GraphCore::version()` of the `__commons__` image that produced the page.
    pub registry_revision: u64,
    /// Canonical digest of every live, visible typed server in that image.
    pub registry_digest: Digest256,
}

/// Request for a bounded page of registered servers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RegisteredServerListRequest {
    /// `None` selects the maximum/default page size of 256.
    #[serde(default)]
    pub limit: Option<u16>,
    #[serde(default)]
    pub cursor: Option<RegisteredServerCursor>,
}

/// One currently live, well-typed `:Server` row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RegisteredServerView {
    pub name: String,
    pub url: String,
    pub resources: serde_json::Value,
    pub ttl_secs: u64,
    pub registered_at_ms: u64,
    pub last_heartbeat_ms: u64,
    pub lease_expires_at_ms: u64,
}

/// One revision- and digest-fenced live registry page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RegisteredServerListPage {
    pub schema_version: u16,
    pub entries: BoundedVec<RegisteredServerView, MAX_REGISTERED_SERVER_PAGE_ENTRIES>,
    pub next_cursor: Option<RegisteredServerCursor>,
    /// Server-authoritative wall-clock observation used for lease filtering.
    pub observed_at_ms: u64,
    pub total_live: u32,
    pub registry_revision: u64,
    pub registry_digest: Digest256,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_rejects_unknown_fields() {
        let encoded = rmp_serde::to_vec_named(&serde_json::json!({
            "limit": 1,
            "cursor": null,
            "request_graph": "tenant:forbidden",
        }))
        .unwrap();
        assert!(rmp_serde::from_slice::<RegisteredServerListRequest>(&encoded).is_err());
    }

    #[test]
    fn page_bound_is_part_of_the_type() {
        let entries = (0..=MAX_REGISTERED_SERVER_PAGE_ENTRIES)
            .map(|ordinal| RegisteredServerView {
                name: format!("server-{ordinal}"),
                url: "http://server".to_string(),
                resources: serde_json::json!({}),
                ttl_secs: 60,
                registered_at_ms: 1,
                last_heartbeat_ms: 1,
                lease_expires_at_ms: 61_000,
            })
            .collect();
        assert!(BoundedVec::<_, MAX_REGISTERED_SERVER_PAGE_ENTRIES>::new(entries).is_err());
    }
}
