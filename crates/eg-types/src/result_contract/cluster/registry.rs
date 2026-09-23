//! Typed request and result bodies for the fleet server registry.

use serde::{Deserialize, Serialize};

use crate::contract::{BoundedVec, Digest256};

/// Current schema of a [`RegisteredServerListPage`].
///
/// 2: every view carries the server's typed `transport` and `desired` state, so
/// the snapshot digest covers a different shape and a v1 cursor cannot resume.
pub const REGISTERED_SERVER_LIST_SCHEMA_VERSION: u16 = 2;

/// Longest registered server name. Mirrors the agent-utilities config-sync
/// bound, so one name is valid on both registration paths.
pub const MAX_REGISTERED_SERVER_NAME_BYTES: usize = 128;

/// Whether `name` is a bounded logical server name (`^[A-Za-z0-9_.-]{1,128}$`).
///
/// The one definition: the registry, its cursors and the fleet catalog's
/// discovery records all validate server names here.
pub fn is_valid_server_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_REGISTERED_SERVER_NAME_BYTES
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
}

/// How a registered server is reached. Closed on purpose: a transport the
/// engine cannot name is `Unspecified`, never a free-form string.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ServerTransport {
    /// A registration that predates the typed field, or did not say.
    #[default]
    Unspecified,
    /// A local child process speaking MCP over stdio.
    Stdio,
    /// MCP streamable HTTP.
    StreamableHttp,
    /// MCP over server-sent events.
    Sse,
    /// Plain HTTP JSON-RPC.
    Http,
}

/// The operator's DESIRED state for a registered server.
///
/// Desired, not observed: whether the server answered its last probe is a
/// discovery observation (`FleetCatalog.RecordDiscovery`), and the two are
/// never collapsed into one status.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ServerDesiredState {
    #[default]
    Enabled,
    Disabled,
}

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
    pub transport: ServerTransport,
    pub desired: ServerDesiredState,
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
                transport: ServerTransport::Http,
                desired: ServerDesiredState::Enabled,
                resources: serde_json::json!({}),
                ttl_secs: 60,
                registered_at_ms: 1,
                last_heartbeat_ms: 1,
                lease_expires_at_ms: 61_000,
            })
            .collect();
        assert!(BoundedVec::<_, MAX_REGISTERED_SERVER_PAGE_ENTRIES>::new(entries).is_err());
    }

    #[test]
    fn server_name_bound_matches_the_config_sync_alphabet() {
        assert!(is_valid_server_name("github-mcp_1.0"));
        assert!(is_valid_server_name(
            &"a".repeat(MAX_REGISTERED_SERVER_NAME_BYTES)
        ));
        for invalid in ["", "has space", "slash/name", "tilde~name", "\u{e9}"] {
            assert!(!is_valid_server_name(invalid), "{invalid:?}");
        }
        assert!(!is_valid_server_name(
            &"a".repeat(MAX_REGISTERED_SERVER_NAME_BYTES + 1)
        ));
    }

    #[test]
    fn desired_state_and_transport_default_for_older_registrations() {
        assert_eq!(ServerTransport::default(), ServerTransport::Unspecified);
        assert_eq!(ServerDesiredState::default(), ServerDesiredState::Enabled);
        let encoded = rmp_serde::to_vec_named(&ServerTransport::StreamableHttp).unwrap();
        assert_eq!(
            rmp_serde::from_slice::<String>(&encoded).unwrap(),
            "streamable_http"
        );
    }
}
