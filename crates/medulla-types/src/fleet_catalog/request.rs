//! What a caller sends to the fleet catalog, and how each request is bounded.

use serde::{Deserialize, Serialize};

use super::vocabulary::{
    DiscoveryOutcome, DiscoveryScope, FleetCatalogKind, FleetOverride, FleetOverrideField,
};
use super::{
    MAX_DISCOVERY_ERROR_BYTES, MAX_FLEET_COMPONENT_ID_BYTES, MAX_FLEET_GRANT_DIGESTS,
    MAX_FLEET_LOOKUP_IDS, MAX_FLEET_PAGE_ROWS, MAX_FLEET_QUERY_BYTES,
};
use crate::contract::{BoundedVec, Digest256, ResourceId};
use crate::result_contract::cluster::is_valid_server_name;

/// The component-id prefix every connector-pack member carries. Overrides and
/// content lookups only ever address pack members.
const PACK_MEMBER_PREFIX: &str = crate::connector_pack::ids::PACK_COMPONENT_ID_PREFIX;

/// How many of each MCP primitive the probe saw.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DiscoveryCounts {
    pub tools: u32,
    pub skills: u32,
    pub prompts: u32,
    pub resources: u32,
}

/// Record one server's latest discovery observation under one scope.
///
/// One record per `(tenant, server_name, scope)`; a later observation replaces
/// the earlier one through a compare-and-set on its revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct FleetDiscoveryRecordRequest {
    /// The registry name the server registers under.
    pub server_name: String,
    pub scope: DiscoveryScope,
    /// The connector whose pack holds what this probe observed. The join from
    /// observation to content: every `mcp:<connector>/...` component inherits
    /// this observation's visibility.
    pub connector: ResourceId,
    pub outcome: DiscoveryOutcome,
    #[serde(default)]
    pub counts: DiscoveryCounts,
    /// `None` writes unconditionally; `Some(0)` requires that no record exists
    /// yet; `Some(n)` requires the stored revision to be exactly `n`.
    #[serde(default)]
    pub expected_revision: Option<u64>,
}

impl FleetDiscoveryRecordRequest {
    pub fn validate(&self) -> Result<(), String> {
        if !is_valid_server_name(&self.server_name) {
            return Err(
                "INVALID_ARGUMENT: server_name must match ^[A-Za-z0-9_.-]{1,128}$".to_string(),
            );
        }
        // A connector containing `/` would make `mcp:<connector>/` a prefix of
        // another connector's component ids.
        if self.connector.as_str().contains('/') {
            return Err("INVALID_ARGUMENT: connector must not contain '/'".to_string());
        }
        match &self.outcome {
            DiscoveryOutcome::Reachable => Ok(()),
            DiscoveryOutcome::Unreachable { error } => validate_error_text(error),
        }
    }
}

fn validate_error_text(error: &str) -> Result<(), String> {
    if error.is_empty() || error.len() > MAX_DISCOVERY_ERROR_BYTES {
        return Err(format!(
            "INVALID_ARGUMENT: an unreachable outcome needs a 1..={MAX_DISCOVERY_ERROR_BYTES} byte error"
        ));
    }
    if error.chars().any(char::is_control) {
        return Err("INVALID_ARGUMENT: discovery error must not contain control characters".into());
    }
    Ok(())
}

/// Set one durable operator override.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct FleetOverrideSetRequest {
    pub component_id: String,
    pub value: FleetOverride,
    /// Same compare-and-set meaning as on a discovery record.
    #[serde(default)]
    pub expected_revision: Option<u64>,
}

impl FleetOverrideSetRequest {
    pub fn validate(&self) -> Result<(), String> {
        validate_member_id(&self.component_id)
    }
}

/// Clear one durable operator override. A cleared override is a tombstone
/// revision, not a deletion, so the compare-and-set chain never restarts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct FleetOverrideClearRequest {
    pub component_id: String,
    pub field: FleetOverrideField,
    #[serde(default)]
    pub expected_revision: Option<u64>,
}

impl FleetOverrideClearRequest {
    pub fn validate(&self) -> Result<(), String> {
        validate_member_id(&self.component_id)
    }
}

fn validate_member_id(component_id: &str) -> Result<(), String> {
    if component_id.len() > MAX_FLEET_COMPONENT_ID_BYTES {
        return Err(format!(
            "INVALID_ARGUMENT: component_id exceeds {MAX_FLEET_COMPONENT_ID_BYTES} bytes"
        ));
    }
    component_id
        .strip_prefix(PACK_MEMBER_PREFIX)
        .filter(|rest| !rest.is_empty())
        .map(|_| ())
        .ok_or_else(|| {
            format!(
                "INVALID_ARGUMENT: component_id must name a connector pack member ('{PACK_MEMBER_PREFIX}...')"
            )
        })
}

/// Where a page resumes, fenced to the snapshot that produced it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct FleetCatalogCursor {
    /// Exclusive `(lowercased name, id)` lower bound for the next page.
    pub after_name: String,
    pub after_id: String,
    /// Digest of the whole visible, filtered snapshot of the page's kind. The
    /// fence is content, not a graph revision: a registry heartbeat that changes
    /// nothing this kind shows must not invalidate a caller's cursor.
    pub snapshot_digest: Digest256,
}

/// Read one bounded page of one kind.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct FleetCatalogListRequest {
    pub kind: FleetCatalogKind,
    /// Case-insensitive substring over name, server name and description.
    #[serde(default)]
    pub query: Option<String>,
    /// The caller's CURRENT grant fingerprints. Only narrows: an OAuth-scoped
    /// row is visible when its principal is the verified caller AND its grant
    /// is listed here; nothing listed here can widen past the caller.
    #[serde(default)]
    pub grant_digests: BoundedVec<Digest256, MAX_FLEET_GRANT_DIGESTS>,
    /// `None` selects the maximum page size.
    #[serde(default)]
    pub limit: Option<u16>,
    #[serde(default)]
    pub cursor: Option<FleetCatalogCursor>,
}

impl FleetCatalogListRequest {
    pub fn validate(&self) -> Result<(), String> {
        if let Some(limit) = self.limit {
            if limit == 0 || usize::from(limit) > MAX_FLEET_PAGE_ROWS {
                return Err(format!(
                    "INVALID_ARGUMENT: fleet catalog limit must be 1..={MAX_FLEET_PAGE_ROWS}"
                ));
            }
        }
        if let Some(query) = &self.query {
            if query.len() > MAX_FLEET_QUERY_BYTES {
                return Err(format!(
                    "INVALID_ARGUMENT: fleet catalog query exceeds {MAX_FLEET_QUERY_BYTES} bytes"
                ));
            }
        }
        if self
            .cursor
            .as_ref()
            .is_some_and(|cursor| cursor.after_id.is_empty())
        {
            return Err("INVALID_ARGUMENT: fleet catalog cursor has an empty after_id".to_string());
        }
        Ok(())
    }

    /// The page size this request asks for.
    pub fn page_limit(&self) -> usize {
        self.limit.map(usize::from).unwrap_or(MAX_FLEET_PAGE_ROWS)
    }
}

/// Read the visible rows for a batch of ids of any kind: component ids for
/// content rows, discovery ids for observations. An id the caller may not see
/// is absent from the answer exactly as an unknown id is.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct FleetCatalogLookupRequest {
    pub ids: BoundedVec<String, MAX_FLEET_LOOKUP_IDS>,
    #[serde(default)]
    pub grant_digests: BoundedVec<Digest256, MAX_FLEET_GRANT_DIGESTS>,
}

impl FleetCatalogLookupRequest {
    pub fn validate(&self) -> Result<(), String> {
        if self.ids.is_empty() {
            return Err("INVALID_ARGUMENT: fleet catalog lookup names no ids".to_string());
        }
        if self
            .ids
            .iter()
            .any(|id| id.is_empty() || id.len() > MAX_FLEET_COMPONENT_ID_BYTES)
        {
            return Err(format!(
                "INVALID_ARGUMENT: every lookup id must be 1..={MAX_FLEET_COMPONENT_ID_BYTES} bytes"
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(outcome: DiscoveryOutcome) -> FleetDiscoveryRecordRequest {
        FleetDiscoveryRecordRequest {
            server_name: "github".to_string(),
            scope: DiscoveryScope::TenantLocal,
            connector: ResourceId::new("github").unwrap(),
            outcome,
            counts: DiscoveryCounts::default(),
            expected_revision: None,
        }
    }

    #[test]
    fn discovery_record_bounds_name_connector_and_error() {
        assert!(record(DiscoveryOutcome::Reachable).validate().is_ok());
        let mut bad_name = record(DiscoveryOutcome::Reachable);
        bad_name.server_name = "no spaces".to_string();
        assert!(bad_name.validate().is_err());
        let mut nested = record(DiscoveryOutcome::Reachable);
        nested.connector = ResourceId::new("github/extra").unwrap();
        assert!(nested.validate().is_err());
        for error in ["", "line\nbreak"] {
            let unreachable = record(DiscoveryOutcome::Unreachable {
                error: error.to_string(),
            });
            assert!(unreachable.validate().is_err(), "{error:?}");
        }
        let unreachable = record(DiscoveryOutcome::Unreachable {
            error: "connection refused".to_string(),
        });
        assert!(unreachable.validate().is_ok());
    }

    #[test]
    fn overrides_only_address_pack_members() {
        let clear = |component_id: &str| FleetOverrideClearRequest {
            component_id: component_id.to_string(),
            field: FleetOverrideField::SkillType,
            expected_revision: None,
        };
        assert!(clear("mcp:skills/skill/triage").validate().is_ok());
        for invalid in ["skill:triage", "mcp:", ""] {
            assert!(clear(invalid).validate().is_err(), "{invalid:?}");
        }
    }

    #[test]
    fn list_limit_is_positive_and_bounded() {
        let list = |limit| FleetCatalogListRequest {
            kind: FleetCatalogKind::Skills,
            query: None,
            grant_digests: BoundedVec::default(),
            limit,
            cursor: None,
        };
        assert_eq!(list(None).page_limit(), MAX_FLEET_PAGE_ROWS);
        assert!(list(Some(1)).validate().is_ok());
        assert!(list(Some(0)).validate().is_err());
        assert!(list(Some(257)).validate().is_err());
    }

    #[test]
    fn lookup_needs_bounded_ids() {
        let lookup = |ids: Vec<String>| FleetCatalogLookupRequest {
            ids: BoundedVec::new(ids).unwrap(),
            grant_digests: BoundedVec::default(),
        };
        assert!(lookup(Vec::new()).validate().is_err());
        assert!(lookup(vec![String::new()]).validate().is_err());
        assert!(lookup(vec!["mcp:a/tool/b".to_string()]).validate().is_ok());
    }
}
