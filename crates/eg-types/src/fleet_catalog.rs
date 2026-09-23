//! The fleet catalog's wire contract (EH-345).
//!
//! There is ONE fleet catalog answer in the engine, and it is not a table. A
//! fleet server's liveness and desired registration are `:Server` rows
//! (`RegisterServer`); what it serves -- tools, prompts, resources, skills --
//! are `AgentComponent` records a connector pack publishes. What neither of
//! those records says is *who observed what*: which principal probed which
//! server under which authorization, and which connector's pack holds what that
//! probe saw. That observation, plus the operator's durable overrides, is what
//! this contract adds, and the read projection joins all three.
//!
//! * [`request`] -- what a caller sends.
//! * [`row`] -- what the projection returns.
//! * [`vocabulary`] -- the closed enums both use.
//!
//! Tenant and principal are never fields a caller supplies: the server stamps
//! both from the verified request context, so a record cannot claim to have
//! been observed by someone else.

use serde::{Deserialize, Serialize};

pub mod request;
pub mod row;
pub mod vocabulary;

pub use request::{
    DiscoveryCounts, FleetCatalogCursor, FleetCatalogListRequest, FleetCatalogLookupRequest,
    FleetDiscoveryRecordRequest, FleetOverrideClearRequest, FleetOverrideSetRequest,
};
pub use row::{
    FleetCatalogLookup, FleetCatalogPage, FleetCatalogRow, FleetComponentRef, FleetDiscoveryRow,
    FleetPromptRow, FleetResourceRow, FleetRowAcl, FleetRowSubject, FleetSkillRow, FleetToolRow,
    FleetWriteReceipt,
};
pub use vocabulary::{
    DiscoveryOutcome, DiscoveryScope, FleetCatalogKind, FleetOverride, FleetOverrideField,
    FleetVisibility, FleetWriteDisposition, ResourceKind, SkillType, SkillTypeSource, ToolMode,
};

/// Format identity (RF-ADR-006) of every fleet catalog page and record.
pub const FLEET_CATALOG_SCHEMA_VERSION: u16 = 1;
/// Most rows one [`FleetCatalogPage`] may carry, and the default page size.
pub const MAX_FLEET_PAGE_ROWS: usize = 256;
/// Most ids one [`FleetCatalogLookupRequest`] may name.
pub const MAX_FLEET_LOOKUP_IDS: usize = 256;
/// Most grant digests one read may present.
pub const MAX_FLEET_GRANT_DIGESTS: usize = 64;
/// Longest case-insensitive substring filter.
pub const MAX_FLEET_QUERY_BYTES: usize = 128;
/// Longest component id an override or lookup may name.
pub const MAX_FLEET_COMPONENT_ID_BYTES: usize = 1024;
/// Longest reachability error a discovery observation may record.
pub const MAX_DISCOVERY_ERROR_BYTES: usize = 1024;
/// Most rows one kind's visible snapshot may hold.
///
/// A page is digest-fenced over the WHOLE visible snapshot of its kind, so the
/// snapshot itself needs a bound. A tenant past it is refused by name
/// (`FLEET_SNAPSHOT_TOO_LARGE`), never silently truncated: a truncated snapshot
/// would page, count and digest a catalog that does not exist.
pub const MAX_FLEET_SNAPSHOT_ROWS: usize = 16_384;

/// Every fleet catalog operation.
///
/// The read/write split and the authorization action live on the op, so the
/// capability ledger and the server's write classifier cannot disagree about
/// an operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum FleetCatalogOp {
    /// Record one server's latest discovery observation under one scope.
    RecordDiscovery {
        request: FleetDiscoveryRecordRequest,
    },
    /// Set one durable operator override on a published component.
    SetOverride { request: FleetOverrideSetRequest },
    /// Clear one durable operator override.
    ClearOverride { request: FleetOverrideClearRequest },
    /// Read one bounded, snapshot-fenced page of one kind.
    List { request: FleetCatalogListRequest },
    /// Read the visible rows for a batch of ids, of any kind.
    Lookup { request: FleetCatalogLookupRequest },
}

impl FleetCatalogOp {
    /// Whether this operation commits durable state.
    pub fn is_mutation(&self) -> bool {
        match self {
            Self::RecordDiscovery { .. }
            | Self::SetOverride { .. }
            | Self::ClearOverride { .. } => true,
            Self::List { .. } | Self::Lookup { .. } => false,
        }
    }

    /// The authorization action this operation needs.
    ///
    /// Recording an observation is the registry's ordinary write privilege --
    /// the same one `RegisterServer` needs. Overriding what a component IS
    /// changes what every principal in the tenant reads, so it is an operator
    /// decision and `admin:` gated.
    pub fn authz_action(&self) -> &'static str {
        match self {
            Self::RecordDiscovery { .. } => "registry:write",
            Self::SetOverride { .. } | Self::ClearOverride { .. } => "admin:fleet-catalog",
            Self::List { .. } | Self::Lookup { .. } => "registry:read",
        }
    }

    /// The op's wire tag, for audit lines and diagnostics.
    pub fn name(&self) -> &'static str {
        match self {
            Self::RecordDiscovery { .. } => "record_discovery",
            Self::SetOverride { .. } => "set_override",
            Self::ClearOverride { .. } => "clear_override",
            Self::List { .. } => "list",
            Self::Lookup { .. } => "lookup",
        }
    }

    /// Structural validation, before any authority is consulted.
    pub fn validate(&self) -> Result<(), String> {
        match self {
            Self::RecordDiscovery { request } => request.validate(),
            Self::SetOverride { request } => request.validate(),
            Self::ClearOverride { request } => request.validate(),
            Self::List { request } => request.validate(),
            Self::Lookup { request } => request.validate(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contract::{BoundedVec, ResourceId};

    fn list() -> FleetCatalogOp {
        FleetCatalogOp::List {
            request: FleetCatalogListRequest {
                kind: FleetCatalogKind::Tools,
                query: None,
                grant_digests: BoundedVec::default(),
                limit: None,
                cursor: None,
            },
        }
    }

    fn record() -> FleetCatalogOp {
        FleetCatalogOp::RecordDiscovery {
            request: FleetDiscoveryRecordRequest {
                server_name: "github".to_string(),
                scope: DiscoveryScope::TenantLocal,
                connector: ResourceId::new("github").unwrap(),
                outcome: DiscoveryOutcome::Reachable,
                counts: DiscoveryCounts::default(),
                expected_revision: None,
            },
        }
    }

    #[test]
    fn reads_and_writes_are_split_on_the_op() {
        assert!(!list().is_mutation());
        assert_eq!(list().authz_action(), "registry:read");
        assert!(record().is_mutation());
        assert_eq!(record().authz_action(), "registry:write");
        let set = FleetCatalogOp::SetOverride {
            request: FleetOverrideSetRequest {
                component_id: "mcp:skills/skill/triage".to_string(),
                value: FleetOverride::SkillType {
                    skill_type: SkillType::Workflow,
                },
                expected_revision: None,
            },
        };
        assert!(set.is_mutation());
        assert_eq!(set.authz_action(), "admin:fleet-catalog");
        assert_eq!(set.name(), "set_override");
        let tag =
            rmp_serde::from_slice::<serde_json::Value>(&rmp_serde::to_vec_named(&set).unwrap())
                .unwrap()["op"]
                .clone();
        assert_eq!(tag, serde_json::json!(set.name()), "name() is the wire tag");
    }

    #[test]
    fn op_wire_form_is_tagged_and_closed() {
        let encoded = rmp_serde::to_vec_named(&list()).unwrap();
        let decoded: FleetCatalogOp = rmp_serde::from_slice(&encoded).unwrap();
        assert_eq!(decoded, list());
        let unknown = rmp_serde::to_vec_named(&serde_json::json!({
            "op": "list",
            "request": {"kind": "tools", "tenant_id": "forged"},
        }))
        .unwrap();
        assert!(rmp_serde::from_slice::<FleetCatalogOp>(&unknown).is_err());
    }
}
