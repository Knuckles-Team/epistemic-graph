//! The `Method::ConnectorPack` operation set.
//!
//! One read (`status`), one bulk import, and five administrative operations.
//! The read/write split and the authorization action both live on the op, so
//! the capability ledger and `server::access::requires_write` cannot drift
//! apart about an operation.

use serde::{Deserialize, Serialize};

use super::index::ConnectorPackIndex;
use crate::agent_library::AgentLibraryMutationContext;
use crate::contract::{BoundedVec, Digest256, ResourceId};

/// The head a caller believes is current, for a compare-and-set import.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PackHeadRef {
    pub binding_revision: u64,
    pub pack_digest: Digest256,
}

/// Read one connector's pack state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ConnectorPackStatusRequest {
    pub tenant_id: String,
    pub connector: ResourceId,
}

/// Import one pack atomically.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ConnectorPackImportRequest {
    pub context: AgentLibraryMutationContext,
    pub index: ConnectorPackIndex,
    /// Compare-and-set against the head the caller last saw.
    #[serde(default)]
    pub expected_head: Option<PackHeadRef>,
    /// An import that withdraws most of a connector's surface is usually a
    /// broken build, not an intention, so it needs a second, administrative
    /// grant rather than the ordinary pack-control one.
    #[serde(default)]
    pub allow_mass_withdrawal: bool,
}

/// Bind a connector to the importer allowed to publish its packs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ConnectorPackBindRequest {
    pub context: AgentLibraryMutationContext,
    pub connector: ResourceId,
    pub importer: String,
}

/// Remove a connector's importer binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ConnectorPackUnbindRequest {
    pub context: AgentLibraryMutationContext,
    pub connector: ResourceId,
}

/// Permanently retire named entries of a connector.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ConnectorPackRetireRequest {
    pub context: AgentLibraryMutationContext,
    pub connector: ResourceId,
    pub uris: BoundedVec<String, 1024>,
}

/// Re-project the current head into the graph.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ConnectorPackReprojectRequest {
    pub context: AgentLibraryMutationContext,
    pub connector: ResourceId,
}

/// Sweep engine-owned bodies no revision holds any more.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ConnectorPackReconcileRequest {
    pub context: AgentLibraryMutationContext,
}

/// Every connector-pack operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ConnectorPackOp {
    Status {
        request: ConnectorPackStatusRequest,
    },
    /// Boxed: an import carries a whole pack index and would otherwise set the
    /// size of every `Method`.
    Import {
        request: Box<ConnectorPackImportRequest>,
    },
    Bind {
        request: ConnectorPackBindRequest,
    },
    Unbind {
        request: ConnectorPackUnbindRequest,
    },
    Retire {
        request: ConnectorPackRetireRequest,
    },
    Reproject {
        request: ConnectorPackReprojectRequest,
    },
    ReconcileBodies {
        request: ConnectorPackReconcileRequest,
    },
}

impl ConnectorPackOp {
    /// Whether this operation commits durable state. Only `status` reads.
    pub fn is_mutation(&self) -> bool {
        !matches!(self, Self::Status { .. })
    }

    /// The authorization action this operation needs.
    ///
    /// Three actions, not one. Reading a connector's surface is ordinary;
    /// publishing a pack is the connector's own privilege; and binding,
    /// retiring, re-projecting or withdrawing at scale changes what the
    /// connector IS ALLOWED to publish, which is an operator decision.
    pub fn authz_action(&self) -> &'static str {
        match self {
            Self::Status { .. } => "agent:pack-read",
            Self::Import { request } if !request.allow_mass_withdrawal => "agent:pack-control",
            Self::Import { .. }
            | Self::Bind { .. }
            | Self::Unbind { .. }
            | Self::Retire { .. }
            | Self::Reproject { .. }
            | Self::ReconcileBodies { .. } => "admin:connector-pack",
        }
    }

    /// The tenant this operation names, compared against the verified request
    /// tenant in the handler.
    pub fn tenant_id(&self) -> &str {
        match self {
            Self::Status { request } => &request.tenant_id,
            Self::Import { request } => &request.context.tenant_id,
            Self::Bind { request } => &request.context.tenant_id,
            Self::Unbind { request } => &request.context.tenant_id,
            Self::Retire { request } => &request.context.tenant_id,
            Self::Reproject { request } => &request.context.tenant_id,
            Self::ReconcileBodies { request } => &request.context.tenant_id,
        }
    }

    /// The connector this operation names, or `None` for the tenant-wide
    /// body reconciliation sweep.
    pub fn connector(&self) -> Option<&ResourceId> {
        match self {
            Self::Status { request } => Some(&request.connector),
            Self::Import { request } => Some(&request.index.connector),
            Self::Bind { request } => Some(&request.connector),
            Self::Unbind { request } => Some(&request.connector),
            Self::Retire { request } => Some(&request.connector),
            Self::Reproject { request } => Some(&request.connector),
            Self::ReconcileBodies { .. } => None,
        }
    }
}
