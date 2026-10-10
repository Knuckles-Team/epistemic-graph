//! The `Method::ConnectorPack` operation set.
//!
//! Five reads (`status`, `catalog_authority_status`, `catalog_owner_principal`,
//! `catalog_binding_status`, `catalog_request_owner_principal`), one bulk import, and
//! administrative operations, including the two catalog-authority writes
//! (`reconcile_catalog` for a mounted child, `attest_self_served_catalog` for a
//! producer serving its own catalog).
//! The read/write split and the authorization action both live on the op, so
//! the capability ledger and `server::access::requires_write` cannot drift
//! apart about an operation.

use serde::{Deserialize, Serialize};

use super::catalog_authority::{McpCatalogAuthorityStatusRequest, McpCatalogReconcileRequest};
use super::index::ConnectorPackIndex;
use super::self_served_catalog::McpSelfServedCatalogAttestRequest;
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
    ReconcileCatalog {
        /// Keep catalog reconciliation from setting the size of every pack operation.
        request: Box<McpCatalogReconcileRequest>,
    },
    /// Issue the binding for a catalog the verified attester serves itself,
    /// pinned to the exact server entry of the connector's next pack. Needs
    /// `connector:catalog-attest` as well as this op's action.
    AttestSelfServedCatalog {
        request: Box<McpSelfServedCatalogAttestRequest>,
    },
    CatalogAuthorityStatus {
        request: McpCatalogAuthorityStatusRequest,
    },
    /// Read the live owner principal only after proving this caller's scoped
    /// mounted-child authority row exists for the requested tenant/server.
    CatalogOwnerPrincipal {
        request: McpCatalogAuthorityStatusRequest,
    },
    /// Read the current tenant/server/scope binding as a pack-control service.
    /// Unlike attester status, this does not confer catalog reconciliation.
    CatalogBindingStatus {
        request: McpCatalogAuthorityStatusRequest,
    },
    /// Return EG's actual AgentLibrary mutation owner to a verified
    /// pack-control request identity for an existing scoped catalog row.
    CatalogRequestOwnerPrincipal {
        request: McpCatalogAuthorityStatusRequest,
    },
}

impl ConnectorPackOp {
    /// Whether this operation commits durable state.
    pub fn is_mutation(&self) -> bool {
        !matches!(
            self,
            Self::Status { .. }
                | Self::CatalogAuthorityStatus { .. }
                | Self::CatalogOwnerPrincipal { .. }
                | Self::CatalogBindingStatus { .. }
                | Self::CatalogRequestOwnerPrincipal { .. }
        )
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
            Self::ReconcileCatalog { .. } | Self::AttestSelfServedCatalog { .. } => {
                "admin:connector-pack"
            }
            Self::CatalogAuthorityStatus { .. } => "connector:catalog-attest",
            Self::CatalogOwnerPrincipal { .. } => "agent:pack-control",
            Self::CatalogBindingStatus { .. } => "agent:pack-control",
            Self::CatalogRequestOwnerPrincipal { .. } => "agent:pack-control",
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
            Self::ReconcileCatalog { request } => &request.context.tenant_id,
            Self::AttestSelfServedCatalog { request } => &request.context.tenant_id,
            Self::CatalogAuthorityStatus { request } => &request.tenant_id,
            Self::CatalogOwnerPrincipal { request } => &request.tenant_id,
            Self::CatalogBindingStatus { request } => &request.tenant_id,
            Self::CatalogRequestOwnerPrincipal { request } => &request.tenant_id,
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
            Self::ReconcileCatalog { .. } => None,
            Self::AttestSelfServedCatalog { request } => Some(&request.connector),
            Self::CatalogAuthorityStatus { .. } => None,
            Self::CatalogOwnerPrincipal { .. } => None,
            Self::CatalogBindingStatus { .. } => None,
            Self::CatalogRequestOwnerPrincipal { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ConnectorPackOp;

    /// R010: pack admission is a single atomic `Import`, not a staged
    /// prepare/commit/abort protocol -- there is no such wire variant to
    /// even reach a handler, because `ConnectorPackOp` is `#[serde(tag =
    /// "op", rename_all = "snake_case", deny_unknown_fields)]` and lists no
    /// `Prepare`/`Commit`/`Abort` case. A request naming one of those `op`
    /// values is refused at deserialization, before any admission logic runs.
    // spec: EG-TYPED-PACKS-R010
    #[test]
    fn pack_admission_exposes_only_import_with_no_staged_protocol_variant() {
        for staged_op in ["prepare", "commit", "abort"] {
            let wire = serde_json::json!({ "op": staged_op, "request": {} });
            let error = serde_json::from_value::<ConnectorPackOp>(wire)
                .expect_err("no Prepare/Commit/Abort variant exists for pack admission");
            assert!(
                error.to_string().contains("unknown variant"),
                "op {staged_op:?} must be refused as an unknown variant, got: {error}"
            );
        }
        // "import" at least names a real variant (its request shape is
        // covered by the served admission tests, not by this wire check).
        let unknown_but_real = serde_json::json!({ "op": "import", "request": {} });
        let error = serde_json::from_value::<ConnectorPackOp>(unknown_but_real)
            .expect_err("an empty request body is still missing required fields");
        assert!(
            !error.to_string().contains("unknown variant"),
            "import IS a real pack-admission variant, got: {error}"
        );
    }
}
