//! Typed per-operation senders for tagged op-family methods.
//!
//! Each row names one already-served operation and the request/result DTOs its
//! dynamic parent selects; the renderer in the parent module emits one typed
//! `send_<method>_<operation>` per row. A registry table, not logic.

/// A typed adapter for one operation inside an existing tagged method.
///
/// This remains generator-owned: it does not mint a Method id or grow a
/// hand-written client facade. The declaration only identifies the already
/// served operation and the request/result DTOs its dynamic parent selects.
pub(super) struct TypedOperationAdapter {
    pub(super) method: &'static str,
    pub(super) operation: &'static str,
    pub(super) request_model: &'static str,
    pub(super) result_model: &'static str,
    /// Tagged union aliases need pydantic's `TypeAdapter`; concrete models
    /// expose `model_validate` directly.
    pub(super) result_is_union: bool,
}

pub(super) const TYPED_OPERATION_ADAPTERS: &[TypedOperationAdapter] = &[
    TypedOperationAdapter {
        method: "AgentComponent",
        operation: "search",
        request_model: "AgentComponentSearchRequest",
        result_model: "AgentComponentSearchPage",
        result_is_union: false,
    },
    TypedOperationAdapter {
        method: "AgentComponent",
        operation: "content",
        request_model: "AgentComponentContentRequest",
        result_model: "AgentComponentContentResult",
        result_is_union: false,
    },
    TypedOperationAdapter {
        method: "AgentComponent",
        operation: "current",
        // Current's Rust wire variant carries its fields directly instead of
        // nesting a separate request DTO. Reuse that generated operation type
        // so Python cannot mint a second shape for the same contract.
        request_model: "AgentComponentOpCurrent",
        result_model: "AgentComponentEntry | None",
        result_is_union: true,
    },
    TypedOperationAdapter {
        method: "FleetCatalog",
        operation: "record_discovery",
        request_model: "FleetDiscoveryRecordRequest",
        result_model: "FleetWriteReceipt",
        result_is_union: false,
    },
    TypedOperationAdapter {
        method: "FleetCatalog",
        operation: "set_override",
        request_model: "FleetOverrideSetRequest",
        result_model: "FleetWriteReceipt",
        result_is_union: false,
    },
    TypedOperationAdapter {
        method: "FleetCatalog",
        operation: "clear_override",
        request_model: "FleetOverrideClearRequest",
        result_model: "FleetWriteReceipt",
        result_is_union: false,
    },
    TypedOperationAdapter {
        method: "FleetCatalog",
        operation: "list",
        request_model: "FleetCatalogListRequest",
        result_model: "FleetCatalogPage",
        result_is_union: false,
    },
    TypedOperationAdapter {
        method: "FleetCatalog",
        operation: "lookup",
        request_model: "FleetCatalogLookupRequest",
        result_model: "FleetCatalogLookup",
        result_is_union: false,
    },
    TypedOperationAdapter {
        method: "ConnectorPack",
        operation: "status",
        request_model: "ConnectorPackStatusRequest",
        result_model: "ConnectorPackStatus",
        result_is_union: false,
    },
    TypedOperationAdapter {
        method: "ConnectorPack",
        operation: "import",
        request_model: "ConnectorPackImportRequest",
        result_model: "PackImportResult",
        result_is_union: true,
    },
    TypedOperationAdapter {
        method: "ConnectorPack",
        operation: "attest_self_served_catalog",
        request_model: "McpSelfServedCatalogAttestRequest",
        result_model: "McpCatalogSnapshotBinding",
        result_is_union: false,
    },
    TypedOperationAdapter {
        method: "ConnectorPack",
        operation: "catalog_authority_status",
        request_model: "McpCatalogAuthorityStatusRequest",
        result_model: "McpCatalogSnapshotBinding | None",
        result_is_union: true,
    },
    TypedOperationAdapter {
        method: "PolicyEvolution",
        operation: "put_capability",
        request_model: "OpenWeightPolicyCapability",
        result_model: "PolicyRecordReceipt",
        result_is_union: false,
    },
    TypedOperationAdapter {
        method: "PolicyEvolution",
        operation: "commit_capture",
        request_model: "PolicyCapture",
        result_model: "PolicyRecordReceipt",
        result_is_union: false,
    },
    TypedOperationAdapter {
        method: "PolicyEvolution",
        operation: "register_model_policy_version",
        request_model: "ModelPolicyVersion",
        result_model: "PolicyRecordReceipt",
        result_is_union: false,
    },
    TypedOperationAdapter {
        method: "PolicyEvolution",
        operation: "commit_training_run",
        request_model: "TrainingRun",
        result_model: "PolicyRecordReceipt",
        result_is_union: false,
    },
    TypedOperationAdapter {
        method: "PolicyEvolution",
        operation: "commit_policy_evaluation",
        request_model: "PolicyEvaluation",
        result_model: "PolicyRecordReceipt",
        result_is_union: false,
    },
    TypedOperationAdapter {
        method: "PolicyEvolution",
        operation: "get",
        request_model: "PolicyRecordGetRequest",
        result_model: "PolicyRecordView | None",
        result_is_union: true,
    },
];
