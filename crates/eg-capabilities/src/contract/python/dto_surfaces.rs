//! The generated nested-DTO surfaces: one registry row per method whose
//! typed Python module [`super::surfaces`] emits.

/// One generated nested-DTO surface. The renderer is schema-driven: roots name
/// JSON-Schema definitions and every transitive `$ref` is collected from the
/// merged request/result definitions. Adding another surface is a registry row, not a
/// hand-written generated module.
pub(super) struct DtoSurface {
    pub(super) method: &'static str,
    pub(super) module: &'static str,
    pub(super) roots: &'static [&'static str],
    pub(super) result_model: Option<&'static str>,
    pub(super) required: bool,
    /// Public wire-format identities owned by the Rust contract. Keeping them
    /// generated prevents a Python builder from copying a release number.
    pub(super) constants: &'static [(&'static str, u64)],
}

pub(super) const DTO_SURFACES: &[DtoSurface] = &[
    // The Decide layer's served surfaces opt in to nested models (DECIDE-LAYER-
    // DESIGN §4.9): only these result markers change; every other method's
    // generated code is byte-identical.
    DtoSurface {
        method: "AgentAssemble",
        module: "decision",
        roots: &["AssemblyRequest", "AssemblyResult", "DecisionRecord"],
        result_model: Some("AssemblyResult"),
        required: true,
        constants: &[
            (
                "DECISION_RECORD_SCHEMA_VERSION",
                eg_types::decision::DECISION_RECORD_SCHEMA_VERSION as u64,
            ),
            (
                "ASSEMBLY_RESULT_SCHEMA_VERSION",
                eg_types::decision::ASSEMBLY_RESULT_SCHEMA_VERSION as u64,
            ),
            (
                "MAX_DECISION_RECORD_BYTES",
                eg_types::decision::MAX_DECISION_RECORD_BYTES as u64,
            ),
        ],
    },
    DtoSurface {
        method: "DecisionCommit",
        module: "decision_commit",
        roots: &["DecisionCommitRequest", "DecisionCommitResult"],
        result_model: Some("DecisionCommitResult"),
        required: true,
        constants: &[(
            "DECISION_COMMIT_RESULT_SCHEMA_VERSION",
            eg_types::decision::DECISION_COMMIT_RESULT_SCHEMA_VERSION as u64,
        )],
    },
    DtoSurface {
        method: "Solve",
        module: "solve",
        roots: &["SolveRequest", "SolveResult"],
        result_model: Some("SolveResult"),
        required: true,
        constants: &[(
            "SOLVE_RESULT_SCHEMA_VERSION",
            eg_types::solve::SOLVE_RESULT_SCHEMA_VERSION as u64,
        )],
    },
    DtoSurface {
        method: "ListRegisteredServers",
        module: "server_registry",
        roots: &[
            "RegisteredServerListRequest",
            "RegisteredServerCursor",
            "RegisteredServerView",
            "RegisteredServerListPage",
        ],
        result_model: Some("RegisteredServerListPage"),
        required: true,
        constants: &[],
    },
    DtoSurface {
        method: "FleetCatalog",
        module: "fleet_catalog",
        roots: &[
            "FleetCatalogOp",
            "FleetWriteReceipt",
            "FleetCatalogPage",
            "FleetCatalogLookup",
        ],
        result_model: None,
        required: true,
        constants: &[(
            "FLEET_CATALOG_SCHEMA_VERSION",
            eg_types::fleet_catalog::FLEET_CATALOG_SCHEMA_VERSION as u64,
        )],
    },
    DtoSurface {
        method: "AgentComponent",
        module: "agent_component",
        roots: &[
            "AgentComponentOp",
            "AgentComponentEntry",
            "AgentComponentContentRequest",
            "AgentComponentContentResult",
            "AgentComponentSearchRequest",
            "AgentComponentSearchPage",
        ],
        result_model: None,
        required: true,
        constants: &[],
    },
    DtoSurface {
        method: "WriteBack",
        module: "write_back",
        roots: &["WriteBackOp", "WriteBackReceiptPage"],
        result_model: None,
        required: true,
        constants: &[],
    },
    DtoSurface {
        method: "ConnectorPack",
        module: "connector_pack",
        roots: &[
            "ConnectorPackOp",
            "ConnectorPackImportRequest",
            "ConnectorPackStatusRequest",
            "ConnectorPackStatus",
            "PackImportResult",
            "PackWriteErrorCode",
        ],
        result_model: None,
        required: true,
        constants: &[(
            "CONNECTOR_PACK_SCHEMA_VERSION",
            eg_types::connector_pack::CONNECTOR_PACK_SCHEMA_VERSION as u64,
        )],
    },
    DtoSurface {
        method: "SourceIngest",
        module: "source_ingestion",
        roots: &[
            "SourceIngestionRequest",
            "SourceIngestionReceipt",
            "SourceIngestStatus",
        ],
        result_model: Some("SourceIngestionReceipt"),
        required: true,
        constants: &[],
    },
    DtoSurface {
        method: "IndexRepository",
        module: "index_repository",
        roots: &["IndexRepositoryScope", "IndexResult"],
        result_model: Some("IndexResult"),
        required: true,
        constants: &[],
    },
    DtoSurface {
        method: "GraphSchema",
        module: "graph_schema",
        roots: &[
            "GraphSchemaOp",
            "GraphSchemaCommitted",
            "GraphSchemaSourceView",
            "GraphSchemaSourcesView",
            "SchemaSourceOriginView",
        ],
        result_model: Some("GraphSchemaCommitted"),
        required: true,
        constants: &[],
    },
    DtoSurface {
        method: "EdgeIndex",
        module: "managed_index",
        roots: &[
            "EdgeIndexOp",
            "EdgeIndexDefinition",
            "EdgeIndexKind",
            "EdgeVectorMetric",
            "EdgeIndexStatusView",
            "EdgeSearchRequest",
            "EdgeSearchQuery",
            "EdgePropertyEquals",
            "EdgeSearchView",
            "EdgeSearchHit",
            "ManagedIndexStatus",
            "ManagedIndexTarget",
            "ManagedIndexState",
            "ManagedIndexFamily",
            "IndexBlock",
            "IndexBlockReason",
        ],
        result_model: Some("EdgeIndexStatusView"),
        required: true,
        constants: &[],
    },
    DtoSurface {
        method: "OwlReason",
        module: "rdf_report",
        roots: &[
            "OwlReasonResult",
            "OwlPropertyFact",
            "OwlExplainResult",
            "ProofNodeWire",
            "DatalogReasoningResult",
            "ShaclValidationReport",
            "ShaclValidationResult",
            "ShaclSeverity",
        ],
        result_model: Some("OwlReasonResult"),
        required: true,
        constants: &[],
    },
    DtoSurface {
        method: "PolicyEvolution",
        module: "policy_evolution",
        roots: &[
            "PolicyEvolutionOp",
            "PolicyRecordReceipt",
            "PolicyRecordView",
        ],
        result_model: None,
        required: true,
        constants: &[
            (
                "POLICY_EVOLUTION_SCHEMA_VERSION",
                eg_types::policy_evolution::POLICY_EVOLUTION_SCHEMA_VERSION as u64,
            ),
            (
                "MAX_TRAINING_INPUTS",
                eg_types::policy_evolution::MAX_TRAINING_INPUTS as u64,
            ),
        ],
    },
];
