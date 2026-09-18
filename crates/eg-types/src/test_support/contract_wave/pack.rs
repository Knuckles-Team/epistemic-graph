//! Valid sample values for the connector pack surface.

use crate::agent_component::DeclaredLatency;
use crate::connector_pack::{
    ConnectorPackBindRequest, ConnectorPackImportRequest, ConnectorPackIndex, ConnectorPackOp,
    ConnectorPackReconcileRequest, ConnectorPackReprojectRequest, ConnectorPackRetireRequest,
    ConnectorPackStatusRequest, ConnectorPackUnbindRequest, PackAnnotations, PackArchiveRef,
    PackCost, PackDisposition, PackEntry, PackEntryKind, PackHeadRef, PackModelFacts, PackProducer,
    PackRef, PackSection, PackViolationCode, PackWarningCode, CONNECTOR_PACK_SCHEMA_VERSION,
};
use crate::contract::{Digest256, ResourceId};

use super::decision::mutation_context;
use super::{bounded, raw_digest};

/// The connector every sample names.
pub fn connector() -> ResourceId {
    ResourceId::new("connector-a").expect("sample connector id is canonical")
}

fn section(offset: u64, byte: u8) -> PackSection {
    PackSection {
        offset,
        length: 128,
        sha256: raw_digest(byte),
    }
}

/// Fully-populated annotations, so no optional field is left unexercised.
pub fn annotations() -> PackAnnotations {
    PackAnnotations {
        provides: bounded(vec!["eg:capability/retrieval".to_string()]),
        requires_capabilities: bounded(vec!["eg:capability/action".to_string()]),
        modalities_in: bounded(vec!["eg:modality/text".to_string()]),
        modalities_out: bounded(vec!["eg:modality/text".to_string()]),
        required_scopes: bounded(vec!["kg:read".to_string()]),
        read_only_hint: Some(true),
        destructive_hint: Some(false),
        idempotent_hint: None,
        open_world_hint: Some(true),
        contract_version: Some("1.4.0".to_string()),
        cost: Some(PackCost {
            currency: "USD".to_string(),
            per_call_micros: Some(2_500),
            input_per_mtok_micros: Some(300_000),
            output_per_mtok_micros: None,
        }),
        latency_declared: Some(DeclaredLatency {
            p50_ms: 120,
            p95_ms: 900,
        }),
        model: Some(PackModelFacts {
            provider: "vendor".to_string(),
            model_identity: "vendor/model-1".to_string(),
            context_window_tokens: 128_000,
            max_output_tokens: 8_192,
            supports_tools: Some(true),
            supports_structured_output: Some(true),
            supports_vision: Some(false),
        }),
        sdk_contract_pin: Some("sdk-d18-1".to_string()),
    }
}

/// One entry of `kind`, with both schema sections and one reference.
pub fn entry(kind: PackEntryKind, name: &str) -> PackEntry {
    PackEntry {
        kind,
        uri: format!("mcp://connector-a/{name}"),
        name: name.to_string(),
        media_type: "application/json".to_string(),
        body: section(0, 0xc1),
        input_schema: Some(section(128, 0xc2)),
        output_schema: Some(section(256, 0xc3)),
        annotations: annotations(),
        references: bounded(vec![PackRef {
            uri: "mcp://connector-a/server".to_string(),
            kind: PackEntryKind::McpServer,
        }]),
    }
}

/// One pack index carrying one entry of every kind.
pub fn index() -> ConnectorPackIndex {
    let kinds = [
        PackEntryKind::Tool,
        PackEntryKind::Skill,
        PackEntryKind::Prompt,
        PackEntryKind::Ontology,
        PackEntryKind::Shapes,
        PackEntryKind::ModelProfile,
        PackEntryKind::A2aCard,
        PackEntryKind::Manifest,
    ];
    ConnectorPackIndex {
        schema_version: CONNECTOR_PACK_SCHEMA_VERSION,
        connector: connector(),
        server: entry(PackEntryKind::McpServer, "server"),
        server_package_version: "2.3.1".to_string(),
        archive: PackArchiveRef {
            blob_digest: super::digest_text(0xc4),
            length: 4_096,
            sha256: raw_digest(0xc5),
        },
        entries: bounded(
            kinds
                .into_iter()
                .enumerate()
                .map(|(position, kind)| entry(kind, &format!("entry-{position}")))
                .collect(),
        ),
        producer: PackProducer {
            name: "agent-packages".to_string(),
            version: "0.9.0".to_string(),
        },
        pack_digest: raw_digest(0xc6),
    }
}

fn head_ref() -> PackHeadRef {
    PackHeadRef {
        binding_revision: 3,
        pack_digest: raw_digest(0xc7),
    }
}

/// Every connector-pack operation, labelled by its wire op tag.
pub fn ops() -> Vec<(&'static str, ConnectorPackOp)> {
    vec![
        (
            "ConnectorPack.status",
            ConnectorPackOp::Status {
                request: ConnectorPackStatusRequest {
                    tenant_id: "tenant-a".to_string(),
                    connector: connector(),
                },
            },
        ),
        (
            "ConnectorPack.import",
            ConnectorPackOp::Import {
                request: Box::new(ConnectorPackImportRequest {
                    context: mutation_context(),
                    index: index(),
                    expected_head: Some(head_ref()),
                    allow_mass_withdrawal: false,
                }),
            },
        ),
        (
            "ConnectorPack.bind",
            ConnectorPackOp::Bind {
                request: ConnectorPackBindRequest {
                    context: mutation_context(),
                    connector: connector(),
                    importer: "importer-a".to_string(),
                },
            },
        ),
        (
            "ConnectorPack.unbind",
            ConnectorPackOp::Unbind {
                request: ConnectorPackUnbindRequest {
                    context: mutation_context(),
                    connector: connector(),
                },
            },
        ),
        (
            "ConnectorPack.retire",
            ConnectorPackOp::Retire {
                request: ConnectorPackRetireRequest {
                    context: mutation_context(),
                    connector: connector(),
                    uris: bounded(vec!["mcp://connector-a/entry-0".to_string()]),
                },
            },
        ),
        (
            "ConnectorPack.reproject",
            ConnectorPackOp::Reproject {
                request: ConnectorPackReprojectRequest {
                    context: mutation_context(),
                    connector: connector(),
                },
            },
        ),
        (
            "ConnectorPack.reconcile_bodies",
            ConnectorPackOp::ReconcileBodies {
                request: ConnectorPackReconcileRequest {
                    context: mutation_context(),
                },
            },
        ),
    ]
}

/// The mass-withdrawal import, which needs the administrative grant.
pub fn mass_withdrawal_import() -> ConnectorPackOp {
    ConnectorPackOp::Import {
        request: Box::new(ConnectorPackImportRequest {
            context: mutation_context(),
            index: index(),
            expected_head: None,
            allow_mass_withdrawal: true,
        }),
    }
}

/// Every rejection code the importer can report.
pub fn every_violation_code() -> Vec<PackViolationCode> {
    vec![
        PackViolationCode::MalformedIndex,
        PackViolationCode::UnknownEntryKind,
        PackViolationCode::PackTooLarge,
        PackViolationCode::ArchiveMissing,
        PackViolationCode::ArchiveDigestMismatch,
        PackViolationCode::MalformedSections,
        PackViolationCode::PackDigestMismatch,
        PackViolationCode::DuplicateComponentId,
        PackViolationCode::ForbiddenEntryKind,
        PackViolationCode::MalformedBody,
        PackViolationCode::MissingToolSchema,
        PackViolationCode::UnknownCapabilityIri,
        PackViolationCode::InvalidAnnotation,
        PackViolationCode::InvalidFacts,
        PackViolationCode::OntologyInvalid,
        PackViolationCode::ShapesInvalid,
        PackViolationCode::OntologyInconsistent,
        PackViolationCode::ValidationBudgetExceeded,
        PackViolationCode::ShaclViolation,
        PackViolationCode::UnresolvedReference,
        PackViolationCode::ReferenceCycle,
        PackViolationCode::InvalidComponent,
        PackViolationCode::ImporterMismatch,
        PackViolationCode::PackMassWithdrawal,
        PackViolationCode::RetiredEntryReturned,
    ]
}

/// Every warning code.
pub fn every_warning_code() -> Vec<PackWarningCode> {
    vec![
        PackWarningCode::EmptyDescription,
        PackWarningCode::UnresolvedCapabilityIri,
        PackWarningCode::DuplicateShapeIri,
    ]
}

/// Every disposition an imported entry can be given.
pub fn every_disposition() -> Vec<PackDisposition> {
    vec![
        PackDisposition::Published,
        PackDisposition::Revised,
        PackDisposition::Unchanged,
        PackDisposition::Withdrawn,
        PackDisposition::Republished,
    ]
}

/// The digest shape nested inside the framed pack digests.
pub fn sample_digest() -> Digest256 {
    raw_digest(0xc8)
}
