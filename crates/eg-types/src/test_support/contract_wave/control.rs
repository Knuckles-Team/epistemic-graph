//! Valid sample values for the schema-source, outbox and component-content
//! surfaces.

use crate::agent_component::AgentComponentContentRequest;
use crate::graph_schema::GraphSchemaOp;
use crate::mutation_outbox::{
    MutationOutboxOp, NativeOutboxStore, OutboxPositionView, OutboxTarget, RewindTarget,
};

use super::pack::connector;

/// Every schema-source operation, labelled by its wire op tag.
pub fn graph_schema_ops() -> Vec<(&'static str, GraphSchemaOp)> {
    vec![
        (
            "GraphSchema.attach",
            GraphSchemaOp::Attach {
                source_id: "operator".to_string(),
                shapes_ttl: Some("@prefix sh: <http://www.w3.org/ns/shacl#> .".to_string()),
                ontology_ttl: Some("@prefix owl: <http://www.w3.org/2002/07/owl#> .".to_string()),
                if_composed_digest: Some(super::digest_text(0xd1)),
            },
        ),
        (
            "GraphSchema.attach_pack",
            GraphSchemaOp::AttachPack {
                connector: connector(),
                if_composed_digest: None,
            },
        ),
        (
            "GraphSchema.detach",
            GraphSchemaOp::Detach {
                source_id: "operator".to_string(),
                if_composed_digest: Some(super::digest_text(0xd2)),
            },
        ),
    ]
}

fn position() -> OutboxPositionView {
    OutboxPositionView {
        sequence: 91,
        created_at_ms: 1_700_000_000_000,
        batch_id: "batch-1".to_string(),
        ordinal: 2,
    }
}

/// Both outbox targets.
pub fn every_outbox_target() -> Vec<OutboxTarget> {
    let mut targets = vec![OutboxTarget::Graph {
        graph: "__commons__".to_string(),
    }];
    for store in [
        NativeOutboxStore::AgentLibrary,
        NativeOutboxStore::SemanticIndex,
        NativeOutboxStore::Jobs,
        NativeOutboxStore::SqlCatalog,
    ] {
        targets.push(OutboxTarget::NativeStore {
            store,
            tenant_id: "tenant-a".to_string(),
        });
    }
    targets
}

/// Every mutation-outbox operation, labelled by its wire op tag.
pub fn outbox_ops() -> Vec<(&'static str, MutationOutboxOp)> {
    vec![
        (
            "MutationOutbox.status",
            MutationOutboxOp::Status {
                target: OutboxTarget::Graph {
                    graph: "__commons__".to_string(),
                },
                consumer: "consumer-a".to_string(),
            },
        ),
        (
            "MutationOutbox.dead_letters",
            MutationOutboxOp::DeadLetters {
                target: OutboxTarget::NativeStore {
                    store: NativeOutboxStore::AgentLibrary,
                    tenant_id: "tenant-a".to_string(),
                },
                consumer: "consumer-a".to_string(),
                after: Some(position()),
                limit: 64,
            },
        ),
        (
            "MutationOutbox.rewind",
            MutationOutboxOp::Rewind {
                target: OutboxTarget::NativeStore {
                    store: NativeOutboxStore::Jobs,
                    tenant_id: "tenant-a".to_string(),
                },
                consumer: "consumer-a".to_string(),
                to: RewindTarget::At {
                    position: position(),
                },
            },
        ),
    ]
}

/// The other rewind target, so both arms are exercised.
pub fn rewind_to_start() -> RewindTarget {
    RewindTarget::Start
}

/// One component-content read.
pub fn component_content_request() -> AgentComponentContentRequest {
    AgentComponentContentRequest {
        tenant_id: "tenant-a".to_string(),
        component_id: "mcp:connector-a/tool/search".to_string(),
        entry_revision: Some(4),
    }
}
