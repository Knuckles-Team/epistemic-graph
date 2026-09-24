//! PB1 + X10: the projection worker is the first Agent Library outbox
//! consumer, a pack is dark until its projection applies, and the operator
//! surface reads and rewinds the worker's stream.

use std::sync::atomic::AtomicBool;

use eg_types::agent_component::{
    AgentComponentKind, AgentComponentOp, AgentComponentSearchPage, AgentComponentSearchRequest,
};
use eg_types::connector_pack::{PackAnnotations, PackEntryKind};
use eg_types::contract::ResourceId;
use eg_types::mutation_outbox::{
    MutationOutboxOp, MutationOutboxStatusView, NativeOutboxScope, NativeOutboxStore,
    OutboxRewindReceipt, OutboxTarget, RewindTarget,
};

use super::{bind, build_pack, imported, ok, tool, Content, Served, ADMIN, TENANT};
use crate::protocol::Method;
use crate::server::connector_pack_projection::worker::{sweep, CONSUMER};

const CONNECTOR: &str = "pack-outbox";

async fn outbox(served: &Served, key: &str, op: MutationOutboxOp) -> crate::protocol::Response {
    served
        .call(key, Method::MutationOutbox { op: Box::new(op) })
        .await
}

fn target() -> OutboxTarget {
    OutboxTarget::NativeStore {
        store: NativeOutboxStore::AgentLibrary,
        tenant_id: TENANT.to_string(),
        scope: NativeOutboxScope::Tenant,
    }
}

async fn searchable_ids(served: &Served) -> Vec<String> {
    let page: AgentComponentSearchPage = ok(
        "Search",
        served
            .call(
                &format!(
                    "search:{}",
                    served.state.read().await.persist_dir.clone().unwrap()
                ),
                Method::AgentComponent {
                    op: AgentComponentOp::Search {
                        request: AgentComponentSearchRequest {
                            tenant_id: TENANT.to_string(),
                            task: None,
                            capabilities: Vec::new(),
                            kinds: vec![AgentComponentKind::Tool],
                            read_only: false,
                            limit: None,
                            cursor: None,
                        },
                    },
                },
            )
            .await,
    );
    page.entries
        .into_iter()
        .map(|entry| entry.component_id)
        .collect()
}

#[tokio::test]
async fn the_worker_consumes_an_import_and_the_operator_can_rewind_it() {
    let served = Served::new();
    bind(&served, CONNECTOR, ADMIN).await;
    imported(
        &served,
        &build_pack(CONNECTOR, &[tool(CONNECTOR, "a", "Tool a.")]),
        None,
    )
    .await;
    let subscribed = AtomicBool::new(false);
    assert!(
        sweep(&served.state, &subscribed).await.unwrap(),
        "the import row is consumed"
    );
    assert!(
        !sweep(&served.state, &subscribed).await.unwrap(),
        "and only once"
    );
    let status: MutationOutboxStatusView = ok(
        "Status",
        outbox(
            &served,
            "outbox:status",
            MutationOutboxOp::Status {
                target: target(),
                consumer: CONSUMER.to_string(),
            },
        )
        .await,
    );
    assert!(status.status.live);
    assert_eq!((status.status.delivered, status.status.pending), (1, 0));
    let rewound: OutboxRewindReceipt = ok(
        "Rewind",
        outbox(
            &served,
            "outbox:rewind",
            MutationOutboxOp::Rewind {
                target: target(),
                consumer: CONSUMER.to_string(),
                to: RewindTarget::Start,
            },
        )
        .await,
    );
    assert!(rewound.completed);
    assert_eq!(rewound.deleted_deliveries, 1);
    assert!(
        sweep(&served.state, &subscribed).await.unwrap(),
        "a rewound row is delivered again"
    );
}

#[tokio::test]
async fn a_pack_without_schema_is_visible_at_once_and_one_with_schema_after_projection() {
    let served = Served::new();
    let _env = served.with_graph_persistence().await;
    bind(&served, CONNECTOR, ADMIN).await;
    imported(
        &served,
        &build_pack(CONNECTOR, &[tool(CONNECTOR, "a", "Tool a.")]),
        None,
    )
    .await;
    assert!(searchable_ids(&served)
        .await
        .contains(&"mcp:pack-outbox/tool/a".to_string()));

    let schema = "schema-pack";
    bind(&served, schema, ADMIN).await;
    let ontology = Content {
        kind: PackEntryKind::Ontology,
        uri: format!("ontology://{schema}/core.ttl"),
        name: "core.ttl".to_string(),
        media_type: "text/turtle",
        body: b"<https://example.org/Thing> a <http://www.w3.org/2002/07/owl#Class> .\n".to_vec(),
        input_schema: None,
        annotations: PackAnnotations::default(),
        references: Vec::new(),
    };
    imported(
        &served,
        &build_pack(schema, &[tool(schema, "b", "Tool b."), ontology]),
        None,
    )
    .await;
    let tool_b = "mcp:schema-pack/tool/b".to_string();
    assert!(
        !searchable_ids(&served).await.contains(&tool_b),
        "a pack with schema is dark until its projection applies"
    );
    let subscribed = AtomicBool::new(false);
    while sweep(&served.state, &subscribed).await.unwrap() {}
    if !searchable_ids(&served).await.contains(&tool_b) {
        // A failed projection is recorded on the head with its cause.
        let store = served.state.write().await.ensure_agent_library().unwrap();
        let status = store
            .connector_pack_status(TENANT, &ResourceId::new(schema).unwrap())
            .unwrap();
        panic!(
            "the projection did not flip the head visible: {:?}",
            status.projection
        );
    }
}

#[tokio::test]
async fn every_native_store_is_reachable_through_its_scope_selector() {
    let served = Served::new();
    let status = |store, scope| MutationOutboxOp::Status {
        target: OutboxTarget::NativeStore {
            store,
            tenant_id: TENANT.to_string(),
            scope,
        },
        consumer: "operator-probe".to_string(),
    };
    let sql = super::refused(
        "unknown SQL resource",
        outbox(
            &served,
            "outbox:sql",
            status(
                NativeOutboxStore::SqlCatalog,
                NativeOutboxScope::SqlResource {
                    resource: "never-written".to_string(),
                },
            ),
        )
        .await,
    );
    assert!(sql.starts_with("OUTBOX_OWNER_UNKNOWN:"), "{sql}");
    let unknown = super::refused(
        "unknown semantic binding",
        outbox(
            &served,
            "outbox:semantic",
            status(
                NativeOutboxStore::SemanticIndex,
                NativeOutboxScope::SemanticBinding {
                    binding_id: "never-admitted".to_string(),
                },
            ),
        )
        .await,
    );
    assert!(unknown.starts_with("OUTBOX_OWNER_UNKNOWN:"), "{unknown}");
    let mismatched = super::refused(
        "tenant scope on a semantic index",
        outbox(
            &served,
            "outbox:mismatch",
            status(NativeOutboxStore::SemanticIndex, NativeOutboxScope::Tenant),
        )
        .await,
    );
    assert!(mismatched.contains("does not address"), "{mismatched}");
}
