//! A pack imported under a tenant maps that tenant's `SourceIngest` records
//! (EG-TYPED-PACKS-R101.9), and a one-tool-per-operation pack of 5000 entries
//! imports and resolves (#656).

use std::time::Instant;

use eg_types::connector_pack::{PackAnnotations, PackEntryKind};
use eg_types::contract::ResourceId;
use eg_types::source_ingestion::SourceIngestionRequest;

use super::{bind, build_pack, imported, tool, Content, Served, ADMIN, TENANT};
use crate::protocol::{GraphType, Method};

const CONNECTOR: &str = "demo-connector";
const GRAPH: &str = "packingest";

fn manifest() -> Content {
    Content {
        kind: PackEntryKind::Manifest,
        uri: format!("manifest://{CONNECTOR}"),
        name: CONNECTOR.to_string(),
        media_type: "application/yaml",
        body: format!(
            "connector: {CONNECTOR}\nschema_mappings:\n  item:\n    ontology_class: Document\n    fields:\n      name: label\n      id: identifier\n"
        )
        .into_bytes(),
        input_schema: None,
        annotations: PackAnnotations::default(),
        references: Vec::new(),
    }
}

/// The shared fixture request, as a first delta with no prior checkpoint.
fn first_ingest() -> SourceIngestionRequest {
    let mut batch = eg_types::test_support::source_ingestion::request()
        .as_batch()
        .clone();
    batch.expected_previous_checkpoint = None;
    SourceIngestionRequest::new(batch).expect("valid source ingestion request")
}

/// Import keys pack heads by the verified tenant; `SourceIngest` must resolve
/// the mapping under that same key, not the route's hashed carrier scope.
// spec: EG-TYPED-PACKS-R101.9
#[tokio::test]
async fn an_imported_pack_maps_source_ingest_records_of_the_same_tenant() {
    let served = Served::new();
    let _guard = served.with_graph_persistence().await;
    bind(&served, CONNECTOR, ADMIN).await;
    imported(&served, &build_pack(CONNECTOR, &[manifest()]), None).await;
    let created = served
        .call_on(
            GRAPH,
            "pack-ingest:create",
            Method::CreateGraph {
                graph_name: GRAPH.to_string(),
                graph_type: GraphType::Global,
            },
        )
        .await;
    assert!(created.error.is_none(), "{created:?}");
    let ingested = served
        .call_on(
            GRAPH,
            "pack-ingest:ingest",
            Method::SourceIngest {
                request: Box::new(first_ingest()),
            },
        )
        .await;
    assert!(
        ingested.error.is_none(),
        "SourceIngest under the import tenant {TENANT} must be accepted: {ingested:?}"
    );
}

/// #656: one tool per API operation puts packs past 1.6k entries; 5000 must
/// import and resolve. Prints the measured import and lookup times.
#[tokio::test]
async fn a_5000_entry_pack_imports_and_resolves() {
    const ENTRIES: usize = 5000;
    let connector = "pack-scale";
    let served = Served::new();
    bind(&served, connector, ADMIN).await;
    let tools: Vec<Content> = (0..ENTRIES)
        .map(|i| tool(connector, &format!("op_{i:05}"), "One API operation."))
        .collect();
    let pack = build_pack(connector, &tools);
    let started = Instant::now();
    let receipt = imported(&served, &pack, None).await;
    let import_ms = started.elapsed().as_millis();
    let started = Instant::now();
    let status = served
        .state
        .write()
        .await
        .ensure_agent_library()
        .unwrap()
        .connector_pack_status(TENANT, &ResourceId::new(connector).unwrap())
        .unwrap();
    let lookup_us = started.elapsed().as_micros();
    println!(
        "{{\"bench\":\"connector_pack_5000\",\"entries\":{ENTRIES},\"import_ms\":{import_ms},\"status_lookup_us\":{lookup_us}}}"
    );
    assert_eq!(receipt.pack_digest, pack.0.pack_digest);
    assert!(
        status.head.is_some(),
        "the imported head resolves: {status:?}"
    );
}
