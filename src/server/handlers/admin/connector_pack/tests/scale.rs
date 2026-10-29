//! A pack's `connector://resources` declaration, imported under a tenant, maps that tenant's `SourceIngest` records
//! (MEDULLA-TYPED-PACKS-R101.9), and a one-tool-per-operation pack of 5000 entries
//! imports and resolves (#656).

use std::time::Instant;

use eg_types::connector_pack::{PackAnnotations, PackEntryKind};
use eg_types::contract::{BoundedVec, ResourceId};
use eg_types::source_ingestion::{SourceIngestionRequest, SourceJson};

use super::{bind, build_pack, imported, tool, Content, Served, ADMIN, TENANT};
use crate::protocol::{GraphType, Method};

const CONNECTOR: &str = "demo-connector";
const GRAPH: &str = "packingest";

/// The `connector://resources` JSON a 0.4.0 connector serves, as the pack holds it.
fn declaration() -> Content {
    Content {
        kind: PackEntryKind::Resource,
        uri: "connector://resources".to_string(),
        name: "connector-resources".to_string(),
        media_type: "application/json",
        body: format!(
            r#"{{"connector":"{CONNECTOR}","schema_mappings":{{"item":{{"fields":{{"id":"identifier","name":"label"}},"ontology_class":"Document"}}}},"sources":[]}}"#
        )
        .into_bytes(),
        input_schema: None,
        annotations: PackAnnotations::default(),
        references: Vec::new(),
    }
}

/// Print the engine's debug log, which names a refusal `Response::err`
/// reports as INTERNAL, so a failure here says which rule refused.
fn show_refusals() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_test_writer()
        .try_init();
}

/// The shared fixture request, as a first delta with no prior checkpoint.
fn first_ingest() -> SourceIngestionRequest {
    let mut batch = eg_types::test_support::source_ingestion::request()
        .as_batch()
        .clone();
    batch.expected_previous_checkpoint = None;
    SourceIngestionRequest::new(batch).expect("valid source ingestion request")
}

/// A served engine with the demo pack imported under the test tenant and the
/// ingest graph created. The guard keeps the graph persistence environment.
async fn pack_graph(label: &str) -> (Served, crate::crypto::TestEnvReadGuard) {
    show_refusals();
    let served = Served::new();
    let guard = served.with_graph_persistence().await;
    bind(&served, CONNECTOR, ADMIN).await;
    imported(&served, &build_pack(CONNECTOR, &[declaration()]), None).await;
    let created = served
        .call_on(
            GRAPH,
            &format!("{label}:create"),
            Method::CreateGraph {
                graph_name: GRAPH.to_string(),
                graph_type: GraphType::Global,
            },
        )
        .await;
    assert!(created.error.is_none(), "{created:?}");
    (served, guard)
}

/// One served `SourceIngest` call on the ingest graph.
async fn ingest(
    served: &Served,
    request_id: &str,
    request: &SourceIngestionRequest,
) -> crate::protocol::Response {
    served
        .call_on(
            GRAPH,
            request_id,
            Method::SourceIngest {
                request: Box::new(request.clone()),
            },
        )
        .await
}

/// Import keys pack heads by the verified tenant; `SourceIngest` must resolve
/// the mapping under that same key, not the route's hashed carrier scope.
// spec: MEDULLA-TYPED-PACKS-R101.9
#[tokio::test]
async fn an_imported_pack_maps_source_ingest_records_of_the_same_tenant() {
    let (served, _guard) = pack_graph("pack-ingest").await;
    let ingested = ingest(&served, "pack-ingest:ingest", &first_ingest()).await;
    assert!(
        ingested.error.is_none(),
        "SourceIngest under the import tenant {TENANT} must be accepted: {ingested:?}"
    );
}

/// The ontology a connector ships beside its declaration: relations are read
/// from its object properties, not from the declaration.
fn relations_ontology() -> Content {
    Content {
        kind: PackEntryKind::Ontology,
        uri: format!("ontology://{CONNECTOR}/core.ttl"),
        name: "core.ttl".to_string(),
        media_type: "text/turtle",
        body: b"@prefix owl: <http://www.w3.org/2002/07/owl#> .\n\
            @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n\
            @prefix ex: <https://example.org/demo#> .\n\
            ex:Document a owl:Class .\n\
            ex:Section a owl:Class .\n\
            ex:contains a owl:ObjectProperty ; rdfs:domain ex:Document ; rdfs:range ex:Section .\n"
            .to_vec(),
        input_schema: None,
        annotations: PackAnnotations::default(),
        references: Vec::new(),
    }
}

/// The resolver's verdict for one mapping reference of the imported demo pack.
async fn resolve(served: &Served, reference: &str) -> Result<String, String> {
    let mut state = served.state.write().await;
    let store = state.ensure_agent_library().unwrap();
    store
        .resolve_connector_schema_mapping(TENANT, &ResourceId::new(CONNECTOR).unwrap(), reference)
        .map(|resolved| {
            let mut fields: Vec<_> = resolved.schema_mapping.fields.into_iter().collect();
            fields.sort();
            format!("{} {fields:?}", resolved.schema_mapping.ontology_class)
        })
}

/// The selector the SDK emits resolves against the declared mapping: the
/// ontology class and field map come from `connector://resources`.
// spec: MEDULLA-TYPED-PACKS-R101.10
#[tokio::test]
async fn the_selector_resolves_to_the_declared_class_and_field_map() {
    let (served, _guard) = pack_graph("decl-resolve").await;
    let resolved = resolve(&served, "connector:demo-connector#mappings/item").await;
    assert_eq!(
        resolved.as_deref(),
        Ok("Document [(\"id\", \"identifier\"), (\"name\", \"label\")]")
    );
}

/// A record type the declaration does not name is refused by type, and a pack
/// with no declaration is unavailable; neither commits anything.
// spec: MEDULLA-TYPED-PACKS-R101.11
#[tokio::test]
async fn an_undeclared_type_is_refused_with_a_typed_error() {
    let (served, _guard) = pack_graph("decl-undeclared").await;
    let refused = resolve(&served, "connector:demo-connector#mappings/ghost").await;
    assert!(
        refused
            .as_ref()
            .is_err_and(|e| e.starts_with("UNKNOWN_MAPPING_REFERENCE:")),
        "{refused:?}"
    );

    let old = resolve(&served, "manifest:demo-connector#schema_mappings/item").await;
    assert!(
        old.as_ref()
            .is_err_and(|e| e.starts_with("UNKNOWN_MAPPING_REFERENCE:")),
        "the manifest: scheme is not accepted: {old:?}"
    );

    let mut batch = first_ingest().as_batch().clone();
    let mut records = batch.records.as_slice().to_vec();
    records[0].mapping_reference = "connector:demo-connector#mappings/ghost".into();
    batch.records = BoundedVec::new(records).expect("bounded source records");
    let request = SourceIngestionRequest::new(batch).expect("valid source ingestion request");
    let ingested = ingest(&served, "decl-undeclared:ingest", &request).await;
    assert!(
        ingested.error.is_some(),
        "an undeclared type must not ingest"
    );

    let bare = Served::new();
    let _bare_guard = bare.with_graph_persistence().await;
    bind(&bare, CONNECTOR, ADMIN).await;
    imported(
        &bare,
        &build_pack(CONNECTOR, &[tool(CONNECTOR, "list_items", "Lists items.")]),
        None,
    )
    .await;
    let unavailable = resolve(&bare, "connector:demo-connector#mappings/item").await;
    assert!(
        unavailable
            .as_ref()
            .is_err_and(|e| e.starts_with("CONNECTOR_SCHEMA_MAPPING_UNAVAILABLE:")),
        "{unavailable:?}"
    );
}

/// Relations resolve from the ontology's object properties, by the exact
/// reference path, and only when the pack declares them.
// spec: MEDULLA-TYPED-PACKS-R101.10
#[cfg(feature = "rdf")]
#[tokio::test]
async fn relations_resolve_from_the_pack_ontology() {
    show_refusals();
    let served = Served::new();
    let _guard = served.with_graph_persistence().await;
    bind(&served, CONNECTOR, ADMIN).await;
    imported(
        &served,
        &build_pack(CONNECTOR, &[declaration(), relations_ontology()]),
        None,
    )
    .await;
    let mut state = served.state.write().await;
    let store = state.ensure_agent_library().unwrap();
    let connector = ResourceId::new(CONNECTOR).unwrap();
    let found = store
        .resolve_connector_relationship_mapping(
            TENANT,
            &connector,
            "connector:demo-connector#relations/Document/contains",
        )
        .expect("the ontology declares Document contains Section");
    assert_eq!(found.relation.target_resource, "Section");
    let missing = store.resolve_connector_relationship_mapping(
        TENANT,
        &connector,
        "connector:demo-connector#relations/Document/cites",
    );
    assert!(missing.is_err_and(|e| e.starts_with("UNKNOWN_RELATIONSHIP_REFERENCE:")));
}

/// The next delta on the same stream: an advanced checkpoint that chains from
/// the first batch's, carrying a changed record.
fn second_ingest(first: &SourceIngestionRequest) -> SourceIngestionRequest {
    let mut batch = first.as_batch().clone();
    batch.expected_previous_checkpoint = Some(batch.provider_checkpoint.clone());
    batch.provider_checkpoint.position =
        SourceJson::new(serde_json::json!({"page": 3})).expect("bounded cursor JSON");
    let mut records = batch.records.as_slice().to_vec();
    records[0].payload = SourceJson::new(serde_json::json!({"name": "one, revised", "id": 1}))
        .expect("bounded source JSON");
    batch.records = BoundedVec::new(records).expect("bounded source records");
    SourceIngestionRequest::new(batch).expect("valid source ingestion request")
}

/// The committed envelope of one `SourceIngest` request, read back durably.
async fn committed_envelope(
    served: &Served,
    request: &SourceIngestionRequest,
) -> eg_types::change_envelope::ChangeEnvelope {
    let persistence = served
        .state
        .read()
        .await
        .persistence
        .clone()
        .expect("graph persistence is configured");
    let digest = request.batch_digest().expect("batch digest").to_hex();
    persistence
        .read_change_envelope(
            &crate::persist::sanitize(GRAPH),
            &format!("source-ingest:{digest}"),
        )
        .await
        .expect("envelope read")
        .expect("the batch committed an envelope")
        .envelope
}

/// A second batch to the same stream chains from the stream's stored content
/// version. Before the stream keying it named a per-batch object with no prior
/// version while claiming the first batch's digest, and the commit refused it
/// `STALE_CONTENT_VERSION: ... has no prior version`.
// spec: MEDULLA-REPO-INGEST-R002.102
#[tokio::test]
async fn a_second_batch_to_the_same_stream_chains_its_content_version() {
    let (served, _guard) = pack_graph("pack-chain").await;
    let first = first_ingest();
    let second = second_ingest(&first);
    for (label, request) in [("first", &first), ("second", &second)] {
        let ingested = ingest(&served, &format!("pack-chain:{label}"), request).await;
        assert!(
            ingested.error.is_none(),
            "the {label} batch to the stream must commit: {ingested:?}"
        );
    }
    let first_version = committed_envelope(&served, &first).await.content_version;
    let second_version = committed_envelope(&served, &second).await.content_version;
    assert_eq!(
        second_version.object_id, first_version.object_id,
        "both batches advance the one stream-keyed content version"
    );
    assert_eq!(first_version.previous_digest, None);
    assert_eq!(
        second_version.previous_digest.as_deref(),
        Some(first_version.digest.as_str()),
        "the second batch chains from the first batch's stored digest"
    );
}

/// #656: one tool per API operation puts packs past 1.6k entries; 5000 must
/// import and resolve. Prints the measured import and lookup times.
#[tokio::test]
async fn a_5000_entry_pack_imports_and_resolves() {
    const ENTRIES: usize = 5000;
    let connector = "pack-scale";
    show_refusals();
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
