use super::*;
use eg_types::graph_schema::GraphSchemaOp;

fn pack_document(uri: &str, body: Arc<str>) -> PackSchemaDocument {
    PackSchemaDocument {
        uri: uri.to_string(),
        body,
    }
}

#[test]
fn pack_projection_graph_identity_is_framed_opaque_and_tenant_safe() {
    let graph_os = eg_types::contract::ResourceId::new("graph-os").unwrap();
    let utilities = eg_types::contract::ResourceId::new("agent-utilities").unwrap();
    let golden = pack_projection_graph_name("tenant-a", &graph_os).unwrap();
    assert_eq!(
        golden,
        "pack__710a40e26f5757a7d0ea74593af842cd8444bd62bc8f31e76d2d37f5d5611947"
    );
    assert_eq!(golden.len(), "pack__".len() + 64);
    assert!(!golden.contains("tenant-a") && !golden.contains("graph-os"));
    assert_ne!(
        golden,
        pack_projection_graph_name("tenant-b", &graph_os).unwrap()
    );
    assert_ne!(
        golden,
        pack_projection_graph_name("tenant-a", &utilities).unwrap()
    );
    assert!(pack_projection_graph_name("tenant-a ", &graph_os).is_err());
}

fn attach(source_id: &str, ontology_ttl: &str) -> GraphSchemaOp {
    GraphSchemaOp::Attach {
        source_id: source_id.to_string(),
        shapes_ttl: None,
        ontology_ttl: Some(ontology_ttl.to_string()),
        if_composed_digest: None,
    }
}

#[test]
fn attach_list_and_detach_share_one_authoritative_source_set() {
    let core = GraphCore::new();
    let initial = list("g", &core).unwrap();
    let core_ids: std::collections::BTreeSet<_> = initial
        .core_sources
        .iter()
        .filter_map(|source| match &source.origin {
            eg_types::graph_schema::SchemaSourceOriginView::Core { .. } => {
                Some(source.source_id.as_str())
            }
            _ => None,
        })
        .collect();
    // The listing reports exactly the immutable core catalog: all 41 core
    // artifacts (35 ontologies, the governance, world-model, swarm-topology,
    // finance and virtual-graph shapes).
    let catalog = core.schema_sources();
    let catalog_ids: std::collections::BTreeSet<_> =
        catalog.core.keys().map(String::as_str).collect();
    assert_eq!(core_ids, catalog_ids);
    assert_eq!(core_ids.len(), 41);
    for anchor in [
        "core:catalog@1",
        "core:foundation@1",
        "core:governance-shapes@1",
        "core:software@1",
    ] {
        assert!(core_ids.contains(anchor), "missing {anchor}");
    }
    assert!(initial
        .core_sources
        .iter()
        .all(|source| { source.shapes_sha256.is_some() ^ source.ontology_sha256.is_some() }));
    let ontology = "@prefix owl: <http://www.w3.org/2002/07/owl#> . @prefix ex: <http://example/> . ex:Local a owl:Class .";
    apply(&core, "g", &attach("admin:local", ontology)).unwrap();
    let listed = list("g", &core).unwrap();
    assert!(listed
        .dynamic_sources
        .iter()
        .any(|source| source.source_id == "admin:local"));
    assert!(listed
        .core_sources
        .iter()
        .any(|source| source.source_id == "core:foundation@1"));

    apply(
        &core,
        "g",
        &GraphSchemaOp::Detach {
            source_id: "admin:local".to_string(),
            if_composed_digest: None,
        },
    )
    .unwrap();
    assert!(!core.schema_sources().dynamic.contains_key("admin:local"));
    assert!(core.schema_sources().core.contains_key("core:foundation@1"));

    apply(&core, "g", &attach("admin:local", ontology)).unwrap();
    assert!(core.schema_sources().dynamic.contains_key("admin:local"));
}

/// EH-355 ruling (c): the full-ABox tableau runs where schema ENTERS. An attached
/// individual in two disjoint classes passes the terminology-scoped composition
/// that restore uses, and is refused at attach with nothing published.
#[test]
fn attach_refuses_an_abox_contradiction_the_terminology_scope_admits() {
    let core = GraphCore::new();
    let before = core.schema_sources();
    let ontology = "@prefix owl: <http://www.w3.org/2002/07/owl#> . \
        @prefix ex: <http://example/> . \
        ex:A a owl:Class ; owl:disjointWith ex:B . ex:B a owl:Class . \
        ex:x a ex:A, ex:B .";
    let mut candidate = (*before).clone();
    candidate
        .attach_dynamic(
            "admin:contradiction".to_string(),
            crate::graph::GraphSchemaSource::new(
                crate::graph::SchemaSourceOrigin::Admin {
                    name: "contradiction".to_string(),
                },
                None,
                Some(Arc::from(ontology)),
                0,
            )
            .unwrap(),
        )
        .unwrap();
    compose::validate_and_compose(&candidate).unwrap();

    let error = apply(&core, "g", &attach("admin:contradiction", ontology)).unwrap_err();
    assert!(error.starts_with("ONTOLOGY_INCONSISTENT"), "{error}");
    assert_eq!(core.schema_sources(), before);
}

/// Attaching `turtle` as source `admin:<name>` is refused as inconsistent
/// and leaves the schema catalog exactly as it was.
fn assert_attach_is_inconsistent(name: &str, turtle: &str) {
    let core = GraphCore::new();
    let before = core.schema_sources();
    let source = format!("admin:{name}");
    let error = apply(&core, "g", &attach(&source, turtle)).unwrap_err();
    assert!(
        error.starts_with("ONTOLOGY_INCONSISTENT"),
        "{name}: {error}"
    );
    assert_eq!(core.schema_sources(), before);
}

/// EH-363: the entry check sees the core's n-ary disjointness (`AllDisjointClasses`)
/// and BFO's `Continuant ⊥ Occurrent`. Each individual below sits in two disjoint
/// core classes: GDC and IC (a taxon typed as an organism), Person and Event, and a
/// specifically dependent continuant that is also a temporal region.
#[test]
fn attach_refuses_an_individual_in_two_disjoint_core_classes() {
    let bfo = "@prefix bfo: <http://purl.obolibrary.org/obo/BFO_> . \
        @prefix kg: <http://knuckles.team/kg#> . @prefix ex: <http://example/> . ";
    for (name, individual) in [
        ("taxon", "ex:vulpes a bfo:0000031, bfo:0000004 ."),
        ("agent", "ex:ada a kg:Person, kg:Event ."),
        ("bfo", "ex:moment a bfo:0000020, bfo:0000008 ."),
    ] {
        assert_attach_is_inconsistent(name, &format!("{bfo}{individual}"));
    }
}

/// EH-363: the entry check honours `rdfs:domain`/`rdfs:range`. `kg:memberOf` has
/// domain and range BFO:IndependentContinuant, and `kg:createdBy` range IC; a
/// process at either end contradicts Continuant ⊥ Occurrent (the shape of "a
/// process PARTICIPATED_IN an event" once participation is continuant-only).
#[test]
fn attach_refuses_a_process_at_a_continuant_only_end_of_an_edge() {
    let prefixes = "@prefix bfo: <http://purl.obolibrary.org/obo/BFO_> . \
        @prefix kg: <http://knuckles.team/kg#> . @prefix ex: <http://example/> . ";
    for (name, data) in [
        (
            "domain",
            "ex:run a bfo:0000015 . ex:run kg:memberOf ex:team .",
        ),
        (
            "range",
            "ex:doc kg:createdBy ex:job . ex:job a bfo:0000015 .",
        ),
    ] {
        assert_attach_is_inconsistent(name, &format!("{prefixes}{data}"));
    }
    let core = GraphCore::new();
    let fine = format!("{prefixes}ex:ada a kg:Person . ex:ada kg:memberOf ex:team .");
    apply(&core, "g", &attach("admin:fine", &fine)).unwrap();
}

#[test]
fn invalid_attach_and_stale_cas_publish_nothing() {
    let core = GraphCore::new();
    let before = core.schema_sources();
    assert!(apply(&core, "g", &attach("admin:broken", "not turtle")).is_err());
    assert_eq!(core.schema_sources(), before);

    let fenced = GraphSchemaOp::Attach {
        source_id: "admin:fenced".to_string(),
        shapes_ttl: None,
        ontology_ttl: Some("@prefix owl: <http://www.w3.org/2002/07/owl#> .".to_string()),
        if_composed_digest: Some("0".repeat(64)),
    };
    assert!(apply(&core, "g", &fenced)
        .unwrap_err()
        .contains("COMPOSED_DIGEST_MISMATCH"));
    assert_eq!(core.schema_sources(), before);
}

#[test]
fn generic_graph_schema_cannot_detach_or_replace_core() {
    let core = GraphCore::new();
    let before = core.schema_sources();
    for op in [
        attach(
            "core:foundation@1",
            "@prefix owl: <http://www.w3.org/2002/07/owl#> .",
        ),
        GraphSchemaOp::Detach {
            source_id: "core:foundation@1".to_string(),
            if_composed_digest: None,
        },
    ] {
        assert!(apply(&core, "g", &op)
            .unwrap_err()
            .contains("SCHEMA_SOURCE_RESERVED"));
    }
    assert_eq!(core.schema_sources(), before);
}

#[test]
fn internal_pack_projection_owns_identity_and_rejects_regression() {
    let core = GraphCore::new();
    let connector = eg_types::contract::ResourceId::new("graph-os").unwrap();
    let ontology_a = Arc::<str>::from(
        "@prefix owl: <http://www.w3.org/2002/07/owl#> . @prefix ex: <http://example/> . ex:A a owl:Class .",
    );
    let ontology_b = Arc::<str>::from(
        "@prefix owl: <http://www.w3.org/2002/07/owl#> . @prefix ex: <http://example/> . ex:B a owl:Class .",
    );

    let first = apply_pack_source(
        &core,
        "pack__graph-os",
        &connector,
        "pack:graph-os:1:aaaa",
        Vec::new(),
        vec![pack_document(
            "ontology://graph-os/a.ttl",
            Arc::clone(&ontology_a),
        )],
        None,
    )
    .unwrap();
    assert!(first.changed);
    let installed = core
        .schema_sources()
        .dynamic
        .get("pack:graph-os")
        .cloned()
        .unwrap();
    assert_eq!(
        installed.origin,
        crate::graph::SchemaSourceOrigin::Pack {
            connector: "graph-os".to_string(),
            record_id: "pack:graph-os:1:aaaa".to_string(),
        }
    );

    let identical = apply_pack_source(
        &core,
        "pack__graph-os",
        &connector,
        "pack:graph-os:1:aaaa",
        Vec::new(),
        vec![pack_document(
            "ontology://graph-os/a.ttl",
            Arc::clone(&ontology_a),
        )],
        Some(&first.composed_digest),
    )
    .unwrap();
    assert!(!identical.changed);
    assert_eq!(identical.composed_digest, first.composed_digest);

    let before = core.schema_sources();
    let error = apply_pack_source(
        &core,
        "pack__graph-os",
        &connector,
        "pack:graph-os:0:bbbb",
        Vec::new(),
        vec![pack_document("ontology://graph-os/b.ttl", ontology_b)],
        None,
    )
    .unwrap_err();
    assert!(error.contains("SCHEMA_SOURCE_REGRESSION"));
    assert_eq!(core.schema_sources(), before);
}

#[test]
fn internal_pack_projection_withdrawal_retains_record_high_water() {
    let core = GraphCore::new();
    let connector = eg_types::contract::ResourceId::new("agent-utilities").unwrap();
    let ontology = Arc::<str>::from(
        "@prefix owl: <http://www.w3.org/2002/07/owl#> . @prefix ex: <http://example/> . ex:A a owl:Class .",
    );
    apply_pack_source(
        &core,
        "pack__test",
        &connector,
        "pack:agent-utilities:1:aaaa",
        Vec::new(),
        vec![pack_document(
            "ontology://agent-utilities/a.ttl",
            Arc::clone(&ontology),
        )],
        None,
    )
    .unwrap();
    let withdrawal = apply_pack_source(
        &core,
        "pack__test",
        &connector,
        "pack:agent-utilities:2:bbbb",
        Vec::new(),
        Vec::new(),
        None,
    )
    .unwrap();
    assert!(withdrawal.changed);
    let tombstone = core
        .schema_sources()
        .dynamic
        .get("pack:agent-utilities")
        .cloned()
        .unwrap();
    assert!(tombstone.shapes_ttl.is_none() && tombstone.ontology_ttl.is_none());
    let persisted = core.to_msgpack().unwrap();
    let restarted = GraphCore::new();
    restarted.from_msgpack(&persisted).unwrap();
    assert_eq!(restarted.schema_sources(), core.schema_sources());
    assert!(apply_pack_source(
        &restarted,
        "pack__test",
        &connector,
        "pack:agent-utilities:1:cccc",
        Vec::new(),
        vec![pack_document("ontology://agent-utilities/a.ttl", ontology)],
        None,
    )
    .unwrap_err()
    .contains("SCHEMA_SOURCE_REGRESSION"));
}

#[test]
fn pack_members_keep_file_local_blank_nodes_and_uri_order_is_stable() {
    let first = PackSchemaDocument {
        uri: "shapes://test/a.ttl".to_string(),
        body: Arc::from(
            "@prefix sh: <http://www.w3.org/ns/shacl#> .\n\
             @prefix ex: <http://example/> .\n\
             ex:A a sh:NodeShape ; sh:property _:b0 .\n\
             _:b0 sh:path ex:a .\n",
        ),
    };
    let second = PackSchemaDocument {
        uri: "shapes://test/b.ttl".to_string(),
        body: Arc::from(
            "@prefix sh: <http://www.w3.org/ns/shacl#> .\n\
             @prefix ex: <http://example/> .\n\
             ex:B a sh:NodeShape ; sh:property _:b0 .\n\
             _:b0 sh:path ex:b .\n",
        ),
    };
    let forward = compose_pack_documents("SHAPES", vec![first.clone(), second.clone()])
        .unwrap()
        .unwrap();
    let reverse = compose_pack_documents("SHAPES", vec![second, first])
        .unwrap()
        .unwrap();
    assert_eq!(
        forward, reverse,
        "archive member order cannot change identity"
    );
    assert_eq!(
        eg_types::contract::Digest256::sha256(forward.as_bytes()),
        eg_types::contract::Digest256::sha256(reverse.as_bytes())
    );

    let triples = eg_rdf::mapping::parse_turtle(&forward).unwrap();
    let blank_subjects: std::collections::BTreeSet<_> = triples
        .iter()
        .filter_map(|triple| match &triple.subject {
            eg_rdf::oxrdf::NamedOrBlankNode::BlankNode(node) => Some(node.as_str().to_string()),
            _ => None,
        })
        .collect();
    assert_eq!(
        blank_subjects.len(),
        2,
        "two local _:b0 labels must not merge"
    );
}

#[test]
fn pack_member_failures_name_the_exact_uri_and_enforce_each_bound() {
    let error = compose_pack_documents(
        "ONTOLOGY",
        vec![PackSchemaDocument {
            uri: "ontology://test/broken.ttl".to_string(),
            body: Arc::from("not turtle"),
        }],
    )
    .unwrap_err();
    assert!(error.contains("ontology://test/broken.ttl"));

    let error = compose_pack_documents(
        "SHAPES",
        vec![PackSchemaDocument {
            uri: "shapes://test/oversized.ttl".to_string(),
            body: Arc::from("x".repeat(eg_types::graph_schema::MAX_SCHEMA_DOCUMENT_BYTES + 1)),
        }],
    )
    .unwrap_err();
    assert!(error.contains("shapes://test/oversized.ttl"));
    assert!(error.contains("TOO_LARGE"));
}
