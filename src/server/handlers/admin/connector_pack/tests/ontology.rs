//! G14 (EH-119, EH-355 ruling (c)): ontology entering a graph through a pack
//! passes the bounded full-ABox check at import. An ABox contradiction -- one
//! individual asserted into two disjoint classes -- is refused by name, which
//! a TBox-only classification could not see.

use eg_types::connector_pack::{
    PackAnnotations, PackEntryKind, PackImportResult, PackViolationCode,
};

use super::{bind, build_pack, import, ok, tool, Content, Served, ADMIN};

const OWL: &str = "http://www.w3.org/2002/07/owl#";

fn ontology(connector: &str, body: String) -> Content {
    Content {
        kind: PackEntryKind::Ontology,
        uri: format!("ontology://{connector}/core.ttl"),
        name: "core.ttl".to_string(),
        media_type: "text/turtle",
        body: body.into_bytes(),
        input_schema: None,
        annotations: PackAnnotations::default(),
        references: Vec::new(),
    }
}

fn disjoint_classes(individual_in_both: bool) -> String {
    let mut body = format!(
        "<https://example.org/A> a <{OWL}Class> .\n\
         <https://example.org/B> a <{OWL}Class> .\n\
         <https://example.org/A> <{OWL}disjointWith> <https://example.org/B> .\n\
         <https://example.org/x> a <https://example.org/A> .\n"
    );
    if individual_in_both {
        body.push_str("<https://example.org/x> a <https://example.org/B> .\n");
    }
    body
}

async fn import_ontology(connector: &str, body: String) -> PackImportResult {
    let served = Served::new();
    bind(&served, connector, ADMIN).await;
    let pack = build_pack(
        connector,
        &[tool(connector, "a", "Tool a."), ontology(connector, body)],
    );
    ok("Import", import(&served, &pack, None).await)
}

#[tokio::test]
async fn an_abox_contradiction_is_refused_as_inconsistent() {
    match import_ontology("g14-inconsistent", disjoint_classes(true)).await {
        PackImportResult::Rejected { violations, .. } => assert_eq!(
            violations.iter().map(|v| v.code).collect::<Vec<_>>(),
            [PackViolationCode::OntologyInconsistent]
        ),
        other => panic!("an inconsistent ABox must be rejected: {other:?}"),
    }
}

#[tokio::test]
async fn a_consistent_ontology_is_imported() {
    match import_ontology("g14-consistent", disjoint_classes(false)).await {
        PackImportResult::Imported { .. } => {}
        other => panic!("a consistent ontology must import: {other:?}"),
    }
}
