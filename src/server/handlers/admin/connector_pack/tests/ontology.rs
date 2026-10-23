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

/// `length` chained subclass axioms: about `length²/2` derivation steps, far
/// more than a test waits for, well inside the G14 budget.
fn subclass_chain(length: usize) -> String {
    const SUB: &str = "http://www.w3.org/2000/01/rdf-schema#subClassOf";
    (0..length)
        .map(|i| {
            format!(
                "<https://example.org/C{i}> <{SUB}> <https://example.org/C{}> .\n",
                i + 1
            )
        })
        .collect()
}

/// EH-536: an import whose client gave up (its request scope is cancelled) while
/// the G14 reasoning runs stops at the next derivation step and frees the
/// tenant's pack lock -- the next pack write is answered, not queued behind the
/// abandoned import.
// spec: EG-TYPED-PACKS-R084
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cancelled_import_does_not_block_the_next_bind() {
    use crate::server::request_scope::{scope, RequestCancel, CANCELLED};
    use std::time::Duration;

    let connector = "eh536-abandoned";
    let served = Served::new();
    bind(&served, connector, ADMIN).await;
    let pack = build_pack(
        connector,
        &[
            tool(connector, "a", "Tool a."),
            ontology(connector, subclass_chain(3_000)),
        ],
    );
    let cancel = RequestCancel::new();
    let abandon = async {
        tokio::time::sleep(Duration::from_millis(500)).await;
        cancel.cancel();
    };
    let (abandoned, ()) = tokio::time::timeout(Duration::from_secs(60), async {
        tokio::join!(scope(cancel.clone(), import(&served, &pack, None)), abandon)
    })
    .await
    .expect("a cancelled import must stop instead of finishing its reasoning");
    assert_eq!(abandoned.error.as_deref(), Some("CANCELLED"));
    assert_eq!(
        abandoned.error_detail.as_deref(),
        CANCELLED.strip_prefix("CANCELLED: ")
    );
    tokio::time::timeout(Duration::from_secs(20), bind(&served, "eh536-next", ADMIN))
        .await
        .expect("the next bind must not queue behind the abandoned import");
}

/// EG-TYPED-PACKS-R105.1: a connector class that subclasses a parent-catalog
/// class joins the parent graph with no warning; a class with no such path is
/// imported and reported as `ONTOLOGY_ORPHAN`, naming the class.
fn parent_linked(orphan: bool) -> String {
    const SUB: &str = "http://www.w3.org/2000/01/rdf-schema#subClassOf";
    let mut body = format!(
        "<https://example.org/conn#Contract> a <{OWL}Class> ;\n\
         <{SUB}> <http://knuckles.team/kg#Agreement> .\n\
         <https://example.org/conn#Lease> a <{OWL}Class> ;\n\
         <{SUB}> <https://example.org/conn#Contract> .\n"
    );
    if orphan {
        body.push_str(&format!(
            "<https://example.org/conn#Widget> a <{OWL}Class> .\n"
        ));
    }
    body
}

fn orphan_warnings(result: &PackImportResult) -> Vec<String> {
    match result {
        PackImportResult::Imported { receipt } => receipt
            .warnings
            .iter()
            .filter(|w| w.code == eg_types::connector_pack::PackWarningCode::OntologyOrphan)
            .map(|w| w.detail.clone())
            .collect(),
        other => panic!("the ontology must import: {other:?}"),
    }
}

// spec: EG-TYPED-PACKS-R105.1
#[tokio::test]
async fn a_class_linked_to_a_parent_class_imports_without_an_orphan_warning() {
    let result = import_ontology("r105-linked", parent_linked(false)).await;
    assert!(orphan_warnings(&result).is_empty(), "{result:?}");
}

// spec: EG-TYPED-PACKS-R105.1
#[tokio::test]
async fn an_orphan_class_is_imported_and_flagged_by_name() {
    let result = import_ontology("r105-orphan", parent_linked(true)).await;
    let warnings = orphan_warnings(&result);
    assert_eq!(warnings.len(), 1, "{result:?}");
    assert!(
        warnings[0].contains("https://example.org/conn#Widget"),
        "{warnings:?}"
    );
    assert!(!warnings[0].contains("conn#Lease"), "{warnings:?}");
}
