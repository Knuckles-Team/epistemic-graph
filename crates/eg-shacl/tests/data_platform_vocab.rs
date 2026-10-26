//! Native SHACL acceptance for the data-platform source-kind extension.

use eg_shacl::validate_turtle;

const SHAPES: &str = include_str!("../../eg-core/ontology/virtual_graph-v1.shapes.ttl");

fn source_connection(source_kind: &str) -> String {
    format!(
        r#"@prefix vg: <http://knuckles.team/kg/virtual-graph#> .
@prefix ex: <http://example.org/data-platform/> .

ex:connection a vg:SourceConnection ;
    vg:sourceKind "{source_kind}" ;
    vg:endpointRef ex:endpoint .

ex:endpoint vg:url "https://example.invalid/data" .
"#
    )
}

// spec: EG-DATAPLAT-VOCAB-R001.3
#[test]
fn document_source_kind_accepted() {
    let document = validate_turtle(SHAPES, &source_connection("document"))
        .expect("document source connection should parse and validate");
    assert!(document.conforms, "document: {:?}", document.results);

    let ftp = validate_turtle(SHAPES, &source_connection("ftp"))
        .expect("ftp source connection should parse before validation");
    assert!(
        !ftp.conforms,
        "ftp must remain outside the closed vocabulary"
    );
    assert!(ftp.results.iter().any(|result| {
        result.path.as_deref() == Some("<http://knuckles.team/kg/virtual-graph#sourceKind>")
    }));
}
