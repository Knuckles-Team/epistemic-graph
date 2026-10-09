//! `core:virtual-graph@1` + `core:virtual-graph-shapes@1` (EG-UNIFIED-DATA-PLANE-R037):
//! the virtual-graph vocabulary and its shapes are an EG core source, so every graph's
//! composed schema carries them. The SHACL fixtures: a well-formed connection, contract
//! and approved mapping conforms under the COMPOSED shapes (the shapes-omitted
//! `ShaclValidate` path) and each planted defect -- a credential-bearing endpoint
//! reference, an unknown source kind, an undiscovered mapped entity, an undiscovered
//! key field and an undiscovered predicate-field-pair field -- is flagged.

use super::test_support::conforms;
use crate::graph::GraphSchemaSources;

const VG_SOURCE: &str = "core:virtual-graph@1";
const VG_SHAPES: &str = "core:virtual-graph-shapes@1";

/// A connection, its discovered contract and one approved mapping. `entity`,
/// `key_field` and `pair_field` are the values a planted defect replaces with an
/// undiscovered name; `kind` and `endpoint` let a planted defect name an unknown
/// source kind or add a credential to the endpoint reference.
fn fixture(
    kind: &str,
    endpoint_extra: &str,
    entity: &str,
    key_field: &str,
    pair_field: &str,
) -> String {
    format!(
        "@prefix vg: <http://knuckles.team/kg/virtual-graph#> .\n\
         <urn:conn> a vg:SourceConnection ; vg:sourceKind \"{kind}\" ; vg:endpointRef <urn:ep> .\n\
         <urn:ep> vg:url \"postgres://example-db.internal/app\"{endpoint_extra} .\n\
         <urn:contract> a vg:MetadataContract ; vg:contractSource <urn:conn> ;\n\
           vg:schemaVersion \"1\" ; vg:contentDigest \"sha256:{:0>64}\" ;\n\
           vg:discoveredEntity \"Patient\" ; vg:discoveredField \"Patient.id\", \"Patient.name\" .\n\
         <urn:mapping> a vg:VirtualMapping ; vg:mapsContract <urn:contract> ;\n\
           vg:mapsOntologyClass <urn:core/Patient> ; vg:mapsEntity \"{entity}\" ;\n\
           vg:keyField \"{key_field}\" ; vg:approved true ;\n\
           vg:hasPredicateFieldPair <urn:pair> .\n\
         <urn:pair> a vg:PredicateFieldPair ; vg:predicate <urn:core/name> ; vg:field \"{pair_field}\" .\n",
        "0"
    )
}

const SOURCE_KIND: &str = "sql";
const NO_EXTRA: &str = "";
const ENTITY: &str = "Patient";
const KEY_FIELD: &str = "Patient.id";
const PAIR_FIELD: &str = "Patient.name";

fn well_formed() -> String {
    fixture(SOURCE_KIND, NO_EXTRA, ENTITY, KEY_FIELD, PAIR_FIELD)
}

#[test]
fn the_virtual_graph_vocabulary_and_shapes_are_core_sources() {
    let sources = GraphSchemaSources::default();
    let vocabulary = sources
        .core
        .get(VG_SOURCE)
        .expect("the virtual-graph vocabulary");
    assert!(vocabulary
        .ontology_ttl
        .as_deref()
        .is_some_and(|d| d.contains("vg:VirtualMapping")));
    let shapes = sources
        .core
        .get(VG_SHAPES)
        .expect("the virtual-graph shapes");
    assert!(shapes
        .shapes_ttl
        .as_deref()
        .is_some_and(|d| d.contains("VirtualMappingShape")));
}

#[test]
fn a_well_formed_connection_contract_and_approved_mapping_conforms() {
    assert!(conforms(&well_formed()));
}

#[test]
fn each_planted_defect_is_flagged() {
    let plants = [
        (
            "unknown source kind",
            fixture("ftp", NO_EXTRA, ENTITY, KEY_FIELD, PAIR_FIELD),
        ),
        (
            "credential-bearing endpoint reference",
            fixture(
                SOURCE_KIND,
                " ; vg:credential \"s3cret\"",
                ENTITY,
                KEY_FIELD,
                PAIR_FIELD,
            ),
        ),
        (
            "undiscovered mapped entity",
            fixture(SOURCE_KIND, NO_EXTRA, "Appointment", KEY_FIELD, PAIR_FIELD),
        ),
        (
            "undiscovered key field",
            fixture(SOURCE_KIND, NO_EXTRA, ENTITY, "Patient.ssn", PAIR_FIELD),
        ),
        (
            "undiscovered predicate-field-pair field",
            fixture(SOURCE_KIND, NO_EXTRA, ENTITY, KEY_FIELD, "Patient.dob"),
        ),
    ];
    for (name, data) in plants {
        assert!(!conforms(&data), "{name} must not conform");
    }
}
