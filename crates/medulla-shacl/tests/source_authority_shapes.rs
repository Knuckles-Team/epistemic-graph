//! Validate `vg:SourceAuthority` rows against the shapes registered by the core
//! virtual-graph schema source (MEDULLA-SRC-AUTH-R002).

use medulla_shacl::{validate_turtle, ValidationReport};

const SHAPES: &str = include_str!("../../medulla-core/ontology/virtual_graph-v1.shapes.ttl");

/// One SQL connection with a discovered contract, an approved mapping of
/// `ex:Host` that projects `ex:hostname`, and a second (mcp) connection with its
/// own contract and mapping. `row` is appended verbatim as the authority facts.
fn report(row: &str) -> ValidationReport {
    let data = format!(
        "@prefix vg: <http://knuckles.team/kg/virtual-graph#> .\n\
         @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n\
         @prefix ex: <http://example.org/> .\n\
         ex:cmdb a vg:SourceConnection ; vg:sourceKind \"sql\" ; vg:endpointRef ex:ep1 ;\n\
           vg:declaredCapability \"read\" .\n\
         ex:ep1 vg:url \"postgres://cmdb.example.invalid/app\" .\n\
         ex:hw a vg:SourceConnection ; vg:sourceKind \"mcp\" ; vg:endpointRef ex:ep2 ;\n\
           vg:declaredCapability \"read\", \"copy\" .\n\
         ex:ep2 vg:url \"https://hw.example.invalid/mcp\" .\n\
         ex:c1 a vg:MetadataContract ; vg:contractSource ex:cmdb ; vg:schemaVersion \"1\" ;\n\
           vg:contentDigest \"sha256:{zero:0>64}\" ; vg:discoveredEntity \"Host\" ;\n\
           vg:discoveredField \"Host.id\", \"Host.name\" .\n\
         ex:m1 a vg:VirtualMapping ; vg:mapsContract ex:c1 ; vg:mapsOntologyClass ex:Host ;\n\
           vg:mapsEntity \"Host\" ; vg:keyField \"Host.id\" ; vg:approved true ;\n\
           vg:hasPredicateFieldPair ex:p1 .\n\
         ex:p1 a vg:PredicateFieldPair ; vg:predicate ex:hostname ; vg:field \"Host.name\" .\n\
         ex:c2 a vg:MetadataContract ; vg:contractSource ex:hw ; vg:schemaVersion \"1\" ;\n\
           vg:contentDigest \"sha256:{zero:0>64}\" ; vg:discoveredEntity \"Host\" ;\n\
           vg:discoveredField \"Host.id\", \"Host.temp\" .\n\
         ex:m2 a vg:VirtualMapping ; vg:mapsContract ex:c2 ; vg:mapsOntologyClass ex:Host ;\n\
           vg:mapsEntity \"Host\" ; vg:keyField \"Host.id\" ; vg:approved true ;\n\
           vg:hasPredicateFieldPair ex:p2 .\n\
         ex:p2 a vg:PredicateFieldPair ; vg:predicate ex:temperature ; vg:field \"Host.temp\" .\n\
         {row}\n",
        zero = "0"
    );
    validate_turtle(SHAPES, &data).expect("registered virtual-graph shapes must validate")
}

const VALID: &str = "ex:a1 a vg:SourceAuthority ; vg:authoritativeSource ex:cmdb ;\n\
    vg:authorityFor ex:Host ; vg:authorityMapping ex:m1 ; vg:authorityRank 1 ;\n\
    vg:authorityContract \"virtual-graph\" .";

fn row_without(line: &str) -> String {
    VALID.replace(line, "")
}

fn refused_with(row: &str, message: &str) {
    let report = report(row);
    assert!(!report.conforms, "must be refused: {row}");
    assert!(
        report.results.iter().any(|result| result
            .message
            .as_deref()
            .unwrap_or("")
            .contains(message)),
        "expected message {message:?}, got {:?}",
        report.results
    );
}

fn accepted(row: &str) {
    let report = report(row);
    assert!(report.conforms, "{row}: {:?}", report.results);
}

// spec: MEDULLA-SRC-AUTH-R002.1
#[test]
fn a_valid_row_passes_and_each_missing_property_fails() {
    accepted(VALID);
    refused_with(
        &row_without("vg:authoritativeSource ex:cmdb ;"),
        "exactly one authoritativeSource",
    );
    refused_with(
        &row_without("vg:authorityFor ex:Host ;"),
        "exactly one authorityFor",
    );
    refused_with(
        &row_without("vg:authorityMapping ex:m1 ;"),
        "exactly one authorityMapping",
    );
    refused_with(
        &row_without("vg:authorityRank 1 ;"),
        "positive integer authorityRank",
    );
    refused_with(
        &VALID.replace("vg:authorityRank 1", "vg:authorityRank 0"),
        "positive integer authorityRank",
    );
    refused_with(
        &VALID.replace("\"virtual-graph\"", "\"ftp\""),
        "authorityContract is api",
    );
}

// spec: MEDULLA-SRC-AUTH-R002.2
#[test]
fn the_mapping_must_belong_to_the_source_and_map_the_class() {
    refused_with(
        &VALID.replace("vg:authorityMapping ex:m1", "vg:authorityMapping ex:m2"),
        "discovered from the authoritativeSource",
    );
    refused_with(
        &VALID.replace("vg:authorityFor ex:Host", "vg:authorityFor ex:Switch"),
        "must map the authorityFor class",
    );
}

// spec: MEDULLA-SRC-AUTH-R002.3
#[test]
fn a_facet_must_be_projected_by_the_mapping() {
    accepted(&VALID.replace(" .", " ; vg:authorityFacet ex:hostname ."));
    refused_with(
        &VALID.replace(" .", " ; vg:authorityFacet ex:temperature ."),
        "projected by a predicate-field pair",
    );
}

// spec: MEDULLA-SRC-AUTH-R002.4
#[test]
fn the_contract_kind_agrees_with_the_source_kind() {
    let api_on_mcp = "ex:a2 a vg:SourceAuthority ; vg:authoritativeSource ex:hw ;\n\
        vg:authorityFor ex:Host ; vg:authorityMapping ex:m2 ; vg:authorityRank 2 ;\n\
        vg:authorityContract \"api\" .";
    refused_with(api_on_mcp, "must agree with the connection's sourceKind");
    accepted(&api_on_mcp.replace("\"api\"", "\"mcp\""));
    // virtual-graph on sql passes (the VALID row).
    accepted(VALID);
}

// spec: MEDULLA-SRC-AUTH-R002.5
#[test]
fn accelerated_needs_reason_freshness_and_copy() {
    let hw = "ex:a2 a vg:SourceAuthority ; vg:authoritativeSource ex:hw ;\n\
        vg:authorityFor ex:Host ; vg:authorityMapping ex:m2 ; vg:authorityRank 2 ;\n\
        vg:authorityContract \"mcp\"";
    let full =
        " ; vg:materialisation \"accelerated\" ; vg:materialisationReason \"rate limit\" ;\n\
        vg:freshnessMaxAgeSeconds \"300\"^^xsd:nonNegativeInteger .";
    accepted(&format!("{hw}{full}"));
    refused_with(
        &format!(
            "{hw}{}",
            full.replace("vg:materialisationReason \"rate limit\" ;", "")
        ),
        "needs a materialisationReason",
    );
    refused_with(
        &format!(
            "{hw}{}",
            full.replace(
                "vg:freshnessMaxAgeSeconds \"300\"^^xsd:nonNegativeInteger",
                ""
            )
        ),
        "needs a freshnessMaxAgeSeconds bound",
    );
    // The cmdb connection declares no `copy` capability.
    let cmdb = VALID.trim_end_matches(" .");
    refused_with(&format!("{cmdb}{full}"), "declares the copy capability");
    accepted(&VALID.replace(" .", " ; vg:materialisation \"virtual\" ."));
}

// spec: MEDULLA-SRC-AUTH-R002.6
#[test]
fn equal_rank_claims_by_two_sources_are_refused() {
    let second = |rank: u8| {
        format!(
            "ex:a2 a vg:SourceAuthority ; vg:authoritativeSource ex:hw ;\n\
             vg:authorityFor ex:Host ; vg:authorityMapping ex:m2 ; vg:authorityRank {rank} ;\n\
             vg:authorityContract \"mcp\" ."
        )
    };
    refused_with(&format!("{VALID}\n{}", second(1)), "equal authorityRank");
    accepted(&format!("{VALID}\n{}", second(2)));
    // A facet row at the same rank is a different (class, facet, scope) key.
    let facet = second(1).replace(" .", " ; vg:authorityFacet ex:temperature .");
    accepted(&format!("{VALID}\n{facet}"));
}
