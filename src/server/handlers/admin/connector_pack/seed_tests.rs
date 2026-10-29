use std::collections::{BTreeMap, BTreeSet};

use eg_types::connector_pack::{SeedGraphImport, SeedPart, SeedPartKey};
use eg_types::contract::{Digest256, ResourceId};

use super::seed::{canonical_digest, prepare_seed_import, SeedImportOutcome, MAX_SEED_TRIPLES};

const SEED_NS: &str = "http://knuckles.team/kg/connector-seed#";

fn digest(byte: u8) -> Digest256 {
    Digest256::from_bytes([byte; 32])
}

fn shapes() -> eg_shacl::Graph {
    eg_shacl::graph_from_turtle(&format!(
        "@prefix sh: <http://www.w3.org/ns/shacl#> .\n\
         @prefix seed: <{SEED_NS}> .\n\
         seed:SeedSubjectShape a sh:NodeShape ;\n\
             sh:targetClass seed:SeedSubject ;\n\
             sh:property [ sh:path seed:fromSeed ; sh:minCount 1 ; sh:maxCount 1 ; sh:nodeKind sh:IRI ] ."
    ))
    .expect("seed test shapes parse")
}

fn part(key: SeedPartKey, turtle: &str) -> SeedPart {
    SeedPart {
        key,
        turtle: turtle.to_string(),
        digest: canonical_digest(turtle).expect("canonical seed digest"),
    }
}

fn seed(catalog_name: &str, content_digest: Digest256) -> SeedGraphImport {
    let mut parts = BTreeMap::new();
    parts.insert(
        SeedPartKey::Identity,
        part(
            SeedPartKey::Identity,
            &format!(
                "<urn:seed:instance> a <{SEED_NS}SeedSubject> ; <{SEED_NS}fromSeed> <urn:seed:r1> ."
            ),
        ),
    );
    parts.insert(
        SeedPartKey::Catalog,
        part(
            SeedPartKey::Catalog,
            &format!("<urn:seed:{catalog_name}> a <{SEED_NS}SeedSubject> ; <{SEED_NS}fromSeed> <urn:seed:r1> ."),
        ),
    );
    parts.insert(
        SeedPartKey::References,
        part(
            SeedPartKey::References,
            "<urn:seed:item> <urn:seed:references> <urn:source:item> .",
        ),
    );
    SeedGraphImport {
        connector: ResourceId::new("demo").unwrap(),
        instance_iri: "urn:connector:demo:instance".to_string(),
        pack_digest: digest(1),
        parts,
        content_digest,
    }
}

/// MEDULLA-TYPED-PACKS-R104.2: a changed catalog part replaces only that part;
/// identity and references remain byte-identical in the one prepared state.
#[test]
fn changed_part_reimports_alone() {
    let shapes = shapes();
    let first = seed("old", digest(2));
    let initial = prepare_seed_import(&first, None, &shapes).expect("initial seed import");
    assert_eq!(initial.outcome, SeedImportOutcome::Inserted);
    assert_eq!(initial.state.parts.len(), 3);
    assert!(initial
        .state
        .parts
        .values()
        .all(|part| part.triple_count <= MAX_SEED_TRIPLES));

    let second = seed("new", digest(3));
    let changed = prepare_seed_import(&second, Some(&initial.state), &shapes)
        .expect("partial seed replacement");
    assert_eq!(
        changed.outcome,
        SeedImportOutcome::Replaced {
            changed_parts: BTreeSet::from([SeedPartKey::Catalog])
        }
    );
    assert_eq!(
        changed.state.parts[&SeedPartKey::Identity],
        initial.state.parts[&SeedPartKey::Identity]
    );
    assert_eq!(
        changed.state.parts[&SeedPartKey::References],
        initial.state.parts[&SeedPartKey::References]
    );
    assert_ne!(
        changed.state.parts[&SeedPartKey::Catalog],
        initial.state.parts[&SeedPartKey::Catalog]
    );
}

#[test]
fn equal_content_digest_is_a_noop_after_the_existing_state_is_known() {
    let shapes = shapes();
    let first = seed("same", digest(4));
    let initial = prepare_seed_import(&first, None, &shapes).expect("initial seed import");
    let unchanged = prepare_seed_import(&first, Some(&initial.state), &shapes)
        .expect("equal digest should be unchanged");
    assert_eq!(unchanged.outcome, SeedImportOutcome::Unchanged);
    assert_eq!(unchanged.state, initial.state);
}

#[test]
fn canonical_digest_ignores_statement_order() {
    let first = "<urn:s> <urn:p> <urn:o> .\n<urn:s> <urn:q> \"value\" .";
    let second = "<urn:s> <urn:q> \"value\" .\n<urn:s> <urn:p> <urn:o> .";
    assert_eq!(canonical_digest(first), canonical_digest(second));
}
