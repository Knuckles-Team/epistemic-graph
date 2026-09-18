//! Golden digest vectors for the 2.27.x contract wave.
//!
//! The pack and decision digests are the two places where an SDK written in
//! another language has to reproduce EG's bytes exactly, and prose cannot be
//! tested. These vectors are GENERATED from the same functions the engine
//! calls, committed, and byte-diffed by `gen_contract --check`, so a change to
//! a framing rule shows up as a diff in a reviewed file rather than as a
//! mismatch discovered by a connector in production.
//!
//! They TEST the layout rules; they do not replace them. The rules live in
//! `eg_types::connector_pack::digest` and `eg_types::decision::digest`.

use eg_types::agent_component::AgentComponentKind;
use eg_types::connector_pack::{digest as pack_digests, ids, PackEntryKind};
use eg_types::decision::digest as decision_digests;
use eg_types::test_support::contract_wave::{decision, pack, statistical};

use super::{normalize, Artifact};

/// One named vector: what was digested, under which domain, and the hex it
/// must produce.
struct Vector {
    name: String,
    domain: String,
    digest: String,
}

/// Render the two committed vector files.
pub(super) fn artifacts() -> Vec<Artifact> {
    vec![
        Artifact {
            path: "contract/fixtures/connector_pack_digest_vectors.json".to_string(),
            bytes: render("connector-pack", pack_vectors()),
        },
        Artifact {
            path: "contract/fixtures/decision_digest_vectors.json".to_string(),
            bytes: render("decision", decision_vectors()),
        },
    ]
}

fn render(family: &str, vectors: Vec<Vector>) -> Vec<u8> {
    let rows: Vec<String> = vectors
        .iter()
        .map(|vector| {
            format!(
                "    {{\n      \"name\": \"{}\",\n      \"domain\": \"{}\",\n      \"sha256\": \
                 \"{}\"\n    }}",
                vector.name, vector.domain, vector.digest
            )
        })
        .collect();
    normalize(format!(
        "{{\n  \"schema\": \"eg-digest-vectors/v1\",\n  \"family\": \"{family}\",\n  \
         \"vectors\": [\n{}\n  ]\n}}\n",
        rows.join(",\n")
    ))
}

fn hex_of(digest: eg_types::contract::Digest256) -> String {
    digest.to_hex()
}

/// Every byte-layout rule of the pack digest family, one vector each.
fn pack_vectors() -> Vec<Vector> {
    let index = pack::index();
    let annotations = pack::annotations();
    let model = annotations
        .model
        .clone()
        .expect("the sample annotations declare a model profile");
    let entry = pack::entry(PackEntryKind::Tool, "search");
    let sorted = vec![
        "eg:capability/retrieval".to_string(),
        "eg:capability/action".to_string(),
        "eg:capability/action".to_string(),
    ];
    vec![
        Vector {
            name: "entry_digest/tool".to_string(),
            domain: domain_of(pack_digests::ENTRY_DIGEST_DOMAIN),
            digest: hex_of(pack_digests::entry_digest(&entry).expect("sample entry digests")),
        },
        Vector {
            name: "entry_digest/server".to_string(),
            domain: domain_of(pack_digests::ENTRY_DIGEST_DOMAIN),
            digest: hex_of(
                pack_digests::entry_digest(&index.server).expect("sample server entry digests"),
            ),
        },
        Vector {
            name: "annotations_digest/full".to_string(),
            domain: domain_of(pack_digests::ANNOTATIONS_DIGEST_DOMAIN),
            digest: hex_of(
                pack_digests::annotations_digest(&annotations).expect("sample annotations digest"),
            ),
        },
        Vector {
            name: "annotations_digest/empty".to_string(),
            domain: domain_of(pack_digests::ANNOTATIONS_DIGEST_DOMAIN),
            digest: hex_of(
                pack_digests::annotations_digest(&Default::default())
                    .expect("empty annotations digest"),
            ),
        },
        Vector {
            name: "model_facts_digest/tri_state".to_string(),
            domain: domain_of(pack_digests::MODEL_FACTS_DIGEST_DOMAIN),
            digest: hex_of(
                pack_digests::model_facts_digest(&model).expect("sample model facts digest"),
            ),
        },
        Vector {
            name: "references_digest/sorted_pairs".to_string(),
            domain: domain_of(pack_digests::REFERENCES_DIGEST_DOMAIN),
            digest: hex_of(
                pack_digests::references_digest(entry.references.as_slice())
                    .expect("sample references digest"),
            ),
        },
        Vector {
            name: "list_digest/sorted_and_deduplicated".to_string(),
            domain: domain_of(pack_digests::ENTRY_DIGEST_DOMAIN),
            digest: hex_of(
                pack_digests::list_digest(pack_digests::ENTRY_DIGEST_DOMAIN, &sorted)
                    .expect("sample list digest"),
            ),
        },
        Vector {
            name: "pack_digest/one_entry_per_kind".to_string(),
            domain: domain_of(pack_digests::PACK_DIGEST_DOMAIN),
            digest: hex_of(pack_digests::pack_digest(&index).expect("sample pack digest")),
        },
        Vector {
            name: "component_id/escaped".to_string(),
            domain: "eg/connector-pack-component-id/v1".to_string(),
            digest: ids::pack_component_id(
                index.connector.as_str(),
                PackEntryKind::Tool,
                "search files/~ 100%",
            ),
        },
        Vector {
            name: "component_id/unescaped".to_string(),
            domain: "eg/connector-pack-component-id/v1".to_string(),
            digest: ids::pack_component_id(
                index.connector.as_str(),
                PackEntryKind::ModelProfile,
                "model-1.0_x",
            ),
        },
    ]
}

/// The five decision digests over fixed samples.
fn decision_vectors() -> Vec<Vector> {
    let solved = decision::every_decision_outcome()
        .into_iter()
        .next()
        .expect("the solved outcome is first");
    let record = decision::record(solved);
    let catalog = [
        (
            "component-b",
            "sha256:22222222222222222222222222222222222222222222222222222222222222",
            eg_types::agent_library::AgentLibraryLifecycle::Published,
        ),
        (
            "component-a",
            "sha256:11111111111111111111111111111111111111111111111111111111111111",
            eg_types::agent_library::AgentLibraryLifecycle::Retired,
        ),
    ];
    vec![
        Vector {
            name: "record_digest/solved".to_string(),
            domain: decision_digests::DECISION_RECORD_DIGEST_DOMAIN.to_string(),
            digest: decision_digests::record_digest(&record),
        },
        Vector {
            name: "inputs_digest/library_candidates".to_string(),
            domain: decision_digests::DECISION_INPUTS_DIGEST_DOMAIN.to_string(),
            digest: decision_digests::inputs_digest(&record.inputs),
        },
        Vector {
            name: "policy_digest/default_shape".to_string(),
            domain: decision_digests::DECISION_POLICY_DIGEST_DOMAIN.to_string(),
            digest: decision_digests::policy_digest(&decision::policy()),
        },
        Vector {
            name: "catalog_digest/unsorted_input".to_string(),
            domain: decision_digests::DECISION_CATALOG_DIGEST_DOMAIN.to_string(),
            digest: decision_digests::catalog_digest(&catalog),
        },
        Vector {
            name: "ontology_digest/native_vocabulary".to_string(),
            domain: eg_types::agent_ontology::AGENT_ONTOLOGY_DIGEST_DOMAIN.to_string(),
            digest: eg_types::agent_ontology::ontology_digest(),
        },
        Vector {
            name: "statistical_record_digest/acted".to_string(),
            domain: decision_digests::STATISTICAL_RECORD_DIGEST_DOMAIN.to_string(),
            digest: statistical_record_digest(),
        },
        Vector {
            name: "component_kind/decision_record_token".to_string(),
            domain: "eg/agent-component-kind/v3".to_string(),
            digest: AgentComponentKind::DecisionRecord.as_str().to_string(),
        },
    ]
}

/// The statistical record digest, over a record whose outcome is `Acted`.
fn statistical_record_digest() -> String {
    let outcome = statistical::every_statistical_outcome()
        .into_iter()
        .next()
        .expect("the acted outcome is first");
    decision_digests::digest_text(decision_digests::STATISTICAL_RECORD_DIGEST_DOMAIN, &outcome)
}

fn domain_of(domain: &[u8]) -> String {
    String::from_utf8(domain.to_vec()).expect("every digest domain is ASCII")
}
