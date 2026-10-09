// CONCEPT:EH-280 — a synthetic two-branch repository: `main` and `feature`
// share `pkg/util.py` byte for byte and differ in `pkg/app.py`.

use super::index_branches;
use crate::parser::branch_scope::{blob_node_id, branch_node_id, content_digest, file_version_id};
use crate::parser::enrichment_admission::{native_admissions, NativePolicyVerdict, UnitKey};
use eg_types::contract::BoundedVec;
use eg_types::ingestion_wire::{
    IndexFileStatus, IndexFileVersion, IndexRef, IndexRefStatus, IndexRepositoryScope, IndexResult,
    IndexTombstone,
};

const REPO: &str = "forge/team/project";
const UTIL: &str = "def shared():\n    return 1\n";
const APP_MAIN: &str = "from pkg.util import shared\n\ndef run():\n    return shared()\n";
const APP_FEATURE: &str = "from pkg.util import shared\n\ndef run():\n    return shared() + 1\n";

use eg_types::test_support::repository_index::live_ref as live;

fn member(ref_name: &str, path: &str, content: &str) -> IndexFileVersion {
    eg_types::test_support::repository_index::file_version(
        ref_name,
        path,
        content_digest(content.as_bytes()),
    )
}

fn scope(
    refs: Vec<IndexRef>,
    versions: Vec<IndexFileVersion>,
    tombstones: Vec<IndexTombstone>,
) -> IndexRepositoryScope {
    IndexRepositoryScope {
        repository_id: REPO.to_string(),
        refs: BoundedVec::new(refs).expect("bounded refs"),
        file_versions: BoundedVec::new(versions).expect("bounded versions"),
        tombstones: BoundedVec::new(tombstones).expect("bounded tombstones"),
    }
}

fn two_branch_scope() -> IndexRepositoryScope {
    scope(
        vec![live("main", 'a'), live("feature", 'b')],
        vec![
            member("main", "pkg/util.py", UTIL),
            member("main", "pkg/app.py", APP_MAIN),
            member("feature", "pkg/util.py", UTIL),
            member("feature", "pkg/app.py", APP_FEATURE),
        ],
        Vec::new(),
    )
}

/// The three UNIQUE blobs of the two branches — `pkg/util.py` ships once.
fn unique_blobs() -> Vec<(String, Vec<u8>)> {
    [
        ("pkg/util.py", UTIL),
        ("pkg/app.py", APP_MAIN),
        ("pkg/app.py", APP_FEATURE),
    ]
    .iter()
    .map(|(path, body)| (path.to_string(), body.as_bytes().to_vec()))
    .collect()
}

fn index(scope: &IndexRepositoryScope) -> IndexResult {
    index_branches(unique_blobs(), scope).expect("valid branch-aware batch")
}

fn edges_of<'a>(result: &'a IndexResult, edge_type: &str) -> Vec<(&'a str, &'a str)> {
    result
        .edges
        .iter()
        .filter(|edge| edge.edge_type == edge_type)
        .map(|edge| (edge.source.as_str(), edge.target.as_str()))
        .collect()
}

fn nodes_of<'a>(result: &'a IndexResult, node_type: &str) -> Vec<&'a str> {
    result
        .nodes
        .iter()
        .filter(|node| node.node_type == node_type)
        .map(|node| node.node_id.as_str())
        .collect()
}

fn version(path: &str, content: &str) -> String {
    file_version_id(REPO, path, &content_digest(content.as_bytes()))
}

fn symbols_named<'a>(result: &'a IndexResult, name: &str) -> Vec<&'a str> {
    result
        .nodes
        .iter()
        .filter(|node| node.node_type == "SYMBOL")
        .filter(|node| node.properties.get("name").map(String::as_str) == Some(name))
        .map(|node| node.node_id.as_str())
        .collect()
}

// spec: EG-REPO-INGEST-R007
#[test]
fn shared_blob_is_parsed_once_and_its_symbols_attach_to_the_blob() {
    let result = index(&two_branch_scope());

    assert!(result.edges.iter().all(|edge| {
        matches!(
            edge.properties.get("evidence_rung").map(String::as_str),
            Some("EXTRACTED" | "INFERRED" | "DERIVED")
        )
    }));
    assert!(result
        .edges
        .iter()
        .filter(|edge| matches!(edge.edge_type.as_str(), "hasFileVersion" | "hasBlob"))
        .all(|edge| edge.properties.get("evidence_rung").map(String::as_str) == Some("EXTRACTED")));

    let shared = symbols_named(&result, "shared");
    assert_eq!(
        shared.len(),
        1,
        "a blob on two branches is parsed exactly once"
    );
    assert_eq!(
        symbols_named(&result, "run").len(),
        2,
        "two distinct app blobs"
    );
    let util_blob = blob_node_id(&content_digest(UTIL.as_bytes()));
    assert!(edges_of(&result, "IMPLEMENTS").contains(&(util_blob.as_str(), shared[0])));
    assert!(
        result
            .edges
            .iter()
            .all(|edge| !edge.source.starts_with("file:")),
        "no edge may stay anchored on a parse-unit name"
    );
    assert_eq!(result.files_parsed, 3);
    assert_eq!(nodes_of(&result, "Blob").len(), 3);
    assert_eq!(result.native_rung_evidence.len(), 3);
    assert!(result
        .native_rung_evidence
        .iter()
        .all(|evidence| evidence.extracted_facts > 0
            && evidence.symbol_resolution_completed
            && evidence.statistical_completed));
    assert!(serde_json::to_value(&result)
        .unwrap()
        .get("native_rung_evidence")
        .is_none());
}

#[test]
fn outcomes_keep_submission_order_and_submitted_names() {
    let result = index(&two_branch_scope());
    let names: Vec<&str> = result
        .file_outcomes
        .iter()
        .map(|outcome| outcome.file_path.as_str())
        .collect();
    assert_eq!(names, ["pkg/util.py", "pkg/app.py", "pkg/app.py"]);
    assert!(result
        .file_outcomes
        .iter()
        .all(|outcome| outcome.status == IndexFileStatus::Success));
    assert_eq!(
        result.file_outcomes[2].content_digest,
        content_digest(APP_FEATURE.as_bytes())
    );
}

#[test]
fn branches_reference_exactly_their_own_file_versions() {
    let result = index(&two_branch_scope());
    let main = branch_node_id(REPO, "main");
    let feature = branch_node_id(REPO, "feature");
    let util = version("pkg/util.py", UTIL);
    let mut members = edges_of(&result, "hasFileVersion");
    members.sort_unstable();
    let app_main = version("pkg/app.py", APP_MAIN);
    let app_feature = version("pkg/app.py", APP_FEATURE);
    let mut expected = vec![
        (main.as_str(), util.as_str()),
        (main.as_str(), app_main.as_str()),
        (feature.as_str(), util.as_str()),
        (feature.as_str(), app_feature.as_str()),
    ];
    expected.sort_unstable();
    assert_eq!(members, expected);
    assert_eq!(
        nodes_of(&result, "FileVersion").len(),
        3,
        "shared path+blob is one node"
    );
    assert_eq!(
        nodes_of(&result, "Branch"),
        [feature.as_str(), main.as_str()]
    );
    assert_eq!(edges_of(&result, "hasBlob").len(), 3);
}

#[test]
fn imports_resolve_per_ref_between_file_versions() {
    let result = index(&two_branch_scope());
    let util = version("pkg/util.py", UTIL);
    let mut depends = edges_of(&result, "depends_on");
    depends.sort_unstable();
    let app_main = version("pkg/app.py", APP_MAIN);
    let app_feature = version("pkg/app.py", APP_FEATURE);
    let mut expected = vec![
        (app_main.as_str(), util.as_str()),
        (app_feature.as_str(), util.as_str()),
    ];
    expected.sort_unstable();
    assert_eq!(depends, expected);
    assert_eq!(result.imports_resolved, 2, "one import per ref membership");
    assert!(
        result.calls_resolved >= 2,
        "run -> shared binds on the shared blob"
    );
}

#[test]
fn a_ref_without_the_import_target_gets_no_dependency() {
    let lonely = scope(
        vec![live("orphan", 'c')],
        vec![member("orphan", "pkg/app.py", APP_MAIN)],
        Vec::new(),
    );
    let blob = vec![("pkg/app.py".to_string(), APP_MAIN.as_bytes().to_vec())];
    let result = index_branches(blob, &lonely).expect("valid batch");
    assert!(edges_of(&result, "depends_on").is_empty());
    assert_eq!(result.imports_unresolved, 1);
}

#[test]
fn symbol_identity_does_not_depend_on_the_shipping_path() {
    let at = |path: &str| {
        let single = scope(
            vec![live("main", 'a')],
            vec![member("main", path, UTIL)],
            Vec::new(),
        );
        let blob = vec![(path.to_string(), UTIL.as_bytes().to_vec())];
        let result = index_branches(blob, &single).expect("valid batch");
        symbols_named(&result, "shared")[0].to_string()
    };
    assert_eq!(at("pkg/util.py"), at("vendored/copy/helpers.py"));
}

#[test]
fn projection_is_deterministic_under_input_permutation() {
    let forward = index(&two_branch_scope());
    let mut permuted = two_branch_scope();
    let mut refs = permuted.refs.as_slice().to_vec();
    refs.reverse();
    let mut versions = permuted.file_versions.as_slice().to_vec();
    versions.reverse();
    permuted.refs = BoundedVec::new(refs).expect("bounded");
    permuted.file_versions = BoundedVec::new(versions).expect("bounded");
    let backward = index(&permuted);
    let key = |result: &IndexResult| {
        let nodes: Vec<String> = result
            .nodes
            .iter()
            .map(|node| node.node_id.clone())
            .collect();
        let edges: Vec<String> = result
            .edges
            .iter()
            .map(|edge| format!("{}|{}|{}", edge.source, edge.edge_type, edge.target))
            .collect();
        (nodes, edges)
    };
    assert_eq!(key(&forward), key(&backward));
}

#[test]
fn tombstones_project_removal_with_successor() {
    let prior = "def gone():\n    return 0\n";
    let removal = IndexTombstone {
        ref_name: "feature".to_string(),
        path: "pkg/old.py".to_string(),
        prior_blob_digest: content_digest(prior.as_bytes()),
        successor_path: Some("pkg/new.py".to_string()),
    };
    let with_tombstone = scope(
        vec![live("main", 'a'), live("feature", 'b')],
        two_branch_scope().file_versions.as_slice().to_vec(),
        vec![removal],
    );
    let result = index(&with_tombstone);
    let removed = result
        .edges
        .iter()
        .find(|edge| edge.edge_type == "removesFileVersion")
        .expect("tombstone edge");
    assert_eq!(removed.source, branch_node_id(REPO, "feature"));
    assert_eq!(removed.target, version("pkg/old.py", prior));
    assert_eq!(removed.properties["successor_path"], "pkg/new.py");
    let prior_blob = blob_node_id(&content_digest(prior.as_bytes()));
    assert!(nodes_of(&result, "Blob").contains(&prior_blob.as_str()));
}

#[test]
fn a_deleted_ref_carries_only_tombstones() {
    let mut deleted = live("gone", 'd');
    deleted.status = IndexRefStatus::Deleted;
    let invalid = scope(
        vec![deleted],
        vec![member("gone", "pkg/util.py", UTIL)],
        Vec::new(),
    );
    let error = index_branches(Vec::new(), &invalid).expect_err("deleted ref with files");
    assert!(error.contains("deleted ref"), "{error}");
}

// spec: EG-REPO-INGEST-R007
#[test]
fn malformed_scopes_are_refused() {
    let unbound = scope(vec![live("main", 'a')], Vec::new(), Vec::new());
    let error = index_branches(unique_blobs(), &unbound).expect_err("unbound blob");
    assert!(error.contains("not bound"), "{error}");

    let undeclared = scope(
        vec![live("main", 'a')],
        vec![member("dev", "a.py", UTIL)],
        Vec::new(),
    );
    assert!(index_branches(Vec::new(), &undeclared).is_err());

    let mut mutable = live("main", 'a');
    mutable.revision_id = "main".to_string();
    assert!(index_branches(Vec::new(), &scope(vec![mutable], Vec::new(), Vec::new())).is_err());

    let twice = vec![
        ("pkg/util.py".to_string(), UTIL.as_bytes().to_vec()),
        ("pkg/util.py".to_string(), UTIL.as_bytes().to_vec()),
    ];
    let error = index_branches(twice, &two_branch_scope()).expect_err("duplicate blob");
    assert!(error.contains("twice"), "{error}");
}

// spec: EG-REPO-INGEST-R007
#[test]
fn empty_import_target_does_not_falsely_abstain_from_resolution() {
    let empty = "";
    let scoped = scope(
        vec![live("main", 'a')],
        vec![
            member("main", "pkg/util.py", empty),
            member("main", "pkg/app.py", APP_MAIN),
        ],
        Vec::new(),
    );
    let result = index_branches(
        vec![
            ("pkg/util.py".into(), Vec::new()),
            ("pkg/app.py".into(), APP_MAIN.as_bytes().to_vec()),
        ],
        &scoped,
    )
    .unwrap();
    let key = UnitKey {
        content_digest: content_digest(empty.as_bytes()),
        parser_capability_digest: result.file_outcomes[0].parser_capability_digest.clone(),
    };
    let evidence = result
        .native_rung_evidence
        .iter()
        .find(|item| item.content_digest == key.content_digest)
        .unwrap();
    assert_eq!(evidence.extracted_facts, 0);
    assert!(
        evidence.inferred_facts > 0,
        "import target gained a resolved edge"
    );
    let policy = std::collections::BTreeMap::from([(
        key.clone(),
        NativePolicyVerdict {
            allowed: true,
            compute_units: 1,
        },
    )]);
    assert!(!native_admissions(&result.native_rung_evidence, &policy).contains_key(&key));
}
