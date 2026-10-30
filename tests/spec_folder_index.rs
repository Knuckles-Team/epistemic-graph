//! EG-ORCH-VOCAB-R012.6 hot-path proof: `IndexRepository` over a fixture spec
//! folder, driven through the real `dispatch` shell over a redb-backed server
//! state, writes one task, one goal, one document, one memory and one policy
//! node that a graph query then observes.

#![cfg(all(feature = "ast", feature = "redb"))]

mod common;
#[path = "common/test_support.rs"]
mod test_support;

use eg_types::contract::BoundedVec;
use eg_types::ingestion_wire::{IndexRepositoryScope};
use epistemic_graph::protocol::{GraphType, Method, Response, ResultPayload};
use sha2::{Digest, Sha256};

const SECRET: &str = "spec-folder-index-secret";
const GRAPH: &str = "specfolderindex";

const FIXTURE: [(&str, &str); 5] = [
    ("specs/a/tasks.md", "- [ ] write the thing\n"),
    ("specs/a/spec.md", "# Spec\n"),
    ("specs/a/objective.md", "---\ngoal: ship it\n---\nbody\n"),
    ("specs/memory/notes.md", "remember\n"),
    ("specs/constitution.md", "# Constitution\n"),
];

fn digest(content: &[u8]) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(content)))
}

async fn call(state: &test_support::SharedState, id: u64, method: Method) -> Response {
    let response =
        test_support::dispatch(state, test_support::request(SECRET, id, GRAPH, method)).await;
    assert!(response.error.is_none(), "request {id}: {:?}", response.error);
    response
}

async fn nodes_of(state: &test_support::SharedState, id: u64, label: &str) -> usize {
    let method = Method::GetNodesByLabel {
        label: label.to_string(),
        after: None,
        limit: 10,
    };
    match call(state, id, method).await.result {
        Some(ResultPayload::NodeList(nodes)) => nodes.len(),
        other => panic!("unexpected GetNodesByLabel payload {other:?}"),
    }
}

// spec: EG-ORCH-VOCAB-R012.6
#[test]
fn index_repository_on_spec_folder_yields_five_kinds() {
    test_support::on_engine(async {
        test_support::provision_encryption_key_once("spec-folder-index-encryption-key");
        let dir = test_support::fresh_dir("eg-specfolder");
        let dir_s = dir.to_string_lossy().to_string();
        let (_backend, state) =
            test_support::redb_state_at(SECRET, common::current_isolation(), &dir_s);
        call(
            &state,
            1,
            Method::CreateGraph {
                graph_name: GRAPH.to_string(),
                graph_type: GraphType::Global,
            },
        )
        .await;
        let entries: Vec<(&str, &serde_bytes::Bytes)> = FIXTURE
            .iter()
            .map(|(path, content)| (*path, serde_bytes::Bytes::new(content.as_bytes())))
            .collect();
        let versions = FIXTURE
            .iter()
            .map(|(path, content)| {
                eg_types::test_support::repository_index::file_version(
                    "main",
                    path,
                    digest(content.as_bytes()),
                )
            })
            .collect();
        let scope = IndexRepositoryScope {
            repository_id: "local-git:team/specs".to_string(),
            refs: BoundedVec::new(vec![eg_types::test_support::repository_index::live_ref(
                "main", 'a',
            )])
            .unwrap(),
            file_versions: BoundedVec::new(versions).unwrap(),
            tombstones: BoundedVec::new(Vec::new()).unwrap(),
        };
        call(
            &state,
            2,
            Method::IndexRepository {
                files_msgpack: rmp_serde::to_vec(&entries).unwrap(),
                scope: Some(Box::new(scope)),
            },
        )
        .await;
        for (id, label) in [
            "SpecTask",
            "SpecGoal",
            "Specification",
            "SpecMemory",
            "SpecPolicy",
        ]
        .into_iter()
        .enumerate()
        {
            assert_eq!(
                nodes_of(&state, 10 + id as u64, label).await,
                1,
                "one {label} node is queryable after IndexRepository"
            );
        }
    });
}
