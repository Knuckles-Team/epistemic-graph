//! EH-280 — a branch-aware `IndexRepository` batch is DURABLE.
//!
//! Driven through the real `dispatch` shell over a redb-backed `ServerState`:
//! a two-branch batch (`main` and `feature` share `pkg/util.py`) commits its
//! `:Blob` / `:FileVersion` / `:Branch -> :FileVersion` projection through the
//! ChangeEnvelope authority; an identical re-send replays without advancing the
//! graph; a tombstone batch removes the membership edge; and after the durable
//! tier is shut down and reopened into a FRESH server state, the projection is
//! still there.
//!
//! Each test runs on the engine's own runtime shape (driver thread and Tokio
//! workers at `ENGINE_WORKER_STACK_BYTES`), exactly as production serves the
//! request: the full graph-scoped write path is deeper than a default-sized
//! `#[tokio::test]` thread allows in a debug build.

#![cfg(all(feature = "ast", feature = "redb"))]

mod common;
#[path = "common/test_support.rs"]
mod test_support;

use eg_types::contract::BoundedVec;
use eg_types::ingestion_wire::{IndexFileVersion, IndexRepositoryScope, IndexTombstone};
use epistemic_graph::protocol::{GraphType, Method, Response, ResultPayload};
use sha2::{Digest, Sha256};

const SECRET: &str = "repository-index-durable-secret";
const GRAPH: &str = "repoindexdurable";
const UTIL: &[u8] = b"def shared():\n    return 1\n";
const APP: &[u8] = b"from pkg.util import shared\n\ndef run():\n    return shared()\n";
const NEW: &[u8] = b"def new():\n    return 3\n";

fn digest(content: &[u8]) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(content)))
}

use eg_types::test_support::repository_index::live_ref as live;

fn member(ref_name: &str, path: &str, content: &[u8]) -> IndexFileVersion {
    eg_types::test_support::repository_index::file_version(ref_name, path, digest(content))
}

fn scope(versions: Vec<IndexFileVersion>, tombstones: Vec<IndexTombstone>) -> IndexRepositoryScope {
    IndexRepositoryScope {
        repository_id: "local-git:team/project".to_string(),
        refs: BoundedVec::new(vec![live("main", 'a'), live("feature", 'b')]).unwrap(),
        file_versions: BoundedVec::new(versions).unwrap(),
        tombstones: BoundedVec::new(tombstones).unwrap(),
    }
}

fn first_scope() -> IndexRepositoryScope {
    scope(
        vec![
            member("main", "pkg/util.py", UTIL),
            member("main", "pkg/app.py", APP),
            member("feature", "pkg/util.py", UTIL),
            member("feature", "pkg/app.py", APP),
            member("feature", "pkg/new.py", NEW),
        ],
        Vec::new(),
    )
}

/// The canonical `[[name, bin], ...]` source collection.
fn files_msgpack(files: &[(&str, &[u8])]) -> Vec<u8> {
    let entries: Vec<(&str, &serde_bytes::Bytes)> = files
        .iter()
        .map(|(path, content)| (*path, serde_bytes::Bytes::new(content)))
        .collect();
    rmp_serde::to_vec(&entries).unwrap()
}

async fn call(state: &test_support::SharedState, id: u64, method: Method) -> Response {
    let response =
        test_support::dispatch(state, test_support::request(SECRET, id, GRAPH, method)).await;
    assert!(
        response.error.is_none(),
        "request {id} failed: {:?}",
        response.error
    );
    response
}

async fn index(
    state: &test_support::SharedState,
    id: u64,
    files: &[(&str, &[u8])],
    scope: IndexRepositoryScope,
) -> serde_json::Value {
    let method = Method::IndexRepository {
        files_msgpack: files_msgpack(files),
        scope: Some(Box::new(scope)),
    };
    match call(state, id, method).await.result {
        Some(ResultPayload::Json(value)) => value,
        Some(ResultPayload::Raw(bytes)) => rmp_serde::from_slice(&bytes).unwrap(),
        other => panic!("unexpected IndexRepository payload {other:?}"),
    }
}

/// The id of the projected node of `kind` whose `key` property is `value`.
fn node_id(result: &serde_json::Value, kind: &str, key: &str, value: &str) -> String {
    result["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|node| node["node_type"] == kind && node["properties"][key] == value)
        .unwrap_or_else(|| panic!("no {kind} with {key}={value}"))["node_id"]
        .as_str()
        .unwrap()
        .to_string()
}

fn file_version(result: &serde_json::Value, path: &str, content: &[u8]) -> String {
    result["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|node| {
            node["node_type"] == "FileVersion"
                && node["properties"]["path"] == path
                && node["properties"]["content_digest"] == digest(content)
        })
        .unwrap_or_else(|| panic!("no FileVersion {path}"))["node_id"]
        .as_str()
        .unwrap()
        .to_string()
}

async fn has_edge(state: &test_support::SharedState, id: u64, source: &str, target: &str) -> bool {
    let method = Method::HasEdge {
        source_id: source.to_string(),
        target_id: target.to_string(),
    };
    match call(state, id, method).await.result {
        Some(ResultPayload::Bool(present)) => present,
        other => panic!("unexpected HasEdge payload {other:?}"),
    }
}

async fn has_node(state: &test_support::SharedState, id: u64, node: &str) -> bool {
    let method = Method::HasNode {
        node_id: node.to_string(),
    };
    match call(state, id, method).await.result {
        Some(ResultPayload::Bool(present)) => present,
        other => panic!("unexpected HasNode payload {other:?}"),
    }
}

fn graph_version(state: &test_support::SharedState) -> u64 {
    state
        .try_read()
        .expect("state is not contended")
        .registry
        .get(GRAPH)
        .expect("graph registered")
        .core
        .version()
}

/// Run `test` on the engine's runtime shape and propagate its failure.
fn on_engine<F: std::future::Future<Output = ()> + 'static>(test: fn() -> F) {
    let driver = epistemic_graph::server::spawn_engine_driver(move || {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_stack_size(epistemic_graph::server::ENGINE_WORKER_STACK_BYTES)
            .enable_all()
            .build()
            .expect("build engine runtime")
            .block_on(test());
    })
    .expect("spawn engine driver");
    epistemic_graph::server::join_engine_driver(driver).expect("engine test body failed");
}

#[test]
fn scoped_index_commits_replays_tombstones_and_survives_restart() {
    on_engine(commit_replay_tombstone_restart);
}

#[test]
fn repository_code_with_decorators_and_home_paths_commits() {
    on_engine(code_with_decorators_and_home_paths);
}

#[test]
fn one_path_with_diverged_blobs_commits_in_one_batch() {
    on_engine(one_path_diverged_across_refs);
}

#[test]
fn an_empty_source_file_commits() {
    on_engine(empty_source_file);
}

#[cfg(feature = "blob")]
#[test]
fn scoped_index_pins_source_content_in_blob_cas() {
    on_engine(index_with_durable_source_refs);
}

#[cfg(feature = "blob")]
async fn index_with_durable_source_refs() {
    use epistemic_graph::server::blob::store::{ChunkStore, RedbChunkStore};
    use epistemic_graph::server::blob::BlobCursors;
    use std::sync::Arc;

    let (_dir, dir_s, _backend, state) = durable_graph("eg-repoindex-cas").await;
    let store = Arc::new(RedbChunkStore::open(&dir_s).unwrap());
    state.write().await.blob = Some(Arc::new(BlobCursors::new(store.clone())));
    let result = index(
        &state,
        2,
        &[("pkg/util.py", UTIL)],
        scope(vec![member("main", "pkg/util.py", UTIL)], Vec::new()),
    )
    .await;
    let node = result["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|node| {
            node["node_type"] == "Blob" && node["properties"]["content_digest"] == digest(UTIL)
        })
        .expect("indexed Blob");
    let content_ref = node["properties"]["content_ref"].as_str().expect("CAS ref");
    assert_eq!(node["properties"]["content_length"], UTIL.len().to_string());
    let manifest = content_ref.strip_prefix("cas:sha256:").expect("CAS scheme");
    let stored = store
        .get_manifest(manifest)
        .unwrap()
        .expect("durable manifest");
    assert_eq!(stored.len, UTIL.len() as u64);
}

/// A fresh durable engine directory with the test graph created in it.
async fn durable_graph(
    prefix: &str,
) -> (
    std::path::PathBuf,
    String,
    test_support::SharedPersistence,
    test_support::SharedState,
) {
    test_support::provision_encryption_key_once("repository-index-durable-encryption-key");
    let dir = test_support::fresh_dir(prefix);
    let dir_s = dir.to_string_lossy().to_string();
    let (backend, state) = test_support::redb_state_at(SECRET, common::current_isolation(), &dir_s);
    call(
        &state,
        1,
        Method::CreateGraph {
            graph_name: GRAPH.to_string(),
            graph_type: GraphType::Global,
        },
    )
    .await;
    (dir, dir_s, backend, state)
}

async fn commit_replay_tombstone_restart() {
    let (dir, dir_s, backend, state) = durable_graph("eg-repoindex").await;

    // ── Commit: three unique blobs for five memberships over two branches ──
    let blobs: [(&str, &[u8]); 3] = [
        ("pkg/app.py", APP),
        ("pkg/new.py", NEW),
        ("pkg/util.py", UTIL),
    ];
    let first = index(&state, 2, &blobs, first_scope()).await;
    let main = node_id(&first, "Branch", "ref_name", "main");
    let feature = node_id(&first, "Branch", "ref_name", "feature");
    let util = file_version(&first, "pkg/util.py", UTIL);
    let new = file_version(&first, "pkg/new.py", NEW);
    let util_blob = format!("blob:{}", digest(UTIL));
    assert!(has_edge(&state, 3, &main, &util).await);
    assert!(has_edge(&state, 4, &feature, &util).await);
    assert!(has_edge(&state, 5, &feature, &new).await);
    assert!(
        !has_edge(&state, 6, &main, &new).await,
        "new.py is not on main"
    );
    assert!(has_edge(&state, 7, &util, &util_blob).await);
    let shared = node_id(&first, "SYMBOL", "name", "shared");
    assert!(
        has_edge(&state, 8, &util_blob, &shared).await,
        "symbols attach to :Blob"
    );

    // ── Idempotent re-run: the identical batch replays, the graph stays put ──
    let committed = graph_version(&state);
    let again = index(&state, 9, &blobs, first_scope()).await;
    assert_eq!(again["nodes"], first["nodes"]);
    assert_eq!(
        graph_version(&state),
        committed,
        "an unchanged batch must not commit twice"
    );

    // ── Tombstone: new.py leaves feature; its blob is already indexed ──
    let removal = IndexTombstone {
        ref_name: "feature".to_string(),
        path: "pkg/new.py".to_string(),
        prior_blob_digest: digest(NEW),
        successor_path: None,
    };
    index(&state, 10, &[], scope(Vec::new(), vec![removal])).await;
    assert!(
        !has_edge(&state, 11, &feature, &new).await,
        "tombstone removes membership"
    );
    assert!(has_edge(&state, 12, &feature, &util).await);

    // ── Restart: reopen the durable tier into a fresh server state ──
    backend.shutdown();
    state.write().await.persistence = None;
    drop(state);
    drop(backend);
    let reopened = test_support::reopen_with_bounded_retry(
        || test_support::open_redb_backend(dir_s.clone()),
        "reopen durable tier",
    )
    .await;
    let restarted = test_support::state_with(
        SECRET,
        common::current_isolation(),
        Some(dir_s.clone()),
        Some(reopened.clone()),
    );
    reopened
        .load_all(&restarted)
        .await
        .expect("reload durable graphs");
    assert!(has_node(&restarted, 20, &util_blob).await);
    assert!(has_node(&restarted, 21, &shared).await);
    assert!(has_edge(&restarted, 22, &main, &util).await);
    assert!(has_edge(&restarted, 23, &util_blob, &shared).await);
    assert!(
        !has_edge(&restarted, 24, &feature, &new).await,
        "the tombstone is durable"
    );

    reopened.shutdown();
    restarted.write().await.persistence = None;
    drop(restarted);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Real code is repository content, not host identity: a decorator (`@`) and a
/// repository path with a `home` segment must commit durably (EH-280).
async fn code_with_decorators_and_home_paths() {
    const VIEWS: &[u8] = b"from dataclasses import dataclass\n\n\n@dataclass\nclass Settings:\n    name: str = \"x\"\n\n\n@dataclass(frozen=True)\nclass Page:\n    title: str = \"t\"\n";
    let (dir, _, backend, state) = durable_graph("eg-repoindex-code").await;
    let code = IndexRepositoryScope {
        repository_id: "local-git:team/web".to_string(),
        refs: BoundedVec::new(vec![live("main", 'c')]).unwrap(),
        file_versions: BoundedVec::new(vec![member("main", "app/home/views.py", VIEWS)]).unwrap(),
        tombstones: BoundedVec::default(),
    };
    let result = index(&state, 2, &[("app/home/views.py", VIEWS)], code).await;
    let main = node_id(&result, "Branch", "ref_name", "main");
    let views = file_version(&result, "app/home/views.py", VIEWS);
    let settings = node_id(&result, "SYMBOL", "name", "Settings");
    assert!(has_edge(&state, 3, &main, &views).await);
    assert!(has_node(&state, 4, &settings).await);

    backend.shutdown();
    state.write().await.persistence = None;
    drop(state);
    let _ = std::fs::remove_dir_all(&dir);
}

/// `pkg/app.py` differs between `main` and `feature`: the transport ships each
/// UNIQUE blob named by its (shared) path, so one batch carries two entries
/// under one name. Entries are content-keyed, so this commits and each branch
/// reaches exactly its own version (the multi-ref first ingest of any real
/// repository).
async fn one_path_diverged_across_refs() {
    const FEATURE_APP: &[u8] = b"def run():\n    return 2\n";
    let (dir, _, backend, state) = durable_graph("eg-repoindex-diverged").await;
    let diverged = scope(
        vec![
            member("main", "pkg/app.py", APP),
            member("feature", "pkg/app.py", FEATURE_APP),
        ],
        Vec::new(),
    );
    let blobs: [(&str, &[u8]); 2] = [("pkg/app.py", APP), ("pkg/app.py", FEATURE_APP)];
    let result = index(&state, 2, &blobs, diverged).await;
    let outcomes = result["file_outcomes"].as_array().unwrap();
    assert_eq!(outcomes.len(), 2, "one outcome per submitted blob");
    let main = node_id(&result, "Branch", "ref_name", "main");
    let feature = node_id(&result, "Branch", "ref_name", "feature");
    let main_app = file_version(&result, "pkg/app.py", APP);
    let feature_app = file_version(&result, "pkg/app.py", FEATURE_APP);
    assert_ne!(main_app, feature_app);
    assert!(has_edge(&state, 3, &main, &main_app).await);
    assert!(has_edge(&state, 4, &feature, &feature_app).await);
    assert!(!has_edge(&state, 5, &main, &feature_app).await);
    assert!(!has_edge(&state, 6, &feature, &main_app).await);

    backend.shutdown();
    state.write().await.persistence = None;
    drop(state);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A 0-byte `pkg/__init__.py` (every Python package has one) is valid input:
/// it parses to no symbols but still projects its `:Blob` and `:FileVersion`,
/// and the batch commits durably.
async fn empty_source_file() {
    let (dir, _, backend, state) = durable_graph("eg-repoindex-empty").await;
    let with_empty = scope(
        vec![
            member("main", "pkg/__init__.py", b""),
            member("main", "pkg/util.py", UTIL),
        ],
        Vec::new(),
    );
    let blobs: [(&str, &[u8]); 2] = [("pkg/__init__.py", b""), ("pkg/util.py", UTIL)];
    let result = index(&state, 2, &blobs, with_empty).await;
    let main = node_id(&result, "Branch", "ref_name", "main");
    let init = file_version(&result, "pkg/__init__.py", b"");
    let empty_blob = format!("blob:{}", digest(b""));
    assert!(has_edge(&state, 3, &main, &init).await);
    assert!(has_edge(&state, 4, &init, &empty_blob).await);

    backend.shutdown();
    state.write().await.persistence = None;
    drop(state);
    let _ = std::fs::remove_dir_all(&dir);
}
