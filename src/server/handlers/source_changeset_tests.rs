//! Served EG-SCM changesets through the real signed dispatch surface: blobs
//! uploaded through the CAS, changesets submitted, landed and reverted on the
//! branch chain held in the Agent Library control owner.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use serde::de::DeserializeOwned;
use tokio::sync::RwLock;

use eg_types::source_changeset::{
    ChangedPath, ChangesetState, ChangesetSubmission, GateOutcome, PathOp, SourceBranchHead,
    SourceChangesetOp, SourceChangesetRecord,
};

use crate::protocol::{Method, Response, ResultPayload};
use crate::server::auth::dispatch_test_on_heap;
use crate::server::state::ServerState;

const SECRET: &str = "source-changeset-served-fixture-secret"; // sanitizer:ignore
const TENANT: &str = "tenant-shared"; // the tenant auth::request_context_policy expects under cfg(test)
const AUTHOR: &str = "scm-author";

struct Served {
    _dir: tempfile::TempDir,
    state: Arc<RwLock<ServerState>>,
}

fn next_id() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

impl Served {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let mut state = ServerState::new_for_test(SECRET, ServerState::test_isolation(AUTHOR));
        state.persist_dir = Some(dir.path().to_string_lossy().into_owned());
        let blob_dir = dir.path().join("blob");
        std::fs::create_dir_all(&blob_dir).unwrap();
        state.blob = Some(crate::server::handlers::blob::cursors_for_test(
            blob_dir.to_str().unwrap(),
        ));
        Self {
            _dir: dir,
            state: Arc::new(RwLock::new(state)),
        }
    }

    async fn call(&self, method: Method) -> Response {
        let id = next_id();
        let request = crate::server::auth::scoped_test_request(
            SECRET,
            id,
            method,
            crate::server::auth::ScopedTestCaller {
                principal: AUTHOR,
                tenant: TENANT,
                scopes: &["*"],
                nonce: &format!("scm-served-nonce-{id}"),
                idempotency_key: &format!("scm-served-key-{id}"),
            },
        );
        dispatch_test_on_heap(&self.state, request).await
    }

    async fn op(&self, op: SourceChangesetOp) -> Response {
        self.call(Method::SourceChangeset { op: Box::new(op) }).await
    }

    async fn upload(&self, bytes: &[u8]) -> String {
        let cursor: u64 = ok(
            "BlobBegin",
            self.call(Method::BlobBegin { chunk_size: 1 << 20 }).await,
        );
        let _: serde_json::Value = ok(
            "BlobChunkPut",
            self.call(Method::BlobChunkPut {
                cursor,
                data: bytes.to_vec(),
            })
            .await,
        );
        ok("BlobCommit", self.call(Method::BlobCommit { cursor }).await)
    }

    async fn submit(&self, parents: Vec<String>, paths: Vec<ChangedPath>) -> Response {
        let submission = ChangesetSubmission {
            tenant_id: TENANT.into(),
            repo: "repo".into(),
            branch: "main".into(),
            parents,
            message: "feat: change\n\nSpec: EG-SCM-R002".into(),
            changed_paths: paths,
            run_trace_id: None,
        };
        self.op(SourceChangesetOp::Submit {
            submission: Box::new(submission),
        })
        .await
    }

    async fn land(&self, id: &str) -> Response {
        self.op(SourceChangesetOp::Land {
            tenant_id: TENANT.into(),
            changeset_id: id.into(),
        })
        .await
    }

    async fn head(&self) -> SourceBranchHead {
        ok(
            "Head",
            self.op(SourceChangesetOp::Head {
                tenant_id: TENANT.into(),
                repo: "repo".into(),
                branch: "main".into(),
            })
            .await,
        )
    }
}

fn ok<T: DeserializeOwned>(label: &str, response: Response) -> T {
    assert!(response.error.is_none(), "{label} was refused: {response:?}");
    let value = match response.result {
        Some(ResultPayload::Raw(bytes)) => {
            return rmp_serde::from_slice(&bytes)
                .unwrap_or_else(|error| panic!("{label} result does not decode: {error}"));
        }
        Some(ResultPayload::Count(count)) => serde_json::json!(count),
        Some(ResultPayload::String(text)) => serde_json::json!(text),
        Some(ResultPayload::Bool(flag)) => serde_json::json!(flag),
        Some(ResultPayload::Json(value)) => value,
        other => panic!("{label} answered an unexpected payload shape: {other:?}"),
    };
    serde_json::from_value(value)
        .unwrap_or_else(|error| panic!("{label} result does not decode: {error}"))
}

fn refused(label: &str, response: Response, code: &str) {
    let text = format!("{:?} {:?}", response.error, response.error_detail);
    assert!(response.error.is_some(), "{label} was accepted: {response:?}");
    assert!(text.contains(code), "{label} refused with {text}, want {code}");
}

fn add(path: &str, blob: &str) -> ChangedPath {
    ChangedPath {
        path: path.into(),
        op: PathOp::Add,
        old_blob: None,
        new_blob: Some(blob.into()),
        mode: 0o100644,
    }
}

fn modify(path: &str, old: &str, new: &str) -> ChangedPath {
    ChangedPath {
        path: path.into(),
        op: PathOp::Modify,
        old_blob: Some(old.into()),
        new_blob: Some(new.into()),
        mode: 0o100644,
    }
}

/// Submit and land; return the landed record.
async fn commit(served: &Served, parents: Vec<String>, paths: Vec<ChangedPath>) -> SourceChangesetRecord {
    let proposed: SourceChangesetRecord = ok("Submit", served.submit(parents, paths).await);
    ok("Land", served.land(&proposed.changeset.id).await)
}

// spec: EG-SCM-R002
#[tokio::test(flavor = "multi_thread")]
async fn changeset_stores_only_changed_paths_and_appends_to_the_chain() {
    let served = Served::new();
    let (a, b, c) = (
        served.upload(b"a1").await,
        served.upload(b"b1").await,
        served.upload(b"c1").await,
    );
    let root = commit(&served, vec![], vec![add("a", &a), add("b", &b), add("c", &c)]).await;
    assert_eq!(root.state, ChangesetState::Landed);
    assert_eq!(root.landed_sequence, Some(1));
    let (a2, b2) = (served.upload(b"a2").await, served.upload(b"b2").await);
    let two = commit(
        &served,
        vec![root.changeset.id.clone()],
        vec![modify("a", &a, &a2), modify("b", &b, &b2)],
    )
    .await;
    assert_eq!(two.changeset.changed_paths.len(), 2);
    assert_eq!(two.changeset.parents, [root.changeset.id.clone()]);
    assert_eq!(two.changeset.trailers.spec_ids, ["EG-SCM-R002"]);
    let head = served.head().await;
    assert_eq!((head.sequence, head.head), (2, Some(two.changeset.id.clone())));
    let read: Option<SourceChangesetRecord> = ok(
        "Get",
        served
            .op(SourceChangesetOp::Get {
                tenant_id: TENANT.into(),
                changeset_id: two.changeset.id.clone(),
            })
            .await,
    );
    assert_eq!(read.as_ref(), Some(&two));

    // Refusals leave the chain unchanged.
    refused("empty", served.submit(vec![two.changeset.id.clone()], vec![]).await, "SOURCE_EMPTY_CHANGESET");
    let unknown = "0".repeat(64);
    refused(
        "unreadable blob",
        served.submit(vec![two.changeset.id.clone()], vec![add("d", &unknown)]).await,
        "SOURCE_BLOB_UNREADABLE",
    );
    let d = served.upload(b"d1").await;
    let off_chain: SourceChangesetRecord =
        ok("Submit", served.submit(vec!["not-on-chain".into()], vec![add("d", &d)]).await);
    refused("stale", served.land(&off_chain.changeset.id).await, "SOURCE_STALE_BASE");
    assert_eq!(served.head().await.sequence, 2);
}

// EG-SCM-R003 (partial: overlap refuses; the server rebase is not built yet)
#[tokio::test(flavor = "multi_thread")]
async fn disjoint_writers_land_and_overlap_conflicts() {
    let served = Served::new();
    let (a, b) = (served.upload(b"a1").await, served.upload(b"b1").await);
    let root = commit(&served, vec![], vec![add("a", &a), add("b", &b)]).await;
    let base = vec![root.changeset.id.clone()];
    let (a2, b2, a3) = (
        served.upload(b"a2").await,
        served.upload(b"b2").await,
        served.upload(b"a3").await,
    );
    let w1: SourceChangesetRecord = ok("w1", served.submit(base.clone(), vec![modify("a", &a, &a2)]).await);
    let w2: SourceChangesetRecord = ok("w2", served.submit(base.clone(), vec![modify("b", &b, &b2)]).await);
    let w3: SourceChangesetRecord = ok("w3", served.submit(base, vec![modify("a", &a, &a3)]).await);
    let landed: SourceChangesetRecord = ok("land w1", served.land(&w1.changeset.id).await);
    assert_eq!(landed.landed_sequence, Some(2));
    // w2 was written against the old head but touches a disjoint path.
    let landed: SourceChangesetRecord = ok("land w2", served.land(&w2.changeset.id).await);
    assert_eq!(landed.landed_sequence, Some(3));
    // w3 overlaps w1 on `a`: a typed conflict naming every path.
    refused("overlap", served.land(&w3.changeset.id).await, "SOURCE_CONFLICT: a");
    assert_eq!(served.head().await.sequence, 3);
}

// spec: EG-SCM-R008
#[tokio::test(flavor = "multi_thread")]
async fn failed_verdict_blocks_landing_and_verdicts_are_sealed() {
    let served = Served::new();
    let a = served.upload(b"a1").await;
    let proposed: SourceChangesetRecord = ok("Submit", served.submit(vec![], vec![add("a", &a)]).await);
    let verdict = |gate: &str, verdict| SourceChangesetOp::RecordVerdict {
        tenant_id: TENANT.into(),
        changeset_id: proposed.changeset.id.clone(),
        gate_id: gate.into(),
        input_digest: proposed.changeset.id.clone(),
        verdict,
        evidence_digest: "evidence".into(),
        duration_ms: 7,
    };
    let sealed: SourceChangesetRecord = ok("pass", served.op(verdict("fmt", GateOutcome::Succeeded)).await);
    assert_eq!(sealed.verdicts.len(), 1);
    refused("overwrite", served.op(verdict("fmt", GateOutcome::Failed)).await, "SOURCE_VERDICT_SEALED");
    let mut wrong = verdict("types", GateOutcome::Succeeded);
    if let SourceChangesetOp::RecordVerdict { input_digest, .. } = &mut wrong {
        *input_digest = "other".into();
    }
    refused("digest", served.op(wrong).await, "SOURCE_INVALID_NAME");
    let _: SourceChangesetRecord = ok("fail", served.op(verdict("tests", GateOutcome::Failed)).await);
    refused("land", served.land(&proposed.changeset.id).await, "SOURCE_GATE_FAILED: tests");
    assert_eq!(served.head().await.sequence, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn revert_lands_the_inverse_and_refuses_dependents() {
    let served = Served::new();
    let (a, b) = (served.upload(b"a1").await, served.upload(b"b1").await);
    let root = commit(&served, vec![], vec![add("a", &a)]).await;
    let a2 = served.upload(b"a2").await;
    let edit = commit(&served, vec![root.changeset.id.clone()], vec![modify("a", &a, &a2)]).await;
    let other = commit(&served, vec![edit.changeset.id.clone()], vec![add("b", &b)]).await;
    let revert = |id: &str| SourceChangesetOp::Revert {
        tenant_id: TENANT.into(),
        changeset_id: id.into(),
        message: "revert: undo".into(),
    };
    refused("dependent", served.op(revert(&root.changeset.id)).await, "SOURCE_REVERT_DEPENDENT");
    let undone: SourceChangesetRecord = ok("revert", served.op(revert(&edit.changeset.id)).await);
    assert_eq!(undone.revert_of.as_deref(), Some(edit.changeset.id.as_str()));
    assert_eq!(undone.changeset.parents, [other.changeset.id.clone()]);
    assert_eq!(undone.changeset.changed_paths[0].new_blob.as_deref(), Some(a.as_str()));
    assert_eq!(served.head().await.head, Some(undone.changeset.id));
}
