//! The served endpoint: `Method::SqlSourceBatch` through the authenticated
//! request boundary, the capability scope gate, request preflight, the durable
//! mutation replay ledger and the data-plane router -- not the handler alone.

use super::*;
use crate::protocol::{Method, Request};
use crate::server::auth::{
    compute_verified_envelope_token, dispatch_test_on_heap, VerifiedEnvelopeParams,
};
use eg_types::acl::RequestContextClaims;
use eg_types::contract::RecordBytes;

const SECRET: &str = "sql-source-served-secret";
const WRITER: &str = "bob";

struct Served {
    fixture: Fixture,
    state: Arc<RwLock<ServerState>>,
}

impl Served {
    fn new() -> Self {
        let fixture = Fixture::new();
        fixture.grant_insert(WRITER);
        let mut state = ServerState::new_for_test(SECRET, ServerState::test_isolation(WRITER));
        state.persist_dir = Some(fixture.directory.to_str().unwrap().into());
        Self {
            fixture,
            state: Arc::new(RwLock::new(state)),
        }
    }

    fn signed(&self, attempt: Attempt<'_>, batch: SqlSourceBatchRequest) -> Request {
        let mut request = Request {
            id: attempt.id,
            graph: "__commons__".to_string(),
            auth_token: String::new(),
            agent_id: Some(WRITER.to_string()),
            method: Method::SqlSourceBatch { batch },
        };
        let context = RequestContextClaims {
            principal: WRITER.to_string(),
            tenant: "tenant-a".to_string(),
            audience: "epistemic-graph-test".to_string(),
            agent_id: WRITER.to_string(),
            roles: Vec::new(),
            scopes: attempt
                .scopes
                .iter()
                .map(|scope| scope.to_string())
                .collect(),
            policy_version: "policy-test".to_string(),
            delegation: Vec::new(),
            node: None,
            priority: None,
        };
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        request.auth_token = compute_verified_envelope_token(
            SECRET,
            &request,
            &VerifiedEnvelopeParams {
                context: &context,
                timestamp,
                nonce: attempt.nonce,
                idempotency_key: "sql-source-operation",
            },
        );
        request
    }

    async fn send(&self, request: Request) -> Response {
        dispatch_test_on_heap(&self.state, request).await
    }

    fn rows(&self) -> usize {
        self.fixture.store().scan("issues").unwrap().len()
    }
}

struct Attempt<'a> {
    id: u64,
    nonce: &'a str,
    scopes: &'a [&'a str],
}

const WRITE: &[&str] = &["kg:write"];

fn committed(response: &Response) -> SqlSourceBatchResult {
    assert!(response.error.is_none(), "{:?}", response.error);
    let Some(ResultPayload::Raw(bytes)) = &response.result else {
        panic!("SQL source result must be raw MessagePack")
    };
    eg_storage::decode_ledger_record(bytes).unwrap()
}

fn refusal(response: &Response) -> &str {
    assert!(response.result.is_none());
    response.error.as_deref().expect("request must be refused")
}

#[tokio::test]
async fn served_batch_commits_once_and_replays_its_durable_result() {
    let served = Served::new();
    let batch = served.fixture.request(WRITER);
    let attempt = |id, nonce| Attempt {
        id,
        nonce,
        scopes: WRITE,
    };
    let first = served.signed(attempt(1, "served-a"), batch.clone());
    let result = committed(&served.send(first.clone()).await);
    assert_eq!(result.affected_count, 1);
    assert_eq!(result.canonical_digests, batch.canonical_digests().unwrap());
    assert_eq!(served.rows(), 1);

    // The identical signed attempt is a replay, not a second publication.
    refusal(&served.send(first).await);
    assert_eq!(served.rows(), 1);

    // A fresh attempt of the same operation returns the stored result.
    let retry = served.signed(attempt(2, "served-b"), batch.clone());
    assert_eq!(committed(&served.send(retry).await), result);
    assert_eq!(served.rows(), 1);

    // The operation key cannot be reused for different content.
    let altered = change(&batch, |batch| {
        batch.mapping_descriptor.content = RecordBytes::new(b"another mapping".to_vec()).unwrap();
    });
    let conflict = served
        .send(served.signed(attempt(3, "served-c"), altered))
        .await;
    assert!(refusal(&conflict).contains("IDEMPOTENCY_CONFLICT"));
    assert_eq!(served.rows(), 1);
}

#[tokio::test]
async fn served_batch_is_refused_without_write_authority() {
    let served = Served::new();
    let batch = served.fixture.request(WRITER);
    let read_only = Attempt {
        id: 1,
        nonce: "served-read",
        scopes: &["kg:read"],
    };
    let denied = served.send(served.signed(read_only, batch.clone())).await;
    assert!(refusal(&denied).contains("lacks required scope 'query:sql'"));

    // The capability action admits the request at the boundary; publication
    // still requires the coarse write verb, like every other owner mutation.
    let action_only = Attempt {
        id: 2,
        nonce: "served-action",
        scopes: &["query:sql"],
    };
    let denied = served.send(served.signed(action_only, batch)).await;
    assert!(refusal(&denied).contains("requires kg:write"));
    assert_eq!(served.rows(), 0);
}

#[tokio::test]
async fn served_batch_requires_the_existing_insert_grant() {
    let served = Served::new();
    let foreign = change(&served.fixture.request(WRITER), |batch| {
        batch.table = id("not_granted");
    });
    let attempt = Attempt {
        id: 1,
        nonce: "served-grant",
        scopes: WRITE,
    };
    let denied = served.send(served.signed(attempt, foreign)).await;
    assert!(refusal(&denied).contains("ACCESS_DENIED"));
    assert_eq!(served.rows(), 0);
}

#[test]
fn source_batch_is_a_local_only_cluster_route() {
    let fixture = Fixture::new();
    let method = Method::SqlSourceBatch {
        batch: fixture.request(WRITER),
    };
    assert_eq!(
        crate::server::mutation::cluster_mutation_route(&method),
        crate::server::mutation::ClusterMutationRoute::LocalOnly
    );
}

/// A live `MultiRaft` placement authority: the request is refused at the
/// consensus boundary with the typed local-only error, before any proposal, and
/// the SQL owner is never opened for publication.
#[cfg(feature = "raft")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clustered_placement_refuses_source_batch_before_publication() {
    let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
    let served = Served::new();
    let raft_dir = served.fixture.directory.join("raft-node");
    std::fs::create_dir_all(&raft_dir).unwrap();
    let backend: Arc<dyn crate::server::persistence::PersistenceBackend> = Arc::new(
        crate::server::persistence::redb_backend::RedbBackend::open(
            raft_dir.to_str().unwrap().to_string(),
            4096,
        )
        .unwrap(),
    );
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let multi = crate::raft::multi::MultiRaft::start(
        1,
        format!("127.0.0.1:{port}"),
        backend,
        crate::raft::AppCtx {
            state: served.state.clone(),
            router: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(
        served.state.read().await.placement_authority(),
        crate::server::state::PlacementAuthorityKind::MultiRaft
    );
    let attempt = Attempt {
        id: 1,
        nonce: "served-cluster",
        scopes: WRITE,
    };
    let response = served
        .send(served.signed(attempt, served.fixture.request(WRITER)))
        .await;
    assert_eq!(
        refusal(&response),
        crate::server::mutation::LOCAL_ONLY_CLUSTER_REFUSAL
    );
    assert_eq!(served.rows(), 0);
    multi.shutdown().await;
}
