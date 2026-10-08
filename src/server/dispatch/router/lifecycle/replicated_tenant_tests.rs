//! Real native serialization/application checks for opaque creator identities.
//! No listeners or cluster processes: both graph and policy use temporary Redb.

use std::path::Path;
use std::sync::Arc;

use eg_types::identity::*;
use tokio::sync::RwLock;

use crate::isolation::{AccessLevel, AgentIdentity, AgentRole, IsolationLayer};
use crate::protocol::{GraphType, Method};
use crate::raft::{
    NativeMutationCommand, RaftMutationContext, RaftMutationTiming, RaftRequest, ReplicatedMutation,
};
use crate::server::access::CarrierAuthority;
use crate::server::auth::VerifiedRequestContext;
use crate::server::dispatch::test_support::{send, verified};
use crate::server::dispatch::{apply_replicated_native, authoritative_now_ms};
use crate::server::persistence::redb_backend::RedbBackend;
use crate::server::ServerState;

const GRAPH: &str = "tenant__subjectbinding__default";
const SECRET: &str = "synthetic-replicated-tenant-test-secret";

fn open_layer(dir: &Path) -> IsolationLayer {
    let authority = crate::store_authority::process_authority();
    IsolationLayer::with_persist_dir(
        dir,
        authority.as_ref(),
        authority.principal(),
        &authority.proof(),
    )
    .expect("open temporary durable identity policy")
}

fn apply_identity(layer: &mut IsolationLayer, op: IdentityOp, principal: &str, scope: &str) {
    let stamp = IdentityStamp::for_actor(IdentityActor {
        principal_id: principal.to_string(),
        delegated: false,
        scopes: [scope.to_string()].into(),
    });
    layer
        .try_apply_identity(
            &op,
            &stamp,
            authoritative_now_ms(),
            &eg_capabilities::scopes::ScopeRegistry,
        )
        .expect("fixture identity operation");
}

fn create_user(layer: &mut IsolationLayer, principal: &str, index: usize) {
    apply_identity(
        layer,
        IdentityOp::User(UserOp::Create {
            request: CreateUserRequest {
                username: format!("subject-{index}"),
                kind: UserKind::Human,
                principal_id: Some(principal.to_string()),
                display_name: None,
                email: None,
                roles: Default::default(),
                groups: Default::default(),
                password: Secret::default(),
                must_change: false,
            },
        }),
        BOOTSTRAP_PRINCIPAL,
        IDENTITY_ADMIN_SCOPE,
    );
}

fn durable_state(dir: &Path, principals: &[&str]) -> Arc<RwLock<ServerState>> {
    let mut layer = open_layer(dir);
    layer
        .try_bootstrap_system_identity(AgentIdentity {
            agent_id: "synthetic-engine-root".to_string(),
            role: AgentRole::System,
            teams: Vec::new(),
            roles: Vec::new(),
        })
        .unwrap();
    apply_identity(
        &mut layer,
        IdentityOp::Config(ConfigOp::Initialize {
            request: InitializeRequest {
                mode: AuthMode::None,
                admin_username: None,
                admin_password: Secret::default(),
            },
        }),
        "synthetic-broker",
        IDENTITY_AUTHENTICATE_SCOPE,
    );
    for (index, principal) in principals.iter().enumerate() {
        create_user(&mut layer, principal, index);
    }
    let mut state = ServerState::new_for_test(SECRET, layer);
    state.persistence = Some(Arc::new(
        RedbBackend::open_with_shards(dir.to_string_lossy().into_owned(), 64, 1).unwrap(),
    ));
    Arc::new(RwLock::new(state))
}

fn create_graph() -> Method {
    Method::CreateGraph {
        graph_name: GRAPH.to_string(),
        graph_type: GraphType::Agent,
    }
}

fn native_request(context: &VerifiedRequestContext) -> RaftRequest {
    let carrier = CarrierAuthority::from_verified(context).unwrap();
    let now = authoritative_now_ms();
    let authority = RaftMutationContext::from_verified_request(
        "synthetic-replicated-tenant-batch".to_string(),
        11,
        context.attempt_nonce(),
        carrier.tenant_scope(),
        carrier.actor_scope().to_string(),
        false,
        RaftMutationTiming {
            placement_epoch: 0,
            fencing_token: None,
            created_at_ms: now,
        },
    )
    .unwrap();
    RaftRequest {
        graph_fname: crate::persist::sanitize(GRAPH),
        graph_name: GRAPH.to_string(),
        graph_type: GraphType::Agent,
        command: ReplicatedMutation::Native {
            command: NativeMutationCommand::from_public_method(create_graph(), SECRET).unwrap(),
        },
        committed_at_ms: now,
        mutation: authority,
    }
}

async fn apply_serialized(state: &Arc<RwLock<ServerState>>, context: &VerifiedRequestContext) {
    let request = native_request(context);
    request.validate().unwrap();
    let wire = rmp_serde::to_vec_named(&request).unwrap();
    let decoded: RaftRequest = rmp_serde::from_slice(&wire).unwrap();
    decoded.validate().unwrap();
    assert_eq!(
        decoded.mutation.principal_fingerprint,
        CarrierAuthority::from_verified(context)
            .unwrap()
            .actor_scope()
    );
    let ReplicatedMutation::Native { command } = decoded.command else {
        panic!("expected native command");
    };
    let method = command.open_public_method(SECRET).unwrap().unwrap();
    let response = apply_replicated_native(
        state,
        decoded.graph_name,
        decoded.mutation.request_id,
        decoded.committed_at_ms,
        &decoded.mutation,
        method,
    )
    .await;
    assert!(response.error.is_none(), "{:?}", response.error);
    assert!(state.read().await.registry.exists(GRAPH));
}

fn assert_access(layer: &IsolationLayer, principal: &str, allowed: bool) {
    for action in [AccessLevel::Read, AccessLevel::Write] {
        assert_eq!(
            layer.check_access(principal, GRAPH, GraphType::Agent, None, action),
            allowed
        );
    }
    let roles = &layer.rbac().identity_store().user(principal).unwrap().roles;
    assert_eq!(roles.contains("tenant:subjectbinding"), allowed);
}

fn refresh(layer: &mut IsolationLayer, principal: &str) {
    apply_identity(
        layer,
        IdentityOp::User(UserOp::Update {
            request: UserUpdate {
                principal_id: principal.to_string(),
                username: None,
                display_name: Some("synthetic refreshed subject".to_string()),
                email: None,
            },
        }),
        BOOTSTRAP_PRINCIPAL,
        IDENTITY_ADMIN_SCOPE,
    );
}

async fn check_subjects(principal: &str, alias: Option<&str>, replicated: bool) {
    let dir = tempfile::tempdir().unwrap();
    let mut principals = vec![principal];
    principals.extend(alias);
    let state = durable_state(dir.path(), &principals);
    let context = verified(principal, &["graph:admin"], &[]);
    let context = VerifiedRequestContext::from_verified_claims_with_nonce(
        context.claims().clone(),
        context.idempotency_key().to_string(),
        Some(eg_types::contract::Nonce::from_bytes([7; 32])),
    );
    for subject in &principals {
        assert_access(&state.read().await.isolation, subject, false);
    }
    let before = serde_json::to_value(state.read().await.isolation.rbac()).unwrap();
    if replicated {
        apply_serialized(&state, &context).await;
        assert_eq!(
            serde_json::to_value(state.read().await.isolation.rbac()).unwrap(),
            before
        );
    } else {
        let response = send(&state, context, create_graph()).await;
        assert!(response.error.is_none(), "{:?}", response.error);
    }
    for subject in &principals {
        assert_access(
            &state.read().await.isolation,
            subject,
            !replicated && *subject == principal,
        );
    }
    state.write().await.persistence.take().unwrap().shutdown();
    drop(state);
    let mut reopened = open_layer(dir.path());
    if replicated {
        assert_eq!(serde_json::to_value(reopened.rbac()).unwrap(), before);
    }
    for subject in principals {
        assert_access(&reopened, subject, !replicated && subject == principal);
        refresh(&mut reopened, subject);
        assert_access(&reopened, subject, !replicated && subject == principal);
    }
}

#[tokio::test]
async fn replicated_creator_never_binds_a_different_managed_fingerprint_subject() {
    let principal = "usr:synthetic-creator";
    let alias = verified(principal, &["graph:admin"], &[]).principal_persistence_id();
    check_subjects(principal, Some(&alias), true).await;
}

#[tokio::test]
async fn replicated_fingerprint_self_identity_also_withholds_unproven_binding() {
    let principal = format!("principal:sha256:{}", "a".repeat(64));
    check_subjects(&principal, None, true).await;
}

#[tokio::test]
async fn local_verified_creator_keeps_its_managed_binding_after_reopen_and_refresh() {
    let principal = "usr:synthetic-local-creator";
    let alias = verified(principal, &["graph:admin"], &[]).principal_persistence_id();
    check_subjects(principal, Some(&alias), false).await;
}
