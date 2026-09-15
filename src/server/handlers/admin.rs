//! M3 catalog-driven resharding ADMIN RPC (CONCEPT:EG-KG.backend.m3-admin-dispatch).
//!
//! The wire surface that DRIVES the M3 ops the engine already has the building blocks for:
//! online single-node resharding (CONCEPT:EG-KG.backend.catalog-shard-resolve `RedbBackend::reshard_graph`), the durable
//! tenant catalog (CONCEPT:EG-KG.sharding.empty-catalog-routing `RedbBackend::catalog`), the rebalancing planner
//! (CONCEPT:EG-KG.sharding.even-load-rebalance `rebalance::plan_rebalance`), and its execution (CONCEPT:EG-KG.backend.r3-plan-execution
//! `RedbBackend::rebalance_execute`). These CALL the existing persistence APIs — they do not
//! reimplement them.
//!
//! All are durable-redb-only. The module is always declared so the dispatch routing chain is
//! identical across builds; in a build WITHOUT `redb` the catalog/reshard/planner don't
//! exist, so the handler returns a clean "not available in this build" error.

#[cfg(not(feature = "redb"))]
use crate::protocol::{Method, Response};
#[cfg(not(feature = "redb"))]
use crate::server::state::ServerState;
#[cfg(not(feature = "redb"))]
use eg_types::contract::Nonce;
#[cfg(not(feature = "redb"))]
use std::sync::Arc;
#[cfg(not(feature = "redb"))]
use tokio::sync::RwLock;

#[cfg(feature = "redb")]
mod agent;
#[cfg(feature = "redb")]
mod backup;
#[cfg(feature = "redb")]
mod cluster;
#[cfg(feature = "redb")]
mod saga;

#[cfg(all(test, feature = "redb"))]
pub(crate) use agent::{bind_agent_library_context, bind_agent_library_draft};
#[cfg(feature = "redb")]
pub(crate) use agent::{
    handle_agent_component, handle_agent_graph, handle_agent_library, handle_agent_template,
};
#[cfg(all(test, feature = "redb"))]
pub(crate) use backup::backup_bundle_name;
#[cfg(feature = "redb")]
pub(crate) use cluster::try_handle;
#[cfg(any(feature = "compute-dist", feature = "matview"))]
pub(crate) use saga::begin_admin_saga;
#[cfg(feature = "redb")]
pub(crate) use saga::{
    begin_authenticated_admin_saga, begin_named_admin_saga_with_nonce,
    begin_named_admin_saga_with_private_payload_and_nonce, current_admin_saga_authority,
    finish_admin_saga, resume_named_admin_saga, scope_admin_saga_authority, AdminSaga,
    AdminSagaPayload,
};

/// Non-redb build: every admin method returns a clean "not available" error.
#[cfg(not(feature = "redb"))]
pub(crate) async fn try_handle(
    _state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    _caller: Option<&str>,
    _attempt_nonce: Option<Nonce>,
    method: Method,
) -> Result<Response, Method> {
    match method {
        Method::Reshard { .. }
        | Method::CatalogAssign { .. }
        | Method::CatalogReassign { .. }
        | Method::CatalogRemove { .. }
        | Method::CatalogList
        | Method::RebalancePlan { .. }
        | Method::RebalanceExecute { .. }
        | Method::Backup { .. }
        | Method::Restore { .. } => Ok(Response::err(
            req_id,
            "M3 resharding admin is not available in this build (requires the `redb` feature)",
        )),
        other => Err(other),
    }
}

#[cfg(all(test, feature = "redb"))]
mod security_tests {
    use super::backup_bundle_name;

    #[test]
    fn backup_bundle_names_are_logical_not_paths() {
        for valid in ["scheduled-001", "snapshot_2", "release.3"] {
            assert_eq!(backup_bundle_name(valid).unwrap(), valid);
        }
        for invalid in [
            "",
            ".hidden",
            "../snapshot",
            "nested/snapshot",
            "C:\\snapshot",
            "snapshot\n",
        ] {
            assert!(backup_bundle_name(invalid).is_err(), "accepted {invalid:?}");
        }
    }
}

#[cfg(all(test, feature = "redb"))]
mod agent_library_security_tests {
    use super::{backup_bundle_name, bind_agent_library_context, bind_agent_library_draft};
    use crate::acl::RequestContextClaims;
    use crate::protocol::{Method, Request, ResultPayload};
    use crate::server::authority_context::VerifiedRequestContext;
    use crate::server::persistence::durable_stores::BundledStoreSource;
    use eg_types::contract::Nonce;
    use eg_types::{
        AgentLibraryEntryDraft, AgentLibraryMutationContext, AgentLibraryMutationKind,
        AgentLibraryOp, AgentLibraryPublishRequest, AgentLibraryRetireRequest,
        AgentLibraryStatusRequest,
    };
    use redb::{ReadableDatabase, ReadableTableMetadata, TableDefinition};
    use sha2::{Digest, Sha256};
    use std::sync::Arc;
    use tokio::sync::RwLock;

    const LEDGER_BATCHES: TableDefinition<'static, (&str, &str), &[u8]> =
        TableDefinition::new("ledger_batches");
    const LEDGER_OUTBOX: TableDefinition<'static, (&str, &str, u32), &[u8]> =
        TableDefinition::new("ledger_outbox");
    // The durable table names are `eg_storage::tables`' own (they are
    // `pub(crate)` there, so this owner-inspection test restates them). The
    // typed replay receipt lives in `mutation_replay_operations` -- the
    // `mutation_` prefix is part of the name, not a namespace this test may
    // drop, as `server::persistence::agent_library`'s own reader shows.
    const REPLAY_OPERATIONS: TableDefinition<'static, (&str, &str), &[u8]> =
        TableDefinition::new("mutation_replay_operations");

    fn digest(byte: char) -> String {
        format!("sha256:{}", byte.to_string().repeat(64))
    }

    fn forged_context(
        _store: &crate::server::persistence::agent_library::AgentLibraryStore,
        tenant_id: &str,
    ) -> AgentLibraryMutationContext {
        AgentLibraryMutationContext {
            request_id: 1,
            principal: format!("principal:sha256:{}", "f".repeat(64)),
            caller_principal: format!("principal:sha256:{}", "e".repeat(64)),
            attempt_nonce: Nonce::from_bytes([0xf; 32]),
            tenant_id: tenant_id.to_string(),
            actor_scope: "body-forged-scope".to_string(),
            purpose_id: "body-forged-purpose".to_string(),
            policy_revision: "body-forged-policy".to_string(),
            policy_digest: digest('f'),
            policy_decision_id: "body-forged-decision".to_string(),
            idempotency_key: "body-forged-key".to_string(),
            expected_revision: Some(0),
            trace_id: Some("body-forged-trace".to_string()),
            created_at_ms: 1,
        }
    }

    /// `definition`, with every component it pins actually published.
    ///
    /// A publish now RESOLVES each pinned component inside its write
    /// transaction, so a route test that expects the publish to REACH the store
    /// has to seed them; a test that expects a refusal before the store does
    /// not.
    fn seeded_definition(
        store: &crate::server::persistence::agent_library::AgentLibraryStore,
        tenant_id: &str,
    ) -> AgentLibraryEntryDraft {
        let mut draft = definition(tenant_id);
        crate::server::persistence::agent_component::seed_draft_components_for_test(
            store, &mut draft, 50,
        );
        draft
    }

    fn definition(tenant_id: &str) -> AgentLibraryEntryDraft {
        AgentLibraryEntryDraft {
            agent_id: "agent-a".to_string(),
            package_id: "package-a".to_string(),
            version: "1.0.0".to_string(),
            role: "researcher".to_string(),
            role_digest: digest('1'),
            system_prompt: eg_types::agent_component::ComponentDependency {
                component_id: "prompt:a".to_string(),
                kind: eg_types::agent_component::AgentComponentKind::SystemPrompt,
                definition_digest: digest('2'),
            },
            tools: vec![eg_types::agent_component::ComponentDependency {
                component_id: "tool:search".to_string(),
                kind: eg_types::agent_component::AgentComponentKind::Tool,
                definition_digest: digest('3'),
            }],
            skills: vec![eg_types::agent_component::ComponentDependency {
                component_id: "skill:research".to_string(),
                kind: eg_types::agent_component::AgentComponentKind::Skill,
                definition_digest: digest('4'),
            }],
            model_profile: eg_types::agent_component::ComponentDependency {
                component_id: "model:default".to_string(),
                kind: eg_types::agent_component::AgentComponentKind::ModelProfile,
                definition_digest: digest('5'),
            },
            model_identity: "model:default".to_string(),
            ontologies: vec![eg_types::agent_component::ComponentDependency {
                component_id: "ontology:core".to_string(),
                kind: eg_types::agent_component::AgentComponentKind::Ontology,
                definition_digest: digest('6'),
            }],
            tenant_id: tenant_id.to_string(),
            actor_scope: "definition:builder-a".to_string(),
            purpose_id: "agent-library:definition".to_string(),
            policy_digest: digest('7'),
            source_revision: "source:42".to_string(),
            source_revision_digest: digest('8'),
            runtime: Default::default(),
            instantiated_from: None,
        }
    }

    fn claims(
        principal: &str,
        tenant: &str,
        scopes: &[&str],
        policy_version: &str,
    ) -> RequestContextClaims {
        RequestContextClaims {
            principal: principal.to_string(),
            tenant: tenant.to_string(),
            audience: "epistemic-graph-test".to_string(),
            agent_id: "agent-a".to_string(),
            scopes: scopes.iter().map(|scope| (*scope).to_string()).collect(),
            delegation: vec![principal.to_string(), "agent-a".to_string()],
            policy_version: policy_version.to_string(),
            ..RequestContextClaims::default()
        }
    }

    fn signed_agent_library_request(
        secret: &str,
        claims: &RequestContextClaims,
        id: u64,
        op: AgentLibraryOp,
        wire_nonce: &str,
        idempotency_key: &str,
    ) -> Request {
        let mut request = Request {
            id,
            graph: claims.tenant.clone(),
            auth_token: String::new(),
            agent_id: Some(claims.agent_id.clone()),
            method: Method::AgentLibrary { op },
        };
        request.auth_token = crate::server::auth::compute_verified_envelope_token(
            secret,
            &request,
            &crate::server::auth::VerifiedEnvelopeParams {
                context: claims,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("system clock")
                    .as_secs(),
                nonce: wire_nonce,
                idempotency_key,
            },
        );
        request
    }

    fn response_write_result(
        response: crate::protocol::Response,
    ) -> eg_types::AgentLibraryWriteResult {
        assert!(
            response.error.is_none(),
            "unexpected route error: {:?}",
            response.error
        );
        let Some(ResultPayload::Raw(bytes)) = response.result else {
            panic!("Agent Library route did not return a raw typed result");
        };
        rmp_serde::from_slice(&bytes).expect("decode Agent Library route result")
    }

    /// Outbox rows the fixture's own component seeds contribute.
    ///
    /// An agent publish now RESOLVES its pinned components, so the five
    /// components `seeded_definition` publishes are real owner writes with real
    /// outbox rows. The agent-library assertions below stay discriminating: they
    /// still pin the number of AGENT rows, offset by a constant.
    const SEEDED_COMPONENT_OUTBOX: usize = 5;

    fn native_outbox_count(
        store: &crate::server::persistence::agent_library::AgentLibraryStore,
    ) -> usize {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("agent_library.redb");
        assert!(store.copy_into(&path).unwrap() >= 1);
        let database = redb::Database::open(&path).unwrap();
        let read = database.begin_read().unwrap();
        read.open_table(LEDGER_OUTBOX).unwrap().len().unwrap() as usize
    }

    #[test]
    fn backup_bundle_names_are_logical_not_paths() {
        for valid in ["scheduled-001", "snapshot_2", "release.3"] {
            assert_eq!(backup_bundle_name(valid).unwrap(), valid);
        }
        for invalid in [
            "",
            ".hidden",
            "../snapshot",
            "nested/snapshot",
            "C:\\snapshot",
            "snapshot\n",
        ] {
            assert!(backup_bundle_name(invalid).is_err(), "accepted {invalid:?}");
        }
    }

    #[test]
    fn agent_library_route_binds_verified_action_and_preserves_definition_provenance() {
        let directory = tempfile::tempdir().unwrap();
        let store = crate::server::persistence::agent_library::AgentLibraryStore::open(
            directory.path().to_str().unwrap(),
        )
        .unwrap();
        let nonce = Nonce::from_bytes([0x11; 32]);
        let verified = VerifiedRequestContext::from_verified_claims_with_nonce(
            RequestContextClaims {
                principal: "caller-a".to_string(),
                tenant: "tenant-a".to_string(),
                audience: "epistemic-graph".to_string(),
                agent_id: "agent-a".to_string(),
                scopes: vec!["agent:library-write".to_string()],
                policy_version: "policy-signed-v2".to_string(),
                ..RequestContextClaims::default()
            },
            "signed-idempotency-key".to_string(),
            Some(nonce),
        );
        let body = forged_context(&store, "tenant-a");
        let bound =
            bind_agent_library_context(&store, 42, &verified, body, "agent-library:publish", true)
                .unwrap();
        assert_eq!(bound.request_id, 42);
        assert_eq!(bound.principal, store.owner_principal());
        assert_eq!(bound.caller_principal, verified.principal_persistence_id());
        assert_eq!(bound.attempt_nonce, nonce);
        assert_eq!(bound.tenant_id, "tenant-a");
        assert_eq!(bound.actor_scope, verified.principal_persistence_id());
        assert_eq!(bound.purpose_id, "agent-library:publish");
        assert_eq!(bound.policy_revision, "policy-signed-v2");
        assert_eq!(bound.idempotency_key, "signed-idempotency-key");
        assert_eq!(bound.trace_id, None);

        let draft = definition("tenant-a");
        let retained = bind_agent_library_draft(draft.clone(), &verified, &bound).unwrap();
        assert_eq!(retained, draft);
        assert_ne!(retained.actor_scope, bound.actor_scope);
        assert_ne!(retained.purpose_id, bound.purpose_id);
        assert_ne!(retained.policy_digest, bound.policy_digest);
    }

    #[test]
    fn agent_library_write_route_rejects_missing_verified_nonce() {
        let directory = tempfile::tempdir().unwrap();
        let store = crate::server::persistence::agent_library::AgentLibraryStore::open(
            directory.path().to_str().unwrap(),
        )
        .unwrap();
        let verified = VerifiedRequestContext::from_verified_claims(
            RequestContextClaims {
                principal: "caller-a".to_string(),
                tenant: "tenant-a".to_string(),
                audience: "epistemic-graph".to_string(),
                agent_id: "agent-a".to_string(),
                scopes: vec!["agent:library-write".to_string()],
                policy_version: "policy-signed-v2".to_string(),
                ..RequestContextClaims::default()
            },
            "signed-idempotency-key".to_string(),
        );
        let error = bind_agent_library_context(
            &store,
            43,
            &verified,
            forged_context(&store, "tenant-a"),
            "agent-library:publish",
            true,
        )
        .unwrap_err();
        assert!(error.contains("authenticated attempt nonce"));
    }

    #[test]
    fn agent_library_signed_route_uses_wire_nonce_and_idempotency() {
        let directory = tempfile::tempdir().unwrap();
        let store = crate::server::persistence::agent_library::AgentLibraryStore::open(
            directory.path().to_str().unwrap(),
        )
        .unwrap();
        let claims = RequestContextClaims {
            principal: "agent-a".to_string(),
            tenant: "tenant-shared".to_string(),
            audience: "epistemic-graph-test".to_string(),
            agent_id: "agent-a".to_string(),
            scopes: vec!["agent:library-write".to_string()],
            policy_version: "policy-test".to_string(),
            ..RequestContextClaims::default()
        };
        let mut request = crate::protocol::Request {
            id: 44,
            graph: "tenant-shared".to_string(),
            auth_token: String::new(),
            agent_id: Some("agent-a".to_string()),
            method: crate::protocol::Method::AgentLibrary {
                op: AgentLibraryOp::Publish {
                    request: Box::new(AgentLibraryPublishRequest {
                        context: forged_context(&store, "tenant-shared"),
                        entry: seeded_definition(&store, "tenant-shared"),
                    }),
                },
            },
        };
        request.auth_token = crate::server::auth::compute_verified_envelope_token(
            "agent-library-test-secret",
            &request,
            &crate::server::auth::VerifiedEnvelopeParams {
                context: &claims,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                nonce: "wire-agent-library-nonce",
                idempotency_key: "wire-agent-library-key",
            },
        );
        let verified =
            crate::server::auth::verify_request("agent-library-test-secret", &request).unwrap();
        let context = match request.method {
            crate::protocol::Method::AgentLibrary {
                op: AgentLibraryOp::Publish { request },
            } => bind_agent_library_context(
                &store,
                44,
                &verified,
                request.context,
                "agent-library:publish",
                true,
            )
            .unwrap(),
            _ => panic!("expected Agent Library publish route"),
        };
        assert_eq!(context.idempotency_key, "wire-agent-library-key");
        assert_eq!(
            context.attempt_nonce,
            Nonce::from_bytes(
                Sha256::digest(b"eg/wire-nonce/v1\0wire-agent-library-nonce",).into()
            )
        );
        assert_eq!(
            context.caller_principal,
            verified.principal_persistence_id()
        );
        assert_eq!(context.tenant_id, "tenant-shared");
        assert_eq!(context.principal, store.owner_principal());
    }

    #[tokio::test]
    async fn signed_public_dispatch_persists_publish_retire_action_and_receipts() {
        // Synthetic HMAC fixture for `ServerState::new_for_test` inside
        // `#[cfg(all(test, feature = "redb"))]`: it authenticates nothing outside
        // this process and is never a live credential.
        let secret = "agent-library-public-dispatch-secret"; // sanitizer:ignore
        let directory = tempfile::tempdir().unwrap();
        let mut server_state = crate::server::state::ServerState::new_for_test(
            secret,
            crate::isolation::IsolationLayer::new(),
        );
        server_state.persist_dir = Some(directory.path().to_string_lossy().into_owned());
        let state = Arc::new(RwLock::new(server_state));
        let store = {
            let mut guard = state.write().await;
            guard.ensure_agent_library().unwrap()
        };

        let publish_claims = claims(
            "caller-a",
            "tenant-shared",
            &["agent:library-write"],
            "policy-test",
        );
        let publish_key = "public-publish-key";
        let publish_request = signed_agent_library_request(
            secret,
            &publish_claims,
            501,
            AgentLibraryOp::Publish {
                request: Box::new(AgentLibraryPublishRequest {
                    context: forged_context(&store, "tenant-shared"),
                    entry: seeded_definition(&store, "tenant-shared"),
                }),
            },
            "public-publish-nonce",
            publish_key,
        );
        let expected_publish_actor = VerifiedRequestContext::from_verified_claims(
            publish_claims.clone(),
            publish_key.to_string(),
        )
        .principal_persistence_id();
        let published =
            response_write_result(crate::server::dispatch::dispatch(&state, publish_request).await);
        assert!(!published.replayed);
        assert_eq!(published.entry.entry_revision, 1);
        assert_eq!(
            store.revisions("tenant-shared", "agent-a").unwrap().len(),
            1
        );
        assert_eq!(native_outbox_count(&store), SEEDED_COMPONENT_OUTBOX + 1);

        // A retry with a fresh authenticated nonce after reopening the owner
        // resolves from the native replay receipt. It must not append another
        // outbox row or require the retained definition to be rebuilt.
        let replay_request = signed_agent_library_request(
            secret,
            &publish_claims,
            506,
            AgentLibraryOp::Publish {
                request: Box::new(AgentLibraryPublishRequest {
                    context: forged_context(&store, "tenant-shared"),
                    entry: seeded_definition(&store, "tenant-shared"),
                }),
            },
            "public-publish-retry-nonce",
            publish_key,
        );
        drop(store);
        state.write().await.agent_library = None;
        let replayed =
            response_write_result(crate::server::dispatch::dispatch(&state, replay_request).await);
        assert!(replayed.replayed);
        assert_eq!(replayed.entry, published.entry);
        let store = {
            let guard = state.read().await;
            guard.agent_library.as_ref().unwrap().clone()
        };
        assert_eq!(native_outbox_count(&store), SEEDED_COMPONENT_OUTBOX + 1);

        // All three query operations stay on the native snapshot/status paths;
        // none creates an outbox record.
        let read_claims = claims(
            "caller-a",
            "tenant-shared",
            &["agent:library-read"],
            "policy-test",
        );
        for (id, op) in [
            (
                507,
                AgentLibraryOp::Current {
                    tenant_id: "tenant-shared".to_string(),
                    agent_id: "agent-a".to_string(),
                },
            ),
            (
                508,
                AgentLibraryOp::History {
                    tenant_id: "tenant-shared".to_string(),
                    agent_id: "agent-a".to_string(),
                },
            ),
            (
                509,
                AgentLibraryOp::Status {
                    request: AgentLibraryStatusRequest {
                        context: forged_context(&store, "tenant-shared"),
                        agent_id: "agent-a".to_string(),
                        kind: AgentLibraryMutationKind::Publish,
                    },
                },
            ),
        ] {
            let response = crate::server::dispatch::dispatch(
                &state,
                signed_agent_library_request(
                    secret,
                    &read_claims,
                    id,
                    op,
                    &format!("public-read-nonce-{id}"),
                    &format!("public-read-key-{id}"),
                ),
            )
            .await;
            assert!(response.error.is_none(), "query failed: {response:?}");
        }
        assert_eq!(native_outbox_count(&store), SEEDED_COMPONENT_OUTBOX + 1);

        let mut retire_body = forged_context(&store, "tenant-shared");
        retire_body.expected_revision = Some(1);
        let retire_claims = claims(
            "caller-b",
            "tenant-shared",
            &["agent:library-write"],
            "policy-test",
        );
        let retire_key = "public-retire-key";
        let retire_request = signed_agent_library_request(
            secret,
            &retire_claims,
            502,
            AgentLibraryOp::Retire {
                request: AgentLibraryRetireRequest {
                    context: retire_body,
                    agent_id: "agent-a".to_string(),
                },
            },
            "public-retire-nonce",
            retire_key,
        );
        let expected_retire_actor = VerifiedRequestContext::from_verified_claims(
            retire_claims.clone(),
            retire_key.to_string(),
        )
        .principal_persistence_id();
        let retired =
            response_write_result(crate::server::dispatch::dispatch(&state, retire_request).await);
        assert!(!retired.replayed);
        assert!(retired.entry.is_retired());
        assert_eq!(
            store.revisions("tenant-shared", "agent-a").unwrap().len(),
            2
        );
        assert_eq!(native_outbox_count(&store), SEEDED_COMPONENT_OUTBOX + 2);

        // Copy the live owner through its backup seam and inspect the copied
        // durable rows. This proves the public signed route reached the native
        // owner, typed replay receipt, and outbox rather than only returning a
        // handler-shaped response.
        let backup_dir = tempfile::tempdir().unwrap();
        let backup_path = backup_dir.path().join("agent_library.redb");
        assert!(store.copy_into(&backup_path).unwrap() >= 1);
        let database = redb::Database::open(&backup_path).unwrap();
        let read = database.begin_read().unwrap();
        let identity = eg_types::MutationScopeIdentity::fixed_native(
            "tenant-shared",
            eg_types::mutation_batch::DurabilityDomain::ControlPlane,
            "agent-library",
            "agent-library:v1",
        )
        .unwrap();
        let scope_key = eg_storage::ledger_scope_key(&identity);
        let batches = read.open_table(LEDGER_BATCHES).unwrap();
        let publish_batch = batches
            .get((
                scope_key.as_str(),
                format!("agent-library/v1/{publish_key}").as_str(),
            ))
            .unwrap()
            .expect("published batch in copied owner")
            .value()
            .to_vec();
        let publish_batch = eg_storage::decode_batch_record(&publish_batch).unwrap();
        assert!(publish_batch.result_msgpack.is_some());
        let result = eg_types::msgpack::decode_bounded::<eg_types::mutation::MutationResult>(
            publish_batch.result_msgpack.as_deref().unwrap(),
            eg_types::msgpack::MsgpackLimits::new(
                16 * 1024 * 1024,
                200_000,
                eg_types::msgpack::DEFAULT_MAX_DEPTH,
            ),
        )
        .unwrap();
        assert!(matches!(
            result,
            eg_types::mutation::MutationResult::DomainResult { .. }
        ));
        let replay_operations = read.open_table(REPLAY_OPERATIONS).unwrap();
        let publish_replay = replay_operations
            .get((scope_key.as_str(), publish_key))
            .unwrap()
            .expect("published typed replay receipt")
            .value()
            .to_vec();
        let publish_replay =
            eg_storage::decode_ledger_record::<eg_storage::OperationReplayRow>(&publish_replay)
                .unwrap();
        assert!(matches!(
            publish_replay.recorded,
            eg_storage::RecordedOperation::Receipt(_)
        ));
        let outbox = read.open_table(LEDGER_OUTBOX).unwrap();
        let publish_outbox = outbox
            .get((
                scope_key.as_str(),
                format!("agent-library/v1/{publish_key}").as_str(),
                0,
            ))
            .unwrap()
            .expect("published outbox row in copied owner")
            .value()
            .to_vec();
        let publish_outbox = eg_storage::decode_outbox_record(&publish_outbox).unwrap();
        assert_eq!(
            publish_outbox.intent.headers.get("actor"),
            Some(&expected_publish_actor)
        );
        let event = eg_types::msgpack::decode_bounded::<eg_types::AgentLibraryOutboxEvent>(
            &publish_outbox.intent.payload,
            eg_types::msgpack::MsgpackLimits::new(
                16 * 1024 * 1024,
                200_000,
                eg_types::msgpack::DEFAULT_MAX_DEPTH,
            ),
        )
        .unwrap();
        assert_eq!(event.performing_actor, expected_publish_actor);
        assert_eq!(event.entry.actor_scope, "definition:builder-a");

        let retire_batch = batches
            .get((
                scope_key.as_str(),
                format!("agent-library/v1/{retire_key}").as_str(),
            ))
            .unwrap()
            .expect("retire batch in copied owner")
            .value()
            .to_vec();
        let retire_batch = eg_storage::decode_batch_record(&retire_batch).unwrap();
        let retire_replay = replay_operations
            .get((scope_key.as_str(), retire_key))
            .unwrap()
            .expect("retire typed replay receipt")
            .value()
            .to_vec();
        let retire_replay =
            eg_storage::decode_ledger_record::<eg_storage::OperationReplayRow>(&retire_replay)
                .unwrap();
        assert!(matches!(
            retire_replay.recorded,
            eg_storage::RecordedOperation::Receipt(_)
        ));
        let retire_outbox = outbox
            .get((
                scope_key.as_str(),
                format!("agent-library/v1/{retire_key}").as_str(),
                0,
            ))
            .unwrap()
            .expect("retire outbox row in copied owner")
            .value()
            .to_vec();
        let retire_outbox = eg_storage::decode_outbox_record(&retire_outbox).unwrap();
        assert_eq!(
            retire_outbox.intent.headers.get("actor"),
            Some(&expected_retire_actor)
        );
        let retire_event = eg_types::msgpack::decode_bounded::<eg_types::AgentLibraryOutboxEvent>(
            &retire_outbox.intent.payload,
            eg_types::msgpack::MsgpackLimits::new(
                16 * 1024 * 1024,
                200_000,
                eg_types::msgpack::DEFAULT_MAX_DEPTH,
            ),
        )
        .unwrap();
        assert_eq!(retire_event.performing_actor, expected_retire_actor);
        assert_eq!(retire_event.entry.actor_scope, "definition:builder-a");
        assert!(retire_event.entry.is_retired());
        assert_ne!(
            publish_batch.committed_version,
            retire_batch.committed_version
        );

        // The original publish key with changed content is a conflict. Even
        // with a fresh nonce, the native operation identity rejects it before
        // any owner row, receipt, or outbox mutation.
        let mut changed = seeded_definition(&store, "tenant-shared");
        changed.role = "different-role".to_string();
        let mut changed_context = forged_context(&store, "tenant-shared");
        changed_context.expected_revision = Some(0);
        let conflict = crate::server::dispatch::dispatch(
            &state,
            signed_agent_library_request(
                secret,
                &publish_claims,
                510,
                AgentLibraryOp::Publish {
                    request: Box::new(AgentLibraryPublishRequest {
                        context: changed_context,
                        entry: changed,
                    }),
                },
                "public-publish-conflict-nonce",
                publish_key,
            ),
        )
        .await;
        assert!(
            conflict.error.is_some(),
            "changed payload unexpectedly succeeded"
        );
        assert_eq!(native_outbox_count(&store), SEEDED_COMPONENT_OUTBOX + 2);

        // The same authenticated route boundary rejects a body that names a
        // different tenant, and scope omissions fail before the owner opens.
        let wrong_tenant = signed_agent_library_request(
            secret,
            &publish_claims,
            503,
            AgentLibraryOp::Publish {
                request: Box::new(AgentLibraryPublishRequest {
                    context: forged_context(&store, "tenant-b"),
                    entry: definition("tenant-b"),
                }),
            },
            "wrong-tenant-nonce",
            "wrong-tenant-key",
        );
        let wrong_tenant_response = crate::server::dispatch::dispatch(&state, wrong_tenant).await;
        assert!(
            wrong_tenant_response
                .error
                .as_deref()
                .is_some_and(|error| error.starts_with("ACCESS_DENIED")),
            "{wrong_tenant_response:?}"
        );
        assert!(store.revisions("tenant-b", "agent-a").unwrap().is_empty());

        let read_only_claims = claims(
            "caller-a",
            "tenant-shared",
            &["agent:library-read"],
            "policy-test",
        );
        let missing_write = signed_agent_library_request(
            secret,
            &read_only_claims,
            504,
            AgentLibraryOp::Publish {
                request: Box::new(AgentLibraryPublishRequest {
                    context: forged_context(&store, "tenant-shared"),
                    entry: seeded_definition(&store, "tenant-shared"),
                }),
            },
            "missing-write-nonce",
            "missing-write-key",
        );
        let missing_write_response = crate::server::dispatch::dispatch(&state, missing_write).await;
        assert!(
            missing_write_response
                .error
                .as_deref()
                .is_some_and(|error| error.contains("agent:library-write")),
            "{missing_write_response:?}"
        );

        let write_only_claims = claims(
            "caller-a",
            "tenant-shared",
            &["agent:library-write"],
            "policy-test",
        );
        let missing_read = signed_agent_library_request(
            secret,
            &write_only_claims,
            505,
            AgentLibraryOp::Current {
                tenant_id: "tenant-shared".to_string(),
                agent_id: "agent-a".to_string(),
            },
            "missing-read-nonce",
            "missing-read-key",
        );
        let missing_read_response = crate::server::dispatch::dispatch(&state, missing_read).await;
        assert!(
            missing_read_response
                .error
                .as_deref()
                .is_some_and(|error| error.contains("agent:library-read")),
            "{missing_read_response:?}"
        );
    }
}
