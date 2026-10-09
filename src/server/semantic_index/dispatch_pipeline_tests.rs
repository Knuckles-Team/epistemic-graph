//! End-to-end wiring proof for `Method::SemanticIndex`.
//!
//! The module above is the AUTHORITY seam; this module is the DISPATCH seam,
//! and it exists because the expensive defects in this program were never
//! logic defects. They were wiring gaps that every unit test passed through:
//! this whole subsystem sat uncompiled for days while its own tests looked
//! green, because nothing ever declared the module.
//!
//! So none of these tests call `SemanticIndexService` or the adapter. Every one
//! of them builds a signed `Request`, hands it to the real
//! `crate::server::dispatch::dispatch`, and asserts on the `Response` -- the
//! exact path an external connector takes, through envelope verification,
//! `requires_write`, the capability policy, the router arm, and the handler.
//! A test that reached past any of those would prove nothing about whether the
//! method is reachable at all, which is the only thing that has ever gone wrong
//! here.
use std::sync::Arc;

use tokio::sync::RwLock;

use super::sql_source_read_tests::{
    binding_draft, commit_sql_change, dispatch_table_fixture, selector,
};
use crate::acl::RequestContextClaims;
use crate::protocol::Request;
use crate::protocol::{Method, Response};
use crate::server::access::CarrierAuthority;
use crate::server::auth::{
    compute_verified_envelope_token, VerifiedEnvelopeParams, VerifiedRequestContext,
};
use crate::test_support::decode_raw_response as ok;
use eg_types::semantic_index::{
    SemanticBinding, SemanticBindingState, SemanticIndexCommand, SemanticIndexFilter,
    SemanticIndexOp, SemanticIndexOutcome, SemanticIndexRequest, SemanticIndexResponse,
    SemanticIndexResult, SemanticPolicyIdentity, SemanticQueueClass, SemanticStage,
    SemanticStageIntentDraft, SemanticStageLeasePage, SemanticStageOutcome,
    SemanticStagePredecessor, SemanticStageReceipt, SemanticStageScope, SemanticStageTransition,
};

/// Synthetic HMAC fixture for `ServerState::new_for_test`: it authenticates
/// nothing outside this process and is never a live credential.
const SECRET: &str = "semantic-index-dispatch-fixture-secret"; // sanitizer:ignore
/// `auth::request_context_policy` is a FIXED triple under `cfg(test)`:
/// audience `epistemic-graph-test`, tenant `tenant-shared`, policy version
/// `policy-test`. A signed envelope that names anything else is refused at
/// the request boundary before the method is ever looked at, so these are
/// not arbitrary fixture names.
const TENANT: &str = "tenant-shared";
const WORKER: &str = "semantic-dispatch-worker";

fn dispatch_state(persist_dir: &std::path::Path) -> Arc<RwLock<crate::server::state::ServerState>> {
    let mut state = crate::server::state::ServerState::new_for_test(
        SECRET,
        crate::isolation::IsolationLayer::new(),
    );
    state.persist_dir = Some(persist_dir.to_string_lossy().into_owned());
    Arc::new(RwLock::new(state))
}

fn claims(principal: &str) -> RequestContextClaims {
    RequestContextClaims {
        principal: principal.to_string(),
        tenant: TENANT.to_string(),
        audience: "epistemic-graph-test".to_string(),
        agent_id: principal.to_string(),
        scopes: vec!["*".to_string()],
        // Empty: this is a NON-delegated context (the principal IS the
        // agent), and auth refuses a non-delegated context that still
        // carries a chain.
        delegation: Vec::new(),
        policy_version: "policy-test".to_string(),
        ..RequestContextClaims::default()
    }
}

/// One signed `Method::SemanticIndex` request, exactly as a connector mints
/// it. The attempt nonce and idempotency key the handler uses are derived
/// from THIS envelope, not from the op body.
fn signed(id: u64, op: SemanticIndexOp) -> Request {
    let context = claims(WORKER);
    let mut request = Request {
        id,
        graph: TENANT.to_string(),
        auth_token: String::new(),
        agent_id: Some(WORKER.to_string()),
        method: Method::SemanticIndex { op: Box::new(op) },
    };
    request.auth_token = compute_verified_envelope_token(
        SECRET,
        &request,
        &VerifiedEnvelopeParams {
            context: &context,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock")
                .as_secs(),
            nonce: &format!("semantic-dispatch-nonce-{id}"),
            idempotency_key: &format!("semantic-dispatch-key-{id}"),
        },
    );
    request
}

/// A completion stamp comfortably after the engine's own ACL decision
/// instant, which the handler samples inside the dispatch call and the test
/// therefore cannot observe beforehand.
fn future_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock")
        .as_millis() as u64
        + 60_000
}

fn worker_authority() -> CarrierAuthority {
    CarrierAuthority::from_verified(&VerifiedRequestContext::from_verified_claims(
        claims(WORKER),
        "semantic-dispatch-key-0".to_string(),
    ))
    .unwrap()
}

fn refused(label: &str, response: Response) -> String {
    response
        .error_detail
        .unwrap_or_else(|| panic!("{label} was accepted but must be refused"))
}

/// Drive one source from admission to a leased, completed S1 and a published
/// S2, entirely through `dispatch`.
///
/// This is the wiring proof, and it asserts the two properties that make the
/// queue a PIPELINE rather than a work list:
///   * the queue class is honoured -- a `Fast` consumer is not handed the
///     `Medium` S2 row, and vice versa;
///   * the predecessor relation is STRUCTURAL, not advisory -- S2 does not
///     exist to be claimed until S1's completion derives it, and a
///     transition presented against a lease that does not name it is
///     refused outright.
#[tokio::test]
async fn dispatch_drives_a_source_through_s1_and_publishes_s2() {
    let worker = worker_authority();
    let fixture = dispatch_table_fixture(&worker, TENANT);
    let draft = binding_draft(&worker, &fixture.snapshot);
    let binding_id = draft.binding_id.clone();

    let state = dispatch_state(&fixture.persist_dir);

    // ---- admit the binding -------------------------------------------
    let _: serde_json::Value = ok(
        "AdmitBinding",
        crate::server::dispatch::dispatch(
            &state,
            signed(
                901,
                SemanticIndexOp::AdmitBinding {
                    tenant_id: TENANT.to_string(),
                    binding_id: binding_id.clone(),
                    draft: Box::new(draft.clone()),
                    idempotency_key: "semantic-dispatch-admit".to_string(),
                },
            ),
        )
        .await,
    );

    // The public RF-019 envelope reaches the same durable binding owner
    // through the verified carrier; its named GET result is a typed DTO.
    let admitted = SemanticBinding::create(draft.clone()).unwrap();
    let public_request = SemanticIndexRequest {
        request_id: "semantic-public-get-901".to_string(),
        tenant_id: worker.tenant_scope().to_string(),
        actor_scope: worker.actor_scope().to_string(),
        effective_actor_scope: worker.agent_id().to_string(),
        purpose_id: admitted.purpose_id.clone(),
        policy_identity: admitted.policy_identity.clone(),
        command: SemanticIndexCommand::GetBinding {
            binding_id: binding_id.clone(),
        },
        approval: None,
    };
    let public: SemanticIndexResponse = ok(
        "semantic_binding_get",
        crate::server::dispatch::dispatch(
            &state,
            signed(
                9011,
                SemanticIndexOp::GetBindingRequest {
                    request: Box::new(public_request.clone()),
                },
            ),
        )
        .await,
    );
    public.validate().unwrap();
    assert!(matches!(
        public.outcome,
        SemanticIndexOutcome::Accepted {
            result: SemanticIndexResult::Binding { binding },
            ..
        } if binding.binding_id == binding_id
    ));

    let mut changed_policy = admitted.policy_identity.components.clone();
    changed_policy.row_policy_revision += 1;
    let wrong_policy = SemanticIndexRequest {
        policy_identity: SemanticPolicyIdentity::create(
            worker.tenant_scope(),
            worker.agent_id(),
            admitted.purpose_id.as_str(),
            changed_policy,
        )
        .unwrap(),
        ..public_request.clone()
    };
    let hidden: SemanticIndexResponse = ok(
        "semantic_binding_get under another policy",
        crate::server::dispatch::dispatch(
            &state,
            signed(
                9013,
                SemanticIndexOp::GetBindingRequest {
                    request: Box::new(wrong_policy),
                },
            ),
        )
        .await,
    );
    assert!(matches!(hidden.outcome, SemanticIndexOutcome::NotFound));

    let unsupported = SemanticIndexRequest {
        command: SemanticIndexCommand::ListBindings {
            filter: SemanticIndexFilter {
                source_entity_ids: Vec::new(),
                required_source_revision: None,
                max_results: 10,
            },
            cursor: None,
        },
        ..public_request
    };
    let denied = crate::server::dispatch::dispatch(
        &state,
        signed(
            9012,
            SemanticIndexOp::GetBindingRequest {
                request: Box::new(unsupported),
            },
        ),
    )
    .await;
    assert!(refused("unsupported public semantic command", denied)
        .contains("only semantic_binding_get"));

    // ---- S1 admission from the authoritative SQL wakeup ---------------
    let _: serde_json::Value = ok(
        "AdmitSourceRecord",
        crate::server::dispatch::dispatch(
            &state,
            signed(
                902,
                SemanticIndexOp::AdmitSourceRecord {
                    tenant_id: TENANT.to_string(),
                    binding_id: binding_id.clone(),
                    record: Box::new(fixture.dirty.clone()),
                },
            ),
        )
        .await,
    );

    // A claim is only legal against a Building binding, so the state
    // machine has to move through the wire too.
    let _: serde_json::Value = ok(
        "TransitionBinding",
        crate::server::dispatch::dispatch(
            &state,
            signed(
                903,
                SemanticIndexOp::TransitionBinding {
                    tenant_id: TENANT.to_string(),
                    binding_id: binding_id.clone(),
                    expected_generation: draft.generation,
                    next_state: SemanticBindingState::Building,
                    idempotency_key: "semantic-dispatch-build".to_string(),
                },
            ),
        )
        .await,
    );

    let _: serde_json::Value = ok(
        "SubscribeStageConsumer",
        crate::server::dispatch::dispatch(
            &state,
            signed(
                904,
                SemanticIndexOp::SubscribeStageConsumer {
                    tenant_id: TENANT.to_string(),
                    binding_id: binding_id.clone(),
                    consumer: WORKER.to_string(),
                },
            ),
        )
        .await,
    );

    // ---- queue class, leg one: a Medium worker must not get the Fast S1
    let medium_first: SemanticStageLeasePage = ok(
        "ClaimStageLeases(Medium)",
        crate::server::dispatch::dispatch(
            &state,
            signed(
                905,
                SemanticIndexOp::ClaimStageLeases {
                    tenant_id: TENANT.to_string(),
                    binding_id: binding_id.clone(),
                    consumer: WORKER.to_string(),
                    queue_class: SemanticQueueClass::Medium,
                    limit: 8,
                    lease_ms: 60_000,
                },
            ),
        )
        .await,
    );
    // X10-T2: the Medium consumer never even leases the Fast S1 -- it is
    // on another topic -- so no wrong-class release can spend its retries.
    assert!(
        medium_first.entries.is_empty(),
        "S1 is a Fast row and must not be handed to a Medium consumer"
    );

    // ---- queue class, leg two: the Fast worker gets exactly the S1 -----
    let fast: SemanticStageLeasePage = ok(
        "ClaimStageLeases(Fast)",
        crate::server::dispatch::dispatch(
            &state,
            signed(
                906,
                SemanticIndexOp::ClaimStageLeases {
                    tenant_id: TENANT.to_string(),
                    binding_id: binding_id.clone(),
                    consumer: WORKER.to_string(),
                    queue_class: SemanticQueueClass::Fast,
                    limit: 8,
                    lease_ms: 60_000,
                },
            ),
        )
        .await,
    );
    assert_eq!(fast.entries.len(), 1, "the admitted S1 is claimable");
    let entry = fast.entries.into_iter().next().unwrap();
    assert_eq!(entry.intent.stage, SemanticStage::SourceCommit);
    assert_eq!(entry.queue_class, SemanticQueueClass::Fast);

    // The S2 this S1 will publish. Its predecessor proof names the S1
    // receipt, which does not exist yet.
    let s1_intent = entry.intent.clone();
    let s1_receipt_digest = s1_intent.intent_digest;
    let premature_s2 = SemanticStageIntentDraft {
        binding_id: binding_id.clone(),
        binding_digest: s1_intent.binding_digest,
        generation: s1_intent.generation,
        stage: SemanticStage::GraphProjection,
        scope: SemanticStageScope::Entity {
            source_entity_id: s1_intent
                .scope
                .source_entity_id()
                .expect("an S1 intent is entity scoped")
                .to_string(),
        },
        source_revision: s1_intent.source_revision.clone(),
        input_digest: s1_receipt_digest,
        predecessor: SemanticStagePredecessor::EntityReceipt {
            stage: SemanticStage::SourceCommit,
            receipt_digest: s1_receipt_digest,
        },
    };

    // ---- a transition may only be completed against ITS OWN lease -----
    //
    // The queue-level half of the predecessor proof was already asserted
    // above: the Medium claim found nothing, because S2 is not enqueued
    // until S1 completes. This is the lease-level half -- presenting an S2
    // transition against the S1 lease that is actually held.
    let premature = SemanticStageTransition {
        intent: eg_types::semantic_index::SemanticStageIntent::create(premature_s2)
            .expect("the S2 intent is well formed"),
        receipt: SemanticStageReceipt {
            intent_digest: s1_receipt_digest,
            output_digest: s1_receipt_digest,
            cursor: "graph-projection:premature".to_string(),
            completed_at: format!("unix-ms:{}", future_ms()),
            outcome: SemanticStageOutcome::Completed,
        },
        generation_checkpoint: None,
    };
    let error = refused(
        "an S2 transition against the S1 lease",
        crate::server::dispatch::dispatch(
            &state,
            signed(
                907,
                SemanticIndexOp::CompleteStage {
                    tenant_id: TENANT.to_string(),
                    binding_id: binding_id.clone(),
                    lease: Box::new(entry.lease.clone()),
                    transition: Box::new(premature),
                    artifact: Box::new(eg_types::semantic_index::SemanticStageArtifact::None),
                    successor: None,
                },
            ),
        )
        .await,
    );
    assert!(
        !error.is_empty(),
        "a transition the held lease does not name must be refused by name"
    );

    // ---- complete S1 and publish the S2 successor ---------------------
    let s1_transition = SemanticStageTransition {
        intent: s1_intent.clone(),
        receipt: SemanticStageReceipt {
            intent_digest: s1_intent.intent_digest,
            output_digest: fixture.source_digest,
            cursor: "sql-source:complete".to_string(),
            completed_at: format!("unix-ms:{}", future_ms()),
            outcome: SemanticStageOutcome::Completed,
        },
        generation_checkpoint: None,
    };
    let _: serde_json::Value = ok(
        "CompleteSqlSourceStage",
        crate::server::dispatch::dispatch(
            &state,
            signed(
                908,
                SemanticIndexOp::CompleteSqlSourceStage {
                    tenant_id: TENANT.to_string(),
                    binding_id: binding_id.clone(),
                    lease: Box::new(entry.lease.clone()),
                    transition: Box::new(s1_transition),
                    // `None`, not a caller-built S2. The store DERIVES the
                    // successor from the completed transition itself
                    // (`validate_successor_intent`'s `SourceCommit` arm), so
                    // the predecessor receipt digest and input digest that
                    // bind S2 to this exact S1 are computed from the
                    // committed receipt rather than asserted by the caller.
                    // A caller-supplied successor is accepted only when it
                    // matches that derivation exactly.
                    successor: None,
                    page_cursor: None,
                },
            ),
        )
        .await,
    );

    // ---- the published S2 is a Medium row, and only a Medium worker
    //      is handed it ------------------------------------------------
    let medium: SemanticStageLeasePage = ok(
        "ClaimStageLeases(Medium) after S1",
        crate::server::dispatch::dispatch(
            &state,
            signed(
                909,
                SemanticIndexOp::ClaimStageLeases {
                    tenant_id: TENANT.to_string(),
                    binding_id: binding_id.clone(),
                    consumer: WORKER.to_string(),
                    queue_class: SemanticQueueClass::Medium,
                    limit: 8,
                    lease_ms: 60_000,
                },
            ),
        )
        .await,
    );
    assert_eq!(
        medium.entries.len(),
        1,
        "completing S1 publishes its S2 successor onto the Medium queue"
    );
    assert_eq!(
        medium.entries[0].intent.stage,
        SemanticStage::GraphProjection
    );
    assert_eq!(medium.entries[0].queue_class, SemanticQueueClass::Medium);
}

/// One tenant may not reach another's binding by naming its id.
#[tokio::test]
async fn dispatch_refuses_a_cross_tenant_semantic_operation() {
    let worker = worker_authority();
    let fixture = dispatch_table_fixture(&worker, TENANT);
    let state = dispatch_state(&fixture.persist_dir);

    let denied = crate::server::dispatch::dispatch(
        &state,
        signed(
            920,
            SemanticIndexOp::Binding {
                tenant_id: "tenant-somebody-else".to_string(),
                binding_id: "binding:documents-body".to_string(),
            },
        ),
    )
    .await;
    assert_eq!(denied.error.as_deref(), Some("ACCESS_DENIED"));
    let error = refused("a cross-tenant read", denied);
    assert!(
        error.contains("semantic index tenant must match verified request tenant"),
        "a cross-tenant semantic read must be refused by name, got: {error}"
    );
    let draft = binding_draft(&worker, &fixture.snapshot);
    let binding = SemanticBinding::create(draft).unwrap();
    let wrong_scope = "tenant:unverified";
    let public_request = SemanticIndexRequest {
        request_id: "cross-tenant-public-get".to_string(),
        tenant_id: wrong_scope.to_string(),
        actor_scope: worker.actor_scope().to_string(),
        effective_actor_scope: worker.agent_id().to_string(),
        purpose_id: binding.purpose_id.clone(),
        policy_identity: SemanticPolicyIdentity::create(
            wrong_scope,
            worker.agent_id(),
            binding.purpose_id.as_str(),
            binding.policy_identity.components.clone(),
        )
        .unwrap(),
        command: SemanticIndexCommand::GetBinding {
            binding_id: binding.binding_id,
        },
        approval: None,
    };
    let denied = crate::server::dispatch::dispatch(
        &state,
        signed(
            921,
            SemanticIndexOp::GetBindingRequest {
                request: Box::new(public_request),
            },
        ),
    )
    .await;
    assert!(refused("cross-tenant public semantic read", denied)
        .contains("does not match verified carrier"));
    let _ = selector();
}

/// A claim above the named bound is refused BY THAT NAME, not by a store
/// message about an anonymous "bounded consumer budget".
// spec: EG-IDENTITY-R005
#[tokio::test]
async fn dispatch_refuses_an_unbounded_stage_claim_by_name() {
    let worker = worker_authority();
    let fixture = dispatch_table_fixture(&worker, TENANT);
    let state = dispatch_state(&fixture.persist_dir);

    let error = refused(
        "an over-limit claim",
        crate::server::dispatch::dispatch(
            &state,
            signed(
                930,
                SemanticIndexOp::ClaimStageLeases {
                    tenant_id: TENANT.to_string(),
                    binding_id: "binding:documents-body".to_string(),
                    consumer: WORKER.to_string(),
                    queue_class: SemanticQueueClass::Fast,
                    limit: eg_types::semantic_index::MAX_SEMANTIC_STAGE_CLAIM_LIMIT + 1,
                    lease_ms: 60_000,
                },
            ),
        )
        .await,
    );
    assert!(
        error.contains("limit"),
        "the refusal must name `limit`: {error}"
    );
    let _ = commit_sql_change;
}

/// EG-IDENTITY-R005: a worker presents ONE verified identity across every
/// queue class it processes, rather than switching principals per class.
///
/// Every op below carries a client-claimed `consumer` field naming a
/// DIFFERENT, unverified principal for each class -- exactly what a
/// compromised or misconfigured caller would send. If the handler ever
/// trusted that field, the two classes' durable leases would be attributed
/// to two different principals. They are not: `claim()` and `subscribe()`
/// (src/server/handlers/semantic_index/worker.rs) derive the lease owner
/// solely from `ctx.authority.agent_id()`, the one identity the envelope
/// signature verified, so both classes' leases -- Fast's S1 and Medium's
/// published S2 successor -- are attributed to the same verified worker,
/// and the claimed field is never read for authorization.
// spec: EG-IDENTITY-R005
#[tokio::test]
async fn dispatch_attributes_every_queue_class_to_the_one_verified_principal() {
    let worker = worker_authority();
    let fixture = dispatch_table_fixture(&worker, TENANT);
    let draft = binding_draft(&worker, &fixture.snapshot);
    let binding_id = draft.binding_id.clone();
    let state = dispatch_state(&fixture.persist_dir);

    let _: serde_json::Value = ok(
        "AdmitBinding",
        crate::server::dispatch::dispatch(
            &state,
            signed(
                940,
                SemanticIndexOp::AdmitBinding {
                    tenant_id: TENANT.to_string(),
                    binding_id: binding_id.clone(),
                    draft: Box::new(draft.clone()),
                    idempotency_key: "semantic-identity-admit".to_string(),
                },
            ),
        )
        .await,
    );
    let _: serde_json::Value = ok(
        "AdmitSourceRecord",
        crate::server::dispatch::dispatch(
            &state,
            signed(
                941,
                SemanticIndexOp::AdmitSourceRecord {
                    tenant_id: TENANT.to_string(),
                    binding_id: binding_id.clone(),
                    record: Box::new(fixture.dirty.clone()),
                },
            ),
        )
        .await,
    );
    let _: serde_json::Value = ok(
        "TransitionBinding",
        crate::server::dispatch::dispatch(
            &state,
            signed(
                942,
                SemanticIndexOp::TransitionBinding {
                    tenant_id: TENANT.to_string(),
                    binding_id: binding_id.clone(),
                    expected_generation: draft.generation,
                    next_state: SemanticBindingState::Building,
                    idempotency_key: "semantic-identity-build".to_string(),
                },
            ),
        )
        .await,
    );
    // Every envelope below is signed by the SAME verified principal
    // (`worker_authority()`'s WORKER), but the op body itself names a
    // DIFFERENT, unverified consumer per call -- the thing an identity
    // defect would let through.
    let _: serde_json::Value = ok(
        "SubscribeStageConsumer",
        crate::server::dispatch::dispatch(
            &state,
            signed(
                943,
                SemanticIndexOp::SubscribeStageConsumer {
                    tenant_id: TENANT.to_string(),
                    binding_id: binding_id.clone(),
                    consumer: "claimed-fast-identity".to_string(),
                },
            ),
        )
        .await,
    );

    // ---- Fast leg: claim S1 under a spoofed `consumer` field -----------
    let fast: SemanticStageLeasePage = ok(
        "ClaimStageLeases(Fast)",
        crate::server::dispatch::dispatch(
            &state,
            signed(
                944,
                SemanticIndexOp::ClaimStageLeases {
                    tenant_id: TENANT.to_string(),
                    binding_id: binding_id.clone(),
                    consumer: "claimed-fast-identity".to_string(),
                    queue_class: SemanticQueueClass::Fast,
                    limit: 8,
                    lease_ms: 60_000,
                },
            ),
        )
        .await,
    );
    assert_eq!(fast.entries.len(), 1, "the admitted S1 is claimable");
    let fast_entry = fast.entries.into_iter().next().unwrap();
    let fast_parts =
        eg_core::compute::semantic_ann_codes::stage_consumer_parts(&fast_entry.lease.consumer)
            .expect("a leased row's consumer is `<worker>#<class>`");
    assert_eq!(
        fast_parts.0, WORKER,
        "the Fast lease is attributed to the verified envelope principal, \
         never the op body's claimed `consumer` field"
    );
    assert_eq!(fast_parts.1, SemanticQueueClass::Fast);

    // ---- complete S1, publishing its Medium S2 successor ---------------
    let s1_intent = fast_entry.intent.clone();
    let s1_transition = SemanticStageTransition {
        intent: s1_intent.clone(),
        receipt: SemanticStageReceipt {
            intent_digest: s1_intent.intent_digest,
            output_digest: fixture.source_digest,
            cursor: "sql-source:identity-complete".to_string(),
            completed_at: format!("unix-ms:{}", future_ms()),
            outcome: SemanticStageOutcome::Completed,
        },
        generation_checkpoint: None,
    };
    let _: serde_json::Value = ok(
        "CompleteSqlSourceStage",
        crate::server::dispatch::dispatch(
            &state,
            signed(
                945,
                SemanticIndexOp::CompleteSqlSourceStage {
                    tenant_id: TENANT.to_string(),
                    binding_id: binding_id.clone(),
                    lease: Box::new(fast_entry.lease.clone()),
                    transition: Box::new(s1_transition),
                    successor: None,
                    page_cursor: None,
                },
            ),
        )
        .await,
    );

    // ---- Medium leg: claim S2 under a DIFFERENT spoofed `consumer` -----
    let medium: SemanticStageLeasePage = ok(
        "ClaimStageLeases(Medium)",
        crate::server::dispatch::dispatch(
            &state,
            signed(
                946,
                SemanticIndexOp::ClaimStageLeases {
                    tenant_id: TENANT.to_string(),
                    binding_id: binding_id.clone(),
                    consumer: "claimed-medium-identity".to_string(),
                    queue_class: SemanticQueueClass::Medium,
                    limit: 8,
                    lease_ms: 60_000,
                },
            ),
        )
        .await,
    );
    assert_eq!(
        medium.entries.len(),
        1,
        "completing S1 publishes its S2 successor onto the Medium queue"
    );
    let medium_entry = medium.entries.into_iter().next().unwrap();
    let medium_parts =
        eg_core::compute::semantic_ann_codes::stage_consumer_parts(&medium_entry.lease.consumer)
            .expect("a leased row's consumer is `<worker>#<class>`");
    assert_eq!(
        medium_parts.0, WORKER,
        "the Medium lease is attributed to the verified envelope principal, \
         never the op body's claimed `consumer` field"
    );
    assert_eq!(medium_parts.1, SemanticQueueClass::Medium);

    // ---- the one assertion this requirement is actually about ----------
    // Fast and Medium are different topics, claimed under different spoofed
    // `consumer` fields, yet the durable record of who did the work names
    // the SAME verified principal for both -- one identity across every
    // queue class, not one identity per class.
    assert_eq!(
        fast_parts.0, medium_parts.0,
        "a worker processing multiple queue classes must present the same \
         verified principal in its audit records for each class"
    );
}
