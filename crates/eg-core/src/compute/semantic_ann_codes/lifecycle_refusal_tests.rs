//! Branch proofs for the store's error rendering, binding admission and
//! lifecycle refusals, binding-head corruption, S6's binding-state rule, and
//! refresh proofs.
//!
//! Each refusal is identified by its own rule's message, and each fixture is
//! shown to be admissible once the rule it breaks is repaired.

use std::path::PathBuf;
use std::sync::Arc;

use eg_storage::{OwnerLayout, SEMANTIC_BINDINGS, SEMANTIC_HEADS};
use eg_types::contract::Nonce;
use eg_types::semantic_index::{
    SemanticBinding, SemanticBindingState, SemanticBindingStateTransition, SemanticDigest,
    SemanticIndexMutation, SemanticSourceProgress, SemanticSqlSourceIdentity,
    SemanticSqlSourceManifest, SemanticSqlSourceManifestDraft,
};

use super::activation::live_transition;
use super::door::store_file_name;
use super::stage_pipeline_tests::REVISION;
use super::stage_refusal_tests::{
    completed_prior, put_progress_row, put_stage_row, refusal, revision, s1_of, seed_rows,
};
use super::tests::{
    binding_for_generation, open_store, pending_binding, sql_source_identity, tmp_dir, BINDING,
    TENANT,
};
use super::{OperationAttribution, SemanticCodeError, SemanticCodeStore, SemanticMutationReceipt};
use crate::test_scope_grant::{TestScopeVerifier, TEST_PRINCIPAL, TEST_PROOF};

const ACTOR: &str = "actor-lifecycle-refusal";

fn digest(seed: u8) -> SemanticDigest {
    SemanticDigest::from_bytes([seed; 32])
}

fn nonce(seed: u8) -> Nonce {
    Nonce::from_bytes([seed; 32])
}

fn advanced(binding: &SemanticBinding, next: SemanticBindingState) -> SemanticBinding {
    let transition = SemanticBindingStateTransition::create(binding, next, "test").unwrap();
    binding.apply_state_transition(&transition).unwrap()
}

fn open_binding(
    dir: &std::path::Path,
    binding: &str,
) -> Result<SemanticCodeStore, SemanticCodeError> {
    SemanticCodeStore::open(
        dir,
        Arc::new(TestScopeVerifier {
            layout: OwnerLayout::SemanticIndex,
        }),
        TEST_PRINCIPAL,
        TEST_PROOF,
        TENANT,
        binding,
    )
}

/// One store in its own directory, removed when the test ends.
struct Scratch {
    dir: PathBuf,
    codes: SemanticCodeStore,
}

impl Scratch {
    fn open(tag: &str) -> Self {
        let dir = tmp_dir(tag);
        let codes = open_store(&dir);
        Self { dir, codes }
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

#[test]
fn errors_and_the_store_render_their_context_without_the_door() {
    let io = SemanticCodeError::from(std::io::Error::other("disk gone"));
    assert!(matches!(&io, SemanticCodeError::Io(error) if error.to_string() == "disk gone"));
    assert_eq!(io.to_string(), "semantic code store io error: disk gone");
    for (error, rendered) in [
        (
            SemanticCodeError::Kernel("k".to_string()),
            "semantic code store kernel error: k",
        ),
        (
            SemanticCodeError::Corrupt("c".to_string()),
            "semantic code store content error: c",
        ),
        (
            SemanticCodeError::Refused("r".to_string()),
            "semantic code store refused: r",
        ),
    ] {
        assert_eq!(error.to_string(), rendered);
    }

    let scratch = Scratch::open("refusal-render");
    assert_eq!(
        format!("{:?}", scratch.codes),
        format!("SemanticCodeStore {{ tenant: {TENANT:?}, binding: {BINDING:?}, .. }}")
    );
    // The open store's own file is not a directory, so a store cannot be
    // created beneath it.
    let owner_file = scratch.dir.join(store_file_name(TENANT, BINDING));
    assert!(owner_file.is_file());
    let opened = open_binding(&owner_file.join("store"), BINDING);
    assert!(matches!(opened, Err(SemanticCodeError::Io(_))));
}

#[test]
fn a_binding_outside_this_owner_or_pending_state_is_never_admitted() {
    let scratch = Scratch::open("refusal-binding-owner");
    let pending = pending_binding(REVISION);
    let foreign = open_binding(&scratch.dir, "semantic-binding-b").unwrap();
    let outside = refusal(foreign.store_binding(&pending, 1));
    assert!(
        outside.contains("outside this store's authenticated owner"),
        "{outside}"
    );
    let outside_operation =
        refusal(foreign.store_binding_operation(&pending, 2, ACTOR, "foreign-binding", nonce(1)));
    assert!(
        outside_operation.contains("outside this store's authenticated owner"),
        "{outside_operation}"
    );
    assert!(foreign.read_binding().unwrap().is_none());
    drop(foreign);

    let codes = &scratch.codes;
    let building =
        refusal(codes.store_binding(&advanced(&pending, SemanticBindingState::Building), 3));
    assert!(
        building.contains("admission requires pending durable state"),
        "{building}"
    );
    let live = refusal(codes.transition_binding_operation(
        1,
        SemanticBindingState::Live,
        4,
        ACTOR,
        "go-live",
        nonce(2),
    ));
    assert!(
        live.contains("may only start a pending build or disable a live binding"),
        "{live}"
    );
    let unbound = refusal(codes.transition_binding_operation(
        1,
        SemanticBindingState::Building,
        5,
        ACTOR,
        "build-unbound",
        nonce(3),
    ));
    assert!(
        unbound.contains("state transition has no durable binding"),
        "{unbound}"
    );
    let undroppable = refusal(codes.drop_binding_operation(1, 6, ACTOR, "drop-unbound", nonce(4)));
    assert!(
        undroppable.contains("drop has no durable binding"),
        "{undroppable}"
    );
    assert!(codes.read_binding().unwrap().is_none());
    codes.store_binding(&pending, 7).unwrap();
}

#[test]
fn an_admitted_binding_is_neither_replaced_nor_admitted_under_a_second_key() {
    let scratch = Scratch::open("refusal-binding-admitted");
    let codes = &scratch.codes;
    let pending = pending_binding(REVISION);
    codes.store_binding(&pending, 1).unwrap();
    let different = binding_for_generation(&revision('a', 2), 1);
    let replaced = refusal(codes.store_binding(&different, 2));
    assert!(
        replaced.contains("identity already names different bytes"),
        "{replaced}"
    );
    let second_key =
        refusal(codes.store_binding_operation(&pending, 3, ACTOR, "second-key", nonce(5)));
    assert!(
        second_key.contains("already admitted; retry its original idempotency key"),
        "{second_key}"
    );
    let different_key =
        refusal(codes.store_binding_operation(&different, 4, ACTOR, "different-key", nonce(6)));
    assert!(
        different_key.contains("identity already names different bytes"),
        "{different_key}"
    );
    assert_eq!(codes.read_binding().unwrap(), Some(pending.clone()));
    assert!(codes.store_binding(&pending, 5).unwrap().replayed);
}

#[test]
fn a_binding_head_without_its_exact_row_reads_as_corruption() {
    let scratch = Scratch::open("refusal-binding-head");
    let codes = &scratch.codes;
    let pending = pending_binding(REVISION);
    codes.store_binding(&pending, 1).unwrap();
    seed_rows(codes, "head-without-row", 61, |_, rows| {
        rows.open_table(SEMANTIC_HEADS)
            .unwrap()
            .insert((TENANT, BINDING), 2)
            .unwrap();
        Ok(())
    });
    let missing = codes.read_binding().expect_err("a head without its row");
    assert!(
        matches!(&missing, SemanticCodeError::Corrupt(message)
            if message.contains("names a missing binding row")),
        "{missing}"
    );
    let generation_one = pending.to_canonical_cbor().unwrap();
    seed_rows(codes, "head-over-foreign-row", 62, |_, rows| {
        rows.open_table(SEMANTIC_BINDINGS)
            .unwrap()
            .insert((TENANT, BINDING, 2), generation_one.as_slice())
            .unwrap();
        Ok(())
    });
    let mismatched = codes.read_binding().expect_err("a head over another row");
    assert!(
        matches!(&mismatched, SemanticCodeError::Corrupt(message)
            if message.contains("does not match its serving head")),
        "{mismatched}"
    );
}

#[test]
fn s6_publishes_only_from_a_durably_building_binding() {
    let pending = pending_binding(REVISION);
    let refused = |binding: &SemanticBinding| refusal(live_transition(binding));
    let from_pending = refused(&pending);
    assert!(
        from_pending.contains("durable Pending-to-Building transition before publication"),
        "{from_pending}"
    );
    let building = advanced(&pending, SemanticBindingState::Building);
    let live = advanced(&building, SemanticBindingState::Live);
    let from_live = refused(&live);
    assert!(
        from_live.contains("already live without an exact replay record"),
        "{from_live}"
    );
    let from_disabled = refused(&advanced(&live, SemanticBindingState::Disabled));
    assert!(
        from_disabled.contains("cannot publish a disabled, dropping, or failed binding"),
        "{from_disabled}"
    );
    let published = live_transition(&building).unwrap();
    assert_eq!(published.next, SemanticBindingState::Live);
}

/// A refresh's replacement binding, its source manifest, and its S1 intent.
#[derive(Clone)]
struct RefreshRequest {
    replacement: SemanticBinding,
    manifest: SemanticSqlSourceManifest,
    intent: eg_types::semantic_index::SemanticStageIntent,
}

fn refresh_request(
    generation: u64,
    source_revision: &str,
    identity: &SemanticSqlSourceIdentity,
) -> RefreshRequest {
    let replacement = binding_for_generation(source_revision, generation);
    let input = digest(103);
    let components = &replacement.policy_identity.components;
    let manifest = SemanticSqlSourceManifest::create(SemanticSqlSourceManifestDraft {
        binding_id: replacement.binding_id.clone(),
        binding_digest: replacement.binding_digest,
        generation,
        source_identity: identity.clone(),
        source_revision: source_revision.to_string(),
        source_content_digest: input,
        source_schema_revision: 1,
        source_schema_digest: replacement.source_schema_digest.clone(),
        source_field_set_digest: replacement.source_field_set_digest.clone(),
        source_acl_revision: components.source_acl_revision,
        source_acl_digest: components.source_acl_digest.clone(),
        authorization_receipt_digest: digest(104),
        completed_receipt_digest: digest(105),
        completed_at: "2026-09-08T00:07:00Z".to_string(),
    })
    .unwrap();
    let intent = s1_of(
        &replacement,
        &identity.source_entity_id(),
        source_revision,
        input,
    );
    RefreshRequest {
        replacement,
        manifest,
        intent,
    }
}

fn refresh(
    codes: &SemanticCodeStore,
    expected_generation: u64,
    request: &RefreshRequest,
    idempotency_key: &str,
    seed: u8,
) -> Result<SemanticMutationReceipt, SemanticCodeError> {
    codes.refresh_binding_operation_with_s1(
        expected_generation,
        &request.replacement,
        &request.manifest,
        &request.intent,
        8,
        OperationAttribution {
            actor: ACTOR,
            idempotency_key,
            nonce: nonce(seed),
        },
    )
}

fn live_binding() -> SemanticBinding {
    let building = advanced(&pending_binding(REVISION), SemanticBindingState::Building);
    advanced(&building, SemanticBindingState::Live)
}

fn seed_live_head(codes: &SemanticCodeStore, live: &SemanticBinding) {
    let bytes = live.to_canonical_cbor().unwrap();
    seed_rows(codes, "live-head", 90, |_, rows| {
        rows.open_table(SEMANTIC_BINDINGS)
            .unwrap()
            .insert((TENANT, BINDING, 1), bytes.as_slice())
            .unwrap();
        rows.open_table(SEMANTIC_HEADS)
            .unwrap()
            .insert((TENANT, BINDING), 1)
            .unwrap();
        Ok(())
    });
}

#[test]
fn a_refresh_request_is_refused_before_it_reads_a_live_predecessor() {
    let scratch = Scratch::open("refusal-refresh-request");
    let codes = &scratch.codes;
    let identity = sql_source_identity(&live_binding(), 101);
    let epoch_two = revision('a', 2);
    let request = refresh_request(2, &epoch_two, &identity);
    let unbound = RefreshRequest {
        intent: s1_of(
            &request.replacement,
            &identity.source_entity_id(),
            &epoch_two,
            digest(99),
        ),
        ..request.clone()
    };
    let content = refusal(refresh(codes, 1, &unbound, "unbound-intent", 11));
    assert!(
        content.contains("not bound to the replacement source manifest"),
        "{content}"
    );
    let unseeded = refusal(refresh(codes, 1, &request, "no-binding", 12));
    assert!(
        unseeded.contains("refresh has no durable binding"),
        "{unseeded}"
    );
    let building = RefreshRequest {
        replacement: advanced(&request.replacement, SemanticBindingState::Building),
        ..request.clone()
    };
    let owner = refusal(refresh(codes, 1, &building, "not-pending", 13));
    assert!(owner.contains("outside this pending owner"), "{owner}");
}

#[test]
fn a_refresh_replaces_only_the_expected_live_generation_with_its_successor() {
    let scratch = Scratch::open("refusal-refresh-head");
    let codes = &scratch.codes;
    let live = live_binding();
    seed_live_head(codes, &live);
    let identity = sql_source_identity(&live, 101);
    let entity = identity.source_entity_id();
    let (prior, prior_row, receipt_key) = completed_prior(&live, &entity, REVISION, 1);
    let prior_bytes = prior.to_canonical_cbor().unwrap();
    put_progress_row(codes, "prior", 91, &entity, &prior_bytes);
    put_stage_row(codes, "prior-receipt", 92, &receipt_key, &prior_row);

    let epoch_two = revision('a', 2);
    let request = refresh_request(2, &epoch_two, &identity);
    let expected = refusal(refresh(codes, 2, &request, "wrong-generation", 21));
    assert!(
        expected.contains("requires the expected live generation"),
        "{expected}"
    );
    let skipped = refresh_request(3, &epoch_two, &identity);
    let authority = refusal(refresh(codes, 1, &skipped, "skipped-generation", 22));
    assert!(
        authority.contains("changed an immutable binding authority"),
        "{authority}"
    );
    let unseen = refresh_request(2, &epoch_two, &sql_source_identity(&live, 111));
    let progress = refusal(refresh(codes, 1, &unseen, "unseen-entity", 23));
    assert!(
        progress.contains("lacks the old generation source progress proof"),
        "{progress}"
    );

    let admitted = refresh(codes, 1, &request, "admitted", 24).unwrap();
    assert!(!admitted.replayed);
    assert_eq!(codes.read_binding().unwrap().unwrap().generation, 2);
}

#[test]
fn a_refresh_predecessor_must_be_completed_exact_current_and_unsuperseded() {
    let scratch = Scratch::open("refusal-refresh-predecessor");
    let codes = &scratch.codes;
    let live = live_binding();
    seed_live_head(codes, &live);
    let identity = sql_source_identity(&live, 101);
    let entity = identity.source_entity_id();
    let epoch_two = revision('a', 2);
    let request = refresh_request(2, &epoch_two, &identity);
    let (prior, prior_row, receipt_key) = completed_prior(&live, &entity, REVISION, 1);
    let put_prior = |tag: &str, seed: u8, progress: &SemanticSourceProgress| {
        let bytes = progress.to_canonical_cbor().unwrap();
        put_progress_row(codes, tag, seed, &entity, &bytes);
    };

    put_prior(
        "incomplete",
        93,
        &SemanticSourceProgress {
            completed_stage: None,
            completed_receipt_digest: None,
            ..prior.clone()
        },
    );
    let incomplete = refusal(refresh(codes, 1, &request, "incomplete", 31));
    assert!(
        incomplete.contains("requires a completed predecessor receipt"),
        "{incomplete}"
    );

    put_prior("completed", 94, &prior);
    let binding_row = SemanticIndexMutation::StoreBinding {
        binding: Box::new(pending_binding(REVISION)),
    }
    .to_canonical_cbor()
    .unwrap();
    put_stage_row(codes, "non-stage-receipt", 95, &receipt_key, &binding_row);
    let inexact = refusal(refresh(codes, 1, &request, "non-stage", 32));
    assert!(
        inexact.contains("not the exact durable stage receipt"),
        "{inexact}"
    );

    put_stage_row(codes, "exact-receipt", 96, &receipt_key, &prior_row);
    put_prior(
        "superseded",
        97,
        &SemanticSourceProgress {
            superseded_by_revision: Some(revision('a', 3)),
            ..prior.clone()
        },
    );
    let superseded = refusal(refresh(codes, 1, &request, "superseded", 33));
    assert!(
        superseded.contains("was already superseded"),
        "{superseded}"
    );

    let (newer, newer_row, newer_key) = completed_prior(&live, &entity, &epoch_two, 2);
    put_prior("newer", 98, &newer);
    put_stage_row(codes, "newer-receipt", 99, &newer_key, &newer_row);
    let beyond = refresh_request(2, &revision('a', 3), &identity);
    let stale = refusal(refresh(codes, 1, &beyond, "newer", 34));
    assert!(stale.contains("predecessor proof is stale"), "{stale}");

    put_prior("current", 100, &prior);
    let admitted = refresh(codes, 1, &request, "admitted", 35).unwrap();
    assert!(!admitted.replayed);
}
