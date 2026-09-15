//! Focused proofs for the caller-attributed disable and drop lifecycle.
//!
//! No other test drives `drop_binding_operation`, or the `Live -> Disabled`
//! branch of `transition_binding_operation` that removes the active pointer.
//! Both were restructured around one re-read of the binding they decided
//! against and one binding-state write, so these tests pin every refusal and
//! effect of both paths, including their idempotent replays.

use eg_storage::{SEMANTIC_BINDINGS, SEMANTIC_HEADS, SEMANTIC_POINTERS, SEMANTIC_TOMBSTONES};
use eg_types::contract::Nonce;
use eg_types::semantic_index::{
    SemanticActivePointer, SemanticBinding, SemanticBindingState, SemanticBindingStateTransition,
    SemanticDigest, SemanticTombstone,
};

use super::batch::MetadataMutation;
use super::tests::{binding_for_generation, open_store, tmp_dir, BINDING, TENANT};
use super::{SemanticCodeError, SemanticCodeStore, SemanticMutationReceipt};

const REVISION: &str =
    "sql-source:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:epoch:1";
const ACTOR: &str = "actor-lifecycle";

fn nonce(seed: u8) -> Nonce {
    Nonce::from_bytes([seed; 32])
}

fn advanced(binding: &SemanticBinding, next: SemanticBindingState) -> SemanticBinding {
    let transition = SemanticBindingStateTransition::create(binding, next, "test").unwrap();
    binding.apply_state_transition(&transition).unwrap()
}

/// A durably live generation-one binding with the active pointer that serves
/// it, seeded through the store's own serving-scope door.
fn seed_live_binding(codes: &SemanticCodeStore) -> SemanticBinding {
    let pending = binding_for_generation(REVISION, 1);
    let live = advanced(
        &advanced(&pending, SemanticBindingState::Building),
        SemanticBindingState::Live,
    );
    let pointer = SemanticActivePointer {
        tenant_id: TENANT.to_string(),
        binding_id: BINDING.to_string(),
        binding_digest: live.binding_digest,
        generation: 1,
        source_revision: live.source_revision.clone(),
        vector_target_id: live.vector_target_id.clone(),
        lexical_index_identity: live.lexical_index_identity.clone(),
        ann_index_identity: live.ann_index_identity.clone(),
        composite_policy_digest: live.policy_digest.clone(),
        activation_receipt_digest: SemanticDigest::from_bytes([111; 32]),
        activated_at: "2026-09-08T00:08:00Z".to_string(),
    };
    let digest = SemanticDigest::from_bytes([0x5d; 32]);
    let mutation = MetadataMutation {
        batch_id: "semantic-index:lifecycle-seed",
        event_type: "semantic_lifecycle_seed",
        subject: "generation:1",
        mutation_digest: digest,
    };
    let live_bytes = live.to_canonical_cbor().unwrap();
    let pointer_bytes = pointer.to_canonical_cbor().unwrap();
    codes
        .door
        .commit_metadata_fenced(
            |version| codes.metadata_batch(codes.door.owner(), version, mutation, Vec::new(), 1),
            digest,
            1,
            None,
            |_, rows| {
                let owner = (TENANT, BINDING);
                let mut bindings = rows.open_table(SEMANTIC_BINDINGS).unwrap();
                bindings
                    .insert((TENANT, BINDING, 1), live_bytes.as_slice())
                    .unwrap();
                drop(bindings);
                rows.open_table(SEMANTIC_HEADS)
                    .unwrap()
                    .insert(owner, 1)
                    .unwrap();
                let mut pointers = rows.open_table(SEMANTIC_POINTERS).unwrap();
                pointers.insert(owner, pointer_bytes.as_slice()).unwrap();
                Ok(())
            },
        )
        .unwrap();
    live
}

fn disable(
    codes: &SemanticCodeStore,
    generation: u64,
    key: &str,
    seed: u8,
) -> Result<SemanticMutationReceipt, SemanticCodeError> {
    codes.transition_binding_operation(
        generation,
        SemanticBindingState::Disabled,
        u64::from(seed),
        ACTOR,
        key,
        nonce(seed),
    )
}

fn refusal(result: Result<SemanticMutationReceipt, SemanticCodeError>) -> String {
    result
        .expect_err("the lifecycle operation must be refused")
        .to_string()
}

fn durable_state(codes: &SemanticCodeStore) -> SemanticBindingState {
    codes.read_binding().unwrap().unwrap().durable_state
}

fn tombstone_of(codes: &SemanticCodeStore, generation: u64) -> Option<SemanticTombstone> {
    let read = codes.door.serving_read().unwrap();
    let tombstones = read.open_owner_table(SEMANTIC_TOMBSTONES).unwrap();
    let raw = tombstones.get((TENANT, BINDING, generation)).unwrap()?;
    Some(SemanticTombstone::from_canonical_cbor(raw.value()).unwrap())
}

#[test]
fn disabling_a_live_binding_removes_its_active_pointer_and_replays() {
    let dir = tmp_dir("lifecycle-disable");
    let codes = open_store(&dir);
    seed_live_binding(&codes);
    assert_eq!(codes.live_generation().unwrap(), Some(1));

    let stale = refusal(disable(&codes, 2, "disable-stale", 1));
    assert!(stale.contains("generation is stale"), "{stale}");
    let building = codes.transition_binding_operation(
        1,
        SemanticBindingState::Building,
        2,
        ACTOR,
        "build-live",
        nonce(2),
    );
    let invalid = refusal(building);
    assert!(
        invalid.contains("not valid for the durable binding state"),
        "{invalid}"
    );

    let disabled = disable(&codes, 1, "disable", 3).unwrap();
    assert!(!disabled.replayed);
    assert_eq!(durable_state(&codes), SemanticBindingState::Disabled);
    assert_eq!(
        codes.live_generation().unwrap(),
        None,
        "disabling must remove the active pointer in the same write"
    );

    let replay = disable(&codes, 1, "disable", 4).unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.batch_id, disabled.batch_id);
    let changed = refusal(disable(&codes, 2, "disable", 5));
    assert!(changed.contains("different content"), "{changed}");
    let again = refusal(disable(&codes, 1, "disable-again", 6));
    assert!(
        again.contains("not valid for the durable binding state"),
        "{again}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn dropping_requires_a_disabled_generation_then_tombstones_it_once() {
    let dir = tmp_dir("lifecycle-drop");
    let codes = open_store(&dir);
    let live = seed_live_binding(&codes);

    let unattributed = refusal(codes.drop_binding_operation(1, 1, " ", "drop", nonce(1)));
    assert!(unattributed.contains("verified actor"), "{unattributed}");
    let while_live = refusal(codes.drop_binding_operation(1, 2, ACTOR, "drop-live", nonce(2)));
    assert!(
        while_live.contains("expected disabled or failed generation"),
        "{while_live}"
    );
    assert_eq!(tombstone_of(&codes, 1), None);

    disable(&codes, 1, "disable", 3).unwrap();
    let stale = refusal(codes.drop_binding_operation(2, 4, ACTOR, "drop-stale", nonce(4)));
    assert!(
        stale.contains("expected disabled or failed generation"),
        "{stale}"
    );

    let dropped = codes
        .drop_binding_operation(1, 5, ACTOR, "drop", nonce(5))
        .unwrap();
    assert!(!dropped.replayed);
    assert_eq!(durable_state(&codes), SemanticBindingState::Dropping);
    let tombstone = tombstone_of(&codes, 1).expect("a drop writes the generation's tombstone");
    assert_eq!(
        (tombstone.binding_digest, tombstone.generation),
        (live.binding_digest, 1)
    );

    let replay = codes
        .drop_binding_operation(1, 6, ACTOR, "drop", nonce(6))
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.batch_id, dropped.batch_id);
    assert_eq!(tombstone_of(&codes, 1), Some(tombstone));
    let changed = refusal(codes.drop_binding_operation(2, 7, ACTOR, "drop", nonce(7)));
    assert!(changed.contains("different content"), "{changed}");
    let _ = std::fs::remove_dir_all(&dir);
}
