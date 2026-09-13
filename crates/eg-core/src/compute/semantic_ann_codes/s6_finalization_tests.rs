//! Branch proofs for S6 finalization beyond the one publication the main
//! integration test drives: proofs that do not match the lease, the replay
//! shortcut's refusals, and the prior active pointer's two edge cases.
//!
//! Every refusal here must leave generation one live and write no S6 stage
//! row; every publication must leave exactly the binding states it names.

use eg_storage::SemanticIndexOwner;
use eg_storage::{ANN_CODES, SEMANTIC_BINDINGS, SEMANTIC_POINTERS, SEMANTIC_STAGES};
use eg_transaction::OutboxClaimBudget;
use eg_transaction::{AdmittedMutation, AdmittedOwnerWrite};
use eg_types::mutation_batch::MutationOutboxLease;
use eg_types::semantic_index::{
    SemanticActivePointer, SemanticBinding, SemanticBindingState, SemanticDigest,
    SemanticIndexMutation,
};

use super::batch::MetadataMutation;
use super::tests::{
    binding_for_generation, seeded_s6_publication, warmed_store, S6Fixture, BINDING, TENANT,
};
use super::SemanticCodeError;
use crate::compute::semantic::SemanticGenerationImage;

fn claim(fixture: &S6Fixture) -> MutationOutboxLease {
    let mut budget = OutboxClaimBudget::new(1, 5_000, 11).unwrap();
    fixture
        .codes
        .claim_stage_leases(fixture.consumer, &mut budget)
        .unwrap()
        .claims
        .into_iter()
        .next()
        .expect("the seeded S6 intent is claimable")
}

fn finalize(
    fixture: &S6Fixture,
    lease: &MutationOutboxLease,
    checkpoint: &eg_types::semantic_index::SemanticGenerationCheckpoint,
    image: &SemanticGenerationImage,
) -> Result<super::SemanticMutationReceipt, SemanticCodeError> {
    fixture.codes.finalize_generation(
        lease,
        &fixture.s6_transition,
        checkpoint,
        &fixture.activation_artifact,
        image,
        13,
    )
}

/// Overwrite owner rows directly through the store's own serving-scope door.
fn rewrite<F>(fixture: &S6Fixture, tag: &str, seed: u8, apply: F)
where
    F: FnOnce(
        &AdmittedMutation<'_, SemanticIndexOwner>,
        &AdmittedOwnerWrite<'_, SemanticIndexOwner>,
    ) -> Result<(), SemanticCodeError>,
{
    let codes = &fixture.codes;
    let digest = SemanticDigest::from_bytes([seed; 32]);
    let batch_id = format!("semantic-index:s6-branch:{tag}");
    let mutation = MetadataMutation {
        batch_id: &batch_id,
        event_type: "semantic_s6_branch_seed",
        subject: "generation:2",
        mutation_digest: digest,
    };
    codes
        .door
        .commit_metadata_fenced(
            |version| codes.metadata_batch(codes.door.owner(), version, mutation, Vec::new(), 12),
            digest,
            12,
            None,
            apply,
        )
        .unwrap();
}

fn put_stage_row(fixture: &S6Fixture, tag: &str, seed: u8, mutation: &SemanticIndexMutation) {
    let bytes = mutation.to_canonical_cbor().unwrap();
    let key = fixture.s6_stage_key.clone();
    rewrite(fixture, tag, seed, |_, rows| {
        rows.open_table(SEMANTIC_STAGES)
            .unwrap()
            .insert((TENANT, BINDING, key.as_str()), bytes.as_slice())
            .unwrap();
        Ok(())
    });
}

fn put_pointer(fixture: &S6Fixture, tag: &str, seed: u8, pointer: Option<&SemanticActivePointer>) {
    let bytes = pointer.map(|pointer| pointer.to_canonical_cbor().unwrap());
    rewrite(fixture, tag, seed, |_, rows| {
        let mut pointers = rows.open_table(SEMANTIC_POINTERS).unwrap();
        match &bytes {
            Some(bytes) => pointers
                .insert((TENANT, BINDING), bytes.as_slice())
                .map(|_| ()),
            None => pointers.remove((TENANT, BINDING)).map(|_| ()),
        }
        .unwrap();
        Ok(())
    });
}

fn binding_at(fixture: &S6Fixture, generation: u64) -> SemanticBinding {
    let read = fixture.codes.door.serving_read().unwrap();
    let bindings = read.open_owner_table(SEMANTIC_BINDINGS).unwrap();
    let raw = bindings
        .get((TENANT, BINDING, generation))
        .unwrap()
        .unwrap();
    SemanticBinding::from_canonical_cbor(raw.value()).unwrap()
}

fn refusal(result: Result<super::SemanticMutationReceipt, SemanticCodeError>) -> String {
    result
        .expect_err("S6 finalization must be refused")
        .to_string()
}

/// Nothing was published: generation one still serves, generation two is
/// still building, and no S6 stage row exists.
fn assert_unpublished(fixture: &S6Fixture) {
    assert_eq!(fixture.codes.live_generation().unwrap(), Some(1));
    assert_eq!(
        fixture.codes.read_binding().unwrap().unwrap(),
        fixture.generation_two
    );
    assert!(fixture
        .codes
        .read_stage_mutation(fixture.s6_transition.intent.intent_digest)
        .unwrap()
        .is_none());
}

fn exact_finalization(fixture: &S6Fixture) -> SemanticIndexMutation {
    SemanticIndexMutation::FinalizeGeneration {
        checkpoint: Box::new(fixture.s6_checkpoint.clone()),
        artifact: fixture.activation_artifact.clone(),
    }
}

#[test]
fn s6_refuses_a_checkpoint_or_image_that_is_not_the_leased_publication() {
    let fixture = seeded_s6_publication("s6-branch-mismatch");
    let lease = claim(&fixture);

    let wrong_checkpoint = finalize(
        &fixture,
        &lease,
        &fixture.lexical_checkpoint,
        &fixture.image,
    );
    let refused = refusal(wrong_checkpoint);
    assert!(
        refused.contains("S6 checkpoint does not match its leased transition"),
        "{refused}"
    );

    let (other_model, _) = warmed_store(0);
    let other_image = other_model.export_generation().unwrap();
    let wrong_image = finalize(&fixture, &lease, &fixture.s6_checkpoint, &other_image);
    let refused = refusal(wrong_image);
    assert!(
        refused.contains("S6 ANN image does not match the durable binding model"),
        "{refused}"
    );

    assert_unpublished(&fixture);
    let _ = std::fs::remove_dir_all(&fixture.dir);
}

#[test]
fn s6_replay_refuses_a_stage_row_it_did_not_publish() {
    let fixture = seeded_s6_publication("s6-branch-replay");
    let lease = claim(&fixture);

    // A canonical mutation of another kind under the S6 intent's key.
    let foreign = SemanticIndexMutation::StoreBinding {
        binding: Box::new(binding_for_generation(fixture.revision_two, 2)),
    };
    put_stage_row(&fixture, "foreign-stage-row", 41, &foreign);
    let refused = refusal(finalize(
        &fixture,
        &lease,
        &fixture.s6_checkpoint,
        &fixture.image,
    ));
    assert!(
        refused.contains("already has a different durable finalization"),
        "{refused}"
    );

    // The exact finalization bytes without the live pointer that publishing
    // them would have moved: generation one still serves.
    put_stage_row(
        &fixture,
        "exact-stage-row",
        42,
        &exact_finalization(&fixture),
    );
    let refused = refusal(finalize(
        &fixture,
        &lease,
        &fixture.s6_checkpoint,
        &fixture.image,
    ));
    assert!(
        refused.contains("S6 replay has no matching durable live pointer"),
        "{refused}"
    );
    assert_eq!(fixture.codes.live_generation().unwrap(), Some(1));
    assert_eq!(
        fixture.codes.read_binding().unwrap().unwrap(),
        fixture.generation_two
    );
    let _ = std::fs::remove_dir_all(&fixture.dir);
}

#[test]
fn s6_rolls_back_when_the_active_pointer_names_a_future_generation() {
    let fixture = seeded_s6_publication("s6-branch-future-pointer");
    let lease = claim(&fixture);
    // A canonical pointer to a generation beyond the binding head.
    let generation_three = binding_for_generation(fixture.revision_two, 3);
    let future = SemanticActivePointer {
        binding_digest: generation_three.binding_digest,
        generation: 3,
        source_revision: generation_three.source_revision.clone(),
        vector_target_id: generation_three.vector_target_id.clone(),
        lexical_index_identity: generation_three.lexical_index_identity.clone(),
        ann_index_identity: generation_three.ann_index_identity.clone(),
        composite_policy_digest: generation_three.policy_digest.clone(),
        ..fixture.pointer_one.clone()
    };
    put_pointer(&fixture, "future-pointer", 43, Some(&future));

    let refused = refusal(finalize(
        &fixture,
        &lease,
        &fixture.s6_checkpoint,
        &fixture.image,
    ));
    assert!(
        refused.contains("S6 active pointer names a future generation"),
        "{refused}"
    );
    assert_eq!(
        fixture.codes.read_binding().unwrap().unwrap(),
        fixture.generation_two
    );
    assert_eq!(binding_at(&fixture, 1), fixture.generation_one_live);
    assert!(fixture
        .codes
        .read_stage_mutation(fixture.s6_transition.intent.intent_digest)
        .unwrap()
        .is_none());
    let _ = std::fs::remove_dir_all(&fixture.dir);
}

#[test]
fn s6_without_a_prior_active_pointer_publishes_without_demoting() {
    let fixture = seeded_s6_publication("s6-branch-no-pointer");
    let lease = claim(&fixture);
    put_pointer(&fixture, "no-pointer", 44, None);

    let receipt = finalize(&fixture, &lease, &fixture.s6_checkpoint, &fixture.image)
        .expect("S6 publishes with no prior active pointer");
    assert!(!receipt.replayed);
    assert_eq!(fixture.codes.live_generation().unwrap(), Some(2));
    assert_eq!(
        binding_at(&fixture, 1).durable_state,
        SemanticBindingState::Live,
        "with no prior pointer there is no generation to demote"
    );
    assert_eq!(
        binding_at(&fixture, 2).durable_state,
        SemanticBindingState::Live
    );
    assert_serving_reads(&fixture);
    let _ = std::fs::remove_dir_all(&fixture.dir);
}

/// The published image is served byte for byte, an unpublished generation is
/// absent, and every malformed code row reads as corruption rather than as a
/// short image.
fn assert_serving_reads(fixture: &S6Fixture) {
    let codes = &fixture.codes;
    let (generation, served) = codes.read_live().unwrap().expect("a live image is served");
    assert_eq!((generation, &served), (2, &fixture.image));
    assert_eq!(
        codes.read_generation(2).unwrap().as_ref(),
        Some(&fixture.image)
    );
    assert!(codes.read_generation(1).unwrap().is_none());

    let meta_len = fixture.image.index.codes.meta.len() as u64;
    let header = |tag: &str, seed: u8, bytes: Vec<u8>| {
        rewrite(fixture, tag, seed, move |_, rows| {
            let mut table = rows.open_table(ANN_CODES).unwrap();
            table
                .insert((TENANT, BINDING, 2, "meta"), bytes.as_slice())
                .unwrap();
            Ok(())
        });
    };
    header("short-meta", 45, (meta_len + 1).to_le_bytes().to_vec());
    let short = codes
        .read_generation(2)
        .expect_err("a short part is corrupt")
        .to_string();
    assert!(
        short.contains(&format!("restored {meta_len} of {} bytes", meta_len + 1)),
        "{short}"
    );
    header("lengthless-meta", 46, vec![1, 2, 3]);
    let lengthless = codes
        .read_generation(2)
        .expect_err("a header without a length is corrupt")
        .to_string();
    assert!(
        lengthless.contains("part `meta` has no length"),
        "{lengthless}"
    );
    header("restored-meta", 47, meta_len.to_le_bytes().to_vec());
    rewrite(fixture, "missing-chunk", 48, |_, rows| {
        let mut table = rows.open_table(ANN_CODES).unwrap();
        table.remove((TENANT, BINDING, 2, "meta:00000000")).unwrap();
        Ok(())
    });
    let missing = codes
        .read_live()
        .expect_err("a missing chunk is corrupt")
        .to_string();
    assert!(
        missing.contains("part `meta` chunk 0 is missing"),
        "{missing}"
    );
}
