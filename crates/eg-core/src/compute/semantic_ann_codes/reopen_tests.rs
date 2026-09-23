//! Reopening a binding's owner resolves its already-bound serving scope
//! read-only (`door::bind_scope`), and the reopened owner is fully usable:
//! the operator view answers the same outbox, and the normal write path still
//! commits -- so the ledger tables a skipped re-bootstrap would have created
//! are there.

use eg_transaction::{OutboxClaimBudget, OutboxView, OutboxViewAnswer};
use eg_types::contract::Nonce;
use eg_types::semantic_index::{
    SemanticBindingState, SemanticDigest, SemanticQueueClass, SemanticStage, SemanticStageIntent,
    SemanticStageIntentDraft, SemanticStagePredecessor, SemanticStageScope,
};

use super::tests::{open_store, pending_binding, tmp_dir};
use super::{stage_consumer, SemanticCodeStore};

const WORKER: &str = "operator-reopen";
const NOW: u64 = 1_000;

fn intent(
    binding: &eg_types::semantic_index::SemanticBinding,
    entity: &str,
    seed: u8,
) -> SemanticStageIntent {
    SemanticStageIntent::create(SemanticStageIntentDraft {
        binding_id: binding.binding_id.clone(),
        binding_digest: binding.binding_digest,
        generation: binding.generation,
        scope: SemanticStageScope::Entity {
            source_entity_id: entity.to_string(),
        },
        source_revision: binding.source_revision.clone(),
        stage: SemanticStage::SourceCommit,
        predecessor: SemanticStagePredecessor::None,
        input_digest: SemanticDigest::from_bytes([seed; 32]),
    })
    .unwrap()
}

fn status(codes: &SemanticCodeStore) -> eg_types::mutation_outbox::MutationOutboxStatusView {
    let view = OutboxView::Status {
        consumer: stage_consumer(WORKER, SemanticQueueClass::Fast),
    };
    match codes.outbox_operator_view(&view, NOW).unwrap() {
        OutboxViewAnswer::Status(status) => status,
        other => panic!("a status view answered {other:?}"),
    }
}

#[test]
fn a_reopened_owner_answers_the_same_outbox_and_still_writes() {
    let dir = tmp_dir("operator-reopen");
    let binding = pending_binding(
        "sql-source:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb:epoch:1",
    );
    let before = {
        let codes = open_store(&dir);
        codes.store_binding(&binding, 1).unwrap();
        codes
            .transition_binding_operation(
                1,
                SemanticBindingState::Building,
                2,
                "actor-reopen",
                "reopen-build",
                Nonce::from_bytes([81; 32]),
            )
            .unwrap();
        codes.subscribe_stage_consumer(WORKER).unwrap();
        codes
            .enqueue_stage_intent(&intent(&binding, "e1", 1), 3)
            .unwrap();
        codes
            .enqueue_stage_intent(&intent(&binding, "e2", 2), 4)
            .unwrap();
        status(&codes)
    };
    assert_eq!(before.status.pending, 2);

    // (a) the reopened owner answers the identical view.
    let codes = open_store(&dir);
    assert_eq!(status(&codes), before);

    // (b) the ordinary write path still commits after a read-only reopen.
    codes
        .enqueue_stage_intent(&intent(&binding, "e3", 3), 5)
        .unwrap();
    assert_eq!(status(&codes).status.pending, 3);
    let mut budget = OutboxClaimBudget::new(32, 5_000, NOW).unwrap();
    let claimed = codes.claim_stage_leases(WORKER, &mut budget).unwrap();
    assert_eq!(claimed.claims.len(), 3);
    drop(codes);
    let _ = std::fs::remove_dir_all(&dir);
}
