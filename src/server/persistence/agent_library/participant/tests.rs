//! Real owner-store reservation, writer exclusion and recovery tests.
use super::*;
use crate::server::persistence::agent_fixtures::{mutation_context, open_agent_store};
use crate::server::persistence::decision_record::tests::{
    commit_context, decided, seed_library, TENANT,
};
use eg_types::AgentComponentRetireRequest;

struct Decision(Option<bool>);
impl LibraryReservationDecisionSource for Decision {
    fn terminal_decision(
        &self,
        record: &LibraryReservation,
    ) -> Result<Option<CoordinatorDecisionCertificate>, String> {
        Ok(self.0.map(|commit| CoordinatorDecisionCertificate {
            version: 1,
            binding: record.binding.clone(),
            commit,
            decided_at_ms: 99,
        }))
    }
}

fn binding(transaction_id: &str) -> CoordinatorDecisionBinding {
    CoordinatorDecisionBinding {
        transaction_id: transaction_id.into(),
        tenant: TENANT.into(),
        coordinator_scope: "coordinator-incarnation-1".into(),
        request_digest: [1; 32],
        authority_deadline_ms: 100,
    }
}

fn prepare(store: &AgentLibraryStore) -> LibraryReservation {
    let owner = store.scope_handle(TENANT).unwrap();
    let txn = store.mutations.open_write(&owner).unwrap();
    let version = store.mutations.current_version(&txn, &owner).unwrap();
    txn.abort().unwrap();
    store
        .prepare_library_reservation(
            binding("admission-1"),
            version,
            commit_context(store, "reserved-decision", 1),
            decided(store).record,
        )
        .unwrap()
}

fn retire(store: &AgentLibraryStore) -> Result<eg_types::AgentComponentWriteResult, String> {
    store.retire_component(AgentComponentRetireRequest {
        context: mutation_context(
            store,
            TENANT,
            "retire-model",
            2,
            1,
            "agent-component:retire",
        ),
        component_id: "model-a".into(),
    })
}

// spec: EG-DECISION-ENGINE-R036.104
#[test]
fn pending_survives_reopen_and_blocks_withdrawal_until_proven_abort() {
    let (dir, store) = open_agent_store();
    seed_library(&store);
    let record = prepare(&store);
    assert!(retire(&store)
        .unwrap_err()
        .contains("unresolved reservation"));
    drop(store);
    let store = AgentLibraryStore::open(dir.path().to_str().unwrap()).unwrap();
    assert_eq!(
        store.pending_library_reservation().unwrap(),
        Some(record.clone())
    );
    assert!(!store
        .resolve_library_reservation(&record, &Decision(None))
        .unwrap());
    assert!(retire(&store).is_err());
    assert!(store
        .resolve_library_reservation(&record, &Decision(Some(false)))
        .unwrap());
    assert!(store.pending_library_reservation().unwrap().is_none());
    assert!(store
        .decision_record_body(TENANT, &record.record.record_id)
        .unwrap()
        .is_none());
    assert!(store
        .resolve_library_reservation(&record, &Decision(Some(false)))
        .unwrap());
    retire(&store).unwrap();
}

// spec: EG-DECISION-ENGINE-R036.104
#[test]
fn commit_materializes_sealed_decision_and_consumes_prepare_atomically() {
    let (dir, store) = open_agent_store();
    seed_library(&store);
    let reservation = prepare(&store);
    assert!(store
        .commit_decision_record(
            commit_context(&store, "other", 3),
            &reservation.record,
            &reservation.record.inputs.catalog_digest
        )
        .is_err());
    assert!(store
        .resolve_library_reservation(&reservation, &Decision(Some(true)))
        .unwrap());
    assert!(store.pending_library_reservation().unwrap().is_none());
    let content = store
        .decision_record_content(TENANT, &reservation.record.record_id)
        .unwrap();
    let actual: DecisionRecord = serde_json::from_slice(&content.body).unwrap();
    assert_eq!(actual, reservation.record);
    drop(store);
    let store = AgentLibraryStore::open(dir.path().to_str().unwrap()).unwrap();
    assert!(store
        .resolve_library_reservation(&reservation, &Decision(Some(true)))
        .unwrap());
    assert!(store.pending_library_reservation().unwrap().is_none());
}

// spec: EG-DECISION-ENGINE-R036.104
#[test]
fn changed_intent_and_stale_prepare_refuse_without_releasing_original() {
    let (_dir, store) = open_agent_store();
    seed_library(&store);
    let original = prepare(&store);
    assert_eq!(
        store
            .prepare_library_reservation(
                original.binding.clone(),
                original.source_version,
                original.context.clone(),
                original.record.clone()
            )
            .unwrap(),
        original
    );
    assert!(store
        .prepare_library_reservation(
            CoordinatorDecisionBinding {
                request_digest: [2; 32],
                ..original.binding.clone()
            },
            original.source_version,
            original.context.clone(),
            original.record.clone()
        )
        .is_err());
    assert_eq!(
        store.pending_library_reservation().unwrap(),
        Some(original.clone())
    );
    store
        .resolve_library_reservation(&original, &Decision(Some(false)))
        .unwrap();
    assert!(store
        .prepare_library_reservation(
            original.binding.clone(),
            original.source_version,
            original.context.clone(),
            original.record.clone()
        )
        .is_err());
    assert!(store.pending_library_reservation().unwrap().is_none());
}

// spec: EG-DECISION-ENGINE-R036.104
#[test]
fn terminal_commit_cannot_reinterpret_a_proven_abort() {
    let (_dir, store) = open_agent_store();
    seed_library(&store);
    let record = prepare(&store);
    store
        .resolve_library_reservation(&record, &Decision(Some(false)))
        .unwrap();
    assert!(store
        .resolve_library_reservation(&record, &Decision(Some(true)))
        .unwrap_err()
        .contains("already aborted"));
    assert!(store
        .decision_record_body(TENANT, &record.record.record_id)
        .unwrap()
        .is_none());
}

// spec: EG-DECISION-ENGINE-R036.104
#[test]
fn stale_head_refuses_before_reserving_any_owner_rows() {
    let (_dir, store) = open_agent_store();
    seed_library(&store);
    let decision = decided(&store).record;
    retire(&store).unwrap();
    let owner = store.scope_handle(TENANT).unwrap();
    let txn = store.mutations.open_write(&owner).unwrap();
    let version = store.mutations.current_version(&txn, &owner).unwrap();
    txn.abort().unwrap();
    assert!(store
        .prepare_library_reservation(
            binding("stale"),
            version,
            commit_context(&store, "stale-decision", 4),
            decision
        )
        .is_err());
    assert!(store.pending_library_reservation().unwrap().is_none());
}

// spec: EG-DECISION-ENGINE-R036.104
#[test]
fn maintenance_cannot_advance_reserved_owner_version_or_strand_abort() {
    let (_dir, store) = open_agent_store();
    seed_library(&store);
    let record = prepare(&store);
    let invoked = std::cell::Cell::new(false);
    let error = store
        .maintain_control_rows(TENANT, "decision_artifacts", |_| {
            invoked.set(true);
            Ok(())
        })
        .unwrap_err();
    assert!(error.contains("unresolved reservation"));
    assert!(!invoked.get());
    let owner = store.scope_handle(TENANT).unwrap();
    let txn = store.mutations.open_write(&owner).unwrap();
    assert_eq!(
        store.mutations.current_version(&txn, &owner).unwrap(),
        record.source_version + 1
    );
    txn.abort().unwrap();
    store
        .resolve_library_reservation(&record, &Decision(Some(false)))
        .unwrap();
    store
        .maintain_control_rows(TENANT, "decision_artifacts", |_| {
            invoked.set(true);
            Ok(())
        })
        .unwrap();
    assert!(invoked.get());
}

// spec: EG-DECISION-ENGINE-R036.104
#[test]
fn prepare_receipt_uses_the_ledger_messagepack_contract() {
    let (_dir, store) = open_agent_store();
    seed_library(&store);
    let record = prepare(&store);
    let owner = store.scope_handle(TENANT).unwrap();
    let read = store.kernel.read_scope(&owner).unwrap();
    let prepare = batch(&store, &record, "prepare", record.source_version).unwrap();
    let receipt = eg_transaction::read_ledger(&read, &prepare.batch_id)
        .unwrap()
        .unwrap();
    let decoded: Option<LibraryReservation> =
        rmp_serde::from_slice(receipt.result_msgpack.as_ref().unwrap()).unwrap();
    assert_eq!(decoded, Some(record));
}

struct StoredCertificate(CoordinatorDecisionCertificate);
impl LibraryReservationDecisionSource for StoredCertificate {
    fn terminal_decision(
        &self,
        _: &LibraryReservation,
    ) -> Result<Option<CoordinatorDecisionCertificate>, String> {
        Ok(Some(self.0.clone()))
    }
}

// spec: EG-DECISION-ENGINE-R036.104
#[test]
fn certificate_must_bind_every_prepare_field_and_original_decision_time() {
    let (_dir, store) = open_agent_store();
    seed_library(&store);
    let record = prepare(&store);
    let good = CoordinatorDecisionCertificate {
        version: 1,
        binding: record.binding.clone(),
        commit: true,
        decided_at_ms: 99,
    };
    let mut mismatches = Vec::new();
    let mut wrong = good.clone();
    wrong.binding.transaction_id.push_str("-other");
    mismatches.push(wrong);
    let mut wrong = good.clone();
    wrong.binding.tenant.push_str("-other");
    mismatches.push(wrong);
    let mut wrong = good.clone();
    wrong.binding.coordinator_scope.push_str("-other");
    mismatches.push(wrong);
    let mut wrong = good.clone();
    wrong.binding.request_digest = [2; 32];
    mismatches.push(wrong);
    let mut wrong = good.clone();
    wrong.binding.authority_deadline_ms += 1;
    mismatches.push(wrong);
    let mut wrong = good.clone();
    wrong.decided_at_ms = 100;
    mismatches.push(wrong);
    for wrong in mismatches {
        assert!(store
            .resolve_library_reservation(&record, &StoredCertificate(wrong))
            .is_err());
        assert_eq!(
            store.pending_library_reservation().unwrap(),
            Some(record.clone())
        );
        assert!(store
            .decision_record_body(TENANT, &record.record.record_id)
            .unwrap()
            .is_none());
    }
    // Delayed recovery uses the original pre-deadline stamp, not recovery time.
    assert!(store
        .resolve_library_reservation(&record, &StoredCertificate(good))
        .unwrap());
}

// spec: EG-DECISION-ENGINE-R036.104
#[test]
fn proven_abort_can_release_after_the_authority_deadline() {
    let (_dir, store) = open_agent_store();
    seed_library(&store);
    let record = prepare(&store);
    let proof = CoordinatorDecisionCertificate {
        version: 1,
        binding: record.binding.clone(),
        commit: false,
        decided_at_ms: 101,
    };
    assert!(store
        .resolve_library_reservation(&record, &StoredCertificate(proof))
        .unwrap());
    assert!(store.pending_library_reservation().unwrap().is_none());
    assert!(store
        .decision_record_body(TENANT, &record.record.record_id)
        .unwrap()
        .is_none());
}

// spec: EG-DECISION-ENGINE-R036.104
#[test]
fn resolved_transaction_cannot_claim_new_intent_at_a_fresh_version() {
    let (_dir, store) = open_agent_store();
    seed_library(&store);
    let original = prepare(&store);
    store
        .resolve_library_reservation(&original, &Decision(Some(false)))
        .unwrap();
    let owner = store.scope_handle(TENANT).unwrap();
    let txn = store.mutations.open_write(&owner).unwrap();
    let fresh_version = store.mutations.current_version(&txn, &owner).unwrap();
    txn.abort().unwrap();
    let changed = commit_context(&store, "changed-intent", 7);
    assert!(store
        .prepare_library_reservation(
            original.binding.clone(),
            fresh_version,
            changed.clone(),
            original.record.clone()
        )
        .is_err());
    assert!(store.pending_library_reservation().unwrap().is_none());
    let mut forged = original.clone();
    forged.context = changed;
    forged.source_version = fresh_version;
    // Even an otherwise valid original certificate cannot authorize changed intent.
    assert!(store
        .resolve_library_reservation(&forged, &Decision(Some(true)))
        .is_err());
    assert!(store
        .decision_record_body(TENANT, &original.record.record_id)
        .unwrap()
        .is_none());
}
