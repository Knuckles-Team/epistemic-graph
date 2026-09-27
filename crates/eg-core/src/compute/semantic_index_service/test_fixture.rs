use eg_storage::{OwnerLayout, PhysicalStoreIdentity, ScopeGrantVerifier};
use eg_transaction::OutboxClaimBudget;
use eg_types::contract::Nonce;
use eg_types::mutation_batch::{MutationOutboxLease, MutationOutboxRecord};
use eg_types::semantic_index::{
    SemanticBinding, SemanticBindingState, SemanticDigest, SemanticSqlSourceIdentity, SqlColumnRef,
};
use eg_types::semantic_index::{SemanticIndexError, SemanticSourceDirtyIntent};
use eg_types::MutationScopeIdentity;
use std::sync::{Arc, Mutex};

use super::{
    SemanticIndexService, SemanticSqlSourceReadPage, SemanticSqlSourceReadPort,
    SemanticSqlSourceRecord, SemanticSqlSourceValue,
};

/// Test owner grant with the same tenant, principal and proof fences used by
/// the production open path. Suites supply their distinct authenticated
/// identities; no fixture silently grants a foreign tenant.
pub(super) struct TestSemanticScopeVerifier {
    pub tenant: &'static str,
    pub principal: &'static str,
    pub proof: &'static [u8],
}

impl TestSemanticScopeVerifier {
    fn validate_scope(
        &self,
        layout: OwnerLayout,
        identity: &MutationScopeIdentity,
        principal: &str,
        proof: &[u8],
    ) -> Result<(), String> {
        if layout != OwnerLayout::SemanticIndex
            || identity.tenant().as_str() != self.tenant
            || principal != self.principal
            || proof != self.proof
        {
            return Err("semantic test scope authority rejected".to_string());
        }
        Ok(())
    }
}

impl ScopeGrantVerifier for TestSemanticScopeVerifier {
    fn verify(
        &self,
        _physical: &PhysicalStoreIdentity,
        layout: OwnerLayout,
        identity: &MutationScopeIdentity,
        principal: &str,
        proof: &[u8],
    ) -> Result<(), String> {
        self.validate_scope(layout, identity, principal, proof)
    }
}

/// Build the exact raw SQL observation used by both admission suites. The
/// caller supplies its authoritative selector and revision so a test can
/// exercise foreign-epoch rejection without altering the governed metadata.
pub(super) fn source_record(
    binding: &SemanticBinding,
    selector: &SqlColumnRef,
    identity_digest: SemanticDigest,
    source_revision: &str,
    value: SemanticSqlSourceValue,
) -> SemanticSqlSourceRecord {
    SemanticSqlSourceRecord {
        source_identity: SemanticSqlSourceIdentity::create(
            selector,
            binding.tenant_id.clone(),
            identity_digest,
        ),
        source_revision: source_revision.into(),
        value,
        source_schema_revision: 9,
        source_schema_digest: binding.source_schema_digest.clone(),
        source_field_set_digest: binding.source_field_set_digest.clone(),
        source_acl_revision: binding.policy_identity.components.source_acl_revision,
        source_acl_digest: binding.policy_identity.components.source_acl_digest.clone(),
        authorization_receipt_digest: SemanticDigest::from_bytes([40; 32]),
    }
}

/// Read-only, complete source page with observable cursor probes. Tests use
/// this to assert retries preserve the caller's cursor and snapshot proof.
pub(super) struct ScriptedSourcePort {
    first: SemanticSqlSourceReadPage,
    second: Option<SemanticSqlSourceReadPage>,
    second_cursor: Vec<u8>,
    fail_second_read: bool,
    calls: Arc<Mutex<Vec<Option<Vec<u8>>>>>,
}

impl ScriptedSourcePort {
    pub(super) fn empty(revision: String, calls: Arc<Mutex<Vec<Option<Vec<u8>>>>>) -> Self {
        Self {
            first: SemanticSqlSourceReadPage {
                source_revision: revision,
                complete_snapshot_receipt_digest: Some(SemanticDigest::from_bytes([78; 32])),
                sources: Vec::new(),
                next_cursor: None,
                complete: true,
            },
            second: None,
            second_cursor: Vec::new(),
            fail_second_read: false,
            calls,
        }
    }

    pub(super) fn two_pages(
        revision: String,
        first_sources: Vec<SemanticSqlSourceRecord>,
        second_sources: Vec<SemanticSqlSourceRecord>,
        complete_receipt: SemanticDigest,
        second_cursor: &[u8],
        fail_second_read: bool,
        calls: Arc<Mutex<Vec<Option<Vec<u8>>>>>,
    ) -> Self {
        Self {
            first: SemanticSqlSourceReadPage {
                source_revision: revision.clone(),
                complete_snapshot_receipt_digest: None,
                sources: first_sources,
                next_cursor: Some(second_cursor.to_vec()),
                complete: false,
            },
            second: Some(SemanticSqlSourceReadPage {
                source_revision: revision,
                complete_snapshot_receipt_digest: Some(complete_receipt),
                sources: second_sources,
                next_cursor: None,
                complete: true,
            }),
            second_cursor: second_cursor.to_vec(),
            fail_second_read,
            calls,
        }
    }
}

impl SemanticSqlSourceReadPort for ScriptedSourcePort {
    fn read_current_sql_source_page(
        &self,
        _binding: &SemanticBinding,
        _wakeup: &SemanticSourceDirtyIntent,
        _record: &MutationOutboxRecord,
        cursor: Option<&[u8]>,
    ) -> Result<SemanticSqlSourceReadPage, SemanticIndexError> {
        self.calls.lock().unwrap().push(cursor.map(<[u8]>::to_vec));
        match (&self.second, cursor) {
            (None, _) | (Some(_), None) => Ok(self.first.clone()),
            (Some(page), Some(cursor))
                if cursor == self.second_cursor.as_slice() && !self.fail_second_read =>
            {
                Ok(page.clone())
            }
            _ => Err(SemanticIndexError::SourceManifestMismatch),
        }
    }
}

/// Enter Building and claim one S1 lease through the durable consumer path.
pub(super) fn claim_single_source_stage(
    service: &SemanticIndexService,
    generation: u64,
) -> MutationOutboxLease {
    service
        .transition_binding_operation(
            generation,
            SemanticBindingState::Building,
            3,
            "semantic:index-maintainer",
            "open-stage-consumer",
            Nonce::from_bytes([91; 32]),
        )
        .unwrap();
    service
        .subscribe_stage_consumer("semantic-s1-worker")
        .unwrap();
    let mut budget = OutboxClaimBudget::new(1, 5_000, 4).unwrap();
    let outcome = service
        .claim_stage_leases("semantic-s1-worker", &mut budget)
        .unwrap();
    assert_eq!(outcome.claims.len(), 1);
    outcome.claims.into_iter().next().unwrap()
}
