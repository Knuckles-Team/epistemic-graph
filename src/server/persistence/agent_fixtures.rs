//! Test fixtures shared by the agent-hierarchy store tests and by the handler
//! tests that drive those stores.
//!
//! Every layer's tests build the same mutation context, open the same
//! temporary store and read the same ledger record. One definition keeps the
//! four test modules from drifting apart on what a well-formed attempt is.

use eg_types::agent_library::AgentLibraryMutationContext;

use super::agent_library::{batch_id, current_agent_library_policy_digest, AgentLibraryStore};

/// A fresh Agent Library owner in its own temporary directory.
pub(crate) fn open_agent_store() -> (tempfile::TempDir, AgentLibraryStore) {
    let dir = tempfile::tempdir().unwrap();
    let store = AgentLibraryStore::open(dir.path().to_str().unwrap()).unwrap();
    (dir, store)
}

/// One write attempt's mutation context.
///
/// `nonce` is both the attempt nonce (`[nonce; 32]`) and the request id, so an
/// attempt is traceable by it. The policy decision id is named after the layer
/// the purpose belongs to: `agent-graph:publish` decides under
/// `agent-graph:decision:policy-v1`.
pub(crate) fn mutation_context(
    store: &AgentLibraryStore,
    tenant_id: &str,
    key: &str,
    nonce: u8,
    expected_revision: u64,
    purpose_id: &str,
) -> AgentLibraryMutationContext {
    let layer = purpose_id.split(':').next().unwrap_or(purpose_id);
    AgentLibraryMutationContext {
        request_id: u64::from(nonce),
        principal: store.owner_principal().to_string(),
        caller_principal: format!("principal:sha256:{}", "a".repeat(64)),
        attempt_nonce: eg_types::contract::Nonce::from_bytes([nonce; 32]),
        tenant_id: tenant_id.to_string(),
        actor_scope: "action-scope:a".to_string(),
        purpose_id: purpose_id.to_string(),
        policy_revision: "policy-v1".to_string(),
        policy_digest: current_agent_library_policy_digest().unwrap(),
        policy_decision_id: format!("{layer}:decision:policy-v1"),
        idempotency_key: key.to_string(),
        expected_revision: Some(expected_revision),
        trace_id: None,
        created_at_ms: 10,
    }
}

/// The committed ledger record one idempotency key filed in a tenant's scope.
pub(crate) fn ledger_record(
    store: &AgentLibraryStore,
    tenant_id: &str,
    idempotency_key: &str,
) -> eg_types::MutationBatchRecord {
    let owner = store.scope_handle(tenant_id).unwrap();
    let read = store.kernel.read_scope(&owner).unwrap();
    let batch_id = batch_id(idempotency_key).unwrap();
    eg_transaction::read_ledger(&read, &batch_id)
        .unwrap()
        .expect("durable status record")
}

/// Which of the four layers hold a record under `record_id` in `tenant_id`:
/// component, library entry, graph, template.
pub(crate) fn layers_holding(
    store: &AgentLibraryStore,
    tenant_id: &str,
    record_id: &str,
) -> [bool; 4] {
    [
        store
            .current_component(tenant_id, record_id)
            .unwrap()
            .is_some(),
        store.current(tenant_id, record_id).unwrap().is_some(),
        store.current_graph(tenant_id, record_id).unwrap().is_some(),
        store
            .current_template(tenant_id, record_id)
            .unwrap()
            .is_some(),
    ]
}
