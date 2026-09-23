//! The Agent Library's mutation outbox, as its consumers and operators see it.
//!
//! Every Agent Library write publishes on its tenant's native ledger scope:
//! component revisions on `eg.agent-component.revision.v1`, pack imports on
//! `eg.connector-pack.import.v1`. The pack projection worker is the first
//! consumer of that outbox (PB1); `Method::MutationOutbox` is how an operator
//! reads and rewinds it (X10). Both go through the owner-generic
//! [`crate::server::outbox_operator`] functions over this store's kernel and
//! the tenant's scope handle.

use eg_transaction::{OutboxClaimBudget, OutboxClaimOutcome};
use eg_types::mutation_batch::MutationOutboxLease;

use crate::server::outbox_operator::{
    operate, read_view, OutboxView, OutboxViewAnswer, OutboxWrite, OutboxWriteReply,
};
use crate::server::persistence::agent_library::AgentLibraryStore;

impl AgentLibraryStore {
    /// Answer one operator view of a consumer's stream in `tenant_id`'s scope.
    pub(crate) fn outbox_view(
        &self,
        tenant_id: &str,
        view: &OutboxView,
        now_ms: u64,
    ) -> Result<OutboxViewAnswer, String> {
        let owner = self.scope_handle(tenant_id)?;
        let read = self.kernel.read_scope(&owner)?;
        read_view(&self.mutations, &read, view, now_ms)
    }

    /// Apply one reject or rewind in `tenant_id`'s scope.
    pub(crate) fn outbox_write(
        &self,
        tenant_id: &str,
        write: OutboxWrite,
    ) -> Result<OutboxWriteReply, String> {
        let owner = self.scope_handle(tenant_id)?;
        operate(&self.mutations, &owner, write)
    }

    /// Durably subscribe `consumer` to `topic` in `tenant_id`'s scope.
    /// Idempotent for the same topic; refused for a different one.
    pub(crate) fn outbox_subscribe(
        &self,
        tenant_id: &str,
        consumer: &str,
        topic: &str,
    ) -> Result<(), String> {
        let owner = self.scope_handle(tenant_id)?;
        self.mutations.outbox_subscribe(&owner, consumer, topic)
    }

    /// Lease pending rows of `consumer` in `tenant_id`'s scope.
    pub(crate) fn outbox_claim(
        &self,
        tenant_id: &str,
        consumer: &str,
        budget: &mut OutboxClaimBudget,
    ) -> Result<OutboxClaimOutcome, String> {
        let owner = self.scope_handle(tenant_id)?;
        self.mutations.outbox_claim(&owner, consumer, budget)
    }

    /// Acknowledge one held lease in `tenant_id`'s scope.
    pub(crate) fn outbox_ack(
        &self,
        tenant_id: &str,
        lease: &MutationOutboxLease,
        now_ms: u64,
    ) -> Result<(), String> {
        let owner = self.scope_handle(tenant_id)?;
        self.mutations.outbox_ack(&owner, lease, now_ms).map(drop)
    }
}
