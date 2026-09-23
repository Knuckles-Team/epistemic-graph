//! The operator surface over one SQL catalog scope's mutation outbox (X10).
//!
//! A SQL catalog keys its ledger by `(tenant scope, resource)`; the server's
//! `Method::MutationOutbox` names that pair and reaches the same generic views
//! every other outbox owner serves.

use super::{sql_scope_identity, TableStore};

impl TableStore {
    /// The operator view of `resource`'s outbox in `tenant_scope`.
    pub fn outbox_operator_view(
        &self,
        tenant_scope: &str,
        resource: &str,
        view: &eg_transaction::OutboxView,
        now_ms: u64,
    ) -> Result<eg_transaction::OutboxViewAnswer, String> {
        let identity = sql_scope_identity(tenant_scope, resource)?;
        self.authority.outbox_operator_view(&identity, view, now_ms)
    }

    /// An operator rewind on `resource`'s outbox in `tenant_scope`.
    pub fn outbox_operator_write(
        &self,
        tenant_scope: &str,
        resource: &str,
        write: eg_transaction::OutboxWrite,
    ) -> Result<eg_transaction::OutboxWriteReply, String> {
        let identity = sql_scope_identity(tenant_scope, resource)?;
        self.authority.outbox_operator_write(&identity, write)
    }
}
