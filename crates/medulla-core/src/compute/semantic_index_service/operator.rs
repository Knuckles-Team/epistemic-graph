use super::*;

impl SemanticIndexService {
    /// The operator view of this binding's outbox (`Method::MutationOutbox`).
    pub fn outbox_operator_view(
        &self,
        view: &medulla_transaction::OutboxView,
        now_ms: u64,
    ) -> Result<medulla_transaction::OutboxViewAnswer, SemanticCodeError> {
        self.store.outbox_operator_view(view, now_ms)
    }

    /// An operator rewind on this binding's outbox (`Method::MutationOutbox`).
    pub fn outbox_operator_write(
        &self,
        write: medulla_transaction::OutboxWrite,
    ) -> Result<medulla_transaction::OutboxWriteReply, SemanticCodeError> {
        self.store.outbox_operator_write(write)
    }
}
