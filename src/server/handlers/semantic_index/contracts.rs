//! Conversions from semantic owner runtime values to the declared ingestion
//! result-contract DTOs. The owner crate keeps its receipt/status types private
//! to the runtime crate DAG; the wire contract mirrors their serialized fields.

use eg_core::compute::semantic_ann_codes::SemanticMutationReceipt as RuntimeReceipt;
use eg_core::compute::semantic_index_service::{
    SemanticSqlSourcePageAdmission as RuntimePageAdmission,
    SemanticSqlSourceReconciliationAdmission as RuntimeReconciliationAdmission,
};
use eg_transaction::OutboxStatus;
use eg_types::mutation_outbox::OutboxConsumerStatus;
use eg_types::result_contract::ingestion::{
    SemanticMutationReceipt, SemanticOutboxStatus, SemanticSqlSourcePageAdmission,
    SemanticSqlSourceReconciliationAdmission,
};
use eg_types::semantic_index::SemanticStageTransition;

pub(super) fn receipt(value: RuntimeReceipt) -> SemanticMutationReceipt {
    SemanticMutationReceipt {
        batch_id: value.batch_id,
        mutation_digest: value.mutation_digest,
        source_version: value.source_version,
        target_version: value.target_version,
        replayed: value.replayed,
    }
}

pub(super) fn page(value: RuntimePageAdmission) -> SemanticSqlSourcePageAdmission {
    SemanticSqlSourcePageAdmission {
        receipts: value.receipts.into_iter().map(receipt).collect(),
        next_cursor: value.next_cursor,
        complete: value.complete,
    }
}

pub(super) fn reconciliation(
    value: RuntimeReconciliationAdmission,
) -> SemanticSqlSourceReconciliationAdmission {
    SemanticSqlSourceReconciliationAdmission {
        receipts: value.receipts.into_iter().map(receipt).collect(),
        source_revision: value.source_revision,
        page_count: value.page_count,
        complete: value.complete,
        wakeup_consumed: value.wakeup_consumed,
        rows_seen: value.rows_seen,
        source_bytes_seen: value.source_bytes_seen,
    }
}

pub(super) fn outbox_status(value: OutboxStatus) -> SemanticOutboxStatus {
    SemanticOutboxStatus {
        status: OutboxConsumerStatus {
            consumer: value.consumer,
            topic: value.topic,
            live: value.live,
            capacity: value.capacity,
            inflight: value.inflight,
            inflight_is_lower_bound: value.inflight_is_lower_bound,
            pending: value.pending,
            pending_is_lower_bound: value.pending_is_lower_bound,
            delivered: value.delivered,
            dead_lettered: value.dead_lettered,
            oldest_pending_age_ms: value.oldest_pending_age_ms,
            lag_rows: value.lag_rows,
            lag_versions: value.lag_versions,
            saturated: value.saturated,
            index_complete: value.index_complete,
        },
        consecutive_claims: value.consecutive_claims,
        total_claims: value.total_claims,
    }
}

pub(super) fn replay(
    value: Option<(SemanticStageTransition, RuntimeReceipt)>,
) -> Option<(SemanticStageTransition, SemanticMutationReceipt)> {
    value.map(|(transition, receipt_value)| (transition, receipt(receipt_value)))
}
