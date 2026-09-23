//! Decide-layer job rows, evaluation receipts and fit drafts (EH-062, EH-040).
//!
//! They live in the tenant-scoped Agent Library control owner, beside the
//! components a receipt qualifies, so the `DecisionHead` publish check reads a
//! receipt from the same authority that commits the head. One table, three
//! key families (`job:`, `receipt:`, `draft:`), every row written once: a job
//! runs to its terminal state inside its submit, so there is nothing to update.

use eg_storage::{AgentLibraryOwner, DECISION_ARTIFACTS};
use eg_transaction::AdmittedOwnerWrite;
use redb::ReadableTable;

use super::agent_library::AgentLibraryStore;

/// Key of a job row.
pub fn job_key(job_id: &str) -> String {
    format!("job:{job_id}")
}

/// Key of an evaluation receipt.
pub fn receipt_key(receipt_digest: &str) -> String {
    format!("receipt:{receipt_digest}")
}

/// Key of a fit draft.
pub fn draft_key(draft_sha256: &str) -> String {
    format!("draft:{draft_sha256}")
}

fn put_absent(
    owner: &AdmittedOwnerWrite<'_, AgentLibraryOwner>,
    tenant_id: &str,
    rows: &[(String, Vec<u8>)],
) -> Result<(), String> {
    let mut table = owner.open_table(DECISION_ARTIFACTS)?;
    for (key, bytes) in rows {
        let existing = table
            .get((tenant_id, key.as_str()))
            .map_err(|error| error.to_string())?
            .map(|row| row.value().to_vec());
        match existing {
            Some(stored) if &stored == bytes => {}
            Some(_) => {
                return Err(format!(
                    "IDEMPOTENCY_CONFLICT: decision artifact {key} already holds other bytes"
                ))
            }
            None => {
                table
                    .insert((tenant_id, key.as_str()), bytes.as_slice())
                    .map_err(|error| error.to_string())?;
            }
        }
    }
    Ok(())
}

impl AgentLibraryStore {
    /// Write `rows` for `tenant_id` in one owner transaction. A row that is
    /// already present with the same bytes is a replay; with other bytes the
    /// whole write is refused.
    pub fn put_decision_artifacts(
        &self,
        tenant_id: &str,
        rows: &[(String, Vec<u8>)],
    ) -> Result<(), String> {
        self.maintain_control_rows(tenant_id, "decision_artifacts", |owner| {
            put_absent(owner, tenant_id, rows)
        })
    }

    /// Read one decision artifact of `tenant_id`.
    pub fn decision_artifact(&self, tenant_id: &str, key: &str) -> Result<Option<Vec<u8>>, String> {
        let read = self.read()?;
        let table = read.open_owner_table(DECISION_ARTIFACTS)?;
        Ok(table
            .get((tenant_id, key))
            .map_err(|error| error.to_string())?
            .map(|row| row.value().to_vec()))
    }
}

/// Decode one stored decision artifact.
pub fn decode_artifact<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
    noun: &str,
) -> Result<T, String> {
    eg_types::msgpack::decode_bounded(
        bytes,
        eg_types::msgpack::MsgpackLimits::new(16 * 1024 * 1024, 1_000_000, 64),
    )
    .map_err(|_| format!("CORRUPT_DECISION_ARTIFACT: invalid {noun}"))
}

/// Encode one decision artifact for storage.
pub fn encode_artifact<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, String> {
    rmp_serde::to_vec_named(value).map_err(|error| error.to_string())
}

/// Promotion by publish (EH-040): a `DecisionHead` revision is admitted only
/// when the receipt it names exists in this tenant, PASSED, and qualified
/// exactly these bytes (`head_digest` equals the revision's content digest).
/// Receipts are written once and never removed, so this check cannot race the
/// publish it guards.
pub fn require_head_receipt(
    store: &AgentLibraryStore,
    request: &eg_types::agent_component::AgentComponentPublishRequest,
) -> Result<(), String> {
    use eg_types::decision::statistical::StatisticalErrorCode::EvaluationReceiptMismatch;
    if request.component.kind != eg_types::agent_component::AgentComponentKind::DecisionHead {
        return Ok(());
    }
    let Some(digest) = request.evaluation_receipt_digest.as_deref() else {
        return Err(
            EvaluationReceiptMismatch.refusal("a decision head names no evaluation receipt")
        );
    };
    let bytes = store
        .decision_artifact(&request.context.tenant_id, &receipt_key(digest))?
        .ok_or_else(|| {
            EvaluationReceiptMismatch.refusal("no evaluation receipt with that digest")
        })?;
    let receipt: eg_types::decision::DecisionEvalReceipt =
        decode_artifact(&bytes, "evaluation receipt")?;
    if !receipt.passed {
        return Err(EvaluationReceiptMismatch.refusal(format!(
            "the receipt failed its promotion gates: {}",
            receipt.failed_gates.as_slice().join(", ")
        )));
    }
    if receipt.head_digest != request.component.content_digest {
        return Err(EvaluationReceiptMismatch.refusal("the receipt qualified a different head"));
    }
    Ok(())
}
