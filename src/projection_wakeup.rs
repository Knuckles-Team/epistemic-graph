//! Feature-independent encoding of the mutation projection wake-up.
//!
//! The redb retry ledger and the socket batch producer must derive identical
//! outbox bytes even when either `redb` or `server` is compiled alone. Keep
//! this pure module above both feature-gated owners.

use crate::mutation_batch::MutationOperation;
use sha2::{Digest, Sha256};

/// Encode the ordinary digest-only projection notice.
pub(crate) fn projection_summary_for_operations(
    operations: &[MutationOperation],
) -> Result<Vec<u8>, String> {
    let encoded_operations = rmp_serde::to_vec_named(operations).map_err(|e| e.to_string())?;
    rmp_serde::to_vec_named(&serde_json::json!({
        "schema": "epistemic.mutation.projection.v1",
        "operations": operations.len(),
        "operations_sha256": hex::encode(Sha256::digest(&encoded_operations)),
    }))
    .map_err(|e| e.to_string())
}

/// Encode the exact feature-aware payload used in both outbox production and
/// native retry comparison. The typed wake-up stays behind its own feature.
pub(crate) fn projection_payload_for_operations(
    operations: &[MutationOperation],
) -> Result<Vec<u8>, String> {
    #[cfg(feature = "epistemic-tms")]
    {
        let encoded_operations =
            rmp_serde::to_vec_named(operations).map_err(|error| error.to_string())?;
        let methods = operations
            .iter()
            .map(|operation| operation.method.clone())
            .collect::<Vec<_>>();
        let wakeup = eg_epistemic::ReasoningProjectionWakeup::bounded(
            operations.len(),
            hex::encode(Sha256::digest(encoded_operations)),
            eg_epistemic::ReasoningProjectionWakeup::events_for_methods(&methods),
        )?;
        rmp_serde::to_vec_named(&wakeup).map_err(|error| error.to_string())
    }

    #[cfg(not(feature = "epistemic-tms"))]
    {
        projection_summary_for_operations(operations)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_operations_have_the_same_feature_selected_projection_contract() {
        let operations = Vec::<MutationOperation>::new();
        let encoded = rmp_serde::to_vec_named(&operations).unwrap();
        let expected_digest = hex::encode(Sha256::digest(&encoded));
        let payload = projection_payload_for_operations(&operations).unwrap();
        #[cfg(feature = "epistemic-tms")]
        {
            let notice: eg_epistemic::ReasoningProjectionWakeup =
                rmp_serde::from_slice(&payload).unwrap();
            assert_eq!(notice.operation_count, 0);
            assert_eq!(notice.operations_sha256, expected_digest);
            assert!(notice.events.is_empty());
        }
        #[cfg(not(feature = "epistemic-tms"))]
        {
            let notice: serde_json::Value = rmp_serde::from_slice(&payload).unwrap();
            assert_eq!(notice["schema"], "epistemic.mutation.projection.v1");
            assert_eq!(notice["operations"], 0);
            assert_eq!(notice["operations_sha256"], expected_digest);
        }
    }
}
