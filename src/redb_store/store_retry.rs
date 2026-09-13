use super::store_prelude::*;
use super::*;

pub(crate) fn durable_msgpack_limits() -> eg_types::msgpack::MsgpackLimits {
    eg_types::msgpack::MsgpackLimits::new(
        MAX_DURABLE_MSGPACK_BYTES,
        MAX_DURABLE_MSGPACK_ITEMS,
        eg_types::msgpack::DEFAULT_MAX_DEPTH,
    )
}

/// Return a native WorkItem/resource mutation with only authority-owned
/// lifecycle fields normalized. All other request fields remain serialized and
/// therefore participate in exact idempotency comparison. This is intentionally
/// a dedicated native replay seam, not a generic byte-comparison relaxation for
/// MutationBatch.
pub(crate) fn native_retry_method(method: &Method) -> Option<Method> {
    match method {
        Method::ReserveWorkItemResources { request } => {
            let mut request = request.clone();
            request.now_ms = 0;
            Some(Method::ReserveWorkItemResources { request })
        }
        Method::ReleaseWorkItemResources { request } => {
            let mut request = request.clone();
            request.now_ms = 0;
            Some(Method::ReleaseWorkItemResources { request })
        }
        Method::ReclaimWorkItemResources { request } => {
            let mut request = request.clone();
            request.now_ms = 0;
            Some(Method::ReclaimWorkItemResources { request })
        }
        Method::UpdateResourceHost { request } => {
            let mut request = request.clone();
            request.now_ms = 0;
            Some(Method::UpdateResourceHost { request })
        }
        Method::SubmitWorkItem { request } => {
            let mut request = request.clone();
            normalize_submit_context(&mut request.context);
            Some(Method::SubmitWorkItem { request })
        }
        Method::SubmitWorkItems { request } => {
            let mut request = request.clone();
            normalize_submit_context(&mut request.context);
            for child in &mut request.requests {
                normalize_submit_context(&mut child.context);
            }
            Some(Method::SubmitWorkItems { request })
        }
        _ => None,
    }
}

/// The transport/request context is provenance, but a retry may legitimately
/// carry a fresh request/trace/expiry window. Keep the security-bearing scope
/// and subject fields in the idempotency comparison while normalizing only the
/// authority-issued temporal/correlation fields.
pub(crate) fn normalize_submit_context(context: &mut crate::epistemic_operations::RequestContext) {
    context.request_id.clear();
    context.trace_id.clear();
    context.issued_at_ms = 0;
    context.expires_at_ms = 0;
    context.placement_epoch = None;
}

pub(crate) fn native_retry_method_key(method: &Method) -> Result<Option<Vec<u8>>, String> {
    native_retry_method(method)
        .map(|method| rmp_serde::to_vec_named(&method).map_err(|error| error.to_string()))
        .transpose()
}

pub(crate) fn native_retry_operations(
    operations: &[MutationOperation],
) -> Option<Vec<MutationOperation>> {
    if operations.is_empty()
        || operations
            .iter()
            .any(|operation| !is_native_retry_method(&operation.method))
    {
        return None;
    }
    let methods = operations
        .iter()
        .map(|operation| native_retry_method(&operation.method))
        .collect::<Option<Vec<_>>>()?;
    Some(
        operations
            .iter()
            .zip(methods)
            .map(|(operation, method)| MutationOperation {
                ordinal: operation.ordinal,
                surface: operation.surface,
                domain: operation.domain,
                method,
            })
            .collect(),
    )
}

/// Encode the derived projection wake-up for an immutable operation list.
///
/// Native resource retries compare this derived intent after normalizing the
/// authority-owned lifecycle timestamp. Keeping the digest construction here
/// prevents the retry path from drifting from the producer in
/// `server::mutation_batch::finish_batch`.
///
/// BUG-PE-037: moved here from `server::mutation_batch` (which imports Tokio
/// and is gated on `server`) -- this and `projection_payload_for_operations`
/// below are pure (sha2/rmp_serde/serde_json only) and this module's own doc
/// comment above is explicit that it "compiles under `--features redb`
/// ALONE (no `server`)"; the prior location broke exactly that contract for
/// `native_retry_outbox_match`, below, one of this module's own callers.
/// `server::mutation_batch::finish_batch` now calls
/// `crate::redb_store::projection_payload_for_operations` instead of
/// keeping a second copy.
pub(crate) fn projection_summary_for_operations(
    operations: &[MutationOperation],
) -> Result<Vec<u8>, String> {
    let encoded_operations = rmp_serde::to_vec_named(operations).map_err(|e| e.to_string())?;
    use sha2::{Digest, Sha256};
    rmp_serde::to_vec_named(&serde_json::json!({
        "schema": "epistemic.mutation.projection.v1",
        "operations": operations.len(),
        "operations_sha256": hex::encode(Sha256::digest(&encoded_operations)),
    }))
    .map_err(|e| e.to_string())
}

/// Encode the feature-aware projection wake-up payload for an operation list.
///
/// `epistemic-tms` replaces the ordinary summary with a typed
/// `ReasoningProjectionWakeup`. Retry reconciliation must derive the same
/// payload as the producer, including that feature-specific shape.
pub(crate) fn projection_payload_for_operations(
    operations: &[MutationOperation],
) -> Result<Vec<u8>, String> {
    #[cfg(feature = "epistemic-tms")]
    {
        use sha2::{Digest, Sha256};

        let encoded_operations =
            rmp_serde::to_vec_named(operations).map_err(|error| error.to_string())?;
        let methods = operations
            .iter()
            .map(|operation| operation.method.clone())
            .collect::<Vec<_>>();
        let wakeup = eg_epistemic::ReasoningProjectionWakeup::new(
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

/// Compare the derived projection wake-up for a retry.  Native resource
/// operations contain one authority-owned `now_ms`, so the raw outbox digest
/// changes when a retry is admitted at a later leader time even though the
/// immutable operation is identical.  Rebuild the digest from the same
/// normalized operation list used by the operation comparator, while keeping
/// topic/key/header metadata exact and rejecting arbitrary payload changes.
pub(crate) fn native_retry_outbox_match(
    stored_operations: &[MutationOperation],
    proposed_operations: &[MutationOperation],
    stored_outbox: &[MutationOutboxIntent],
    proposed_outbox: &[MutationOutboxIntent],
    operations_match: bool,
) -> Result<bool, String> {
    let Some(stored_normalized) = native_retry_operations(stored_operations) else {
        return Ok(stored_outbox == proposed_outbox);
    };
    let Some(proposed_normalized) = native_retry_operations(proposed_operations) else {
        return Ok(false);
    };
    if !operations_match || stored_outbox.len() != proposed_outbox.len() {
        return Ok(false);
    }
    let stored_normalized_payload = projection_payload_for_operations(&stored_normalized)?;
    let proposed_normalized_payload = projection_payload_for_operations(&proposed_normalized)?;
    if stored_normalized_payload != proposed_normalized_payload {
        return Ok(false);
    }
    // The producer hashes the original operation list, including its historical
    // authority-owned timestamp. Authenticate each stored/proposed intent against
    // its own operation list before comparing the normalized retry meaning; never
    // require an original intent to equal a digest that the producer did not emit.
    let stored_original_payload = projection_payload_for_operations(stored_operations)?;
    let proposed_original_payload = projection_payload_for_operations(proposed_operations)?;
    Ok(stored_outbox
        .iter()
        .zip(proposed_outbox)
        .all(|(stored, proposed)| {
            stored.topic == proposed.topic
                && stored.key == proposed.key
                && stored.headers == proposed.headers
                && stored.payload == stored_original_payload
                && proposed.payload == proposed_original_payload
        }))
}

pub(crate) fn is_native_retry_method(method: &Method) -> bool {
    native_retry_method(method).is_some()
}

/// A resource batch may be replayed after its placement leader changes.  The
/// durable operation/idempotency key remains the retry identity; placement
/// metadata is historical routing proof and may advance monotonically for that
/// exact replay.  A backwards route is never accepted.
pub(crate) fn native_resource_placement_replay_match(
    stored: &MutationBatch,
    proposed: &MutationBatch,
    operations_match: bool,
) -> bool {
    if !operations_match
        || stored.operations.len() != 1
        || proposed.operations.len() != 1
        || !is_native_retry_method(&stored.operations[0].method)
        || !is_native_retry_method(&proposed.operations[0].method)
    {
        return false;
    }
    let stored_epoch = stored.placement_epoch;
    let proposed_epoch = proposed.placement_epoch;
    if proposed_epoch > stored_epoch {
        // A failover advances the catalog epoch but keeps the placement group's
        // fencing token.  Requiring the exact prior token prevents a caller from
        // manufacturing a higher epoch (or swapping in an unrelated group) at
        // the persistence boundary; dispatch supplies the current route proof.
        return stored.fencing_token.is_some() && proposed.fencing_token == stored.fencing_token;
    }
    proposed_epoch == stored_epoch && proposed.fencing_token == stored.fencing_token
}

/// One positional operation pair of a retry comparison.  Ordinal/surface/domain
/// must agree first (unchanged order), then the methods: both retry-keyed and
/// equal, or both unkeyed with byte-identical msgpack encodings.
pub(crate) fn mutation_operation_retry_match(
    stored: &MutationOperation,
    proposed: &MutationOperation,
) -> Result<bool, String> {
    if stored.ordinal != proposed.ordinal
        || stored.surface != proposed.surface
        || stored.domain != proposed.domain
    {
        return Ok(false);
    }
    match (
        native_retry_method_key(&stored.method)?,
        native_retry_method_key(&proposed.method)?,
    ) {
        (Some(stored_key), Some(proposed_key)) => Ok(stored_key == proposed_key),
        (None, None) => {
            let stored_bytes =
                rmp_serde::to_vec_named(&stored.method).map_err(|error| error.to_string())?;
            let proposed_bytes =
                rmp_serde::to_vec_named(&proposed.method).map_err(|error| error.to_string())?;
            Ok(stored_bytes == proposed_bytes)
        }
        (Some(_), None) | (None, Some(_)) => Ok(false),
    }
}

pub(crate) fn mutation_operations_retry_match(
    stored: &[MutationOperation],
    proposed: &[MutationOperation],
) -> Result<bool, String> {
    if stored.len() != proposed.len() {
        return Ok(false);
    }
    for (stored, proposed) in stored.iter().zip(proposed) {
        if !mutation_operation_retry_match(stored, proposed)? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// The operation envelope a graph-shard test fixture batch carries.
///
/// Minted through the one public constructor, so a fixture cannot become a
/// second minting path: everything it does not name is this deployment's
/// documented constant, exactly as a producer with no verified request carrier
/// gets. The attempt nonce is server-minted, so rebuilding a fixture for the
/// same key is a FRESH attempt over the same stable operation -- which is the
/// case the kernel must replay rather than refuse.
#[cfg(test)]
pub(crate) fn fixture_operation_envelope(
    identity: &eg_types::MutationScopeIdentity,
    actor: &str,
    request_id: u64,
    idempotency_key: &str,
) -> eg_types::mutation_batch::MutationEnvelope {
    let method =
        eg_types::contract::MethodId::new(eg_types::mutation_batch::BATCH_COMPILED_METHODS)
            .expect("the reserved batch method id is canonical");
    eg_types::mutation_batch::MutationEnvelope::for_scope(
        eg_types::mutation_batch::CompiledScope {
            identity,
            actor,
            serving_principal: actor,
            request_id,
            idempotency_key,
            nonce: eg_types::contract::Nonce::minted(),
            now_ms: 0,
        },
        eg_types::mutation_batch::CompiledOperation {
            method_schema_id: eg_types::mutation_batch::method_schema_id(&method)
                .expect("a reserved batch method has a derived schema id"),
            method,
            method_schema_digest: eg_types::contract::Digest256::from_bytes([0_u8; 32]),
            canonical_payload_digest: eg_types::contract::Digest256::from_bytes([1_u8; 32]),
        },
    )
    .expect("a fixture scope mints a valid operation envelope")
}
