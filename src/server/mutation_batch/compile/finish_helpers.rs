//! Scope and outbox construction for the canonical compiled batch.

use std::collections::BTreeMap;

use crate::mutation_batch::{
    IncarnationId, LogicalName, MutationOperation, MutationOutboxIntent, MutationScopeIdentity,
    ScopeTenantId, VersionExpectation,
};

use super::{CompileBatch, CompiledOutbox, COMPILED_BATCH_INCARNATION};

/// Derive the durable scope only after the caller and reserved-name checks.
pub(super) fn compiled_scope_identity(
    ctx: &CompileBatch<'_>,
    operations: &[MutationOperation],
    graph_scope: bool,
    scope_override: Option<MutationScopeIdentity>,
) -> Result<(MutationScopeIdentity, VersionExpectation), String> {
    let tenant_id = ScopeTenantId::new(ctx.tenant.to_string())?;
    let resource_name = LogicalName::new(ctx.graph.to_string())?;
    let incarnation_id = IncarnationId::new(COMPILED_BATCH_INCARNATION)
        .expect("COMPILED_BATCH_INCARNATION is a valid static incarnation id");
    let expected_version = ctx.expected_graph_version.ok_or_else(|| {
        "mutation batch requires its actual observed version under v1: VersionExpectation has \
         no unversioned arm available to an ordinary tenant (see validate_version_expectation); \
         pass the real current version instead of None"
            .to_string()
    })?;
    let (identity, version_expectation) = match scope_override {
        Some(identity) => {
            let expectation = if graph_scope {
                VersionExpectation::Graph(expected_version)
            } else {
                VersionExpectation::Native(expected_version)
            };
            (identity, expectation)
        }
        None if graph_scope => (
            MutationScopeIdentity::graph(tenant_id, resource_name, incarnation_id),
            VersionExpectation::Graph(expected_version),
        ),
        None => {
            let domain = operations
                .first()
                .map(|operation| operation.domain)
                .ok_or_else(|| {
                    "mutation batch has no operations to derive its native domain from".to_string()
                })?;
            (
                MutationScopeIdentity::native(tenant_id, domain, resource_name, incarnation_id)?,
                VersionExpectation::Native(expected_version),
            )
        }
    };
    Ok((identity, version_expectation))
}

/// Construct every intent before minting the envelope that covers their bytes.
pub(super) fn compiled_outbox(
    outbox_plan: CompiledOutbox,
    identity: &MutationScopeIdentity,
    summary: Vec<u8>,
    scope_digest: &str,
    actor: &str,
    batch_id: &str,
) -> Result<Vec<MutationOutboxIntent>, String> {
    let terminal_outcome = outbox_plan
        .extra
        .iter()
        .any(|intent| intent.topic == eg_types::outcome_bundle::RUN_EVENT_OUTBOX_TOPIC);
    let mut outbox = if terminal_outcome {
        // A terminal receipt's conditional RunEvent is the batch's one outbox
        // currency. The generic projection wake-up would publish a second row
        // for the same WorkItem transition and would survive no-op/fenced
        // results unless the native terminal result filtered it too.
        outbox_plan.extra
    } else {
        let projection = MutationOutboxIntent {
            topic: "engine.projection.rebuild".to_string(),
            key: batch_id.to_string(),
            payload: summary,
            // `actor` is the verified caller's fingerprint on EVERY batch this
            // module compiles, whichever ledger commits it. It is the single
            // source of caller attribution, so an owner-store batch — whose
            // `context.principal` must be the store's serving principal — loses
            // none, and a graph batch gains no second, divergent copy.
            headers: BTreeMap::from([
                ("scope_sha256".to_string(), scope_digest.to_string()),
                ("actor".to_string(), actor.to_string()),
            ]),
        };
        let mut outbox = vec![projection];
        outbox.extend(outbox_plan.extra);
        outbox
    };
    if terminal_outcome {
        for intent in &mut outbox {
            if intent.topic == eg_types::outcome_bundle::RUN_EVENT_OUTBOX_TOPIC {
                // The conditional terminal event replaces the generic
                // projection row, so carry the same universal attribution and
                // scope binding on the sole row that remains.
                intent
                    .headers
                    .insert("scope_sha256".to_string(), scope_digest.to_string());
                intent
                    .headers
                    .insert("actor".to_string(), actor.to_string());
            }
        }
    }
    if let Some(input_digest) = outbox_plan.semantic_source_dirty_input {
        let source_scope_digest = eg_types::semantic_index::SemanticDigest::from_bytes(
            *identity.binding_digest().as_bytes(),
        );
        let intent = eg_types::semantic_index::SemanticSourceDirtyIntent::new(
            source_scope_digest,
            input_digest,
        );
        outbox.push(MutationOutboxIntent {
            topic: eg_types::semantic_index::SEMANTIC_SOURCE_DIRTY_TOPIC.to_string(),
            key: batch_id.to_string(),
            payload: intent.to_canonical_cbor().map_err(|error| {
                format!("semantic source-dirty intent encoding rejected: {error:?}")
            })?,
            headers: BTreeMap::new(),
        });
    }
    Ok(outbox)
}
