//! Batch construction and caller-batch binding for one graph shard.
//!
//! The shard's own drain/maintenance batches and the one boundary where a
//! caller-compiled batch is rebound onto the shard scope that commits it.

use eg_storage::{GraphShardOwner, OwnedStoreHandle, GRAPH_SHARD_TENANT};
use eg_types::mutation_batch::{authority_scope_for, DurabilityDomain, MutationScope};
use eg_types::protocol::Method;
use eg_types::{
    MutationBatch, MutationEnvelope, MutationOperation, MutationOutboxIntent,
    MutationScopeIdentity, MutationSurface, VersionExpectation, MUTATION_BATCH_VERSION,
};

use super::graph_scope_identity;

/// The spelling of one component of a maintenance claim key.
///
/// A physical shard key is a STORAGE name: `redb_store::sanitize` represents
/// every byte outside `[A-Za-z0-9-_.]` as a `~xx` escape (or the whole name as
/// a bounded `~h<sha256>` key), and an operation id composed from one inherits
/// the same escapes. The canonical identifier alphabet that `IdempotencyKey`
/// enforces deliberately excludes `~`
/// (`eg_types::contract::identifiers::validate_canonical_id`), so embedding
/// either part verbatim made the drain batch id unconstructible: `admit_drain`
/// / `admit_maintenance` on a graph whose logical name carries punctuation
/// failed closed with "idempotency key must use the canonical ASCII identifier
/// alphabet", and such a graph could not be drained, purged or checkpointed at
/// all.
///
/// An escaped part therefore travels hex-spelled, the same device the
/// maintenance envelope's SUBJECT already uses for exactly this reason
/// (`ResourceId::from_physical_graph_key`). `~` is the ONLY character
/// `sanitize` can emit that the canonical alphabet rejects, so a part without
/// one is already canonical and keeps its readable spelling -- no existing
/// drain id changes.
fn canonical_claim_part(value: &str) -> std::borrow::Cow<'_, str> {
    if !value.contains('~') {
        return std::borrow::Cow::Borrowed(value);
    }
    let mut spelled = String::with_capacity("hex:".len() + value.len() * 2);
    spelled.push_str("hex:");
    for byte in value.bytes() {
        use std::fmt::Write as _;
        write!(&mut spelled, "{byte:02x}").expect("writing to String cannot fail");
    }
    std::borrow::Cow::Owned(spelled)
}

/// The shard's own batch for one scope of one drain, at that scope's in-lock
/// version.
///
/// `drain_id` is unique per ATTEMPT, deliberately. The coalesced path has no
/// replay requirement -- before the cutover it carried no batch identity at all,
/// no idempotency row and no receipt -- and exactly-once for a replicated entry
/// is already carried by the Raft applied index, which is persisted after the
/// effect lands and never regresses. Deriving the id from `(raft_group, index)`
/// instead would manufacture a conflicting replay out of a path that never
/// needed one: re-admitting the same id at a moved version fails the ledger's
/// whole-batch identity comparison rather than replaying.
pub(super) fn drain_batch(
    owner: &OwnedStoreHandle<GraphShardOwner>,
    drain_id: &str,
    scope_name: &str,
    version: u64,
) -> Result<MutationBatch, String> {
    let batch_id = format!(
        "shard_drain/{}:{}",
        canonical_claim_part(scope_name),
        canonical_claim_part(drain_id)
    );
    let identity = owner.identity().clone();
    let batch = MutationBatch {
        schema_version: MUTATION_BATCH_VERSION,
        batch_id: batch_id.clone(),
        envelope: MutationEnvelope::maintenance_for_scope(
            &identity,
            owner.principal(),
            "shard-drain",
            &batch_id,
        )?,
        identity,
        placement_epoch: 0,
        version_expectation: VersionExpectation::Graph(version),
        fencing_token: None,
        authoritative_state: None,
        operations: vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Other,
            domain: DurabilityDomain::GraphRows,
            method: Method::ApplyMutation {
                event_type: "shard_drain".to_string(),
                query: batch_id,
            },
        }],
        outbox: Vec::new(),
        created_at_ms: 0,
    };
    batch.validate()?;
    Ok(batch)
}

/// Rebind one compiled batch onto the shard scope that will commit it.
///
/// RF-RULING-004 application note 2: a graph-shard mutation scope is
/// `(reserved shard tenant, graph name, graph incarnation)`, derived from the
/// durable name alone. Application note 1: the verified caller survives in the
/// authority context and outbox `actor` header, while the operation envelope's
/// serving principal becomes the store's. This is the ONE place a batch crosses from the request boundary
/// into the shard, so it is the one place both rewrites happen.
pub(crate) fn bind_caller_batch(
    owner: &OwnedStoreHandle<GraphShardOwner>,
    graph_fname: &str,
    batch: &MutationBatch,
) -> Result<MutationBatch, String> {
    ensure_caller_batch_targets_graph(graph_fname, batch)?;
    let mut bound = batch.clone();
    bound.identity = graph_scope_identity(graph_fname)?;
    // A terminal RunEvent is admitted under the shard's canonical scope, not
    // the caller tenant that was present while the batch was compiled. Bind
    // its scope header at this same boundary so the durable preflight checks
    // the identity that will actually own the outbox row. The caller's actor
    // header remains untouched and continues to carry request attribution.
    stamp_run_event_scope_headers(&mut bound)?;
    let schema_digest = bound
        .envelope
        .operation()
        .ok_or_else(|| "a caller batch must carry an operation envelope".to_string())?
        .method_schema_digest;
    let MutationEnvelope::Operation(operation) = &mut bound.envelope else {
        return Err("a caller batch must carry an operation envelope".to_string());
    };
    operation.serving_principal = owner.principal().to_string();
    bound.reseal_envelope(schema_digest)?;
    bound.validate()?;
    Ok(bound)
}

/// Prove the request-boundary facts while the original caller identity is
/// still present.
///
/// Rebinding first would let a batch compiled for graph A be admitted to graph
/// B: the later route check would observe only the newly stamped shard
/// identity. The authority scope is derived from the same identity by the
/// compiler, so compare both its structured value and its tenant before
/// changing either field.
fn ensure_caller_batch_targets_graph(
    graph_fname: &str,
    batch: &MutationBatch,
) -> Result<(), String> {
    if batch.identity.tenant().as_str() == GRAPH_SHARD_TENANT {
        return Err(format!(
            "'{GRAPH_SHARD_TENANT}' is the graph shard's reserved scope tenant and cannot be a caller tenant"
        ));
    }
    batch.validate()?;
    let operation = batch
        .envelope
        .operation()
        .ok_or_else(|| "a caller batch must carry an operation envelope".to_string())?;
    let MutationScope::Graph { graph } = batch.identity.scope() else {
        return Err("a caller batch must carry a graph scope identity".to_string());
    };
    if crate::redb_store::sanitize(graph.as_str()) != graph_fname {
        return Err(format!(
            "caller mutation scope graph '{}' does not match requested graph '{}'",
            graph.as_str(),
            graph_fname
        ));
    }
    if operation.authority.authority_scope != authority_scope_for(&batch.identity)? {
        return Err(
            "caller mutation authority scope does not match mutation scope identity".into(),
        );
    }
    if operation.authority.tenant.as_str() != batch.identity.tenant().as_str() {
        return Err(
            "caller mutation authority tenant does not match mutation scope identity".into(),
        );
    }
    ensure_run_event_scope_headers(batch)
}

fn is_run_event(intent: &MutationOutboxIntent) -> bool {
    intent.topic == eg_types::outcome_bundle::RUN_EVENT_OUTBOX_TOPIC
}

/// `sha256(tenant || 0x00 || graph)` of a graph-scoped identity: the value a
/// terminal RunEvent's `scope_sha256` header must carry.
fn run_event_scope_digest(
    identity: &MutationScopeIdentity,
    unscoped: &str,
) -> Result<String, String> {
    use sha2::{Digest, Sha256};

    let graph = identity
        .scope()
        .graph_name()
        .ok_or_else(|| unscoped.to_string())?;
    let mut digest = Sha256::new();
    digest.update(identity.tenant().as_str().as_bytes());
    digest.update([0]);
    digest.update(graph.as_str().as_bytes());
    Ok(hex::encode(digest.finalize()))
}

/// Every caller terminal RunEvent must already be bound to the caller scope.
fn ensure_run_event_scope_headers(batch: &MutationBatch) -> Result<(), String> {
    if !batch.outbox.iter().any(is_run_event) {
        return Ok(());
    }
    let caller_scope_digest = run_event_scope_digest(
        &batch.identity,
        "caller terminal batch identity is not graph-scoped",
    )?;
    let unbound = batch
        .outbox
        .iter()
        .filter(|intent| is_run_event(intent))
        .any(|intent| {
            intent.headers.get("scope_sha256").map(String::as_str)
                != Some(caller_scope_digest.as_str())
        });
    if unbound {
        return Err(
            "terminal run-event outbox header 'scope_sha256' is not bound to caller mutation scope"
                .to_string(),
        );
    }
    Ok(())
}

/// Re-stamp every terminal RunEvent's `scope_sha256` with the bound identity.
fn stamp_run_event_scope_headers(bound: &mut MutationBatch) -> Result<(), String> {
    if !bound.outbox.iter().any(is_run_event) {
        return Ok(());
    }
    let scope_digest = run_event_scope_digest(
        &bound.identity,
        "bound terminal batch identity is not graph-scoped",
    )?;
    for intent in bound
        .outbox
        .iter_mut()
        .filter(|intent| is_run_event(intent))
    {
        intent
            .headers
            .insert("scope_sha256".to_string(), scope_digest.clone());
    }
    Ok(())
}
