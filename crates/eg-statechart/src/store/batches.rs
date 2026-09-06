//! Scope identity and `MutationBatch` construction for the statechart store.
//!
//! Split out of `store.rs` so the store module stays close to the KISS
//! `lines_per_file` cap: these are pure constructors with no store state. Every
//! batch here targets the one fixed native `statechart-instances` scope
//! (`instance_mutation_identity`), so creation, transition and definition writes
//! share ONE totally-ordered mutation log.

use super::*;

/// Fixed mutation-domain scope for statechart instance writes. A single native
/// scope gives every instance transition one totally-ordered mutation log —
/// exactly what a Raft state machine orders and replays — mirroring `eg-jobs`'
/// fixed `native`/`analytics-jobs` scope. `INSTANCE_MUTATION_INCARNATION` is a
/// fixed literal rather than one derived from the physical file: this scope has
/// exactly one lifecycle generation for the life of a `statecharts.redb` (compare
/// `crates/eg-jobs/src/store.rs`'s `ANALYTICS_JOB_SCOPE_INCARNATION` and
/// `crates/eg-types/src/mutation_batch.rs`'s own `"incarnation:bootstrap:1"`
/// fixtures for a fixed scope) -- it is not a placeholder standing in for an
/// unknown value.
pub(super) const INSTANCE_MUTATION_TENANT: &str = "native";
pub(super) const INSTANCE_MUTATION_GRAPH: &str = "statechart-instances";
pub(super) const INSTANCE_MUTATION_INCARNATION: &str = "incarnation:eg-statechart:statechart-instances:1";

/// Operator-facing identity of the ONE physical `statecharts.redb` owner file.
/// Independent of the logical scope above: it names the physical authority
/// boundary the storage kernel stamps into the owner manifest, so a file created
/// for another owner can never be opened as this one.
pub(super) const STATECHART_PHYSICAL_STORE: &str = "eg-statechart:statechart-instances";

/// Typed identity for the fixed `INSTANCE_MUTATION_*` native scope, shared by
/// [`instance_batch`] and [`creation_batch`].
pub(super) fn instance_mutation_identity() -> Result<MutationScopeIdentity> {
    MutationScopeIdentity::fixed_native(
        INSTANCE_MUTATION_TENANT,
        MutationDomain::Lifecycle,
        INSTANCE_MUTATION_GRAPH,
        INSTANCE_MUTATION_INCARNATION,
    )
    .map_err(codec_err)
}

/// Give each durable instance image the same deterministic, digest-only
/// `MutationBatch` `eg-jobs`' `internal_job_batch` stamps on every `JOBS` transition,
/// so an `instantiate` or a firing `send_event` carries identical atomic
/// status/version/fence/idempotency/outbox evidence and is Raft-orderable and
/// crash-consistent across nodes. The batch identity is a pure hash of the resulting
/// instance image (`encoded`), so a re-proposed transition replays idempotently
/// rather than double-applying.
pub(super) fn instance_batch(
    instance: &MachineInstance,
    encoded: &[u8],
    owner: &OwnedStoreHandle<StatechartOwner>,
    expected_version: u64,
) -> Result<MutationBatch> {
    use sha2::{Digest, Sha256};
    let digest = hex::encode(Sha256::digest(encoded));
    // RF-RULING-004: the batch actor is the principal the storage kernel
    // authenticated THIS store's serving scope for -- one physical file serves
    // one bound scope and one principal, and the mutation kernel refuses any
    // batch naming another. Per-instance attribution is not lost: `actor` is a
    // field of the persisted instance image, and its server-hashed form travels
    // on the outbox row below.
    let principal = owner.principal().to_string();
    let actor_digest = format!(
        "principal:sha256:{}",
        hex::encode(Sha256::digest(instance.actor.as_bytes()))
    );
    let batch_id = format!("statechart-transition:{digest}");
    let operation = MutationOperation {
        ordinal: 0,
        surface: MutationSurface::Lifecycle,
        domain: MutationDomain::Lifecycle,
        method: Method::ApplyMutation {
            event_type: "statechart_instance_transition".to_string(),
            query: format!("sha256:{digest}"),
        },
    };
    let batch = MutationBatch {
        schema_version: MUTATION_BATCH_VERSION,
        batch_id: batch_id.clone(),
        context: MutationRequestContext {
            request_id: 0,
            principal,
            purpose: None,
            policy_fingerprint: None,
            trace_id: None,
            // No admission boundary verifies a capability for this internal,
            // firing-transition mutation -- it is a plain `Native`-versioned
            // mutation, not the reserved-system `Unversioned` path, so it
            // legitimately needs none. Empty is the true fact here, not a
            // default standing in for an unknown value.
            verified_capabilities: std::collections::BTreeSet::new(),
        },
        identity: owner.identity().clone(),
        placement_epoch: 0,
        idempotency_key: batch_id.clone(),
        // The scope's live authoritative version, supplied by the caller (see
        // this function's callers, which all read it via
        // `StatechartStore::mutation_version` before admitting the write) --
        // `MutationKernelV1::finish` requires `VersionExpectation::Native` to
        // equal the scope's CURRENT authoritative version.
        version_expectation: VersionExpectation::Native(expected_version),
        fencing_token: None,
        authoritative_state: None,
        operations: vec![operation.clone()],
        outbox: vec![MutationOutboxIntent {
            topic: "engine.statechart-instance.transitioned".to_string(),
            key: batch_id,
            payload: rmp_serde::to_vec_named(&operation).map_err(codec_err)?,
            // The server-hashed owner of the instance. The actor is already an
            // opaque scope; never emit a raw label.
            headers: std::collections::BTreeMap::from([("actor".to_string(), actor_digest)]),
        }],
        created_at_ms: instance.updated_at_ms.max(0) as u64,
    };
    batch.validate().map_err(codec_err)?;
    Ok(batch)
}

/// The definition-write sibling of [`instance_batch`], admitted as a
/// **maintenance** mutation (RF-RULING-005).
///
/// A definition write carries no caller identity — a chart is content-addressed
/// and identical whoever stores it — so it is outside operation-replay
/// semantics, but it is still a full ledgered, fenced, version-bumping mutation
/// because an un-ledgered owner write would be a second authority. The
/// idempotency key IS the content address, so storing a byte-identical chart
/// twice replays instead of rewriting, which is exactly `define`'s documented
/// contract.
pub(super) fn definition_batch(
    def_id: &str,
    identity: &MutationScopeIdentity,
    principal: &str,
    expected_version: u64,
) -> Result<MutationBatch> {
    let batch_id = format!("statechart-definition:{def_id}");
    let operation = MutationOperation {
        ordinal: 0,
        surface: MutationSurface::Lifecycle,
        domain: MutationDomain::Lifecycle,
        method: Method::ApplyMutation {
            event_type: "statechart_definition_store".to_string(),
            query: def_id.to_string(),
        },
    };
    let batch = MutationBatch {
        schema_version: MUTATION_BATCH_VERSION,
        batch_id: batch_id.clone(),
        context: MutationRequestContext {
            request_id: 0,
            principal: principal.to_string(),
            purpose: None,
            policy_fingerprint: None,
            trace_id: None,
            // A maintenance mutation claims no capability: it is a plain
            // `Native`-versioned write, not the reserved-system `Unversioned`
            // path. Empty is the true fact here, not a placeholder.
            verified_capabilities: std::collections::BTreeSet::new(),
        },
        identity: identity.clone(),
        placement_epoch: 0,
        idempotency_key: batch_id.clone(),
        version_expectation: VersionExpectation::Native(expected_version),
        fencing_token: None,
        authoritative_state: None,
        operations: vec![operation.clone()],
        outbox: vec![MutationOutboxIntent {
            topic: "engine.statechart-definition.stored".to_string(),
            key: batch_id,
            payload: rmp_serde::to_vec_named(&operation).map_err(codec_err)?,
            headers: std::collections::BTreeMap::new(),
        }],
        created_at_ms: 0,
    };
    batch.validate().map_err(codec_err)?;
    Ok(batch)
}

/// Derive a NEW instance's id from its pre-agreed, pre-Raft-proposal request
/// identity rather than a local `AtomicU64` counter — the creation-time analogue
/// of `instance_batch`'s result-content-addressing, and the direct mirror of
/// `eg-jobs::job_id_for_batch`. `request_batch_id` is already a caller-computed
/// opaque digest (e.g. of request id + method), so this only domain-separates it
/// into an instance id shaped like the local-counter form (`sc-<hex>`).
pub(super) fn instance_id_for_request(request_batch_id: &str) -> InstanceId {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    digest.update(b"eg-statechart.consensus-instance-id.v1\0");
    digest.update(request_batch_id.as_bytes());
    format!("sc-{}", hex::encode(&digest.finalize()[..16]))
}

/// The creation-time sibling of `instance_batch`: rather than content-addressing
/// the batch from the persisted image (which is circular for a brand-new instance
/// — the image embeds the id this batch must also help derive), the batch
/// identity IS the caller-supplied `request_batch_id`, fixed before Raft proposal
/// and therefore identical on every replica. Same fixed `(tenant, graph)` scope
/// and gateway shape as `instance_batch`, so creation and transition batches share
/// ONE totally-ordered mutation log, per this module's `INSTANCE_MUTATION_*` doc.
pub(super) fn creation_batch(
    instance: &MachineInstance,
    request_batch_id: &str,
    owner: &OwnedStoreHandle<StatechartOwner>,
    expected_version: u64,
) -> Result<MutationBatch> {
    use sha2::{Digest, Sha256};
    // See `instance_batch`: the batch actor is this store's bound serving
    // principal; the instance's own actor travels server-hashed on the outbox.
    let principal = owner.principal().to_string();
    let actor_digest = format!(
        "principal:sha256:{}",
        hex::encode(Sha256::digest(instance.actor.as_bytes()))
    );
    let batch_id = request_batch_id.to_string();
    let operation = MutationOperation {
        ordinal: 0,
        surface: MutationSurface::Lifecycle,
        domain: MutationDomain::Lifecycle,
        method: Method::ApplyMutation {
            event_type: "statechart_instance_create".to_string(),
            query: batch_id.clone(),
        },
    };
    let batch = MutationBatch {
        schema_version: MUTATION_BATCH_VERSION,
        batch_id: batch_id.clone(),
        context: MutationRequestContext {
            request_id: 0,
            principal,
            purpose: None,
            policy_fingerprint: None,
            trace_id: None,
            // No admission boundary verifies a capability for this internal
            // creation mutation -- it is a plain `Native`-versioned mutation,
            // not the reserved-system `Unversioned` path, so it legitimately
            // needs none. Empty is the true fact here, not a default standing
            // in for an unknown value.
            verified_capabilities: std::collections::BTreeSet::new(),
        },
        identity: owner.identity().clone(),
        placement_epoch: 0,
        idempotency_key: batch_id.clone(),
        // The scope's live authoritative version, supplied by the caller -- see
        // `instance_batch`'s identical note just above.
        version_expectation: VersionExpectation::Native(expected_version),
        fencing_token: None,
        authoritative_state: None,
        operations: vec![operation.clone()],
        outbox: vec![MutationOutboxIntent {
            topic: "engine.statechart-instance.instantiated".to_string(),
            key: batch_id,
            payload: rmp_serde::to_vec_named(&operation).map_err(codec_err)?,
            headers: std::collections::BTreeMap::from([("actor".to_string(), actor_digest)]),
        }],
        created_at_ms: instance.updated_at_ms.max(0) as u64,
    };
    batch.validate().map_err(codec_err)?;
    Ok(batch)
}

/// Decode a committed instance image out of a replayed `MutationBatchRecord`
/// (mirrors `eg-jobs::decode_job_result`) — the value `commit_instance_blob`
/// returns on `Begin::Replay`.
pub(super) fn decode_instance_result(record: &MutationBatchRecord) -> Result<MachineInstance> {
    let bytes = record
        .result_msgpack
        .as_deref()
        .ok_or_else(|| codec_err("committed statechart instance batch has no result"))?;
    decode_stored(bytes)
}
