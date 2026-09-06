//! Canonical MutationBatch construction from validated methods.

use std::collections::BTreeMap;
use std::sync::Arc;

use sha2::{Digest, Sha256};

use crate::graph::GraphCore;
use crate::mutation_batch::{
    IncarnationId, LogicalName, MutationBatch, MutationDomain, MutationOperation,
    MutationOutboxIntent, MutationRequestContext, MutationScopeIdentity, MutationStateDescriptor,
    MutationSurface, TenantId, VersionExpectation, MUTATION_BATCH_VERSION,
};
use crate::protocol::Method;
use crate::server::persistence::PersistenceBackend;

use super::canonical::{domain_for, lower_canonical_operation, surface_for};
use super::digest::principal_fingerprint;

/// Resolve the current graph version without treating RAM as a substitute for a
/// missing durable authority. Version zero is the sole implicit bootstrap state;
/// once RAM has advanced, the durable version row must exist and agree with it.
pub(crate) async fn authoritative_graph_version(
    persistence: &Arc<dyn PersistenceBackend>,
    graph_fname: &str,
    core: &GraphCore,
) -> Result<u64, String> {
    let projected_version = core.version();
    match persistence.read_mutation_graph_version(graph_fname).await? {
        Some(version) if version == projected_version => Ok(version),
        Some(version) => Err(format!(
            "authoritative graph version {version} does not match the serving projection {projected_version}"
        )),
        None if projected_version == 0 => Ok(0),
        None => Err(format!(
            "authoritative graph version is missing while the serving projection is at {projected_version}"
        )),
    }
}

/// Fields known by a mutation surface after authz/placement/OCC planning.
///
/// # The one principal rule every batch this module compiles obeys
///
/// `MutationRequestContext::principal` is **the principal the committing ledger
/// requires**, and the verified caller ALWAYS travels separately, as the
/// `actor` header of the batch's outbox row. There is no field whose meaning
/// changes with the batch: [`CompileBatch::principal`] is always the verified
/// caller, the outbox `actor` header is always its fingerprint, and
/// `context.principal` is always the committing ledger's own requirement.
///
/// Two ledgers commit the batches this module builds, and they require
/// different principals because they are different authorities:
///
/// * a **store-authoritative** domain ([`MutationDomain::forbidden_in_graph_scope`]
///   — KV, blob, time-series, analytics-job, semantic-index) commits into a
///   kernel-owned owner store. One physical file serves ONE bound scope under
///   ONE principal, and `eg_transaction::AdmittedMutation::owner_rows` refuses
///   any batch naming another (`owner.principal() != batch.context.principal`).
///   That principal is this engine's bound serving principal,
///   `store_authority::ENGINE_PRINCIPAL` — the only principal
///   `EngineScopeAuthority` mints a grant for. Stamping the caller's fingerprint
///   instead made every served KV/blob/time-series/job batch write fail at
///   runtime with "owner write capability does not match admitted batch".
/// * every **other** domain commits through the graph kernel or a ledger-only
///   coordinator, neither of which is an owner store; both key replay ownership
///   on the caller, so the caller's fingerprint IS what those ledgers require
///   (`handlers/txn.rs`, `handlers/admin.rs`, `wire/mod.rs`, `raft/store.rs`,
///   `dispatch/graph_pipeline.rs` all compare it).
///
/// This is the same rule C1 applied inside `eg-statechart` and `eg-jobs`: the
/// ledger principal is the serving principal, and per-instance attribution moves
/// to the outbox `actor` header. The persisted image's own `actor` field, where a
/// domain has one, is untouched.
pub(crate) struct CompileBatch<'a> {
    pub batch_id: &'a str,
    pub request_id: u64,
    /// The verified caller. Never written to a durable row raw: it is
    /// fingerprinted into the outbox `actor` header, and — for the ledgers that
    /// require it — into `context.principal`. See the type's own docs.
    pub principal: Option<&'a str>,
    pub tenant: &'a str,
    pub graph: &'a str,
    pub placement_epoch: u64,
    pub idempotency_key: &'a str,
    pub expected_graph_version: Option<u64>,
    pub fencing_token: Option<u64>,
    pub created_at_ms: u64,
    pub default_surface: MutationSurface,
    /// Optional authenticated staged material used by runtime-result mutations.
    pub authoritative_state: Option<MutationStateDescriptor>,
}

/// Compile an ordered engine write-set into the universal durable unit.
pub(crate) fn compile_methods(
    ctx: CompileBatch<'_>,
    methods: Vec<Method>,
) -> Result<MutationBatch, String> {
    #[cfg(feature = "epistemic-tms")]
    let reasoning_events = eg_epistemic::ReasoningProjectionWakeup::events_for_methods(&methods);
    let state_backed = ctx.authoritative_state.is_some();
    let operations = methods
        .into_iter()
        .enumerate()
        .map(|(ordinal, method)| {
            let surface = surface_for(&method).unwrap_or(ctx.default_surface);
            let domain = domain_for(&method, surface);
            let method = if state_backed {
                opaque_state_operation(&method)?
            } else {
                lower_canonical_operation(method)
            };
            Ok::<_, String>(MutationOperation {
                ordinal: ordinal as u32,
                surface,
                domain,
                method,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let graph_scope = derive_compiled_methods_scope(&operations)?;
    let batch = finish_batch(ctx, operations, graph_scope)?;
    #[cfg(feature = "epistemic-tms")]
    let batch = {
        let mut batch = batch;
        install_reasoning_wakeup(&mut batch, reasoning_events)?;
        batch
    };
    Ok(batch)
}

/// The scope every batch [`compile_methods`] builds commits under, derived from
/// the operations rather than assumed.
///
/// Every `compile_methods` caller (`commit_work_item`, `commit_lifecycle`,
/// `commit_internal_graph_methods`, `mutation.rs`'s gateway commits) commits
/// through `PersistenceBackend::commit_mutation_batch{,_state}`, which the redb
/// backend routes to `commit_mutation_batch_inner`'s `mutation_batch_graph_name`
/// -- and that fails closed on anything but `MutationScope::Graph`, regardless of
/// the per-operation `domain` tag. WorkItem/Lifecycle methods are tagged
/// `ControlPlane`/`Lifecycle`, domains that MAY own a native scope, yet still
/// physically commit into the target graph's own redb file; that is why the scope
/// is a property of the commit ROUTE, and this compiler has exactly one.
///
/// So the derived answer is the graph scope, and the derivation's whole job is to
/// REFUSE -- here, by name -- a method whose authority is its own store. Such a
/// method has no route through this compiler at all: a native scope would die in
/// `mutation_batch_graph_name` with "mutation batch is not graph-scoped" after the
/// caller already believed the write was compiled, and a graph scope dies in
/// `MutationBatch::validate` with a message that names no method. Neither is a
/// diagnosis. A store-authoritative method must be compiled by
/// [`compile_opaque_method`], whose caller commits it through its own store.
fn derive_compiled_methods_scope(operations: &[MutationOperation]) -> Result<bool, String> {
    match operations
        .iter()
        .find(|operation| operation.domain.forbidden_in_graph_scope())
    {
        Some(operation) => Err(format!(
            "graph-routed compiler cannot compile operation {} classified into the \
             store-authoritative domain '{}': its authoritative state and version counter \
             live in that store, so it has no graph-committed route -- compile it through \
             its own store's compiler instead",
            operation.ordinal,
            operation.domain.canonical_name(),
        )),
        None => Ok(true),
    }
}

/// Compile one payload-bearing surface operation as an opaque digest. SQL catalog
/// statements and other independently-staged domains must retain a canonical
/// status/idempotency/outbox record without persisting query text, bound parameters,
/// repository paths, document bodies, or caller identifiers.
pub(crate) fn compile_opaque_method(
    ctx: CompileBatch<'_>,
    method: &Method,
    surface: MutationSurface,
    domain: MutationDomain,
    event_type: &str,
) -> Result<MutationBatch, String> {
    let encoded = rmp_serde::to_vec_named(method).map_err(|e| e.to_string())?;
    use sha2::{Digest, Sha256};
    let operation = MutationOperation {
        ordinal: 0,
        surface,
        domain,
        method: Method::ApplyMutation {
            event_type: event_type.to_string(),
            query: format!("sha256:{}", hex::encode(Sha256::digest(encoded))),
        },
    };
    // Unlike `compile_methods`, this compiler's callers span both the
    // graph-routed kernel and genuinely native stores committed independently
    // of it (for example the blob CAS's `commit_native_batch`, which never
    // touches `commit_mutation_batch_inner`). The caller-supplied `domain` is
    // authoritative for which one applies: a graph domain always means the
    // graph kernel, so mirror `MutationDomain::requires_native_scope` here: an
    // "either" domain (lifecycle/control-plane/cross-modal/multi-graph) belongs
    // in the graph scope, since that is the route these callers commit through.
    let graph_scope = !domain.requires_native_scope();
    let batch = finish_batch(ctx, vec![operation], graph_scope)?;
    #[cfg(feature = "epistemic-tms")]
    let batch = {
        let mut batch = batch;
        install_reasoning_wakeup(
            &mut batch,
            vec![eg_epistemic::IncrementalReasoningEvent::InvalidateAll],
        )?;
        batch
    };
    Ok(batch)
}

/// Compile a coordinator operation already represented by a SHA-256 digest.  This
/// is the binding used for encrypted private recovery material: only the digest is
/// retained in the canonical batch/outbox, while ciphertext is stored out-of-line
/// by the coordinator's native transaction.
pub(crate) fn compile_opaque_digest(
    ctx: CompileBatch<'_>,
    digest: &str,
    surface: MutationSurface,
    domain: MutationDomain,
    event_type: &str,
) -> Result<MutationBatch, String> {
    if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(
            "opaque coordinator digest must be a 64-character SHA-256 hex value".to_string(),
        );
    }
    let operation = MutationOperation {
        ordinal: 0,
        surface,
        domain,
        method: Method::ApplyMutation {
            event_type: event_type.to_string(),
            query: format!("sha256:{}", digest.to_ascii_lowercase()),
        },
    };
    // Same reasoning as `compile_opaque_method`: the caller-supplied `domain`
    // decides whether this reaches the graph kernel or a native store.
    let graph_scope = !domain.requires_native_scope();
    let batch = finish_batch(ctx, vec![operation], graph_scope)?;
    #[cfg(feature = "epistemic-tms")]
    let batch = {
        let mut batch = batch;
        install_reasoning_wakeup(
            &mut batch,
            vec![eg_epistemic::IncrementalReasoningEvent::InvalidateAll],
        )?;
        batch
    };
    Ok(batch)
}

/// Compile a cross-modal coordinator record. A digest-only operation/manifest binds
/// graph methods and vector/blob/time-series payloads into idempotency and outbox
/// identity without copying those potentially sensitive values into coordinator
/// metadata; authoritative rows remain the recovery source.
pub(crate) fn compile_crossmodal(
    ctx: CompileBatch<'_>,
    modality_payload: &[u8],
    graph_method_count: usize,
    vector_count: usize,
    blob_ref_count: usize,
    measurement_count: usize,
) -> Result<MutationBatch, String> {
    use sha2::{Digest, Sha256};
    let digest = hex::encode(Sha256::digest(modality_payload));
    let operation = MutationOperation {
        ordinal: 0,
        surface: MutationSurface::Transaction,
        domain: MutationDomain::CrossModal,
        method: Method::ApplyMutation {
            event_type: "crossmodal_operation".to_string(),
            query: format!("sha256:{digest}"),
        },
    };
    // Always graph-scoped: `compile_crossmodal` hardcodes `MutationDomain::
    // CrossModal` and its record is committed via `PersistenceBackend::
    // commit_mutation_batch_crossmodal`, which routes to the same
    // graph-routed `commit_mutation_batch_inner` kernel as `compile_methods`.
    let mut batch = finish_batch(ctx, vec![operation], true)?;
    #[cfg(feature = "epistemic-tms")]
    install_reasoning_wakeup(
        &mut batch,
        vec![eg_epistemic::IncrementalReasoningEvent::InvalidateAll],
    )?;
    let manifest = serde_json::json!({
        "schema": "epistemic.crossmodal.manifest.v1",
        "payload_sha256": digest,
        "graph_methods": graph_method_count,
        "vectors": vector_count,
        "blob_refs": blob_ref_count,
        "measurements": measurement_count,
    });
    batch.outbox.push(MutationOutboxIntent {
        topic: "engine.crossmodal.committed".to_string(),
        key: batch.batch_id.clone(),
        payload: rmp_serde::to_vec_named(&manifest).map_err(|e| e.to_string())?,
        headers: BTreeMap::new(),
    });
    batch.validate()?;
    Ok(batch)
}

#[cfg(feature = "epistemic-tms")]
fn install_reasoning_wakeup(
    batch: &mut MutationBatch,
    events: Vec<eg_epistemic::IncrementalReasoningEvent>,
) -> Result<(), String> {
    use sha2::{Digest, Sha256};

    let operations =
        rmp_serde::to_vec_named(&batch.operations).map_err(|error| error.to_string())?;
    let wakeup = eg_epistemic::ReasoningProjectionWakeup::new(
        batch.operations.len(),
        hex::encode(Sha256::digest(operations)),
        events,
    )?;
    let payload = rmp_serde::to_vec_named(&wakeup).map_err(|error| error.to_string())?;
    let intent = batch
        .outbox
        .iter_mut()
        .find(|intent| intent.topic == "engine.projection.rebuild")
        .ok_or_else(|| "MutationBatch has no reasoning projection wake-up".to_string())?;
    intent.payload = payload;
    batch.validate()?;
    Ok(())
}

/// Fixed lifecycle-generation placeholder for every `MutationScopeIdentity` this
/// module compiles. `IncarnationId` is a new v1 concept (v2's `MutationBatch` had
/// no equivalent field) meant to distinguish a scope's lifecycle "generations" --
/// e.g. so a write from before a graph was deleted and recreated under the same
/// name cannot be replayed into the new incarnation. Wiring a REAL per-scope
/// generation counter (a candidate already exists: `read_mutation_lifecycle_head`,
/// "Current lifecycle generation for retry fencing") would require threading an
/// `.await`ed read through every one of `compile_methods`/`compile_opaque_method`/
/// `compile_opaque_digest`/`compile_crossmodal`'s many callers across the crate
/// (`kv.rs`, `dispatch.rs`, `raft/store.rs`, every `handlers/*.rs` -- files this
/// lane does not own and the migration contract does not scope). Using a fixed,
/// non-resource-derived constant here is a mechanical placeholder that satisfies
/// the type (`IncarnationId::new` rejects anything resource-shaped by construction
/// elsewhere, but does not require per-generation freshness), not a security
/// decision -- flagged in the migration report for follow-up.
pub(crate) use eg_types::mutation_batch::COMPILED_BATCH_INCARNATION;

/// Compile the universal `MutationScopeIdentity`/`VersionExpectation` pair shared
/// by every batch this module builds.
///
/// `graph_scope`: true builds `MutationScopeIdentity::graph(..)` with
/// `VersionExpectation::Graph(_)`; false builds `MutationScopeIdentity::native(..)`
/// (domain taken from the first compiled operation) with
/// `VersionExpectation::Native(_)`. v1's `VersionExpectation` has no "unversioned"
/// arm available to an ordinary tenant (`Unversioned` requires the reserved system
/// tenant, a ControlPlane/Lifecycle native domain, AND a verified capability --
/// see `validate_version_expectation`), so every caller of this module must supply
/// its actual observed version through `CompileBatch::expected_graph_version`
/// rather than `None`; `finish_batch` fails closed instead of inventing one.
fn finish_batch(
    ctx: CompileBatch<'_>,
    operations: Vec<MutationOperation>,
    graph_scope: bool,
) -> Result<MutationBatch, String> {
    #[cfg(feature = "raft")]
    let (placement_epoch, fencing_token) =
        crate::server::dispatch::replicated_placement_authority()
            .unwrap_or((ctx.placement_epoch, ctx.fencing_token));
    #[cfg(not(feature = "raft"))]
    let (placement_epoch, fencing_token) = (ctx.placement_epoch, ctx.fencing_token);
    // Projection wake-ups need correlation and integrity, not a second copy of node
    // properties, query text, document bodies, or identifiers. Bind the outbox row to
    // the canonical operation list with a digest-only manifest; the authoritative
    // batch/state remains the recovery source.
    //
    // `crate::redb_store` (the durable-row module) is `#[cfg(feature = "redb")]` in
    // lib.rs -- correctly so for most of its content, which genuinely needs the
    // `redb` crate -- but its projection-wakeup encoder is pure serialization +
    // hashing with no redb dependency, and this call site (every `compile_methods`,
    // every backend) carries no cfg of its own. Same shape as BUG-CX-104 / the
    // `dispatch.rs` fix (commit `8f27c425`): a caller reaching a cfg-gated producer
    // it doesn't itself gate. `redb_store.rs`/`lib.rs` are outside this lane's
    // ownership (`plans/complex/DISPATCH-REGISTRY.tsv` scopes WD10-P-SLIM to this
    // file + `handlers/graph_ops.rs`), so the fix lives here: branch on the same
    // feature the producer is gated on, and for the `not(redb)` arm, encode the
    // identical payload shape locally rather than making `redb_store` unconditional
    // (which would pull the `redb`/`eg-storage`/`eg-transaction` deps into every slim build).
    #[cfg(feature = "redb")]
    let summary = crate::redb_store::projection_payload_for_operations(&operations)?;
    #[cfg(not(feature = "redb"))]
    let summary = projection_wakeup_payload_without_redb(&operations)?;
    let mut scope_digest = Sha256::new();
    scope_digest.update(ctx.tenant.as_bytes());
    scope_digest.update([0]);
    scope_digest.update(ctx.graph.as_bytes());
    let scope_digest = hex::encode(scope_digest.finalize());
    let actor = principal_fingerprint(ctx.principal.ok_or_else(|| {
        "durable mutation authority requires a verified principal".to_string()
    })?)?;
    let principal = ledger_principal(&operations, &actor)?;
    let tenant_id = TenantId::new(ctx.tenant.to_string())?;
    let resource_name = LogicalName::new(ctx.graph.to_string())?;
    let incarnation_id = IncarnationId::new(COMPILED_BATCH_INCARNATION)
        .expect("COMPILED_BATCH_INCARNATION is a valid static incarnation id");
    let expected_version = ctx.expected_graph_version.ok_or_else(|| {
        "mutation batch requires its actual observed version under v1: VersionExpectation has \
         no unversioned arm available to an ordinary tenant (see validate_version_expectation); \
         pass the real current version instead of None"
            .to_string()
    })?;
    let (identity, version_expectation) = if graph_scope {
        (
            MutationScopeIdentity::graph(tenant_id, resource_name, incarnation_id),
            VersionExpectation::Graph(expected_version),
        )
    } else {
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
    };
    let batch = MutationBatch {
        schema_version: MUTATION_BATCH_VERSION,
        batch_id: ctx.batch_id.to_string(),
        context: MutationRequestContext {
            request_id: ctx.request_id,
            principal,
            purpose: None,
            policy_fingerprint: None,
            trace_id: None,
            // No batch built by this module ever needs `Unversioned`
            // (see `expected_version` above), so no code path here needs
            // `MutationCapability::UnversionedSystemMutation` or any other
            // verified capability -- empty is correct, not a placeholder.
            verified_capabilities: Default::default(),
        },
        identity,
        placement_epoch,
        idempotency_key: ctx.idempotency_key.to_string(),
        version_expectation,
        fencing_token,
        authoritative_state: ctx.authoritative_state,
        operations,
        outbox: vec![MutationOutboxIntent {
            topic: "engine.projection.rebuild".to_string(),
            key: ctx.batch_id.to_string(),
            payload: summary,
            // `actor` is the verified caller's fingerprint on EVERY batch this
            // module compiles, whichever ledger commits it. It is the single
            // source of caller attribution, so an owner-store batch — whose
            // `context.principal` must be the store's serving principal — loses
            // none, and a graph batch gains no second, divergent copy.
            headers: BTreeMap::from([
                ("scope_sha256".to_string(), scope_digest),
                ("actor".to_string(), actor),
            ]),
        }],
        created_at_ms: ctx.created_at_ms,
    };
    batch.validate()?;
    Ok(batch)
}

/// The principal the ledger that will commit `operations` requires — the one
/// rule documented on [`CompileBatch`].
///
/// A store-authoritative domain's authoritative state and version counter live
/// in its own kernel-owned file, so its batch is admitted through
/// `eg_transaction::AdmittedMutation::owner_rows`, which accepts only the
/// principal that file's serving scope is bound under. Every other domain is
/// committed by the graph kernel or a ledger-only coordinator, whose replay
/// ownership is keyed on the caller.
///
/// `MutationBatch::validate` already rejects a graph scope carrying a
/// store-authoritative operation, and a native scope whose declared domain
/// disagrees with its operations, so answering over the operation list gives the
/// same answer as answering over the scope for every batch that can validate.
fn ledger_principal(operations: &[MutationOperation], actor: &str) -> Result<String, String> {
    if !operations
        .iter()
        .any(|operation| operation.domain.forbidden_in_graph_scope())
    {
        return Ok(actor.to_string());
    }
    // Every store-authoritative domain is reachable only under a feature that
    // implies `redb` (`kv`, `blob`, `tsdb`, `jobs`, `ann-redb`), because the
    // owner store it names is a redb file. Without `redb` this binary owns no
    // owner store at all, so such an operation cannot be committed by anything
    // and must not be given a principal that implies it could be.
    #[cfg(feature = "redb")]
    {
        Ok(crate::store_authority::ENGINE_PRINCIPAL.to_string())
    }
    #[cfg(not(feature = "redb"))]
    {
        Err(
            "store-authoritative mutation domain has no owner store in a build without redb"
                .to_string(),
        )
    }
}

/// `finish_batch`'s outbox projection-wakeup payload for builds without the `redb`
/// feature, where `crate::redb_store::projection_payload_for_operations` doesn't
/// exist (that module is `#[cfg(feature = "redb")]`). Encodes the identical shape
/// that function produces -- `epistemic-tms`'s typed `ReasoningProjectionWakeup` when
/// that feature is also on, else the plain digest-only summary -- so a redb-less
/// build's outbox row is byte-for-byte what a redb build would have written for the
/// same operations. See the `finish_batch` call site for why this duplication exists
/// instead of ungating `redb_store.rs` itself.
#[cfg(not(feature = "redb"))]
fn projection_wakeup_payload_without_redb(
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
        let wakeup = eg_epistemic::ReasoningProjectionWakeup::new(
            operations.len(),
            hex::encode(Sha256::digest(encoded_operations)),
            eg_epistemic::ReasoningProjectionWakeup::events_for_methods(&methods),
        )?;
        rmp_serde::to_vec_named(&wakeup).map_err(|error| error.to_string())
    }
    #[cfg(not(feature = "epistemic-tms"))]
    {
        let encoded_operations = rmp_serde::to_vec_named(operations).map_err(|e| e.to_string())?;
        rmp_serde::to_vec_named(&serde_json::json!({
            "schema": "epistemic.mutation.projection.v1",
            "operations": operations.len(),
            "operations_sha256": hex::encode(Sha256::digest(&encoded_operations)),
        }))
        .map_err(|e| e.to_string())
    }
}

/// State-backed commits deliberately retain no caller payload, repository path,
/// query text, document body, or identifiers from the original operation. The
/// authoritative snapshot or row delta is supplied out-of-line and digest-verified; this
/// opaque operation fingerprint is sufficient for idempotency/audit correlation.
fn opaque_state_operation(method: &Method) -> Result<Method, String> {
    use sha2::{Digest, Sha256};
    let encoded = rmp_serde::to_vec_named(method).map_err(|e| e.to_string())?;
    Ok(Method::ApplyMutation {
        event_type: "authoritative_state_operation".to_string(),
        query: format!("sha256:{}", hex::encode(Sha256::digest(encoded))),
    })
}
