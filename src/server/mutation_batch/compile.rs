//! Canonical MutationBatch construction from validated methods.

use std::collections::BTreeMap;
use std::sync::Arc;

use sha2::{Digest, Sha256};

use crate::graph::GraphCore;
use crate::mutation_batch::{
    DurabilityDomain, IncarnationId, LogicalName, MutationBatch, MutationOperation,
    MutationOutboxIntent, MutationScopeIdentity, MutationStateDescriptor, MutationSurface,
    ScopeTenantId, VersionExpectation, MUTATION_BATCH_VERSION,
};
use crate::protocol::Method;
use crate::server::persistence::PersistenceBackend;

use super::canonical::{domain_for, lower_canonical_operation, surface_for};
use super::digest::{principal_fingerprint, ENGINE_LEDGER_PRINCIPAL};
use super::terminal_outcome::lower_terminal_outcome_extensions;

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
/// `MutationEnvelope`'s serving principal is **the principal the committing
/// ledger requires**, and the verified caller ALWAYS travels separately, as the
/// `actor` header of the batch's outbox row AND as the envelope's own
/// `authority.actor`, which is inside the stable replay identity. There is no field whose meaning
/// changes with the batch: [`CompileBatch::principal`] is always the verified
/// caller, the outbox `actor` header is always its fingerprint, and
/// `context.principal` is always the committing ledger's own requirement.
///
/// Two ledgers commit the batches this module builds, and they require
/// different principals because they are different authorities:
///
/// * a **store-authoritative** domain ([`DurabilityDomain::forbidden_in_graph_scope`]
///   — KV, blob, time-series, analytics-job, semantic-index) commits into a
///   kernel-owned owner store. One physical file serves ONE bound scope under
///   ONE principal, and `eg_transaction::AdmittedMutation::owner_rows` refuses
///   any batch naming another (`owner.principal() != batch.serving_principal()`).
///   That principal is this engine's bound serving principal,
///   `store_authority::ENGINE_PRINCIPAL` — the only principal
///   `EngineScopeAuthority` mints a grant for. Stamping the caller's fingerprint
///   instead made every served KV/blob/time-series/job batch write fail at
///   runtime with "owner write capability does not match admitted batch".
/// * every **other** domain commits through the graph kernel or a ledger-only
///   coordinator, neither of which is an owner store; both key replay ownership
///   on the caller, so the caller's fingerprint IS what those ledgers require
///   (`handlers/txn.rs`, `handlers/admin.rs`, `wire/mod.rs`, `raft/store.rs`,
///   `dispatch/graph_pipeline.rs` all compare it -- through the ONE checked
///   accessor `MutationBatchRecord::committing_actor`, never a private
///   re-derivation).
///
/// This is the same rule C1 applied inside `eg-statechart` and `eg-jobs`: the
/// ledger principal is the serving principal, and per-instance attribution moves
/// to the outbox `actor` header. The persisted image's own `actor` field, where a
/// domain has one, is untouched.
pub(crate) struct CompileBatch<'a> {
    pub batch_id: &'a str,
    pub request_id: u64,
    /// The VERIFIED transport nonce when this request carried one, `None` when
    /// the attempt nonce is server-minted.
    ///
    /// `NonceReplayKey` is the ATTEMPT identity: the same nonce is rejected, a
    /// fresh nonce over the same stable operation replays. A caller may not
    /// choose it -- a caller-pinned value would let a caller make its own retry
    /// undeliverable, and the caller's stable retry identity is
    /// `idempotency_key`, which it does supply. The one nonce that travels from
    /// outside is the request envelope's, which the request boundary has already
    /// verified under the MAC; carrying it here is what lets the kernel's
    /// `mutation_replay_nonces` rows be the SINGLE anti-replay authority for a
    /// mutation, instead of the per-node `RedbReplayLedger` deciding the same
    /// question a second time.
    pub attempt_nonce: Option<eg_types::contract::Nonce>,
    /// The verified caller. Never written to a durable row raw: it is
    /// fingerprinted into the outbox `actor` header and into the envelope's
    /// `authority.actor`, which is what the stable replay identity compares.
    /// See the type's own docs.
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
    let (methods, terminal_outbox) = lower_terminal_outcome_extensions(ctx.batch_id, methods)?;
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
    finish_batch(
        ctx,
        operations,
        graph_scope,
        CompiledOutbox {
            extra: terminal_outbox,
            semantic_source_dirty_input: None,
            #[cfg(feature = "epistemic-tms")]
            reasoning_events,
        },
    )
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
    domain: DurabilityDomain,
    event_type: &str,
) -> Result<MutationBatch, String> {
    let encoded = rmp_serde::to_vec_named(method).map_err(|e| e.to_string())?;
    let input_digest: [u8; 32] = Sha256::digest(&encoded).into();
    let operation = MutationOperation {
        ordinal: 0,
        surface,
        domain,
        method: Method::ApplyMutation {
            event_type: event_type.to_string(),
            query: format!("sha256:{}", hex::encode(input_digest)),
        },
    };
    // Unlike `compile_methods`, this compiler's callers span both the
    // graph-routed kernel and genuinely native stores committed independently
    // of it (for example the blob CAS's `commit_native_batch`, which never
    // touches `commit_mutation_batch_inner`). The caller-supplied `domain` is
    // authoritative for which one applies: a graph domain always means the
    // graph kernel, so mirror `DurabilityDomain::requires_native_scope` here: an
    // "either" domain (lifecycle/control-plane/cross-modal/multi-graph) belongs
    // in the graph scope, since that is the route these callers commit through.
    let graph_scope = !domain.requires_native_scope();
    finish_batch(
        ctx,
        vec![operation],
        graph_scope,
        CompiledOutbox {
            extra: Vec::new(),
            semantic_source_dirty_input: (domain == DurabilityDomain::SqlCatalog).then_some(
                eg_types::semantic_index::SemanticDigest::from_bytes(input_digest),
            ),
            #[cfg(feature = "epistemic-tms")]
            reasoning_events: vec![eg_epistemic::IncrementalReasoningEvent::InvalidateAll],
        },
    )
}

/// Compile a coordinator operation already represented by a SHA-256 digest.  This
/// is the binding used for encrypted private recovery material: only the digest is
/// retained in the canonical batch/outbox, while ciphertext is stored out-of-line
/// by the coordinator's native transaction.
pub(crate) fn compile_opaque_digest(
    ctx: CompileBatch<'_>,
    digest: &str,
    surface: MutationSurface,
    domain: DurabilityDomain,
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
    finish_batch(
        ctx,
        vec![operation],
        graph_scope,
        CompiledOutbox {
            extra: Vec::new(),
            semantic_source_dirty_input: None,
            #[cfg(feature = "epistemic-tms")]
            reasoning_events: vec![eg_epistemic::IncrementalReasoningEvent::InvalidateAll],
        },
    )
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
        domain: DurabilityDomain::CrossModal,
        method: Method::ApplyMutation {
            event_type: "crossmodal_operation".to_string(),
            query: format!("sha256:{digest}"),
        },
    };
    // Always graph-scoped: `compile_crossmodal` hardcodes `DurabilityDomain::
    // CrossModal` and its record is committed via `PersistenceBackend::
    // commit_mutation_batch_crossmodal`, which routes to the same
    // graph-routed `commit_mutation_batch_inner` kernel as `compile_methods`.
    let batch_id = ctx.batch_id.to_string();
    let manifest = serde_json::json!({
        "schema": "epistemic.crossmodal.manifest.v1",
        "payload_sha256": digest,
        "graph_methods": graph_method_count,
        "vectors": vector_count,
        "blob_refs": blob_ref_count,
        "measurements": measurement_count,
    });
    // The manifest intent is an INPUT, not something pushed onto the finished
    // batch: the envelope's canonical payload digest covers the outbox, so an
    // intent appended after the mint would leave the envelope under-covering its
    // own body -- which `MutationBatch::validate` now refuses.
    finish_batch(
        ctx,
        vec![operation],
        true,
        CompiledOutbox {
            extra: vec![MutationOutboxIntent {
                topic: "engine.crossmodal.committed".to_string(),
                key: batch_id,
                payload: rmp_serde::to_vec_named(&manifest).map_err(|e| e.to_string())?,
                headers: BTreeMap::new(),
            }],
            semantic_source_dirty_input: None,
            #[cfg(feature = "epistemic-tms")]
            reasoning_events: vec![eg_epistemic::IncrementalReasoningEvent::InvalidateAll],
        },
    )
}

/// The projection wake-up payload for one batch's operations.
///
/// It replaces `install_reasoning_wakeup`, which rewrote the finished batch's
/// outbox payload in place. That is now unreachable by construction: the
/// envelope's canonical payload digest covers the outbox, so the payload has to
/// be final before the envelope is minted, and this computes it there.
#[cfg(feature = "epistemic-tms")]
fn reasoning_wakeup_payload(
    operations: &[MutationOperation],
    events: Vec<eg_epistemic::IncrementalReasoningEvent>,
) -> Result<Vec<u8>, String> {
    let encoded = rmp_serde::to_vec_named(operations).map_err(|error| error.to_string())?;
    let wakeup = eg_epistemic::ReasoningProjectionWakeup::new(
        operations.len(),
        hex::encode(Sha256::digest(encoded)),
        events,
    )?;
    rmp_serde::to_vec_named(&wakeup).map_err(|error| error.to_string())
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
/// Everything a compiled batch's outbox carries beyond its projection wake-up.
///
/// It is an INPUT to [`finish_batch`], not something a caller appends
/// afterwards. The envelope's canonical payload digest covers the outbox, and
/// `MutationBatch::validate` re-checks that it covers THIS batch, so an intent
/// pushed after the mint is refused at admission. Passing the intents in is the
/// construction that makes that unreachable.
#[derive(Default)]
pub(crate) struct CompiledOutbox {
    /// Intents appended after the projection wake-up, in order.
    pub extra: Vec<MutationOutboxIntent>,
    /// Stable SQL operation input used to build a typed source-dirty intent
    /// after [`finish_batch`] has constructed the canonical native identity.
    pub semantic_source_dirty_input: Option<eg_types::semantic_index::SemanticDigest>,
    /// The reasoning wake-up events the projection intent's payload encodes.
    /// Empty means the plain digest-only summary.
    #[cfg(feature = "epistemic-tms")]
    pub reasoning_events: Vec<eg_epistemic::IncrementalReasoningEvent>,
}

fn finish_batch(
    ctx: CompileBatch<'_>,
    operations: Vec<MutationOperation>,
    graph_scope: bool,
    outbox_plan: CompiledOutbox,
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
    // `dispatch.rs` fix (commit `bc280437`): a caller reaching a cfg-gated producer
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
    // The typed wake-up is computed HERE, before the envelope is minted, rather
    // than installed over the finished batch afterwards: the envelope's
    // canonical payload digest covers the outbox, so a payload rewritten after
    // the mint would leave the envelope covering bytes the batch no longer has.
    #[cfg(feature = "epistemic-tms")]
    let summary = if outbox_plan.reasoning_events.is_empty() {
        summary
    } else {
        reasoning_wakeup_payload(&operations, outbox_plan.reasoning_events)?
    };
    let mut scope_digest = Sha256::new();
    scope_digest.update(ctx.tenant.as_bytes());
    scope_digest.update([0]);
    scope_digest.update(ctx.graph.as_bytes());
    let scope_digest = hex::encode(scope_digest.finalize());
    let actor =
        principal_fingerprint(ctx.principal.ok_or_else(|| {
            "durable mutation authority requires a verified principal".to_string()
        })?)?;
    // One rule, no per-domain arm: the batch context principal is the serving
    // principal every kernel-owned store in this process is bound under, and
    // the verified caller is the outbox `actor` header.
    //
    // RF-RULING-004's application note (2026-09-06) made this uniform. The
    // per-domain form it replaced answered "caller" for graph-scoped batches and
    // "serving principal" only for store-authoritative ones, which was right
    // while the graph shard was a raw file. Once `graph-N.redb` is a kernel-owned
    // store under `OwnerLayout::GraphShard`, EVERY shard row is an owner row, so
    // a graph batch is admitted through
    // `eg_transaction::AdmittedMutation::owner_rows` exactly like a KV or blob
    // batch -- and that path accepts only the principal the file's serving scope
    // is bound under. A per-domain answer makes every served graph mutation fail
    // at runtime the moment the shard is cut over.
    //
    // Nothing is lost: the caller is on EVERY batch this module compiles, in the
    // `actor` header, and every replay check that used to compare
    // `context.principal` against a caller now compares that header.
    let principal = ENGINE_LEDGER_PRINCIPAL.to_string();
    reject_reserved_shard_identifiers(ctx.tenant, ctx.graph)?;
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
            key: ctx.batch_id.to_string(),
            payload: summary,
            // `actor` is the verified caller's fingerprint on EVERY batch this
            // module compiles, whichever ledger commits it. It is the single
            // source of caller attribution, so an owner-store batch — whose
            // `context.principal` must be the store's serving principal — loses
            // none, and a graph batch gains no second, divergent copy.
            headers: BTreeMap::from([
                ("scope_sha256".to_string(), scope_digest.clone()),
                ("actor".to_string(), actor.clone()),
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
                    .insert("scope_sha256".to_string(), scope_digest.clone());
                intent.headers.insert("actor".to_string(), actor.clone());
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
            key: ctx.batch_id.to_string(),
            payload: intent.to_canonical_cbor().map_err(|error| {
                format!("semantic source-dirty intent encoding rejected: {error:?}")
            })?,
            headers: BTreeMap::new(),
        });
    }
    let envelope = compiled_envelope(&ctx, &identity, &actor, &principal, &operations, &outbox)?;
    let batch = MutationBatch {
        schema_version: MUTATION_BATCH_VERSION,
        batch_id: ctx.batch_id.to_string(),
        envelope,
        identity,
        placement_epoch,
        version_expectation,
        fencing_token,
        authoritative_state: ctx.authoritative_state,
        operations,
        outbox,
        created_at_ms: ctx.created_at_ms,
    };
    batch.validate()?;
    Ok(batch)
}

/// Mint the admission envelope for one compiled batch.
///
/// This is where the four minting rules become code, once, for every entrypoint
/// this module serves:
///
/// * the ATTEMPT nonce is the verified transport nonce when the request carried
///   one and is server-minted otherwise -- never caller-chosen;
/// * the CANONICAL PAYLOAD digest covers the operation content and structurally
///   excludes the OCC expectation, the route and the clock, which is what makes
///   a legitimate retry a replay instead of an `IDEMPOTENCY_CONFLICT`;
/// * the METHOD identity comes from the contract catalog `gen_contract` compiles
///   in, so it cannot be a file read at admission time;
/// * the POLICY epoch is the deployment constant `POLICY_EPOCH`, and the policy
///   digest is computed from the method's own declared policy, so "the policy
///   changed" is a real conflict rather than a nominal one.
fn compiled_envelope(
    ctx: &CompileBatch<'_>,
    identity: &MutationScopeIdentity,
    actor: &str,
    serving_principal: &str,
    operations: &[MutationOperation],
    outbox: &[MutationOutboxIntent],
) -> Result<eg_types::mutation_batch::MutationEnvelope, String> {
    let content = eg_types::mutation_batch::BatchContent {
        operations,
        outbox,
        authoritative_state: ctx.authoritative_state.as_ref(),
    };
    let method =
        eg_types::mutation_batch::batch_method_id(operations, ctx.authoritative_state.is_some())?;
    let operation = eg_types::mutation_batch::CompiledOperation::for_content(
        identity,
        content,
        method_schema_digest(&method)?,
    )?;
    let mut parts = eg_types::mutation_batch::CompiledEnvelope::new(
        eg_types::mutation_batch::CompiledScope {
            identity,
            actor,
            serving_principal,
            request_id: ctx.request_id,
            idempotency_key: ctx.idempotency_key,
            nonce: ctx
                .attempt_nonce
                .unwrap_or_else(eg_types::contract::Nonce::minted),
            now_ms: ctx.created_at_ms,
        },
        operation,
    )?;
    parts.catalog_digest = contract_catalog_digest()?;
    parts.policy_digest = effective_policy_digest(&method, operations)?;
    eg_types::mutation_batch::MutationEnvelope::for_compiled_batch(parts)
}

/// The request-schema digest one method's identity binds.
///
/// A method the contract declares gets its generated row's digest. A reserved
/// batch id -- the shape of a multi-operation or state-backed batch -- has no
/// wire method and therefore no request schema, so its digest frames the
/// reserved id itself rather than pretending a schema exists. The `SchemaId`
/// itself is derived by the one documented rule in
/// `eg_types::mutation_batch::method_schema_id`, which the generated table also
/// follows, so the two cannot disagree.
fn method_schema_digest(
    method: &eg_types::contract::MethodId,
) -> Result<eg_types::contract::Digest256, String> {
    match eg_capabilities::method_schema(method.as_str()) {
        Some((_, digest)) => Ok(eg_types::contract::Digest256::from_bytes(digest)),
        None => eg_types::mutation_batch::reserved_method_schema_digest(method),
    }
}

/// The contract catalog digest every identity minted by this engine binds.
fn contract_catalog_digest() -> Result<eg_types::contract::Digest256, String> {
    eg_types::contract::Digest256::parse(eg_capabilities::CONTRACT_CATALOG_DIGEST)
        .map_err(|_| "the compiled-in contract catalog digest is not a sha256 value".to_string())
}

/// The digest of the policy this batch was admitted under.
///
/// Computed from data that already exists -- the configured policy revision and
/// the method policies `eg-capabilities` declares for the batch's own operations
/// -- so a policy change moves the identity and conflicts a reused idempotency
/// key, which is what RF-RULING-004 means by "a changed policy conflicts".
pub(crate) fn effective_policy_digest(
    method: &eg_types::contract::MethodId,
    operations: &[MutationOperation],
) -> Result<eg_types::contract::Digest256, String> {
    let mut folded = eg_types::contract::Digest256::framed(
        b"eg/effective-policy-methods/v1",
        &[method.as_str().as_bytes()],
    )?;
    for operation in operations {
        let policy = eg_capabilities::policy(&operation.method);
        folded = eg_types::contract::Digest256::framed(
            b"eg/effective-policy-method/v1",
            &[
                folded.as_bytes(),
                policy.authz_action.as_bytes(),
                &[u8::from(policy.mutates), u8::from(policy.is_durable())],
            ],
        )?;
    }
    eg_types::contract::Digest256::framed(
        b"eg/effective-policy/v1",
        &[
            policy_revision().as_bytes(),
            folded.as_bytes(),
            &eg_types::mutation_batch::POLICY_EPOCH.to_be_bytes(),
        ],
    )
}

/// The deployment's configured policy revision, or the documented constant a
/// deployment that configured none carries.
///
/// It is inside the replay identity, so a deployment that later configures a
/// real revision correctly conflicts a key minted under the unset one rather
/// than silently replaying an operation decided under a different policy.
fn policy_revision() -> String {
    std::env::var("EPISTEMIC_GRAPH_POLICY_VERSION")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| eg_types::mutation_batch::UNSET_POLICY_REVISION.to_string())
}

/// Refuse the two reserved shard identifiers to a request-boundary caller.
///
/// RF-RULING-004 application note 2 makes a graph-shard scope
/// `(GRAPH_SHARD_TENANT, graph, incarnation)`, and note 1 reserves
/// `GRAPH_SHARD_CONTROL_GRAPH` for the shard file's own file-wide rows. Both are
/// scope-identity components, so a caller able to name either could compile a
/// batch whose identity COLLIDES with the shard's own — taking the shard's OCC
/// counter and fence, or writing under its control scope.
///
/// Checked here because this is the one place a caller-supplied tenant and graph
/// become a `MutationScopeIdentity`. `redb_store::reject_reserved_graph` guards
/// the durable chokepoints below for the graph half; nothing below this point
/// ever sees the tenant, which is why the tenant half can only be caught here.
#[cfg(feature = "redb")]
fn reject_reserved_shard_identifiers(tenant: &str, graph: &str) -> Result<(), String> {
    if tenant == eg_storage::GRAPH_SHARD_TENANT {
        return Err(
            "'__shard__' is the graph shard's reserved scope tenant and cannot be a caller tenant"
                .to_string(),
        );
    }
    if graph == eg_storage::GRAPH_SHARD_CONTROL_GRAPH {
        return Err(
            "'__shard_control__' is the shard's reserved control scope and cannot be a caller graph"
                .to_string(),
        );
    }
    Ok(())
}

/// Without `redb` this binary owns no shard file, so neither name is reserved
/// against anything — but the refusal is kept so a slim build cannot be the one
/// that mints an identity a redb build would refuse.
#[cfg(not(feature = "redb"))]
fn reject_reserved_shard_identifiers(tenant: &str, graph: &str) -> Result<(), String> {
    if tenant == "__shard__" || graph == "__shard_control__" {
        return Err("reserved graph-shard identifier cannot be supplied by a caller".to_string());
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_run_event_replaces_generic_projection_intent() {
        let operation = MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Transaction,
            domain: DurabilityDomain::ControlPlane,
            method: Method::RemoveNode {
                node_id: "work:terminal".into(),
            },
        };
        let terminal_event = MutationOutboxIntent {
            topic: eg_types::outcome_bundle::RUN_EVENT_OUTBOX_TOPIC.into(),
            key: "terminal-batch".into(),
            payload: vec![1],
            headers: BTreeMap::new(),
        };
        let batch = finish_batch(
            CompileBatch {
                batch_id: "terminal-batch",
                request_id: 1,
                attempt_nonce: None,
                principal: Some("agent:terminal-test"),
                tenant: "tenant-a",
                graph: "graph-a",
                placement_epoch: 0,
                idempotency_key: "terminal-idempotency",
                expected_graph_version: Some(0),
                fencing_token: None,
                created_at_ms: 1,
                default_surface: MutationSurface::Transaction,
                authoritative_state: None,
            },
            vec![operation],
            true,
            CompiledOutbox {
                extra: vec![terminal_event],
                semantic_source_dirty_input: None,
                #[cfg(feature = "epistemic-tms")]
                reasoning_events: Vec::new(),
            },
        )
        .expect("terminal outbox should produce a valid batch");

        assert_eq!(batch.outbox.len(), 1);
        assert_eq!(
            batch.outbox[0].topic,
            eg_types::outcome_bundle::RUN_EVENT_OUTBOX_TOPIC
        );
        let expected_actor = principal_fingerprint("agent:terminal-test").unwrap();
        assert_eq!(batch.outbox[0].headers.get("actor"), Some(&expected_actor));
        let expected_scope = hex::encode(Sha256::digest(b"tenant-a\0graph-a"));
        assert_eq!(
            batch.outbox[0].headers.get("scope_sha256"),
            Some(&expected_scope)
        );
    }
}
