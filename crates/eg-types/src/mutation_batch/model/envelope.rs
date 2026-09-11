//! The admission header every durable batch carries (RF-RULING-004/005).
//!
//! A [`MutationEnvelope`] is what the mutation kernel admits a batch *under*: the
//! verified authority for a caller operation, or the store's own declaration for
//! an owner-maintenance write. It replaces the deleted `MutationRequestContext`,
//! whose six fields could answer neither "which operation is this" nor "which
//! attempt is this" -- the two questions RF-RULING-004 gives separate identities.
//!
//! # Why this is an enum and not a class argument
//!
//! `MutationClass` used to be an argument to admission, so nothing could stop a
//! caller-identified write from being admitted as maintenance, and nothing
//! required an operation batch to carry a replay identity at all. Both invalid
//! states are unrepresentable here: an [`OperationEnvelope`] cannot exist without
//! the authority its identities are derived from, and a [`MaintenanceEnvelope`]
//! has no authority to derive them from.
//!
//! # Why the two identities are derived, never stored
//!
//! [`OperationReplayIdentity`] and [`NonceReplayKey`] are *functions* of the
//! authority plus the compiled method/payload facts, so storing them alongside
//! their inputs would be two copies of one value (RF-ADR-001) and would let a
//! forged or stale digest ride a genuine context. They are computed on demand by
//! [`OperationEnvelope::operation_identity`] and
//! [`OperationEnvelope::nonce_replay_key`], which is also what makes
//! "the identity structurally cannot contain a timestamp" checkable: the identity
//! type has no such field and this envelope hands it none.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use super::MutationCapability;
use super::{
    MutationOperation, MutationOutboxIntent, MutationScope, MutationScopeIdentity,
    MutationStateDescriptor,
};
use crate::authority::{AuthorityContext, AuthorityScope, NonceReplayKey, OperationReplayIdentity};
use crate::contract::{
    ActorId, AudienceId, BoundedVec, Digest256, IdempotencyKey, IngressSurface, MethodId, Nonce,
    OpaqueId, Operation, PolicyRevision, ProtocolId, PurposeKind, ResourceId, SchemaId, ScopeKind,
    TenantId, UtcUnixNanos,
};

/// The engine has one policy generation per deployment.
///
/// Deliberately NOT a per-process load counter: the policy epoch is inside
/// [`OperationReplayIdentity`], so a per-process value would give the same
/// operation a different identity after a restart and turn every crash-retry
/// replay into an `IDEMPOTENCY_CONFLICT`. It goes live when a durable policy
/// store lands and can name a real generation.
pub const POLICY_EPOCH: u64 = 0;

/// The admission header of one durable batch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum MutationEnvelope {
    /// A caller operation: verified authority, one stable operation identity and
    /// one attempt nonce identity.
    Operation(Box<OperationEnvelope>),
    /// An owner-maintenance write: no caller, therefore no operation identity and
    /// no attempt nonce (RF-RULING-005).
    Maintenance(MaintenanceEnvelope),
}

/// The verified authority one caller operation was admitted under.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct OperationEnvelope {
    /// The complete attempt-specific authority: nonce, request/trace id, issue
    /// and expiry, catalog digest, actor, audience, tenant, authority scope,
    /// purpose, policy revision/epoch/digest and the caller's idempotency key.
    pub authority: AuthorityContext,
    /// The one method this operation is. For a multi-operation batch it is the
    /// reserved batch descriptor id and every member's method is folded into
    /// `canonical_payload_digest` instead.
    pub method: MethodId,
    pub method_schema_id: SchemaId,
    pub method_schema_digest: Digest256,
    /// A digest over the operation CONTENT alone -- scope, operations, outbox
    /// intents and any authoritative-state digest. It deliberately excludes the
    /// OCC expectation, the route (placement epoch / fencing token), the batch
    /// id, timestamps and the request/trace ids, because a second attempt of the
    /// same operation legitimately re-observes all of them.
    pub canonical_payload_digest: Digest256,
    /// The principal the COMMITTING LEDGER requires -- for every domain, the
    /// store's serving principal (RF-RULING-004 application note). Caller
    /// attribution is the outbox `actor` header and lives nowhere else.
    pub serving_principal: String,
    /// Capabilities verified at the authenticated admission boundary. Attempt
    /// facts, not identity: they gate `VersionExpectation::Unversioned` and are
    /// deliberately outside `OperationReplayIdentity`.
    pub verified_capabilities: BTreeSet<MutationCapability>,
}

/// An owner-maintenance write: compaction, retention, index initialization, a
/// content-addressed definition insert.
///
/// It is a full mutation -- ledgered, fenced, class-rowed and version-bumping --
/// but it has no caller, so it owns no [`OperationReplayIdentity`], can never
/// consume an attempt nonce, and claims first-wins on `maintenance_key` alone.
/// `kind` and `subject` are mandatory so the ledger row NAMES what it wrote
/// rather than recording that something was written.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct MaintenanceEnvelope {
    pub serving_principal: String,
    /// What kind of maintenance this is: `index-rebuild`, `series-evict`, ...
    pub kind: ResourceId,
    /// The exact subject the ledger row must name.
    pub subject: ResourceId,
    /// `digest(kind, subject, scope, version)` -- the first-wins claim key.
    pub maintenance_key: IdempotencyKey,
}

/// The canonical `OpaqueId` spelling of a dispatch request number.
///
/// One encoder, so the certification-fault matcher and the compile path cannot
/// disagree about what "request 7" is called inside an authority context.
pub fn request_opaque_id(request_id: u64) -> String {
    format!("request:{request_id}")
}

/// The dispatch request number one authority context was minted for.
///
/// The inverse of [`request_opaque_id`], kept beside it so the encoding has one
/// definition and no reader re-derives the prefix. `None` for a context minted
/// by something other than a dispatch request -- which is a fact about the
/// batch, never a value to invent.
pub fn request_number(request_id: &OpaqueId) -> Option<u64> {
    request_id
        .as_str()
        .strip_prefix("request:")
        .and_then(|value| value.parse().ok())
}

/// The dispatch request number a batch was compiled for, if it has one.
pub fn batch_request_number(batch: &crate::mutation_batch::MutationBatch) -> Option<u64> {
    batch
        .envelope
        .operation()
        .and_then(|operation| request_number(&operation.authority.request_id))
}

/// Every value [`MutationEnvelope::for_compiled_batch`] needs, named once.
///
/// A parameter struct rather than twenty positional arguments: this is the seam
/// the request boundary fills, and a positional list of same-typed identifiers is
/// exactly the shape that silently transposes two of them. [`Self::new`] fills
/// the deployment-constant half so a producer names only what is its own; the
/// three fields a verified request carrier supplies -- `catalog_digest`,
/// `policy_revision`, `policy_digest` -- are public and overridden there.
pub struct CompiledEnvelope {
    pub catalog_digest: Digest256,
    pub tenant: String,
    /// The verified caller's fingerprint. Never the serving principal.
    pub actor: String,
    pub audience: String,
    pub serving_principal: String,
    pub ingress_surface: String,
    pub authority_scope: AuthorityScope,
    pub request_id: String,
    pub trace_id: String,
    pub nonce: Nonce,
    pub idempotency_key: String,
    pub policy_revision: String,
    pub policy_decision_id: String,
    pub policy_digest: Digest256,
    pub issued_at_ms: u64,
    pub expires_at_ms: u64,
    pub method: MethodId,
    pub method_schema_id: SchemaId,
    pub method_schema_digest: Digest256,
    pub canonical_payload_digest: Digest256,
    pub verified_capabilities: BTreeSet<MutationCapability>,
}

/// How long an admitted authority context is valid for. It is attempt metadata
/// -- outside `OperationReplayIdentity` -- so its only job is to make expiry
/// representable at all; the kernel never re-checks it after admission.
const AUTHORITY_VALIDITY_MS: u64 = 60_000;

/// Who is writing, where, and under which retry key: the facts a producer owns.
pub struct CompiledScope<'a> {
    pub identity: &'a MutationScopeIdentity,
    /// The verified caller's fingerprint. Never the serving principal.
    pub actor: &'a str,
    /// The principal the committing ledger requires.
    pub serving_principal: &'a str,
    pub request_id: u64,
    pub idempotency_key: &'a str,
    /// The attempt nonce. Server-minted per attempt, or the verified transport
    /// nonce when the request carried one.
    pub nonce: Nonce,
    pub now_ms: u64,
}

/// What is being written: the contract-registry identity of the operation plus
/// the digest of its content.
pub struct CompiledOperation {
    pub method: MethodId,
    pub method_schema_id: SchemaId,
    pub method_schema_digest: Digest256,
    pub canonical_payload_digest: Digest256,
}

impl CompiledEnvelope {
    /// A compiled envelope with every deployment-constant field filled in.
    ///
    /// The three a verified request carrier owns -- `catalog_digest`,
    /// `policy_revision`, `policy_digest` -- are public fields the request
    /// boundary overwrites; a producer with no carrier keeps this deployment's
    /// documented constants, which are inside the replay identity and therefore
    /// conflict correctly against keys minted under different ones.
    pub fn new(scope: CompiledScope<'_>, operation: CompiledOperation) -> Result<Self, String> {
        Ok(Self {
            catalog_digest: Digest256::from_bytes([0_u8; 32]),
            tenant: scope.identity.tenant().as_str().to_string(),
            actor: scope.actor.to_string(),
            audience: ENGINE_AUDIENCE.to_string(),
            serving_principal: scope.serving_principal.to_string(),
            ingress_surface: DEFAULT_INGRESS_SURFACE.to_string(),
            authority_scope: authority_scope_for(scope.identity)?,
            request_id: request_opaque_id(scope.request_id),
            trace_id: request_opaque_id(scope.request_id),
            nonce: scope.nonce,
            idempotency_key: scope.idempotency_key.to_string(),
            policy_revision: UNSET_POLICY_REVISION.to_string(),
            policy_decision_id: request_opaque_id(scope.request_id),
            policy_digest: Digest256::from_bytes([0_u8; 32]),
            issued_at_ms: scope.now_ms,
            expires_at_ms: scope.now_ms.saturating_add(AUTHORITY_VALIDITY_MS),
            method: operation.method,
            method_schema_id: operation.method_schema_id,
            method_schema_digest: operation.method_schema_digest,
            canonical_payload_digest: operation.canonical_payload_digest,
            verified_capabilities: BTreeSet::new(),
        })
    }
}

const RESERVED_GRAPH_SHARD_TENANT: &str = "__shard__";

/// The authority scope one mutation scope targets.
///
/// The mutation scope identity already names exactly what is being written --
/// tenant, kind, resource -- so deriving the authority scope from it is what
/// keeps the two from disagreeing. A native scope additionally carries its
/// durability domain as a parent, because two domains' stores may legitimately
/// hold a resource of the same name and they are not one authority. This is the
/// strict logical/caller path; the graph shard's physical path is explicit in
/// [`authority_scope_for_physical_graph`].
pub fn authority_scope_for(identity: &MutationScopeIdentity) -> Result<AuthorityScope, String> {
    let tenant = TenantId::new(identity.tenant().as_str())?;
    let (kind, scope_id, mut parents) = match identity.scope() {
        MutationScope::Graph { graph } => (
            "graph",
            ResourceId::new(graph.as_str())?,
            vec![ResourceId::new(tenant.as_str())?],
        ),
        MutationScope::Native { domain, resource } => (
            "native",
            ResourceId::new(resource.as_str())?,
            vec![
                ResourceId::new(tenant.as_str())?,
                ResourceId::new(format!("domain:{}", domain.canonical_name()))?,
            ],
        ),
    };
    parents.sort();
    parents.dedup();
    Ok(AuthorityScope {
        kind: ScopeKind::new(kind)?,
        scope_id,
        tenant: Some(tenant),
        parent_scope_ids: BoundedVec::new(parents)?,
        graph_incarnation: None,
    })
}

/// The shard's physical graph authority conversion.
///
/// A caller's [`authority_scope_for`] path stays strict: a `~xx` spelling is
/// not a logical resource id and is rejected before shard rebinding. The graph
/// shard is the one trusted physical boundary that stores sanitized names in
/// its scope identity. The conversion retains the complete physical spelling
/// in an internal subject namespace, so ordinary escapes and hash keys cannot
/// alias. Its reserved tenant is the type-level fence that keeps this
/// conversion out of caller identities; the request compiler rejects that
/// tenant for external callers.
pub(crate) fn authority_scope_for_physical_graph(
    identity: &MutationScopeIdentity,
) -> Result<AuthorityScope, String> {
    if identity.tenant().as_str() != RESERVED_GRAPH_SHARD_TENANT {
        return Err("physical graph authority requires the reserved shard tenant".to_string());
    }
    let MutationScope::Graph { graph } = identity.scope() else {
        return Err("physical graph authority requires a graph scope".to_string());
    };
    let tenant = TenantId::new(identity.tenant().as_str())?;
    let tenant_resource = ResourceId::new(tenant.as_str())?;
    Ok(AuthorityScope {
        kind: ScopeKind::new("graph")?,
        scope_id: ResourceId::from_physical_graph_key(graph.as_str())?,
        tenant: Some(tenant),
        parent_scope_ids: BoundedVec::new(vec![tenant_resource])?,
        graph_incarnation: None,
    })
}

fn authority_scope_for_bound_identity(
    identity: &MutationScopeIdentity,
) -> Result<AuthorityScope, String> {
    if identity.tenant().as_str() == RESERVED_GRAPH_SHARD_TENANT
        && matches!(identity.scope(), MutationScope::Graph { .. })
    {
        authority_scope_for_physical_graph(identity)
    } else {
        authority_scope_for(identity)
    }
}

/// The reserved descriptor ids a MULTI-operation batch's identity carries.
///
/// `OperationReplayIdentity` names ONE method. When a batch compiles exactly one
/// operation the method is that operation's own id; otherwise it is the reserved
/// id for the shape of the batch, and every member's `(MethodId, SchemaId,
/// schema digest)` is folded into `canonical_payload_digest` instead. That keeps
/// the identity total without inventing per-batch method semantics.
pub const BATCH_COMPILED_METHODS: &str = "batch.compiled_methods";
pub const BATCH_CROSSMODAL: &str = "batch.crossmodal";
pub const BATCH_AUTHORITATIVE_STATE: &str = "batch.authoritative_state";
/// The method a batch with no contract-registry row carries -- an owner store's
/// own write, which is a mutation the engine contract declares no wire method
/// for.
pub const BATCH_OWNER_WRITE: &str = "batch.owner_write";

/// The method one compiled batch's replay identity carries.
///
/// `OperationReplayIdentity` names ONE method, and a batch may compile several
/// operations. The rule, applied here once instead of per entrypoint: exactly
/// one operation means that operation's own serde tag; anything else means the
/// reserved id for the SHAPE of the batch, and every member's method travels in
/// `canonical_payload_digest` instead. The identity stays total without
/// inventing per-batch method semantics.
pub fn batch_method_id(
    operations: &[MutationOperation],
    authoritative_state: bool,
) -> Result<MethodId, String> {
    if authoritative_state {
        return MethodId::new(BATCH_AUTHORITATIVE_STATE);
    }
    match operations {
        [] => Err("a mutation batch has no operation to derive its method from".to_string()),
        [single] => MethodId::new(single.method.tag_name()),
        _ => MethodId::new(BATCH_COMPILED_METHODS),
    }
}

/// The digest of one batch's operation CONTENT.
///
/// INCLUDED, and why: the scope (two tenants' identical effects are different
/// operations); every operation's ordinal, surface, domain and canonical bytes;
/// every outbox intent's topic, key, payload digest and sorted headers; and the
/// authoritative-state digest, because a state-backed batch's payload IS its
/// snapshot.
///
/// EXCLUDED, and why: `batch_id` (a derived name, not content), `created_at_ms`
/// (wall clock), `placement_epoch` and `fencing_token` (route, re-observed per
/// attempt), `version_expectation` (an OCC observation, re-taken per attempt --
/// this is the exclusion that makes a crash-retry a replay instead of a
/// conflict), the state descriptor's source/target graph versions (OCC again),
/// and the request and trace ids (attempt metadata). Every one of those
/// legitimately differs between two attempts of the SAME operation, which is
/// exactly why the whole-batch byte comparison this replaces could never accept
/// a legitimate retry.
pub fn canonical_payload_digest(
    identity: &MutationScopeIdentity,
    operations: &[MutationOperation],
    outbox: &[MutationOutboxIntent],
    authoritative_state: Option<&MutationStateDescriptor>,
) -> Result<Digest256, String> {
    let scope = authority_scope_for_bound_identity(identity)?.digest()?;
    let operations = digest_operations(operations)?;
    let outbox = digest_outbox(outbox)?;
    let state = match authoritative_state {
        Some(state) => Digest256::framed(
            b"eg/canonical-payload-state/v1",
            &[state.algorithm.as_bytes(), state.digest.as_bytes()],
        )?,
        None => Digest256::framed(b"eg/canonical-payload-state/v1", &[])?,
    };
    Digest256::framed(
        b"eg/canonical-payload/v1",
        &[
            scope.as_bytes(),
            operations.as_bytes(),
            outbox.as_bytes(),
            state.as_bytes(),
        ],
    )
}

/// Blank the AUTHORITY-STAMPED clock a dispatch writes into a request body.
///
/// `now_ms` on the resource/capacity request DTOs is not caller content. The
/// protocol says so ("Dispatch must normalize/overwrite that field from the
/// authoritative engine clock before authorization or persistence; a
/// client-supplied timestamp is never trusted") and
/// `dispatch::graph_pipeline::stamp_resource_and_capacity_timestamps`
/// unconditionally rewrites it from `authoritative_now_ms()` on EVERY
/// dispatch -- so two attempts of one operation carry two different values
/// through no act of the caller's.
///
/// Because the field lives INSIDE the method body, it was hashed verbatim into
/// the canonical payload digest and therefore into the stable
/// `OperationReplayIdentity`. A genuine retry of any of these methods almost
/// always lands on a different millisecond, so it presented a different stable
/// identity under the same idempotency key and was refused with
/// `IDEMPOTENCY_CONFLICT: ... was already used by a different operation`. No
/// terminal WorkItem resource or capacity mutation could be retried at all.
///
/// This is the same exclusion the digest already applies to `created_at_ms`,
/// the OCC expectation, the placement epoch and the request/trace ids -- the
/// values "re-observed per attempt" listed in `canonical_payload_digest`'s own
/// doc -- and the same normalization `redb_store::native_retry_method` has long
/// applied to these exact fields for the older, lower-level native retry
/// comparator that runs AFTER this identity check and so was never reached.
/// `now_ms` stays fully live for the operation's effect (lease, expiry and
/// fairness computation all still read the freshly stamped value); it simply
/// stops being part of WHICH operation this is.
/// The operation list with every authority-stamped clock blanked, for any
/// producer that derives IDENTITY-BEARING bytes from it.
///
/// There is exactly one such rule and this is it. The outbox projection payload
/// is folded into the same `canonical_payload_digest` as the operations
/// themselves, so normalizing only one of the two halves leaves the identity
/// varying per attempt just as before -- which is precisely what happened when
/// `digest_operations` was normalized and `finish_batch`'s
/// `projection_payload_for_operations` was not.
pub fn identity_normalized_operations(
    operations: &[MutationOperation],
) -> std::borrow::Cow<'_, [MutationOperation]> {
    use std::borrow::Cow;
    if !operations
        .iter()
        .any(|operation| matches!(identity_normalized_method(&operation.method), Cow::Owned(_)))
    {
        return Cow::Borrowed(operations);
    }
    Cow::Owned(
        operations
            .iter()
            .map(|operation| MutationOperation {
                ordinal: operation.ordinal,
                surface: operation.surface,
                domain: operation.domain,
                method: identity_normalized_method(&operation.method).into_owned(),
            })
            .collect(),
    )
}

fn identity_normalized_method(
    method: &crate::protocol::Method,
) -> std::borrow::Cow<'_, crate::protocol::Method> {
    use crate::protocol::Method;
    use std::borrow::Cow;
    let mut owned = match method {
        Method::ReserveWorkItemResources { .. }
        | Method::ReleaseWorkItemResources { .. }
        | Method::ReclaimWorkItemResources { .. }
        | Method::UpdateResourceHost { .. }
        | Method::AcquireCapacity { .. }
        | Method::RenewCapacity { .. }
        | Method::ReleaseCapacity { .. }
        | Method::ReclaimExpiredCapacity { .. }
        | Method::UpdateCapacityCell { .. } => method.clone(),
        other => return Cow::Borrowed(other),
    };
    match &mut owned {
        Method::ReserveWorkItemResources { request }
        | Method::ReleaseWorkItemResources { request }
        | Method::ReclaimWorkItemResources { request } => request.now_ms = 0,
        Method::UpdateResourceHost { request } => request.now_ms = 0,
        Method::AcquireCapacity { request } => request.now_ms = 0,
        Method::RenewCapacity { request } | Method::ReleaseCapacity { request } => {
            request.now_ms = 0
        }
        Method::ReclaimExpiredCapacity { request } => request.now_ms = 0,
        Method::UpdateCapacityCell { request } => request.now_ms = 0,
        _ => unreachable!("the clone arm above and this blanking arm must name the same methods"),
    }
    Cow::Owned(owned)
}

fn digest_operations(operations: &[MutationOperation]) -> Result<Digest256, String> {
    let mut folded = Digest256::framed(b"eg/canonical-payload-operations/v1", &[])?;
    for operation in operations {
        let normalized = identity_normalized_method(&operation.method);
        let method = rmp_serde::to_vec_named(normalized.as_ref()).map_err(|e| e.to_string())?;
        let ordinal = operation.ordinal.to_be_bytes();
        folded = Digest256::framed(
            b"eg/canonical-payload-operation/v1",
            &[
                folded.as_bytes(),
                &ordinal,
                format!("{:?}", operation.surface).as_bytes(),
                operation.domain.canonical_name().as_bytes(),
                method.as_slice(),
            ],
        )?;
    }
    Ok(folded)
}

fn digest_outbox(outbox: &[MutationOutboxIntent]) -> Result<Digest256, String> {
    let mut folded = Digest256::framed(b"eg/canonical-payload-outbox/v1", &[])?;
    for intent in outbox {
        let payload = Digest256::framed(b"eg/canonical-payload-intent/v1", &[&intent.payload])?;
        // `headers` is a `BTreeMap`, so iteration is already the sorted order the
        // digest needs; folding pairwise keeps a key/value boundary that a
        // concatenation would let two different header sets collide across.
        let mut headers = Digest256::framed(b"eg/canonical-payload-headers/v1", &[])?;
        for (key, value) in &intent.headers {
            headers = Digest256::framed(
                b"eg/canonical-payload-header/v1",
                &[headers.as_bytes(), key.as_bytes(), value.as_bytes()],
            )?;
        }
        folded = Digest256::framed(
            b"eg/canonical-payload-intent/v1",
            &[
                folded.as_bytes(),
                intent.topic.as_bytes(),
                intent.key.as_bytes(),
                payload.as_bytes(),
                headers.as_bytes(),
            ],
        )?;
    }
    Ok(folded)
}

/// The request-schema digest of a reserved batch method.
///
/// A reserved id (`batch.compiled_methods` and its siblings) names the SHAPE of
/// a multi-operation batch, which the engine contract declares no wire method
/// and therefore no request schema for. Framing the id itself keeps the identity
/// total and keeps the three reserved shapes distinct, without pretending a
/// schema exists.
pub fn reserved_method_schema_digest(method: &MethodId) -> Result<Digest256, String> {
    Digest256::framed(
        b"eg/reserved-batch-method/v1",
        &[method.as_str().as_bytes()],
    )
}

/// The generated request schema one `Method` variant's id points at.
///
/// Every descriptor's `request_schema` is `SchemaRef::MethodVariant`, and
/// `gen_contract` emits those variants into one document keyed by the serde tag,
/// so this pointer is a derivation with one rule rather than 408 authored
/// strings.
pub fn method_schema_id(method: &MethodId) -> Result<SchemaId, String> {
    SchemaId::new(format!(
        "contract/schemas/method.request.json#/methods/{}",
        method.as_str()
    ))
}

/// This engine is the audience of every authority it admits.
pub const ENGINE_AUDIENCE: &str = "epistemic-graph";
/// The surface a batch compiled inside the engine arrives on when no verified
/// request carrier named one.
pub const DEFAULT_INGRESS_SURFACE: &str = "au_rest";
/// The policy revision of a deployment that configured none. It is inside the
/// replay identity, so a deployment that later configures a real revision
/// correctly conflicts with keys minted under this one.
pub const UNSET_POLICY_REVISION: &str = "policy-unset";

impl CompiledOperation {
    /// The contract identity and content digest one batch's own body implies.
    ///
    /// Every producer mints its envelope through this, so the digest inside the
    /// envelope is by construction the digest OF the batch it rides on. That is
    /// what `MutationBatch::validate` then re-checks: an envelope that
    /// under-covers or mis-covers its own body -- an intent appended after the
    /// mint, an operation edited in place -- is refused rather than committed,
    /// because a replay resolved on such a digest could return a recorded result
    /// whose outbox or operations differ from what the caller proposed.
    pub fn for_content(
        identity: &MutationScopeIdentity,
        content: BatchContent<'_>,
        method_schema_digest: Digest256,
    ) -> Result<Self, String> {
        let method = batch_method_id(content.operations, content.authoritative_state.is_some())?;
        Ok(Self {
            method_schema_id: method_schema_id(&method)?,
            method,
            method_schema_digest,
            canonical_payload_digest: canonical_payload_digest(
                identity,
                content.operations,
                content.outbox,
                content.authoritative_state,
            )?,
        })
    }
}

/// The body of one compiled batch: everything its canonical payload digest
/// covers, named once so a producer cannot pass three of the four.
pub struct BatchContent<'a> {
    pub operations: &'a [MutationOperation],
    pub outbox: &'a [MutationOutboxIntent],
    pub authoritative_state: Option<&'a MutationStateDescriptor>,
}

impl MutationEnvelope {
    /// Mint the operation envelope for one compiled batch.
    ///
    /// This is the ONE constructor the compile path calls, so the four minting
    /// rules (server-minted attempt nonce, content-only payload digest, registry
    /// method identity, deployment policy epoch) have exactly one implementation
    /// instead of one per entrypoint.
    pub fn for_compiled_batch(parts: CompiledEnvelope) -> Result<Self, String> {
        let authority_scope = parts.authority_scope;
        let purpose_resource = authority_scope.scope_id.clone();
        let mut authority = AuthorityContext {
            schema_version: ResourceId::new(crate::authority::AUTHORITY_CONTEXT_SCHEMA_V1)?,
            protocol_id: ProtocolId::new(crate::authority::AUTHORITY_PROTOCOL_V1)?,
            catalog_digest: parts.catalog_digest,
            request_id: OpaqueId::new(parts.request_id.as_str())?,
            trace_id: OpaqueId::new(parts.trace_id.as_str())?,
            ingress_surface: IngressSurface::new(parts.ingress_surface.as_str())?,
            actor: ActorId::new(parts.actor.as_str())?,
            audience: AudienceId::new(parts.audience.as_str())?,
            tenant: TenantId::new(parts.tenant.as_str())?,
            authority_scope,
            purpose_kind: PurposeKind::new("graph_write")?,
            purpose_resource: Some(purpose_resource),
            operation: Operation::new("mutation")?,
            policy_revision: PolicyRevision::new(parts.policy_revision.as_str())?,
            policy_epoch: POLICY_EPOCH,
            policy_decision_id: OpaqueId::new(parts.policy_decision_id.as_str())?,
            policy_digest: parts.policy_digest,
            issued_at: millis_to_nanos(parts.issued_at_ms)?,
            expires_at: millis_to_nanos(parts.expires_at_ms)?,
            nonce: parts.nonce,
            idempotency_key: Some(IdempotencyKey::new(parts.idempotency_key.as_str())?),
            context_digest: Digest256::from_bytes([0_u8; 32]),
        };
        authority.context_digest = authority.recompute_context_digest()?;
        let envelope = OperationEnvelope {
            authority,
            method: parts.method,
            method_schema_id: parts.method_schema_id,
            method_schema_digest: parts.method_schema_digest,
            canonical_payload_digest: parts.canonical_payload_digest,
            serving_principal: parts.serving_principal,
            verified_capabilities: parts.verified_capabilities,
        };
        envelope.validate()?;
        Ok(Self::Operation(Box::new(envelope)))
    }

    /// The operation envelope for a batch on one mutation scope, with every
    /// deployment-constant field defaulted.
    ///
    /// The composition of [`authority_scope_for`], [`CompiledEnvelope::new`] and
    /// [`Self::for_compiled_batch`] -- one call for a producer that has no
    /// verified request carrier to override the carrier fields with. It is not a
    /// second path: it builds the same `CompiledEnvelope` and hands it to the
    /// same constructor.
    pub fn for_scope(
        scope: CompiledScope<'_>,
        operation: CompiledOperation,
    ) -> Result<Self, String> {
        Self::for_compiled_batch(CompiledEnvelope::new(scope, operation)?)
    }

    /// Declare one owner-maintenance write.
    pub fn maintenance(
        serving_principal: &str,
        kind: &str,
        subject: &str,
        maintenance_key: &str,
    ) -> Result<Self, String> {
        let envelope = MaintenanceEnvelope {
            serving_principal: serving_principal.to_string(),
            kind: ResourceId::new(kind)?,
            subject: ResourceId::new(subject)?,
            maintenance_key: IdempotencyKey::new(maintenance_key)?,
        };
        Ok(Self::Maintenance(envelope))
    }

    /// An owner-maintenance write whose subject is the scope's own resource.
    ///
    /// The common shape: a compaction, retention, snapshot or index write that
    /// maintains a whole store rather than one named row. The finer-grained
    /// target it acted on travels in the operation, where a value too large or
    /// too free-form to be a `ResourceId` can live; what the ledger row names
    /// structurally is the store.
    pub fn maintenance_for_scope(
        identity: &MutationScopeIdentity,
        serving_principal: &str,
        kind: &str,
        maintenance_key: &str,
    ) -> Result<Self, String> {
        let subject = authority_scope_for_bound_identity(identity)?.scope_id;
        Self::maintenance(serving_principal, kind, subject.as_str(), maintenance_key)
    }

    /// The one durable ledger key for this batch: the caller's idempotency key
    /// for an operation, the maintenance claim key for a maintenance write.
    pub fn idempotency_key(&self) -> &str {
        match self {
            Self::Operation(envelope) => envelope.idempotency_key(),
            Self::Maintenance(envelope) => envelope.maintenance_key.as_str(),
        }
    }

    /// The principal the committing ledger requires. Never an owner.
    pub fn serving_principal(&self) -> &str {
        match self {
            Self::Operation(envelope) => envelope.serving_principal.as_str(),
            Self::Maintenance(envelope) => envelope.serving_principal.as_str(),
        }
    }

    pub fn operation(&self) -> Option<&OperationEnvelope> {
        match self {
            Self::Operation(envelope) => Some(envelope),
            Self::Maintenance(_) => None,
        }
    }

    pub fn is_maintenance(&self) -> bool {
        matches!(self, Self::Maintenance(_))
    }

    /// Capabilities verified at admission. A maintenance write has no caller and
    /// therefore no verified capability set.
    pub fn verified_capabilities(&self) -> Option<&BTreeSet<MutationCapability>> {
        self.operation().map(|envelope| &envelope.verified_capabilities)
    }

    pub fn validate(&self) -> Result<(), String> {
        match self {
            Self::Operation(envelope) => envelope.validate(),
            Self::Maintenance(envelope) => {
                validate_serving_principal(&envelope.serving_principal)
            }
        }
    }
}

/// The committing ledger is always bound to an opaque serving principal. Keep
/// this check on the envelope itself so a deserialized maintenance declaration
/// cannot become valid merely because it is wrapped in a `MutationBatch`.
pub(crate) fn validate_serving_principal(principal: &str) -> Result<(), String> {
    let valid = principal
        .strip_prefix("principal:sha256:")
        .is_some_and(|digest| {
            digest.len() == 64
                && digest
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        });
    if valid {
        Ok(())
    } else {
        Err("mutation principal authority must be an opaque digest".to_string())
    }
}

impl OperationEnvelope {
    /// The caller's stable retry key -- the one place it lives.
    pub fn idempotency_key(&self) -> &str {
        self.authority
            .idempotency_key
            .as_ref()
            .map_or("", IdempotencyKey::as_str)
    }

    /// The stable operation identity, derived rather than stored.
    pub fn operation_identity(&self) -> Result<OperationReplayIdentity, String> {
        OperationReplayIdentity::from_context(
            &self.authority,
            self.method.clone(),
            self.method_schema_id.clone(),
            self.method_schema_digest,
            self.canonical_payload_digest,
        )
    }

    /// The attempt identity, derived rather than stored.
    pub fn nonce_replay_key(&self) -> Result<NonceReplayKey, String> {
        NonceReplayKey::from_context(&self.authority)
    }

    pub fn validate(&self) -> Result<(), String> {
        self.authority.validate()?;
        if self.authority.idempotency_key.is_none() {
            return Err("operation envelope requires an idempotency key".to_string());
        }
        self.operation_identity()?.validate()
    }
}

/// Authority timestamps are UTC unix NANOSECONDS; every compiled batch carries
/// milliseconds. One conversion, checked, instead of a `* 1_000_000` at each
/// producer.
fn millis_to_nanos(millis: u64) -> Result<UtcUnixNanos, String> {
    i64::try_from(millis)
        .ok()
        .and_then(|millis| millis.checked_mul(1_000_000))
        .map(UtcUnixNanos::new)
        .ok_or_else(|| "authority timestamp exceeds the representable nanosecond range".to_string())
}
