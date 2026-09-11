//! Shared fixtures for the mutation-kernel test modules.

mod backup_replay;
mod confinement;
mod fault_restart;
mod graft;
mod ledger;
mod outbox;
mod recovery_binding;
mod replay;
mod scope_group;

use crate::read::{read_ledger, read_outbox, read_private_payload, version};
use crate::tables::{FENCES, OUTBOX, PRIVATE_PAYLOADS};
use crate::{Begin, MutationKernel};
use eg_storage::{
    strict_recovery_evidence, BlobOwner, LedgerOnlyOwner, OwnedStoreHandle, OwnerDomain,
    OwnerLayout, PhysicalStoreIdentity, PrivatePayloadIntegrity, ScopeGrantVerifier,
    StorageKernel,
};
use eg_types::authority::{
    AuthorityContext, AuthorityScope, NonceReplayKey, OperationReplayIdentity,
    AUTHORITY_CONTEXT_SCHEMA_V1, AUTHORITY_PROTOCOL_V1,
};
use eg_types::contract::{
    ActorId, AudienceId, BoundedVec, Digest256, IdempotencyKey, IngressSurface,
    MethodId, MutationDisposition, Nonce, OpaqueId, Operation, PolicyRevision,
    ProtocolId, PurposeKind, ResourceId, SchemaId, ScopeKind, TenantId, UtcUnixNanos,
};
use eg_types::mutation::{MutationReceipt, MutationResult};
use eg_types::mutation_batch::{
    IncarnationId, LogicalName, DurabilityDomain, MutationSurface, ScopeTenantId,
    VersionExpectation,
};
use eg_types::protocol::Method;
use eg_types::{MutationBatch, MutationOperation, MutationScopeIdentity, MUTATION_BATCH_VERSION};
use redb::{TableDefinition, TableHandle};
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;

/// One `cas_blobs` owner-row key.
///
/// `OwnerLayout::Blob`'s owner tables are bounded by the LAYOUT, not by a scope
/// component in the key (`open_owner_table`'s own doc), so a test that wants to
/// say "this row belongs to that serving scope" writes the scope into the key
/// text. Two scopes therefore still get two distinct rows, which is what these
/// tests actually assert.
fn blob_key(scope: &str, object: &str) -> String {
    format!("{scope}|{object}")
}

const PRINCIPAL: &str =
    "principal:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

struct TestIntegrity;

impl PrivatePayloadIntegrity for TestIntegrity {
    fn authenticate(&self, sealed: &[u8], expected_digest: &str) -> Result<(), String> {
        let expected = format!("sealed:{expected_digest}");
        (sealed == expected.as_bytes())
            .then_some(())
            .ok_or_else(|| "test canonical integrity rejection".to_string())
    }
}

struct TestScopeVerifier {
    tenant: &'static str,
    principal: &'static str,
    layout: OwnerLayout,
}

impl ScopeGrantVerifier for TestScopeVerifier {
    fn verify(
        &self,
        _physical: &PhysicalStoreIdentity,
        layout: OwnerLayout,
        identity: &MutationScopeIdentity,
        principal: &str,
        proof: &[u8],
    ) -> Result<(), String> {
        if layout != self.layout
            || identity.tenant().as_str() != self.tenant
            || principal != self.principal
            || proof != b"verified"
        {
            return Err("test scope authority rejected".to_string());
        }
        Ok(())
    }
}

fn verifier(tenant: &'static str, layout: OwnerLayout) -> TestScopeVerifier {
    TestScopeVerifier {
        tenant,
        principal: PRINCIPAL,
        layout,
    }
}

fn native_identity(tenant: &str, incarnation: &str) -> MutationScopeIdentity {
    MutationScopeIdentity::native(
        ScopeTenantId::new(tenant).unwrap(),
        DurabilityDomain::BlobStore,
        LogicalName::new("blob-catalog").unwrap(),
        IncarnationId::new(incarnation).unwrap(),
    )
    .unwrap()
}

/// One caller-operation batch on `identity`, admitted under `PRINCIPAL`.
///
/// The envelope is minted through the one public constructor, so a fixture
/// cannot become a second minting path: everything it does not name is this
/// deployment's documented constant, exactly as a producer with no verified
/// request carrier gets.
/// The operation envelope a fixture batch carries.
///
/// The attempt nonce is server-minted, exactly as a producer's is: it is the
/// ATTEMPT identity, so two distinct batches in one scope must never share one,
/// and a retry of the SAME operation is a FRESH attempt over an unchanged stable
/// identity -- the case the kernel must replay.
fn operation_envelope(
    identity: &MutationScopeIdentity,
    batch_id: &str,
) -> eg_types::mutation_batch::MutationEnvelope {
    let method =
        eg_types::contract::MethodId::new(eg_types::mutation_batch::BATCH_COMPILED_METHODS)
            .unwrap();
    eg_types::mutation_batch::MutationEnvelope::for_scope(
        eg_types::mutation_batch::CompiledScope {
            identity,
            actor: PRINCIPAL,
            serving_principal: PRINCIPAL,
            request_id: 1,
            idempotency_key: &format!("retry-{batch_id}"),
            nonce: eg_types::contract::Nonce::minted(),
            now_ms: 1,
        },
        eg_types::mutation_batch::CompiledOperation {
            method_schema_id: eg_types::mutation_batch::method_schema_id(&method).unwrap(),
            method,
            method_schema_digest: eg_types::contract::Digest256::from_bytes([1_u8; 32]),
            canonical_payload_digest: eg_types::contract::Digest256::from_bytes([2_u8; 32]),
        },
    )
    .unwrap()
}

/// The SAME operation, retried: a FRESH attempt nonce over an unchanged stable
/// identity.
///
/// That is what a producer builds on a retry -- `finish_batch` mints a new nonce
/// every time it compiles -- and it is the case the kernel must resolve
/// `ReplayedResult` for. Re-submitting the byte-identical batch value instead is
/// a duplicated ATTEMPT, which is a caller bug and is refused by name.
fn retry_of(batch: &MutationBatch) -> MutationBatch {
    let mut retried = batch.clone();
    retried.envelope = operation_envelope(&retried.identity, &retried.batch_id);
    retried
        .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
        .expect("a retried fixture batch reseals its envelope");
    retried
}

/// One owner-MAINTENANCE batch on `identity` (RF-RULING-005).
///
/// A maintenance write has no caller, so its envelope carries no authority to
/// derive an operation identity from and no attempt nonce to consume. Admitting
/// an operation batch as maintenance is now refused by `commit::begin`, which is
/// what makes the class structural rather than an admission argument.
fn maintenance_batch(identity: MutationScopeIdentity, batch_id: &str) -> MutationBatch {
    let mut batch = batch(identity, batch_id);
    batch.envelope = eg_types::mutation_batch::MutationEnvelope::maintenance(
        PRINCIPAL,
        "fixture_maintenance",
        batch_id,
        &format!("retry-{batch_id}"),
    )
    .expect("a fixture maintenance envelope is valid");
    batch
}

fn batch(identity: MutationScopeIdentity, batch_id: &str) -> MutationBatch {
    let envelope = operation_envelope(&identity, batch_id);
    let mut batch = MutationBatch {
        schema_version: MUTATION_BATCH_VERSION,
        batch_id: batch_id.to_string(),
        envelope,
        identity,
        placement_epoch: 0,
        version_expectation: VersionExpectation::Native(0),
        fencing_token: None,
        authoritative_state: None,
        operations: vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Other,
            domain: DurabilityDomain::BlobStore,
            method: Method::ApplyMutation {
                event_type: "blob_test".to_string(),
                query: "opaque".to_string(),
            },
        }],
        outbox: Vec::new(),
        created_at_ms: 1,
    };
    batch
        .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
        .expect("a fixture batch reseals its envelope over its final body");
    batch
}

fn recovery_batch(identity: MutationScopeIdentity, batch_id: &str) -> (MutationBatch, Vec<u8>) {
    let digest = "b".repeat(64);
    let mut batch = batch(identity, batch_id);
    batch.operations[0].method = Method::ApplyMutation {
        event_type: "transaction_recovery_plan".to_string(),
        query: format!("sha256:{digest}"),
    };
    batch
        .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
        .expect("a fixture batch reseals its envelope over its final body");
    (batch, format!("sealed:{digest}").into_bytes())
}

/// One owner file plus its single mutation kernel.
struct Fixture {
    kernel: StorageKernel,
    mutations: MutationKernel,
}

impl Fixture {
    fn create<D: OwnerDomain>(
        path: &Path,
        physical: &str,
        integrity: Option<Arc<dyn PrivatePayloadIntegrity>>,
    ) -> Self {
        let kernel = StorageKernel::create_owner::<D>(
            path,
            PhysicalStoreIdentity::new(physical).unwrap(),
            integrity,
        )
        .unwrap();
        Self::split(kernel)
    }

    fn open<D: OwnerDomain>(
        path: &Path,
        physical: &str,
        integrity: Option<Arc<dyn PrivatePayloadIntegrity>>,
    ) -> Self {
        let kernel = StorageKernel::open_owner::<D>(
            path,
            PhysicalStoreIdentity::new(physical).unwrap(),
            integrity,
        )
        .unwrap();
        Self::split(kernel)
    }

    /// The one mutation authority this fixture's kernel holds. Only test code
    /// that must write a deliberately malformed row needs it.
    fn mutations_authority(&self) -> &eg_storage::MutationOwnerAuthority {
        self.mutations.authority()
    }

    fn split(kernel: StorageKernel) -> Self {
        let (kernel, authority) = kernel.into_read_and_mutation_authority().unwrap();
        Self {
            kernel,
            mutations: MutationKernel::new(authority),
        }
    }

    fn bind<D: OwnerDomain>(
        &self,
        verifier: &dyn ScopeGrantVerifier,
        identity: MutationScopeIdentity,
    ) -> OwnedStoreHandle<D> {
        let grant = self
            .kernel
            .authenticate_scope::<D>(verifier, identity, PRINCIPAL.to_string(), b"verified")
            .unwrap();
        self.kernel.bind_serving_scope(grant, 0).unwrap()
    }
}

fn ledger_fixture(
    path: &Path,
    identity: MutationScopeIdentity,
) -> (Fixture, OwnedStoreHandle<LedgerOnlyOwner>) {
    let fixture = Fixture::create::<LedgerOnlyOwner>(path, "physical:test:ledger-only", None);
    let owner =
        fixture.bind::<LedgerOnlyOwner>(&verifier("tenant-a", OwnerLayout::LedgerOnly), identity);
    (fixture, owner)
}

fn apply_batch<D: OwnerDomain>(
    fixture: &Fixture,
    owner: &OwnedStoreHandle<D>,
    batch: &MutationBatch,
) {
    let (write, begun) = fixture.mutations.admit(owner, batch).unwrap();
    let source_version = match begun {
        Begin::Apply { source_version } => source_version,
        Begin::Replay(_) => panic!("unexpected replay"),
    };
    if D::LAYOUT != OwnerLayout::LedgerOnly {
        write
            .owner_rows(owner, batch)
            .unwrap()
            .finish_owner()
            .unwrap();
    }
    fixture
        .mutations
        .finish(&write, batch, None, 2, source_version)
        .unwrap();
    fixture.mutations.commit(write, batch).unwrap();
}

fn digest_of(byte: u8) -> Digest256 {
    Digest256::from_bytes([byte; 32])
}

/// One attempt context. `nonce` varies the attempt; every other field is the
/// stable operation identity.
fn context(nonce: u8, request: &str, idempotency: &str) -> AuthorityContext {
    let scope_id = ResourceId::new("graph:tenant:a/g").unwrap();
    let mut value = AuthorityContext {
        schema_version: ResourceId::new(AUTHORITY_CONTEXT_SCHEMA_V1).unwrap(),
        protocol_id: ProtocolId::new(AUTHORITY_PROTOCOL_V1).unwrap(),
        catalog_digest: digest_of(1),
        request_id: OpaqueId::new(request).unwrap(),
        trace_id: OpaqueId::new(format!("trace-{request}")).unwrap(),
        ingress_surface: IngressSurface::new("au_mcp").unwrap(),
        actor: ActorId::new("actor:a").unwrap(),
        audience: AudienceId::new("eg").unwrap(),
        tenant: TenantId::new("tenant:a").unwrap(),
        authority_scope: AuthorityScope {
            kind: ScopeKind::new("graph").unwrap(),
            scope_id: scope_id.clone(),
            tenant: Some(TenantId::new("tenant:a").unwrap()),
            parent_scope_ids: BoundedVec::new(vec![ResourceId::new("tenant:a").unwrap()])
                .unwrap(),
            graph_incarnation: None,
        },
        purpose_kind: PurposeKind::new("graph_write").unwrap(),
        purpose_resource: Some(scope_id),
        operation: Operation::new("mutation").unwrap(),
        policy_revision: PolicyRevision::new("policy:1").unwrap(),
        policy_epoch: 7,
        policy_decision_id: OpaqueId::new("decision:1").unwrap(),
        policy_digest: digest_of(2),
        issued_at: UtcUnixNanos::new(100),
        expires_at: UtcUnixNanos::new(10_100),
        nonce: Nonce::from_bytes([nonce; 32]),
        idempotency_key: Some(IdempotencyKey::new(idempotency).unwrap()),
        context_digest: digest_of(0),
    };
    value.context_digest = value.recompute_context_digest().unwrap();
    value
}

fn operation_identity(
    context: &AuthorityContext,
    method: &str,
    payload: Digest256,
) -> OperationReplayIdentity {
    OperationReplayIdentity::from_context(
        context,
        MethodId::new(method).unwrap(),
        SchemaId::new("mutation-envelope.v1").unwrap(),
        digest_of(8),
        payload,
    )
    .unwrap()
}

/// A committed receipt bound to both replay digests.
fn receipt(
    id: &str,
    operation: &OperationReplayIdentity,
    nonce: &NonceReplayKey,
) -> MutationReceipt {
    let result = MutationResult::ReceiptOnly;
    let value = MutationReceipt {
        receipt_id: OpaqueId::new(id).unwrap(),
        mutation_id: OpaqueId::new(format!("mutation-{id}")).unwrap(),
        scope: operation.authority_scope.clone(),
        authority_receipt_id: OpaqueId::new("authority:1").unwrap(),
        authority_evidence_digest: digest_of(11),
        disposition: MutationDisposition::new("committed").unwrap(),
        operation_replay_digest: operation.digest().unwrap(),
        nonce_replay_digest: nonce.digest().unwrap(),
        envelope_digest: digest_of(12),
        effect_id: Some(OpaqueId::new(format!("effect-{id}")).unwrap()),
        effect_digest: Some(digest_of(13)),
        result_digest: result.digest().unwrap(),
        commit_id: Some(OpaqueId::new(format!("commit-{id}")).unwrap()),
        result,
        recorded_at: UtcUnixNanos::new(200),
    };
    value.validate().unwrap();
    value
}

/// Resolve one attempt the way a caller must: inside a real admitted write,
/// which is then discarded because this helper decides nothing durable.
fn resolve<D: OwnerDomain>(
    fixture: &Fixture,
    owner: &OwnedStoreHandle<D>,
    operation: &OperationReplayIdentity,
    nonce: &NonceReplayKey,
) -> crate::ReplayResolution {
    let write = fixture.mutations.open_write(owner).unwrap();
    let resolution = fixture
        .mutations
        .resolve_replay(&write, operation, nonce)
        .unwrap();
    write.abort().unwrap();
    resolution
}
