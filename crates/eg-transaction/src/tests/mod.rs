//! Shared fixtures for the mutation-kernel test modules.

mod backup_replay;
mod fault_restart;
mod ledger;
mod recovery_binding;
mod replay;

use crate::read::{read_ledger, read_outbox, read_private_payload, version};
use crate::tables::{FENCES, OUTBOX, PRIVATE_PAYLOADS};
use crate::{Begin, MutationKernelV1};
use eg_storage::{
    strict_recovery_evidence, BlobOwner, LedgerOnlyOwner, OwnedStoreHandle, OwnerDomain,
    OwnerLayout, PhysicalStoreIdentity, PrivatePayloadIntegrity, ScopeGrantVerifier,
    StorageKernelV1,
};
use eg_types::mutation_batch::{
    IncarnationId, LogicalName, MutationDomain, MutationRequestContext, MutationSurface, TenantId,
    VersionExpectation,
};
use eg_types::authority::{
    AuthorityContextV1, AuthorityScopeV1, NonceReplayKeyV1, OperationReplayIdentityV1,
    AUTHORITY_CONTEXT_SCHEMA_V1, AUTHORITY_PROTOCOL_V1,
};
use eg_types::contract::{
    ActorIdV1, AudienceIdV1, BoundedVecV1, Digest256V1, IdempotencyKeyV1, IngressSurfaceV1,
    MethodIdV1, MutationDispositionV1, NonceV1, OpaqueIdV1, OperationV1, PolicyRevisionV1,
    ProtocolIdV1, PurposeKindV1, ResourceIdV1, SchemaIdV1, ScopeKindV1, TenantIdV1,
    UtcUnixNanosV1,
};
use eg_types::mutation::{MutationReceiptV1, MutationResultV1};
use eg_types::protocol::Method;
use eg_types::{
    MutationBatch, MutationOperation, MutationScopeIdentity, MUTATION_BATCH_VERSION,
};
use redb::{TableDefinition, TableHandle};
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;

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
        TenantId::new(tenant).unwrap(),
        MutationDomain::BlobStore,
        LogicalName::new("blob-catalog").unwrap(),
        IncarnationId::new(incarnation).unwrap(),
    )
    .unwrap()
}

fn batch(identity: MutationScopeIdentity, batch_id: &str) -> MutationBatch {
    MutationBatch {
        schema_version: MUTATION_BATCH_VERSION,
        batch_id: batch_id.to_string(),
        context: MutationRequestContext {
            request_id: 1,
            principal: PRINCIPAL.to_string(),
            purpose: None,
            policy_fingerprint: None,
            trace_id: None,
            verified_capabilities: BTreeSet::new(),
        },
        identity,
        placement_epoch: 0,
        idempotency_key: format!("retry-{batch_id}"),
        version_expectation: VersionExpectation::Native(0),
        fencing_token: None,
        authoritative_state: None,
        operations: vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Other,
            domain: MutationDomain::BlobStore,
            method: Method::ApplyMutation {
                event_type: "blob_test".to_string(),
                query: "opaque".to_string(),
            },
        }],
        outbox: Vec::new(),
        created_at_ms: 1,
    }
}

fn recovery_batch(identity: MutationScopeIdentity, batch_id: &str) -> (MutationBatch, Vec<u8>) {
    let digest = "b".repeat(64);
    let mut batch = batch(identity, batch_id);
    batch.operations[0].method = Method::ApplyMutation {
        event_type: "transaction_recovery_plan".to_string(),
        query: format!("sha256:{digest}"),
    };
    (batch, format!("sealed:{digest}").into_bytes())
}

/// One owner file plus its single mutation kernel.
struct Fixture {
    kernel: StorageKernelV1,
    mutations: MutationKernelV1,
}

impl Fixture {
    fn create<D: OwnerDomain>(
        path: &Path,
        physical: &str,
        integrity: Option<Arc<dyn PrivatePayloadIntegrity>>,
    ) -> Self {
        let kernel = StorageKernelV1::create_owner::<D>(
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
        let kernel = StorageKernelV1::open_owner::<D>(
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

    fn split(kernel: StorageKernelV1) -> Self {
        let (kernel, authority) = kernel.into_read_and_mutation_authority().unwrap();
        Self {
            kernel,
            mutations: MutationKernelV1::new(authority),
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

fn ledger_fixture(path: &Path, identity: MutationScopeIdentity) -> (Fixture, OwnedStoreHandle<LedgerOnlyOwner>) {
    let fixture = Fixture::create::<LedgerOnlyOwner>(path, "physical:test:ledger-only", None);
    let owner = fixture.bind::<LedgerOnlyOwner>(&verifier("tenant-a", OwnerLayout::LedgerOnly), identity);
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
        write.owner_rows(owner, batch).unwrap().finish_owner().unwrap();
    }
    fixture
        .mutations
        .finish(&write, batch, None, 2, source_version)
        .unwrap();
    fixture.mutations.commit(write, batch).unwrap();
}

fn digest_of(byte: u8) -> Digest256V1 {
    Digest256V1::from_bytes([byte; 32])
}

/// One attempt context. `nonce` varies the attempt; every other field is the
/// stable operation identity.
fn context(nonce: u8, request: &str, idempotency: &str) -> AuthorityContextV1 {
    let scope_id = ResourceIdV1::new("graph:tenant:a/g").unwrap();
    let mut value = AuthorityContextV1 {
        schema_version: ResourceIdV1::new(AUTHORITY_CONTEXT_SCHEMA_V1).unwrap(),
        protocol_id: ProtocolIdV1::new(AUTHORITY_PROTOCOL_V1).unwrap(),
        catalog_digest: digest_of(1),
        request_id: OpaqueIdV1::new(request).unwrap(),
        trace_id: OpaqueIdV1::new(format!("trace-{request}")).unwrap(),
        ingress_surface: IngressSurfaceV1::new("au_mcp").unwrap(),
        actor: ActorIdV1::new("actor:a").unwrap(),
        audience: AudienceIdV1::new("eg").unwrap(),
        tenant: TenantIdV1::new("tenant:a").unwrap(),
        authority_scope: AuthorityScopeV1 {
            kind: ScopeKindV1::new("graph").unwrap(),
            scope_id: scope_id.clone(),
            tenant: Some(TenantIdV1::new("tenant:a").unwrap()),
            parent_scope_ids: BoundedVecV1::new(vec![ResourceIdV1::new("tenant:a").unwrap()])
                .unwrap(),
            graph_incarnation: None,
        },
        purpose_kind: PurposeKindV1::new("graph_write").unwrap(),
        purpose_resource: Some(scope_id),
        operation: OperationV1::new("mutation").unwrap(),
        policy_revision: PolicyRevisionV1::new("policy:1").unwrap(),
        policy_epoch: 7,
        policy_decision_id: OpaqueIdV1::new("decision:1").unwrap(),
        policy_digest: digest_of(2),
        issued_at: UtcUnixNanosV1::new(100),
        expires_at: UtcUnixNanosV1::new(10_100),
        nonce: NonceV1::from_bytes([nonce; 32]),
        idempotency_key: Some(IdempotencyKeyV1::new(idempotency).unwrap()),
        context_digest: digest_of(0),
    };
    value.context_digest = value.recompute_context_digest().unwrap();
    value
}

fn operation_identity(
    context: &AuthorityContextV1,
    method: &str,
    payload: Digest256V1,
) -> OperationReplayIdentityV1 {
    OperationReplayIdentityV1::from_context(
        context,
        MethodIdV1::new(method).unwrap(),
        SchemaIdV1::new("mutation-envelope.v1").unwrap(),
        digest_of(8),
        payload,
    )
    .unwrap()
}

/// A committed receipt bound to both replay digests.
fn receipt(
    id: &str,
    operation: &OperationReplayIdentityV1,
    nonce: &NonceReplayKeyV1,
) -> MutationReceiptV1 {
    let result = MutationResultV1::ReceiptOnly;
    let value = MutationReceiptV1 {
        receipt_id: OpaqueIdV1::new(id).unwrap(),
        mutation_id: OpaqueIdV1::new(format!("mutation-{id}")).unwrap(),
        scope: operation.authority_scope.clone(),
        authority_receipt_id: OpaqueIdV1::new("authority:1").unwrap(),
        authority_evidence_digest: digest_of(11),
        disposition: MutationDispositionV1::new("committed").unwrap(),
        operation_replay_digest: operation.digest().unwrap(),
        nonce_replay_digest: nonce.digest().unwrap(),
        envelope_digest: digest_of(12),
        effect_id: Some(OpaqueIdV1::new(format!("effect-{id}")).unwrap()),
        effect_digest: Some(digest_of(13)),
        result_digest: result.digest().unwrap(),
        commit_id: Some(OpaqueIdV1::new(format!("commit-{id}")).unwrap()),
        result,
        recorded_at: UtcUnixNanosV1::new(200),
    };
    value.validate().unwrap();
    value
}

/// Resolve one attempt the way a caller must: inside a real admitted write,
/// which is then discarded because this helper decides nothing durable.
fn resolve<D: OwnerDomain>(
    fixture: &Fixture,
    owner: &OwnedStoreHandle<D>,
    operation: &OperationReplayIdentityV1,
    nonce: &NonceReplayKeyV1,
) -> crate::ReplayResolution {
    let write = fixture.mutations.open_write(owner).unwrap();
    let resolution = fixture
        .mutations
        .resolve_replay(&write, operation, nonce)
        .unwrap();
    write.abort().unwrap();
    resolution
}
