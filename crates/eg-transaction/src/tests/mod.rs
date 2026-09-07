//! Shared fixtures for the mutation-kernel test modules.

mod backup_replay;
mod confinement;
mod fault_restart;
mod ledger;
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
    IncarnationId, LogicalName, DurabilityDomain, MutationRequestContext, MutationSurface, ScopeTenantId,
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
            domain: DurabilityDomain::BlobStore,
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
