//! Durable RF-020 Agent Library entries.
//!
//! The Agent Library is a small native ControlPlane owner file.  The revision
//! table is append-only and the head table is updated beside it in the same
//! admitted mutation as the kernel receipt and outbox row.  That keeps the
//! complete definition history and the serving pointer under one authority;
//! the mutation ledger is used for ordering and replay, not as the definition
//! history itself.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;

use parking_lot::Mutex;
use redb::{ReadableTable, ReadableTableMetadata};

use eg_storage::{
    OwnedStoreHandle, PhysicalStoreIdentity, RecordedOperation, ScopedRead, StorageKernel,
};
use eg_transaction::{AdmittedOwnerWrite, MutationKernel};
use eg_types::mutation::{MutationReceipt, MutationResult};
use eg_types::mutation_batch::{
    BatchContent, CompiledEnvelope, CompiledOperation, CompiledScope, DurabilityDomain,
    MutationEnvelope, MutationOperation, MutationOutboxIntent,
};
use eg_types::{
    AgentLibraryCommittedResult, AgentLibraryEntry, AgentLibraryEntryDraft,
    AgentLibraryMutationContext, AgentLibraryMutationKind, AgentLibraryOutboxEvent,
    AgentLibraryPublishRequest, AgentLibraryRetireRequest, AgentLibraryStatusRequest,
    AgentLibraryWriteResult, MutationBatch, AGENT_LIBRARY_OUTBOX_SCHEMA_VERSION,
    AGENT_LIBRARY_RESULT_SCHEMA_ID,
};

use super::durable_stores::BundledStoreSource;

mod batch;
mod history;
mod receipt;
mod replay;
mod write;

pub(super) use batch::native_lifecycle_batch;
use history::read_history;
pub(super) use receipt::{owner_batch_receipt, owner_receipt, OwnerReceiptInput};
use replay::{receipt_result, record_result, replay_record};

/// Persist-dir file for the EG-owned Agent Library.
pub const AGENT_LIBRARY_FILE: &str = "agent_library.redb";
/// Stable physical identity used by staged backup adoption.
pub(crate) const AGENT_LIBRARY_PHYSICAL_STORE: &str = "epistemic-graph:agent-library";
const AGENT_LIBRARY_BEFORE_CONNECTOR_PACKS_AND_WRITE_BACK: eg_storage::LayoutPredecessor =
    eg_storage::LayoutPredecessor {
        layout: eg_storage::OwnerLayout::AgentLibrary,
        label: "Agent Library before connector packs and governed write-back",
        owner_tables: &[
            "agent_library",
            "agent_library_heads",
            "agent_graph",
            "agent_graph_heads",
            "agent_component",
            "agent_component_heads",
            "agent_template",
            "agent_template_heads",
        ],
        data_lost: "its pre-ConnectorPack and governed-write-back Agent Library rows are intentionally not upgraded",
        file_name: AGENT_LIBRARY_FILE,
    };
const AGENT_LIBRARY_SCOPE_RESOURCE: &str = "agent-library";
const AGENT_LIBRARY_SCOPE_INCARNATION: &str = "agent-library:v1";
const AGENT_LIBRARY_BOOTSTRAP_TENANT: &str = "agent-library-bootstrap";
const AGENT_LIBRARY_BOOTSTRAP_RESOURCE: &str = "agent-library-bootstrap";
const AGENT_LIBRARY_OUTBOX_TOPIC: &str = "eg.agent-library.entry.v1";
/// `pub(super)` for [`super::agent_pin_resolution`]: the bound on how deep one
/// pin lookup may scan this layer's history is this layer's own.
pub(super) const MAX_AGENT_LIBRARY_REVISIONS: usize = 16_384;
const MAX_AGENT_LIBRARY_HISTORY_BYTES: usize = 256 * 1024 * 1024;
/// One durable Agent Library owner file. It binds one native ControlPlane
/// serving scope per tenant while sharing the physical tables and mutation
/// kernel, so replay, fencing, and outbox evidence stay tenant-scoped.
pub struct AgentLibraryStore {
    pub(super) kernel: StorageKernel,
    pub(super) mutations: MutationKernel,
    bootstrap: Arc<OwnedStoreHandle<eg_storage::AgentLibraryOwner>>,
    scopes: Mutex<HashMap<String, Arc<OwnedStoreHandle<eg_storage::AgentLibraryOwner>>>>,
    serving_principal: String,
}

impl std::fmt::Debug for AgentLibraryStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AgentLibraryStore")
            .finish_non_exhaustive()
    }
}

impl AgentLibraryStore {
    /// Open or create the kernel-owned Agent Library file and its native scope.
    pub fn open(persist_dir: &str) -> Result<Self, String> {
        let path = Path::new(persist_dir).join(AGENT_LIBRARY_FILE);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        }
        let physical = PhysicalStoreIdentity::new(AGENT_LIBRARY_PHYSICAL_STORE)?;
        let kernel = if path.exists() {
            eg_storage::refuse_known_predecessor(
                &path,
                &AGENT_LIBRARY_BEFORE_CONNECTOR_PACKS_AND_WRITE_BACK,
            )?;
            StorageKernel::open_owner::<eg_storage::AgentLibraryOwner>(&path, physical, None)
        } else {
            StorageKernel::create_owner::<eg_storage::AgentLibraryOwner>(&path, physical, None)
        }?;
        let (kernel, authority) = kernel.into_read_and_mutation_authority()?;
        let mutations = MutationKernel::new(authority);
        let bootstrap_identity = eg_types::MutationScopeIdentity::fixed_native(
            AGENT_LIBRARY_BOOTSTRAP_TENANT,
            DurabilityDomain::ControlPlane,
            AGENT_LIBRARY_BOOTSTRAP_RESOURCE,
            AGENT_LIBRARY_SCOPE_INCARNATION,
        )?;
        let bootstrap = bind_scope(&kernel, &bootstrap_identity)?;
        let serving_principal = bootstrap.principal().to_string();
        mutations.bootstrap_ledger(&bootstrap)?;
        Ok(Self {
            kernel,
            mutations,
            bootstrap,
            scopes: Mutex::new(HashMap::new()),
            serving_principal,
        })
    }

    /// The process-owned serving principal stamped into the native mutation
    /// envelope. Caller attribution travels separately in the action receipt.
    pub fn owner_principal(&self) -> &str {
        &self.serving_principal
    }

    pub(super) fn scope_handle(
        &self,
        tenant_id: &str,
    ) -> Result<Arc<OwnedStoreHandle<eg_storage::AgentLibraryOwner>>, String> {
        eg_types::agent_library::validate_key(tenant_id, "agent-library")?;
        if let Some(handle) = self.scopes.lock().get(tenant_id) {
            return Ok(Arc::clone(handle));
        }
        let identity = eg_types::MutationScopeIdentity::fixed_native(
            tenant_id,
            DurabilityDomain::ControlPlane,
            AGENT_LIBRARY_SCOPE_RESOURCE,
            AGENT_LIBRARY_SCOPE_INCARNATION,
        )?;
        let handle = bind_scope(&self.kernel, &identity)?;
        // The physical owner is shared by tenants, but each tenant has its own
        // native mutation ledger/version/fence namespace.  A freshly bound
        // tenant must therefore receive the same ledger census bootstrap as
        // the process bootstrap scope before the first write can resolve
        // replay or read its authoritative version.
        self.mutations.bootstrap_ledger(&handle)?;
        self.scopes
            .lock()
            .insert(tenant_id.to_string(), Arc::clone(&handle));
        Ok(handle)
    }

    pub(super) fn read(&self) -> Result<ScopedRead<'_, eg_storage::AgentLibraryOwner>, String> {
        self.kernel.read_scope(&self.bootstrap)
    }

    /// Read the current head for one tenant/agent pair.
    pub fn current(
        &self,
        tenant_id: &str,
        agent_id: &str,
    ) -> Result<Option<AgentLibraryEntry>, String> {
        eg_types::agent_library::validate_key(tenant_id, agent_id)?;
        let read = self.read()?;
        let (_, entries) = read_history(&read, tenant_id, agent_id)?;
        Ok(entries.into_iter().last())
    }

    /// Read the retained append-only revision stream for one tenant/agent pair.
    /// The stream includes a final retired tombstone when the entry is retired.
    pub fn revisions(
        &self,
        tenant_id: &str,
        agent_id: &str,
    ) -> Result<Vec<AgentLibraryEntry>, String> {
        eg_types::agent_library::validate_key(tenant_id, agent_id)?;
        let read = self.read()?;
        let (_, entries) = read_history(&read, tenant_id, agent_id)?;
        Ok(entries)
    }

    // There is deliberately NO `entry_ref(tenant, agent, revision)` here.
    //
    // It existed, was `pub`, had no caller, and was named exactly what an
    // integrator reaches for when it wants a delegation target -- while
    // filtering on the PINNED revision's lifecycle. A tombstone is a separate,
    // later revision, so the pinned row stays `Published` forever and that
    // filter can never see a retirement: the function would happily hand back a
    // reference to a withdrawn agent. The graph and template resolvers document
    // and avoid exactly this trap (`resolve_composed_graph`,
    // `instantiate_template`), and so does `retained_agent` on the delegation
    // path, which reads the HEAD's lifecycle first.
    //
    // Anything that needs a delegable reference must go through a resolver that
    // checks the head. `revisions()` above still serves the historical read.

    /// Read the terminal mutation receipt for one caller operation, if it has
    /// committed. This is the typed status route's durable source of truth.
    pub fn status(
        &self,
        request: AgentLibraryStatusRequest,
    ) -> Result<Option<AgentLibraryWriteResult>, String> {
        validate_context(self, &request.context)?;
        eg_types::agent_library::validate_key(&request.context.tenant_id, &request.agent_id)?;
        let owner = self.scope_handle(&request.context.tenant_id)?;
        let batch_id = batch_id(&request.context.idempotency_key)?;
        let Some(evidence) = read_status_evidence(self, &owner, &request.context, &batch_id)?
        else {
            return Ok(None);
        };
        if receipt_result(&evidence.receipt)? != record_result(&evidence.record)? {
            return Err(
                "CORRUPT_MUTATION_LEDGER: Agent Library status result differs from its receipt"
                    .to_string(),
            );
        }
        let expected_event_bytes = evidence
            .record
            .batch
            .outbox
            .first()
            .map(|intent| intent.payload.as_slice())
            .ok_or_else(|| "Agent Library receipt has no outbox event".to_string())?;
        let result = replay_record(
            &evidence.record,
            &request.context,
            request.kind,
            &request.agent_id,
            None,
            expected_event_bytes,
        )?;
        Ok(Some(result))
    }
}

struct AgentLibraryStatusEvidence {
    record: eg_types::MutationBatchRecord,
    receipt: MutationReceipt,
}

fn read_status_evidence(
    store: &AgentLibraryStore,
    owner: &Arc<OwnedStoreHandle<eg_storage::AgentLibraryOwner>>,
    context: &AgentLibraryMutationContext,
    batch_id: &str,
) -> Result<Option<AgentLibraryStatusEvidence>, String> {
    let read = store.kernel.read_scope(owner)?;
    let record = eg_transaction::read_ledger(&read, batch_id)?;
    let replay_row = eg_transaction::read_replay_operation(&read, &context.idempotency_key)?;
    let (record, replay_row) =
        match (record, replay_row) {
            (None, None) => return Ok(None),
            (None, Some(_)) => return Err(
                "CORRUPT_MUTATION_LEDGER: Agent Library status has a dangling typed replay receipt"
                    .to_string(),
            ),
            (Some(_), None) => {
                return Err(
                    "CORRUPT_MUTATION_LEDGER: Agent Library status has no typed replay receipt"
                        .to_string(),
                );
            }
            (Some(record), Some(replay_row)) => (record, replay_row),
        };
    let receipt = match &replay_row.recorded {
        RecordedOperation::Receipt(receipt) => receipt.as_ref(),
        RecordedOperation::Batch(_) => {
            return Err(
                "CORRUPT_MUTATION_LEDGER: Agent Library status has no typed replay receipt"
                    .to_string(),
            );
        }
    };
    receipt.validate()?;
    validate_status_identity(
        owner.as_ref(),
        context,
        batch_id,
        &record,
        &replay_row,
        receipt,
    )?;
    Ok(Some(AgentLibraryStatusEvidence {
        record,
        receipt: receipt.clone(),
    }))
}

fn validate_status_identity(
    owner: &OwnedStoreHandle<eg_storage::AgentLibraryOwner>,
    context: &AgentLibraryMutationContext,
    batch_id: &str,
    record: &eg_types::MutationBatchRecord,
    replay_row: &eg_storage::OperationReplayRow,
    receipt: &MutationReceipt,
) -> Result<(), String> {
    let expected_scope = eg_types::mutation_batch::authority_scope_for(owner.identity())?;
    if replay_row.identity != *owner.identity()
        || replay_row.idempotency_key != context.idempotency_key
        || replay_row.batch_id != batch_id
        || record.identity != *owner.identity()
        || record.batch.batch_id != batch_id
        || receipt.disposition.as_str() != "committed"
        || receipt.scope != expected_scope
        || receipt.operation_replay_digest != replay_row.operation_replay_digest
    {
        return Err(
            "CORRUPT_MUTATION_LEDGER: Agent Library status receipt identity is invalid".to_string(),
        );
    }
    Ok(())
}

fn bind_scope(
    kernel: &StorageKernel,
    identity: &eg_types::MutationScopeIdentity,
) -> Result<Arc<OwnedStoreHandle<eg_storage::AgentLibraryOwner>>, String> {
    let authority = crate::store_authority::process_authority();
    let grant = kernel.authenticate_scope::<eg_storage::AgentLibraryOwner>(
        authority.as_ref(),
        identity.clone(),
        authority.principal().to_string(),
        &authority.proof(),
    )?;
    kernel.bind_serving_scope(grant, 0).map(Arc::new)
}

/// Ask the canonical replay kernel about this attempt nonce before reading any
/// retained definition or current version. This is a nonce-only lookup in the
/// same admitted write; the stable operation identity is resolved immediately
/// afterwards, still before any owner rows or current version are read.
pub(super) fn resolve_nonce_first(
    mutations: &MutationKernel,
    txn: &eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
    context: &AgentLibraryMutationContext,
) -> Result<eg_types::authority::NonceReplayKey, String> {
    let nonce = eg_types::authority::NonceReplayKey {
        protocol_id: eg_types::contract::ProtocolId::new(
            eg_types::authority::AUTHORITY_PROTOCOL_V1,
        )?,
        catalog_digest: eg_types::contract::Digest256::parse(
            eg_capabilities::CONTRACT_CATALOG_DIGEST,
        )?,
        audience: eg_types::contract::AudienceId::new(eg_types::mutation_batch::ENGINE_AUDIENCE)?,
        actor: eg_types::contract::ActorId::new(context.caller_principal.clone())?,
        tenant: eg_types::contract::TenantId::new(context.tenant_id.clone())?,
        nonce: context.attempt_nonce,
    };
    if let Some(idempotency_key) = mutations.resolve_nonce(txn, &nonce)? {
        return Err(format!(
            "REPLAY_NONCE_CONSUMED: attempt nonce already consumed by '{idempotency_key}'"
        ));
    }
    Ok(nonce)
}

/// Stamp the owner's current policy digest and the operation's purpose.
///
/// `purpose_id` rather than a record-typed enum, for the same reason as
/// [`agent_library_operation_identity`]: both record families in this owner
/// need it, and the purpose is the only thing that differs.
pub(super) fn admitted_context(
    context: &AgentLibraryMutationContext,
    purpose_id: &str,
) -> Result<AgentLibraryMutationContext, String> {
    let mut admitted = context.clone();
    admitted.policy_digest = current_agent_library_policy_digest()?;
    admitted.purpose_id = purpose_id.to_string();
    Ok(admitted)
}

/// Build the stable logical operation identity before the domain opens any
/// retained revision row. The full batch later carries its physical outbox
/// bytes; this identity includes only retry-stable Agent Library intent.
/// The replay identity of one owner mutation.
///
/// `kind_name` rather than a record-typed enum: this owner carries two record
/// families (RF-ADR-008), and every part of the identity below is already
/// expressed in primitives. The name is folded into the canonical payload
/// digest, so an entry operation and a graph operation on the same id and
/// revision can never resolve to the same replay identity.
pub(super) fn agent_library_operation_identity(
    owner: &OwnedStoreHandle<eg_storage::AgentLibraryOwner>,
    context: &AgentLibraryMutationContext,
    kind_name: &str,
    agent_id: &str,
    expected_revision: u64,
    definition_digest: Option<&str>,
) -> Result<eg_types::authority::OperationReplayIdentity, String> {
    use eg_types::authority::AUTHORITY_PROTOCOL_V1;
    use eg_types::contract::{
        ActorId, AudienceId, Digest256, IdempotencyKey, MethodId, Operation, PolicyRevision,
        ProtocolId, PurposeKind, ResourceId, SchemaId, TenantId,
    };

    let scope = eg_types::mutation_batch::authority_scope_for(owner.identity())?;
    let catalog_digest = Digest256::parse(eg_capabilities::CONTRACT_CATALOG_DIGEST)?;
    let method = MethodId::new("ApplyMutation")?;
    let (method_schema_id, method_schema_digest) = eg_capabilities::method_schema("ApplyMutation")
        .map(|(schema, digest)| (SchemaId::new(schema), Digest256::from_bytes(digest)))
        .ok_or_else(|| "ApplyMutation is missing from the contract catalog".to_string())?;
    let method_schema_id = method_schema_id?;
    let definition_digest = definition_digest.unwrap_or("");
    let canonical_payload_digest = Digest256::framed(
        b"eg/agent-library-operation/v1",
        &[
            kind_name.as_bytes(),
            context.tenant_id.as_bytes(),
            agent_id.as_bytes(),
            &expected_revision.to_be_bytes(),
            definition_digest.as_bytes(),
            context.actor_scope.as_bytes(),
            context.purpose_id.as_bytes(),
            context.policy_revision.as_bytes(),
            context.policy_digest.as_bytes(),
            context.policy_decision_id.as_bytes(),
        ],
    )?;
    let operation = eg_types::authority::OperationReplayIdentity {
        schema_version: ResourceId::new("operation-replay-identity.v1")?,
        protocol_id: ProtocolId::new(AUTHORITY_PROTOCOL_V1)?,
        catalog_digest,
        tenant: TenantId::new(context.tenant_id.clone())?,
        actor: ActorId::new(context.caller_principal.clone())?,
        audience: AudienceId::new(eg_types::mutation_batch::ENGINE_AUDIENCE)?,
        authority_scope: scope.clone(),
        operation: Operation::new("mutation")?,
        purpose_kind: PurposeKind::new("graph_write")?,
        purpose_resource: Some(scope.scope_id.clone()),
        method,
        method_schema_id,
        method_schema_digest,
        canonical_payload_digest,
        policy_revision: PolicyRevision::new(context.policy_revision.clone())?,
        policy_epoch: eg_types::mutation_batch::POLICY_EPOCH,
        policy_digest: parse_prefixed_digest(&context.policy_digest)?,
        idempotency_key: IdempotencyKey::new(context.idempotency_key.clone())?,
    };
    operation.validate()?;
    Ok(operation)
}

pub(super) fn validate_context(
    store: &AgentLibraryStore,
    context: &AgentLibraryMutationContext,
) -> Result<(), String> {
    context.validate()?;
    if context.principal != store.owner_principal() {
        return Err(
            "agent library context principal must be the authenticated EG process principal"
                .to_string(),
        );
    }
    Ok(())
}

fn validate_context_matches_draft(
    context: &AgentLibraryMutationContext,
    draft: &AgentLibraryEntryDraft,
) -> Result<(), String> {
    if context.tenant_id != draft.tenant_id {
        return Err("agent library context tenant does not match its definition".to_string());
    }
    Ok(())
}

pub(super) fn require_expected_revision(expected: u64, current: u64) -> Result<(), String> {
    if expected == current {
        Ok(())
    } else {
        Err(format!(
            "STALE_AGENT_LIBRARY_REVISION: expected {expected} but current is {current}"
        ))
    }
}

pub(super) fn next_revision(expected: u64) -> Result<u64, String> {
    let next = expected
        .checked_add(1)
        .ok_or_else(|| "agent library revision overflow".to_string())?;
    if next > MAX_AGENT_LIBRARY_REVISIONS as u64 {
        return Err(format!(
            "agent library revision limit exceeded: maximum is {MAX_AGENT_LIBRARY_REVISIONS}"
        ));
    }
    Ok(next)
}

fn agent_library_operations(
    kind: AgentLibraryMutationKind,
    entry: &AgentLibraryEntry,
) -> Vec<MutationOperation> {
    super::agent_revision::lifecycle_operations(
        format!("agent_library_{}", write::library_verb(kind).as_str()),
        entry.definition_digest.clone(),
    )
}

pub(super) fn effective_agent_library_policy_digest(
    operations: &[MutationOperation],
) -> Result<eg_types::contract::Digest256, String> {
    crate::server::mutation_batch::effective_policy_digest(
        &eg_types::contract::MethodId::new("ApplyMutation")?,
        operations,
    )
}

pub(crate) fn current_agent_library_policy_digest() -> Result<String, String> {
    let operations = super::agent_revision::lifecycle_operations(
        "agent_library_policy".to_string(),
        String::new(),
    );
    Ok(format!(
        "sha256:{}",
        effective_agent_library_policy_digest(&operations)?.to_hex()
    ))
}

pub(super) fn parse_prefixed_digest(value: &str) -> Result<eg_types::contract::Digest256, String> {
    value
        .strip_prefix("sha256:")
        .ok_or_else(|| "agent library policy digest is not canonical".to_string())
        .and_then(eg_types::contract::Digest256::parse)
}

fn apply_entry_rows(
    owner_write: &AdmittedOwnerWrite<'_, eg_storage::AgentLibraryOwner>,
    context: &AgentLibraryMutationContext,
    expected_revision: u64,
    entry: &AgentLibraryEntry,
    entry_bytes: &[u8],
) -> Result<(), String> {
    let mut heads = owner_write.open_table(eg_storage::AGENT_LIBRARY_HEADS)?;
    let actual_revision = heads
        .get((context.tenant_id.as_str(), entry.agent_id.as_str()))
        .map_err(|error| error.to_string())?
        .map(|value| value.value())
        .unwrap_or(0);
    require_expected_revision(expected_revision, actual_revision)?;
    if entry.entry_revision != next_revision(expected_revision)? {
        return Err("agent library entry revision does not follow its expected head".to_string());
    }
    let mut revisions = owner_write.open_table(eg_storage::AGENT_LIBRARY_REVISIONS)?;
    if actual_revision > 0 {
        let current = revisions
            .get((
                context.tenant_id.as_str(),
                entry.agent_id.as_str(),
                actual_revision,
            ))
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "agent library head points to a missing revision".to_string())?;
        let current = decode_entry(current.value())?;
        if current.is_retired() {
            return Err("retired agent library entries cannot be resurrected".to_string());
        }
    }
    if revisions
        .get((
            context.tenant_id.as_str(),
            entry.agent_id.as_str(),
            entry.entry_revision,
        ))
        .map_err(|error| error.to_string())?
        .is_some()
    {
        return Err("agent library revision row already exists".to_string());
    }
    revisions
        .insert(
            (
                context.tenant_id.as_str(),
                entry.agent_id.as_str(),
                entry.entry_revision,
            ),
            entry_bytes,
        )
        .map_err(|error| error.to_string())?;
    heads
        .insert(
            (context.tenant_id.as_str(), entry.agent_id.as_str()),
            entry.entry_revision,
        )
        .map_err(|error| error.to_string())?;
    Ok(())
}

/// Encode the stable Agent Library result through the shared DomainResult
/// contract. The legacy batch record still carries opaque bytes, but those
/// bytes now have one schema-tagged representation instead of reusing the
/// outbox event as a second result authority. A newer finish seam can persist
/// this exact `MutationResult` directly in the typed receipt.
fn encode_domain_result(result: &AgentLibraryCommittedResult) -> Result<Vec<u8>, String> {
    let mutation_result = domain_result_for(result)?;
    eg_storage::encode_bounded(&mutation_result, "agent library domain result")
}

fn domain_result_for(result: &AgentLibraryCommittedResult) -> Result<MutationResult, String> {
    result.validate()?;
    let payload = eg_storage::encode_bounded(result, "agent library domain result payload")?;
    let payload = eg_types::contract::RecordBytes::new(payload)?;
    Ok(MutationResult::DomainResult {
        schema_id: eg_types::contract::SchemaId::new(AGENT_LIBRARY_RESULT_SCHEMA_ID)?,
        payload_digest: payload.digest()?,
        payload,
    })
}

/// `pub(super)` for [`super::agent_pin_resolution`], which resolves a pin
/// against this layer and therefore has to read this layer's rows.
pub(super) fn decode_entry(bytes: &[u8]) -> Result<AgentLibraryEntry, String> {
    let entry: AgentLibraryEntry = super::agent_row::decode(bytes, "Agent Library row")?;
    entry.validate()?;
    Ok(entry)
}

fn validate_entry_key(
    entry: &AgentLibraryEntry,
    tenant_id: &str,
    agent_id: &str,
    revision: u64,
) -> Result<(), String> {
    if entry.tenant_id != tenant_id
        || entry.agent_id != agent_id
        || entry.entry_revision != revision
    {
        return Err("Agent Library row key does not match its typed entry".to_string());
    }
    Ok(())
}

pub(super) fn batch_id(idempotency_key: &str) -> Result<String, String> {
    if idempotency_key.is_empty()
        || idempotency_key.trim() != idempotency_key
        || idempotency_key.chars().any(char::is_control)
    {
        return Err("agent library idempotency key is invalid".to_string());
    }
    Ok(format!("agent-library/v1/{idempotency_key}"))
}

impl BundledStoreSource for AgentLibraryStore {
    fn file_name(&self) -> &'static str {
        AGENT_LIBRARY_FILE
    }

    fn copy_into(&self, destination: &Path) -> Result<u64, String> {
        let (revisions, heads) = {
            let read = self.read()?;
            let revisions = read
                .open_owner_table(eg_storage::AGENT_LIBRARY_REVISIONS)?
                .len()
                .map_err(|error| error.to_string())?;
            let heads = read
                .open_owner_table(eg_storage::AGENT_LIBRARY_HEADS)?
                .len()
                .map_err(|error| error.to_string())?;
            (revisions, heads)
        };
        let counts = eg_storage::backup_recovery_store(&self.kernel, destination)?;
        Ok(revisions
            .saturating_add(heads)
            .saturating_add(counts.batches))
    }

    fn is_durable(&self) -> bool {
        true
    }
}

/// Publish one L2 agent and return the `definition_digest` that resolves it.
///
/// Shared by the graph and template test modules: publishing a graph now
/// RESOLVES every `Agent` node's pin, so a fixture can no longer invent a
/// digest -- it has to be the entry's real one. One definition, so the modules
/// cannot drift.
///
/// Idempotent per store, and deterministic: an entry's definition digest is a
/// hash over its content alone -- no revision, no timestamp -- so seeding the
/// same agent into several stores yields byte-identical pins. The components it
/// is assembled from are seeded first, because L2 -> L1 resolution requires
/// them to exist.
#[cfg(test)]
pub(crate) fn seed_agent_for_test(
    store: &AgentLibraryStore,
    tenant_id: &str,
    agent_id: &str,
    nonce_index: u8,
) -> String {
    if let Some(existing) = store
        .current(tenant_id, agent_id)
        .expect("read a seeded agent")
    {
        return existing.definition_digest;
    }
    let mut entry = seed_agent_draft_for_test(tenant_id, agent_id, nonce_index);
    super::agent_component::seed_draft_components_for_test(store, &mut entry, nonce_index);
    // A seeding nonce can never collide with a test's own: every test module
    // builds its nonces as `[n; 32]` for a small `n`.
    let mut nonce_bytes = [0xEEu8; 32];
    nonce_bytes[0] = 0xC0u8.wrapping_add(nonce_index);
    let policy_digest = current_agent_library_policy_digest().unwrap();
    store
        .publish(AgentLibraryPublishRequest {
            context: AgentLibraryMutationContext {
                request_id: 90_000 + u64::from(nonce_index),
                principal: store.owner_principal().to_string(),
                caller_principal: format!("principal:sha256:{}", "a".repeat(64)),
                attempt_nonce: eg_types::contract::Nonce::from_bytes(nonce_bytes),
                tenant_id: tenant_id.to_string(),
                actor_scope: "action-scope:agent-seed".to_string(),
                purpose_id: "agent-library:publish".to_string(),
                policy_revision: "policy-v1".to_string(),
                policy_digest,
                policy_decision_id: "agent-library:decision:policy-v1".to_string(),
                idempotency_key: format!("agent-seed:{tenant_id}:{agent_id}"),
                expected_revision: Some(0),
                trace_id: None,
                created_at_ms: 5,
            },
            entry,
        })
        .expect("the fixture's agent publishes")
        .entry
        .definition_digest
}

/// The agent draft [`seed_agent_for_test`] publishes, with its component pins
/// still carrying placeholder digests for `seed_draft_components_for_test` to
/// rewrite.
#[cfg(test)]
pub(crate) fn seed_agent_draft_for_test(
    tenant_id: &str,
    agent_id: &str,
    nonce_index: u8,
) -> AgentLibraryEntryDraft {
    use eg_types::agent_component::{AgentComponentKind, ComponentDependency};
    let placeholder = format!("sha256:{}", "0".repeat(64));
    let pin = |component_id: &str, kind: AgentComponentKind| ComponentDependency {
        component_id: component_id.to_string(),
        kind,
        definition_digest: placeholder.clone(),
    };
    AgentLibraryEntryDraft {
        agent_id: agent_id.to_string(),
        package_id: "agent-package".to_string(),
        version: "1.0.0".to_string(),
        role: "researcher".to_string(),
        role_digest: format!("sha256:{}", "1".repeat(64)),
        system_prompt: pin("prompt:agent", AgentComponentKind::SystemPrompt),
        tools: vec![pin("tool:search", AgentComponentKind::Tool)],
        skills: vec![pin("skill:reason", AgentComponentKind::Skill)],
        model_profile: pin("model-profile:default", AgentComponentKind::ModelProfile),
        model_identity: "model:default".to_string(),
        ontologies: vec![pin("ontology:agent", AgentComponentKind::Ontology)],
        tenant_id: tenant_id.to_string(),
        actor_scope: "definition:builder-a".to_string(),
        purpose_id: "agent-library:definition".to_string(),
        policy_digest: format!("sha256:{}", "7".repeat(64)),
        // Distinct per agent, so two seeded agents are two different records
        // rather than one definition published under two ids.
        source_revision: format!("agent-seed:{nonce_index}"),
        source_revision_digest: format!("sha256:{}", "8".repeat(64)),
        runtime: Default::default(),
        instantiated_from: None,
    }
}

#[cfg(test)]
mod tests {
    use super::replay::record_event;
    use super::*;
    use crate::server::persistence::durable_stores::BundledStoreSource;
    use eg_types::contract::Nonce;
    use eg_types::mutation_batch::MutationBatchStatus;

    const LEDGER_BATCHES: redb::TableDefinition<'static, (&str, &str), &[u8]> =
        redb::TableDefinition::new("ledger_batches");
    const REPLAY_OPERATIONS: redb::TableDefinition<'static, (&str, &str), &[u8]> =
        redb::TableDefinition::new("mutation_replay_operations");

    fn digest(byte: char) -> String {
        format!("sha256:{}", byte.to_string().repeat(64))
    }

    /// How many component batches `draft` commits before an agent can publish:
    /// system prompt, tool, skill, model profile, ontology.
    const SEEDED_COMPONENT_BATCHES: usize = 5;

    /// Publish one L1 component and return the pin that resolves it.
    ///
    /// Shared with the other three layers' test modules -- see
    /// `seed_component_for_test`.
    fn component_pin(
        store: &AgentLibraryStore,
        tenant_id: &str,
        component_id: &str,
        kind: eg_types::agent_component::AgentComponentKind,
        seed: u8,
    ) -> eg_types::agent_component::ComponentDependency {
        super::super::agent_component::seed_component_for_test(
            store,
            tenant_id,
            component_id,
            kind,
            seed,
        )
    }

    /// The shared agent draft with its five components seeded under nonces
    /// 1..=5 and a fixed source revision.
    fn draft(store: &AgentLibraryStore, tenant_id: &str, agent_id: &str) -> AgentLibraryEntryDraft {
        let mut draft = seed_agent_draft_for_test(tenant_id, agent_id, 0);
        draft.source_revision = "source-revision:42".to_string();
        super::super::agent_component::seed_draft_components_for_test(store, &mut draft, 1);
        draft
    }

    /// One write attempt's replay coordinates.  These three are what the store
    /// admits an attempt on: the idempotency key it is filed under, the attempt
    /// nonce that distinguishes a retry from a fresh attempt under that key, and
    /// the revision the attempt claims to have observed.  A test that varies one
    /// of them is varying the attempt, not the caller, so they move together.
    struct WriteAttempt<'a> {
        key: &'a str,
        /// Also the fixture's `request_id`, so an attempt is traceable by nonce.
        nonce: u8,
        expected_revision: u64,
    }

    /// Who the attempt is made by and under what authority.  `caller_byte`
    /// expands into both the caller principal and its action scope, and the
    /// policy revision expands into the decision id, so these are one identity
    /// rather than three knobs: the replay tests assert that changing any part
    /// of it makes the SAME idempotency key an IDEMPOTENCY_CONFLICT.
    struct AdmissionAuthority<'a> {
        caller_byte: char,
        policy_revision: &'a str,
        purpose_id: &'a str,
    }

    fn context(
        store: &AgentLibraryStore,
        tenant_id: &str,
        attempt: WriteAttempt<'_>,
        authority: AdmissionAuthority<'_>,
        created_at_ms: u64,
    ) -> AgentLibraryMutationContext {
        let WriteAttempt {
            key,
            nonce,
            expected_revision,
        } = attempt;
        let AdmissionAuthority {
            caller_byte,
            policy_revision,
            purpose_id,
        } = authority;
        AgentLibraryMutationContext {
            request_id: u64::from(nonce),
            principal: store.owner_principal().to_string(),
            caller_principal: format!("principal:sha256:{}", caller_byte.to_string().repeat(64)),
            attempt_nonce: Nonce::from_bytes([nonce; 32]),
            tenant_id: tenant_id.to_string(),
            actor_scope: format!("action-scope:{caller_byte}"),
            purpose_id: purpose_id.to_string(),
            policy_revision: policy_revision.to_string(),
            policy_digest: current_agent_library_policy_digest().unwrap(),
            policy_decision_id: format!("agent-library:decision:{policy_revision}"),
            idempotency_key: key.to_string(),
            expected_revision: Some(expected_revision),
            trace_id: None,
            created_at_ms,
        }
    }

    #[test]
    fn real_store_replays_atomically_scopes_tenants_and_restores() {
        let source_dir = tempfile::tempdir().unwrap();
        let store = AgentLibraryStore::open(source_dir.path().to_str().unwrap()).unwrap();
        let definition = draft(&store, "tenant-a", "agent-a");
        let publish_context = context(
            &store,
            "tenant-a",
            WriteAttempt {
                key: "shared-key",
                nonce: 1,
                expected_revision: 0,
            },
            AdmissionAuthority {
                caller_byte: 'a',
                policy_revision: "policy-v1",
                purpose_id: "agent-library:publish",
            },
            10,
        );
        let published = store
            .publish(AgentLibraryPublishRequest {
                context: publish_context.clone(),
                entry: definition.clone(),
            })
            .unwrap();
        assert!(!published.replayed);
        assert_eq!(published.entry.entry_revision, 1);
        assert_eq!(published.entry.as_draft(), definition);
        assert!(published.entry.created_at_ms > publish_context.created_at_ms);

        let owner_a = store.scope_handle("tenant-a").unwrap();
        let read_a = store.kernel.read_scope(&owner_a).unwrap();
        let first_batch_id = batch_id("shared-key").unwrap();
        let first_record = eg_transaction::read_ledger(&read_a, &first_batch_id)
            .unwrap()
            .expect("first publish receipt");
        assert_eq!(first_record.status, MutationBatchStatus::Committed);
        assert_eq!(first_record.batch.outbox.len(), 1);
        let first_outbox = eg_transaction::read_outbox(&read_a, &first_batch_id).unwrap();
        assert_eq!(first_outbox.len(), 1);
        let first_result = record_result(&first_record).unwrap();
        assert_eq!(first_result.entry, published.entry);
        assert_eq!(first_result.batch_id, first_batch_id);
        assert_eq!(
            first_result.committed_version,
            first_record.committed_version.target().unwrap()
        );
        assert_eq!(
            first_record.committing_actor().unwrap(),
            publish_context.caller_principal.as_str()
        );
        let first_event = record_event(&first_record).unwrap();
        assert_eq!(first_event.entry.agent_id, "agent-a");
        assert_eq!(first_event.entry.actor_scope, "definition:builder-a");
        assert_eq!(
            first_event.performing_actor,
            publish_context.caller_principal
        );
        assert_eq!(first_event.action_actor_scope, publish_context.actor_scope);
        assert_eq!(
            first_outbox[0].intent.headers["actor"],
            publish_context.caller_principal.as_str()
        );
        assert_eq!(
            first_outbox[0].intent.payload,
            eg_storage::encode_bounded(&first_event, "agent library test outbox event").unwrap()
        );
        let mut legacy_record = first_record.clone();
        legacy_record.result_msgpack = Some(
            eg_storage::encode_bounded(&first_event, "legacy agent library test result").unwrap(),
        );
        assert_eq!(record_result(&legacy_record).unwrap(), first_result);
        let mut misdirected = first_record.clone();
        misdirected.batch.outbox[0].key = "tenant-a:agent-a:999".to_string();
        assert!(replay_record(
            &misdirected,
            &publish_context,
            AgentLibraryMutationKind::Publish,
            "agent-a",
            Some(&definition),
            first_outbox[0].intent.payload.as_slice(),
        )
        .is_err());
        let mut mismatched_result = first_record.clone();
        mismatched_result.result_msgpack = Some(vec![0]);
        assert!(replay_record(
            &mismatched_result,
            &publish_context,
            AgentLibraryMutationKind::Publish,
            "agent-a",
            Some(&definition),
            first_outbox[0].intent.payload.as_slice(),
        )
        .is_err());
        drop(read_a);

        let mut forged_serving_context = publish_context.clone();
        forged_serving_context.principal = format!("principal:sha256:{}", "f".repeat(64));
        forged_serving_context.idempotency_key = "forged-serving-key".to_string();
        forged_serving_context.attempt_nonce = Nonce::from_bytes([10; 32]);
        assert!(store
            .publish(AgentLibraryPublishRequest {
                context: forged_serving_context,
                entry: definition.clone(),
            })
            .unwrap_err()
            .contains("authenticated EG process principal"));
        assert_eq!(store.revisions("tenant-a", "agent-a").unwrap().len(), 1);

        let nonce_reuse = store.publish(AgentLibraryPublishRequest {
            context: publish_context.clone(),
            entry: definition.clone(),
        });
        assert!(nonce_reuse.unwrap_err().contains("REPLAY_NONCE_CONSUMED"));

        // The nonce decision is made before retained-target and revision-limit
        // reads. Reusing the consumed nonce with a deliberately impossible
        // target therefore returns the nonce error and cannot expose a target
        // lookup/limit error or mutate the owner.
        let consumed_nonce_before_target = context(
            &store,
            "tenant-a",
            WriteAttempt {
                key: "nonce-before-target",
                nonce: 1,
                expected_revision: MAX_AGENT_LIBRARY_REVISIONS as u64,
            },
            AdmissionAuthority {
                caller_byte: 'a',
                policy_revision: "policy-v1",
                purpose_id: "agent-library:publish",
            },
            999,
        );
        assert!(store
            .publish(AgentLibraryPublishRequest {
                context: consumed_nonce_before_target,
                entry: definition.clone(),
            })
            .unwrap_err()
            .contains("REPLAY_NONCE_CONSUMED"));
        assert_eq!(store.revisions("tenant-a", "agent-a").unwrap().len(), 1);

        let over_limit = context(
            &store,
            "tenant-a",
            WriteAttempt {
                key: "over-limit",
                nonce: 11,
                expected_revision: MAX_AGENT_LIBRARY_REVISIONS as u64,
            },
            AdmissionAuthority {
                caller_byte: 'a',
                policy_revision: "policy-v1",
                purpose_id: "agent-library:publish",
            },
            1_000,
        );
        assert!(store
            .publish(AgentLibraryPublishRequest {
                context: over_limit,
                entry: definition.clone(),
            })
            .unwrap_err()
            .contains("revision limit exceeded"));
        assert_eq!(store.revisions("tenant-a", "agent-a").unwrap().len(), 1);

        let replay_context = context(
            &store,
            "tenant-a",
            WriteAttempt {
                key: "shared-key",
                nonce: 2,
                expected_revision: 0,
            },
            AdmissionAuthority {
                caller_byte: 'a',
                policy_revision: "policy-v1",
                purpose_id: "agent-library:publish",
            },
            999,
        );
        let replayed = store
            .publish(AgentLibraryPublishRequest {
                context: replay_context,
                entry: definition.clone(),
            })
            .unwrap();
        assert!(replayed.replayed);
        assert_eq!(replayed.entry, published.entry);
        let replay_nonce_reuse = store.publish(AgentLibraryPublishRequest {
            context: context(
                &store,
                "tenant-a",
                WriteAttempt {
                    key: "shared-key",
                    nonce: 2,
                    expected_revision: 0,
                },
                AdmissionAuthority {
                    caller_byte: 'a',
                    policy_revision: "policy-v1",
                    purpose_id: "agent-library:publish",
                },
                1_001,
            ),
            entry: definition.clone(),
        });
        assert!(replay_nonce_reuse
            .unwrap_err()
            .contains("REPLAY_NONCE_CONSUMED"));
        let read_a = store.kernel.read_scope(&owner_a).unwrap();
        // One agent-library batch, plus the components the fixture had to
        // publish first: an agent publish now RESOLVES every pinned component,
        // so the seeds are part of this owner's ledger. Still discriminating --
        // a second agent batch would make it SEEDED_COMPONENT_BATCHES + 2.
        assert_eq!(
            eg_transaction::read_batches(&read_a).unwrap().len(),
            SEEDED_COMPONENT_BATCHES + 1
        );
        assert_eq!(
            eg_transaction::read_outbox(&read_a, &first_batch_id)
                .unwrap()
                .len(),
            1
        );
        drop(read_a);

        let changed_actor = context(
            &store,
            "tenant-a",
            WriteAttempt {
                key: "shared-key",
                nonce: 3,
                expected_revision: 0,
            },
            AdmissionAuthority {
                caller_byte: 'b',
                policy_revision: "policy-v1",
                purpose_id: "agent-library:publish",
            },
            10,
        );
        assert!(store
            .publish(AgentLibraryPublishRequest {
                context: changed_actor,
                entry: definition.clone(),
            })
            .unwrap_err()
            .contains("IDEMPOTENCY_CONFLICT"));

        let changed_policy = context(
            &store,
            "tenant-a",
            WriteAttempt {
                key: "shared-key",
                nonce: 4,
                expected_revision: 0,
            },
            AdmissionAuthority {
                caller_byte: 'a',
                policy_revision: "policy-v2",
                purpose_id: "agent-library:publish",
            },
            10,
        );
        assert!(store
            .publish(AgentLibraryPublishRequest {
                context: changed_policy,
                entry: definition.clone(),
            })
            .unwrap_err()
            .contains("IDEMPOTENCY_CONFLICT"));

        let stale_context = context(
            &store,
            "tenant-a",
            WriteAttempt {
                key: "new-key",
                nonce: 5,
                expected_revision: 0,
            },
            AdmissionAuthority {
                caller_byte: 'a',
                policy_revision: "policy-v1",
                purpose_id: "agent-library:publish",
            },
            10,
        );
        assert!(store
            .publish(AgentLibraryPublishRequest {
                context: stale_context,
                entry: definition.clone(),
            })
            .unwrap_err()
            .contains("STALE_AGENT_LIBRARY_REVISION"));

        let retire_context = context(
            &store,
            "tenant-a",
            WriteAttempt {
                key: "retire-key",
                nonce: 6,
                expected_revision: 1,
            },
            AdmissionAuthority {
                caller_byte: 'b',
                policy_revision: "policy-v2",
                purpose_id: "agent-library:retire",
            },
            20,
        );
        let retired = store
            .retire(AgentLibraryRetireRequest {
                context: retire_context.clone(),
                agent_id: "agent-a".to_string(),
            })
            .unwrap();
        assert!(!retired.replayed);
        assert!(retired.entry.is_retired());
        assert_eq!(retired.entry.created_at_ms, published.entry.created_at_ms);
        assert!(retired.entry.updated_at_ms >= published.entry.updated_at_ms);
        assert_eq!(retired.entry.as_draft(), definition);

        let retired_retry = store
            .retire(AgentLibraryRetireRequest {
                context: context(
                    &store,
                    "tenant-a",
                    WriteAttempt {
                        key: "retire-key",
                        nonce: 7,
                        expected_revision: 1,
                    },
                    AdmissionAuthority {
                        caller_byte: 'b',
                        policy_revision: "policy-v2",
                        purpose_id: "agent-library:retire",
                    },
                    2_000,
                ),
                agent_id: "agent-a".to_string(),
            })
            .unwrap();
        assert!(retired_retry.replayed);
        assert_eq!(retired_retry.entry, retired.entry);

        let status = store
            .status(AgentLibraryStatusRequest {
                context: retire_context,
                agent_id: "agent-a".to_string(),
                kind: AgentLibraryMutationKind::Retire,
            })
            .unwrap()
            .expect("retire status");
        assert!(status.replayed);
        assert_eq!(status.entry, retired.entry);
        let history = store.revisions("tenant-a", "agent-a").unwrap();
        assert_eq!(history.len(), 2);
        assert!(history[1].is_retired());

        // A publish retry that was originally committed before retirement must
        // still resolve from its typed receipt after the agent's current head
        // becomes a tombstone. It cannot read the retired head and attempt a
        // second revision, so the owner version and physical outbox remain
        // unchanged.
        let read_before_old_publish_replay = store.kernel.read_scope(&owner_a).unwrap();
        let batches_before_old_publish_replay =
            eg_transaction::read_batches(&read_before_old_publish_replay).unwrap();
        let version_before_old_publish_replay =
            eg_transaction::version(&read_before_old_publish_replay).unwrap();
        let outbox_before_old_publish_replay: usize = batches_before_old_publish_replay
            .iter()
            .map(|record| {
                eg_transaction::read_outbox(&read_before_old_publish_replay, &record.batch.batch_id)
                    .unwrap()
                    .len()
            })
            .sum();
        drop(read_before_old_publish_replay);
        let old_publish_retry = store
            .publish(AgentLibraryPublishRequest {
                context: context(
                    &store,
                    "tenant-a",
                    WriteAttempt {
                        key: "shared-key",
                        nonce: 12,
                        expected_revision: 0,
                    },
                    AdmissionAuthority {
                        caller_byte: 'a',
                        policy_revision: "policy-v1",
                        purpose_id: "agent-library:publish",
                    },
                    4_000,
                ),
                entry: definition.clone(),
            })
            .unwrap();
        assert!(old_publish_retry.replayed);
        assert_eq!(old_publish_retry.entry, published.entry);
        let read_after_old_publish_replay = store.kernel.read_scope(&owner_a).unwrap();
        let batches_after_old_publish_replay =
            eg_transaction::read_batches(&read_after_old_publish_replay).unwrap();
        let outbox_after_old_publish_replay: usize = batches_after_old_publish_replay
            .iter()
            .map(|record| {
                eg_transaction::read_outbox(&read_after_old_publish_replay, &record.batch.batch_id)
                    .unwrap()
                    .len()
            })
            .sum();
        assert_eq!(
            eg_transaction::version(&read_after_old_publish_replay).unwrap(),
            version_before_old_publish_replay
        );
        assert_eq!(
            batches_after_old_publish_replay.len(),
            batches_before_old_publish_replay.len()
        );
        assert_eq!(
            outbox_after_old_publish_replay,
            outbox_before_old_publish_replay
        );
        drop(read_after_old_publish_replay);

        let resurrection = context(
            &store,
            "tenant-a",
            WriteAttempt {
                key: "resurrection-key",
                nonce: 8,
                expected_revision: 2,
            },
            AdmissionAuthority {
                caller_byte: 'a',
                policy_revision: "policy-v1",
                purpose_id: "agent-library:publish",
            },
            30,
        );
        assert!(store
            .publish(AgentLibraryPublishRequest {
                context: resurrection,
                entry: definition.clone(),
            })
            .unwrap_err()
            .contains("resurrected"));

        let tenant_b_definition = draft(&store, "tenant-b", "agent-b");
        let tenant_b = store
            .publish(AgentLibraryPublishRequest {
                context: context(
                    &store,
                    "tenant-b",
                    WriteAttempt {
                        key: "shared-key",
                        nonce: 9,
                        expected_revision: 0,
                    },
                    AdmissionAuthority {
                        caller_byte: 'c',
                        policy_revision: "policy-v1",
                        purpose_id: "agent-library:publish",
                    },
                    40,
                ),
                entry: tenant_b_definition.clone(),
            })
            .unwrap();
        assert!(!tenant_b.replayed);
        assert_eq!(tenant_b.entry.tenant_id, "tenant-b");
        assert!(store
            .current("tenant-a", "agent-a")
            .unwrap()
            .unwrap()
            .is_retired());
        assert_eq!(
            store.current("tenant-b", "agent-b").unwrap(),
            Some(tenant_b.entry)
        );

        drop(owner_a);
        drop(store);
        let reopened = AgentLibraryStore::open(source_dir.path().to_str().unwrap()).unwrap();
        assert_eq!(reopened.revisions("tenant-a", "agent-a").unwrap(), history);
        assert_eq!(
            reopened
                .current("tenant-b", "agent-b")
                .unwrap()
                .unwrap()
                .agent_id,
            "agent-b"
        );

        // Exercise both replay and status against a newly opened store. The
        // receipt and operation replay rows must be sufficient; no process
        // local cache or pre-reopen handle may participate in the answer.
        let reopened_owner_a = reopened.scope_handle("tenant-a").unwrap();
        let reopened_read_before = reopened.kernel.read_scope(&reopened_owner_a).unwrap();
        let reopened_batches_before = eg_transaction::read_batches(&reopened_read_before).unwrap();
        let reopened_version_before = eg_transaction::version(&reopened_read_before).unwrap();
        let reopened_outbox_before: usize = reopened_batches_before
            .iter()
            .map(|record| {
                eg_transaction::read_outbox(&reopened_read_before, &record.batch.batch_id)
                    .unwrap()
                    .len()
            })
            .sum();
        drop(reopened_read_before);
        let reopened_publish_retry = reopened
            .publish(AgentLibraryPublishRequest {
                context: context(
                    &reopened,
                    "tenant-a",
                    WriteAttempt {
                        key: "shared-key",
                        nonce: 13,
                        expected_revision: 0,
                    },
                    AdmissionAuthority {
                        caller_byte: 'a',
                        policy_revision: "policy-v1",
                        purpose_id: "agent-library:publish",
                    },
                    5_000,
                ),
                entry: definition.clone(),
            })
            .unwrap();
        assert!(reopened_publish_retry.replayed);
        assert_eq!(reopened_publish_retry.entry, published.entry);
        let reopened_status = reopened
            .status(AgentLibraryStatusRequest {
                context: context(
                    &reopened,
                    "tenant-a",
                    WriteAttempt {
                        key: "shared-key",
                        nonce: 14,
                        expected_revision: 0,
                    },
                    AdmissionAuthority {
                        caller_byte: 'a',
                        policy_revision: "policy-v1",
                        purpose_id: "agent-library:publish",
                    },
                    6_000,
                ),
                agent_id: "agent-a".to_string(),
                kind: AgentLibraryMutationKind::Publish,
            })
            .unwrap()
            .expect("publish status after process reopen");
        assert!(reopened_status.replayed);
        assert_eq!(reopened_status.entry, published.entry);
        let reopened_read_after = reopened.kernel.read_scope(&reopened_owner_a).unwrap();
        let reopened_batches_after = eg_transaction::read_batches(&reopened_read_after).unwrap();
        let reopened_outbox_after: usize = reopened_batches_after
            .iter()
            .map(|record| {
                eg_transaction::read_outbox(&reopened_read_after, &record.batch.batch_id)
                    .unwrap()
                    .len()
            })
            .sum();
        assert_eq!(
            eg_transaction::version(&reopened_read_after).unwrap(),
            reopened_version_before
        );
        assert_eq!(reopened_batches_after.len(), reopened_batches_before.len());
        assert_eq!(reopened_outbox_after, reopened_outbox_before);

        let backup_dir = tempfile::tempdir().unwrap();
        let backup_path = backup_dir.path().join(AGENT_LIBRARY_FILE);
        let copied = reopened.copy_into(&backup_path).unwrap();
        assert!(copied >= 1);
        let backup = AgentLibraryStore::open(backup_dir.path().to_str().unwrap()).unwrap();
        assert_eq!(backup.revisions("tenant-a", "agent-a").unwrap(), history);
        assert_eq!(
            backup
                .current("tenant-b", "agent-b")
                .unwrap()
                .unwrap()
                .agent_id,
            "agent-b"
        );
    }

    // ---- L2 -> L1: every pinned component is RESOLVED at admission ----
    //
    // Before this, `persistence/agent_library.rs` contained no reference to the
    // component tables at all: a pin was checked for well-formedness and
    // nothing else, so 64 invented hex characters published an agent claiming a
    // tool it was never granted, and `capability_digest` then attested to the
    // claim. L1 had no production reader.

    fn publish_draft(
        store: &AgentLibraryStore,
        entry: AgentLibraryEntryDraft,
        key: &str,
        nonce: u8,
    ) -> Result<AgentLibraryWriteResult, String> {
        let context = context(
            store,
            &entry.tenant_id.clone(),
            WriteAttempt {
                key,
                nonce,
                expected_revision: 0,
            },
            AdmissionAuthority {
                caller_byte: 'a',
                policy_revision: "policy-v1",
                purpose_id: "agent-library:publish",
            },
            10,
        );
        store.publish(AgentLibraryPublishRequest { context, entry })
    }

    #[test]
    fn an_agent_pinning_a_component_that_does_not_exist_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let store = AgentLibraryStore::open(dir.path().to_str().unwrap()).unwrap();
        let mut entry = draft(&store, "tenant-a", "agent-a");
        entry.tools[0].component_id = "tool:never-published".to_string();
        let error = publish_draft(&store, entry, "key-1", 1)
            .expect_err("an unresolvable pin must never become a revision");
        assert!(
            error.contains("does not exist in this tenant"),
            "got: {error}"
        );
        assert!(store.current("tenant-a", "agent-a").unwrap().is_none());
    }

    #[test]
    fn an_agent_pinning_an_invented_digest_is_refused() {
        // The failure scenario the unresolved edge allowed: the component id is
        // real, the kind is right, the digest is 64 valid hex characters -- and
        // no revision was ever published with it.
        let dir = tempfile::tempdir().unwrap();
        let store = AgentLibraryStore::open(dir.path().to_str().unwrap()).unwrap();
        let mut entry = draft(&store, "tenant-a", "agent-a");
        entry.tools[0].definition_digest = digest('9');
        let error = publish_draft(&store, entry, "key-1", 1)
            .expect_err("a fabricated digest must never become a revision");
        assert!(
            error.contains("no retained revision matches the pinned definition digest"),
            "got: {error}"
        );
    }

    #[test]
    fn an_agent_pinning_a_component_under_the_wrong_kind_is_refused() {
        // Local validation checks the kind against the SLOT. This checks it
        // against the component that was actually published -- the half nothing
        // could check without resolving.
        let dir = tempfile::tempdir().unwrap();
        let store = AgentLibraryStore::open(dir.path().to_str().unwrap()).unwrap();
        let mut entry = draft(&store, "tenant-a", "agent-a");
        let prompt = entry.system_prompt.clone();
        entry.tools[0] = eg_types::agent_component::ComponentDependency {
            component_id: prompt.component_id.clone(),
            kind: eg_types::agent_component::AgentComponentKind::Tool,
            definition_digest: prompt.definition_digest.clone(),
        };
        let error = publish_draft(&store, entry, "key-1", 1)
            .expect_err("a prompt wired into a tool slot must be refused");
        assert!(error.contains("but it is a system_prompt"), "got: {error}");
    }

    #[test]
    fn an_agent_pinning_another_tenants_component_is_refused() {
        // Resolution is by (id, kind, digest) alone, so without the tenant
        // prefix a caller who learns a digest would hold a component it was
        // never granted.
        let dir = tempfile::tempdir().unwrap();
        let store = AgentLibraryStore::open(dir.path().to_str().unwrap()).unwrap();
        let theirs = component_pin(
            &store,
            "tenant-b",
            "tool:theirs",
            eg_types::agent_component::AgentComponentKind::Tool,
            9,
        );
        let mut entry = draft(&store, "tenant-a", "agent-a");
        entry.tools[0] = theirs;
        let error = publish_draft(&store, entry, "key-1", 1)
            .expect_err("a cross-tenant pin must be refused");
        assert!(
            error.contains("does not exist in this tenant"),
            "got: {error}"
        );
    }

    #[test]
    fn an_agent_pinning_a_retired_component_is_refused_but_old_pins_still_resolve() {
        // A retired component stays RESOLVABLE -- agents that already pin it
        // keep working -- while nothing NEW may be built on it. The same
        // retained-but-not-buildable rule composition applies to graphs.
        let dir = tempfile::tempdir().unwrap();
        let store = AgentLibraryStore::open(dir.path().to_str().unwrap()).unwrap();
        let entry = draft(&store, "tenant-a", "agent-a");
        publish_draft(&store, entry.clone(), "key-1", 1).expect("publishes while live");

        let retire_context = AgentLibraryMutationContext {
            request_id: 950,
            principal: store.owner_principal().to_string(),
            caller_principal: format!("principal:sha256:{}", "a".repeat(64)),
            attempt_nonce: Nonce::from_bytes([0xD0u8; 32]),
            tenant_id: "tenant-a".to_string(),
            actor_scope: "action-scope:seed".to_string(),
            purpose_id: "agent-component:retire".to_string(),
            policy_revision: "policy-v1".to_string(),
            policy_digest: current_agent_library_policy_digest().unwrap(),
            policy_decision_id: "agent-component:decision:policy-v1".to_string(),
            idempotency_key: "component-retire:tool:search".to_string(),
            expected_revision: Some(1),
            trace_id: None,
            created_at_ms: 20,
        };
        store
            .retire_component(eg_types::agent_component::AgentComponentRetireRequest {
                context: retire_context,
                component_id: "tool:search".to_string(),
            })
            .expect("retires");

        let mut next = entry.clone();
        next.agent_id = "agent-b".to_string();
        let error = publish_draft(&store, next, "key-2", 2)
            .expect_err("nothing new may be built on a withdrawn component");
        assert!(error.contains("which is retired"), "got: {error}");

        // The agent published BEFORE the retirement is untouched.
        assert!(store.current("tenant-a", "agent-a").unwrap().is_some());
    }

    #[test]
    fn a_component_pinning_a_component_that_does_not_exist_is_refused() {
        // L1 -> L1: a component's own `requires` are pinned references too.
        let dir = tempfile::tempdir().unwrap();
        let store = AgentLibraryStore::open(dir.path().to_str().unwrap()).unwrap();
        let context = AgentLibraryMutationContext {
            request_id: 960,
            principal: store.owner_principal().to_string(),
            caller_principal: format!("principal:sha256:{}", "a".repeat(64)),
            attempt_nonce: Nonce::from_bytes([0xD1u8; 32]),
            tenant_id: "tenant-a".to_string(),
            actor_scope: "action-scope:seed".to_string(),
            purpose_id: "agent-component:publish".to_string(),
            policy_revision: "policy-v1".to_string(),
            policy_digest: current_agent_library_policy_digest().unwrap(),
            policy_decision_id: "agent-component:decision:policy-v1".to_string(),
            idempotency_key: "component-requires".to_string(),
            expected_revision: Some(0),
            trace_id: None,
            created_at_ms: 5,
        };
        let error = store
            .publish_component(eg_types::agent_component::AgentComponentPublishRequest {
                context,
                evaluation_receipt_digest: None,
                component: eg_types::agent_component::AgentComponentDraft {
                    component_id: "skill:depends".to_string(),
                    kind: eg_types::agent_component::AgentComponentKind::Skill,
                    version: "1.0.0".to_string(),
                    content_digest: digest('1'),
                    content_ref: None,
                    facts: eg_types::agent_component::AgentComponentFacts::Opaque,
                    provenance: eg_types::agent_component::ComponentProvenance::Native,
                    summary: "a skill with a dependency".to_string(),
                    classification: Vec::new(),
                    requires: vec![eg_types::agent_component::ComponentDependency {
                        component_id: "tool:never-published".to_string(),
                        kind: eg_types::agent_component::AgentComponentKind::Tool,
                        definition_digest: digest('9'),
                    }],
                    declared_capabilities: Vec::new(),
                    required_capabilities: Vec::new(),
                    declared_required_capabilities: Vec::new(),
                    attributes: Default::default(),
                    tenant_id: "tenant-a".to_string(),
                    actor_scope: "action-scope:seed".to_string(),
                    purpose_id: "agent-component:publish".to_string(),
                    policy_digest: current_agent_library_policy_digest().unwrap(),
                    source_revision: "rev-seed".to_string(),
                    source_revision_digest: digest('8'),
                },
            })
            .expect_err("an unresolvable `requires` must be refused");
        assert!(
            error.contains("does not exist in this tenant"),
            "got: {error}"
        );
    }

    #[test]
    fn revision_limit_guard_is_checked_before_owner_rows() {
        assert_eq!(
            next_revision((MAX_AGENT_LIBRARY_REVISIONS - 1) as u64).unwrap(),
            MAX_AGENT_LIBRARY_REVISIONS as u64
        );
        assert!(next_revision(MAX_AGENT_LIBRARY_REVISIONS as u64)
            .unwrap_err()
            .contains("revision limit exceeded"));
    }

    #[test]
    fn status_rejects_persisted_batch_result_tamper_after_reopen() {
        let source_dir = tempfile::tempdir().unwrap();
        let store = AgentLibraryStore::open(source_dir.path().to_str().unwrap()).unwrap();
        let definition = draft(&store, "tenant-a", "agent-a");
        let publish_context = context(
            &store,
            "tenant-a",
            WriteAttempt {
                key: "persisted-status-tamper",
                nonce: 1,
                expected_revision: 0,
            },
            AdmissionAuthority {
                caller_byte: 'a',
                policy_revision: "policy-v1",
                purpose_id: "agent-library:publish",
            },
            10,
        );
        store
            .publish(AgentLibraryPublishRequest {
                context: publish_context,
                entry: definition,
            })
            .unwrap();
        let owner = store.scope_handle("tenant-a").unwrap();
        let scope_key = eg_storage::ledger_scope_key(owner.identity());
        let persisted_batch_id = batch_id("persisted-status-tamper").unwrap();
        let path = source_dir.path().join(AGENT_LIBRARY_FILE);
        drop(owner);
        drop(store);

        // Keep the batch structurally valid but replace its opaque result
        // bytes. Recovery must reject the mismatch against the typed replay
        // receipt before Status can project from either row.
        let database = redb::Database::open(&path).unwrap();
        let write = database.begin_write().unwrap();
        let mut batches = write.open_table(LEDGER_BATCHES).unwrap();
        let bytes = batches
            .get((scope_key.as_str(), persisted_batch_id.as_str()))
            .unwrap()
            .expect("persisted Agent Library batch")
            .value()
            .to_vec();
        let mut record = eg_storage::decode_batch_record(&bytes).unwrap();
        record.result_msgpack = Some(vec![0]);
        let encoded = eg_storage::encode_bounded(&record, "tampered Agent Library batch").unwrap();
        batches
            .insert(
                (scope_key.as_str(), persisted_batch_id.as_str()),
                encoded.as_slice(),
            )
            .unwrap();
        drop(batches);
        write.commit().unwrap();
        drop(database);

        let error = match AgentLibraryStore::open(source_dir.path().to_str().unwrap()) {
            Ok(_) => panic!("a typed replay result tamper must fail recovery"),
            Err(error) => error,
        };
        assert!(
            error.contains("typed replay receipt differs from its committed result"),
            "{error}"
        );
    }

    #[test]
    fn recovery_rejects_redirected_replay_batch_link() {
        let source_dir = tempfile::tempdir().unwrap();
        let store = AgentLibraryStore::open(source_dir.path().to_str().unwrap()).unwrap();
        let definition = draft(&store, "tenant-a", "agent-a");
        store
            .publish(AgentLibraryPublishRequest {
                context: context(
                    &store,
                    "tenant-a",
                    WriteAttempt {
                        key: "redirected-status-link",
                        nonce: 1,
                        expected_revision: 0,
                    },
                    AdmissionAuthority {
                        caller_byte: 'a',
                        policy_revision: "policy-v1",
                        purpose_id: "agent-library:publish",
                    },
                    10,
                ),
                entry: definition,
            })
            .unwrap();

        let owner = store.scope_handle("tenant-a").unwrap();
        let scope_key = eg_storage::ledger_scope_key(owner.identity());
        let path = source_dir.path().join(AGENT_LIBRARY_FILE);
        drop(owner);
        drop(store);

        let database = redb::Database::open(&path).unwrap();
        let write = database.begin_write().unwrap();
        let mut operations = write.open_table(REPLAY_OPERATIONS).unwrap();
        let row_bytes = operations
            .get((scope_key.as_str(), "redirected-status-link"))
            .unwrap()
            .expect("persisted replay row")
            .value()
            .to_vec();
        let mut row: eg_storage::OperationReplayRow =
            eg_storage::decode_ledger_record(&row_bytes).unwrap();
        row.batch_id = "redirected-status-batch".to_string();
        let encoded = eg_storage::encode_bounded(&row, "redirected replay row").unwrap();
        operations
            .insert(
                (scope_key.as_str(), "redirected-status-link"),
                encoded.as_slice(),
            )
            .unwrap();
        drop(operations);
        write.commit().unwrap();
        drop(database);

        let error = match AgentLibraryStore::open(source_dir.path().to_str().unwrap()) {
            Ok(_) => panic!("a redirected replay batch link must fail recovery"),
            Err(error) => error,
        };
        assert!(
            error.contains("mutation receipt key row points elsewhere"),
            "{error}"
        );
    }
}
