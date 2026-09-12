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
use eg_transaction::{AdmittedOwnerWrite, Begin, MutationKernel, ReplayResolution};
use eg_types::mutation::{MutationReceipt, MutationResult};
use eg_types::mutation_batch::{
    BatchContent, CompiledEnvelope, CompiledOperation, CompiledScope, DurabilityDomain,
    MutationBatchStatus, MutationEnvelope, MutationOperation, MutationOutboxIntent,
    MutationSurface, VersionExpectation,
};
use eg_types::protocol::Method;
use eg_types::{
    AgentLibraryCommittedResult, AgentLibraryEntry, AgentLibraryEntryDraft, AgentLibraryLifecycle,
    AgentLibraryMutationContext, AgentLibraryMutationKind, AgentLibraryOutboxEvent,
    AgentLibraryPublishRequest, AgentLibraryRetireRequest, AgentLibraryStatusRequest,
    AgentLibraryWriteResult, MutationBatch, AGENT_LIBRARY_OUTBOX_SCHEMA_VERSION,
    AGENT_LIBRARY_RESULT_SCHEMA_ID, MUTATION_BATCH_VERSION,
};

use super::durable_stores::BundledStoreSource;

/// Persist-dir file for the EG-owned Agent Library.
pub const AGENT_LIBRARY_FILE: &str = "agent_library.redb";
/// Stable physical identity used by staged backup adoption.
pub(crate) const AGENT_LIBRARY_PHYSICAL_STORE: &str = "epistemic-graph:agent-library";
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
        validate_context(&self, &request.context)?;
        eg_types::agent_library::validate_key(&request.context.tenant_id, &request.agent_id)?;
        let owner = self.scope_handle(&request.context.tenant_id)?;
        let batch_id = batch_id(&request.context.idempotency_key)?;
        let read = self.kernel.read_scope(&owner)?;
        let record = eg_transaction::read_ledger(&read, &batch_id)?;
        let replay_row =
            eg_transaction::read_replay_operation(&read, &request.context.idempotency_key)?;
        let (record, replay_row) = match (record, replay_row) {
            (None, None) => return Ok(None),
            (None, Some(_)) => return Err(
                "CORRUPT_MUTATION_LEDGER: Agent Library status has a dangling typed replay receipt"
                    .to_string(),
            ),
            (Some(_), None) => {
                return Err(
                    "CORRUPT_MUTATION_LEDGER: Agent Library status has no typed replay receipt"
                        .to_string(),
                )
            }
            (Some(record), Some(replay_row)) => (record, replay_row),
        };
        let RecordedOperation::Receipt(receipt) = replay_row.recorded else {
            return Err(
                "CORRUPT_MUTATION_LEDGER: Agent Library status has no typed replay receipt"
                    .to_string(),
            );
        };
        let receipt = *receipt;
        receipt.validate()?;
        let expected_scope = eg_types::mutation_batch::authority_scope_for(owner.identity())?;
        if replay_row.identity != *owner.identity()
            || replay_row.idempotency_key != request.context.idempotency_key
            || replay_row.batch_id != batch_id
            || record.identity != *owner.identity()
            || record.batch.batch_id != batch_id
            || receipt.disposition.as_str() != "committed"
            || receipt.scope != expected_scope
            || receipt.operation_replay_digest != replay_row.operation_replay_digest
        {
            return Err(
                "CORRUPT_MUTATION_LEDGER: Agent Library status receipt identity is invalid"
                    .to_string(),
            );
        }
        if receipt_result(&receipt)? != record_result(&record)? {
            return Err(
                "CORRUPT_MUTATION_LEDGER: Agent Library status result differs from its receipt"
                    .to_string(),
            );
        }
        let expected_event_bytes = record
            .batch
            .outbox
            .first()
            .map(|intent| intent.payload.as_slice())
            .ok_or_else(|| "Agent Library receipt has no outbox event".to_string())?;
        let result = replay_record(
            &record,
            &request.context,
            request.kind,
            &request.agent_id,
            None,
            expected_event_bytes,
        )?;
        Ok(Some(result))
    }

    fn entry_at_revision_in_write(
        &self,
        write: &eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
        tenant_id: &str,
        agent_id: &str,
        revision: u64,
    ) -> Result<Option<AgentLibraryEntry>, String> {
        if revision == 0 {
            return Ok(None);
        }
        let revisions = write.open_read_table(eg_storage::AGENT_LIBRARY_REVISIONS)?;
        let Some(value) = revisions.get((tenant_id, agent_id, revision))? else {
            return Ok(None);
        };
        let entry = decode_entry(value.value())?;
        validate_entry_key(&entry, tenant_id, agent_id, revision)?;
        Ok(Some(entry))
    }

    /// Resolve every cross-record reference ONE AGENT DRAFT makes.
    ///
    /// L2 -> L1: every component the agent is assembled from, plus the values a
    /// template instantiation bound into it. Both are pins, and a pin nothing
    /// resolves is a claim nothing checks whichever list it sits in -- but they
    /// are gathered here rather than folded into `dependencies()`, which
    /// answers "what is this agent ASSEMBLED from" and must not start answering
    /// a different question.
    ///
    /// L2 -> TEMPLATE: `instantiated_from` is what makes "which agents came
    /// from this template?" a traversal rather than a guess. Unresolved it was
    /// a free-text provenance claim that `definition_digest` then attested to.
    ///
    /// # Why an agent DRAFT rather than an Agent Library publish
    ///
    /// Two admissions put an `AgentLibraryEntryDraft` on durable storage: this
    /// layer's own publish, and a TEMPLATE publish, whose `base` is one. They
    /// ask the identical question of it, so they ask it in one place -- `subject`
    /// is the only thing that differs, and only so a refusal names the record
    /// the caller was actually publishing.
    ///
    /// Sharing it also keeps the template side honest as the contract moves.
    /// `AgentTemplateDraft::validate` refuses a `base` that is itself a
    /// template instance, so today a base carries no `instantiated_from` and
    /// the template and binding halves below are unreachable from that caller.
    /// A resolver written to today's reachability -- just
    /// `base.dependencies()` -- would silently stop covering the base the day
    /// that rule relaxed. This one would not.
    pub(super) fn admit_entry_references_in_write(
        &self,
        write: &super::agent_pin_resolution::Write<'_>,
        tenant_id: &str,
        subject: &str,
        entry: &AgentLibraryEntryDraft,
    ) -> Result<(), String> {
        let mut components = entry.dependencies();
        components.extend(
            entry
                .instantiated_from
                .iter()
                .flat_map(|instance| instance.bindings.values()),
        );
        self.resolve_component_pins_in_write(write, tenant_id, subject, &components)?;
        let templates: Vec<super::agent_pin_resolution::TemplatePin<'_>> = entry
            .instantiated_from
            .iter()
            .map(|instance| super::agent_pin_resolution::TemplatePin {
                template_id: &instance.template_id,
                definition_digest: &instance.definition_digest,
                entry_revision: Some(instance.entry_revision),
            })
            .collect();
        super::agent_pin_resolution::resolve_template_pins_in_write(
            write, tenant_id, subject, &templates,
        )
    }

    /// Publish a new immutable Agent Library definition revision.
    pub fn publish(
        &self,
        request: AgentLibraryPublishRequest,
    ) -> Result<AgentLibraryWriteResult, String> {
        validate_context(self, &request.context)?;
        request.entry.validate()?;
        validate_context_matches_draft(&request.context, &request.entry)?;
        let expected_revision = request.context.expected_revision.ok_or_else(|| {
            "agent library writes require an explicit expected_revision".to_string()
        })?;
        let owner = self.scope_handle(&request.context.tenant_id)?;
        let txn = self.mutations.open_write(&owner)?;
        let nonce = match resolve_nonce_first(&self.mutations, &txn, &request.context) {
            Ok(nonce) => nonce,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        let next_revision = match next_revision(expected_revision) {
            Ok(next_revision) => next_revision,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        let definition_digest = AgentLibraryEntry::publish(request.entry.clone(), next_revision, 0)
            .map(|entry| entry.definition_digest)?;
        let replay_context = admitted_context(&request.context, "agent-library:publish")?;
        let operation = agent_library_operation_identity(
            &owner,
            &replay_context,
            "publish",
            &request.entry.agent_id,
            expected_revision,
            Some(&definition_digest),
        )?;
        let replay = match self.mutations.resolve_replay(&txn, &operation, &nonce) {
            Ok(replay) => replay,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        let replayed = match replayed_receipt(
            &self.mutations,
            &txn,
            replay,
            &operation,
            &replay_context,
            AgentLibraryMutationKind::Publish,
            &request.entry.agent_id,
            Some(&request.entry),
        ) {
            Ok(replayed) => replayed,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        if let Some((result, receipt)) = replayed {
            self.mutations
                .finalize_replay_receipt(&txn, &operation, &nonce, &receipt)?;
            self.mutations.commit_replay_receipt(txn)?;
            return Ok(result);
        }
        // L2 -> L1: every component this agent is assembled from must actually
        // exist, at the exact revision it pins, in this tenant, under the kind
        // the slot expects, and not withdrawn.
        //
        // Local validation checks only that a pin is WELL-FORMED. Without this
        // resolution, 64 invented hex characters publish an agent that claims a
        // component -- a tool, a system prompt, a model profile -- it was never
        // granted, and `capability_digest` then attests to the claim. It also
        // makes the documented property true: "republishing a component changes
        // its digest, so an agent assembled from the old one no longer
        // resolves" has no meaning until something resolves it.
        //
        // Inside this transaction, deliberately: resolving from a separate read
        // could admit an agent whose component was retired between the two.
        // After the replay check, so a retry of a committed publish is not
        // re-resolved against a tree that may have changed since.
        // L2 -> TEMPLATE travels with it: an instantiated agent records WHICH
        // template, at which revision, with which bindings. See
        // `admit_entry_references_in_write`.
        if let Err(error) = self.admit_entry_references_in_write(
            &txn,
            &request.context.tenant_id,
            "agent library entry",
            &request.entry,
        ) {
            txn.abort()?;
            return Err(error);
        }
        let previous_updated_at_ms = if expected_revision == 0 {
            0
        } else {
            self.entry_at_revision_in_write(
                &txn,
                &request.context.tenant_id,
                &request.entry.agent_id,
                expected_revision,
            )?
            .map(|entry| entry.updated_at_ms)
            .unwrap_or(0)
        };
        let mut write_context = replay_context.clone();
        write_context.created_at_ms =
            crate::server::dispatch::authoritative_now_ms().max(previous_updated_at_ms);
        // A retry may carry a different transport timestamp.  If this exact
        // target revision is already retained, reconstruct the event from the
        // durable row so its canonical payload remains byte-identical and the
        // replay kernel can decide before OCC/effect admission.
        let entry = if let Some(existing) = self.entry_at_revision_in_write(
            &txn,
            &request.context.tenant_id,
            &request.entry.agent_id,
            next_revision,
        )? {
            if existing.as_draft() != request.entry {
                txn.abort()?;
                return Err(
                    "IDEMPOTENCY_CONFLICT: target Agent Library revision has different definition"
                        .to_string(),
                );
            }
            existing
        } else {
            AgentLibraryEntry::publish(
                request.entry.clone(),
                next_revision,
                write_context.created_at_ms,
            )?
        };
        self.commit_entry_in_write(
            txn,
            &owner,
            &write_context,
            expected_revision,
            AgentLibraryMutationKind::Publish,
            entry,
            &operation,
            &nonce,
        )
    }

    /// Retire the current definition with a durable tombstone revision.
    pub fn retire(
        &self,
        request: AgentLibraryRetireRequest,
    ) -> Result<AgentLibraryWriteResult, String> {
        validate_context(self, &request.context)?;
        eg_types::agent_library::validate_key(&request.context.tenant_id, &request.agent_id)?;
        let expected_revision = request.context.expected_revision.ok_or_else(|| {
            "agent library writes require an explicit expected_revision".to_string()
        })?;
        let owner = self.scope_handle(&request.context.tenant_id)?;
        let txn = self.mutations.open_write(&owner)?;
        let nonce = match resolve_nonce_first(&self.mutations, &txn, &request.context) {
            Ok(nonce) => nonce,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        let next_revision = match next_revision(expected_revision) {
            Ok(next_revision) => next_revision,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        let replay_context = admitted_context(&request.context, "agent-library:retire")?;
        let operation = agent_library_operation_identity(
            &owner,
            &replay_context,
            "retire",
            &request.agent_id,
            expected_revision,
            None,
        )?;
        let replay = match self.mutations.resolve_replay(&txn, &operation, &nonce) {
            Ok(replay) => replay,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        let replayed = match replayed_receipt(
            &self.mutations,
            &txn,
            replay,
            &operation,
            &replay_context,
            AgentLibraryMutationKind::Retire,
            &request.agent_id,
            None,
        ) {
            Ok(replayed) => replayed,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        if let Some((result, receipt)) = replayed {
            self.mutations
                .finalize_replay_receipt(&txn, &operation, &nonce, &receipt)?;
            self.mutations.commit_replay_receipt(txn)?;
            return Ok(result);
        }
        let retained = self
            .entry_at_revision_in_write(
                &txn,
                &request.context.tenant_id,
                &request.agent_id,
                expected_revision,
            )?
            .ok_or_else(|| "agent library entry does not exist".to_string())?;
        if retained.is_retired() {
            txn.abort()?;
            return Err("agent library entry is already retired".to_string());
        }
        let mut write_context = replay_context.clone();
        write_context.created_at_ms =
            crate::server::dispatch::authoritative_now_ms().max(retained.updated_at_ms);
        let entry = if let Some(existing) = self.entry_at_revision_in_write(
            &txn,
            &request.context.tenant_id,
            &request.agent_id,
            next_revision,
        )? {
            if existing.is_retired() && existing.as_draft() == retained.as_draft() {
                existing
            } else {
                txn.abort()?;
                return Err(
                    "IDEMPOTENCY_CONFLICT: target Agent Library revision has different lifecycle"
                        .to_string(),
                );
            }
        } else {
            retained.retire(next_revision, write_context.created_at_ms)?
        };
        self.commit_entry_in_write(
            txn,
            &owner,
            &write_context,
            expected_revision,
            AgentLibraryMutationKind::Retire,
            entry,
            &operation,
            &nonce,
        )
    }

    fn commit_entry_in_write(
        &self,
        txn: eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
        owner: &OwnedStoreHandle<eg_storage::AgentLibraryOwner>,
        context: &AgentLibraryMutationContext,
        expected_revision: u64,
        kind: AgentLibraryMutationKind,
        entry: AgentLibraryEntry,
        operation: &eg_types::authority::OperationReplayIdentity,
        nonce: &eg_types::authority::NonceReplayKey,
    ) -> Result<AgentLibraryWriteResult, String> {
        let operations = agent_library_operations(kind, &entry);
        let policy_digest = effective_agent_library_policy_digest(&operations)?;
        let mut admitted_context = context.clone();
        admitted_context.policy_digest = format!("sha256:{}", policy_digest.to_hex());
        let event = AgentLibraryOutboxEvent::new(kind, entry.clone(), &admitted_context)?;
        let event_bytes = eg_storage::encode_bounded(&event, "agent library outbox event")?;
        let entry_bytes = eg_storage::encode_bounded(&entry, "agent library revision")?;
        let batch_id = batch_id(&admitted_context.idempotency_key)?;
        let authoritative_version = match self.mutations.current_version(&txn, owner) {
            Ok(version) => version,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        let batch = match build_batch(
            owner,
            &admitted_context,
            kind,
            &entry,
            authoritative_version,
            &batch_id,
            event_bytes.clone(),
        ) {
            Ok(batch) => batch,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        let begun = match txn.begin_with_replay_identity(&batch, operation, nonce) {
            Ok(begun) => begun,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        let source_version = match begun {
            Begin::Apply {
                source_version: Some(source_version),
            } => source_version,
            Begin::Apply {
                source_version: None,
            } => {
                txn.abort()?;
                return Err("agent library admission has no native source version".to_string());
            }
            Begin::Replay(_) => {
                txn.abort()?;
                return Err(
                    "CORRUPT_MUTATION_LEDGER: replay became visible after a fresh admission decision"
                        .to_string(),
                );
            }
        };
        if source_version != authoritative_version {
            txn.abort()?;
            return Err("agent library source version changed while admitting write".to_string());
        }
        let committed_version = source_version
            .checked_add(1)
            .ok_or_else(|| "agent library committed version overflow".to_string())?;
        let stable_result =
            AgentLibraryCommittedResult::new(entry.clone(), batch_id.clone(), committed_version)?;
        let result_bytes = encode_domain_result(&stable_result)?;
        let receipt = owner_receipt(
            operation,
            nonce,
            &batch,
            "agent-library",
            AGENT_LIBRARY_OUTBOX_TOPIC,
            &format!(
                "{}:{}:{}",
                entry.tenant_id, entry.agent_id, entry.entry_revision
            ),
            &event_bytes,
            &expected_headers_from_event(&event),
            domain_result_for(&stable_result)?,
            committed_version,
            admitted_context.created_at_ms,
        )?;

        let owner_write_result: Result<(), String> = (|| {
            let owner_write = txn.owner_rows(owner, &batch)?;
            apply_entry_rows(
                &owner_write,
                &admitted_context,
                expected_revision,
                &entry,
                entry_bytes.as_slice(),
            )?;
            owner_write.finish_owner()
        })();
        if let Err(error) = owner_write_result {
            txn.abort()?;
            return Err(error);
        }
        let record = match self.mutations.finish_with_replay(
            &txn,
            &batch,
            Some(result_bytes),
            admitted_context.created_at_ms,
            Some(source_version),
            (operation, nonce, &receipt),
        ) {
            Ok(record) => record,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        let recorded_version = record
            .committed_version
            .target()
            .ok_or_else(|| "agent library commit has no target version".to_string())?;
        if recorded_version != committed_version {
            txn.abort()?;
            return Err(
                "agent library result version differs from the committed version".to_string(),
            );
        }
        self.mutations.commit(txn, &batch)?;
        Ok(stable_result.response(false))
    }
}

fn replayed_receipt(
    mutations: &MutationKernel,
    txn: &eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
    replay: ReplayResolution,
    operation: &eg_types::authority::OperationReplayIdentity,
    context: &AgentLibraryMutationContext,
    kind: AgentLibraryMutationKind,
    agent_id: &str,
    draft: Option<&AgentLibraryEntryDraft>,
) -> Result<Option<(AgentLibraryWriteResult, MutationReceipt)>, String> {
    let recorded = match replay {
        ReplayResolution::Fresh => return Ok(None),
        ReplayResolution::NonceRejected { idempotency_key } => {
            return Err(format!(
                "REPLAY_NONCE_CONSUMED: attempt nonce already consumed by '{idempotency_key}'"
            ));
        }
        ReplayResolution::Conflict { .. } => {
            return Err(
                "IDEMPOTENCY_CONFLICT: key was already used by a different Agent Library mutation"
                    .to_string(),
            );
        }
        ReplayResolution::ReplayedResult(recorded) => *recorded,
    };
    let RecordedOperation::Receipt(receipt) = recorded else {
        return Err(
            "CORRUPT_MUTATION_LEDGER: Agent Library replay is missing its typed receipt"
                .to_string(),
        );
    };
    let receipt = *receipt;
    receipt.validate()?;
    if receipt.disposition.as_str() != "committed"
        || receipt.operation_replay_digest != operation.digest()?
        || receipt.scope != operation.authority_scope
    {
        return Err(
            "CORRUPT_MUTATION_LEDGER: Agent Library replay receipt identity is invalid".to_string(),
        );
    }
    let stable_result = receipt_result(&receipt)?;
    let expected_batch_id = batch_id(&context.idempotency_key)?;
    let expected_entry_revision = context
        .expected_revision
        .and_then(|revision| revision.checked_add(1));
    if stable_result.batch_id != expected_batch_id
        || stable_result.entry.agent_id != agent_id
        || stable_result.entry.tenant_id != context.tenant_id
        || expected_entry_revision != Some(stable_result.entry.entry_revision)
        || stable_result.entry.lifecycle
            != match kind {
                AgentLibraryMutationKind::Publish => AgentLibraryLifecycle::Published,
                AgentLibraryMutationKind::Retire => AgentLibraryLifecycle::Retired,
            }
        || draft.is_some_and(|expected| stable_result.entry.as_draft() != *expected)
    {
        return Err(
            "IDEMPOTENCY_CONFLICT: key was already used by a different Agent Library mutation"
                .to_string(),
        );
    }
    let event = AgentLibraryOutboxEvent::new(kind, stable_result.entry.clone(), context)?;
    let event_bytes = eg_storage::encode_bounded(&event, "agent library replay event")?;
    let expected_key = format!(
        "{}:{}:{}",
        stable_result.entry.tenant_id,
        stable_result.entry.agent_id,
        stable_result.entry.entry_revision
    );
    let headers = expected_headers(context, &stable_result.entry);
    let effect_digest = agent_library_effect_digest(
        AGENT_LIBRARY_OUTBOX_TOPIC,
        &expected_key,
        &event_bytes,
        &headers,
    )?;
    if receipt.effect_digest != Some(effect_digest) {
        return Err(
            "CORRUPT_MUTATION_LEDGER: Agent Library replay receipt does not bind its outbox"
                .to_string(),
        );
    }

    // A typed replay receipt is only authoritative when the kernel can show
    // the one committed batch, its operation class and its physical outbox
    // row in the same owner snapshot.  The replay table alone is not enough:
    // accepting it after a batch, class or outbox row was redirected would
    // return a result for an effect the ledger did not actually commit.
    let Some((record, class, physical_outbox)) =
        mutations.read_replay_evidence(txn, &stable_result.batch_id)?
    else {
        return Err(
            "CORRUPT_MUTATION_LEDGER: Agent Library replay receipt points to a missing batch"
                .to_string(),
        );
    };
    record.validate().map_err(|error| {
        format!("CORRUPT_MUTATION_LEDGER: Agent Library replay batch is invalid: {error}")
    })?;
    if record.status != MutationBatchStatus::Committed
        || class != eg_storage::MutationClass::Operation
        || record.identity != *txn.scope()
        || record.batch.identity != *txn.scope()
        || record.batch.batch_id != stable_result.batch_id
        || record.batch.operations.len() != 1
        || record.batch.outbox.len() != 1
        || record.committed_version.target() != Some(stable_result.committed_version)
        || record.batch.idempotency_key() != context.idempotency_key.as_str()
        || record.batch.serving_principal() != context.principal.as_str()
        || record.committing_actor()? != context.caller_principal.as_str()
    {
        return Err(
            "CORRUPT_MUTATION_LEDGER: Agent Library replay batch identity is invalid".to_string(),
        );
    }
    let expected_operation = agent_library_operations(kind, &stable_result.entry)
        .into_iter()
        .next()
        .ok_or_else(|| {
            "CORRUPT_MUTATION_LEDGER: Agent Library replay operation is missing".to_string()
        })?;
    let actual_operation_bytes = eg_storage::encode_bounded(
        &record.batch.operations[0],
        "Agent Library replay operation",
    )?;
    let expected_operation_bytes =
        eg_storage::encode_bounded(&expected_operation, "Agent Library expected operation")?;
    if actual_operation_bytes != expected_operation_bytes {
        return Err(
            "CORRUPT_MUTATION_LEDGER: Agent Library replay operation differs from its receipt"
                .to_string(),
        );
    }
    let receipt_result_bytes =
        eg_storage::encode_bounded(&receipt.result, "mutation receipt result")?;
    if record.result_msgpack.as_deref() != Some(receipt_result_bytes.as_slice()) {
        return Err(
            "CORRUPT_MUTATION_LEDGER: Agent Library replay result bytes differ from its receipt"
                .to_string(),
        );
    }
    let physical_envelope = record.batch.envelope.operation().ok_or_else(|| {
        "CORRUPT_MUTATION_LEDGER: Agent Library replay batch is not an operation".to_string()
    })?;
    let physical_operation = physical_envelope.operation_identity()?;
    let mut comparable_operation = physical_operation;
    comparable_operation.canonical_payload_digest = operation.canonical_payload_digest;
    let physical_nonce = physical_envelope.nonce_replay_key()?;
    if comparable_operation != *operation || physical_nonce.digest()? != receipt.nonce_replay_digest
    {
        return Err(
            "CORRUPT_MUTATION_LEDGER: Agent Library replay operation identity is invalid"
                .to_string(),
        );
    }
    let intent = &record.batch.outbox[0];
    if intent.topic != AGENT_LIBRARY_OUTBOX_TOPIC
        || intent.key != expected_key
        || intent.payload != event_bytes
        || intent.headers != headers
    {
        return Err(
            "CORRUPT_MUTATION_LEDGER: Agent Library replay batch outbox intent differs".to_string(),
        );
    }
    if physical_outbox.len() != 1 {
        return Err(
            "CORRUPT_MUTATION_LEDGER: Agent Library replay must contain one outbox row".to_string(),
        );
    }
    let outbox = &physical_outbox[0];
    outbox.validate().map_err(|error| {
        format!("CORRUPT_MUTATION_LEDGER: Agent Library replay outbox row is invalid: {error}")
    })?;
    if outbox.batch_id != record.batch.batch_id
        || outbox.ordinal != 0
        || outbox.identity != *txn.scope()
        || outbox.committed_version != record.committed_version
        || outbox.created_at_ms != record.batch.created_at_ms
        || outbox.intent != *intent
    {
        return Err(
            "CORRUPT_MUTATION_LEDGER: Agent Library replay outbox row differs from its batch"
                .to_string(),
        );
    }
    if record_event(&record)? != event {
        return Err(
            "CORRUPT_MUTATION_LEDGER: Agent Library replay event differs from its receipt"
                .to_string(),
        );
    }
    Ok(Some((stable_result.response(true), receipt)))
}

fn receipt_result(receipt: &MutationReceipt) -> Result<AgentLibraryCommittedResult, String> {
    let MutationResult::DomainResult {
        schema_id, payload, ..
    } = &receipt.result
    else {
        return Err("Agent Library replay receipt has no DomainResult".to_string());
    };
    if schema_id.as_str() != AGENT_LIBRARY_RESULT_SCHEMA_ID {
        return Err("Agent Library replay receipt has an unexpected result schema".to_string());
    }
    let result: AgentLibraryCommittedResult =
        super::agent_row::decode(payload.as_slice(), "Agent Library replay result payload")?;
    result.validate()?;
    Ok(result)
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

fn read_history(
    read: &ScopedRead<'_, eg_storage::AgentLibraryOwner>,
    tenant_id: &str,
    agent_id: &str,
) -> Result<(Option<u64>, Vec<AgentLibraryEntry>), String> {
    let head = read
        .open_owner_table(eg_storage::AGENT_LIBRARY_HEADS)?
        .get((tenant_id, agent_id))
        .map_err(|error| error.to_string())?
        .map(|value| value.value());
    let table = read.open_owner_table(eg_storage::AGENT_LIBRARY_REVISIONS)?;
    let mut entries = Vec::new();
    let mut bytes = 0usize;
    for row in table
        .range((tenant_id, agent_id, 0)..=(tenant_id, agent_id, u64::MAX))
        .map_err(|error| error.to_string())?
    {
        let (key, value) = row.map_err(|error| error.to_string())?;
        let (row_tenant, row_agent, revision) = key.value();
        if row_tenant != tenant_id || row_agent != agent_id {
            return Err("agent library revision range escaped its key prefix".to_string());
        }
        bytes = bytes
            .checked_add(value.value().len())
            .filter(|total| *total <= MAX_AGENT_LIBRARY_HISTORY_BYTES)
            .ok_or_else(|| "agent library revision history exceeds resource limits".to_string())?;
        if entries.len() >= MAX_AGENT_LIBRARY_REVISIONS {
            return Err("agent library revision history exceeds resource limits".to_string());
        }
        let entry = decode_entry(value.value())?;
        validate_entry_key(&entry, tenant_id, agent_id, revision)?;
        entries.push(entry);
    }
    match (head, entries.last()) {
        (None, None) => return Ok((None, entries)),
        (None, Some(_)) => return Err("agent library revisions exist without a head".to_string()),
        (Some(_), None) => {
            return Err("agent library head points to an empty revision chain".to_string())
        }
        (Some(head), Some(last)) if head != last.entry_revision => {
            return Err("agent library head does not match the final revision".to_string())
        }
        (Some(_), Some(_)) => {}
    }
    for (index, entry) in entries.iter().enumerate() {
        let expected = (index as u64).saturating_add(1);
        if entry.entry_revision != expected {
            return Err("agent library revision chain has a gap".to_string());
        }
        if entry.is_retired() {
            let Some(previous) = index.checked_sub(1).and_then(|i| entries.get(i)) else {
                return Err("agent library revision one cannot be a tombstone".to_string());
            };
            if previous.is_retired()
                || !entries[index + 1..].is_empty()
                || entry.as_draft() != previous.as_draft()
                || entry.created_at_ms != previous.created_at_ms
                || entry.updated_at_ms < previous.updated_at_ms
            {
                return Err(
                    "agent library tombstone does not preserve its prior definition".to_string(),
                );
            }
        }
    }
    Ok((head, entries))
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
    let event_type = match kind {
        AgentLibraryMutationKind::Publish => "agent_library_publish",
        AgentLibraryMutationKind::Retire => "agent_library_retire",
    };
    vec![MutationOperation {
        ordinal: 0,
        surface: MutationSurface::Lifecycle,
        domain: DurabilityDomain::ControlPlane,
        method: Method::ApplyMutation {
            event_type: event_type.to_string(),
            query: entry.definition_digest.clone(),
        },
    }]
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
    let operations = vec![MutationOperation {
        ordinal: 0,
        surface: MutationSurface::Lifecycle,
        domain: DurabilityDomain::ControlPlane,
        method: Method::ApplyMutation {
            event_type: "agent_library_policy".to_string(),
            query: String::new(),
        },
    }];
    Ok(format!(
        "sha256:{}",
        effective_agent_library_policy_digest(&operations)?.to_hex()
    ))
}

fn build_batch(
    owner: &eg_storage::OwnedStoreHandle<eg_storage::AgentLibraryOwner>,
    context: &AgentLibraryMutationContext,
    kind: AgentLibraryMutationKind,
    entry: &AgentLibraryEntry,
    version: u64,
    batch_id: &str,
    event_bytes: Vec<u8>,
) -> Result<MutationBatch, String> {
    let key = format!(
        "{}:{}:{}",
        entry.tenant_id, entry.agent_id, entry.entry_revision
    );
    let mut headers = BTreeMap::new();
    headers.insert(
        "schema_version".to_string(),
        AGENT_LIBRARY_OUTBOX_SCHEMA_VERSION.to_string(),
    );
    headers.insert("tenant_id".to_string(), entry.tenant_id.clone());
    headers.insert("agent_id".to_string(), entry.agent_id.clone());
    headers.insert(
        "entry_revision".to_string(),
        entry.entry_revision.to_string(),
    );
    headers.insert(
        "definition_digest".to_string(),
        entry.definition_digest.clone(),
    );
    headers.insert(
        "definition_actor_scope".to_string(),
        entry.actor_scope.clone(),
    );
    headers.insert(
        "definition_purpose_id".to_string(),
        entry.purpose_id.clone(),
    );
    headers.insert(
        "definition_policy_digest".to_string(),
        entry.policy_digest.clone(),
    );
    headers.insert("actor".to_string(), context.caller_principal.clone());
    headers.insert(
        "action_actor_scope".to_string(),
        context.actor_scope.clone(),
    );
    headers.insert("action_purpose_id".to_string(), context.purpose_id.clone());
    headers.insert(
        "action_policy_revision".to_string(),
        context.policy_revision.clone(),
    );
    headers.insert(
        "action_policy_digest".to_string(),
        context.policy_digest.clone(),
    );
    headers.insert(
        "action_policy_decision_id".to_string(),
        context.policy_decision_id.clone(),
    );
    headers.insert(
        "source_revision_digest".to_string(),
        entry.source_revision_digest.clone(),
    );
    let operations = agent_library_operations(kind, entry);
    let outbox = vec![MutationOutboxIntent {
        topic: AGENT_LIBRARY_OUTBOX_TOPIC.to_string(),
        key,
        payload: event_bytes,
        headers,
    }];
    let content = BatchContent {
        operations: &operations,
        outbox: &outbox,
        authoritative_state: None,
    };
    let method_schema_digest = eg_capabilities::method_schema("ApplyMutation")
        .map(|(_, digest)| eg_types::contract::Digest256::from_bytes(digest))
        .ok_or_else(|| "ApplyMutation is missing from the contract catalog".to_string())?;
    let compiled = CompiledOperation::for_content(owner.identity(), content, method_schema_digest)?;
    let mut compiled_envelope = CompiledEnvelope::new(
        CompiledScope {
            identity: owner.identity(),
            actor: &context.caller_principal,
            serving_principal: owner.principal(),
            request_id: context.request_id,
            idempotency_key: context.idempotency_key.as_str(),
            nonce: context.attempt_nonce,
            now_ms: context.created_at_ms,
        },
        compiled,
    )?;
    compiled_envelope.catalog_digest =
        eg_types::contract::Digest256::parse(eg_capabilities::CONTRACT_CATALOG_DIGEST)?;
    compiled_envelope.policy_digest = parse_prefixed_digest(&context.policy_digest)?;
    compiled_envelope.policy_revision = context.policy_revision.clone();
    compiled_envelope.policy_decision_id = context.policy_decision_id.clone();
    let batch = MutationBatch {
        schema_version: MUTATION_BATCH_VERSION,
        batch_id: batch_id.to_string(),
        envelope: MutationEnvelope::for_compiled_batch(compiled_envelope)?,
        identity: owner.identity().clone(),
        placement_epoch: 0,
        version_expectation: VersionExpectation::Native(version),
        fencing_token: None,
        authoritative_state: None,
        operations,
        outbox,
        created_at_ms: context.created_at_ms,
    };
    batch.validate_write_budget()?;
    Ok(batch)
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

fn replay_record(
    record: &eg_types::MutationBatchRecord,
    context: &AgentLibraryMutationContext,
    kind: AgentLibraryMutationKind,
    agent_id: &str,
    draft: Option<&AgentLibraryEntryDraft>,
    expected_event_bytes: &[u8],
) -> Result<AgentLibraryWriteResult, String> {
    let event = record_event(record)?;
    let stable_result = record_result(record)?;
    let intent = record
        .batch
        .outbox
        .first()
        .ok_or_else(|| "Agent Library receipt has no outbox event".to_string())?;
    let expected_batch_id = batch_id(&context.idempotency_key)?;
    let expected_key = format!(
        "{}:{}:{}",
        context.tenant_id, agent_id, event.entry.entry_revision
    );
    let expected_headers = expected_headers(context, &event.entry);
    let exact_receipt = record.status == MutationBatchStatus::Committed
        && record.validate_identity().is_ok()
        && record.batch.batch_id == expected_batch_id
        && record.batch.idempotency_key() == context.idempotency_key
        && record.batch.identity.tenant().as_str() == context.tenant_id
        && record.batch.serving_principal() == context.principal
        && record.batch.operations.len() == 1
        && record.batch.outbox.len() == 1
        && record.committing_actor()? == context.caller_principal.as_str()
        && intent.topic == AGENT_LIBRARY_OUTBOX_TOPIC
        && intent.key == expected_key
        && intent.payload == expected_event_bytes
        && intent.headers == expected_headers
        && stable_result.batch_id == expected_batch_id
        && stable_result.entry == event.entry
        && stable_result.committed_version == record.committed_version.target().unwrap_or(0);
    if !exact_receipt
        || event.kind != kind
        || event.entry.agent_id != agent_id
        || event.entry.tenant_id != context.tenant_id
        || event.performing_actor != context.caller_principal
        || event.action_actor_scope != context.actor_scope
        || event.action_purpose_id != context.purpose_id
        || event.action_policy_revision != context.policy_revision
        || event.action_policy_digest != context.policy_digest
        || event.action_policy_decision_id != context.policy_decision_id
        || event.entry.lifecycle
            != match kind {
                AgentLibraryMutationKind::Publish => AgentLibraryLifecycle::Published,
                AgentLibraryMutationKind::Retire => AgentLibraryLifecycle::Retired,
            }
        || draft.is_some_and(|expected| event.entry.as_draft() != *expected)
    {
        return Err(
            "IDEMPOTENCY_CONFLICT: key was already used by a different Agent Library mutation"
                .to_string(),
        );
    }
    if record.committed_version.target().is_none() {
        return Err("replayed Agent Library receipt has no target version".to_string());
    }
    Ok(stable_result.response(true))
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

/// The typed receipt one committed owner mutation records.
///
/// Takes the effect in primitives (`topic`, `key`, `event_bytes`, `headers`)
/// and an already-built `MutationResult`, rather than the entry-typed event and
/// result it used to. Both record families in this owner (RF-ADR-008) mint the
/// same receipt shape over different payloads, and every field below is derived
/// from the operation, the nonce, the batch, or those primitives.
///
/// `slug` names the record family in the receipt's opaque ids so an entry
/// receipt and a graph receipt are distinguishable in the ledger.
#[allow(clippy::too_many_arguments)]
pub(super) fn owner_receipt(
    operation: &eg_types::authority::OperationReplayIdentity,
    nonce: &eg_types::authority::NonceReplayKey,
    batch: &MutationBatch,
    slug: &str,
    topic: &str,
    key: &str,
    event_bytes: &[u8],
    headers: &BTreeMap<String, String>,
    mutation_result: MutationResult,
    committed_version: u64,
    committed_at_ms: u64,
) -> Result<MutationReceipt, String> {
    use eg_types::contract::{MutationDisposition, OpaqueId, UtcUnixNanos};

    let operation_digest = operation.digest()?;
    let nonce_digest = nonce.digest()?;
    let context_digest = batch
        .envelope
        .operation()
        .map(|envelope| envelope.authority.context_digest)
        .unwrap_or(operation_digest);
    let suffix = operation_digest.to_hex();
    let effect_digest = agent_library_effect_digest(topic, key, event_bytes, headers)?;
    let recorded_at = committed_at_ms
        .checked_mul(1_000_000)
        .and_then(|value| i64::try_from(value).ok())
        .map(UtcUnixNanos::new)
        .ok_or_else(|| "agent library receipt time exceeds the supported range".to_string())?;
    let receipt = MutationReceipt {
        receipt_id: OpaqueId::new(format!("{slug}-receipt-{suffix}"))?,
        mutation_id: OpaqueId::new(format!("{slug}-mutation-{suffix}"))?,
        scope: operation.authority_scope.clone(),
        authority_receipt_id: OpaqueId::new(format!("{slug}-authority-{suffix}"))?,
        authority_evidence_digest: context_digest,
        disposition: MutationDisposition::new("committed")?,
        operation_replay_digest: operation_digest,
        nonce_replay_digest: nonce_digest,
        envelope_digest: context_digest,
        effect_id: Some(OpaqueId::new(format!("{slug}-effect-{suffix}"))?),
        effect_digest: Some(effect_digest),
        result_digest: mutation_result.digest()?,
        commit_id: Some(OpaqueId::new(format!(
            "{slug}-commit-{suffix}-{committed_version}"
        ))?),
        result: mutation_result,
        recorded_at,
    };
    receipt.validate()?;
    Ok(receipt)
}

fn expected_headers_from_event(event: &AgentLibraryOutboxEvent) -> BTreeMap<String, String> {
    BTreeMap::from([
        (
            "schema_version".to_string(),
            AGENT_LIBRARY_OUTBOX_SCHEMA_VERSION.to_string(),
        ),
        ("tenant_id".to_string(), event.entry.tenant_id.clone()),
        ("agent_id".to_string(), event.entry.agent_id.clone()),
        (
            "entry_revision".to_string(),
            event.entry.entry_revision.to_string(),
        ),
        (
            "definition_digest".to_string(),
            event.entry.definition_digest.clone(),
        ),
        (
            "definition_actor_scope".to_string(),
            event.entry.actor_scope.clone(),
        ),
        (
            "definition_purpose_id".to_string(),
            event.entry.purpose_id.clone(),
        ),
        (
            "definition_policy_digest".to_string(),
            event.entry.policy_digest.clone(),
        ),
        ("actor".to_string(), event.performing_actor.clone()),
        (
            "action_actor_scope".to_string(),
            event.action_actor_scope.clone(),
        ),
        (
            "action_purpose_id".to_string(),
            event.action_purpose_id.clone(),
        ),
        (
            "action_policy_revision".to_string(),
            event.action_policy_revision.clone(),
        ),
        (
            "action_policy_digest".to_string(),
            event.action_policy_digest.clone(),
        ),
        (
            "action_policy_decision_id".to_string(),
            event.action_policy_decision_id.clone(),
        ),
        (
            "source_revision_digest".to_string(),
            event.entry.source_revision_digest.clone(),
        ),
    ])
}

pub(super) fn agent_library_effect_digest(
    topic: &str,
    key: &str,
    payload: &[u8],
    headers: &BTreeMap<String, String>,
) -> Result<eg_types::contract::Digest256, String> {
    let mut header_digest =
        eg_types::contract::Digest256::framed(b"eg/agent-library-effect-headers/v1", &[])?;
    for (header, value) in headers {
        header_digest = eg_types::contract::Digest256::framed(
            b"eg/agent-library-effect-header/v1",
            &[
                header_digest.as_bytes(),
                header.as_bytes(),
                value.as_bytes(),
            ],
        )?;
    }
    eg_types::contract::Digest256::framed(
        b"eg/agent-library-effect/v1",
        &[
            topic.as_bytes(),
            key.as_bytes(),
            payload,
            header_digest.as_bytes(),
        ],
    )
}

fn record_result(
    record: &eg_types::MutationBatchRecord,
) -> Result<AgentLibraryCommittedResult, String> {
    let bytes = record
        .result_msgpack
        .as_deref()
        .ok_or_else(|| "Agent Library receipt has no typed domain result".to_string())?;
    let decoded = super::agent_row::try_decode::<MutationResult>(bytes);
    let mutation_result = match decoded {
        Ok(mutation_result) => mutation_result,
        // v6 Batch rows encoded the outbox event itself as result_msgpack.
        // Keep those rows readable for status/recovery projections, while the
        // nonce-first operation path remains Receipt-only and therefore cannot
        // silently treat a legacy row as replay authority.
        Err(_) => {
            let event: AgentLibraryOutboxEvent =
                super::agent_row::decode(bytes, "Agent Library domain result")?;
            event.validate()?;
            let committed_version = record
                .committed_version
                .target()
                .ok_or_else(|| "legacy Agent Library receipt has no target version".to_string())?;
            return AgentLibraryCommittedResult::new(
                event.entry,
                record.batch.batch_id.clone(),
                committed_version,
            );
        }
    };
    mutation_result.validate()?;
    let MutationResult::DomainResult {
        schema_id, payload, ..
    } = mutation_result
    else {
        return Err("Agent Library receipt does not contain a DomainResult".to_string());
    };
    if schema_id.as_str() != AGENT_LIBRARY_RESULT_SCHEMA_ID {
        return Err("Agent Library receipt has an unexpected result schema".to_string());
    }
    let result: AgentLibraryCommittedResult =
        super::agent_row::decode(payload.as_slice(), "Agent Library domain result payload")?;
    result.validate()?;
    Ok(result)
}

fn record_event(record: &eg_types::MutationBatchRecord) -> Result<AgentLibraryOutboxEvent, String> {
    if record.batch.outbox.len() != 1 {
        return Err("Agent Library receipt must contain exactly one outbox event".to_string());
    }
    let intent = record
        .batch
        .outbox
        .first()
        .ok_or_else(|| "Agent Library receipt has no outbox event".to_string())?;
    let event: AgentLibraryOutboxEvent =
        super::agent_row::decode(intent.payload.as_slice(), "Agent Library outbox event")?;
    event.validate()?;
    Ok(event)
}

fn expected_headers(
    context: &AgentLibraryMutationContext,
    entry: &AgentLibraryEntry,
) -> BTreeMap<String, String> {
    BTreeMap::from([
        (
            "schema_version".to_string(),
            AGENT_LIBRARY_OUTBOX_SCHEMA_VERSION.to_string(),
        ),
        ("tenant_id".to_string(), entry.tenant_id.clone()),
        ("agent_id".to_string(), entry.agent_id.clone()),
        (
            "entry_revision".to_string(),
            entry.entry_revision.to_string(),
        ),
        (
            "definition_digest".to_string(),
            entry.definition_digest.clone(),
        ),
        (
            "definition_actor_scope".to_string(),
            entry.actor_scope.clone(),
        ),
        (
            "definition_purpose_id".to_string(),
            entry.purpose_id.clone(),
        ),
        (
            "definition_policy_digest".to_string(),
            entry.policy_digest.clone(),
        ),
        ("actor".to_string(), context.caller_principal.clone()),
        (
            "action_actor_scope".to_string(),
            context.actor_scope.clone(),
        ),
        ("action_purpose_id".to_string(), context.purpose_id.clone()),
        (
            "action_policy_revision".to_string(),
            context.policy_revision.clone(),
        ),
        (
            "action_policy_digest".to_string(),
            context.policy_digest.clone(),
        ),
        (
            "action_policy_decision_id".to_string(),
            context.policy_decision_id.clone(),
        ),
        (
            "source_revision_digest".to_string(),
            entry.source_revision_digest.clone(),
        ),
    ])
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
    let mut entry = seed_agent_draft_for_test(store, tenant_id, agent_id, nonce_index);
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
    _store: &AgentLibraryStore,
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

    fn draft(store: &AgentLibraryStore, tenant_id: &str, agent_id: &str) -> AgentLibraryEntryDraft {
        use eg_types::agent_component::AgentComponentKind;
        AgentLibraryEntryDraft {
            agent_id: agent_id.to_string(),
            package_id: "agent-package".to_string(),
            version: "1.0.0".to_string(),
            role: "researcher".to_string(),
            role_digest: digest('1'),
            system_prompt: component_pin(
                store,
                tenant_id,
                "prompt:agent",
                AgentComponentKind::SystemPrompt,
                1,
            ),
            tools: vec![component_pin(
                store,
                tenant_id,
                "tool:search",
                AgentComponentKind::Tool,
                2,
            )],
            skills: vec![component_pin(
                store,
                tenant_id,
                "skill:reason",
                AgentComponentKind::Skill,
                3,
            )],
            model_profile: component_pin(
                store,
                tenant_id,
                "model-profile:default",
                AgentComponentKind::ModelProfile,
                4,
            ),
            model_identity: "model:default".to_string(),
            ontologies: vec![component_pin(
                store,
                tenant_id,
                "ontology:agent",
                AgentComponentKind::Ontology,
                5,
            )],
            tenant_id: tenant_id.to_string(),
            actor_scope: "definition:builder-a".to_string(),
            purpose_id: "agent-library:definition".to_string(),
            policy_digest: digest('7'),
            source_revision: "source-revision:42".to_string(),
            source_revision_digest: digest('8'),
            runtime: Default::default(),
            instantiated_from: None,
        }
    }

    fn context(
        store: &AgentLibraryStore,
        tenant_id: &str,
        key: &str,
        nonce: u8,
        expected_revision: u64,
        caller_byte: char,
        policy_revision: &str,
        purpose_id: &str,
        created_at_ms: u64,
    ) -> AgentLibraryMutationContext {
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
            "shared-key",
            1,
            0,
            'a',
            "policy-v1",
            "agent-library:publish",
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
            "nonce-before-target",
            1,
            MAX_AGENT_LIBRARY_REVISIONS as u64,
            'a',
            "policy-v1",
            "agent-library:publish",
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
            "over-limit",
            11,
            MAX_AGENT_LIBRARY_REVISIONS as u64,
            'a',
            "policy-v1",
            "agent-library:publish",
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
            "shared-key",
            2,
            0,
            'a',
            "policy-v1",
            "agent-library:publish",
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
                "shared-key",
                2,
                0,
                'a',
                "policy-v1",
                "agent-library:publish",
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
            "shared-key",
            3,
            0,
            'b',
            "policy-v1",
            "agent-library:publish",
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
            "shared-key",
            4,
            0,
            'a',
            "policy-v2",
            "agent-library:publish",
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
            "new-key",
            5,
            0,
            'a',
            "policy-v1",
            "agent-library:publish",
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
            "retire-key",
            6,
            1,
            'b',
            "policy-v2",
            "agent-library:retire",
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
                    "retire-key",
                    7,
                    1,
                    'b',
                    "policy-v2",
                    "agent-library:retire",
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
                    "shared-key",
                    12,
                    0,
                    'a',
                    "policy-v1",
                    "agent-library:publish",
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
            "resurrection-key",
            8,
            2,
            'a',
            "policy-v1",
            "agent-library:publish",
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
                    "shared-key",
                    9,
                    0,
                    'c',
                    "policy-v1",
                    "agent-library:publish",
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
                    "shared-key",
                    13,
                    0,
                    'a',
                    "policy-v1",
                    "agent-library:publish",
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
                    "shared-key",
                    14,
                    0,
                    'a',
                    "policy-v1",
                    "agent-library:publish",
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
            key,
            nonce,
            0,
            'a',
            "policy-v1",
            "agent-library:publish",
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
                    provides: Vec::new(),
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
            "persisted-status-tamper",
            1,
            0,
            'a',
            "policy-v1",
            "agent-library:publish",
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
                    "redirected-status-link",
                    1,
                    0,
                    'a',
                    "policy-v1",
                    "agent-library:publish",
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
