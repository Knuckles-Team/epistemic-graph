//! Durable revisions for agent COMPONENTS -- layer 1 of the agent hierarchy
//! (RF-ADR-008).
//!
//! Model profiles, prompts, tools, MCP servers/prompts/resources, skills,
//! schemas and predicates: the parts that [`super::agent_library`] entries are
//! assembled from and [`super::agent_graph`] graphs compose. Published into the
//! SAME owner file as both, because a question about an agent system should be
//! answerable from one place rather than by reconciling three
//! (RF-RULING-004).
//!
//! Source packages and MCP servers remain the ORIGIN of these; EG is where they
//! are recorded so they can be queried. `ComponentProvenance` on each record is
//! what makes that split work -- without it a re-ingest cannot tell an updated
//! component from a new one, and "which agents break if this server changes?"
//! has nothing to traverse.
//!
//! The revision protocol is the same one the other two layers use, and reuses
//! the same machinery from [`super::agent_library`]. See that module's notes on
//! why the shared parts are shared by parameter rather than by a trait, and why
//! generalizing is safer once there are several tested implementations than
//! before there is one.

use std::collections::BTreeMap;
use std::sync::Arc;

use redb::ReadableTable;

use eg_storage::{OwnedStoreHandle, RecordedOperation, ScopedRead};
use eg_transaction::{AdmittedOwnerWrite, Begin, ReplayResolution};

use eg_types::agent_component::{
    AgentComponentCommittedResult, AgentComponentEntry, AgentComponentMutationKind,
    AgentComponentOutboxEvent, AgentComponentPublishRequest, AgentComponentRetireRequest,
    AgentComponentStatusRequest, AGENT_COMPONENT_SCHEMA_VERSION,
};
use eg_types::agent_library::{AgentLibraryLifecycle, AgentLibraryMutationContext};
use eg_types::mutation::MutationResult;
use eg_types::mutation_batch::{
    BatchContent, CompiledEnvelope, CompiledOperation, CompiledScope, DurabilityDomain,
    MutationBatch, MutationEnvelope, MutationOperation, MutationOutboxIntent, MutationSurface,
    VersionExpectation, MUTATION_BATCH_VERSION,
};
use eg_types::protocol::Method;

use super::agent_library::{
    admitted_context, agent_library_operation_identity, batch_id,
    effective_agent_library_policy_digest, next_revision, owner_receipt, require_expected_revision,
    resolve_nonce_first, validate_context, AgentLibraryStore,
};

const AGENT_COMPONENT_OUTBOX_TOPIC: &str = "eg.agent-component.revision.v1";
const AGENT_COMPONENT_RESULT_SCHEMA_ID: &str = "agent-component-result.v1";
const MAX_AGENT_COMPONENT_REVISIONS: usize = 16_384;
const MAX_AGENT_COMPONENT_HISTORY_BYTES: usize = 256 * 1024 * 1024;
/// Head rows one search PAGE may examine.
///
/// Correctly named for what it bounds, unlike the revision bound this once
/// borrowed: `MAX_AGENT_COMPONENT_REVISIONS` means "revisions of ONE component"
/// and says nothing about how many components a tenant may hold. Reusing it
/// made a tenant past 16,384 components permanently unsearchable -- a refusal
/// with no cursor to page past it. This bounds one page's work instead; a tenant
/// larger than it pages, it is never refused.
const MAX_AGENT_COMPONENT_SEARCH_SCAN: usize = 4_096;
/// Encoded row bytes one search PAGE may accumulate.
///
/// The count bound alone does not bound the response: every other read op in
/// this family pairs a count with a byte bound, and search was the one that did
/// not. Exceeded by at most one row, because a page that returned nothing would
/// not make progress.
const MAX_AGENT_COMPONENT_SEARCH_BYTES: usize = 8 * 1024 * 1024;
/// Most DISTINCT component pins one publish may resolve.
///
/// A publish adds one point lookup per distinct pin, so the fan-out needs its
/// own bound rather than inheriting whatever the per-list bounds multiply out
/// to. Set above the largest LEGAL pin count of ANY subject that resolves
/// through this helper, so it refuses abuse and never a record that validates:
///
/// * a COMPONENT pins up to `MAX_DEPENDENCIES` (256) requirements plus the one
///   MCP server its provenance names -- 257;
/// * a LIBRARY ENTRY pins `MAX_REFERENCE_COUNT` (1,024) tools, skills,
///   ontologies, toolset refs and validator refs, plus four scalars, plus up
///   to `MAX_PARAMS` (32) template-instance bindings -- 5,156;
/// * a GRAPH pins, per node, two data contracts and either a decision
///   predicate or up to `MAX_BINDINGS` (64) template bindings, across
///   `MAX_NODES` (256) nodes, plus a condition on each of `MAX_EDGES` (1,024)
///   edges, plus its synthesis evidence -- 17,921, which is what raised this
///   bound from 8,192 when graph pins started resolving.
const MAX_RESOLVED_COMPONENT_PINS: usize = 32_768;
/// Most revision rows one publish may read while resolving its pins.
///
/// The second half of the cost bound: the pin count caps how many components
/// are looked up, this caps how deep each lookup may search when a pin names
/// something other than the component's HEAD.
const MAX_COMPONENT_PIN_RESOLUTION_ROWS: usize = 131_072;

/// What a committed graph write returns to the caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentComponentWriteResult {
    pub result: AgentComponentCommittedResult,
    pub replayed: bool,
}

impl AgentLibraryStore {
    /// Publish the next graph revision.
    pub fn publish_component(
        &self,
        request: AgentComponentPublishRequest,
    ) -> Result<AgentComponentWriteResult, String> {
        validate_context(self, &request.context)?;
        request.component.validate()?;
        if request.context.tenant_id != request.component.tenant_id {
            return Err(
                "agent component publish context tenant does not match the graph's tenant".to_string(),
            );
        }
        let expected_revision = request
            .context
            .expected_revision
            .ok_or_else(|| "agent component writes require an explicit expected_revision".to_string())?;
        let owner = self.scope_handle(&request.context.tenant_id)?;
        let txn = self.mutations.open_write(&owner)?;

        // Every early return past this point must abort the transaction: an
        // open write that is neither committed nor aborted holds the owner's
        // write lock for the life of the process.
        let nonce = match resolve_nonce_first(&self.mutations, &txn, &request.context) {
            Ok(nonce) => nonce,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        let next = match next_revision(expected_revision) {
            Ok(next) => next,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        let replay_context =
            match admitted_context(&request.context, "agent-component:publish") {
                Ok(context) => context,
                Err(error) => {
                    txn.abort()?;
                    return Err(error);
                }
            };
        let operation = match agent_library_operation_identity(
            &owner,
            &replay_context,
            "component-publish",
            &request.component.component_id,
            expected_revision,
            Some(&request.component.content_digest),
        ) {
            Ok(operation) => operation,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        let replay = match self.mutations.resolve_replay(&txn, &operation, &nonce) {
            Ok(replay) => replay,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        if let Some(result) = match replayed_component(replay, &request.component.component_id) {
            Ok(replayed) => replayed,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        } {
            self.mutations.commit_replay_receipt(txn)?;
            return Ok(result);
        }

        // L1 -> L1: a component's own `requires` are pinned references too, and
        // the same argument applies -- resolution is by (id, kind, digest)
        // alone, so an unresolved pin is a claim nothing checks. The MCP server
        // an ingested tool/prompt/resource is provenanced to is the SAME kind
        // of pin and is resolved in the same pass: `pinned_components()` is the
        // one list, so a second pin set cannot be forgotten here.
        //
        // The reference graph stays acyclic for the reason the module header
        // gives: a dependency can only pin a digest that already exists.
        // Resolution does not need a cycle check, it needs to prove the pin is
        // real.
        if let Err(error) = self.resolve_component_pins_in_write(
            &txn,
            &request.context.tenant_id,
            "agent component",
            &request.component.pinned_components(),
        ) {
            txn.abort()?;
            return Err(error);
        }

        let entry = match AgentComponentEntry::create(
            request.component,
            next,
            AgentLibraryLifecycle::Published,
            replay_context.created_at_ms,
            replay_context.created_at_ms,
        ) {
            Ok(entry) => entry,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        self.commit_component_in_write(
            txn,
            &owner,
            &replay_context,
            expected_revision,
            AgentComponentMutationKind::Publish,
            entry,
            &operation,
            &nonce,
        )
    }

    /// Retain the current graph revision as a durable tombstone.
    pub fn retire_component(
        &self,
        request: AgentComponentRetireRequest,
    ) -> Result<AgentComponentWriteResult, String> {
        validate_context(self, &request.context)?;
        let expected_revision = request
            .context
            .expected_revision
            .ok_or_else(|| "agent component writes require an explicit expected_revision".to_string())?;
        let owner = self.scope_handle(&request.context.tenant_id)?;
        let txn = self.mutations.open_write(&owner)?;
        let nonce = match resolve_nonce_first(&self.mutations, &txn, &request.context) {
            Ok(nonce) => nonce,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        let next = match next_revision(expected_revision) {
            Ok(next) => next,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        let replay_context =
            match admitted_context(&request.context, "agent-component:retire") {
                Ok(context) => context,
                Err(error) => {
                    txn.abort()?;
                    return Err(error);
                }
            };
        let operation = match agent_library_operation_identity(
            &owner,
            &replay_context,
            "component-retire",
            &request.component_id,
            expected_revision,
            None,
        ) {
            Ok(operation) => operation,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        let replay = match self.mutations.resolve_replay(&txn, &operation, &nonce) {
            Ok(replay) => replay,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        if let Some(result) = match replayed_component(replay, &request.component_id) {
            Ok(replayed) => replayed,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        } {
            self.mutations.commit_replay_receipt(txn)?;
            return Ok(result);
        }

        let current = match self.component_at_revision_in_write(
            &txn,
            &request.context.tenant_id,
            &request.component_id,
            expected_revision,
        ) {
            Ok(Some(current)) => current,
            Ok(None) => {
                txn.abort()?;
                return Err("agent component has no revision to retire".to_string());
            }
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        let tombstone = match current.retire(next, replay_context.created_at_ms) {
            Ok(tombstone) => tombstone,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        self.commit_component_in_write(
            txn,
            &owner,
            &replay_context,
            expected_revision,
            AgentComponentMutationKind::Retire,
            tombstone,
            &operation,
            &nonce,
        )
    }

    /// The head revision of one graph, or `None` if it was never published.
    pub fn current_component(
        &self,
        tenant_id: &str,
        component_id: &str,
    ) -> Result<Option<AgentComponentEntry>, String> {
        eg_types::agent_library::validate_key(tenant_id, component_id)?;
        let read = self.read()?;
        Ok(read_component_history(&read, tenant_id, component_id)?
            .1
            .into_iter()
            .last())
    }

    /// Every retained revision of one graph, oldest first.
    pub fn component_revisions(
        &self,
        tenant_id: &str,
        component_id: &str,
    ) -> Result<Vec<AgentComponentEntry>, String> {
        eg_types::agent_library::validate_key(tenant_id, component_id)?;
        let read = self.read()?;
        Ok(read_component_history(&read, tenant_id, component_id)?.1)
    }

    /// Resolve a prior attempt's durable outcome without re-committing it.
    pub fn component_status(
        &self,
        request: AgentComponentStatusRequest,
    ) -> Result<Option<AgentComponentWriteResult>, String> {
        validate_context(self, &request.context)?;
        eg_types::agent_library::validate_key(&request.context.tenant_id, &request.component_id)?;
        let owner = self.scope_handle(&request.context.tenant_id)?;
        let batch_id = batch_id(&request.context.idempotency_key)?;
        let read = self.kernel.read_scope(&owner)?;
        let Some(record) = eg_transaction::read_ledger(&read, &batch_id)? else {
            return Ok(None);
        };
        let Some(result_bytes) = record.result_msgpack.as_ref() else {
            return Err(
                "CORRUPT_MUTATION_LEDGER: agent component status has no typed result".to_string(),
            );
        };
        let committed = decode_committed_result(result_bytes)?;
        if committed.component.component_id != request.component_id {
            return Err(
                "CORRUPT_MUTATION_LEDGER: agent component status resolved a different graph"
                    .to_string(),
            );
        }
        Ok(Some(AgentComponentWriteResult {
            result: committed,
            replayed: true,
        }))
    }

    /// Find the published components that answer a capability search.
    ///
    /// The durable half of *"what does an agent trying to do XYZ need?"*: the
    /// request resolves a task to capabilities through EG's native ontology,
    /// and each component's classification is matched by subsumption.
    ///
    /// Only HEAD revisions are considered, and only published ones. A search is
    /// asking "what could I build with today", so a superseded revision or a
    /// withdrawn component is not an answer -- while both remain readable by
    /// id, because an existing agent that pinned one still needs to resolve it.
    ///
    /// # Three bounds, one page
    ///
    /// A page stops at whichever comes first: the caller's `limit`, the scan
    /// bound, or the byte bound. Each caps a different resource -- how much the
    /// caller asked for, how much of the corpus this call walks, and how large
    /// the response may get -- and none of them REFUSES. Whenever a page stops
    /// early it returns a cursor, so every one of the three is resumable and a
    /// large tenant is paged rather than cut off. That is the difference from
    /// the unpaginated form this replaces, where the single scan bound was a
    /// permanent cliff and the response size was bounded only by the corpus.
    ///
    /// A page can legitimately be EMPTY and still carry a cursor: the scan bound
    /// applies to rows examined, not rows matched. Callers loop until
    /// `next_cursor` is `None`.
    pub fn search_components(
        &self,
        request: &eg_types::agent_component::AgentComponentSearchRequest,
    ) -> Result<eg_types::agent_component::AgentComponentSearchPage, String> {
        request.validate()?;
        let resume_after = match &request.cursor {
            Some(cursor) => Some(eg_types::agent_component::decode_search_cursor(
                &request.tenant_id,
                cursor,
            )?),
            None => None,
        };
        let limit = request.page_limit();
        let read = self.read()?;
        let heads = read.open_owner_table(eg_storage::AGENT_COMPONENT_HEADS)?;
        let revisions = read.open_owner_table(eg_storage::AGENT_COMPONENT_REVISIONS)?;
        let mut matched = Vec::new();
        let mut scanned = 0usize;
        let mut bytes = 0usize;
        // The last row this page CONSUMED, so the cursor always resumes
        // strictly after a row the caller has already been shown. Every bound
        // is therefore checked BEFORE a row is consumed, never after.
        let mut last_consumed: Option<String> = None;
        let mut truncated = false;
        // A cursor only supplies the component half of the key; the tenant half
        // is always the request's, so paging cannot walk out of the tenant
        // prefix no matter what a caller hands back.
        let start = resume_after.as_deref().unwrap_or("");
        for row in heads
            .range((request.tenant_id.as_str(), start)..)
            .map_err(|error| error.to_string())?
        {
            let (key, head_revision) = row.map_err(|error| error.to_string())?;
            let (row_tenant, component_id) = key.value();
            // `range_from` is open-ended: without this the scan walks into the
            // NEXT tenant's components and would return them.
            if row_tenant != request.tenant_id {
                break;
            }
            // The cursor is EXCLUSIVE; the range start is inclusive.
            if resume_after.as_deref() == Some(component_id) {
                continue;
            }
            if matched.len() >= limit
                || scanned >= MAX_AGENT_COMPONENT_SEARCH_SCAN
                || (scanned > 0 && bytes >= MAX_AGENT_COMPONENT_SEARCH_BYTES)
            {
                truncated = true;
                break;
            }
            scanned += 1;
            let Some(value) = revisions
                .get((row_tenant, component_id, head_revision.value()))
                .map_err(|error| error.to_string())?
            else {
                return Err("agent component head points to a missing revision".to_string());
            };
            bytes = bytes.saturating_add(value.value().len());
            let entry = decode_component(value.value())?;
            if request.matches(&entry) {
                matched.push(entry);
            }
            last_consumed = Some(component_id.to_string());
        }
        let next_cursor = if truncated {
            last_consumed.map(|component_id| {
                eg_types::agent_component::encode_search_cursor(
                    &request.tenant_id,
                    &component_id,
                )
            })
        } else {
            None
        };
        Ok(eg_types::agent_component::AgentComponentSearchPage {
            entries: matched,
            next_cursor,
        })
    }

    /// Resolve every pinned L1 component reference, inside the write
    /// transaction that is admitting the record which pins them.
    ///
    /// # Why this exists
    ///
    /// A pinned reference is resolved by `(component_id, kind,
    /// definition_digest)` alone. Without this, a caller who merely KNOWS a
    /// digest -- or invents 64 hex characters -- publishes a record claiming a
    /// component it was never granted, and every downstream reader is told the
    /// claim is legitimate. RF-ADR-008 gives exactly that justification for the
    /// one edge it originally built (graph composition); it applies verbatim
    /// here, and without it L1 has no production reader at all: the layer whose
    /// purpose is to make "which agents use this tool?" a traversal is
    /// write-only.
    ///
    /// # Why INSIDE the transaction
    ///
    /// Same reason `validate_composition` resolves inside the graph publish
    /// txn: resolving from a separate read could admit a record whose
    /// dependency was retired between the two reads.
    ///
    /// # What is checked, per pin
    ///
    /// It must exist in THIS tenant; its kind must be the kind the pin names
    /// (and local validation has already checked that kind against the slot);
    /// a retained revision must carry the exact pinned `definition_digest`; and
    /// the component's HEAD must not be retired -- a retired component stays
    /// RESOLVABLE, so records that already pin it keep working, but nothing new
    /// may be built on something withdrawn. That is the same
    /// retained-but-not-buildable rule composition already applies to graphs.
    pub(super) fn resolve_component_pins_in_write(
        &self,
        write: &eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
        tenant_id: &str,
        subject: &str,
        pins: &[&eg_types::agent_component::ComponentDependency],
    ) -> Result<(), String> {
        // Deduplicated: an agent that pins the same prompt component from two
        // slots should cost one lookup, not two, and the fan-out bound should
        // count what is actually resolved.
        let mut distinct: std::collections::BTreeSet<(&str, &str, &str)> =
            std::collections::BTreeSet::new();
        for pin in pins {
            distinct.insert((
                pin.component_id.as_str(),
                pin.kind.as_str(),
                pin.definition_digest.as_str(),
            ));
        }
        if distinct.len() > MAX_RESOLVED_COMPONENT_PINS {
            return Err(format!(
                "{subject} pins more than {MAX_RESOLVED_COMPONENT_PINS} distinct components"
            ));
        }
        let heads = write.open_read_table(eg_storage::AGENT_COMPONENT_HEADS)?;
        let revisions = write.open_read_table(eg_storage::AGENT_COMPONENT_REVISIONS)?;
        let mut rows = 0usize;
        for (component_id, kind, definition_digest) in distinct {
            let Some(head_revision) = heads.get((tenant_id, component_id))?.map(|v| v.value())
            else {
                return Err(format!(
                    "{subject} pins component '{component_id}', which does not exist in this tenant"
                ));
            };
            rows += 1;
            let head = revisions
                .get((tenant_id, component_id, head_revision))?
                .ok_or_else(|| "agent component head points to a missing revision".to_string())?;
            let head = decode_component(head.value())?;
            if head.lifecycle == AgentLibraryLifecycle::Retired {
                return Err(format!(
                    "{subject} pins component '{component_id}', which is retired"
                ));
            }
            // The HEAD first: the overwhelmingly common pin is the current
            // revision, and hitting it turns the whole resolution into one read.
            //
            // The fallback scan reuses the table handle opened above rather than
            // opening its own: redb refuses a second open of the same table
            // while the first handle is alive, so a helper that opened it again
            // turned every non-HEAD pin into a transaction error.
            let resolved = if head.definition_digest == definition_digest {
                Some(head)
            } else {
                let mut found = None;
                let mut scanned = 0usize;
                for row in revisions.range_from((tenant_id, component_id, 0))? {
                    let (key, value) = row.map_err(|error| error.to_string())?;
                    let (row_tenant, row_component, _) = key.value();
                    // `range_from` is open-ended, so the prefix has to be
                    // re-checked per row: without the break this walks into the
                    // NEXT component's revisions and could resolve a digest
                    // belonging to a different one.
                    if row_tenant != tenant_id || row_component != component_id {
                        break;
                    }
                    scanned += 1;
                    rows += 1;
                    if scanned > MAX_AGENT_COMPONENT_REVISIONS {
                        return Err(
                            "agent component history exceeds its retained revision bound"
                                .to_string(),
                        );
                    }
                    if rows > MAX_COMPONENT_PIN_RESOLUTION_ROWS {
                        return Err(format!(
                            "{subject} reference resolution exceeds its \
                             {MAX_COMPONENT_PIN_RESOLUTION_ROWS}-row bound"
                        ));
                    }
                    let entry = decode_component(value.value())?;
                    if entry.definition_digest == definition_digest {
                        found = Some(entry);
                        break;
                    }
                }
                found
            };
            let Some(resolved) = resolved else {
                return Err(format!(
                    "{subject} pins a revision of component '{component_id}' that was never \
                     published: no retained revision matches the pinned definition digest"
                ));
            };
            // Belt and braces against a row that does not match its physical
            // key: the scan is already tenant-prefixed.
            if resolved.tenant_id != tenant_id {
                return Err(format!(
                    "{subject} pins component '{component_id}', which belongs to another tenant"
                ));
            }
            if resolved.kind.as_str() != kind {
                return Err(format!(
                    "{subject} pins component '{component_id}' as a {kind}, but it is a {}",
                    resolved.kind.as_str()
                ));
            }
            if rows > MAX_COMPONENT_PIN_RESOLUTION_ROWS {
                return Err(format!(
                    "{subject} reference resolution exceeds its {MAX_COMPONENT_PIN_RESOLUTION_ROWS}\
                     -row bound"
                ));
            }
        }
        Ok(())
    }

    fn component_at_revision_in_write(
        &self,
        write: &eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
        tenant_id: &str,
        component_id: &str,
        revision: u64,
    ) -> Result<Option<AgentComponentEntry>, String> {
        if revision == 0 {
            return Ok(None);
        }
        let revisions = write.open_read_table(eg_storage::AGENT_COMPONENT_REVISIONS)?;
        let Some(value) = revisions.get((tenant_id, component_id, revision))? else {
            return Ok(None);
        };
        let entry = decode_component(value.value())?;
        if entry.tenant_id != tenant_id
            || entry.component_id != component_id
            || entry.entry_revision != revision
        {
            return Err(
                "CORRUPT_MUTATION_LEDGER: agent component row does not match its physical key"
                    .to_string(),
            );
        }
        Ok(Some(entry))
    }

    #[allow(clippy::too_many_arguments)]
    fn commit_component_in_write(
        &self,
        txn: eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
        owner: &OwnedStoreHandle<eg_storage::AgentLibraryOwner>,
        context: &AgentLibraryMutationContext,
        expected_revision: u64,
        kind: AgentComponentMutationKind,
        entry: AgentComponentEntry,
        operation: &eg_types::authority::OperationReplayIdentity,
        nonce: &eg_types::authority::NonceReplayKey,
    ) -> Result<AgentComponentWriteResult, String> {
        let operations = component_operations(kind, &entry);
        let policy_digest = effective_agent_library_policy_digest(&operations)?;
        let mut admitted = context.clone();
        admitted.policy_digest = format!("sha256:{}", policy_digest.to_hex());

        let event = AgentComponentOutboxEvent {
            schema_version: AGENT_COMPONENT_SCHEMA_VERSION,
            kind,
            component: entry.clone(),
            performing_actor: admitted.caller_principal.clone(),
            action_actor_scope: admitted.actor_scope.clone(),
        };
        event.validate()?;
        let event_bytes = eg_storage::encode_bounded(&event, "agent component outbox event")?;
        let entry_bytes = eg_storage::encode_bounded(&entry, "agent component revision")?;
        let batch_id = batch_id(&admitted.idempotency_key)?;

        let authoritative_version = match self.mutations.current_version(&txn, owner) {
            Ok(version) => version,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        let batch = match build_component_batch(
            owner,
            &admitted,
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
                return Err("agent component admission has no native source version".to_string());
            }
            Begin::Replay(_) => {
                txn.abort()?;
                return Err("CORRUPT_MUTATION_LEDGER: replay became visible after a fresh admission decision".to_string());
            }
        };
        if source_version != authoritative_version {
            txn.abort()?;
            return Err("agent component source version changed while admitting write".to_string());
        }
        let committed_version = match source_version.checked_add(1) {
            Some(version) => version,
            None => {
                txn.abort()?;
                return Err("agent component committed version overflow".to_string());
            }
        };
        let stable_result = AgentComponentCommittedResult {
            schema_version: AGENT_COMPONENT_SCHEMA_VERSION,
            component: entry.clone(),
            batch_id: batch_id.clone(),
            committed_version,
        };
        let result_bytes = match encode_component_domain_result(&stable_result) {
            Ok(bytes) => bytes,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };

        let owner_write_result: Result<(), String> = (|| {
            let owner_write = txn.owner_rows(owner, &batch)?;
            apply_component_rows(
                &owner_write,
                &admitted,
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

        let key = format!(
            "{}:{}:{}",
            entry.tenant_id, entry.component_id, entry.entry_revision
        );
        let headers = component_outbox_headers(&entry);
        let receipt = match owner_receipt(
            operation,
            nonce,
            &batch,
            "agent-component",
            AGENT_COMPONENT_OUTBOX_TOPIC,
            &key,
            &event_bytes,
            &headers,
            component_domain_result(&stable_result)?,
            committed_version,
            admitted.created_at_ms,
        ) {
            Ok(receipt) => receipt,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        let record = match self.mutations.finish_with_replay(
            &txn,
            &batch,
            Some(result_bytes),
            admitted.created_at_ms,
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
            .ok_or_else(|| "agent component commit has no target version".to_string())?;
        if recorded_version != committed_version {
            txn.abort()?;
            return Err("agent component result version differs from the committed version".to_string());
        }
        self.mutations.commit(txn, &batch)?;
        Ok(AgentComponentWriteResult {
            result: stable_result,
            replayed: false,
        })
    }
}

/// Head CAS plus the append-only revision row, in the graph tables.
fn apply_component_rows(
    owner_write: &AdmittedOwnerWrite<'_, eg_storage::AgentLibraryOwner>,
    context: &AgentLibraryMutationContext,
    expected_revision: u64,
    entry: &AgentComponentEntry,
    entry_bytes: &[u8],
) -> Result<(), String> {
    let mut heads = owner_write.open_table(eg_storage::AGENT_COMPONENT_HEADS)?;
    let actual_revision = heads
        .get((context.tenant_id.as_str(), entry.component_id.as_str()))
        .map_err(|error| error.to_string())?
        .map(|value| value.value())
        .unwrap_or(0);
    require_expected_revision(expected_revision, actual_revision)?;
    if entry.entry_revision != next_revision(expected_revision)? {
        return Err("agent component entry revision does not follow its expected head".to_string());
    }
    let mut revisions = owner_write.open_table(eg_storage::AGENT_COMPONENT_REVISIONS)?;
    if actual_revision > 0 {
        let current = revisions
            .get((
                context.tenant_id.as_str(),
                entry.component_id.as_str(),
                actual_revision,
            ))
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "agent component head points to a missing revision".to_string())?;
        let current = decode_component(current.value())?;
        if current.lifecycle == AgentLibraryLifecycle::Retired {
            return Err("retired agent components cannot be resurrected".to_string());
        }
    }
    if revisions
        .get((
            context.tenant_id.as_str(),
            entry.component_id.as_str(),
            entry.entry_revision,
        ))
        .map_err(|error| error.to_string())?
        .is_some()
    {
        return Err("agent component revision already exists".to_string());
    }
    revisions
        .insert(
            (
                context.tenant_id.as_str(),
                entry.component_id.as_str(),
                entry.entry_revision,
            ),
            entry_bytes,
        )
        .map_err(|error| error.to_string())?;
    heads
        .insert(
            (context.tenant_id.as_str(), entry.component_id.as_str()),
            entry.entry_revision,
        )
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn component_operations(
    kind: AgentComponentMutationKind,
    entry: &AgentComponentEntry,
) -> Vec<MutationOperation> {
    let event_type = match kind {
        AgentComponentMutationKind::Publish => "agent_component_publish",
        AgentComponentMutationKind::Retire => "agent_component_retire",
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

fn component_outbox_headers(entry: &AgentComponentEntry) -> BTreeMap<String, String> {
    BTreeMap::from([
        (
            "schema_version".to_string(),
            AGENT_COMPONENT_SCHEMA_VERSION.to_string(),
        ),
        ("tenant_id".to_string(), entry.tenant_id.clone()),
        ("component_id".to_string(), entry.component_id.clone()),
        (
            "entry_revision".to_string(),
            entry.entry_revision.to_string(),
        ),
        ("definition_digest".to_string(), entry.definition_digest.clone()),
        ("definition_actor_scope".to_string(), entry.actor_scope.clone()),
        ("definition_purpose_id".to_string(), entry.purpose_id.clone()),
        (
            "definition_policy_digest".to_string(),
            entry.policy_digest.clone(),
        ),
    ])
}

fn build_component_batch(
    owner: &OwnedStoreHandle<eg_storage::AgentLibraryOwner>,
    context: &AgentLibraryMutationContext,
    kind: AgentComponentMutationKind,
    entry: &AgentComponentEntry,
    version: u64,
    batch_id: &str,
    event_bytes: Vec<u8>,
) -> Result<MutationBatch, String> {
    let key = format!(
        "{}:{}:{}",
        entry.tenant_id, entry.component_id, entry.entry_revision
    );
    let operations = component_operations(kind, entry);
    let outbox = vec![MutationOutboxIntent {
        topic: AGENT_COMPONENT_OUTBOX_TOPIC.to_string(),
        key,
        payload: event_bytes,
        headers: component_outbox_headers(entry),
    }];
    // The envelope is minted from the batch's FINAL operations and outbox --
    // `MutationBatch::validate` compares its canonical payload digest against
    // exactly these, so building it from anything earlier fails closed with
    // "mutation batch content does not match its envelope's canonical payload
    // digest".
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
    compiled_envelope.policy_digest =
        super::agent_library::parse_prefixed_digest(&context.policy_digest)?;
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

fn read_component_history(
    read: &ScopedRead<'_, eg_storage::AgentLibraryOwner>,
    tenant_id: &str,
    component_id: &str,
) -> Result<(Option<u64>, Vec<AgentComponentEntry>), String> {
    let head = read
        .open_owner_table(eg_storage::AGENT_COMPONENT_HEADS)?
        .get((tenant_id, component_id))
        .map_err(|error| error.to_string())?
        .map(|value| value.value());
    let table = read.open_owner_table(eg_storage::AGENT_COMPONENT_REVISIONS)?;
    let mut entries = Vec::new();
    let mut bytes = 0usize;
    for row in table
        .range((tenant_id, component_id, 0)..=(tenant_id, component_id, u64::MAX))
        .map_err(|error| error.to_string())?
    {
        let (key, value) = row.map_err(|error| error.to_string())?;
        // Bounded: a caller can otherwise ask for an unbounded amount of work
        // by publishing revisions.
        if entries.len() >= MAX_AGENT_COMPONENT_REVISIONS {
            return Err("agent component history exceeds its retained revision bound".to_string());
        }
        bytes = bytes.saturating_add(value.value().len());
        if bytes > MAX_AGENT_COMPONENT_HISTORY_BYTES {
            return Err("agent component history exceeds its retained byte bound".to_string());
        }
        let (row_tenant, row_graph, row_revision) = key.value();
        let entry = decode_component(value.value())?;
        if entry.tenant_id != row_tenant
            || entry.component_id != row_graph
            || entry.entry_revision != row_revision
        {
            return Err(
                "CORRUPT_MUTATION_LEDGER: agent component row does not match its physical key"
                    .to_string(),
            );
        }
        entries.push(entry);
    }
    Ok((head, entries))
}

fn decode_component(bytes: &[u8]) -> Result<AgentComponentEntry, String> {
    let entry: AgentComponentEntry = super::agent_row::decode(bytes, "agent component row")?;
    entry.validate()?;
    Ok(entry)
}

fn decode_committed_result(bytes: &[u8]) -> Result<AgentComponentCommittedResult, String> {
    let result: AgentComponentCommittedResult =
        super::agent_row::decode(bytes, "agent component result")?;
    result.component.validate()?;
    Ok(result)
}

fn component_domain_result(result: &AgentComponentCommittedResult) -> Result<MutationResult, String> {
    super::agent_row::domain_result(result, AGENT_COMPONENT_RESULT_SCHEMA_ID, "agent component")
}

fn encode_component_domain_result(result: &AgentComponentCommittedResult) -> Result<Vec<u8>, String> {
    eg_storage::encode_bounded(&component_domain_result(result)?, "agent component domain result")
}

/// Turn a resolved replay into a caller result, or `None` when it is fresh.
fn replayed_component(
    replay: ReplayResolution,
    component_id: &str,
) -> Result<Option<AgentComponentWriteResult>, String> {
    let recorded = match replay {
        ReplayResolution::Fresh => return Ok(None),
        ReplayResolution::NonceRejected { idempotency_key } => {
            return Err(format!(
                "REPLAY_NONCE_CONSUMED: attempt nonce already consumed by '{idempotency_key}'"
            ));
        }
        ReplayResolution::Conflict { .. } => {
            return Err(
                "IDEMPOTENCY_CONFLICT: key was already used by a different agent component mutation"
                    .to_string(),
            );
        }
        ReplayResolution::ReplayedResult(recorded) => *recorded,
    };
    let RecordedOperation::Receipt(receipt) = recorded else {
        return Err(
            "CORRUPT_MUTATION_LEDGER: agent component replay is missing its typed receipt".to_string(),
        );
    };
    let receipt = *receipt;
    receipt.validate()?;
    let MutationResult::DomainResult { payload, .. } = &receipt.result else {
        return Err(
            "CORRUPT_MUTATION_LEDGER: agent component replay receipt carries no domain result"
                .to_string(),
        );
    };
    let committed = decode_committed_result(payload.as_slice())?;
    if committed.component.component_id != component_id {
        return Err(
            "CORRUPT_MUTATION_LEDGER: agent component replay resolved a different graph".to_string(),
        );
    }
    Ok(Some(AgentComponentWriteResult {
        result: committed,
        replayed: true,
    }))
}

/// The shared `Arc` type the server state holds. Re-exported so the handler does
/// not have to name the library store to reach the graph surface.
pub type AgentComponentStoreRef = Arc<AgentLibraryStore>;

/// Publish one L1 component and return the pin that resolves it.
///
/// Shared by every test module that has to build an agent, a template instance
/// or a graph: publishing now RESOLVES each pinned component, so a fixture can
/// no longer invent a digest -- it has to be the component's real
/// `definition_digest`. One definition, so the four modules cannot drift.
///
/// Idempotent per store, and deterministic: a component's digest is a hash over
/// its content alone -- no revision, no timestamp -- so seeding the same
/// component into several stores yields byte-identical pins.
#[cfg(test)]
pub(crate) fn seed_component_for_test(
    store: &AgentLibraryStore,
    tenant_id: &str,
    component_id: &str,
    kind: eg_types::agent_component::AgentComponentKind,
    nonce_index: u8,
) -> eg_types::agent_component::ComponentDependency {
    use eg_types::agent_component::{
        AgentComponentDraft, AgentComponentFacts, AgentComponentKind, ComponentProvenance,
        PromptMode, ToolEffect,
    };
    let definition_digest = match store
        .current_component(tenant_id, component_id)
        .expect("read a seeded component")
    {
        Some(existing) => existing.definition_digest,
        None => {
            let facts = match kind {
                AgentComponentKind::SystemPrompt => AgentComponentFacts::SystemPrompt {
                    prompt_mode: PromptMode::Static,
                    token_estimate: 128,
                    variables: Vec::new(),
                },
                AgentComponentKind::Tool => AgentComponentFacts::Tool {
                    effect: ToolEffect::Read,
                    required_scopes: Vec::new(),
                },
                AgentComponentKind::Toolset => AgentComponentFacts::Toolset {
                    transport: eg_types::agent_component::ToolsetTransport::Function,
                },
                AgentComponentKind::ModelProfile => AgentComponentFacts::ModelProfile {
                    provider: "seed-provider".to_string(),
                    model_identity: "model:seed".to_string(),
                    context_window_tokens: 8_192,
                    max_output_tokens: 1_024,
                    supports_tools: true,
                    supports_structured_output: true,
                    supports_vision: false,
                },
                _ => AgentComponentFacts::Opaque,
            };
            // A seeding nonce can never collide with a test's own: every test
            // module builds its nonces as `[n; 32]` for a small `n`.
            let mut nonce_bytes = [0xEEu8; 32];
            nonce_bytes[0] = 0xA0u8.wrapping_add(nonce_index);
            let policy_digest =
                super::agent_library::current_agent_library_policy_digest().unwrap();
            store
                .publish_component(AgentComponentPublishRequest {
                    context: AgentLibraryMutationContext {
                        request_id: 80_000 + u64::from(nonce_index),
                        principal: store.owner_principal().to_string(),
                        caller_principal: format!("principal:sha256:{}", "a".repeat(64)),
                        attempt_nonce: eg_types::contract::Nonce::from_bytes(nonce_bytes),
                        tenant_id: tenant_id.to_string(),
                        actor_scope: "action-scope:component-seed".to_string(),
                        purpose_id: "agent-component:publish".to_string(),
                        policy_revision: "policy-v1".to_string(),
                        policy_digest: policy_digest.clone(),
                        policy_decision_id: "agent-component:decision:policy-v1".to_string(),
                        idempotency_key: format!("component-seed:{tenant_id}:{component_id}"),
                        expected_revision: Some(0),
                        trace_id: None,
                        created_at_ms: 5,
                    },
                    component: AgentComponentDraft {
                        component_id: component_id.to_string(),
                        kind,
                        version: "1.0.0".to_string(),
                        content_digest: format!("sha256:{}", "1".repeat(64)),
                        content_ref: None,
                        facts,
                        provenance: ComponentProvenance::Native,
                        summary: format!("seeded {component_id}"),
                        classification: Vec::new(),
                        requires: Vec::new(),
                        provides: Vec::new(),
                        attributes: Default::default(),
                        tenant_id: tenant_id.to_string(),
                        actor_scope: "action-scope:component-seed".to_string(),
                        purpose_id: "agent-component:publish".to_string(),
                        policy_digest,
                        source_revision: "component-seed:1".to_string(),
                        source_revision_digest: format!("sha256:{}", "8".repeat(64)),
                    },
                })
                .expect("the fixture's component publishes")
                .result
                .component
                .definition_digest
        }
    };
    eg_types::agent_component::ComponentDependency {
        component_id: component_id.to_string(),
        kind,
        definition_digest,
    }
}

/// Seed every component an agent draft pins and rewrite each pin to the real
/// digest.
#[cfg(test)]
pub(crate) fn seed_draft_components_for_test(
    store: &AgentLibraryStore,
    draft: &mut eg_types::agent_library::AgentLibraryEntryDraft,
    nonce_base: u8,
) {
    let tenant_id = draft.tenant_id.clone();
    let mut pins: Vec<&mut eg_types::agent_component::ComponentDependency> =
        vec![&mut draft.system_prompt, &mut draft.model_profile];
    pins.extend(draft.tools.iter_mut());
    pins.extend(draft.skills.iter_mut());
    pins.extend(draft.ontologies.iter_mut());
    pins.extend(draft.runtime.toolset_refs.iter_mut());
    pins.extend(draft.runtime.output_validator_refs.iter_mut());
    pins.extend(draft.runtime.deps_contract.iter_mut());
    pins.extend(draft.runtime.output_contract.iter_mut());
    // The values a template instantiation bound into this agent are pinned
    // components too, and admission resolves them with the rest -- so a fixture
    // that left them at an invented digest would be refused.
    pins.extend(
        draft
            .instantiated_from
            .iter_mut()
            .flat_map(|instance| instance.bindings.values_mut()),
    );
    for (index, pin) in pins.into_iter().enumerate() {
        let seeded = seed_component_for_test(
            store,
            &tenant_id,
            &pin.component_id,
            pin.kind,
            nonce_base.wrapping_add(u8::try_from(index).unwrap_or(0)),
        );
        pin.definition_digest = seeded.definition_digest;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eg_types::agent_component::{
        AgentComponentDraft, AgentComponentFacts, AgentComponentKind, AgentComponentSearchRequest,
        ComponentDependency, ComponentProvenance, ToolEffect,
    };
    use eg_types::contract::Nonce;

    fn digest(byte: char) -> String {
        format!("sha256:{}", byte.to_string().repeat(64))
    }

    /// The MCP server every ingested `tool()` fixture is provenanced to.
    ///
    /// Publishing a tool RESOLVES that provenance pin, so the server has to
    /// exist at the pinned revision before any tool in this module can
    /// publish. That is the point of the pin, and the fixtures seed it rather
    /// than the assertions being relaxed to tolerate a dangling one.
    const MCP_SERVER_ID: &str = "mcp:search-server";
    /// Attempt-nonce space reserved for the fixture seed, above every nonce a
    /// test picks for itself, so seeding can never consume a test's nonce.
    const SEED_NONCE: u8 = 250;

    /// The server record a tool's provenance points at. Native provenance: an
    /// MCP server cannot itself be provenanced to one.
    fn mcp_server_draft(tenant_id: &str) -> AgentComponentDraft {
        AgentComponentDraft {
            component_id: MCP_SERVER_ID.to_string(),
            kind: AgentComponentKind::McpServer,
            version: "1.0.0".to_string(),
            content_digest: digest('1'),
            content_ref: None,
            facts: AgentComponentFacts::Opaque,
            provenance: ComponentProvenance::Native,
            summary: "the search mcp server".to_string(),
            // Deliberately unclassified: every task-constrained search in this
            // module asserts on which TOOLS it finds, and a seeded server that
            // matched a task would change those answers.
            classification: Vec::new(),
            requires: Vec::new(),
            provides: Vec::new(),
            attributes: Default::default(),
            tenant_id: tenant_id.to_string(),
            actor_scope: "action-scope:a".to_string(),
            purpose_id: "agent-component:publish".to_string(),
            policy_digest: super::super::agent_library::current_agent_library_policy_digest()
                .unwrap(),
            source_revision: "rev-1".to_string(),
            source_revision_digest: digest('8'),
        }
    }

    /// The digest a tool's provenance must pin. Derived from the same draft the
    /// seed publishes, so the two cannot drift: a fixture that pinned a
    /// hand-written digest would be refused by resolution, correctly.
    fn mcp_server_digest(tenant_id: &str) -> String {
        AgentComponentEntry::publish(mcp_server_draft(tenant_id), 1, 10)
            .expect("the fixture server is a valid draft")
            .definition_digest
    }

    fn seed_mcp_server(store: &AgentLibraryStore, tenant_id: &str, nonce: u8) {
        store
            .publish_component(AgentComponentPublishRequest {
                context: context_for(
                    tenant_id,
                    store,
                    &format!("{tenant_id}-mcp-server-seed"),
                    nonce,
                    0,
                    "agent-component:publish",
                ),
                component: mcp_server_draft(tenant_id),
            })
            .expect("the fixture mcp server seeds");
    }

    fn tool(component_id: &str, capability: &str, effect: ToolEffect) -> AgentComponentDraft {
        tool_for("tenant-a", component_id, capability, effect)
    }

    fn tool_for(
        tenant_id: &str,
        component_id: &str,
        capability: &str,
        effect: ToolEffect,
    ) -> AgentComponentDraft {
        AgentComponentDraft {
            component_id: component_id.to_string(),
            kind: AgentComponentKind::Tool,
            version: "1.0.0".to_string(),
            content_digest: digest('1'),
            content_ref: None,
            facts: AgentComponentFacts::Tool {
                effect,
                required_scopes: Vec::new(),
            },
            provenance: ComponentProvenance::McpServer {
                server: ComponentDependency {
                    component_id: MCP_SERVER_ID.to_string(),
                    kind: AgentComponentKind::McpServer,
                    definition_digest: mcp_server_digest(tenant_id),
                },
                upstream_name: component_id.to_string(),
            },
            summary: format!("tool {component_id}"),
            classification: vec![capability.to_string()],
            requires: Vec::new(),
            provides: Vec::new(),
            attributes: Default::default(),
            tenant_id: tenant_id.to_string(),
            actor_scope: "action-scope:a".to_string(),
            purpose_id: "agent-component:publish".to_string(),
            policy_digest: super::super::agent_library::current_agent_library_policy_digest()
                .unwrap(),
            source_revision: "rev-1".to_string(),
            source_revision_digest: digest('8'),
        }
    }

    fn context(
        store: &AgentLibraryStore,
        key: &str,
        nonce: u8,
        expected_revision: u64,
        purpose_id: &str,
    ) -> AgentLibraryMutationContext {
        context_for("tenant-a", store, key, nonce, expected_revision, purpose_id)
    }

    #[allow(clippy::too_many_arguments)]
    fn context_for(
        tenant_id: &str,
        store: &AgentLibraryStore,
        key: &str,
        nonce: u8,
        expected_revision: u64,
        purpose_id: &str,
    ) -> AgentLibraryMutationContext {
        AgentLibraryMutationContext {
            request_id: u64::from(nonce),
            principal: store.owner_principal().to_string(),
            caller_principal: format!("principal:sha256:{}", "a".repeat(64)),
            attempt_nonce: Nonce::from_bytes([nonce; 32]),
            tenant_id: tenant_id.to_string(),
            actor_scope: "action-scope:a".to_string(),
            purpose_id: purpose_id.to_string(),
            policy_revision: "policy-v1".to_string(),
            policy_digest: super::super::agent_library::current_agent_library_policy_digest()
                .unwrap(),
            policy_decision_id: "agent-component:decision:policy-v1".to_string(),
            idempotency_key: key.to_string(),
            expected_revision: Some(expected_revision),
            trace_id: None,
            created_at_ms: 10,
        }
    }

    fn open_store() -> (tempfile::TempDir, AgentLibraryStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = AgentLibraryStore::open(dir.path().to_str().unwrap()).unwrap();
        seed_mcp_server(&store, "tenant-a", SEED_NONCE);
        (dir, store)
    }

    #[test]
    fn a_published_component_is_durable_and_reads_back() {
        let (_dir, store) = open_store();
        let published = store
            .publish_component(AgentComponentPublishRequest {
                context: context(&store, "key-1", 1, 0, "agent-component:publish"),
                component: tool(
                    "tool:web-search",
                    "eg:capability/retrieval/web-search",
                    ToolEffect::Read,
                ),
            })
            .unwrap();
        assert!(!published.replayed);
        assert_eq!(published.result.component.entry_revision, 1);

        let current = store
            .current_component("tenant-a", "tool:web-search")
            .unwrap()
            .unwrap();
        assert_eq!(current, published.result.component);
    }

    #[test]
    fn a_byte_identical_retry_replays_rather_than_publishing_twice() {
        let (_dir, store) = open_store();
        let first = store
            .publish_component(AgentComponentPublishRequest {
                context: context(&store, "key-1", 1, 0, "agent-component:publish"),
                component: tool("tool:a", "eg:capability/retrieval/web-search", ToolEffect::Read),
            })
            .unwrap();
        let mut retry = context(&store, "key-1", 2, 0, "agent-component:publish");
        retry.created_at_ms = 99;
        let replayed = store
            .publish_component(AgentComponentPublishRequest {
                context: retry,
                component: tool("tool:a", "eg:capability/retrieval/web-search", ToolEffect::Read),
            })
            .unwrap();
        assert!(replayed.replayed);
        assert_eq!(replayed.result, first.result);
        assert_eq!(
            store.component_revisions("tenant-a", "tool:a").unwrap().len(),
            1
        );
    }

    #[test]
    fn the_three_layers_share_one_owner_without_colliding() {
        // Components, agents and graphs all live in `agent_library.redb`. If any
        // pair shared a key space, publishing one would overwrite another.
        let (_dir, store) = open_store();
        store
            .publish_component(AgentComponentPublishRequest {
                context: context(&store, "key-1", 1, 0, "agent-component:publish"),
                component: tool("same-id", "eg:capability/retrieval/web-search", ToolEffect::Read),
            })
            .unwrap();
        assert!(store.current_component("tenant-a", "same-id").unwrap().is_some());
        assert!(store.current("tenant-a", "same-id").unwrap().is_none());
        assert!(store.current_graph("tenant-a", "same-id").unwrap().is_none());
    }

    #[test]
    fn the_store_reopens_after_a_component_commit() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        {
            let store = AgentLibraryStore::open(path).unwrap();
            seed_mcp_server(&store, "tenant-a", SEED_NONCE);
            store
                .publish_component(AgentComponentPublishRequest {
                    context: context(&store, "key-1", 1, 0, "agent-component:publish"),
                    component: tool(
                        "tool:a",
                        "eg:capability/retrieval/web-search",
                        ToolEffect::Read,
                    ),
                })
                .unwrap();
        }
        let reopened = AgentLibraryStore::open(path).expect("owner reopens");
        assert!(reopened
            .current_component("tenant-a", "tool:a")
            .unwrap()
            .is_some());
    }

    // ---- provenance is a PIN, and admission resolves it ----

    #[test]
    fn a_component_provenanced_to_a_server_that_does_not_exist_is_refused() {
        // `server_component_id` used to be a bare id, so no check could tell an
        // ingest that named the reviewed server from one that named anything
        // at all. It is a `ComponentDependency` now, resolved like every other.
        let (_dir, store) = open_store();
        let mut orphan = tool("tool:web", "eg:capability/retrieval/web-search", ToolEffect::Read);
        orphan.provenance = ComponentProvenance::McpServer {
            server: ComponentDependency {
                component_id: "mcp:ghost-server".to_string(),
                kind: AgentComponentKind::McpServer,
                definition_digest: mcp_server_digest("tenant-a"),
            },
            upstream_name: "search".to_string(),
        };
        let error = store
            .publish_component(AgentComponentPublishRequest {
                context: context(&store, "key-1", 1, 0, "agent-component:publish"),
                component: orphan,
            })
            .expect_err("an unresolvable provenance pin must be refused");
        assert!(error.contains("which does not exist in this tenant"), "got: {error}");
        assert!(store.current_component("tenant-a", "tool:web").unwrap().is_none());
    }

    #[test]
    fn a_component_provenanced_to_an_invented_server_digest_is_refused() {
        // The half an id alone could never carry: WHICH revision of the server
        // this tool's surface was read from.
        let (_dir, store) = open_store();
        let mut stale = tool("tool:web", "eg:capability/retrieval/web-search", ToolEffect::Read);
        stale.provenance = ComponentProvenance::McpServer {
            server: ComponentDependency {
                component_id: MCP_SERVER_ID.to_string(),
                kind: AgentComponentKind::McpServer,
                definition_digest: digest('7'),
            },
            upstream_name: "search".to_string(),
        };
        let error = store
            .publish_component(AgentComponentPublishRequest {
                context: context(&store, "key-1", 1, 0, "agent-component:publish"),
                component: stale,
            })
            .expect_err("a digest no server revision carries must be refused");
        assert!(error.contains("that was never published"), "got: {error}");
    }

    #[test]
    fn a_component_provenanced_to_a_record_that_is_not_a_server_is_refused() {
        // The kind travels inside the pin, so admission refuses a provenance
        // that names a real record of the wrong kind rather than discovering it
        // when something tries to call the server.
        let (_dir, store) = open_store();
        let toolset = seed_component_for_test(
            &store,
            "tenant-a",
            "toolset:search",
            AgentComponentKind::Toolset,
            60,
        );
        let mut mislabelled =
            tool("tool:web", "eg:capability/retrieval/web-search", ToolEffect::Read);
        mislabelled.provenance = ComponentProvenance::McpServer {
            server: ComponentDependency {
                component_id: toolset.component_id,
                // The pin CLAIMS McpServer -- local validation is satisfied --
                // while the record it names is a toolset.
                kind: AgentComponentKind::McpServer,
                definition_digest: toolset.definition_digest,
            },
            upstream_name: "search".to_string(),
        };
        let error = store
            .publish_component(AgentComponentPublishRequest {
                context: context(&store, "key-1", 1, 0, "agent-component:publish"),
                component: mislabelled,
            })
            .expect_err("a provenance naming a non-server must be refused");
        assert!(error.contains("but it is a toolset"), "got: {error}");
    }

    // ---- the query the layer exists for ----

    fn seed_search_corpus(store: &AgentLibraryStore) {
        let corpus = [
            ("tool:web", "eg:capability/retrieval/web-search", ToolEffect::Read),
            ("tool:vector", "eg:capability/retrieval/vector-search", ToolEffect::Read),
            ("tool:summarize", "eg:capability/analysis/summarize", ToolEffect::Read),
            ("tool:deploy", "eg:capability/action/process-exec", ToolEffect::Write),
        ];
        for (index, (id, capability, effect)) in corpus.iter().enumerate() {
            let nonce = u8::try_from(index + 1).unwrap();
            store
                .publish_component(AgentComponentPublishRequest {
                    context: context(
                        store,
                        &format!("key-{index}"),
                        nonce,
                        0,
                        "agent-component:publish",
                    ),
                    component: tool(id, capability, *effect),
                })
                .unwrap();
        }
    }

    fn search(tenant: &str, task: Option<&str>, read_only: bool) -> AgentComponentSearchRequest {
        AgentComponentSearchRequest {
            tenant_id: tenant.to_string(),
            task: task.map(str::to_string),
            capabilities: Vec::new(),
            kinds: Vec::new(),
            read_only,
            limit: None,
            cursor: None,
        }
    }

    #[test]
    fn a_task_search_returns_what_an_agent_doing_it_would_need() {
        let (_dir, store) = open_store();
        seed_search_corpus(&store);
        let found = store
            .search_components(&search("tenant-a", Some("eg:task/research"), false))
            .unwrap()
            .entries;
        let ids: Vec<&str> = found.iter().map(|c| c.component_id.as_str()).collect();
        // Both retrieval tools match the task's general `retrieval` need by
        // subsumption, and so does the summarizer.
        assert!(ids.contains(&"tool:web"), "{ids:?}");
        assert!(ids.contains(&"tool:vector"), "{ids:?}");
        assert!(ids.contains(&"tool:summarize"), "{ids:?}");
        assert!(!ids.contains(&"tool:deploy"), "a deploy tool is not research: {ids:?}");
    }

    #[test]
    fn a_read_only_search_excludes_every_side_effecting_component() {
        let (_dir, store) = open_store();
        seed_search_corpus(&store);
        let found = store
            .search_components(&search("tenant-a", Some("eg:task/operate"), true))
            .unwrap()
            .entries;
        assert!(
            found.iter().all(|c| !c.is_side_effecting()),
            "read_only must exclude every write tool"
        );
        // And without the filter the same task DOES surface the write tool --
        // proving the filter is doing work rather than the corpus being empty.
        let unfiltered = store
            .search_components(&search("tenant-a", Some("eg:task/operate"), false))
            .unwrap()
            .entries;
        assert!(unfiltered.iter().any(|c| c.is_side_effecting()));
    }

    #[test]
    fn a_search_is_scoped_to_its_tenant() {
        // Searching the HIGHER-sorting tenant proves nothing: the scan is
        // `heads.range((tenant_id, "")..)`, so with only `tenant-a` rows seeded
        // a search for `tenant-b` starts past every row and returns empty
        // before any isolation logic runs -- the per-row prefix check whose
        // comment says "without this the scan walks into the NEXT tenant's
        // components" would never execute, and deleting it would not fail.
        //
        // So: seed BOTH tenants and search the LOWER-sorting one, which is the
        // only arrangement in which the open-ended range actually reaches a
        // foreign row and has to break on it.
        let (_dir, store) = open_store();
        seed_search_corpus(&store);
        seed_mcp_server(&store, "tenant-b", SEED_NONCE - 1);
        for (index, (id, capability)) in [
            ("tool:web", "eg:capability/retrieval/web-search"),
            ("tool:vector", "eg:capability/retrieval/vector-search"),
            ("tool:summarize", "eg:capability/analysis/summarize"),
        ]
        .iter()
        .enumerate()
        {
            let nonce = u8::try_from(index + 100).unwrap();
            store
                .publish_component(AgentComponentPublishRequest {
                    context: context_for(
                        "tenant-b",
                        &store,
                        &format!("tenant-b-key-{index}"),
                        nonce,
                        0,
                        "agent-component:publish",
                    ),
                    component: tool_for("tenant-b", id, capability, ToolEffect::Read),
                })
                .unwrap();
        }
        assert!(
            "tenant-a" < "tenant-b",
            "this test only reaches the guard while tenant-a sorts first"
        );

        let found = store
            .search_components(&search("tenant-a", Some("eg:task/research"), false))
            .unwrap()
            .entries;
        assert!(!found.is_empty(), "the searched tenant's own rows must match");
        assert!(
            found.iter().all(|component| component.tenant_id == "tenant-a"),
            "another tenant's components must not leak: {:?}",
            found
                .iter()
                .map(|component| (&component.tenant_id, &component.component_id))
                .collect::<Vec<_>>()
        );

        // And the higher-sorting tenant still resolves its own rows, so the
        // break is scoping the scan rather than truncating it.
        let theirs = store
            .search_components(&search("tenant-b", Some("eg:task/research"), false))
            .unwrap()
            .entries;
        assert!(!theirs.is_empty());
        assert!(theirs.iter().all(|component| component.tenant_id == "tenant-b"));
    }

    #[test]
    fn a_retired_component_stops_matching_but_stays_readable() {
        // A search asks "what could I build with today", so a withdrawn
        // component is not an answer. It must still resolve by id, because an
        // agent that pinned it needs to.
        let (_dir, store) = open_store();
        store
            .publish_component(AgentComponentPublishRequest {
                context: context(&store, "key-1", 1, 0, "agent-component:publish"),
                component: tool("tool:web", "eg:capability/retrieval/web-search", ToolEffect::Read),
            })
            .unwrap();
        assert_eq!(
            store
                .search_components(&search("tenant-a", Some("eg:task/research"), false))
                .unwrap()
                .entries
                .len(),
            1
        );
        store
            .retire_component(AgentComponentRetireRequest {
                context: context(&store, "key-2", 2, 1, "agent-component:retire"),
                component_id: "tool:web".to_string(),
            })
            .unwrap();
        assert!(store
            .search_components(&search("tenant-a", Some("eg:task/research"), false))
            .unwrap()
            .entries
            .is_empty());
        assert!(store
            .current_component("tenant-a", "tool:web")
            .unwrap()
            .is_some());
        assert_eq!(
            store.component_revisions("tenant-a", "tool:web").unwrap().len(),
            2,
            "both the publish and its tombstone are retained"
        );
    }

    #[test]
    fn a_search_with_neither_a_task_nor_a_capability_is_refused() {
        let (_dir, store) = open_store();
        let error = store
            .search_components(&search("tenant-a", None, false))
            .expect_err("an unconstrained search must be refused");
        assert!(error.contains("task or at least one capability"), "got: {error}");
    }

    // ---- pagination ----

    fn paged(
        tenant: &str,
        task: Option<&str>,
        limit: u32,
        cursor: Option<String>,
    ) -> AgentComponentSearchRequest {
        let mut request = search(tenant, task, false);
        request.limit = Some(limit);
        request.cursor = cursor;
        request
    }

    #[test]
    fn a_search_pages_through_a_tenant_and_returns_exactly_the_unpaged_set() {
        // The point of the cursor: a tenant is never cut off, however small the
        // page. At `limit = 1` the corpus is only reachable by paging, so this
        // fails outright if the cursor does not advance.
        let (_dir, store) = open_store();
        seed_search_corpus(&store);
        let whole: Vec<String> = store
            .search_components(&search("tenant-a", Some("eg:task/research"), false))
            .unwrap()
            .entries
            .into_iter()
            .map(|component| component.component_id)
            .collect();
        assert!(whole.len() >= 3, "the corpus must need more than one page");

        let mut seen: Vec<String> = Vec::new();
        let mut cursor = None;
        let mut pages = 0usize;
        loop {
            let page = store
                .search_components(&paged(
                    "tenant-a",
                    Some("eg:task/research"),
                    1,
                    cursor.clone(),
                ))
                .unwrap();
            assert!(page.entries.len() <= 1, "a page must honour its limit");
            seen.extend(page.entries.into_iter().map(|c| c.component_id));
            pages += 1;
            assert!(pages < 64, "paging must terminate");
            match page.next_cursor {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        assert_eq!(seen, whole, "paging must yield the unpaged set, in order");
    }

    #[test]
    fn paging_cannot_walk_out_of_the_tenant_prefix() {
        // The cursor supplies only the COMPONENT half of the key; the tenant
        // half is always the request's. So even resuming from the very last row
        // of the lower-sorting tenant -- the only position from which the
        // open-ended range reaches a foreign row -- must not surface one.
        let (_dir, store) = open_store();
        seed_search_corpus(&store);
        seed_mcp_server(&store, "tenant-b", SEED_NONCE - 1);
        for (index, (id, capability)) in [
            ("tool:web", "eg:capability/retrieval/web-search"),
            ("tool:vector", "eg:capability/retrieval/vector-search"),
        ]
        .iter()
        .enumerate()
        {
            let nonce = u8::try_from(index + 100).unwrap();
            store
                .publish_component(AgentComponentPublishRequest {
                    context: context_for(
                        "tenant-b",
                        &store,
                        &format!("tenant-b-key-{index}"),
                        nonce,
                        0,
                        "agent-component:publish",
                    ),
                    component: tool_for("tenant-b", id, capability, ToolEffect::Read),
                })
                .unwrap();
        }
        assert!("tenant-a" < "tenant-b");

        let mut cursor = None;
        let mut pages = 0usize;
        loop {
            let page = store
                .search_components(&paged(
                    "tenant-a",
                    Some("eg:task/research"),
                    1,
                    cursor.clone(),
                ))
                .unwrap();
            assert!(
                page.entries.iter().all(|c| c.tenant_id == "tenant-a"),
                "a foreign row leaked while paging: {:?}",
                page.entries
                    .iter()
                    .map(|c| (&c.tenant_id, &c.component_id))
                    .collect::<Vec<_>>()
            );
            pages += 1;
            assert!(pages < 64, "paging must terminate");
            match page.next_cursor {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
    }

    #[test]
    fn another_tenants_cursor_is_refused_by_name() {
        let (_dir, store) = open_store();
        seed_search_corpus(&store);
        let cursor = store
            .search_components(&paged("tenant-a", Some("eg:task/research"), 1, None))
            .unwrap()
            .next_cursor
            .expect("a truncated page mints a cursor");
        let error = store
            .search_components(&paged(
                "tenant-b",
                Some("eg:task/research"),
                1,
                Some(cursor.clone()),
            ))
            .expect_err("a transplanted cursor must be refused");
        assert!(error.contains("not minted for this tenant"), "got: {error}");
        // And the same cursor is still good for the tenant it was minted for,
        // so the refusal is the binding rather than the cursor being unusable.
        store
            .search_components(&paged(
                "tenant-a",
                Some("eg:task/research"),
                1,
                Some(cursor),
            ))
            .expect("its own tenant still resumes");
    }

    #[test]
    fn a_malformed_cursor_is_refused_by_name() {
        let (_dir, store) = open_store();
        seed_search_corpus(&store);
        for bad in ["zz", &"a".repeat(31), "0123456789abcdef"] {
            let error = store
                .search_components(&paged(
                    "tenant-a",
                    Some("eg:task/research"),
                    1,
                    Some(bad.to_string()),
                ))
                .expect_err("a forged cursor must be refused");
            assert!(
                error.contains("malformed") || error.contains("not minted"),
                "got: {error}"
            );
        }
    }

    #[test]
    fn a_search_limit_outside_its_bound_is_refused() {
        let (_dir, store) = open_store();
        for limit in [
            0,
            eg_types::agent_component::MAX_AGENT_COMPONENT_SEARCH_LIMIT + 1,
        ] {
            let error = store
                .search_components(&paged("tenant-a", Some("eg:task/research"), limit, None))
                .expect_err("an out-of-range limit must be refused");
            assert!(error.contains("limit must be"), "got: {error}");
        }
    }
}
