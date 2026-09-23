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
//! The revision protocol is the one [`super::agent_revision`] writes once for
//! components, graphs and templates; [`ComponentLayer`] is what this layer
//! supplies to it.

use std::ops::ControlFlow;
use std::sync::Arc;

use eg_storage::ScopedRead;

use eg_types::agent_component::{
    AgentComponentCommittedResult, AgentComponentEntry, AgentComponentMutationKind,
    AgentComponentOutboxEvent, AgentComponentPublishRequest, AgentComponentRetireRequest,
    AgentComponentStatusRequest, AGENT_COMPONENT_SCHEMA_VERSION,
};
use eg_types::agent_library::{AgentLibraryLifecycle, AgentLibraryMutationContext};

use super::agent_library::AgentLibraryStore;
use super::agent_revision::{
    decode_committed, decode_revision, ledger_status_record, status_result_bytes,
    RevisionDefinition, RevisionLayer, RevisionTables, RevisionVerb,
};
mod pins;
mod write;

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
    /// The head revision of one component, or `None` if it was never published.
    pub fn current_component(
        &self,
        tenant_id: &str,
        component_id: &str,
    ) -> Result<Option<AgentComponentEntry>, String> {
        Ok(self
            .component_revisions(tenant_id, component_id)?
            .into_iter()
            .last())
    }

    /// Every retained revision of one component, oldest first.
    pub fn component_revisions(
        &self,
        tenant_id: &str,
        component_id: &str,
    ) -> Result<Vec<AgentComponentEntry>, String> {
        super::agent_revision::revision_history::<ComponentLayer>(
            self,
            component_tables(),
            tenant_id,
            component_id,
        )
    }

    /// Resolve a prior attempt's durable outcome without re-committing it.
    pub fn component_status(
        &self,
        request: AgentComponentStatusRequest,
    ) -> Result<Option<AgentComponentWriteResult>, String> {
        let Some((_owner, record)) =
            ledger_status_record(self, &request.context, &request.component_id)?
        else {
            return Ok(None);
        };
        let committed =
            decode_committed::<ComponentLayer>(status_result_bytes::<ComponentLayer>(&record)?)?;
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
        let resume_after = request
            .cursor
            .as_deref()
            .map(|cursor| {
                eg_types::agent_component::decode_search_cursor(&request.tenant_id, cursor)
            })
            .transpose()?;
        let read = self.read()?;
        scan_component_search_page(&read, request, resume_after.as_deref())
    }
}

fn scan_component_search_page(
    read: &ScopedRead<'_, eg_storage::AgentLibraryOwner>,
    request: &eg_types::agent_component::AgentComponentSearchRequest,
    resume_after: Option<&str>,
) -> Result<eg_types::agent_component::AgentComponentSearchPage, String> {
    let limit = request.page_limit();
    let heads = read.open_owner_table(eg_storage::AGENT_COMPONENT_HEADS)?;
    let revisions = read.open_owner_table(eg_storage::AGENT_COMPONENT_REVISIONS)?;
    let mut matched = Vec::new();
    let mut scanned = 0usize;
    let mut bytes = 0usize;
    // The last row this page CONSUMED, so the cursor always resumes strictly
    // after a row the caller has already been shown. Every bound is therefore
    // checked BEFORE a row is consumed, never after.
    let mut last_consumed: Option<String> = None;
    let mut truncated = false;
    // A cursor only supplies the component half of the key; the tenant half is
    // always the request's, so paging cannot walk out of the tenant prefix no
    // matter what a caller hands back.
    let start = resume_after.unwrap_or("");
    scan_tenant_heads(
        &heads,
        &request.tenant_id,
        start,
        |component_id, head_revision| {
            // The cursor is EXCLUSIVE; the range start is inclusive.
            if resume_after == Some(component_id) {
                return Ok(ControlFlow::Continue(()));
            }
            if matched.len() >= limit
                || scanned >= MAX_AGENT_COMPONENT_SEARCH_SCAN
                || (scanned > 0 && bytes >= MAX_AGENT_COMPONENT_SEARCH_BYTES)
            {
                truncated = true;
                return Ok(ControlFlow::Break(()));
            }
            scanned += 1;
            let value =
                head_revision_row(&revisions, &request.tenant_id, component_id, head_revision)?;
            bytes = bytes.saturating_add(value.value().len());
            let entry = decode_revision::<ComponentLayer>(value.value())?;
            if request.matches(&entry) {
                matched.push(entry);
            }
            last_consumed = Some(component_id.to_string());
            Ok(ControlFlow::Continue(()))
        },
    )?;
    let next_cursor = match (truncated, last_consumed) {
        (true, Some(component_id)) => Some(eg_types::agent_component::encode_search_cursor(
            &request.tenant_id,
            &component_id,
        )),
        _ => None,
    };
    Ok(eg_types::agent_component::AgentComponentSearchPage {
        entries: matched,
        next_cursor,
    })
}

/// Visit one tenant's component heads from `start` (inclusive), in key order,
/// as `(component_id, head_revision)`, until `visit` breaks or the tenant ends.
/// `range` is open-ended: the tenant bound is enforced HERE, once, so no
/// caller's scan can walk into the next tenant's components.
pub(super) fn scan_tenant_heads(
    heads: &redb::ReadOnlyTable<(&'static str, &'static str), u64>,
    tenant_id: &str,
    start: &str,
    mut visit: impl FnMut(&str, u64) -> Result<ControlFlow<()>, String>,
) -> Result<(), String> {
    for row in heads
        .range((tenant_id, start)..)
        .map_err(|error| error.to_string())?
    {
        let (key, head_revision) = row.map_err(|error| error.to_string())?;
        let (row_tenant, component_id) = key.value();
        if row_tenant != tenant_id || visit(component_id, head_revision.value())?.is_break() {
            break;
        }
    }
    Ok(())
}

/// The revision row a head points at; a head naming a missing revision is a
/// corrupt owner, refused by name.
pub(super) fn head_revision_row(
    revisions: &redb::ReadOnlyTable<(&'static str, &'static str, u64), &'static [u8]>,
    tenant_id: &str,
    component_id: &str,
    head_revision: u64,
) -> Result<redb::AccessGuard<'static, &'static [u8]>, String> {
    revisions
        .get((tenant_id, component_id, head_revision))
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "agent component head points to a missing revision".to_string())
}

/// [`ComponentLayer::verb`]'s dispatch, kept as a free function so the
/// trivial two-arm shape every other `RevisionLayer` implementor's `verb`
/// has doesn't show as a regression on the trait method itself now that
/// `AgentComponentMutationKind` carries two more variants than the trait's
/// other implementors' own kind types do.
fn component_verb(kind: AgentComponentMutationKind) -> RevisionVerb {
    match kind {
        AgentComponentMutationKind::Publish => RevisionVerb::Publish,
        AgentComponentMutationKind::Retire => RevisionVerb::Retire,
        // A pack entry that vanished from its connector's current pack
        // (Withdraw, reversible) or returned in a later import
        // (Republish) -- see `AgentComponentMutationKind`'s own doc.
        AgentComponentMutationKind::Withdraw => RevisionVerb::Withdraw,
        AgentComponentMutationKind::Republish => RevisionVerb::Republish,
    }
}

/// The component layer of the shared revision protocol.
pub(super) struct ComponentLayer;

/// The component layer's head and revision tables.
pub(super) fn component_tables() -> RevisionTables {
    RevisionTables {
        heads: eg_storage::AGENT_COMPONENT_HEADS,
        revisions: eg_storage::AGENT_COMPONENT_REVISIONS,
    }
}

impl RevisionLayer for ComponentLayer {
    type Entry = AgentComponentEntry;
    type Kind = AgentComponentMutationKind;
    type Committed = AgentComponentCommittedResult;
    type WriteResult = AgentComponentWriteResult;

    const NOUN: &'static str = "agent component";
    /// Not "component": this layer's refusals have always named the record a
    /// graph, and callers match on them.
    const RECORD: &'static str = "graph";
    const SLUG: &'static str = "agent-component";
    const OPERATION: &'static str = "component";
    const TOPIC: &'static str = AGENT_COMPONENT_OUTBOX_TOPIC;
    const RESULT_SCHEMA_ID: &'static str = AGENT_COMPONENT_RESULT_SCHEMA_ID;
    const SCHEMA_VERSION: u16 = AGENT_COMPONENT_SCHEMA_VERSION;
    const ID_HEADER: &'static str = "component_id";
    const RETIRE: AgentComponentMutationKind = AgentComponentMutationKind::Retire;
    const MAX_REVISIONS: usize = MAX_AGENT_COMPONENT_REVISIONS;
    const MAX_HISTORY_BYTES: usize = MAX_AGENT_COMPONENT_HISTORY_BYTES;

    fn revision_definition(component: &AgentComponentEntry) -> RevisionDefinition<'_> {
        RevisionDefinition {
            tenant_id: &component.tenant_id,
            record_id: &component.component_id,
            entry_revision: component.entry_revision,
            lifecycle: component.lifecycle,
            definition_digest: &component.definition_digest,
            actor_scope: &component.actor_scope,
            purpose_id: &component.purpose_id,
            policy_digest: &component.policy_digest,
        }
    }

    fn validate_entry(component: &AgentComponentEntry) -> Result<(), String> {
        component.validate()
    }

    fn verb(kind: AgentComponentMutationKind) -> RevisionVerb {
        component_verb(kind)
    }

    fn encode_outbox_event(
        kind: AgentComponentMutationKind,
        component: &AgentComponentEntry,
        context: &AgentLibraryMutationContext,
    ) -> Result<Vec<u8>, String> {
        let event = AgentComponentOutboxEvent {
            schema_version: AGENT_COMPONENT_SCHEMA_VERSION,
            kind,
            component: component.clone(),
            performing_actor: context.caller_principal.clone(),
            action_actor_scope: context.actor_scope.clone(),
        };
        event.validate()?;
        eg_storage::encode_bounded(&event, "agent component outbox event")
    }

    fn retired_revision(
        component: &AgentComponentEntry,
        entry_revision: u64,
        retired_at_ms: u64,
    ) -> Result<AgentComponentEntry, String> {
        component.retire(entry_revision, retired_at_ms)
    }

    fn committed(
        component: AgentComponentEntry,
        batch_id: String,
        committed_version: u64,
    ) -> AgentComponentCommittedResult {
        AgentComponentCommittedResult {
            schema_version: AGENT_COMPONENT_SCHEMA_VERSION,
            component,
            batch_id,
            committed_version,
        }
    }

    fn committed_entry(result: &AgentComponentCommittedResult) -> &AgentComponentEntry {
        &result.component
    }

    fn committed_binding(result: &AgentComponentCommittedResult) -> (&str, u64) {
        (&result.batch_id, result.committed_version)
    }

    fn write_result(
        result: AgentComponentCommittedResult,
        replayed: bool,
    ) -> AgentComponentWriteResult {
        AgentComponentWriteResult { result, replayed }
    }
}

/// The shared `Arc` type the server state holds. Re-exported so the handler does
/// not have to name the library store to reach the graph surface.
pub type AgentComponentStoreRef = Arc<AgentLibraryStore>;

/// A minimal, valid `AgentComponentDraft` for `tenant_id`/`component_id`:
/// every RF-020 contract field a caller doesn't override gets its "nothing
/// declared" value (empty classification/dependency/capability lists, no
/// attributes, `Opaque` facts, `Native` provenance). Every fixture builder in
/// this module starts from this and overrides only what it actually varies,
/// so the next mandatory `AgentComponentDraft` field is a one-line change
/// here instead of a same-line change at every call site -- precisely the
/// growth that pushed these builders' shared boilerplate over the clone
/// gate's detection floor once already.
#[cfg(test)]
pub(crate) fn test_component_draft(
    tenant_id: &str,
    component_id: &str,
) -> eg_types::agent_component::AgentComponentDraft {
    use eg_types::agent_component::{
        AgentComponentDraft, AgentComponentKind, DraftPublication, DraftSubject,
    };
    AgentComponentDraft::bare(
        DraftSubject {
            component_id,
            kind: AgentComponentKind::Tool,
            version: "1.0.0",
            content_digest: &format!("sha256:{}", "1".repeat(64)),
            summary: &format!("test component {component_id}"),
        },
        DraftPublication {
            tenant_id,
            actor_scope: "action-scope:a",
            purpose_id: "agent-component:publish",
            policy_digest: &super::agent_library::current_agent_library_policy_digest().unwrap(),
            source_revision: "rev-1",
            source_revision_digest: &format!("sha256:{}", "8".repeat(64)),
        },
    )
}

/// The `AgentComponentFacts::Tool` shape every fixture in this module wants:
/// `effect` varies, every selection/hint field stays at "not declared". A
/// free function rather than inlining the 10 neutral fields at each of this
/// module's two `Tool`-kind fixture builders, for the same reason
/// `test_component_draft` exists.
#[cfg(test)]
pub(crate) fn test_tool_facts(
    effect: eg_types::agent_component::ToolEffect,
) -> eg_types::agent_component::AgentComponentFacts {
    eg_types::agent_component::AgentComponentFacts::Tool {
        effect,
        required_scopes: Vec::new(),
        input_schema_digest: None,
        output_schema_digest: None,
        read_only_hint: None,
        destructive_hint: None,
        idempotent_hint: None,
        open_world_hint: None,
        modalities: Default::default(),
        cost: None,
        latency_declared: None,
    }
}

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
        AgentComponentDraft, AgentComponentFacts, AgentComponentKind, PromptMode, ToolEffect,
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
                AgentComponentKind::Tool => test_tool_facts(ToolEffect::Read),
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
                    modalities: Default::default(),
                    cost: None,
                    latency_declared: None,
                    latency_observed_ref: None,
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
                    evaluation_receipt_digest: None,
                    component: AgentComponentDraft {
                        kind,
                        facts,
                        summary: format!("seeded {component_id}"),
                        actor_scope: "action-scope:component-seed".to_string(),
                        policy_digest,
                        source_revision: "component-seed:1".to_string(),
                        ..test_component_draft(tenant_id, component_id)
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
    use super::super::agent_fixtures::mutation_context;
    use super::*;
    use eg_types::agent_component::{
        AgentComponentDraft, AgentComponentKind, AgentComponentSearchRequest, ComponentDependency,
        ComponentProvenance, ToolEffect,
    };

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
            kind: AgentComponentKind::McpServer,
            summary: "the search mcp server".to_string(),
            // Deliberately unclassified: every task-constrained search in this
            // module asserts on which TOOLS it finds, and a seeded server that
            // matched a task would change those answers.
            ..super::test_component_draft(tenant_id, MCP_SERVER_ID)
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
                context: mutation_context(
                    store,
                    tenant_id,
                    &format!("{tenant_id}-mcp-server-seed"),
                    nonce,
                    0,
                    "agent-component:publish",
                ),
                evaluation_receipt_digest: None,
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
            facts: super::test_tool_facts(effect),
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
            ..super::test_component_draft(tenant_id, component_id)
        }
    }

    fn context(
        store: &AgentLibraryStore,
        key: &str,
        nonce: u8,
        expected_revision: u64,
        purpose_id: &str,
    ) -> AgentLibraryMutationContext {
        mutation_context(store, "tenant-a", key, nonce, expected_revision, purpose_id)
    }

    fn open_store() -> (tempfile::TempDir, AgentLibraryStore) {
        let (dir, store) = super::super::agent_fixtures::open_agent_store();
        seed_mcp_server(&store, "tenant-a", SEED_NONCE);
        (dir, store)
    }

    #[test]
    fn a_published_component_is_durable_and_reads_back() {
        let (_dir, store) = open_store();
        let published = store
            .publish_component(AgentComponentPublishRequest {
                context: context(&store, "key-1", 1, 0, "agent-component:publish"),
                evaluation_receipt_digest: None,
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
                evaluation_receipt_digest: None,
                component: tool(
                    "tool:a",
                    "eg:capability/retrieval/web-search",
                    ToolEffect::Read,
                ),
            })
            .unwrap();
        let mut retry = context(&store, "key-1", 2, 0, "agent-component:publish");
        retry.created_at_ms = 99;
        let replayed = store
            .publish_component(AgentComponentPublishRequest {
                context: retry,
                evaluation_receipt_digest: None,
                component: tool(
                    "tool:a",
                    "eg:capability/retrieval/web-search",
                    ToolEffect::Read,
                ),
            })
            .unwrap();
        assert!(replayed.replayed);
        assert_eq!(replayed.result, first.result);
        assert_eq!(
            store
                .component_revisions("tenant-a", "tool:a")
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn a_replayed_fresh_nonce_is_consumed_before_returning() {
        let (_dir, store) = open_store();
        let component = tool(
            "tool:a",
            "eg:capability/retrieval/web-search",
            ToolEffect::Read,
        );
        store
            .publish_component(AgentComponentPublishRequest {
                context: context(&store, "key-1", 1, 0, "agent-component:publish"),
                evaluation_receipt_digest: None,
                component: component.clone(),
            })
            .unwrap();

        let replayed = store
            .publish_component(AgentComponentPublishRequest {
                context: context(&store, "key-1", 2, 0, "agent-component:publish"),
                evaluation_receipt_digest: None,
                component: component.clone(),
            })
            .unwrap();
        assert!(replayed.replayed);

        let error = store
            .publish_component(AgentComponentPublishRequest {
                context: context(&store, "key-1", 2, 0, "agent-component:publish"),
                evaluation_receipt_digest: None,
                component,
            })
            .expect_err("a replay nonce must be consumed before the replay returns");
        assert!(error.contains("REPLAY_NONCE_CONSUMED"), "got: {error}");
        assert_eq!(
            store
                .component_revisions("tenant-a", "tool:a")
                .unwrap()
                .len(),
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
                evaluation_receipt_digest: None,
                component: tool(
                    "same-id",
                    "eg:capability/retrieval/web-search",
                    ToolEffect::Read,
                ),
            })
            .unwrap();
        assert_eq!(
            super::super::agent_fixtures::layers_holding(&store, "tenant-a", "same-id"),
            [true, false, false, false],
            "only the component layer holds the id"
        );
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
                    evaluation_receipt_digest: None,
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
        let mut orphan = tool(
            "tool:web",
            "eg:capability/retrieval/web-search",
            ToolEffect::Read,
        );
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
                evaluation_receipt_digest: None,
                component: orphan,
            })
            .expect_err("an unresolvable provenance pin must be refused");
        assert!(
            error.contains("which does not exist in this tenant"),
            "got: {error}"
        );
        assert!(store
            .current_component("tenant-a", "tool:web")
            .unwrap()
            .is_none());
    }

    #[test]
    fn a_component_provenanced_to_an_invented_server_digest_is_refused() {
        // The half an id alone could never carry: WHICH revision of the server
        // this tool's surface was read from.
        let (_dir, store) = open_store();
        let mut stale = tool(
            "tool:web",
            "eg:capability/retrieval/web-search",
            ToolEffect::Read,
        );
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
                evaluation_receipt_digest: None,
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
        let mut mislabelled = tool(
            "tool:web",
            "eg:capability/retrieval/web-search",
            ToolEffect::Read,
        );
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
                evaluation_receipt_digest: None,
                component: mislabelled,
            })
            .expect_err("a provenance naming a non-server must be refused");
        assert!(error.contains("but it is a toolset"), "got: {error}");
    }

    // ---- the query the layer exists for ----

    fn seed_search_corpus(store: &AgentLibraryStore) {
        let corpus = [
            (
                "tool:web",
                "eg:capability/retrieval/web-search",
                ToolEffect::Read,
            ),
            (
                "tool:vector",
                "eg:capability/retrieval/vector-search",
                ToolEffect::Read,
            ),
            (
                "tool:summarize",
                "eg:capability/analysis/summarize",
                ToolEffect::Read,
            ),
            (
                "tool:deploy",
                "eg:capability/action/process-exec",
                ToolEffect::Write,
            ),
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
                    evaluation_receipt_digest: None,
                    component: tool(id, capability, *effect),
                })
                .unwrap();
        }
    }

    /// Publish read-only tools into `tenant-b` under nonces 100 and up.
    fn seed_tenant_b_tools(store: &AgentLibraryStore, tools: &[(&str, &str)]) {
        for (index, (id, capability)) in tools.iter().enumerate() {
            let nonce = u8::try_from(index + 100).unwrap();
            store
                .publish_component(AgentComponentPublishRequest {
                    context: mutation_context(
                        store,
                        "tenant-b",
                        &format!("tenant-b-key-{index}"),
                        nonce,
                        0,
                        "agent-component:publish",
                    ),
                    evaluation_receipt_digest: None,
                    component: tool_for("tenant-b", id, capability, ToolEffect::Read),
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
        assert!(
            !ids.contains(&"tool:deploy"),
            "a deploy tool is not research: {ids:?}"
        );
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
        seed_tenant_b_tools(
            &store,
            &[
                ("tool:web", "eg:capability/retrieval/web-search"),
                ("tool:vector", "eg:capability/retrieval/vector-search"),
                ("tool:summarize", "eg:capability/analysis/summarize"),
            ],
        );
        assert!(
            "tenant-a" < "tenant-b",
            "this test only reaches the guard while tenant-a sorts first"
        );

        let found = store
            .search_components(&search("tenant-a", Some("eg:task/research"), false))
            .unwrap()
            .entries;
        assert!(
            !found.is_empty(),
            "the searched tenant's own rows must match"
        );
        assert!(
            found
                .iter()
                .all(|component| component.tenant_id == "tenant-a"),
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
        assert!(theirs
            .iter()
            .all(|component| component.tenant_id == "tenant-b"));
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
                evaluation_receipt_digest: None,
                component: tool(
                    "tool:web",
                    "eg:capability/retrieval/web-search",
                    ToolEffect::Read,
                ),
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
            store
                .component_revisions("tenant-a", "tool:web")
                .unwrap()
                .len(),
            2,
            "both the publish and its tombstone are retained"
        );
    }

    #[test]
    fn a_search_with_no_selector_is_refused() {
        let (_dir, store) = open_store();
        let error = store
            .search_components(&search("tenant-a", None, false))
            .expect_err("an unconstrained search must be refused");
        assert!(error.contains("task, capability, or kind"), "got: {error}");
    }

    #[test]
    fn a_kind_only_search_lists_that_kind() {
        let (_dir, store) = open_store();
        seed_search_corpus(&store);
        let mut request = search("tenant-a", None, false);
        request.kinds = vec![AgentComponentKind::Tool];
        request.limit = Some(2);

        let first = store.search_components(&request).unwrap();
        assert_eq!(first.entries.len(), 2);
        assert!(first
            .entries
            .iter()
            .all(|entry| entry.kind == AgentComponentKind::Tool));
        assert!(first.next_cursor.is_some(), "the listing must paginate");

        request.cursor = first.next_cursor;
        let second = store.search_components(&request).unwrap();
        assert_eq!(second.entries.len(), 2);
        assert!(second
            .entries
            .iter()
            .all(|entry| entry.kind == AgentComponentKind::Tool));
        assert!(second.next_cursor.is_none());
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
        seed_tenant_b_tools(
            &store,
            &[
                ("tool:web", "eg:capability/retrieval/web-search"),
                ("tool:vector", "eg:capability/retrieval/vector-search"),
            ],
        );
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
