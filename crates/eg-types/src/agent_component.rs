//! Layer 1 of the agent hierarchy: the COMPONENTS an agent is assembled from
//! (RF-ADR-008).
//!
//! [`crate::agent_library`] records what one agent IS; [`crate::agent_graph`]
//! records how several are composed. Both of those reference their parts by
//! opaque string plus digest, which is enough to prove a part has not changed
//! and not enough to say what it *is*. This module makes the parts themselves
//! durable, digest-bound records in the same owner.
//!
//! # What "native" buys, concretely
//!
//! While a component is a string, EG cannot answer any of these:
//!
//! * which agents use this MCP tool, so what breaks if its schema changes?
//! * which model profiles can satisfy this agent's output contract?
//! * what tools does this skill actually require?
//! * is this prompt reachable from an agent whose policy forbids that purpose?
//!
//! Every one of them is a graph query over components and their dependencies,
//! and every one is unanswerable over opaque references. That is the whole
//! point of an AI *context* engine: the context has to be queryable, not just
//! attested.
//!
//! # What this module deliberately does NOT store
//!
//! Not the content. [`crate::agent_library`]'s rule stands: raw prompts,
//! secrets, tool output and runtime context have no field here and therefore
//! cannot become part of durable identity. What comes in is the component's
//! *identity*, its *structure* (what it requires), and the *capability facts*
//! an optimizer needs to choose it. What stays out is the text.
//!
//! So `content_digest` pins the bytes wherever they live, and
//! [`AgentComponentFacts`] carries the shape. A prompt contributes its mode,
//! token budget and variable names; never its wording.
//!
//! # Components form a DAG
//!
//! [`ComponentDependency`] pins each requirement by `(id, kind, digest)`, so
//! components compose into a directed graph exactly as agent graphs do — and
//! it is acyclic for the same structural reason (see
//! [`crate::agent_graph`]'s composition notes): a dependency can only pin a
//! digest that already exists, and a new revision's digest cannot be known to
//! anything published before it.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

use crate::agent_library::AgentLibraryLifecycle;

pub mod content;
pub mod facts;
pub mod search;

mod validation;

pub use content::{
    AgentComponentContentRequest, AgentComponentContentResult, COMPONENT_CONTENT_SCHEMA_VERSION,
    COMPONENT_MEDIA_TYPE_ATTRIBUTE, DEFAULT_COMPONENT_MEDIA_TYPE, MAX_COMPONENT_BODY_BYTES,
};
pub use facts::{
    AgentComponentFacts, CostFacts, DeclaredCost, DeclaredLatency, FactQuality, ModalityFacts,
    ObservationRef, PriceSource, PromptMode, ToolEffect, ToolsetTransport,
};
pub use search::{
    decode_search_cursor, encode_search_cursor, AgentComponentSearchPage,
    AgentComponentSearchRequest, AgentComponentStatusRequest,
};

/// Advanced to 2 by the pre-freeze contract review.
/// [`ComponentProvenance::McpServer`] stopped naming its server by bare id and
/// now PINS it as a [`ComponentDependency`], so the definition digest's input
/// set gained the server's kind and digest. Bumping here is what makes a v1
/// record a TYPED rejection ("schema version 1 is no longer accepted") rather
/// than a confusing digest mismatch on a row that re-derives to a different
/// value for a reason nothing states -- the same reasoning
/// `agent_library.rs:17-20` gives for its own bump.
pub const AGENT_COMPONENT_SCHEMA_VERSION: u16 = 3;
/// Format-identity constant (RF-ADR-006), advanced with the schema version:
/// the digest covers a different shape, so it must be minted under a different
/// domain or two different definitions could collide across the bump.
pub const AGENT_COMPONENT_DIGEST_DOMAIN: &[u8] = b"au-eg/agent-component-definition/v3";
/// Format-identity constant (RF-ADR-006) for the tenant binding inside an
/// opaque search cursor. See [`encode_search_cursor`].
pub const AGENT_COMPONENT_SEARCH_CURSOR_DOMAIN: &[u8] = b"au-eg/agent-component-search-cursor/v1";

/// Most components one search PAGE may return.
///
/// A page bound, not a corpus bound: a caller past it pages, it does not get
/// refused. Distinct from [`MAX_AGENT_COMPONENT_SEARCH_CURSOR_BYTES`] and from
/// the store's scan/byte bounds, each of which caps a different resource.
pub const MAX_AGENT_COMPONENT_SEARCH_LIMIT: u32 = 256;
/// Longest opaque cursor a caller may hand back.
pub const MAX_AGENT_COMPONENT_SEARCH_CURSOR_BYTES: usize = 16 * 1024;

const MAX_TEXT_BYTES: usize = 4 * 1024;
const MAX_DEPENDENCIES: usize = 256;
const MAX_CAPABILITIES: usize = 64;
const MAX_ATTRIBUTES: usize = 64;
const MAX_VARIABLES: usize = 256;
const MAX_SCOPES: usize = 64;
const DIGEST_PREFIX: &str = "sha256:";

/// What kind of part this is.
///
/// Closed on purpose. Every kind is either referenced from an agent slot —
/// [`crate::agent_library::AgentLibraryEntry`],
/// [`crate::agent_library::AgentRuntimeContract`] or
/// [`crate::agent_graph`] — or is catalog content an agent is VALIDATED or
/// DECIDED against. Both belong to the same closed set for the same reason: a
/// part nothing can name is a part nothing can reason about.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum AgentComponentKind {
    /// An LLM and the profile it is invoked under (`model_profile_ref`).
    ModelProfile,
    /// Instructions, static or dynamic (`system_prompt_ref`).
    SystemPrompt,
    /// One callable tool (`tool_refs`). An MCP server's tools ingest as these.
    Tool,
    /// A group of tools with one provenance: an MCP server, a function
    /// toolset, a skill pack (`toolset_refs`).
    Toolset,
    /// An MCP server itself -- the thing a set of tools/prompts/resources is
    /// served BY. Modelled separately from `Toolset` because impact analysis
    /// asks about the server ("this server is down / its schema moved --
    /// which agents are affected?"), and that question needs the server to be
    /// a node, not an attribute of each tool.
    McpServer,
    /// An MCP prompt: a named, parameterized prompt template a server offers.
    McpPrompt,
    /// An MCP resource: addressable context a server exposes for reading.
    McpResource,
    /// A pydantic-ai skill (`skill_refs`).
    Skill,
    /// An ontology or vocabulary (`ontology_refs`).
    Ontology,
    /// A deps/output contract schema (`deps_contract`, `output_contract`).
    Schema,
    /// An `@agent.output_validator` (`output_validator_refs`).
    OutputValidator,
    /// A predicate behind a graph decision node or edge condition
    /// (`AgentGraphNodeKind::Decision::decision`, `AgentGraphEdge::condition`).
    /// Both slots pin a `ComponentDependency` of this kind, so republishing a
    /// predicate cannot re-route an already-approved graph.
    Predicate,
    /// A SHACL shapes graph a component or request graph is validated against.
    Shapes,
    /// An A2A agent card: an external agent's own declaration of itself.
    A2aAgentCard,
    /// One durable decision record. Published by the engine only; a caller
    /// that tries to publish one is refused by name.
    DecisionRecord,
    /// A decision policy body: what "better" means and when to abstain.
    DecisionPolicy,
    /// A fitted statistical decision head.
    DecisionHead,
    /// The feature schema a decision head is fitted and evaluated against.
    FeatureSchema,
    /// A scoring rubric a decision is judged by.
    Rubric,
    /// A natural-language template a decision surface renders with.
    NlTemplate,
}

impl AgentComponentKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ModelProfile => "model_profile",
            Self::SystemPrompt => "system_prompt",
            Self::Tool => "tool",
            Self::Toolset => "toolset",
            Self::McpServer => "mcp_server",
            Self::McpPrompt => "mcp_prompt",
            Self::McpResource => "mcp_resource",
            Self::Skill => "skill",
            Self::Ontology => "ontology",
            Self::Schema => "schema",
            Self::OutputValidator => "output_validator",
            Self::Predicate => "predicate",
            Self::Shapes => "shapes",
            Self::A2aAgentCard => "a2a_agent_card",
            Self::DecisionRecord => "decision_record",
            Self::DecisionPolicy => "decision_policy",
            Self::DecisionHead => "decision_head",
            Self::FeatureSchema => "feature_schema",
            Self::Rubric => "rubric",
            Self::NlTemplate => "nl_template",
        }
    }

    /// The authorization action publishing this kind needs.
    ///
    /// Curating the four statistical catalog kinds is a DIFFERENT privilege
    /// from publishing a tool: a rubric or a fitted head decides what the
    /// engine will do, so it is administrative even though it is still a
    /// component. The mapping lives here, next to the kinds, so the capability
    /// ledger and the access classifier read the same answer.
    pub fn publish_authz_action(self) -> &'static str {
        match self {
            Self::DecisionPolicy => "admin:decision-policy",
            Self::DecisionHead => "admin:decision-head",
            Self::FeatureSchema | Self::Rubric | Self::NlTemplate => "admin:decision-catalog",
            Self::ModelProfile
            | Self::SystemPrompt
            | Self::Tool
            | Self::Toolset
            | Self::McpServer
            | Self::McpPrompt
            | Self::McpResource
            | Self::Skill
            | Self::Ontology
            | Self::Schema
            | Self::OutputValidator
            | Self::Predicate
            | Self::Shapes
            | Self::A2aAgentCard
            | Self::DecisionRecord => "agent:component-write",
        }
    }
}

/// Component-id prefixes no caller may publish or retire.
///
/// An `mcp:` id belongs to a connector pack import and a `decision:` id to a
/// committed decision record. Both are minted by the engine from content it
/// validated, so a caller-supplied one could only be a forgery of the
/// provenance the id itself asserts.
pub const RESERVED_COMPONENT_ID_PREFIXES: &[&str] = &["mcp:", "decision:"];

/// Whether `component_id` is engine-owned.
pub fn is_reserved_component_id(component_id: &str) -> bool {
    RESERVED_COMPONENT_ID_PREFIXES
        .iter()
        .any(|prefix| component_id.starts_with(prefix))
}

/// One pinned requirement of a component.
///
/// The `kind` is carried alongside the id so a dependency can be type-checked
/// without resolving it: a [`AgentComponentKind::Tool`] whose input schema
/// points at a [`AgentComponentKind::ModelProfile`] is rejected structurally,
/// not at run time. The `definition_digest` pins the exact revision, so a
/// dependency cannot silently change under a component that was reviewed
/// against it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ComponentDependency {
    pub component_id: String,
    pub kind: AgentComponentKind,
    pub definition_digest: String,
}

/// Where a component was ingested FROM.
///
/// Source packages and MCP servers remain the origin of skills, tools, prompts
/// and resources; EG is where they are recorded so they can be queried. That
/// split only works if the origin travels with the record: without it, "which
/// agents break if this MCP server changes?" cannot be answered, and a
/// re-ingest cannot tell an updated component from a new one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "origin", rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ComponentProvenance {
    /// Declared directly in EG; no upstream system owns it.
    Native,
    /// Ingested from a source package (an `agent-packages/*` distribution).
    SourcePackage {
        package_id: String,
        package_version: String,
    },
    /// Ingested by listing an MCP server's surface.
    ///
    /// `server` PINS the [`AgentComponentKind::McpServer`] record by
    /// `(id, kind, digest)`, so the server is one node every tool/prompt/
    /// resource it serves hangs off AND the server a record claims is the
    /// server that was reviewed. It was a bare `server_component_id: String`
    /// -- the last unpinned cross-record reference in this contract -- and an
    /// id alone is unresolvable in principle: no admission check can tell an
    /// ingest that named the reviewed server from one that named a different
    /// server, or a republished one whose tool surface has since moved.
    ///
    /// A full [`ComponentDependency`] rather than an id+digest pair, because
    /// the kind belongs inside the pin exactly as it does everywhere else
    /// here: resolution is by `(id, kind, digest)`, and carrying the kind is
    /// what lets admission refuse a provenance that names a `Toolset` where
    /// the serving server belongs instead of discovering it at execution.
    McpServer {
        server: ComponentDependency,
        /// The name the server itself uses. Distinct from `component_id`,
        /// which is EG-scoped: two servers may both expose a `search` tool.
        upstream_name: String,
    },
}

impl ComponentProvenance {
    fn validate(&self) -> Result<(), String> {
        match self {
            Self::Native => Ok(()),
            Self::SourcePackage {
                package_id,
                package_version,
            } => {
                validate_text("provenance package_id", package_id)?;
                validate_text("provenance package_version", package_version)
            }
            Self::McpServer {
                server,
                upstream_name,
            } => {
                validate_text("provenance server component_id", &server.component_id)?;
                validate_digest(
                    "provenance server definition_digest",
                    &server.definition_digest,
                )?;
                // The kind half of the pin is checked HERE as well as at
                // resolution: a provenance that names a toolset is malformed
                // whether or not a store is open, and refusing it locally keeps
                // the error legible instead of surfacing as a resolution
                // mismatch against a record that does exist.
                if server.kind != AgentComponentKind::McpServer {
                    return Err(format!(
                        "agent component provenance must pin an mcp_server component, got '{}'",
                        server.kind.as_str()
                    ));
                }
                validate_text("provenance upstream_name", upstream_name)
            }
        }
    }

    /// The component record this provenance PINS, if any.
    ///
    /// The resolver half of the pin: admission has to prove the named server
    /// exists at the pinned revision, in this tenant, under the pinned kind,
    /// exactly as it already proves `requires` does. Exposed as one accessor
    /// so a future provenance variant that pins a record cannot be added
    /// without a caller here noticing.
    pub fn pinned_component(&self) -> Option<&ComponentDependency> {
        match self {
            Self::McpServer { server, .. } => Some(server),
            Self::Native | Self::SourcePackage { .. } => None,
        }
    }

    fn label(&self) -> &'static str {
        match self {
            Self::Native => "native",
            Self::SourcePackage { .. } => "source_package",
            Self::McpServer { .. } => "mcp_server",
        }
    }
}

/// Caller-supplied inputs for one component revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AgentComponentDraft {
    pub component_id: String,
    pub kind: AgentComponentKind,
    pub version: String,
    /// Pins the component's actual bytes, wherever they live. The bytes
    /// themselves are deliberately not stored here.
    pub content_digest: String,
    /// How to fetch those bytes. `None` for a component that is fully
    /// described by its facts (a model profile names a provider model; there
    /// is no artifact to fetch).
    #[serde(default)]
    pub content_ref: Option<String>,
    pub facts: AgentComponentFacts,
    /// Where this component came from. See [`ComponentProvenance`].
    pub provenance: ComponentProvenance,
    /// The component's own description of itself, bounded.
    ///
    /// This is INTERFACE, not content, and the distinction is what decides
    /// whether it belongs here. An MCP tool's description is what a model
    /// reads to decide whether to call it, so it is part of the tool's
    /// contract and has to be queryable. A system prompt's *body* is content
    /// and stays out; only its shape (mode, budget, variables) comes in.
    pub summary: String,
    /// Ontology terms this component is classified under -- the terms it
    /// PROVIDES. Matched against a task's required capabilities by
    /// subsumption (`crate::agent_ontology::satisfies`), which is what makes
    /// "what does an agent doing XYZ need?" a traversal rather than a prompt.
    #[serde(default)]
    pub classification: Vec<String>,
    /// What this component needs, each pinned to an exact revision.
    #[serde(default)]
    pub requires: Vec<ComponentDependency>,
    /// Capability IRIs the publisher DECLARES this component provides, as
    /// distinct from the curated `classification` above. A declaration is a
    /// claim and is recorded as one, so a decision that leans on it is
    /// classified by that claim.
    #[serde(default)]
    pub declared_capabilities: Vec<String>,
    /// Capability IRIs this component NEEDS from whatever it is assembled
    /// with. The assembly layer's coverage question is over this list.
    #[serde(default)]
    pub required_capabilities: Vec<String>,
    /// The publisher's own statement of what it requires, kept beside the
    /// curated `required_capabilities` for the same reason
    /// `declared_capabilities` is kept beside `classification`.
    #[serde(default)]
    pub declared_required_capabilities: Vec<String>,
    /// Extension point. New facts belong here until they earn a typed field,
    /// so the vocabulary can grow without a wire break.
    #[serde(default)]
    pub attributes: BTreeMap<String, String>,
    pub tenant_id: String,
    pub actor_scope: String,
    pub purpose_id: String,
    pub policy_digest: String,
    pub source_revision: String,
    pub source_revision_digest: String,
}

/// One durable, published component revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AgentComponentEntry {
    pub schema_version: u16,
    pub component_id: String,
    pub kind: AgentComponentKind,
    pub version: String,
    pub content_digest: String,
    #[serde(default)]
    pub content_ref: Option<String>,
    pub facts: AgentComponentFacts,
    pub provenance: ComponentProvenance,
    pub summary: String,
    #[serde(default)]
    pub classification: Vec<String>,
    #[serde(default)]
    pub requires: Vec<ComponentDependency>,
    #[serde(default)]
    pub declared_capabilities: Vec<String>,
    #[serde(default)]
    pub required_capabilities: Vec<String>,
    #[serde(default)]
    pub declared_required_capabilities: Vec<String>,
    #[serde(default)]
    pub attributes: BTreeMap<String, String>,
    pub tenant_id: String,
    pub actor_scope: String,
    pub purpose_id: String,
    pub policy_digest: String,
    pub source_revision: String,
    pub source_revision_digest: String,
    pub entry_revision: u64,
    pub lifecycle: AgentLibraryLifecycle,
    pub definition_digest: String,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

impl AgentComponentEntry {
    pub fn publish(
        draft: AgentComponentDraft,
        entry_revision: u64,
        published_at_ms: u64,
    ) -> Result<Self, String> {
        Self::create(
            draft,
            entry_revision,
            AgentLibraryLifecycle::Published,
            published_at_ms,
            published_at_ms,
        )
    }

    pub fn create(
        draft: AgentComponentDraft,
        entry_revision: u64,
        lifecycle: AgentLibraryLifecycle,
        created_at_ms: u64,
        updated_at_ms: u64,
    ) -> Result<Self, String> {
        draft.validate()?;
        if entry_revision == 0 {
            return Err("agent component entry revision must start at one".to_string());
        }
        if updated_at_ms < created_at_ms {
            return Err("agent component entry update time precedes creation time".to_string());
        }
        let definition_digest = definition_digest(&draft);
        let entry = Self {
            schema_version: AGENT_COMPONENT_SCHEMA_VERSION,
            component_id: draft.component_id,
            kind: draft.kind,
            version: draft.version,
            content_digest: draft.content_digest,
            content_ref: draft.content_ref,
            facts: draft.facts,
            provenance: draft.provenance,
            summary: draft.summary,
            classification: draft.classification,
            requires: draft.requires,
            declared_capabilities: draft.declared_capabilities,
            required_capabilities: draft.required_capabilities,
            declared_required_capabilities: draft.declared_required_capabilities,
            attributes: draft.attributes,
            tenant_id: draft.tenant_id,
            actor_scope: draft.actor_scope,
            purpose_id: draft.purpose_id,
            policy_digest: draft.policy_digest,
            source_revision: draft.source_revision,
            source_revision_digest: draft.source_revision_digest,
            entry_revision,
            lifecycle,
            definition_digest,
            created_at_ms,
            updated_at_ms,
        };
        entry.validate()?;
        Ok(entry)
    }

    pub fn retire(&self, entry_revision: u64, retired_at_ms: u64) -> Result<Self, String> {
        self.validate()?;
        if self.lifecycle == AgentLibraryLifecycle::Retired {
            return Err("agent component entry is already retired".to_string());
        }
        if entry_revision <= self.entry_revision {
            return Err("agent component tombstone revision must advance".to_string());
        }
        let retired = Self {
            entry_revision,
            lifecycle: AgentLibraryLifecycle::Retired,
            updated_at_ms: retired_at_ms,
            ..self.clone()
        };
        retired.validate()?;
        Ok(retired)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != AGENT_COMPONENT_SCHEMA_VERSION {
            return Err("agent component entry schema version is unsupported".to_string());
        }
        self.as_draft().validate()?;
        if self.entry_revision == 0 {
            return Err("agent component entry revision must start at one".to_string());
        }
        if !is_digest(&self.definition_digest) {
            return Err("agent component definition_digest is not a sha256 digest".to_string());
        }
        // Only a connector pack's own members can be withdrawn: withdrawal is
        // the pack importer saying "this entry is no longer served", and no
        // other publisher has that authority over a component id.
        if self.lifecycle == AgentLibraryLifecycle::Withdrawn
            && !self.component_id.starts_with("mcp:")
        {
            return Err(
                "WITHDRAWN_NOT_ALLOWED: only a connector pack member may be withdrawn".to_string(),
            );
        }
        if self.definition_digest != definition_digest(&self.as_draft()) {
            return Err("agent component definition_digest does not match its definition".into());
        }
        Ok(())
    }

    pub fn as_draft(&self) -> AgentComponentDraft {
        AgentComponentDraft {
            component_id: self.component_id.clone(),
            kind: self.kind,
            version: self.version.clone(),
            content_digest: self.content_digest.clone(),
            content_ref: self.content_ref.clone(),
            facts: self.facts.clone(),
            provenance: self.provenance.clone(),
            summary: self.summary.clone(),
            classification: self.classification.clone(),
            requires: self.requires.clone(),
            declared_capabilities: self.declared_capabilities.clone(),
            required_capabilities: self.required_capabilities.clone(),
            declared_required_capabilities: self.declared_required_capabilities.clone(),
            attributes: self.attributes.clone(),
            tenant_id: self.tenant_id.clone(),
            actor_scope: self.actor_scope.clone(),
            purpose_id: self.purpose_id.clone(),
            policy_digest: self.policy_digest.clone(),
            source_revision: self.source_revision.clone(),
            source_revision_digest: self.source_revision_digest.clone(),
        }
    }
}

impl AgentComponentDraft {
    /// Every L1 record this component pins: what it `requires`, plus the MCP
    /// server its provenance names.
    ///
    /// One list rather than two call sites, so admission cannot resolve one
    /// pin set and silently miss the other -- which is exactly how the
    /// provenance reference stayed unresolved while `requires` was resolved.
    pub fn pinned_components(&self) -> Vec<&ComponentDependency> {
        let mut all: Vec<&ComponentDependency> = self.requires.iter().collect();
        all.extend(self.provenance.pinned_component());
        all
    }

    pub fn validate(&self) -> Result<(), String> {
        validation::validate_draft(self)
    }
}

fn definition_digest(draft: &AgentComponentDraft) -> String {
    let mut hasher = Sha256::new();
    hasher.update(AGENT_COMPONENT_DIGEST_DOMAIN);
    put_text(&mut hasher, &draft.component_id);
    put_text(&mut hasher, draft.kind.as_str());
    put_text(&mut hasher, &draft.version);
    put_text(&mut hasher, &draft.content_digest);
    put_opt_text(&mut hasher, draft.content_ref.as_deref());
    put_facts(&mut hasher, &draft.facts);
    put_provenance(&mut hasher, &draft.provenance);
    put_text(&mut hasher, &draft.summary);
    let mut classification = draft.classification.clone();
    classification.sort();
    put_names(&mut hasher, &classification);
    // Sorted: two callers that declared the same requirements in a different
    // order have declared the SAME component, and a digest that disagreed
    // would make an otherwise identical publish a different revision.
    let mut requires = draft.requires.clone();
    requires.sort();
    hasher.update((requires.len() as u64).to_be_bytes());
    for dependency in &requires {
        put_text(&mut hasher, &dependency.component_id);
        put_text(&mut hasher, dependency.kind.as_str());
        put_text(&mut hasher, &dependency.definition_digest);
    }
    put_capability_lists(&mut hasher, draft);
    hasher.update((draft.attributes.len() as u64).to_be_bytes());
    for (name, value) in &draft.attributes {
        put_text(&mut hasher, name);
        put_text(&mut hasher, value);
    }
    put_text(&mut hasher, &draft.tenant_id);
    put_text(&mut hasher, &draft.actor_scope);
    put_text(&mut hasher, &draft.purpose_id);
    put_text(&mut hasher, &draft.policy_digest);
    put_text(&mut hasher, &draft.source_revision);
    put_text(&mut hasher, &draft.source_revision_digest);
    format!("{DIGEST_PREFIX}{}", hex::encode(hasher.finalize()))
}

fn put_facts(hasher: &mut Sha256, facts: &AgentComponentFacts) {
    put_text(hasher, facts.label());
    match facts {
        AgentComponentFacts::ModelProfile {
            provider,
            model_identity,
            context_window_tokens,
            max_output_tokens,
            supports_tools,
            supports_structured_output,
            supports_vision,
            modalities,
            cost,
            latency_declared,
            latency_observed_ref,
        } => {
            put_text(hasher, provider);
            put_text(hasher, model_identity);
            hasher.update(context_window_tokens.to_be_bytes());
            hasher.update(max_output_tokens.to_be_bytes());
            hasher.update([
                u8::from(*supports_tools),
                u8::from(*supports_structured_output),
                u8::from(*supports_vision),
            ]);
            put_selection_facts(hasher, modalities, cost.as_ref(), latency_declared.as_ref());
            put_opt_text(
                hasher,
                latency_observed_ref
                    .as_ref()
                    .map(|r| r.evaluation_id.as_str()),
            );
            put_opt_text(
                hasher,
                latency_observed_ref.as_ref().map(|r| r.digest.as_str()),
            );
        }
        AgentComponentFacts::SystemPrompt {
            prompt_mode,
            token_estimate,
            variables,
        } => {
            put_text(
                hasher,
                match prompt_mode {
                    PromptMode::Static => "static",
                    PromptMode::Dynamic => "dynamic",
                },
            );
            hasher.update(token_estimate.to_be_bytes());
            put_names(hasher, variables);
        }
        AgentComponentFacts::Tool {
            effect,
            required_scopes,
            input_schema_digest,
            output_schema_digest,
            read_only_hint,
            destructive_hint,
            idempotent_hint,
            open_world_hint,
            modalities,
            cost,
            latency_declared,
        } => {
            put_text(
                hasher,
                match effect {
                    ToolEffect::Read => "read",
                    ToolEffect::Write => "write",
                },
            );
            put_names(hasher, required_scopes);
            put_opt_text(hasher, input_schema_digest.as_deref());
            put_opt_text(hasher, output_schema_digest.as_deref());
            put_tristate(hasher, *read_only_hint);
            put_tristate(hasher, *destructive_hint);
            put_tristate(hasher, *idempotent_hint);
            put_tristate(hasher, *open_world_hint);
            put_selection_facts(hasher, modalities, cost.as_ref(), latency_declared.as_ref());
        }
        AgentComponentFacts::Toolset { transport } => put_text(
            hasher,
            match transport {
                ToolsetTransport::Mcp => "mcp",
                ToolsetTransport::Function => "function",
                ToolsetTransport::Skill => "skill",
            },
        ),
        AgentComponentFacts::Opaque => {}
    }
}

fn put_provenance(hasher: &mut Sha256, provenance: &ComponentProvenance) {
    put_text(hasher, provenance.label());
    match provenance {
        ComponentProvenance::Native => {}
        ComponentProvenance::SourcePackage {
            package_id,
            package_version,
        } => {
            put_text(hasher, package_id);
            put_text(hasher, package_version);
        }
        ComponentProvenance::McpServer {
            server,
            upstream_name,
        } => {
            put_text(hasher, &server.component_id);
            put_text(hasher, server.kind.as_str());
            put_text(hasher, &server.definition_digest);
            put_text(hasher, upstream_name);
        }
    }
}

/// The three capability lists, each sorted, so two callers that declared the
/// same capabilities in a different order have declared the SAME component.
fn put_capability_lists(hasher: &mut Sha256, draft: &AgentComponentDraft) {
    for names in [
        &draft.declared_capabilities,
        &draft.required_capabilities,
        &draft.declared_required_capabilities,
    ] {
        let mut sorted = names.clone();
        sorted.sort();
        put_names(hasher, &sorted);
    }
}

/// Absent, false and true are three distinct values in the digest, so an
/// unknown hint can never hash the same as a declared `false`.
fn put_tristate(hasher: &mut Sha256, value: Option<bool>) {
    hasher.update([match value {
        None => 0u8,
        Some(false) => 1,
        Some(true) => 2,
    }]);
}

/// The three selection facts BOTH selectable kinds carry. One writer, so a
/// model profile and a tool cannot digest the same declared cost differently.
fn put_selection_facts(
    hasher: &mut Sha256,
    modalities: &ModalityFacts,
    cost: Option<&CostFacts>,
    latency: Option<&DeclaredLatency>,
) {
    put_names(hasher, &modalities.input);
    put_names(hasher, &modalities.output);
    match cost {
        None => hasher.update([0u8]),
        Some(cost) => {
            hasher.update([1u8]);
            put_text(hasher, &cost.declared.currency);
            for price in [
                cost.declared.per_call_micros,
                cost.declared.input_per_mtok_micros,
                cost.declared.output_per_mtok_micros,
            ] {
                hasher.update(price.unwrap_or_default().to_be_bytes());
                hasher.update([u8::from(price.is_some())]);
            }
            put_price_source(hasher, &cost.price_source);
            put_text(hasher, quality_token(cost.quality));
        }
    }
    match latency {
        None => hasher.update([0u8]),
        Some(latency) => {
            hasher.update([1u8]);
            hasher.update(latency.p50_ms.to_be_bytes());
            hasher.update(latency.p95_ms.to_be_bytes());
        }
    }
}

fn put_price_source(hasher: &mut Sha256, source: &PriceSource) {
    match source {
        PriceSource::Publisher => put_text(hasher, "publisher"),
        PriceSource::ConnectorPack {
            connector,
            entry_digest,
        } => {
            put_text(hasher, "connector_pack");
            put_text(hasher, connector);
            put_text(hasher, entry_digest);
        }
        PriceSource::Operator { reference } => {
            put_text(hasher, "operator");
            put_text(hasher, reference);
        }
    }
}

fn quality_token(quality: FactQuality) -> &'static str {
    match quality {
        FactQuality::Measured => "measured",
        FactQuality::Estimated => "estimated",
        FactQuality::Declared => "declared",
        FactQuality::Unavailable => "unavailable",
    }
}

fn put_text(hasher: &mut Sha256, value: &str) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value.as_bytes());
}

fn put_opt_text(hasher: &mut Sha256, value: Option<&str>) {
    match value {
        None => hasher.update([0u8]),
        Some(text) => {
            hasher.update([1u8]);
            put_text(hasher, text);
        }
    }
}

fn put_names(hasher: &mut Sha256, names: &[String]) {
    hasher.update((names.len() as u64).to_be_bytes());
    for name in names {
        put_text(hasher, name);
    }
}

fn validate_names(field: &str, names: &[String], limit: usize) -> Result<(), String> {
    if names.len() > limit {
        return Err(format!("agent component {field} exceeds its item limit"));
    }
    let mut unique = BTreeSet::new();
    for name in names {
        validate_text(field, name)?;
        if !unique.insert(name) {
            return Err(format!("agent component {field} contains a duplicate"));
        }
    }
    Ok(())
}

fn is_digest(value: &str) -> bool {
    let Some(encoded) = value.strip_prefix(DIGEST_PREFIX) else {
        return false;
    };
    encoded.len() == 64
        && encoded
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

fn validate_digest(field: &str, value: &str) -> Result<(), String> {
    if !is_digest(value) {
        return Err(format!("agent component {field} is not a sha256 digest"));
    }
    Ok(())
}

fn validate_text(field: &str, value: &str) -> Result<(), String> {
    if value.is_empty()
        || value.len() > MAX_TEXT_BYTES
        || value.trim() != value
        || value.chars().any(char::is_control)
    {
        return Err(format!("agent component {field} is invalid"));
    }
    Ok(())
}

/// Which durable mutation a component operation performs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum AgentComponentMutationKind {
    Publish,
    Retire,
    /// A pack entry that vanished from its connector's current pack. Unlike
    /// `Retire` this is REVERSIBLE: the entry coming back republishes it.
    Withdraw,
    /// A withdrawn pack entry returning in a later import.
    Republish,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AgentComponentPublishRequest {
    /// Shared with the other two layers: components are published into the
    /// same owner as the agents and graphs that reference them, so they share
    /// its mutation context rather than growing a parallel copy of it.
    pub context: crate::agent_library::AgentLibraryMutationContext,
    pub component: AgentComponentDraft,
    /// `sha256:<hex>` of the evaluation receipt that qualified this revision.
    /// REQUIRED for [`AgentComponentKind::DecisionHead`]: a head decides what
    /// the engine does, so publishing one without the receipt that measured it
    /// is the one case where "we can evaluate it later" is not recoverable.
    #[serde(default)]
    pub evaluation_receipt_digest: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AgentComponentRetireRequest {
    pub context: crate::agent_library::AgentLibraryMutationContext,
    pub component_id: String,
}

/// Typed component wire operations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum AgentComponentOp {
    /// Boxed for the same reason as the other two layers: a publish request
    /// carries a whole draft and would otherwise set the size of every
    /// `Method`.
    Publish {
        request: Box<AgentComponentPublishRequest>,
    },
    Retire {
        request: AgentComponentRetireRequest,
    },
    Current {
        tenant_id: String,
        component_id: String,
    },
    History {
        tenant_id: String,
        component_id: String,
    },
    Status {
        request: AgentComponentStatusRequest,
    },
    /// Capability search -- the query this layer exists for.
    Search {
        request: AgentComponentSearchRequest,
    },
    /// Read one revision's engine-owned bytes back, digest-verified.
    Content {
        request: AgentComponentContentRequest,
    },
}

impl AgentComponentOp {
    pub fn is_mutation(&self) -> bool {
        matches!(self, Self::Publish { .. } | Self::Retire { .. })
    }

    /// The authorization action this operation needs.
    ///
    /// Publishing is kind-dependent: the four statistical catalog kinds are
    /// administrative (see [`AgentComponentKind::publish_authz_action`]), the
    /// rest are ordinary component writes. Everything else here is a read.
    pub fn authz_action(&self) -> &'static str {
        match self {
            Self::Publish { request } => request.component.kind.publish_authz_action(),
            Self::Retire { .. } => "agent:component-write",
            Self::Current { .. }
            | Self::History { .. }
            | Self::Status { .. }
            | Self::Search { .. }
            | Self::Content { .. } => "agent:component-read",
        }
    }

    pub fn tenant_id(&self) -> &str {
        component_op_tenant_id(self)
    }

    pub fn validate(&self) -> Result<(), String> {
        match self {
            Self::Publish { request } => validate_publish(request),
            Self::Retire { request } => {
                request.context.validate()?;
                validate_text("component_id", &request.component_id)?;
                refuse_reserved_id(&request.component_id)
            }
            Self::Status { request } => {
                request.context.validate()?;
                validate_text("component_id", &request.component_id)
            }
            Self::Search { request } => request.validate(),
            Self::Content { request } => {
                validate_text("tenant_id", &request.tenant_id)?;
                validate_text("component_id", &request.component_id)
            }
            Self::Current {
                tenant_id,
                component_id,
            }
            | Self::History {
                tenant_id,
                component_id,
            } => {
                validate_text("tenant_id", tenant_id)?;
                validate_text("component_id", component_id)
            }
        }
    }
}

/// Where each operation carries the tenant it names.
///
/// A free resolver rather than a method body, so the op's classifiers
/// ([`AgentComponentOp::is_mutation`], [`AgentComponentOp::authz_action`],
/// [`AgentComponentOp::tenant_id`]) each stay a one-line statement of WHICH
/// walk they are -- the same shape the digest and validation walks in this file
/// already have.
fn component_op_tenant_id(op: &AgentComponentOp) -> &str {
    match op {
        AgentComponentOp::Publish { request } => &request.context.tenant_id,
        AgentComponentOp::Retire { request } => &request.context.tenant_id,
        AgentComponentOp::Status { request } => &request.context.tenant_id,
        AgentComponentOp::Search { request } => &request.tenant_id,
        AgentComponentOp::Content { request } => &request.tenant_id,
        AgentComponentOp::Current { tenant_id, .. }
        | AgentComponentOp::History { tenant_id, .. } => tenant_id,
    }
}

/// Refuse an engine-owned component id by name.
///
/// `RESERVED_COMPONENT_ID` rather than a generic validation error, because the
/// caller's mistake is a category error about who owns the id, and a message
/// about "invalid characters" would send them looking in the wrong place.
fn refuse_reserved_id(component_id: &str) -> Result<(), String> {
    if is_reserved_component_id(component_id) {
        return Err(format!(
            "RESERVED_COMPONENT_ID: '{component_id}' is minted by the engine and cannot be \
             published or retired by a caller"
        ));
    }
    Ok(())
}

fn validate_publish(request: &AgentComponentPublishRequest) -> Result<(), String> {
    request.context.validate()?;
    request.component.validate()?;
    if request.context.tenant_id != request.component.tenant_id {
        return Err(
            "agent component publish context tenant does not match the component's".to_string(),
        );
    }
    refuse_reserved_id(&request.component.component_id)?;
    if request.component.kind == AgentComponentKind::DecisionRecord {
        return Err(
            "FORBIDDEN_COMPONENT_KIND: a decision record is committed by DecisionCommit, never \
             published directly"
                .to_string(),
        );
    }
    validate_receipt_digest(request)
}

fn validate_receipt_digest(request: &AgentComponentPublishRequest) -> Result<(), String> {
    let head = request.component.kind == AgentComponentKind::DecisionHead;
    match (&request.evaluation_receipt_digest, head) {
        (None, true) => Err(
            "EVALUATION_RECEIPT_REQUIRED: publishing a decision head needs the digest of the \
             evaluation receipt that qualified it"
                .to_string(),
        ),
        (Some(digest), _) => validate_digest("evaluation_receipt_digest", digest),
        (None, false) => Ok(()),
    }
}

/// What a committed component mutation returns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AgentComponentCommittedResult {
    pub schema_version: u16,
    pub component: AgentComponentEntry,
    pub batch_id: String,
    pub committed_version: u64,
}

/// The outbox event one committed component revision emits.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AgentComponentOutboxEvent {
    pub schema_version: u16,
    pub kind: AgentComponentMutationKind,
    pub component: AgentComponentEntry,
    pub performing_actor: String,
    pub action_actor_scope: String,
}

impl AgentComponentOutboxEvent {
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != AGENT_COMPONENT_SCHEMA_VERSION {
            return Err("agent component outbox schema version is unsupported".to_string());
        }
        self.component.validate()?;
        // Exhaustive, and deliberately not a `_` arm: a new lifecycle must be
        // given an outbox kind here rather than silently inheriting one.
        let allowed: &[AgentComponentMutationKind] = match self.component.lifecycle {
            AgentLibraryLifecycle::Published => &[
                AgentComponentMutationKind::Publish,
                AgentComponentMutationKind::Republish,
            ],
            AgentLibraryLifecycle::Retired => &[AgentComponentMutationKind::Retire],
            AgentLibraryLifecycle::Withdrawn => &[AgentComponentMutationKind::Withdraw],
        };
        if !allowed.contains(&self.kind) {
            return Err(
                "agent component outbox kind does not match the entry's lifecycle".to_string(),
            );
        }
        validate_text("performing_actor", &self.performing_actor)?;
        validate_text("action_actor_scope", &self.action_actor_scope)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One named edit to a definition, for the digest-binding table below.
    type NamedMutation<T> = (&'static str, fn(&mut T));

    fn digest(seed: char) -> String {
        format!("sha256:{}", seed.to_string().repeat(64))
    }

    fn draft(component_id: &str, kind: AgentComponentKind) -> AgentComponentDraft {
        AgentComponentDraft {
            component_id: component_id.into(),
            kind,
            version: "1.0.0".into(),
            content_digest: digest('1'),
            content_ref: None,
            facts: AgentComponentFacts::Opaque,
            provenance: ComponentProvenance::Native,
            summary: format!("test component {component_id}"),
            classification: Vec::new(),
            requires: Vec::new(),
            declared_capabilities: Vec::new(),
            required_capabilities: Vec::new(),
            declared_required_capabilities: Vec::new(),
            attributes: BTreeMap::new(),
            tenant_id: "tenant-a".into(),
            actor_scope: "agent-builder".into(),
            purpose_id: "agent-construction".into(),
            policy_digest: digest('7'),
            source_revision: "rev-1".into(),
            source_revision_digest: digest('8'),
        }
    }

    /// Model facts with every selection field at its neutral value, so a test
    /// states only the numbers it is about.
    fn model_facts(window: u32, output: u32) -> AgentComponentFacts {
        AgentComponentFacts::ModelProfile {
            provider: "anthropic".into(),
            model_identity: "claude-opus-5".into(),
            context_window_tokens: window,
            max_output_tokens: output,
            supports_tools: true,
            supports_structured_output: true,
            supports_vision: true,
            modalities: ModalityFacts::default(),
            cost: None,
            latency_declared: None,
            latency_observed_ref: None,
        }
    }

    /// The selection facts a digest-coverage mutator overrides, one at a time.
    #[derive(Default)]
    struct ToolFactOverrides {
        input_schema_digest: Option<String>,
        read_only_hint: Option<bool>,
        modalities: ModalityFacts,
        cost: Option<CostFacts>,
        latency_declared: Option<DeclaredLatency>,
    }

    /// The baseline tool facts with `overrides` applied.
    fn tool_facts_with(overrides: ToolFactOverrides) -> AgentComponentFacts {
        AgentComponentFacts::Tool {
            effect: ToolEffect::Read,
            required_scopes: vec!["scope:read".into()],
            input_schema_digest: overrides.input_schema_digest,
            output_schema_digest: None,
            read_only_hint: overrides.read_only_hint,
            destructive_hint: None,
            idempotent_hint: None,
            open_world_hint: None,
            modalities: overrides.modalities,
            cost: overrides.cost,
            latency_declared: overrides.latency_declared,
        }
    }

    /// Tool facts with every selection field at its neutral value.
    fn tool_facts(effect: ToolEffect, required_scopes: Vec<String>) -> AgentComponentFacts {
        AgentComponentFacts::Tool {
            effect,
            required_scopes,
            input_schema_digest: None,
            output_schema_digest: None,
            read_only_hint: None,
            destructive_hint: None,
            idempotent_hint: None,
            open_world_hint: None,
            modalities: ModalityFacts::default(),
            cost: None,
            latency_declared: None,
        }
    }

    fn model() -> AgentComponentDraft {
        let mut source = draft("model:opus", AgentComponentKind::ModelProfile);
        source.facts = model_facts(200_000, 64_000);
        source
    }

    #[test]
    fn a_component_publishes_and_round_trips() {
        let entry = AgentComponentEntry::publish(model(), 1, 1_000).expect("publishes");
        entry.validate().expect("valid");
        assert_eq!(entry.as_draft(), model());
        assert_eq!(entry.kind, AgentComponentKind::ModelProfile);
    }

    #[test]
    fn facts_must_match_the_kind() {
        // Otherwise a Tool could carry model facts, and every query that reads
        // facts by kind is wrong in a way nothing else detects.
        let mut mismatched = draft("tool:search", AgentComponentKind::Tool);
        mismatched.facts = model_facts(1_000, 100);
        let error = mismatched.validate().expect_err("must be refused");
        assert!(error.contains("cannot carry"), "got: {error}");
    }

    #[test]
    fn opaque_facts_are_legal_for_every_kind() {
        for kind in [
            AgentComponentKind::Schema,
            AgentComponentKind::Ontology,
            AgentComponentKind::Skill,
            AgentComponentKind::OutputValidator,
            AgentComponentKind::Predicate,
            AgentComponentKind::Tool,
        ] {
            draft("component:x", kind)
                .validate()
                .unwrap_or_else(|error| panic!("{kind:?} with opaque facts: {error}"));
        }
    }

    #[test]
    fn a_model_that_cannot_emit_its_own_window_is_refused() {
        let mut broken = model();
        broken.facts = model_facts(1_000, 2_000);
        let error = broken.validate().expect_err("must be refused");
        assert!(error.contains("exceeds its context window"), "got: {error}");
    }

    #[test]
    fn a_static_prompt_cannot_declare_variables() {
        let mut broken = draft("prompt:research", AgentComponentKind::SystemPrompt);
        broken.facts = AgentComponentFacts::SystemPrompt {
            prompt_mode: PromptMode::Static,
            token_estimate: 500,
            variables: vec!["topic".into()],
        };
        let error = broken.validate().expect_err("must be refused");
        assert!(
            error.contains("nothing \\\n                         resolves them")
                || error.contains("resolves them"),
            "got: {error}"
        );
    }

    #[test]
    fn a_component_cannot_require_itself() {
        let mut broken = draft("tool:search", AgentComponentKind::Tool);
        broken.requires.push(ComponentDependency {
            component_id: "tool:search".into(),
            kind: AgentComponentKind::Schema,
            definition_digest: digest('a'),
        });
        let error = broken.validate().expect_err("must be refused");
        assert!(error.contains("cannot require itself"), "got: {error}");
    }

    #[test]
    fn duplicate_dependencies_are_refused() {
        let mut broken = draft("tool:search", AgentComponentKind::Tool);
        for _ in 0..2 {
            broken.requires.push(ComponentDependency {
                component_id: "schema:search-input".into(),
                kind: AgentComponentKind::Schema,
                definition_digest: digest('a'),
            });
        }
        let error = broken.validate().expect_err("must be refused");
        assert!(error.contains("twice"), "got: {error}");
    }

    #[test]
    fn dependency_order_does_not_change_the_digest() {
        // Two callers declaring the same requirements in a different order have
        // declared the SAME component; a digest that disagreed would make an
        // otherwise identical publish a distinct revision.
        let mut a = draft("tool:search", AgentComponentKind::Tool);
        a.requires = vec![
            ComponentDependency {
                component_id: "schema:in".into(),
                kind: AgentComponentKind::Schema,
                definition_digest: digest('a'),
            },
            ComponentDependency {
                component_id: "schema:out".into(),
                kind: AgentComponentKind::Schema,
                definition_digest: digest('b'),
            },
        ];
        let mut b = a.clone();
        b.requires.reverse();
        assert_eq!(
            AgentComponentEntry::publish(a, 1, 1)
                .unwrap()
                .definition_digest,
            AgentComponentEntry::publish(b, 1, 1)
                .unwrap()
                .definition_digest
        );
    }

    #[test]
    fn every_field_that_selection_turns_on_is_bound_into_the_digest() {
        // A stored-but-unhashed fact could be changed after review: an operator
        // who approved a read-only tool could find it writing, or a model
        // silently swapped for a cheaper one.
        let baseline = AgentComponentEntry::publish(model(), 1, 1_000)
            .unwrap()
            .definition_digest;

        let mutations: Vec<NamedMutation<AgentComponentDraft>> = vec![
            ("version", |d| d.version = "2.0.0".into()),
            ("content_digest", |d| d.content_digest = digest('2')),
            ("content_ref", |d| d.content_ref = Some("artifact:x".into())),
            ("summary", |d| d.summary = "something else".into()),
            ("classification", |d| {
                d.classification = vec!["eg:capability/generation/text".into()]
            }),
            ("provenance", |d| {
                d.provenance = ComponentProvenance::SourcePackage {
                    package_id: "pkg:models".into(),
                    package_version: "1.0.0".into(),
                }
            }),
            ("attributes", |d| {
                d.attributes.insert("tier".into(), "premium".into());
            }),
            ("requires", |d| {
                d.requires.push(ComponentDependency {
                    component_id: "schema:in".into(),
                    kind: AgentComponentKind::Schema,
                    definition_digest: digest('a'),
                })
            }),
            ("facts.provider", |d| {
                if let AgentComponentFacts::ModelProfile { provider, .. } = &mut d.facts {
                    *provider = "openai".into();
                }
            }),
            ("facts.context_window", |d| {
                if let AgentComponentFacts::ModelProfile {
                    context_window_tokens,
                    ..
                } = &mut d.facts
                {
                    *context_window_tokens = 128_000;
                }
            }),
            ("facts.supports_tools", |d| {
                if let AgentComponentFacts::ModelProfile { supports_tools, .. } = &mut d.facts {
                    *supports_tools = false;
                }
            }),
        ];
        for (field, mutate) in mutations {
            let mut altered = model();
            mutate(&mut altered);
            let digest = AgentComponentEntry::publish(altered, 1, 1_000)
                .unwrap_or_else(|error| panic!("{field}: {error}"))
                .definition_digest;
            assert_ne!(digest, baseline, "{field} is stored but not bound");
        }
    }

    // ---- ingestion provenance ----

    /// The pinned MCP server every served component's provenance names.
    fn mcp_server_pin(seed: char) -> ComponentDependency {
        ComponentDependency {
            component_id: "mcp:search-server".into(),
            kind: AgentComponentKind::McpServer,
            definition_digest: digest(seed),
        }
    }

    fn mcp_tool(component_id: &str, capability: &str, effect: ToolEffect) -> AgentComponentDraft {
        let mut source = draft(component_id, AgentComponentKind::Tool);
        source.facts = tool_facts(effect, Vec::new());
        source.provenance = ComponentProvenance::McpServer {
            server: mcp_server_pin('a'),
            upstream_name: "search".into(),
        };
        source.classification = vec![capability.into()];
        source
    }

    #[test]
    fn an_mcp_served_component_must_name_its_server() {
        // Without this, a re-ingest cannot tell which records belong to a
        // server, and "which agents break if this server changes?" degrades to
        // a scan over free text.
        let mut orphan = draft("mcp:prompt:summarize", AgentComponentKind::McpPrompt);
        orphan.provenance = ComponentProvenance::Native;
        let error = orphan.validate().expect_err("must be refused");
        assert!(error.contains("must be provenanced"), "got: {error}");

        let mut bound = orphan.clone();
        bound.provenance = ComponentProvenance::McpServer {
            server: mcp_server_pin('a'),
            upstream_name: "summarize".into(),
        };
        bound.validate().expect("a served prompt names its server");

        // The server half of the provenance is a PIN, not a bare id: it
        // carries the kind, and a provenance that names something which is not
        // an MCP server is refused before any store is opened.
        let mut mis_kinded = bound.clone();
        mis_kinded.provenance = ComponentProvenance::McpServer {
            server: ComponentDependency {
                component_id: "toolset:search".into(),
                kind: AgentComponentKind::Toolset,
                definition_digest: digest('a'),
            },
            upstream_name: "summarize".into(),
        };
        let error = mis_kinded.validate().expect_err("must be refused");
        assert!(
            error.contains("must pin an mcp_server component"),
            "got: {error}"
        );
    }

    #[test]
    fn an_mcp_server_is_not_provenanced_to_itself() {
        let mut recursive = draft("mcp:search-server", AgentComponentKind::McpServer);
        recursive.provenance = ComponentProvenance::McpServer {
            server: mcp_server_pin('a'),
            upstream_name: "self".into(),
        };
        let error = recursive.validate().expect_err("must be refused");
        assert!(
            error.contains("cannot itself be provenanced"),
            "got: {error}"
        );
    }

    #[test]
    fn a_source_package_component_records_the_package_it_came_from() {
        let mut skill = draft("skill:research", AgentComponentKind::Skill);
        skill.provenance = ComponentProvenance::SourcePackage {
            package_id: "agent-packages/research".into(),
            package_version: "2.1.0".into(),
        };
        let entry = AgentComponentEntry::publish(skill, 1, 1_000).expect("publishes");
        let ComponentProvenance::SourcePackage { package_id, .. } = &entry.provenance else {
            panic!("provenance must survive the round trip");
        };
        assert_eq!(package_id, "agent-packages/research");
    }

    // ---- the query that motivates the whole layer ----

    #[test]
    fn a_task_resolves_to_the_components_an_agent_would_need() {
        // "What does an agent trying to do research need?" answered by
        // traversal: task -> required capabilities -> components, with
        // subsumption doing the generalizing.
        let web = AgentComponentEntry::publish(
            mcp_tool(
                "tool:web-search",
                "eg:capability/retrieval/web-search",
                ToolEffect::Read,
            ),
            1,
            1_000,
        )
        .unwrap();
        let summarize = AgentComponentEntry::publish(
            mcp_tool(
                "tool:summarize",
                "eg:capability/analysis/summarize",
                ToolEffect::Read,
            ),
            1,
            1_000,
        )
        .unwrap();
        let deployer = AgentComponentEntry::publish(
            mcp_tool(
                "tool:deploy",
                "eg:capability/action/process-exec",
                ToolEffect::Write,
            ),
            1,
            1_000,
        )
        .unwrap();

        assert!(web.is_applicable_to_task("eg:task/research"));
        assert!(summarize.is_applicable_to_task("eg:task/research"));
        assert!(
            !deployer.is_applicable_to_task("eg:task/research"),
            "a deploy tool is not what a research agent needs"
        );
        assert!(deployer.is_applicable_to_task("eg:task/operate"));

        // The specific satisfies the general, and not the other way round.
        assert!(web.satisfies_capability("eg:capability/retrieval"));
        assert!(!summarize.satisfies_capability("eg:capability/retrieval"));
    }

    #[test]
    fn side_effect_is_true_when_either_source_says_so() {
        // The typed effect comes from the ingest, the classification from
        // whoever curated it. They can disagree, and "safe" is the wrong
        // default for the one property you cannot take back.
        let declared_only = AgentComponentEntry::publish(
            mcp_tool("tool:a", "eg:capability/generation/text", ToolEffect::Write),
            1,
            1_000,
        )
        .unwrap();
        assert!(declared_only.is_side_effecting(), "declared write");

        let classified_only = AgentComponentEntry::publish(
            mcp_tool(
                "tool:b",
                "eg:capability/action/message-send",
                ToolEffect::Read,
            ),
            1,
            1_000,
        )
        .unwrap();
        assert!(
            classified_only.is_side_effecting(),
            "classified under action"
        );

        let neither = AgentComponentEntry::publish(
            mcp_tool(
                "tool:c",
                "eg:capability/retrieval/web-search",
                ToolEffect::Read,
            ),
            1,
            1_000,
        )
        .unwrap();
        assert!(!neither.is_side_effecting());
    }

    #[test]
    fn a_retired_component_keeps_its_definition_and_digest() {
        let entry = AgentComponentEntry::publish(model(), 1, 1_000).expect("publishes");
        let tombstone = entry.retire(2, 2_000).expect("retires");
        assert_eq!(tombstone.lifecycle, AgentLibraryLifecycle::Retired);
        assert_eq!(tombstone.definition_digest, entry.definition_digest);
        assert_eq!(tombstone.facts, entry.facts);
        assert!(
            tombstone.retire(3, 3_000).is_err(),
            "retiring twice must fail"
        );
    }

    // ---- stored-row compatibility ----

    /// A model profile stored before its selection facts existed decodes with
    /// every optional fact absent -- never as zero -- and a row still carrying
    /// the deleted `provides` list is refused by name rather than read.
    #[test]
    fn a_stored_row_without_optional_facts_decodes_and_provides_is_refused() {
        let mut model = draft("model:a", AgentComponentKind::ModelProfile);
        model.facts = model_facts(8_000, 1_000);
        let entry = AgentComponentEntry::publish(model, 1, 1_000).expect("publishes");
        let mut row = serde_json::to_value(&entry).expect("encodes");
        let facts = row["facts"].as_object_mut().expect("tagged facts object");
        for optional in [
            "modalities",
            "cost",
            "latency_declared",
            "latency_observed_ref",
        ] {
            facts.remove(optional);
        }
        let decoded: AgentComponentEntry =
            serde_json::from_value(row.clone()).expect("a row without optional facts decodes");
        let AgentComponentFacts::ModelProfile {
            cost,
            latency_declared,
            latency_observed_ref,
            modalities,
            ..
        } = &decoded.facts
        else {
            panic!("model facts decode as model facts");
        };
        assert!(cost.is_none() && latency_declared.is_none() && latency_observed_ref.is_none());
        assert_eq!(modalities, &ModalityFacts::default());
        decoded
            .validate()
            .expect("the decoded row still re-derives its digest");

        row["provides"] = serde_json::json!(["eg:capability/retrieval"]);
        let refused = serde_json::from_value::<AgentComponentEntry>(row)
            .expect_err("a row carrying the deleted field is refused");
        assert!(refused.to_string().contains("provides"), "{refused}");
    }

    // ---- digest coverage ----

    /// A tool component with every optional slot populated, so no mutator below
    /// is vacuous. `draft()` leaves `content_ref`, `classification`,
    /// `requires` and `attributes` empty and its facts `Opaque`.
    fn full_draft() -> AgentComponentDraft {
        let mut full = draft("tool:search", AgentComponentKind::Tool);
        full.content_ref = Some("cas:tool:search".into());
        full.facts = tool_facts(ToolEffect::Read, vec!["scope:read".into()]);
        full.provenance = ComponentProvenance::McpServer {
            server: mcp_server_pin('a'),
            upstream_name: "search".into(),
        };
        full.classification = vec!["eg:capability/retrieval/web-search".into()];
        full.requires = vec![ComponentDependency {
            component_id: "mcp:search-server".into(),
            kind: AgentComponentKind::McpServer,
            definition_digest: digest('a'),
        }];
        full.attributes = BTreeMap::from([("vendor".to_string(), "acme".to_string())]);
        full
    }

    #[test]
    fn every_stored_definition_field_moves_the_digest() {
        // A stored-but-UNHASHED field is how a reviewed component gets
        // silently altered: the digest an approver signed off on still matches
        // after the change. The destructuring is the tripwire -- a field added
        // to `AgentComponentDraft` stops this test compiling until it is
        // covered below.
        let AgentComponentDraft {
            component_id: _,
            kind: _,
            version: _,
            content_digest: _,
            content_ref: _,
            facts: _,
            provenance: _,
            summary: _,
            classification: _,
            requires: _,
            declared_capabilities: _,
            required_capabilities: _,
            declared_required_capabilities: _,
            attributes: _,
            tenant_id: _,
            actor_scope: _,
            purpose_id: _,
            policy_digest: _,
            source_revision: _,
            source_revision_digest: _,
        } = full_draft();

        type Mutator = (&'static str, fn(&mut AgentComponentDraft));
        let mutators: &[Mutator] = &[
            ("component_id", |d| d.component_id = "tool:other".into()),
            ("kind", |d| {
                // Tool facts belong to a Tool, so the facts move with the kind
                // or publish refuses the draft for an unrelated reason.
                d.kind = AgentComponentKind::Skill;
                d.facts = AgentComponentFacts::Opaque;
            }),
            ("version", |d| d.version = "2.0.0".into()),
            ("content_digest", |d| d.content_digest = digest('c')),
            ("content_ref", |d| d.content_ref = None),
            ("facts", |d| {
                d.facts = tool_facts(ToolEffect::Write, vec!["scope:read".into()])
            }),
            // The wave's new selection facts are digest inputs too: a cost or a
            // hint that moved without moving the digest would let an approved
            // revision be re-costed under the signature that approved it.
            ("facts.input_schema_digest", |d| {
                d.facts = tool_facts_with(ToolFactOverrides {
                    input_schema_digest: Some(digest('d')),
                    ..ToolFactOverrides::default()
                })
            }),
            ("facts.read_only_hint", |d| {
                d.facts = tool_facts_with(ToolFactOverrides {
                    read_only_hint: Some(false),
                    ..ToolFactOverrides::default()
                })
            }),
            ("facts.modalities", |d| {
                d.facts = tool_facts_with(ToolFactOverrides {
                    modalities: ModalityFacts {
                        input: vec!["eg:modality/text".into()],
                        output: Vec::new(),
                    },
                    ..ToolFactOverrides::default()
                })
            }),
            ("facts.cost", |d| {
                d.facts = tool_facts_with(ToolFactOverrides {
                    cost: Some(CostFacts {
                        declared: DeclaredCost {
                            currency: "USD".into(),
                            per_call_micros: Some(10),
                            input_per_mtok_micros: None,
                            output_per_mtok_micros: None,
                        },
                        price_source: PriceSource::Publisher,
                        quality: FactQuality::Declared,
                    }),
                    ..ToolFactOverrides::default()
                })
            }),
            ("facts.latency_declared", |d| {
                d.facts = tool_facts_with(ToolFactOverrides {
                    latency_declared: Some(DeclaredLatency {
                        p50_ms: 10,
                        p95_ms: 20,
                    }),
                    ..ToolFactOverrides::default()
                })
            }),
            ("provenance", |d| d.provenance = ComponentProvenance::Native),
            // The variant's OWN fields, not just the choice of variant: an
            // `McpServer` provenance carries a pin, and a pin whose digest is
            // stored but unhashed is a reference that can be repointed at a
            // different revision of the server without moving the digest an
            // approver signed off on.
            ("provenance.server.component_id", |d| {
                d.provenance = ComponentProvenance::McpServer {
                    server: ComponentDependency {
                        component_id: "mcp:other-server".into(),
                        kind: AgentComponentKind::McpServer,
                        definition_digest: digest('a'),
                    },
                    upstream_name: "search".into(),
                }
            }),
            ("provenance.server.definition_digest", |d| {
                d.provenance = ComponentProvenance::McpServer {
                    server: mcp_server_pin('b'),
                    upstream_name: "search".into(),
                }
            }),
            // `provenance.server.kind` has no mutator: validation pins it to
            // exactly `McpServer`, so there is no PUBLISHABLE draft that
            // differs in it alone. It is hashed anyway, so the field stays
            // covered if that constraint is ever relaxed.
            ("provenance.upstream_name", |d| {
                d.provenance = ComponentProvenance::McpServer {
                    server: mcp_server_pin('a'),
                    upstream_name: "find".into(),
                }
            }),
            ("summary", |d| d.summary = "a different tool".into()),
            ("classification", |d| {
                d.classification = vec!["eg:capability/analysis/summarize".into()]
            }),
            ("requires", |d| {
                d.requires = vec![ComponentDependency {
                    component_id: "mcp:other-server".into(),
                    kind: AgentComponentKind::McpServer,
                    definition_digest: digest('b'),
                }]
            }),
            ("declared_capabilities", |d| {
                d.declared_capabilities = vec!["urn:vendor:summarize".into()]
            }),
            ("required_capabilities", |d| {
                d.required_capabilities = vec!["eg:capability/action".into()]
            }),
            ("declared_required_capabilities", |d| {
                d.declared_required_capabilities = vec!["urn:vendor:act".into()]
            }),
            ("attributes", |d| {
                d.attributes = BTreeMap::from([("vendor".to_string(), "other".to_string())])
            }),
            ("tenant_id", |d| d.tenant_id = "tenant-b".into()),
            ("actor_scope", |d| d.actor_scope = "operator".into()),
            ("purpose_id", |d| d.purpose_id = "agent-rebuild".into()),
            ("policy_digest", |d| d.policy_digest = digest('c')),
            ("source_revision", |d| d.source_revision = "rev-2".into()),
            ("source_revision_digest", |d| {
                d.source_revision_digest = digest('c')
            }),
        ];

        let baseline = AgentComponentEntry::publish(full_draft(), 1, 1_000)
            .expect("baseline publishes")
            .definition_digest;
        for (field, mutate) in mutators {
            let mut altered = full_draft();
            mutate(&mut altered);
            assert_ne!(
                altered,
                full_draft(),
                "{field}: the mutator changed nothing, so the test proves nothing"
            );
            let moved = AgentComponentEntry::publish(altered, 1, 1_000)
                .unwrap_or_else(|error| panic!("{field} must still publish: {error}"))
                .definition_digest;
            assert_ne!(
                moved, baseline,
                "{field} is stored but not covered by the definition digest"
            );
        }
    }

    #[test]
    fn declaring_a_set_in_another_order_is_the_same_component() {
        // `classification`, `requires` and `declared_capabilities` are SETS -- duplicates are
        // refused -- so declaration order carries no meaning and must not make
        // an otherwise identical publish a different revision. The sorts in
        // `definition_digest` are what makes that true; nothing else permutes
        // these lists.
        let mut base = full_draft();
        base.classification = vec![
            "eg:capability/retrieval/web-search".into(),
            "eg:capability/analysis/summarize".into(),
        ];
        base.declared_capabilities = vec![
            "eg:capability/retrieval/web-search".into(),
            "eg:capability/analysis/summarize".into(),
        ];
        base.requires = vec![
            ComponentDependency {
                component_id: "mcp:search-server".into(),
                kind: AgentComponentKind::McpServer,
                definition_digest: digest('a'),
            },
            ComponentDependency {
                component_id: "mcp:other-server".into(),
                kind: AgentComponentKind::McpServer,
                definition_digest: digest('b'),
            },
        ];
        let mut permuted = base.clone();
        permuted.classification.reverse();
        permuted.declared_capabilities.reverse();
        permuted.requires.reverse();
        assert_ne!(permuted, base, "the permutation must actually permute");

        assert_eq!(
            AgentComponentEntry::publish(base, 1, 1_000)
                .expect("publishes")
                .definition_digest,
            AgentComponentEntry::publish(permuted, 1, 1_000)
                .expect("publishes")
                .definition_digest
        );
    }
}
