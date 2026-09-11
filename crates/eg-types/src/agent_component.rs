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

pub const AGENT_COMPONENT_SCHEMA_VERSION: u16 = 1;
pub const AGENT_COMPONENT_DIGEST_DOMAIN: &[u8] = b"au-eg/agent-component-definition/v1";

const MAX_TEXT_BYTES: usize = 4 * 1024;
const MAX_DEPENDENCIES: usize = 256;
const MAX_CAPABILITIES: usize = 64;
const MAX_ATTRIBUTES: usize = 64;
const MAX_VARIABLES: usize = 256;
const MAX_SCOPES: usize = 64;
const DIGEST_PREFIX: &str = "sha256:";

/// What kind of part this is.
///
/// Closed on purpose. Every kind here is referenced by name from
/// [`crate::agent_library::AgentLibraryEntry`] or
/// [`crate::agent_library::AgentRuntimeContract`] or
/// [`crate::agent_graph`], so the set is the set of things an agent is
/// actually built out of — not an open taxonomy.
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
        }
    }
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

/// How a prompt's text is resolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum PromptMode {
    /// Fixed at publish time; `content_digest` pins the text itself.
    Static,
    /// Produced per run; `content_digest` can only pin the FUNCTION, so the
    /// text the model saw must be bound again at resolution.
    Dynamic,
}

/// Whether invoking a tool can change anything.
///
/// Not a hint. A read-only agent graph is one whose reachable tools are all
/// [`ToolEffect::Read`], and that is a property worth being able to prove
/// before a run rather than discover during one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ToolEffect {
    Read,
    Write,
}

/// Kind-specific facts an optimizer needs in order to CHOOSE a component.
///
/// Typed for the kinds selection actually turns on, and
/// [`AgentComponentFacts::Opaque`] for the rest. New facts go in
/// `attributes` on the entry rather than as new variants, so extending the
/// vocabulary does not break the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "facts", rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum AgentComponentFacts {
    ModelProfile {
        provider: String,
        model_identity: String,
        context_window_tokens: u32,
        max_output_tokens: u32,
        supports_tools: bool,
        supports_structured_output: bool,
        supports_vision: bool,
    },
    SystemPrompt {
        prompt_mode: PromptMode,
        /// Budgeting input: a graph's prompts have to fit its model's context.
        token_estimate: u32,
        /// Names a dynamic prompt expects to be given. Empty for a static one.
        variables: Vec<String>,
    },
    Tool {
        effect: ToolEffect,
        /// Authz actions a caller must hold. An agent that cannot hold them
        /// cannot be given this tool.
        required_scopes: Vec<String>,
    },
    Toolset {
        transport: ToolsetTransport,
    },
    /// Every other kind. Structure and dependencies still apply; there is just
    /// nothing kind-specific that selection turns on.
    Opaque,
}

/// Where a toolset's tools come from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ToolsetTransport {
    /// An MCP server.
    Mcp,
    /// In-process functions.
    Function,
    /// A skill pack.
    Skill,
}

impl AgentComponentFacts {
    /// The kind these facts are only valid for, or `None` for `Opaque`.
    fn required_kind(&self) -> Option<AgentComponentKind> {
        match self {
            Self::ModelProfile { .. } => Some(AgentComponentKind::ModelProfile),
            Self::SystemPrompt { .. } => Some(AgentComponentKind::SystemPrompt),
            Self::Tool { .. } => Some(AgentComponentKind::Tool),
            Self::Toolset { .. } => Some(AgentComponentKind::Toolset),
            Self::Opaque => None,
        }
    }

    fn label(&self) -> &'static str {
        match self {
            Self::ModelProfile { .. } => "model_profile",
            Self::SystemPrompt { .. } => "system_prompt",
            Self::Tool { .. } => "tool",
            Self::Toolset { .. } => "toolset",
            Self::Opaque => "opaque",
        }
    }

    fn validate(&self) -> Result<(), String> {
        match self {
            Self::ModelProfile {
                provider,
                model_identity,
                context_window_tokens,
                max_output_tokens,
                ..
            } => {
                validate_text("provider", provider)?;
                validate_text("model_identity", model_identity)?;
                if *context_window_tokens == 0 {
                    return Err("agent component context_window_tokens must be non-zero".into());
                }
                if *max_output_tokens == 0 {
                    return Err("agent component max_output_tokens must be non-zero".into());
                }
                // A model that cannot emit as much as its own window claims is
                // a transcription error, and it would make every budget
                // computed from these two numbers wrong.
                if max_output_tokens > context_window_tokens {
                    return Err(
                        "agent component max_output_tokens exceeds its context window".into(),
                    );
                }
                Ok(())
            }
            Self::SystemPrompt {
                prompt_mode,
                variables,
                ..
            } => {
                if *prompt_mode == PromptMode::Static && !variables.is_empty() {
                    return Err(
                        "agent component static prompt cannot declare variables: nothing \
                         resolves them"
                            .into(),
                    );
                }
                validate_names("variables", variables, MAX_VARIABLES)
            }
            Self::Tool {
                required_scopes, ..
            } => validate_names("required_scopes", required_scopes, MAX_SCOPES),
            Self::Toolset { .. } | Self::Opaque => Ok(()),
        }
    }
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
    /// Ingested by listing an MCP server's surface. `server_component_id`
    /// points at the [`AgentComponentKind::McpServer`] record, so the server
    /// is one node every tool/prompt/resource it serves hangs off.
    McpServer {
        server_component_id: String,
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
                server_component_id,
                upstream_name,
            } => {
                validate_text("provenance server_component_id", server_component_id)?;
                validate_text("provenance upstream_name", upstream_name)
            }
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
    /// Capability names this component satisfies. The matching half of
    /// `requires` for selection: an optimizer looks for a component that
    /// PROVIDES what a role needs.
    #[serde(default)]
    pub provides: Vec<String>,
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
    pub provides: Vec<String>,
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
            provides: draft.provides,
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
            provides: self.provides.clone(),
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
    pub fn validate(&self) -> Result<(), String> {
        for (field, value) in [
            ("component_id", self.component_id.as_str()),
            ("version", self.version.as_str()),
            ("tenant_id", self.tenant_id.as_str()),
            ("actor_scope", self.actor_scope.as_str()),
            ("purpose_id", self.purpose_id.as_str()),
            ("source_revision", self.source_revision.as_str()),
        ] {
            validate_text(field, value)?;
        }
        for (field, value) in [
            ("content_digest", self.content_digest.as_str()),
            ("policy_digest", self.policy_digest.as_str()),
            ("source_revision_digest", self.source_revision_digest.as_str()),
        ] {
            validate_digest(field, value)?;
        }
        if let Some(content_ref) = &self.content_ref {
            validate_text("content_ref", content_ref)?;
        }

        // Facts and kind must agree. Without this a `Tool` could carry model
        // facts, and every query that reads facts by kind would be wrong in a
        // way nothing else detects.
        if let Some(required) = self.facts.required_kind() {
            if required != self.kind {
                return Err(format!(
                    "agent component kind '{}' cannot carry '{}' facts",
                    self.kind.as_str(),
                    self.facts.label()
                ));
            }
        }
        self.facts.validate()?;
        self.provenance.validate()?;
        validate_text("summary", &self.summary)?;

        // An MCP tool/prompt/resource must name the server it came from, and
        // only those kinds may. Without this a re-ingest cannot tell which
        // records belong to a server, so "which agents break if this server
        // changes?" degrades to a scan over free text.
        let mcp_served = matches!(
            self.kind,
            AgentComponentKind::McpPrompt | AgentComponentKind::McpResource
        );
        match (&self.provenance, mcp_served) {
            (ComponentProvenance::McpServer { .. }, _) if self.kind == AgentComponentKind::McpServer => {
                return Err(
                    "an mcp_server component cannot itself be provenanced to an mcp server"
                        .to_string(),
                )
            }
            (provenance, true) if !matches!(provenance, ComponentProvenance::McpServer { .. }) => {
                return Err(format!(
                    "agent component kind '{}' must be provenanced to the mcp server that \
                     serves it, got '{}'",
                    self.kind.as_str(),
                    provenance.label()
                ))
            }
            _ => {}
        }

        validate_names("classification", &self.classification, MAX_CAPABILITIES)?;

        if self.requires.len() > MAX_DEPENDENCIES {
            return Err("agent component has too many dependencies".to_string());
        }
        let mut seen = BTreeSet::new();
        for dependency in &self.requires {
            validate_text("dependency component_id", &dependency.component_id)?;
            validate_digest("dependency definition_digest", &dependency.definition_digest)?;
            // Self-reference is the one cycle a single record CAN express, and
            // it is unrepresentable in a valid one: a dependency pins a digest,
            // and a component's own digest covers its dependencies, so pinning
            // yourself is a hash preimage. Rejecting by id makes the intent
            // explicit rather than relying on that.
            if dependency.component_id == self.component_id {
                return Err(format!(
                    "agent component '{}' cannot require itself",
                    self.component_id
                ));
            }
            if !seen.insert((&dependency.component_id, dependency.kind)) {
                return Err(format!(
                    "agent component requires '{}' ({}) twice",
                    dependency.component_id,
                    dependency.kind.as_str()
                ));
            }
        }
        validate_names("provides", &self.provides, MAX_CAPABILITIES)?;

        if self.attributes.len() > MAX_ATTRIBUTES {
            return Err("agent component has too many attributes".to_string());
        }
        for (name, value) in &self.attributes {
            validate_text("attribute name", name)?;
            if value.len() > MAX_TEXT_BYTES {
                return Err("agent component attribute value exceeds its size limit".to_string());
            }
        }
        Ok(())
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
    let mut provides = draft.provides.clone();
    provides.sort();
    put_names(&mut hasher, &provides);
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
        } => {
            put_text(
                hasher,
                match effect {
                    ToolEffect::Read => "read",
                    ToolEffect::Write => "write",
                },
            );
            put_names(hasher, required_scopes);
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
            server_component_id,
            upstream_name,
        } => {
            put_text(hasher, server_component_id);
            put_text(hasher, upstream_name);
        }
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AgentComponentRetireRequest {
    pub context: crate::agent_library::AgentLibraryMutationContext,
    pub component_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AgentComponentStatusRequest {
    pub context: crate::agent_library::AgentLibraryMutationContext,
    pub component_id: String,
    pub kind: AgentComponentMutationKind,
}

/// Find components by what they can do.
///
/// The wire form of *"what does an agent trying to do XYZ need?"*. Either
/// `task` (resolved to capabilities through the native ontology) or explicit
/// `capabilities` may be given; giving both intersects them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AgentComponentSearchRequest {
    pub tenant_id: String,
    /// An ontology task term, e.g. `eg:task/research`.
    #[serde(default)]
    pub task: Option<String>,
    /// Capability terms the component must satisfy by subsumption.
    #[serde(default)]
    pub capabilities: Vec<String>,
    /// Restrict to these kinds. Empty means every kind.
    #[serde(default)]
    pub kinds: Vec<AgentComponentKind>,
    /// When true, exclude anything side-effecting. The reason this is a
    /// first-class filter rather than a caller-side one: assembling a
    /// read-only agent is a common, security-relevant request, and a caller
    /// that has to filter afterwards can forget to.
    #[serde(default)]
    pub read_only: bool,
}

impl AgentComponentSearchRequest {
    pub fn validate(&self) -> Result<(), String> {
        validate_text("tenant_id", &self.tenant_id)?;
        if let Some(task) = &self.task {
            validate_text("task", task)?;
        }
        validate_names("capabilities", &self.capabilities, MAX_CAPABILITIES)?;
        if self.kinds.len() > 16 {
            return Err("agent component search names too many kinds".to_string());
        }
        if self.task.is_none() && self.capabilities.is_empty() {
            return Err(
                "agent component search needs a task or at least one capability".to_string(),
            );
        }
        Ok(())
    }

    /// The capabilities a component must satisfy to match.
    pub fn required_capabilities(&self) -> Vec<String> {
        let mut required: Vec<String> = self.capabilities.clone();
        if let Some(task) = &self.task {
            for capability in crate::agent_ontology::capabilities_for_task(task) {
                if !required.iter().any(|existing| existing == capability) {
                    required.push((*capability).to_string());
                }
            }
        }
        required
    }

    /// Whether one component answers this search.
    pub fn matches(&self, component: &AgentComponentEntry) -> bool {
        if component.lifecycle != AgentLibraryLifecycle::Published {
            return false;
        }
        if !self.kinds.is_empty() && !self.kinds.contains(&component.kind) {
            return false;
        }
        if self.read_only && component.is_side_effecting() {
            return false;
        }
        // ANY, not ALL: a component is a part. A research agent needs
        // retrieval AND summarization, and no single tool provides both --
        // requiring every capability of a task would return nothing.
        self.required_capabilities()
            .iter()
            .any(|required| component.satisfies_capability(required))
    }
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
}

impl AgentComponentOp {
    pub fn is_mutation(&self) -> bool {
        matches!(self, Self::Publish { .. } | Self::Retire { .. })
    }

    pub fn tenant_id(&self) -> &str {
        match self {
            Self::Publish { request } => &request.context.tenant_id,
            Self::Retire { request } => &request.context.tenant_id,
            Self::Status { request } => &request.context.tenant_id,
            Self::Search { request } => &request.tenant_id,
            Self::Current { tenant_id, .. } | Self::History { tenant_id, .. } => tenant_id,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        match self {
            Self::Publish { request } => {
                request.context.validate()?;
                request.component.validate()?;
                if request.context.tenant_id != request.component.tenant_id {
                    return Err(
                        "agent component publish context tenant does not match the component's"
                            .to_string(),
                    );
                }
                Ok(())
            }
            Self::Retire { request } => {
                request.context.validate()?;
                validate_text("component_id", &request.component_id)
            }
            Self::Status { request } => {
                request.context.validate()?;
                validate_text("component_id", &request.component_id)
            }
            Self::Search { request } => request.validate(),
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
        let expected = match self.component.lifecycle {
            AgentLibraryLifecycle::Published => AgentComponentMutationKind::Publish,
            AgentLibraryLifecycle::Retired => AgentComponentMutationKind::Retire,
        };
        if self.kind != expected {
            return Err(
                "agent component outbox kind does not match the entry's lifecycle".to_string(),
            );
        }
        validate_text("performing_actor", &self.performing_actor)?;
        validate_text("action_actor_scope", &self.action_actor_scope)
    }
}

impl AgentComponentEntry {
    /// Whether this component answers a need for `required_capability`.
    ///
    /// Subsumption, not equality: a component classified
    /// `eg:capability/retrieval/web-search` satisfies a need for
    /// `eg:capability/retrieval`. The direction matters and is asymmetric --
    /// see [`crate::agent_ontology::satisfies`].
    pub fn satisfies_capability(&self, required_capability: &str) -> bool {
        self.classification
            .iter()
            .any(|provided| crate::agent_ontology::satisfies(provided, required_capability))
    }

    /// Whether this component is usable for `task_iri` -- it satisfies at
    /// least one capability that task requires.
    ///
    /// The native half of *"what does an agent trying to do XYZ need?"*: the
    /// task resolves to capabilities through the baked-in ontology, and those
    /// match components by subsumption. No model in the loop, and the answer
    /// is reproducible.
    pub fn is_applicable_to_task(&self, task_iri: &str) -> bool {
        crate::agent_ontology::capabilities_for_task(task_iri)
            .iter()
            .any(|required| self.satisfies_capability(required))
    }

    /// Whether using this component can change anything.
    ///
    /// True when it is a write tool, or when it is classified anywhere under
    /// `eg:capability/action`. Both are checked because the two facts come
    /// from different places -- the typed [`ToolEffect`] is declared by the
    /// ingest, the classification by whoever curated it -- and a component is
    /// side-effecting if EITHER says so. Treating a disagreement as "safe"
    /// would be the wrong default for the one property you cannot take back.
    pub fn is_side_effecting(&self) -> bool {
        let declared_write = matches!(
            self.facts,
            AgentComponentFacts::Tool {
                effect: ToolEffect::Write,
                ..
            }
        );
        declared_write
            || self
                .classification
                .iter()
                .any(|term| crate::agent_ontology::is_a(term, "eg:capability/action"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
            provides: Vec::new(),
            attributes: BTreeMap::new(),
            tenant_id: "tenant-a".into(),
            actor_scope: "agent-builder".into(),
            purpose_id: "agent-construction".into(),
            policy_digest: digest('7'),
            source_revision: "rev-1".into(),
            source_revision_digest: digest('8'),
        }
    }

    fn model() -> AgentComponentDraft {
        let mut source = draft("model:opus", AgentComponentKind::ModelProfile);
        source.facts = AgentComponentFacts::ModelProfile {
            provider: "anthropic".into(),
            model_identity: "claude-opus-5".into(),
            context_window_tokens: 200_000,
            max_output_tokens: 64_000,
            supports_tools: true,
            supports_structured_output: true,
            supports_vision: true,
        };
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
        mismatched.facts = AgentComponentFacts::ModelProfile {
            provider: "anthropic".into(),
            model_identity: "claude-opus-5".into(),
            context_window_tokens: 1_000,
            max_output_tokens: 100,
            supports_tools: true,
            supports_structured_output: true,
            supports_vision: false,
        };
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
        broken.facts = AgentComponentFacts::ModelProfile {
            provider: "anthropic".into(),
            model_identity: "claude-opus-5".into(),
            context_window_tokens: 1_000,
            max_output_tokens: 2_000,
            supports_tools: true,
            supports_structured_output: true,
            supports_vision: false,
        };
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
        assert!(error.contains("nothing \\\n                         resolves them") || error.contains("resolves them"), "got: {error}");
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
            AgentComponentEntry::publish(a, 1, 1).unwrap().definition_digest,
            AgentComponentEntry::publish(b, 1, 1).unwrap().definition_digest
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

        let mutations: Vec<(&str, fn(&mut AgentComponentDraft))> = vec![
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
            ("provides", |d| d.provides = vec!["cap:reasoning".into()]),
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

    fn mcp_tool(component_id: &str, capability: &str, effect: ToolEffect) -> AgentComponentDraft {
        let mut source = draft(component_id, AgentComponentKind::Tool);
        source.facts = AgentComponentFacts::Tool {
            effect,
            required_scopes: Vec::new(),
        };
        source.provenance = ComponentProvenance::McpServer {
            server_component_id: "mcp:search-server".into(),
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
            server_component_id: "mcp:search-server".into(),
            upstream_name: "summarize".into(),
        };
        bound.validate().expect("a served prompt names its server");
    }

    #[test]
    fn an_mcp_server_is_not_provenanced_to_itself() {
        let mut recursive = draft("mcp:search-server", AgentComponentKind::McpServer);
        recursive.provenance = ComponentProvenance::McpServer {
            server_component_id: "mcp:search-server".into(),
            upstream_name: "self".into(),
        };
        let error = recursive.validate().expect_err("must be refused");
        assert!(error.contains("cannot itself be provenanced"), "got: {error}");
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
            mcp_tool("tool:web-search", "eg:capability/retrieval/web-search", ToolEffect::Read),
            1,
            1_000,
        )
        .unwrap();
        let summarize = AgentComponentEntry::publish(
            mcp_tool("tool:summarize", "eg:capability/analysis/summarize", ToolEffect::Read),
            1,
            1_000,
        )
        .unwrap();
        let deployer = AgentComponentEntry::publish(
            mcp_tool("tool:deploy", "eg:capability/action/process-exec", ToolEffect::Write),
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
            mcp_tool("tool:b", "eg:capability/action/message-send", ToolEffect::Read),
            1,
            1_000,
        )
        .unwrap();
        assert!(classified_only.is_side_effecting(), "classified under action");

        let neither = AgentComponentEntry::publish(
            mcp_tool("tool:c", "eg:capability/retrieval/web-search", ToolEffect::Read),
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
        assert!(tombstone.retire(3, 3_000).is_err(), "retiring twice must fail");
    }

    // ---- digest coverage ----

    /// A tool component with every optional slot populated, so no mutator below
    /// is vacuous. `draft()` leaves `content_ref`, `classification`,
    /// `requires`, `provides` and `attributes` empty and its facts `Opaque`.
    fn full_draft() -> AgentComponentDraft {
        let mut full = draft("tool:search", AgentComponentKind::Tool);
        full.content_ref = Some("cas:tool:search".into());
        full.facts = AgentComponentFacts::Tool {
            effect: ToolEffect::Read,
            required_scopes: vec!["scope:read".into()],
        };
        full.provenance = ComponentProvenance::McpServer {
            server_component_id: "mcp:search-server".into(),
            upstream_name: "search".into(),
        };
        full.classification = vec!["eg:capability/retrieval/web-search".into()];
        full.requires = vec![ComponentDependency {
            component_id: "mcp:search-server".into(),
            kind: AgentComponentKind::McpServer,
            definition_digest: digest('a'),
        }];
        full.provides = vec!["eg:capability/retrieval/web-search".into()];
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
            provides: _,
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
                d.facts = AgentComponentFacts::Tool {
                    effect: ToolEffect::Write,
                    required_scopes: vec!["scope:read".into()],
                }
            }),
            ("provenance", |d| d.provenance = ComponentProvenance::Native),
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
            ("provides", |d| {
                d.provides = vec!["eg:capability/analysis/summarize".into()]
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
        // `classification`, `requires` and `provides` are SETS -- duplicates are
        // refused -- so declaration order carries no meaning and must not make
        // an otherwise identical publish a different revision. The sorts in
        // `definition_digest` are what makes that true; nothing else permutes
        // these lists.
        let mut base = full_draft();
        base.classification = vec![
            "eg:capability/retrieval/web-search".into(),
            "eg:capability/analysis/summarize".into(),
        ];
        base.provides = vec![
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
        permuted.provides.reverse();
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
