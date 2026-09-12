//! Durable identity contract for the RF-020 Agent Library.
//!
//! The Agent Library records the identity of a built agent, not its runtime
//! execution.  Component contents remain owned by their source/package
//! systems; this contract carries the immutable references and digests that
//! make one built definition reproducible and policy/provenance visible.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

use crate::agent_component::AgentComponentKind;
use crate::contract::Nonce;
use crate::mutation_batch::ScopeTenantId;

/// Advanced whenever the meaning of a stored entry changes, so a reader built
/// against the older meaning gives a typed version rejection instead of a
/// confusing digest mismatch. RF-ADR-008 took it to 4 when
/// [`AgentRuntimeContract`] joined `definition_digest`; the pre-freeze contract
/// review takes it to 5, because [`AgentLibraryEntry::tool_surface_digest`]
/// -- the value delegation admits as the capability proof -- now covers
/// `runtime.toolset_refs` as well as `tools`. The stored fields are unchanged,
/// but what an entry PROVES is not, and a reader that computes the old proof
/// from a v5 entry would under-state the agent's tool surface.
pub const AGENT_LIBRARY_ENTRY_SCHEMA_VERSION: u16 = 5;
pub const AGENT_LIBRARY_OUTBOX_SCHEMA_VERSION: u16 = 1;
pub const AGENT_LIBRARY_RESULT_SCHEMA_VERSION: u16 = 1;
pub const AGENT_LIBRARY_RESULT_SCHEMA_ID: &str = "agent-library-result.v1";
/// Hash-domain separator. Advanced with the schema version so two entries that
/// mean different things cannot share a definition digest even when their
/// remaining fields match -- at v5, two byte-identical entries carry different
/// capability proofs, so they are not the same definition. This is a
/// format-identity constant, which RF-ADR-006 exempts from the
/// no-version-suffixes rule.
pub const AGENT_LIBRARY_DEFINITION_DIGEST_DOMAIN: &[u8] = b"au-eg/agent-library-definition/v5";

/// Hash-domain separator for [`AgentLibraryEntry::tool_surface_digest`], the
/// value delegation admits as an agent's capability proof. Separate from the
/// definition domain because the two answer different questions: the definition
/// digest is "is this the same agent?", the tool-surface digest is "does this
/// agent hold exactly these tools?". Also a format-identity constant
/// (RF-ADR-006).
pub const AGENT_LIBRARY_TOOL_SURFACE_DIGEST_DOMAIN: &[u8] = b"au-eg/agent-library-tool-surface/v1";

const MAX_TEXT_BYTES: usize = 4 * 1024;
const MAX_REFERENCE_COUNT: usize = 1_024;
const MAX_REFERENCE_BYTES: usize = 4 * 1024;
const DIGEST_PREFIX: &str = "sha256:";

/// The retained lifecycle of one Agent Library definition.
///
/// `Retired` is a durable tombstone.  It remains in the revision stream and
/// cannot be replaced by a later publish, preserving the definition's
/// provenance and preventing silent resurrection of an agent identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum AgentLibraryLifecycle {
    Published,
    Retired,
}

pub use crate::agent_component::ComponentDependency;

/// Whether the system prompt is fixed at publish time or resolved per run.
///
/// pydantic-ai distinguishes `Agent(system_prompt=…)` from `@agent.system_prompt`
/// / `@agent.instructions`. The distinction is durable identity, not style: a
/// static prompt's digest pins the prompt itself, while a dynamic prompt's
/// digest can only pin the FUNCTION that produces it. A dynamic prompt must
/// therefore be digest-bound again at resolution time, and a reader that cannot
/// tell the two apart cannot know whether `system_prompt_digest` covers the text
/// the model actually saw.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum AgentPromptMode {
    Static,
    Dynamic,
}

/// How the model is asked to produce the agent's structured output.
///
/// Mirrors pydantic-ai's `ToolOutput` / `NativeOutput` / `PromptedOutput`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum AgentOutputMode {
    /// Free text — pydantic-ai's default `output_type=str`.
    Text,
    /// A synthesized tool call carries the structured result.
    Tool,
    /// The provider's own structured-output mode.
    Native,
    /// The schema is described in the prompt and parsed from the reply.
    Prompted,
}

/// Bounded model-invocation settings (pydantic-ai `ModelSettings`).
///
/// Every continuous setting is an INTEGER in thousandths, never a float. These
/// values are hashed into `definition_digest`, and floats have no canonical
/// byte form for that purpose: `NaN != NaN` breaks `Eq`, `0.0` and `-0.0` are
/// equal but hash differently, and the same value can format differently across
/// platforms and serde versions. A reproducible definition digest cannot be
/// built on any of that, so the wire type carries milli-units and the harness
/// divides by 1000 when it constructs the model settings.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AgentModelSettings {
    /// Sampling temperature in thousandths (`700` = 0.7).
    pub temperature_milli: Option<u32>,
    /// Nucleus-sampling mass in thousandths (`950` = 0.95).
    pub top_p_milli: Option<u32>,
    pub max_output_tokens: Option<u32>,
    pub seed: Option<u64>,
    pub parallel_tool_calls: Option<bool>,
    pub stop_sequences: Vec<String>,
    pub request_timeout_ms: Option<u32>,
}

/// Retry budgets that belong to the agent definition, not to one delegation.
///
/// Distinct from `KgDelegateRequest::max_attempts`, which is how many times the
/// ENGINE will re-admit the work item. These are how many times the agent will
/// ask the model again within a single run after a tool error or an output that
/// fails validation (pydantic-ai `retries` / `output_retries`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AgentRetryPolicy {
    pub model_retries: u16,
    pub output_retries: u16,
}

/// Per-run consumption ceilings (pydantic-ai `UsageLimits`).
///
/// `None` means "the engine's bounded default", never "unlimited" — an agent
/// definition cannot opt out of a ceiling, only decline to tighten it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AgentUsageLimits {
    pub request_limit: Option<u32>,
    pub input_tokens_limit: Option<u64>,
    pub output_tokens_limit: Option<u64>,
    pub tool_calls_limit: Option<u32>,
}

/// The part of an agent's definition that governs how it RUNS, as opposed to
/// which components it is assembled from.
///
/// Grouped into one nested type rather than flattened into
/// [`AgentLibraryEntryDraft`] deliberately. The draft, the entry, `create`,
/// `retire` and `as_draft` each restate every field, so a flat shape would pay
/// five edits per field and grow a 23-field struct to 35 — the exact sprawl
/// RF-ADR-008 exists to avoid. It also names a real concept: two agents with
/// the same components but different output contracts are different agents.
///
/// Every field participates in `definition_digest`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AgentRuntimeContract {
    /// The agent's typed input (pydantic-ai `deps_type`). `None` = no deps.
    pub deps_contract: Option<ComponentDependency>,
    /// The agent's typed output (pydantic-ai `output_type`). `None` with
    /// [`AgentOutputMode::Text`] is the plain-text default.
    pub output_contract: Option<ComponentDependency>,
    pub output_mode: Option<AgentOutputMode>,
    /// `@agent.output_validator` functions, by reference.
    pub output_validator_refs: Vec<ComponentDependency>,
    /// Grouped tool sources — a `FunctionToolset`, an MCP server, a skill pack.
    ///
    /// `tools` on the entry is the flattened resolution of these WHERE THE
    /// PUBLISHER CAN FLATTEN THEM, so a reader that only cares which individual
    /// tools exist need not walk the grouping. The engine does not, and cannot,
    /// enforce that: a toolset is an L1 reference, its contents live in the
    /// system that owns it, and resolving an MCP server's tool list is not
    /// something this contract can do at publish time. Treating the flattening
    /// as an invariant would therefore be asserting a property nothing checks.
    ///
    /// So the capability proof covers BOTH lists instead — see
    /// [`AgentLibraryEntry::tool_surface_digest`]. An agent that names a toolset
    /// it did not flatten still has that toolset inside the digest delegation
    /// admits, rather than a proof over a strict subset of what it can call.
    pub toolset_refs: Vec<ComponentDependency>,
    pub model_settings: AgentModelSettings,
    pub retry_policy: AgentRetryPolicy,
    pub usage_limits: AgentUsageLimits,
    /// `None` is read as [`AgentPromptMode::Static`].
    pub prompt_mode: Option<AgentPromptMode>,
}

impl AgentRuntimeContract {
    pub fn prompt_mode(&self) -> AgentPromptMode {
        self.prompt_mode.unwrap_or(AgentPromptMode::Static)
    }

    pub fn output_mode(&self) -> AgentOutputMode {
        self.output_mode.unwrap_or(AgentOutputMode::Text)
    }

    fn validate(&self) -> Result<(), String> {
        if let Some(deps) = &self.deps_contract {
            validate_dependency("deps_contract", deps, AgentComponentKind::Schema)?;
        }
        if let Some(output) = &self.output_contract {
            validate_dependency("output_contract", output, AgentComponentKind::Schema)?;
        }
        // A structured output mode with no contract has nothing to structure
        // against, and would be silently served as text.
        if matches!(
            self.output_mode(),
            AgentOutputMode::Tool | AgentOutputMode::Native | AgentOutputMode::Prompted
        ) && self.output_contract.is_none()
        {
            return Err(
                "agent library output_mode requires an output_contract to structure against"
                    .to_string(),
            );
        }
        validate_dependencies(
            "output_validator_refs",
            &self.output_validator_refs,
            AgentComponentKind::OutputValidator,
            true,
        )?;
        validate_dependencies(
            "toolset_refs",
            &self.toolset_refs,
            AgentComponentKind::Toolset,
            true,
        )?;
        validate_optional_refs("stop_sequences", &self.model_settings.stop_sequences)?;
        for (field, value) in [
            ("temperature_milli", self.model_settings.temperature_milli),
            ("top_p_milli", self.model_settings.top_p_milli),
        ] {
            // Thousandths of a probability-like setting: 0..=2000 covers every
            // provider's accepted temperature range and refuses a caller that
            // passed a raw float (`0.7` truncated to `0`) or a percentage.
            if value.is_some_and(|milli| milli > 2_000) {
                return Err(format!("agent library {field} is out of range"));
            }
        }
        Ok(())
    }
}

/// Caller-supplied, immutable definition inputs for one built agent.
///
/// Every component is represented by an opaque reference and a digest.  Raw
/// prompts, secrets, tool output, and runtime context deliberately have no
/// field in this type and therefore cannot become part of durable identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AgentLibraryEntryDraft {
    pub agent_id: String,
    pub package_id: String,
    pub version: String,
    pub role: String,
    pub role_digest: String,
    /// The components this agent is assembled from, each pinned to an exact
    /// L1 revision (RF-ADR-008). These were opaque `*_ref` strings plus a
    /// per-set digest; pinning them by `(component_id, kind, definition_digest)`
    /// is what connects the hierarchy:
    ///
    /// * "which agents use this tool?" becomes a traversal, not a string match;
    /// * republishing a component changes its digest, so an agent assembled
    ///   from the old one no longer resolves -- silently inheriting a changed
    ///   tool is no longer possible;
    /// * each slot's `kind` is checked, so a tool cannot be wired in where a
    ///   model profile belongs.
    ///
    /// The former `*_set_digest` fields are gone: a digest over the whole set
    /// was pinning what the per-item digests now pin individually, and
    /// `definition_digest` already covers the sorted list.
    pub system_prompt: ComponentDependency,
    pub tools: Vec<ComponentDependency>,
    pub skills: Vec<ComponentDependency>,
    pub model_profile: ComponentDependency,
    /// Kept alongside `model_profile`: the profile is the pinned component,
    /// this is the model it names. A reader answering "what model does this
    /// agent run on?" should not have to resolve a component to find out.
    pub model_identity: String,
    pub ontologies: Vec<ComponentDependency>,
    pub tenant_id: String,
    pub actor_scope: String,
    pub purpose_id: String,
    pub policy_digest: String,
    /// The source revision is an opaque, source-controlled revision identity;
    /// its content/provenance digest is carried separately below.
    pub source_revision: String,
    pub source_revision_digest: String,
    /// How this agent RUNS -- output/deps contracts, model settings, budgets.
    /// See [`AgentRuntimeContract`]. An empty contract is a plain-text agent
    /// with engine-default budgets.
    #[serde(default)]
    pub runtime: AgentRuntimeContract,
    /// Which template produced this agent, if any (RF-ADR-008 item C).
    ///
    /// `None` for a hand-authored agent. Present on an instance so "which agents
    /// came from this template?" is a traversal -- the same reason components
    /// are pinned rather than named. Otherwise inert: an instance is admitted,
    /// delegated and pinned exactly like any other agent, which is what keeps
    /// those paths free of a template-aware branch.
    #[serde(default)]
    pub instantiated_from: Option<crate::agent_template::TemplateInstanceRef>,
}

/// One durable Agent Library revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AgentLibraryEntry {
    pub schema_version: u16,
    pub agent_id: String,
    pub package_id: String,
    pub version: String,
    pub role: String,
    pub role_digest: String,
    /// The components this agent is assembled from, each pinned to an exact
    /// L1 revision (RF-ADR-008). These were opaque `*_ref` strings plus a
    /// per-set digest; pinning them by `(component_id, kind, definition_digest)`
    /// is what connects the hierarchy:
    ///
    /// * "which agents use this tool?" becomes a traversal, not a string match;
    /// * republishing a component changes its digest, so an agent assembled
    ///   from the old one no longer resolves -- silently inheriting a changed
    ///   tool is no longer possible;
    /// * each slot's `kind` is checked, so a tool cannot be wired in where a
    ///   model profile belongs.
    ///
    /// The former `*_set_digest` fields are gone: a digest over the whole set
    /// was pinning what the per-item digests now pin individually, and
    /// `definition_digest` already covers the sorted list.
    pub system_prompt: ComponentDependency,
    pub tools: Vec<ComponentDependency>,
    pub skills: Vec<ComponentDependency>,
    pub model_profile: ComponentDependency,
    /// Kept alongside `model_profile`: the profile is the pinned component,
    /// this is the model it names. A reader answering "what model does this
    /// agent run on?" should not have to resolve a component to find out.
    pub model_identity: String,
    pub ontologies: Vec<ComponentDependency>,
    pub tenant_id: String,
    pub actor_scope: String,
    pub purpose_id: String,
    pub policy_digest: String,
    pub source_revision: String,
    pub source_revision_digest: String,
    /// How this agent RUNS -- output/deps contracts, model settings, budgets.
    /// See [`AgentRuntimeContract`]. An empty contract is a plain-text agent
    /// with engine-default budgets.
    #[serde(default)]
    pub runtime: AgentRuntimeContract,
    /// Which template produced this agent, if any (RF-ADR-008 item C).
    ///
    /// `None` for a hand-authored agent. Present on an instance so "which agents
    /// came from this template?" is a traversal -- the same reason components
    /// are pinned rather than named. Otherwise inert: an instance is admitted,
    /// delegated and pinned exactly like any other agent, which is what keeps
    /// those paths free of a template-aware branch.
    #[serde(default)]
    pub instantiated_from: Option<crate::agent_template::TemplateInstanceRef>,
    pub entry_revision: u64,
    pub lifecycle: AgentLibraryLifecycle,
    /// Digest of the immutable definition fields.  Lifecycle, revision and
    /// timestamps are revision metadata and are intentionally excluded.
    pub definition_digest: String,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

impl AgentLibraryEntryDraft {
    /// Every L1 component this draft pins -- the same list
    /// [`AgentLibraryEntry::dependencies`] returns, available BEFORE a revision
    /// exists.
    ///
    /// Admission needs it there: a draft's pins are resolved against the durable
    /// component store inside the publish transaction, which happens before the
    /// entry is minted.
    pub fn dependencies(&self) -> Vec<&ComponentDependency> {
        pinned_components(
            &self.system_prompt,
            &self.model_profile,
            &self.tools,
            &self.skills,
            &self.ontologies,
            &self.runtime,
        )
    }
}

/// The one definition of "what an agent is assembled from".
///
/// Shared by the draft and the entry so the two can never drift: a slot missing
/// from one list would be a reference nothing resolves.
fn pinned_components<'a>(
    system_prompt: &'a ComponentDependency,
    model_profile: &'a ComponentDependency,
    tools: &'a [ComponentDependency],
    skills: &'a [ComponentDependency],
    ontologies: &'a [ComponentDependency],
    runtime: &'a AgentRuntimeContract,
) -> Vec<&'a ComponentDependency> {
    let mut all = vec![system_prompt, model_profile];
    all.extend(tools);
    all.extend(skills);
    all.extend(ontologies);
    all.extend(&runtime.toolset_refs);
    all.extend(&runtime.output_validator_refs);
    all.extend(runtime.deps_contract.iter());
    all.extend(runtime.output_contract.iter());
    all
}

impl AgentLibraryEntry {
    /// Every L1 component this agent is assembled from, in one list.
    ///
    /// The traversal side of the hierarchy: "which agents use this component?"
    /// is answered by asking each agent what it depends on, rather than by
    /// matching strings across six differently-named fields.
    pub fn dependencies(&self) -> Vec<&ComponentDependency> {
        pinned_components(
            &self.system_prompt,
            &self.model_profile,
            &self.tools,
            &self.skills,
            &self.ontologies,
            &self.runtime,
        )
    }

    /// Whether this agent is assembled from `component_id`, at any pinned
    /// revision.
    pub fn uses_component(&self, component_id: &str) -> bool {
        self.dependencies()
            .iter()
            .any(|dependency| dependency.component_id == component_id)
    }

    /// Whether this agent is assembled from exactly this pinned revision.
    ///
    /// The distinction from [`Self::uses_component`] is the impact question: an
    /// agent that uses a component but NOT this revision of it is one whose pin
    /// is now stale, which is precisely what a republish should surface.
    pub fn uses_component_revision(&self, component_id: &str, definition_digest: &str) -> bool {
        self.dependencies().iter().any(|dependency| {
            dependency.component_id == component_id
                && dependency.definition_digest == definition_digest
        })
    }

    /// The digest of this agent's whole TOOL SURFACE: its flat `tools` and the
    /// `toolset_refs` it draws further tools from.
    ///
    /// This is what `kg-delegate` admits as `capability_digest`, so its input
    /// set is the answer to "what may this agent call?". Covering `tools` alone
    /// made that proof cover a strict SUBSET of the surface: an entry could
    /// pin `tools: [read-only-tool]` and `toolset_refs: [mcp-server:admin]`,
    /// and the admitted proof would say nothing about the second. Both lists
    /// are hashed with their own length prefix, so moving a reference from one
    /// to the other moves the digest.
    ///
    /// DERIVED from the pinned dependencies rather than stored. A stored set
    /// digest is a second, independent statement of the same fact and CAN
    /// disagree with the set it describes -- nothing recomputed it. A derived
    /// one cannot.
    pub fn tool_surface_digest(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(AGENT_LIBRARY_TOOL_SURFACE_DIGEST_DOMAIN);
        put_dependencies(&mut hasher, &self.tools);
        put_dependencies(&mut hasher, &self.runtime.toolset_refs);
        format!("{DIGEST_PREFIX}{}", hex::encode(hasher.finalize()))
    }

    pub fn skill_set_digest(&self) -> String {
        set_digest(b"au-eg/agent-library-skill-set/v1", &self.skills)
    }

    pub fn ontology_set_digest(&self) -> String {
        set_digest(b"au-eg/agent-library-ontology-set/v1", &self.ontologies)
    }

    /// The pinned prompt component's digest.
    pub fn system_prompt_digest(&self) -> &str {
        &self.system_prompt.definition_digest
    }

    /// The pinned model-profile component's digest.
    pub fn model_profile_digest(&self) -> &str {
        &self.model_profile.definition_digest
    }

    /// The pinned prompt component's id.
    pub fn system_prompt_ref(&self) -> &str {
        &self.system_prompt.component_id
    }

    /// The pinned model-profile component's id.
    pub fn model_profile_ref(&self) -> &str {
        &self.model_profile.component_id
    }

    /// Construct the first or next published revision from a validated draft.
    pub fn publish(
        draft: AgentLibraryEntryDraft,
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

    /// Retain the same immutable definition as a tombstone at a newer revision.
    pub fn retire(&self, entry_revision: u64, retired_at_ms: u64) -> Result<Self, String> {
        self.validate()?;
        if self.lifecycle == AgentLibraryLifecycle::Retired {
            return Err("agent library entry is already retired".to_string());
        }
        if entry_revision <= self.entry_revision {
            return Err("agent library tombstone revision must advance".to_string());
        }
        let retired = Self {
            schema_version: self.schema_version,
            agent_id: self.agent_id.clone(),
            package_id: self.package_id.clone(),
            version: self.version.clone(),
            role: self.role.clone(),
            role_digest: self.role_digest.clone(),
            system_prompt: self.system_prompt.clone(),
            tools: self.tools.clone(),
            skills: self.skills.clone(),
            model_profile: self.model_profile.clone(),
            model_identity: self.model_identity.clone(),
            ontologies: self.ontologies.clone(),
            tenant_id: self.tenant_id.clone(),
            actor_scope: self.actor_scope.clone(),
            purpose_id: self.purpose_id.clone(),
            policy_digest: self.policy_digest.clone(),
            source_revision: self.source_revision.clone(),
            source_revision_digest: self.source_revision_digest.clone(),
            runtime: self.runtime.clone(),
            instantiated_from: self.instantiated_from.clone(),
            entry_revision,
            lifecycle: AgentLibraryLifecycle::Retired,
            definition_digest: self.definition_digest.clone(),
            created_at_ms: self.created_at_ms,
            updated_at_ms: retired_at_ms,
        };
        retired.validate()?;
        Ok(retired)
    }

    /// Build a typed entry and derive its immutable definition digest.
    pub fn create(
        draft: AgentLibraryEntryDraft,
        entry_revision: u64,
        lifecycle: AgentLibraryLifecycle,
        created_at_ms: u64,
        updated_at_ms: u64,
    ) -> Result<Self, String> {
        draft.validate()?;
        if entry_revision == 0 {
            return Err("agent library entry revision must start at one".to_string());
        }
        if updated_at_ms < created_at_ms {
            return Err("agent library entry update time precedes creation time".to_string());
        }
        let definition_digest = definition_digest(&draft);
        let entry = Self {
            schema_version: AGENT_LIBRARY_ENTRY_SCHEMA_VERSION,
            agent_id: draft.agent_id,
            package_id: draft.package_id,
            version: draft.version,
            role: draft.role,
            role_digest: draft.role_digest,
            system_prompt: draft.system_prompt,
            tools: draft.tools,
            skills: draft.skills,
            model_profile: draft.model_profile,
            model_identity: draft.model_identity,
            ontologies: draft.ontologies,
            tenant_id: draft.tenant_id,
            actor_scope: draft.actor_scope,
            purpose_id: draft.purpose_id,
            policy_digest: draft.policy_digest,
            source_revision: draft.source_revision,
            source_revision_digest: draft.source_revision_digest,
            runtime: draft.runtime,
            instantiated_from: draft.instantiated_from,
            entry_revision,
            lifecycle,
            definition_digest,
            created_at_ms,
            updated_at_ms,
        };
        entry.validate()?;
        Ok(entry)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != AGENT_LIBRARY_ENTRY_SCHEMA_VERSION {
            return Err("unsupported agent library entry schema".to_string());
        }
        if self.entry_revision == 0 {
            return Err("agent library entry revision must start at one".to_string());
        }
        if self.updated_at_ms < self.created_at_ms {
            return Err("agent library entry update time precedes creation time".to_string());
        }
        validate_draft(&self.as_draft())?;
        if !is_digest(&self.definition_digest) {
            return Err("agent library definition_digest is not a sha256 digest".to_string());
        }
        if self.definition_digest != definition_digest(&self.as_draft()) {
            return Err("agent library definition digest does not match its fields".to_string());
        }
        Ok(())
    }

    pub fn is_retired(&self) -> bool {
        self.lifecycle == AgentLibraryLifecycle::Retired
    }

    pub fn as_draft(&self) -> AgentLibraryEntryDraft {
        AgentLibraryEntryDraft {
            agent_id: self.agent_id.clone(),
            package_id: self.package_id.clone(),
            version: self.version.clone(),
            role: self.role.clone(),
            role_digest: self.role_digest.clone(),
            system_prompt: self.system_prompt.clone(),
            tools: self.tools.clone(),
            skills: self.skills.clone(),
            model_profile: self.model_profile.clone(),
            model_identity: self.model_identity.clone(),
            ontologies: self.ontologies.clone(),
            tenant_id: self.tenant_id.clone(),
            actor_scope: self.actor_scope.clone(),
            purpose_id: self.purpose_id.clone(),
            policy_digest: self.policy_digest.clone(),
            source_revision: self.source_revision.clone(),
            source_revision_digest: self.source_revision_digest.clone(),
            runtime: self.runtime.clone(),
            instantiated_from: self.instantiated_from.clone(),
        }
    }
}

impl AgentLibraryEntryDraft {
    pub fn validate(&self) -> Result<(), String> {
        validate_draft(self)
    }
}

/// Mutation kind carried by an Agent Library outbox event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum AgentLibraryMutationKind {
    Publish,
    Retire,
}

/// Typed payload emitted with the row/head commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AgentLibraryOutboxEvent {
    pub schema_version: u16,
    pub kind: AgentLibraryMutationKind,
    pub entry: AgentLibraryEntry,
    /// Opaque authenticated caller identity that performed this mutation.
    /// This is action provenance; the entry's immutable definition fields are
    /// retained independently below.
    pub performing_actor: String,
    pub action_actor_scope: String,
    pub action_purpose_id: String,
    pub action_policy_revision: String,
    pub action_policy_digest: String,
    pub action_policy_decision_id: String,
}

impl AgentLibraryOutboxEvent {
    pub fn new(
        kind: AgentLibraryMutationKind,
        entry: AgentLibraryEntry,
        context: &AgentLibraryMutationContext,
    ) -> Result<Self, String> {
        entry.validate()?;
        context.validate()?;
        Ok(Self {
            schema_version: AGENT_LIBRARY_OUTBOX_SCHEMA_VERSION,
            kind,
            entry,
            performing_actor: context.caller_principal.clone(),
            action_actor_scope: context.actor_scope.clone(),
            action_purpose_id: context.purpose_id.clone(),
            action_policy_revision: context.policy_revision.clone(),
            action_policy_digest: context.policy_digest.clone(),
            action_policy_decision_id: context.policy_decision_id.clone(),
        })
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != AGENT_LIBRARY_OUTBOX_SCHEMA_VERSION {
            return Err("unsupported agent library outbox schema".to_string());
        }
        self.entry.validate()?;
        validate_principal(&self.performing_actor)?;
        validate_text("action_actor_scope", &self.action_actor_scope)?;
        validate_text("action_purpose_id", &self.action_purpose_id)?;
        validate_text("action_policy_revision", &self.action_policy_revision)?;
        validate_digest("action_policy_digest", &self.action_policy_digest)?;
        validate_text("action_policy_decision_id", &self.action_policy_decision_id)?;
        let lifecycle_matches = matches!(
            (self.kind, self.entry.lifecycle),
            (
                AgentLibraryMutationKind::Publish,
                AgentLibraryLifecycle::Published
            ) | (
                AgentLibraryMutationKind::Retire,
                AgentLibraryLifecycle::Retired
            )
        );
        if lifecycle_matches {
            Ok(())
        } else {
            Err("agent library outbox kind does not match entry lifecycle".to_string())
        }
    }
}

/// Caller and authorization facts required by one Agent Library write.
///
/// The storage seam copies these values into the durable action receipt and
/// checks the tenant before admission. The request boundary derives them from
/// the verified carrier; the entry's actor scope, purpose, and policy digest
/// remain historical definition provenance and are retained independently.
/// Authentication and policy decisions remain above this pure DTO.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AgentLibraryMutationContext {
    pub request_id: u64,
    /// Serving authority for the native owner ledger. The authenticated
    /// caller is carried separately in `caller_principal`.
    pub principal: String,
    /// Privacy-safe authenticated caller identity, bound by the request
    /// boundary and copied to the action receipt/outbox actor header.
    pub caller_principal: String,
    /// The verified attempt nonce. Every Agent Library mutation must receive
    /// this from its authenticated producer; the storage owner never mints a
    /// fallback nonce because doing so would create a second replay authority.
    pub attempt_nonce: Nonce,
    pub tenant_id: String,
    pub actor_scope: String,
    pub purpose_id: String,
    pub policy_revision: String,
    pub policy_digest: String,
    pub policy_decision_id: String,
    pub idempotency_key: String,
    pub expected_revision: Option<u64>,
    pub trace_id: Option<String>,
    pub created_at_ms: u64,
}

impl AgentLibraryMutationContext {
    pub fn validate(&self) -> Result<(), String> {
        validate_key(&self.tenant_id, "tenant_id")?;
        validate_text("actor_scope", &self.actor_scope)?;
        validate_text("purpose_id", &self.purpose_id)?;
        validate_text("policy_revision", &self.policy_revision)?;
        validate_digest("policy_digest", &self.policy_digest)?;
        validate_text("policy_decision_id", &self.policy_decision_id)?;
        validate_text("idempotency_key", &self.idempotency_key)?;
        validate_principal(&self.principal)?;
        validate_principal(&self.caller_principal)?;
        if let Some(trace_id) = &self.trace_id {
            validate_text("trace_id", trace_id)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AgentLibraryPublishRequest {
    pub context: AgentLibraryMutationContext,
    pub entry: AgentLibraryEntryDraft,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AgentLibraryRetireRequest {
    pub context: AgentLibraryMutationContext,
    pub agent_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AgentLibraryStatusRequest {
    pub context: AgentLibraryMutationContext,
    pub agent_id: String,
    pub kind: AgentLibraryMutationKind,
}

/// Typed Agent Library wire operations. The persistence owner remains the
/// single implementation for every operation; this enum only carries the
/// authenticated request across the existing engine dispatch boundary.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum AgentLibraryOp {
    /// Boxed: a publish request carries the whole entry draft and is far
    /// larger than every other operation, so an unboxed variant would make
    /// each `AgentLibraryOp` -- and through it each `Method` -- pay that
    /// size. `Box` is transparent to serde, so the wire form is unchanged.
    Publish {
        request: Box<AgentLibraryPublishRequest>,
    },
    Retire {
        request: AgentLibraryRetireRequest,
    },
    Current {
        tenant_id: String,
        agent_id: String,
    },
    History {
        tenant_id: String,
        agent_id: String,
    },
    Status {
        request: AgentLibraryStatusRequest,
    },
}

/// Receipt returned by the EG Agent Library storage seam.
///
/// The entry is the exact committed revision. `replayed` is set only when the
/// caller's idempotency key reconciles to an already committed receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AgentLibraryWriteResult {
    pub entry: AgentLibraryEntry,
    pub batch_id: String,
    pub committed_version: u64,
    pub replayed: bool,
}

/// Stable domain payload persisted in the mutation receipt.
///
/// This deliberately excludes the response-only `replayed` flag.  A fresh
/// commit and every fresh-nonce replay decode this same payload; the route
/// adds `replayed` only while forming the ephemeral response.  Keeping this
/// payload separate from [`AgentLibraryOutboxEvent`] also prevents action
/// headers from becoming an accidental second result representation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AgentLibraryCommittedResult {
    pub schema_version: u16,
    pub entry: AgentLibraryEntry,
    pub batch_id: String,
    pub committed_version: u64,
}

impl AgentLibraryCommittedResult {
    pub fn new(
        entry: AgentLibraryEntry,
        batch_id: String,
        committed_version: u64,
    ) -> Result<Self, String> {
        let result = Self {
            schema_version: AGENT_LIBRARY_RESULT_SCHEMA_VERSION,
            entry,
            batch_id,
            committed_version,
        };
        result.validate()?;
        Ok(result)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != AGENT_LIBRARY_RESULT_SCHEMA_VERSION {
            return Err("unsupported agent library result schema".to_string());
        }
        self.entry.validate()?;
        if self.batch_id.is_empty()
            || self.batch_id.trim() != self.batch_id
            || self.batch_id.chars().any(char::is_control)
        {
            return Err("agent library result batch_id is invalid".to_string());
        }
        Ok(())
    }

    pub fn response(self, replayed: bool) -> AgentLibraryWriteResult {
        AgentLibraryWriteResult {
            entry: self.entry,
            batch_id: self.batch_id,
            committed_version: self.committed_version,
            replayed,
        }
    }
}

/// Validate one tenant/agent lookup key before opening a durable row range.
pub fn validate_key(tenant_id: &str, agent_id: &str) -> Result<(), String> {
    ScopeTenantId::new(tenant_id.to_string())?;
    validate_text("agent_id", agent_id)
}

fn validate_draft(draft: &AgentLibraryEntryDraft) -> Result<(), String> {
    validate_key(&draft.tenant_id, &draft.agent_id)?;
    for (field, value) in [
        ("package_id", draft.package_id.as_str()),
        ("version", draft.version.as_str()),
        ("role", draft.role.as_str()),
        ("model_identity", draft.model_identity.as_str()),
        ("actor_scope", draft.actor_scope.as_str()),
        ("purpose_id", draft.purpose_id.as_str()),
        ("source_revision", draft.source_revision.as_str()),
    ] {
        validate_text(field, value)?;
    }
    for (field, value) in [
        ("role_digest", draft.role_digest.as_str()),
        ("policy_digest", draft.policy_digest.as_str()),
        (
            "source_revision_digest",
            draft.source_revision_digest.as_str(),
        ),
    ] {
        validate_digest(field, value)?;
    }
    // Each slot accepts exactly one component kind. Without this a tool could
    // be wired where a model profile belongs, and every query that reads an
    // agent's parts by kind would be wrong in a way nothing else detects.
    validate_dependency(
        "system_prompt",
        &draft.system_prompt,
        AgentComponentKind::SystemPrompt,
    )?;
    validate_dependency(
        "model_profile",
        &draft.model_profile,
        AgentComponentKind::ModelProfile,
    )?;
    validate_dependencies("tools", &draft.tools, AgentComponentKind::Tool, false)?;
    validate_dependencies("skills", &draft.skills, AgentComponentKind::Skill, false)?;
    validate_dependencies(
        "ontologies",
        &draft.ontologies,
        AgentComponentKind::Ontology,
        false,
    )?;
    draft.runtime.validate()?;
    if let Some(instance) = &draft.instantiated_from {
        instance.validate()?;
    }
    Ok(())
}

fn validate_refs(field: &str, refs: &[String]) -> Result<(), String> {
    if refs.is_empty() || refs.len() > MAX_REFERENCE_COUNT {
        return Err(format!("{field} has an invalid item count"));
    }
    let mut unique = BTreeSet::new();
    for reference in refs {
        if reference.len() > MAX_REFERENCE_BYTES {
            return Err(format!("{field} reference exceeds its size limit"));
        }
        validate_text(field, reference)?;
        if !unique.insert(reference) {
            return Err(format!("{field} contains a duplicate reference"));
        }
    }
    Ok(())
}

fn validate_text(field: &str, value: &str) -> Result<(), String> {
    if value.is_empty()
        || value.len() > MAX_TEXT_BYTES
        || value.trim() != value
        || value.chars().any(char::is_control)
    {
        return Err(format!("agent library {field} is invalid"));
    }
    Ok(())
}

fn validate_digest(field: &str, value: &str) -> Result<(), String> {
    if !is_digest(value) {
        return Err(format!("agent library {field} is not a sha256 digest"));
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

fn validate_principal(value: &str) -> Result<(), String> {
    let valid = value
        .strip_prefix("principal:sha256:")
        .is_some_and(|digest| {
            digest.len() == 64
                && digest
                    .bytes()
                    .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
        });
    if valid {
        Ok(())
    } else {
        Err("agent library principal must be an opaque digest".to_string())
    }
}

/// The definition digest a draft would publish under.
///
/// Public so [`crate::agent_template`] can fold a template's base agent into the
/// template's own identity: a template's digest must move whenever its default
/// agent does.
pub fn draft_definition_digest(draft: &AgentLibraryEntryDraft) -> String {
    definition_digest(draft)
}

fn definition_digest(draft: &AgentLibraryEntryDraft) -> String {
    let mut hasher = Sha256::new();
    hasher.update(AGENT_LIBRARY_DEFINITION_DIGEST_DOMAIN);
    put_text(&mut hasher, &draft.agent_id);
    put_text(&mut hasher, &draft.package_id);
    put_text(&mut hasher, &draft.version);
    put_text(&mut hasher, &draft.role);
    put_text(&mut hasher, &draft.role_digest);
    put_dependency(&mut hasher, &draft.system_prompt);
    put_dependencies(&mut hasher, &draft.tools);
    put_dependencies(&mut hasher, &draft.skills);
    put_dependency(&mut hasher, &draft.model_profile);
    put_text(&mut hasher, &draft.model_identity);
    put_dependencies(&mut hasher, &draft.ontologies);
    put_text(&mut hasher, &draft.tenant_id);
    put_text(&mut hasher, &draft.actor_scope);
    put_text(&mut hasher, &draft.purpose_id);
    put_text(&mut hasher, &draft.policy_digest);
    put_text(&mut hasher, &draft.source_revision);
    put_text(&mut hasher, &draft.source_revision_digest);
    put_runtime_contract(&mut hasher, &draft.runtime);
    put_instantiated_from(&mut hasher, draft.instantiated_from.as_ref());
    format!("{DIGEST_PREFIX}{}", hex::encode(hasher.finalize()))
}

fn put_text(hasher: &mut Sha256, value: &str) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value.as_bytes());
}

/// Hash an optional value with an explicit presence discriminant.
///
/// Without the leading byte, `None` and `Some("")` hash identically, so a
/// caller could drop a component from a definition without changing its digest.
fn put_opt_text(hasher: &mut Sha256, value: Option<&str>) {
    match value {
        None => hasher.update([0u8]),
        Some(text) => {
            hasher.update([1u8]);
            put_text(hasher, text);
        }
    }
}

fn put_opt_u64(hasher: &mut Sha256, value: Option<u64>) {
    match value {
        None => hasher.update([0u8]),
        Some(number) => {
            hasher.update([1u8]);
            hasher.update(number.to_be_bytes());
        }
    }
}

fn put_component_ref(hasher: &mut Sha256, value: Option<&ComponentDependency>) {
    match value {
        None => hasher.update([0u8]),
        Some(component) => {
            hasher.update([1u8]);
            put_dependency(hasher, component);
        }
    }
}

/// Fold the runtime contract into the definition digest.
///
/// Field order here IS the wire contract: changing it changes every published
/// entry's `definition_digest`. Append new fields at the end, never insert.
fn put_runtime_contract(hasher: &mut Sha256, runtime: &AgentRuntimeContract) {
    put_component_ref(hasher, runtime.deps_contract.as_ref());
    put_component_ref(hasher, runtime.output_contract.as_ref());
    put_opt_text(
        hasher,
        runtime.output_mode.map(|mode| match mode {
            AgentOutputMode::Text => "text",
            AgentOutputMode::Tool => "tool",
            AgentOutputMode::Native => "native",
            AgentOutputMode::Prompted => "prompted",
        }),
    );
    put_dependencies(hasher, &runtime.output_validator_refs);
    put_dependencies(hasher, &runtime.toolset_refs);
    let settings = &runtime.model_settings;
    put_opt_u64(hasher, settings.temperature_milli.map(u64::from));
    put_opt_u64(hasher, settings.top_p_milli.map(u64::from));
    put_opt_u64(hasher, settings.max_output_tokens.map(u64::from));
    put_opt_u64(hasher, settings.seed);
    put_opt_u64(hasher, settings.parallel_tool_calls.map(u64::from));
    put_refs(hasher, &settings.stop_sequences);
    put_opt_u64(hasher, settings.request_timeout_ms.map(u64::from));
    hasher.update(runtime.retry_policy.model_retries.to_be_bytes());
    hasher.update(runtime.retry_policy.output_retries.to_be_bytes());
    let limits = &runtime.usage_limits;
    put_opt_u64(hasher, limits.request_limit.map(u64::from));
    put_opt_u64(hasher, limits.input_tokens_limit);
    put_opt_u64(hasher, limits.output_tokens_limit);
    put_opt_u64(hasher, limits.tool_calls_limit.map(u64::from));
    put_opt_text(
        hasher,
        runtime.prompt_mode.map(|mode| match mode {
            AgentPromptMode::Static => "static",
            AgentPromptMode::Dynamic => "dynamic",
        }),
    );
}

/// A reference list that may legitimately be empty, unlike [`validate_refs`],
/// which requires at least one entry.
fn validate_optional_refs(field: &str, refs: &[String]) -> Result<(), String> {
    if refs.is_empty() {
        return Ok(());
    }
    validate_refs(field, refs)
}

/// One pinned dependency in a slot that accepts exactly one component kind.
fn validate_dependency(
    field: &str,
    dependency: &ComponentDependency,
    expected: AgentComponentKind,
) -> Result<(), String> {
    validate_text(field, &dependency.component_id)?;
    validate_digest(field, &dependency.definition_digest)?;
    if dependency.kind != expected {
        return Err(format!(
            "agent library {field} must reference a {} component, got {}",
            expected.as_str(),
            dependency.kind.as_str()
        ));
    }
    Ok(())
}

/// A list of pinned dependencies, all of one kind.
///
/// `may_be_empty` distinguishes a slot an agent cannot do without (its tools)
/// from one it may legitimately not use (its output validators).
fn validate_dependencies(
    field: &str,
    dependencies: &[ComponentDependency],
    expected: AgentComponentKind,
    may_be_empty: bool,
) -> Result<(), String> {
    if dependencies.is_empty() {
        if may_be_empty {
            return Ok(());
        }
        return Err(format!(
            "agent library {field} must name at least one component"
        ));
    }
    if dependencies.len() > MAX_REFERENCE_COUNT {
        return Err(format!("{field} has an invalid item count"));
    }
    let mut unique = BTreeSet::new();
    for dependency in dependencies {
        validate_dependency(field, dependency, expected)?;
        if !unique.insert(&dependency.component_id) {
            return Err(format!("{field} contains a duplicate reference"));
        }
    }
    Ok(())
}

/// A digest over one pinned dependency set, order-independent.
fn set_digest(domain: &[u8], dependencies: &[ComponentDependency]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    put_dependencies(&mut hasher, dependencies);
    format!("{DIGEST_PREFIX}{}", hex::encode(hasher.finalize()))
}

/// Bind the template provenance into the definition digest.
///
/// Hashed, not merely stored: two agents with identical components but different
/// template provenance are different agents, and an instance whose recorded
/// bindings could be edited after publication would be one whose history lies.
fn put_instantiated_from(
    hasher: &mut Sha256,
    instance: Option<&crate::agent_template::TemplateInstanceRef>,
) {
    match instance {
        None => hasher.update([0u8]),
        Some(instance) => {
            hasher.update([1u8]);
            put_text(hasher, &instance.template_id);
            hasher.update(instance.entry_revision.to_be_bytes());
            put_text(hasher, &instance.definition_digest);
            hasher.update((instance.bindings.len() as u64).to_be_bytes());
            for (name, binding) in &instance.bindings {
                put_text(hasher, name);
                put_dependency(hasher, binding);
            }
        }
    }
}

fn put_dependency(hasher: &mut Sha256, dependency: &ComponentDependency) {
    put_text(hasher, &dependency.component_id);
    put_text(hasher, dependency.kind.as_str());
    put_text(hasher, &dependency.definition_digest);
}

fn put_dependencies(hasher: &mut Sha256, dependencies: &[ComponentDependency]) {
    // Sorted: two callers listing the same parts in a different order have
    // assembled the SAME agent.
    let mut sorted: Vec<&ComponentDependency> = dependencies.iter().collect();
    sorted.sort();
    hasher.update((sorted.len() as u64).to_be_bytes());
    for dependency in sorted {
        put_dependency(hasher, dependency);
    }
}

fn put_refs(hasher: &mut Sha256, refs: &[String]) {
    hasher.update((refs.len() as u64).to_be_bytes());
    for reference in refs {
        put_text(hasher, reference);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest(seed: char) -> String {
        format!("sha256:{}", seed.to_string().repeat(64))
    }

    fn dependency(component_id: &str, kind: AgentComponentKind, seed: char) -> ComponentDependency {
        ComponentDependency {
            component_id: component_id.into(),
            kind,
            definition_digest: digest(seed),
        }
    }

    fn draft() -> AgentLibraryEntryDraft {
        AgentLibraryEntryDraft {
            agent_id: "agent:example".into(),
            package_id: "package:example".into(),
            version: "1.0.0".into(),
            role: "role:researcher".into(),
            role_digest: digest('1'),
            system_prompt: dependency("prompt:example", AgentComponentKind::SystemPrompt, '2'),
            tools: vec![dependency("tool:search", AgentComponentKind::Tool, '3')],
            skills: vec![dependency("skill:research", AgentComponentKind::Skill, '4')],
            model_profile: dependency(
                "model-profile:default",
                AgentComponentKind::ModelProfile,
                '5',
            ),
            model_identity: "model:example".into(),
            ontologies: vec![dependency(
                "ontology:core",
                AgentComponentKind::Ontology,
                '6',
            )],
            tenant_id: "tenant-a".into(),
            actor_scope: "agent-builder".into(),
            purpose_id: "agent-construction".into(),
            policy_digest: digest('7'),
            source_revision: "source-revision:42".into(),
            source_revision_digest: digest('8'),
            runtime: AgentRuntimeContract::default(),
            instantiated_from: None,
        }
    }

    fn context() -> AgentLibraryMutationContext {
        AgentLibraryMutationContext {
            request_id: 1,
            principal: format!("principal:sha256:{}", "a".repeat(64)),
            caller_principal: format!("principal:sha256:{}", "b".repeat(64)),
            attempt_nonce: Nonce::from_bytes([9; 32]),
            tenant_id: "tenant-a".into(),
            actor_scope: "agent-builder".into(),
            purpose_id: "agent-construction".into(),
            policy_revision: "policy-v1".into(),
            policy_digest: digest('7'),
            policy_decision_id: "decision:1".into(),
            idempotency_key: "request:1".into(),
            expected_revision: Some(0),
            trace_id: Some("trace:1".into()),
            created_at_ms: 10,
        }
    }

    #[test]
    fn definition_digest_is_stable_across_revision_metadata() {
        let published = AgentLibraryEntry::publish(draft(), 1, 10).unwrap();
        let retired = published.retire(2, 20).unwrap();
        assert_eq!(published.definition_digest, retired.definition_digest);
        assert!(retired.is_retired());
        retired.validate().unwrap();
    }

    #[test]
    fn malformed_component_digest_is_rejected() {
        let mut value = draft();
        value.policy_digest = "policy".into();
        assert!(value.validate().is_err());
    }

    #[test]
    fn outbox_event_validates_the_complete_entry() {
        let entry = AgentLibraryEntry::publish(draft(), 1, 10).unwrap();
        let event =
            AgentLibraryOutboxEvent::new(AgentLibraryMutationKind::Publish, entry, &context())
                .unwrap();
        event.validate().unwrap();
    }

    #[test]
    fn outbox_kind_cannot_relabel_a_tombstone() {
        let entry = AgentLibraryEntry::publish(draft(), 1, 10)
            .unwrap()
            .retire(2, 20)
            .unwrap();
        let event = AgentLibraryOutboxEvent {
            schema_version: AGENT_LIBRARY_OUTBOX_SCHEMA_VERSION,
            kind: AgentLibraryMutationKind::Publish,
            entry,
            performing_actor: context().caller_principal,
            action_actor_scope: context().actor_scope,
            action_purpose_id: context().purpose_id,
            action_policy_revision: context().policy_revision,
            action_policy_digest: context().policy_digest,
            action_policy_decision_id: context().policy_decision_id,
        };
        assert!(event.validate().is_err());
    }

    #[test]
    fn mutation_context_requires_an_attempt_nonce() {
        let context = context();
        let mut value = serde_json::to_value(&context).unwrap();
        let serde_json::Value::Object(fields) = &mut value else {
            panic!("mutation context must encode as a named map");
        };
        fields.remove("attempt_nonce");
        assert!(serde_json::from_value::<AgentLibraryMutationContext>(value).is_err());
    }

    #[test]
    fn committed_result_is_stable_without_replay_response_metadata() {
        let entry = AgentLibraryEntry::publish(draft(), 1, 10).unwrap();
        let result = AgentLibraryCommittedResult::new(entry.clone(), "batch:1".into(), 7).unwrap();
        let value = serde_json::to_value(&result).unwrap();
        assert_eq!(
            value.get("schema_version").and_then(|v| v.as_u64()),
            Some(1)
        );
        assert!(value.get("replayed").is_none());
        assert_eq!(result.clone().response(false).entry, entry);
        assert!(result.validate().is_ok());
    }

    // ---- RF-ADR-008: the runtime contract ----

    fn contract() -> AgentRuntimeContract {
        AgentRuntimeContract {
            deps_contract: Some(ComponentDependency {
                component_id: "contract:deps:research".into(),
                kind: AgentComponentKind::Schema,
                definition_digest: digest('a'),
            }),
            output_contract: Some(ComponentDependency {
                component_id: "contract:output:report".into(),
                kind: AgentComponentKind::Schema,
                definition_digest: digest('b'),
            }),
            output_mode: Some(AgentOutputMode::Tool),
            output_validator_refs: vec![dependency(
                "validator:cites-sources",
                AgentComponentKind::OutputValidator,
                'd',
            )],
            toolset_refs: vec![dependency(
                "toolset:mcp:search",
                AgentComponentKind::Toolset,
                'e',
            )],
            model_settings: AgentModelSettings {
                temperature_milli: Some(700),
                top_p_milli: Some(950),
                max_output_tokens: Some(4096),
                seed: Some(42),
                parallel_tool_calls: Some(true),
                stop_sequences: vec!["</done>".into()],
                request_timeout_ms: Some(60_000),
            },
            retry_policy: AgentRetryPolicy {
                model_retries: 2,
                output_retries: 1,
            },
            usage_limits: AgentUsageLimits {
                request_limit: Some(10),
                input_tokens_limit: Some(100_000),
                output_tokens_limit: Some(8_000),
                tool_calls_limit: Some(20),
            },
            prompt_mode: Some(AgentPromptMode::Dynamic),
        }
    }

    #[test]
    fn a_populated_runtime_contract_publishes_and_round_trips() {
        let mut source = draft();
        source.runtime = contract();
        let entry = AgentLibraryEntry::publish(source.clone(), 1, 1_000).expect("publishes");
        entry.validate().expect("entry is valid");
        assert_eq!(entry.runtime, contract());
        // `as_draft` is what `validate` re-derives the digest from, so a field it
        // forgets to carry would make every published entry fail its own check.
        assert_eq!(entry.as_draft(), source);
    }

    #[test]
    fn every_runtime_field_changes_the_definition_digest() {
        // The contract is only worth anything if it is actually BOUND. A field
        // that is stored but not hashed can be altered after publication without
        // invalidating the entry -- a silent swap of the model settings, the
        // output contract, or the token budget an operator approved.
        // The destructuring is the tripwire: a field added to
        // `AgentRuntimeContract` stops this test compiling until it is covered.
        let AgentRuntimeContract {
            deps_contract: _,
            output_contract: _,
            output_mode: _,
            output_validator_refs: _,
            toolset_refs: _,
            model_settings:
                AgentModelSettings {
                    temperature_milli: _,
                    top_p_milli: _,
                    max_output_tokens: _,
                    seed: _,
                    parallel_tool_calls: _,
                    stop_sequences: _,
                    request_timeout_ms: _,
                },
            retry_policy:
                AgentRetryPolicy {
                    model_retries: _,
                    output_retries: _,
                },
            usage_limits:
                AgentUsageLimits {
                    request_limit: _,
                    input_tokens_limit: _,
                    output_tokens_limit: _,
                    tool_calls_limit: _,
                },
            prompt_mode: _,
        } = contract();

        let mut base = draft();
        base.runtime = contract();
        let baseline = AgentLibraryEntry::publish(base.clone(), 1, 1_000)
            .expect("baseline publishes")
            .definition_digest;

        let mutations: Vec<(&str, fn(&mut AgentRuntimeContract))> = vec![
            ("deps_contract", |r| r.deps_contract = None),
            ("output_contract", |r| {
                r.output_contract = Some(ComponentDependency {
                    component_id: "contract:output:other".into(),
                    kind: AgentComponentKind::Schema,
                    definition_digest: digest('c'),
                })
            }),
            ("output_mode", |r| {
                r.output_mode = Some(AgentOutputMode::Native)
            }),
            ("output_validator_refs", |r| {
                r.output_validator_refs = vec![dependency(
                    "validator:other",
                    AgentComponentKind::OutputValidator,
                    'f',
                )]
            }),
            ("toolset_refs", |r| {
                r.toolset_refs = vec![dependency(
                    "toolset:other",
                    AgentComponentKind::Toolset,
                    '0',
                )]
            }),
            ("temperature_milli", |r| {
                r.model_settings.temperature_milli = Some(701)
            }),
            ("top_p_milli", |r| r.model_settings.top_p_milli = Some(951)),
            ("max_output_tokens", |r| {
                r.model_settings.max_output_tokens = Some(4097)
            }),
            ("seed", |r| r.model_settings.seed = Some(43)),
            ("parallel_tool_calls", |r| {
                r.model_settings.parallel_tool_calls = Some(false)
            }),
            ("stop_sequences", |r| {
                r.model_settings.stop_sequences = vec!["</stop>".into()]
            }),
            ("request_timeout_ms", |r| {
                r.model_settings.request_timeout_ms = Some(60_001)
            }),
            ("model_retries", |r| r.retry_policy.model_retries = 3),
            ("output_retries", |r| r.retry_policy.output_retries = 2),
            ("request_limit", |r| r.usage_limits.request_limit = Some(11)),
            ("input_tokens_limit", |r| {
                r.usage_limits.input_tokens_limit = Some(100_001)
            }),
            ("output_tokens_limit", |r| {
                r.usage_limits.output_tokens_limit = Some(8_001)
            }),
            ("tool_calls_limit", |r| {
                r.usage_limits.tool_calls_limit = Some(21)
            }),
            ("prompt_mode", |r| {
                r.prompt_mode = Some(AgentPromptMode::Static)
            }),
        ];

        for (field, mutate) in mutations {
            let mut altered = base.clone();
            mutate(&mut altered.runtime);
            assert_ne!(
                altered, base,
                "{field}: the mutator changed nothing, so the test proves nothing"
            );
            let digest = AgentLibraryEntry::publish(altered, 1, 1_000)
                .unwrap_or_else(|error| panic!("{field} variant publishes: {error}"))
                .definition_digest;
            assert_ne!(
                digest, baseline,
                "{field} is stored but not bound: it can be changed without \
                 invalidating definition_digest"
            );
        }
    }

    #[test]
    fn an_absent_component_is_distinguishable_from_an_empty_one() {
        // `None` and `Some("")` must not hash alike, or a component could be
        // dropped from a definition without changing its digest.
        let mut absent = draft();
        absent.runtime.deps_contract = None;
        let absent_digest = AgentLibraryEntry::publish(absent, 1, 1_000)
            .expect("absent publishes")
            .definition_digest;

        let mut empty = draft();
        empty.runtime.deps_contract = Some(ComponentDependency {
            component_id: "contract:deps:none".into(),
            kind: AgentComponentKind::Schema,
            definition_digest: digest('a'),
        });
        let empty_digest = AgentLibraryEntry::publish(empty, 1, 1_000)
            .expect("present publishes")
            .definition_digest;
        assert_ne!(absent_digest, empty_digest);
    }

    #[test]
    fn a_structured_output_mode_requires_a_contract_to_structure_against() {
        for mode in [
            AgentOutputMode::Tool,
            AgentOutputMode::Native,
            AgentOutputMode::Prompted,
        ] {
            let mut source = draft();
            source.runtime.output_mode = Some(mode);
            source.runtime.output_contract = None;
            let error = AgentLibraryEntry::publish(source, 1, 1_000)
                .expect_err("a structured mode with no contract must fail closed");
            assert!(error.contains("output_contract"), "got: {error}");
        }
    }

    #[test]
    fn a_raw_float_temperature_is_refused_rather_than_silently_truncated() {
        // A caller passing pydantic-ai's `0.7` through an integer field lands on
        // 0, which is a VALID temperature and would silently make the agent
        // deterministic. The range check cannot catch that one, so the honest
        // guard is the type: the field is documented as thousandths. What the
        // check does catch is the other direction -- a percentage or a raw
        // millisecond value landing in a probability field.
        let mut source = draft();
        source.runtime.model_settings.temperature_milli = Some(60_000);
        let error =
            AgentLibraryEntry::publish(source, 1, 1_000).expect_err("out of range is refused");
        assert!(error.contains("temperature_milli"), "got: {error}");
    }

    // ---- the L2 -> L1 link (RF-ADR-008) ----

    #[test]
    fn an_agent_enumerates_every_component_it_is_assembled_from() {
        // The traversal the link exists for. Before this, an agent's parts were
        // opaque strings spread over six differently-named fields, so "which
        // agents use this component?" was a string match across all of them.
        let mut source = draft();
        source.runtime = contract();
        let entry = AgentLibraryEntry::publish(source, 1, 1_000).expect("publishes");

        let ids: Vec<&str> = entry
            .dependencies()
            .iter()
            .map(|dependency| dependency.component_id.as_str())
            .collect();
        for expected in [
            "prompt:example",
            "model-profile:default",
            "tool:search",
            "skill:research",
            "ontology:core",
            "toolset:mcp:search",
            "validator:cites-sources",
            "contract:deps:research",
            "contract:output:report",
        ] {
            assert!(ids.contains(&expected), "{expected} missing from {ids:?}");
        }
        assert!(entry.uses_component("tool:search"));
        assert!(!entry.uses_component("tool:never-wired"));
    }

    #[test]
    fn republishing_a_component_makes_an_agents_pin_stale_and_detectably_so() {
        // The impact question. `uses_component` still matches -- the agent does
        // use that component -- while `uses_component_revision` does not, which
        // is exactly the signal a republish should produce. Before pinning,
        // nothing distinguished the two.
        let entry = AgentLibraryEntry::publish(draft(), 1, 1_000).expect("publishes");
        assert!(entry.uses_component_revision("tool:search", &digest('3')));
        assert!(entry.uses_component("tool:search"));
        assert!(
            !entry.uses_component_revision("tool:search", &digest('9')),
            "a different revision of the same component must not match"
        );
    }

    #[test]
    fn a_slot_refuses_the_wrong_component_kind() {
        // Without the kind check a tool could be wired where a model profile
        // belongs, and every query that reads an agent's parts by kind would be
        // wrong in a way nothing else detects.
        let mut wrong = draft();
        wrong.model_profile = dependency("tool:search", AgentComponentKind::Tool, '3');
        let error = AgentLibraryEntry::publish(wrong, 1, 1_000).expect_err("must be refused");
        assert!(
            error.contains("must reference a model_profile component"),
            "got: {error}"
        );

        let mut wrong_tool = draft();
        wrong_tool.tools = vec![dependency(
            "model-profile:default",
            AgentComponentKind::ModelProfile,
            '5',
        )];
        let error = AgentLibraryEntry::publish(wrong_tool, 1, 1_000).expect_err("must be refused");
        assert!(
            error.contains("must reference a tool component"),
            "got: {error}"
        );
    }

    #[test]
    fn dependency_order_does_not_change_the_definition_digest() {
        let mut a = draft();
        a.tools = vec![
            dependency("tool:one", AgentComponentKind::Tool, '1'),
            dependency("tool:two", AgentComponentKind::Tool, '2'),
        ];
        let mut b = a.clone();
        b.tools.reverse();
        assert_eq!(
            AgentLibraryEntry::publish(a, 1, 1)
                .unwrap()
                .definition_digest,
            AgentLibraryEntry::publish(b, 1, 1)
                .unwrap()
                .definition_digest
        );
    }

    #[test]
    fn an_agent_with_no_tools_is_refused_but_validators_are_optional() {
        // The distinction `validate_dependencies`' `may_be_empty` encodes: an
        // agent without tools is almost certainly a mistake; one without output
        // validators is ordinary.
        let mut toolless = draft();
        toolless.tools.clear();
        let error = AgentLibraryEntry::publish(toolless, 1, 1_000).expect_err("must be refused");
        assert!(
            error.contains("tools must name at least one"),
            "got: {error}"
        );

        let mut no_validators = draft();
        no_validators.runtime = contract();
        no_validators.runtime.output_validator_refs.clear();
        AgentLibraryEntry::publish(no_validators, 1, 1_000).expect("validators are optional");
    }

    #[test]
    fn a_retired_entry_keeps_its_runtime_contract() {
        let mut source = draft();
        source.runtime = contract();
        let entry = AgentLibraryEntry::publish(source, 1, 1_000).expect("publishes");
        let tombstone = entry.retire(2, 2_000).expect("retires");
        assert_eq!(tombstone.runtime, contract());
        assert_eq!(tombstone.definition_digest, entry.definition_digest);
    }

    // ---- digest coverage ----

    #[test]
    fn every_stored_definition_field_moves_the_digest() {
        // A stored-but-UNHASHED field is how an approved agent gets silently
        // altered: the digest an approver signed off on still matches after the
        // change. The destructuring is the tripwire -- a field added to
        // `AgentLibraryEntryDraft` stops this test compiling until it is
        // covered below.
        let AgentLibraryEntryDraft {
            agent_id: _,
            package_id: _,
            version: _,
            role: _,
            role_digest: _,
            system_prompt: _,
            tools: _,
            skills: _,
            model_profile: _,
            model_identity: _,
            ontologies: _,
            tenant_id: _,
            actor_scope: _,
            purpose_id: _,
            policy_digest: _,
            source_revision: _,
            source_revision_digest: _,
            runtime: _,
            instantiated_from: _,
        } = full_draft();

        type Mutator = (&'static str, fn(&mut AgentLibraryEntryDraft));
        let mutators: &[Mutator] = &[
            ("agent_id", |d| d.agent_id = "agent:other".into()),
            ("package_id", |d| d.package_id = "package:other".into()),
            ("version", |d| d.version = "2.0.0".into()),
            ("role", |d| d.role = "role:reviewer".into()),
            ("role_digest", |d| d.role_digest = digest('c')),
            ("system_prompt", |d| {
                d.system_prompt = dependency("prompt:other", AgentComponentKind::SystemPrompt, 'c')
            }),
            ("tools", |d| {
                d.tools = vec![dependency("tool:other", AgentComponentKind::Tool, 'c')]
            }),
            ("skills", |d| {
                d.skills = vec![dependency("skill:other", AgentComponentKind::Skill, 'c')]
            }),
            ("model_profile", |d| {
                d.model_profile =
                    dependency("model-profile:other", AgentComponentKind::ModelProfile, 'c')
            }),
            ("model_identity", |d| {
                d.model_identity = "model:other".into()
            }),
            ("ontologies", |d| {
                d.ontologies = vec![dependency(
                    "ontology:other",
                    AgentComponentKind::Ontology,
                    'c',
                )]
            }),
            ("tenant_id", |d| d.tenant_id = "tenant-b".into()),
            ("actor_scope", |d| d.actor_scope = "operator".into()),
            ("purpose_id", |d| d.purpose_id = "agent-rebuild".into()),
            ("policy_digest", |d| d.policy_digest = digest('c')),
            ("source_revision", |d| {
                d.source_revision = "source-revision:43".into()
            }),
            ("source_revision_digest", |d| {
                d.source_revision_digest = digest('c')
            }),
            // Every individual runtime field is covered by
            // `every_runtime_field_changes_the_definition_digest`; this proves
            // the sub-struct is reached from the top-level draft at all.
            ("runtime", |d| d.runtime.model_settings.seed = Some(4_242)),
            ("instantiated_from", |d| d.instantiated_from = None),
        ];

        let baseline = AgentLibraryEntry::publish(full_draft(), 1, 1_000)
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
            let moved = AgentLibraryEntry::publish(altered, 1, 1_000)
                .unwrap_or_else(|error| panic!("{field} must still publish: {error}"))
                .definition_digest;
            assert_ne!(
                moved, baseline,
                "{field} is stored but not covered by the definition digest"
            );
        }
    }

    /// A draft with every optional slot populated, so a mutator always has
    /// something to change. `draft()` leaves `runtime` default and
    /// `instantiated_from` absent, which would make those two mutators vacuous.
    fn full_draft() -> AgentLibraryEntryDraft {
        let mut full = draft();
        full.runtime = contract();
        full.instantiated_from = Some(crate::agent_template::TemplateInstanceRef {
            template_id: "template:researcher".into(),
            entry_revision: 3,
            definition_digest: digest('9'),
            bindings: std::collections::BTreeMap::from([(
                "model".to_string(),
                dependency("model-profile:cheap", AgentComponentKind::ModelProfile, 'b'),
            )]),
        });
        full
    }

    #[test]
    fn the_capability_proof_covers_the_toolsets_too() {
        // `tool_surface_digest` is what `kg-delegate` admits as
        // `capability_digest`. Covering `tools` alone made that proof cover a
        // strict SUBSET of what the agent can call: an entry could pin a
        // read-only tool and an admin MCP toolset, and the proof would say
        // nothing about the second.
        let base = AgentLibraryEntry::publish(draft(), 1, 1_000).expect("publishes");
        let mut with_toolset = draft();
        with_toolset.runtime.toolset_refs = vec![dependency(
            "toolset:mcp:admin",
            AgentComponentKind::Toolset,
            'e',
        )];
        let widened = AgentLibraryEntry::publish(with_toolset, 1, 1_000).expect("publishes");

        assert_eq!(base.tools, widened.tools, "the flat tool list is untouched");
        assert_ne!(
            base.tool_surface_digest(),
            widened.tool_surface_digest(),
            "adding a toolset must move the capability proof"
        );

        // The flat list still moves it too -- widening the input set must not
        // have cost the original coverage.
        let mut other_tool = draft();
        other_tool.tools = vec![dependency("tool:other", AgentComponentKind::Tool, '3')];
        let other_tool = AgentLibraryEntry::publish(other_tool, 1, 1_000).expect("publishes");
        assert_ne!(base.tool_surface_digest(), other_tool.tool_surface_digest());
    }
}
