//! Parameterized agents (RF-ADR-008 item C).
//!
//! A template is *a published agent plus declared axes of variation*: the same
//! role on a cheaper model, the same research agent against a different search
//! tool. It is deliberately NOT a partially-specified agent.
//!
//! # Why a template is a complete base plus substitutions
//!
//! The obvious design is holes — sentinel values in the draft that instantiation
//! fills in. It was rejected: a draft with `"${model}"` where a
//! [`crate::agent_component::ComponentDependency`] belongs is not a valid draft,
//! so `validate()` would have to be relaxed to accept it, and every reader
//! downstream would have to know which fields might be sentinels. The type would
//! stop meaning what it says.
//!
//! So a template carries a base draft that is valid on its own — it already
//! names a real model, a real prompt, real tools — and declares which of those
//! components a caller may REPLACE. That gives three properties worth having:
//!
//! * the base is publishable and runnable as-is, so a template always has a
//!   working default;
//! * `validate()` keeps its meaning everywhere, with no sentinel-aware readers;
//! * instantiation is total — apply substitutions to something valid and the
//!   result is valid, or the substitution itself was refused.
//!
//! # The property that keeps delegation one code path
//!
//! Instantiating a template yields an ordinary [`crate::agent_library::AgentLibraryEntry`]
//! whose `definition_digest` covers the **bound** values. An instance is
//! therefore admitted, delegated, pinned and traversed exactly like a
//! hand-published agent — no template-aware branch anywhere downstream.
//!
//! The one thing an instance does carry is [`TemplateInstanceRef`]: which
//! template, at which revision, with which bindings. Not for execution — for the
//! same reason L1 exists at all, so "which agents came from this template?" is a
//! traversal rather than a guess.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

use crate::agent_component::{AgentComponentKind, ComponentDependency};
use crate::agent_library::{AgentLibraryEntryDraft, AgentLibraryLifecycle};

pub const AGENT_TEMPLATE_SCHEMA_VERSION: u16 = 1;
pub const AGENT_TEMPLATE_DIGEST_DOMAIN: &[u8] = b"au-eg/agent-template-definition/v1";

const MAX_TEXT_BYTES: usize = 4 * 1024;
const MAX_PARAMS: usize = 32;
const DIGEST_PREFIX: &str = "sha256:";

/// One declared axis of variation.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct TemplateParam {
    /// What a caller names when binding.
    pub name: String,
    /// The component in the base definition this parameter replaces.
    ///
    /// Identified by `component_id`, not by slot index. An index would be
    /// brittle in exactly the way that matters: reordering a tool list -- which
    /// `definition_digest` deliberately treats as a no-op, since order carries
    /// no meaning -- would silently repoint every parameter.
    pub replaces: String,
    /// The kind the replacement must be. Checked against both the base
    /// component and the binding, so a parameter cannot be used to smuggle a
    /// tool in where a model profile belongs.
    pub kind: AgentComponentKind,
    /// When false, leaving this parameter unbound keeps the base component.
    pub required: bool,
    /// Human-facing description of the axis, bounded.
    pub summary: String,
}

/// Where an instantiated agent came from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct TemplateInstanceRef {
    pub template_id: String,
    pub entry_revision: u64,
    pub definition_digest: String,
    /// Parameter name -> the component it was bound to. Ordered, so an
    /// instance's digest does not depend on map iteration order.
    pub bindings: BTreeMap<String, ComponentDependency>,
}

impl TemplateInstanceRef {
    pub fn validate(&self) -> Result<(), String> {
        validate_text("template_id", &self.template_id)?;
        if self.entry_revision == 0 {
            return Err("agent template instance revision must be a retained revision".into());
        }
        validate_digest("definition_digest", &self.definition_digest)?;
        if self.bindings.len() > MAX_PARAMS {
            return Err("agent template instance has too many bindings".to_string());
        }
        for (name, binding) in &self.bindings {
            validate_text("binding name", name)?;
            validate_text("binding component_id", &binding.component_id)?;
            validate_digest("binding definition_digest", &binding.definition_digest)?;
        }
        Ok(())
    }
}

/// Caller-supplied inputs for one template revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AgentTemplateDraft {
    pub template_id: String,
    pub version: String,
    /// The default agent. Valid and runnable on its own.
    pub base: AgentLibraryEntryDraft,
    pub params: Vec<TemplateParam>,
    pub tenant_id: String,
    pub actor_scope: String,
    pub purpose_id: String,
    pub policy_digest: String,
}

/// One durable, published template revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AgentTemplateEntry {
    pub schema_version: u16,
    pub template_id: String,
    pub version: String,
    pub base: AgentLibraryEntryDraft,
    pub params: Vec<TemplateParam>,
    pub tenant_id: String,
    pub actor_scope: String,
    pub purpose_id: String,
    pub policy_digest: String,
    pub entry_revision: u64,
    pub lifecycle: AgentLibraryLifecycle,
    pub definition_digest: String,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

impl AgentTemplateEntry {
    pub fn publish(
        draft: AgentTemplateDraft,
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
        draft: AgentTemplateDraft,
        entry_revision: u64,
        lifecycle: AgentLibraryLifecycle,
        created_at_ms: u64,
        updated_at_ms: u64,
    ) -> Result<Self, String> {
        draft.validate()?;
        if entry_revision == 0 {
            return Err("agent template entry revision must start at one".to_string());
        }
        if updated_at_ms < created_at_ms {
            return Err("agent template entry update time precedes creation time".to_string());
        }
        let definition_digest = definition_digest(&draft);
        let entry = Self {
            schema_version: AGENT_TEMPLATE_SCHEMA_VERSION,
            template_id: draft.template_id,
            version: draft.version,
            base: draft.base,
            params: draft.params,
            tenant_id: draft.tenant_id,
            actor_scope: draft.actor_scope,
            purpose_id: draft.purpose_id,
            policy_digest: draft.policy_digest,
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
            return Err("agent template entry is already retired".to_string());
        }
        if entry_revision <= self.entry_revision {
            return Err("agent template tombstone revision must advance".to_string());
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
        if self.schema_version != AGENT_TEMPLATE_SCHEMA_VERSION {
            return Err("agent template entry schema version is unsupported".to_string());
        }
        self.as_draft().validate()?;
        if self.entry_revision == 0 {
            return Err("agent template entry revision must start at one".to_string());
        }
        if !is_digest(&self.definition_digest) {
            return Err("agent template definition_digest is not a sha256 digest".to_string());
        }
        if self.definition_digest != definition_digest(&self.as_draft()) {
            return Err("agent template definition_digest does not match its definition".into());
        }
        Ok(())
    }

    pub fn as_draft(&self) -> AgentTemplateDraft {
        AgentTemplateDraft {
            template_id: self.template_id.clone(),
            version: self.version.clone(),
            base: self.base.clone(),
            params: self.params.clone(),
            tenant_id: self.tenant_id.clone(),
            actor_scope: self.actor_scope.clone(),
            purpose_id: self.purpose_id.clone(),
            policy_digest: self.policy_digest.clone(),
        }
    }

    /// Apply `bindings` and return an ordinary agent draft.
    ///
    /// The result is a normal [`AgentLibraryEntryDraft`]: publish it and the
    /// entry is indistinguishable from a hand-authored one apart from its
    /// `instantiated_from` provenance. That is what keeps admission and
    /// delegation free of any template-aware branch.
    ///
    /// `agent_id` is supplied by the caller rather than derived: two instances
    /// of one template are two different agents, and deriving a name from the
    /// bindings would make the id change whenever a binding did.
    pub fn instantiate(
        &self,
        agent_id: &str,
        bindings: &BTreeMap<String, ComponentDependency>,
    ) -> Result<AgentLibraryEntryDraft, String> {
        self.validate()?;
        if self.lifecycle == AgentLibraryLifecycle::Retired {
            return Err("a retired agent template cannot be instantiated".to_string());
        }
        validate_text("agent_id", agent_id)?;

        let declared: BTreeMap<&str, &TemplateParam> = self
            .params
            .iter()
            .map(|param| (param.name.as_str(), param))
            .collect();
        // An unknown binding is refused rather than ignored: silently dropping
        // it would hand back an agent that is not the one the caller asked for,
        // and the caller would have no way to tell.
        for name in bindings.keys() {
            if !declared.contains_key(name.as_str()) {
                return Err(format!(
                    "agent template '{}' declares no parameter '{name}'",
                    self.template_id
                ));
            }
        }
        for param in &self.params {
            if param.required && !bindings.contains_key(&param.name) {
                return Err(format!(
                    "agent template parameter '{}' is required",
                    param.name
                ));
            }
        }

        let mut draft = self.base.clone();
        draft.agent_id = agent_id.to_string();
        for param in &self.params {
            let Some(binding) = bindings.get(&param.name) else {
                continue; // optional and unbound: the base component stands
            };
            if binding.kind != param.kind {
                return Err(format!(
                    "agent template parameter '{}' takes a {} component, got {}",
                    param.name,
                    param.kind.as_str(),
                    binding.kind.as_str()
                ));
            }
            if !substitute(&mut draft, &param.replaces, param.kind, binding) {
                return Err(format!(
                    "agent template parameter '{}' replaces '{}', which is not in the base \
                     definition any more",
                    param.name, param.replaces
                ));
            }
        }
        draft.instantiated_from = Some(TemplateInstanceRef {
            template_id: self.template_id.clone(),
            entry_revision: self.entry_revision,
            definition_digest: self.definition_digest.clone(),
            bindings: bindings.clone(),
        });
        // The substituted draft has to stand on its own -- a binding that
        // duplicates a component already in the same slot, for instance, is
        // caught here rather than at publish.
        draft.validate()?;
        Ok(draft)
    }
}

/// Replace the one dependency with `component_id == replaces` and the given
/// kind. Returns whether exactly one was found and replaced.
fn substitute(
    draft: &mut AgentLibraryEntryDraft,
    replaces: &str,
    kind: AgentComponentKind,
    binding: &ComponentDependency,
) -> bool {
    let mut replaced = false;
    let mut swap = |slot: &mut ComponentDependency| {
        if slot.component_id == replaces && slot.kind == kind {
            *slot = binding.clone();
            replaced = true;
        }
    };
    swap(&mut draft.system_prompt);
    swap(&mut draft.model_profile);
    for slot in draft
        .tools
        .iter_mut()
        .chain(draft.skills.iter_mut())
        .chain(draft.ontologies.iter_mut())
        .chain(draft.runtime.toolset_refs.iter_mut())
        .chain(draft.runtime.output_validator_refs.iter_mut())
    {
        swap(slot);
    }
    for slot in draft
        .runtime
        .deps_contract
        .iter_mut()
        .chain(draft.runtime.output_contract.iter_mut())
    {
        swap(slot);
    }
    replaced
}

impl AgentTemplateDraft {
    pub fn validate(&self) -> Result<(), String> {
        for (field, value) in [
            ("template_id", self.template_id.as_str()),
            ("version", self.version.as_str()),
            ("tenant_id", self.tenant_id.as_str()),
            ("actor_scope", self.actor_scope.as_str()),
            ("purpose_id", self.purpose_id.as_str()),
        ] {
            validate_text(field, value)?;
        }
        validate_digest("policy_digest", &self.policy_digest)?;
        // The base must be a real agent. A template whose default does not
        // validate is a template that can produce nothing.
        self.base.validate()?;
        if self.base.tenant_id != self.tenant_id {
            return Err("agent template base belongs to a different tenant".to_string());
        }
        if self.base.instantiated_from.is_some() {
            return Err(
                "agent template base cannot itself be a template instance: templates of \
                 templates would make an instance's provenance ambiguous"
                    .to_string(),
            );
        }
        if self.params.len() > MAX_PARAMS {
            return Err("agent template declares too many parameters".to_string());
        }
        let mut names = BTreeSet::new();
        let mut replaced = BTreeSet::new();
        for param in &self.params {
            validate_text("param name", &param.name)?;
            validate_text("param replaces", &param.replaces)?;
            validate_text("param summary", &param.summary)?;
            if !names.insert(&param.name) {
                return Err(format!("agent template declares '{}' twice", param.name));
            }
            // Two parameters replacing the same component would make the result
            // depend on application order, which nothing defines.
            if !replaced.insert((&param.replaces, param.kind)) {
                return Err(format!(
                    "agent template has two parameters replacing '{}'",
                    param.replaces
                ));
            }
            // The base must actually contain what the parameter claims to
            // replace, or the parameter is dead and a caller binding it would
            // get an agent that ignored them.
            let occurrences = count_occurrences(&self.base, &param.replaces, param.kind);
            if occurrences == 0 {
                return Err(format!(
                    "agent template parameter '{}' replaces '{}' ({}), which the base \
                     definition does not contain",
                    param.name,
                    param.replaces,
                    param.kind.as_str()
                ));
            }
            if occurrences > 1 {
                return Err(format!(
                    "agent template parameter '{}' replaces '{}', which appears {occurrences} \
                     times in the base definition -- the substitution would be ambiguous",
                    param.name, param.replaces
                ));
            }
        }
        Ok(())
    }
}

fn count_occurrences(
    draft: &AgentLibraryEntryDraft,
    component_id: &str,
    kind: AgentComponentKind,
) -> usize {
    let mut all: Vec<&ComponentDependency> = vec![&draft.system_prompt, &draft.model_profile];
    all.extend(&draft.tools);
    all.extend(&draft.skills);
    all.extend(&draft.ontologies);
    all.extend(&draft.runtime.toolset_refs);
    all.extend(&draft.runtime.output_validator_refs);
    all.extend(draft.runtime.deps_contract.iter());
    all.extend(draft.runtime.output_contract.iter());
    all.iter()
        .filter(|dependency| dependency.component_id == component_id && dependency.kind == kind)
        .count()
}

/// The content digest of one template DEFINITION, independent of its revision.
///
/// Public because the durable store needs it before an entry exists: the
/// replay identity of a publish attempt is minted from the definition the
/// caller asked for, so a byte-identical retry resolves to the same operation.
/// Mirrors [`crate::agent_library::draft_definition_digest`].
pub fn draft_definition_digest(draft: &AgentTemplateDraft) -> String {
    definition_digest(draft)
}

fn definition_digest(draft: &AgentTemplateDraft) -> String {
    let mut hasher = Sha256::new();
    hasher.update(AGENT_TEMPLATE_DIGEST_DOMAIN);
    put_text(&mut hasher, &draft.template_id);
    put_text(&mut hasher, &draft.version);
    // The base's own digest, so a template's identity moves whenever its
    // default agent does.
    put_text(
        &mut hasher,
        &crate::agent_library::draft_definition_digest(&draft.base),
    );
    let mut params = draft.params.clone();
    params.sort();
    hasher.update((params.len() as u64).to_be_bytes());
    for param in &params {
        put_text(&mut hasher, &param.name);
        put_text(&mut hasher, &param.replaces);
        put_text(&mut hasher, param.kind.as_str());
        hasher.update([u8::from(param.required)]);
        put_text(&mut hasher, &param.summary);
    }
    put_text(&mut hasher, &draft.tenant_id);
    put_text(&mut hasher, &draft.actor_scope);
    put_text(&mut hasher, &draft.purpose_id);
    put_text(&mut hasher, &draft.policy_digest);
    format!("{DIGEST_PREFIX}{}", hex::encode(hasher.finalize()))
}

fn put_text(hasher: &mut Sha256, value: &str) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value.as_bytes());
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
        return Err(format!("agent template {field} is not a sha256 digest"));
    }
    Ok(())
}

fn validate_text(field: &str, value: &str) -> Result<(), String> {
    if value.is_empty()
        || value.len() > MAX_TEXT_BYTES
        || value.trim() != value
        || value.chars().any(char::is_control)
    {
        return Err(format!("agent template {field} is invalid"));
    }
    Ok(())
}

/// Which durable mutation a template operation performs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum AgentTemplateMutationKind {
    Publish,
    Retire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AgentTemplatePublishRequest {
    /// Shared with the other three layers: templates are published into the
    /// same owner as the components, agents and graphs they are built from, so
    /// they share its mutation context rather than growing a parallel copy of
    /// it.
    pub context: crate::agent_library::AgentLibraryMutationContext,
    pub template: AgentTemplateDraft,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AgentTemplateRetireRequest {
    pub context: crate::agent_library::AgentLibraryMutationContext,
    pub template_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AgentTemplateStatusRequest {
    pub context: crate::agent_library::AgentLibraryMutationContext,
    pub template_id: String,
    pub kind: AgentTemplateMutationKind,
}

/// Bind a template's parameters and get back an ordinary agent draft.
///
/// A READ: it resolves durable state and computes, but commits nothing. The
/// caller publishes the returned draft through `AgentLibrary::Publish` exactly
/// as it would a hand-authored one -- which is the whole point of the design,
/// and the reason there is no template-aware branch in admission or delegation.
///
/// `entry_revision` pins which template revision to bind. `None` means the
/// head. Pinning matters because [`TemplateInstanceRef`] records a revision:
/// reproducing an existing instance means binding the revision it named, not
/// whatever the head has since become.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AgentTemplateInstantiateRequest {
    pub tenant_id: String,
    pub template_id: String,
    /// The revision to bind, or the head when absent.
    #[serde(default)]
    pub entry_revision: Option<u64>,
    /// The instance's own agent id. Supplied, not derived -- see
    /// [`AgentTemplateEntry::instantiate`].
    pub agent_id: String,
    #[serde(default)]
    pub bindings: BTreeMap<String, ComponentDependency>,
}

impl AgentTemplateInstantiateRequest {
    pub fn validate(&self) -> Result<(), String> {
        validate_text("tenant_id", &self.tenant_id)?;
        validate_text("template_id", &self.template_id)?;
        validate_text("agent_id", &self.agent_id)?;
        if self.entry_revision == Some(0) {
            return Err("agent template instantiate revision must be a retained revision".into());
        }
        // Bounded before any store work: an unbounded binding map would let a
        // caller set the cost of the resolve.
        if self.bindings.len() > MAX_PARAMS {
            return Err("agent template instantiate names too many bindings".to_string());
        }
        for (name, binding) in &self.bindings {
            validate_text("binding name", name)?;
            validate_text("binding component_id", &binding.component_id)?;
            validate_digest("binding definition_digest", &binding.definition_digest)?;
        }
        Ok(())
    }
}

/// Typed template wire operations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum AgentTemplateOp {
    /// Boxed for the same reason as the other three layers: a publish request
    /// carries a whole draft -- and this one embeds an entire
    /// [`AgentLibraryEntryDraft`] as its base, so it is the largest of the
    /// four -- and would otherwise set the size of every `Method`. `Box` is
    /// transparent to serde, so the wire form is unchanged.
    Publish {
        request: Box<AgentTemplatePublishRequest>,
    },
    Retire {
        request: AgentTemplateRetireRequest,
    },
    Current {
        tenant_id: String,
        template_id: String,
    },
    History {
        tenant_id: String,
        template_id: String,
    },
    Status {
        request: AgentTemplateStatusRequest,
    },
    /// Bind parameters and return an ordinary agent draft -- the query this
    /// layer exists for. A read: nothing is committed until the caller
    /// publishes the draft through the agent library.
    ///
    /// NOT boxed, unlike `Publish`: this request carries only ids and a
    /// bounded binding map, so it is smaller than `Status`'s mutation context
    /// and boxing it would buy an allocation on a read path for nothing.
    Instantiate {
        request: AgentTemplateInstantiateRequest,
    },
}

impl AgentTemplateOp {
    /// Whether this operation commits a durable revision.
    ///
    /// The access layer and the capability policy both need this split, and
    /// deriving it here means they cannot disagree about it. `Instantiate`
    /// belongs on the read side deliberately: it produces a draft, and the
    /// separate `AgentLibrary::Publish` that stores the result is what carries
    /// the write privilege.
    pub fn is_mutation(&self) -> bool {
        matches!(self, Self::Publish { .. } | Self::Retire { .. })
    }

    pub fn tenant_id(&self) -> &str {
        match self {
            Self::Publish { request } => &request.context.tenant_id,
            Self::Retire { request } => &request.context.tenant_id,
            Self::Status { request } => &request.context.tenant_id,
            Self::Instantiate { request } => &request.tenant_id,
            Self::Current { tenant_id, .. } | Self::History { tenant_id, .. } => tenant_id,
        }
    }

    pub fn template_id(&self) -> &str {
        match self {
            Self::Publish { request } => &request.template.template_id,
            Self::Retire { request } => &request.template_id,
            Self::Status { request } => &request.template_id,
            Self::Instantiate { request } => &request.template_id,
            Self::Current { template_id, .. } | Self::History { template_id, .. } => template_id,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        match self {
            Self::Publish { request } => {
                request.context.validate()?;
                request.template.validate()?;
                if request.context.tenant_id != request.template.tenant_id {
                    return Err(
                        "agent template publish context tenant does not match the template's"
                            .to_string(),
                    );
                }
                Ok(())
            }
            Self::Retire { request } => {
                request.context.validate()?;
                validate_text("template_id", &request.template_id)
            }
            Self::Status { request } => {
                request.context.validate()?;
                validate_text("template_id", &request.template_id)
            }
            Self::Instantiate { request } => request.validate(),
            Self::Current {
                tenant_id,
                template_id,
            }
            | Self::History {
                tenant_id,
                template_id,
            } => {
                validate_text("tenant_id", tenant_id)?;
                validate_text("template_id", template_id)
            }
        }
    }
}

/// What a committed template mutation returns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AgentTemplateCommittedResult {
    pub schema_version: u16,
    pub template: AgentTemplateEntry,
    pub batch_id: String,
    pub committed_version: u64,
}

/// The outbox event one committed template revision emits.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AgentTemplateOutboxEvent {
    pub schema_version: u16,
    pub kind: AgentTemplateMutationKind,
    pub template: AgentTemplateEntry,
    pub performing_actor: String,
    pub action_actor_scope: String,
}

impl AgentTemplateOutboxEvent {
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != AGENT_TEMPLATE_SCHEMA_VERSION {
            return Err("agent template outbox schema version is unsupported".to_string());
        }
        self.template.validate()?;
        let expected = match self.template.lifecycle {
            AgentLibraryLifecycle::Published => AgentTemplateMutationKind::Publish,
            AgentLibraryLifecycle::Retired => AgentTemplateMutationKind::Retire,
        };
        if self.kind != expected {
            return Err(
                "agent template outbox kind does not match the entry's lifecycle".to_string(),
            );
        }
        validate_text("performing_actor", &self.performing_actor)?;
        validate_text("action_actor_scope", &self.action_actor_scope)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent_library::{AgentLibraryEntry, AgentRuntimeContract};

    fn digest(seed: char) -> String {
        format!("sha256:{}", seed.to_string().repeat(64))
    }

    fn dep(component_id: &str, kind: AgentComponentKind, seed: char) -> ComponentDependency {
        ComponentDependency {
            component_id: component_id.into(),
            kind,
            definition_digest: digest(seed),
        }
    }

    fn base() -> AgentLibraryEntryDraft {
        AgentLibraryEntryDraft {
            agent_id: "agent:researcher".into(),
            package_id: "package:research".into(),
            version: "1.0.0".into(),
            role: "researcher".into(),
            role_digest: digest('1'),
            system_prompt: dep("prompt:research", AgentComponentKind::SystemPrompt, '2'),
            tools: vec![
                dep("tool:web-search", AgentComponentKind::Tool, '3'),
                dep("tool:summarize", AgentComponentKind::Tool, '4'),
            ],
            skills: vec![dep("skill:research", AgentComponentKind::Skill, '5')],
            model_profile: dep("model:opus", AgentComponentKind::ModelProfile, '6'),
            model_identity: "claude-opus-5".into(),
            ontologies: vec![dep("ontology:core", AgentComponentKind::Ontology, '7')],
            tenant_id: "tenant-a".into(),
            actor_scope: "agent-builder".into(),
            purpose_id: "agent-construction".into(),
            policy_digest: digest('8'),
            source_revision: "rev-1".into(),
            source_revision_digest: digest('9'),
            runtime: AgentRuntimeContract::default(),
            instantiated_from: None,
        }
    }

    fn param(name: &str, replaces: &str, kind: AgentComponentKind, required: bool) -> TemplateParam {
        TemplateParam {
            name: name.into(),
            replaces: replaces.into(),
            kind,
            required,
            summary: format!("the {name} axis"),
        }
    }

    fn draft() -> AgentTemplateDraft {
        AgentTemplateDraft {
            template_id: "template:researcher".into(),
            version: "1.0.0".into(),
            base: base(),
            params: vec![
                param("model", "model:opus", AgentComponentKind::ModelProfile, false),
                param("search", "tool:web-search", AgentComponentKind::Tool, false),
            ],
            tenant_id: "tenant-a".into(),
            actor_scope: "agent-builder".into(),
            purpose_id: "agent-construction".into(),
            policy_digest: digest('8'),
        }
    }

    fn template() -> AgentTemplateEntry {
        AgentTemplateEntry::publish(draft(), 1, 1_000).expect("publishes")
    }

    #[test]
    fn a_template_publishes_and_round_trips() {
        let entry = template();
        entry.validate().expect("valid");
        assert_eq!(entry.as_draft(), draft());
    }

    #[test]
    fn an_unbound_template_instantiates_to_its_base_defaults() {
        // The property that makes the base-plus-substitutions design work: a
        // template always has a working default, so instantiating with nothing
        // bound is meaningful rather than an error.
        let instance = template()
            .instantiate("agent:researcher-default", &BTreeMap::new())
            .expect("instantiates");
        assert_eq!(instance.model_profile.component_id, "model:opus");
        assert_eq!(instance.tools[0].component_id, "tool:web-search");
        assert_eq!(instance.agent_id, "agent:researcher-default");
    }

    #[test]
    fn an_instance_is_an_ordinary_agent_and_publishes_like_one() {
        // THE property that keeps admission and delegation free of any
        // template-aware branch.
        let bindings = BTreeMap::from([(
            "model".to_string(),
            dep("model:haiku", AgentComponentKind::ModelProfile, 'a'),
        )]);
        let instance = template()
            .instantiate("agent:researcher-cheap", &bindings)
            .expect("instantiates");
        let entry = AgentLibraryEntry::publish(instance, 1, 2_000)
            .expect("an instance publishes exactly like a hand-authored agent");
        entry.validate().expect("valid");
        assert_eq!(entry.model_profile.component_id, "model:haiku");
        // And its provenance is recorded, so the template is traversable.
        let provenance = entry
            .instantiated_from
            .as_ref()
            .expect("an instance records its template");
        assert_eq!(provenance.template_id, "template:researcher");
        assert_eq!(provenance.entry_revision, 1);
        assert!(provenance.bindings.contains_key("model"));
    }

    #[test]
    fn the_bound_values_are_what_the_instance_digest_covers() {
        // Two instances differing only in a binding must be different agents.
        let cheap = template()
            .instantiate(
                "agent:x",
                &BTreeMap::from([(
                    "model".to_string(),
                    dep("model:haiku", AgentComponentKind::ModelProfile, 'a'),
                )]),
            )
            .unwrap();
        let big = template()
            .instantiate(
                "agent:x",
                &BTreeMap::from([(
                    "model".to_string(),
                    dep("model:opus", AgentComponentKind::ModelProfile, 'b'),
                )]),
            )
            .unwrap();
        assert_ne!(
            AgentLibraryEntry::publish(cheap, 1, 1).unwrap().definition_digest,
            AgentLibraryEntry::publish(big, 1, 1).unwrap().definition_digest
        );
    }

    #[test]
    fn an_unknown_binding_is_refused_rather_than_ignored() {
        // Silently dropping it would hand back an agent that is not the one the
        // caller asked for, with no way for them to tell.
        let error = template()
            .instantiate(
                "agent:x",
                &BTreeMap::from([(
                    "nonexistent".to_string(),
                    dep("model:haiku", AgentComponentKind::ModelProfile, 'a'),
                )]),
            )
            .expect_err("must be refused");
        assert!(error.contains("declares no parameter 'nonexistent'"), "got: {error}");
    }

    #[test]
    fn a_required_parameter_must_be_bound() {
        let mut source = draft();
        source.params[0].required = true;
        let entry = AgentTemplateEntry::publish(source, 1, 1_000).unwrap();
        let error = entry
            .instantiate("agent:x", &BTreeMap::new())
            .expect_err("must be refused");
        assert!(error.contains("is required"), "got: {error}");
    }

    #[test]
    fn a_binding_of_the_wrong_kind_is_refused() {
        let error = template()
            .instantiate(
                "agent:x",
                &BTreeMap::from([(
                    "model".to_string(),
                    dep("tool:other", AgentComponentKind::Tool, 'a'),
                )]),
            )
            .expect_err("must be refused");
        assert!(error.contains("takes a model_profile component"), "got: {error}");
    }

    #[test]
    fn a_parameter_must_replace_something_the_base_actually_contains() {
        // Otherwise the parameter is dead and a caller binding it would get an
        // agent that quietly ignored them.
        let mut source = draft();
        source.params.push(param(
            "ghost",
            "tool:not-in-the-base",
            AgentComponentKind::Tool,
            false,
        ));
        let error = AgentTemplateEntry::publish(source, 1, 1_000).expect_err("must be refused");
        assert!(error.contains("does not contain"), "got: {error}");
    }

    #[test]
    fn an_ambiguous_substitution_is_refused_at_publish() {
        // The same component in two slots means "replace it" has no single
        // answer, and it is caught when the template is PUBLISHED rather than
        // when someone instantiates it.
        //
        // Reaching this case takes care, and that is itself worth recording:
        // per-slot kind checking makes most of it unrepresentable, because two
        // slots that accept different kinds cannot hold the same
        // `(component_id, kind)` pair, and two occurrences within one slot are
        // already refused as duplicates. The one same-kind pair that remains is
        // `deps_contract` and `output_contract`, both `Schema` -- and an agent
        // whose input and output share a schema is perfectly ordinary (a
        // refiner). So this is the real case, not a contrived one.
        let shared = dep("schema:document", AgentComponentKind::Schema, 'd');
        let mut source = draft();
        source.base.runtime.deps_contract = Some(shared.clone());
        source.base.runtime.output_contract = Some(shared);
        source.base.runtime.output_mode = Some(crate::agent_library::AgentOutputMode::Tool);
        source.params.push(param(
            "schema",
            "schema:document",
            AgentComponentKind::Schema,
            false,
        ));
        let error = AgentTemplateEntry::publish(source, 1, 1_000).expect_err("must be refused");
        assert!(error.contains("would be ambiguous"), "got: {error}");
    }

    #[test]
    fn two_parameters_cannot_replace_the_same_component() {
        // The result would depend on application order, which nothing defines.
        let mut source = draft();
        source.params.push(param(
            "model_again",
            "model:opus",
            AgentComponentKind::ModelProfile,
            false,
        ));
        let error = AgentTemplateEntry::publish(source, 1, 1_000).expect_err("must be refused");
        assert!(error.contains("two parameters replacing"), "got: {error}");
    }

    #[test]
    fn duplicate_parameter_names_are_refused() {
        let mut source = draft();
        source.params.push(param(
            "model",
            "tool:summarize",
            AgentComponentKind::Tool,
            false,
        ));
        let error = AgentTemplateEntry::publish(source, 1, 1_000).expect_err("must be refused");
        assert!(error.contains("declares 'model' twice"), "got: {error}");
    }

    #[test]
    fn a_template_of_a_template_is_refused() {
        // A base that is itself an instance would make an instance's provenance
        // ambiguous: two templates would both claim it.
        let mut source = draft();
        source.base.instantiated_from = Some(TemplateInstanceRef {
            template_id: "template:other".into(),
            entry_revision: 1,
            definition_digest: digest('c'),
            bindings: BTreeMap::new(),
        });
        let error = AgentTemplateEntry::publish(source, 1, 1_000).expect_err("must be refused");
        assert!(error.contains("templates of"), "got: {error}");
    }

    #[test]
    fn a_binding_that_duplicates_an_existing_tool_is_refused() {
        // Swapping the search tool for one the agent already has would leave a
        // duplicate in the tool list. The substituted draft has to stand on its
        // own, so this is caught at instantiation rather than at publish.
        let error = template()
            .instantiate(
                "agent:x",
                &BTreeMap::from([(
                    "search".to_string(),
                    dep("tool:summarize", AgentComponentKind::Tool, '4'),
                )]),
            )
            .expect_err("must be refused");
        assert!(error.contains("duplicate"), "got: {error}");
    }

    #[test]
    fn a_retired_template_cannot_be_instantiated() {
        let retired = template().retire(2, 2_000).expect("retires");
        let error = retired
            .instantiate("agent:x", &BTreeMap::new())
            .expect_err("must be refused");
        assert!(error.contains("retired"), "got: {error}");
    }

    #[test]
    fn a_templates_digest_moves_when_its_base_agent_does() {
        // A template's identity has to follow its default: otherwise two
        // templates with different defaults could share a digest.
        let baseline = template().definition_digest;
        let mut changed = draft();
        changed.base.model_identity = "claude-haiku-4-5".into();
        assert_ne!(
            AgentTemplateEntry::publish(changed, 1, 1_000).unwrap().definition_digest,
            baseline
        );
        let mut reworded = draft();
        reworded.params[0].summary = "something else".into();
        assert_ne!(
            AgentTemplateEntry::publish(reworded, 1, 1_000).unwrap().definition_digest,
            baseline
        );
    }

    #[test]
    fn parameter_order_does_not_change_the_template_digest() {
        let mut reordered = draft();
        reordered.params.reverse();
        assert_eq!(
            AgentTemplateEntry::publish(reordered, 1, 1_000).unwrap().definition_digest,
            template().definition_digest
        );
    }
    // ---- wire operations ----

    fn context() -> crate::agent_library::AgentLibraryMutationContext {
        crate::agent_library::AgentLibraryMutationContext {
            request_id: 1,
            principal: format!("principal:sha256:{}", "0".repeat(64)),
            caller_principal: format!("principal:sha256:{}", "a".repeat(64)),
            attempt_nonce: crate::contract::Nonce::from_bytes([7; 32]),
            tenant_id: "tenant-a".into(),
            actor_scope: "agent-builder".into(),
            purpose_id: "agent-template:publish".into(),
            policy_revision: "policy-v1".into(),
            policy_digest: digest('8'),
            policy_decision_id: "agent-template:decision".into(),
            idempotency_key: "key-1".into(),
            expected_revision: Some(0),
            trace_id: None,
            created_at_ms: 10,
        }
    }

    #[test]
    fn publish_and_retire_are_the_only_mutating_operations() {
        // The access layer and the capability policy both delegate to this, so
        // a misclassification here would silently move a write onto the read
        // path in BOTH of them at once.
        assert!(AgentTemplateOp::Publish {
            request: Box::new(AgentTemplatePublishRequest {
                context: context(),
                template: draft(),
            }),
        }
        .is_mutation());
        assert!(AgentTemplateOp::Retire {
            request: AgentTemplateRetireRequest {
                context: context(),
                template_id: "template:researcher".into(),
            },
        }
        .is_mutation());
        for op in [
            AgentTemplateOp::Current {
                tenant_id: "tenant-a".into(),
                template_id: "template:researcher".into(),
            },
            AgentTemplateOp::History {
                tenant_id: "tenant-a".into(),
                template_id: "template:researcher".into(),
            },
            AgentTemplateOp::Status {
                request: AgentTemplateStatusRequest {
                    context: context(),
                    template_id: "template:researcher".into(),
                    kind: AgentTemplateMutationKind::Publish,
                },
            },
            AgentTemplateOp::Instantiate {
                request: AgentTemplateInstantiateRequest {
                    tenant_id: "tenant-a".into(),
                    template_id: "template:researcher".into(),
                    entry_revision: None,
                    agent_id: "agent:cheap".into(),
                    bindings: BTreeMap::new(),
                },
            },
        ] {
            assert!(!op.is_mutation(), "{op:?} must classify as a read");
        }
    }

    #[test]
    fn a_publish_whose_context_names_another_tenant_is_refused() {
        let mut request = AgentTemplatePublishRequest {
            context: context(),
            template: draft(),
        };
        request.context.tenant_id = "tenant-b".into();
        let error = AgentTemplateOp::Publish {
            request: Box::new(request),
        }
        .validate()
        .expect_err("a cross-tenant publish must be refused");
        assert!(error.contains("tenant"), "got: {error}");
    }

    #[test]
    fn an_instantiate_refuses_an_unbounded_binding_map_by_name() {
        let mut bindings = BTreeMap::new();
        for index in 0..=MAX_PARAMS {
            bindings.insert(
                format!("param-{index}"),
                dep("tool:x", AgentComponentKind::Tool, '3'),
            );
        }
        let error = AgentTemplateOp::Instantiate {
            request: AgentTemplateInstantiateRequest {
                tenant_id: "tenant-a".into(),
                template_id: "template:researcher".into(),
                entry_revision: None,
                agent_id: "agent:cheap".into(),
                bindings,
            },
        }
        .validate()
        .expect_err("an over-large binding map must be refused");
        assert!(error.contains("too many bindings"), "got: {error}");
    }

    #[test]
    fn an_instantiate_cannot_pin_revision_zero() {
        // Revision zero is "no revision yet", never a retained one. Accepting
        // it would turn a typo into a head read the caller did not ask for.
        let error = AgentTemplateOp::Instantiate {
            request: AgentTemplateInstantiateRequest {
                tenant_id: "tenant-a".into(),
                template_id: "template:researcher".into(),
                entry_revision: Some(0),
                agent_id: "agent:cheap".into(),
                bindings: BTreeMap::new(),
            },
        }
        .validate()
        .expect_err("revision zero must be refused");
        assert!(error.contains("retained revision"), "got: {error}");
    }

    #[test]
    fn the_publish_request_is_boxed_so_a_template_op_stays_small() {
        // A template's publish request embeds an entire agent draft as its
        // base, so it is the largest request of the four layers. Unboxed it
        // would set the size of every `Method` in the protocol.
        use std::mem::size_of;
        assert!(
            size_of::<AgentTemplateOp>()
                <= size_of::<crate::agent_component::AgentComponentOp>(),
            "AgentTemplateOp is {} bytes, larger than the AgentComponentOp it mirrors ({})",
            size_of::<AgentTemplateOp>(),
            size_of::<crate::agent_component::AgentComponentOp>()
        );
    }

    #[test]
    fn an_outbox_event_whose_kind_contradicts_its_lifecycle_is_refused() {
        let event = AgentTemplateOutboxEvent {
            schema_version: AGENT_TEMPLATE_SCHEMA_VERSION,
            kind: AgentTemplateMutationKind::Retire,
            template: template(),
            performing_actor: "principal:a".into(),
            action_actor_scope: "agent-builder".into(),
        };
        let error = event
            .validate()
            .expect_err("a published entry cannot carry a retire event");
        assert!(error.contains("lifecycle"), "got: {error}");
    }

    // ---- digest coverage ----

    #[test]
    fn every_stored_definition_field_moves_the_digest() {
        // A stored-but-UNHASHED field is how an approved template gets
        // silently altered: the digest an approver signed off on still
        // matches after the change. The destructuring is the tripwire -- a
        // field added to `AgentTemplateDraft` stops this test compiling until
        // it is covered below.
        let AgentTemplateDraft {
            template_id: _,
            version: _,
            base: _,
            params: _,
            tenant_id: _,
            actor_scope: _,
            purpose_id: _,
            policy_digest: _,
        } = draft();

        type Mutator = (&'static str, fn(&mut AgentTemplateDraft));
        let mutators: &[Mutator] = &[
            ("template_id", |d| d.template_id = "template:other".into()),
            ("version", |d| d.version = "2.0.0".into()),
            ("base", |d| d.base.version = "9.9.9".into()),
            ("params.name", |d| {
                d.params[0].name = "model-tier".into();
            }),
            ("params.replaces", |d| {
                d.params[1].replaces = "tool:summarize".into();
            }),
            ("params.kind", |d| {
                // `replaces` has to move with the kind, or the parameter no
                // longer names anything in the base and publish refuses it.
                d.params[1].kind = AgentComponentKind::Skill;
                d.params[1].replaces = "skill:research".into();
            }),
            ("params.required", |d| d.params[0].required = true),
            ("params.summary", |d| {
                d.params[0].summary = "a different axis".into();
            }),
            ("tenant_id", |d| {
                // The base carries the same tenant and validate() enforces it.
                d.tenant_id = "tenant-b".into();
                d.base.tenant_id = "tenant-b".into();
            }),
            ("actor_scope", |d| d.actor_scope = "operator".into()),
            ("purpose_id", |d| d.purpose_id = "agent-rebuild".into()),
            ("policy_digest", |d| d.policy_digest = digest('a')),
        ];

        let baseline = template().definition_digest;
        for (field, mutate) in mutators {
            let mut altered = draft();
            mutate(&mut altered);
            assert_ne!(
                altered,
                draft(),
                "{field}: the mutator changed nothing, so the test proves nothing"
            );
            let moved = AgentTemplateEntry::publish(altered, 1, 1_000)
                .unwrap_or_else(|error| panic!("{field} must still publish: {error}"))
                .definition_digest;
            assert_ne!(
                moved, baseline,
                "{field} is stored but not covered by the definition digest"
            );
        }
    }
}
