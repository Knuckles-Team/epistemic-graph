//! Kind-specific facts an optimizer needs in order to CHOOSE a component.
//!
//! Split out of the parent module because this is the half the Decide layer
//! reads: what a component costs, how fast it is, which modalities it speaks,
//! and what its tool contract looks like. The parent keeps identity, structure
//! and the definition digest.
//!
//! Every fact here is DECLARED unless it says otherwise. A declaration is a
//! claim, and [`CostFacts::quality`] is what says so on the wire, so a decision
//! that leans on one is classified by that claim rather than by the confidence
//! of its conclusion.

use serde::{Deserialize, Serialize};

use super::{
    validate_names, validate_text, AgentComponentKind, ModelInputCapabilities, ModelInputMediaType,
    MAX_SCOPES, MAX_VARIABLES,
};

/// Longest declared latency that is a latency rather than a typo: one day.
pub const MAX_DECLARED_LATENCY_MS: u32 = 86_400_000;
/// Largest micro-denominated price a component may declare.
pub const MAX_PRICE_MICROS: u64 = 1_000_000_000_000_000;
/// Most modality IRIs one direction may declare.
pub const MAX_MODALITY_ITEMS: usize = 16;

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

/// What a tool may change beyond returning a value. Absent reads as
/// [`ToolSideEffects::ExternalWrite`], the least permissive answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ToolSideEffects {
    None,
    WorkspaceWrite,
    ExternalWrite,
    Network,
}

impl ToolSideEffects {
    const ALL: [Self; 4] = [
        Self::None,
        Self::WorkspaceWrite,
        Self::ExternalWrite,
        Self::Network,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::WorkspaceWrite => "workspace_write",
            Self::ExternalWrite => "external_write",
            Self::Network => "network",
        }
    }
}

/// Who asserted a tool's effect facts. Absent reads as
/// [`ToolFactDeclarer::Connector`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ToolFactDeclarer {
    /// The connector author declared it.
    Connector,
    /// Copied from an MCP `ToolAnnotations` hint, which the MCP specification
    /// says must not be trusted.
    McpHint,
    /// An evaluation certified it.
    Certified,
}

impl ToolFactDeclarer {
    const ALL: [Self; 3] = [Self::Connector, Self::McpHint, Self::Certified];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Connector => "connector",
            Self::McpHint => "mcp_hint",
            Self::Certified => "certified",
        }
    }
}

/// Decode one closed token, naming the field and the accepted set on refusal.
fn closed_token<'de, D, T: Copy>(
    deserializer: D,
    field: &str,
    all: &[T],
    token: fn(T) -> &'static str,
) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    all.iter()
        .copied()
        .find(|candidate| token(*candidate) == value)
        .ok_or_else(|| {
            let accepted: Vec<&str> = all.iter().map(|candidate| token(*candidate)).collect();
            serde::de::Error::custom(format!(
                "{field}: unknown value `{value}`; expected one of {}",
                accepted.join(", ")
            ))
        })
}

impl<'de> Deserialize<'de> for ToolSideEffects {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        closed_token(deserializer, "side_effects", &Self::ALL, Self::as_str)
    }
}

impl<'de> Deserialize<'de> for ToolFactDeclarer {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        closed_token(deserializer, "declared_by", &Self::ALL, Self::as_str)
    }
}

/// Which modalities a component consumes and produces.
///
/// Native `eg:modality/*` IRIs, sorted and unique, so two components that
/// speak the same modalities digest identically whatever order they declared
/// them in.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ModalityFacts {
    #[serde(default)]
    pub input: Vec<String>,
    #[serde(default)]
    pub output: Vec<String>,
}

/// How strong one declared fact is.
///
/// A connector pack carries no measurement and no operator attestation, so a
/// price read out of one is neither measured nor estimated: it is
/// [`FactQuality::Declared`], and a record that used it says so.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum FactQuality {
    Measured,
    Estimated,
    Declared,
    Unavailable,
}

/// Who says what this costs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "source", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum PriceSource {
    /// The model or tool publisher's own list price.
    Publisher,
    /// A connector pack entry, pinned by the entry's digest.
    ConnectorPack {
        connector: String,
        entry_digest: String,
    },
    /// An operator override, with the reference that justifies it.
    Operator { reference: String },
}

/// What one invocation costs, exactly, in a named currency.
///
/// Integer micros, never a float: a budget comparison that depends on binary
/// rounding is a budget that decides differently on two hosts. The same
/// shape a connector pack entry declares for itself
/// ([`crate::connector_pack::PackCost`]) — one owner for "what does this
/// invocation cost", whether the number arrived via a pack import or was
/// otherwise declared.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DeclaredCost {
    /// ISO 4217: exactly three ASCII uppercase letters.
    pub currency: String,
    #[serde(default)]
    pub per_call_micros: Option<u64>,
    #[serde(default)]
    pub input_per_mtok_micros: Option<u64>,
    #[serde(default)]
    pub output_per_mtok_micros: Option<u64>,
}

/// What one invocation costs, plus who says so and how strongly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CostFacts {
    #[serde(flatten)]
    pub declared: DeclaredCost,
    pub price_source: PriceSource,
    pub quality: FactQuality,
}

/// A publisher's declared latency profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DeclaredLatency {
    pub p50_ms: u32,
    pub p95_ms: u32,
}

/// A pointer to a recorded evaluation, for a fact that was MEASURED rather
/// than declared.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ObservationRef {
    pub evaluation_id: String,
    pub digest: String,
}

/// A measured value bound to the evaluation that produced it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "contract-schema", schemars(rename = "MeasuredFact_{T}"))]
pub struct MeasuredFact<T> {
    pub value: T,
    #[serde(deserialize_with = "deserialize_evaluation_ref")]
    pub evaluation_run_ref: ObservationRef,
}

impl<T> MeasuredFact<T> {
    /// Apply the same reference rules as measured latency.
    pub fn validate(&self) -> Result<(), String> {
        validate_observation(Some(&self.evaluation_run_ref))
    }
}

fn deserialize_evaluation_ref<'de, D>(deserializer: D) -> Result<ObservationRef, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let reference = ObservationRef::deserialize(deserializer)?;
    validate_observation(Some(&reference)).map_err(serde::de::Error::custom)?;
    Ok(reference)
}

/// Evaluation-derived ability to use a shell.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ShellProficiency {
    Low,
    Medium,
    High,
}

impl ShellProficiency {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
        }
    }
}

/// Evaluation-derived tendency to stop or continue verification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum StoppingBehaviour {
    StopsEarly,
    Balanced,
    OverVerifies,
}

impl StoppingBehaviour {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::StopsEarly => "stops_early",
            Self::Balanced => "balanced",
            Self::OverVerifies => "over_verifies",
        }
    }
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
        /// Declaration bound by this component's definition digest. Missing is unknown.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        input_capabilities: Option<ModelInputCapabilities>,
        #[serde(default)]
        modalities: ModalityFacts,
        #[serde(default)]
        cost: Option<CostFacts>,
        #[serde(default)]
        latency_declared: Option<DeclaredLatency>,
        /// The evaluation that MEASURED this profile's latency, when one
        /// exists. A declared latency and a measured one are different claims
        /// and a selection that cannot tell them apart cannot say which it used.
        #[serde(default)]
        latency_observed_ref: Option<ObservationRef>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        shell_proficiency: Option<MeasuredFact<ShellProficiency>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        stopping_behaviour: Option<MeasuredFact<StoppingBehaviour>>,
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
        /// `sha256:<hex>` of the served-and-serialized input schema section.
        #[serde(default)]
        input_schema_digest: Option<String>,
        /// `None` means the tool declares no output schema.
        #[serde(default)]
        output_schema_digest: Option<String>,
        #[serde(default)]
        read_only_hint: Option<bool>,
        #[serde(default)]
        destructive_hint: Option<bool>,
        #[serde(default)]
        idempotent_hint: Option<bool>,
        #[serde(default)]
        open_world_hint: Option<bool>,
        #[serde(default)]
        modalities: ModalityFacts,
        #[serde(default)]
        cost: Option<CostFacts>,
        #[serde(default)]
        latency_declared: Option<DeclaredLatency>,
        /// Declared side effects. Absent is unknown, read as `external_write`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        side_effects: Option<ToolSideEffects>,
        /// Who declared the effect facts. Absent reads as `connector`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        declared_by: Option<ToolFactDeclarer>,
        /// Declared schema size in tokens. Absent is unknown, never zero.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        schema_tokens: Option<u32>,
        /// `true` when `read_only_hint` came from an uncertified MCP hint, so
        /// no decision may treat it as a proven read-only fact.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        read_only_unproven: bool,
    },
    Toolset {
        transport: ToolsetTransport,
    },
    /// Every other kind. Structure and dependencies still apply; there is just
    /// nothing kind-specific that selection turns on.
    Opaque,
}

impl AgentComponentFacts {
    /// The kind these facts are only valid for, or `None` for `Opaque`.
    pub(super) fn required_kind(&self) -> Option<AgentComponentKind> {
        match self {
            Self::ModelProfile { .. } => Some(AgentComponentKind::ModelProfile),
            Self::SystemPrompt { .. } => Some(AgentComponentKind::SystemPrompt),
            Self::Tool { .. } => Some(AgentComponentKind::Tool),
            Self::Toolset { .. } => Some(AgentComponentKind::Toolset),
            Self::Opaque => None,
        }
    }

    pub(super) fn label(&self) -> &'static str {
        match self {
            Self::ModelProfile { .. } => "model_profile",
            Self::SystemPrompt { .. } => "system_prompt",
            Self::Tool { .. } => "tool",
            Self::Toolset { .. } => "toolset",
            Self::Opaque => "opaque",
        }
    }

    pub(super) fn validate(&self) -> Result<(), String> {
        match self {
            Self::ModelProfile {
                provider,
                model_identity,
                context_window_tokens,
                max_output_tokens,
                modalities,
                cost,
                latency_declared,
                latency_observed_ref,
                shell_proficiency,
                stopping_behaviour,
                supports_vision,
                input_capabilities,
                ..
            } => {
                validate_model_identity(provider, model_identity)?;
                validate_input_capabilities(input_capabilities.as_ref(), *supports_vision)?;
                validate_window(*context_window_tokens, *max_output_tokens)?;
                validate_observation(latency_observed_ref.as_ref())?;
                if let Some(fact) = shell_proficiency {
                    fact.validate()?;
                }
                if let Some(fact) = stopping_behaviour {
                    fact.validate()?;
                }
                validate_selection_facts(modalities, cost.as_ref(), latency_declared.as_ref())
            }
            Self::SystemPrompt {
                prompt_mode,
                variables,
                ..
            } => validate_prompt(*prompt_mode, variables),
            Self::Tool {
                required_scopes,
                input_schema_digest,
                output_schema_digest,
                modalities,
                cost,
                latency_declared,
                schema_tokens,
                ..
            } => {
                if *schema_tokens == Some(0) {
                    return Err("agent component schema_tokens must be non-zero".into());
                }
                validate_names("required_scopes", required_scopes, MAX_SCOPES)?;
                validate_schema_digest("input_schema_digest", input_schema_digest.as_deref())?;
                validate_schema_digest("output_schema_digest", output_schema_digest.as_deref())?;
                validate_selection_facts(modalities, cost.as_ref(), latency_declared.as_ref())
            }
            Self::Toolset { .. } | Self::Opaque => Ok(()),
        }
    }
}

fn validate_input_capabilities(
    capabilities: Option<&ModelInputCapabilities>,
    supports_vision: bool,
) -> Result<(), String> {
    let Some(capabilities) = capabilities else {
        return Ok(());
    };
    capabilities.validate()?;
    if !supports_vision
        && capabilities
            .media_types
            .iter()
            .any(|media| *media != ModelInputMediaType::Text)
    {
        return Err("image input requires declared vision support".into());
    }
    Ok(())
}

fn validate_model_identity(provider: &str, model_identity: &str) -> Result<(), String> {
    validate_text("provider", provider)?;
    validate_text("model_identity", model_identity)
}

fn validate_window(context_window_tokens: u32, max_output_tokens: u32) -> Result<(), String> {
    if context_window_tokens == 0 {
        return Err("agent component context_window_tokens must be non-zero".into());
    }
    if max_output_tokens == 0 {
        return Err("agent component max_output_tokens must be non-zero".into());
    }
    // A model that cannot emit as much as its own window claims is a
    // transcription error, and it would make every budget computed from these
    // two numbers wrong.
    if max_output_tokens > context_window_tokens {
        return Err("agent component max_output_tokens exceeds its context window".into());
    }
    Ok(())
}

fn validate_prompt(prompt_mode: PromptMode, variables: &[String]) -> Result<(), String> {
    if prompt_mode == PromptMode::Static && !variables.is_empty() {
        return Err(
            "agent component static prompt cannot declare variables: nothing resolves them".into(),
        );
    }
    validate_names("variables", variables, MAX_VARIABLES)
}

/// The three facts BOTH selectable kinds carry. One function, so a model
/// profile and a tool cannot drift into two different ideas of a valid cost.
fn validate_selection_facts(
    modalities: &ModalityFacts,
    cost: Option<&CostFacts>,
    latency: Option<&DeclaredLatency>,
) -> Result<(), String> {
    validate_modalities(modalities)?;
    if let Some(cost) = cost {
        validate_cost(cost)?;
    }
    match latency {
        Some(latency) => validate_latency(latency),
        None => Ok(()),
    }
}

fn validate_modalities(modalities: &ModalityFacts) -> Result<(), String> {
    validate_modality_list("modalities.input", &modalities.input)?;
    validate_modality_list("modalities.output", &modalities.output)
}

fn validate_modality_list(field: &str, values: &[String]) -> Result<(), String> {
    validate_names(field, values, MAX_MODALITY_ITEMS)?;
    if values.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(format!(
            "agent component {field} must be sorted and free of duplicates"
        ));
    }
    Ok(())
}

fn validate_cost(cost: &CostFacts) -> Result<(), String> {
    let currency_is_iso = cost.declared.currency.len() == 3
        && cost
            .declared
            .currency
            .bytes()
            .all(|character| character.is_ascii_uppercase());
    if !currency_is_iso {
        return Err("agent component cost currency must be three ISO 4217 letters".into());
    }
    let prices = [
        cost.declared.per_call_micros,
        cost.declared.input_per_mtok_micros,
        cost.declared.output_per_mtok_micros,
    ];
    if prices
        .iter()
        .flatten()
        .any(|price| *price > MAX_PRICE_MICROS)
    {
        return Err(format!(
            "agent component cost exceeds the {MAX_PRICE_MICROS} micro bound"
        ));
    }
    validate_price_source(&cost.price_source)
}

fn validate_price_source(source: &PriceSource) -> Result<(), String> {
    match source {
        PriceSource::Publisher => Ok(()),
        PriceSource::ConnectorPack {
            connector,
            entry_digest,
        } => {
            validate_text("price_source.connector", connector)?;
            validate_text("price_source.entry_digest", entry_digest)
        }
        PriceSource::Operator { reference } => validate_text("price_source.reference", reference),
    }
}

fn validate_latency(latency: &DeclaredLatency) -> Result<(), String> {
    if latency.p50_ms > latency.p95_ms {
        return Err("agent component declared p50 latency exceeds its p95".into());
    }
    if latency.p95_ms > MAX_DECLARED_LATENCY_MS {
        return Err(format!(
            "agent component declared latency exceeds {MAX_DECLARED_LATENCY_MS} ms"
        ));
    }
    Ok(())
}

fn validate_observation(observation: Option<&ObservationRef>) -> Result<(), String> {
    match observation {
        None => Ok(()),
        Some(observation) => {
            validate_text(
                "latency_observed_ref.evaluation_id",
                &observation.evaluation_id,
            )?;
            validate_text("latency_observed_ref.digest", &observation.digest)
        }
    }
}

fn validate_schema_digest(field: &str, value: Option<&str>) -> Result<(), String> {
    match value {
        None => Ok(()),
        Some(value) if super::is_digest(value) => Ok(()),
        Some(_) => Err(format!("agent component {field} must be sha256:<64 hex>")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // spec: MEDULLA-TYPED-PACKS-R048.2
    #[test]
    fn measured_fact_requires_run_ref() {
        type Fact = MeasuredFact<ShellProficiency>;
        for value in [
            ShellProficiency::Low,
            ShellProficiency::Medium,
            ShellProficiency::High,
        ] {
            let fact = Fact {
                value,
                evaluation_run_ref: ObservationRef {
                    evaluation_id: "eval:42".into(),
                    digest: "existing-opaque-digest".into(),
                },
            };
            assert!(fact.validate().is_ok());
            assert_eq!(
                serde_json::from_value::<Fact>(serde_json::to_value(&fact).unwrap()).unwrap(),
                fact
            );
        }
        for reference in [
            serde_json::Value::Null,
            serde_json::json!({}),
            serde_json::json!({"evaluation_id":"eval:42"}),
        ] {
            assert!(serde_json::from_value::<Fact>(
                serde_json::json!({"value":"high", "evaluation_run_ref":reference})
            )
            .is_err());
        }
        assert!(serde_json::from_value::<Fact>(serde_json::json!({"value":"high"})).is_err());
        for field in ["evaluation_id", "digest"] {
            for bad in [
                "".to_string(),
                " padded".into(),
                "bad\nref".into(),
                "x".repeat(super::super::MAX_TEXT_BYTES + 1),
            ] {
                let mut reference =
                    serde_json::json!({"evaluation_id":"eval:42", "digest":"opaque"});
                reference[field] = bad.into();
                assert!(serde_json::from_value::<Fact>(
                    serde_json::json!({"value":"high", "evaluation_run_ref":reference})
                )
                .is_err());
            }
        }
    }

    #[test]
    fn zero_context_window_is_rejected() {
        let error = validate_window(0, 1).expect_err("a zero context window is invalid");
        assert_eq!(
            error,
            "agent component context_window_tokens must be non-zero"
        );
    }
}
