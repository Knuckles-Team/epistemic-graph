//! Builders for assembly inputs. Candidates are built as CandidateFacts
//! directly: there is deliberately no description or summary field on a
//! candidate, which is the structural reason prompt-injection text in a
//! component's description cannot reach the decision.

use eg_types::agent_component::{
    AgentComponentFacts, AgentComponentKind, CostFacts, DeclaredCost, DeclaredLatency, FactQuality,
    ModalityFacts, PriceSource, PromptMode, ToolEffect,
};
use eg_types::agent_library::AgentLibraryLifecycle;
use eg_types::contract::BoundedVec;
use eg_types::decision::{
    digest, AssemblyRequest, AssemblyRequirements, CandidateFacts, DecisionInputs, DecisionPolicy,
    DecisionPolicyRef, LibraryCandidateScope, PremiseClass, PremiseProvenance, PremiseRef,
};

use super::super::RecordIdentity;

pub const TENANT: &str = "tenant-a";

pub fn bounded<T, const N: usize>(values: Vec<T>) -> BoundedVec<T, N> {
    BoundedVec::new(values).expect("fixture values fit their bound")
}

/// A distinct, well-formed digest for `seed`.
pub fn digest_of(seed: &str) -> String {
    digest::digest_text("eg/test-fixture/v1", &seed)
}

fn claim(component_id: &str) -> PremiseRef {
    PremiseRef {
        subject: component_id.to_string(),
        fact: "classification".to_string(),
        class: PremiseClass::Claim,
        provenance: PremiseProvenance::Publisher {
            component_id: component_id.to_string(),
            definition_digest: digest_of(component_id),
        },
    }
}

pub fn candidate(
    component_id: &str,
    kind: AgentComponentKind,
    classification: &[&str],
) -> CandidateFacts {
    CandidateFacts {
        component_id: component_id.to_string(),
        kind,
        entry_revision: 1,
        definition_digest: digest_of(component_id),
        lifecycle: AgentLibraryLifecycle::Published,
        classification: bounded(classification.iter().map(|t| t.to_string()).collect()),
        required_capabilities: BoundedVec::default(),
        declared_capabilities: BoundedVec::default(),
        requires: BoundedVec::default(),
        facts: AgentComponentFacts::Opaque,
        fact_premises: bounded(vec![claim(component_id)]),
    }
}

pub fn cost(per_call_micros: Option<u64>) -> Option<CostFacts> {
    Some(CostFacts {
        declared: DeclaredCost {
            currency: "USD".to_string(),
            per_call_micros,
            input_per_mtok_micros: None,
            output_per_mtok_micros: None,
        },
        price_source: PriceSource::Publisher,
        quality: FactQuality::Declared,
    })
}

pub struct ModelSpec {
    pub window: u32,
    pub tools: bool,
    pub per_call: Option<u64>,
    pub p95: Option<u32>,
}

pub fn model(component_id: &str, spec: ModelSpec) -> CandidateFacts {
    let mut model = candidate(
        component_id,
        AgentComponentKind::ModelProfile,
        &["eg:capability/generation/text"],
    );
    model.facts = AgentComponentFacts::ModelProfile {
        provider: "provider".to_string(),
        model_identity: format!("{component_id}-identity"),
        context_window_tokens: spec.window,
        max_output_tokens: 1,
        supports_tools: spec.tools,
        supports_structured_output: true,
        supports_vision: false,
        modalities: ModalityFacts {
            input: vec!["eg:modality/text".to_string()],
            output: vec!["eg:modality/text".to_string()],
        },
        cost: cost(spec.per_call),
        latency_declared: spec.p95.map(|p95_ms| DeclaredLatency { p50_ms: 1, p95_ms }),
        latency_observed_ref: None,
    };
    model
}

pub fn cheap_model(component_id: &str) -> CandidateFacts {
    model(
        component_id,
        ModelSpec {
            window: 128_000,
            tools: true,
            per_call: Some(10),
            p95: Some(100),
        },
    )
}

pub fn prompt(component_id: &str, tokens: u32) -> CandidateFacts {
    let mut prompt = candidate(
        component_id,
        AgentComponentKind::SystemPrompt,
        &["eg:capability/reasoning/plan"],
    );
    prompt.facts = AgentComponentFacts::SystemPrompt {
        prompt_mode: PromptMode::Static,
        token_estimate: tokens,
        variables: Vec::new(),
    };
    prompt
}

pub fn tool(component_id: &str, classification: &[&str], per_call: Option<u64>) -> CandidateFacts {
    let mut tool = candidate(component_id, AgentComponentKind::Tool, classification);
    tool.facts = AgentComponentFacts::Tool {
        effect: ToolEffect::Read,
        required_scopes: Vec::new(),
        input_schema_digest: None,
        output_schema_digest: None,
        read_only_hint: None,
        destructive_hint: None,
        idempotent_hint: None,
        open_world_hint: None,
        modalities: ModalityFacts::default(),
        cost: cost(per_call),
        latency_declared: Some(DeclaredLatency {
            p50_ms: 1,
            p95_ms: 50,
        }),
    };
    tool
}

pub fn all_kinds() -> Vec<AgentComponentKind> {
    vec![
        AgentComponentKind::ModelProfile,
        AgentComponentKind::SystemPrompt,
        AgentComponentKind::Tool,
        AgentComponentKind::Toolset,
        AgentComponentKind::Skill,
        AgentComponentKind::Ontology,
        AgentComponentKind::A2aAgentCard,
    ]
}

pub fn request(tasks: &[&str], capabilities: &[&str]) -> AssemblyRequest {
    AssemblyRequest {
        tenant_id: TENANT.to_string(),
        requirements: AssemblyRequirements {
            tasks: bounded(tasks.iter().map(|t| t.to_string()).collect()),
            capabilities: bounded(capabilities.iter().map(|t| t.to_string()).collect()),
            ..AssemblyRequirements::default()
        },
        candidates: LibraryCandidateScope {
            kinds: bounded(all_kinds()),
            classification_under: None,
        },
        templates: BoundedVec::default(),
        policy: DecisionPolicyRef::Default,
        solver: None,
    }
}

/// Complete, self-consistent inputs under `policy`.
pub fn inputs_with(
    request: AssemblyRequest,
    candidates: Vec<CandidateFacts>,
    policy: DecisionPolicy,
) -> DecisionInputs {
    super::super::inputs(request, candidates, Vec::new(), policy).expect("fixture inputs fit")
}

pub fn inputs(request: AssemblyRequest, candidates: Vec<CandidateFacts>) -> DecisionInputs {
    inputs_with(request, candidates, DecisionPolicy::engine_default())
}

pub fn identity() -> RecordIdentity {
    RecordIdentity {
        tenant_id: TENANT.to_string(),
        caller_principal: "caller-a".to_string(),
        created_at_ms: 1_700_000_000_000,
    }
}

/// The research routing item: one model, one prompt, and tools that cover
/// retrieval and summarisation at different prices.
/// The skill and ontology every assembled agent needs one of.
pub fn agent_basics() -> Vec<CandidateFacts> {
    vec![
        candidate("skill-a", AgentComponentKind::Skill, &[]),
        candidate("ontology-a", AgentComponentKind::Ontology, &[]),
    ]
}

pub fn research_library() -> Vec<CandidateFacts> {
    let mut library = agent_basics();
    library.extend(vec![
        cheap_model("model-cheap"),
        model(
            "model-dear",
            ModelSpec {
                window: 200_000,
                tools: true,
                per_call: Some(900),
                p95: Some(400),
            },
        ),
        prompt("prompt-research", 2_000),
        tool("tool-web", &["eg:capability/retrieval/web-search"], Some(5)),
        tool(
            "tool-vector",
            &["eg:capability/retrieval/vector-search"],
            Some(3),
        ),
        tool(
            "tool-summarize",
            &["eg:capability/analysis/summarize"],
            Some(2),
        ),
        tool(
            "tool-swiss",
            &[
                "eg:capability/retrieval/web-search",
                "eg:capability/analysis/summarize",
            ],
            Some(50),
        ),
    ]);
    library
}
