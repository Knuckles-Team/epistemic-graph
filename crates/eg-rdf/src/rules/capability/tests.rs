use eg_types::agent_component::{
    AgentComponentFacts, DeclaredLatency, ModalityFacts, ObservationRef, ToolEffect,
    ToolsetTransport,
};

use super::*;
use crate::rules::ASSERTED_RULE;

fn model(window: u32, vision: bool, output: &[&str]) -> AgentComponentFacts {
    AgentComponentFacts::ModelProfile {
        provider: "example".into(),
        model_identity: "m".into(),
        context_window_tokens: window,
        max_output_tokens: 4_096,
        supports_tools: true,
        supports_structured_output: false,
        supports_vision: vision,
        modalities: ModalityFacts {
            input: vec!["eg:modality/text".into()],
            output: output.iter().map(|m| m.to_string()).collect(),
        },
        cost: None,
        latency_declared: Some(DeclaredLatency {
            p50_ms: 300,
            p95_ms: 900,
        }),
        latency_observed_ref: None,
    }
}

fn tool(effect: ToolEffect) -> AgentComponentFacts {
    AgentComponentFacts::Tool {
        effect,
        required_scopes: Vec::new(),
        input_schema_digest: None,
        output_schema_digest: None,
        read_only_hint: Some(true),
        destructive_hint: None,
        idempotent_hint: None,
        open_world_hint: None,
        modalities: ModalityFacts::default(),
        cost: None,
        latency_declared: None,
    }
}

fn classify(components: &[(&str, &AgentComponentFacts)]) -> CapabilityClassification {
    let profiled: Vec<ProfiledComponent<'_>> = components
        .iter()
        .map(|&(component_id, facts)| ProfiledComponent {
            component_id,
            facts,
        })
        .collect();
    classify_components(
        &profiled,
        &ClassificationPolicy::default(),
        &Ontology::default(),
        &RuleSet::new(),
    )
}

#[test]
fn a_model_profile_is_classified_from_its_declared_facts() {
    let big = model(200_000, true, &["eg:modality/text"]);
    let small = model(8_000, false, &["eg:modality/text"]);
    let out = classify(&[("model:big", &big), ("model:small", &small)]);
    for class in [
        "eg:profile/tool-calling",
        "eg:profile/vision",
        "eg:profile/long-context",
        "eg:profile/low-latency",
        "eg:capability/generation/text",
    ] {
        assert!(out.has_class("model:big", class), "big lacks {class}");
    }
    assert!(!out.has_class("model:small", "eg:profile/long-context"));
    assert!(!out.has_class("model:small", "eg:profile/vision"));
    assert!(!out.has_class("model:big", "eg:profile/structured-output"));
    assert!(!out.has_class("model:big", "eg:profile/measured-latency"));
}

#[test]
fn a_derived_capability_is_subsumed_up_the_native_hierarchy_with_a_proof() {
    let facts = model(8_000, false, &["eg:modality/text"]);
    let out = classify(&[("model:m", &facts)]);
    let classes = &out.components["model:m"];
    let generation = classes
        .iter()
        .find(|c| c.class == "eg:capability/generation")
        .expect("broader of generation/text is derived");
    assert_eq!(
        generation.proof.rule,
        "broader:eg:capability/generation/text"
    );
    let text = &generation.proof.premises[0];
    assert_eq!(text.predicate, "eg:capability/generation/text");
    assert_eq!(text.rule, "capability/generation-text");
    // The leaves are the declared facts themselves.
    assert!(text
        .premises
        .iter()
        .all(|premise| premise.rule == ASSERTED_RULE));
    assert!(text
        .premises
        .iter()
        .any(|premise| premise.predicate == "eg:fact/output_modality"));
    assert!(out.has_class("model:m", "eg:capability"));
}

#[test]
fn tools_are_split_by_their_declared_effect() {
    let read = tool(ToolEffect::Read);
    let write = tool(ToolEffect::Write);
    let out = classify(&[("tool:r", &read), ("tool:w", &write)]);
    assert!(out.has_class("tool:r", "eg:profile/read-only-tool"));
    assert!(!out.has_class("tool:r", "eg:profile/side-effecting-tool"));
    assert!(out.has_class("tool:w", "eg:profile/side-effecting-tool"));
}

#[test]
fn a_caller_rule_extends_the_classification_in_the_same_fixpoint() {
    let facts = model(200_000, true, &["eg:modality/text"]);
    let mut extra = RuleSet::new();
    extra.register(Rule {
        name: "caller/multimodal-analyst".into(),
        body: vec![
            component_class("eg:profile/vision"),
            component_class("eg:profile/long-context"),
        ],
        head: vec![component_class("example:multimodal-analyst")],
        conf: 0.9,
    });
    let profiled = [ProfiledComponent {
        component_id: "model:m",
        facts: &facts,
    }];
    let out = classify_components(
        &profiled,
        &ClassificationPolicy::default(),
        &Ontology::default(),
        &extra,
    );
    let derived = out.components["model:m"]
        .iter()
        .find(|c| c.class == "example:multimodal-analyst")
        .expect("caller rule fired over derived classes");
    assert!((derived.confidence - 0.9).abs() < 1e-12);
    assert_eq!(derived.proof.premises.len(), 2);
}

#[test]
fn measured_latency_opaque_and_toolset_components_project_their_own_atoms() {
    let mut measured = model(8_000, false, &[]);
    if let AgentComponentFacts::ModelProfile {
        latency_observed_ref,
        ..
    } = &mut measured
    {
        *latency_observed_ref = Some(ObservationRef {
            evaluation_id: "eval:1".into(),
            digest: "sha256:x".into(),
        });
    }
    let toolset = AgentComponentFacts::Toolset {
        transport: ToolsetTransport::Mcp,
    };
    let out = classify(&[
        ("model:m", &measured),
        ("toolset:t", &toolset),
        ("opaque:o", &AgentComponentFacts::Opaque),
    ]);
    assert!(out.has_class("model:m", "eg:profile/measured-latency"));
    assert!(out.components["opaque:o"].is_empty());
    let atoms = component_atoms(&ProfiledComponent {
        component_id: "toolset:t",
        facts: &toolset,
    });
    assert!(atoms
        .iter()
        .any(|(pred, args, _)| pred == TRANSPORT && args[1] == "mcp"));
}

#[test]
fn every_builtin_rule_names_a_distinct_rule() {
    let rules = builtin_classification_rules(&ClassificationPolicy::default());
    let mut names = rules.names();
    let total = names.len();
    names.sort();
    names.dedup();
    assert_eq!(names.len(), total);
    assert!(total > DERIVATION_RULES.len());
}

/// The thresholds are policy inputs: a stricter policy drops a class the defaults
/// derive, a looser one adds a class the defaults do not.
#[test]
fn a_non_default_policy_changes_the_classification() {
    let facts = model(200_000, false, &["eg:modality/text"]);
    let profiled = [ProfiledComponent {
        component_id: "model:m",
        facts: &facts,
    }];
    let run = |policy: ClassificationPolicy| {
        classify_components(&profiled, &policy, &Ontology::default(), &RuleSet::new())
    };
    let defaults = run(ClassificationPolicy::default());
    assert!(defaults.has_class("model:m", "eg:profile/long-context"));
    assert!(defaults.has_class("model:m", "eg:profile/low-latency"));
    let strict = run(ClassificationPolicy {
        long_context_tokens: 1_000_000,
        low_latency_p95_ms: 500,
    });
    assert!(!strict.has_class("model:m", "eg:profile/long-context"));
    assert!(!strict.has_class("model:m", "eg:profile/low-latency"));
    let small = model(8_000, false, &["eg:modality/text"]);
    let loose = classify_components(
        &[ProfiledComponent {
            component_id: "model:s",
            facts: &small,
        }],
        &ClassificationPolicy {
            long_context_tokens: 4_000,
            ..ClassificationPolicy::default()
        },
        &Ontology::default(),
        &RuleSet::new(),
    );
    assert!(loose.has_class("model:s", "eg:profile/long-context"));
}
