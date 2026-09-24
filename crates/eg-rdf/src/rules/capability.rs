//! Rule-derived classification over agent-component metadata (EH-200).
//!
//! An `AgentComponentFacts` record declares what a component IS — a model profile's
//! context window, modalities, tool support and latency; a tool's effect and hints —
//! but not which classes that makes it a member of. Selection asks the class question
//! ("a long-context vision model", "a read-only tool", "something that generates
//! text"), and answering it from the raw fields in every caller is the naming-by-
//! convention trap again. This module derives the classes ONCE, by rules, with a proof.
//!
//! 1. The typed facts are projected to ground atoms under the `eg:fact/*` and
//!    `eg:component/*` vocabulary ([`component_atoms`]).
//! 2. The built-in [`DERIVATION_RULES`] derive `eg:profile/*` classes and the native
//!    `eg:capability/*` terms a fact entails; every `broader` edge of the native
//!    [`AGENT_ONTOLOGY`] becomes a subsumption rule, so a derived
//!    `eg:capability/generation/text` also derives `eg:capability/generation` and
//!    `eg:capability`. Caller rules and OWL axioms run in the SAME fixpoint.
//! 3. Each derived class comes back with its [`RuleProofNode`] (EH-197): the rule that
//!    fired and the declared facts it fired over.
//!
//! Every derived class rests on DECLARED facts. The proof names them, so a decision
//! that leans on a derived class can still classify its premise as a claim.

use std::collections::BTreeMap;

use eg_types::agent_component::{AgentComponentFacts, ModalityFacts, ToolEffect};
use eg_types::agent_ontology::AGENT_ONTOLOGY;

use super::proof::RuleProofNode;
use super::{reason_facts, Atom, RTerm, Rule, RuleSet};
use crate::owl::Ontology;

/// Unary class of a model-profile component.
pub const MODEL_PROFILE: &str = "eg:component/model_profile";
/// Unary class of a tool component.
pub const TOOL: &str = "eg:component/tool";
/// Unary class of a toolset component.
pub const TOOLSET: &str = "eg:component/toolset";
/// Unary class of a system-prompt component.
pub const SYSTEM_PROMPT: &str = "eg:component/system_prompt";

const SUPPORTS_TOOLS: &str = "eg:fact/supports_tools";
const SUPPORTS_STRUCTURED_OUTPUT: &str = "eg:fact/supports_structured_output";
const SUPPORTS_VISION: &str = "eg:fact/supports_vision";
const INPUT_MODALITY: &str = "eg:fact/input_modality";
const OUTPUT_MODALITY: &str = "eg:fact/output_modality";
const CONTEXT_WINDOW_TOKENS: &str = "eg:fact/context_window_tokens";
const MAX_OUTPUT_TOKENS: &str = "eg:fact/max_output_tokens";
const P95_LATENCY_MS: &str = "eg:fact/p95_latency_ms";
const LATENCY_MEASURED: &str = "eg:fact/latency_measured";
const EFFECT: &str = "eg:fact/effect";
const READ_ONLY_HINT: &str = "eg:fact/read_only_hint";
const DESTRUCTIVE_HINT: &str = "eg:fact/destructive_hint";
const IDEMPOTENT_HINT: &str = "eg:fact/idempotent_hint";
const OPEN_WORLD_HINT: &str = "eg:fact/open_world_hint";
const TRANSPORT: &str = "eg:fact/transport";

/// Default smallest context window (tokens) that makes a model `eg:profile/long-context`.
pub const DEFAULT_LONG_CONTEXT_TOKENS: u32 = 128_000;
/// Default largest declared p95 latency (ms) that makes a component
/// `eg:profile/low-latency`.
pub const DEFAULT_LOW_LATENCY_P95_MS: u32 = 1_000;

/// The tunable parameters the built-in classification rules read. The defaults are the
/// documented `DEFAULT_*` constants; a caller that means something else by "long
/// context" or "low latency" passes its own policy instead of editing a rule.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClassificationPolicy {
    pub long_context_tokens: u32,
    pub low_latency_p95_ms: u32,
}

impl Default for ClassificationPolicy {
    fn default() -> Self {
        Self {
            long_context_tokens: DEFAULT_LONG_CONTEXT_TOKENS,
            low_latency_p95_ms: DEFAULT_LOW_LATENCY_P95_MS,
        }
    }
}

/// A threshold a built-in rule takes from the [`ClassificationPolicy`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PolicyBound {
    LongContextTokens,
    LowLatencyP95Ms,
}

impl ClassificationPolicy {
    /// The value this policy gives `bound`.
    pub fn bound(&self, bound: PolicyBound) -> u32 {
        match bound {
            PolicyBound::LongContextTokens => self.long_context_tokens,
            PolicyBound::LowLatencyP95Ms => self.low_latency_p95_ms,
        }
    }
}

/// One body atom of a built-in derivation rule, over the component variable `?c`.
#[derive(Clone, Copy, Debug)]
pub enum BodyAtom {
    /// `class(?c)`.
    Is(&'static str),
    /// `fact(?c, value)`.
    Has(&'static str, &'static str),
    /// `fact(?c, ?v) ∧ ?v >= policy bound`.
    AtLeast(&'static str, PolicyBound),
    /// `fact(?c, ?v) ∧ ?v <= policy bound`.
    AtMost(&'static str, PolicyBound),
}

/// A built-in derivation rule: `body → class(?c)`, named `rule`.
#[derive(Clone, Copy, Debug)]
pub struct DerivationRule {
    pub rule: &'static str,
    pub body: &'static [BodyAtom],
    pub class: &'static str,
}

const fn derive(
    rule: &'static str,
    body: &'static [BodyAtom],
    class: &'static str,
) -> DerivationRule {
    DerivationRule { rule, body, class }
}

/// The built-in classification rules. Every class here is either an `eg:profile/*`
/// selection trait or a native `eg:capability/*` term a declared fact entails.
pub const DERIVATION_RULES: &[DerivationRule] = &[
    derive(
        "profile/tool-calling",
        &[BodyAtom::Is(MODEL_PROFILE), BodyAtom::Is(SUPPORTS_TOOLS)],
        "eg:profile/tool-calling",
    ),
    derive(
        "profile/structured-output",
        &[
            BodyAtom::Is(MODEL_PROFILE),
            BodyAtom::Is(SUPPORTS_STRUCTURED_OUTPUT),
        ],
        "eg:profile/structured-output",
    ),
    derive(
        "profile/vision",
        &[BodyAtom::Is(MODEL_PROFILE), BodyAtom::Is(SUPPORTS_VISION)],
        "eg:profile/vision",
    ),
    derive(
        "profile/vision-input",
        &[BodyAtom::Has(INPUT_MODALITY, "eg:modality/image")],
        "eg:profile/vision",
    ),
    derive(
        "profile/long-context",
        &[
            BodyAtom::Is(MODEL_PROFILE),
            BodyAtom::AtLeast(CONTEXT_WINDOW_TOKENS, PolicyBound::LongContextTokens),
        ],
        "eg:profile/long-context",
    ),
    derive(
        "profile/low-latency",
        &[BodyAtom::AtMost(
            P95_LATENCY_MS,
            PolicyBound::LowLatencyP95Ms,
        )],
        "eg:profile/low-latency",
    ),
    derive(
        "profile/measured-latency",
        &[BodyAtom::Is(LATENCY_MEASURED)],
        "eg:profile/measured-latency",
    ),
    derive(
        "profile/read-only-tool",
        &[BodyAtom::Is(TOOL), BodyAtom::Has(EFFECT, "read")],
        "eg:profile/read-only-tool",
    ),
    derive(
        "profile/side-effecting-tool",
        &[BodyAtom::Is(TOOL), BodyAtom::Has(EFFECT, "write")],
        "eg:profile/side-effecting-tool",
    ),
    derive(
        "capability/generation-text",
        &[
            BodyAtom::Is(MODEL_PROFILE),
            BodyAtom::Has(OUTPUT_MODALITY, "eg:modality/text"),
        ],
        "eg:capability/generation/text",
    ),
    derive(
        "capability/generation-image",
        &[
            BodyAtom::Is(MODEL_PROFILE),
            BodyAtom::Has(OUTPUT_MODALITY, "eg:modality/image"),
        ],
        "eg:capability/generation/image",
    ),
    derive(
        "capability/generation-speech",
        &[
            BodyAtom::Is(MODEL_PROFILE),
            BodyAtom::Has(OUTPUT_MODALITY, "eg:modality/audio"),
        ],
        "eg:capability/generation/speech",
    ),
];

/// One component to classify.
#[derive(Clone, Copy, Debug)]
pub struct ProfiledComponent<'a> {
    pub component_id: &'a str,
    pub facts: &'a AgentComponentFacts,
}

/// One class a component was derived to belong to, with its proof.
#[derive(Clone, Debug, PartialEq)]
pub struct DerivedClass {
    pub class: String,
    pub confidence: f64,
    pub proof: RuleProofNode,
}

/// Every component's derived classes, sorted by class IRI.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CapabilityClassification {
    pub components: BTreeMap<String, Vec<DerivedClass>>,
}

impl CapabilityClassification {
    /// Whether `component_id` was derived to belong to `class`.
    pub fn has_class(&self, component_id: &str, class: &str) -> bool {
        self.components
            .get(component_id)
            .is_some_and(|classes| classes.iter().any(|c| c.class == class))
    }
}

/// A ground atom `(predicate, args, confidence)`.
pub type GroundAtom = (String, Vec<String>, f64);

/// Classify `components` under the built-in rules (parameterised by `policy`), the
/// native ontology's subsumption, `ontology`'s own axioms and the caller's `extra`
/// rules, in one fixpoint.
pub fn classify_components(
    components: &[ProfiledComponent<'_>],
    policy: &ClassificationPolicy,
    ontology: &Ontology,
    extra: &RuleSet,
) -> CapabilityClassification {
    let atoms: Vec<GroundAtom> = components.iter().flat_map(component_atoms).collect();
    let mut rules = builtin_classification_rules(policy);
    for rule in extra.rules() {
        rules.register(rule.clone());
    }
    let result = reason_facts(ontology, &atoms, &rules);
    let mut out = CapabilityClassification::default();
    for component in components {
        out.components
            .insert(component.component_id.to_string(), Vec::new());
    }
    for (class, args, confidence) in &result.derived {
        let [component_id] = args.as_slice() else {
            continue;
        };
        let Some(classes) = out.components.get_mut(component_id) else {
            continue;
        };
        if let Some(proof) = result.derivations.proof(class, args) {
            classes.push(DerivedClass {
                class: class.clone(),
                confidence: *confidence,
                proof,
            });
        }
    }
    out
}

/// The built-in rule set: [`DERIVATION_RULES`] with `policy`'s thresholds, plus one
/// subsumption rule per `broader` edge of the native ontology.
pub fn builtin_classification_rules(policy: &ClassificationPolicy) -> RuleSet {
    let mut rules = RuleSet::new();
    for spec in DERIVATION_RULES {
        rules.register(derivation_rule(spec, policy));
    }
    for term in AGENT_ONTOLOGY {
        if let Some(broader) = term.broader {
            rules.register(Rule {
                name: format!("broader:{}", term.iri),
                body: vec![component_class(term.iri)],
                head: vec![component_class(broader)],
                conf: 1.0,
            });
        }
    }
    rules
}

fn derivation_rule(spec: &DerivationRule, policy: &ClassificationPolicy) -> Rule {
    Rule {
        name: spec.rule.to_string(),
        body: spec
            .body
            .iter()
            .flat_map(|atom| body_atoms(atom, policy))
            .collect(),
        head: vec![component_class(spec.class)],
        conf: 1.0,
    }
}

/// The rule atoms one [`BodyAtom`] expands to. Each thresholded fact gets its own
/// value variable, so two thresholds in one rule never alias.
fn body_atoms(atom: &BodyAtom, policy: &ClassificationPolicy) -> Vec<Atom> {
    match *atom {
        BodyAtom::Is(class) => vec![component_class(class)],
        BodyAtom::Has(fact, value) => vec![component_fact(fact, RTerm::Const(value.into()))],
        BodyAtom::AtLeast(fact, bound) => {
            threshold(fact, "greaterThanOrEqual", policy.bound(bound))
        }
        BodyAtom::AtMost(fact, bound) => threshold(fact, "lessThanOrEqual", policy.bound(bound)),
    }
}

fn threshold(fact: &str, comparison: &str, bound: u32) -> Vec<Atom> {
    let value = format!("v_{}", fact.replace(['/', ':'], "_"));
    vec![
        component_fact(fact, RTerm::Var(value.clone())),
        Atom {
            pred: format!("swrlb:{comparison}"),
            args: vec![RTerm::Var(value), RTerm::Const(bound.to_string())],
        },
    ]
}

fn component_class(class: &str) -> Atom {
    Atom {
        pred: class.to_string(),
        args: vec![RTerm::Var("c".into())],
    }
}

fn component_fact(fact: &str, value: RTerm) -> Atom {
    Atom {
        pred: fact.to_string(),
        args: vec![RTerm::Var("c".into()), value],
    }
}

/// The ground atoms one component's typed facts project to.
pub fn component_atoms(component: &ProfiledComponent<'_>) -> Vec<GroundAtom> {
    let mut atoms = AtomSink {
        id: component.component_id,
        out: Vec::new(),
    };
    match component.facts {
        AgentComponentFacts::ModelProfile {
            context_window_tokens,
            max_output_tokens,
            supports_tools,
            supports_structured_output,
            supports_vision,
            modalities,
            latency_declared,
            latency_observed_ref,
            ..
        } => {
            atoms.class(MODEL_PROFILE);
            atoms.flags(&[
                (SUPPORTS_TOOLS, *supports_tools),
                (SUPPORTS_STRUCTURED_OUTPUT, *supports_structured_output),
                (SUPPORTS_VISION, *supports_vision),
                (LATENCY_MEASURED, latency_observed_ref.is_some()),
            ]);
            atoms.fact(CONTEXT_WINDOW_TOKENS, context_window_tokens.to_string());
            atoms.fact(MAX_OUTPUT_TOKENS, max_output_tokens.to_string());
            atoms.modalities(modalities);
            atoms.latency(latency_declared.map(|latency| latency.p95_ms));
        }
        AgentComponentFacts::Tool {
            effect,
            read_only_hint,
            destructive_hint,
            idempotent_hint,
            open_world_hint,
            modalities,
            latency_declared,
            ..
        } => {
            atoms.class(TOOL);
            atoms.fact(EFFECT, effect_value(*effect).to_string());
            atoms.flags(&[
                (READ_ONLY_HINT, *read_only_hint == Some(true)),
                (DESTRUCTIVE_HINT, *destructive_hint == Some(true)),
                (IDEMPOTENT_HINT, *idempotent_hint == Some(true)),
                (OPEN_WORLD_HINT, *open_world_hint == Some(true)),
            ]);
            atoms.modalities(modalities);
            atoms.latency(latency_declared.map(|latency| latency.p95_ms));
        }
        AgentComponentFacts::Toolset { transport } => {
            atoms.class(TOOLSET);
            let transport = serde_json::to_value(transport)
                .ok()
                .and_then(|value| value.as_str().map(str::to_string));
            if let Some(transport) = transport {
                atoms.fact(TRANSPORT, transport);
            }
        }
        AgentComponentFacts::SystemPrompt { .. } => atoms.class(SYSTEM_PROMPT),
        AgentComponentFacts::Opaque => {}
    }
    atoms.out
}

fn effect_value(effect: ToolEffect) -> &'static str {
    match effect {
        ToolEffect::Read => "read",
        ToolEffect::Write => "write",
    }
}

/// Collects one component's ground atoms.
struct AtomSink<'a> {
    id: &'a str,
    out: Vec<GroundAtom>,
}

impl AtomSink<'_> {
    fn class(&mut self, class: &str) {
        self.out
            .push((class.to_string(), vec![self.id.to_string()], 1.0));
    }

    fn fact(&mut self, fact: &str, value: String) {
        self.out
            .push((fact.to_string(), vec![self.id.to_string(), value], 1.0));
    }

    fn flags(&mut self, flags: &[(&str, bool)]) {
        for (flag, set) in flags {
            if *set {
                self.class(flag);
            }
        }
    }

    fn modalities(&mut self, modalities: &ModalityFacts) {
        for input in &modalities.input {
            self.fact(INPUT_MODALITY, input.clone());
        }
        for output in &modalities.output {
            self.fact(OUTPUT_MODALITY, output.clone());
        }
    }

    fn latency(&mut self, p95_ms: Option<u32>) {
        if let Some(p95_ms) = p95_ms {
            self.fact(P95_LATENCY_MS, p95_ms.to_string());
        }
    }
}

#[cfg(test)]
mod tests;
