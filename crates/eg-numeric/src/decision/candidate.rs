//! The engine's view of one candidate: only the facts features may read.
//!
//! Built from a published library component or from one visible graph row.
//! A candidate carries no reference back to its store, so a feature cannot
//! reach past it into rows the caller cannot see.

use std::collections::BTreeMap;

use eg_types::agent_component::{
    AgentComponentEntry, AgentComponentFacts, AgentComponentKind, FactQuality,
};
use eg_types::decision::statistical::body::decode_body;
use eg_types::decision::statistical::nl::NlTemplateBody;

/// Text field name of a component's own summary.
pub const SUMMARY_TEXT: &str = "summary";
/// Text field name of an NL template's utterances, joined.
pub const NL_UTTERANCES_TEXT: &str = "nl.utterances";
/// Text field name of an NL template's labels, joined.
pub const NL_LABELS_TEXT: &str = "nl.labels";

/// Everything a feature may read about one option.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CandidateView {
    pub id: String,
    pub classification: Vec<String>,
    pub cost_micros: Option<u64>,
    pub cost_quality: Option<FactQuality>,
    pub p95_ms: Option<u32>,
    pub updated_at_ms: Option<u64>,
    /// Named numeric facts, already on `Q32`.
    pub numbers: BTreeMap<String, i64>,
    /// Named text fields.
    pub texts: BTreeMap<String, String>,
}

fn declared_cost(facts: &AgentComponentFacts) -> (Option<u64>, Option<FactQuality>, Option<u32>) {
    match facts {
        AgentComponentFacts::ModelProfile {
            cost,
            latency_declared,
            ..
        }
        | AgentComponentFacts::Tool {
            cost,
            latency_declared,
            ..
        } => (
            cost.as_ref().and_then(|c| c.declared.per_call_micros),
            cost.as_ref().map(|c| c.quality),
            latency_declared.map(|l| l.p95_ms),
        ),
        AgentComponentFacts::SystemPrompt { .. }
        | AgentComponentFacts::Toolset { .. }
        | AgentComponentFacts::Opaque => (None, None, None),
    }
}

fn template_texts(entry: &AgentComponentEntry, texts: &mut BTreeMap<String, String>) {
    if entry.kind != AgentComponentKind::NlTemplate {
        return;
    }
    if let Ok(body) = decode_body::<NlTemplateBody>(&entry.content_digest, &entry.attributes) {
        let utterances: Vec<&str> = body.utterances.iter().map(String::as_str).collect();
        let labels: Vec<&str> = body.labels.iter().map(String::as_str).collect();
        texts.insert(NL_UTTERANCES_TEXT.to_string(), utterances.join(" "));
        texts.insert(NL_LABELS_TEXT.to_string(), labels.join(" "));
    }
}

impl CandidateView {
    /// The view of one published library component.
    pub fn from_component(entry: &AgentComponentEntry) -> Self {
        let (cost_micros, cost_quality, p95_ms) = declared_cost(&entry.facts);
        let mut texts = BTreeMap::from([(SUMMARY_TEXT.to_string(), entry.summary.clone())]);
        template_texts(entry, &mut texts);
        Self {
            id: entry.component_id.clone(),
            classification: entry.classification.clone(),
            cost_micros,
            cost_quality,
            p95_ms,
            updated_at_ms: Some(entry.updated_at_ms),
            numbers: BTreeMap::new(),
            texts,
        }
    }

    /// The view of one visible graph row.
    pub fn from_row(
        id: String,
        numbers: BTreeMap<String, i64>,
        texts: BTreeMap<String, String>,
    ) -> Self {
        Self {
            id,
            numbers,
            texts,
            ..Self::default()
        }
    }
}
