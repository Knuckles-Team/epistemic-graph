//! Rule G12: what an entry DECLARES must be well-formed before it becomes a
//! fact (pack design §3.10.1, §5.3 split rule, ruling D5).
//!
//! * A capability IRI in the `eg:` namespace must be a native term under
//!   `eg:capability`: a typo there would otherwise look covered while covering
//!   nothing (`UNKNOWN_CAPABILITY_IRI`).
//! * An absolute IRI in any other namespace is accepted as a declared claim,
//!   never read by coverage, and reported (`UNRESOLVED_CAPABILITY_IRI`).
//! * A modality must be a native `eg:modality` term.
//! * Cost and latency are bounded integers, the currency is an ISO 4217 code,
//!   and model facts are coherent.
//!
//! Every value is a claim; this rule only refuses claims that cannot mean
//! anything.

use eg_types::agent_ontology::{is_a, is_native, CAPABILITY_ROOT, MODALITY_ROOT};
use eg_types::connector_pack::{
    PackAnnotations, PackEntryKind, PackViolationCode, PackWarningCode,
};

/// Largest declared price, in micros.
const MAX_DECLARED_MICROS: u64 = 1_000_000_000_000_000;
/// Largest declared latency: one day.
const MAX_DECLARED_LATENCY_MS: u64 = 86_400_000;

/// What one entry's annotations violate and warn about.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct AnnotationFindings {
    pub(super) violations: Vec<(PackViolationCode, &'static str)>,
    pub(super) warnings: Vec<(PackWarningCode, String)>,
}

/// Check every declaration of one entry.
pub(super) fn check_annotations(
    kind: PackEntryKind,
    annotations: &PackAnnotations,
) -> AnnotationFindings {
    let mut findings = AnnotationFindings::default();
    if annotations.tool_mode.is_some() && kind != PackEntryKind::Tool {
        findings.violations.push((
            PackViolationCode::InvalidAnnotation,
            "a tool mode is declared on an entry that is not a tool",
        ));
    }
    for iri in annotations
        .provides
        .iter()
        .chain(annotations.requires_capabilities.iter())
    {
        check_capability(iri, &mut findings);
    }
    for iri in annotations
        .modalities_in
        .iter()
        .chain(annotations.modalities_out.iter())
    {
        if !native_under(iri, MODALITY_ROOT) {
            findings.violations.push((
                PackViolationCode::UnknownCapabilityIri,
                "a modality must be a native eg:modality term",
            ));
        }
    }
    check_cost_and_latency(annotations, &mut findings);
    if annotations.schema_tokens == Some(0) {
        findings.violations.push((
            PackViolationCode::InvalidAnnotation,
            "schema_tokens must be at least 1; omit it when unknown",
        ));
    }
    if let Some(model) = &annotations.model {
        if model.context_window_tokens == 0 {
            findings.violations.push((
                PackViolationCode::InvalidFacts,
                "contextWindowTokens must be at least 1",
            ));
        }
        if model.max_output_tokens > model.context_window_tokens {
            findings.violations.push((
                PackViolationCode::InvalidFacts,
                "max_output_tokens exceeds context_window_tokens",
            ));
        }
    }
    findings
}

fn check_capability(iri: &str, findings: &mut AnnotationFindings) {
    if !absolute_iri(iri) {
        findings.violations.push((
            PackViolationCode::InvalidAnnotation,
            "capability is not an absolute IRI",
        ));
    } else if iri.starts_with("eg:") {
        if !native_under(iri, CAPABILITY_ROOT) {
            findings.violations.push((
                PackViolationCode::UnknownCapabilityIri,
                "an eg: capability must be a native term under eg:capability",
            ));
        }
    } else if resolves_parent_import(iri) {
        // `ontology-import:<parent>`: the pack builder lifted the ontology's
        // `owl:imports` of the parent catalog into this requirement; the engine
        // resolves it against the parent catalog (EG-TYPED-PACKS-R105).
    } else {
        findings.warnings.push((
            PackWarningCode::UnresolvedCapabilityIri,
            format!("{iri} is a declared claim outside the native vocabulary"),
        ));
    }
}

/// An `ontology-import:` requirement naming the parent catalog or one of its
/// modules is resolved by the engine, never an unresolved claim.
fn resolves_parent_import(iri: &str) -> bool {
    iri.strip_prefix("ontology-import:")
        .is_some_and(parent_ontology_iri)
}

#[cfg(feature = "rdf")]
fn parent_ontology_iri(iri: &str) -> bool {
    eg_rdf::pack::is_parent_ontology_iri(iri)
}

#[cfg(not(feature = "rdf"))]
fn parent_ontology_iri(_iri: &str) -> bool {
    false
}

fn check_cost_and_latency(annotations: &PackAnnotations, findings: &mut AnnotationFindings) {
    if let Some(cost) = &annotations.cost {
        let prices = [
            cost.per_call_micros,
            cost.input_per_mtok_micros,
            cost.output_per_mtok_micros,
        ];
        if !iso_4217(&cost.currency)
            || prices
                .iter()
                .flatten()
                .any(|micros| *micros > MAX_DECLARED_MICROS)
        {
            findings.violations.push((
                PackViolationCode::InvalidAnnotation,
                "cost needs an ISO 4217 currency and prices within 10^15 micros",
            ));
        }
    }
    if let Some(latency) = &annotations.latency_declared {
        if latency.p50_ms > latency.p95_ms || u64::from(latency.p95_ms) > MAX_DECLARED_LATENCY_MS {
            findings.violations.push((
                PackViolationCode::InvalidAnnotation,
                "declared latency needs p50 <= p95 <= one day",
            ));
        }
    }
}

fn native_under(iri: &str, root: &str) -> bool {
    is_native(iri) && iri != root && is_a(iri, root)
}

fn absolute_iri(value: &str) -> bool {
    value.split_once(':').is_some_and(|(scheme, rest)| {
        scheme
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphabetic)
            && !rest.is_empty()
            && scheme.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || byte == b'+' || byte == b'-' || byte == b'.'
            })
    }) && !value
        .bytes()
        .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
}

/// Split already validated capability claims without promoting foreign terms.
pub(super) fn partition_capabilities(values: &[String]) -> (Vec<String>, Vec<String>) {
    values
        .iter()
        .cloned()
        .partition(|iri| iri.starts_with("eg:"))
}

fn iso_4217(code: &str) -> bool {
    code.len() == 3 && code.bytes().all(|byte| byte.is_ascii_uppercase())
}

#[cfg(test)]
mod tests {
    use super::*;
    use eg_types::agent_component::{DeclaredCost, DeclaredLatency};

    fn codes(annotations: &PackAnnotations) -> Vec<PackViolationCode> {
        check_annotations(PackEntryKind::Tool, annotations)
            .violations
            .into_iter()
            .map(|(code, _)| code)
            .collect()
    }

    fn providing(iri: &str) -> PackAnnotations {
        PackAnnotations {
            provides: eg_types::contract::BoundedVec::new(vec![iri.to_string()]).unwrap(),
            ..PackAnnotations::default()
        }
    }

    // spec: EG-TYPED-PACKS-R013
    #[test]
    fn the_split_rule_rejects_eg_typos_and_keeps_foreign_claims() {
        let native = eg_types::agent_ontology::descendants(CAPABILITY_ROOT)
            .into_iter()
            .find(|term| *term != CAPABILITY_ROOT)
            .unwrap();
        assert!(
            check_annotations(PackEntryKind::Tool, &providing(native))
                == AnnotationFindings::default()
        );
        assert_eq!(
            codes(&providing("eg:capability/retrieval/web-searc")),
            [PackViolationCode::UnknownCapabilityIri]
        );
        assert_eq!(
            codes(&providing("eg:modality/text")),
            [PackViolationCode::UnknownCapabilityIri]
        );
        assert_eq!(
            codes(&providing("not an iri")),
            [PackViolationCode::InvalidAnnotation]
        );
        let foreign = check_annotations(
            PackEntryKind::Tool,
            &providing("https://example.org/cap/playback"),
        );
        assert!(foreign.violations.is_empty());
        assert_eq!(
            foreign.warnings[0].0,
            PackWarningCode::UnresolvedCapabilityIri
        );
    }

    // spec: EG-TYPED-PACKS-R105.1
    #[cfg(feature = "rdf")]
    #[test]
    fn parent_ontology_import_requirement_resolves() {
        let requiring = |iri: &str| PackAnnotations {
            requires_capabilities: eg_types::contract::BoundedVec::new(vec![iri.to_string()])
                .unwrap(),
            ..PackAnnotations::default()
        };
        let parent = check_annotations(
            PackEntryKind::Ontology,
            &requiring("ontology-import:http://knuckles.team/kg"),
        );
        assert!(parent.violations.is_empty() && parent.warnings.is_empty());
        let remote = check_annotations(
            PackEntryKind::Ontology,
            &requiring("ontology-import:https://example.org/other"),
        );
        assert_eq!(
            remote.warnings[0].0,
            PackWarningCode::UnresolvedCapabilityIri
        );
    }

    // spec: EG-TYPED-PACKS-R046
    // spec: EG-TYPED-PACKS-R047
    #[test]
    fn cost_latency_and_model_facts_are_bounded() {
        let cost = |currency: &str, micros: u64| PackAnnotations {
            cost: Some(DeclaredCost {
                currency: currency.to_string(),
                per_call_micros: Some(micros),
                input_per_mtok_micros: None,
                output_per_mtok_micros: None,
            }),
            ..PackAnnotations::default()
        };
        assert!(codes(&cost("USD", 10)).is_empty());
        assert_eq!(
            codes(&cost("usd1", 10)),
            [PackViolationCode::InvalidAnnotation]
        );
        assert_eq!(
            codes(&cost("USD", MAX_DECLARED_MICROS + 1)),
            [PackViolationCode::InvalidAnnotation]
        );
        let late = PackAnnotations {
            latency_declared: Some(DeclaredLatency {
                p50_ms: 900,
                p95_ms: 100,
            }),
            ..PackAnnotations::default()
        };
        assert_eq!(codes(&late), [PackViolationCode::InvalidAnnotation]);
    }

    #[test]
    fn a_tool_mode_belongs_to_tools_only() {
        let declared = PackAnnotations {
            tool_mode: Some(eg_types::connector_pack::PackToolMode::Condensed),
            ..PackAnnotations::default()
        };
        assert!(check_annotations(PackEntryKind::Tool, &declared)
            .violations
            .is_empty());
        assert_eq!(
            check_annotations(PackEntryKind::Prompt, &declared).violations[0].0,
            PackViolationCode::InvalidAnnotation
        );
    }

    // spec: EG-TYPED-PACKS-R045.4
    #[test]
    fn unknown_side_effect_is_refused() {
        for (field, value) in [("side_effects", "filesystem"), ("declared_by", "vendor")] {
            let error =
                serde_json::from_value::<PackAnnotations>(serde_json::json!({ field: value }))
                    .expect_err("an unknown closed value never reaches admission");
            assert!(error
                .to_string()
                .starts_with(&format!("{field}: unknown value")));
        }
        let zero = PackAnnotations {
            schema_tokens: Some(0),
            ..PackAnnotations::default()
        };
        let findings = check_annotations(PackEntryKind::Tool, &zero);
        assert_eq!(findings.violations.len(), 1);
        assert_eq!(
            findings.violations[0].0,
            PackViolationCode::InvalidAnnotation
        );
        assert!(findings.violations[0].1.contains("schema_tokens"));
        let sized = PackAnnotations {
            schema_tokens: Some(1),
            ..PackAnnotations::default()
        };
        assert!(codes(&sized).is_empty());
    }

    #[test]
    fn context_window_zero_is_refused_by_the_pack_validator() {
        let model = |window: u64, output: u64| PackAnnotations {
            model: Some(eg_types::connector_pack::PackModelFacts {
                provider: "vendor".into(),
                model_identity: "vendor/m".into(),
                context_window_tokens: window,
                max_output_tokens: output,
                supports_tools: Some(true),
                supports_structured_output: Some(true),
                supports_vision: Some(false),
                input_capabilities: None,
                shell_proficiency: None,
                stopping_behaviour: None,
            }),
            ..PackAnnotations::default()
        };
        let findings = check_annotations(PackEntryKind::ModelProfile, &model(0, 0));
        assert_eq!(findings.violations.len(), 1);
        assert_eq!(findings.violations[0].0, PackViolationCode::InvalidFacts);
        assert!(findings.violations[0].1.contains("contextWindowTokens"));
        assert!(check_annotations(PackEntryKind::ModelProfile, &model(1, 1))
            .violations
            .is_empty());
    }
}

#[cfg(test)]
#[path = "capability_tests.rs"]
mod capability_tests;

#[cfg(test)]
#[path = "modality_tests.rs"]
mod modality_tests;
