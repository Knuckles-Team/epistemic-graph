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
    if let Some(model) = &annotations.model {
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
    } else {
        findings.warnings.push((
            PackWarningCode::UnresolvedCapabilityIri,
            format!("{iri} is a declared claim outside the native vocabulary"),
        ));
    }
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
        !scheme.is_empty()
            && !rest.is_empty()
            && scheme.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || byte == b'+' || byte == b'-' || byte == b'.'
            })
    }) && !value
        .bytes()
        .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
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
}
