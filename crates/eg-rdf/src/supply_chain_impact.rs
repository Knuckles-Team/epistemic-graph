//! Medulla's side of the served supply-chain impact method (EG-CQ-XSRC-R006.5).
//!
//! Per the registry rule, Arbora exposes the intent tool over its registry;
//! this module is the method body Medulla serves: it plans the question's
//! ontology classes through [`source_selection::select_sources`] (sources
//! come from the ontology's `vg:SourceAuthority` rows, never from the query
//! text) and assembles the typed [`SupplyChainImpactReport`] from the planned
//! rows, authority premises and abstentions. The served surface itself is
//! cross-repo (owner: Arbora) and is not claimed here; this slice fixes the
//! method body and proves its output matches the typed model.

use eg_types::supply_chain_report::{
    assemble_supply_chain_report, ImpactRow, ReportAbstention, ReportError, SourcePremise,
    SupplyChainImpactReport,
};

use crate::source_selection::{select_sources, CoverageFixture, CoverageResult, Premise};

/// Plan `roots` (the question's ontology classes) against the authority
/// fixture and assemble the supply-chain impact report from the recorded
/// rows. A class whose rank-1 source is not connected becomes a typed
/// abstention, never a silent fallback (EG-CQ-XSRC-R005.4).
pub fn supply_chain_impact(
    fixture: &CoverageFixture,
    roots: &[String],
    hop_budget: usize,
    rows: &[ImpactRow],
) -> Result<SupplyChainImpactReport, ReportError> {
    let results = select_sources(fixture, roots, hop_budget);
    let mut premises: Vec<SourcePremise> = Vec::new();
    let mut abstentions: Vec<ReportAbstention> = Vec::new();
    for result in &results {
        match result {
            CoverageResult::Covered {
                premises: covered_premises,
                ..
            } => {
                for premise in covered_premises {
                    if let Premise::Authority {
                        row,
                        source,
                        class,
                        rank,
                        ..
                    } = premise
                    {
                        premises.push(SourcePremise {
                            source: source.clone(),
                            class_served: class.clone(),
                            row: row.clone(),
                            rank: *rank,
                        });
                    }
                }
            }
            CoverageResult::Uncovered { class, .. } => {
                abstentions.push(ReportAbstention::Uncovered {
                    class: class.clone(),
                })
            }
            CoverageResult::Disconnected { class } => {
                abstentions.push(ReportAbstention::Disconnected {
                    class: class.clone(),
                });
            }
            CoverageResult::Unconnected { class, source, .. } => {
                abstentions.push(ReportAbstention::Unconnected {
                    class: class.clone(),
                    source: source.clone(),
                });
            }
        }
    }
    assemble_supply_chain_report(rows, &premises, &abstentions)
}

#[cfg(test)]
mod tests {
    use super::*;

    // spec: EG-CQ-XSRC-R006.5
    #[test]
    fn the_method_body_returns_a_report_matching_the_typed_model() {
        let fixture = CoverageFixture::new()
            .with_relation("Service", "usesComponent", "ComponentVersion")
            .with_relation(
                "ComponentVersion",
                "supplyChainVersionOf",
                "SoftwareComponent",
            )
            .with_relation("SoftwareComponent", "publishedBy", "Supplier")
            .with_relation("ComponentVersion", "affectsComponent", "Vulnerability")
            .with_authority("Supplier", "erpnext-suppliers", "erpnext-mcp", 5, 1)
            .with_authority(
                "Vulnerability",
                "gitlab-vulnerabilities",
                "gitlab-mcp",
                3,
                1,
            )
            .with_authority("ContainerImage", "dockerhub-images", "dockerhub-mcp", 4, 1)
            .with_unconnected_source("dockerhub-mcp");
        let roots = vec![
            "Service".to_string(),
            "SoftwareComponent".to_string(),
            "Supplier".to_string(),
            "Vulnerability".to_string(),
            "ContainerImage".to_string(),
        ];
        let rows = vec![ImpactRow {
            service: "http://knuckles.team/xsrc#checkout".to_string(),
            supplier: "http://knuckles.team/xsrc#supplier-acme".to_string(),
            component_version: "http://knuckles.team/xsrc#version-1.18.0".to_string(),
            vulnerability: Some("CVE-2021-23017".to_string()),
            impact_kind: eg_types::supply_chain_report::ImpactKind::RuntimeChain,
            source: "gitlab-mcp".to_string(),
        }];
        let report = supply_chain_impact(&fixture, &roots, 2, &rows).expect("the report assembles");
        let encoded = serde_json::to_string(&report).expect("the report encodes");
        let decoded: SupplyChainImpactReport =
            serde_json::from_str(&encoded).expect("the encoding is the typed model");
        assert_eq!(decoded, report);
        assert_eq!(report.service_impacts.len(), 1);
        assert!(!report.service_impacts[0].items.is_empty());
        assert!(report
            .sources_used
            .iter()
            .any(|source| source.source == "erpnext-mcp"));
        // The unconnected dockerhub source is a typed abstention, not a fallback.
        assert!(report.abstentions.iter().any(|abstention| matches!(
            abstention,
            ReportAbstention::Unconnected { source, class }
                if source == "dockerhub-mcp" && class == "ContainerImage"
        )));
    }
}
