//! The typed supply-chain impact report (MEDULLA-CQ-XSRC-R006.1) and its pure
//! assembly from the planned query's rows and premises (MEDULLA-CQ-XSRC-R006.4).
//!
//! One report answers "how does the supply chain affect our services?": the
//! impact per service, the supply-chain items behind each (supplier, component
//! version, vulnerability, impact kind), the sources used and why (the
//! authority premise), and typed abstentions. A report item names the source
//! that served it — an item with no source is refused, not defaulted. A
//! report carrying an abstention is still returned, never an error.
//!
//! Assembly is a pure, deterministic function of its inputs: same rows,
//! premises and abstentions, same encoded bytes.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

/// The unified report: impact per service, the items behind each, the sources
/// used and why, and the typed abstentions.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SupplyChainImpactReport {
    /// One entry per impacted service, sorted by service IRI.
    pub service_impacts: Vec<ServiceImpact>,
    /// The sources that served the answer, each with the authority row that
    /// named it for the class it served, sorted by source.
    pub sources_used: Vec<SourceUsed>,
    /// The typed abstentions; a non-empty list never turns the report into an
    /// error.
    pub abstentions: Vec<ReportAbstention>,
}

/// The impact of the supply chain on one service.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ServiceImpact {
    /// The service IRI.
    pub service: String,
    /// The supply-chain items behind the service; every item carries the
    /// source that served it.
    pub items: Vec<ServiceImpactItem>,
}

/// How a service reaches a component version.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ImpactKind {
    /// Directly from a repository dependency list.
    DirectDependency,
    /// Through the runtime chain `deployedAs → usesImage → containsComponent`.
    RuntimeChain,
}

impl ImpactKind {
    fn sort_key(&self) -> u8 {
        match self {
            Self::DirectDependency => 0,
            Self::RuntimeChain => 1,
        }
    }
}

/// One supply-chain item behind a service. An item without a source is refused
/// by [`ServiceImpactItem::try_new`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ServiceImpactItem {
    /// The supplier IRI.
    pub supplier: String,
    /// The component version IRI.
    pub component_version: String,
    /// The vulnerability identifier (CVE id or finding id), when one affects
    /// the component version.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vulnerability: Option<String>,
    /// How the service reaches the component version.
    pub impact_kind: ImpactKind,
    /// The source that served this item's facts.
    pub source: String,
}

/// Why a report could not be built from the planned rows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReportError {
    /// A `ServiceImpact` item named no source.
    MissingItemSource,
}

impl std::fmt::Display for ReportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingItemSource => {
                write!(
                    f,
                    "a service-impact item named no source; items are refused, not defaulted"
                )
            }
        }
    }
}

impl std::error::Error for ReportError {}

impl ServiceImpactItem {
    /// Build one item, refusing a source-less item (MEDULLA-CQ-XSRC-R006.1).
    pub fn try_new(
        supplier: &str,
        component_version: &str,
        vulnerability: Option<&str>,
        impact_kind: ImpactKind,
        source: &str,
    ) -> Result<Self, ReportError> {
        if source.trim().is_empty() {
            return Err(ReportError::MissingItemSource);
        }
        Ok(Self {
            supplier: supplier.to_string(),
            component_version: component_version.to_string(),
            vulnerability: vulnerability.map(str::to_string),
            impact_kind,
            source: source.to_string(),
        })
    }

    fn sort_key(&self) -> (u8, String, String, String) {
        (
            self.impact_kind.sort_key(),
            self.supplier.clone(),
            self.component_version.clone(),
            self.vulnerability.clone().unwrap_or_default(),
        )
    }
}

/// One source that served part of the answer, with the authority premise that
/// justified it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SourceUsed {
    /// The source name (the connector the operator sees in the report).
    pub source: String,
    /// The ontology class the source served.
    pub class_served: String,
    /// The `vg:SourceAuthority` row that named this source for the class, with
    /// its rank.
    pub authority_premise: String,
}

/// A typed abstention: why a class or source is absent from the answer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ReportAbstention {
    /// The rank-1 source for `class` is not connected (MEDULLA-CQ-XSRC-R005.4).
    Unconnected { source: String, class: String },
    /// No approved virtual mapping serves `class`.
    Uncovered { class: String },
    /// `class` has no object-property path to a queried root.
    Disconnected { class: String },
    /// The mapping that would serve `class` is not approved.
    UnapprovedMapping { class: String, mapping: String },
}

impl ReportAbstention {
    fn sort_key(&self) -> (u8, String, String) {
        match self {
            Self::Unconnected { source, class } => (0, source.clone(), class.clone()),
            Self::Uncovered { class } => (1, String::new(), class.clone()),
            Self::Disconnected { class } => (2, String::new(), class.clone()),
            Self::UnapprovedMapping { class, mapping } => (3, mapping.clone(), class.clone()),
        }
    }
}

/// One recorded row of the planned query, with the source that served it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImpactRow {
    /// The service IRI the row concerns.
    pub service: String,
    /// The supplier IRI behind the row's component.
    pub supplier: String,
    /// The component version IRI.
    pub component_version: String,
    /// The vulnerability identifier, when one affects the component version.
    pub vulnerability: Option<String>,
    /// How the service reaches the component version.
    pub impact_kind: ImpactKind,
    /// The source that served this row's facts (blank rows are refused).
    pub source: String,
}

/// One authority premise read at plan time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourcePremise {
    /// The source the row named.
    pub source: String,
    /// The ontology class the row serves.
    pub class_served: String,
    /// The `vg:SourceAuthority` row identifier.
    pub row: String,
    /// The row's rank (lower wins).
    pub rank: u64,
}

/// Pure assembly (MEDULLA-CQ-XSRC-R006.4): the planned query's rows, the authority
/// premises and the abstentions become one report. Deterministic — impacts and
/// items are sorted, sources are deduplicated and sorted, abstentions are
/// sorted — so equal inputs yield byte-identical encodings.
pub fn assemble_supply_chain_report(
    rows: &[ImpactRow],
    premises: &[SourcePremise],
    abstentions: &[ReportAbstention],
) -> Result<SupplyChainImpactReport, ReportError> {
    let mut by_service: BTreeMap<String, Vec<ServiceImpactItem>> = BTreeMap::new();
    for row in rows {
        let item = ServiceImpactItem::try_new(
            &row.supplier,
            &row.component_version,
            row.vulnerability.as_deref(),
            row.impact_kind,
            &row.source,
        )?;
        by_service
            .entry(row.service.clone())
            .or_default()
            .push(item);
    }
    let service_impacts = by_service
        .into_iter()
        .map(|(service, mut items)| {
            items.sort_by_key(|item| item.sort_key());
            ServiceImpact { service, items }
        })
        .collect();

    let mut sources: BTreeSet<(String, String, String, u64)> = BTreeSet::new();
    for premise in premises {
        sources.insert((
            premise.source.clone(),
            premise.class_served.clone(),
            format!("{} (rank {})", premise.row, premise.rank),
            premise.rank,
        ));
    }
    let sources_used = sources
        .into_iter()
        .map(|(source, class_served, authority_premise, _)| SourceUsed {
            source,
            class_served,
            authority_premise,
        })
        .collect();

    let mut abstentions = abstentions.to_vec();
    abstentions.sort_by_key(|abstention| abstention.sort_key());

    Ok(SupplyChainImpactReport {
        service_impacts,
        sources_used,
        abstentions,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(service: &str, supplier: &str, version: &str, source: &str) -> ImpactRow {
        ImpactRow {
            service: service.to_string(),
            supplier: supplier.to_string(),
            component_version: version.to_string(),
            vulnerability: None,
            impact_kind: ImpactKind::RuntimeChain,
            source: source.to_string(),
        }
    }

    // spec: MEDULLA-CQ-XSRC-R006.1
    #[test]
    fn a_valid_report_builds_and_round_trips_the_wire_encoding() {
        let report = assemble_supply_chain_report(
            &[
                row(
                    "http://knuckles.team/xsrc#service-a",
                    "http://knuckles.team/xsrc#supplier-acme",
                    "http://knuckles.team/xsrc#version-1.18.0",
                    "dockerhub-mcp",
                ),
                row(
                    "http://knuckles.team/xsrc#service-a",
                    "http://knuckles.team/xsrc#supplier-acme",
                    "http://knuckles.team/xsrc#version-1.1.1k",
                    "erpnext-mcp",
                ),
            ],
            &[SourcePremise {
                source: "dockerhub-mcp".to_string(),
                class_served: "http://knuckles.team/kg/infrastructure#ContainerImage".to_string(),
                row: "xsaa:container-image-dockerhub".to_string(),
                rank: 1,
            }],
            &[ReportAbstention::Uncovered {
                class: "http://knuckles.team/kg#SupplyChainItem".to_string(),
            }],
        )
        .unwrap();
        assert_eq!(report.service_impacts.len(), 1);
        assert_eq!(report.service_impacts[0].items.len(), 2);
        assert_eq!(report.sources_used.len(), 1);
        assert_eq!(report.abstentions.len(), 1);
        let encoded = serde_json::to_string(&report).unwrap();
        let decoded: SupplyChainImpactReport = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, report);
    }

    // spec: MEDULLA-CQ-XSRC-R006.1
    #[test]
    fn an_item_without_a_source_is_refused() {
        let rows = vec![row(
            "http://knuckles.team/xsrc#service-a",
            "http://knuckles.team/xsrc#supplier-acme",
            "http://knuckles.team/xsrc#version-1.18.0",
            "",
        )];
        assert_eq!(
            assemble_supply_chain_report(&rows, &[], &[]),
            Err(ReportError::MissingItemSource)
        );
        assert_eq!(
            ServiceImpactItem::try_new(
                "supplier",
                "version",
                None,
                ImpactKind::DirectDependency,
                "   ",
            ),
            Err(ReportError::MissingItemSource)
        );
    }

    // spec: MEDULLA-CQ-XSRC-R006.4
    #[test]
    fn assembly_is_byte_for_byte_deterministic_over_recorded_rows() {
        let rows = vec![
            row(
                "http://knuckles.team/xsrc#service-b",
                "http://knuckles.team/xsrc#supplier-globex",
                "http://knuckles.team/xsrc#version-2.4.1",
                "dockerhub-mcp",
            ),
            row(
                "http://knuckles.team/xsrc#service-a",
                "http://knuckles.team/xsrc#supplier-acme",
                "http://knuckles.team/xsrc#version-1.1.1k",
                "erpnext-mcp",
            ),
            row(
                "http://knuckles.team/xsrc#service-a",
                "http://knuckles.team/xsrc#supplier-acme",
                "http://knuckles.team/xsrc#version-1.18.0",
                "gitlab-mcp",
            ),
        ];
        let premises = vec![
            SourcePremise {
                source: "erpnext-mcp".to_string(),
                class_served: "http://knuckles.team/kg#Supplier".to_string(),
                row: "xsaa:supplier-erpnext".to_string(),
                rank: 1,
            },
            SourcePremise {
                source: "dockerhub-mcp".to_string(),
                class_served: "http://knuckles.team/kg/infrastructure#ContainerImage".to_string(),
                row: "xsaa:container-image-dockerhub".to_string(),
                rank: 1,
            },
            // A duplicate premise must not duplicate the source.
            SourcePremise {
                source: "dockerhub-mcp".to_string(),
                class_served: "http://knuckles.team/kg/infrastructure#ContainerImage".to_string(),
                row: "xsaa:container-image-dockerhub".to_string(),
                rank: 1,
            },
        ];
        let abstentions = vec![
            ReportAbstention::Unconnected {
                source: "erpnext-mcp".to_string(),
                class: "http://knuckles.team/kg#SupplyChainItem".to_string(),
            },
            ReportAbstention::Disconnected {
                class: "http://knuckles.team/kg#Legacy".to_string(),
            },
        ];
        // The expected encoding, byte for byte: impacts sorted by service,
        // items sorted within, sources deduplicated and sorted, abstentions
        // sorted by kind.
        let expected = r#"{"service_impacts":[{"service":"http://knuckles.team/xsrc#service-a","items":[{"supplier":"http://knuckles.team/xsrc#supplier-acme","component_version":"http://knuckles.team/xsrc#version-1.1.1k","impact_kind":"runtime_chain","source":"erpnext-mcp"},{"supplier":"http://knuckles.team/xsrc#supplier-acme","component_version":"http://knuckles.team/xsrc#version-1.18.0","impact_kind":"runtime_chain","source":"gitlab-mcp"}]},{"service":"http://knuckles.team/xsrc#service-b","items":[{"supplier":"http://knuckles.team/xsrc#supplier-globex","component_version":"http://knuckles.team/xsrc#version-2.4.1","impact_kind":"runtime_chain","source":"dockerhub-mcp"}]}],"sources_used":[{"source":"dockerhub-mcp","class_served":"http://knuckles.team/kg/infrastructure#ContainerImage","authority_premise":"xsaa:container-image-dockerhub (rank 1)"},{"source":"erpnext-mcp","class_served":"http://knuckles.team/kg#Supplier","authority_premise":"xsaa:supplier-erpnext (rank 1)"}],"abstentions":[{"kind":"unconnected","source":"erpnext-mcp","class":"http://knuckles.team/kg#SupplyChainItem"},{"kind":"disconnected","class":"http://knuckles.team/kg#Legacy"}]}"#;
        let report = assemble_supply_chain_report(&rows, &premises, &abstentions).unwrap();
        assert_eq!(serde_json::to_string(&report).unwrap(), expected);
        // The assembly is a pure function: the same inputs yield the same bytes.
        let again = assemble_supply_chain_report(&rows, &premises, &abstentions).unwrap();
        assert_eq!(
            serde_json::to_string(&again).unwrap(),
            expected,
            "assembly must be byte-for-byte deterministic"
        );
    }
}
