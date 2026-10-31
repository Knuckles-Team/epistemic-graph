//! Authority audit trail (MEDULLA-SRC-AUTH-R007.1).
//!
//! Every cross-source answer carries "sources used and why" (rollup R007).
//! The audit is the wire type the selection builds from its verdicts and
//! premises only (R007.2), lists the rows it set aside with a closed reason
//! (R007.3), and which the cross-source report publishes as `sources_used`
//! (R007.4). Building, wiring and printing it are the later children; this
//! module fixes the typed model and its wire invariants.

use serde::{Deserialize, Serialize};

/// The trust role of a data source (MEDULLA-SRC-AUTH-R010): one per source,
/// closed set. A conflict on one concept prefers the higher role before rank.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum SourceRole {
    /// Current-state authority: preferred on conflicts.
    LiveTruth,
    /// Background context: answers what live truth does not.
    HistoricalContext,
    /// Inspiration only: never serves a question that needs current state.
    Inspiration,
}

/// Why a row was considered but not used (MEDULLA-SRC-AUTH-R003.5, R006.1, R010.4).
/// `Proposed` covers proposed rows, which are listed, never selected.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum NotUsedReason {
    /// The source is absent from the owner-scoped foreign catalog.
    Unconnected,
    /// The mapping or row is not operator-approved.
    UnapprovedMapping,
    /// The copy is older than its freshness bound and no live route exists.
    Stale,
    /// The row's contract kind disagrees with the connection's source kind.
    ContractMismatch,
    /// An unresolvable tie; both rows are listed, none chosen by name.
    Ambiguous,
    /// The row was proposed, not approved: listed, never selected.
    Proposed,
    /// The source's role cannot answer a question that needs current state.
    RoleInsufficient,
}

/// The materialisation mode a used row resolved to (MEDULLA-SRC-AUTH-R008):
/// `live` for a `virtual` row, `accelerated` for a copy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum AuditSourceMode {
    /// Served live against the source (the `virtual` default).
    Live,
    /// Served from the accelerated copy; carries its stored reason.
    Accelerated,
}

/// One row of the audit: a source considered for one concept, whether it was
/// used, and why (its justifying premises) or why not (a closed reason).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AuditEntry {
    /// The source that owns the row.
    pub source: String,
    /// The ontology class the row serves.
    pub concept: String,
    /// The facet (property) within the class, when the row is facet-scoped.
    pub facet: Option<String>,
    /// The source's trust role, when the selection graded roles (R010).
    pub role: Option<SourceRole>,
    /// The justifying premises, verbatim in verdict order; empty for a row
    /// that was set aside before its premises could count.
    pub premises: Vec<String>,
    /// The reason a `not_used` row was set aside; `None` for a used row.
    pub reason: Option<NotUsedReason>,
    /// The mode a used row resolved to; `None` for a not-used row.
    pub mode: Option<AuditSourceMode>,
    /// The stored reason an `accelerated` row carried (R008.2); required
    /// exactly when `mode` is `Accelerated`.
    pub materialisation_reason: Option<String>,
}

/// The audit block every cross-source answer carries: the sources used and
/// why, plus every row set aside and why.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AuthorityAudit {
    /// The sources used in the answer, in selection order.
    pub used: Vec<AuditEntry>,
    /// The rows considered but not used, each with its closed reason.
    pub not_used: Vec<AuditEntry>,
}

/// The audit violates its wire invariants, refused before publication.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InvalidAudit {
    BlankSource,
    BlankConcept,
    BlankFacet,
    UsedWithoutPremises,
    UsedWithReason,
    UsedWithoutMode,
    AcceleratedWithoutReason,
    LiveWithMaterialisationReason,
    NotUsedWithoutReason,
}

impl std::fmt::Display for InvalidAudit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::BlankSource => "audit entry source must not be blank",
            Self::BlankConcept => "audit entry concept must not be blank",
            Self::BlankFacet => "audit entry facet must not be blank",
            Self::UsedWithoutPremises => "a used source must carry its premises",
            Self::UsedWithReason => "a used source carries no not-used reason",
            Self::UsedWithoutMode => "a used source must name its mode",
            Self::AcceleratedWithoutReason => "an accelerated mode must carry its reason",
            Self::LiveWithMaterialisationReason => "a live mode carries no materialisation reason",
            Self::NotUsedWithoutReason => "a not-used row must carry its reason",
        };
        write!(f, "{message}")
    }
}

impl std::error::Error for InvalidAudit {}

impl AuditEntry {
    /// The identity of the row as the audit names it.
    pub fn names(&self) -> (&str, &str) {
        (&self.source, &self.concept)
    }

    /// Check this entry's invariants in the `used` or `not_used` position.
    pub fn check(&self, used: bool) -> Result<(), InvalidAudit> {
        if self.source.trim().is_empty() {
            return Err(InvalidAudit::BlankSource);
        }
        if self.concept.trim().is_empty() {
            return Err(InvalidAudit::BlankConcept);
        }
        if self
            .facet
            .as_deref()
            .is_some_and(|facet| facet.trim().is_empty())
        {
            return Err(InvalidAudit::BlankFacet);
        }
        if used {
            if self.premises.is_empty() {
                return Err(InvalidAudit::UsedWithoutPremises);
            }
            if self.reason.is_some() {
                return Err(InvalidAudit::UsedWithReason);
            }
            let mode = self.mode.ok_or(InvalidAudit::UsedWithoutMode)?;
            match mode {
                AuditSourceMode::Live => {
                    if self.materialisation_reason.is_some() {
                        return Err(InvalidAudit::LiveWithMaterialisationReason);
                    }
                }
                AuditSourceMode::Accelerated => {
                    if self.materialisation_reason.is_none() {
                        return Err(InvalidAudit::AcceleratedWithoutReason);
                    }
                }
            }
        } else if self.reason.is_none() {
            return Err(InvalidAudit::NotUsedWithoutReason);
        }
        Ok(())
    }
}

impl AuthorityAudit {
    /// Check the whole block: every used entry in its used position, every
    /// set-aside row in its not-used position.
    pub fn validate(&self) -> Result<(), InvalidAudit> {
        for entry in &self.used {
            entry.check(true)?;
        }
        for entry in &self.not_used {
            entry.check(false)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn used_entry() -> AuditEntry {
        AuditEntry {
            source: "svc:servicenow".to_string(),
            concept: "Incident".to_string(),
            facet: Some("severity".to_string()),
            role: Some(SourceRole::LiveTruth),
            premises: vec![
                "mapping approved for Incident from svc:servicenow".to_string(),
                "rank 1 beats rank 2 by authority rank".to_string(),
            ],
            reason: None,
            mode: Some(AuditSourceMode::Live),
            materialisation_reason: None,
        }
    }

    fn not_used_entry(reason: NotUsedReason) -> AuditEntry {
        AuditEntry {
            source: "svc:pagerduty".to_string(),
            concept: "Incident".to_string(),
            facet: None,
            role: None,
            premises: Vec::new(),
            reason: Some(reason),
            mode: None,
            materialisation_reason: None,
        }
    }

    fn full_audit() -> AuthorityAudit {
        let mut accelerated = used_entry();
        accelerated.source = "svc:cmdb".to_string();
        accelerated.mode = Some(AuditSourceMode::Accelerated);
        accelerated.materialisation_reason = Some("hot subset declared".to_string());
        AuthorityAudit {
            used: vec![used_entry(), accelerated],
            not_used: vec![
                not_used_entry(NotUsedReason::Proposed),
                not_used_entry(NotUsedReason::Stale),
            ],
        }
    }

    // spec: MEDULLA-SRC-AUTH-R007.1
    #[test]
    fn authority_audit_round_trips_over_the_wire() {
        let audit = full_audit();
        assert_eq!(audit.validate(), Ok(()));
        let encoded = serde_json::to_string(&audit).unwrap();
        let decoded: AuthorityAudit = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, audit);
        let value = serde_json::to_value(&audit).unwrap();
        // The wire names are the closed, published ones.
        assert_eq!(value["used"][0]["role"], "live_truth");
        assert_eq!(value["used"][1]["mode"], "accelerated");
        assert_eq!(value["not_used"][0]["reason"], "proposed");
        assert_eq!(value["not_used"][1]["reason"], "stale");
        assert_eq!(
            serde_json::from_value::<AuthorityAudit>(value).unwrap(),
            audit
        );
    }

    // spec: MEDULLA-SRC-AUTH-R007.1
    #[test]
    fn the_wire_refuses_unknown_fields_and_open_values() {
        for (index, value) in [
            // Unknown fields are refused at both levels.
            json!({"used": [], "not_used": [], "extra": true}),
            json!({"used": [{"source": "s", "concept": "c", "facet": null, "role": null,
                "premises": ["p"], "reason": null, "mode": "live",
                "materialisation_reason": null, "extra": 1}],
                "not_used": []}),
            // Open values are refused in every closed enum.
            json!({"used": [{"source": "s", "concept": "c", "facet": null,
                "role": "sometimes", "premises": ["p"], "reason": null,
                "mode": "live", "materialisation_reason": null}],
                "not_used": []}),
            json!({"used": [],
                "not_used": [{"source": "s", "concept": "c", "facet": null,
                "role": null, "premises": [], "reason": "expired",
                "mode": null, "materialisation_reason": null}]}),
            json!({"used": [{"source": "s", "concept": "c", "facet": null,
                "role": null, "premises": ["p"], "reason": null,
                "mode": "copied", "materialisation_reason": null}],
                "not_used": []}),
        ]
        .iter()
        .enumerate()
        {
            assert!(
                serde_json::from_value::<AuthorityAudit>(value.clone()).is_err(),
                "the wire accepted the value it must refuse at index {index}"
            );
        }
    }

    // spec: MEDULLA-SRC-AUTH-R007.1
    #[test]
    fn used_entries_must_carry_their_why_and_their_mode() {
        let mut audit = full_audit();
        audit.used[0].premises.clear();
        assert_eq!(audit.validate(), Err(InvalidAudit::UsedWithoutPremises));
        audit = full_audit();
        audit.used[0].reason = Some(NotUsedReason::Ambiguous);
        assert_eq!(audit.validate(), Err(InvalidAudit::UsedWithReason));
        audit = full_audit();
        audit.used[0].mode = None;
        assert_eq!(audit.validate(), Err(InvalidAudit::UsedWithoutMode));
        audit = full_audit();
        audit.used[1].materialisation_reason = None;
        assert_eq!(
            audit.validate(),
            Err(InvalidAudit::AcceleratedWithoutReason)
        );
        audit = full_audit();
        audit.used[0].materialisation_reason = Some("no copy".to_string());
        assert_eq!(
            audit.validate(),
            Err(InvalidAudit::LiveWithMaterialisationReason)
        );
    }

    // spec: MEDULLA-SRC-AUTH-R007.1
    #[test]
    fn set_aside_rows_carry_exactly_their_reason_and_no_blank_names() {
        let mut audit = full_audit();
        audit.not_used[0].reason = None;
        assert_eq!(audit.validate(), Err(InvalidAudit::NotUsedWithoutReason));
        audit = full_audit();
        audit.used[0].source = "  ".to_string();
        assert_eq!(audit.validate(), Err(InvalidAudit::BlankSource));
        audit = full_audit();
        audit.used[0].concept.clear();
        assert_eq!(audit.validate(), Err(InvalidAudit::BlankConcept));
        audit = full_audit();
        audit.used[0].facet = Some(" ".to_string());
        assert_eq!(audit.validate(), Err(InvalidAudit::BlankFacet));
    }

    // spec: MEDULLA-SRC-AUTH-R007.1
    #[cfg(feature = "contract-schema")]
    #[test]
    fn authority_audit_contract_schema_snapshot() {
        let schema = serde_json::to_value(schemars::schema_for!(AuthorityAudit)).unwrap();
        let defs = &schema["$defs"];
        let entry = &defs["AuditEntry"];
        // The non-Option fields are required, in declaration order; the Option
        // fields are nullable (absent or null on the wire).
        let required: Vec<&str> = entry["required"]
            .as_array()
            .unwrap()
            .iter()
            .map(|field| field.as_str().unwrap())
            .collect();
        assert_eq!(required, ["source", "concept", "premises"]);
        for field in [
            "source",
            "concept",
            "facet",
            "role",
            "premises",
            "reason",
            "mode",
            "materialisation_reason",
        ] {
            assert!(
                entry["properties"].get(field).is_some(),
                "{field} must exist"
            );
        }
        for nullable in ["facet", "role", "reason", "mode", "materialisation_reason"] {
            let property = &entry["properties"][nullable];
            let accepts_null = property["type"]
                .as_array()
                .is_some_and(|types| types.iter().any(|t| t == "null"))
                || property["anyOf"]
                    .as_array()
                    .is_some_and(|arms| arms.iter().any(|arm| arm["type"] == "null"));
            assert!(accepts_null, "{nullable} must accept null");
        }
        assert_eq!(entry["additionalProperties"], json!(false));
        // The closed enums publish their exact values (schemars renders a
        // unit enum as a oneOf over consts, in declaration order).
        fn published_values(def: &serde_json::Value) -> Vec<&str> {
            def["oneOf"]
                .as_array()
                .unwrap()
                .iter()
                .map(|arm| arm["const"].as_str().unwrap())
                .collect()
        }
        assert_eq!(
            published_values(&defs["SourceRole"]),
            ["live_truth", "historical_context", "inspiration"]
        );
        assert_eq!(
            published_values(&defs["AuditSourceMode"]),
            ["live", "accelerated"]
        );
        assert_eq!(
            published_values(&defs["NotUsedReason"]),
            [
                "unconnected",
                "unapproved_mapping",
                "stale",
                "contract_mismatch",
                "ambiguous",
                "proposed",
                "role_insufficient"
            ]
        );
    }
}
