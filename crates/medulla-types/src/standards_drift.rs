//! Standards, capability cohorts, drift scores and consolidation proposals
//! (MEDULLA-UNIFIED-DATA-PLANE-R701). Drift is the weighted fraction of a
//! standard's rules an asset satisfies. An asset with no applicable
//! standard is `not_applicable`, never zero. A consolidation recommendation
//! is only a proposal and never spans tenants.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

/// One weighted rule of a standard: the asset satisfies it when it holds
/// `required_fact`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StandardRule {
    pub id: String,
    pub required_fact: String,
    pub weight: f64,
}

/// A standard that applies to assets of one capability class.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Standard {
    pub id: String,
    pub capability_class: String,
    pub rules: Vec<StandardRule>,
}

/// One asset as seen by drift scoring.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StandardsAsset {
    pub id: String,
    pub tenant: String,
    pub capability_classes: BTreeSet<String>,
    pub facts: BTreeSet<String>,
    pub cost: u64,
}

/// The assets that share one capability class.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityCohort {
    pub capability_class: String,
    pub assets: Vec<String>,
}

/// A drift score, or `not_applicable` when no standard covers the asset.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "state")]
pub enum DriftValue {
    Scored {
        score: f64,
        satisfied: Vec<String>,
        failed: Vec<String>,
    },
    NotApplicable,
}

/// One asset's drift against one standard.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DriftScore {
    pub asset: String,
    pub standard: Option<String>,
    pub score: DriftValue,
}

/// A proposal to consolidate assets of one cohort.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ConsolidationRecommendation {
    pub capability_class: String,
    pub tenant: String,
    pub assets: Vec<String>,
    pub overlap: f64,
    pub cost_saving: u64,
    pub score: f64,
    pub proposal_only: bool,
}

/// A refused recommendation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StandardsRefusal {
    /// `CONSOLIDATION_CROSS_TENANT`
    CrossTenant { tenants: Vec<String> },
    /// `CONSOLIDATION_TOO_FEW_ASSETS`
    TooFewAssets,
}

impl StandardsRefusal {
    pub fn code(&self) -> &'static str {
        match self {
            Self::CrossTenant { .. } => "CONSOLIDATION_CROSS_TENANT",
            Self::TooFewAssets => "CONSOLIDATION_TOO_FEW_ASSETS",
        }
    }
}

/// Form cohorts in one pass over the assets' capability classes.
pub fn form_cohorts(assets: &[StandardsAsset]) -> Vec<CapabilityCohort> {
    let mut by_class: BTreeMap<&str, Vec<String>> = BTreeMap::new();
    for asset in assets {
        for class in &asset.capability_classes {
            by_class.entry(class).or_default().push(asset.id.clone());
        }
    }
    by_class
        .into_iter()
        .map(|(class, assets)| CapabilityCohort {
            capability_class: class.to_string(),
            assets,
        })
        .collect()
}

/// Score `asset` against the first standard that applies to one of its
/// capability classes, or report `not_applicable`.
pub fn drift_score(asset: &StandardsAsset, standards: &[Standard]) -> DriftScore {
    let Some(standard) = standards
        .iter()
        .find(|s| asset.capability_classes.contains(&s.capability_class))
    else {
        return DriftScore {
            asset: asset.id.clone(),
            standard: None,
            score: DriftValue::NotApplicable,
        };
    };
    let (mut satisfied, mut failed) = (Vec::new(), Vec::new());
    let (mut got, mut total) = (0.0, 0.0);
    for rule in &standard.rules {
        let weight = rule.weight.max(0.0);
        total += weight;
        if asset.facts.contains(&rule.required_fact) {
            got += weight;
            satisfied.push(rule.id.clone());
        } else {
            failed.push(rule.id.clone());
        }
    }
    let score = if total > 0.0 {
        DriftValue::Scored {
            score: got / total,
            satisfied,
            failed,
        }
    } else {
        DriftValue::NotApplicable
    };
    DriftScore {
        asset: asset.id.clone(),
        standard: Some(standard.id.clone()),
        score,
    }
}

/// Recommend consolidating `assets` of one capability class. The score is
/// the Jaccard overlap of their facts weighted by the cost saved by keeping
/// only the cheapest asset. Refuses assets of more than one tenant.
pub fn recommend_consolidation(
    capability_class: &str,
    assets: &[&StandardsAsset],
) -> Result<ConsolidationRecommendation, StandardsRefusal> {
    if assets.len() < 2 {
        return Err(StandardsRefusal::TooFewAssets);
    }
    let tenants: BTreeSet<&str> = assets.iter().map(|a| a.tenant.as_str()).collect();
    if tenants.len() > 1 {
        return Err(StandardsRefusal::CrossTenant {
            tenants: tenants.into_iter().map(str::to_string).collect(),
        });
    }
    let mut common = assets[0].facts.clone();
    let mut union = BTreeSet::new();
    for asset in assets {
        common.retain(|fact| asset.facts.contains(fact));
        union.extend(asset.facts.iter().cloned());
    }
    let overlap = if union.is_empty() {
        0.0
    } else {
        common.len() as f64 / union.len() as f64
    };
    let total: u64 = assets.iter().map(|a| a.cost).sum();
    let cheapest = assets.iter().map(|a| a.cost).min().unwrap_or(0);
    let cost_saving = total - cheapest;
    Ok(ConsolidationRecommendation {
        capability_class: capability_class.to_string(),
        tenant: assets[0].tenant.clone(),
        assets: assets.iter().map(|a| a.id.clone()).collect(),
        overlap,
        cost_saving,
        score: overlap * (cost_saving as f64).ln_1p(),
        proposal_only: true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn asset(id: &str, tenant: &str, class: &str, facts: &[&str], cost: u64) -> StandardsAsset {
        StandardsAsset {
            id: id.to_string(),
            tenant: tenant.to_string(),
            capability_classes: BTreeSet::from([class.to_string()]),
            facts: facts.iter().map(|f| f.to_string()).collect(),
            cost,
        }
    }

    fn standard() -> Standard {
        Standard {
            id: "db-baseline".to_string(),
            capability_class: "database".to_string(),
            rules: vec![
                StandardRule {
                    id: "tls".to_string(),
                    required_fact: "tls".to_string(),
                    weight: 3.0,
                },
                StandardRule {
                    id: "backup".to_string(),
                    required_fact: "backup".to_string(),
                    weight: 1.0,
                },
            ],
        }
    }

    // spec: MEDULLA-UNIFIED-DATA-PLANE-R701.1
    #[test]
    fn standards_model_round_trips() {
        let original = standard();
        let json = serde_json::to_string(&original).unwrap();
        assert_eq!(serde_json::from_str::<Standard>(&json).unwrap(), original);
        let score = drift_score(&asset("pg1", "t", "database", &["tls"], 1), &[standard()]);
        let json = serde_json::to_string(&score).unwrap();
        assert_eq!(serde_json::from_str::<DriftScore>(&json).unwrap(), score);
        let na = drift_score(&asset("q", "t", "queue", &[], 1), &[standard()]);
        let json = serde_json::to_string(&na).unwrap();
        assert!(json.contains(r#""state":"not_applicable""#), "{json}");
        assert!(serde_json::from_str::<DriftValue>(r#"{"state":"zero"}"#).is_err());
    }

    // spec: MEDULLA-UNIFIED-DATA-PLANE-R701.2
    #[test]
    fn cohorts_drift_and_recommendation_scale() {
        for n in [10usize, 1_000, 100_000] {
            let assets: Vec<StandardsAsset> = (0..n)
                .map(|i| {
                    let class = if i % 2 == 0 { "database" } else { "queue" };
                    asset(&format!("a{i}"), "t", class, &["tls"], 1)
                })
                .collect();
            let cohorts = form_cohorts(&assets);
            assert_eq!(cohorts.len(), 2);
            assert_eq!(cohorts.iter().map(|c| c.assets.len()).sum::<usize>(), n);
        }
        let score = drift_score(&asset("pg1", "t", "database", &["tls"], 1), &[standard()]);
        let DriftValue::Scored { score, .. } = score.score else {
            panic!("database asset is scored");
        };
        assert!((score - 0.75).abs() < 1e-12);
        let a = asset("a", "t", "database", &["tls", "backup"], 10);
        let b = asset("b", "t", "database", &["tls", "backup"], 4);
        let c = asset("c", "t", "database", &["tls"], 4);
        let same = recommend_consolidation("database", &[&a, &b]).unwrap();
        let partial = recommend_consolidation("database", &[&a, &c]).unwrap();
        assert!(same.overlap > partial.overlap);
        assert!(same.score > partial.score);
        assert!(same.proposal_only);
    }

    // spec: MEDULLA-UNIFIED-DATA-PLANE-R701.4
    #[test]
    fn cross_tenant_refused_and_missing_standard_is_not_zero() {
        let a = asset("a", "t1", "database", &["tls"], 1);
        let b = asset("b", "t2", "database", &["tls"], 1);
        let error = recommend_consolidation("database", &[&a, &b]).unwrap_err();
        assert_eq!(error.code(), "CONSOLIDATION_CROSS_TENANT");
        let none = drift_score(&asset("q", "t", "queue", &[], 1), &[standard()]);
        assert_eq!(none.score, DriftValue::NotApplicable);
        assert_eq!(none.standard, None);
    }

    // spec: MEDULLA-UNIFIED-DATA-PLANE-R701.5
    #[test]
    fn score_lists_satisfied_and_failed_rules() {
        let stored = serde_json::to_string(&drift_score(
            &asset("pg1", "t", "database", &["backup"], 1),
            &[standard()],
        ))
        .unwrap();
        let read: DriftScore = serde_json::from_str(&stored).unwrap();
        let DriftValue::Scored {
            score,
            satisfied,
            failed,
        } = read.score
        else {
            panic!("scored");
        };
        assert_eq!(satisfied, vec!["backup".to_string()]);
        assert_eq!(failed, vec!["tls".to_string()]);
        assert!((score - 0.25).abs() < 1e-12);
    }
}
