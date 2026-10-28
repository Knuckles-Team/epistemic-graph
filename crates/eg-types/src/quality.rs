//! Data quality rules and scores (EG-UNIFIED-DATA-PLANE-R762.1). A rule
//! targets one dataset or column in one closed quality dimension; a score
//! records one evaluation of it, native or from an outside checker.

use serde::{Deserialize, Serialize};

/// The closed set of quality dimensions shared by native and outside scores.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QualityDimension {
    Completeness,
    Uniqueness,
    Validity,
    Consistency,
    Freshness,
    Accuracy,
}

/// How severe a failing rule is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QualitySeverity {
    Info,
    Warning,
    Critical,
}

/// One quality rule. `threshold` is the minimum passing value in `[0, 1]`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QualityRule {
    pub id: String,
    pub target: String,
    pub dimension: QualityDimension,
    pub expression: String,
    pub threshold: f64,
    pub severity: QualitySeverity,
}

/// One evaluation of a rule.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QualityScore {
    pub rule: String,
    pub value: f64,
    pub passed: bool,
    pub at: i64,
}

/// A rule failed validation, with its stable refusal code.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InvalidQualityRule {
    /// `QUALITY_RULE_INVALID`: no id, target or expression.
    RuleInvalid(String),
    /// `QUALITY_THRESHOLD_INVALID`: threshold outside `[0, 1]`.
    ThresholdInvalid,
}

impl InvalidQualityRule {
    pub fn code(&self) -> &'static str {
        match self {
            Self::RuleInvalid(_) => "QUALITY_RULE_INVALID",
            Self::ThresholdInvalid => "QUALITY_THRESHOLD_INVALID",
        }
    }
}

impl std::fmt::Display for InvalidQualityRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RuleInvalid(reason) => write!(f, "{}: {reason}", self.code()),
            Self::ThresholdInvalid => write!(f, "{}: threshold outside 0 to 1", self.code()),
        }
    }
}

impl std::error::Error for InvalidQualityRule {}

impl QualityRule {
    /// Refuse a rule with a blank id, target or expression, or a threshold
    /// outside `[0, 1]`.
    pub fn validate(&self) -> Result<(), InvalidQualityRule> {
        for (name, value) in [
            ("id", &self.id),
            ("target", &self.target),
            ("expression", &self.expression),
        ] {
            if value.trim().is_empty() {
                return Err(InvalidQualityRule::RuleInvalid(format!("blank {name}")));
            }
        }
        if !(0.0..=1.0).contains(&self.threshold) {
            return Err(InvalidQualityRule::ThresholdInvalid);
        }
        Ok(())
    }

    /// Score one measured value against this rule.
    pub fn score(&self, value: f64, at: i64) -> QualityScore {
        QualityScore {
            rule: self.id.clone(),
            value,
            passed: value >= self.threshold,
            at,
        }
    }
}

/// Where a proposed rule stands in the change-request flow.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "state")]
pub enum RuleState {
    Proposed,
    Approved { approver: String },
}

/// A rule proposed from a column profile (EG-UNIFIED-DATA-PLANE-R762.7).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProposedQualityRule {
    pub rule: QualityRule,
    pub state: RuleState,
    /// Why the profile suggested this rule.
    pub basis: String,
}

impl ProposedQualityRule {
    /// The rule to evaluate, only once approved. A proposed rule never
    /// evaluates as active.
    pub fn active(&self) -> Option<&QualityRule> {
        match self.state {
            RuleState::Approved { .. } => Some(&self.rule),
            RuleState::Proposed => None,
        }
    }

    /// Record the approval; a blank approver changes nothing.
    pub fn approve(&mut self, approver: &str) -> bool {
        if approver.trim().is_empty() {
            return false;
        }
        self.state = RuleState::Approved {
            approver: approver.to_string(),
        };
        true
    }
}

fn digit_pattern(value: &str) -> Option<usize> {
    (!value.is_empty() && value.chars().all(|c| c.is_ascii_digit())).then_some(value.len())
}

/// Propose candidate rules from one profile: a low null ratio yields a
/// completeness rule, a key-like distinct count a uniqueness rule, and a
/// fixed-width digit pattern a validity rule. Every candidate is `Proposed`.
pub fn propose_rules(profile: &crate::profile::ColumnProfile) -> Vec<ProposedQualityRule> {
    let target = format!("{}.{}", profile.dataset, profile.column);
    let candidate = |suffix: &str, dimension, expression: String, threshold, basis: String| {
        ProposedQualityRule {
            rule: QualityRule {
                id: format!("{target}.{suffix}"),
                target: target.clone(),
                dimension,
                expression,
                threshold,
                severity: QualitySeverity::Warning,
            },
            state: RuleState::Proposed,
            basis,
        }
    };
    let mut out = Vec::new();
    let null_ratio = profile.null_ratio();
    if profile.rows > 0 && null_ratio < 0.05 {
        out.push(candidate(
            "completeness",
            QualityDimension::Completeness,
            format!("count({}) / count(*)", profile.column),
            ((1.0 - null_ratio) * 100.0).floor() / 100.0,
            format!("null ratio {null_ratio:.4}"),
        ));
    }
    let non_null = profile.rows - profile.nulls;
    if non_null > 0 && profile.distinct_estimate >= non_null {
        out.push(candidate(
            "uniqueness",
            QualityDimension::Uniqueness,
            format!("count(distinct {0}) / count({0})", profile.column),
            1.0,
            "distinct count equals non-null rows".to_string(),
        ));
    }
    let widths = (
        profile.min.as_deref().and_then(digit_pattern),
        profile.max.as_deref().and_then(digit_pattern),
    );
    if let (Some(a), Some(b)) = widths {
        if a == b {
            out.push(candidate(
                "validity",
                QualityDimension::Validity,
                format!("{} ~ '^[0-9]{{{a}}}$'", profile.column),
                1.0,
                format!("min and max are {a}-digit values"),
            ));
        }
    }
    out
}
