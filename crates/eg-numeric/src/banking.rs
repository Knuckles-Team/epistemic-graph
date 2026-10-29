//! Banking risk calculations: AML screening, capital ratios, credit loss and
//! settlement legs (MEDULLA-FINANCE-PRIMITIVES-R901).
//!
//! Money is an integer count of minor units; threshold and capital-minimum
//! comparisons use exact integer arithmetic so a boundary value is never
//! decided by float rounding. Every kernel is pure and linear in its input.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Minimum CET1 to risk-weighted assets, in percent times ten (4.5 percent).
const CET1_MIN_PER_MILLE: i128 = 45;
/// Minimum total capital to risk-weighted assets (8 percent).
const TOTAL_CAPITAL_MIN_PER_MILLE: i128 = 80;
/// Minimum Tier 1 to total exposure (3 percent).
const LEVERAGE_MIN_PER_MILLE: i128 = 30;

/// A refused banking calculation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BankingError {
    /// Risk-weighted assets were zero or negative.
    CapitalRwaInvalid,
    /// Total exposure was zero or negative.
    CapitalExposureInvalid,
    /// A PD, LGD or EAD was non-finite or out of range.
    CreditInputInvalid { field: &'static str },
    /// An AML threshold was zero or negative, or an amount negative.
    AmlInputInvalid { field: &'static str },
    /// A severity name was not one of the closed set.
    UnknownSeverity(String),
    /// A settlement party identifier was empty.
    SettlementPartyInvalid,
}

impl BankingError {
    /// The stable wire code of this refusal.
    pub fn code(&self) -> &'static str {
        match self {
            BankingError::CapitalRwaInvalid => "CAPITAL_RWA_INVALID",
            BankingError::CapitalExposureInvalid => "CAPITAL_EXPOSURE_INVALID",
            BankingError::CreditInputInvalid { .. } => "CREDIT_INPUT_INVALID",
            BankingError::AmlInputInvalid { .. } => "AML_INPUT_INVALID",
            BankingError::UnknownSeverity(_) => "AML_SEVERITY_UNKNOWN",
            BankingError::SettlementPartyInvalid => "SETTLEMENT_PARTY_INVALID",
        }
    }
}

impl fmt::Display for BankingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.code())?;
        match self {
            BankingError::CreditInputInvalid { field }
            | BankingError::AmlInputInvalid { field } => {
                write!(f, ": {field}")
            }
            BankingError::UnknownSeverity(name) => write!(f, ": {name}"),
            _ => Ok(()),
        }
    }
}

impl std::error::Error for BankingError {}

/// The closed AML alert severity set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Severity {
    Info,
    Warning,
    Critical,
    ReportRequired,
}

impl Severity {
    /// Parse a severity name; any name outside the closed set refuses.
    pub fn parse(name: &str) -> Result<Self, BankingError> {
        match name {
            "info" => Ok(Severity::Info),
            "warning" => Ok(Severity::Warning),
            "critical" => Ok(Severity::Critical),
            "report-required" => Ok(Severity::ReportRequired),
            other => Err(BankingError::UnknownSeverity(other.to_string())),
        }
    }
}

/// One AML threshold rule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AmlRule {
    pub rule_id: String,
    pub version: u32,
    /// Reporting threshold in minor units.
    pub threshold_minor: i64,
}

/// What an AML screen detected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AmlAlertKind {
    /// An amount in `[0.8 t, t)`: just under the reporting threshold.
    Structuring,
    /// An amount at or above the threshold.
    Threshold,
}

/// One raised AML alert.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AmlAlert {
    pub rule_id: String,
    pub rule_version: u32,
    pub kind: AmlAlertKind,
    pub severity: Severity,
    pub amount_minor: i64,
}

/// Screen one transaction against `rule`: structuring for an amount in
/// `[0.8 t, t)` (warning), a threshold alert at or above `t`
/// (report-required), otherwise no alert.
pub fn screen_transaction(
    rule: &AmlRule,
    amount_minor: i64,
) -> Result<Option<AmlAlert>, BankingError> {
    if rule.threshold_minor <= 0 {
        return Err(BankingError::AmlInputInvalid { field: "threshold" });
    }
    if amount_minor < 0 {
        return Err(BankingError::AmlInputInvalid { field: "amount" });
    }
    let kind = if amount_minor >= rule.threshold_minor {
        AmlAlertKind::Threshold
    } else if i128::from(amount_minor) * 5 >= i128::from(rule.threshold_minor) * 4 {
        AmlAlertKind::Structuring
    } else {
        return Ok(None);
    };
    let severity = match kind {
        AmlAlertKind::Structuring => Severity::Warning,
        AmlAlertKind::Threshold => Severity::ReportRequired,
    };
    Ok(Some(AmlAlert {
        rule_id: rule.rule_id.clone(),
        rule_version: rule.version,
        kind,
        severity,
        amount_minor,
    }))
}

/// Regulatory capital and exposure, in minor units.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapitalInputs {
    pub cet1: i64,
    pub at1: i64,
    pub tier2: i64,
    pub risk_weighted_assets: i64,
    pub total_exposure: i64,
}

/// Capital ratios and their minimum checks.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct CapitalRatios {
    pub cet1_ratio: f64,
    pub tier1_ratio: f64,
    pub total_capital_ratio: f64,
    /// Tier 1 divided by total exposure.
    pub leverage_ratio: f64,
    pub cet1_meets_minimum: bool,
    pub total_capital_meets_minimum: bool,
    pub leverage_meets_minimum: bool,
}

/// Divide CET1, Tier 1 and total capital by risk-weighted assets and compare
/// with the 4.5 percent CET1, 8 percent total capital and 3 percent Tier 1 to
/// total exposure minimums. Each minimum is decided in exact integers.
pub fn capital_ratios(inputs: &CapitalInputs) -> Result<CapitalRatios, BankingError> {
    if inputs.risk_weighted_assets <= 0 {
        return Err(BankingError::CapitalRwaInvalid);
    }
    if inputs.total_exposure <= 0 {
        return Err(BankingError::CapitalExposureInvalid);
    }
    let rwa = i128::from(inputs.risk_weighted_assets);
    let exposure = i128::from(inputs.total_exposure);
    let cet1 = i128::from(inputs.cet1);
    let tier1 = cet1 + i128::from(inputs.at1);
    let total = tier1 + i128::from(inputs.tier2);
    Ok(CapitalRatios {
        cet1_ratio: cet1 as f64 / rwa as f64,
        tier1_ratio: tier1 as f64 / rwa as f64,
        total_capital_ratio: total as f64 / rwa as f64,
        leverage_ratio: tier1 as f64 / exposure as f64,
        cet1_meets_minimum: cet1 * 1000 >= rwa * CET1_MIN_PER_MILLE,
        total_capital_meets_minimum: total * 1000 >= rwa * TOTAL_CAPITAL_MIN_PER_MILLE,
        leverage_meets_minimum: tier1 * 1000 >= exposure * LEVERAGE_MIN_PER_MILLE,
    })
}

/// One credit exposure: probability of default, loss given default and
/// exposure at default (minor units).
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct CreditExposure {
    pub pd: f64,
    pub lgd: f64,
    pub ead_minor: i64,
}

/// Expected loss: PD times LGD times EAD. A PD or LGD outside `[0, 1]`, or a
/// negative EAD, refuses.
pub fn expected_loss(exposure: &CreditExposure) -> Result<f64, BankingError> {
    if !exposure.pd.is_finite() || !(0.0..=1.0).contains(&exposure.pd) {
        return Err(BankingError::CreditInputInvalid { field: "pd" });
    }
    if !exposure.lgd.is_finite() || !(0.0..=1.0).contains(&exposure.lgd) {
        return Err(BankingError::CreditInputInvalid { field: "lgd" });
    }
    if exposure.ead_minor < 0 {
        return Err(BankingError::CreditInputInvalid { field: "ead" });
    }
    Ok(exposure.pd * exposure.lgd * exposure.ead_minor as f64)
}

/// One ordered leg of a settlement chain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SettlementLeg {
    pub sequence: u32,
    pub from: String,
    pub to: String,
    pub amount_minor: i64,
}

/// Build the ordered legs `origin -> correspondent_1 -> ... -> beneficiary`
/// carrying `amount_minor` on each.
pub fn settlement_chain(
    origin: &str,
    correspondents: &[&str],
    beneficiary: &str,
    amount_minor: i64,
) -> Result<Vec<SettlementLeg>, BankingError> {
    let parties: Vec<&str> = std::iter::once(origin)
        .chain(correspondents.iter().copied())
        .chain(std::iter::once(beneficiary))
        .collect();
    if parties.iter().any(|party| party.trim().is_empty()) {
        return Err(BankingError::SettlementPartyInvalid);
    }
    if amount_minor <= 0 {
        return Err(BankingError::AmlInputInvalid { field: "amount" });
    }
    Ok(parties
        .windows(2)
        .enumerate()
        .map(|(index, pair)| SettlementLeg {
            sequence: index as u32 + 1,
            from: pair[0].to_string(),
            to: pair[1].to_string(),
            amount_minor,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule() -> AmlRule {
        AmlRule {
            rule_id: "ctr-10k".to_string(),
            version: 3,
            threshold_minor: 1_000_000,
        }
    }

    // spec: MEDULLA-FINANCE-PRIMITIVES-R901.1
    #[test]
    fn banking_types_round_trip_and_an_unknown_severity_is_rejected() {
        let alert = screen_transaction(&rule(), 1_000_000).unwrap().unwrap();
        let json = serde_json::to_string(&alert).unwrap();
        assert_eq!(serde_json::from_str::<AmlAlert>(&json).unwrap(), alert);
        let inputs = CapitalInputs {
            cet1: 50,
            at1: 10,
            tier2: 20,
            risk_weighted_assets: 1000,
            total_exposure: 1500,
        };
        let json = serde_json::to_string(&inputs).unwrap();
        assert_eq!(
            serde_json::from_str::<CapitalInputs>(&json).unwrap(),
            inputs
        );
        let ratios = capital_ratios(&inputs).unwrap();
        let json = serde_json::to_string(&ratios).unwrap();
        assert_eq!(
            serde_json::from_str::<CapitalRatios>(&json).unwrap(),
            ratios
        );
        let exposure = CreditExposure {
            pd: 0.02,
            lgd: 0.45,
            ead_minor: 1_000,
        };
        let json = serde_json::to_string(&exposure).unwrap();
        assert_eq!(
            serde_json::from_str::<CreditExposure>(&json).unwrap(),
            exposure
        );
        let legs = settlement_chain("A", &["B"], "C", 5).unwrap();
        let json = serde_json::to_string(&legs).unwrap();
        assert_eq!(
            serde_json::from_str::<Vec<SettlementLeg>>(&json).unwrap(),
            legs
        );
        let rule_json = serde_json::to_string(&rule()).unwrap();
        assert_eq!(serde_json::from_str::<AmlRule>(&rule_json).unwrap(), rule());
        for (name, severity) in [
            ("info", Severity::Info),
            ("warning", Severity::Warning),
            ("critical", Severity::Critical),
            ("report-required", Severity::ReportRequired),
        ] {
            assert_eq!(Severity::parse(name).unwrap(), severity);
            let json = serde_json::to_string(&severity).unwrap();
            assert_eq!(json, format!("\"{name}\""));
        }
        assert_eq!(
            Severity::parse("fatal").unwrap_err(),
            BankingError::UnknownSeverity("fatal".to_string())
        );
        assert!(serde_json::from_str::<Severity>("\"fatal\"").is_err());
    }

    // spec: MEDULLA-FINANCE-PRIMITIVES-R901.2
    #[test]
    fn screening_alerts_at_below_and_above_each_boundary() {
        let rule = rule();
        // 0.8 t = 800_000 exactly.
        assert_eq!(screen_transaction(&rule, 799_999).unwrap(), None);
        let at_lower = screen_transaction(&rule, 800_000).unwrap().unwrap();
        assert_eq!(at_lower.kind, AmlAlertKind::Structuring);
        assert_eq!(at_lower.severity, Severity::Warning);
        let below_t = screen_transaction(&rule, 999_999).unwrap().unwrap();
        assert_eq!(below_t.kind, AmlAlertKind::Structuring);
        let at_t = screen_transaction(&rule, 1_000_000).unwrap().unwrap();
        assert_eq!(at_t.kind, AmlAlertKind::Threshold);
        assert_eq!(at_t.severity, Severity::ReportRequired);
        let above = screen_transaction(&rule, 5_000_000).unwrap().unwrap();
        assert_eq!(above.kind, AmlAlertKind::Threshold);
        assert_eq!(above.rule_version, 3);
    }

    // spec: MEDULLA-FINANCE-PRIMITIVES-R901.2
    #[test]
    fn capital_minimums_are_decided_at_exact_boundaries() {
        // CET1 4.5 percent of RWA, total capital 8 percent, Tier 1 3 percent of exposure.
        let at_minimum = CapitalInputs {
            cet1: 45,
            at1: 0,
            tier2: 35,
            risk_weighted_assets: 1000,
            total_exposure: 1500,
        };
        let ratios = capital_ratios(&at_minimum).unwrap();
        assert!(ratios.cet1_meets_minimum);
        assert!(ratios.total_capital_meets_minimum);
        assert!(ratios.leverage_meets_minimum);
        assert_eq!(ratios.cet1_ratio, 0.045);
        assert_eq!(ratios.total_capital_ratio, 0.08);
        assert_eq!(ratios.leverage_ratio, 0.03);

        let below = CapitalInputs {
            cet1: 44,
            at1: 0,
            tier2: 35,
            risk_weighted_assets: 1000,
            total_exposure: 1500,
        };
        let ratios = capital_ratios(&below).unwrap();
        assert!(!ratios.cet1_meets_minimum);
        assert!(!ratios.total_capital_meets_minimum);
        assert!(!ratios.leverage_meets_minimum);

        let above = CapitalInputs {
            cet1: 60,
            at1: 20,
            tier2: 30,
            risk_weighted_assets: 1000,
            total_exposure: 1500,
        };
        let ratios = capital_ratios(&above).unwrap();
        assert!(ratios.cet1_meets_minimum);
        assert!(ratios.total_capital_meets_minimum);
        assert!(ratios.leverage_meets_minimum);
        assert_eq!(ratios.tier1_ratio, 0.08);
        assert_eq!(ratios.total_capital_ratio, 0.11);
    }

    // spec: MEDULLA-FINANCE-PRIMITIVES-R901.2
    #[test]
    fn expected_loss_is_pd_times_lgd_times_ead() {
        let loss = expected_loss(&CreditExposure {
            pd: 0.02,
            lgd: 0.45,
            ead_minor: 1_000_000,
        })
        .unwrap();
        assert!((loss - 9_000.0).abs() < 1e-9);
        for (pd, lgd) in [(0.0, 0.5), (1.0, 1.0), (0.5, 0.0)] {
            assert!(expected_loss(&CreditExposure {
                pd,
                lgd,
                ead_minor: 10
            })
            .is_ok());
        }
        assert_eq!(
            expected_loss(&CreditExposure {
                pd: 0.0,
                lgd: 1.0,
                ead_minor: 10
            })
            .unwrap(),
            0.0
        );
    }

    // spec: MEDULLA-FINANCE-PRIMITIVES-R901.2
    #[test]
    fn settlement_chain_orders_legs_through_the_correspondents() {
        let legs = settlement_chain("origin", &["corr-1", "corr-2"], "beneficiary", 250).unwrap();
        let hops: Vec<(u32, &str, &str)> = legs
            .iter()
            .map(|leg| (leg.sequence, leg.from.as_str(), leg.to.as_str()))
            .collect();
        assert_eq!(
            hops,
            vec![
                (1, "origin", "corr-1"),
                (2, "corr-1", "corr-2"),
                (3, "corr-2", "beneficiary"),
            ]
        );
        assert!(legs.iter().all(|leg| leg.amount_minor == 250));
        assert_eq!(settlement_chain("a", &[], "b", 1).unwrap().len(), 1);
    }

    // spec: MEDULLA-FINANCE-PRIMITIVES-R901.4
    #[test]
    fn zero_or_negative_rwa_is_refused_with_its_code() {
        for rwa in [0, -1] {
            let error = capital_ratios(&CapitalInputs {
                cet1: 10,
                at1: 0,
                tier2: 0,
                risk_weighted_assets: rwa,
                total_exposure: 100,
            })
            .unwrap_err();
            assert_eq!(error.code(), "CAPITAL_RWA_INVALID");
        }
    }

    // spec: MEDULLA-FINANCE-PRIMITIVES-R901.4
    #[test]
    fn pd_or_lgd_outside_the_unit_interval_is_refused_with_its_code() {
        for (pd, lgd) in [
            (-0.1, 0.5),
            (1.1, 0.5),
            (0.5, -0.1),
            (0.5, 1.1),
            (f64::NAN, 0.5),
            (0.5, f64::INFINITY),
        ] {
            let error = expected_loss(&CreditExposure {
                pd,
                lgd,
                ead_minor: 10,
            })
            .unwrap_err();
            assert_eq!(error.code(), "CREDIT_INPUT_INVALID");
        }
    }
}
