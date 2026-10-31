// CONCEPT:MEDULLA-KG.domains.finance-compute.factor-research — factor ranking
// (MEDULLA-FINANCE-PRIMITIVES-R018).
//
// Ranks candidate factors by their predictive skill. Information coefficient
// and information ratio come from the one shared kernel in
// `medulla_numeric::evaluation::skill`; nothing here re-implements them. This is the
// pure ranking core: graph-node recording and the served wire route are later
// R018 children.

use std::cmp::Ordering;
use std::fmt;

use medulla_numeric::evaluation::skill::{information_coefficient, information_ratio};
use serde::{Deserialize, Serialize};

/// The stable code of an abstention on too little data.
pub const FACTOR_INSUFFICIENT_DATA: &str = "FACTOR_INSUFFICIENT_DATA";

/// A refused factor ranking. No record is produced by a refusal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FactorError {
    /// Fewer than two observations, or no factors, to rank.
    InsufficientData,
    /// A factor id or provenance key was empty, or provenance was empty.
    InvalidRecord { field: &'static str },
    /// A factor's signal length differs from the forward-returns length.
    LengthMismatch { factor_id: String },
}

impl FactorError {
    /// The stable wire code of this refusal.
    pub fn code(&self) -> &'static str {
        match self {
            FactorError::InsufficientData => FACTOR_INSUFFICIENT_DATA,
            FactorError::InvalidRecord { .. } => "FACTOR_RECORD_INVALID",
            FactorError::LengthMismatch { .. } => "FACTOR_LENGTH_MISMATCH",
        }
    }
}

impl fmt::Display for FactorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.code())
    }
}

impl std::error::Error for FactorError {}

/// One ranked factor with the provenance of its inputs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FactorRecord {
    pub factor_id: String,
    pub information_coefficient: f64,
    pub information_ratio: f64,
    /// 1-based rank; 1 is the strongest factor.
    pub rank: u32,
    pub input_provenance_keys: Vec<String>,
}

impl FactorRecord {
    /// Build a record, refusing an empty id or an empty or blank provenance list.
    pub fn new(
        factor_id: impl Into<String>,
        information_coefficient: f64,
        information_ratio: f64,
        rank: u32,
        input_provenance_keys: Vec<String>,
    ) -> Result<Self, FactorError> {
        let factor_id = factor_id.into();
        if factor_id.trim().is_empty() {
            return Err(FactorError::InvalidRecord { field: "factor_id" });
        }
        if input_provenance_keys.is_empty()
            || input_provenance_keys
                .iter()
                .any(|key| key.trim().is_empty())
        {
            return Err(FactorError::InvalidRecord {
                field: "input_provenance_keys",
            });
        }
        Ok(Self {
            factor_id,
            information_coefficient,
            information_ratio,
            rank,
            input_provenance_keys,
        })
    }
}

/// One candidate factor: its signal over time and where it came from.
#[derive(Debug, Clone, PartialEq)]
pub struct FactorInput {
    pub factor_id: String,
    pub signal: Vec<f64>,
    pub input_provenance_keys: Vec<String>,
}

/// Rank `factors` by absolute information ratio, strongest first.
///
/// Each factor's information coefficient is taken against `forward_returns`;
/// its information ratio applies the fundamental law with its finite
/// observation count as breadth. Ties break by factor id so the order is
/// deterministic. Fewer than two observations, or no factors, abstains with
/// `FACTOR_INSUFFICIENT_DATA` and no record.
pub fn rank_factors(
    factors: &[FactorInput],
    forward_returns: &[f64],
) -> Result<Vec<FactorRecord>, FactorError> {
    if factors.is_empty() || forward_returns.len() < 2 {
        return Err(FactorError::InsufficientData);
    }
    let mut scored = Vec::with_capacity(factors.len());
    for factor in factors {
        if factor.signal.len() != forward_returns.len() {
            return Err(FactorError::LengthMismatch {
                factor_id: factor.factor_id.clone(),
            });
        }
        let breadth = factor
            .signal
            .iter()
            .zip(forward_returns)
            .filter(|(s, r)| s.is_finite() && r.is_finite())
            .count();
        if breadth < 2 {
            return Err(FactorError::InsufficientData);
        }
        let ic = information_coefficient(&factor.signal, forward_returns);
        let ir = information_ratio(ic, breadth as f64);
        scored.push((factor, ic, ir));
    }
    scored.sort_by(|a, b| {
        b.2.abs()
            .partial_cmp(&a.2.abs())
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.0.factor_id.cmp(&b.0.factor_id))
    });
    scored
        .into_iter()
        .enumerate()
        .map(|(index, (factor, ic, ir))| {
            FactorRecord::new(
                factor.factor_id.clone(),
                ic,
                ir,
                index as u32 + 1,
                factor.input_provenance_keys.clone(),
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input(id: &str, signal: &[f64]) -> FactorInput {
        FactorInput {
            factor_id: id.to_string(),
            signal: signal.to_vec(),
            input_provenance_keys: vec![format!("bars/{id}")],
        }
    }

    const RETURNS: [f64; 6] = [0.01, -0.02, 0.03, -0.01, 0.02, -0.03];

    // spec: MEDULLA-FINANCE-PRIMITIVES-R018.1
    #[test]
    fn a_factor_record_round_trips_and_rejects_an_empty_provenance_list() {
        let record =
            FactorRecord::new("momentum", 0.4, 0.98, 1, vec!["bars/momentum".to_string()]).unwrap();
        let json = serde_json::to_string(&record).unwrap();
        assert_eq!(serde_json::from_str::<FactorRecord>(&json).unwrap(), record);
        let error = FactorRecord::new("momentum", 0.4, 0.98, 1, Vec::new()).unwrap_err();
        assert_eq!(
            error,
            FactorError::InvalidRecord {
                field: "input_provenance_keys"
            }
        );
        assert!(FactorRecord::new("", 0.4, 0.98, 1, vec!["k".to_string()]).is_err());
        assert!(FactorRecord::new("f", 0.4, 0.98, 1, vec![" ".to_string()]).is_err());
    }

    // spec: MEDULLA-FINANCE-PRIMITIVES-R018.2
    #[test]
    fn three_factors_rank_by_absolute_information_ratio() {
        // `anti` is the perfectly negated return series: |IC| = 1, the strongest.
        let anti: Vec<f64> = RETURNS.iter().map(|r| -r).collect();
        let strong = input("anti", &anti);
        // `noise` is nearly uncorrelated with the returns.
        let noise = input("noise", &[1.0, 1.0, -1.0, -1.0, 1.0, -1.0]);
        // `mild` agrees on sign with some noise.
        let mild = input("mild", &[0.5, -0.1, 0.4, -0.3, 0.1, -0.2]);
        let ranked = rank_factors(&[noise, mild, strong], &RETURNS).unwrap();
        let order: Vec<&str> = ranked.iter().map(|r| r.factor_id.as_str()).collect();
        assert_eq!(order, vec!["anti", "mild", "noise"]);
        assert_eq!(
            ranked.iter().map(|r| r.rank).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert!((ranked[0].information_coefficient + 1.0).abs() < 1e-12);
        // IR = IC * sqrt(breadth) with breadth 6.
        assert!((ranked[0].information_ratio + 6.0_f64.sqrt()).abs() < 1e-12);
        assert!(ranked[0].information_ratio.abs() > ranked[1].information_ratio.abs());
        assert!(ranked[1].information_ratio.abs() > ranked[2].information_ratio.abs());
        assert_eq!(ranked[0].input_provenance_keys, vec!["bars/anti"]);
        // Deterministic replay.
        let again = rank_factors(
            &[
                input("noise", &[1.0, 1.0, -1.0, -1.0, 1.0, -1.0]),
                input("mild", &[0.5, -0.1, 0.4, -0.3, 0.1, -0.2]),
                input("anti", &RETURNS.iter().map(|r| -r).collect::<Vec<_>>()),
            ],
            &RETURNS,
        )
        .unwrap();
        assert_eq!(ranked, again);
    }

    // spec: MEDULLA-FINANCE-PRIMITIVES-R018.4
    #[test]
    fn empty_or_single_row_input_abstains_with_its_code_and_no_record() {
        let empty = rank_factors(&[], &[]).unwrap_err();
        assert_eq!(empty.code(), "FACTOR_INSUFFICIENT_DATA");
        let no_rows = rank_factors(&[input("f", &[])], &[]).unwrap_err();
        assert_eq!(no_rows.code(), "FACTOR_INSUFFICIENT_DATA");
        let one_row = rank_factors(&[input("f", &[1.0])], &[0.1]).unwrap_err();
        assert_eq!(one_row.code(), "FACTOR_INSUFFICIENT_DATA");
        let no_factors = rank_factors(&[], &RETURNS).unwrap_err();
        assert_eq!(no_factors, FactorError::InsufficientData);
        // A factor with fewer than two finite pairs also abstains.
        let thin = rank_factors(
            &[input(
                "f",
                &[f64::NAN, f64::NAN, f64::NAN, f64::NAN, f64::NAN, 1.0],
            )],
            &RETURNS,
        )
        .unwrap_err();
        assert_eq!(thin.code(), "FACTOR_INSUFFICIENT_DATA");
        let mismatch = rank_factors(&[input("f", &[1.0, 2.0, 3.0])], &RETURNS).unwrap_err();
        assert_eq!(mismatch.code(), "FACTOR_LENGTH_MISMATCH");
    }
}
