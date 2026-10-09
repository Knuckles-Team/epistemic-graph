//! Caller-declared options: the candidate source of a decision point whose
//! options are the caller's own (a retrieval plan, an ingestion lane, a model
//! route) rather than library components or graph rows.
//!
//! Every fact here is a CLAIM made by the caller, recorded as such: a record
//! over declared options is never stronger than a claim, and it is visible to
//! the declaring principal only, because nothing but that principal vouches
//! for it. Declaring an option grants it nothing -- the decision still only
//! chooses among the options it was handed, or abstains.

use serde::{Deserialize, Serialize};

use crate::contract::BoundedVec;

/// Most options one declared source may carry (the assembly candidate cap).
pub const MAX_DECLARED_OPTIONS: usize = 64;

/// One named numeric fact of a declared option, already on the `Q32` scale.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DeclaredNumber {
    pub key: String,
    pub q32: i64,
}

/// One named text field of a declared option.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DeclaredText {
    pub key: String,
    pub text: String,
}

/// One option a caller declares.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DeclaredOption {
    pub option_id: String,
    /// Native capability IRIs the option claims; read by `CoverageFraction`.
    #[serde(default)]
    pub classification: BoundedVec<String, 16>,
    /// Sorted by key and unique.
    #[serde(default)]
    pub numbers: BoundedVec<DeclaredNumber, 32>,
    /// Sorted by key and unique.
    #[serde(default)]
    pub texts: BoundedVec<DeclaredText, 8>,
}

fn strictly_sorted<'a>(keys: impl Iterator<Item = &'a str>) -> bool {
    let keys: Vec<&str> = keys.collect();
    keys.iter().all(|key| !key.is_empty()) && keys.windows(2).all(|w| w[0] < w[1])
}

/// The validating check of a declared option set: at least one option,
/// option ids strictly sorted (so the matrix order is the declared order),
/// and every option's fact keys strictly sorted.
pub fn check_declared(options: &[DeclaredOption]) -> Result<(), String> {
    if options.is_empty() {
        return Err("a declared candidate source names at least one option".to_string());
    }
    if !strictly_sorted(options.iter().map(|o| o.option_id.as_str())) {
        return Err("declared option ids must be non-empty, sorted and unique".to_string());
    }
    let facts_sorted = options.iter().all(|option| {
        strictly_sorted(option.numbers.iter().map(|n| n.key.as_str()))
            && strictly_sorted(option.texts.iter().map(|t| t.key.as_str()))
    });
    if !facts_sorted {
        return Err("declared fact keys must be non-empty, sorted and unique".to_string());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn option(id: &str, keys: &[&str]) -> DeclaredOption {
        DeclaredOption {
            option_id: id.to_string(),
            classification: BoundedVec::default(),
            numbers: BoundedVec::new(
                keys.iter()
                    .map(|key| DeclaredNumber {
                        key: key.to_string(),
                        q32: 1 << 32,
                    })
                    .collect(),
            )
            .unwrap(),
            texts: BoundedVec::default(),
        }
    }

    // spec: EG-DECISION-ENGINE-R037
    #[test]
    fn a_declared_set_is_sorted_unique_and_non_empty() {
        assert!(check_declared(&[option("a", &["x", "y"]), option("b", &[])]).is_ok());
        assert!(check_declared(&[]).is_err());
        assert!(check_declared(&[option("b", &[]), option("a", &[])]).is_err());
        assert!(check_declared(&[option("a", &[]), option("a", &[])]).is_err());
        assert!(check_declared(&[option("a", &["y", "x"])]).is_err());
        assert!(check_declared(&[option("", &[])]).is_err());
    }
}
