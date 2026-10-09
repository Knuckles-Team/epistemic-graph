//! Finance wire DTOs.

#[cfg(feature = "finance")]
use serde::{Deserialize, Serialize};

// ── finance ────────────────────────────────────────────────────────────────

/// A single order in the book (matched by `eg-compute::finance::exchange`).
#[cfg(feature = "finance")]
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct Order {
    pub id: String,
    pub side: String, // "buy" or "sell"
    pub price: f64,
    pub quantity: f64,
    pub timestamp: u64,
}

/// One fiscal year of standardized financial-statement inputs (forensic scores).
#[cfg(feature = "finance")]
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct YearData {
    pub sales: f64,
    pub cogs: f64,
    pub sga: f64,
    pub net_income: f64,
    pub cfo: f64, // operating cash flow
    pub receivables: f64,
    pub current_assets: f64,
    pub current_liabilities: f64,
    pub ppe_net: f64,
    pub depreciation: f64,
    pub total_assets: f64,
    pub total_liabilities: f64,
    pub long_term_debt: f64,
    pub retained_earnings: f64,
    pub ebit: f64,
    pub market_cap: f64,
    pub shares: f64,
}

// ── finance-v1 record-type wire version gate (EG-FINANCE-PRIMITIVES-R004.3) ─

/// The finance-v1 record schema version this server accepts over the wire.
#[cfg(feature = "finance")]
pub const FINANCE_V1_SCHEMA_VERSION: &str = "finance-v1";

/// Why a finance-v1 record was refused at the wire boundary.
#[cfg(feature = "finance")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinanceRecordWireRefusal {
    pub reason: String,
}

/// Admit a finance-v1 record's declared schema version, or refuse it
/// (EG-FINANCE-PRIMITIVES-R004.3): server wiring accepts only the current
/// `finance-v1` schema version; an unknown or empty declared version is
/// refused rather than silently accepted or guessed at as a migration.
#[cfg(feature = "finance")]
pub fn admit_finance_record_version(
    declared_version: &str,
) -> Result<(), FinanceRecordWireRefusal> {
    if declared_version == FINANCE_V1_SCHEMA_VERSION {
        Ok(())
    } else {
        Err(FinanceRecordWireRefusal {
            reason: format!(
                "unknown finance record schema version '{declared_version}'; only '{FINANCE_V1_SCHEMA_VERSION}' is accepted"
            ),
        })
    }
}

#[cfg(all(test, feature = "finance"))]
mod finance_record_version_tests {
    use super::*;

    #[test]
    fn the_current_schema_version_is_admitted() {
        assert!(admit_finance_record_version(FINANCE_V1_SCHEMA_VERSION).is_ok());
    }

    #[test]
    fn an_unknown_schema_version_is_refused() {
        let refusal = admit_finance_record_version("finance-v0").unwrap_err();
        assert!(refusal.reason.contains("finance-v0"));
    }

    #[test]
    fn an_empty_schema_version_is_refused() {
        assert!(admit_finance_record_version("").is_err());
    }
}
