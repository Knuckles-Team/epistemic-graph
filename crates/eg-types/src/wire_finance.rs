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
