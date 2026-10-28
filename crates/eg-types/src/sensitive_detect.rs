//! Typed records for sensitive-data detectors and their scan results.
//!
//! Scanning and claim promotion live above this pure-data crate. The detector
//! method is closed so an unknown algorithm cannot silently enter the contract.

use serde::{Deserialize, Serialize};

/// Supported ways to identify sensitive values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum SensitiveDetectionMethod {
    Pattern,
    Checksum,
    Dictionary,
    Model,
}

/// A versionable detector configuration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SensitiveDetector {
    pub id: String,
    pub category: String,
    pub method: SensitiveDetectionMethod,
    pub threshold: f64,
}

/// Aggregate evidence produced by applying a detector to one asset.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct Detection {
    pub asset: String,
    pub detector: String,
    pub sample_hits: u64,
    pub ratio: f64,
}
