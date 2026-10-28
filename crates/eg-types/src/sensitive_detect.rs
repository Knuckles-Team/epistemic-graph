//! Sensitive-data detectors and their detections (EG-TYPED-PACKS-R803.1).
//!
//! A detection stores hit counts, never the matched values. Scanning, the
//! profiling-job wiring and claim promotion are later children of
//! `EG-TYPED-PACKS-R803`.

use serde::{Deserialize, Serialize};

/// Longest identifier, category or asset reference a record may carry.
pub const MAX_DETECTOR_TEXT_BYTES: usize = 512;
/// Most values one scan samples per column.
pub const MAX_SAMPLE_VALUES: u32 = 10_000;
/// The stable refusal code for a threshold outside `0..=1`.
pub const DETECTOR_THRESHOLD_INVALID: &str = "DETECTOR_THRESHOLD_INVALID";

/// How a detector recognises a sensitive value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DetectorMethod {
    Pattern,
    Checksum,
    Dictionary,
    Model,
}

/// One configured detector.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SensitiveDetector {
    pub id: String,
    pub category: String,
    pub method: DetectorMethod,
    /// Hit ratio, in `0..=1`, at which a column yields a proposed claim.
    pub threshold: f64,
}

/// What one detector found in one asset's sample. Counts only.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct Detection {
    pub asset: String,
    pub detector: String,
    pub sample_hits: u32,
    /// `sample_hits` divided by the sample size, in `0..=1`.
    pub ratio: f64,
}

/// A record field that failed validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DetectorFieldError {
    pub code: &'static str,
    pub field: &'static str,
}

impl std::fmt::Display for DetectorFieldError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.field)
    }
}

impl std::error::Error for DetectorFieldError {}

fn text(field: &'static str, value: &str) -> Result<(), DetectorFieldError> {
    if value.trim().is_empty()
        || value.trim() != value
        || value.len() > MAX_DETECTOR_TEXT_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(DetectorFieldError {
            code: "INVALID_FIELD",
            field,
        });
    }
    Ok(())
}

fn unit_interval(value: f64) -> bool {
    value.is_finite() && (0.0..=1.0).contains(&value)
}

impl SensitiveDetector {
    pub fn validate(&self) -> Result<(), DetectorFieldError> {
        text("id", &self.id)?;
        text("category", &self.category)?;
        if !unit_interval(self.threshold) {
            return Err(DetectorFieldError {
                code: DETECTOR_THRESHOLD_INVALID,
                field: "threshold",
            });
        }
        Ok(())
    }
}

impl Detection {
    pub fn validate(&self) -> Result<(), DetectorFieldError> {
        text("asset", &self.asset)?;
        text("detector", &self.detector)?;
        if self.sample_hits > MAX_SAMPLE_VALUES {
            return Err(DetectorFieldError {
                code: "INVALID_FIELD",
                field: "sample_hits",
            });
        }
        if !unit_interval(self.ratio) {
            return Err(DetectorFieldError {
                code: "INVALID_FIELD",
                field: "ratio",
            });
        }
        Ok(())
    }
}
