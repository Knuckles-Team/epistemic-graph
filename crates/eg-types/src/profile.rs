//! Column profile record (EG-UNIFIED-DATA-PLANE-R761.1): the statistics one
//! `DatasetProfile` job records for one column of an attached or native
//! dataset, and how they were measured.

use serde::{Deserialize, Serialize};

/// How a profile was measured: an aggregate pushed to the source, or a
/// sample of `size` rows.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum ProfileMethod {
    Pushdown,
    Sample { size: u64 },
}

/// One value and its count among the most frequent values.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopValue {
    pub value: String,
    pub count: u64,
}

/// One histogram bucket over `[lower, upper)`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistogramBucket {
    pub lower: String,
    pub upper: String,
    pub count: u64,
}

/// The profile of one column.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnProfile {
    pub dataset: String,
    pub column: String,
    pub rows: u64,
    pub nulls: u64,
    pub distinct_estimate: u64,
    pub min: Option<String>,
    pub max: Option<String>,
    pub top_k: Vec<TopValue>,
    pub histogram: Vec<HistogramBucket>,
    pub profiled_at: i64,
    pub method: ProfileMethod,
}

/// A profile whose counts contradict each other.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InvalidColumnProfile {
    NullsExceedRows,
    DistinctExceedsNonNull,
    BlankIdentity,
}

impl std::fmt::Display for InvalidColumnProfile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::NullsExceedRows => "null count exceeds row count",
            Self::DistinctExceedsNonNull => "distinct estimate exceeds non-null rows",
            Self::BlankIdentity => "profile names no dataset or column",
        };
        write!(f, "{message}")
    }
}

impl std::error::Error for InvalidColumnProfile {}

impl ColumnProfile {
    /// Fraction of rows that are null, `0.0` for an empty dataset.
    pub fn null_ratio(&self) -> f64 {
        if self.rows == 0 {
            0.0
        } else {
            self.nulls as f64 / self.rows as f64
        }
    }

    /// Refuse a profile with no identity or contradictory counts.
    pub fn validate(&self) -> Result<(), InvalidColumnProfile> {
        if self.dataset.trim().is_empty() || self.column.trim().is_empty() {
            return Err(InvalidColumnProfile::BlankIdentity);
        }
        if self.nulls > self.rows {
            return Err(InvalidColumnProfile::NullsExceedRows);
        }
        if self.distinct_estimate > self.rows - self.nulls {
            return Err(InvalidColumnProfile::DistinctExceedsNonNull);
        }
        Ok(())
    }
}
