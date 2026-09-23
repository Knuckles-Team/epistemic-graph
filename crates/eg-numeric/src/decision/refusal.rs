//! One error shape for the decision computation: a closed code and a detail.

use eg_types::decision::statistical::StatisticalErrorCode;
use eg_types::decision::DecisionErrorCode;

use crate::detkernel::StatError;

/// A typed refusal. Rendered as `"CODE: detail"` by the served surface. The
/// code is always the token of a closed code enum: either this surface's own
/// [`StatisticalErrorCode`] or the shared [`DecisionErrorCode`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refusal {
    pub code: &'static str,
    pub detail: String,
}

/// The result shape of every fallible decision computation.
pub type RefusalResult<T> = Result<T, Refusal>;

impl Refusal {
    pub fn new(code: StatisticalErrorCode, detail: impl Into<String>) -> Self {
        Self {
            code: code.as_str(),
            detail: detail.into(),
        }
    }

    /// A refusal under the shared decision vocabulary.
    pub fn decision(code: DecisionErrorCode, detail: impl Into<String>) -> Self {
        Self {
            code: code.as_str(),
            detail: detail.into(),
        }
    }

    /// The one text form: `"CODE: detail"`.
    pub fn render(&self) -> String {
        format!("{}: {}", self.code, self.detail)
    }
}

impl From<StatError> for Refusal {
    fn from(error: StatError) -> Self {
        Self::new(StatisticalErrorCode::NumericRefused, error.to_string())
    }
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.render())
    }
}
