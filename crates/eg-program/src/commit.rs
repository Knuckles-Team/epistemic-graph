//! Promotion commit seam.
//!
//! `eg-program` can decide which candidate satisfies policy, but it cannot mutate
//! graph state. The caller must lower the selected candidate to the engine's
//! existing `ChangeEnvelope`/`MutationBatch` and submit it through the one governed
//! persistence authority.

use std::future::Future;
use std::pin::Pin;

use eg_types::{ChangeEnvelope, MutationBatchCommit};

use crate::{OptimizationResult, ProgramError};

#[derive(Clone, Debug)]
pub struct PromotionCommit {
    pub result: OptimizationResult,
    pub envelope: ChangeEnvelope,
}

impl PromotionCommit {
    pub fn validate(&self) -> Result<(), ProgramError> {
        let candidate = self
            .result
            .selected_candidate()
            .filter(|_| self.result.promoted)
            .ok_or(ProgramError::InvalidCommit)?;
        self.envelope
            .validate()
            .map_err(|_| ProgramError::InvalidCommit)?;
        if self.envelope.content_version.object_id != candidate.candidate_ref.as_str()
            || self.envelope.content_version.digest != candidate.content_digest
        {
            return Err(ProgramError::InvalidCommit);
        }
        Ok(())
    }
}

/// Adapter implemented by the engine's existing mutation coordinator. There is no
/// direct storage or graph-write API in this crate.
pub trait GovernedPromotionSink: Send + Sync {
    fn commit<'a>(
        &'a self,
        promotion: PromotionCommit,
    ) -> Pin<Box<dyn Future<Output = Result<MutationBatchCommit, ProgramError>> + Send + 'a>>;
}
