use serde::{Deserialize, Serialize};

use super::digest::SemanticDigest;
use super::identity::{validate_text, SemanticBinding};
use super::provenance::{SemanticLineage, SemanticLineageDraft};
use super::request::{SemanticIndexOperation, SEMANTIC_FILTER_MAX_RESULTS};
use super::stage::{
    SemanticQueueClass, SEMANTIC_FAST_CAPACITY, SEMANTIC_HEAVY_CONCURRENCY,
    SEMANTIC_LIGHT_CONCURRENCY, SEMANTIC_MEDIUM_CAPACITY, SEMANTIC_MEDIUM_CONCURRENCY,
    SEMANTIC_QUEUE_PROFILE_ID, SEMANTIC_SLOW_HEAVY_CAPACITY,
};
use super::state::{SemanticIndexError, SemanticIndexStatus, SemanticQueueStatus};

const SEMANTIC_QUEUE_RESULT_MAX: usize = 3;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SemanticSearchHit {
    pub binding_id: String,
    pub source_entity_id: String,
    pub source_revision: String,
    pub binding_digest: SemanticDigest,
    pub generation: u64,
    pub purpose_id: String,
    pub policy_digest: String,
    pub model_digest: String,
    pub preprocess_digest: String,
    pub lexical_index_digest: SemanticDigest,
    pub ann_index_digest: SemanticDigest,
    pub lexical_score: Option<f32>,
    pub vector_score: Option<f32>,
    pub fused_score: f32,
    /// Receipt for an affirmative server-side authorization decision. Absence
    /// is not representable on a materialized hit.
    pub authorization_receipt_digest: SemanticDigest,
    pub generation_checkpoint_digest: SemanticDigest,
    pub freshness_lag_ms: u64,
    pub fusion_explanation: String,
    pub lineage_digest: SemanticDigest,
}

impl SemanticSearchHit {
    pub fn validate(&self) -> Result<(), SemanticIndexError> {
        validate_text("fusion_explanation", &self.fusion_explanation)?;
        let lineage = SemanticLineage::create(SemanticLineageDraft {
            binding_id: self.binding_id.clone(),
            binding_digest: self.binding_digest,
            generation: self.generation,
            source_entity_id: self.source_entity_id.clone(),
            source_revision: self.source_revision.clone(),
            purpose_id: self.purpose_id.clone(),
            policy_digest: self.policy_digest.clone(),
            model_digest: self.model_digest.clone(),
            preprocess_digest: self.preprocess_digest.clone(),
            lexical_index_digest: self.lexical_index_digest,
            ann_index_digest: self.ann_index_digest,
            generation_checkpoint_digest: self.generation_checkpoint_digest,
            authorization_receipt_digest: self.authorization_receipt_digest,
        })?;
        if lineage.lineage_digest != self.lineage_digest {
            return Err(SemanticIndexError::LineageMismatch);
        }
        for score in [self.lexical_score, self.vector_score]
            .into_iter()
            .flatten()
            .chain(std::iter::once(self.fused_score))
        {
            if !score.is_finite() {
                return Err(SemanticIndexError::InvalidField {
                    field: "search_score".to_string(),
                    reason: "must be finite".to_string(),
                });
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum SemanticIndexResult {
    Binding {
        binding: Box<SemanticBinding>,
    },
    BindingList {
        bindings: Vec<SemanticBinding>,
        next_cursor: Option<String>,
    },
    Search {
        hits: Vec<SemanticSearchHit>,
        warnings: Vec<String>,
    },
    QueueStatus {
        queues: Vec<SemanticQueueStatus>,
    },
    Empty,
}

impl SemanticIndexResult {
    fn is_valid_for(&self, operation: SemanticIndexOperation) -> bool {
        match self {
            Self::Binding { .. } => matches!(
                operation,
                SemanticIndexOperation::CreateBinding
                    | SemanticIndexOperation::GetBinding
                    | SemanticIndexOperation::RefreshBinding
                    | SemanticIndexOperation::DisableBinding
            ),
            Self::BindingList { .. } => operation == SemanticIndexOperation::ListBindings,
            Self::Search { .. } => operation == SemanticIndexOperation::Search,
            Self::QueueStatus { .. } => operation == SemanticIndexOperation::QueueStatus,
            Self::Empty => operation == SemanticIndexOperation::DropBinding,
        }
    }

    fn validate_payload(&self) -> Result<(), SemanticIndexError> {
        match self {
            Self::Binding { binding } => binding.validate(),
            Self::BindingList {
                bindings,
                next_cursor,
            } => validate_binding_list(bindings, next_cursor),
            Self::Search { hits, warnings } => validate_search_results(hits, warnings),
            Self::QueueStatus { queues } => validate_queue_results(queues),
            Self::Empty => Ok(()),
        }
    }
}

fn validate_binding_list(
    bindings: &[SemanticBinding],
    next_cursor: &Option<String>,
) -> Result<(), SemanticIndexError> {
    validate_result_count(bindings.len())?;
    for binding in bindings {
        binding.validate()?;
    }
    if let Some(cursor) = next_cursor {
        validate_text("next_cursor", cursor)?;
    }
    Ok(())
}

fn validate_search_results(
    hits: &[SemanticSearchHit],
    warnings: &[String],
) -> Result<(), SemanticIndexError> {
    validate_result_count(hits.len())?;
    validate_result_count(warnings.len())?;
    for hit in hits {
        hit.validate()?;
    }
    for warning in warnings {
        validate_text("search_warning", warning)?;
    }
    Ok(())
}

fn validate_queue_results(queues: &[SemanticQueueStatus]) -> Result<(), SemanticIndexError> {
    if queues.len() > SEMANTIC_QUEUE_RESULT_MAX {
        return Err(SemanticIndexError::InvalidField {
            field: "semantic_queue_count".to_string(),
            reason: format!("must not exceed {SEMANTIC_QUEUE_RESULT_MAX}"),
        });
    }
    let mut seen = [false; SEMANTIC_QUEUE_RESULT_MAX];
    for queue in queues {
        validate_queue_status(queue)?;
        let index = queue_class_index(queue.class);
        if std::mem::replace(&mut seen[index], true) {
            return Err(SemanticIndexError::InvalidField {
                field: "semantic_queue_class".to_string(),
                reason: "queue classes must be unique".to_string(),
            });
        }
    }
    Ok(())
}

fn validate_result_count(count: usize) -> Result<(), SemanticIndexError> {
    if count > SEMANTIC_FILTER_MAX_RESULTS as usize {
        return Err(SemanticIndexError::InvalidField {
            field: "semantic_result_count".to_string(),
            reason: format!("must not exceed {SEMANTIC_FILTER_MAX_RESULTS}"),
        });
    }
    Ok(())
}

fn validate_queue_status(queue: &SemanticQueueStatus) -> Result<(), SemanticIndexError> {
    for (field, value) in [
        ("queue_profile_id", queue.queue_profile_id.as_str()),
        ("tenant_id", queue.tenant_id.as_str()),
        ("trace_id", queue.trace_id.as_str()),
    ] {
        validate_text(field, value)?;
    }
    if queue.queue_profile_id != SEMANTIC_QUEUE_PROFILE_ID
        || queue.capacity != queue_capacity(queue.class)
        || queue.inflight > queue_concurrency(queue.class)
        || queue.consecutive_claim_percent > 100
    {
        return Err(SemanticIndexError::InvalidField {
            field: "semantic_queue_status".to_string(),
            reason: "capacity, inflight, or fairness percentage is invalid".to_string(),
        });
    }
    Ok(())
}

fn queue_concurrency(class: SemanticQueueClass) -> u32 {
    match class {
        SemanticQueueClass::Fast => SEMANTIC_LIGHT_CONCURRENCY,
        SemanticQueueClass::Medium => SEMANTIC_MEDIUM_CONCURRENCY,
        SemanticQueueClass::SlowHeavy => SEMANTIC_HEAVY_CONCURRENCY,
    }
}

fn queue_class_index(class: SemanticQueueClass) -> usize {
    match class {
        SemanticQueueClass::Fast => 0,
        SemanticQueueClass::Medium => 1,
        SemanticQueueClass::SlowHeavy => 2,
    }
}

fn queue_capacity(class: SemanticQueueClass) -> u32 {
    match class {
        SemanticQueueClass::Fast => SEMANTIC_FAST_CAPACITY,
        SemanticQueueClass::Medium => SEMANTIC_MEDIUM_CAPACITY,
        SemanticQueueClass::SlowHeavy => SEMANTIC_SLOW_HEAVY_CAPACITY,
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum SemanticIndexOutcome {
    Accepted {
        result: SemanticIndexResult,
        receipt_digest: Option<SemanticDigest>,
    },
    DeferredBackpressured {
        receipt_digest: SemanticDigest,
    },
    Partial {
        result: SemanticIndexResult,
        receipt_digest: Option<SemanticDigest>,
    },
    Rejected {
        error: SemanticIndexError,
        receipt_digest: Option<SemanticDigest>,
    },
    NotFound,
}

impl SemanticIndexOutcome {
    pub fn status(&self) -> SemanticIndexStatus {
        match self {
            Self::Accepted { .. } => SemanticIndexStatus::Accepted,
            Self::DeferredBackpressured { .. } => SemanticIndexStatus::DeferredBackpressured,
            Self::Partial { .. } => SemanticIndexStatus::Partial,
            Self::Rejected { .. } => SemanticIndexStatus::Rejected,
            Self::NotFound => SemanticIndexStatus::NotFound,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SemanticIndexResponse {
    pub request_id: String,
    pub operation: SemanticIndexOperation,
    pub outcome: SemanticIndexOutcome,
}

impl SemanticIndexResponse {
    pub fn validate(&self) -> Result<(), SemanticIndexError> {
        validate_text("request_id", &self.request_id)?;
        let result = match &self.outcome {
            SemanticIndexOutcome::Accepted {
                result,
                receipt_digest,
            } => {
                if self.operation.requires_approval() && receipt_digest.is_none() {
                    return Err(SemanticIndexError::MutationReceiptRequired {
                        operation: self.operation,
                    });
                }
                result
            }
            SemanticIndexOutcome::Partial { result, .. } => {
                if self.operation.requires_approval() {
                    return Err(SemanticIndexError::PartialMutationOutcome {
                        operation: self.operation,
                    });
                }
                result
            }
            SemanticIndexOutcome::DeferredBackpressured { .. }
            | SemanticIndexOutcome::Rejected { .. }
            | SemanticIndexOutcome::NotFound => return Ok(()),
        };
        if result.is_valid_for(self.operation) {
            result.validate_payload()
        } else {
            Err(SemanticIndexError::OperationResultMismatch {
                operation: self.operation,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn response_result_must_match_the_declared_operation() {
        let response = SemanticIndexResponse {
            request_id: "request-a".into(),
            operation: SemanticIndexOperation::Search,
            outcome: SemanticIndexOutcome::Accepted {
                result: SemanticIndexResult::Empty,
                receipt_digest: None,
            },
        };
        assert_eq!(
            response.validate(),
            Err(SemanticIndexError::OperationResultMismatch {
                operation: SemanticIndexOperation::Search,
            })
        );
    }

    #[test]
    fn mutating_responses_require_durable_receipts_and_cannot_be_partial() {
        let accepted = SemanticIndexResponse {
            request_id: "request-a".into(),
            operation: SemanticIndexOperation::DropBinding,
            outcome: SemanticIndexOutcome::Accepted {
                result: SemanticIndexResult::Empty,
                receipt_digest: None,
            },
        };
        assert_eq!(
            accepted.validate(),
            Err(SemanticIndexError::MutationReceiptRequired {
                operation: SemanticIndexOperation::DropBinding,
            })
        );

        let partial = SemanticIndexResponse {
            request_id: "request-b".into(),
            operation: SemanticIndexOperation::DropBinding,
            outcome: SemanticIndexOutcome::Partial {
                result: SemanticIndexResult::Empty,
                receipt_digest: Some(SemanticDigest::from_bytes([1; 32])),
            },
        };
        assert_eq!(
            partial.validate(),
            Err(SemanticIndexError::PartialMutationOutcome {
                operation: SemanticIndexOperation::DropBinding,
            })
        );
    }

    #[test]
    fn queue_status_uses_the_exact_profile_capacity_and_class_set() {
        let queue = |class, capacity| SemanticQueueStatus {
            queue_profile_id: SEMANTIC_QUEUE_PROFILE_ID.into(),
            class,
            capacity,
            inflight: 0,
            oldest_age_ms: 0,
            lag_rows: 0,
            lag_ms: 0,
            retry_count: 0,
            rejection_count: 0,
            live: true,
            saturated: false,
            tenant_id: "tenant-a".into(),
            consecutive_claim_percent: 0,
            fairness_relaxation_count: 0,
            trace_id: "trace-a".into(),
        };
        assert!(validate_queue_results(&[
            queue(SemanticQueueClass::Fast, SEMANTIC_FAST_CAPACITY),
            queue(SemanticQueueClass::Medium, SEMANTIC_MEDIUM_CAPACITY),
            queue(SemanticQueueClass::SlowHeavy, SEMANTIC_SLOW_HEAVY_CAPACITY,),
        ])
        .is_ok());
        assert!(validate_queue_results(&[
            queue(SemanticQueueClass::Fast, SEMANTIC_FAST_CAPACITY),
            queue(SemanticQueueClass::Fast, SEMANTIC_FAST_CAPACITY),
        ])
        .is_err());
        assert!(validate_queue_results(&[queue(SemanticQueueClass::Medium, 1)]).is_err());
        let mut medium_overcommitted = queue(SemanticQueueClass::Medium, SEMANTIC_MEDIUM_CAPACITY);
        medium_overcommitted.inflight = SEMANTIC_MEDIUM_CONCURRENCY + 1;
        assert!(validate_queue_results(&[medium_overcommitted]).is_err());

        let mut heavy_overcommitted =
            queue(SemanticQueueClass::SlowHeavy, SEMANTIC_SLOW_HEAVY_CAPACITY);
        heavy_overcommitted.inflight = SEMANTIC_HEAVY_CONCURRENCY + 1;
        assert!(validate_queue_results(&[heavy_overcommitted]).is_err());
    }
}
