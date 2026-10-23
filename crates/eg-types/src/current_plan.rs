//! Native current-plan storage contract. Authority is supplied by the verified
//! mutation context, never by a principal field in the request.
use serde::{Deserialize, Serialize};

use crate::decision::task_plan::{plan_explicit_tasks, TaskPlan, TaskPlanProposal};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum PlanStatus {
    Pending,
    InProgress,
    Done,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PlanItem {
    pub text: String,
    pub status: PlanStatus,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CurrentPlanSnapshot {
    pub run_id: String,
    pub revision: u64,
    pub observed_at_ms: u64,
    pub expires_at_ms: u64,
    pub items: Vec<PlanItem>,
    /// Immutable executable attachment, not scaffold text or authority. The
    /// native owner transaction verifies the original proposal before binding.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_plan: Option<TaskPlan>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PublishCurrentPlanRequest {
    pub tenant: String,
    pub snapshot: CurrentPlanSnapshot,
    pub worker_id: String,
    pub lease_epoch: u64,
    pub fencing_token: u64,
    pub idempotency_key: String,
    /// Required only with an explicit `snapshot.task_plan`. Recomputing from
    /// the original declaration order verifies even the plan's input digest.
    /// Omit both fields on later scaffold updates to retain the bound plan.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_plan_proposal: Option<TaskPlanProposal>,
}
impl PublishCurrentPlanRequest {
    pub fn validate(&self) -> Result<(), String> {
        self.validate_identifiers()?;
        if self.snapshot.observed_at_ms >= self.snapshot.expires_at_ms {
            return Err("current-plan validity interval is empty".into());
        }
        if self
            .snapshot
            .items
            .iter()
            .any(|item| item.text.trim().is_empty())
        {
            return Err("current-plan items require nonblank text".into());
        }
        let bytes: usize = self.snapshot.items.iter().map(|item| item.text.len()).sum();
        if self.snapshot.items.len() > 1024 || bytes > 1024 * 1024 {
            return Err("current-plan payload exceeds bounds".into());
        }
        match (&self.task_plan_proposal, &self.snapshot.task_plan) {
            (None, None) => {}
            (Some(proposal), Some(plan)) => {
                if &plan_explicit_tasks(proposal)? != plan {
                    return Err("current-plan task plan does not match canonical proposal".into());
                }
            }
            _ => {
                return Err("current-plan task plan and proposal must be supplied together".into())
            }
        }
        Ok(())
    }

    fn validate_identifiers(&self) -> Result<(), String> {
        for value in [
            &self.tenant,
            &self.snapshot.run_id,
            &self.worker_id,
            &self.idempotency_key,
        ] {
            if value.trim().is_empty() || value.len() > 512 {
                return Err("current-plan identifier is empty or oversized".into());
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CurrentPlanPublished {
    pub run_id: String,
    pub revision: u64,
    pub payload_digest: String,
    pub changed_work_item_ids: Vec<String>,
}

/// Shared typed publication fixture for native identity and validation tests.
#[cfg(any(test, feature = "test-support"))]
pub fn sample_publish_request() -> PublishCurrentPlanRequest {
    PublishCurrentPlanRequest {
        tenant: "tenant".into(),
        worker_id: "worker".into(),
        lease_epoch: 1,
        fencing_token: 1,
        idempotency_key: "key".into(),
        task_plan_proposal: None,
        snapshot: CurrentPlanSnapshot {
            run_id: "run".into(),
            revision: 0,
            observed_at_ms: 1,
            expires_at_ms: 2,
            items: vec![],
            task_plan: None,
        },
    }
}

/// Original proposal order intentionally differs from canonical task order.
#[cfg(any(test, feature = "test-support"))]
pub fn sample_task_plan_publish_request() -> PublishCurrentPlanRequest {
    let proposal: TaskPlanProposal = serde_json::from_value(serde_json::json!({
        "tasks": [
            {"task_id": "join", "prompt": "Combine evidence", "depends_on": ["b", "a"]},
            {"task_id": "b", "prompt": "Analyze source b"},
            {"task_id": "a", "prompt": "Analyze source a"}
        ],
        "output_task_id": "join", "max_concurrency": 2, "max_steps": 3
    }))
    .expect("valid task plan fixture");
    let mut request = sample_publish_request();
    request.snapshot.task_plan = Some(plan_explicit_tasks(&proposal).expect("valid task plan"));
    request.task_plan_proposal = Some(proposal);
    request
}

#[cfg(test)]
mod tests;
