//! Native current-plan storage contract. Authority is supplied by the verified
//! mutation context, never by a principal field in the request.
use serde::{Deserialize, Serialize};

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
}
impl PublishCurrentPlanRequest {
    pub fn validate(&self) -> Result<(), String> {
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
        snapshot: CurrentPlanSnapshot {
            run_id: "run".into(),
            revision: 0,
            observed_at_ms: 1,
            expires_at_ms: 2,
            items: vec![],
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // spec: EG-DURABLE-KERNEL-R028.5
    #[test]
    fn current_plan_status_matches_scaffold_contract() {
        assert_eq!(
            serde_json::to_string(&PlanStatus::Done).unwrap(),
            "\"done\""
        );
        assert!(serde_json::from_str::<PlanStatus>("\"completed\"").is_err());
        let mut request = sample_publish_request();
        assert!(request.validate().is_ok());
        request.snapshot.items.push(PlanItem {
            text: " ".into(),
            status: PlanStatus::Pending,
        });
        assert!(request.validate().is_err());
    }
}
