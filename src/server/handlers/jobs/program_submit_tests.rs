//! Served `ProgramOptimize` submit refusals: the request goes through the same
//! `submit_job_program_optimize` the `AnalyticsJob` submit arm calls, and the
//! wire response carries the declared refusal code. A refused submit yields no
//! `JobKind`, so no job, result or revision row can exist for it.

use super::{submit::submit_job_program_optimize, CarrierAuthority};
use crate::server::auth::VerifiedRequestContext;
use eg_program::testing::request;
use eg_program::{ExampleSplit, OptimizationRequest, OptimizerKind};

fn authority() -> CarrierAuthority {
    CarrierAuthority::from_verified(&VerifiedRequestContext::from_verified_claims(
        crate::acl::RequestContextClaims {
            principal: "program-submit-test".into(),
            agent_id: "program-submit-test".into(),
            tenant: "program-submit-tenant".into(),
            audience: "epistemic-graph".into(),
            policy_version: "test".into(),
            ..Default::default()
        },
        "program-submit-test-key".into(),
    ))
    .expect("verified fixture authority")
}

fn submit(bytes: Vec<u8>) -> Result<(), String> {
    let mut capabilities = Vec::new();
    submit_job_program_optimize(
        &authority(),
        810,
        "program-submit-policy",
        "program-submit",
        bytes,
        &mut capabilities,
    )
    .map(|_| ())
    .map_err(|response| response.error.expect("a refusal carries a code"))
}

fn encoded(request: &OptimizationRequest) -> Vec<u8> {
    rmp_serde::to_vec_named(request).expect("encode request")
}

// spec: EG-DECISION-ENGINE-R810.4
#[test]
fn each_invalid_program_request_is_refused_with_its_declared_code() {
    assert_eq!(submit(encoded(&request(OptimizerKind::LabeledFewShot))), Ok(()));

    let mut no_heldout = request(OptimizerKind::LabeledFewShot);
    no_heldout
        .corpus
        .examples
        .retain(|example| example.split == ExampleSplit::Train);
    assert_eq!(
        submit(encoded(&no_heldout)),
        Err("PROGRAM_HELDOUT_MISSING".to_string())
    );

    let mut over_budget = request(OptimizerKind::LabeledFewShot);
    over_budget.budget.max_candidates = usize::MAX;
    assert_eq!(
        submit(encoded(&over_budget)),
        Err("PROGRAM_BUDGET_EXCEEDED".to_string())
    );

    let mut unknown = serde_json::to_value(request(OptimizerKind::LabeledFewShot)).unwrap();
    unknown["optimizer"] = serde_json::json!("gradient_magic");
    let bytes = rmp_serde::to_vec_named(&unknown).expect("encode");
    assert_eq!(submit(bytes), Err("PROGRAM_OPTIMIZER_UNKNOWN".to_string()));
}
