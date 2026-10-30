use super::*;

fn window(tokens: u64) -> WindowBudget {
    WindowBudget::with_default_fraction(tokens)
}

// spec: EG-DECISION-ENGINE-R137.1
#[test]
fn context_budget_round_trips() {
    let limits = ParallelMapLimits::default();
    let budget = window(10_000);
    let plan = decide_over_window(9_000, &budget, &limits).unwrap();
    let value = serde_json::to_value((&limits, &budget, &plan)).unwrap();
    assert_eq!(
        serde_json::from_value::<(ParallelMapLimits, WindowBudget, OverWindowPlan)>(value.clone())
            .unwrap(),
        (limits, budget, plan)
    );
    for (index, field) in [(0, "max_depth"), (1, "window_tokens"), (2, "depth")] {
        assert!(value[index].get(field).is_some(), "{field} is on the wire");
    }
    let mut unknown = value[2].clone();
    unknown["model"] = serde_json::json!("x");
    assert!(serde_json::from_value::<OverWindowPlan>(unknown).is_err());
    let mut unknown = value[0].clone();
    unknown["retries"] = serde_json::json!(1);
    assert!(serde_json::from_value::<ParallelMapLimits>(unknown).is_err());
    let mut unknown = value[1].clone();
    unknown["reserved"] = serde_json::json!(1);
    assert!(serde_json::from_value::<WindowBudget>(unknown).is_err());
    let kind = serde_json::to_value(crate::decision::statistical::QuestionKind::ContextBudget);
    assert_eq!(kind.unwrap(), "context_budget");
}

// spec: EG-DECISION-ENGINE-R137.2
#[test]
fn over_window_prompt_is_split_within_limits() {
    let limits = ParallelMapLimits::default();
    assert_eq!(
        (limits.max_depth, limits.max_items, limits.max_model_calls),
        (3, 1000, 1000)
    );
    let budget = window(10_000);
    assert_eq!(budget.threshold_tokens(), 8_000, "default fraction is 0.8");
    let fits = decide_over_window(8_000, &budget, &limits).unwrap();
    assert_eq!((fits.exceeds, fits.chunks, fits.depth), (false, 1, 0));
    let plan = decide_over_window(9_000, &budget, &limits).unwrap();
    assert!(plan.exceeds);
    assert!(plan.depth >= 1 && plan.depth <= limits.max_depth);
    assert!(plan.chunk_tokens <= budget.threshold_tokens());
    assert!(u64::from(plan.chunks) * plan.chunk_tokens >= 9_000);
    assert_eq!((plan.chunks, plan.depth, plan.model_calls), (2, 1, 3));
    let deep = decide_over_window(60_000, &budget, &limits).unwrap();
    assert_eq!((deep.chunks, deep.depth, deep.chunk_tokens), (8, 3, 7_500));
}

// spec: EG-DECISION-ENGINE-R137.2
#[test]
fn over_window_refusals_name_the_limit() {
    let budget = window(10_000);
    let limits = ParallelMapLimits::default();
    assert_eq!(
        decide_over_window(70_000, &budget, &limits).unwrap_err(),
        ContextBudgetRefusal::BudgetExhausted {
            limit: ContextBudgetLimit::MaxDepth,
            observed: 3
        }
    );
    let tight = ParallelMapLimits {
        max_items: 2,
        ..limits
    };
    assert_eq!(
        decide_over_window(20_000, &budget, &tight).unwrap_err(),
        ContextBudgetRefusal::TooManyItems {
            max_items: 2,
            observed: 4
        }
    );
    let calls = ParallelMapLimits {
        max_model_calls: 2,
        ..limits
    };
    assert_eq!(
        decide_over_window(9_000, &budget, &calls).unwrap_err(),
        ContextBudgetRefusal::BudgetExhausted {
            limit: ContextBudgetLimit::MaxModelCalls,
            observed: 3
        }
    );
    let cost = ParallelMapLimits {
        max_cost: 9_000,
        ..limits
    };
    assert_eq!(
        decide_over_window(9_001, &budget, &cost).unwrap_err(),
        ContextBudgetRefusal::BudgetExhausted {
            limit: ContextBudgetLimit::MaxCost,
            observed: 9_002
        }
    );
    let wire = serde_json::to_value(decide_over_window(1, &window(1), &limits).unwrap_err());
    assert_eq!(wire.unwrap()["reason"], "budget_exhausted");
}

fn candidate(
    id: &str,
    unit: &str,
    resolution: &str,
    start: u64,
    end: u64,
    weight: u64,
) -> ContextPackageCandidate {
    ContextPackageCandidate {
        candidate_id: id.into(),
        evidence_unit: unit.into(),
        resolution: resolution.into(),
        source_id: format!("source:{unit}"),
        cited_range: PackageCitedRange { start, end },
        source_revision: format!("rev:{unit}"),
        content_digest: format!("sha256:{id}"),
        tokenizer_id: "tokenizer:eg".into(),
        tokenizer_version: "v3".into(),
        token_weight: weight,
        value: UnitRationalWire::new(7, 10).unwrap(),
        measurement_provenance: format!("measured:{id}"),
    }
}

fn package_request() -> ContextPackageRequest {
    ContextPackageRequest {
        budget: ContextPackageBudget {
            window_tokens: 128_000,
            reserved_output_tokens: 8_000,
            caller_token_ceiling: Some(96_000),
            monetary_ceiling: Some(PackageMoney {
                currency: "usd".into(),
                minor_units: 100_000,
            }),
            price_per_input_token: Some(PackageMoney {
                currency: "usd".into(),
                minor_units: 1,
            }),
            latency_ceiling_ms: Some(4_000),
            token_throughput_per_s: Some(2_500),
        },
        candidates: BoundedVec::new(vec![
            candidate("pkg-a", "unit-1", "coarse", 0, 1_200, 4_000),
            candidate("pkg-b", "unit-1", "fine", 0, 3_000, 9_000),
            candidate("pkg-c", "unit-2", "coarse", 100, 900, 2_000),
        ])
        .unwrap(),
        marginal_value_floor: UnitRationalWire::new(1, 5).unwrap(),
        solver_budget: 4_096,
    }
}

// spec: EG-DECISION-ENGINE-R137.7
#[test]
fn package_records_round_trip() {
    let request = package_request();
    let wire = serde_json::to_string(&request).unwrap();
    assert_eq!(
        serde_json::from_str::<ContextPackageRequest>(&wire).unwrap(),
        request
    );

    // Every constraint, source/range/revision and measurement identity
    // survives the round trip.
    let value = serde_json::to_value(&request).unwrap();
    let first = &value["candidates"][0];
    for field in [
        "candidate_id",
        "evidence_unit",
        "resolution",
        "source_id",
        "cited_range",
        "source_revision",
        "content_digest",
        "tokenizer_id",
        "tokenizer_version",
        "token_weight",
        "value",
        "measurement_provenance",
    ] {
        assert!(first.get(field).is_some(), "{field} is on the wire");
    }
    assert_eq!(value["budget"]["caller_token_ceiling"], 96_000);
    assert_eq!(value["budget"]["monetary_ceiling"]["currency"], "usd");
    assert_eq!(value["marginal_value_floor"]["numerator"], 1);

    // The selection and its refusals round trip too.
    let selection = ContextPackageSelection {
        selected_candidate_ids: BoundedVec::new(vec!["pkg-a".into(), "pkg-c".into()]).unwrap(),
        effective_capacity_tokens: 96_000,
        total_tokens: 6_000,
        total_cost: Some(PackageMoney {
            currency: "usd".into(),
            minor_units: 6_000,
        }),
        total_latency_ms: 2_400,
        objective: 7_000_000,
        input_digest: "sha256:inputs".into(),
        model_digest: "sha256:model".into(),
        certificate_digest: "sha256:cert".into(),
        verified: true,
    };
    let wire = serde_json::to_string(&selection).unwrap();
    assert_eq!(
        serde_json::from_str::<ContextPackageSelection>(&wire).unwrap(),
        selection
    );
    let wire =
        serde_json::to_string(&PackageRecordRefusal::WeightOverflow { observed: 42 }).unwrap();
    assert!(wire.contains("\"reason\":\"weight_overflow\""));
    assert_eq!(
        serde_json::from_str::<PackageRecordRefusal>(&wire).unwrap(),
        PackageRecordRefusal::WeightOverflow { observed: 42 }
    );

    // Validation refuses duplicate candidate ids.
    let mut dup = package_request();
    dup.candidates = BoundedVec::new(vec![
        candidate("pkg-a", "unit-1", "coarse", 0, 1_200, 4_000),
        candidate("pkg-a", "unit-2", "coarse", 0, 1_200, 4_000),
    ])
    .unwrap();
    assert!(matches!(
        dup.checked(),
        Err(PackageRecordRefusal::DuplicateCandidateId { .. })
    ));

    // ... and inconsistent resolution groups.
    let mut inconsistent = package_request();
    inconsistent.candidates = BoundedVec::new(vec![
        candidate("pkg-a", "unit-1", "coarse", 0, 1_200, 4_000),
        candidate("pkg-b", "unit-1", "coarse", 0, 3_000, 9_000),
    ])
    .unwrap();
    assert!(matches!(
        inconsistent.checked(),
        Err(PackageRecordRefusal::InconsistentResolutionGroup {
            candidate_id,
            evidence_unit,
            resolution
        }) if candidate_id == "pkg-b"
            && evidence_unit == "unit-1"
            && resolution == "coarse"
    ));

    // ... and malformed ranges.
    let mut inverted = package_request();
    inverted.candidates = BoundedVec::new(vec![candidate(
        "pkg-a",
        "unit-1",
        "coarse",
        900,
        100,
        4_000,
    )])
    .unwrap();
    assert!(matches!(
        inverted.checked(),
        Err(PackageRecordRefusal::MalformedRange { .. })
    ));

    // A negative measurement is refused by the unsigned type on the way in.
    let mut value = serde_json::to_value(package_request()).unwrap();
    value["candidates"][0]["token_weight"] = serde_json::json!(-1);
    assert!(serde_json::from_value::<ContextPackageRequest>(value).is_err());

    // A weight sum that overflows the token counter refuses before solving.
    let mut overflow = package_request();
    overflow.candidates = BoundedVec::new(vec![
        candidate("pkg-a", "unit-1", "coarse", 0, 1, u64::MAX),
        candidate("pkg-b", "unit-2", "coarse", 0, 1, 1),
    ])
    .unwrap();
    assert!(matches!(
        overflow.checked(),
        Err(PackageRecordRefusal::WeightOverflow { .. })
    ));
}
