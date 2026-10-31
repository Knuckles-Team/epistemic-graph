use super::*;

fn window(tokens: u64) -> WindowBudget {
    WindowBudget::with_default_fraction(tokens)
}

// spec: MEDULLA-DECISION-ENGINE-R137.1
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

// spec: MEDULLA-DECISION-ENGINE-R137.2
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

// spec: MEDULLA-DECISION-ENGINE-R137.2
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
