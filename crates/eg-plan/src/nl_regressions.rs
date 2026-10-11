//! Regression coverage for the public NL planning/execution entry points.

use super::*;

struct MockPlanner {
    canned: String,
}

impl NlPlanner for MockPlanner {
    fn plan(&self, _nl: &str, _hint: &str) -> Result<String, String> {
        Ok(self.canned.clone())
    }
}

#[test]
fn legacy_nl_entry_points_refuse_over_budget_output_before_parsing() {
    let fx = crate::fixture::build();
    let ctx = PlanCtx::new(&fx.view, &fx.semantic);
    // Invalid syntax distinguishes budget-first refusal from parser refusal.
    let planner = MockPlanner {
        canned: "!".repeat(NlQueryBudget::default().max_uql_len + 1),
    };
    for error in [
        plan_and_execute(&planner, "x", "", &ctx).unwrap_err(),
        plan_and_execute_opt(Some(&planner), "x", "", &ctx).unwrap_err(),
    ] {
        assert!(error.contains("exceeds budget"), "{error}");
    }
}

#[test]
fn legacy_nl_entry_points_accept_output_at_the_budget_boundary() {
    let fx = crate::fixture::build();
    let ctx = PlanCtx::new(&fx.view, &fx.semantic);
    let query = "MATCH (:Doc) |> LIMIT 1";
    let planner = MockPlanner {
        canned: format!(
            "{query}{}",
            " ".repeat(NlQueryBudget::default().max_uql_len - query.len())
        ),
    };
    let direct = plan_and_execute(&planner, "x", "", &ctx).unwrap();
    let optional = plan_and_execute_opt(Some(&planner), "x", "", &ctx)
        .unwrap()
        .unwrap();
    assert_eq!(direct.len(), 1);
    assert_eq!(direct.id_set(), optional.id_set());
}

// Covers the read-only guarantee tracked by EG-FEDERATED-QUERY-R058.3.2.
#[test]
fn nl_entry_points_refuse_mutating_statements() {
    let fx = crate::fixture::build();
    let ctx = PlanCtx::new(&fx.view, &fx.semantic);
    for query in [
        "CREATE (:Doc)",
        "MATCH (:Doc) DELETE n",
        "MATCH (:Doc) |> SET year = 1900",
        "MATCH (:Doc) |> LIMIT 1; DROP TABLE nodes",
    ] {
        let planner = MockPlanner {
            canned: query.into(),
        };
        assert!(
            plan_and_execute(&planner, "x", "", &ctx).is_err(),
            "{query}"
        );
        assert!(
            plan_and_execute_opt(Some(&planner), "x", "", &ctx).is_err(),
            "{query}"
        );
        assert!(
            matches!(
                plan_only(&planner, "x", "", NlQueryBudget::default()),
                Err(NlQueryError::ParseFailed { .. })
            ),
            "{query}"
        );
        assert!(
            matches!(
                plan_and_execute_typed(&planner, "x", "", NlQueryBudget::default(), &ctx),
                Err(NlQueryError::ParseFailed { .. })
            ),
            "{query}"
        );
    }
    let planner = MockPlanner {
        canned: "MATCH (:Doc) WHERE year > 2024 |> LIMIT 5".into(),
    };
    assert_eq!(
        plan_and_execute(&planner, "x", "", &ctx)
            .unwrap()
            .id_set()
            .len(),
        3
    );
}
