//! `ATTRIBUTE` (EH-523) through the served UQL path: parse → optimize → execute →
//! channels. The kernel's own oracles (brute force, axioms, CI coverage) live in
//! eg-numeric; these prove the stage wires the rows, values and channels through.

#[cfg(feature = "numeric")]
mod served {
    use eg_core::compute::semantic::SemanticStore;
    use eg_core::graph::GraphCore;
    use eg_types::wire::{UqlResult, UqlRow};
    use serde_json::json;

    use crate::exec::PlanCtx;
    use crate::uql::serve::run_statement;
    use crate::uql::{parse_statement, Params};

    fn blob(v: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&v).unwrap()
    }

    /// Five services on two pods, each with a p95 latency; `s5` has no latency.
    fn fixture() -> (eg_core::graph::GraphView, SemanticStore) {
        let core = GraphCore::new();
        let services = [
            ("s1", 120.0, "pod-a"),
            ("s2", 40.0, "pod-a"),
            ("s3", 300.0, "pod-b"),
            ("s4", 75.0, "pod-b"),
        ];
        for (id, latency, pod) in services {
            let props = json!({ "type": "Service", "latency_ms": latency, "pod": pod });
            core.add_node(id.into(), blob(props));
        }
        core.add_node("s5".into(), blob(json!({ "type": "Other" })));
        (core.analysis_snapshot(), SemanticStore::new())
    }

    fn rows(src: &str) -> Result<(Vec<String>, Vec<UqlRow>), String> {
        let (view, semantic) = fixture();
        let ctx = PlanCtx::new(&view, &semantic);
        let stmt = parse_statement(src, &Params::new()).map_err(|e| e.render(src))?;
        match run_statement(&stmt, &ctx)? {
            UqlResult::Rows { columns, rows, .. } => Ok((columns, rows)),
            other => panic!("rows expected, got {other:?}"),
        }
    }

    fn channel(rows: &[UqlRow], index: usize) -> Vec<f64> {
        rows.iter()
            .map(|r| r.channels[index].expect("channel written"))
            .collect()
    }

    #[test]
    fn shapley_contributions_sum_to_the_grand_value_and_rank_descending() {
        let (columns, rows) =
            rows("MATCH (:Service) |> ATTRIBUTE MAX OF latency_ms SHAPLEY |> RETURN attribution")
                .unwrap();
        assert_eq!(columns, vec!["attribution"]);
        let phi = channel(&rows, 0);
        let total: f64 = phi.iter().sum();
        assert!(
            (total - 300.0).abs() < 1e-3,
            "Σφ = {total}, v(N) = max = 300"
        );
        assert!(phi.windows(2).all(|w| w[0] >= w[1]), "descending: {phi:?}");
        assert_eq!(rows[0].id, "s3");
    }

    #[test]
    fn a_linear_split_of_a_sum_is_each_rows_own_value() {
        let (_, rows) =
            rows("MATCH (:Service) |> ATTRIBUTE SUM OF latency_ms LINEAR |> RETURN attribution")
                .unwrap();
        let got: Vec<(String, f64)> = rows
            .iter()
            .map(|r| (r.id.clone(), f64::from(r.score.unwrap())))
            .collect();
        let want = [("s3", 300.0), ("s1", 120.0), ("s4", 75.0), ("s2", 40.0)];
        for ((id, value), (want_id, want_value)) in got.iter().zip(want) {
            assert_eq!(id, want_id);
            assert!((value - want_value).abs() < 1e-3);
        }
    }

    #[test]
    fn a_sampled_estimate_reports_its_interval_and_replays_from_its_seed() {
        let src = "MATCH (:Service) |> ATTRIBUTE P95 OF latency_ms SHAPLEY SAMPLES 400 SEED 7 \
                   |> RETURN attribution, attribution_ci";
        let (columns, first) = rows(src).unwrap();
        assert_eq!(columns, vec!["attribution", "attribution_ci"]);
        assert!(channel(&first, 1).iter().all(|w| *w >= 0.0));
        let (_, second) = rows(src).unwrap();
        assert_eq!(first, second);
    }

    #[test]
    fn owen_values_group_rows_by_a_property() {
        let (_, rows) = rows(
            "MATCH (:Service) |> ATTRIBUTE MAX OF latency_ms OWEN BY pod |> RETURN attribution",
        )
        .unwrap();
        let total: f64 = channel(&rows, 0).iter().sum();
        assert!((total - 300.0).abs() < 1e-3);
    }

    #[test]
    fn refusals_name_their_reason() {
        let error = rows("MATCH (:Service) |> ATTRIBUTE MAX OF latency_ms LINEAR").unwrap_err();
        assert!(error.contains("ATTRIBUTION_NON_ADDITIVE"), "{error}");
        let error = rows("MATCH () |> ATTRIBUTE SUM OF latency_ms SHAPLEY").unwrap_err();
        assert!(error.contains("s5"), "{error}");
        let error = rows("MATCH (:Service) |> ATTRIBUTE SUM OF SCORE SHAPLEY").unwrap_err();
        assert!(error.contains("no numeric value"), "{error}");
    }
}

#[cfg(not(feature = "numeric"))]
#[test]
fn a_build_without_numeric_refuses_the_stage_by_feature() {
    use crate::uql::{parse_statement, Params, UqlCode};
    let error = parse_statement(
        "MATCH (:Service) |> ATTRIBUTE SUM OF latency_ms LINEAR",
        &Params::new(),
    )
    .unwrap_err();
    assert_eq!(error.code, UqlCode::FeatureNotInBuild);
    assert!(error.msg.contains("`numeric`"), "{}", error.msg);
}

#[test]
fn spellings_print_canonically() {
    use eg_types::wire::{uql_op, AttributionInput, AttributionMethod, AttributionValue, Op};
    let op = Op::Attribute {
        input: AttributionInput::Score,
        value: AttributionValue::Percentile { p: 50 },
        method: AttributionMethod::Owen { by: "value".into() },
    };
    assert_eq!(uql_op(&op).unwrap(), "ATTRIBUTE P50 OF SCORE OWEN BY value");
    let bad = Op::Attribute {
        input: AttributionInput::Score,
        value: AttributionValue::Percentile { p: 100 },
        method: AttributionMethod::Linear,
    };
    assert!(uql_op(&bad).is_err());
}
