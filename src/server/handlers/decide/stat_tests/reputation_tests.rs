//! EH-525: the learned `reputation` relation and the `SOURCE RELIABILITY` stage that
//! reads it. Only independent outcomes of records the caller may read count; a named
//! evaluator's grant (EH-395) lets it evaluate, never read; below `min_support` a row
//! carries no numbers.

use super::retrieval_tests::{clone_executed, clone_record, evaluate_by, executed_template, sql};
use super::*;
use serde_json::{json, Value};

/// `n` executed `plan-hyde` records, `successes` of them independently judged good.
async fn labelled(h: &Harness, n: usize, successes: usize) {
    let template = executed_template(h).await;
    for i in 0..n {
        let id = format!("rec-{i}");
        clone_executed(h, &template, &id);
        evaluate_by(h, "evaluator", &id, i < successes).await;
    }
    // A self-report by the decider must never count.
    clone_record(h, &template, "rec-self");
    evaluate_by(h, "decider", "rec-self", false).await;
}

const OPTION_ROW: &str =
    "SELECT subject_kind, status, trials, successes FROM reputation WHERE subject = 'plan-hyde'";

#[tokio::test]
async fn reputations_count_only_independent_outcomes_the_caller_may_read() {
    let h = Harness::new().await;
    labelled(&h, 12, 9).await;
    assert_eq!(
        sql(&h, "decider", OPTION_ROW).await,
        vec![vec![
            json!("option"),
            json!("estimated"),
            json!(12),
            json!(9)
        ]],
        "twelve independent labels; the self-report is not one"
    );
    let agent = "SELECT trials FROM reputation WHERE subject_kind = 'agent' AND subject = 'retriever-agent'";
    assert_eq!(sql(&h, "decider", agent).await, vec![vec![json!(12)]]);
    let bounds = sql(
        &h,
        "decider",
        "SELECT lower, mean, upper FROM reputation WHERE subject = 'plan-hyde'",
    )
    .await;
    let [lower, mean, upper] = [0, 1, 2].map(|i| bounds[0][i].as_f64().unwrap());
    assert!(lower < mean && mean < upper && mean > 0.5, "{bounds:?}");
    for who in ["evaluator", "stranger"] {
        let count = sql(&h, who, "SELECT count(*) FROM reputation").await;
        assert_eq!(
            count,
            vec![vec![json!(0)]],
            "{who} reads no record, so no reputation"
        );
    }
}

#[tokio::test]
async fn below_the_support_floor_a_reputation_has_no_numbers() {
    let h = Harness::new().await;
    labelled(&h, 3, 3).await;
    assert_eq!(
        sql(&h, "decider", OPTION_ROW).await,
        vec![vec![
            json!("option"),
            json!("insufficient_history"),
            Value::Null,
            Value::Null
        ]]
    );
}

/// `SOURCE RELIABILITY 'plan-hyde'` over one row, as `who` runs it: (belief, lo, hi).
#[cfg(feature = "epistemic")]
fn reliability(h: &Harness, who: &str) -> (Option<f64>, Option<f64>, Option<f64>) {
    use eg_types::wire::UqlResult;
    let verified = VerifiedRequestContext::verified_for_test_in_tenant(who, TENANT);
    let authority = crate::server::access::CarrierAuthority::from_verified(&verified).unwrap();
    let views = super::super::stat_view::DecisionViews::of(h.store.clone(), &authority);
    let core = crate::graph::GraphCore::new();
    core.add_node(
        "d1".into(),
        rmp_serde::to_vec_named(&json!({"type": "Doc"})).unwrap(),
    );
    let graph = core.analysis_snapshot();
    let semantic = eg_core::compute::semantic::SemanticStore::new();
    let ctx = eg_plan::PlanCtx::new(&graph, &semantic).with_decisions(&views);
    let text = "MATCH () |> SOURCE RELIABILITY 'plan-hyde' |> RETURN belief, reliability_lo, reliability_hi";
    let stmt = eg_plan::uql::parse_statement(text, &eg_plan::uql::Params::new()).unwrap();
    let UqlResult::Rows { rows, .. } = eg_plan::uql::serve::run_statement(&stmt, &ctx).unwrap()
    else {
        panic!("rows")
    };
    (
        rows[0].channels[0],
        rows[0].channels[1],
        rows[0].channels[2],
    )
}

#[cfg(feature = "epistemic")]
#[tokio::test]
async fn source_reliability_is_learned_from_the_visible_log() {
    let h = Harness::new().await;
    labelled(&h, 12, 9).await;
    let (learned, lo, hi) = reliability(&h, "decider");
    let (learned, lo, hi) = (learned.unwrap(), lo.unwrap(), hi.unwrap());
    assert!(lo < learned && learned < hi, "{lo} < {learned} < {hi}");
    let (prior, none_lo, none_hi) = reliability(&h, "stranger");
    assert_eq!(
        (none_lo, none_hi),
        (None, None),
        "no visible outcome: the prior stands"
    );
    assert_ne!(prior, Some(learned));
}
