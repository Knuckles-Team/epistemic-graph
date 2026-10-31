//! MEDULLA-FINANCE-PRIMITIVES-R901.3 / R613.3 / R018.3: the served banking, regime,
//! execution, pairs and factor-rank methods run the shared kernels through the
//! real signed `dispatch` surface, and a refusal carries the kernel's stable code.
#![cfg(all(feature = "server", feature = "security", feature = "finance"))]

mod common;
#[path = "common/test_support.rs"]
mod test_support;

use medulla::protocol::{GraphType, Method, Response, ResultPayload};
use serde_json::Value;

const SECRET: &str = "medulla-fin-served-numeric-kernels-secret";
const GRAPH: &str = "fin-numeric-kernels";

async fn call(method: Method) -> Response {
    common::configure_authority();
    let state = test_support::durable_state(SECRET, common::current_isolation());
    let create = Method::CreateGraph {
        graph_name: GRAPH.to_string(),
        graph_type: GraphType::Global,
    };
    let created =
        test_support::dispatch(&state, test_support::request(SECRET, 1, GRAPH, create)).await;
    assert!(
        created.error.is_none(),
        "CreateGraph: {:?}",
        created.error_detail
    );
    test_support::dispatch(&state, test_support::request(SECRET, 2, GRAPH, method)).await
}

fn body(response: &Response) -> Value {
    match response.result.as_ref() {
        Some(ResultPayload::Raw(bytes)) => rmp_serde::from_slice(bytes).expect("msgpack result"),
        other => panic!(
            "expected a raw result, got {other:?} / {:?}",
            response.error_detail
        ),
    }
}

fn refusal_code_in_detail(response: &Response, code: &str) {
    assert!(response.result.is_none());
    assert_eq!(response.error.as_deref(), Some("INVALID_ARGUMENT"));
    let detail = response.error_detail.as_deref().unwrap_or_default();
    assert!(detail.contains(code), "{code} not in {detail:?}");
}

fn aml(amount_minor: i64, threshold_minor: i64) -> Method {
    Method::FinanceAmlScreen {
        rule_id: "ctr".into(),
        version: 3,
        threshold_minor,
        amount_minor,
    }
}

#[tokio::test(flavor = "multi_thread")]
// spec: MEDULLA-FINANCE-PRIMITIVES-R901.3
async fn banking_methods_run_their_kernels() {
    let structuring = body(&call(aml(8_500, 10_000)).await);
    assert_eq!(structuring["alert"]["kind"], "structuring");
    assert_eq!(structuring["alert"]["severity"], "warning");
    let threshold = body(&call(aml(12_000, 10_000)).await);
    assert_eq!(threshold["alert"]["severity"], "report-required");
    assert!(body(&call(aml(100, 10_000)).await)["alert"].is_null());

    let ratios = body(
        &call(Method::FinanceCapitalRatios {
            cet1: 60,
            at1: 20,
            tier2: 20,
            risk_weighted_assets: 1_000,
            total_exposure: 2_000,
        })
        .await,
    );
    assert_eq!(ratios["cet1_ratio"], 0.06);
    assert_eq!(ratios["total_capital_ratio"], 0.1);
    assert_eq!(ratios["cet1_meets_minimum"], true);

    let loss = call(Method::FinanceExpectedLoss {
        pd: 0.1,
        lgd: 0.5,
        ead_minor: 1_000,
    })
    .await;
    assert!(matches!(loss.result, Some(ResultPayload::Float(v)) if (v - 50.0).abs() < 1e-9));

    let legs = body(
        &call(Method::FinanceSettlementChain {
            origin: "A".into(),
            correspondents: vec!["B".into()],
            beneficiary: "C".into(),
            amount_minor: 500,
        })
        .await,
    );
    assert_eq!(legs.as_array().map(Vec::len), Some(2));
    assert_eq!(legs[1]["from"], "B");
}

#[tokio::test(flavor = "multi_thread")]
// spec: MEDULLA-FINANCE-PRIMITIVES-R901.3
async fn banking_refusals_carry_the_kernel_code() {
    refusal_code_in_detail(&call(aml(1, 0)).await, "AML_INPUT_INVALID");
    let rwa = call(Method::FinanceCapitalRatios {
        cet1: 1,
        at1: 0,
        tier2: 0,
        risk_weighted_assets: 0,
        total_exposure: 1,
    })
    .await;
    refusal_code_in_detail(&rwa, "CAPITAL_RWA_INVALID");
    let pd = call(Method::FinanceExpectedLoss {
        pd: 1.5,
        lgd: 0.5,
        ead_minor: 1,
    })
    .await;
    refusal_code_in_detail(&pd, "CREDIT_INPUT_INVALID");
}

#[tokio::test(flavor = "multi_thread")]
// spec: MEDULLA-FINANCE-PRIMITIVES-R613.3
async fn execution_methods_run_their_kernels() {
    let regime = body(
        &call(Method::FinanceRegimeChain {
            returns: vec![0.02, -0.02, 0.0, 0.02, 0.02, -0.02, 0.0, 0.02],
            threshold: 0.01,
            horizon: 2,
        })
        .await,
    );
    assert_eq!(regime["states"], serde_json::json!(["down", "flat", "up"]));
    assert_eq!(regime["transitions"], 7);
    assert_eq!(regime["solver_status"], "closed_form");

    let plan = body(
        &call(Method::FinanceExecutionSchedule {
            symbol: "AAPL".into(),
            total_shares: 1_000.0,
            horizon: 1.0,
            slices: 4,
            volatility: 1.0,
            risk_aversion: 1.0,
            temporary_impact: 1.0,
            permanent_impact: 0.0,
            continuous: false,
        })
        .await,
    );
    let traded: f64 = plan["trades"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t.as_f64().unwrap())
        .sum();
    assert!(
        (traded - 1_000.0).abs() < 1e-6,
        "trades sum to the position: {traded}"
    );
    assert_eq!(plan["solver_status"], "closed_form");

    let quote = body(
        &call(Method::FinanceMarketMakingQuote {
            symbol: "AAPL".into(),
            mid: 100.0,
            inventory: 0.0,
            risk_aversion: 0.1,
            volatility: 0.2,
            time_remaining: 1.0,
            order_arrival_decay: 1.5,
        })
        .await,
    );
    assert_eq!(quote["reservation_price"], 100.0);
    assert!(quote["bid"].as_f64().unwrap() < quote["ask"].as_f64().unwrap());

    let pairs = body(
        &call(Method::FinancePairsSignal {
            y_name: "Y".into(),
            x_name: "X".into(),
            y: vec![1.0, 2.0, 3.0, 4.0, 9.0],
            x: vec![1.0, 2.0, 3.0, 4.0, 5.0],
            hedge_ratio: 1.0,
            entry_z: 1.0,
        })
        .await,
    );
    assert_eq!(pairs["side"], "short_spread");
}

#[tokio::test(flavor = "multi_thread")]
// spec: MEDULLA-FINANCE-PRIMITIVES-R613.3
async fn execution_refusals_carry_the_kernel_code() {
    let bad = call(Method::FinanceExecutionSchedule {
        symbol: "AAPL".into(),
        total_shares: 1_000.0,
        horizon: 1.0,
        slices: 4,
        volatility: 1.0,
        risk_aversion: 0.0,
        temporary_impact: 1.0,
        permanent_impact: 0.0,
        continuous: false,
    })
    .await;
    refusal_code_in_detail(&bad, "EXEC_PARAM_INVALID: risk_aversion");
}

fn rank(signals: Vec<Vec<f64>>, forward: Vec<f64>) -> Method {
    let ids: Vec<String> = (0..signals.len()).map(|i| format!("f{i}")).collect();
    let keys = ids.iter().map(|id| vec![format!("src:{id}")]).collect();
    Method::FinanceFactorRank {
        factor_ids: ids,
        signals,
        provenance_keys: keys,
        forward_returns: forward,
    }
}

#[tokio::test(flavor = "multi_thread")]
// spec: MEDULLA-FINANCE-PRIMITIVES-R018.3
async fn factor_rank_orders_by_absolute_information_ratio() {
    let forward = vec![0.01, 0.02, 0.03, 0.04, 0.05, 0.06];
    let strong = forward.clone();
    let inverse: Vec<f64> = forward.iter().map(|v| -v).collect();
    let noisy = vec![0.3, -0.1, 0.2, -0.4, 0.1, 0.0];
    let ranked = body(&call(rank(vec![noisy, strong, inverse], forward)).await);
    let records = ranked.as_array().expect("records");
    assert_eq!(records.len(), 3);
    assert_eq!(records[2]["factor_id"], "f0", "the noisy factor ranks last");
    assert_eq!(records[0]["rank"], 1);
    assert_eq!(
        records[0]["input_provenance_keys"].as_array().map(Vec::len),
        Some(1)
    );
}

#[tokio::test(flavor = "multi_thread")]
// spec: MEDULLA-FINANCE-PRIMITIVES-R018.3
async fn factor_rank_abstains_on_thin_input() {
    refusal_code_in_detail(
        &call(rank(vec![vec![1.0]], vec![1.0])).await,
        "FACTOR_INSUFFICIENT_DATA",
    );
    refusal_code_in_detail(
        &call(rank(vec![], vec![])).await,
        "FACTOR_INSUFFICIENT_DATA",
    );
}
