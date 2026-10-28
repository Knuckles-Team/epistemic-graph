//! EG-FINANCE-PRIMITIVES-R900.3: the served `FinancePositionSize` and
//! `FinanceDistributionShift` routes run the shared `eg_numeric::sizing` kernels
//! through the real signed `dispatch` surface.
#![cfg(all(feature = "server", feature = "security", feature = "finance"))]

mod common;
#[path = "common/test_support.rs"]
mod test_support;

use epistemic_graph::protocol::{GraphType, Method, Response, ResultPayload};

const SECRET: &str = "eg-fin-r900-3-served-sizing-secret";

const GRAPH: &str = "fin-r900-3";

/// A served call: one signed dispatch against a fresh state holding the graph.
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

fn position(
    accuracy: f64,
    win_loss_ratio: f64,
    fraction: Option<f64>,
    max_risk: Option<f64>,
) -> Method {
    Method::FinancePositionSize {
        accuracy,
        win_loss_ratio,
        fraction,
        max_risk,
    }
}

fn float(response: &Response) -> f64 {
    match response.result.as_ref() {
        Some(ResultPayload::Float(value)) => *value,
        other => panic!(
            "expected a float result, got {other:?} / {:?} {:?}",
            response.error, response.error_detail
        ),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn position_size_runs_the_sizing_kernel_through_dispatch() {
    // p=0.6, b=2 -> full Kelly 0.4; quarter Kelly 0.1; capped at 0.05.
    let capped = call(position(0.6, 2.0, Some(0.25), Some(0.05))).await;
    assert!((float(&capped) - 0.05).abs() < 1e-12);
    // Defaults (half Kelly, 0.02 cap) apply when fraction/max_risk are omitted.
    let defaulted = call(position(0.6, 2.0, None, None)).await;
    assert!((float(&defaulted) - 0.02).abs() < 1e-12);
    // A losing edge sizes to zero.
    assert_eq!(
        float(&call(position(0.1, 1.0, Some(0.5), Some(0.5))).await),
        0.0
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn position_size_refuses_an_invalid_accuracy_with_the_kernel_code() {
    let refused = call(position(1.5, 2.0, None, None)).await;
    assert!(refused.result.is_none());
    assert_eq!(refused.error.as_deref(), Some("INVALID_ARGUMENT"));
    assert!(refused
        .error_detail
        .as_deref()
        .is_some_and(|d| d.contains("SIZING_INPUT_INVALID")));
}

#[tokio::test(flavor = "multi_thread")]
async fn distribution_shift_runs_the_ks_kernel_through_dispatch() {
    let reference: Vec<f64> = (0..200).map(|i| i as f64 / 200.0).collect();
    let moved: Vec<f64> = reference.iter().map(|v| v + 5.0).collect();
    let shifted = call(Method::FinanceDistributionShift {
        reference: reference.clone(),
        current: moved,
        threshold: 0.5,
    })
    .await;
    let same = call(Method::FinanceDistributionShift {
        reference: reference.clone(),
        current: reference,
        threshold: 0.5,
    })
    .await;
    for (response, expect_shifted) in [(&shifted, true), (&same, false)] {
        let Some(ResultPayload::Raw(bytes)) = response.result.as_ref() else {
            panic!("expected a raw result, got {:?}", response.error);
        };
        let value: serde_json::Value = rmp_serde::from_slice(bytes).expect("msgpack result");
        assert_eq!(value["shifted"], expect_shifted);
    }
    let empty = call(Method::FinanceDistributionShift {
        reference: vec![],
        current: vec![1.0],
        threshold: 0.5,
    })
    .await;
    assert!(empty
        .error_detail
        .as_deref()
        .is_some_and(|d| d.contains("SHIFT_SAMPLE_EMPTY")));
}
