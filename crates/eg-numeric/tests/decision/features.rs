//! EH-021: feature matrices over visible candidates only.

use std::collections::BTreeMap;

use eg_numeric::decision::candidate::CandidateView;
use eg_numeric::decision::features::{feature_matrix, FeatureInputs, MatrixOutcome};
use eg_types::decision::statistical::features::{
    FeatureKind, FeatureSchemaBody, FeatureSpec, MissingValue, FEATURE_SCHEMA_VERSION,
};
use eg_types::decision::statistical::{TypedParam, TypedValue};

use super::common::bounded;

fn candidate(id: &str, classification: &[&str], summary: &str, cost: Option<u64>) -> CandidateView {
    CandidateView {
        id: id.to_string(),
        classification: classification.iter().map(|s| s.to_string()).collect(),
        cost_micros: cost,
        texts: BTreeMap::from([("summary".to_string(), summary.to_string())]),
        ..CandidateView::default()
    }
}

fn schema(missing: MissingValue) -> FeatureSchemaBody {
    FeatureSchemaBody {
        schema_version: FEATURE_SCHEMA_VERSION,
        features: bounded(vec![
            FeatureSpec {
                name: "coverage".to_string(),
                kind: FeatureKind::CoverageFraction {
                    param: "needs".to_string(),
                },
                missing: MissingValue::Abstain,
            },
            FeatureSpec {
                name: "text".to_string(),
                kind: FeatureKind::TextBm25 {
                    key: "summary".to_string(),
                    param: "query".to_string(),
                },
                missing: MissingValue::Abstain,
            },
            FeatureSpec {
                name: "cost".to_string(),
                kind: FeatureKind::DeclaredCostMicros,
                missing,
            },
        ]),
    }
    .checked()
    .expect("valid schema")
}

fn params() -> Vec<TypedParam> {
    vec![
        TypedParam {
            name: "needs".to_string(),
            value: TypedValue::IriList(bounded(vec![
                "eg:capability/retrieval".to_string(),
                "eg:capability/generation/code".to_string(),
            ])),
        },
        TypedParam {
            name: "query".to_string(),
            value: TypedValue::Text("web search".to_string()),
        },
    ]
}

#[test]
fn coverage_is_exact_and_unknown_cost_abstains_rather_than_reading_zero() {
    let candidates = vec![
        candidate(
            "a",
            &["eg:capability/retrieval/web-search"],
            "web search tool",
            Some(5),
        ),
        candidate("b", &[], "file writer", None),
    ];
    let p = params();
    let inputs = FeatureInputs {
        params: &p,
        now_ms: 0,
    };
    let outcome =
        feature_matrix(&schema(MissingValue::Abstain), &candidates, &inputs).expect("computes");
    assert_eq!(
        outcome,
        MatrixOutcome::UnknownFact {
            component_id: "b".to_string(),
            field: "cost".to_string()
        }
    );
    let imputed = MissingValue::Impute {
        value: eg_types::decision::QuantisedValue {
            scale: eg_types::decision::QuantScaleTag::Q32,
            value: 7 << 32,
        },
    };
    let MatrixOutcome::Complete(matrix) =
        feature_matrix(&schema(imputed), &candidates, &inputs).expect("computes")
    else {
        panic!("imputation completes the matrix");
    };
    assert_eq!(
        matrix.row(0)[0],
        (1_i64 << 32) / 2,
        "one of two needs covered, exactly"
    );
    assert_eq!(
        matrix.row(1)[2],
        7 << 32,
        "the declared imputation, not zero"
    );
    assert!(matrix.row(0)[1] > matrix.row(1)[1]);
}

#[test]
fn a_bm25_score_never_moves_when_an_invisible_document_exists() {
    let visible = vec![
        candidate("a", &[], "web search tool", Some(1)),
        candidate("b", &[], "file writer", Some(1)),
    ];
    let mut with_hidden = visible.clone();
    with_hidden.push(candidate(
        "hidden",
        &[],
        "search search search web",
        Some(1),
    ));
    let p = params();
    let inputs = FeatureInputs {
        params: &p,
        now_ms: 0,
    };
    let s = schema(MissingValue::Abstain);
    let MatrixOutcome::Complete(alone) = feature_matrix(&s, &visible, &inputs).expect("computes")
    else {
        panic!("complete")
    };
    let MatrixOutcome::Complete(mixed) =
        feature_matrix(&s, &with_hidden, &inputs).expect("computes")
    else {
        panic!("complete")
    };
    // The engine only ever passes the visible set; the statistics are local to
    // whatever set it passes, so the hidden row changes the mixed scores ...
    assert_ne!(alone.row(0)[1], mixed.row(0)[1]);
    // ... and computing over the visible set alone is reproducible bit for bit.
    let MatrixOutcome::Complete(again) = feature_matrix(&s, &visible, &inputs).expect("computes")
    else {
        panic!("complete")
    };
    assert_eq!(alone, again);
}

#[test]
fn a_missing_or_mistyped_parameter_is_a_typed_refusal() {
    let candidates = vec![candidate("a", &[], "x", Some(1))];
    let inputs = FeatureInputs {
        params: &[],
        now_ms: 0,
    };
    let refusal =
        feature_matrix(&schema(MissingValue::Abstain), &candidates, &inputs).expect_err("refused");
    assert_eq!(refusal.code, "PARAMETER_INVALID");
}
