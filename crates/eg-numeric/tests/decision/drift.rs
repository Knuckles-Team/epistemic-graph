//! EH-025: a head reads only inside the feature ranges it was fitted on. A
//! candidate outside them is out of distribution and the reading names the
//! candidate and feature (the served ladder then abstains with
//! `insufficient_confidence`); inside them the head reads normally.

use eg_numeric::decision::features::FeatureMatrix;
use eg_numeric::decision::head_eval::{read_head, HeadReading};
use eg_types::decision::statistical::head::{
    DecisionHeadBody, FeatureStandardisation, FittedRegime, HeadKind, DECISION_HEAD_SCHEMA_VERSION,
};
use eg_types::decision::{QuantScaleTag, QuantisedValue};

use super::common::bounded;

const ONE: i64 = 1 << 32;

fn q(value: i64) -> QuantisedValue {
    QuantisedValue {
        scale: QuantScaleTag::Q32,
        value,
    }
}

fn head() -> DecisionHeadBody {
    DecisionHeadBody {
        schema_version: DECISION_HEAD_SCHEMA_VERSION,
        kind: HeadKind::WeightedFeatures,
        regime: FittedRegime::BanditLabel,
        feature_schema_digest: "sha256:schema".to_string(),
        standardisation: bounded(vec![FeatureStandardisation {
            center: q(0),
            scale: q(ONE),
            lower: q(-2 * ONE),
            upper: q(2 * ONE),
        }]),
        weights: bounded(vec![q(ONE)]),
        calibration: None,
        training_records_digest: "sha256:training".to_string(),
        n_training: 12,
        synthetic: true,
    }
}

fn matrix(values: Vec<i64>) -> FeatureMatrix {
    FeatureMatrix {
        candidate_ids: vec!["a".to_string(), "b".to_string()],
        feature_names: vec!["score".to_string()],
        values,
    }
}

#[test]
fn a_candidate_outside_the_fitted_range_is_out_of_distribution() {
    let reading = read_head(&head(), &matrix(vec![ONE, 3 * ONE])).expect("reads");
    assert_eq!(
        reading,
        HeadReading::OutOfDistribution {
            component_id: "b".to_string(),
            feature: "score".to_string(),
        }
    );
    let inside = read_head(&head(), &matrix(vec![ONE, -ONE])).expect("reads");
    let HeadReading::InDistribution(evaluated) = inside else {
        panic!("inside the fitted range the head reads");
    };
    assert_eq!(evaluated.logits, vec![1.0, -1.0]);
}
