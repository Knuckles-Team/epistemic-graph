//! Golden output recorded by executing the deleted AU implementation.
use super::*;

// spec: MEDULLA-DURABLE-KERNEL-R082.6
#[test]
fn deleted_au_six_transaction_order_and_thresholds() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/association_mining_order.json"
    ))
    .unwrap();
    let transactions: Vec<Vec<String>> =
        serde_json::from_value(fixture["transactions"].clone()).unwrap();
    assert_eq!(transactions.len(), 6);
    for case in fixture["cases"].as_array().unwrap() {
        for algorithm in [Algorithm::Apriori, Algorithm::FpGrowth, Algorithm::Eclat] {
            let actual = mine_labeled(
                &transactions,
                case["min_support"].as_f64().unwrap(),
                case["min_confidence"].as_f64().unwrap(),
                algorithm,
            );
            let expected = case["rules"].as_array().unwrap();
            assert_eq!(
                actual.len(),
                expected.len(),
                "{} {algorithm:?}",
                case["name"]
            );
            for (rule, recorded) in actual.iter().zip(expected) {
                let labels = |key: &str| {
                    serde_json::from_value::<Vec<String>>(recorded[key].clone()).unwrap()
                };
                assert_eq!(rule.antecedent, labels("antecedent"));
                assert_eq!(rule.consequent, labels("consequent"));
                for (metric, value) in [
                    ("support", rule.support),
                    ("confidence", rule.confidence),
                    ("lift", rule.lift),
                ] {
                    assert!((value - recorded[metric].as_f64().unwrap()).abs() < 1e-14);
                }
            }
        }
    }
}

// spec: MEDULLA-DURABLE-KERNEL-R082.6
#[test]
fn minimum_count_and_zero_consequent_lift_boundaries() {
    assert_eq!(min_count(0.0, 6), 1);
    assert_eq!(min_count(0.34, 6), 3);
    assert_eq!(min_count(1.0 / 3.0, 6), 2);
    let supports = HashMap::from([(vec![0], 2), (vec![1], 0)]);
    let itemset = FrequentItemset {
        items: vec![0, 1],
        count: 0,
        support: 0.0,
    };
    let rule = rule_for_mask(&itemset, 1, &supports, 6, 0.0).unwrap();
    assert_eq!(rule.lift, 0.0);
    assert_eq!(rule.confidence, 0.0);
}
