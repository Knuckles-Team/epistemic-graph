//! Round-trip coverage for the architecture-view wire model.

use medulla_types::architecture_view::{ArchitectureView, ViewContainer, ViewEdge};

#[test]
fn view_types_round_trip() {
    let expected = ArchitectureView {
        containers: vec![ViewContainer {
            name: "orders-api".to_owned(),
            technology: "Rust service".to_owned(),
            support: vec![
                "services/orders/src/main.rs".to_owned(),
                "deploy/orders.yaml".to_owned(),
            ],
        }],
        edges: vec![ViewEdge {
            from: "orders-api".to_owned(),
            to: "payments-api".to_owned(),
            kind: "calls".to_owned(),
            call_count: 3,
        }],
    };

    let encoded = serde_json::to_vec(&expected).expect("architecture view serializes");
    let decoded: ArchitectureView =
        serde_json::from_slice(&encoded).expect("architecture view deserializes");

    assert_eq!(decoded, expected);
}
