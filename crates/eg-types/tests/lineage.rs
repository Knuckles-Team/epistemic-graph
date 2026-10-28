use eg_types::lineage::{ColumnLineageEdge, ColumnTransform};

// spec: EG-UNIFIED-DATA-PLANE-R763.1
#[test]
fn column_edge_round_trips() {
    let edge = ColumnLineageEdge {
        source_column: "pg:app.public.orders.amount".to_string(),
        target_column: "pg:app.public.daily_revenue.total".to_string(),
        transform: ColumnTransform::Aggregate,
        query_digest: "sha256:0f1e".to_string(),
    };
    let json = serde_json::to_string(&edge).unwrap();
    assert!(json.contains("\"aggregate\""));
    assert_eq!(
        serde_json::from_str::<ColumnLineageEdge>(&json).unwrap(),
        edge
    );
    for transform in ["identity", "expression", "aggregate", "filter_only"] {
        let value = serde_json::Value::String(transform.to_string());
        assert!(serde_json::from_value::<ColumnTransform>(value).is_ok());
    }
    let unknown = json.replace("aggregate", "window");
    assert!(serde_json::from_str::<ColumnLineageEdge>(&unknown).is_err());
}
