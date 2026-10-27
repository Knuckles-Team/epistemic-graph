//! Served ShEx data-selection proof for EG-FEDERATED-QUERY-R084.2.
//!
//! The handler must validate nonblank `data_graph` as inline Turtle, while an
//! empty or whitespace-only value must select the projected live graph.  The
//! two calls below use the same schema and shape map but deliberately opposite
//! data so that accidentally reusing one source would fail the assertion.

use super::*;

const PERSON_NAME_SCHEMA: &str = r#"{
  "type": "Schema",
  "shapes": [
    {
      "type": "Shape",
      "id": "http://example.org/PersonShape",
      "expression": {
        "type": "TripleConstraint",
        "predicate": "http://example.org/name",
        "valueExpr": {
          "type": "NodeConstraint",
          "datatype": "http://www.w3.org/2001/XMLSchema#string"
        },
        "min": 1,
        "max": 1
      }
    }
  ]
}"#;

const ALICE_SHAPE_MAP: [[&str; 2]; 1] =
    [["http://example.org/alice", "http://example.org/PersonShape"]];

fn shex_report(response: &Response) -> eg_types::rdf_report::ShexValidationReport {
    assert_ok(response);
    match &response.result {
        Some(ResultPayload::Json(value)) => serde_json::from_value(value.clone())
            .expect("ShexValidate JSON must decode as its wire report"),
        other => panic!(
            "expected ShexValidate JSON result, got {other:?}; error={:?}",
            response.error
        ),
    }
}

#[tokio::test]
async fn inline_and_live_data_selection() {
    let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
    let state = test_state();

    // Seed only the live graph's non-matching predicate through the normal
    // AddNode gateway.  The inline document below supplies the matching name.
    let live_node = Method::AddNode {
        node_id: "<http://example.org/alice>".into(),
        properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({
            "http://example.org/other": "live-only"
        }))
        .expect("encode live fixture properties"),
    };
    assert_ok(&dispatch_on_heap(&state, request(1, "__commons__", None, live_node)).await);

    let inline = dispatch_on_heap(
        &state,
        request(
            2,
            "__commons__",
            None,
            Method::ShexValidate {
                schema: PERSON_NAME_SCHEMA.into(),
                data_graph:
                    r#"<http://example.org/alice> <http://example.org/name> "Inline Alice" ."#
                        .into(),
                shape_map: ALICE_SHAPE_MAP
                    .iter()
                    .map(|[node, shape]| [(*node).into(), (*shape).into()])
                    .collect(),
            },
        ),
    )
    .await;
    let inline_report = shex_report(&inline);
    assert!(
        inline_report.conforms,
        "inline Turtle must satisfy the shape"
    );
    assert_eq!(inline_report.results.len(), 1);
    assert!(inline_report.results[0].conforms);

    // Whitespace is the live-graph sentinel.  The inline result above must not
    // have been imported, so the projected live graph still fails the required
    // `ex:name` constraint.
    let live = dispatch_on_heap(
        &state,
        request(
            3,
            "__commons__",
            None,
            Method::ShexValidate {
                schema: PERSON_NAME_SCHEMA.into(),
                data_graph: " \n\t ".into(),
                shape_map: ALICE_SHAPE_MAP
                    .iter()
                    .map(|[node, shape]| [(*node).into(), (*shape).into()])
                    .collect(),
            },
        ),
    )
    .await;
    let live_report = shex_report(&live);
    assert!(
        !live_report.conforms,
        "whitespace data_graph must select live RDF"
    );
    assert_eq!(live_report.results.len(), 1);
    assert!(!live_report.results[0].conforms);
}
