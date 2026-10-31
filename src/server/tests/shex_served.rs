//! Served ShEx data-selection proof for MEDULLA-FEDERATED-QUERY-R084.2.
//!
//! The handler must validate nonblank `data_graph` as inline Turtle, while an
//! empty or whitespace-only value must select the projected live graph.  The
//! paired calls below use identical data with different schemas to prove that
//! whitespace actually selects the live graph rather than an empty graph.

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

const OTHER_VALUE_SCHEMA: &str = r#"{
  "type": "Schema",
  "shapes": [
    {
      "type": "Shape",
      "id": "http://example.org/OtherShape",
      "expression": {
        "type": "TripleConstraint",
        "predicate": "http://example.org/other",
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

const INLINE_PERSON_GRAPH: &str =
    r#"<http://example.org/alice> <http://example.org/name> "Inline Alice" ."#;
const LIVE_OTHER_GRAPH: &str =
    r#"<http://example.org/alice> <http://example.org/other> "live-only" ."#;
const ALICE_SHAPE_MAP: [[&str; 2]; 1] =
    [["http://example.org/alice", "http://example.org/PersonShape"]];
const ALICE_OTHER_SHAPE_MAP: [[&str; 2]; 1] =
    [["http://example.org/alice", "http://example.org/OtherShape"]];

fn shex_report(response: &Response) -> medulla_types::rdf_report::ShexValidationReport {
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

// spec: MEDULLA-FEDERATED-QUERY-R084.2
#[tokio::test]
async fn inline_and_live_data_selection() {
    let _env_read_lock = medulla_kernel::crypto::acquire_test_env_read_lock().await;
    let state = test_state();

    // Seed a genuine RDF literal through the RDF write surface. A plain AddNode
    // string property is not a typed RDF literal cell and is intentionally not
    // emitted by the live RDF exporter.
    #[cfg(feature = "shacl")]
    super::configure_icv_enforce(&state, 0, "__commons__").await;
    let live_seed = Method::AddTriples {
        turtle: LIVE_OTHER_GRAPH.into(),
        ntriples: String::new(),
    };
    assert_ok(&dispatch_on_heap(&state, request(1, "__commons__", None, live_seed)).await);

    let inline = dispatch_on_heap(
        &state,
        request(
            2,
            "__commons__",
            None,
            Method::ShexValidate {
                schema: PERSON_NAME_SCHEMA.into(),
                data_graph: INLINE_PERSON_GRAPH.into(),
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

    // Positively identify the selected source: the same inline document has no
    // `ex:other`, while the live graph contains it. An empty graph would fail
    // both cases and therefore could not satisfy this pair of assertions.
    let inline_other = dispatch_on_heap(
        &state,
        request(
            4,
            "__commons__",
            None,
            Method::ShexValidate {
                schema: OTHER_VALUE_SCHEMA.into(),
                data_graph: INLINE_PERSON_GRAPH.into(),
                shape_map: ALICE_OTHER_SHAPE_MAP
                    .iter()
                    .map(|[node, shape]| [(*node).into(), (*shape).into()])
                    .collect(),
            },
        ),
    )
    .await;
    let inline_other_report = shex_report(&inline_other);
    assert!(
        !inline_other_report.conforms,
        "the inline document must not contain the live-only predicate"
    );
    assert_eq!(inline_other_report.results.len(), 1);
    assert!(!inline_other_report.results[0].conforms);

    let live_other = dispatch_on_heap(
        &state,
        request(
            5,
            "__commons__",
            None,
            Method::ShexValidate {
                schema: OTHER_VALUE_SCHEMA.into(),
                data_graph: " \n\t ".into(),
                shape_map: ALICE_OTHER_SHAPE_MAP
                    .iter()
                    .map(|[node, shape]| [(*node).into(), (*shape).into()])
                    .collect(),
            },
        ),
    )
    .await;
    let live_other_report = shex_report(&live_other);
    assert!(
        live_other_report.conforms,
        "whitespace data_graph must use the seeded live ex:other value"
    );
    assert_eq!(live_other_report.results.len(), 1);
    assert!(live_other_report.results[0].conforms);
}
