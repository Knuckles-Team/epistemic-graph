//! Samples and request frames for the `eg2.` method-body vectors
//! (`contract/fixtures/method_body_vectors.json`, rendered by `gen_contract`).
//!
//! A vector pairs the request a client sends with the canonical body the server
//! re-derives from it (`Method::canonical_body_of_request_frame`). Python replays
//! every vector through its own signer, so a client that restates field order,
//! defaults or widths instead of delegating to eg-types fails by name.

use crate::protocol::{Method, Request};

/// Typed samples beside the schema-synthesized ones: every contract-wave
/// surface, plus the methods whose request validates its own semantics while
/// decoding, which a schema-minimal sample therefore cannot satisfy.
pub fn typed_samples() -> Vec<(&'static str, Method)> {
    let mut samples = super::contract_wave::contract_wave_samples();
    samples.push(("Decide.harness_config", harness_config_sample()));
    samples.push((
        "ApplyChangeEnvelope",
        Method::ApplyChangeEnvelope {
            envelope: Box::new(super::change_envelope::minimal_envelope()),
        },
    ));
    samples.push((
        "SourceIngest",
        Method::SourceIngest {
            request: Box::new(super::source_ingestion::request()),
        },
    ));
    #[cfg(feature = "query")]
    samples.push(("SqlSourceBatch", sql_source_batch()));
    // A two-level tagged op (`family` + `op`): a schema-minimal sample
    // cannot pick a consistent pair, so the vector carries a real one.
    samples.push((
        "Identity",
        Method::Identity {
            op: crate::identity::IdentityOp::Config(crate::identity::ConfigOp::Get),
            stamp: None,
        },
    ));
    samples
}

/// A typed harness question exercises the shared client codec's new enum value.
fn harness_config_sample() -> Method {
    use crate::contract::BoundedVec;
    use crate::decision::{QuestionKind, TypedParam, TypedValue};

    let mut request = super::contract_wave::statistical::decide_request();
    request.question.kind = QuestionKind::HarnessConfig;
    request.question.question_id = "harness.configuration".into();
    request.head = None;
    request.params = BoundedVec::new(vec![
        TypedParam {
            name: "expected_trajectory_tokens".into(),
            value: TypedValue::Int(2048),
        },
        TypedParam {
            name: "model_profile_ref".into(),
            value: TypedValue::Iri("eg:model/example".into()),
        },
        TypedParam {
            name: "priority".into(),
            value: TypedValue::Text("interactive".into()),
        },
        TypedParam {
            name: "task_class".into(),
            value: TypedValue::Iri("eg:task/research".into()),
        },
        TypedParam {
            name: "usable_context_budget_tokens".into(),
            value: TypedValue::Int(4096),
        },
    ])
    .expect("five harness inputs fit the parameter bound");
    Method::Decide {
        request: Box::new(request),
    }
}

#[cfg(feature = "query")]
fn sql_source_batch() -> Method {
    use super::sql_source::{batch, SqlSourceTarget};
    use crate::contract::Digest256;
    use crate::storage_wire::{SqlSourceBatchRequest, SqlSourceCell};

    let target = SqlSourceTarget {
        table: "issues",
        columns: &["id", "done"],
        schema_version: 1,
        schema_digest: Digest256::from_bytes([3; 32]),
    };
    let rows = vec![vec![SqlSourceCell::Int(1), SqlSourceCell::Bool(false)]];
    Method::SqlSourceBatch {
        batch: SqlSourceBatchRequest::new(batch(&target, rows))
            .expect("the shared SQL source fixture is a valid batch"),
    }
}

/// A `{"method", "params"?}` JSON request as the MessagePack a client sends.
pub fn json_request_bytes(request: &serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(request).expect("a JSON request encodes")
}

/// The request frame for a `{"method", "params"?}` JSON request.
pub fn json_request_frame(request: &serde_json::Value) -> Vec<u8> {
    let mut frame = request
        .as_object()
        .cloned()
        .expect("a method request is a JSON object");
    // The frame fields the transport decodes beside the flattened `Method`.
    let fields = [
        ("id", serde_json::json!(0)),
        ("graph", serde_json::json!("")),
        ("auth_token", serde_json::json!("")),
        ("agent_id", serde_json::Value::Null),
    ];
    for (field, value) in fields {
        frame.insert(field.to_string(), value);
    }
    json_request_bytes(&serde_json::Value::Object(frame))
}

/// The request frame for a typed method.
pub fn typed_request_frame(method: Method) -> Vec<u8> {
    let request = Request {
        id: 0,
        graph: String::new(),
        auth_token: String::new(),
        agent_id: None,
        method,
    };
    rmp_serde::to_vec_named(&request).expect("a typed request frame encodes")
}
