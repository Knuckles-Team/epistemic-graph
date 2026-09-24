//! Golden `eg2.` canonical bodies for every catalog method (au-core R5).
//!
//! The server MACs `Method::canonical_body_bytes()` of the request it DECODED,
//! never the bytes a client sent, so a client signs correctly only if it
//! reproduces the server's re-serialization exactly. Each vector pairs a
//! client-shaped request with the canonical body the transport derives from
//! it; `tests/test_method_body_vectors.py` replays every vector through the
//! Python signer. Two request shapes are covered:
//!
//! * schema-synthesized, one per method: required fields only (serde defaults
//!   omitted), nullable fields explicitly null, keys in alphabetical order --
//!   the shape a pydantic `model_dump` sender produces, where field-order and
//!   default drift used to fail the MAC;
//! * typed: every contract-wave surface plus each method whose request
//!   validates its semantics while decoding, encoded from its Rust value.
//!
//! Every catalog method must be covered by at least one decodable vector.
//!
//! Each vector also carries the `eg2.` envelope MAC the engine computes for
//! that body under one fixed, published envelope (`envelope` in the file), so
//! a client in any language can assert its whole signature -- body hash and
//! envelope framing -- against the engine's own HMAC, not a restatement of it.

mod synth;

use eg_types::acl::RequestContextClaims;
use eg_types::protocol::{build_envelope_v2_bytes, Method};
use eg_types::test_support::method_bodies::{
    json_request_bytes, json_request_frame, typed_request_frame, typed_samples,
};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

use super::{pretty, Artifact};

const PATH: &str = "contract/fixtures/method_body_vectors.json";

/// The fixed envelope every vector's MAC is minted under. Test-only values:
/// the secret authenticates nothing outside this file.
const SECRET: &str = "method-body-vector-secret";
const REQUEST_ID: u64 = 7;
const GRAPH: &str = "graph:vectors";
const TIMESTAMP: u64 = 1_758_000_000;
const NONCE: &str = "method-body-vector-nonce";
const IDEMPOTENCY_KEY: &str = "method-body-vector:idempotency";

fn claims() -> RequestContextClaims {
    RequestContextClaims {
        principal: "service:vectors".to_string(),
        tenant: "tenant:vectors".to_string(),
        audience: "engine:vectors".to_string(),
        agent_id: "service:vectors".to_string(),
        roles: vec!["client".to_string()],
        scopes: vec!["graph:read".to_string()],
        policy_version: "policy:vectors".to_string(),
        delegation: Vec::new(),
        ..RequestContextClaims::default()
    }
}

/// The published envelope, as the file's `envelope` object.
fn envelope_json() -> serde_json::Value {
    serde_json::json!({
        "secret": SECRET,
        "request_id": REQUEST_ID,
        "graph": GRAPH,
        "context": claims(),
        "timestamp": TIMESTAMP,
        "nonce": NONCE,
        "idempotency_key": IDEMPOTENCY_KEY,
    })
}

/// The engine's `eg2.` MAC (`envelope_v2_mac`) of one canonical body under
/// the published envelope.
fn envelope_mac(method: &str, canonical: &[u8]) -> String {
    let bytes = build_envelope_v2_bytes(
        REQUEST_ID,
        GRAPH,
        method,
        &hex::encode(Sha256::digest(canonical)),
        &claims(),
        TIMESTAMP,
        NONCE,
        IDEMPOTENCY_KEY,
    );
    let mut mac =
        Hmac::<Sha256>::new_from_slice(SECRET.as_bytes()).expect("HMAC accepts any key length");
    mac.update(&bytes);
    hex::encode(mac.finalize().into_bytes())
}

/// One client request and the canonical body the server re-derives from it.
struct Vector {
    label: String,
    method: String,
    request: Vec<u8>,
    canonical: Vec<u8>,
}

/// Render the committed vector file.
pub(super) fn artifacts() -> Vec<Artifact> {
    let document = super::schema::method_request_document();
    let methods = document
        .get("methods")
        .and_then(serde_json::Value::as_object)
        .expect("the method request document carries a methods map");
    let mut vectors = schema_vectors(&synth::Synth::new(&document), methods);
    vectors.extend(typed_vectors());
    assert_every_method_covered(methods, &vectors);
    vectors.sort_by(|left, right| left.label.cmp(&right.label));
    vec![Artifact {
        path: PATH.to_string(),
        bytes: render(&vectors),
    }]
}

/// One schema-minimal vector per method whose minimal request decodes.
fn schema_vectors(
    synth: &synth::Synth<'_>,
    methods: &serde_json::Map<String, serde_json::Value>,
) -> Vec<Vector> {
    methods
        .iter()
        .filter_map(|(id, subschema)| {
            let request = synth.request(id, subschema);
            let frame = json_request_frame(&request);
            let canonical = Method::canonical_body_of_request_frame(&frame).ok()?;
            Some(Vector {
                label: id.clone(),
                method: id.clone(),
                request: json_request_bytes(&request),
                canonical,
            })
        })
        .collect()
}

/// One vector per typed sample; a typed sample that does not decode is a
/// defect in the sample itself.
fn typed_vectors() -> Vec<Vector> {
    typed_samples()
        .into_iter()
        .map(|(label, method)| {
            let request = method.canonical_body_bytes();
            let name = method.tag_name();
            let canonical = Method::canonical_body_of_request_frame(&typed_request_frame(method))
                .unwrap_or_else(|error| panic!("typed sample {label} does not decode: {error}"));
            Vector {
                label: format!("sample:{label}"),
                method: name,
                request,
                canonical,
            }
        })
        .collect()
}

fn assert_every_method_covered(
    methods: &serde_json::Map<String, serde_json::Value>,
    vectors: &[Vector],
) {
    let missing: Vec<&str> = methods
        .keys()
        .map(String::as_str)
        .filter(|id| !vectors.iter().any(|vector| vector.method == *id))
        .collect();
    assert!(
        missing.is_empty(),
        "no decodable method-body sample for {}: add a typed sample to \
         eg_types::test_support::method_bodies::typed_samples",
        missing.join(", ")
    );
}

fn render(vectors: &[Vector]) -> Vec<u8> {
    let rows: Vec<serde_json::Value> = vectors
        .iter()
        .map(|vector| {
            serde_json::json!({
                "label": vector.label,
                "method": vector.method,
                "request_msgpack": hex::encode(&vector.request),
                "canonical_sha256": hex::encode(Sha256::digest(&vector.canonical)),
                "canonical_len": vector.canonical.len(),
                "mac": envelope_mac(&vector.method, &vector.canonical),
            })
        })
        .collect();
    pretty(&serde_json::json!({
        "schema": "eg-method-body-vectors/v1",
        "envelope": envelope_json(),
        "vectors": rows,
    }))
}
