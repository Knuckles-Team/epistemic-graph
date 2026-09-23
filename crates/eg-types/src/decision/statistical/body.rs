//! Where the statistical catalog kinds keep their bodies.
//!
//! A `FeatureSchema`, `DecisionHead` or `NlTemplate` component (a
//! `DecisionPolicy` uses the policy's own single `decision.policy` attribute)
//! is small, typed data the engine itself must read at decision
//! time. A component record stores no content, and the only engine-held body
//! store (Blob CAS) is written by pack import alone, so these bodies travel in
//! the component's own `attributes`: the canonical JSON of the body, hex-coded
//! and split into numbered chunks. `content_digest` is `sha256:<hex>` of the
//! JSON bytes, so the definition digest -- which covers both the attributes and
//! the content digest -- pins the body exactly, and a reader re-hashes before
//! it trusts a single byte.
//!
//! Hex, not raw JSON: an attribute value may not begin or end with whitespace
//! or hold a control character, and a chunk boundary inside a JSON string
//! could produce either.

use std::collections::BTreeMap;

use serde::de::DeserializeOwned;
use serde::Serialize;
use sha2::{Digest, Sha256};

/// The attribute-name prefix of every body chunk: `decision.body/00`, ...
pub const BODY_ATTRIBUTE_PREFIX: &str = "decision.body/";
/// Hex characters per chunk; one attribute value holds at most 4 KiB.
pub const BODY_CHUNK_HEX: usize = 4_000;
/// Chunks one body may occupy, leaving room for other attributes.
pub const MAX_BODY_CHUNKS: usize = 48;

/// A body ready to publish: its content digest and the attributes carrying it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedBody {
    pub content_digest: String,
    pub attributes: BTreeMap<String, String>,
    pub bytes: Vec<u8>,
}

/// `sha256:<hex>` of `bytes`.
pub fn content_digest_of(bytes: &[u8]) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(bytes)))
}

/// The canonical bytes of a body: compact JSON in field order.
pub fn canonical_body_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, String> {
    serde_json::to_vec(value).map_err(|error| format!("decision body does not encode: {error}"))
}

fn chunk_name(index: usize) -> String {
    format!("{BODY_ATTRIBUTE_PREFIX}{index:02}")
}

/// Encode `value` into body attributes.
pub fn encode_body<T: Serialize>(value: &T) -> Result<EncodedBody, String> {
    let bytes = canonical_body_bytes(value)?;
    let hex_text = hex::encode(&bytes);
    let chunks: Vec<&[u8]> = hex_text.as_bytes().chunks(BODY_CHUNK_HEX).collect();
    if chunks.len() > MAX_BODY_CHUNKS {
        return Err(format!(
            "decision body of {} bytes exceeds {MAX_BODY_CHUNKS} attribute chunks",
            bytes.len()
        ));
    }
    let attributes = chunks
        .into_iter()
        .enumerate()
        .map(|(index, chunk)| {
            (
                chunk_name(index),
                String::from_utf8_lossy(chunk).into_owned(),
            )
        })
        .collect();
    Ok(EncodedBody {
        content_digest: content_digest_of(&bytes),
        attributes,
        bytes,
    })
}

/// Reassemble a body's bytes from its attributes and check them against
/// `content_digest`. Chunks must be numbered densely from `00`.
pub fn body_bytes(
    content_digest: &str,
    attributes: &BTreeMap<String, String>,
) -> Result<Vec<u8>, String> {
    let mut hex_text = String::new();
    let mut index = 0usize;
    while let Some(chunk) = attributes.get(&chunk_name(index)) {
        hex_text.push_str(chunk);
        index += 1;
    }
    let declared = attributes
        .keys()
        .filter(|name| name.starts_with(BODY_ATTRIBUTE_PREFIX))
        .count();
    if index == 0 || declared != index {
        return Err("decision body chunks are missing or not densely numbered".to_string());
    }
    let bytes = hex::decode(hex_text).map_err(|_| "decision body chunk is not hex".to_string())?;
    if content_digest_of(&bytes) != content_digest {
        return Err("decision body does not match its content digest".to_string());
    }
    Ok(bytes)
}

/// Decode a typed body from its attributes, verifying the digest first.
pub fn decode_body<T: DeserializeOwned>(
    content_digest: &str,
    attributes: &BTreeMap<String, String>,
) -> Result<T, String> {
    let bytes = body_bytes(content_digest, attributes)?;
    serde_json::from_slice(&bytes)
        .map_err(|error| format!("decision body does not decode: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_body_round_trips_and_a_tampered_chunk_is_refused() {
        let value: Vec<String> = (0..900).map(|n| format!("feature {n}")).collect();
        let encoded = encode_body(&value).expect("encodes");
        assert!(encoded.attributes.len() > 1, "the sample spans chunks");
        let back: Vec<String> =
            decode_body(&encoded.content_digest, &encoded.attributes).expect("decodes");
        assert_eq!(back, value);

        let mut tampered = encoded.attributes.clone();
        let first = tampered.get_mut(&chunk_name(0)).expect("chunk 00");
        first.replace_range(0..2, "00");
        assert!(decode_body::<Vec<String>>(&encoded.content_digest, &tampered).is_err());

        let mut gapped = encoded.attributes;
        gapped.remove(&chunk_name(0));
        assert!(body_bytes(&encoded.content_digest, &gapped).is_err());
    }

    #[test]
    fn every_chunk_is_a_valid_attribute_value() {
        let encoded = encode_body(&" padded value ".repeat(700)).expect("encodes");
        for value in encoded.attributes.values() {
            assert!(value.len() <= 4 * 1024);
            assert_eq!(value.trim(), value);
            assert!(!value.chars().any(char::is_control));
        }
    }
}
