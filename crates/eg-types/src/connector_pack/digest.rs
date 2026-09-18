//! The framed digest layout a pack is identified by.
//!
//! Every digest here is `Digest256::framed`, whose framing already length-
//! prefixes the domain and every field, so no separator can be forged out of
//! field content. The rules the layout depends on, in one place:
//!
//! * a digest nested inside another is framed as its 32 RAW bytes; the
//!   `sha256:<hex>` text form appears only in records and on the wire;
//! * every list of strings is sorted by UTF-8 byte order and de-duplicated
//!   before hashing, so declaration order is not identity;
//! * integers are 8-byte big-endian; an absent optional is an EMPTY field;
//! * an optional boolean is one byte: `0x00` absent, `0x01` false, `0x02` true,
//!   so "unknown" can never hash the same as "declared false".
//!
//! The archive layout and `server_package_version` are deliberately outside
//! `pack_digest`: re-packing identical content, or a server release that
//! serves it unchanged, is the same pack.

use super::annotations::{PackAnnotations, PackCost, PackModelFacts};
use super::index::{ConnectorPackIndex, PackEntry, PackEntryKind, PackRef};
use crate::agent_component::DeclaredLatency;
use crate::contract::Digest256;

/// Framing domain of one pack entry.
pub const ENTRY_DIGEST_DOMAIN: &[u8] = b"eg/connector-pack-entry/v1";
/// Framing domain of one entry's annotations.
pub const ANNOTATIONS_DIGEST_DOMAIN: &[u8] = b"eg/connector-pack-annotations/v1";
/// Framing domain of a declared model profile.
pub const MODEL_FACTS_DIGEST_DOMAIN: &[u8] = b"eg/cp-model-facts/v1";
/// Framing domain of one entry's outgoing references.
pub const REFERENCES_DIGEST_DOMAIN: &[u8] = b"eg/connector-pack-references/v1";
/// Framing domain of a whole pack.
pub const PACK_DIGEST_DOMAIN: &[u8] = b"eg/connector-pack/v1";

const PROVIDES_DOMAIN: &[u8] = b"eg/cp-provides/v1";
const REQUIRES_CAPABILITIES_DOMAIN: &[u8] = b"eg/cp-requires-capabilities/v1";
const MODALITIES_IN_DOMAIN: &[u8] = b"eg/cp-modalities-in/v1";
const MODALITIES_OUT_DOMAIN: &[u8] = b"eg/cp-modalities-out/v1";
const REQUIRED_SCOPES_DOMAIN: &[u8] = b"eg/cp-required-scopes/v1";

/// The wire token of one entry kind. Exactly the serde `snake_case` spelling,
/// because the digest and the wire must name a kind the same way.
pub fn entry_kind_token(kind: PackEntryKind) -> &'static str {
    match kind {
        PackEntryKind::McpServer => "mcp_server",
        PackEntryKind::Tool => "tool",
        PackEntryKind::Skill => "skill",
        PackEntryKind::Prompt => "prompt",
        PackEntryKind::Ontology => "ontology",
        PackEntryKind::Shapes => "shapes",
        PackEntryKind::ModelProfile => "model_profile",
        PackEntryKind::A2aCard => "a2a_card",
        PackEntryKind::Manifest => "manifest",
    }
}

/// `list(domain, items)`: the framed digest of a sorted, de-duplicated list.
pub fn list_digest(domain: &[u8], items: &[String]) -> Result<Digest256, String> {
    let mut sorted: Vec<&str> = items.iter().map(String::as_str).collect();
    sorted.sort_unstable();
    sorted.dedup();
    let fields: Vec<&[u8]> = sorted.iter().map(|item| item.as_bytes()).collect();
    Digest256::framed(domain, &fields)
}

/// One optional boolean, as its single digest byte.
fn tri(value: Option<bool>) -> [u8; 1] {
    [match value {
        None => 0,
        Some(false) => 1,
        Some(true) => 2,
    }]
}

/// One optional integer: eight big-endian bytes, or an empty field.
fn optional_u64(value: Option<u64>) -> Vec<u8> {
    value
        .map(|value| value.to_be_bytes().to_vec())
        .unwrap_or_default()
}

/// The digest of a declared model profile.
pub fn model_facts_digest(facts: &PackModelFacts) -> Result<Digest256, String> {
    Digest256::framed(
        MODEL_FACTS_DIGEST_DOMAIN,
        &[
            facts.provider.as_bytes(),
            facts.model_identity.as_bytes(),
            &facts.context_window_tokens.to_be_bytes(),
            &facts.max_output_tokens.to_be_bytes(),
            &tri(facts.supports_tools),
            &tri(facts.supports_structured_output),
            &tri(facts.supports_vision),
        ],
    )
}

/// The digest of one entry's annotations.
pub fn annotations_digest(annotations: &PackAnnotations) -> Result<Digest256, String> {
    let lists = annotation_lists(annotations)?;
    let cost = annotations.cost.as_ref();
    let currency = cost
        .map(|cost| cost.currency.as_bytes())
        .unwrap_or_default();
    let prices = declared_prices(cost);
    let latencies = declared_latencies(annotations.latency_declared.as_ref());
    let model = match &annotations.model {
        None => Vec::new(),
        Some(model) => model_facts_digest(model)?.as_bytes().to_vec(),
    };
    let contract_version = optional_text(annotations.contract_version.as_deref());
    let sdk_pin = optional_text(annotations.sdk_contract_pin.as_deref());
    Digest256::framed(
        ANNOTATIONS_DIGEST_DOMAIN,
        &[
            lists[0].as_bytes(),
            lists[1].as_bytes(),
            lists[2].as_bytes(),
            lists[3].as_bytes(),
            lists[4].as_bytes(),
            &tri(annotations.read_only_hint),
            &tri(annotations.destructive_hint),
            &tri(annotations.idempotent_hint),
            &tri(annotations.open_world_hint),
            contract_version,
            currency,
            prices[0].as_slice(),
            prices[1].as_slice(),
            prices[2].as_slice(),
            latencies[0].as_slice(),
            latencies[1].as_slice(),
            model.as_slice(),
            sdk_pin,
        ],
    )
}

/// The five annotation lists, each already sorted and de-duplicated by
/// [`list_digest`], in the order the layout fixes.
fn annotation_lists(annotations: &PackAnnotations) -> Result<[Digest256; 5], String> {
    Ok([
        list_digest(PROVIDES_DOMAIN, annotations.provides.as_slice())?,
        list_digest(
            REQUIRES_CAPABILITIES_DOMAIN,
            annotations.requires_capabilities.as_slice(),
        )?,
        list_digest(MODALITIES_IN_DOMAIN, annotations.modalities_in.as_slice())?,
        list_digest(MODALITIES_OUT_DOMAIN, annotations.modalities_out.as_slice())?,
        list_digest(
            REQUIRED_SCOPES_DOMAIN,
            annotations.required_scopes.as_slice(),
        )?,
    ])
}

/// The three declared prices, each eight big-endian bytes or an empty field.
fn declared_prices(cost: Option<&PackCost>) -> [Vec<u8>; 3] {
    [
        optional_u64(cost.and_then(|cost| cost.per_call_micros)),
        optional_u64(cost.and_then(|cost| cost.input_per_mtok_micros)),
        optional_u64(cost.and_then(|cost| cost.output_per_mtok_micros)),
    ]
}

/// The declared p50 and p95, in the same form.
fn declared_latencies(latency: Option<&DeclaredLatency>) -> [Vec<u8>; 2] {
    [
        optional_u64(latency.map(|latency| u64::from(latency.p50_ms))),
        optional_u64(latency.map(|latency| u64::from(latency.p95_ms))),
    ]
}

fn optional_text(value: Option<&str>) -> &[u8] {
    value.map(str::as_bytes).unwrap_or_default()
}

/// The digest of one entry's outgoing references, as `(uri, kind)` pairs
/// sorted by URI bytes and then kind token.
pub fn references_digest(references: &[PackRef]) -> Result<Digest256, String> {
    let mut pairs: Vec<(&str, &'static str)> = references
        .iter()
        .map(|reference| (reference.uri.as_str(), entry_kind_token(reference.kind)))
        .collect();
    pairs.sort_unstable();
    let mut fields: Vec<&[u8]> = Vec::with_capacity(pairs.len() * 2);
    for (uri, kind) in &pairs {
        fields.push(uri.as_bytes());
        fields.push(kind.as_bytes());
    }
    Digest256::framed(REFERENCES_DIGEST_DOMAIN, &fields)
}

/// The digest of one pack entry.
pub fn entry_digest(entry: &PackEntry) -> Result<Digest256, String> {
    let annotations = annotations_digest(&entry.annotations)?;
    let references = references_digest(entry.references.as_slice())?;
    let input = entry
        .input_schema
        .as_ref()
        .map(|section| section.sha256.as_bytes().to_vec())
        .unwrap_or_default();
    let output = entry
        .output_schema
        .as_ref()
        .map(|section| section.sha256.as_bytes().to_vec())
        .unwrap_or_default();
    Digest256::framed(
        ENTRY_DIGEST_DOMAIN,
        &[
            entry_kind_token(entry.kind).as_bytes(),
            entry.uri.as_bytes(),
            entry.name.as_bytes(),
            entry.media_type.as_bytes(),
            entry.body.sha256.as_bytes(),
            input.as_slice(),
            output.as_slice(),
            annotations.as_bytes(),
            references.as_bytes(),
        ],
    )
}

/// The digest of a whole pack: its connector, its server entry, and every
/// other entry's digest in URI order.
pub fn pack_digest(index: &ConnectorPackIndex) -> Result<Digest256, String> {
    let server = entry_digest(&index.server)?;
    let mut entries: Vec<&PackEntry> = index.entries.iter().collect();
    entries.sort_by(|left, right| left.uri.cmp(&right.uri));
    let digests = entries
        .iter()
        .map(|entry| entry_digest(entry))
        .collect::<Result<Vec<Digest256>, String>>()?;
    let count = (digests.len() as u64).to_be_bytes();
    let mut fields: Vec<&[u8]> = vec![
        index.connector.as_str().as_bytes(),
        server.as_bytes(),
        &count,
    ];
    fields.extend(digests.iter().map(|digest| digest.as_bytes().as_slice()));
    Digest256::framed(PACK_DIGEST_DOMAIN, &fields)
}
