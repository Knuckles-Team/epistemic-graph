//! The shared MessagePack boundary of the agent hierarchy.
//!
//! The four agent layers -- component (L1), library entry (L2), graph (L3) and
//! template -- are published into the same owner file, with the same row
//! shape, under the same resource policy. Each of them had written that policy
//! out again: a private `MAX_AGENT_<LAYER>_ROW_BYTES`/`_ROW_ITEMS` pair and a
//! `eg_types::msgpack::MsgpackLimits::new(..)` literal per decode site, twelve
//! sites in all, every one of them the same three numbers.
//!
//! Twelve copies of a bound are twelve places for it to drift, and a bound
//! that drifts silently is the interesting failure: a layer whose row limit
//! was quietly raised accepts a row its siblings would refuse, in the file
//! they share. So the bound lives here, once, and the layers ask for it.
//!
//! The committed DOMAIN RESULT is the same story in the write direction:
//! three of the four layers store one, and each had written out the same four
//! steps -- encode bounded, wrap as `RecordBytes`, take its digest, carry it
//! under the layer's own schema id.
//!
//! # What does NOT live here
//!
//! The per-layer `decode_*` readers stay with their record types. What each
//! layer adds over this function is the part that genuinely differs -- which
//! type is being decoded, what it is called in a refusal, and its own
//! `validate()` -- and that belongs beside the type, not in a module that
//! knows nothing about any of them.

/// Most bytes one agent-hierarchy row may occupy.
///
/// Sized for the largest legal row rather than the typical one: a graph shape
/// carries up to `MAX_NODES` nodes, each with its own pinned contracts, and a
/// template carries a complete base agent draft.
const MAX_AGENT_ROW_BYTES: usize = 16 * 1024 * 1024;
/// Most MessagePack items one agent-hierarchy row may contain.
const MAX_AGENT_ROW_ITEMS: usize = 200_000;

/// The one decode policy every agent-hierarchy row is read under.
const AGENT_ROW_LIMITS: eg_types::msgpack::MsgpackLimits = eg_types::msgpack::MsgpackLimits::new(
    MAX_AGENT_ROW_BYTES,
    MAX_AGENT_ROW_ITEMS,
    eg_types::msgpack::DEFAULT_MAX_DEPTH,
);

/// Decode one agent-hierarchy row, bounded.
///
/// `what` names the row in the refusal -- "agent template row", "Agent Library
/// outbox event" -- because the decode error is deliberately opaque
/// (`MsgpackValidationError` reflects no parser internals), so the only thing
/// a caller can say about a bad row is which row it was.
///
/// Structural validation is NOT performed here: it is the decoded type's own
/// `validate()`, and which sub-record to validate differs per type (a
/// committed result validates the entry it carries, not itself). The layer
/// that names the type is the one that knows.
pub(super) fn decode<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
    what: &str,
) -> Result<T, String> {
    eg_types::msgpack::decode_bounded::<T>(bytes, AGENT_ROW_LIMITS)
        .map_err(|_| format!("{what} is invalid or exceeds resource limits"))
}

/// Decode one agent-hierarchy row, bounded, surrendering the decode error.
///
/// For the one caller that must distinguish "this row is not a `T`" from "this
/// row is unreadable": `record_result` falls back to a v6 row shape when the
/// current one does not parse, and a `String` refusal there would discard the
/// distinction the fallback turns on.
pub(super) fn try_decode<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
) -> Result<T, eg_types::msgpack::MsgpackValidationError> {
    eg_types::msgpack::decode_bounded::<T>(bytes, AGENT_ROW_LIMITS)
}

/// Wrap one layer's committed result as the `MutationResult` its receipt
/// carries.
///
/// Identical in every layer that stores one, because a domain result is the
/// same thing at each: the committed result encoded under
/// [`eg_storage::encode_bounded`], carried by the layer's own schema id with
/// its own payload digest beside it. Only `schema_id` and the label differ,
/// and the label only so a refusal names the layer.
pub(super) fn domain_result<T: serde::Serialize>(
    result: &T,
    schema_id: &str,
    what: &str,
) -> Result<eg_types::mutation::MutationResult, String> {
    let payload = eg_storage::encode_bounded(result, &format!("{what} domain result payload"))?;
    let payload = eg_types::contract::RecordBytes::new(payload)?;
    Ok(eg_types::mutation::MutationResult::DomainResult {
        schema_id: eg_types::contract::SchemaId::new(schema_id)?,
        payload_digest: payload.digest()?,
        payload,
    })
}
